//! MCP 2025-11-25 OAuth authorization-code and PKCE dependency adapter.

use std::{
    collections::BTreeSet,
    fs,
    io::Write,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chacha20poly1305::{
    KeyInit as _, XChaCha20Poly1305, XNonce,
    aead::{Aead as _, Payload},
};
use rand::Rng as _;
use reqwest::header::WWW_AUTHENTICATE;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::time::timeout;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_util::sync::CancellationToken;
use url::Url;
use zeroize::Zeroize as _;

use super::{
    DependencyOAuthStart, DependencyOAuthStatus, DependencyOAuthStatusKind,
    DependencyTransportConfig, McpDependency, McpDependencyError, Server, validate_secure_url,
};

const STATE_SCHEMA: u32 = 1;
const TRANSACTION_LIFETIME_MS: i64 = 10 * 60 * 1_000;
const REFRESH_SKEW_MS: i64 = 30_000;
const MAX_METADATA_BYTES: usize = 256 * 1_024;
const MAX_TOKEN_BYTES: usize = 64 * 1_024;
const MAX_SECRET_BYTES: usize = 16 * 1_024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum DurableStatus {
    Unauthorized,
    Pending,
    ExchangeDispatched,
    Authorized,
    RefreshDispatched,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PendingAuthorization {
    transaction_id: String,
    state_hash: String,
    verifier_reference: String,
    discovery_hash: String,
    resource: String,
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    client_id: String,
    redirect_uri: String,
    scope: Vec<String>,
    created_at_ms: i64,
    expires_at_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct TokenReference {
    reference: String,
    token_endpoint: String,
    resource: String,
    client_id: String,
    scope: Vec<String>,
    expires_at_ms: Option<i64>,
    refreshable: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DurableOAuthState {
    schema_version: u32,
    server_id: String,
    server_identity: String,
    status: DurableStatus,
    pending: Option<PendingAuthorization>,
    token: Option<TokenReference>,
}

#[derive(Deserialize, Serialize)]
struct StoredOAuthState {
    checksum: String,
    state: DurableOAuthState,
}

#[derive(Clone, Deserialize, Serialize)]
struct StoredSecret {
    reference: String,
    access_token: Option<String>,
    refresh_token: Option<String>,
    verifier: Option<String>,
}

#[derive(Deserialize, Serialize)]
struct AuthenticatedSecret {
    schema_version: u32,
    reference: String,
    nonce: String,
    ciphertext: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProtectedResourceMetadata {
    resource: String,
    authorization_servers: Vec<String>,
    #[serde(default)]
    scopes_supported: Vec<String>,
    #[serde(default)]
    bearer_methods_supported: Vec<String>,
}

#[derive(Deserialize)]
struct AuthorizationServerMetadata {
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    #[serde(default)]
    code_challenge_methods_supported: Vec<String>,
    #[serde(default)]
    grant_types_supported: Vec<String>,
    #[serde(default)]
    token_endpoint_auth_methods_supported: Vec<String>,
    #[serde(default)]
    protected_resources: Vec<String>,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    expires_in: Option<u64>,
    refresh_token: Option<String>,
    scope: Option<String>,
}

struct OAuthConfig<'a> {
    url: &'a str,
    authorization_server: &'a str,
    client_id: &'a str,
    client_secret_environment: Option<&'a str>,
    redirect_uri: &'a str,
    scopes: &'a [String],
}

struct Discovery {
    resource: String,
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    scope: Vec<String>,
    hash: String,
}

impl McpDependency {
    #[allow(
        clippy::too_many_lines,
        reason = "discovery, listener binding, and durable pending-state publication form one fail-closed transaction"
    )]
    pub(super) async fn oauth_begin(
        &self,
        server_id: &str,
        cancellation_id: &str,
    ) -> Result<DependencyOAuthStart, McpDependencyError> {
        if cancellation_id.is_empty() || cancellation_id.len() > 1_024 {
            return Err(McpDependencyError::InvalidRequest);
        }
        let server = self.server(server_id)?;
        let config = oauth_config(&server)?;
        let lock = self.oauth_lock(server_id)?;
        let _guard = lock.lock().await;
        if let Some(state) = self.read_oauth_state(&server)?
            && state.status == DurableStatus::Pending
            && state
                .pending
                .as_ref()
                .is_some_and(|pending| pending.expires_at_ms > now_ms().unwrap_or(i64::MAX))
        {
            return Err(McpDependencyError::OAuthTransaction);
        }
        let cancellation = self.register_oauth_cancellation(cancellation_id).await?;
        let discovered = tokio::select! {
            () = cancellation.cancelled() => Err(McpDependencyError::Cancelled),
            result = timeout(self.config.request_timeout, self.discover_oauth(&config)) => {
                result.map_err(|_| McpDependencyError::Timeout)?
            }
        };
        self.active.lock().await.remove(cancellation_id);
        let discovered = discovered?;
        let verifier = random_urlsafe(64);
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let state = random_urlsafe(32);
        let transaction_id = random_urlsafe(24);
        let verifier_reference = format!("oauth-pkce-{}", random_urlsafe(18));
        self.write_secret(
            &verifier_reference,
            &StoredSecret {
                reference: verifier_reference.clone(),
                access_token: None,
                refresh_token: None,
                verifier: Some(verifier),
            },
        )?;
        let created_at_ms = now_ms()?;
        let expires_at_ms = created_at_ms
            .checked_add(TRANSACTION_LIFETIME_MS)
            .ok_or(McpDependencyError::OAuthState)?;
        let pending = PendingAuthorization {
            transaction_id: transaction_id.clone(),
            state_hash: blake3::hash(state.as_bytes()).to_hex().to_string(),
            verifier_reference,
            discovery_hash: discovered.hash,
            resource: discovered.resource.clone(),
            issuer: discovered.issuer,
            authorization_endpoint: discovered.authorization_endpoint.clone(),
            token_endpoint: discovered.token_endpoint,
            client_id: config.client_id.to_owned(),
            redirect_uri: config.redirect_uri.to_owned(),
            scope: discovered.scope.clone(),
            created_at_ms,
            expires_at_ms,
        };
        let mut authorization_url = Url::parse(&discovered.authorization_endpoint)
            .map_err(|_| McpDependencyError::OAuthMetadata)?;
        {
            let mut query = authorization_url.query_pairs_mut();
            query
                .append_pair("response_type", "code")
                .append_pair("client_id", config.client_id)
                .append_pair("redirect_uri", config.redirect_uri)
                .append_pair("code_challenge", &challenge)
                .append_pair("code_challenge_method", "S256")
                .append_pair("state", &state)
                .append_pair("resource", &discovered.resource);
            if !discovered.scope.is_empty() {
                query.append_pair("scope", &discovered.scope.join(" "));
            }
        }
        self.write_oauth_state(
            &server,
            &DurableOAuthState {
                schema_version: STATE_SCHEMA,
                server_id: server_id.to_owned(),
                server_identity: server.identity.clone(),
                status: DurableStatus::Pending,
                pending: Some(pending.clone()),
                token: None,
            },
        )?;
        if let Err(error) = self
            .install_callback_listener(
                &server,
                transaction_id.clone(),
                config.redirect_uri,
                expires_at_ms,
            )
            .await
        {
            let mut failed = self
                .read_oauth_state(&server)?
                .ok_or(McpDependencyError::OAuthState)?;
            self.invalidate_pending(&server, &mut failed, &pending)?;
            return Err(error);
        }
        Ok(DependencyOAuthStart {
            server_id: server_id.to_owned(),
            transaction_id,
            authorization_url: authorization_url.into(),
            expires_at_ms,
            configuration_hash: server.identity.clone(),
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "callback validation, ambiguous-dispatch sealing, exchange, and token publication are one security transaction"
    )]
    pub(super) async fn oauth_complete(
        &self,
        server_id: &str,
        transaction_id: &str,
        callback_uri: &str,
        cancellation_id: &str,
    ) -> Result<DependencyOAuthStatus, McpDependencyError> {
        if transaction_id.is_empty()
            || transaction_id.len() > 256
            || callback_uri.len() > 8_192
            || cancellation_id.is_empty()
        {
            return Err(McpDependencyError::InvalidRequest);
        }
        let server = self.server(server_id)?;
        let config = oauth_config(&server)?;
        let lock = self.oauth_lock(server_id)?;
        let _guard = lock.lock().await;
        let mut state = self
            .read_oauth_state(&server)?
            .ok_or(McpDependencyError::OAuthTransaction)?;
        let pending = state
            .pending
            .clone()
            .filter(|pending| {
                state.status == DurableStatus::Pending
                    && pending.transaction_id == transaction_id
                    && pending.expires_at_ms >= now_ms().unwrap_or(i64::MAX)
            })
            .ok_or(McpDependencyError::OAuthTransaction)?;
        validate_pending_config(&pending, &config)?;
        let callback =
            Url::parse(callback_uri).map_err(|_| McpDependencyError::OAuthTransaction)?;
        let configured_redirect =
            Url::parse(config.redirect_uri).map_err(|_| McpDependencyError::OAuthTransaction)?;
        if callback.scheme() != configured_redirect.scheme()
            || callback.host_str() != configured_redirect.host_str()
            || callback.port_or_known_default() != configured_redirect.port_or_known_default()
            || callback.path() != configured_redirect.path()
            || callback.fragment().is_some()
        {
            return Err(McpDependencyError::OAuthTransaction);
        }
        let parameters = callback
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();
        if parameters.iter().any(|(key, _)| key == "error") {
            self.invalidate_pending(&server, &mut state, &pending)?;
            return Err(McpDependencyError::OAuthTransaction);
        }
        let code = exactly_one(&parameters, "code")?;
        let returned_state = exactly_one(&parameters, "state")?;
        if blake3::hash(returned_state.as_bytes()).to_hex().as_str() != pending.state_hash {
            return Err(McpDependencyError::OAuthTransaction);
        }
        let secret = self.read_secret(&pending.verifier_reference)?;
        let verifier = secret
            .verifier
            .as_deref()
            .ok_or(McpDependencyError::OAuthState)?;
        state.status = DurableStatus::ExchangeDispatched;
        self.write_oauth_state(&server, &state)?;
        let cancellation = self.register_oauth_cancellation(cancellation_id).await?;
        let exchange = tokio::select! {
            () = cancellation.cancelled() => Err(McpDependencyError::Cancelled),
            result = timeout(
                self.config.request_timeout,
                self.exchange_code(&config, &pending, code, verifier),
            ) => result.map_err(|_| McpDependencyError::Timeout)?,
        };
        self.active.lock().await.remove(cancellation_id);
        let token = match exchange {
            Ok(token) => token,
            Err(error) => {
                self.invalidate_pending(&server, &mut state, &pending)?;
                return Err(error);
            }
        };
        let token_reference = format!("oauth-token-{}", random_urlsafe(18));
        let expires_at_ms = token.expires_in.map(expires_at_ms).transpose()?;
        let scopes = parse_token_scopes(token.scope.as_deref(), &pending.scope)?;
        self.write_secret(
            &token_reference,
            &StoredSecret {
                reference: token_reference.clone(),
                access_token: Some(token.access_token),
                refresh_token: token.refresh_token.clone(),
                verifier: None,
            },
        )?;
        self.remove_secret(&pending.verifier_reference)?;
        state.status = DurableStatus::Authorized;
        state.pending = None;
        state.token = Some(TokenReference {
            reference: token_reference,
            token_endpoint: pending.token_endpoint,
            resource: pending.resource,
            client_id: pending.client_id,
            scope: scopes.clone(),
            expires_at_ms,
            refreshable: token.refresh_token.is_some(),
        });
        self.write_oauth_state(&server, &state)?;
        Ok(DependencyOAuthStatus {
            server_id: server_id.to_owned(),
            status: DependencyOAuthStatusKind::Authorized,
            transaction_id: None,
            expires_at_ms,
            scopes,
            configuration_hash: server.identity.clone(),
        })
    }

    pub(super) fn oauth_redacted_status(
        &self,
        server_id: &str,
    ) -> Result<DependencyOAuthStatus, McpDependencyError> {
        let server = self.server(server_id)?;
        oauth_config(&server)?;
        let state = self.read_oauth_state(&server)?;
        Ok(redact(server_id, &server.identity, state.as_ref()))
    }

    pub(super) async fn oauth_cancel(
        &self,
        server_id: &str,
        transaction_id: &str,
    ) -> Result<DependencyOAuthStatus, McpDependencyError> {
        let server = self.server(server_id)?;
        oauth_config(&server)?;
        let lock = self.oauth_lock(server_id)?;
        let _guard = lock.lock().await;
        let mut state = self
            .read_oauth_state(&server)?
            .ok_or(McpDependencyError::OAuthTransaction)?;
        let pending = state
            .pending
            .clone()
            .filter(|pending| pending.transaction_id == transaction_id)
            .ok_or(McpDependencyError::OAuthTransaction)?;
        if let Some(callback) = self
            .oauth_callbacks
            .lock()
            .await
            .remove(&callback_key(server_id, transaction_id))
        {
            callback.cancel();
        }
        self.remove_secret(&pending.verifier_reference)?;
        state.status = DurableStatus::Unauthorized;
        state.pending = None;
        self.write_oauth_state(&server, &state)?;
        Ok(redact(server_id, &server.identity, Some(&state)))
    }

    pub(super) async fn oauth_access_token(
        &self,
        server: &Server,
    ) -> Result<String, McpDependencyError> {
        let config = oauth_config(server)?;
        let lock = self.oauth_lock(&server.config.id)?;
        let _guard = lock.lock().await;
        let mut state = self
            .read_oauth_state(server)?
            .ok_or(McpDependencyError::OAuthRequired)?;
        let mut token = state
            .token
            .clone()
            .filter(|_| state.status == DurableStatus::Authorized)
            .ok_or(McpDependencyError::OAuthRequired)?;
        validate_token_config(&token, &config)?;
        let now = now_ms()?;
        if token
            .expires_at_ms
            .is_some_and(|expiry| expiry <= now.saturating_add(REFRESH_SKEW_MS))
        {
            if !token.refreshable {
                return Err(McpDependencyError::OAuthRequired);
            }
            let current = self.read_secret(&token.reference)?;
            let refresh = current
                .refresh_token
                .as_deref()
                .ok_or(McpDependencyError::OAuthRequired)?;
            state.status = DurableStatus::RefreshDispatched;
            self.write_oauth_state(server, &state)?;
            let refreshed = timeout(
                self.config.request_timeout,
                self.refresh_token(&config, &token, refresh),
            )
            .await
            .map_err(|_| McpDependencyError::Timeout)??;
            let new_reference = format!("oauth-token-{}", random_urlsafe(18));
            let new_expiry = refreshed.expires_in.map(expires_at_ms).transpose()?;
            let scopes = parse_token_scopes(refreshed.scope.as_deref(), &token.scope)?;
            self.write_secret(
                &new_reference,
                &StoredSecret {
                    reference: new_reference.clone(),
                    access_token: Some(refreshed.access_token),
                    refresh_token: refreshed
                        .refresh_token
                        .or_else(|| current.refresh_token.clone()),
                    verifier: None,
                },
            )?;
            let old_reference = token.reference.clone();
            token.reference = new_reference;
            token.expires_at_ms = new_expiry;
            token.scope = scopes;
            state.token = Some(token.clone());
            state.status = DurableStatus::Authorized;
            self.write_oauth_state(server, &state)?;
            let _ = self.remove_secret(&old_reference);
        }
        self.read_secret(&token.reference)?
            .access_token
            .filter(|value| valid_secret(value))
            .ok_or(McpDependencyError::OAuthState)
    }

    async fn register_oauth_cancellation(
        &self,
        cancellation_id: &str,
    ) -> Result<CancellationToken, McpDependencyError> {
        let token = CancellationToken::new();
        if self
            .active
            .lock()
            .await
            .insert(cancellation_id.to_owned(), token.clone())
            .is_some()
        {
            return Err(McpDependencyError::DuplicateCancellation);
        }
        Ok(token)
    }

    fn oauth_lock(&self, server_id: &str) -> Result<ArcLock, McpDependencyError> {
        self.oauth_locks
            .get(server_id)
            .cloned()
            .map(ArcLock)
            .ok_or(McpDependencyError::OAuthRequired)
    }

    async fn discover_oauth(
        &self,
        config: &OAuthConfig<'_>,
    ) -> Result<Discovery, McpDependencyError> {
        let challenge = self
            .client
            .get(config.url)
            .header("accept", "application/json")
            .send()
            .await
            .map_err(|_| McpDependencyError::Transport)?;
        let challenged_metadata = if challenge.status() == reqwest::StatusCode::UNAUTHORIZED {
            parse_resource_metadata_challenge(challenge.headers().get(WWW_AUTHENTICATE))?
        } else {
            None
        };
        let mut candidates = Vec::new();
        if let Some(value) = challenged_metadata {
            candidates.push(value);
        } else {
            candidates.extend(protected_resource_candidates(config.url)?);
        }
        let mut resource_metadata = None;
        for candidate in candidates {
            if let Ok(value) = self
                .fetch_json::<ProtectedResourceMetadata>(&candidate)
                .await
            {
                resource_metadata = Some(value);
                break;
            }
        }
        let resource_metadata = resource_metadata.ok_or(McpDependencyError::OAuthMetadata)?;
        if canonical_resource(&resource_metadata.resource)? != canonical_resource(config.url)?
            || !resource_metadata
                .authorization_servers
                .iter()
                .any(|value| same_url(value, config.authorization_server))
            || resource_metadata
                .bearer_methods_supported
                .iter()
                .any(|value| value != "header")
        {
            return Err(McpDependencyError::OAuthMetadata);
        }
        let issuer = canonical_issuer(config.authorization_server)?;
        let mut server_metadata = None;
        for candidate in authorization_metadata_candidates(&issuer)? {
            if let Ok(value) = self
                .fetch_json::<AuthorizationServerMetadata>(&candidate)
                .await
                && same_url(&value.issuer, &issuer)
            {
                server_metadata = Some(value);
                break;
            }
        }
        let server_metadata = server_metadata.ok_or(McpDependencyError::OAuthMetadata)?;
        validate_authorization_metadata(&server_metadata, config, &resource_metadata.resource)?;
        let scope = if config.scopes.is_empty() {
            normalize_scopes(&resource_metadata.scopes_supported)?
        } else {
            let configured = normalize_scopes(config.scopes)?;
            if !resource_metadata.scopes_supported.is_empty()
                && configured.iter().any(|scope| {
                    !resource_metadata
                        .scopes_supported
                        .iter()
                        .any(|value| value == scope)
                })
            {
                return Err(McpDependencyError::OAuthMetadata);
            }
            configured
        };
        let canonical = serde_json::to_vec(&(
            &resource_metadata.resource,
            &issuer,
            &server_metadata.authorization_endpoint,
            &server_metadata.token_endpoint,
            config.client_id,
            config.redirect_uri,
            &scope,
        ))
        .map_err(|_| McpDependencyError::OAuthMetadata)?;
        Ok(Discovery {
            resource: canonical_resource(&resource_metadata.resource)?,
            issuer,
            authorization_endpoint: server_metadata.authorization_endpoint,
            token_endpoint: server_metadata.token_endpoint,
            scope,
            hash: blake3::hash(&canonical).to_hex().to_string(),
        })
    }

    async fn fetch_json<T: for<'de> Deserialize<'de>>(
        &self,
        url: &str,
    ) -> Result<T, McpDependencyError> {
        validate_secure_url(url, false).map_err(|_| McpDependencyError::OAuthMetadata)?;
        let response = self
            .client
            .get(url)
            .header("accept", "application/json")
            .send()
            .await
            .map_err(|_| McpDependencyError::Transport)?;
        if !response.status().is_success() {
            return Err(McpDependencyError::OAuthMetadata);
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|_| McpDependencyError::Transport)?;
        if bytes.is_empty() || bytes.len() > MAX_METADATA_BYTES {
            return Err(McpDependencyError::OAuthMetadata);
        }
        serde_json::from_slice(&bytes).map_err(|_| McpDependencyError::OAuthMetadata)
    }

    async fn exchange_code(
        &self,
        config: &OAuthConfig<'_>,
        pending: &PendingAuthorization,
        code: &str,
        verifier: &str,
    ) -> Result<TokenResponse, McpDependencyError> {
        let mut form = vec![
            ("grant_type", "authorization_code".to_owned()),
            ("code", code.to_owned()),
            ("redirect_uri", pending.redirect_uri.clone()),
            ("client_id", pending.client_id.clone()),
            ("code_verifier", verifier.to_owned()),
            ("resource", pending.resource.clone()),
        ];
        append_client_secret(&mut form, config)?;
        self.send_token_request(&pending.token_endpoint, &form)
            .await
    }

    async fn refresh_token(
        &self,
        config: &OAuthConfig<'_>,
        token: &TokenReference,
        refresh_token: &str,
    ) -> Result<TokenResponse, McpDependencyError> {
        let mut form = vec![
            ("grant_type", "refresh_token".to_owned()),
            ("refresh_token", refresh_token.to_owned()),
            ("client_id", token.client_id.clone()),
            ("resource", token.resource.clone()),
        ];
        if !token.scope.is_empty() {
            form.push(("scope", token.scope.join(" ")));
        }
        append_client_secret(&mut form, config)?;
        self.send_token_request(&token.token_endpoint, &form).await
    }

    async fn send_token_request(
        &self,
        endpoint: &str,
        form: &[(&str, String)],
    ) -> Result<TokenResponse, McpDependencyError> {
        validate_secure_url(endpoint, false).map_err(|_| McpDependencyError::OAuthMetadata)?;
        let response = self
            .client
            .post(endpoint)
            .header("accept", "application/json")
            .form(form)
            .send()
            .await
            .map_err(|_| McpDependencyError::Transport)?;
        if !response.status().is_success() {
            return Err(McpDependencyError::OAuthToken);
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|_| McpDependencyError::Transport)?;
        if bytes.is_empty() || bytes.len() > MAX_TOKEN_BYTES {
            return Err(McpDependencyError::OAuthToken);
        }
        let token: TokenResponse =
            serde_json::from_slice(&bytes).map_err(|_| McpDependencyError::OAuthToken)?;
        if !token.token_type.eq_ignore_ascii_case("bearer")
            || !valid_secret(&token.access_token)
            || token
                .refresh_token
                .as_deref()
                .is_some_and(|value| !valid_secret(value))
            || token.expires_in == Some(0)
        {
            return Err(McpDependencyError::OAuthToken);
        }
        Ok(token)
    }

    fn state_path(&self, server: &Server) -> PathBuf {
        self.config
            .oauth_state_root
            .join(format!("{}.json", server.config.id))
    }

    fn secret_path(&self, reference: &str) -> Result<PathBuf, McpDependencyError> {
        if !valid_reference(reference) {
            return Err(McpDependencyError::OAuthState);
        }
        Ok(self
            .config
            .oauth_state_root
            .join("secrets")
            .join(format!("{reference}.json")))
    }

    fn read_oauth_state(
        &self,
        server: &Server,
    ) -> Result<Option<DurableOAuthState>, McpDependencyError> {
        let path = self.state_path(server);
        let state = read_recoverable_json::<StoredOAuthState>(&path)?
            .map(|stored| {
                let bytes = serde_json::to_vec(&stored.state)
                    .map_err(|_| McpDependencyError::OAuthState)?;
                if blake3::hash(&bytes).to_hex().as_str() != stored.checksum {
                    return Err(McpDependencyError::OAuthState);
                }
                Ok(stored.state)
            })
            .transpose()?;
        if let Some(state) = &state {
            validate_durable_state(state, server)?;
        }
        Ok(state)
    }

    fn write_oauth_state(
        &self,
        server: &Server,
        state: &DurableOAuthState,
    ) -> Result<(), McpDependencyError> {
        validate_durable_state(state, server)?;
        let bytes = serde_json::to_vec(state).map_err(|_| McpDependencyError::OAuthState)?;
        write_recoverable_json(
            &self.state_path(server),
            &StoredOAuthState {
                checksum: blake3::hash(&bytes).to_hex().to_string(),
                state: state.clone(),
            },
        )
    }

    fn read_secret(&self, reference: &str) -> Result<StoredSecret, McpDependencyError> {
        let path = self.secret_path(reference)?;
        let stored = read_recoverable_json::<AuthenticatedSecret>(&path)?
            .ok_or(McpDependencyError::OAuthState)?;
        if stored.schema_version != 1 || stored.reference != reference {
            return Err(McpDependencyError::OAuthState);
        }
        let nonce = URL_SAFE_NO_PAD
            .decode(stored.nonce)
            .map_err(|_| McpDependencyError::OAuthState)?;
        let ciphertext = URL_SAFE_NO_PAD
            .decode(stored.ciphertext)
            .map_err(|_| McpDependencyError::OAuthState)?;
        if nonce.len() != 24 || ciphertext.is_empty() || ciphertext.len() > MAX_SECRET_BYTES * 2 {
            return Err(McpDependencyError::OAuthState);
        }
        let cipher = XChaCha20Poly1305::new((&*self.oauth_mac_key).into());
        let mut plaintext = cipher
            .decrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: &ciphertext,
                    aad: reference.as_bytes(),
                },
            )
            .map_err(|_| McpDependencyError::OAuthState)?;
        let secret: StoredSecret =
            serde_json::from_slice(&plaintext).map_err(|_| McpDependencyError::OAuthState)?;
        plaintext.zeroize();
        if secret.reference != reference
            || secret
                .access_token
                .as_deref()
                .is_some_and(|value| !valid_secret(value))
            || secret
                .refresh_token
                .as_deref()
                .is_some_and(|value| !valid_secret(value))
            || secret
                .verifier
                .as_deref()
                .is_some_and(|value| value.len() < 43 || value.len() > 128)
        {
            return Err(McpDependencyError::OAuthState);
        }
        Ok(secret)
    }

    fn write_secret(
        &self,
        reference: &str,
        secret: &StoredSecret,
    ) -> Result<(), McpDependencyError> {
        if secret.reference != reference {
            return Err(McpDependencyError::OAuthState);
        }
        let mut plaintext =
            serde_json::to_vec(secret).map_err(|_| McpDependencyError::OAuthState)?;
        if plaintext.len() > MAX_SECRET_BYTES {
            return Err(McpDependencyError::OAuthState);
        }
        let mut nonce = [0_u8; 24];
        rand::rng().fill(&mut nonce);
        let cipher = XChaCha20Poly1305::new((&*self.oauth_mac_key).into());
        let ciphertext = cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: &plaintext,
                    aad: reference.as_bytes(),
                },
            )
            .map_err(|_| McpDependencyError::OAuthState)?;
        plaintext.zeroize();
        let path = self.secret_path(reference)?;
        write_recoverable_json(
            &path,
            &AuthenticatedSecret {
                schema_version: 1,
                reference: reference.to_owned(),
                nonce: URL_SAFE_NO_PAD.encode(nonce),
                ciphertext: URL_SAFE_NO_PAD.encode(ciphertext),
            },
        )
    }

    fn remove_secret(&self, reference: &str) -> Result<(), McpDependencyError> {
        let path = self.secret_path(reference)?;
        for candidate in [&path, &path.with_extension("backup")] {
            if candidate.exists() {
                fs::remove_file(candidate).map_err(|_| McpDependencyError::OAuthState)?;
            }
        }
        Ok(())
    }

    fn invalidate_pending(
        &self,
        server: &Server,
        state: &mut DurableOAuthState,
        pending: &PendingAuthorization,
    ) -> Result<(), McpDependencyError> {
        self.remove_secret(&pending.verifier_reference)?;
        state.status = DurableStatus::Failed;
        state.pending = None;
        self.write_oauth_state(server, state)
    }

    #[allow(
        clippy::collapsible_if,
        reason = "listener failure and best-effort durable invalidation are deliberately separate recovery stages"
    )]
    pub(super) fn recover_oauth_callbacks(&self) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        for server in self.servers.values() {
            if !matches!(
                server.config.transport,
                DependencyTransportConfig::StreamableHttpOAuth { .. }
            ) {
                continue;
            }
            let Ok(Some(state)) = self.read_oauth_state(server) else {
                continue;
            };
            let Some(pending) = state
                .pending
                .filter(|_| state.status == DurableStatus::Pending)
            else {
                continue;
            };
            let Ok(config) = oauth_config(server) else {
                continue;
            };
            let dependency = self.clone();
            let server = std::sync::Arc::clone(server);
            let redirect_uri = config.redirect_uri.to_owned();
            handle.spawn(async move {
                if pending.expires_at_ms <= now_ms().unwrap_or(i64::MAX)
                    || dependency
                        .install_callback_listener(
                            &server,
                            pending.transaction_id.clone(),
                            &redirect_uri,
                            pending.expires_at_ms,
                        )
                        .await
                        .is_err()
                {
                    if let Ok(Some(mut state)) = dependency.read_oauth_state(&server)
                        && let Some(pending) = state.pending.clone()
                    {
                        let _ = dependency.invalidate_pending(&server, &mut state, &pending);
                    }
                }
            });
        }
    }

    async fn install_callback_listener(
        &self,
        server: &std::sync::Arc<Server>,
        transaction_id: String,
        redirect_uri: &str,
        expires_at_ms: i64,
    ) -> Result<(), McpDependencyError> {
        let redirect =
            Url::parse(redirect_uri).map_err(|_| McpDependencyError::InvalidConfiguration)?;
        let host = redirect
            .host_str()
            .ok_or(McpDependencyError::InvalidConfiguration)?;
        let port = redirect
            .port()
            .ok_or(McpDependencyError::InvalidConfiguration)?;
        let bind_host = if host == "localhost" {
            "127.0.0.1"
        } else {
            host
        };
        let listener = TcpListener::bind((bind_host, port))
            .await
            .map_err(|_| McpDependencyError::ServerUnavailable)?;
        let key = callback_key(&server.config.id, &transaction_id);
        let cancellation = CancellationToken::new();
        if self
            .oauth_callbacks
            .lock()
            .await
            .insert(key.clone(), cancellation.clone())
            .is_some()
        {
            return Err(McpDependencyError::OAuthTransaction);
        }
        let dependency = self.clone();
        let server = std::sync::Arc::clone(server);
        let redirect_uri = redirect_uri.to_owned();
        tokio::spawn(async move {
            let remaining = expires_at_ms
                .saturating_sub(now_ms().unwrap_or(expires_at_ms))
                .max(0);
            let accepted = tokio::select! {
                () = cancellation.cancelled() => None,
                value = timeout(
                    std::time::Duration::from_millis(u64::try_from(remaining).unwrap_or(0)),
                    listener.accept(),
                ) => value.ok().and_then(Result::ok),
            };
            if let Some((mut stream, peer)) = accepted {
                let mut buffer = vec![0_u8; 8_192];
                let result = async {
                    if !peer.ip().is_loopback() {
                        return Err(McpDependencyError::OAuthTransaction);
                    }
                    let count = stream
                        .read(&mut buffer)
                        .await
                        .map_err(|_| McpDependencyError::Transport)?;
                    let callback = parse_loopback_request(&buffer[..count], &redirect_uri)?;
                    dependency
                        .oauth_complete(
                            &server.config.id,
                            &transaction_id,
                            &callback,
                            &format!("oauth-callback-{}", random_urlsafe(12)),
                        )
                        .await
                }
                .await;
                buffer.zeroize();
                let (status, body) = if result.is_ok() {
                    (
                        "200 OK",
                        "Authorization completed. You may close this window.",
                    )
                } else {
                    if let Ok(Some(mut state)) = dependency.read_oauth_state(&server)
                        && let Some(pending) = state.pending.clone()
                    {
                        let _ = dependency.invalidate_pending(&server, &mut state, &pending);
                    }
                    (
                        "400 Bad Request",
                        "Authorization failed. Return to AgentMod.",
                    )
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-store\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
            dependency.oauth_callbacks.lock().await.remove(&key);
        });
        Ok(())
    }
}

struct ArcLock(std::sync::Arc<tokio::sync::Mutex<()>>);

impl std::ops::Deref for ArcLock {
    type Target = tokio::sync::Mutex<()>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn oauth_config(server: &Server) -> Result<OAuthConfig<'_>, McpDependencyError> {
    let DependencyTransportConfig::StreamableHttpOAuth {
        url,
        authorization_server,
        client_id,
        client_secret_environment,
        redirect_uri,
        scopes,
    } = &server.config.transport
    else {
        return Err(McpDependencyError::OAuthRequired);
    };
    Ok(OAuthConfig {
        url,
        authorization_server,
        client_id,
        client_secret_environment: client_secret_environment.as_deref(),
        redirect_uri,
        scopes,
    })
}

fn validate_pending_config(
    pending: &PendingAuthorization,
    config: &OAuthConfig<'_>,
) -> Result<(), McpDependencyError> {
    if pending.client_id != config.client_id
        || pending.redirect_uri != config.redirect_uri
        || !same_url(&pending.resource, config.url)
        || !same_url(&pending.issuer, config.authorization_server)
    {
        return Err(McpDependencyError::OAuthTransaction);
    }
    let canonical = serde_json::to_vec(&(
        &pending.resource,
        &pending.issuer,
        &pending.authorization_endpoint,
        &pending.token_endpoint,
        &pending.client_id,
        &pending.redirect_uri,
        &pending.scope,
    ))
    .map_err(|_| McpDependencyError::OAuthTransaction)?;
    if blake3::hash(&canonical).to_hex().as_str() != pending.discovery_hash {
        return Err(McpDependencyError::OAuthTransaction);
    }
    Ok(())
}

fn validate_token_config(
    token: &TokenReference,
    config: &OAuthConfig<'_>,
) -> Result<(), McpDependencyError> {
    if token.client_id != config.client_id || !same_url(&token.resource, config.url) {
        return Err(McpDependencyError::OAuthState);
    }
    Ok(())
}

fn validate_authorization_metadata(
    metadata: &AuthorizationServerMetadata,
    config: &OAuthConfig<'_>,
    resource: &str,
) -> Result<(), McpDependencyError> {
    if !same_url(&metadata.issuer, config.authorization_server)
        || !metadata
            .code_challenge_methods_supported
            .iter()
            .any(|value| value == "S256")
        || (!metadata.grant_types_supported.is_empty()
            && !metadata
                .grant_types_supported
                .iter()
                .any(|value| value == "authorization_code"))
        || (!metadata.token_endpoint_auth_methods_supported.is_empty()
            && !metadata
                .token_endpoint_auth_methods_supported
                .iter()
                .any(|value| {
                    if config.client_secret_environment.is_some() {
                        value == "client_secret_post"
                    } else {
                        value == "none"
                    }
                }))
        || (!metadata.protected_resources.is_empty()
            && !metadata
                .protected_resources
                .iter()
                .any(|value| same_url(value, resource)))
    {
        return Err(McpDependencyError::OAuthMetadata);
    }
    validate_secure_url(&metadata.authorization_endpoint, true)
        .map_err(|_| McpDependencyError::OAuthMetadata)?;
    validate_secure_url(&metadata.token_endpoint, false)
        .map_err(|_| McpDependencyError::OAuthMetadata)
}

fn protected_resource_candidates(resource: &str) -> Result<Vec<String>, McpDependencyError> {
    let parsed = Url::parse(resource).map_err(|_| McpDependencyError::OAuthMetadata)?;
    let origin = parsed.origin().ascii_serialization();
    let path = parsed.path().trim_start_matches('/');
    let mut candidates = Vec::new();
    if !path.is_empty() {
        candidates.push(format!(
            "{origin}/.well-known/oauth-protected-resource/{path}"
        ));
    }
    candidates.push(format!("{origin}/.well-known/oauth-protected-resource"));
    Ok(candidates)
}

fn authorization_metadata_candidates(issuer: &str) -> Result<Vec<String>, McpDependencyError> {
    let parsed = Url::parse(issuer).map_err(|_| McpDependencyError::OAuthMetadata)?;
    let origin = parsed.origin().ascii_serialization();
    let path = parsed.path().trim_matches('/');
    if path.is_empty() {
        Ok(vec![
            format!("{origin}/.well-known/oauth-authorization-server"),
            format!("{origin}/.well-known/openid-configuration"),
        ])
    } else {
        Ok(vec![
            format!("{origin}/.well-known/oauth-authorization-server/{path}"),
            format!("{origin}/.well-known/openid-configuration/{path}"),
            format!("{origin}/{path}/.well-known/openid-configuration"),
        ])
    }
}

fn parse_resource_metadata_challenge(
    header: Option<&reqwest::header::HeaderValue>,
) -> Result<Option<String>, McpDependencyError> {
    let Some(header) = header else {
        return Ok(None);
    };
    let value = header
        .to_str()
        .map_err(|_| McpDependencyError::OAuthMetadata)?;
    for component in value.split(',') {
        let component = component.trim();
        let candidate = component
            .strip_prefix("resource_metadata=")
            .or_else(|| component.strip_prefix("Bearer resource_metadata="));
        if let Some(candidate) = candidate {
            let candidate = candidate.trim_matches('"');
            validate_secure_url(candidate, false).map_err(|_| McpDependencyError::OAuthMetadata)?;
            return Ok(Some(candidate.to_owned()));
        }
    }
    Ok(None)
}

fn canonical_resource(value: &str) -> Result<String, McpDependencyError> {
    let mut parsed = Url::parse(value).map_err(|_| McpDependencyError::OAuthMetadata)?;
    if parsed.fragment().is_some()
        || parsed.query().is_some()
        || parsed.username() != ""
        || parsed.password().is_some()
    {
        return Err(McpDependencyError::OAuthMetadata);
    }
    parsed.set_fragment(None);
    if parsed.path() == "/" {
        parsed.set_path("");
    }
    Ok(parsed.into())
}

fn canonical_issuer(value: &str) -> Result<String, McpDependencyError> {
    validate_secure_url(value, false).map_err(|_| McpDependencyError::OAuthMetadata)?;
    canonical_resource(value)
}

fn same_url(left: &str, right: &str) -> bool {
    canonical_resource(left).ok() == canonical_resource(right).ok()
}

fn normalize_scopes(values: &[String]) -> Result<Vec<String>, McpDependencyError> {
    if values.len() > 64
        || values.iter().any(|value| {
            value.is_empty() || value.len() > 256 || value.chars().any(char::is_whitespace)
        })
    {
        return Err(McpDependencyError::OAuthMetadata);
    }
    let unique = values.iter().cloned().collect::<BTreeSet<_>>();
    if unique.len() != values.len() {
        return Err(McpDependencyError::OAuthMetadata);
    }
    Ok(unique.into_iter().collect())
}

fn append_client_secret(
    form: &mut Vec<(&'static str, String)>,
    config: &OAuthConfig<'_>,
) -> Result<(), McpDependencyError> {
    if let Some(variable) = config.client_secret_environment {
        let value = std::env::var(variable).map_err(|_| McpDependencyError::SecretUnavailable)?;
        if !valid_secret(&value) {
            return Err(McpDependencyError::SecretUnavailable);
        }
        form.push(("client_secret", value));
    }
    Ok(())
}

fn parse_token_scopes(
    value: Option<&str>,
    requested: &[String],
) -> Result<Vec<String>, McpDependencyError> {
    let scopes = value.map_or_else(
        || requested.to_vec(),
        |value| value.split_ascii_whitespace().map(str::to_owned).collect(),
    );
    let scopes = normalize_scopes(&scopes).map_err(|_| McpDependencyError::OAuthToken)?;
    if scopes
        .iter()
        .any(|scope| !requested.iter().any(|requested| requested == scope))
    {
        return Err(McpDependencyError::OAuthToken);
    }
    Ok(scopes)
}

fn exactly_one<'a>(
    parameters: &'a [(String, String)],
    name: &str,
) -> Result<&'a str, McpDependencyError> {
    let mut values = parameters
        .iter()
        .filter(|(key, _)| key == name)
        .map(|(_, value)| value.as_str());
    let value = values.next().filter(|value| !value.is_empty());
    if value.is_none() || values.next().is_some() {
        return Err(McpDependencyError::OAuthTransaction);
    }
    Ok(value.unwrap_or_default())
}

fn callback_key(server_id: &str, transaction_id: &str) -> String {
    format!("{server_id}:{transaction_id}")
}

fn parse_loopback_request(bytes: &[u8], redirect_uri: &str) -> Result<String, McpDependencyError> {
    let request = std::str::from_utf8(bytes).map_err(|_| McpDependencyError::OAuthTransaction)?;
    let first = request
        .split("\r\n")
        .next()
        .ok_or(McpDependencyError::OAuthTransaction)?;
    let mut components = first.split_ascii_whitespace();
    if components.next() != Some("GET") {
        return Err(McpDependencyError::OAuthTransaction);
    }
    let target = components
        .next()
        .filter(|value| value.starts_with('/') && value.len() <= 8_192)
        .ok_or(McpDependencyError::OAuthTransaction)?;
    if components.next() != Some("HTTP/1.1") || components.next().is_some() {
        return Err(McpDependencyError::OAuthTransaction);
    }
    let redirect = Url::parse(redirect_uri).map_err(|_| McpDependencyError::OAuthTransaction)?;
    let mut callback = redirect.clone();
    let target = Url::parse(&format!("http://loopback{target}"))
        .map_err(|_| McpDependencyError::OAuthTransaction)?;
    callback.set_path(target.path());
    callback.set_query(target.query());
    Ok(callback.into())
}

fn expires_at_ms(seconds: u64) -> Result<i64, McpDependencyError> {
    let milliseconds = seconds
        .checked_mul(1_000)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(McpDependencyError::OAuthToken)?;
    now_ms()?
        .checked_add(milliseconds)
        .ok_or(McpDependencyError::OAuthToken)
}

fn now_ms() -> Result<i64, McpDependencyError> {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| McpDependencyError::OAuthState)?
        .as_millis();
    i64::try_from(value).map_err(|_| McpDependencyError::OAuthState)
}

fn random_urlsafe(bytes: usize) -> String {
    let mut value = vec![0_u8; bytes];
    rand::rng().fill(value.as_mut_slice());
    URL_SAFE_NO_PAD.encode(value)
}

fn valid_secret(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_SECRET_BYTES && !value.chars().any(char::is_control)
}

fn valid_reference(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn validate_durable_state(
    state: &DurableOAuthState,
    server: &Server,
) -> Result<(), McpDependencyError> {
    let status_matches = match state.status {
        DurableStatus::Pending | DurableStatus::ExchangeDispatched => {
            state.pending.is_some() && state.token.is_none()
        }
        DurableStatus::Authorized | DurableStatus::RefreshDispatched => {
            state.pending.is_none() && state.token.is_some()
        }
        DurableStatus::Unauthorized | DurableStatus::Failed => {
            state.pending.is_none() && state.token.is_none()
        }
    };
    if state.schema_version != STATE_SCHEMA
        || state.server_id != server.config.id
        || state.server_identity != server.identity
        || !status_matches
        || state.pending.as_ref().is_some_and(|pending| {
            !valid_reference(&pending.transaction_id)
                || !valid_reference(&pending.verifier_reference)
                || pending.state_hash.len() != 64
                || pending.discovery_hash.len() != 64
                || pending.created_at_ms > pending.expires_at_ms
        })
        || state
            .token
            .as_ref()
            .is_some_and(|token| !valid_reference(&token.reference))
    {
        return Err(McpDependencyError::OAuthState);
    }
    Ok(())
}

fn redact(
    server_id: &str,
    configuration_hash: &str,
    state: Option<&DurableOAuthState>,
) -> DependencyOAuthStatus {
    let status = state.map_or(
        DependencyOAuthStatusKind::Unauthorized,
        |state| match state.status {
            DurableStatus::Unauthorized => DependencyOAuthStatusKind::Unauthorized,
            DurableStatus::Pending => DependencyOAuthStatusKind::Pending,
            DurableStatus::ExchangeDispatched | DurableStatus::RefreshDispatched => {
                DependencyOAuthStatusKind::Failed
            }
            DurableStatus::Authorized => DependencyOAuthStatusKind::Authorized,
            DurableStatus::Failed => DependencyOAuthStatusKind::Failed,
        },
    );
    DependencyOAuthStatus {
        server_id: server_id.to_owned(),
        status,
        transaction_id: state
            .and_then(|state| state.pending.as_ref())
            .map(|pending| pending.transaction_id.clone()),
        expires_at_ms: state
            .and_then(|state| state.pending.as_ref().map(|pending| pending.expires_at_ms))
            .or_else(|| state.and_then(|state| state.token.as_ref()?.expires_at_ms)),
        scopes: state
            .and_then(|state| state.token.as_ref())
            .map_or_else(Vec::new, |token| token.scope.clone()),
        configuration_hash: state.map_or_else(
            || configuration_hash.to_owned(),
            |state| state.server_identity.clone(),
        ),
    }
}

fn read_recoverable_json<T: for<'de> Deserialize<'de>>(
    path: &Path,
) -> Result<Option<T>, McpDependencyError> {
    let backup = path.with_extension("backup");
    match (read_json(path), read_json(&backup)) {
        (Ok(Some(value)), _) => {
            if backup.exists() {
                fs::remove_file(backup).map_err(|_| McpDependencyError::OAuthState)?;
            }
            Ok(Some(value))
        }
        (Ok(None) | Err(_), Ok(Some(value))) => {
            if path.exists() {
                fs::remove_file(path).map_err(|_| McpDependencyError::OAuthState)?;
            }
            fs::rename(backup, path).map_err(|_| McpDependencyError::OAuthState)?;
            Ok(Some(value))
        }
        (Ok(None), Ok(None)) => Ok(None),
        _ => Err(McpDependencyError::OAuthState),
    }
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Option<T>, McpDependencyError> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path).map_err(|_| McpDependencyError::OAuthState)?;
    if bytes.is_empty() || bytes.len() > MAX_SECRET_BYTES * 4 {
        return Err(McpDependencyError::OAuthState);
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| McpDependencyError::OAuthState)
}

fn write_recoverable_json<T: Serialize>(path: &Path, value: &T) -> Result<(), McpDependencyError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|_| McpDependencyError::OAuthState)?;
    }
    let temporary = path.with_extension(format!("{}.next", uuid::Uuid::now_v7()));
    let backup = path.with_extension("backup");
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temporary)
        .map_err(|_| McpDependencyError::OAuthState)?;
    serde_json::to_writer(&mut file, value).map_err(|_| McpDependencyError::OAuthState)?;
    file.write_all(b"\n")
        .and_then(|()| file.sync_all())
        .map_err(|_| McpDependencyError::OAuthState)?;
    if backup.exists() {
        fs::remove_file(&backup).map_err(|_| McpDependencyError::OAuthState)?;
    }
    if path.exists() {
        fs::rename(path, &backup).map_err(|_| McpDependencyError::OAuthState)?;
    }
    if fs::rename(&temporary, path).is_err() {
        if backup.exists() {
            let _ = fs::rename(&backup, path);
        }
        let _ = fs::remove_file(&temporary);
        return Err(McpDependencyError::OAuthState);
    }
    if backup.exists() {
        fs::remove_file(backup).map_err(|_| McpDependencyError::OAuthState)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_state(status: DurableStatus) -> DurableOAuthState {
        DurableOAuthState {
            schema_version: STATE_SCHEMA,
            server_id: "protected".to_owned(),
            server_identity: "ab".repeat(32),
            status,
            pending: None,
            token: None,
        }
    }

    #[test]
    fn dispatched_exchange_and_refresh_are_redacted_as_failed() {
        let mut exchange = base_state(DurableStatus::ExchangeDispatched);
        exchange.pending = Some(PendingAuthorization {
            transaction_id: "transaction".to_owned(),
            state_hash: "cd".repeat(32),
            verifier_reference: "verifier-reference".to_owned(),
            discovery_hash: "ef".repeat(32),
            resource: "https://mcp.example/resource".to_owned(),
            issuer: "https://login.example".to_owned(),
            authorization_endpoint: "https://login.example/authorize".to_owned(),
            token_endpoint: "https://login.example/token".to_owned(),
            client_id: "client".to_owned(),
            redirect_uri: "http://127.0.0.1:49152/callback".to_owned(),
            scope: vec!["tools.read".to_owned()],
            created_at_ms: 1,
            expires_at_ms: 2,
        });
        let mut refresh = base_state(DurableStatus::RefreshDispatched);
        refresh.token = Some(TokenReference {
            reference: "token-reference".to_owned(),
            token_endpoint: "https://login.example/token".to_owned(),
            resource: "https://mcp.example/resource".to_owned(),
            client_id: "client".to_owned(),
            scope: vec!["tools.read".to_owned()],
            expires_at_ms: Some(2),
            refreshable: true,
        });

        let exchange_status = redact("protected", &"ab".repeat(32), Some(&exchange));
        let refresh_status = redact("protected", &"ab".repeat(32), Some(&refresh));
        assert_eq!(exchange_status.status, DependencyOAuthStatusKind::Failed);
        assert_eq!(refresh_status.status, DependencyOAuthStatusKind::Failed);
        assert_eq!(exchange_status.configuration_hash, "ab".repeat(32));
        assert_eq!(refresh_status.configuration_hash, "ab".repeat(32));
    }

    #[test]
    fn unauthorized_status_retains_live_configuration_identity() {
        let configuration_hash = "ab".repeat(32);
        let status = redact("protected", &configuration_hash, None);
        assert_eq!(status.status, DependencyOAuthStatusKind::Unauthorized);
        assert_eq!(status.configuration_hash, configuration_hash);
    }
}
