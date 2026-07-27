//! External HTTP, HTML, search, secret, DNS, and artifact adapters.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentmod_primitives::{ArtifactId, ContentHash, TimestampMillis};
use agentmod_protocol_support::authorization::{
    AuthorizationKey, ExpectedAuthorization, verify_authorization,
};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use reqwest::{
    Method, Proxy,
    header::{HeaderMap, HeaderName, HeaderValue},
    redirect::Policy,
};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{fs, io::AsyncWriteExt, net::lookup_host, sync::Mutex};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

const REDACTED: &str = "[REDACTED]";

/// Authorization material owned by the dependency layer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyAuthorization {
    /// Authenticated local owner.
    pub owner_id: String,
    /// Runtime session.
    pub session_id: String,
    /// Protocol call.
    pub call_id: String,
    /// Tool action.
    pub action: String,
    /// Runtime-supplied digest.
    pub normalized_digest: String,
    /// Signed single-use grant.
    pub grant: String,
    /// Exact canonical operation bytes recomputed by this host.
    pub canonical_operation: Vec<u8>,
    /// Protocol cancellation identifier.
    pub cancellation_id: String,
}

/// Header value that is either literal or resolved through a secret adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DependencyHeaderValue {
    /// Non-secret literal value.
    Literal(String),
    /// Opaque secret reference.
    SecretReference(String),
}

/// Dependency-owned request body.
#[derive(Clone, Debug, PartialEq)]
pub enum DependencyBody {
    /// No body.
    Empty,
    /// UTF-8 text.
    Text(String),
    /// JSON.
    Json(serde_json::Value),
    /// URL-encoded form fields.
    Form(BTreeMap<String, String>),
    /// Raw binary.
    Binary(Vec<u8>),
}

/// Dependency-owned HTTP request.
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyHttpRequest {
    /// Authorization.
    pub authorization: DependencyAuthorization,
    /// Method.
    pub method: String,
    /// Absolute URL.
    pub url: String,
    /// Query pairs.
    pub query: BTreeMap<String, String>,
    /// Headers.
    pub headers: BTreeMap<String, DependencyHeaderValue>,
    /// Body.
    pub body: DependencyBody,
    /// Redirect limit.
    pub max_redirects: u8,
    /// Timeout.
    pub timeout: Duration,
    /// Maximum accepted response bytes.
    pub max_response_bytes: usize,
    /// Maximum inline response bytes.
    pub max_inline_bytes: usize,
}

/// Dependency-owned fetch request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyFetchRequest {
    /// Authorization.
    pub authorization: DependencyAuthorization,
    /// Absolute URL.
    pub url: String,
    /// Redirect limit.
    pub max_redirects: u8,
    /// Timeout.
    pub timeout: Duration,
    /// Maximum accepted response bytes.
    pub max_response_bytes: usize,
    /// Maximum inline extracted characters.
    pub max_inline_bytes: usize,
    /// Whether a cached representation may be used.
    pub use_cache: bool,
}

/// Dependency-owned search request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencySearchRequest {
    /// Authorization.
    pub authorization: DependencyAuthorization,
    /// Search query.
    pub query: String,
    /// Result count.
    pub count: u8,
    /// Freshness hint.
    pub freshness: Option<String>,
    /// Domain allowlist applied to normalized results.
    pub domain_allowlist: Vec<String>,
    /// Domain denylist applied to normalized results.
    pub domain_denylist: Vec<String>,
    /// Search language.
    pub language: Option<String>,
    /// Locale/country.
    pub locale: Option<String>,
    /// Timeout.
    pub timeout: Duration,
}

/// Bounded external response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyHttpResponse {
    /// Status.
    pub status: u16,
    /// Canonical final URL.
    pub final_url: String,
    /// Redacted response headers.
    pub headers: BTreeMap<String, String>,
    /// Content type.
    pub content_type: Option<String>,
    /// Bounded text or base64 projection.
    pub inline_body: String,
    /// Whether projection is base64.
    pub body_is_base64: bool,
    /// Total bytes received.
    pub total_bytes: u64,
    /// Full immutable artifact, when truncated.
    pub artifact: Option<ArtifactId>,
    /// Whether inline body is incomplete.
    pub truncated: bool,
}

/// Extracted hyperlink.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyLink {
    /// Link label.
    pub text: String,
    /// Absolute URL.
    pub url: String,
}

/// Extracted Web representation.
#[allow(
    clippy::struct_excessive_bools,
    reason = "orthogonal wire facts are clearer than a combinatorial state enum"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyFetchResponse {
    /// Canonical URL.
    pub canonical_url: String,
    /// Page title.
    pub title: Option<String>,
    /// Description metadata.
    pub description: Option<String>,
    /// Clean text projection.
    pub text: String,
    /// Markdown-like projection.
    pub markdown: String,
    /// Bounded links.
    pub links: Vec<DependencyLink>,
    /// MIME type.
    pub content_type: Option<String>,
    /// PDF response.
    pub is_pdf: bool,
    /// Likely requires JavaScript.
    pub javascript_required: bool,
    /// Full response artifact.
    pub artifact: Option<ArtifactId>,
    /// Whether extracted projection was truncated.
    pub truncated: bool,
    /// Cache hit.
    pub cached: bool,
}

/// Provider-independent search result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencySearchResult {
    /// Result title.
    pub title: String,
    /// Canonical URL.
    pub url: String,
    /// Provider snippet.
    pub snippet: String,
    /// Publication date when supplied.
    pub published_at: Option<String>,
}

/// Search response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencySearchResponse {
    /// Provider-neutral result records.
    pub results: Vec<DependencySearchResult>,
    /// Non-secret provider identifier for provenance.
    pub provider: String,
}

/// Deterministic offline search fixture.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MockSearchDocument {
    /// Searchable title.
    pub title: String,
    /// URL.
    pub url: String,
    /// Searchable snippet.
    pub snippet: String,
    /// Optional publication date.
    pub published_at: Option<String>,
}

/// Configured provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SearchProvider {
    /// Brave Search API using an opaque secret reference.
    Brave {
        /// Secret store reference for `X-Subscription-Token`.
        api_key_reference: String,
    },
    /// Deterministic local documents.
    Mock {
        /// Fixed documents.
        documents: Vec<MockSearchDocument>,
    },
}

/// Mandatory network policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NetworkPolicy {
    /// Allowed hostname patterns. Empty means no public host is allowed.
    pub allowed_domains: Vec<String>,
    /// Denied hostname patterns, evaluated first.
    pub denied_domains: Vec<String>,
    /// Allow literal and resolved private/local addresses.
    pub allow_private_network: bool,
    /// Allow unencrypted HTTP.
    pub allow_plain_http: bool,
    /// Allowed methods.
    pub allowed_methods: BTreeSet<String>,
}

/// External adapter configuration.
#[derive(Clone, Debug)]
pub struct WebDependencyConfig {
    /// Host artifact directory.
    pub artifact_root: PathBuf,
    /// Authorization key value loaded at composition time.
    pub authorization_key_hex: String,
    /// Bootstrap owner identity; request values must match.
    pub owner_id: String,
    /// Bootstrap session identity; request values must match.
    pub session_id: String,
    /// Maximum retained unexpired authorization nonces.
    pub maximum_replay_entries: usize,
    /// Maximum concurrently registered external calls.
    pub maximum_active_calls: usize,
    /// Mandatory outbound network policy.
    pub network_policy: NetworkPolicy,
    /// Maximum redirect hops.
    pub maximum_redirects: u8,
    /// Maximum per-request duration.
    pub maximum_timeout: Duration,
    /// Maximum response bytes.
    pub maximum_response_bytes: usize,
    /// Maximum inline response bytes.
    pub maximum_inline_bytes: usize,
    /// Maximum URL characters.
    pub maximum_url_length: usize,
    /// Maximum request headers.
    pub maximum_headers: usize,
    /// Maximum encoded request body bytes.
    pub maximum_request_body_bytes: usize,
    /// Optional explicit proxy.
    pub proxy_url: Option<String>,
    /// Maximum in-memory fetch cache entries.
    pub cache_entries: usize,
    /// Search adapter.
    pub search_provider: SearchProvider,
}

/// Secret lookup abstraction implemented only in dependency.
#[async_trait]
pub trait SecretDependencyPort: Send + Sync {
    /// Resolves an opaque reference without returning it to upper layers.
    async fn resolve(&self, reference: &str) -> Result<String, WebDependencyError>;
}

/// Environment-backed secret dependency.
#[derive(Clone, Copy, Debug, Default)]
pub struct EnvironmentSecretDependency;

#[async_trait]
impl SecretDependencyPort for EnvironmentSecretDependency {
    async fn resolve(&self, reference: &str) -> Result<String, WebDependencyError> {
        let variable = reference
            .strip_prefix("env:")
            .ok_or(WebDependencyError::SecretUnavailable)?;
        if variable.is_empty()
            || variable.len() > 128
            || !variable
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(WebDependencyError::SecretUnavailable);
        }
        std::env::var(variable).map_err(|_| WebDependencyError::SecretUnavailable)
    }
}

/// External dependency contract consumed only by data.
#[async_trait]
pub trait WebDependencyPort: Send + Sync {
    /// Performs an authorized HTTP request.
    async fn request(
        &self,
        request: DependencyHttpRequest,
    ) -> Result<DependencyHttpResponse, WebDependencyError>;
    /// Fetches and extracts a page.
    async fn fetch(
        &self,
        request: DependencyFetchRequest,
    ) -> Result<DependencyFetchResponse, WebDependencyError>;
    /// Searches with the configured provider.
    async fn search(
        &self,
        request: DependencySearchRequest,
    ) -> Result<DependencySearchResponse, WebDependencyError>;
    /// Cancels an active operation and returns its call ID.
    async fn cancel(&self, cancellation_id: &str) -> Result<String, WebDependencyError>;
}

#[derive(Clone, Debug)]
struct CachedFetch {
    response: DependencyFetchResponse,
}

#[derive(Clone, Debug)]
struct ActiveCall {
    call_id: String,
    token: CancellationToken,
}

/// Reqwest-backed dependency with deterministic mock search support.
#[derive(Clone)]
pub struct ReqwestWebDependency<S> {
    config: Arc<WebDependencyConfig>,
    authorization_key: Arc<AuthorizationKey>,
    secret: S,
    replay: Arc<Mutex<ReplayState>>,
    active_calls: Arc<Mutex<BTreeMap<String, ActiveCall>>>,
    cache: Arc<Mutex<BTreeMap<String, CachedFetch>>>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct ReplayPayload {
    generation: u64,
    nonces: BTreeMap<String, i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ReplayEnvelope {
    payload: ReplayPayload,
    checksum: String,
}

#[derive(Debug)]
struct ReplayState {
    directory: PathBuf,
    payload: ReplayPayload,
}

impl<S> ReqwestWebDependency<S> {
    /// Validates adapter configuration.
    ///
    /// # Errors
    ///
    /// Rejects missing bounds, invalid domains, invalid proxy URLs, and malformed keys.
    pub fn new(mut config: WebDependencyConfig, secret: S) -> Result<Self, WebDependencyError> {
        if config.artifact_root.as_os_str().is_empty()
            || config.maximum_redirects > 20
            || config.maximum_timeout.is_zero()
            || config.maximum_response_bytes == 0
            || config.maximum_inline_bytes == 0
            || config.maximum_inline_bytes > config.maximum_response_bytes
            || config.maximum_url_length == 0
            || config.maximum_headers == 0
            || config.maximum_request_body_bytes == 0
            || config.cache_entries > 10_000
            || config.network_policy.allowed_methods.is_empty()
            || config.owner_id.trim().is_empty()
            || config.session_id.trim().is_empty()
            || config.maximum_replay_entries == 0
            || config.maximum_active_calls == 0
        {
            return Err(WebDependencyError::InvalidConfiguration);
        }
        validate_domain_patterns(&config.network_policy.allowed_domains)?;
        validate_domain_patterns(&config.network_policy.denied_domains)?;
        if let Some(proxy) = &config.proxy_url {
            let parsed = Url::parse(proxy).map_err(|_| WebDependencyError::InvalidConfiguration)?;
            if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
                return Err(WebDependencyError::InvalidConfiguration);
            }
        }
        let key = AuthorizationKey::from_hex(&config.authorization_key_hex)
            .map_err(|_| WebDependencyError::InvalidConfiguration)?;
        config.authorization_key_hex.clear();
        let replay = load_replay_state(&config.artifact_root)?;
        Ok(Self {
            config: Arc::new(config),
            authorization_key: Arc::new(key),
            secret,
            replay: Arc::new(Mutex::new(replay)),
            active_calls: Arc::new(Mutex::new(BTreeMap::new())),
            cache: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }
}

impl<S: SecretDependencyPort> ReqwestWebDependency<S> {
    async fn authorize(
        &self,
        authorization: &DependencyAuthorization,
        expected_action: &str,
        canonical_operation: &[u8],
    ) -> Result<(), WebDependencyError> {
        if authorization.owner_id != self.config.owner_id
            || authorization.session_id != self.config.session_id
            || authorization.action != expected_action
        {
            return Err(WebDependencyError::Authorization);
        }
        let digest = ContentHash::digest(canonical_operation);
        if digest.to_hex() != authorization.normalized_digest {
            return Err(WebDependencyError::Authorization);
        }
        let current_time = now();
        let claims = verify_authorization(
            &authorization.grant,
            &self.authorization_key,
            ExpectedAuthorization {
                owner: &authorization.owner_id,
                session: &authorization.session_id,
                call_id: &authorization.call_id,
                action: &authorization.action,
                normalized_digest: digest,
            },
            current_time,
        )
        .map_err(|_| WebDependencyError::Authorization)?;
        let nonce_key = format!("{}:{}:{}", claims.owner, claims.session, claims.nonce);
        let mut replay = self.replay.lock().await;
        let mut next = replay.payload.clone();
        next.nonces
            .retain(|_, expiry| *expiry >= current_time.get());
        if next.nonces.contains_key(&nonce_key) {
            return Err(WebDependencyError::AuthorizationReplay);
        }
        if next.nonces.len() >= self.config.maximum_replay_entries {
            return Err(WebDependencyError::ResourceLimit);
        }
        next.generation = next
            .generation
            .checked_add(1)
            .ok_or(WebDependencyError::ResourceLimit)?;
        next.nonces.insert(nonce_key, claims.expires_at.get());
        persist_replay_state(&replay.directory, &next)?;
        replay.payload = next;
        Ok(())
    }

    async fn register(
        &self,
        authorization: &DependencyAuthorization,
    ) -> Result<CancellationToken, WebDependencyError> {
        let token = CancellationToken::new();
        let mut active = self.active_calls.lock().await;
        if active.len() >= self.config.maximum_active_calls {
            return Err(WebDependencyError::ResourceLimit);
        }
        if active
            .insert(
                authorization.cancellation_id.clone(),
                ActiveCall {
                    call_id: authorization.call_id.clone(),
                    token: token.clone(),
                },
            )
            .is_some()
        {
            return Err(WebDependencyError::DuplicateCancellation);
        }
        Ok(token)
    }

    async fn unregister(&self, cancellation_id: &str) {
        self.active_calls.lock().await.remove(cancellation_id);
    }

    async fn execute_http(
        &self,
        request: &DependencyHttpRequest,
        token: &CancellationToken,
    ) -> Result<DependencyHttpResponse, WebDependencyError> {
        validate_request_bounds(&self.config, request)?;
        let method = Method::from_bytes(request.method.as_bytes())
            .map_err(|_| WebDependencyError::InvalidRequest)?;
        let mut url = Url::parse(&request.url).map_err(|_| WebDependencyError::InvalidUrl)?;
        {
            let mut query = url.query_pairs_mut();
            for (key, value) in &request.query {
                query.append_pair(key, value);
            }
        }
        let headers = self.resolve_headers(&request.headers).await?;
        let body = encode_body(&request.body)?;
        let deadline = tokio::time::Instant::now() + request.timeout;
        let mut redirects = 0_u8;
        loop {
            let addresses = validate_target(&url, &self.config.network_policy).await?;
            let client = build_client(&self.config, &url, &addresses)?;
            let mut builder = client.request(method.clone(), url.clone());
            builder = builder.headers(headers.clone());
            if let Some((bytes, content_type)) = &body {
                builder = builder.body(bytes.clone());
                if let Some(content_type) = content_type {
                    builder = builder.header("content-type", *content_type);
                }
            }
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .ok_or(WebDependencyError::Timeout)?;
            let response = tokio::select! {
                () = token.cancelled() => return Err(WebDependencyError::Cancelled),
                result = tokio::time::timeout(remaining, builder.send()) => {
                    result.map_err(|_| WebDependencyError::Timeout)?
                        .map_err(map_reqwest)?
                }
            };
            if response.status().is_redirection() {
                if redirects >= request.max_redirects {
                    return Err(WebDependencyError::RedirectLimit);
                }
                let location = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .ok_or(WebDependencyError::InvalidRedirect)?;
                url = url
                    .join(location)
                    .map_err(|_| WebDependencyError::InvalidRedirect)?;
                redirects = redirects.saturating_add(1);
                continue;
            }
            return self
                .read_response(
                    response,
                    url,
                    request.max_response_bytes,
                    request.max_inline_bytes,
                    token,
                )
                .await;
        }
    }

    async fn resolve_headers(
        &self,
        headers: &BTreeMap<String, DependencyHeaderValue>,
    ) -> Result<HeaderMap, WebDependencyError> {
        let mut mapped = HeaderMap::new();
        for (name, value) in headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| WebDependencyError::InvalidRequest)?;
            let value = match value {
                DependencyHeaderValue::Literal(value) => value.clone(),
                DependencyHeaderValue::SecretReference(reference) => {
                    self.secret.resolve(reference).await?
                }
            };
            let value =
                HeaderValue::from_str(&value).map_err(|_| WebDependencyError::InvalidRequest)?;
            mapped.insert(name, value);
        }
        Ok(mapped)
    }

    async fn read_response(
        &self,
        mut response: reqwest::Response,
        final_url: Url,
        maximum: usize,
        inline_maximum: usize,
        token: &CancellationToken,
    ) -> Result<DependencyHttpResponse, WebDependencyError> {
        let status = response.status().as_u16();
        let headers = redact_headers(response.headers());
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned);
        let mut body = Vec::new();
        loop {
            let chunk = tokio::select! {
                () = token.cancelled() => return Err(WebDependencyError::Cancelled),
                result = response.chunk() => result.map_err(map_reqwest)?,
            };
            let Some(chunk) = chunk else {
                break;
            };
            if body.len().saturating_add(chunk.len()) > maximum {
                return Err(WebDependencyError::ResponseTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        let total_bytes =
            u64::try_from(body.len()).map_err(|_| WebDependencyError::ResponseTooLarge)?;
        let truncated = body.len() > inline_maximum;
        let artifact = if truncated {
            Some(write_artifact(&self.config.artifact_root, &body).await?)
        } else {
            None
        };
        let inline = &body[..body.len().min(inline_maximum)];
        let textual = is_textual(content_type.as_deref()) && std::str::from_utf8(inline).is_ok();
        let inline_body = if textual {
            String::from_utf8_lossy(inline).into_owned()
        } else {
            BASE64.encode(inline)
        };
        Ok(DependencyHttpResponse {
            status,
            final_url: final_url.into(),
            headers,
            content_type,
            inline_body,
            body_is_base64: !textual,
            total_bytes,
            artifact,
            truncated,
        })
    }

    async fn search_brave(
        &self,
        request: &DependencySearchRequest,
        api_key_reference: &str,
        token: &CancellationToken,
    ) -> Result<DependencySearchResponse, WebDependencyError> {
        let key = self.secret.resolve(api_key_reference).await?;
        let mut query = BTreeMap::new();
        query.insert("q".to_owned(), request.query.clone());
        query.insert("count".to_owned(), request.count.to_string());
        if let Some(freshness) = &request.freshness {
            query.insert("freshness".to_owned(), freshness.clone());
        }
        if let Some(language) = &request.language {
            query.insert("search_lang".to_owned(), language.clone());
        }
        if let Some(locale) = &request.locale {
            query.insert("country".to_owned(), locale.clone());
        }
        let mut headers = BTreeMap::new();
        headers.insert(
            "accept".to_owned(),
            DependencyHeaderValue::Literal("application/json".to_owned()),
        );
        headers.insert(
            "api-version".to_owned(),
            DependencyHeaderValue::Literal("2023-01-01".to_owned()),
        );
        headers.insert(
            "x-subscription-token".to_owned(),
            DependencyHeaderValue::Literal(key),
        );
        let response = self
            .execute_http(
                &DependencyHttpRequest {
                    authorization: request.authorization.clone(),
                    method: "GET".to_owned(),
                    url: "https://api.search.brave.com/res/v1/web/search".to_owned(),
                    query,
                    headers,
                    body: DependencyBody::Empty,
                    max_redirects: 0,
                    timeout: request.timeout,
                    max_response_bytes: self.config.maximum_response_bytes.min(4 * 1024 * 1024),
                    max_inline_bytes: self.config.maximum_inline_bytes.min(4 * 1024 * 1024),
                },
                token,
            )
            .await?;
        let value: serde_json::Value = serde_json::from_str(&response.inline_body)
            .map_err(|_| WebDependencyError::InvalidProviderResponse)?;
        let values = value
            .pointer("/web/results")
            .and_then(serde_json::Value::as_array)
            .ok_or(WebDependencyError::InvalidProviderResponse)?;
        let results = values
            .iter()
            .filter_map(|value| {
                Some(DependencySearchResult {
                    title: value.get("title")?.as_str()?.to_owned(),
                    url: value.get("url")?.as_str()?.to_owned(),
                    snippet: value
                        .get("description")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    published_at: value
                        .get("page_age")
                        .or_else(|| value.get("age"))
                        .and_then(serde_json::Value::as_str)
                        .map(ToOwned::to_owned),
                })
            })
            .collect();
        Ok(DependencySearchResponse {
            results,
            provider: "brave".to_owned(),
        })
    }
}

#[async_trait]
impl<S: SecretDependencyPort> WebDependencyPort for ReqwestWebDependency<S> {
    async fn request(
        &self,
        request: DependencyHttpRequest,
    ) -> Result<DependencyHttpResponse, WebDependencyError> {
        let canonical = canonical_http_operation(&request)?;
        self.authorize(&request.authorization, "http.request", &canonical)
            .await?;
        let token = self.register(&request.authorization).await?;
        let cancellation_id = request.authorization.cancellation_id.clone();
        let result = self.execute_http(&request, &token).await;
        self.unregister(&cancellation_id).await;
        result
    }

    async fn fetch(
        &self,
        request: DependencyFetchRequest,
    ) -> Result<DependencyFetchResponse, WebDependencyError> {
        let canonical = canonical_fetch_operation(&request)?;
        self.authorize(&request.authorization, "web.fetch", &canonical)
            .await?;
        let token = self.register(&request.authorization).await?;
        let cancellation_id = request.authorization.cancellation_id.clone();
        if request.use_cache
            && let Some(cached) = self.cache.lock().await.get(&request.url).cloned()
        {
            self.unregister(&cancellation_id).await;
            let mut response = cached.response;
            response.cached = true;
            return Ok(response);
        }
        let http = self
            .execute_http(
                &DependencyHttpRequest {
                    authorization: request.authorization.clone(),
                    method: "GET".to_owned(),
                    url: request.url.clone(),
                    query: BTreeMap::new(),
                    headers: BTreeMap::from([(
                        "accept".to_owned(),
                        DependencyHeaderValue::Literal(
                            "text/html,application/xhtml+xml,application/pdf;q=0.8,*/*;q=0.1"
                                .to_owned(),
                        ),
                    )]),
                    body: DependencyBody::Empty,
                    max_redirects: request.max_redirects,
                    timeout: request.timeout,
                    max_response_bytes: request.max_response_bytes,
                    max_inline_bytes: request.max_inline_bytes,
                },
                &token,
            )
            .await;
        self.unregister(&cancellation_id).await;
        let http = http?;
        let mut response = extract_fetch(http, request.max_inline_bytes)?;
        if request.use_cache && self.config.cache_entries > 0 {
            let mut cache = self.cache.lock().await;
            if cache.len() >= self.config.cache_entries
                && let Some(key) = cache.keys().next().cloned()
            {
                cache.remove(&key);
            }
            cache.insert(
                request.url,
                CachedFetch {
                    response: response.clone(),
                },
            );
        }
        response.cached = false;
        Ok(response)
    }

    async fn search(
        &self,
        request: DependencySearchRequest,
    ) -> Result<DependencySearchResponse, WebDependencyError> {
        let canonical = canonical_search_operation(&request)?;
        self.authorize(&request.authorization, "web.search", &canonical)
            .await?;
        if request.query.trim().is_empty() || request.count == 0 {
            return Err(WebDependencyError::InvalidRequest);
        }
        let token = self.register(&request.authorization).await?;
        let cancellation_id = request.authorization.cancellation_id.clone();
        let result = match &self.config.search_provider {
            SearchProvider::Mock { documents } => Ok(mock_search(documents, &request)),
            SearchProvider::Brave { api_key_reference } => {
                self.search_brave(&request, api_key_reference, &token).await
            }
        };
        self.unregister(&cancellation_id).await;
        result
    }

    async fn cancel(&self, cancellation_id: &str) -> Result<String, WebDependencyError> {
        let active = self
            .active_calls
            .lock()
            .await
            .get(cancellation_id)
            .cloned()
            .ok_or(WebDependencyError::UnknownCancellation)?;
        active.token.cancel();
        Ok(active.call_id)
    }
}

/// Reconstructs the exact provider-visible HTTP operation from dependency-owned fields.
///
/// # Errors
///
/// Returns an authorization error when a duration or body cannot be represented.
pub fn canonical_http_operation(
    request: &DependencyHttpRequest,
) -> Result<Vec<u8>, WebDependencyError> {
    let headers: BTreeMap<_, _> = request
        .headers
        .iter()
        .map(|(name, value)| {
            (
                name.clone(),
                match value {
                    DependencyHeaderValue::Literal(value) => {
                        serde_json::Value::String(value.clone())
                    }
                    DependencyHeaderValue::SecretReference(reference) => {
                        serde_json::json!({"secret_ref": reference})
                    }
                },
            )
        })
        .collect();
    let body = match &request.body {
        DependencyBody::Empty => serde_json::json!({"kind":"empty"}),
        DependencyBody::Text(value) => serde_json::json!({"kind":"text","value":value}),
        DependencyBody::Json(value) => serde_json::json!({"kind":"json","value":value}),
        DependencyBody::Form(value) => serde_json::json!({"kind":"form","value":value}),
        DependencyBody::Binary(value) => {
            serde_json::json!({"kind":"binary_base64","value":BASE64.encode(value)})
        }
    };
    canonical_operation_bytes(
        "http.request",
        &request.authorization.cancellation_id,
        &serde_json::json!({
            "method": request.method,
            "url": request.url,
            "query": request.query,
            "headers": headers,
            "body": body,
            "max_redirects": request.max_redirects,
            "timeout_ms": duration_millis(request.timeout)?,
            "max_response_bytes": request.max_response_bytes,
            "max_inline_bytes": request.max_inline_bytes,
        }),
    )
}

/// Reconstructs the exact provider-visible fetch operation.
///
/// # Errors
///
/// Returns an authorization error when the timeout cannot be represented.
pub fn canonical_fetch_operation(
    request: &DependencyFetchRequest,
) -> Result<Vec<u8>, WebDependencyError> {
    canonical_operation_bytes(
        "web.fetch",
        &request.authorization.cancellation_id,
        &serde_json::json!({
            "url": request.url,
            "max_redirects": request.max_redirects,
            "timeout_ms": duration_millis(request.timeout)?,
            "max_response_bytes": request.max_response_bytes,
            "max_inline_bytes": request.max_inline_bytes,
            "use_cache": request.use_cache,
        }),
    )
}

/// Reconstructs the exact provider-visible search operation.
///
/// # Errors
///
/// Returns an authorization error when the timeout cannot be represented.
pub fn canonical_search_operation(
    request: &DependencySearchRequest,
) -> Result<Vec<u8>, WebDependencyError> {
    canonical_operation_bytes(
        "web.search",
        &request.authorization.cancellation_id,
        &serde_json::json!({
            "query": request.query,
            "count": request.count,
            "freshness": request.freshness,
            "domain_allowlist": request.domain_allowlist,
            "domain_denylist": request.domain_denylist,
            "language": request.language,
            "locale": request.locale,
            "timeout_ms": duration_millis(request.timeout)?,
        }),
    )
}

fn duration_millis(duration: Duration) -> Result<u64, WebDependencyError> {
    u64::try_from(duration.as_millis()).map_err(|_| WebDependencyError::Authorization)
}

fn canonical_operation_bytes(
    action: &str,
    cancellation_id: &str,
    arguments: &serde_json::Value,
) -> Result<Vec<u8>, WebDependencyError> {
    serde_json::to_vec(&(action, cancellation_id, normalize_json(arguments)))
        .map_err(|_| WebDependencyError::Authorization)
}

fn normalize_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let sorted: BTreeMap<_, _> = map
                .iter()
                .map(|(key, value)| (key.clone(), normalize_json(value)))
                .collect();
            serde_json::to_value(sorted).unwrap_or(serde_json::Value::Null)
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(normalize_json).collect())
        }
        _ => value.clone(),
    }
}

fn load_replay_state(artifact_root: &Path) -> Result<ReplayState, WebDependencyError> {
    let directory = artifact_root.join("authorization-replay");
    std::fs::create_dir_all(&directory).map_err(redacted_io)?;
    let mut candidates = Vec::new();
    for entry in std::fs::read_dir(&directory).map_err(redacted_io)? {
        let path = entry.map_err(redacted_io)?.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            let bytes = std::fs::read(&path).map_err(redacted_io)?;
            let envelope: ReplayEnvelope =
                serde_json::from_slice(&bytes).map_err(|_| WebDependencyError::ReplayState)?;
            let payload_bytes = serde_json::to_vec(&envelope.payload)
                .map_err(|_| WebDependencyError::ReplayState)?;
            if ContentHash::digest(&payload_bytes).to_hex() != envelope.checksum {
                return Err(WebDependencyError::ReplayState);
            }
            candidates.push((path, envelope.payload));
        }
    }
    candidates.sort_by_key(|(_, payload)| payload.generation);
    let payload = candidates
        .last()
        .map_or_else(ReplayPayload::default, |(_, payload)| payload.clone());
    for (path, candidate) in candidates {
        if candidate.generation != payload.generation {
            let _ = std::fs::remove_file(path);
        }
    }
    Ok(ReplayState { directory, payload })
}

fn persist_replay_state(
    directory: &Path,
    payload: &ReplayPayload,
) -> Result<(), WebDependencyError> {
    let payload_bytes = serde_json::to_vec(payload).map_err(|_| WebDependencyError::ReplayState)?;
    let envelope = ReplayEnvelope {
        payload: payload.clone(),
        checksum: ContentHash::digest(&payload_bytes).to_hex(),
    };
    let bytes = serde_json::to_vec(&envelope).map_err(|_| WebDependencyError::ReplayState)?;
    let path = directory.join(format!("replay-{:020}.json", payload.generation));
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .map_err(redacted_io)?;
    std::io::Write::write_all(&mut file, &bytes).map_err(redacted_io)?;
    file.sync_all().map_err(redacted_io)?;
    for entry in std::fs::read_dir(directory).map_err(redacted_io)? {
        let old = entry.map_err(redacted_io)?.path();
        if old != path && old.extension().is_some_and(|extension| extension == "json") {
            let _ = std::fs::remove_file(old);
        }
    }
    Ok(())
}

fn validate_request_bounds(
    config: &WebDependencyConfig,
    request: &DependencyHttpRequest,
) -> Result<(), WebDependencyError> {
    if request.timeout.is_zero()
        || request.timeout > config.maximum_timeout
        || request.max_redirects > config.maximum_redirects
        || request.max_response_bytes == 0
        || request.max_response_bytes > config.maximum_response_bytes
        || request.max_inline_bytes == 0
        || request.max_inline_bytes > request.max_response_bytes
        || request.max_inline_bytes > config.maximum_inline_bytes
        || request.url.len() > config.maximum_url_length
        || request.headers.len() > config.maximum_headers
        || body_encoded_size(&request.body) > config.maximum_request_body_bytes
        || !config
            .network_policy
            .allowed_methods
            .contains(&request.method.to_ascii_uppercase())
    {
        return Err(WebDependencyError::PolicyDenied);
    }
    Ok(())
}

fn body_encoded_size(body: &DependencyBody) -> usize {
    match body {
        DependencyBody::Empty => 0,
        DependencyBody::Text(value) => value.len(),
        DependencyBody::Json(value) => value.to_string().len(),
        DependencyBody::Form(values) => values
            .iter()
            .map(|(key, value)| key.len().saturating_add(value.len()).saturating_add(2))
            .sum(),
        DependencyBody::Binary(value) => value.len(),
    }
}

async fn validate_target(
    url: &Url,
    policy: &NetworkPolicy,
) -> Result<Vec<SocketAddr>, WebDependencyError> {
    if !matches!(url.scheme(), "https" | "http")
        || (url.scheme() == "http" && !policy.allow_plain_http)
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(WebDependencyError::PolicyDenied);
    }
    let host = url.host_str().ok_or(WebDependencyError::InvalidUrl)?;
    if !domain_allowed(host, policy) {
        return Err(WebDependencyError::PolicyDenied);
    }
    let port = url
        .port_or_known_default()
        .ok_or(WebDependencyError::InvalidUrl)?;
    let addresses: Vec<SocketAddr> = lookup_host((host, port))
        .await
        .map_err(|_| WebDependencyError::Dns)?
        .collect();
    if addresses.is_empty() {
        return Err(WebDependencyError::Dns);
    }
    if !policy.allow_private_network
        && addresses
            .iter()
            .any(|address| is_private_or_special(address.ip()))
    {
        return Err(WebDependencyError::PrivateNetworkDenied);
    }
    Ok(addresses)
}

fn build_client(
    config: &WebDependencyConfig,
    url: &Url,
    addresses: &[SocketAddr],
) -> Result<reqwest::Client, WebDependencyError> {
    let host = url.host_str().ok_or(WebDependencyError::InvalidUrl)?;
    let mut builder = reqwest::Client::builder()
        .redirect(Policy::none())
        .https_only(!config.network_policy.allow_plain_http)
        .resolve_to_addrs(host, addresses);
    if let Some(proxy_url) = &config.proxy_url {
        let proxy = Proxy::all(proxy_url).map_err(|_| WebDependencyError::InvalidConfiguration)?;
        builder = builder.proxy(proxy);
    } else {
        builder = builder.no_proxy();
    }
    builder.build().map_err(map_reqwest)
}

fn domain_allowed(host: &str, policy: &NetworkPolicy) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    !policy
        .denied_domains
        .iter()
        .any(|pattern| domain_matches(&host, pattern))
        && policy
            .allowed_domains
            .iter()
            .any(|pattern| domain_matches(&host, pattern))
}

fn domain_matches(host: &str, pattern: &str) -> bool {
    let pattern = pattern.trim_end_matches('.').to_ascii_lowercase();
    if let Some(suffix) = pattern.strip_prefix("*.") {
        host != suffix && host.ends_with(&format!(".{suffix}"))
    } else {
        host == pattern
    }
}

fn validate_domain_patterns(patterns: &[String]) -> Result<(), WebDependencyError> {
    if patterns.iter().any(|pattern| {
        let value = pattern.strip_prefix("*.").unwrap_or(pattern);
        value.is_empty()
            || value.len() > 253
            || value.starts_with('.')
            || value.ends_with('.')
            || value
                .bytes()
                .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-')))
    }) {
        Err(WebDependencyError::InvalidConfiguration)
    } else {
        Ok(())
    }
}

fn is_private_or_special(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_private_v4(ip),
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return is_private_v4(mapped);
            }
            ip.is_unspecified()
                || ip.is_loopback()
                || ip.is_multicast()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || is_documentation_v6(ip)
        }
    }
}

fn is_private_v4(ip: Ipv4Addr) -> bool {
    ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.octets()[0] == 0
        || ip.octets()[0] >= 240
        || (ip.octets()[0] == 100 && (64..=127).contains(&ip.octets()[1]))
        || (ip.octets()[0] == 198 && matches!(ip.octets()[1], 18 | 19))
}

fn is_documentation_v6(ip: Ipv6Addr) -> bool {
    let segments = ip.segments();
    segments[0] == 0x2001 && segments[1] == 0x0db8
}

type EncodedBody = Option<(Vec<u8>, Option<&'static str>)>;

fn encode_body(body: &DependencyBody) -> Result<EncodedBody, WebDependencyError> {
    match body {
        DependencyBody::Empty => Ok(None),
        DependencyBody::Text(value) => Ok(Some((
            value.as_bytes().to_vec(),
            Some("text/plain; charset=utf-8"),
        ))),
        DependencyBody::Json(value) => serde_json::to_vec(value)
            .map(|bytes| Some((bytes, Some("application/json"))))
            .map_err(|_| WebDependencyError::InvalidRequest),
        DependencyBody::Form(values) => {
            let encoded = url::form_urlencoded::Serializer::new(String::new())
                .extend_pairs(values)
                .finish();
            Ok(Some((
                encoded.into_bytes(),
                Some("application/x-www-form-urlencoded"),
            )))
        }
        DependencyBody::Binary(bytes) => {
            Ok(Some((bytes.clone(), Some("application/octet-stream"))))
        }
    }
}

fn redact_headers(headers: &HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .map(|(name, value)| {
            let name_string = name.as_str().to_owned();
            let value = if is_sensitive_header(name.as_str()) {
                REDACTED.to_owned()
            } else {
                value
                    .to_str()
                    .map_or_else(|_| "[BINARY]".to_owned(), ToOwned::to_owned)
            };
            (name_string, value)
        })
        .collect()
}

fn is_sensitive_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "authorization"
            | "cookie"
            | "set-cookie"
            | "proxy-authorization"
            | "x-api-key"
            | "x-subscription-token"
    )
}

fn is_textual(content_type: Option<&str>) -> bool {
    content_type.is_some_and(|value| {
        let value = value.to_ascii_lowercase();
        value.starts_with("text/")
            || value.contains("json")
            || value.contains("xml")
            || value.contains("javascript")
            || value.contains("x-www-form-urlencoded")
    })
}

async fn write_artifact(root: &Path, bytes: &[u8]) -> Result<ArtifactId, WebDependencyError> {
    fs::create_dir_all(root)
        .await
        .map_err(|_| WebDependencyError::Artifact)?;
    let id = ArtifactId::from_uuid(Uuid::now_v7());
    let final_path = root.join(format!("{id}.bin"));
    let temporary_path = root.join(format!(".{id}.tmp"));
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary_path)
        .await
        .map_err(|_| WebDependencyError::Artifact)?;
    file.write_all(bytes)
        .await
        .map_err(|_| WebDependencyError::Artifact)?;
    file.sync_all()
        .await
        .map_err(|_| WebDependencyError::Artifact)?;
    drop(file);
    fs::rename(&temporary_path, &final_path)
        .await
        .map_err(|_| WebDependencyError::Artifact)?;
    Ok(id)
}

fn extract_fetch(
    response: DependencyHttpResponse,
    maximum: usize,
) -> Result<DependencyFetchResponse, WebDependencyError> {
    let is_pdf = response
        .content_type
        .as_deref()
        .is_some_and(|value| value.to_ascii_lowercase().contains("application/pdf"))
        || response.inline_body.starts_with("%PDF-");
    if is_pdf {
        return Ok(DependencyFetchResponse {
            canonical_url: response.final_url,
            title: None,
            description: None,
            text: String::new(),
            markdown: String::new(),
            links: Vec::new(),
            content_type: response.content_type,
            is_pdf: true,
            javascript_required: false,
            artifact: response.artifact,
            truncated: response.truncated,
            cached: false,
        });
    }
    if response.body_is_base64 {
        return Err(WebDependencyError::UnsupportedContent);
    }
    let document = Html::parse_document(&response.inline_body);
    let title = selector_text(&document, "title");
    let description = Selector::parse("meta[name=\"description\"]")
        .ok()
        .and_then(|selector| document.select(&selector).next())
        .and_then(|element| element.value().attr("content"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let base = Url::parse(&response.final_url).map_err(|_| WebDependencyError::InvalidUrl)?;
    let links = extract_links(&document, &base);
    let raw_text = extract_main_text(&document);
    let (text, text_truncated) = truncate_utf8(&raw_text, maximum);
    let markdown = text.clone();
    let script_count = Selector::parse("script")
        .ok()
        .map_or(0, |selector| document.select(&selector).count());
    let javascript_required = text.trim().len() < 80 && script_count >= 3;
    Ok(DependencyFetchResponse {
        canonical_url: response.final_url,
        title,
        description,
        text,
        markdown,
        links,
        content_type: response.content_type,
        is_pdf: false,
        javascript_required,
        artifact: response.artifact,
        truncated: response.truncated || text_truncated,
        cached: false,
    })
}

fn selector_text(document: &Html, selector: &str) -> Option<String> {
    let selector = Selector::parse(selector).ok()?;
    let value = document
        .select(&selector)
        .next()?
        .text()
        .collect::<Vec<_>>()
        .join(" ");
    let value = collapse_whitespace(&value);
    (!value.is_empty()).then_some(value)
}

fn extract_main_text(document: &Html) -> String {
    for candidate in ["main", "article", "body"] {
        if let Some(value) = selector_text(document, candidate) {
            return value;
        }
    }
    String::new()
}

fn extract_links(document: &Html, base: &Url) -> Vec<DependencyLink> {
    let Ok(selector) = Selector::parse("a[href]") else {
        return Vec::new();
    };
    document
        .select(&selector)
        .filter_map(|element| {
            let href = element.value().attr("href")?;
            let url = base.join(href).ok()?;
            if !matches!(url.scheme(), "http" | "https") {
                return None;
            }
            Some(DependencyLink {
                text: collapse_whitespace(&element.text().collect::<Vec<_>>().join(" ")),
                url: url.into(),
            })
        })
        .take(200)
        .collect()
}

fn collapse_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn truncate_utf8(value: &str, maximum: usize) -> (String, bool) {
    if value.len() <= maximum {
        return (value.to_owned(), false);
    }
    let mut end = maximum;
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    (value[..end].to_owned(), true)
}

fn mock_search(
    documents: &[MockSearchDocument],
    request: &DependencySearchRequest,
) -> DependencySearchResponse {
    let terms: Vec<String> = request
        .query
        .split_whitespace()
        .map(str::to_ascii_lowercase)
        .collect();
    let mut scored: Vec<(usize, &MockSearchDocument)> = documents
        .iter()
        .filter(|document| search_domain_allowed(&document.url, request))
        .filter_map(|document| {
            let haystack = format!("{} {}", document.title, document.snippet).to_ascii_lowercase();
            let score = terms
                .iter()
                .filter(|term| haystack.contains(term.as_str()))
                .count();
            (score > 0).then_some((score, document))
        })
        .collect();
    scored.sort_by(|(left_score, left), (right_score, right)| {
        right_score
            .cmp(left_score)
            .then_with(|| left.url.cmp(&right.url))
    });
    DependencySearchResponse {
        results: scored
            .into_iter()
            .take(usize::from(request.count))
            .map(|(_, document)| DependencySearchResult {
                title: document.title.clone(),
                url: document.url.clone(),
                snippet: document.snippet.clone(),
                published_at: document.published_at.clone(),
            })
            .collect(),
        provider: "mock".to_owned(),
    }
}

fn search_domain_allowed(url: &str, request: &DependencySearchRequest) -> bool {
    let Ok(url) = Url::parse(url) else {
        return false;
    };
    let Some(host) = url.host_str() else {
        return false;
    };
    !request
        .domain_denylist
        .iter()
        .any(|pattern| domain_matches(host, pattern))
        && (request.domain_allowlist.is_empty()
            || request
                .domain_allowlist
                .iter()
                .any(|pattern| domain_matches(host, pattern)))
}

fn now() -> TimestampMillis {
    let milliseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis();
    TimestampMillis::new(i64::try_from(milliseconds).unwrap_or(i64::MAX))
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "used directly as the Result::map_err conversion function"
)]
fn map_reqwest(error: reqwest::Error) -> WebDependencyError {
    if error.is_timeout() {
        WebDependencyError::Timeout
    } else if error.is_connect() {
        WebDependencyError::Connection
    } else {
        WebDependencyError::Transport
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "external filesystem details are deliberately reduced at the dependency boundary"
)]
fn redacted_io(_error: std::io::Error) -> WebDependencyError {
    WebDependencyError::ReplayState
}

/// Redacted external adapter failures.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum WebDependencyError {
    /// Invalid composition configuration.
    #[error("web dependency configuration is invalid")]
    InvalidConfiguration,
    /// Authorization failed.
    #[error("web action authorization failed")]
    Authorization,
    /// Nonce was already consumed.
    #[error("web action authorization was replayed")]
    AuthorizationReplay,
    /// Durable replay state could not be read or committed safely.
    #[error("web authorization replay state is unavailable")]
    ReplayState,
    /// A configured resource bound was reached.
    #[error("web dependency resource limit exceeded")]
    ResourceLimit,
    /// Cancellation ID already active.
    #[error("web cancellation identifier is already active")]
    DuplicateCancellation,
    /// Cancellation ID unknown.
    #[error("web cancellation identifier is unknown")]
    UnknownCancellation,
    /// Operation cancelled.
    #[error("web operation was cancelled")]
    Cancelled,
    /// Request shape invalid.
    #[error("web request is invalid")]
    InvalidRequest,
    /// URL invalid.
    #[error("web URL is invalid")]
    InvalidUrl,
    /// Mandatory policy denied the action.
    #[error("mandatory network policy denied the action")]
    PolicyDenied,
    /// DNS resolution failed.
    #[error("web target resolution failed")]
    Dns,
    /// Private/special target denied.
    #[error("private or special network target denied")]
    PrivateNetworkDenied,
    /// Redirect malformed.
    #[error("web redirect is invalid")]
    InvalidRedirect,
    /// Redirect limit exhausted.
    #[error("web redirect limit exceeded")]
    RedirectLimit,
    /// Deadline elapsed.
    #[error("web request timed out")]
    Timeout,
    /// Connection failed.
    #[error("web connection failed")]
    Connection,
    /// Other transport failure.
    #[error("web transport failed")]
    Transport,
    /// Response was larger than the hard bound.
    #[error("web response exceeded the configured size bound")]
    ResponseTooLarge,
    /// Artifact persistence failed.
    #[error("web response artifact could not be stored")]
    Artifact,
    /// Secret reference unavailable.
    #[error("web secret reference is unavailable")]
    SecretUnavailable,
    /// Provider response invalid.
    #[error("search provider returned an invalid response")]
    InvalidProviderResponse,
    /// Content cannot be extracted.
    #[error("web content type is unsupported")]
    UnsupportedContent,
}

#[cfg(test)]
mod tests {
    use agentmod_protocol_support::authorization::{AuthorizationClaims, seal_authorization};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;

    #[derive(Clone, Copy)]
    struct NoSecrets;

    #[async_trait]
    impl SecretDependencyPort for NoSecrets {
        async fn resolve(&self, _: &str) -> Result<String, WebDependencyError> {
            Err(WebDependencyError::SecretUnavailable)
        }
    }

    fn config(root: PathBuf, port_domain: &str) -> WebDependencyConfig {
        WebDependencyConfig {
            artifact_root: root,
            authorization_key_hex: "07".repeat(32),
            owner_id: "owner".to_owned(),
            session_id: "session".to_owned(),
            maximum_replay_entries: 128,
            maximum_active_calls: 8,
            network_policy: NetworkPolicy {
                allowed_domains: vec![port_domain.to_owned()],
                denied_domains: Vec::new(),
                allow_private_network: true,
                allow_plain_http: true,
                allowed_methods: BTreeSet::from(["GET".to_owned()]),
            },
            maximum_redirects: 3,
            maximum_timeout: Duration::from_secs(5),
            maximum_response_bytes: 4096,
            maximum_inline_bytes: 16,
            maximum_url_length: 2048,
            maximum_headers: 32,
            maximum_request_body_bytes: 1024,
            proxy_url: None,
            cache_entries: 2,
            search_provider: SearchProvider::Mock {
                documents: vec![
                    MockSearchDocument {
                        title: "Rust language".to_owned(),
                        url: "https://example.com/rust".to_owned(),
                        snippet: "Safe systems programming".to_owned(),
                        published_at: Some("2025-01-01".to_owned()),
                    },
                    MockSearchDocument {
                        title: "Cooking".to_owned(),
                        url: "https://example.net/cook".to_owned(),
                        snippet: "Recipes".to_owned(),
                        published_at: None,
                    },
                ],
            },
        }
    }

    fn authorization(
        action: &str,
        canonical: Vec<u8>,
        call: &str,
        cancellation_id: &str,
    ) -> DependencyAuthorization {
        let digest = ContentHash::digest(&canonical);
        let claims = AuthorizationClaims {
            owner: "owner".to_owned(),
            session: "session".to_owned(),
            call_id: call.to_owned(),
            action: action.to_owned(),
            normalized_digest: digest,
            issued_at: TimestampMillis::new(now().get() - 100),
            expires_at: TimestampMillis::new(now().get() + 10_000),
            nonce: format!("nonce-{call}"),
        };
        DependencyAuthorization {
            owner_id: claims.owner.clone(),
            session_id: claims.session.clone(),
            call_id: claims.call_id.clone(),
            action: action.to_owned(),
            normalized_digest: digest.to_hex(),
            grant: seal_authorization(&claims, &AuthorizationKey::from_bytes([7; 32]))
                .expect("seal"),
            canonical_operation: canonical,
            cancellation_id: cancellation_id.to_owned(),
        }
    }

    async fn test_server(response: &'static [u8]) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listen");
        let address = listener.local_addr().expect("address");
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = vec![0_u8; 2048];
            let _ = stream.read(&mut request).await.expect("read");
            stream.write_all(response).await.expect("write");
        });
        (format!("http://localhost:{}", address.port()), task)
    }

    #[tokio::test]
    async fn authorized_http_is_bounded_and_spills_an_artifact() {
        let (url, server) = test_server(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 20\r\n\r\n01234567890123456789",
        )
        .await;
        let root = tempfile::tempdir().expect("root");
        let dependency =
            ReqwestWebDependency::new(config(root.path().to_path_buf(), "localhost"), NoSecrets)
                .expect("dependency");
        let mut request = DependencyHttpRequest {
            authorization: authorization("http.request", Vec::new(), "call-1", "cancel-1"),
            method: "GET".to_owned(),
            url,
            query: BTreeMap::new(),
            headers: BTreeMap::new(),
            body: DependencyBody::Empty,
            max_redirects: 1,
            timeout: Duration::from_secs(2),
            max_response_bytes: 100,
            max_inline_bytes: 8,
        };
        let canonical = canonical_http_operation(&request).expect("canonical");
        request.authorization = authorization("http.request", canonical, "call-1", "cancel-1");
        let response = dependency.request(request).await.expect("request");
        assert_eq!(response.inline_body, "01234567");
        assert!(response.truncated);
        assert!(response.artifact.is_some());
        server.await.expect("server");
    }

    #[tokio::test]
    async fn redirect_to_denied_domain_is_blocked() {
        let (url, server) = test_server(
            b"HTTP/1.1 302 Found\r\nLocation: https://denied.invalid/private\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        let root = tempfile::tempdir().expect("root");
        let mut adapter_config = config(root.path().to_path_buf(), "localhost");
        adapter_config
            .network_policy
            .denied_domains
            .push("denied.invalid".to_owned());
        let dependency = ReqwestWebDependency::new(adapter_config, NoSecrets).expect("dependency");
        let mut request = DependencyHttpRequest {
            authorization: authorization("http.request", Vec::new(), "call-2", "cancel-2"),
            method: "GET".to_owned(),
            url,
            query: BTreeMap::new(),
            headers: BTreeMap::new(),
            body: DependencyBody::Empty,
            max_redirects: 1,
            timeout: Duration::from_secs(2),
            max_response_bytes: 100,
            max_inline_bytes: 8,
        };
        let canonical = canonical_http_operation(&request).expect("canonical");
        request.authorization = authorization("http.request", canonical, "call-2", "cancel-2");
        let error = dependency
            .request(request)
            .await
            .expect_err("redirect target denied");
        assert_eq!(error, WebDependencyError::PolicyDenied);
        server.await.expect("server");
    }

    #[tokio::test]
    async fn authorization_nonce_is_single_use() {
        let root = tempfile::tempdir().expect("root");
        let dependency =
            ReqwestWebDependency::new(config(root.path().to_path_buf(), "localhost"), NoSecrets)
                .expect("dependency");
        let mut request = DependencySearchRequest {
            authorization: authorization("web.search", Vec::new(), "call-3", "cancel-3"),
            query: "Rust systems".to_owned(),
            count: 5,
            freshness: None,
            domain_allowlist: vec!["example.com".to_owned()],
            domain_denylist: Vec::new(),
            language: None,
            locale: None,
            timeout: Duration::from_secs(1),
        };
        let canonical = canonical_search_operation(&request).expect("canonical");
        request.authorization = authorization("web.search", canonical, "call-3", "cancel-3");
        let response = dependency.search(request.clone()).await.expect("search");
        assert_eq!(response.results.len(), 1);
        assert_eq!(
            dependency.search(request).await,
            Err(WebDependencyError::AuthorizationReplay)
        );
    }

    #[tokio::test]
    async fn authorization_nonce_remains_consumed_after_restart() {
        let root = tempfile::tempdir().expect("root");
        let adapter_config = config(root.path().to_path_buf(), "localhost");
        let mut request = DependencySearchRequest {
            authorization: authorization(
                "web.search",
                Vec::new(),
                "restart-call",
                "restart-cancel",
            ),
            query: "Rust systems".to_owned(),
            count: 1,
            freshness: None,
            domain_allowlist: Vec::new(),
            domain_denylist: Vec::new(),
            language: None,
            locale: None,
            timeout: Duration::from_secs(1),
        };
        let canonical = canonical_search_operation(&request).expect("canonical");
        request.authorization =
            authorization("web.search", canonical, "restart-call", "restart-cancel");
        let first =
            ReqwestWebDependency::new(adapter_config.clone(), NoSecrets).expect("dependency");
        first.search(request.clone()).await.expect("first");
        drop(first);
        let restarted =
            ReqwestWebDependency::new(adapter_config, NoSecrets).expect("restarted dependency");
        assert_eq!(
            restarted.search(request).await,
            Err(WebDependencyError::AuthorizationReplay)
        );
    }

    #[tokio::test]
    async fn configured_identity_is_enforced_at_dependency_boundary() {
        let root = tempfile::tempdir().expect("root");
        let dependency =
            ReqwestWebDependency::new(config(root.path().to_path_buf(), "localhost"), NoSecrets)
                .expect("dependency");
        let mut request = DependencySearchRequest {
            authorization: authorization(
                "web.search",
                Vec::new(),
                "identity-call",
                "identity-cancel",
            ),
            query: "Rust systems".to_owned(),
            count: 1,
            freshness: None,
            domain_allowlist: Vec::new(),
            domain_denylist: Vec::new(),
            language: None,
            locale: None,
            timeout: Duration::from_secs(1),
        };
        let canonical = canonical_search_operation(&request).expect("canonical");
        request.authorization =
            authorization("web.search", canonical, "identity-call", "identity-cancel");
        request.authorization.owner_id = "other-owner".to_owned();
        assert_eq!(
            dependency.search(request).await,
            Err(WebDependencyError::Authorization)
        );
    }

    #[tokio::test]
    async fn fetch_extracts_metadata_text_and_absolute_links() {
        let body = "<html><head><title>AgentMod Docs</title><meta name=\"description\" content=\"Developer agent\"></head><body><main>Hello <a href=\"/guide\">guide</a></main></body></html>";
        let wire = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        let leaked: &'static [u8] = Box::leak(wire.into_bytes().into_boxed_slice());
        let (url, server) = test_server(leaked).await;
        let root = tempfile::tempdir().expect("root");
        let mut adapter_config = config(root.path().to_path_buf(), "localhost");
        adapter_config.maximum_inline_bytes = 1024;
        let dependency = ReqwestWebDependency::new(adapter_config, NoSecrets).expect("dependency");
        let mut request = DependencyFetchRequest {
            authorization: authorization("web.fetch", Vec::new(), "call-fetch", "cancel-fetch"),
            url: url.clone(),
            max_redirects: 1,
            timeout: Duration::from_secs(2),
            max_response_bytes: 2048,
            max_inline_bytes: 1024,
            use_cache: false,
        };
        let canonical = canonical_fetch_operation(&request).expect("canonical");
        request.authorization = authorization("web.fetch", canonical, "call-fetch", "cancel-fetch");
        let response = dependency.fetch(request).await.expect("fetch");
        assert_eq!(response.title.as_deref(), Some("AgentMod Docs"));
        assert_eq!(response.description.as_deref(), Some("Developer agent"));
        assert_eq!(response.text, "Hello guide");
        assert_eq!(response.links.len(), 1);
        assert!(response.links[0].url.starts_with(&url));
        server.await.expect("server");
    }

    #[test]
    fn private_network_classification_is_conservative() {
        assert!(is_private_or_special(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(is_private_or_special(IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert!(is_private_or_special(IpAddr::V4(Ipv4Addr::new(
            100, 64, 1, 1
        ))));
        assert!(!is_private_or_special(IpAddr::V4(Ipv4Addr::new(
            8, 8, 8, 8
        ))));
    }

    #[test]
    fn sensitive_response_headers_are_redacted() {
        let mut headers = HeaderMap::new();
        headers.insert("set-cookie", HeaderValue::from_static("token=secret"));
        headers.insert("content-type", HeaderValue::from_static("text/plain"));
        let redacted = redact_headers(&headers);
        assert_eq!(redacted["set-cookie"], REDACTED);
        assert_eq!(redacted["content-type"], "text/plain");
    }
}
