//! `WebDriver` transport, browser lifecycle, security, and artifact adapters.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    net::IpAddr,
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
use reqwest::Method;
use serde::Serialize;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::{fs, sync::Mutex};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

const ELEMENT_KEY: &str = "element-6066-11e4-a52e-4f735466cecf";

/// Network and browser bounds.
#[derive(Clone, Debug)]
pub struct BrowserDependencyConfig {
    /// Loopback or TLS `WebDriver` endpoint.
    pub webdriver_url: String,
    /// Requested browser name.
    pub browser_name: String,
    /// Root for screenshots, downloads, and replay records.
    pub artifact_root: PathBuf,
    /// Request timeout.
    pub request_timeout: Duration,
    /// Maximum source projection.
    pub maximum_inline_bytes: usize,
    /// Maximum screenshot or download bytes.
    pub maximum_artifact_bytes: usize,
    /// Maximum URL length.
    pub maximum_url_length: usize,
    /// Explicit destination domain allowlist; empty permits public HTTPS.
    pub allowed_domains: BTreeSet<String>,
    /// Whether navigation to loopback destinations is permitted.
    pub allow_loopback: bool,
    /// Runtime owner identity.
    pub authorization_owner: String,
    /// Runtime session identity.
    pub authorization_session: String,
    /// Hex-encoded authorization key.
    pub authorization_key_hex: String,
}

/// Dependency-owned exact authorization.
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyAuthorization {
    /// Tool call.
    pub call_id: String,
    /// Exact action.
    pub action: String,
    /// Canonical operation digest.
    pub normalized_digest: String,
    /// Signed grant.
    pub grant: String,
    /// Expanded arguments.
    pub arguments: Value,
    /// Bound cancellation.
    pub cancellation_id: String,
}

/// Browser operation.
#[derive(Clone, Debug, PartialEq)]
pub enum DependencyBrowserAction {
    /// Create the managed browser session.
    Start,
    /// Navigate and revalidate the final URL.
    Navigate {
        /// Destination URL.
        url: String,
    },
    /// Inspect bounded rendered markup, URL, and title.
    Inspect {
        /// Inline byte bound.
        maximum_bytes: usize,
    },
    /// Capture a PNG screenshot.
    Screenshot,
    /// Click a CSS-selected element.
    Click {
        /// CSS selector.
        selector: String,
    },
    /// Replace the value of a CSS-selected element.
    Type {
        /// CSS selector.
        selector: String,
        /// Replacement text.
        text: String,
    },
    /// Submit the form containing a CSS-selected element.
    Submit {
        /// CSS selector inside the form.
        selector: String,
    },
    /// Fetch through the rendered page and persist the result.
    Download {
        /// Download URL.
        url: String,
        /// Maximum accepted bytes.
        maximum_bytes: usize,
    },
    /// Close the managed session.
    Close,
}

/// Dependency request.
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyBrowserRequest {
    /// Authorization.
    pub authorization: DependencyAuthorization,
    /// Operation.
    pub action: DependencyBrowserAction,
}

/// Dependency result.
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyBrowserResponse {
    /// Bounded structured result.
    pub result: Value,
    /// Optional artifact ID.
    pub artifact: Option<String>,
    /// Whether inline data is incomplete.
    pub truncated: bool,
}

/// Dependency interface consumed only by browser data.
#[async_trait]
pub trait BrowserDependencyPort: Send + Sync {
    /// Executes one authorized operation.
    async fn execute(
        &self,
        request: DependencyBrowserRequest,
    ) -> Result<DependencyBrowserResponse, BrowserDependencyError>;
    /// Cancels one active call.
    async fn cancel(&self, cancellation_id: &str) -> Result<(), BrowserDependencyError>;
    /// Reports whether the driver endpoint is responsive.
    async fn health(&self) -> Result<Value, BrowserDependencyError>;
    /// Closes the managed browser session.
    async fn shutdown(&self);
}

/// Concrete `WebDriver` dependency.
#[derive(Clone)]
pub struct WebDriverBrowserDependency {
    config: Arc<BrowserDependencyConfig>,
    endpoint: Arc<Url>,
    client: reqwest::Client,
    session: Arc<Mutex<Option<String>>>,
    active: Arc<Mutex<BTreeMap<String, CancellationToken>>>,
    authorization_key: Arc<AuthorizationKey>,
}

impl WebDriverBrowserDependency {
    /// Validates and constructs the adapter.
    ///
    /// # Errors
    ///
    /// Rejects unsafe endpoints, bounds, identities, or keys.
    pub fn new(mut config: BrowserDependencyConfig) -> Result<Self, BrowserDependencyError> {
        let endpoint =
            Url::parse(&config.webdriver_url).map_err(|_| BrowserDependencyError::Configuration)?;
        let driver_is_loopback = endpoint
            .host_str()
            .and_then(|host| host.parse::<IpAddr>().ok())
            .is_some_and(|address| address.is_loopback())
            || endpoint.host_str() == Some("localhost");
        if config.browser_name.trim().is_empty()
            || config.artifact_root.as_os_str().is_empty()
            || config.request_timeout.is_zero()
            || config.maximum_inline_bytes == 0
            || config.maximum_artifact_bytes == 0
            || config.maximum_url_length == 0
            || config.authorization_owner.trim().is_empty()
            || config.authorization_session.trim().is_empty()
            || !matches!(endpoint.scheme(), "https" | "http")
            || (endpoint.scheme() == "http" && !driver_is_loopback)
        {
            return Err(BrowserDependencyError::Configuration);
        }
        let authorization_key = AuthorizationKey::from_hex(&config.authorization_key_hex)
            .map_err(|_| BrowserDependencyError::Configuration)?;
        config.authorization_key_hex.clear();
        std::fs::create_dir_all(config.artifact_root.join("authorization-replay"))
            .map_err(|_| BrowserDependencyError::Artifact)?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(config.request_timeout)
            .build()
            .map_err(|_| BrowserDependencyError::Configuration)?;
        Ok(Self {
            config: Arc::new(config),
            endpoint: Arc::new(endpoint),
            client,
            session: Arc::new(Mutex::new(None)),
            active: Arc::new(Mutex::new(BTreeMap::new())),
            authorization_key: Arc::new(authorization_key),
        })
    }

    fn authorize(
        &self,
        authorization: &DependencyAuthorization,
    ) -> Result<(), BrowserDependencyError> {
        let digest = ContentHash::digest(
            &canonical_operation(
                &authorization.action,
                &authorization.arguments,
                &authorization.cancellation_id,
            )
            .map_err(|_| BrowserDependencyError::Authorization)?,
        );
        if digest.to_hex() != authorization.normalized_digest {
            return Err(BrowserDependencyError::Authorization);
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| BrowserDependencyError::Authorization)?
            .as_millis();
        let claims = verify_authorization(
            &authorization.grant,
            &self.authorization_key,
            ExpectedAuthorization {
                owner: &self.config.authorization_owner,
                session: &self.config.authorization_session,
                call_id: &authorization.call_id,
                action: &authorization.action,
                normalized_digest: digest,
            },
            TimestampMillis::new(
                i64::try_from(now).map_err(|_| BrowserDependencyError::Authorization)?,
            ),
        )
        .map_err(|_| BrowserDependencyError::Authorization)?;
        let nonce_hash = blake3::hash(claims.nonce.as_bytes()).to_hex();
        let path = self
            .config
            .artifact_root
            .join("authorization-replay")
            .join(format!("{nonce_hash}.used"));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    BrowserDependencyError::AuthorizationReplay
                } else {
                    BrowserDependencyError::Artifact
                }
            })?;
        file.write_all(b"used\n")
            .and_then(|()| file.sync_all())
            .map_err(|_| BrowserDependencyError::Artifact)
    }

    async fn perform(
        &self,
        action: DependencyBrowserAction,
    ) -> Result<DependencyBrowserResponse, BrowserDependencyError> {
        match action {
            DependencyBrowserAction::Start => self.start().await,
            DependencyBrowserAction::Navigate { url } => self.navigate(&url).await,
            DependencyBrowserAction::Inspect { maximum_bytes } => self.inspect(maximum_bytes).await,
            DependencyBrowserAction::Screenshot => self.screenshot().await,
            DependencyBrowserAction::Click { selector } => self.click(&selector).await,
            DependencyBrowserAction::Type { selector, text } => {
                self.type_text(&selector, &text).await
            }
            DependencyBrowserAction::Submit { selector } => self.submit(&selector).await,
            DependencyBrowserAction::Download { url, maximum_bytes } => {
                self.download(&url, maximum_bytes).await
            }
            DependencyBrowserAction::Close => self.close().await,
        }
    }

    async fn start(&self) -> Result<DependencyBrowserResponse, BrowserDependencyError> {
        if let Some(session) = self.session.lock().await.clone() {
            return Ok(response(json!({"session_id":session,"reused":true})));
        }
        let value = self
            .webdriver(
                Method::POST,
                "session",
                Some(json!({
                    "capabilities": {
                        "alwaysMatch": {
                            "browserName": self.config.browser_name,
                        }
                    }
                })),
            )
            .await?;
        let session_id = value
            .get("sessionId")
            .or_else(|| value.get("session_id"))
            .and_then(Value::as_str)
            .ok_or(BrowserDependencyError::Protocol)?
            .to_owned();
        *self.session.lock().await = Some(session_id.clone());
        Ok(response(json!({
            "session_id":session_id,
            "browser":self.config.browser_name,
            "reused":false,
        })))
    }

    async fn navigate(
        &self,
        requested_url: &str,
    ) -> Result<DependencyBrowserResponse, BrowserDependencyError> {
        self.validate_destination(requested_url)?;
        let session = self.require_session().await?;
        self.webdriver(
            Method::POST,
            &format!("session/{session}/url"),
            Some(json!({"url":requested_url})),
        )
        .await?;
        let final_url = self
            .webdriver(Method::GET, &format!("session/{session}/url"), None)
            .await?
            .as_str()
            .ok_or(BrowserDependencyError::Protocol)?
            .to_owned();
        self.validate_destination(&final_url)?;
        let title = self
            .webdriver(Method::GET, &format!("session/{session}/title"), None)
            .await?
            .as_str()
            .unwrap_or_default()
            .to_owned();
        Ok(response(json!({
            "requested_url":requested_url,
            "url":final_url,
            "title":title,
        })))
    }

    async fn inspect(
        &self,
        maximum_bytes: usize,
    ) -> Result<DependencyBrowserResponse, BrowserDependencyError> {
        if maximum_bytes == 0 || maximum_bytes > self.config.maximum_inline_bytes {
            return Err(BrowserDependencyError::InvalidRequest);
        }
        let session = self.require_session().await?;
        let source = self
            .webdriver(Method::GET, &format!("session/{session}/source"), None)
            .await?
            .as_str()
            .ok_or(BrowserDependencyError::Protocol)?
            .to_owned();
        let url = self
            .webdriver(Method::GET, &format!("session/{session}/url"), None)
            .await?;
        let title = self
            .webdriver(Method::GET, &format!("session/{session}/title"), None)
            .await?;
        let (source, truncated) = truncate_utf8(&source, maximum_bytes);
        Ok(DependencyBrowserResponse {
            result: json!({"url":url,"title":title,"html":source}),
            artifact: None,
            truncated,
        })
    }

    async fn screenshot(&self) -> Result<DependencyBrowserResponse, BrowserDependencyError> {
        let session = self.require_session().await?;
        let encoded = self
            .webdriver(Method::GET, &format!("session/{session}/screenshot"), None)
            .await?
            .as_str()
            .ok_or(BrowserDependencyError::Protocol)?
            .to_owned();
        let bytes = BASE64
            .decode(encoded)
            .map_err(|_| BrowserDependencyError::Protocol)?;
        if bytes.len() > self.config.maximum_artifact_bytes {
            return Err(BrowserDependencyError::TooLarge);
        }
        let artifact = write_artifact(&self.config.artifact_root, &bytes, "image/png").await?;
        Ok(DependencyBrowserResponse {
            result: json!({"mime_type":"image/png","bytes":bytes.len()}),
            artifact: Some(artifact),
            truncated: false,
        })
    }

    async fn click(
        &self,
        selector: &str,
    ) -> Result<DependencyBrowserResponse, BrowserDependencyError> {
        let (session, element) = self.element(selector).await?;
        self.webdriver(
            Method::POST,
            &format!("session/{session}/element/{element}/click"),
            Some(json!({})),
        )
        .await?;
        Ok(response(json!({"selector":selector,"clicked":true})))
    }

    async fn type_text(
        &self,
        selector: &str,
        text: &str,
    ) -> Result<DependencyBrowserResponse, BrowserDependencyError> {
        if text.len() > 64 * 1024 {
            return Err(BrowserDependencyError::InvalidRequest);
        }
        let (session, element) = self.element(selector).await?;
        self.webdriver(
            Method::POST,
            &format!("session/{session}/element/{element}/clear"),
            Some(json!({})),
        )
        .await?;
        self.webdriver(
            Method::POST,
            &format!("session/{session}/element/{element}/value"),
            Some(json!({"text":text})),
        )
        .await?;
        Ok(response(json!({
            "selector":selector,
            "characters":text.chars().count(),
        })))
    }

    async fn submit(
        &self,
        selector: &str,
    ) -> Result<DependencyBrowserResponse, BrowserDependencyError> {
        let (session, element) = self.element(selector).await?;
        self.webdriver(
            Method::POST,
            &format!("session/{session}/execute/sync"),
            Some(json!({
                "script":"const e=arguments[0]; const f=e.form||e.closest('form'); if(!f) throw new Error('no form'); if(f.requestSubmit) f.requestSubmit(); else f.submit();",
                "args":[{ELEMENT_KEY:element}],
            })),
        )
        .await?;
        Ok(response(json!({"selector":selector,"submitted":true})))
    }

    async fn download(
        &self,
        requested_url: &str,
        maximum_bytes: usize,
    ) -> Result<DependencyBrowserResponse, BrowserDependencyError> {
        self.validate_destination(requested_url)?;
        if maximum_bytes == 0 || maximum_bytes > self.config.maximum_artifact_bytes {
            return Err(BrowserDependencyError::InvalidRequest);
        }
        let session = self.require_session().await?;
        let value = self
            .webdriver(
                Method::POST,
                &format!("session/{session}/execute/async"),
                Some(json!({
                    "script":"const u=arguments[0], done=arguments[arguments.length-1]; fetch(u,{credentials:'include'}).then(async r=>{const b=new Uint8Array(await r.arrayBuffer()); let s=''; for(const x of b)s+=String.fromCharCode(x); done({url:r.url,mime:r.headers.get('content-type')||'application/octet-stream',base64:btoa(s)});}).catch(e=>done({error:String(e)}));",
                    "args":[requested_url],
                })),
            )
            .await?;
        if value.get("error").is_some() {
            return Err(BrowserDependencyError::Remote);
        }
        let final_url = value
            .get("url")
            .and_then(Value::as_str)
            .unwrap_or(requested_url);
        self.validate_destination(final_url)?;
        let bytes = BASE64
            .decode(
                value
                    .get("base64")
                    .and_then(Value::as_str)
                    .ok_or(BrowserDependencyError::Protocol)?,
            )
            .map_err(|_| BrowserDependencyError::Protocol)?;
        if bytes.len() > maximum_bytes {
            return Err(BrowserDependencyError::TooLarge);
        }
        let mime = value
            .get("mime")
            .and_then(Value::as_str)
            .unwrap_or("application/octet-stream");
        let artifact = write_artifact(&self.config.artifact_root, &bytes, mime).await?;
        Ok(DependencyBrowserResponse {
            result: json!({"url":final_url,"mime_type":mime,"bytes":bytes.len()}),
            artifact: Some(artifact),
            truncated: false,
        })
    }

    async fn close(&self) -> Result<DependencyBrowserResponse, BrowserDependencyError> {
        let Some(session) = self.session.lock().await.take() else {
            return Ok(response(json!({"closed":false})));
        };
        self.webdriver(Method::DELETE, &format!("session/{session}"), None)
            .await?;
        Ok(response(json!({"closed":true})))
    }

    async fn element(&self, selector: &str) -> Result<(String, String), BrowserDependencyError> {
        validate_selector(selector)?;
        let session = self.require_session().await?;
        let value = self
            .webdriver(
                Method::POST,
                &format!("session/{session}/element"),
                Some(json!({"using":"css selector","value":selector})),
            )
            .await?;
        let element = value
            .get(ELEMENT_KEY)
            .or_else(|| value.get("ELEMENT"))
            .and_then(Value::as_str)
            .ok_or(BrowserDependencyError::Protocol)?
            .to_owned();
        Ok((session, element))
    }

    async fn require_session(&self) -> Result<String, BrowserDependencyError> {
        self.session
            .lock()
            .await
            .clone()
            .ok_or(BrowserDependencyError::NoSession)
    }

    async fn webdriver(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, BrowserDependencyError> {
        let url = self
            .endpoint
            .join(path)
            .map_err(|_| BrowserDependencyError::Protocol)?;
        let mut request = self.client.request(method, url);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .map_err(|_| BrowserDependencyError::Transport)?;
        if !response.status().is_success() {
            return Err(BrowserDependencyError::Remote);
        }
        let body: Value = response
            .json()
            .await
            .map_err(|_| BrowserDependencyError::Protocol)?;
        let value = body.get("value").cloned().unwrap_or(body);
        if value.get("error").is_some() {
            return Err(BrowserDependencyError::Remote);
        }
        Ok(value)
    }

    fn validate_destination(&self, value: &str) -> Result<(), BrowserDependencyError> {
        if value.len() > self.config.maximum_url_length {
            return Err(BrowserDependencyError::NetworkPolicy);
        }
        let url = Url::parse(value).map_err(|_| BrowserDependencyError::NetworkPolicy)?;
        if url.username() != "" || url.password().is_some() || url.fragment().is_some() {
            return Err(BrowserDependencyError::NetworkPolicy);
        }
        let host = url
            .host_str()
            .ok_or(BrowserDependencyError::NetworkPolicy)?
            .to_ascii_lowercase();
        let loopback = host == "localhost"
            || host
                .parse::<IpAddr>()
                .is_ok_and(|address| address.is_loopback());
        if (url.scheme() != "https" && !(url.scheme() == "http" && loopback))
            || (loopback && !self.config.allow_loopback)
            || host
                .parse::<IpAddr>()
                .is_ok_and(|address| !is_public_or_loopback(address))
            || (!self.config.allowed_domains.is_empty()
                && !self
                    .config
                    .allowed_domains
                    .iter()
                    .any(|domain| host == *domain || host.ends_with(&format!(".{domain}"))))
        {
            return Err(BrowserDependencyError::NetworkPolicy);
        }
        Ok(())
    }
}

#[async_trait]
impl BrowserDependencyPort for WebDriverBrowserDependency {
    async fn execute(
        &self,
        request: DependencyBrowserRequest,
    ) -> Result<DependencyBrowserResponse, BrowserDependencyError> {
        self.authorize(&request.authorization)?;
        let cancellation_id = request.authorization.cancellation_id;
        let token = CancellationToken::new();
        if self
            .active
            .lock()
            .await
            .insert(cancellation_id.clone(), token.clone())
            .is_some()
        {
            return Err(BrowserDependencyError::DuplicateCancellation);
        }
        let result = tokio::select! {
            () = token.cancelled() => Err(BrowserDependencyError::Cancelled),
            value = self.perform(request.action) => value,
        };
        self.active.lock().await.remove(&cancellation_id);
        result
    }

    async fn cancel(&self, cancellation_id: &str) -> Result<(), BrowserDependencyError> {
        self.active
            .lock()
            .await
            .get(cancellation_id)
            .cloned()
            .ok_or(BrowserDependencyError::UnknownCancellation)?
            .cancel();
        Ok(())
    }

    async fn health(&self) -> Result<Value, BrowserDependencyError> {
        let response = self
            .client
            .get(
                self.endpoint
                    .join("status")
                    .map_err(|_| BrowserDependencyError::Protocol)?,
            )
            .send()
            .await
            .map_err(|_| BrowserDependencyError::Transport)?;
        Ok(json!({
            "healthy":response.status().is_success(),
            "session_active":self.session.lock().await.is_some(),
        }))
    }

    async fn shutdown(&self) {
        let _ = self.close().await;
    }
}

/// Dependency failure classes contained at the adapter boundary.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum BrowserDependencyError {
    /// Invalid bootstrap.
    #[error("invalid browser dependency configuration")]
    Configuration,
    /// Invalid operation.
    #[error("invalid browser request")]
    InvalidRequest,
    /// Authorization failed.
    #[error("browser authorization failed")]
    Authorization,
    /// Grant replay.
    #[error("browser authorization was replayed")]
    AuthorizationReplay,
    /// No active browser.
    #[error("browser session is not active")]
    NoSession,
    /// URL policy rejected the destination.
    #[error("browser network policy rejected the destination")]
    NetworkPolicy,
    /// `WebDriver` transport failed.
    #[error("browser driver transport failed")]
    Transport,
    /// `WebDriver` rejected the command.
    #[error("browser driver rejected the command")]
    Remote,
    /// Invalid `WebDriver` response.
    #[error("browser driver protocol failed")]
    Protocol,
    /// Artifact persistence failed.
    #[error("browser artifact persistence failed")]
    Artifact,
    /// Output exceeded a configured bound.
    #[error("browser output exceeded its bound")]
    TooLarge,
    /// Cancellation identifier is already active.
    #[error("duplicate browser cancellation identifier")]
    DuplicateCancellation,
    /// Cancellation identifier is unknown.
    #[error("unknown browser cancellation identifier")]
    UnknownCancellation,
    /// Operation was cancelled.
    #[error("browser operation cancelled")]
    Cancelled,
}

/// Canonical bytes independently reconstructed by runtime and dependency.
///
/// # Errors
///
/// Rejects non-object arguments and serialization failure.
pub fn canonical_operation(
    action: &str,
    arguments: &Value,
    cancellation_id: &str,
) -> Result<Vec<u8>, BrowserDependencyError> {
    if action.trim().is_empty() || !arguments.is_object() || cancellation_id.trim().is_empty() {
        return Err(BrowserDependencyError::InvalidRequest);
    }
    serde_json::to_vec(&(action, normalize_json(arguments), cancellation_id))
        .map_err(|_| BrowserDependencyError::Protocol)
}

fn normalize_json(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let sorted: BTreeMap<_, _> = object
                .iter()
                .map(|(key, value)| (key.clone(), normalize_json(value)))
                .collect();
            serde_json::to_value(sorted).unwrap_or(Value::Null)
        }
        Value::Array(values) => Value::Array(values.iter().map(normalize_json).collect()),
        _ => value.clone(),
    }
}

fn validate_selector(value: &str) -> Result<(), BrowserDependencyError> {
    if value.trim().is_empty() || value.len() > 4096 || value.contains('\0') {
        Err(BrowserDependencyError::InvalidRequest)
    } else {
        Ok(())
    }
}

fn is_public_or_loopback(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(value) => {
            !(value.is_private()
                || value.is_link_local()
                || value.is_broadcast()
                || value.is_documentation()
                || value.is_unspecified())
                || value.is_loopback()
        }
        IpAddr::V6(value) => {
            !(value.is_unique_local() || value.is_unicast_link_local() || value.is_unspecified())
                || value.is_loopback()
        }
    }
}

fn truncate_utf8(value: &str, maximum: usize) -> (String, bool) {
    if value.len() <= maximum {
        return (value.to_owned(), false);
    }
    let mut boundary = maximum;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    (value[..boundary].to_owned(), true)
}

fn response(result: Value) -> DependencyBrowserResponse {
    DependencyBrowserResponse {
        result,
        artifact: None,
        truncated: false,
    }
}

#[derive(Serialize)]
struct ArtifactMetadata<'a> {
    artifact_id: &'a str,
    content_hash: String,
    mime_type: &'a str,
    byte_size: usize,
    producer: &'static str,
    security_classification: &'static str,
    retention: &'static str,
}

async fn write_artifact(
    root: &Path,
    bytes: &[u8],
    mime: &str,
) -> Result<String, BrowserDependencyError> {
    fs::create_dir_all(root)
        .await
        .map_err(|_| BrowserDependencyError::Artifact)?;
    let id = ArtifactId::from_uuid(Uuid::now_v7()).to_string();
    let temporary = root.join(format!(".{id}.tmp"));
    let final_path = root.join(format!("{id}.bin"));
    fs::write(&temporary, bytes)
        .await
        .map_err(|_| BrowserDependencyError::Artifact)?;
    fs::rename(&temporary, &final_path)
        .await
        .map_err(|_| BrowserDependencyError::Artifact)?;
    let metadata = ArtifactMetadata {
        artifact_id: &id,
        content_hash: ContentHash::digest(bytes).to_hex(),
        mime_type: mime,
        byte_size: bytes.len(),
        producer: "browser-host",
        security_classification: "private",
        retention: "session",
    };
    fs::write(
        root.join(format!("{id}.metadata.json")),
        serde_json::to_vec(&metadata).map_err(|_| BrowserDependencyError::Artifact)?,
    )
    .await
    .map_err(|_| BrowserDependencyError::Artifact)?;
    Ok(id)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc, time::Duration};

    use serde_json::{Value, json};
    use tempfile::TempDir;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::Mutex,
    };

    use super::{BrowserDependencyConfig, BrowserDependencyPort, WebDriverBrowserDependency};

    #[tokio::test]
    async fn real_webdriver_fixture_covers_rendering_interaction_artifacts_and_shutdown() {
        let (address, seen, server) = spawn_fixture().await;
        let root = TempDir::new().expect("tempdir");
        let dependency = WebDriverBrowserDependency::new(BrowserDependencyConfig {
            webdriver_url: format!("http://{address}/"),
            browser_name: "fixture".to_owned(),
            artifact_root: root.path().to_path_buf(),
            request_timeout: Duration::from_secs(2),
            maximum_inline_bytes: 16,
            maximum_artifact_bytes: 1024,
            maximum_url_length: 4096,
            allowed_domains: BTreeSet::new(),
            allow_loopback: true,
            authorization_owner: "runtime".to_owned(),
            authorization_session: "session".to_owned(),
            authorization_key_hex: "11".repeat(32),
        })
        .expect("dependency");

        let started = dependency.start().await.expect("start");
        assert_eq!(started.result["session_id"], "fixture-session");
        let navigation = dependency
            .navigate("http://127.0.0.1/page")
            .await
            .expect("navigate");
        assert_eq!(navigation.result["title"], "Fixture");
        let inspection = dependency.inspect(16).await.expect("inspect");
        assert!(inspection.truncated);
        dependency.click("#button").await.expect("click");
        dependency.type_text("#input", "hello").await.expect("type");
        dependency.submit("#input").await.expect("submit");
        let screenshot = dependency.screenshot().await.expect("screenshot");
        assert!(
            root.path()
                .join(format!("{}.bin", screenshot.artifact.expect("artifact")))
                .is_file()
        );
        let download = dependency
            .download("http://127.0.0.1/file", 1024)
            .await
            .expect("download");
        assert!(
            root.path()
                .join(format!(
                    "{}.metadata.json",
                    download.artifact.expect("artifact")
                ))
                .is_file()
        );
        assert_eq!(dependency.health().await.expect("health")["healthy"], true);
        assert_eq!(
            dependency.close().await.expect("close").result["closed"],
            true
        );

        let paths = seen.lock().await.clone();
        assert!(paths.contains(&"POST /session".to_owned()));
        assert!(paths.contains(&"GET /session/fixture-session/screenshot".to_owned()));
        assert!(paths.contains(&"POST /session/fixture-session/execute/async".to_owned()));
        assert!(paths.contains(&"DELETE /session/fixture-session".to_owned()));
        server.abort();
    }

    async fn spawn_fixture() -> (
        std::net::SocketAddr,
        Arc<Mutex<Vec<String>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listen");
        let address = listener.local_addr().expect("address");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let server_seen = Arc::clone(&seen);
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let seen = Arc::clone(&server_seen);
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let mut buffer = [0_u8; 4096];
                    loop {
                        let count = stream.read(&mut buffer).await.expect("read");
                        request.extend_from_slice(&buffer[..count]);
                        if request.windows(4).any(|window| window == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let header_end = request
                        .windows(4)
                        .position(|window| window == b"\r\n\r\n")
                        .expect("header")
                        + 4;
                    let headers = String::from_utf8_lossy(&request[..header_end]).into_owned();
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .and_then(|value| value.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    while request.len() < header_end + content_length {
                        let count = stream.read(&mut buffer).await.expect("body");
                        request.extend_from_slice(&buffer[..count]);
                    }
                    let first = headers.lines().next().expect("request line");
                    let mut parts = first.split_whitespace();
                    let method = parts.next().expect("method");
                    let path = parts.next().expect("path");
                    seen.lock().await.push(format!("{method} {path}"));
                    let value = route(method, path);
                    let body = serde_json::to_vec(&json!({"value":value})).expect("json");
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        body.len()
                    );
                    stream.write_all(response.as_bytes()).await.expect("head");
                    stream.write_all(&body).await.expect("response");
                });
            }
        });
        (address, seen, server)
    }

    fn route(method: &str, path: &str) -> Value {
        match (method, path) {
            ("POST", "/session") => {
                json!({"sessionId":"fixture-session","capabilities":{"browserName":"fixture"}})
            }
            ("GET", "/session/fixture-session/url") => json!("http://127.0.0.1/page"),
            ("GET", "/session/fixture-session/title") => json!("Fixture"),
            ("GET", "/session/fixture-session/source") => {
                json!("<html><body>rendered fixture content</body></html>")
            }
            ("GET", "/session/fixture-session/screenshot") => json!("iVBORw0KGgo="),
            ("POST", "/session/fixture-session/element") => {
                json!({super::ELEMENT_KEY:"element-1"})
            }
            ("POST", "/session/fixture-session/execute/async") => json!({
                "url":"http://127.0.0.1/file",
                "mime":"text/plain",
                "base64":"ZG93bmxvYWQ=",
            }),
            _ => Value::Null,
        }
    }
}
