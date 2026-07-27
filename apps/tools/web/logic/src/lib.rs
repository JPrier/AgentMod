//! Provider-independent Web host business rules.

use std::{collections::BTreeMap, time::Duration};

use agentmod_web_host_data::{
    DataAuthorization, DataBody, DataHeaderValue, FetchDataRecord, FetchDataRequest,
    HttpDataRecord, HttpDataRequest, SearchDataRecord, SearchDataRequest, WebDataError,
    WebDataPort,
};
use async_trait::async_trait;
use thiserror::Error;

/// Logic-owned caller identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebIdentity {
    /// Owner.
    pub owner_id: String,
    /// Session.
    pub session_id: String,
}

/// Logic-owned authorization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebAuthorization {
    /// Caller.
    pub identity: WebIdentity,
    /// Call.
    pub call_id: String,
    /// Action.
    pub tool: String,
    /// Digest.
    pub normalized_digest: String,
    /// Grant.
    pub grant: String,
    /// Canonical operation.
    pub canonical_operation: Vec<u8>,
    /// Cancellation ID.
    pub cancellation_id: String,
}

/// Logic header value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeaderValue {
    /// Literal.
    Literal(String),
    /// Secret reference.
    SecretReference(String),
}

/// Logic body.
#[derive(Clone, Debug, PartialEq)]
pub enum RequestBody {
    /// Empty.
    Empty,
    /// Text.
    Text(String),
    /// JSON.
    Json(serde_json::Value),
    /// Form.
    Form(BTreeMap<String, String>),
    /// Binary.
    Binary(Vec<u8>),
}

/// HTTP use case.
#[derive(Clone, Debug, PartialEq)]
pub struct HttpRequestCommand {
    /// Authorization.
    pub authorization: WebAuthorization,
    /// Method.
    pub method: String,
    /// Absolute URL.
    pub url: String,
    /// Query.
    pub query: BTreeMap<String, String>,
    /// Headers.
    pub headers: BTreeMap<String, HeaderValue>,
    /// Body.
    pub body: RequestBody,
    /// Redirect limit.
    pub max_redirects: u8,
    /// Timeout.
    pub timeout: Duration,
    /// Response bound.
    pub max_response_bytes: usize,
    /// Projection bound.
    pub max_inline_bytes: usize,
}

/// Fetch use case.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchCommand {
    /// Authorization.
    pub authorization: WebAuthorization,
    /// URL.
    pub url: String,
    /// Redirect limit.
    pub max_redirects: u8,
    /// Timeout.
    pub timeout: Duration,
    /// Response bound.
    pub max_response_bytes: usize,
    /// Projection bound.
    pub max_inline_bytes: usize,
    /// Cache permission.
    pub use_cache: bool,
}

/// Search use case.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchCommand {
    /// Authorization.
    pub authorization: WebAuthorization,
    /// Query.
    pub query: String,
    /// Count.
    pub count: u8,
    /// Freshness.
    pub freshness: Option<String>,
    /// Result allowlist.
    pub domain_allowlist: Vec<String>,
    /// Result denylist.
    pub domain_denylist: Vec<String>,
    /// Language.
    pub language: Option<String>,
    /// Locale.
    pub locale: Option<String>,
    /// Timeout.
    pub timeout: Duration,
}

/// Logic HTTP result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpResult {
    /// Status.
    pub status: u16,
    /// Final URL.
    pub final_url: String,
    /// Redacted headers.
    pub headers: BTreeMap<String, String>,
    /// MIME.
    pub content_type: Option<String>,
    /// Projection.
    pub body: String,
    /// Base64 flag.
    pub body_is_base64: bool,
    /// Full size.
    pub total_bytes: u64,
    /// Artifact.
    pub artifact_id: Option<String>,
    /// Truncation.
    pub truncated: bool,
}

/// Logic link result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LinkResult {
    /// Label.
    pub text: String,
    /// URL.
    pub url: String,
}

/// Logic fetch result.
#[allow(
    clippy::struct_excessive_bools,
    reason = "orthogonal user-visible facts are not mutually exclusive states"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchResult {
    /// Canonical URL.
    pub canonical_url: String,
    /// Title.
    pub title: Option<String>,
    /// Description.
    pub description: Option<String>,
    /// Clean text.
    pub text: String,
    /// Markdown.
    pub markdown: String,
    /// Links.
    pub links: Vec<LinkResult>,
    /// MIME.
    pub content_type: Option<String>,
    /// PDF signal.
    pub is_pdf: bool,
    /// JavaScript signal.
    pub javascript_required: bool,
    /// Artifact.
    pub artifact_id: Option<String>,
    /// Truncation.
    pub truncated: bool,
    /// Cache hit.
    pub cached: bool,
}

/// Logic search item.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchItem {
    /// Title.
    pub title: String,
    /// URL.
    pub url: String,
    /// Snippet.
    pub snippet: String,
    /// Publication date.
    pub published_at: Option<String>,
}

/// Logic search result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchResult {
    /// Results.
    pub results: Vec<SearchItem>,
    /// Provider provenance.
    pub provider: String,
}

/// Business limits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebLogicConfig {
    /// Maximum URL characters.
    pub maximum_url_length: usize,
    /// Maximum query characters.
    pub maximum_query_length: usize,
    /// Maximum result count.
    pub maximum_search_results: u8,
    /// Maximum headers.
    pub maximum_headers: usize,
    /// Maximum body bytes.
    pub maximum_request_body_bytes: usize,
    /// Maximum timeout.
    pub maximum_timeout: Duration,
    /// Maximum redirects.
    pub maximum_redirects: u8,
    /// Maximum response.
    pub maximum_response_bytes: usize,
    /// Maximum projection.
    pub maximum_inline_bytes: usize,
}

/// Logic contract consumed only by service.
#[async_trait]
pub trait WebLogicPort: Send + Sync {
    /// Executes HTTP.
    async fn request(&self, command: HttpRequestCommand) -> Result<HttpResult, WebLogicError>;
    /// Fetches a page.
    async fn fetch(&self, command: FetchCommand) -> Result<FetchResult, WebLogicError>;
    /// Searches.
    async fn search(&self, command: SearchCommand) -> Result<SearchResult, WebLogicError>;
    /// Cancels an active operation.
    async fn cancel(&self, cancellation_id: &str) -> Result<String, WebLogicError>;
}

/// Web use-case coordinator.
#[derive(Clone)]
pub struct WebLogic<D> {
    data: D,
    config: WebLogicConfig,
}

impl<D> WebLogic<D> {
    /// Constructs validated logic.
    ///
    /// # Errors
    ///
    /// Rejects unusable limits.
    pub fn new(data: D, config: WebLogicConfig) -> Result<Self, WebLogicError> {
        if config.maximum_url_length == 0
            || config.maximum_query_length == 0
            || config.maximum_search_results == 0
            || config.maximum_headers == 0
            || config.maximum_request_body_bytes == 0
            || config.maximum_timeout.is_zero()
            || config.maximum_response_bytes == 0
            || config.maximum_inline_bytes == 0
            || config.maximum_inline_bytes > config.maximum_response_bytes
        {
            return Err(WebLogicError::InvalidConfiguration);
        }
        Ok(Self { data, config })
    }
}

#[async_trait]
impl<D: WebDataPort> WebLogicPort for WebLogic<D> {
    async fn request(&self, mut command: HttpRequestCommand) -> Result<HttpResult, WebLogicError> {
        validate_authorization(&command.authorization)?;
        command.method.make_ascii_uppercase();
        if !matches!(
            command.method.as_str(),
            "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" | "OPTIONS"
        ) || !valid_url_shape(&command.url, self.config.maximum_url_length)
            || command.headers.len() > self.config.maximum_headers
            || body_size(&command.body) > self.config.maximum_request_body_bytes
            || !valid_common_bounds(
                command.timeout,
                command.max_redirects,
                command.max_response_bytes,
                command.max_inline_bytes,
                &self.config,
            )
        {
            return Err(WebLogicError::InvalidCommand);
        }
        let result = self
            .data
            .request(HttpDataRequest {
                authorization: map_authorization(command.authorization),
                method: command.method,
                url: command.url,
                query: command.query,
                headers: command
                    .headers
                    .into_iter()
                    .map(|(name, value)| (name, map_header(value)))
                    .collect(),
                body: map_body(command.body),
                max_redirects: command.max_redirects,
                timeout: command.timeout,
                max_response_bytes: command.max_response_bytes,
                max_inline_bytes: command.max_inline_bytes,
            })
            .await
            .map_err(map_data_error)?;
        Ok(map_http(result))
    }

    async fn fetch(&self, command: FetchCommand) -> Result<FetchResult, WebLogicError> {
        validate_authorization(&command.authorization)?;
        if !valid_url_shape(&command.url, self.config.maximum_url_length)
            || !valid_common_bounds(
                command.timeout,
                command.max_redirects,
                command.max_response_bytes,
                command.max_inline_bytes,
                &self.config,
            )
        {
            return Err(WebLogicError::InvalidCommand);
        }
        let result = self
            .data
            .fetch(FetchDataRequest {
                authorization: map_authorization(command.authorization),
                url: command.url,
                max_redirects: command.max_redirects,
                timeout: command.timeout,
                max_response_bytes: command.max_response_bytes,
                max_inline_bytes: command.max_inline_bytes,
                use_cache: command.use_cache,
            })
            .await
            .map_err(map_data_error)?;
        Ok(map_fetch(result))
    }

    async fn search(&self, command: SearchCommand) -> Result<SearchResult, WebLogicError> {
        validate_authorization(&command.authorization)?;
        let query = command.query.trim();
        if query.is_empty()
            || query.len() > self.config.maximum_query_length
            || command.count == 0
            || command.count > self.config.maximum_search_results
            || command.timeout.is_zero()
            || command.timeout > self.config.maximum_timeout
            || command
                .freshness
                .as_ref()
                .is_some_and(|value| value.len() > 64)
            || command
                .language
                .as_ref()
                .is_some_and(|value| value.len() > 16)
            || command
                .locale
                .as_ref()
                .is_some_and(|value| value.len() > 16)
        {
            return Err(WebLogicError::InvalidCommand);
        }
        let result = self
            .data
            .search(SearchDataRequest {
                authorization: map_authorization(command.authorization),
                query: query.to_owned(),
                count: command.count,
                freshness: command.freshness,
                domain_allowlist: command.domain_allowlist,
                domain_denylist: command.domain_denylist,
                language: command.language,
                locale: command.locale,
                timeout: command.timeout,
            })
            .await
            .map_err(map_data_error)?;
        Ok(map_search(result))
    }

    async fn cancel(&self, cancellation_id: &str) -> Result<String, WebLogicError> {
        if cancellation_id.is_empty() || cancellation_id.len() > 128 {
            return Err(WebLogicError::InvalidCommand);
        }
        self.data
            .cancel(cancellation_id)
            .await
            .map_err(map_data_error)
    }
}

fn validate_authorization(value: &WebAuthorization) -> Result<(), WebLogicError> {
    if value.identity.owner_id.is_empty()
        || value.identity.session_id.is_empty()
        || value.call_id.is_empty()
        || value.tool.is_empty()
        || value.normalized_digest.len() != 64
        || value.grant.is_empty()
        || value.canonical_operation.is_empty()
        || value.cancellation_id.is_empty()
    {
        Err(WebLogicError::InvalidAuthorization)
    } else {
        Ok(())
    }
}

fn valid_url_shape(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && !value.chars().any(char::is_control)
        && (value.starts_with("https://") || value.starts_with("http://"))
}

fn valid_common_bounds(
    timeout: Duration,
    redirects: u8,
    response: usize,
    inline: usize,
    config: &WebLogicConfig,
) -> bool {
    !timeout.is_zero()
        && timeout <= config.maximum_timeout
        && redirects <= config.maximum_redirects
        && response > 0
        && response <= config.maximum_response_bytes
        && inline > 0
        && inline <= response
        && inline <= config.maximum_inline_bytes
}

fn body_size(value: &RequestBody) -> usize {
    match value {
        RequestBody::Empty => 0,
        RequestBody::Text(value) => value.len(),
        RequestBody::Json(value) => value.to_string().len(),
        RequestBody::Form(values) => values
            .iter()
            .map(|(key, value)| key.len().saturating_add(value.len()))
            .sum(),
        RequestBody::Binary(value) => value.len(),
    }
}

fn map_authorization(value: WebAuthorization) -> DataAuthorization {
    DataAuthorization {
        owner_id: value.identity.owner_id,
        session_id: value.identity.session_id,
        call_id: value.call_id,
        action: value.tool,
        normalized_digest: value.normalized_digest,
        grant: value.grant,
        canonical_operation: value.canonical_operation,
        cancellation_id: value.cancellation_id,
    }
}

fn map_header(value: HeaderValue) -> DataHeaderValue {
    match value {
        HeaderValue::Literal(value) => DataHeaderValue::Literal(value),
        HeaderValue::SecretReference(value) => DataHeaderValue::SecretReference(value),
    }
}

fn map_body(value: RequestBody) -> DataBody {
    match value {
        RequestBody::Empty => DataBody::Empty,
        RequestBody::Text(value) => DataBody::Text(value),
        RequestBody::Json(value) => DataBody::Json(value),
        RequestBody::Form(value) => DataBody::Form(value),
        RequestBody::Binary(value) => DataBody::Binary(value),
    }
}

fn map_http(value: HttpDataRecord) -> HttpResult {
    HttpResult {
        status: value.status,
        final_url: value.final_url,
        headers: value.headers,
        content_type: value.content_type,
        body: value.inline_body,
        body_is_base64: value.body_is_base64,
        total_bytes: value.total_bytes,
        artifact_id: value.artifact_id,
        truncated: value.truncated,
    }
}

fn map_fetch(value: FetchDataRecord) -> FetchResult {
    FetchResult {
        canonical_url: value.canonical_url,
        title: value.title,
        description: value.description,
        text: value.text,
        markdown: value.markdown,
        links: value
            .links
            .into_iter()
            .map(|link| LinkResult {
                text: link.text,
                url: link.url,
            })
            .collect(),
        content_type: value.content_type,
        is_pdf: value.is_pdf,
        javascript_required: value.javascript_required,
        artifact_id: value.artifact_id,
        truncated: value.truncated,
        cached: value.cached,
    }
}

fn map_search(value: SearchDataRecord) -> SearchResult {
    SearchResult {
        results: value
            .results
            .into_iter()
            .map(|result| SearchItem {
                title: result.title,
                url: result.url,
                snippet: result.snippet,
                published_at: result.published_at,
            })
            .collect(),
        provider: value.provider,
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "used directly as the Result::map_err conversion function"
)]
fn map_data_error(error: WebDataError) -> WebLogicError {
    match error {
        WebDataError::Authorization => WebLogicError::Authorization,
        WebDataError::PolicyDenied => WebLogicError::PolicyDenied,
        WebDataError::Cancelled => WebLogicError::Cancelled,
        WebDataError::Timeout => WebLogicError::Timeout,
        WebDataError::ResponseTooLarge => WebLogicError::ResponseTooLarge,
        WebDataError::SecretUnavailable => WebLogicError::SecretUnavailable,
        WebDataError::Dependency => WebLogicError::Data,
    }
}

/// Logic failures.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum WebLogicError {
    /// Invalid composition limits.
    #[error("web logic configuration is invalid")]
    InvalidConfiguration,
    /// Invalid caller material.
    #[error("web authorization envelope is invalid")]
    InvalidAuthorization,
    /// Invalid command.
    #[error("web command is invalid")]
    InvalidCommand,
    /// Authorization rejected.
    #[error("web action is unauthorized")]
    Authorization,
    /// Policy rejected.
    #[error("web action is denied by policy")]
    PolicyDenied,
    /// Cancelled.
    #[error("web action was cancelled")]
    Cancelled,
    /// Timeout.
    #[error("web action timed out")]
    Timeout,
    /// Response too large.
    #[error("web response exceeded its bound")]
    ResponseTooLarge,
    /// Secret unavailable.
    #[error("web secret is unavailable")]
    SecretUnavailable,
    /// Data layer failed.
    #[error("web data operation failed")]
    Data,
}

#[cfg(test)]
mod tests {
    use agentmod_web_host_data::{FetchDataRecord, HttpDataRecord, SearchDataRecord, WebDataPort};

    use super::*;

    #[derive(Clone, Copy)]
    struct MockData;

    #[async_trait]
    impl WebDataPort for MockData {
        async fn request(&self, request: HttpDataRequest) -> Result<HttpDataRecord, WebDataError> {
            assert_eq!(request.method, "POST");
            Ok(HttpDataRecord {
                status: 200,
                final_url: request.url,
                headers: BTreeMap::new(),
                content_type: None,
                inline_body: "ok".to_owned(),
                body_is_base64: false,
                total_bytes: 2,
                artifact_id: None,
                truncated: false,
            })
        }

        async fn fetch(&self, _: FetchDataRequest) -> Result<FetchDataRecord, WebDataError> {
            Err(WebDataError::PolicyDenied)
        }

        async fn search(
            &self,
            request: SearchDataRequest,
        ) -> Result<SearchDataRecord, WebDataError> {
            assert_eq!(request.query, "rust");
            Ok(SearchDataRecord {
                results: Vec::new(),
                provider: "mock".to_owned(),
            })
        }

        async fn cancel(&self, cancellation_id: &str) -> Result<String, WebDataError> {
            Ok(cancellation_id.to_owned())
        }
    }

    fn auth(tool: &str) -> WebAuthorization {
        WebAuthorization {
            identity: WebIdentity {
                owner_id: "owner".to_owned(),
                session_id: "session".to_owned(),
            },
            call_id: "call".to_owned(),
            tool: tool.to_owned(),
            normalized_digest: "00".repeat(32),
            grant: "grant".to_owned(),
            canonical_operation: b"operation".to_vec(),
            cancellation_id: "cancel".to_owned(),
        }
    }

    fn logic() -> WebLogic<MockData> {
        WebLogic::new(
            MockData,
            WebLogicConfig {
                maximum_url_length: 2048,
                maximum_query_length: 100,
                maximum_search_results: 10,
                maximum_headers: 16,
                maximum_request_body_bytes: 1024,
                maximum_timeout: Duration::from_secs(10),
                maximum_redirects: 5,
                maximum_response_bytes: 4096,
                maximum_inline_bytes: 1024,
            },
        )
        .expect("logic")
    }

    #[tokio::test]
    async fn validates_and_normalizes_http_method() {
        let result = logic()
            .request(HttpRequestCommand {
                authorization: auth("http.request"),
                method: "post".to_owned(),
                url: "https://example.com".to_owned(),
                query: BTreeMap::new(),
                headers: BTreeMap::new(),
                body: RequestBody::Empty,
                max_redirects: 1,
                timeout: Duration::from_secs(1),
                max_response_bytes: 100,
                max_inline_bytes: 50,
            })
            .await
            .expect("request");
        assert_eq!(result.status, 200);
    }

    #[tokio::test]
    async fn rejects_out_of_bounds_search_without_calling_provider() {
        let error = logic()
            .search(SearchCommand {
                authorization: auth("web.search"),
                query: "rust".to_owned(),
                count: 11,
                freshness: None,
                domain_allowlist: Vec::new(),
                domain_denylist: Vec::new(),
                language: None,
                locale: None,
                timeout: Duration::from_secs(1),
            })
            .await
            .expect_err("count rejected");
        assert_eq!(error, WebLogicError::InvalidCommand);
    }
}
