//! Business-facing Web datasets and dependency normalization.

use std::{collections::BTreeMap, time::Duration};

use agentmod_web_host_dependency::{
    DependencyAuthorization, DependencyBody, DependencyFetchRequest, DependencyHeaderValue,
    DependencyHttpRequest, DependencySearchRequest, WebDependencyError, WebDependencyPort,
};
use async_trait::async_trait;
use thiserror::Error;

/// Data-owned authorization request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataAuthorization {
    /// Owner.
    pub owner_id: String,
    /// Session.
    pub session_id: String,
    /// Call.
    pub call_id: String,
    /// Action.
    pub action: String,
    /// Supplied digest.
    pub normalized_digest: String,
    /// Grant.
    pub grant: String,
    /// Canonical operation bytes.
    pub canonical_operation: Vec<u8>,
    /// Cancellation ID.
    pub cancellation_id: String,
}

/// Data-owned header.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataHeaderValue {
    /// Literal.
    Literal(String),
    /// Secret reference.
    SecretReference(String),
}

/// Data-owned body.
#[derive(Clone, Debug, PartialEq)]
pub enum DataBody {
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

/// Data request for HTTP.
#[derive(Clone, Debug, PartialEq)]
pub struct HttpDataRequest {
    /// Authorization.
    pub authorization: DataAuthorization,
    /// Method.
    pub method: String,
    /// URL.
    pub url: String,
    /// Query.
    pub query: BTreeMap<String, String>,
    /// Headers.
    pub headers: BTreeMap<String, DataHeaderValue>,
    /// Body.
    pub body: DataBody,
    /// Redirect bound.
    pub max_redirects: u8,
    /// Timeout.
    pub timeout: Duration,
    /// Response bound.
    pub max_response_bytes: usize,
    /// Projection bound.
    pub max_inline_bytes: usize,
}

/// Data request for fetch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchDataRequest {
    /// Authorization.
    pub authorization: DataAuthorization,
    /// URL.
    pub url: String,
    /// Redirect bound.
    pub max_redirects: u8,
    /// Timeout.
    pub timeout: Duration,
    /// Response bound.
    pub max_response_bytes: usize,
    /// Projection bound.
    pub max_inline_bytes: usize,
    /// Cache hint.
    pub use_cache: bool,
}

/// Data request for search.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchDataRequest {
    /// Authorization.
    pub authorization: DataAuthorization,
    /// Query.
    pub query: String,
    /// Count.
    pub count: u8,
    /// Freshness.
    pub freshness: Option<String>,
    /// Allowed result domains.
    pub domain_allowlist: Vec<String>,
    /// Denied result domains.
    pub domain_denylist: Vec<String>,
    /// Language.
    pub language: Option<String>,
    /// Locale.
    pub locale: Option<String>,
    /// Timeout.
    pub timeout: Duration,
}

/// Data record for HTTP.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpDataRecord {
    /// Status.
    pub status: u16,
    /// Final URL.
    pub final_url: String,
    /// Redacted headers.
    pub headers: BTreeMap<String, String>,
    /// MIME.
    pub content_type: Option<String>,
    /// Projection.
    pub inline_body: String,
    /// Base64 flag.
    pub body_is_base64: bool,
    /// Total bytes.
    pub total_bytes: u64,
    /// Artifact ID.
    pub artifact_id: Option<String>,
    /// Truncation.
    pub truncated: bool,
}

/// Data link record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LinkDataRecord {
    /// Label.
    pub text: String,
    /// Absolute URL.
    pub url: String,
}

/// Data fetch record.
#[allow(
    clippy::struct_excessive_bools,
    reason = "orthogonal normalized facts are preserved explicitly at this boundary"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchDataRecord {
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
    pub links: Vec<LinkDataRecord>,
    /// MIME.
    pub content_type: Option<String>,
    /// PDF.
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

/// Data search result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchResultDataRecord {
    /// Title.
    pub title: String,
    /// URL.
    pub url: String,
    /// Snippet.
    pub snippet: String,
    /// Publication date.
    pub published_at: Option<String>,
}

/// Data search record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchDataRecord {
    /// Results.
    pub results: Vec<SearchResultDataRecord>,
    /// Provider.
    pub provider: String,
}

/// Business-facing data contract consumed only by logic.
#[async_trait]
pub trait WebDataPort: Send + Sync {
    /// Retrieves an HTTP dataset.
    async fn request(&self, request: HttpDataRequest) -> Result<HttpDataRecord, WebDataError>;
    /// Retrieves an extracted page dataset.
    async fn fetch(&self, request: FetchDataRequest) -> Result<FetchDataRecord, WebDataError>;
    /// Retrieves normalized search records.
    async fn search(&self, request: SearchDataRequest) -> Result<SearchDataRecord, WebDataError>;
    /// Routes cancellation.
    async fn cancel(&self, cancellation_id: &str) -> Result<String, WebDataError>;
}

/// Concrete data router.
#[derive(Clone)]
pub struct WebData<D> {
    dependency: D,
}

impl<D> WebData<D> {
    /// Creates a data router.
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

#[async_trait]
impl<D: WebDependencyPort> WebDataPort for WebData<D> {
    async fn request(&self, request: HttpDataRequest) -> Result<HttpDataRecord, WebDataError> {
        let response = self
            .dependency
            .request(DependencyHttpRequest {
                authorization: map_authorization(request.authorization),
                method: request.method,
                url: request.url,
                query: request.query,
                headers: request
                    .headers
                    .into_iter()
                    .map(|(name, value)| (name, map_header(value)))
                    .collect(),
                body: map_body(request.body),
                max_redirects: request.max_redirects,
                timeout: request.timeout,
                max_response_bytes: request.max_response_bytes,
                max_inline_bytes: request.max_inline_bytes,
            })
            .await
            .map_err(map_dependency_error)?;
        Ok(HttpDataRecord {
            status: response.status,
            final_url: response.final_url,
            headers: response.headers,
            content_type: response.content_type,
            inline_body: response.inline_body,
            body_is_base64: response.body_is_base64,
            total_bytes: response.total_bytes,
            artifact_id: response.artifact.map(|id| id.to_string()),
            truncated: response.truncated,
        })
    }

    async fn fetch(&self, request: FetchDataRequest) -> Result<FetchDataRecord, WebDataError> {
        let response = self
            .dependency
            .fetch(DependencyFetchRequest {
                authorization: map_authorization(request.authorization),
                url: request.url,
                max_redirects: request.max_redirects,
                timeout: request.timeout,
                max_response_bytes: request.max_response_bytes,
                max_inline_bytes: request.max_inline_bytes,
                use_cache: request.use_cache,
            })
            .await
            .map_err(map_dependency_error)?;
        Ok(FetchDataRecord {
            canonical_url: response.canonical_url,
            title: response.title,
            description: response.description,
            text: response.text,
            markdown: response.markdown,
            links: response
                .links
                .into_iter()
                .map(|link| LinkDataRecord {
                    text: link.text,
                    url: link.url,
                })
                .collect(),
            content_type: response.content_type,
            is_pdf: response.is_pdf,
            javascript_required: response.javascript_required,
            artifact_id: response.artifact.map(|id| id.to_string()),
            truncated: response.truncated,
            cached: response.cached,
        })
    }

    async fn search(&self, request: SearchDataRequest) -> Result<SearchDataRecord, WebDataError> {
        let response = self
            .dependency
            .search(DependencySearchRequest {
                authorization: map_authorization(request.authorization),
                query: request.query,
                count: request.count,
                freshness: request.freshness,
                domain_allowlist: request.domain_allowlist,
                domain_denylist: request.domain_denylist,
                language: request.language,
                locale: request.locale,
                timeout: request.timeout,
            })
            .await
            .map_err(map_dependency_error)?;
        Ok(SearchDataRecord {
            results: response
                .results
                .into_iter()
                .map(|result| SearchResultDataRecord {
                    title: result.title,
                    url: result.url,
                    snippet: result.snippet,
                    published_at: result.published_at,
                })
                .collect(),
            provider: response.provider,
        })
    }

    async fn cancel(&self, cancellation_id: &str) -> Result<String, WebDataError> {
        self.dependency
            .cancel(cancellation_id)
            .await
            .map_err(map_dependency_error)
    }
}

fn map_authorization(value: DataAuthorization) -> DependencyAuthorization {
    DependencyAuthorization {
        owner_id: value.owner_id,
        session_id: value.session_id,
        call_id: value.call_id,
        action: value.action,
        normalized_digest: value.normalized_digest,
        grant: value.grant,
        canonical_operation: value.canonical_operation,
        cancellation_id: value.cancellation_id,
    }
}

fn map_header(value: DataHeaderValue) -> DependencyHeaderValue {
    match value {
        DataHeaderValue::Literal(value) => DependencyHeaderValue::Literal(value),
        DataHeaderValue::SecretReference(value) => DependencyHeaderValue::SecretReference(value),
    }
}

fn map_body(value: DataBody) -> DependencyBody {
    match value {
        DataBody::Empty => DependencyBody::Empty,
        DataBody::Text(value) => DependencyBody::Text(value),
        DataBody::Json(value) => DependencyBody::Json(value),
        DataBody::Form(value) => DependencyBody::Form(value),
        DataBody::Binary(value) => DependencyBody::Binary(value),
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "used directly as the Result::map_err conversion function"
)]
fn map_dependency_error(error: WebDependencyError) -> WebDataError {
    match error {
        WebDependencyError::Authorization | WebDependencyError::AuthorizationReplay => {
            WebDataError::Authorization
        }
        WebDependencyError::PolicyDenied | WebDependencyError::PrivateNetworkDenied => {
            WebDataError::PolicyDenied
        }
        WebDependencyError::Cancelled => WebDataError::Cancelled,
        WebDependencyError::Timeout => WebDataError::Timeout,
        WebDependencyError::ResponseTooLarge => WebDataError::ResponseTooLarge,
        WebDependencyError::SecretUnavailable => WebDataError::SecretUnavailable,
        _ => WebDataError::Dependency,
    }
}

/// Stable data failures that do not expose adapters.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum WebDataError {
    /// Authorization.
    #[error("web data authorization failed")]
    Authorization,
    /// Mandatory policy.
    #[error("web data policy denied operation")]
    PolicyDenied,
    /// Cancellation.
    #[error("web data operation cancelled")]
    Cancelled,
    /// Timeout.
    #[error("web data operation timed out")]
    Timeout,
    /// Bound exceeded.
    #[error("web data response exceeded bound")]
    ResponseTooLarge,
    /// Missing secret.
    #[error("web data secret unavailable")]
    SecretUnavailable,
    /// Other adapter failure.
    #[error("web data dependency failed")]
    Dependency,
}

#[cfg(test)]
mod tests {
    use agentmod_web_host_dependency::{
        DependencyFetchResponse, DependencyHttpResponse, DependencySearchResponse,
    };

    use super::*;

    #[derive(Clone, Copy)]
    struct MockDependency;

    #[async_trait]
    impl WebDependencyPort for MockDependency {
        async fn request(
            &self,
            request: DependencyHttpRequest,
        ) -> Result<DependencyHttpResponse, WebDependencyError> {
            assert_eq!(request.method, "POST");
            assert!(matches!(request.body, DependencyBody::Json(_)));
            Ok(DependencyHttpResponse {
                status: 201,
                final_url: request.url,
                headers: BTreeMap::new(),
                content_type: Some("application/json".to_owned()),
                inline_body: "{}".to_owned(),
                body_is_base64: false,
                total_bytes: 2,
                artifact: None,
                truncated: false,
            })
        }

        async fn fetch(
            &self,
            _: DependencyFetchRequest,
        ) -> Result<DependencyFetchResponse, WebDependencyError> {
            Err(WebDependencyError::PolicyDenied)
        }

        async fn search(
            &self,
            _: DependencySearchRequest,
        ) -> Result<DependencySearchResponse, WebDependencyError> {
            Err(WebDependencyError::Timeout)
        }

        async fn cancel(&self, cancellation_id: &str) -> Result<String, WebDependencyError> {
            Ok(cancellation_id.to_owned())
        }
    }

    fn authorization() -> DataAuthorization {
        DataAuthorization {
            owner_id: "owner".to_owned(),
            session_id: "session".to_owned(),
            call_id: "call".to_owned(),
            action: "http.request".to_owned(),
            normalized_digest: "00".repeat(32),
            grant: "grant".to_owned(),
            canonical_operation: b"operation".to_vec(),
            cancellation_id: "cancel".to_owned(),
        }
    }

    #[tokio::test]
    async fn maps_request_and_response_at_both_boundaries() {
        let data = WebData::new(MockDependency);
        let result = data
            .request(HttpDataRequest {
                authorization: authorization(),
                method: "POST".to_owned(),
                url: "https://example.com".to_owned(),
                query: BTreeMap::new(),
                headers: BTreeMap::new(),
                body: DataBody::Json(serde_json::json!({"ok": true})),
                max_redirects: 0,
                timeout: Duration::from_secs(1),
                max_response_bytes: 100,
                max_inline_bytes: 50,
            })
            .await
            .expect("record");
        assert_eq!(result.status, 201);
        assert_eq!(result.inline_body, "{}");
    }

    #[tokio::test]
    async fn translates_dependency_error_classes() {
        let data = WebData::new(MockDependency);
        let error = data
            .fetch(FetchDataRequest {
                authorization: authorization(),
                url: "https://example.com".to_owned(),
                max_redirects: 0,
                timeout: Duration::from_secs(1),
                max_response_bytes: 100,
                max_inline_bytes: 50,
                use_cache: false,
            })
            .await
            .expect_err("denied");
        assert_eq!(error, WebDataError::PolicyDenied);
    }
}
