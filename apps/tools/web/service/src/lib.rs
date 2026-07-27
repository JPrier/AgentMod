//! Authenticated tool-protocol endpoints for the Web host.

use std::{collections::BTreeMap, str::FromStr, time::Duration};

use agentmod_tool_protocol::{ToolDescriptor, ToolHostCommand, ToolHostEvent};
use agentmod_web_host_logic::{
    FetchCommand, FetchResult, HeaderValue, HttpRequestCommand, HttpResult, RequestBody,
    SearchCommand, SearchResult, WebAuthorization, WebIdentity, WebLogicError, WebLogicPort,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

const WEB_GROUP: &str = "web";

/// Authenticated local caller identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebHostServiceConfig {
    /// Local connection owner.
    pub owner_id: String,
    /// Runtime session.
    pub session_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct HttpServiceRequest {
    method: String,
    url: String,
    #[serde(default)]
    query: BTreeMap<String, String>,
    #[serde(default)]
    headers: BTreeMap<String, ServiceHeaderValue>,
    #[serde(default)]
    body: ServiceBody,
    #[serde(default = "default_redirects")]
    max_redirects: u8,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
    #[serde(default = "default_response_bytes")]
    max_response_bytes: usize,
    #[serde(default = "default_inline_bytes")]
    max_inline_bytes: usize,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum ServiceHeaderValue {
    Literal(String),
    Secret { secret_ref: String },
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum ServiceBody {
    #[default]
    Empty,
    Text(String),
    Json(Value),
    Form(BTreeMap<String, String>),
    BinaryBase64(String),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FetchServiceRequest {
    url: String,
    #[serde(default = "default_redirects")]
    max_redirects: u8,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
    #[serde(default = "default_response_bytes")]
    max_response_bytes: usize,
    #[serde(default = "default_inline_bytes")]
    max_inline_bytes: usize,
    #[serde(default = "default_cache")]
    use_cache: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SearchServiceRequest {
    query: String,
    #[serde(default = "default_count")]
    count: u8,
    freshness: Option<String>,
    #[serde(default)]
    domain_allowlist: Vec<String>,
    #[serde(default)]
    domain_denylist: Vec<String>,
    language: Option<String>,
    locale: Option<String>,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
}

/// Endpoint errors without lower-layer details.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum WebServiceError {
    /// Missing identity.
    #[error("web host identity configuration is unavailable")]
    MissingConfiguration,
    /// Invalid envelope.
    #[error("web authorization envelope is invalid")]
    InvalidAuthorizationEnvelope,
    /// Unknown tool.
    #[error("unknown web tool")]
    UnknownTool,
    /// Invalid arguments.
    #[error("web tool arguments are invalid")]
    InvalidArguments,
    /// Logic failure.
    #[error("web operation failed")]
    Logic,
}

/// Web service.
#[derive(Clone)]
pub struct WebHostService<L> {
    logic: L,
    config: WebHostServiceConfig,
}

impl<L> WebHostService<L> {
    /// Constructs the endpoint with mandatory identity.
    ///
    /// # Errors
    ///
    /// Rejects empty owner or session IDs.
    pub fn new(logic: L, config: WebHostServiceConfig) -> Result<Self, WebServiceError> {
        if config.owner_id.trim().is_empty() || config.session_id.trim().is_empty() {
            return Err(WebServiceError::MissingConfiguration);
        }
        Ok(Self { logic, config })
    }
}

impl<L: WebLogicPort> WebHostService<L> {
    /// Handles a tool-protocol command.
    ///
    /// # Errors
    ///
    /// Returns redacted endpoint errors for malformed or rejected calls.
    pub async fn handle(
        &self,
        command: ToolHostCommand,
    ) -> Result<Vec<ToolHostEvent>, WebServiceError> {
        match command {
            ToolHostCommand::DiscoverGroups => Ok(vec![ToolHostEvent::Groups {
                groups: vec![WEB_GROUP.to_owned()],
            }]),
            ToolHostCommand::DiscoverTools { groups } => Ok(vec![ToolHostEvent::Tools {
                tools: groups
                    .iter()
                    .any(|group| group == WEB_GROUP)
                    .then(tool_descriptors)
                    .unwrap_or_default(),
            }]),
            ToolHostCommand::Health => Ok(vec![ToolHostEvent::Progress {
                call_id: "health".to_owned(),
                message: "web host ready".to_owned(),
                completed: Some(1),
                total: Some(1),
            }]),
            ToolHostCommand::Cancel { cancellation_id } => {
                let call_id = self
                    .logic
                    .cancel(&cancellation_id.to_string())
                    .await
                    .map_err(map_logic_error)?;
                Ok(vec![ToolHostEvent::Cancelled { call_id }])
            }
            ToolHostCommand::Execute {
                call_id,
                tool,
                arguments,
                normalized_digest,
                authorization_grant,
                cancellation_id,
            } => {
                if call_id.trim().is_empty()
                    || tool.trim().is_empty()
                    || normalized_digest.len() != 64
                    || authorization_grant.trim().is_empty()
                {
                    return Err(WebServiceError::InvalidAuthorizationEnvelope);
                }
                let cancellation_id = cancellation_id.to_string();
                let canonical_operation = canonical_operation(&tool, &arguments, &cancellation_id)?;
                let authorization = WebAuthorization {
                    identity: WebIdentity {
                        owner_id: self.config.owner_id.clone(),
                        session_id: self.config.session_id.clone(),
                    },
                    call_id: call_id.clone(),
                    tool: tool.clone(),
                    normalized_digest,
                    grant: authorization_grant,
                    canonical_operation,
                    cancellation_id,
                };
                self.execute(call_id, &tool, arguments, authorization).await
            }
        }
    }

    async fn execute(
        &self,
        call_id: String,
        tool: &str,
        arguments: Value,
        authorization: WebAuthorization,
    ) -> Result<Vec<ToolHostEvent>, WebServiceError> {
        match tool {
            "http.request" => {
                let request: HttpServiceRequest = parse(arguments)?;
                let result = self
                    .logic
                    .request(map_http_request(request, authorization)?)
                    .await
                    .map_err(map_logic_error)?;
                Ok(completed_http(call_id, result)?)
            }
            "web.fetch" => {
                let request: FetchServiceRequest = parse(arguments)?;
                let result = self
                    .logic
                    .fetch(map_fetch_request(request, authorization))
                    .await
                    .map_err(map_logic_error)?;
                Ok(completed_fetch(call_id, result)?)
            }
            "web.search" => {
                let request: SearchServiceRequest = parse(arguments)?;
                let result = self
                    .logic
                    .search(map_search_request(request, authorization))
                    .await
                    .map_err(map_logic_error)?;
                Ok(completed_search(call_id, result))
            }
            _ => Err(WebServiceError::UnknownTool),
        }
    }
}

fn canonical_operation(
    tool: &str,
    arguments: &Value,
    cancellation_id: &str,
) -> Result<Vec<u8>, WebServiceError> {
    let expanded = match tool {
        "http.request" => serde_json::to_value(parse::<HttpServiceRequest>(arguments.clone())?),
        "web.fetch" => serde_json::to_value(parse::<FetchServiceRequest>(arguments.clone())?),
        "web.search" => serde_json::to_value(parse::<SearchServiceRequest>(arguments.clone())?),
        _ => return Err(WebServiceError::UnknownTool),
    }
    .map_err(|_| WebServiceError::InvalidArguments)?;
    serde_json::to_vec(&(tool, cancellation_id, normalize_json(&expanded)))
        .map_err(|_| WebServiceError::InvalidArguments)
}

fn normalize_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted: BTreeMap<_, _> = map
                .iter()
                .map(|(key, value)| (key.clone(), normalize_json(value)))
                .collect();
            serde_json::to_value(sorted).unwrap_or(Value::Null)
        }
        Value::Array(values) => Value::Array(values.iter().map(normalize_json).collect()),
        _ => value.clone(),
    }
}

fn map_http_request(
    value: HttpServiceRequest,
    authorization: WebAuthorization,
) -> Result<HttpRequestCommand, WebServiceError> {
    Ok(HttpRequestCommand {
        authorization,
        method: value.method,
        url: value.url,
        query: value.query,
        headers: value
            .headers
            .into_iter()
            .map(|(name, value)| {
                (
                    name,
                    match value {
                        ServiceHeaderValue::Literal(value) => HeaderValue::Literal(value),
                        ServiceHeaderValue::Secret { secret_ref } => {
                            HeaderValue::SecretReference(secret_ref)
                        }
                    },
                )
            })
            .collect(),
        body: match value.body {
            ServiceBody::Empty => RequestBody::Empty,
            ServiceBody::Text(value) => RequestBody::Text(value),
            ServiceBody::Json(value) => RequestBody::Json(value),
            ServiceBody::Form(value) => RequestBody::Form(value),
            ServiceBody::BinaryBase64(value) => RequestBody::Binary(
                BASE64
                    .decode(value)
                    .map_err(|_| WebServiceError::InvalidArguments)?,
            ),
        },
        max_redirects: value.max_redirects,
        timeout: Duration::from_millis(value.timeout_ms),
        max_response_bytes: value.max_response_bytes,
        max_inline_bytes: value.max_inline_bytes,
    })
}

fn map_fetch_request(value: FetchServiceRequest, authorization: WebAuthorization) -> FetchCommand {
    FetchCommand {
        authorization,
        url: value.url,
        max_redirects: value.max_redirects,
        timeout: Duration::from_millis(value.timeout_ms),
        max_response_bytes: value.max_response_bytes,
        max_inline_bytes: value.max_inline_bytes,
        use_cache: value.use_cache,
    }
}

fn map_search_request(
    value: SearchServiceRequest,
    authorization: WebAuthorization,
) -> SearchCommand {
    SearchCommand {
        authorization,
        query: value.query,
        count: value.count,
        freshness: value.freshness,
        domain_allowlist: value.domain_allowlist,
        domain_denylist: value.domain_denylist,
        language: value.language,
        locale: value.locale,
        timeout: Duration::from_millis(value.timeout_ms),
    }
}

fn completed_http(
    call_id: String,
    result: HttpResult,
) -> Result<Vec<ToolHostEvent>, WebServiceError> {
    let artifact = parse_artifact(result.artifact_id)?;
    Ok(vec![
        ToolHostEvent::Started {
            call_id: call_id.clone(),
        },
        ToolHostEvent::Completed {
            call_id,
            result: json!({
                "status": result.status,
                "final_url": result.final_url,
                "headers": result.headers,
                "content_type": result.content_type,
                "body": result.body,
                "body_is_base64": result.body_is_base64,
                "total_bytes": result.total_bytes,
            }),
            artifact,
            truncated: result.truncated,
        },
    ])
}

fn completed_fetch(
    call_id: String,
    result: FetchResult,
) -> Result<Vec<ToolHostEvent>, WebServiceError> {
    let artifact = parse_artifact(result.artifact_id)?;
    Ok(vec![
        ToolHostEvent::Started {
            call_id: call_id.clone(),
        },
        ToolHostEvent::Completed {
            call_id,
            result: json!({
                "canonical_url": result.canonical_url,
                "title": result.title,
                "description": result.description,
                "text": result.text,
                "markdown": result.markdown,
                "links": result.links.into_iter().map(|link| json!({
                    "text": link.text,
                    "url": link.url,
                })).collect::<Vec<_>>(),
                "content_type": result.content_type,
                "is_pdf": result.is_pdf,
                "javascript_required": result.javascript_required,
                "cached": result.cached,
            }),
            artifact,
            truncated: result.truncated,
        },
    ])
}

fn completed_search(call_id: String, result: SearchResult) -> Vec<ToolHostEvent> {
    vec![
        ToolHostEvent::Started {
            call_id: call_id.clone(),
        },
        ToolHostEvent::Completed {
            call_id,
            result: json!({
                "provider": result.provider,
                "results": result.results.into_iter().map(|item| json!({
                    "title": item.title,
                    "url": item.url,
                    "snippet": item.snippet,
                    "published_at": item.published_at,
                    "citation": {
                        "url": item.url,
                        "title": item.title,
                    }
                })).collect::<Vec<_>>(),
            }),
            artifact: None,
            truncated: false,
        },
    ]
}

fn parse_artifact(
    value: Option<String>,
) -> Result<Option<agentmod_primitives::ArtifactId>, WebServiceError> {
    value
        .map(|value| {
            agentmod_primitives::ArtifactId::from_str(&value).map_err(|_| WebServiceError::Logic)
        })
        .transpose()
}

fn parse<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, WebServiceError> {
    serde_json::from_value(value).map_err(|_| WebServiceError::InvalidArguments)
}

const fn default_redirects() -> u8 {
    5
}

const fn default_timeout() -> u64 {
    30_000
}

const fn default_response_bytes() -> usize {
    8 * 1024 * 1024
}

const fn default_inline_bytes() -> usize {
    64 * 1024
}

const fn default_count() -> u8 {
    10
}

const fn default_cache() -> bool {
    true
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "used directly as the Result::map_err conversion function"
)]
fn map_logic_error(error: WebLogicError) -> WebServiceError {
    match error {
        WebLogicError::InvalidAuthorization => WebServiceError::InvalidAuthorizationEnvelope,
        WebLogicError::InvalidCommand => WebServiceError::InvalidArguments,
        _ => WebServiceError::Logic,
    }
}

fn tool_descriptors() -> Vec<ToolDescriptor> {
    vec![
        ToolDescriptor {
            id: "http.request".to_owned(),
            group: WEB_GROUP.to_owned(),
            description:
                "Perform a policy-controlled HTTP request; secret headers use {secret_ref: ...}"
                    .to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["method", "url"],
                "properties": {
                    "method": {"enum": ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]},
                    "url": {"type": "string"},
                    "query": {"type": "object", "additionalProperties": {"type": "string"}},
                    "headers": {"type": "object"},
                    "body": {"type": "object"},
                    "max_redirects": {"type": "integer", "minimum": 0},
                    "timeout_ms": {"type": "integer", "minimum": 1},
                    "max_response_bytes": {"type": "integer", "minimum": 1},
                    "max_inline_bytes": {"type": "integer", "minimum": 1}
                },
                "additionalProperties": false
            }),
            supported_decisions: vec![
                "continue".to_owned(),
                "replace".to_owned(),
                "reject".to_owned(),
                "require_approval".to_owned(),
                "defer".to_owned(),
                "cancel".to_owned(),
            ],
        },
        ToolDescriptor {
            id: "web.fetch".to_owned(),
            group: WEB_GROUP.to_owned(),
            description: "Fetch a page and return bounded clean text, metadata, and links"
                .to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["url"],
                "properties": {
                    "url": {"type": "string"},
                    "max_redirects": {"type": "integer", "minimum": 0},
                    "timeout_ms": {"type": "integer", "minimum": 1},
                    "max_response_bytes": {"type": "integer", "minimum": 1},
                    "max_inline_bytes": {"type": "integer", "minimum": 1},
                    "use_cache": {"type": "boolean"}
                },
                "additionalProperties": false
            }),
            supported_decisions: vec![
                "continue".to_owned(),
                "replace".to_owned(),
                "reject".to_owned(),
                "require_approval".to_owned(),
                "defer".to_owned(),
                "cancel".to_owned(),
            ],
        },
        ToolDescriptor {
            id: "web.search".to_owned(),
            group: WEB_GROUP.to_owned(),
            description: "Search through a provider-independent result contract".to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["query"],
                "properties": {
                    "query": {"type": "string"},
                    "count": {"type": "integer", "minimum": 1},
                    "freshness": {"type": ["string", "null"]},
                    "domain_allowlist": {"type": "array", "items": {"type": "string"}},
                    "domain_denylist": {"type": "array", "items": {"type": "string"}},
                    "language": {"type": ["string", "null"]},
                    "locale": {"type": ["string", "null"]},
                    "timeout_ms": {"type": "integer", "minimum": 1}
                },
                "additionalProperties": false
            }),
            supported_decisions: vec![
                "continue".to_owned(),
                "replace".to_owned(),
                "reject".to_owned(),
                "require_approval".to_owned(),
                "defer".to_owned(),
                "cancel".to_owned(),
            ],
        },
    ]
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use uuid::Uuid;

    use super::*;

    #[derive(Clone, Copy)]
    struct MockLogic;

    #[async_trait]
    impl WebLogicPort for MockLogic {
        async fn request(&self, command: HttpRequestCommand) -> Result<HttpResult, WebLogicError> {
            assert_eq!(command.authorization.identity.owner_id, "owner");
            assert_eq!(command.method, "GET");
            Ok(HttpResult {
                status: 200,
                final_url: command.url,
                headers: BTreeMap::new(),
                content_type: Some("text/plain".to_owned()),
                body: "ok".to_owned(),
                body_is_base64: false,
                total_bytes: 2,
                artifact_id: None,
                truncated: false,
            })
        }

        async fn fetch(&self, _: FetchCommand) -> Result<FetchResult, WebLogicError> {
            Err(WebLogicError::PolicyDenied)
        }

        async fn search(&self, command: SearchCommand) -> Result<SearchResult, WebLogicError> {
            assert_eq!(command.query, "rust");
            Ok(SearchResult {
                results: Vec::new(),
                provider: "mock".to_owned(),
            })
        }

        async fn cancel(&self, cancellation_id: &str) -> Result<String, WebLogicError> {
            Ok(cancellation_id.to_owned())
        }
    }

    fn service() -> WebHostService<MockLogic> {
        WebHostService::new(
            MockLogic,
            WebHostServiceConfig {
                owner_id: "owner".to_owned(),
                session_id: "session".to_owned(),
            },
        )
        .expect("service")
    }

    #[tokio::test]
    async fn service_maps_tool_protocol_to_logic_and_back() {
        let events = service()
            .handle(ToolHostCommand::Execute {
                call_id: "call".to_owned(),
                tool: "http.request".to_owned(),
                arguments: json!({"method": "GET", "url": "https://example.com"}),
                normalized_digest: "00".repeat(32),
                authorization_grant: "grant".to_owned(),
                cancellation_id: agentmod_primitives::CancellationId::from_uuid(
                    uuid::Uuid::now_v7(),
                ),
            })
            .await
            .expect("events");
        assert!(matches!(events[0], ToolHostEvent::Started { .. }));
        assert!(matches!(events[1], ToolHostEvent::Completed { .. }));
    }

    #[tokio::test]
    async fn unknown_tool_and_bad_binary_are_rejected() {
        let command = |tool: &str, arguments: Value| ToolHostCommand::Execute {
            call_id: "call".to_owned(),
            tool: tool.to_owned(),
            arguments,
            normalized_digest: "00".repeat(32),
            authorization_grant: "grant".to_owned(),
            cancellation_id: agentmod_primitives::CancellationId::from_uuid(Uuid::now_v7()),
        };
        assert_eq!(
            service()
                .handle(command("web.unknown", json!({})))
                .await
                .expect_err("unknown"),
            WebServiceError::UnknownTool
        );
        assert_eq!(
            service()
                .handle(command(
                    "http.request",
                    json!({
                        "method": "POST",
                        "url": "https://example.com",
                        "body": {"kind": "binary_base64", "value": "%"}
                    })
                ))
                .await
                .expect_err("binary"),
            WebServiceError::InvalidArguments
        );
    }

    #[tokio::test]
    async fn discovery_is_lazy_by_group() {
        let empty = service()
            .handle(ToolHostCommand::DiscoverTools {
                groups: vec!["filesystem".to_owned()],
            })
            .await
            .expect("discovery");
        assert!(matches!(&empty[0], ToolHostEvent::Tools { tools } if tools.is_empty()));
        let web = service()
            .handle(ToolHostCommand::DiscoverTools {
                groups: vec!["web".to_owned()],
            })
            .await
            .expect("discovery");
        assert!(matches!(&web[0], ToolHostEvent::Tools { tools } if tools.len() == 3));
    }
}
