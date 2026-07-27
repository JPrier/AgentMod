//! Authenticated tool-protocol endpoints for the managed browser host.

use std::str::FromStr;

use agentmod_browser_host_logic::{
    BrowserAuthorization, BrowserCommand, BrowserLogicError, BrowserLogicPort, BrowserLogicRequest,
};
use agentmod_tool_protocol::{ToolDescriptor, ToolHostCommand, ToolHostEvent};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use thiserror::Error;

const GROUP: &str = "browser";
const DEFAULT_INLINE_BYTES: usize = 128 * 1024;
const DEFAULT_ARTIFACT_BYTES: usize = 32 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UrlArguments {
    url: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InspectArguments {
    #[serde(default = "default_inline")]
    maximum_bytes: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectorArguments {
    selector: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TypeArguments {
    selector: String,
    text: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DownloadArguments {
    url: String,
    #[serde(default = "default_artifact")]
    maximum_bytes: usize,
}

/// Endpoint service.
#[derive(Clone)]
pub struct BrowserHostService<L> {
    logic: L,
}

impl<L> BrowserHostService<L> {
    /// Injects logic.
    #[must_use]
    pub const fn new(logic: L) -> Self {
        Self { logic }
    }
}

impl<L: BrowserLogicPort> BrowserHostService<L> {
    /// Handles one protocol command.
    pub async fn handle(&self, command: ToolHostCommand) -> Vec<ToolHostEvent> {
        match command {
            ToolHostCommand::DiscoverGroups => vec![ToolHostEvent::Groups {
                groups: vec![GROUP.to_owned()],
            }],
            ToolHostCommand::DiscoverTools { groups } => vec![ToolHostEvent::Tools {
                tools: if groups.iter().any(|group| group == GROUP) {
                    descriptors()
                } else {
                    Vec::new()
                },
            }],
            ToolHostCommand::Health => {
                let result = self.logic.health().await;
                match result {
                    Ok(value) => completed("health".to_owned(), value, None, false),
                    Err(error) => vec![failed("health", &error)],
                }
            }
            ToolHostCommand::Cancel { cancellation_id } => {
                let call_id = cancellation_id.to_string();
                match self.logic.cancel(&call_id).await {
                    Ok(()) => vec![ToolHostEvent::Cancelled { call_id }],
                    Err(error) => vec![failed(&call_id, &error)],
                }
            }
            ToolHostCommand::Execute {
                call_id,
                tool,
                arguments,
                normalized_digest,
                authorization_grant,
                cancellation_id,
            } => {
                let (command, expanded) = match parse_command(&tool, arguments) {
                    Ok(value) => value,
                    Err(error) => {
                        return vec![ToolHostEvent::Failed {
                            call_id,
                            code: "invalid_arguments".to_owned(),
                            message: error.to_string(),
                            retryable: false,
                        }];
                    }
                };
                let request = BrowserLogicRequest {
                    authorization: BrowserAuthorization {
                        call_id: call_id.clone(),
                        action: tool,
                        normalized_digest,
                        grant: authorization_grant,
                        arguments: expanded,
                        cancellation_id: cancellation_id.to_string(),
                    },
                    command,
                };
                match self.logic.execute(request).await {
                    Ok(value) => {
                        let artifact = value
                            .artifact
                            .map(|id| agentmod_primitives::ArtifactId::from_str(&id))
                            .transpose();
                        match artifact {
                            Ok(artifact) => {
                                completed(call_id, value.result, artifact, value.truncated)
                            }
                            Err(_) => vec![ToolHostEvent::Failed {
                                call_id,
                                code: "mapping".to_owned(),
                                message: "browser result mapping failed".to_owned(),
                                retryable: false,
                            }],
                        }
                    }
                    Err(error) => vec![failed(&call_id, &error)],
                }
            }
        }
    }
}

fn parse_command(
    tool: &str,
    arguments: Value,
) -> Result<(BrowserCommand, Value), BrowserServiceError> {
    match tool {
        "browser.start" => {
            require_empty(arguments)?;
            Ok((BrowserCommand::Start, json!({})))
        }
        "browser.navigate" => {
            let value: UrlArguments = parse(arguments)?;
            Ok((
                BrowserCommand::Navigate {
                    url: value.url.clone(),
                },
                json!({"url":value.url}),
            ))
        }
        "browser.inspect" => {
            let value: InspectArguments = parse(arguments)?;
            Ok((
                BrowserCommand::Inspect {
                    maximum_bytes: value.maximum_bytes,
                },
                json!({"maximum_bytes":value.maximum_bytes}),
            ))
        }
        "browser.screenshot" => {
            require_empty(arguments)?;
            Ok((BrowserCommand::Screenshot, json!({})))
        }
        "browser.click" => {
            let value: SelectorArguments = parse(arguments)?;
            Ok((
                BrowserCommand::Click {
                    selector: value.selector.clone(),
                },
                json!({"selector":value.selector}),
            ))
        }
        "browser.type" => {
            let value: TypeArguments = parse(arguments)?;
            Ok((
                BrowserCommand::Type {
                    selector: value.selector.clone(),
                    text: value.text.clone(),
                },
                json!({"selector":value.selector,"text":value.text}),
            ))
        }
        "browser.submit" => {
            let value: SelectorArguments = parse(arguments)?;
            Ok((
                BrowserCommand::Submit {
                    selector: value.selector.clone(),
                },
                json!({"selector":value.selector}),
            ))
        }
        "browser.download" => {
            let value: DownloadArguments = parse(arguments)?;
            Ok((
                BrowserCommand::Download {
                    url: value.url.clone(),
                    maximum_bytes: value.maximum_bytes,
                },
                json!({"url":value.url,"maximum_bytes":value.maximum_bytes}),
            ))
        }
        "browser.close" => {
            require_empty(arguments)?;
            Ok((BrowserCommand::Close, json!({})))
        }
        _ => Err(BrowserServiceError::UnknownTool),
    }
}

fn completed(
    call_id: String,
    result: Value,
    artifact: Option<agentmod_primitives::ArtifactId>,
    truncated: bool,
) -> Vec<ToolHostEvent> {
    vec![
        ToolHostEvent::Started {
            call_id: call_id.clone(),
        },
        ToolHostEvent::Completed {
            call_id,
            result,
            artifact,
            truncated,
        },
    ]
}

fn failed(call_id: &str, error: &BrowserLogicError) -> ToolHostEvent {
    let (code, retryable) = match error {
        BrowserLogicError::Configuration | BrowserLogicError::Invalid => ("invalid_request", false),
        BrowserLogicError::Denied => ("denied", false),
        BrowserLogicError::NoSession => ("no_session", false),
        BrowserLogicError::TooLarge => ("too_large", false),
        BrowserLogicError::Cancelled => ("cancelled", false),
        BrowserLogicError::Unavailable => ("unavailable", true),
    };
    ToolHostEvent::Failed {
        call_id: call_id.to_owned(),
        code: code.to_owned(),
        message: error.to_string(),
        retryable,
    }
}

fn parse<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, BrowserServiceError> {
    serde_json::from_value(value).map_err(|_| BrowserServiceError::InvalidArguments)
}

fn require_empty(value: Value) -> Result<(), BrowserServiceError> {
    match value {
        Value::Object(object) if object.is_empty() => Ok(()),
        _ => Err(BrowserServiceError::InvalidArguments),
    }
}

fn descriptors() -> Vec<ToolDescriptor> {
    vec![
        descriptor(
            "browser.start",
            "Start a managed browser session",
            json!({}),
        ),
        descriptor(
            "browser.navigate",
            "Navigate to an approved URL and return the final URL",
            json!({"url":{"type":"string"}}),
        ),
        descriptor(
            "browser.inspect",
            "Inspect bounded rendered page HTML",
            json!({"maximum_bytes":{"type":"integer","minimum":1}}),
        ),
        descriptor(
            "browser.screenshot",
            "Capture the current viewport as a private artifact",
            json!({}),
        ),
        descriptor(
            "browser.click",
            "Click the first element matching a CSS selector",
            json!({"selector":{"type":"string"}}),
        ),
        descriptor(
            "browser.type",
            "Replace text in the first CSS-selected element",
            json!({"selector":{"type":"string"},"text":{"type":"string"}}),
        ),
        descriptor(
            "browser.submit",
            "Submit the form containing the first CSS-selected element",
            json!({"selector":{"type":"string"}}),
        ),
        descriptor(
            "browser.download",
            "Download through the authenticated rendered page into an artifact",
            json!({
                "url":{"type":"string"},
                "maximum_bytes":{"type":"integer","minimum":1},
            }),
        ),
        descriptor(
            "browser.close",
            "Close the managed browser session",
            json!({}),
        ),
    ]
}

fn descriptor(id: &str, description: &str, properties: Value) -> ToolDescriptor {
    let required = properties
        .as_object()
        .map(|object| object.keys().cloned().map(Value::String).collect())
        .unwrap_or_default();
    ToolDescriptor {
        id: id.to_owned(),
        group: GROUP.to_owned(),
        description: description.to_owned(),
        input_schema: Value::Object(Map::from_iter([
            ("type".to_owned(), Value::String("object".to_owned())),
            ("properties".to_owned(), properties),
            ("required".to_owned(), Value::Array(required)),
            ("additionalProperties".to_owned(), Value::Bool(false)),
        ])),
        supported_decisions: vec![
            "continue".to_owned(),
            "replace".to_owned(),
            "reject".to_owned(),
            "require_approval".to_owned(),
            "defer".to_owned(),
            "cancel".to_owned(),
        ],
    }
}

const fn default_inline() -> usize {
    DEFAULT_INLINE_BYTES
}

const fn default_artifact() -> usize {
    DEFAULT_ARTIFACT_BYTES
}

/// Endpoint error.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum BrowserServiceError {
    /// Unknown tool.
    #[error("unknown browser tool")]
    UnknownTool,
    /// Invalid arguments.
    #[error("invalid browser tool arguments")]
    InvalidArguments,
}

#[cfg(test)]
mod tests {
    use agentmod_browser_host_logic::{
        BrowserLogicError, BrowserLogicPort, BrowserLogicRequest, BrowserResult,
    };
    use agentmod_primitives::CancellationId;
    use agentmod_tool_protocol::{ToolHostCommand, ToolHostEvent};
    use async_trait::async_trait;
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::BrowserHostService;

    #[derive(Clone)]
    struct MockLogic;

    #[async_trait]
    impl BrowserLogicPort for MockLogic {
        async fn execute(
            &self,
            request: BrowserLogicRequest,
        ) -> Result<BrowserResult, BrowserLogicError> {
            assert_eq!(
                request.authorization.arguments,
                json!({"maximum_bytes":131_072})
            );
            assert!(matches!(
                request.command,
                agentmod_browser_host_logic::BrowserCommand::Inspect {
                    maximum_bytes: 131_072
                }
            ));
            Ok(BrowserResult {
                result: json!({"html":"page"}),
                artifact: None,
                truncated: false,
            })
        }

        async fn cancel(&self, _: &str) -> Result<(), BrowserLogicError> {
            Ok(())
        }

        async fn health(&self) -> Result<Value, BrowserLogicError> {
            Ok(json!({"healthy":true}))
        }

        async fn shutdown(&self) {}
    }

    #[tokio::test]
    async fn service_expands_defaults_before_mapping_to_logic() {
        let events = BrowserHostService::new(MockLogic)
            .handle(ToolHostCommand::Execute {
                call_id: "call".to_owned(),
                tool: "browser.inspect".to_owned(),
                arguments: json!({}),
                normalized_digest: "11".repeat(32),
                authorization_grant: "grant".to_owned(),
                cancellation_id: CancellationId::from_uuid(Uuid::now_v7()),
            })
            .await;
        assert!(matches!(events[0], ToolHostEvent::Started { .. }));
        assert!(matches!(
            events[1],
            ToolHostEvent::Completed {
                ref result,
                truncated: false,
                ..
            } if result["html"] == "page"
        ));
    }

    #[tokio::test]
    async fn service_rejects_unknown_fields_without_calling_logic() {
        let events = BrowserHostService::new(MockLogic)
            .handle(ToolHostCommand::Execute {
                call_id: "call".to_owned(),
                tool: "browser.navigate".to_owned(),
                arguments: json!({"url":"https://example.com","unexpected":true}),
                normalized_digest: "11".repeat(32),
                authorization_grant: "grant".to_owned(),
                cancellation_id: CancellationId::from_uuid(Uuid::now_v7()),
            })
            .await;
        assert!(matches!(
            events.as_slice(),
            [ToolHostEvent::Failed { code, .. }] if code == "invalid_arguments"
        ));
    }
}
