//! Endpoint-facing service layer for the independent harness fixture.

use std::str::FromStr;

use agentmod_harness_fixture_logic::{
    FixtureCatalogLogic, FixtureCatalogResult, FixtureContinueCommand, FixtureContinuationDecision,
    FixtureExecuteCommand, FixtureExecutionLogic, FixtureHealthLogic, FixtureLogicEntry,
    FixtureLogicError, FixtureLogicEvent, FixtureLogicOption,
};
use agentmod_harness_protocol::{
    CatalogProvider, HarnessCommand, HarnessContinuationDecision, HarnessEvent, ProjectedEntry,
    Usage,
};
use agentmod_primitives::ContinuationId;
use thiserror::Error;

/// Service-owned conversation entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FixtureServiceEntry {
    /// System instruction.
    System(String),
    /// User text.
    User(String),
    /// Image input (unsupported).
    Image {
        /// Media type.
        media_type: String,
        /// Base64 data.
        data_base64: String,
    },
    /// Assistant text.
    Assistant(String),
    /// Tool call.
    ToolCall {
        /// Call ID.
        call_id: String,
        /// Tool name.
        tool: String,
        /// JSON arguments.
        arguments_json: String,
    },
    /// Tool result.
    ToolResult {
        /// Call ID.
        call_id: String,
        /// Content.
        content: String,
        /// Truncation marker.
        truncated: bool,
    },
    /// Context summary.
    ContextSummary {
        /// Text.
        text: String,
        /// Start sequence.
        source_start: u64,
        /// End sequence.
        source_end: u64,
    },
    /// Metadata.
    Metadata {
        /// Key.
        key: String,
        /// JSON value.
        value_json: String,
    },
}

/// Service-owned provider option.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureServiceOption {
    /// Option key.
    pub key: String,
    /// Textual value.
    pub value: String,
}

/// Service-owned execute request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureServiceRequest {
    /// Session reference.
    pub session_reference: String,
    /// Provider selection.
    pub provider: String,
    /// Model selection.
    pub model: String,
    /// Projected conversation.
    pub entries: Vec<FixtureServiceEntry>,
    /// Approved options.
    pub options: Vec<FixtureServiceOption>,
    /// Authorization grant.
    pub authorization_grant: String,
    /// Cancellation reference.
    pub cancellation_reference: String,
}

/// Service-owned usage.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FixtureServiceUsage {
    /// Input tokens.
    pub input_tokens: u64,
    /// Output tokens.
    pub output_tokens: u64,
}

/// Service-owned provider event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FixtureServiceEvent {
    /// Request started.
    Started,
    /// Visible text.
    TextDelta(String),
    /// Tool-call fragment.
    ToolCallDelta {
        /// Call ID.
        call_id: String,
        /// Name fragment.
        name_fragment: String,
        /// Arguments fragment.
        arguments_fragment: String,
    },
    /// Complete tool-call proposal.
    ToolCallProposed {
        /// Continuation reference.
        continuation_reference: String,
        /// Call ID.
        call_id: String,
        /// Tool name.
        tool: String,
        /// JSON arguments.
        arguments_json: String,
    },
    /// Normal completion.
    Completed {
        /// Finish reason.
        finish_reason: String,
        /// Usage.
        usage: FixtureServiceUsage,
    },
    /// Cancelled request.
    Cancelled,
    /// Classified failure.
    Failed {
        /// Stable code.
        code: String,
        /// Redacted message.
        message: String,
        /// Retry flag.
        retryable: bool,
    },
}

/// Service-owned execution response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureServiceResponse {
    /// Events in order.
    pub events: Vec<FixtureServiceEvent>,
}

/// Service-owned health response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureServiceHealth {
    /// Ready provider count.
    pub ready_provider_count: u32,
    /// Capability names.
    pub capabilities: Vec<String>,
}

/// Service-owned endpoint response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FixtureServiceReply {
    /// Health projection.
    Health(FixtureServiceHealth),
    /// Catalog projection.
    Catalog(Vec<CatalogProvider>),
}

/// Service failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum FixtureServiceError {
    /// Command kind is not handled by the endpoint.
    #[error("fixture command is not available: {0}")]
    WrongCommand(&'static str),
    /// Wire value could not be mapped safely.
    #[error("fixture wire value is invalid: {0}")]
    InvalidWireValue(String),
    /// Logic rejected or failed the operation.
    #[error("fixture logic failed: {0}")]
    LogicFailed(String),
}

/// Endpoint-facing fixture service.
#[derive(Clone, Debug)]
pub struct FixtureService<L> {
    logic: L,
}

impl<L> FixtureService<L> {
    /// Injects the fixture logic implementation.
    #[must_use]
    pub const fn new(logic: L) -> Self {
        Self { logic }
    }
}

impl<L> FixtureService<L>
where
    L: FixtureExecutionLogic + FixtureHealthLogic + FixtureCatalogLogic,
{
    /// Maps one wire command through the fixture boundaries.
    ///
    /// # Errors
    ///
    /// Returns a service-owned error for unsupported commands or failures.
    #[allow(clippy::unused_async, reason = "the endpoint contract is uniformly async")]
    pub async fn handle_wire_command(
        &self,
        command: &HarnessCommand,
    ) -> Result<FixtureServiceReply, FixtureServiceError> {
        match command {
            HarnessCommand::Health => {
                let health = self
                    .logic
                    .inspect_health()
                    .map_err(|error| map_logic_error(&error))?;
                Ok(FixtureServiceReply::Health(FixtureServiceHealth {
                    ready_provider_count: health.ready_provider_count,
                    capabilities: health.capabilities,
                }))
            }
            HarnessCommand::Catalog => {
                let records = self
                    .logic
                    .inspect_catalog()
                    .map_err(|error| map_logic_error(&error))?;
                Ok(FixtureServiceReply::Catalog(
                    records.into_iter().map(to_wire_catalog).collect(),
                ))
            }
            _ => Err(FixtureServiceError::WrongCommand("execute")),
        }
    }

    /// Executes a wire execute command into protocol events.
    ///
    /// # Errors
    ///
    /// Returns a service-owned error for mapping or execution failures.
    pub async fn execute_wire(
        &self,
        command: &HarnessCommand,
    ) -> Result<Vec<HarnessEvent>, FixtureServiceError> {
        let request = from_wire_command(command)?;
        let result = self
            .logic
            .execute_provider(to_logic_command(request))
            .await
            .map_err(|error| map_logic_error(&error))?;
        result.events.into_iter().map(to_wire_event).collect()
    }

    /// Resolves a wire continuation into protocol events.
    ///
    /// # Errors
    ///
    /// Returns a service-owned error for mapping or execution failures.
    pub async fn continue_wire(
        &self,
        command: &HarnessCommand,
    ) -> Result<Vec<HarnessEvent>, FixtureServiceError> {
        let HarnessCommand::Continue {
            continuation_id,
            decision,
        } = command
        else {
            return Err(FixtureServiceError::WrongCommand("continue"));
        };
        let decision = match decision {
            HarnessContinuationDecision::Continue => FixtureContinuationDecision::Continue,
            HarnessContinuationDecision::ReplaceContext { entries } => {
                FixtureContinuationDecision::ReplaceContext(
                    entries
                        .iter()
                        .map(from_wire_entry)
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter()
                        .map(to_logic_entry)
                        .collect(),
                )
            }
            HarnessContinuationDecision::Reject { reason } => {
                FixtureContinuationDecision::Reject { reason: reason.clone() }
            }
            HarnessContinuationDecision::Cancel { reason } => {
                FixtureContinuationDecision::Cancel { reason: reason.clone() }
            }
        };
        let result = self
            .logic
            .continue_provider(FixtureContinueCommand {
                continuation_reference: continuation_id.to_string(),
                decision,
            })
            .await
            .map_err(|error| map_logic_error(&error))?;
        result.events.into_iter().map(to_wire_event).collect()
    }

    /// Requests cancellation of one in-flight fixture exchange.
    ///
    /// # Errors
    ///
    /// Returns a service-owned error for failures.
    pub async fn cancel_wire(
        &self,
        command: &HarnessCommand,
    ) -> Result<bool, FixtureServiceError> {
        let HarnessCommand::Cancel { cancellation_id } = command else {
            return Err(FixtureServiceError::WrongCommand("cancel"));
        };
        self.logic
            .cancel_provider(&cancellation_id.to_string())
            .await
            .map_err(|error| map_logic_error(&error))
    }
}

fn from_wire_command(
    command: &HarnessCommand,
) -> Result<FixtureServiceRequest, FixtureServiceError> {
    let HarnessCommand::Execute {
        session_id,
        provider,
        model,
        entries,
        options,
        authorization_grant,
        cancellation_id,
    } = command
    else {
        return Err(FixtureServiceError::WrongCommand("execute"));
    };
    let options = options
        .as_object()
        .ok_or_else(|| FixtureServiceError::InvalidWireValue("options must be an object".into()))?
        .iter()
        .map(|(key, value)| {
            let value = value
                .as_str()
                .map_or_else(|| value.to_string(), str::to_owned);
            FixtureServiceOption {
                key: key.clone(),
                value,
            }
        })
        .collect();
    let entries = entries
        .iter()
        .map(from_wire_entry)
        .collect::<Result<_, _>>()?;
    Ok(FixtureServiceRequest {
        session_reference: session_id.to_string(),
        provider: provider.clone(),
        model: model.clone(),
        entries,
        options,
        authorization_grant: authorization_grant.clone(),
        cancellation_reference: cancellation_id.to_string(),
    })
}

fn from_wire_entry(
    entry: &ProjectedEntry,
) -> Result<FixtureServiceEntry, FixtureServiceError> {
    Ok(match entry {
        ProjectedEntry::System { text } => FixtureServiceEntry::System(text.clone()),
        ProjectedEntry::User { text } => FixtureServiceEntry::User(text.clone()),
        ProjectedEntry::Image {
            media_type,
            data_base64,
        } => FixtureServiceEntry::Image {
            media_type: media_type.clone(),
            data_base64: data_base64.clone(),
        },
        ProjectedEntry::Assistant { text } => FixtureServiceEntry::Assistant(text.clone()),
        ProjectedEntry::ToolCall {
            call_id,
            tool,
            arguments,
        } => FixtureServiceEntry::ToolCall {
            call_id: call_id.clone(),
            tool: tool.clone(),
            arguments_json: serde_json::to_string(arguments).map_err(|error| {
                FixtureServiceError::InvalidWireValue(format!("tool arguments: {error}"))
            })?,
        },
        ProjectedEntry::ToolResult {
            call_id,
            content,
            truncated,
        } => FixtureServiceEntry::ToolResult {
            call_id: call_id.clone(),
            content: content.clone(),
            truncated: *truncated,
        },
        ProjectedEntry::ContextSummary {
            text,
            source_start,
            source_end,
        } => FixtureServiceEntry::ContextSummary {
            text: text.clone(),
            source_start: *source_start,
            source_end: *source_end,
        },
        ProjectedEntry::Metadata { key, value } => FixtureServiceEntry::Metadata {
            key: key.clone(),
            value_json: serde_json::to_string(value).map_err(|error| {
                FixtureServiceError::InvalidWireValue(format!("metadata value: {error}"))
            })?,
        },
    })
}

fn to_logic_command(request: FixtureServiceRequest) -> FixtureExecuteCommand {
    FixtureExecuteCommand {
        session_reference: request.session_reference,
        provider: request.provider,
        model: request.model,
        entries: request.entries.into_iter().map(to_logic_entry).collect(),
        options: request
            .options
            .into_iter()
            .map(|option| FixtureLogicOption {
                key: option.key,
                value: option.value,
            })
            .collect(),
        authorization_grant: request.authorization_grant,
        cancellation_reference: request.cancellation_reference,
    }
}

fn to_logic_entry(entry: FixtureServiceEntry) -> FixtureLogicEntry {
    match entry {
        FixtureServiceEntry::System(text) => FixtureLogicEntry::System(text),
        FixtureServiceEntry::User(text) => FixtureLogicEntry::User(text),
        FixtureServiceEntry::Image {
            media_type,
            data_base64,
        } => FixtureLogicEntry::Image {
            media_type,
            data_base64,
        },
        FixtureServiceEntry::Assistant(text) => FixtureLogicEntry::Assistant(text),
        FixtureServiceEntry::ToolCall {
            call_id,
            tool,
            arguments_json,
        } => FixtureLogicEntry::ToolCall {
            call_id,
            tool,
            arguments_json,
        },
        FixtureServiceEntry::ToolResult {
            call_id,
            content,
            truncated,
        } => FixtureLogicEntry::ToolResult {
            call_id,
            content,
            truncated,
        },
        FixtureServiceEntry::ContextSummary {
            text,
            source_start,
            source_end,
        } => FixtureLogicEntry::ContextSummary {
            text,
            source_start,
            source_end,
        },
        FixtureServiceEntry::Metadata { key, value_json } => {
            FixtureLogicEntry::Metadata { key, value_json }
        }
    }
}

fn to_wire_event(event: FixtureLogicEvent) -> Result<HarnessEvent, FixtureServiceError> {
    Ok(match event {
        FixtureLogicEvent::Started => HarnessEvent::Started,
        FixtureLogicEvent::TextDelta(text) => HarnessEvent::TextDelta { text },
        FixtureLogicEvent::ToolCallDelta {
            call_id,
            name_fragment,
            arguments_fragment,
        } => HarnessEvent::ToolCallDelta {
            call_id,
            name_fragment,
            arguments_fragment,
        },
        FixtureLogicEvent::ToolCallProposed {
            continuation_reference,
            call_id,
            tool,
            arguments_json,
        } => HarnessEvent::ToolCallProposed {
            continuation_id: ContinuationId::from_str(&continuation_reference).map_err(
                |error| {
                    FixtureServiceError::InvalidWireValue(format!(
                        "continuation reference: {error}"
                    ))
                },
            )?,
            call_id,
            tool,
            arguments: serde_json::from_str(&arguments_json).map_err(|error| {
                FixtureServiceError::InvalidWireValue(format!("tool proposal arguments: {error}"))
            })?,
        },
        FixtureLogicEvent::Completed {
            finish_reason,
            usage,
        } => HarnessEvent::Completed {
            finish_reason,
            usage: Usage {
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
                reasoning_tokens: 0,
                estimated: false,
                cost: None,
            },
        },
        FixtureLogicEvent::Cancelled => HarnessEvent::Cancelled,
        FixtureLogicEvent::RuntimeRejected { reason } => HarnessEvent::Failed {
            code: "runtime_rejected".into(),
            message: reason,
            retryable: false,
        },
        FixtureLogicEvent::Failed {
            code,
            message,
            retryable,
        } => HarnessEvent::Failed {
            code,
            message,
            retryable,
        },
    })
}

fn to_wire_catalog(record: FixtureCatalogResult) -> CatalogProvider {
    CatalogProvider {
        id: record.id,
        version: record.version,
        models: record.models,
        capabilities: record.capabilities,
        context_limit: Some(8_192),
        tool_support: record.tool_support,
        image_support: record.image_support,
        structured_output_support: record.structured_output_support,
        streaming_support: record.streaming_support,
        pricing_source: record.pricing_source,
        available: record.available,
    }
}

fn map_logic_error(error: &FixtureLogicError) -> FixtureServiceError {
    FixtureServiceError::LogicFailed(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use agentmod_harness_fixture_logic::{FixtureExecuteResult, FixtureHealthResult, FixtureLogicUsage};

    use super::*;

    struct MockLogic {
        commands: Mutex<Vec<FixtureExecuteCommand>>,
    }

    #[async_trait::async_trait]
    impl FixtureExecutionLogic for MockLogic {
        async fn execute_provider(
            &self,
            command: FixtureExecuteCommand,
        ) -> Result<FixtureExecuteResult, FixtureLogicError> {
            self.commands
                .lock()
                .expect("command lock is not poisoned")
                .push(command);
            Ok(FixtureExecuteResult {
                events: vec![
                    FixtureLogicEvent::TextDelta("hello".into()),
                    FixtureLogicEvent::Completed {
                        finish_reason: "stop".into(),
                        usage: FixtureLogicUsage {
                            input_tokens: 2,
                            output_tokens: 1,
                        },
                    },
                ],
            })
        }

        async fn continue_provider(
            &self,
            _command: FixtureContinueCommand,
        ) -> Result<FixtureExecuteResult, FixtureLogicError> {
            Ok(FixtureExecuteResult {
                events: vec![FixtureLogicEvent::Cancelled],
            })
        }

        async fn cancel_provider(
            &self,
            _reference: &str,
        ) -> Result<bool, FixtureLogicError> {
            Ok(false)
        }
    }

    impl FixtureHealthLogic for MockLogic {
        fn inspect_health(&self) -> Result<FixtureHealthResult, FixtureLogicError> {
            Ok(FixtureHealthResult {
                ready_provider_count: 1,
                capabilities: vec!["streaming".into()],
            })
        }
    }

    impl FixtureCatalogLogic for MockLogic {
        fn inspect_catalog(&self) -> Result<Vec<FixtureCatalogResult>, FixtureLogicError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn maps_execute_wire_through_service_boundaries() {
        let service = FixtureService::new(MockLogic {
            commands: Mutex::new(Vec::new()),
        });
        let events = service
            .execute_wire(&wire_command())
            .await
            .expect("wire execution");
        assert_eq!(
            events,
            [
                HarnessEvent::TextDelta {
                    text: "hello".into()
                },
                HarnessEvent::Completed {
                    finish_reason: "stop".into(),
                    usage: Usage {
                        input_tokens: 2,
                        output_tokens: 1,
                        cache_read_tokens: 0,
                        cache_write_tokens: 0,
                        reasoning_tokens: 0,
                        estimated: false,
                        cost: None,
                    },
                }
            ]
        );
        let command = &service
            .logic
            .commands
            .lock()
            .expect("command lock is not poisoned")[0];
        assert_eq!(command.entries, [FixtureLogicEntry::User("hi".into())]);
    }

    #[tokio::test]
    async fn health_and_catalog_replies_are_service_owned() {
        let service = FixtureService::new(MockLogic {
            commands: Mutex::new(Vec::new()),
        });
        match service.handle_wire_command(&HarnessCommand::Health).await {
            Ok(FixtureServiceReply::Health(health)) => {
                assert_eq!(health.ready_provider_count, 1);
            }
            other => panic!("unexpected health reply: {other:?}"),
        }
        assert!(matches!(
            service.handle_wire_command(&HarnessCommand::Catalog).await,
            Ok(FixtureServiceReply::Catalog(_))
        ));
    }

    fn wire_command() -> HarnessCommand {
        HarnessCommand::Execute {
            session_id: "018f6f83-7b80-7000-8000-000000000001"
                .parse()
                .expect("session ID"),
            provider: "fixture-deterministic".into(),
            model: "fixture-model".into(),
            entries: vec![ProjectedEntry::User { text: "hi".into() }],
            options: serde_json::json!({"fixture_scenario": "text"}),
            authorization_grant: "grant".into(),
            cancellation_id: "018f6f83-7b80-7000-8000-000000000002"
                .parse()
                .expect("cancellation ID"),
        }
    }
}
