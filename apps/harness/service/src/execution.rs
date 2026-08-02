//! Provider execution endpoint mapping.

use std::str::FromStr;

use agentmod_harness_logic::execution::{
    ContinueProviderCommand, ExecuteProviderCommand, ExecuteProviderResult, ExecutionLogicError,
    HarnessCancellationLogic, HarnessContinuationLogic, HarnessExecutionLogic,
    LogicContinuationDecision, LogicConversationEntry, LogicProviderEvent,
    LogicProviderFailureKind, LogicProviderOption, LogicRetryClassification, LogicUsage,
};
use agentmod_harness_protocol::{
    CostMetadata, HarnessCommand, HarnessContinuationDecision, HarnessEvent, ProjectedEntry, Usage,
};
use agentmod_primitives::ContinuationId;

use crate::HarnessService;

/// Service-owned projected conversation entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceConversationEntry {
    /// System instruction.
    System(String),
    /// User content.
    User(String),
    /// Provider-visible image input.
    Image {
        /// Image media type.
        media_type: String,
        /// Base64-encoded image bytes.
        data_base64: String,
    },
    /// Assistant content.
    Assistant(String),
    /// Tool call.
    ToolCall {
        /// Call ID.
        call_id: String,
        /// Tool.
        tool: String,
        /// JSON arguments.
        arguments_json: String,
    },
    /// Tool result.
    ToolResult {
        /// Call ID.
        call_id: String,
        /// Bounded content.
        content: String,
        /// Artifact overflow marker.
        truncated: bool,
    },
    /// Context summary.
    ContextSummary {
        /// Summary.
        text: String,
        /// Source start.
        source_start: u64,
        /// Source end.
        source_end: u64,
    },
    /// Provider metadata.
    Metadata {
        /// Key.
        key: String,
        /// JSON value.
        value_json: String,
    },
}

/// Service-owned provider option.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceProviderOption {
    /// Option key.
    pub key: String,
    /// Textual value.
    pub value: String,
}

/// Service-owned execute endpoint request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceExecuteRequest {
    /// Session reference.
    pub session_reference: String,
    /// Provider.
    pub provider: String,
    /// Model.
    pub model: String,
    /// Approved entries.
    pub entries: Vec<ServiceConversationEntry>,
    /// Provider options.
    pub options: Vec<ServiceProviderOption>,
    /// Authorization grant.
    pub authorization_grant: String,
    /// Cancellation reference.
    pub cancellation_reference: String,
}

/// Service-owned provider usage.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ServiceUsage {
    /// Input tokens.
    pub input_tokens: u64,
    /// Output tokens.
    pub output_tokens: u64,
    /// Cache-read tokens.
    pub cache_read_tokens: u64,
    /// Cache-write tokens.
    pub cache_write_tokens: u64,
    /// Provider-reported reasoning tokens.
    pub reasoning_tokens: u64,
    /// True only when usage is estimated rather than provider-reported.
    pub estimated: bool,
}

/// Service-owned pricing-record identity and computed cost.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ServiceCostRecord {
    /// Stable pricing-record source.
    pub source: String,
    /// Pricing-record version.
    pub version: String,
    /// Computed input cost in micro-units of `currency`.
    pub input_cost_micros: u64,
    /// Computed output cost in micro-units of `currency`.
    pub output_cost_micros: u64,
    /// Computed cache-read cost in micro-units of `currency`.
    pub cache_read_cost_micros: u64,
    /// Computed cache-write cost in micro-units of `currency`.
    pub cache_write_cost_micros: u64,
    /// ISO-4217 currency code; empty when the pricing record is unknown.
    pub currency: String,
}

/// Service-owned provider event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceProviderEvent {
    /// Provider started.
    Started,
    /// Visible text.
    TextDelta(String),
    /// Tool-call fragment.
    ToolCallDelta {
        /// Call ID.
        call_id: String,
        /// Name fragment.
        name_fragment: String,
        /// Argument fragment.
        arguments_fragment: String,
    },
    /// Tool call requiring continuation.
    ToolCallProposed {
        /// Continuation reference.
        continuation_reference: String,
        /// Call ID.
        call_id: String,
        /// Tool.
        tool: String,
        /// JSON arguments.
        arguments_json: String,
    },
    /// Provider completion.
    Completed {
        /// Finish reason.
        finish_reason: String,
        /// Usage.
        usage: ServiceUsage,
        /// Pricing-record identity and computed cost.
        cost: Option<ServiceCostRecord>,
    },
    /// Provider cancellation.
    Cancelled,
    /// Provider failure.
    Failed {
        /// Stable code.
        code: String,
        /// Redacted message.
        message: String,
        /// Policy retry flag.
        retryable: bool,
    },
}

/// Service-owned bounded execution response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceExecuteResponse {
    /// Events in provider order.
    pub events: Vec<ServiceProviderEvent>,
}

/// Service-owned continuation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceContinueRequest {
    /// Pending continuation reference.
    pub continuation_reference: String,
    /// Runtime decision mapped out of the wire contract.
    pub decision: ServiceContinuationDecision,
}

/// Service-owned continuation decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceContinuationDecision {
    /// Continue with the prior approved context.
    Continue,
    /// Continue with replacement structured context.
    ReplaceContext(Vec<ServiceConversationEntry>),
    /// Reject the proposal.
    Reject {
        /// Safe reason.
        reason: String,
    },
    /// Cancel the execution.
    Cancel {
        /// Safe reason.
        reason: String,
    },
}

/// Execute endpoint failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionServiceError {
    /// Command is not an execute command.
    WrongCommand,
    /// Wire value could not be mapped safely.
    InvalidWireValue(String),
    /// Logic rejected or failed execution.
    ExecutionFailed(String),
}

impl std::fmt::Display for ExecutionServiceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongCommand => formatter.write_str("wire command is not provider execution"),
            Self::InvalidWireValue(message) => {
                write!(formatter, "execute wire value is invalid: {message}")
            }
            Self::ExecutionFailed(message) => {
                write!(formatter, "provider execution failed: {message}")
            }
        }
    }
}

impl std::error::Error for ExecutionServiceError {}

impl<L> HarnessService<L>
where
    L: HarnessExecutionLogic,
{
    /// Maps a wire execute command through every service boundary and returns
    /// protocol events.
    ///
    /// # Errors
    ///
    /// Returns service-owned mapping or execution errors.
    pub async fn execute_wire(
        &self,
        command: &HarnessCommand,
    ) -> Result<Vec<HarnessEvent>, ExecutionServiceError> {
        let request = from_wire_command(command)?;
        let response = self.execute(request).await?;
        response.events.into_iter().map(to_wire_event).collect()
    }

    /// Executes using only service-owned request and response types.
    ///
    /// # Errors
    ///
    /// Returns a translated logic failure.
    pub async fn execute(
        &self,
        request: ServiceExecuteRequest,
    ) -> Result<ServiceExecuteResponse, ExecutionServiceError> {
        let result = self
            .logic
            .execute_provider(to_logic_command(request))
            .await
            .map_err(|error| map_logic_error(&error))?;
        Ok(map_logic_result(result))
    }
}

impl<L> HarnessService<L>
where
    L: HarnessContinuationLogic,
{
    /// Resolves a wire continuation and returns the next bounded lifecycle events.
    ///
    /// # Errors
    ///
    /// Returns service-owned mapping or translated logic errors. Duplicate
    /// resolution is rejected by harness logic.
    pub async fn continue_wire(
        &self,
        command: &HarnessCommand,
    ) -> Result<Vec<HarnessEvent>, ExecutionServiceError> {
        let HarnessCommand::Continue {
            continuation_id,
            decision,
        } = command
        else {
            return Err(ExecutionServiceError::WrongCommand);
        };
        let service_decision = match decision {
            HarnessContinuationDecision::Continue => ServiceContinuationDecision::Continue,
            HarnessContinuationDecision::ReplaceContext { entries } => {
                ServiceContinuationDecision::ReplaceContext(
                    entries
                        .iter()
                        .map(from_wire_entry)
                        .collect::<Result<_, _>>()?,
                )
            }
            HarnessContinuationDecision::Reject { reason } => ServiceContinuationDecision::Reject {
                reason: reason.clone(),
            },
            HarnessContinuationDecision::Cancel { reason } => ServiceContinuationDecision::Cancel {
                reason: reason.clone(),
            },
        };
        let response = self
            .continue_execution(ServiceContinueRequest {
                continuation_reference: continuation_id.to_string(),
                decision: service_decision,
            })
            .await?;
        response.events.into_iter().map(to_wire_event).collect()
    }

    /// Executes a service-owned continuation request.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionServiceError`] for translated business failure.
    pub async fn continue_execution(
        &self,
        request: ServiceContinueRequest,
    ) -> Result<ServiceExecuteResponse, ExecutionServiceError> {
        let decision = match request.decision {
            ServiceContinuationDecision::Continue => LogicContinuationDecision::Continue,
            ServiceContinuationDecision::ReplaceContext(entries) => {
                LogicContinuationDecision::ReplaceContext(
                    entries.into_iter().map(to_logic_entry).collect(),
                )
            }
            ServiceContinuationDecision::Reject { reason } => {
                LogicContinuationDecision::Reject { reason }
            }
            ServiceContinuationDecision::Cancel { reason } => {
                LogicContinuationDecision::Cancel { reason }
            }
        };
        self.logic
            .continue_provider(ContinueProviderCommand {
                continuation_reference: request.continuation_reference,
                decision,
            })
            .await
            .map(map_logic_result)
            .map_err(|error| map_logic_error(&error))
    }
}

impl<L> HarnessService<L>
where
    L: HarnessCancellationLogic,
{
    /// Requests cancellation of one in-flight provider exchange.
    ///
    /// Returns whether an active exchange was found and cancelled.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionServiceError`] for translated business failure.
    pub async fn cancel_wire(
        &self,
        command: &HarnessCommand,
    ) -> Result<bool, ExecutionServiceError> {
        let HarnessCommand::Cancel { cancellation_id } = command else {
            return Err(ExecutionServiceError::WrongCommand);
        };
        self.cancel_execution(&cancellation_id.to_string())
            .await
    }

    /// Executes a service-owned cancellation request.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionServiceError`] for translated business failure.
    pub async fn cancel_execution(
        &self,
        cancellation_reference: &str,
    ) -> Result<bool, ExecutionServiceError> {
        self.logic
            .cancel_provider(cancellation_reference)
            .await
            .map_err(|error| map_logic_error(&error))
    }
}

fn from_wire_command(
    command: &HarnessCommand,
) -> Result<ServiceExecuteRequest, ExecutionServiceError> {
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
        return Err(ExecutionServiceError::WrongCommand);
    };
    let options = options
        .as_object()
        .ok_or_else(|| ExecutionServiceError::InvalidWireValue("options must be an object".into()))?
        .iter()
        .map(|(key, value)| {
            let value = value
                .as_str()
                .map_or_else(|| value.to_string(), str::to_owned);
            ServiceProviderOption {
                key: key.clone(),
                value,
            }
        })
        .collect();
    let entries = entries
        .iter()
        .map(from_wire_entry)
        .collect::<Result<_, _>>()?;
    Ok(ServiceExecuteRequest {
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
) -> Result<ServiceConversationEntry, ExecutionServiceError> {
    Ok(match entry {
        ProjectedEntry::System { text } => ServiceConversationEntry::System(text.clone()),
        ProjectedEntry::User { text } => ServiceConversationEntry::User(text.clone()),
        ProjectedEntry::Image {
            media_type,
            data_base64,
        } => ServiceConversationEntry::Image {
            media_type: media_type.clone(),
            data_base64: data_base64.clone(),
        },
        ProjectedEntry::Assistant { text } => ServiceConversationEntry::Assistant(text.clone()),
        ProjectedEntry::ToolCall {
            call_id,
            tool,
            arguments,
        } => ServiceConversationEntry::ToolCall {
            call_id: call_id.clone(),
            tool: tool.clone(),
            arguments_json: serde_json::to_string(arguments).map_err(|error| {
                ExecutionServiceError::InvalidWireValue(format!("tool arguments: {error}"))
            })?,
        },
        ProjectedEntry::ToolResult {
            call_id,
            content,
            truncated,
        } => ServiceConversationEntry::ToolResult {
            call_id: call_id.clone(),
            content: content.clone(),
            truncated: *truncated,
        },
        ProjectedEntry::ContextSummary {
            text,
            source_start,
            source_end,
        } => ServiceConversationEntry::ContextSummary {
            text: text.clone(),
            source_start: *source_start,
            source_end: *source_end,
        },
        ProjectedEntry::Metadata { key, value } => ServiceConversationEntry::Metadata {
            key: key.clone(),
            value_json: serde_json::to_string(value).map_err(|error| {
                ExecutionServiceError::InvalidWireValue(format!("metadata value: {error}"))
            })?,
        },
    })
}

fn to_logic_command(request: ServiceExecuteRequest) -> ExecuteProviderCommand {
    ExecuteProviderCommand {
        session_reference: request.session_reference,
        provider: request.provider,
        model: request.model,
        entries: request.entries.into_iter().map(to_logic_entry).collect(),
        options: request
            .options
            .into_iter()
            .map(|option| LogicProviderOption {
                key: option.key,
                value: option.value,
            })
            .collect(),
        authorization_grant: request.authorization_grant,
        cancellation_reference: request.cancellation_reference,
    }
}

fn to_logic_entry(entry: ServiceConversationEntry) -> LogicConversationEntry {
    match entry {
        ServiceConversationEntry::System(text) => LogicConversationEntry::System(text),
        ServiceConversationEntry::User(text) => LogicConversationEntry::User(text),
        ServiceConversationEntry::Image {
            media_type,
            data_base64,
        } => LogicConversationEntry::Image {
            media_type,
            data_base64,
        },
        ServiceConversationEntry::Assistant(text) => LogicConversationEntry::Assistant(text),
        ServiceConversationEntry::ToolCall {
            call_id,
            tool,
            arguments_json,
        } => LogicConversationEntry::ToolCall {
            call_id,
            tool,
            arguments_json,
        },
        ServiceConversationEntry::ToolResult {
            call_id,
            content,
            truncated,
        } => LogicConversationEntry::ToolResult {
            call_id,
            content,
            truncated,
        },
        ServiceConversationEntry::ContextSummary {
            text,
            source_start,
            source_end,
        } => LogicConversationEntry::ContextSummary {
            text,
            source_start,
            source_end,
        },
        ServiceConversationEntry::Metadata { key, value_json } => {
            LogicConversationEntry::Metadata { key, value_json }
        }
    }
}

fn map_logic_result(result: ExecuteProviderResult) -> ServiceExecuteResponse {
    ServiceExecuteResponse {
        events: result.events.into_iter().map(map_logic_event).collect(),
    }
}

fn map_logic_event(event: LogicProviderEvent) -> ServiceProviderEvent {
    match event {
        LogicProviderEvent::Started => ServiceProviderEvent::Started,
        LogicProviderEvent::TextDelta(text) => ServiceProviderEvent::TextDelta(text),
        LogicProviderEvent::ToolCallDelta {
            call_id,
            name_fragment,
            arguments_fragment,
        } => ServiceProviderEvent::ToolCallDelta {
            call_id,
            name_fragment,
            arguments_fragment,
        },
        LogicProviderEvent::ToolCallProposed {
            continuation_reference,
            call_id,
            tool,
            arguments_json,
        } => ServiceProviderEvent::ToolCallProposed {
            continuation_reference,
            call_id,
            tool,
            arguments_json,
        },
        LogicProviderEvent::Completed {
            finish_reason,
            usage,
            cost,
        } => ServiceProviderEvent::Completed {
            finish_reason,
            usage: map_usage(usage),
            cost: cost.map(map_cost),
        },
        LogicProviderEvent::Cancelled => ServiceProviderEvent::Cancelled,
        LogicProviderEvent::RuntimeRejected { reason } => ServiceProviderEvent::Failed {
            code: "runtime_rejected".into(),
            message: reason,
            retryable: false,
        },
        LogicProviderEvent::Failed {
            kind,
            message,
            retry,
        } => ServiceProviderEvent::Failed {
            code: failure_code(kind).into(),
            message,
            retryable: !matches!(retry, LogicRetryClassification::Never),
        },
    }
}

const fn map_usage(usage: LogicUsage) -> ServiceUsage {
    ServiceUsage {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cache_read_tokens: usage.cache_read_tokens,
        cache_write_tokens: usage.cache_write_tokens,
        reasoning_tokens: usage.reasoning_tokens,
        estimated: usage.estimated,
    }
}

fn map_cost(cost: agentmod_harness_logic::execution::LogicCostRecord) -> ServiceCostRecord {
    ServiceCostRecord {
        source: cost.source,
        version: cost.version,
        input_cost_micros: cost.input_cost_micros,
        output_cost_micros: cost.output_cost_micros,
        cache_read_cost_micros: cost.cache_read_cost_micros,
        cache_write_cost_micros: cost.cache_write_cost_micros,
        currency: cost.currency,
    }
}

const fn failure_code(kind: LogicProviderFailureKind) -> &'static str {
    match kind {
        LogicProviderFailureKind::MalformedToolArguments => "malformed_tool_arguments",
        LogicProviderFailureKind::Timeout => "timeout",
        LogicProviderFailureKind::RateLimited => "rate_limited",
        LogicProviderFailureKind::PartialOutputFailure => "partial_output_failure",
        LogicProviderFailureKind::Disconnected => "disconnected",
        LogicProviderFailureKind::AuthenticationFailed => "authentication_failed",
        LogicProviderFailureKind::ProviderOverloaded => "provider_overloaded",
        LogicProviderFailureKind::InvalidRequest => "invalid_request",
        LogicProviderFailureKind::UnsupportedCapability => "unsupported_capability",
        LogicProviderFailureKind::TransportFailure => "transport_failure",
        LogicProviderFailureKind::AmbiguousDisconnect => "ambiguous_disconnect",
        LogicProviderFailureKind::UserCancellation => "user_cancellation",
    }
}

fn to_wire_event(event: ServiceProviderEvent) -> Result<HarnessEvent, ExecutionServiceError> {
    Ok(match event {
        ServiceProviderEvent::Started => HarnessEvent::Started,
        ServiceProviderEvent::TextDelta(text) => HarnessEvent::TextDelta { text },
        ServiceProviderEvent::ToolCallDelta {
            call_id,
            name_fragment,
            arguments_fragment,
        } => HarnessEvent::ToolCallDelta {
            call_id,
            name_fragment,
            arguments_fragment,
        },
        ServiceProviderEvent::ToolCallProposed {
            continuation_reference,
            call_id,
            tool,
            arguments_json,
        } => HarnessEvent::ToolCallProposed {
            continuation_id: ContinuationId::from_str(&continuation_reference).map_err(
                |error| {
                    ExecutionServiceError::InvalidWireValue(format!(
                        "continuation reference: {error}"
                    ))
                },
            )?,
            call_id,
            tool,
            arguments: serde_json::from_str(&arguments_json).map_err(|error| {
                ExecutionServiceError::InvalidWireValue(format!("tool proposal arguments: {error}"))
            })?,
        },
        ServiceProviderEvent::Completed {
            finish_reason,
            usage,
            cost,
        } => HarnessEvent::Completed {
            finish_reason,
            usage: Usage {
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
                cache_read_tokens: usage.cache_read_tokens,
                cache_write_tokens: usage.cache_write_tokens,
                reasoning_tokens: usage.reasoning_tokens,
                estimated: usage.estimated,
                cost: cost.map(|cost| CostMetadata {
                    source: cost.source,
                    version: cost.version,
                    input_cost_micros: cost.input_cost_micros,
                    output_cost_micros: cost.output_cost_micros,
                    cache_read_cost_micros: cost.cache_read_cost_micros,
                    cache_write_cost_micros: cost.cache_write_cost_micros,
                    currency: cost.currency,
                }),
            },
        },
        ServiceProviderEvent::Cancelled => HarnessEvent::Cancelled,
        ServiceProviderEvent::Failed {
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

fn map_logic_error(error: &ExecutionLogicError) -> ExecutionServiceError {
    ExecutionServiceError::ExecutionFailed(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use agentmod_harness_logic::execution::HarnessExecutionLogic;

    use super::*;

    struct MockExecutionLogic {
        commands: Mutex<Vec<ExecuteProviderCommand>>,
    }

    #[async_trait::async_trait]
    impl HarnessExecutionLogic for MockExecutionLogic {
        async fn execute_provider(
            &self,
            command: ExecuteProviderCommand,
        ) -> Result<ExecuteProviderResult, ExecutionLogicError> {
            self.commands
                .lock()
                .expect("command lock is not poisoned")
                .push(command);
            Ok(ExecuteProviderResult {
                events: vec![
                    LogicProviderEvent::TextDelta("hello".into()),
                    LogicProviderEvent::Completed {
                        finish_reason: "stop".into(),
                        usage: LogicUsage {
                            input_tokens: 2,
                            output_tokens: 1,
                            cache_read_tokens: 0,
                            cache_write_tokens: 0,
                            reasoning_tokens: 0,
                            estimated: false,
                        },
                        cost: None,
                    },
                ],
            })
        }
    }

    #[tokio::test]
    async fn maps_execute_wire_to_logic_and_protocol_events() {
        let service = HarnessService::new(MockExecutionLogic {
            commands: Mutex::new(Vec::new()),
        });
        let events = service
            .execute_wire(&wire_command("streaming_text"))
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
        assert_eq!(command.entries, [LogicConversationEntry::User("hi".into())]);
        assert_eq!(
            command.options,
            [LogicProviderOption {
                key: "mock_scenario".into(),
                value: "streaming_text".into()
            }]
        );
    }

    #[tokio::test]
    async fn maps_image_wire_entries_through_the_service_boundary() {
        let service = HarnessService::new(MockExecutionLogic {
            commands: Mutex::new(Vec::new()),
        });
        let mut command = wire_command("text");
        let HarnessCommand::Execute { entries, .. } = &mut command else {
            unreachable!("wire command is execute")
        };
        entries.push(ProjectedEntry::Image {
            media_type: "image/png".into(),
            data_base64: "aGVsbG8=".into(),
        });
        let _ = service
            .execute_wire(&command)
            .await
            .expect("wire execution");
        assert_eq!(
            service
                .logic
                .commands
                .lock()
                .expect("command lock is not poisoned")[0]
                .entries,
            [
                LogicConversationEntry::User("hi".into()),
                LogicConversationEntry::Image {
                    media_type: "image/png".into(),
                    data_base64: "aGVsbG8=".into(),
                },
            ]
        );
    }

    fn wire_command(scenario: &str) -> HarnessCommand {
        HarnessCommand::Execute {
            session_id: "018f6f83-7b80-7000-8000-000000000001"
                .parse()
                .expect("session ID"),
            provider: "deterministic-mock".into(),
            model: "mock-model".into(),
            entries: vec![ProjectedEntry::User { text: "hi".into() }],
            options: serde_json::json!({"mock_scenario": scenario}),
            authorization_grant: "grant".into(),
            cancellation_id: "018f6f83-7b80-7000-8000-000000000002"
                .parse()
                .expect("cancellation ID"),
        }
    }
}
