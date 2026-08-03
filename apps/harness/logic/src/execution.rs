//! Provider-neutral harness execution business behavior.

use agentmod_harness_data::execution::{
    DataConversationEntry, DataCostMetadata, DataProviderEvent, DataProviderFailureKind,
    DataProviderOption, DataRetryClassification, DataUsageRecord, HarnessExecutionData,
    HarnessExecutionDataError, HarnessExecutionDataQuery,
};

use crate::HarnessHealthManager;

/// Logic-owned conversation entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicConversationEntry {
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
    /// Visible assistant content.
    Assistant(String),
    /// Approved tool request.
    ToolCall {
        /// Call ID.
        call_id: String,
        /// Tool.
        tool: String,
        /// JSON arguments.
        arguments_json: String,
    },
    /// Bounded tool result.
    ToolResult {
        /// Call ID.
        call_id: String,
        /// Visible result.
        content: String,
        /// Artifact overflow marker.
        truncated: bool,
    },
    /// Typed context summary.
    ContextSummary {
        /// Summary.
        text: String,
        /// Source start.
        source_start: u64,
        /// Source end.
        source_end: u64,
    },
    /// Provider-visible metadata.
    Metadata {
        /// Key.
        key: String,
        /// JSON value.
        value_json: String,
    },
}

/// Logic-owned provider option.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicProviderOption {
    /// Option key.
    pub key: String,
    /// Textual value.
    pub value: String,
}

/// Logic command for one approved provider request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecuteProviderCommand {
    /// Owning session reference, used for validation and audit only.
    pub session_reference: String,
    /// Selected provider.
    pub provider: String,
    /// Selected model.
    pub model: String,
    /// Approved conversation projection.
    pub entries: Vec<LogicConversationEntry>,
    /// Approved provider options.
    pub options: Vec<LogicProviderOption>,
    /// Short-lived runtime authorization grant.
    pub authorization_grant: String,
    /// Cancellation reference.
    pub cancellation_reference: String,
}

/// Logic-owned usage.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LogicUsage {
    /// Input tokens.
    pub input_tokens: u64,
    /// Output tokens.
    pub output_tokens: u64,
    /// Cache-read tokens.
    pub cache_read_tokens: u64,
    /// Cache-write tokens.
    pub cache_write_tokens: u64,
    /// Provider-reported reasoning/thinking tokens.
    pub reasoning_tokens: u64,
    /// True only when usage is estimated rather than provider-reported.
    pub estimated: bool,
}

/// Logic-owned pricing-record identity and computed cost.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LogicCostMetadata {
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

/// Logic-owned provider failure kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicProviderFailureKind {
    /// Malformed tool arguments.
    MalformedToolArguments,
    /// Timeout.
    Timeout,
    /// Rate limit.
    RateLimited,
    /// Partial output failure.
    PartialOutputFailure,
    /// Disconnection.
    Disconnected,
    /// Provider rejected the supplied credentials.
    AuthenticationFailed,
    /// Provider reported overload or transient server failure.
    ProviderOverloaded,
    /// Provider rejected the request as invalid.
    InvalidRequest,
    /// Provider does not support the requested capability or model.
    UnsupportedCapability,
    /// Transport failed safely before any provider response.
    TransportFailure,
    /// Disconnect after dispatch whose outcome is ambiguous.
    AmbiguousDisconnect,
    /// The caller cancelled the request.
    UserCancellation,
}

/// Logic-owned retry classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicRetryClassification {
    /// No retry.
    Never,
    /// Immediate retry.
    Immediate,
    /// Delayed retry.
    AfterMilliseconds(u64),
}

/// Logic-owned provider lifecycle result event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicProviderEvent {
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
    /// Tool call requiring runtime continuation.
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
    /// Provider completed.
    Completed {
        /// Finish reason.
        finish_reason: String,
        /// Usage.
        usage: LogicUsage,
        /// Pricing-record identity and computed cost.
        cost: Option<LogicCostMetadata>,
    },
    /// Provider cancelled.
    Cancelled,
    /// Runtime rejected a proposed continuation before another provider request.
    RuntimeRejected {
        /// Safe rejection reason.
        reason: String,
    },
    /// Provider failed.
    Failed {
        /// Failure kind.
        kind: LogicProviderFailureKind,
        /// Redacted message.
        message: String,
        /// Retry classification.
        retry: LogicRetryClassification,
    },
}

/// Logic result containing bounded ordered lifecycle events.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecuteProviderResult {
    /// Events in observation order.
    pub events: Vec<LogicProviderEvent>,
}

/// Harness-local state retained while runtime evaluates a provider proposal.
#[derive(Clone, Debug)]
pub(crate) struct PendingProviderExecution {
    command: ExecuteProviderCommand,
    sibling_continuations: Vec<String>,
}

/// Logic-owned runtime continuation decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicContinuationDecision {
    /// Issue a fresh provider request using the prior approved projection.
    Continue,
    /// Issue a fresh provider request using replacement structured context.
    ReplaceContext(Vec<LogicConversationEntry>),
    /// End the execution with a structured runtime rejection.
    Reject {
        /// Safe reason.
        reason: String,
    },
    /// End the execution as cancelled.
    Cancel {
        /// Safe reason retained for harness diagnostics.
        reason: String,
    },
}

/// Logic command resolving a harness-local continuation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContinueProviderCommand {
    /// Pending continuation reference.
    pub continuation_reference: String,
    /// Explicit runtime decision.
    pub decision: LogicContinuationDecision,
}

/// Continuation business interface exposed only to harness service.
pub trait HarnessContinuationLogic {
    /// Resolves one pending proposal exactly once.
    ///
    /// Approved decisions issue a fresh provider request; they never claim to
    /// resume hidden provider reasoning.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionLogicError`] for unknown/duplicate continuations,
    /// invalid replacement context, poisoned state, or provider-data failure.
    fn continue_provider(
        &self,
        command: ContinueProviderCommand,
    ) -> Result<ExecuteProviderResult, ExecutionLogicError>;
}

/// Provider execution business failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionLogicError {
    /// Required command field or bound is invalid.
    InvalidCommand(String),
    /// Data operation failed.
    ExecutionDataUnavailable(String),
    /// Continuation was already resolved or never belonged to this harness.
    UnknownContinuation,
    /// Provider returned a continuation reference already pending.
    DuplicateContinuation,
    /// Harness-local continuation state could not be accessed safely.
    ExecutionStateUnavailable,
}

impl std::fmt::Display for ExecutionLogicError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCommand(message) => {
                write!(formatter, "provider command is invalid: {message}")
            }
            Self::ExecutionDataUnavailable(message) => {
                write!(formatter, "provider execution is unavailable: {message}")
            }
            Self::UnknownContinuation => {
                formatter.write_str("provider continuation is unknown or already resolved")
            }
            Self::DuplicateContinuation => {
                formatter.write_str("provider returned a duplicate pending continuation")
            }
            Self::ExecutionStateUnavailable => {
                formatter.write_str("provider continuation state is unavailable")
            }
        }
    }
}

impl std::error::Error for ExecutionLogicError {}

/// Provider execution business interface exposed only to harness service.
pub trait HarnessExecutionLogic {
    /// Validates and coordinates one approved provider execution.
    ///
    /// # Errors
    ///
    /// Returns logic-owned validation or data-availability errors.
    fn execute_provider(
        &self,
        command: ExecuteProviderCommand,
    ) -> Result<ExecuteProviderResult, ExecutionLogicError>;
}

impl<D> HarnessExecutionLogic for HarnessHealthManager<D>
where
    D: HarnessExecutionData,
{
    fn execute_provider(
        &self,
        command: ExecuteProviderCommand,
    ) -> Result<ExecuteProviderResult, ExecutionLogicError> {
        validate_command(&command)?;
        let pending_command = command.clone();
        let record = self
            .data
            .execute(to_data_query(command, false))
            .map_err(|error| map_data_error(&error))?;
        let events: Vec<_> = record.events.into_iter().map(map_event).collect();
        self.remember_pending(&pending_command, &events)?;
        Ok(ExecuteProviderResult { events })
    }
}

impl<D> HarnessContinuationLogic for HarnessHealthManager<D>
where
    D: HarnessExecutionData,
{
    fn continue_provider(
        &self,
        command: ContinueProviderCommand,
    ) -> Result<ExecuteProviderResult, ExecutionLogicError> {
        if command.continuation_reference.trim().is_empty() {
            return Err(ExecutionLogicError::InvalidCommand(
                "continuation reference is required".into(),
            ));
        }
        let pending = {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| ExecutionLogicError::ExecutionStateUnavailable)?;
            let selected = pending
                .remove(&command.continuation_reference)
                .ok_or(ExecutionLogicError::UnknownContinuation)?;
            for sibling in &selected.sibling_continuations {
                pending.remove(sibling);
            }
            selected
        };
        match command.decision {
            LogicContinuationDecision::Reject { reason } => Ok(ExecuteProviderResult {
                events: vec![LogicProviderEvent::RuntimeRejected { reason }],
            }),
            LogicContinuationDecision::Cancel { reason: _ } => Ok(ExecuteProviderResult {
                events: vec![LogicProviderEvent::Cancelled],
            }),
            LogicContinuationDecision::Continue | LogicContinuationDecision::ReplaceContext(_) => {
                let mut resumed = pending.command;
                if let LogicContinuationDecision::ReplaceContext(entries) = command.decision {
                    if entries.is_empty() || entries.len() > 256 {
                        return Err(ExecutionLogicError::InvalidCommand(
                            "replacement context must contain 1..=256 entries".into(),
                        ));
                    }
                    resumed.entries = entries;
                }
                validate_command(&resumed)?;
                let record = self
                    .data
                    .execute(to_data_query(resumed.clone(), true))
                    .map_err(|error| map_data_error(&error))?;
                let events: Vec<_> = record.events.into_iter().map(map_event).collect();
                self.remember_pending(&resumed, &events)?;
                Ok(ExecuteProviderResult { events })
            }
        }
    }
}

impl<D> HarnessHealthManager<D> {
    fn remember_pending(
        &self,
        command: &ExecuteProviderCommand,
        events: &[LogicProviderEvent],
    ) -> Result<(), ExecutionLogicError> {
        let continuations: Vec<_> = events
            .iter()
            .filter_map(|event| {
                if let LogicProviderEvent::ToolCallProposed {
                    continuation_reference,
                    ..
                } = event
                {
                    Some(continuation_reference.clone())
                } else {
                    None
                }
            })
            .collect();
        if continuations.is_empty() {
            return Ok(());
        }
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| ExecutionLogicError::ExecutionStateUnavailable)?;
        if continuations
            .iter()
            .any(|continuation| pending.contains_key(continuation))
        {
            return Err(ExecutionLogicError::DuplicateContinuation);
        }
        for continuation in &continuations {
            pending.insert(
                continuation.clone(),
                PendingProviderExecution {
                    command: command.clone(),
                    sibling_continuations: continuations
                        .iter()
                        .filter(|sibling| *sibling != continuation)
                        .cloned()
                        .collect(),
                },
            );
        }
        Ok(())
    }
}

fn validate_command(command: &ExecuteProviderCommand) -> Result<(), ExecutionLogicError> {
    if command.session_reference.trim().is_empty()
        || command.provider.trim().is_empty()
        || command.model.trim().is_empty()
        || command.authorization_grant.trim().is_empty()
        || command.cancellation_reference.trim().is_empty()
    {
        return Err(ExecutionLogicError::InvalidCommand(
            "session, provider, model, grant, and cancellation references are required".into(),
        ));
    }
    if command.entries.len() > 256 || command.options.len() > 64 {
        return Err(ExecutionLogicError::InvalidCommand(
            "entry or option bound exceeded".into(),
        ));
    }
    Ok(())
}

fn to_data_query(
    command: ExecuteProviderCommand,
    resumed_after_continuation: bool,
) -> HarnessExecutionDataQuery {
    HarnessExecutionDataQuery {
        provider: command.provider,
        model: command.model,
        entries: command.entries.into_iter().map(map_entry).collect(),
        options: command
            .options
            .into_iter()
            .map(|option| DataProviderOption {
                key: option.key,
                value: option.value,
            })
            .collect(),
        authorization_grant: command.authorization_grant,
        cancellation_reference: command.cancellation_reference,
        resumed_after_continuation,
    }
}

fn map_entry(entry: LogicConversationEntry) -> DataConversationEntry {
    match entry {
        LogicConversationEntry::System(text) => DataConversationEntry::System(text),
        LogicConversationEntry::User(text) => DataConversationEntry::User(text),
        LogicConversationEntry::Image {
            media_type,
            data_base64,
        } => DataConversationEntry::Image {
            media_type,
            data_base64,
        },
        LogicConversationEntry::Assistant(text) => DataConversationEntry::Assistant(text),
        LogicConversationEntry::ToolCall {
            call_id,
            tool,
            arguments_json,
        } => DataConversationEntry::ToolCall {
            call_id,
            tool,
            arguments_json,
        },
        LogicConversationEntry::ToolResult {
            call_id,
            content,
            truncated,
        } => DataConversationEntry::ToolResult {
            call_id,
            content,
            truncated,
        },
        LogicConversationEntry::ContextSummary {
            text,
            source_start,
            source_end,
        } => DataConversationEntry::ContextSummary {
            text,
            source_start,
            source_end,
        },
        LogicConversationEntry::Metadata { key, value_json } => {
            DataConversationEntry::Metadata { key, value_json }
        }
    }
}

fn map_event(event: DataProviderEvent) -> LogicProviderEvent {
    match event {
        DataProviderEvent::Started => LogicProviderEvent::Started,
        DataProviderEvent::TextDelta(text) => LogicProviderEvent::TextDelta(text),
        DataProviderEvent::ToolCallDelta {
            call_id,
            name_fragment,
            arguments_fragment,
        } => LogicProviderEvent::ToolCallDelta {
            call_id,
            name_fragment,
            arguments_fragment,
        },
        DataProviderEvent::ToolCallProposed {
            continuation_reference,
            call_id,
            tool,
            arguments_json,
        } => LogicProviderEvent::ToolCallProposed {
            continuation_reference,
            call_id,
            tool,
            arguments_json,
        },
        DataProviderEvent::Completed {
            finish_reason,
            usage,
            cost,
        } => LogicProviderEvent::Completed {
            finish_reason,
            usage: map_usage(usage),
            cost: cost.map(map_cost),
        },
        DataProviderEvent::Cancelled => LogicProviderEvent::Cancelled,
        DataProviderEvent::Failed {
            kind,
            message,
            retry,
        } => LogicProviderEvent::Failed {
            kind: map_failure_kind(kind),
            message,
            retry: map_retry(retry),
        },
    }
}

const fn map_usage(usage: DataUsageRecord) -> LogicUsage {
    LogicUsage {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cache_read_tokens: usage.cache_read_tokens,
        cache_write_tokens: usage.cache_write_tokens,
        reasoning_tokens: usage.reasoning_tokens,
        estimated: usage.estimated,
    }
}

fn map_cost(cost: DataCostMetadata) -> LogicCostMetadata {
    LogicCostMetadata {
        source: cost.source,
        version: cost.version,
        input_cost_micros: cost.input_cost_micros,
        output_cost_micros: cost.output_cost_micros,
        cache_read_cost_micros: cost.cache_read_cost_micros,
        cache_write_cost_micros: cost.cache_write_cost_micros,
        currency: cost.currency,
    }
}

const fn map_failure_kind(kind: DataProviderFailureKind) -> LogicProviderFailureKind {
    match kind {
        DataProviderFailureKind::MalformedToolArguments => {
            LogicProviderFailureKind::MalformedToolArguments
        }
        DataProviderFailureKind::Timeout => LogicProviderFailureKind::Timeout,
        DataProviderFailureKind::RateLimited => LogicProviderFailureKind::RateLimited,
        DataProviderFailureKind::PartialOutputFailure => {
            LogicProviderFailureKind::PartialOutputFailure
        }
        DataProviderFailureKind::Disconnected => LogicProviderFailureKind::Disconnected,
        DataProviderFailureKind::AuthenticationFailed => {
            LogicProviderFailureKind::AuthenticationFailed
        }
        DataProviderFailureKind::ProviderOverloaded => LogicProviderFailureKind::ProviderOverloaded,
        DataProviderFailureKind::InvalidRequest => LogicProviderFailureKind::InvalidRequest,
        DataProviderFailureKind::UnsupportedCapability => {
            LogicProviderFailureKind::UnsupportedCapability
        }
        DataProviderFailureKind::TransportFailure => LogicProviderFailureKind::TransportFailure,
        DataProviderFailureKind::AmbiguousDisconnect => {
            LogicProviderFailureKind::AmbiguousDisconnect
        }
        DataProviderFailureKind::UserCancellation => LogicProviderFailureKind::UserCancellation,
    }
}

const fn map_retry(retry: DataRetryClassification) -> LogicRetryClassification {
    match retry {
        DataRetryClassification::Never => LogicRetryClassification::Never,
        DataRetryClassification::Immediate => LogicRetryClassification::Immediate,
        DataRetryClassification::AfterMilliseconds(milliseconds) => {
            LogicRetryClassification::AfterMilliseconds(milliseconds)
        }
    }
}

fn map_data_error(error: &HarnessExecutionDataError) -> ExecutionLogicError {
    ExecutionLogicError::ExecutionDataUnavailable(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use agentmod_harness_data::execution::{HarnessExecutionData, HarnessExecutionRecord};

    use super::*;

    struct MockExecutionData {
        queries: Mutex<Vec<HarnessExecutionDataQuery>>,
    }

    struct MultiToolData {
        calls: AtomicUsize,
    }

    impl HarnessExecutionData for MockExecutionData {
        fn execute(
            &self,
            query: HarnessExecutionDataQuery,
        ) -> Result<HarnessExecutionRecord, HarnessExecutionDataError> {
            self.queries
                .lock()
                .expect("query lock is not poisoned")
                .push(query);
            Ok(HarnessExecutionRecord {
                events: vec![DataProviderEvent::Cancelled],
            })
        }
    }

    impl HarnessExecutionData for MultiToolData {
        fn execute(
            &self,
            _query: HarnessExecutionDataQuery,
        ) -> Result<HarnessExecutionRecord, HarnessExecutionDataError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(HarnessExecutionRecord {
                events: if call == 0 {
                    vec![
                        DataProviderEvent::ToolCallProposed {
                            continuation_reference: "continuation-1".into(),
                            call_id: "call-1".into(),
                            tool: "read_file".into(),
                            arguments_json: r#"{"path":"one"}"#.into(),
                        },
                        DataProviderEvent::ToolCallProposed {
                            continuation_reference: "continuation-2".into(),
                            call_id: "call-2".into(),
                            tool: "read_file".into(),
                            arguments_json: r#"{"path":"two"}"#.into(),
                        },
                    ]
                } else {
                    vec![DataProviderEvent::Cancelled]
                },
            })
        }
    }

    #[test]
    fn validates_maps_and_normalizes_execution() {
        let manager = HarnessHealthManager::new(MockExecutionData {
            queries: Mutex::new(Vec::new()),
        });
        let result = manager
            .execute_provider(command())
            .expect("execution result");
        assert_eq!(result.events, [LogicProviderEvent::Cancelled]);
        assert_eq!(
            manager
                .data
                .queries
                .lock()
                .expect("query lock is not poisoned")[0]
                .entries,
            [DataConversationEntry::User("hello".into())]
        );
    }

    #[test]
    fn rejects_missing_authorization_before_data_call() {
        let manager = HarnessHealthManager::new(MockExecutionData {
            queries: Mutex::new(Vec::new()),
        });
        let mut command = command();
        command.authorization_grant.clear();
        assert!(matches!(
            manager.execute_provider(command),
            Err(ExecutionLogicError::InvalidCommand(_))
        ));
        assert!(
            manager
                .data
                .queries
                .lock()
                .expect("query lock is not poisoned")
                .is_empty()
        );
    }

    #[test]
    fn resolving_one_batch_continuation_invalidates_its_siblings() {
        let manager = HarnessHealthManager::new(MultiToolData {
            calls: AtomicUsize::new(0),
        });
        manager
            .execute_provider(command())
            .expect("initial multi-tool request");
        manager
            .continue_provider(ContinueProviderCommand {
                continuation_reference: "continuation-1".into(),
                decision: LogicContinuationDecision::Continue,
            })
            .expect("resolve batch");
        assert_eq!(
            manager.continue_provider(ContinueProviderCommand {
                continuation_reference: "continuation-2".into(),
                decision: LogicContinuationDecision::Continue,
            }),
            Err(ExecutionLogicError::UnknownContinuation)
        );
    }

    fn command() -> ExecuteProviderCommand {
        ExecuteProviderCommand {
            session_reference: "session".into(),
            provider: "provider".into(),
            model: "model".into(),
            entries: vec![LogicConversationEntry::User("hello".into())],
            options: Vec::new(),
            authorization_grant: "grant".into(),
            cancellation_reference: "cancel".into(),
        }
    }
}
