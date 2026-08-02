//! Business behavior for the independent harness fixture.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use agentmod_harness_fixture_data::{
    FixtureCancellationData, FixtureCatalogData, FixtureDataEntry, FixtureDataEvent,
    FixtureDataOption, FixtureDataQuery, FixtureExecutionData, FixtureHealthData,
};
use async_trait::async_trait;
use thiserror::Error;

/// Logic-owned conversation entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FixtureLogicEntry {
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

/// Logic-owned provider option.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureLogicOption {
    /// Option key.
    pub key: String,
    /// Textual value.
    pub value: String,
}

/// Logic-owned execute command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureExecuteCommand {
    /// Session reference.
    pub session_reference: String,
    /// Provider selection.
    pub provider: String,
    /// Model selection.
    pub model: String,
    /// Projected conversation.
    pub entries: Vec<FixtureLogicEntry>,
    /// Approved options.
    pub options: Vec<FixtureLogicOption>,
    /// Authorization grant.
    pub authorization_grant: String,
    /// Cancellation reference.
    pub cancellation_reference: String,
}

/// Logic-owned usage.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FixtureLogicUsage {
    /// Input tokens.
    pub input_tokens: u64,
    /// Output tokens.
    pub output_tokens: u64,
}

/// Logic-owned provider event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FixtureLogicEvent {
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
        usage: FixtureLogicUsage,
    },
    /// Cancelled request.
    Cancelled,
    /// Runtime rejected the proposed continuation.
    RuntimeRejected {
        /// Safe reason.
        reason: String,
    },
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

/// Logic-owned execution result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureExecuteResult {
    /// Events in order.
    pub events: Vec<FixtureLogicEvent>,
}

/// Logic-owned continuation decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FixtureContinuationDecision {
    /// Continue with the prior approved context.
    Continue,
    /// Continue with replacement context.
    ReplaceContext(Vec<FixtureLogicEntry>),
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

/// Logic-owned continuation command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureContinueCommand {
    /// Pending continuation reference.
    pub continuation_reference: String,
    /// Explicit runtime decision.
    pub decision: FixtureContinuationDecision,
}

/// Logic failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum FixtureLogicError {
    /// Required command field or bound is invalid.
    #[error("fixture command is invalid: {0}")]
    InvalidCommand(String),
    /// Data operation failed.
    #[error("fixture data is unavailable: {0}")]
    DataUnavailable(String),
    /// Continuation is unknown or already resolved.
    #[error("fixture continuation is unknown or already resolved")]
    UnknownContinuation,
    /// Continuation state could not be accessed.
    #[error("fixture continuation state is unavailable")]
    StateUnavailable,
}

/// Logic-owned health result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureHealthResult {
    /// Ready provider count.
    pub ready_provider_count: u32,
    /// Capability names.
    pub capabilities: Vec<String>,
}

/// Logic-owned catalog result.
#[allow(clippy::struct_excessive_bools, reason = "capability flags are the catalog contract")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureCatalogResult {
    /// Provider ID.
    pub id: String,
    /// Adapter version.
    pub version: String,
    /// Model IDs.
    pub models: Vec<String>,
    /// Capability names.
    pub capabilities: Vec<String>,
    /// Tool-call support.
    pub tool_support: bool,
    /// Image support.
    pub image_support: bool,
    /// Structured-output support.
    pub structured_output_support: bool,
    /// Streaming support.
    pub streaming_support: bool,
    /// Pricing source.
    pub pricing_source: String,
    /// Whether the provider accepts work.
    pub available: bool,
}

/// Fixture execution business interface.
#[async_trait]
pub trait FixtureExecutionLogic: Send + Sync {
    /// Executes one deterministic provider request.
    ///
    /// # Errors
    ///
    /// Returns logic-owned validation or data errors.
    async fn execute_provider(
        &self,
        command: FixtureExecuteCommand,
    ) -> Result<FixtureExecuteResult, FixtureLogicError>;

    /// Resolves one pending proposal exactly once.
    ///
    /// # Errors
    ///
    /// Returns logic-owned errors for unknown continuations or data failures.
    async fn continue_provider(
        &self,
        command: FixtureContinueCommand,
    ) -> Result<FixtureExecuteResult, FixtureLogicError>;

    /// Requests cancellation of one in-flight exchange.
    ///
    /// # Errors
    ///
    /// Returns logic-owned errors.
    async fn cancel_provider(
        &self,
        cancellation_reference: &str,
    ) -> Result<bool, FixtureLogicError>;
}

/// Fixture health business interface.
pub trait FixtureHealthLogic: Send + Sync {
    /// Evaluates fixture health.
    ///
    /// # Errors
    ///
    /// Returns logic-owned errors.
    fn inspect_health(&self) -> Result<FixtureHealthResult, FixtureLogicError>;
}

/// Fixture catalog business interface.
pub trait FixtureCatalogLogic: Send + Sync {
    /// Reads the bounded fixture catalog.
    ///
    /// # Errors
    ///
    /// Returns logic-owned errors.
    fn inspect_catalog(&self) -> Result<Vec<FixtureCatalogResult>, FixtureLogicError>;
}

#[derive(Clone, Debug)]
struct PendingFixtureExecution {
    command: FixtureExecuteCommand,
}

/// Fixture logic manager backed by one data store.
#[derive(Clone, Debug)]
pub struct FixtureLogicManager<D> {
    data: D,
    pending: Arc<Mutex<BTreeMap<String, PendingFixtureExecution>>>,
}

impl<D> FixtureLogicManager<D> {
    /// Injects the fixture data implementation.
    #[must_use]
    pub fn new(data: D) -> Self {
        Self {
            data,
            pending: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

#[async_trait]
impl<D> FixtureExecutionLogic for FixtureLogicManager<D>
where
    D: FixtureExecutionData + FixtureCancellationData,
{
    async fn execute_provider(
        &self,
        command: FixtureExecuteCommand,
    ) -> Result<FixtureExecuteResult, FixtureLogicError> {
        validate_command(&command)?;
        let pending_command = command.clone();
        let record = self
            .data
            .execute(to_data_query(command, false))
            .await
            .map_err(|error| FixtureLogicError::DataUnavailable(error.to_string()))?;
        let events: Vec<_> = record.events.into_iter().map(map_event).collect();
        remember_pending(&self.pending, &pending_command, &events)?;
        Ok(FixtureExecuteResult { events })
    }

    async fn continue_provider(
        &self,
        command: FixtureContinueCommand,
    ) -> Result<FixtureExecuteResult, FixtureLogicError> {
        if command.continuation_reference.trim().is_empty() {
            return Err(FixtureLogicError::InvalidCommand(
                "continuation reference is required".into(),
            ));
        }
        let pending = {
            let mut pending = self.pending.lock().map_err(|_| {
                FixtureLogicError::StateUnavailable
            })?;
            pending
                .remove(&command.continuation_reference)
                .ok_or(FixtureLogicError::UnknownContinuation)?
        };
        match command.decision {
            FixtureContinuationDecision::Reject { reason } => Ok(FixtureExecuteResult {
                events: vec![FixtureLogicEvent::RuntimeRejected { reason }],
            }),
            FixtureContinuationDecision::Cancel { reason: _ } => Ok(FixtureExecuteResult {
                events: vec![FixtureLogicEvent::Cancelled],
            }),
            FixtureContinuationDecision::Continue | FixtureContinuationDecision::ReplaceContext(_) => {
                let mut resumed = pending.command;
                if let FixtureContinuationDecision::ReplaceContext(entries) = command.decision {
                    if entries.is_empty() || entries.len() > 256 {
                        return Err(FixtureLogicError::InvalidCommand(
                            "replacement context must contain 1..=256 entries".into(),
                        ));
                    }
                    resumed.entries = entries;
                }
                validate_command(&resumed)?;
                let record = self
                    .data
                    .execute(to_data_query(resumed.clone(), true))
                    .await
                    .map_err(|error| FixtureLogicError::DataUnavailable(error.to_string()))?;
                let events: Vec<_> = record.events.into_iter().map(map_event).collect();
                remember_pending(&self.pending, &resumed, &events)?;
                Ok(FixtureExecuteResult { events })
            }
        }
    }

    async fn cancel_provider(
        &self,
        cancellation_reference: &str,
    ) -> Result<bool, FixtureLogicError> {
        if cancellation_reference.trim().is_empty() {
            return Err(FixtureLogicError::InvalidCommand(
                "cancellation reference is required".into(),
            ));
        }
        self.data
            .cancel(cancellation_reference)
            .await
            .map_err(|error| FixtureLogicError::DataUnavailable(error.to_string()))
    }
}

impl<D> FixtureHealthLogic for FixtureLogicManager<D>
where
    D: FixtureHealthData,
{
    fn inspect_health(&self) -> Result<FixtureHealthResult, FixtureLogicError> {
        let record = self
            .data
            .read_health()
            .map_err(|error| FixtureLogicError::DataUnavailable(error.to_string()))?;
        Ok(FixtureHealthResult {
            ready_provider_count: record.ready_provider_count,
            capabilities: record.capabilities.into_iter().collect(),
        })
    }
}

impl<D> FixtureCatalogLogic for FixtureLogicManager<D>
where
    D: FixtureCatalogData,
{
    fn inspect_catalog(&self) -> Result<Vec<FixtureCatalogResult>, FixtureLogicError> {
        let records = self
            .data
            .read_catalog()
            .map_err(|error| FixtureLogicError::DataUnavailable(error.to_string()))?;
        Ok(records
            .into_iter()
            .map(|record| FixtureCatalogResult {
                id: record.id,
                version: record.version,
                models: record.models,
                capabilities: record.capabilities.into_iter().collect(),
                tool_support: record.tool_support,
                image_support: record.image_support,
                structured_output_support: record.structured_output_support,
                streaming_support: record.streaming_support,
                pricing_source: record.pricing_source,
                available: record.available,
            })
            .collect())
    }
}

fn validate_command(command: &FixtureExecuteCommand) -> Result<(), FixtureLogicError> {
    if command.session_reference.trim().is_empty()
        || command.provider.trim().is_empty()
        || command.model.trim().is_empty()
        || command.authorization_grant.trim().is_empty()
        || command.cancellation_reference.trim().is_empty()
    {
        return Err(FixtureLogicError::InvalidCommand(
            "session, provider, model, grant, and cancellation references are required".into(),
        ));
    }
    if command.entries.len() > 256 || command.options.len() > 64 {
        return Err(FixtureLogicError::InvalidCommand(
            "entry or option bound exceeded".into(),
        ));
    }
    Ok(())
}

fn to_data_query(
    command: FixtureExecuteCommand,
    resumed_after_continuation: bool,
) -> FixtureDataQuery {
    FixtureDataQuery {
        provider: command.provider,
        model: command.model,
        entries: command.entries.into_iter().map(map_entry).collect(),
        options: command
            .options
            .into_iter()
            .map(|option| FixtureDataOption {
                key: option.key,
                value: option.value,
            })
            .collect(),
        authorization_grant: command.authorization_grant,
        cancellation_reference: command.cancellation_reference,
        resumed_after_continuation,
    }
}

fn map_entry(entry: FixtureLogicEntry) -> FixtureDataEntry {
    match entry {
        FixtureLogicEntry::System(text) => FixtureDataEntry::System(text),
        FixtureLogicEntry::User(text) => FixtureDataEntry::User(text),
        FixtureLogicEntry::Image {
            media_type,
            data_base64,
        } => FixtureDataEntry::Image {
            media_type,
            data_base64,
        },
        FixtureLogicEntry::Assistant(text) => FixtureDataEntry::Assistant(text),
        FixtureLogicEntry::ToolCall {
            call_id,
            tool,
            arguments_json,
        } => FixtureDataEntry::ToolCall {
            call_id,
            tool,
            arguments_json,
        },
        FixtureLogicEntry::ToolResult {
            call_id,
            content,
            truncated,
        } => FixtureDataEntry::ToolResult {
            call_id,
            content,
            truncated,
        },
        FixtureLogicEntry::ContextSummary {
            text,
            source_start,
            source_end,
        } => FixtureDataEntry::ContextSummary {
            text,
            source_start,
            source_end,
        },
        FixtureLogicEntry::Metadata { key, value_json } => {
            FixtureDataEntry::Metadata { key, value_json }
        }
    }
}

fn map_event(event: FixtureDataEvent) -> FixtureLogicEvent {
    match event {
        FixtureDataEvent::Started => FixtureLogicEvent::Started,
        FixtureDataEvent::TextDelta(text) => FixtureLogicEvent::TextDelta(text),
        FixtureDataEvent::ToolCallDelta {
            call_id,
            name_fragment,
            arguments_fragment,
        } => FixtureLogicEvent::ToolCallDelta {
            call_id,
            name_fragment,
            arguments_fragment,
        },
        FixtureDataEvent::ToolCallProposed {
            continuation_reference,
            call_id,
            tool,
            arguments_json,
        } => FixtureLogicEvent::ToolCallProposed {
            continuation_reference,
            call_id,
            tool,
            arguments_json,
        },
        FixtureDataEvent::Completed {
            finish_reason,
            usage,
        } => FixtureLogicEvent::Completed {
            finish_reason,
            usage: FixtureLogicUsage {
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
            },
        },
        FixtureDataEvent::Cancelled => FixtureLogicEvent::Cancelled,
        FixtureDataEvent::Failed {
            code,
            message,
            retryable,
        } => FixtureLogicEvent::Failed {
            code,
            message,
            retryable,
        },
    }
}

fn remember_pending(
    pending: &Arc<Mutex<BTreeMap<String, PendingFixtureExecution>>>,
    command: &FixtureExecuteCommand,
    events: &[FixtureLogicEvent],
) -> Result<(), FixtureLogicError> {
    let continuations: Vec<_> = events
        .iter()
        .filter_map(|event| {
            if let FixtureLogicEvent::ToolCallProposed {
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
    let mut pending = pending
        .lock()
        .map_err(|_| FixtureLogicError::StateUnavailable)?;
    if continuations
        .iter()
        .any(|continuation| pending.contains_key(continuation))
    {
        return Err(FixtureLogicError::InvalidCommand(
            "fixture returned a duplicate pending continuation".into(),
        ));
    }
    for continuation in continuations {
        pending.insert(
            continuation,
            PendingFixtureExecution {
                command: command.clone(),
            },
        );
    }
    Ok(())
}
