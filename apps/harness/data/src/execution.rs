//! Business-facing provider execution dataset construction.

use agentmod_harness_dependency::execution::{
    DependencyConversationEntry, DependencyProviderEvent, DependencyProviderExecutionRequest,
    DependencyProviderFailureKind, DependencyProviderOption, DependencyRetryClassification,
    DependencyUsage, ProviderExecutionDependency, ProviderExecutionDependencyError,
};

use crate::HarnessHealthDataStore;

/// Data-owned projected conversation entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataConversationEntry {
    /// System instruction.
    System(String),
    /// User content.
    User(String),
    /// Visible assistant content.
    Assistant(String),
    /// Approved tool call.
    ToolCall {
        /// Call ID.
        call_id: String,
        /// Tool name.
        tool: String,
        /// JSON arguments.
        arguments_json: String,
    },
    /// Bounded tool result.
    ToolResult {
        /// Call ID.
        call_id: String,
        /// Visible content.
        content: String,
        /// Artifact overflow marker.
        truncated: bool,
    },
    /// Context summary.
    ContextSummary {
        /// Summary text.
        text: String,
        /// Source start.
        source_start: u64,
        /// Source end.
        source_end: u64,
    },
    /// Provider-visible metadata.
    Metadata {
        /// Metadata key.
        key: String,
        /// JSON value.
        value_json: String,
    },
}

/// Data-owned provider option.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataProviderOption {
    /// Option key.
    pub key: String,
    /// Textual option value.
    pub value: String,
}

/// Data-owned provider execution query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarnessExecutionDataQuery {
    /// Provider selection.
    pub provider: String,
    /// Model selection.
    pub model: String,
    /// Approved entries.
    pub entries: Vec<DataConversationEntry>,
    /// Approved options.
    pub options: Vec<DataProviderOption>,
    /// Runtime-issued authorization grant.
    pub authorization_grant: String,
    /// Cancellation reference.
    pub cancellation_reference: String,
    /// True only for a fresh request approved after a runtime continuation.
    pub resumed_after_continuation: bool,
}

/// Data-owned usage record.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DataUsageRecord {
    /// Input tokens.
    pub input_tokens: u64,
    /// Output tokens.
    pub output_tokens: u64,
    /// Cache reads.
    pub cache_read_tokens: u64,
    /// Cache writes.
    pub cache_write_tokens: u64,
}

/// Data-owned failure kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataProviderFailureKind {
    /// Malformed tool arguments.
    MalformedToolArguments,
    /// Provider timeout.
    Timeout,
    /// Provider rate limit.
    RateLimited,
    /// Failure after partial output.
    PartialOutputFailure,
    /// Provider disconnected.
    Disconnected,
}

/// Data-owned retry classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataRetryClassification {
    /// No retry.
    Never,
    /// Immediate retry.
    Immediate,
    /// Delayed retry.
    AfterMilliseconds(u64),
}

/// Data-owned normalized provider event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataProviderEvent {
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
        /// Argument fragment.
        arguments_fragment: String,
    },
    /// Complete tool-call proposal.
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
    /// Normal completion.
    Completed {
        /// Finish reason.
        finish_reason: String,
        /// Usage.
        usage: DataUsageRecord,
    },
    /// Cancelled request.
    Cancelled,
    /// Classified failure.
    Failed {
        /// Failure kind.
        kind: DataProviderFailureKind,
        /// Redacted message.
        message: String,
        /// Retry classification.
        retry: DataRetryClassification,
    },
}

/// Bounded provider execution business record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarnessExecutionRecord {
    /// Events in provider order.
    pub events: Vec<DataProviderEvent>,
}

/// Data-layer provider execution failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HarnessExecutionDataError {
    /// Translated dependency failure.
    DependencyUnavailable(String),
}

impl std::fmt::Display for HarnessExecutionDataError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DependencyUnavailable(message) => {
                write!(
                    formatter,
                    "provider execution data is unavailable: {message}"
                )
            }
        }
    }
}

impl std::error::Error for HarnessExecutionDataError {}

/// Business provider execution data interface consumed by harness logic.
pub trait HarnessExecutionData {
    /// Executes a provider dataset operation.
    ///
    /// # Errors
    ///
    /// Returns a translated data error without dependency-owned types.
    fn execute(
        &self,
        query: HarnessExecutionDataQuery,
    ) -> Result<HarnessExecutionRecord, HarnessExecutionDataError>;
}

impl<D> HarnessExecutionData for HarnessHealthDataStore<D>
where
    D: ProviderExecutionDependency,
{
    fn execute(
        &self,
        query: HarnessExecutionDataQuery,
    ) -> Result<HarnessExecutionRecord, HarnessExecutionDataError> {
        let request = to_dependency_request(query);
        let response = self
            .dependency
            .execute_provider(request)
            .map_err(|error| map_dependency_error(&error))?;
        Ok(HarnessExecutionRecord {
            events: response.events.into_iter().map(map_event).collect(),
        })
    }
}

fn to_dependency_request(query: HarnessExecutionDataQuery) -> DependencyProviderExecutionRequest {
    DependencyProviderExecutionRequest {
        provider_key: query.provider,
        model_key: query.model,
        entries: query.entries.into_iter().map(map_entry).collect(),
        options: query
            .options
            .into_iter()
            .map(|option| DependencyProviderOption {
                key: option.key,
                value: option.value,
            })
            .collect(),
        authorization_grant: query.authorization_grant,
        cancellation_reference: query.cancellation_reference,
        resumed_after_continuation: query.resumed_after_continuation,
    }
}

fn map_entry(entry: DataConversationEntry) -> DependencyConversationEntry {
    match entry {
        DataConversationEntry::System(text) => DependencyConversationEntry::System(text),
        DataConversationEntry::User(text) => DependencyConversationEntry::User(text),
        DataConversationEntry::Assistant(text) => DependencyConversationEntry::Assistant(text),
        DataConversationEntry::ToolCall {
            call_id,
            tool,
            arguments_json,
        } => DependencyConversationEntry::ToolCall {
            call_id,
            tool,
            arguments_json,
        },
        DataConversationEntry::ToolResult {
            call_id,
            content,
            truncated,
        } => DependencyConversationEntry::ToolResult {
            call_id,
            content,
            truncated,
        },
        DataConversationEntry::ContextSummary {
            text,
            source_start,
            source_end,
        } => DependencyConversationEntry::ContextSummary {
            text,
            source_start,
            source_end,
        },
        DataConversationEntry::Metadata { key, value_json } => {
            DependencyConversationEntry::Metadata { key, value_json }
        }
    }
}

fn map_event(event: DependencyProviderEvent) -> DataProviderEvent {
    match event {
        DependencyProviderEvent::Started => DataProviderEvent::Started,
        DependencyProviderEvent::TextDelta(text) => DataProviderEvent::TextDelta(text),
        DependencyProviderEvent::ToolCallDelta {
            call_id,
            name_fragment,
            arguments_fragment,
        } => DataProviderEvent::ToolCallDelta {
            call_id,
            name_fragment,
            arguments_fragment,
        },
        DependencyProviderEvent::ToolCallProposed {
            continuation_reference,
            call_id,
            tool,
            arguments_json,
        } => DataProviderEvent::ToolCallProposed {
            continuation_reference,
            call_id,
            tool,
            arguments_json,
        },
        DependencyProviderEvent::Completed {
            finish_reason,
            usage,
        } => DataProviderEvent::Completed {
            finish_reason,
            usage: map_usage(usage),
        },
        DependencyProviderEvent::Cancelled => DataProviderEvent::Cancelled,
        DependencyProviderEvent::Failed {
            kind,
            message,
            retry,
        } => DataProviderEvent::Failed {
            kind: map_failure_kind(kind),
            message,
            retry: map_retry(retry),
        },
    }
}

const fn map_usage(usage: DependencyUsage) -> DataUsageRecord {
    DataUsageRecord {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cache_read_tokens: usage.cache_read_tokens,
        cache_write_tokens: usage.cache_write_tokens,
    }
}

const fn map_failure_kind(kind: DependencyProviderFailureKind) -> DataProviderFailureKind {
    match kind {
        DependencyProviderFailureKind::MalformedToolArguments => {
            DataProviderFailureKind::MalformedToolArguments
        }
        DependencyProviderFailureKind::Timeout => DataProviderFailureKind::Timeout,
        DependencyProviderFailureKind::RateLimited => DataProviderFailureKind::RateLimited,
        DependencyProviderFailureKind::PartialOutputFailure => {
            DataProviderFailureKind::PartialOutputFailure
        }
        DependencyProviderFailureKind::Disconnected => DataProviderFailureKind::Disconnected,
    }
}

const fn map_retry(retry: DependencyRetryClassification) -> DataRetryClassification {
    match retry {
        DependencyRetryClassification::Never => DataRetryClassification::Never,
        DependencyRetryClassification::Immediate => DataRetryClassification::Immediate,
        DependencyRetryClassification::AfterMilliseconds(milliseconds) => {
            DataRetryClassification::AfterMilliseconds(milliseconds)
        }
    }
}

fn map_dependency_error(error: &ProviderExecutionDependencyError) -> HarnessExecutionDataError {
    HarnessExecutionDataError::DependencyUnavailable(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use agentmod_harness_dependency::execution::{
        DependencyProviderExecutionResponse, ProviderExecutionDependency,
    };

    use super::*;

    struct MockExecutionDependency {
        requests: Mutex<Vec<DependencyProviderExecutionRequest>>,
    }

    impl ProviderExecutionDependency for MockExecutionDependency {
        fn execute_provider(
            &self,
            request: DependencyProviderExecutionRequest,
        ) -> Result<DependencyProviderExecutionResponse, ProviderExecutionDependencyError> {
            self.requests
                .lock()
                .expect("request lock is not poisoned")
                .push(request);
            Ok(DependencyProviderExecutionResponse {
                events: vec![
                    DependencyProviderEvent::TextDelta("hello".into()),
                    DependencyProviderEvent::Completed {
                        finish_reason: "stop".into(),
                        usage: DependencyUsage {
                            input_tokens: 2,
                            output_tokens: 1,
                            cache_read_tokens: 0,
                            cache_write_tokens: 0,
                        },
                    },
                ],
            })
        }
    }

    #[test]
    fn explicitly_maps_query_and_dependency_events() {
        let store = HarnessHealthDataStore::new(MockExecutionDependency {
            requests: Mutex::new(Vec::new()),
        });
        let record = store
            .execute(HarnessExecutionDataQuery {
                provider: "mock".into(),
                model: "model".into(),
                entries: vec![DataConversationEntry::User("hi".into())],
                options: vec![DataProviderOption {
                    key: "temperature".into(),
                    value: "0".into(),
                }],
                authorization_grant: "grant".into(),
                cancellation_reference: "cancel".into(),
                resumed_after_continuation: false,
            })
            .expect("execution record");
        assert_eq!(
            record.events,
            [
                DataProviderEvent::TextDelta("hello".into()),
                DataProviderEvent::Completed {
                    finish_reason: "stop".into(),
                    usage: DataUsageRecord {
                        input_tokens: 2,
                        output_tokens: 1,
                        cache_read_tokens: 0,
                        cache_write_tokens: 0,
                    },
                }
            ]
        );
        assert_eq!(
            store
                .dependency
                .requests
                .lock()
                .expect("request lock is not poisoned")[0]
                .entries,
            [DependencyConversationEntry::User("hi".into())]
        );
    }
}
