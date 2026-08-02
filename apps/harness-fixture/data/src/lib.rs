//! Business-facing data assembly for the independent harness fixture.

use std::collections::BTreeSet;

use agentmod_harness_fixture_dependency::{
    FixtureCatalogRecord, FixtureConversationEntry, FixtureExecutionRequest,
    FixtureExecutionResponse, FixtureProviderCancellation, FixtureProviderEvent,
    FixtureProviderExecution, FixtureProviderOption,
};
use async_trait::async_trait;
use thiserror::Error;

/// Data-owned conversation entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FixtureDataEntry {
    /// System instruction.
    System(String),
    /// User text.
    User(String),
    /// Image input (unsupported by this fixture).
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

/// Data-owned provider option.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureDataOption {
    /// Option key.
    pub key: String,
    /// Textual value.
    pub value: String,
}

/// Data-owned execution query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureDataQuery {
    /// Provider selection.
    pub provider: String,
    /// Model selection.
    pub model: String,
    /// Projected conversation.
    pub entries: Vec<FixtureDataEntry>,
    /// Approved options.
    pub options: Vec<FixtureDataOption>,
    /// Authorization grant.
    pub authorization_grant: String,
    /// Cancellation reference.
    pub cancellation_reference: String,
    /// True for a resumed request.
    pub resumed_after_continuation: bool,
}

/// Data-owned usage.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FixtureDataUsage {
    /// Input tokens.
    pub input_tokens: u64,
    /// Output tokens.
    pub output_tokens: u64,
}

/// Data-owned provider event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FixtureDataEvent {
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
        usage: FixtureDataUsage,
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

/// Data-owned execution record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureDataRecord {
    /// Events in order.
    pub events: Vec<FixtureDataEvent>,
}

/// Data-owned health record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureHealthRecord {
    /// Ready provider count.
    pub ready_provider_count: u32,
    /// Capability names.
    pub capabilities: BTreeSet<String>,
}

/// Data failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum FixtureDataError {
    /// Translated dependency failure.
    #[error("fixture data is unavailable: {0}")]
    Unavailable(String),
}

/// Data-owned execution interface.
#[async_trait]
pub trait FixtureExecutionData: Send + Sync {
    /// Executes a provider dataset operation.
    ///
    /// # Errors
    ///
    /// Returns a translated data error.
    async fn execute(&self, query: FixtureDataQuery) -> Result<FixtureDataRecord, FixtureDataError>;
}

/// Data-owned cancellation interface.
#[async_trait]
pub trait FixtureCancellationData: Send + Sync {
    /// Requests cancellation of an in-flight exchange.
    ///
    /// # Errors
    ///
    /// Returns a translated data error.
    async fn cancel(&self, reference: &str) -> Result<bool, FixtureDataError>;
}

/// Data-owned catalog interface.
pub trait FixtureCatalogData: Send + Sync {
    /// Reads the bounded fixture catalog.
    ///
    /// # Errors
    ///
    /// Returns a translated data error.
    fn read_catalog(&self) -> Result<Vec<FixtureCatalogRecord>, FixtureDataError>;
}

/// Data-owned health interface.
pub trait FixtureHealthData: Send + Sync {
    /// Reads fixture health.
    ///
    /// # Errors
    ///
    /// Returns a translated data error.
    fn read_health(&self) -> Result<FixtureHealthRecord, FixtureDataError>;
}

/// Fixture data store backed by the deterministic provider dependency.
#[derive(Clone, Debug)]
pub struct FixtureDataStore<D> {
    dependency: D,
}

impl<D> FixtureDataStore<D> {
    /// Injects the provider dependency.
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

#[async_trait]
impl<D> FixtureExecutionData for FixtureDataStore<D>
where
    D: FixtureProviderExecution,
{
    async fn execute(&self, query: FixtureDataQuery) -> Result<FixtureDataRecord, FixtureDataError> {
        let response = self
            .dependency
            .execute(to_dependency_request(query))
            .await
            .map_err(|error| FixtureDataError::Unavailable(error.to_string()))?;
        Ok(to_data_record(response))
    }
}

#[async_trait]
impl<D> FixtureCancellationData for FixtureDataStore<D>
where
    D: FixtureProviderCancellation,
{
    async fn cancel(&self, reference: &str) -> Result<bool, FixtureDataError> {
        self.dependency
            .cancel(reference)
            .await
            .map_err(|error| FixtureDataError::Unavailable(error.to_string()))
    }
}

impl<D> FixtureCatalogData for FixtureDataStore<D>
where
    D: FixtureProviderExecution,
{
    fn read_catalog(&self) -> Result<Vec<FixtureCatalogRecord>, FixtureDataError> {
        Ok(vec![
            agentmod_harness_fixture_dependency::FixtureProviderCatalogDependency::catalog_record(),
        ])
    }
}

impl<D> FixtureHealthData for FixtureDataStore<D>
where
    D: FixtureProviderExecution,
{
    fn read_health(&self) -> Result<FixtureHealthRecord, FixtureDataError> {
        let capabilities =
            agentmod_harness_fixture_dependency::FixtureProviderCatalogDependency::capabilities();
        Ok(FixtureHealthRecord {
            ready_provider_count: 1,
            capabilities,
        })
    }
}

fn to_dependency_request(query: FixtureDataQuery) -> FixtureExecutionRequest {
    FixtureExecutionRequest {
        provider_key: query.provider,
        model_key: query.model,
        entries: query.entries.into_iter().map(map_entry).collect(),
        options: query
            .options
            .into_iter()
            .map(|option| FixtureProviderOption {
                key: option.key,
                value: option.value,
            })
            .collect(),
        authorization_grant: query.authorization_grant,
        cancellation_reference: query.cancellation_reference,
        resumed_after_continuation: query.resumed_after_continuation,
    }
}

fn map_entry(entry: FixtureDataEntry) -> FixtureConversationEntry {
    match entry {
        FixtureDataEntry::System(text) => FixtureConversationEntry::System(text),
        FixtureDataEntry::User(text) => FixtureConversationEntry::User(text),
        FixtureDataEntry::Image {
            media_type,
            data_base64,
        } => FixtureConversationEntry::Image {
            media_type,
            data_base64,
        },
        FixtureDataEntry::Assistant(text) => FixtureConversationEntry::Assistant(text),
        FixtureDataEntry::ToolCall {
            call_id,
            tool,
            arguments_json,
        } => FixtureConversationEntry::ToolCall {
            call_id,
            tool,
            arguments_json,
        },
        FixtureDataEntry::ToolResult {
            call_id,
            content,
            truncated,
        } => FixtureConversationEntry::ToolResult {
            call_id,
            content,
            truncated,
        },
        FixtureDataEntry::ContextSummary {
            text,
            source_start,
            source_end,
        } => FixtureConversationEntry::ContextSummary {
            text,
            source_start,
            source_end,
        },
        FixtureDataEntry::Metadata { key, value_json } => {
            FixtureConversationEntry::Metadata { key, value_json }
        }
    }
}

fn to_data_record(response: FixtureExecutionResponse) -> FixtureDataRecord {
    FixtureDataRecord {
        events: response.events.into_iter().map(map_event).collect(),
    }
}

fn map_event(event: FixtureProviderEvent) -> FixtureDataEvent {
    match event {
        FixtureProviderEvent::Started => FixtureDataEvent::Started,
        FixtureProviderEvent::TextDelta(text) => FixtureDataEvent::TextDelta(text),
        FixtureProviderEvent::ToolCallDelta {
            call_id,
            name_fragment,
            arguments_fragment,
        } => FixtureDataEvent::ToolCallDelta {
            call_id,
            name_fragment,
            arguments_fragment,
        },
        FixtureProviderEvent::ToolCallProposed {
            continuation_reference,
            call_id,
            tool,
            arguments_json,
        } => FixtureDataEvent::ToolCallProposed {
            continuation_reference,
            call_id,
            tool,
            arguments_json,
        },
        FixtureProviderEvent::Completed {
            finish_reason,
            usage,
        } => FixtureDataEvent::Completed {
            finish_reason,
            usage: FixtureDataUsage {
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
            },
        },
        FixtureProviderEvent::Cancelled => FixtureDataEvent::Cancelled,
        FixtureProviderEvent::Failed {
            code,
            message,
            retryable,
        } => FixtureDataEvent::Failed {
            code,
            message,
            retryable,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct MockProvider {
        requests: Mutex<Vec<FixtureExecutionRequest>>,
    }

    #[async_trait]
    impl FixtureProviderExecution for MockProvider {
        async fn execute(
            &self,
            request: FixtureExecutionRequest,
        ) -> Result<FixtureExecutionResponse, agentmod_harness_fixture_dependency::FixtureExecutionError>
        {
            self.requests
                .lock()
                .expect("request lock is not poisoned")
                .push(request);
            Ok(FixtureExecutionResponse {
                events: vec![FixtureProviderEvent::Cancelled],
            })
        }
    }

    #[tokio::test]
    async fn explicitly_maps_query_and_events() {
        let store = FixtureDataStore::new(MockProvider {
            requests: Mutex::new(Vec::new()),
        });
        let record = store
            .execute(FixtureDataQuery {
                provider: "fixture-deterministic".into(),
                model: "fixture-model".into(),
                entries: vec![FixtureDataEntry::User("hi".into())],
                options: vec![FixtureDataOption {
                    key: "fixture_scenario".into(),
                    value: "text".into(),
                }],
                authorization_grant: "grant".into(),
                cancellation_reference: "cancel".into(),
                resumed_after_continuation: false,
            })
            .await
            .expect("record");
        assert_eq!(record.events, [FixtureDataEvent::Cancelled]);
        assert_eq!(
            store
                .dependency
                .requests
                .lock()
                .expect("request lock is not poisoned")[0]
                .entries,
            [FixtureConversationEntry::User("hi".into())]
        );
    }
}
