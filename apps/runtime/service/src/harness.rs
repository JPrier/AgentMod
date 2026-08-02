//! Runtime service endpoints for provider execution through the native harness.
#![allow(
    missing_docs,
    reason = "service-local provider records are self-describing"
)]
use agentmod_runtime_logic::harness as logic;
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub enum ServiceProviderEntry {
    System(String),
    User(String),
    Assistant(String),
    ToolCall {
        call_id: String,
        tool: String,
        arguments: Value,
    },
    ToolResult {
        call_id: String,
        content: String,
        truncated: bool,
    },
    Summary {
        text: String,
        start: u64,
        end: u64,
    },
    Metadata {
        key: String,
        value: Value,
    },
}
#[derive(Clone, Debug, PartialEq)]
pub struct ServiceExecuteProviderRequest {
    pub harness: String,
    pub session_id: String,
    pub provider: String,
    pub model: String,
    pub entries: Vec<ServiceProviderEntry>,
    pub options: Value,
    pub cancellation_id: String,
    pub style: String,
    pub workspace: String,
}
#[derive(Clone, Debug, PartialEq)]
pub enum ServiceProviderDecision {
    Continue,
    Replace(Vec<ServiceProviderEntry>),
    Reject(String),
    Cancel(String),
}
#[derive(Clone, Debug, PartialEq)]
pub enum ServiceProviderEvent {
    Started,
    Text(String),
    ToolDelta {
        call_id: String,
        name: String,
        arguments: String,
    },
    ToolProposed {
        continuation_id: String,
        call_id: String,
        tool: String,
        arguments: Value,
    },
    Completed {
        reason: String,
        input_tokens: u64,
        output_tokens: u64,
        reasoning_tokens: u64,
        estimated: bool,
        cost_micros: u64,
    },
    Cancelled,
    Failed {
        code: String,
        message: String,
        retryable: bool,
    },
}

/// Harness provider result event carried back through runtime service.
#[async_trait]
pub trait ProviderServicePort: Send + Sync {
    async fn execute(
        &self,
        request: ServiceExecuteProviderRequest,
    ) -> Result<Vec<ServiceProviderEvent>, ProviderServiceError>;
    async fn continue_execution(
        &self,
        harness: String,
        id: String,
        decision: ServiceProviderDecision,
    ) -> Result<Vec<ServiceProviderEvent>, ProviderServiceError>;
    async fn cancel(
        &self,
        harness: String,
        id: String,
    ) -> Result<Vec<ServiceProviderEvent>, ProviderServiceError>;
}
#[derive(Clone)]
pub struct ProviderService<L> {
    logic: L,
}
impl<L> ProviderService<L> {
    #[must_use]
    pub const fn new(logic: L) -> Self {
        Self { logic }
    }
}
#[async_trait]
impl<L: logic::ProviderExecutionPort> ProviderServicePort for ProviderService<L> {
    async fn execute(
        &self,
        r: ServiceExecuteProviderRequest,
    ) -> Result<Vec<ServiceProviderEvent>, ProviderServiceError> {
        self.logic
            .execute(logic::ExecuteProviderCommand {
                harness: r.harness,
                session_id: r.session_id,
                provider: r.provider,
                model: r.model,
                entries: r.entries.into_iter().map(map_entry).collect(),
                options: r.options,
                cancellation_id: r.cancellation_id,
                style: r.style,
                workspace: r.workspace,
            })
            .await
            .map(|v| v.into_iter().map(map_event).collect())
            .map_err(map_error)
    }
    async fn continue_execution(
        &self,
        harness: String,
        id: String,
        d: ServiceProviderDecision,
    ) -> Result<Vec<ServiceProviderEvent>, ProviderServiceError> {
        self.logic
            .continue_execution(
                harness,
                id,
                match d {
                    ServiceProviderDecision::Continue => logic::ProviderDecision::Continue,
                    ServiceProviderDecision::Replace(v) => {
                        logic::ProviderDecision::Replace(v.into_iter().map(map_entry).collect())
                    }
                    ServiceProviderDecision::Reject(v) => logic::ProviderDecision::Reject(v),
                    ServiceProviderDecision::Cancel(v) => logic::ProviderDecision::Cancel(v),
                },
            )
            .await
            .map(|v| v.into_iter().map(map_event).collect())
            .map_err(map_error)
    }
    async fn cancel(
        &self,
        harness: String,
        id: String,
    ) -> Result<Vec<ServiceProviderEvent>, ProviderServiceError> {
        self.logic
            .cancel(harness, id)
            .await
            .map(|v| v.into_iter().map(map_event).collect())
            .map_err(map_error)
    }
}
fn map_entry(v: ServiceProviderEntry) -> logic::ProviderEntry {
    match v {
        ServiceProviderEntry::System(v) => logic::ProviderEntry::System(v),
        ServiceProviderEntry::User(v) => logic::ProviderEntry::User(v),
        ServiceProviderEntry::Assistant(v) => logic::ProviderEntry::Assistant(v),
        ServiceProviderEntry::ToolCall {
            call_id,
            tool,
            arguments,
        } => logic::ProviderEntry::ToolCall {
            call_id,
            tool,
            arguments,
        },
        ServiceProviderEntry::ToolResult {
            call_id,
            content,
            truncated,
        } => logic::ProviderEntry::ToolResult {
            call_id,
            content,
            truncated,
        },
        ServiceProviderEntry::Summary { text, start, end } => {
            logic::ProviderEntry::Summary { text, start, end }
        }
        ServiceProviderEntry::Metadata { key, value } => {
            logic::ProviderEntry::Metadata { key, value }
        }
    }
}
fn map_event(v: logic::ProviderEvent) -> ServiceProviderEvent {
    match v {
        logic::ProviderEvent::Started => ServiceProviderEvent::Started,
        logic::ProviderEvent::Text(v) => ServiceProviderEvent::Text(v),
        logic::ProviderEvent::ToolDelta {
            call_id,
            name,
            arguments,
        } => ServiceProviderEvent::ToolDelta {
            call_id,
            name,
            arguments,
        },
        logic::ProviderEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        } => ServiceProviderEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        },
        logic::ProviderEvent::Completed {
            reason,
            input_tokens,
            output_tokens,
            reasoning_tokens,
            estimated,
            cost_micros,
        } => ServiceProviderEvent::Completed {
            reason,
            input_tokens,
            output_tokens,
            reasoning_tokens,
            estimated,
            cost_micros,
        },
        logic::ProviderEvent::Cancelled => ServiceProviderEvent::Cancelled,
        logic::ProviderEvent::Failed {
            code,
            message,
            retryable,
        } => ServiceProviderEvent::Failed {
            code,
            message,
            retryable,
        },
    }
}
#[allow(clippy::needless_pass_by_value)]
fn map_error(v: logic::ProviderExecutionError) -> ProviderServiceError {
    match v {
        logic::ProviderExecutionError::Invalid => ProviderServiceError::Invalid,
        logic::ProviderExecutionError::Unavailable => ProviderServiceError::Unavailable,
        logic::ProviderExecutionError::ApprovalRequired(_) => {
            ProviderServiceError::ApprovalRequired
        }
        logic::ProviderExecutionError::Rejected(_)
        | logic::ProviderExecutionError::Cancelled(_)
        | logic::ProviderExecutionError::UnsupportedDecision
        | logic::ProviderExecutionError::InvalidInterceptionReplacement
        | logic::ProviderExecutionError::Harness { .. } => ProviderServiceError::Rejected,
    }
}
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ProviderServiceError {
    #[error("invalid provider request")]
    Invalid,
    #[error("harness unavailable")]
    Unavailable,
    #[error("provider request requires approval")]
    ApprovalRequired,
    #[error("harness rejected provider request")]
    Rejected,
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct MockLogic {
        executed: Mutex<Vec<logic::ExecuteProviderCommand>>,
    }

    #[async_trait]
    impl logic::ProviderExecutionPort for MockLogic {
        async fn execute(
            &self,
            command: logic::ExecuteProviderCommand,
        ) -> Result<Vec<logic::ProviderEvent>, logic::ProviderExecutionError> {
            self.executed.lock().expect("executed").push(command);
            Ok(vec![
                logic::ProviderEvent::Started,
                logic::ProviderEvent::Text("done".into()),
                logic::ProviderEvent::Completed {
                    reason: "stop".into(),
                    input_tokens: 2,
                    output_tokens: 1,
                    reasoning_tokens: 0,
                    estimated: false,
                    cost_micros: 0,
                },
            ])
        }

        async fn continue_execution(
            &self,
            _harness: String,
            _id: String,
            _decision: logic::ProviderDecision,
        ) -> Result<Vec<logic::ProviderEvent>, logic::ProviderExecutionError> {
            Ok(vec![])
        }

        async fn cancel(
            &self,
            _harness: String,
            _id: String,
        ) -> Result<Vec<logic::ProviderEvent>, logic::ProviderExecutionError> {
            Ok(vec![logic::ProviderEvent::Cancelled])
        }
    }

    #[tokio::test]
    async fn maps_service_types_without_forwarding_protocol_or_grant_types() {
        let service = ProviderService::new(MockLogic {
            executed: Mutex::new(Vec::new()),
        });
        let response = service
            .execute(ServiceExecuteProviderRequest {
                harness: "native".into(),
                session_id: "session".into(),
                provider: "deterministic-mock".into(),
                model: "mock-model".into(),
                entries: vec![ServiceProviderEntry::User("hello".into())],
                options: serde_json::json!({"temperature": 0}),
                cancellation_id: "cancel".into(),
                style: "persistent-chat".into(),
                workspace: "repo".into(),
            })
            .await
            .expect("mapped execution");
        assert_eq!(
            response,
            vec![
                ServiceProviderEvent::Started,
                ServiceProviderEvent::Text("done".into()),
                ServiceProviderEvent::Completed {
                    reason: "stop".into(),
                    input_tokens: 2,
                    output_tokens: 1,
                    reasoning_tokens: 0,
                    estimated: false,
                    cost_micros: 0,
                }
            ]
        );
        let observed = service.logic.executed.lock().expect("executed");
        assert_eq!(observed.len(), 1);
        assert!(matches!(
            observed[0].entries.as_slice(),
            [logic::ProviderEntry::User(value)] if value == "hello"
        ));
    }

    #[tokio::test]
    async fn maps_cancellation_result() {
        let service = ProviderService::new(MockLogic {
            executed: Mutex::new(Vec::new()),
        });
        assert_eq!(
            service
                .cancel("native".into(), "cancel".into())
                .await
                .expect("cancel"),
            vec![ServiceProviderEvent::Cancelled]
        );
    }
}
