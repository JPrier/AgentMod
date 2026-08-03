//! Runtime-owned provider interception and harness execution coordination.
#![allow(
    missing_docs,
    reason = "logic-local provider records are intentionally boundary-specific"
)]

use std::sync::Arc;

use agentmod_event_pipeline::{ActionCapabilities, BlockingPipeline};
use agentmod_primitives::ContentHash;
use agentmod_runtime_data::harness as data;
use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

use crate::{
    action::{ActionProposal, ConsequentialAction, ModelRequestAction, ProposalId},
    interception::{InterceptionOutcome, InterceptorAuditStep, intercept_action},
    permission::PermissionPolicy,
};

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ProviderEntry {
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
pub struct ExecuteProviderCommand {
    pub harness: String,
    pub session_id: String,
    pub provider: String,
    pub model: String,
    pub entries: Vec<ProviderEntry>,
    pub options: Value,
    pub cancellation_id: String,
    pub style: String,
    pub workspace: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ProviderDecision {
    Continue,
    Replace(Vec<ProviderEntry>),
    Reject(String),
    Cancel(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum ProviderEvent {
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

pub struct ProviderEventStream {
    data: data::HarnessDataEventStream,
}

impl ProviderEventStream {
    pub async fn next(&mut self) -> Option<Result<ProviderEvent, ProviderExecutionError>> {
        self.data.next().await.map(|result| {
            result
                .map(map_event)
                .map_err(|_| ProviderExecutionError::Unavailable)
        })
    }
}

#[async_trait]
pub trait ProviderExecutionPort: Send + Sync {
    async fn execute(
        &self,
        command: ExecuteProviderCommand,
    ) -> Result<Vec<ProviderEvent>, ProviderExecutionError>;

    async fn continue_execution(
        &self,
        harness: String,
        id: String,
        decision: ProviderDecision,
    ) -> Result<Vec<ProviderEvent>, ProviderExecutionError>;

    async fn cancel(
        &self,
        harness: String,
        id: String,
    ) -> Result<Vec<ProviderEvent>, ProviderExecutionError>;
}

#[derive(Clone)]
pub struct ProviderExecutionPolicy {
    pub style_pipeline: Arc<BlockingPipeline<ActionProposal>>,
    pub plugin_pipeline: Arc<BlockingPipeline<ActionProposal>>,
    pub user_policy: PermissionPolicy,
    pub mandatory_policy: PermissionPolicy,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AuthorizedProviderRequest {
    pub original: ActionProposal,
    pub executable: ActionProposal,
    pub interceptor_audit: Vec<InterceptorAuditStep>,
    pub session_id: String,
    pub entries: Vec<ProviderEntry>,
    pub cancellation_id: String,
    grant_binding: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PreparedProviderRequest {
    pub original: ActionProposal,
    session_id: String,
    entries: Vec<ProviderEntry>,
    cancellation_id: String,
}

#[derive(Clone)]
pub struct ProviderExecutionLogic<D> {
    data: D,
    policy: ProviderExecutionPolicy,
}

impl<D> ProviderExecutionLogic<D> {
    #[must_use]
    pub const fn new(data: D, policy: ProviderExecutionPolicy) -> Self {
        Self { data, policy }
    }

    /// Builds the immutable provider proposal without evaluating policy or
    /// performing the provider call.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderExecutionError`] for invalid input or serialization.
    pub fn prepare(
        &self,
        command: ExecuteProviderCommand,
    ) -> Result<PreparedProviderRequest, ProviderExecutionError> {
        validate_execute(&command)?;
        let projection_bytes =
            serde_json::to_vec(&command.entries).map_err(|_| ProviderExecutionError::Invalid)?;
        let original = ActionProposal {
            id: ProposalId(format!("model-request:{}", command.cancellation_id)),
            action: ConsequentialAction::ModelRequest(ModelRequestAction {
                harness: command.harness,
                provider: command.provider,
                model: command.model,
                projection_hash: ContentHash::digest(&projection_bytes),
                options: command.options,
            }),
            style: command.style,
            workspace: command.workspace,
            origin: "runtime".into(),
        };
        Ok(PreparedProviderRequest {
            original,
            session_id: command.session_id,
            entries: command.entries,
            cancellation_id: command.cancellation_id,
        })
    }

    /// Evaluates all blocking stages and policies for a previously prepared,
    /// auditable proposal.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderExecutionError`] for any non-approved terminal policy
    /// outcome or invalid interceptor replacement.
    pub async fn authorize_prepared(
        &self,
        prepared: PreparedProviderRequest,
    ) -> Result<AuthorizedProviderRequest, ProviderExecutionError> {
        let PreparedProviderRequest {
            original,
            session_id,
            entries,
            cancellation_id,
        } = prepared;
        let result = intercept_action(
            original.clone(),
            &self.policy.style_pipeline,
            &self.policy.plugin_pipeline,
            ActionCapabilities::all(),
            &self.policy.user_policy,
            &self.policy.mandatory_policy,
        )
        .await;
        let executable = match result.outcome {
            InterceptionOutcome::Approved { executable, .. } => executable,
            InterceptionOutcome::RequireApproval { reason, .. } => {
                return Err(ProviderExecutionError::ApprovalRequired(reason));
            }
            InterceptionOutcome::Rejected { reason } => {
                return Err(ProviderExecutionError::Rejected(reason));
            }
            InterceptionOutcome::Cancelled { reason } => {
                return Err(ProviderExecutionError::Cancelled(reason));
            }
            InterceptionOutcome::Deferred { .. }
            | InterceptionOutcome::Forked { .. }
            | InterceptionOutcome::Aborted { .. } => {
                return Err(ProviderExecutionError::UnsupportedDecision);
            }
        };
        let grant_binding = executable
            .digest()
            .map_err(|_| ProviderExecutionError::Invalid)?
            .to_hex();
        let ConsequentialAction::ModelRequest(action) = &executable.action else {
            return Err(ProviderExecutionError::InvalidInterceptionReplacement);
        };
        let ConsequentialAction::ModelRequest(original_action) = &original.action else {
            return Err(ProviderExecutionError::InvalidInterceptionReplacement);
        };
        let projection_bytes =
            serde_json::to_vec(&entries).map_err(|_| ProviderExecutionError::Invalid)?;
        if action.projection_hash != ContentHash::digest(&projection_bytes)
            || action.harness != original_action.harness
        {
            return Err(ProviderExecutionError::InvalidInterceptionReplacement);
        }
        Ok(AuthorizedProviderRequest {
            original,
            executable,
            interceptor_audit: result.audit,
            session_id,
            entries,
            cancellation_id,
            grant_binding,
        })
    }

    /// Prepares and evaluates a provider request in one call.
    ///
    /// Runtime coordinators that persist proposal audit records should call
    /// [`Self::prepare`] and [`Self::authorize_prepared`] separately.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderExecutionError`] for invalid input or any non-approved
    /// terminal policy outcome.
    pub async fn authorize(
        &self,
        command: ExecuteProviderCommand,
    ) -> Result<AuthorizedProviderRequest, ProviderExecutionError> {
        let prepared = self.prepare(command)?;
        self.authorize_prepared(prepared).await
    }
}

#[async_trait]
impl<D: data::HarnessDataPort> ProviderExecutionPort for ProviderExecutionLogic<D> {
    async fn execute(
        &self,
        command: ExecuteProviderCommand,
    ) -> Result<Vec<ProviderEvent>, ProviderExecutionError> {
        let authorized = self.authorize(command).await?;
        self.execute_authorized(authorized).await
    }

    async fn continue_execution(
        &self,
        harness: String,
        id: String,
        decision: ProviderDecision,
    ) -> Result<Vec<ProviderEvent>, ProviderExecutionError> {
        if id.is_empty() {
            return Err(ProviderExecutionError::Invalid);
        }
        self.exchange(data::HarnessDataCommand::Continue {
            harness_id: harness,
            continuation_id: id,
            decision: match decision {
                ProviderDecision::Continue => data::HarnessDataDecision::Continue,
                ProviderDecision::Replace(entries) => {
                    data::HarnessDataDecision::Replace(entries.into_iter().map(map_entry).collect())
                }
                ProviderDecision::Reject(reason) => data::HarnessDataDecision::Reject(reason),
                ProviderDecision::Cancel(reason) => data::HarnessDataDecision::Cancel(reason),
            },
        })
        .await
    }

    async fn cancel(
        &self,
        harness: String,
        id: String,
    ) -> Result<Vec<ProviderEvent>, ProviderExecutionError> {
        if id.is_empty() {
            return Err(ProviderExecutionError::Invalid);
        }
        self.exchange(data::HarnessDataCommand::Cancel {
            harness_id: harness,
            cancellation_id: id,
        })
        .await
    }
}

impl<D: data::HarnessDataPort> ProviderExecutionLogic<D> {
    /// Executes a request whose exact proposal has already passed runtime policy.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderExecutionError`] when the authorized request is invalid
    /// or the harness dependency fails.
    pub async fn execute_authorized(
        &self,
        authorized: AuthorizedProviderRequest,
    ) -> Result<Vec<ProviderEvent>, ProviderExecutionError> {
        let mut stream = self.execute_authorized_stream(authorized).await?;
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            events.push(event?);
        }
        Ok(events)
    }

    /// Starts a pull-based bounded stream for an already authorized request.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderExecutionError`] for an invalid replacement or unavailable harness data.
    pub async fn execute_authorized_stream(
        &self,
        authorized: AuthorizedProviderRequest,
    ) -> Result<ProviderEventStream, ProviderExecutionError> {
        let ConsequentialAction::ModelRequest(action) = authorized.executable.action else {
            return Err(ProviderExecutionError::InvalidInterceptionReplacement);
        };
        self.stream(data::HarnessDataCommand::Execute {
            harness_id: action.harness,
            session_id: authorized.session_id,
            provider: action.provider,
            model: action.model,
            entries: authorized.entries.into_iter().map(map_entry).collect(),
            options: action.options,
            grant: authorized.grant_binding,
            cancellation_id: authorized.cancellation_id,
        })
        .await
    }

    /// Starts a bounded continuation stream.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderExecutionError`] for an invalid continuation or unavailable harness data.
    pub async fn continue_execution_stream(
        &self,
        harness: String,
        id: String,
        decision: ProviderDecision,
    ) -> Result<ProviderEventStream, ProviderExecutionError> {
        if id.is_empty() {
            return Err(ProviderExecutionError::Invalid);
        }
        self.stream(data::HarnessDataCommand::Continue {
            harness_id: harness,
            continuation_id: id,
            decision: match decision {
                ProviderDecision::Continue => data::HarnessDataDecision::Continue,
                ProviderDecision::Replace(entries) => {
                    data::HarnessDataDecision::Replace(entries.into_iter().map(map_entry).collect())
                }
                ProviderDecision::Reject(reason) => data::HarnessDataDecision::Reject(reason),
                ProviderDecision::Cancel(reason) => data::HarnessDataDecision::Cancel(reason),
            },
        })
        .await
    }

    async fn stream(
        &self,
        command: data::HarnessDataCommand,
    ) -> Result<ProviderEventStream, ProviderExecutionError> {
        self.data
            .exchange_events(command)
            .await
            .map(|data| ProviderEventStream { data })
            .map_err(|_| ProviderExecutionError::Unavailable)
    }

    async fn exchange(
        &self,
        command: data::HarnessDataCommand,
    ) -> Result<Vec<ProviderEvent>, ProviderExecutionError> {
        match self
            .data
            .exchange(command)
            .await
            .map_err(|_| ProviderExecutionError::Unavailable)?
        {
            data::HarnessDataReply::Events(events) => {
                Ok(events.into_iter().map(map_event).collect())
            }
            data::HarnessDataReply::Failed {
                code,
                message,
                retryable,
            } => Err(ProviderExecutionError::Harness {
                code,
                message,
                retryable,
            }),
            data::HarnessDataReply::Health { .. } => Err(ProviderExecutionError::Invalid),
        }
    }
}

fn validate_execute(command: &ExecuteProviderCommand) -> Result<(), ProviderExecutionError> {
    if command.session_id.trim().is_empty()
        || command.harness.trim().is_empty()
        || command.provider.trim().is_empty()
        || command.model.trim().is_empty()
        || command.entries.is_empty()
        || command.entries.len() > 256
        || command.cancellation_id.trim().is_empty()
        || command.style.trim().is_empty()
        || command.workspace.trim().is_empty()
        || !command.options.is_object()
    {
        return Err(ProviderExecutionError::Invalid);
    }
    Ok(())
}

fn map_entry(entry: ProviderEntry) -> data::HarnessDataEntry {
    match entry {
        ProviderEntry::System(value) => data::HarnessDataEntry::System(value),
        ProviderEntry::User(value) => data::HarnessDataEntry::User(value),
        ProviderEntry::Assistant(value) => data::HarnessDataEntry::Assistant(value),
        ProviderEntry::ToolCall {
            call_id,
            tool,
            arguments,
        } => data::HarnessDataEntry::ToolCall {
            call_id,
            tool,
            arguments,
        },
        ProviderEntry::ToolResult {
            call_id,
            content,
            truncated,
        } => data::HarnessDataEntry::ToolResult {
            call_id,
            content,
            truncated,
        },
        ProviderEntry::Summary { text, start, end } => {
            data::HarnessDataEntry::Summary { text, start, end }
        }
        ProviderEntry::Metadata { key, value } => data::HarnessDataEntry::Metadata { key, value },
    }
}

fn map_event(event: data::HarnessDataEvent) -> ProviderEvent {
    match event {
        data::HarnessDataEvent::Started => ProviderEvent::Started,
        data::HarnessDataEvent::Text(value) => ProviderEvent::Text(value),
        data::HarnessDataEvent::ToolDelta {
            call_id,
            name,
            arguments,
        } => ProviderEvent::ToolDelta {
            call_id,
            name,
            arguments,
        },
        data::HarnessDataEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        } => ProviderEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        },
        data::HarnessDataEvent::Completed {
            reason,
            input_tokens,
            output_tokens,
            reasoning_tokens,
            estimated,
            cost_micros,
        } => ProviderEvent::Completed {
            reason,
            input_tokens,
            output_tokens,
            reasoning_tokens,
            estimated,
            cost_micros,
        },
        data::HarnessDataEvent::Cancelled => ProviderEvent::Cancelled,
        data::HarnessDataEvent::Failed {
            code,
            message,
            retryable,
        } => ProviderEvent::Failed {
            code,
            message,
            retryable,
        },
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ProviderExecutionError {
    #[error("invalid provider execution")]
    Invalid,
    #[error("harness unavailable")]
    Unavailable,
    #[error("provider action requires approval: {0}")]
    ApprovalRequired(String),
    #[error("provider action rejected: {0}")]
    Rejected(String),
    #[error("provider action cancelled: {0}")]
    Cancelled(String),
    #[error("provider interceptor returned an unsupported decision")]
    UnsupportedDecision,
    #[error("provider interceptor returned an invalid replacement")]
    InvalidInterceptionReplacement,
    #[error("harness rejected execution ({code}): {message}")]
    Harness {
        code: String,
        message: String,
        retryable: bool,
    },
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use agentmod_event_pipeline::BlockingPipelineBuilder;
    use agentmod_runtime_data::harness::{HarnessDataError, HarnessDataPort, HarnessDataReply};

    use crate::permission::{PermissionEffect, PermissionMatcher, PermissionRule};

    use super::*;

    struct MockData {
        commands: Mutex<Vec<data::HarnessDataCommand>>,
    }

    #[async_trait]
    impl HarnessDataPort for MockData {
        async fn exchange(
            &self,
            command: data::HarnessDataCommand,
        ) -> Result<HarnessDataReply, HarnessDataError> {
            self.commands.lock().expect("commands").push(command);
            Ok(HarnessDataReply::Events(vec![
                data::HarnessDataEvent::Started,
                data::HarnessDataEvent::Completed {
                    reason: "stop".into(),
                    input_tokens: 2,
                    output_tokens: 1,
                    reasoning_tokens: 0,
                    estimated: false,
                    cost_micros: 0,
                },
            ]))
        }

        async fn exchange_events(
            &self,
            command: data::HarnessDataCommand,
        ) -> Result<data::HarnessDataEventStream, HarnessDataError> {
            self.commands.lock().expect("commands").push(command);
            Ok(data::HarnessDataEventStream::from_events(vec![
                data::HarnessDataEvent::Started,
                data::HarnessDataEvent::Completed {
                    reason: "stop".into(),
                    input_tokens: 2,
                    output_tokens: 1,
                    reasoning_tokens: 0,
                    estimated: false,
                    cost_micros: 0,
                },
            ]))
        }
    }

    fn policy(effect: PermissionEffect) -> ProviderExecutionPolicy {
        let pipeline = || {
            Arc::new(
                BlockingPipelineBuilder::<ActionProposal>::new()
                    .compile()
                    .expect("empty pipeline"),
            )
        };
        ProviderExecutionPolicy {
            style_pipeline: pipeline(),
            plugin_pipeline: pipeline(),
            user_policy: PermissionPolicy::new("user", vec![], effect, "user default"),
            mandatory_policy: PermissionPolicy::new(
                "mandatory",
                vec![PermissionRule {
                    id: "deny-other-providers".into(),
                    priority: 10,
                    matcher: PermissionMatcher {
                        provider: Some("blocked".into()),
                        ..PermissionMatcher::default()
                    },
                    effect: PermissionEffect::Deny,
                    reason: "provider blocked".into(),
                }],
                PermissionEffect::Allow,
                "mandatory allow",
            ),
        }
    }

    fn command(provider: &str) -> ExecuteProviderCommand {
        ExecuteProviderCommand {
            harness: "native".into(),
            session_id: "session".into(),
            provider: provider.into(),
            model: "model".into(),
            entries: vec![ProviderEntry::User("hello".into())],
            options: serde_json::json!({}),
            cancellation_id: "cancel".into(),
            style: "persistent-chat".into(),
            workspace: "repo".into(),
        }
    }

    #[tokio::test]
    async fn issues_digest_grant_only_after_both_policies_allow() {
        let data = MockData {
            commands: Mutex::new(Vec::new()),
        };
        let logic = ProviderExecutionLogic::new(data, policy(PermissionEffect::Allow));
        let events = logic
            .execute(command("deterministic-mock"))
            .await
            .expect("run");
        assert!(matches!(
            events.as_slice(),
            [ProviderEvent::Started, ProviderEvent::Completed { .. }]
        ));
        let commands = logic.data.commands.lock().expect("commands");
        let data::HarnessDataCommand::Execute { grant, .. } = &commands[0] else {
            panic!("execute command");
        };
        assert_eq!(grant.len(), 64);
    }

    #[tokio::test]
    async fn mandatory_deny_prevents_harness_access() {
        let data = MockData {
            commands: Mutex::new(Vec::new()),
        };
        let logic = ProviderExecutionLogic::new(data, policy(PermissionEffect::Allow));
        assert_eq!(
            logic.execute(command("blocked")).await,
            Err(ProviderExecutionError::Rejected("provider blocked".into()))
        );
        assert!(logic.data.commands.lock().expect("commands").is_empty());
    }
}
