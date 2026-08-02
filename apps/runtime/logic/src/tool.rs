//! Runtime-owned tool interception and execution coordination.
#![allow(
    missing_docs,
    reason = "logic-local tool records are intentionally boundary-specific"
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

use agentmod_event_pipeline::{ActionCapabilities, BlockingPipeline};
use agentmod_runtime_data::tool as data;
use serde_json::Value;
use thiserror::Error;

use crate::{
    action::{ActionProposal, ConsequentialAction, ProposalId, ToolCallAction},
    interception::{InterceptionOutcome, InterceptorAuditStep, intercept_action},
    permission::{PermissionEffect, PermissionPolicy, revalidate_mandatory_after_approval},
    workspace::{WorkspaceLeaseContract, WorkspaceLeaseMode},
};

#[derive(Clone)]
pub struct ToolExecutionPolicy {
    pub style_pipeline: Arc<BlockingPipeline<ActionProposal>>,
    pub plugin_pipeline: Arc<BlockingPipeline<ActionProposal>>,
    pub user_policy: PermissionPolicy,
    pub mandatory_policy: PermissionPolicy,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PrepareToolCommand {
    pub session_id: String,
    pub workspace: PathBuf,
    pub call_id: String,
    pub tool: String,
    pub arguments: Value,
    pub cancellation_id: String,
    pub style: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PreparedToolRequest {
    pub original: ActionProposal,
    session_id: String,
    workspace: PathBuf,
    call_id: String,
    cancellation_id: String,
    workspace_lease: Option<WorkspaceLeaseContract>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AuthorizedToolRequest {
    pub original: ActionProposal,
    pub executable: ActionProposal,
    pub interceptor_audit: Vec<InterceptorAuditStep>,
    session_id: String,
    workspace: PathBuf,
    call_id: String,
    cancellation_id: String,
    workspace_lease: Option<WorkspaceLeaseContract>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ToolAuthorizationOutcome {
    Authorized(AuthorizedToolRequest),
    ApprovalRequired {
        pending: AuthorizedToolRequest,
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum ToolEvent {
    Started {
        call_id: String,
    },
    Progress {
        call_id: String,
        message: String,
        completed: Option<u64>,
        total: Option<u64>,
    },
    Output {
        call_id: String,
        stream: ToolOutputStream,
        content: String,
    },
    Completed {
        call_id: String,
        result: Value,
        artifact: Option<String>,
        truncated: bool,
    },
    Failed {
        call_id: String,
        code: String,
        message: String,
        retryable: bool,
    },
    Cancelled {
        call_id: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolOutputStream {
    Standard,
    Error,
}

#[derive(Clone)]
pub struct ToolExecutionLogic<D> {
    data: D,
    policy: ToolExecutionPolicy,
    workspace_lease: Option<WorkspaceLeaseContract>,
}

impl<D> ToolExecutionLogic<D> {
    #[must_use]
    pub const fn new(data: D, policy: ToolExecutionPolicy) -> Self {
        Self {
            data,
            policy,
            workspace_lease: None,
        }
    }

    /// Binds an exact canonical child workspace lease to all later
    /// authorization and dependency dispatch checks.
    ///
    /// # Errors
    ///
    /// Rejects a substituted hash or a lease for another effective workspace.
    pub fn with_workspace_lease(
        mut self,
        lease: WorkspaceLeaseContract,
    ) -> Result<Self, ToolExecutionError> {
        lease
            .validate_hash()
            .map_err(|_| ToolExecutionError::WorkspaceAuthorization)?;
        self.workspace_lease = Some(lease);
        Ok(self)
    }

    /// Builds the immutable normalized proposal without dispatching a host.
    ///
    /// # Errors
    ///
    /// Returns [`ToolExecutionError`] for invalid or unsupported requests.
    pub fn prepare(
        &self,
        command: PrepareToolCommand,
    ) -> Result<PreparedToolRequest, ToolExecutionError> {
        if command.session_id.trim().is_empty()
            || command.workspace.as_os_str().is_empty()
            || command.call_id.trim().is_empty()
            || command.tool.trim().is_empty()
            || !command.arguments.is_object()
            || command.cancellation_id.trim().is_empty()
            || command.style.trim().is_empty()
        {
            return Err(ToolExecutionError::Invalid);
        }
        let (tool, group) = canonical_tool(&command.tool)?;
        if let Some(lease) = &self.workspace_lease
            && (lease.effective_root != command.workspace
                || (lease.mode == WorkspaceLeaseMode::SharedReadOnly
                    && workspace_mutating_tool(tool)))
        {
            return Err(ToolExecutionError::WorkspaceAuthorization);
        }
        let original = ActionProposal {
            id: ProposalId(format!("tool-call:{}", command.call_id)),
            action: ConsequentialAction::ToolCall(ToolCallAction {
                tool: tool.into(),
                group: group.into(),
                arguments: command.arguments.clone(),
                source: None,
            }),
            style: command.style,
            workspace: command.workspace.to_string_lossy().into_owned(),
            origin: "runtime".into(),
        };
        Ok(PreparedToolRequest {
            original,
            session_id: command.session_id,
            workspace: command.workspace,
            call_id: command.call_id,
            cancellation_id: command.cancellation_id,
            workspace_lease: self.workspace_lease.clone(),
        })
    }

    /// Runs every blocking stage and policy over a prepared proposal.
    ///
    /// # Errors
    ///
    /// Returns [`ToolExecutionError`] for any non-approved outcome or unsafe
    /// interceptor replacement.
    pub async fn authorize_prepared(
        &self,
        prepared: PreparedToolRequest,
    ) -> Result<AuthorizedToolRequest, ToolExecutionError> {
        match self.authorize_prepared_outcome(prepared).await? {
            ToolAuthorizationOutcome::Authorized(authorized) => Ok(authorized),
            ToolAuthorizationOutcome::ApprovalRequired { reason, .. } => {
                Err(ToolExecutionError::ApprovalRequired(reason))
            }
        }
    }

    /// Runs blocking stages while preserving a durable approval outcome.
    ///
    /// # Errors
    ///
    /// Returns [`ToolExecutionError`] for rejected, cancelled, malformed, or
    /// unsupported interceptor decisions.
    pub async fn authorize_prepared_outcome(
        &self,
        prepared: PreparedToolRequest,
    ) -> Result<ToolAuthorizationOutcome, ToolExecutionError> {
        let result = intercept_action(
            prepared.original.clone(),
            &self.policy.style_pipeline,
            &self.policy.plugin_pipeline,
            ActionCapabilities::all(),
            &self.policy.user_policy,
            &self.policy.mandatory_policy,
        )
        .await;
        let (executable, approval_reason) = match result.outcome {
            InterceptionOutcome::Approved { executable, .. } => (executable, None),
            InterceptionOutcome::RequireApproval {
                proposal, reason, ..
            } => (proposal, Some(reason)),
            InterceptionOutcome::Rejected { reason } => {
                return Err(ToolExecutionError::Rejected(reason));
            }
            InterceptionOutcome::Cancelled { reason } => {
                return Err(ToolExecutionError::Cancelled(reason));
            }
            InterceptionOutcome::Deferred { .. }
            | InterceptionOutcome::Forked { .. }
            | InterceptionOutcome::Aborted { .. } => {
                return Err(ToolExecutionError::UnsupportedDecision);
            }
        };
        let ConsequentialAction::ToolCall(action) = &executable.action else {
            return Err(ToolExecutionError::InvalidReplacement);
        };
        let valid_descriptor = canonical_tool(&action.tool)
            .is_ok_and(|(tool, group)| tool == action.tool && group == action.group);
        if !action.arguments.is_object() || !valid_descriptor {
            return Err(ToolExecutionError::InvalidReplacement);
        }
        validate_workspace_action(
            prepared.workspace_lease.as_ref(),
            action,
            &prepared.workspace,
        )?;
        let authorized = AuthorizedToolRequest {
            original: prepared.original,
            executable,
            interceptor_audit: result.audit,
            session_id: prepared.session_id,
            workspace: prepared.workspace,
            call_id: prepared.call_id,
            cancellation_id: prepared.cancellation_id,
            workspace_lease: prepared.workspace_lease,
        };
        Ok(match approval_reason {
            Some(reason) => ToolAuthorizationOutcome::ApprovalRequired {
                pending: authorized,
                reason,
            },
            None => ToolAuthorizationOutcome::Authorized(authorized),
        })
    }

    /// Reconstructs an approved pending tool call and reapplies mandatory policy.
    ///
    /// # Errors
    ///
    /// Returns [`ToolExecutionError`] when reconstruction fails or mandatory
    /// policy now denies the action.
    pub fn approve_pending(
        &self,
        command: PrepareToolCommand,
    ) -> Result<AuthorizedToolRequest, ToolExecutionError> {
        let prepared = self.prepare(command)?;
        let permission =
            revalidate_mandatory_after_approval(&prepared.original, &self.policy.mandatory_policy);
        if permission.effect == PermissionEffect::Deny {
            return Err(ToolExecutionError::Rejected(permission.reason));
        }
        Ok(AuthorizedToolRequest {
            executable: prepared.original.clone(),
            original: prepared.original,
            interceptor_audit: Vec::new(),
            session_id: prepared.session_id,
            workspace: prepared.workspace,
            call_id: prepared.call_id,
            cancellation_id: prepared.cancellation_id,
            workspace_lease: prepared.workspace_lease,
        })
    }

    /// Runs every blocking and permission stage before claiming a durable
    /// continuation for execution.
    ///
    /// # Errors
    ///
    /// Returns [`ToolExecutionError`] when a stage blocks or mutates the
    /// continuation identity.
    pub async fn authorize_continuation_resume(
        &self,
        session_id: &str,
        workspace: &str,
        style: &str,
        continuation: &str,
    ) -> Result<(), ToolExecutionError> {
        if session_id.trim().is_empty()
            || workspace.trim().is_empty()
            || style.trim().is_empty()
            || continuation.trim().is_empty()
        {
            return Err(ToolExecutionError::Invalid);
        }
        let original = ActionProposal {
            id: ProposalId(format!("continuation-resume:{continuation}")),
            action: ConsequentialAction::ContinuationResume {
                continuation: continuation.to_owned(),
            },
            style: style.to_owned(),
            workspace: workspace.to_owned(),
            origin: "runtime".into(),
        };
        let report = intercept_action(
            original.clone(),
            &self.policy.style_pipeline,
            &self.policy.plugin_pipeline,
            ActionCapabilities::all(),
            &self.policy.user_policy,
            &self.policy.mandatory_policy,
        )
        .await;
        match report.outcome {
            InterceptionOutcome::Approved { executable, .. } if executable == original => Ok(()),
            InterceptionOutcome::Approved { .. } => Err(ToolExecutionError::InvalidReplacement),
            InterceptionOutcome::RequireApproval { reason, .. } => {
                Err(ToolExecutionError::ApprovalRequired(reason))
            }
            InterceptionOutcome::Rejected { reason } => Err(ToolExecutionError::Rejected(reason)),
            InterceptionOutcome::Cancelled { reason } => Err(ToolExecutionError::Cancelled(reason)),
            InterceptionOutcome::Deferred { .. }
            | InterceptionOutcome::Forked { .. }
            | InterceptionOutcome::Aborted { .. } => Err(ToolExecutionError::UnsupportedDecision),
        }
    }
}

impl<D: data::ToolDataPort> ToolExecutionLogic<D> {
    /// Cancels one exact active tool operation when its selected dependency
    /// supports concurrent cancellation.
    ///
    /// # Errors
    ///
    /// Returns a logic-owned error for empty identifiers or data failures.
    pub async fn cancel(&self, cancellation_id: String) -> Result<bool, ToolExecutionError> {
        if cancellation_id.trim().is_empty() {
            return Err(ToolExecutionError::Invalid);
        }
        self.data
            .cancel_tool(data::CancelToolDataRequest { cancellation_id })
            .await
            .map_err(|_| ToolExecutionError::Unavailable)
    }

    /// Dispatches exactly one already-authorized request through runtime data.
    ///
    /// # Errors
    ///
    /// Returns [`ToolExecutionError`] for invalid replacement state or host
    /// unavailability.
    pub async fn execute_authorized(
        &self,
        request: AuthorizedToolRequest,
        receipt_only: bool,
    ) -> Result<Vec<ToolEvent>, ToolExecutionError> {
        let execution_id = request.original.id.0.clone();
        let ConsequentialAction::ToolCall(action) = request.executable.action else {
            return Err(ToolExecutionError::InvalidReplacement);
        };
        validate_workspace_action(
            request.workspace_lease.as_ref(),
            &action,
            &request.workspace,
        )?;
        let workspace_authorization = request
            .workspace_lease
            .as_ref()
            .map(|lease| {
                let read_only = lease.mode == WorkspaceLeaseMode::SharedReadOnly;
                let dispatch_digest = workspace_dispatch_digest(
                    &lease.lease_id,
                    lease.lease_hash,
                    read_only,
                    &action.tool,
                    &action.arguments,
                    &request.cancellation_id,
                )?;
                Ok(data::WorkspaceAuthorizationDataRecord {
                    lease_id: lease.lease_id.clone(),
                    lease_hash: lease.lease_hash,
                    read_only,
                    dispatch_digest,
                })
            })
            .transpose()?;
        let events = self
            .data
            .execute_tool(data::ExecuteToolDataRequest {
                execution_id,
                receipt_only,
                session_id: request.session_id,
                workspace: request.workspace,
                call_id: request.call_id,
                tool: action.tool,
                arguments: action.arguments,
                cancellation_id: request.cancellation_id,
                workspace_authorization,
            })
            .await
            .map_err(|error| match error {
                data::ToolDataError::InvalidConfiguration => {
                    ToolExecutionError::InvalidConfiguration
                }
                data::ToolDataError::ReceiptUnavailable => ToolExecutionError::ReceiptUnavailable,
                data::ToolDataError::Unavailable => ToolExecutionError::Unavailable,
            })?
            .into_iter()
            .map(map_event)
            .collect();
        Ok(events)
    }
}

fn validate_workspace_action(
    lease: Option<&WorkspaceLeaseContract>,
    action: &ToolCallAction,
    workspace: &std::path::Path,
) -> Result<(), ToolExecutionError> {
    let Some(lease) = lease else {
        return Ok(());
    };
    lease
        .validate_hash()
        .map_err(|_| ToolExecutionError::WorkspaceAuthorization)?;
    if lease.effective_root != workspace
        || (lease.mode == WorkspaceLeaseMode::SharedReadOnly
            && workspace_mutating_tool(&action.tool))
    {
        Err(ToolExecutionError::WorkspaceAuthorization)
    } else {
        Ok(())
    }
}

fn workspace_mutating_tool(tool: &str) -> bool {
    matches!(
        tool,
        "filesystem.write"
            | "filesystem.edit"
            | "filesystem.apply_patch"
            | "process.run"
            | "process.start"
            | "process.run_pty"
            | "process.start_pty"
            | "process.input"
            | "git.branch"
            | "git.worktree_create"
            | "git.worktree_cleanup"
            | "git.checkpoint_create"
            | "git.checkpoint_restore"
            | "browser.download"
            | "mcp.invoke"
    )
}

fn workspace_dispatch_digest(
    lease_id: &str,
    lease_hash: agentmod_primitives::ContentHash,
    read_only: bool,
    tool: &str,
    arguments: &Value,
    cancellation_id: &str,
) -> Result<agentmod_primitives::ContentHash, ToolExecutionError> {
    serde_json::to_vec(&(
        "agentmod.workspace-tool-dispatch@1",
        lease_id,
        lease_hash,
        read_only,
        tool,
        canonical_json(arguments),
        cancellation_id,
    ))
    .map(|bytes| agentmod_primitives::ContentHash::digest(&bytes))
    .map_err(|_| ToolExecutionError::WorkspaceAuthorization)
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonical_json).collect()),
        Value::Object(values) => {
            let sorted: BTreeMap<_, _> = values
                .iter()
                .map(|(key, value)| (key.clone(), canonical_json(value)))
                .collect();
            serde_json::to_value(sorted).unwrap_or(Value::Null)
        }
        value => value.clone(),
    }
}

/// Returns the bounded first-party tool-group catalog advertised by the runtime.
///
/// Aliases are intentionally excluded because style manifests bind canonical
/// tool IDs. The returned map contains at most one entry per immutable
/// descriptor above and is safe to pass across the runtime service boundary.
#[must_use]
pub fn canonical_tool_groups() -> BTreeMap<String, BTreeSet<String>> {
    let mut groups = BTreeMap::<String, BTreeSet<String>>::new();
    for descriptor in data::canonical_tool_catalog() {
        groups
            .entry(descriptor.group.to_owned())
            .or_default()
            .insert(descriptor.id.to_owned());
    }
    groups
}

pub(crate) fn canonical_tool(
    tool: &str,
) -> Result<(&'static str, &'static str), ToolExecutionError> {
    data::canonical_tool_catalog()
        .iter()
        .find(|descriptor| descriptor.id == tool || descriptor.aliases.contains(&tool))
        .map(|descriptor| (descriptor.id, descriptor.group))
        .ok_or(ToolExecutionError::UnsupportedTool)
}

fn map_event(event: data::ToolDataEvent) -> ToolEvent {
    match event {
        data::ToolDataEvent::Started { call_id } => ToolEvent::Started { call_id },
        data::ToolDataEvent::Progress {
            call_id,
            message,
            completed,
            total,
        } => ToolEvent::Progress {
            call_id,
            message,
            completed,
            total,
        },
        data::ToolDataEvent::Output {
            call_id,
            stream,
            content,
        } => ToolEvent::Output {
            call_id,
            stream: match stream {
                data::ToolDataOutputStream::Standard => ToolOutputStream::Standard,
                data::ToolDataOutputStream::Error => ToolOutputStream::Error,
            },
            content,
        },
        data::ToolDataEvent::Completed {
            call_id,
            result,
            artifact,
            truncated,
        } => ToolEvent::Completed {
            call_id,
            result,
            artifact,
            truncated,
        },
        data::ToolDataEvent::Failed {
            call_id,
            code,
            message,
            retryable,
        } => ToolEvent::Failed {
            call_id,
            code,
            message,
            retryable,
        },
        data::ToolDataEvent::Cancelled { call_id } => ToolEvent::Cancelled { call_id },
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ToolExecutionError {
    #[error("tool execution is invalid")]
    Invalid,
    #[error("tool is unsupported")]
    UnsupportedTool,
    #[error("tool action requires approval: {0}")]
    ApprovalRequired(String),
    #[error("tool action rejected: {0}")]
    Rejected(String),
    #[error("tool action cancelled: {0}")]
    Cancelled(String),
    #[error("tool interceptor returned an unsupported decision")]
    UnsupportedDecision,
    #[error("tool interceptor returned an invalid replacement")]
    InvalidReplacement,
    #[error("tool action violates the immutable child workspace lease")]
    WorkspaceAuthorization,
    #[error("tool host immutable configuration is invalid")]
    InvalidConfiguration,
    #[error("tool host is unavailable")]
    Unavailable,
    #[error("tool host has no durable terminal receipt")]
    ReceiptUnavailable,
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use agentmod_event_pipeline::{
        BlockingInterceptor, BlockingPipelineBuilder, Decision, FailurePolicy, InterceptorError,
        InterceptorRegistration, OrderingSpec,
    };
    use agentmod_primitives::{Sequence, SessionId};
    use async_trait::async_trait;

    use super::*;

    #[derive(Clone, Default)]
    struct MockData {
        requests: Arc<Mutex<Vec<data::ExecuteToolDataRequest>>>,
    }

    #[async_trait]
    impl data::ToolDataPort for MockData {
        async fn execute_tool(
            &self,
            request: data::ExecuteToolDataRequest,
        ) -> Result<Vec<data::ToolDataEvent>, data::ToolDataError> {
            self.requests
                .lock()
                .expect("requests")
                .push(request.clone());
            Ok(vec![
                data::ToolDataEvent::Started {
                    call_id: request.call_id.clone(),
                },
                data::ToolDataEvent::Completed {
                    call_id: request.call_id,
                    result: serde_json::json!({"ok":true}),
                    artifact: None,
                    truncated: false,
                },
            ])
        }
    }

    #[derive(Clone, Default)]
    struct InvalidConfigurationData;

    #[async_trait]
    impl data::ToolDataPort for InvalidConfigurationData {
        async fn execute_tool(
            &self,
            _request: data::ExecuteToolDataRequest,
        ) -> Result<Vec<data::ToolDataEvent>, data::ToolDataError> {
            Err(data::ToolDataError::InvalidConfiguration)
        }
    }

    struct ReplacePath;

    #[async_trait]
    impl BlockingInterceptor<ActionProposal> for ReplacePath {
        async fn intercept(
            &self,
            mut proposal: ActionProposal,
        ) -> Result<Decision<ActionProposal>, InterceptorError> {
            let ConsequentialAction::ToolCall(action) = &mut proposal.action else {
                return Err(InterceptorError::new("expected tool call"));
            };
            action.arguments = serde_json::json!({"path":"safe.txt"});
            Ok(Decision::Replace(proposal))
        }
    }

    struct ReplaceReadWithWrite;

    #[async_trait]
    impl BlockingInterceptor<ActionProposal> for ReplaceReadWithWrite {
        async fn intercept(
            &self,
            mut proposal: ActionProposal,
        ) -> Result<Decision<ActionProposal>, InterceptorError> {
            let ConsequentialAction::ToolCall(action) = &mut proposal.action else {
                return Err(InterceptorError::new("expected tool call"));
            };
            action.tool = String::from("filesystem.write");
            action.group = String::from("filesystem");
            action.arguments = serde_json::json!({"path":"blocked.txt","content":"blocked"});
            Ok(Decision::Replace(proposal))
        }
    }

    struct RejectResume;

    #[async_trait]
    impl BlockingInterceptor<ActionProposal> for RejectResume {
        async fn intercept(
            &self,
            proposal: ActionProposal,
        ) -> Result<Decision<ActionProposal>, InterceptorError> {
            if matches!(
                proposal.action,
                ConsequentialAction::ContinuationResume { .. }
            ) {
                Ok(Decision::Reject {
                    reason: String::from("resume blocked"),
                })
            } else {
                Ok(Decision::Continue(proposal))
            }
        }
    }

    fn pipeline(
        interceptor: Option<Arc<dyn BlockingInterceptor<ActionProposal>>>,
    ) -> Arc<BlockingPipeline<ActionProposal>> {
        let mut builder = BlockingPipelineBuilder::new();
        if let Some(interceptor) = interceptor {
            builder.register(InterceptorRegistration::new(
                OrderingSpec::new("rewrite", "fixture"),
                Duration::from_secs(1),
                FailurePolicy::Abort,
                interceptor,
            ));
        }
        Arc::new(builder.compile().expect("pipeline"))
    }

    fn policy(
        style_pipeline: Arc<BlockingPipeline<ActionProposal>>,
        effect: crate::permission::PermissionEffect,
    ) -> ToolExecutionPolicy {
        ToolExecutionPolicy {
            style_pipeline,
            plugin_pipeline: pipeline(None),
            user_policy: PermissionPolicy::new("user", vec![], effect, "user"),
            mandatory_policy: PermissionPolicy::new(
                "mandatory",
                vec![],
                crate::permission::PermissionEffect::Allow,
                "mandatory",
            ),
        }
    }

    fn command() -> PrepareToolCommand {
        PrepareToolCommand {
            session_id: "session".into(),
            workspace: PathBuf::from("workspace"),
            call_id: "call".into(),
            tool: "read_file".into(),
            arguments: serde_json::json!({"path":"unsafe.txt"}),
            cancellation_id: "cancel".into(),
            style: "persistent-chat".into(),
        }
    }

    fn shared_read_only_lease(workspace: PathBuf) -> WorkspaceLeaseContract {
        crate::workspace::test_workspace_lease(
            crate::workspace::WorkspaceLeaseOwner {
                parent_session_id: SessionId::from_uuid(uuid::Uuid::from_u128(1)),
                parent_action_sequence: Sequence::new(7).expect("sequence"),
                parent_graph_node_id: String::from("worker-fanout/spawn-worker"),
                task_id: String::from("worker-task"),
            },
            workspace,
        )
    }

    #[test]
    fn every_canonical_tool_round_trips_to_its_declared_group() {
        for descriptor in data::canonical_tool_catalog() {
            assert_eq!(
                canonical_tool(descriptor.id),
                Ok((descriptor.id, descriptor.group)),
                "canonical tool {}",
                descriptor.id
            );
        }
    }

    #[test]
    fn every_tool_alias_resolves_to_its_canonical_identity() {
        let aliases = data::canonical_tool_catalog()
            .iter()
            .flat_map(|descriptor| {
                descriptor
                    .aliases
                    .iter()
                    .map(move |alias| (*alias, descriptor.id, descriptor.group))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            aliases.len(),
            9,
            "update the alias audit when aliases change"
        );
        for (alias, id, group) in aliases {
            assert_eq!(canonical_tool(alias), Ok((id, group)), "alias {alias}");
        }
    }

    #[test]
    fn canonical_tool_ids_and_aliases_are_globally_unique() {
        let mut identifiers = BTreeSet::new();
        for descriptor in data::canonical_tool_catalog() {
            assert!(!descriptor.id.is_empty());
            assert!(!descriptor.group.is_empty());
            assert!(
                identifiers.insert(descriptor.id),
                "duplicate canonical tool ID {}",
                descriptor.id
            );
            for alias in descriptor.aliases {
                assert!(!alias.is_empty());
                assert!(
                    identifiers.insert(alias),
                    "duplicate canonical tool alias {alias}"
                );
            }
        }
    }

    #[test]
    fn unsupported_tool_identifiers_are_rejected() {
        for unsupported in [
            "",
            "filesystem",
            "filesystem.unknown",
            "READ_FILE",
            " read_file",
        ] {
            assert_eq!(
                canonical_tool(unsupported),
                Err(ToolExecutionError::UnsupportedTool)
            );
        }
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "the explicit expected map prevents the catalog test from deriving its oracle from the catalog under test"
    )]
    fn canonical_tool_group_catalog_is_exact() {
        let expected = BTreeMap::from([
            (
                String::from("browser"),
                [
                    "browser.click",
                    "browser.close",
                    "browser.download",
                    "browser.inspect",
                    "browser.navigate",
                    "browser.screenshot",
                    "browser.start",
                    "browser.submit",
                    "browser.type",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            ),
            (
                String::from("filesystem"),
                [
                    "filesystem.apply_patch",
                    "filesystem.edit",
                    "filesystem.glob",
                    "filesystem.grep",
                    "filesystem.list",
                    "filesystem.read",
                    "filesystem.write",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            ),
            (
                String::from("git"),
                [
                    "git.branch",
                    "git.changed_files",
                    "git.checkpoint_create",
                    "git.checkpoint_restore",
                    "git.diff",
                    "git.dirty",
                    "git.discover",
                    "git.export_patch",
                    "git.status",
                    "git.worktree_cleanup",
                    "git.worktree_create",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            ),
            (
                String::from("lsp"),
                [
                    "lsp.code_actions",
                    "lsp.definition",
                    "lsp.diagnostics",
                    "lsp.document_symbols",
                    "lsp.formatting",
                    "lsp.hover",
                    "lsp.project_root",
                    "lsp.references",
                    "lsp.rename",
                    "lsp.signature_help",
                    "lsp.workspace_symbols",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            ),
            (
                String::from("mcp"),
                ["mcp.capabilities", "mcp.invoke", "mcp.server.list"]
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
            ),
            (
                String::from("process"),
                [
                    "process.detach",
                    "process.input",
                    "process.interrupt",
                    "process.kill",
                    "process.list",
                    "process.read",
                    "process.reattach",
                    "process.resize",
                    "process.run",
                    "process.run_pty",
                    "process.start",
                    "process.start_pty",
                    "process.wait",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            ),
            (
                String::from("web"),
                ["http.request", "web.fetch", "web.search"]
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
            ),
        ]);
        assert_eq!(canonical_tool_groups(), expected);
    }

    #[test]
    fn workspace_dispatch_digest_canonicalizes_nested_object_order() {
        let mut first = serde_json::Map::new();
        first.insert(String::from("server_id"), serde_json::json!("fixture"));
        first.insert(String::from("kind"), serde_json::json!("tool"));
        first.insert(String::from("name"), serde_json::json!("echo"));
        first.insert(
            String::from("arguments"),
            serde_json::json!({"second":2,"first":1}),
        );
        let mut second = serde_json::Map::new();
        second.insert(
            String::from("arguments"),
            serde_json::json!({"first":1,"second":2}),
        );
        second.insert(String::from("name"), serde_json::json!("echo"));
        second.insert(String::from("kind"), serde_json::json!("tool"));
        second.insert(String::from("server_id"), serde_json::json!("fixture"));
        let lease_hash = agentmod_primitives::ContentHash::digest(b"lease");

        assert_eq!(
            workspace_dispatch_digest(
                "lease-id",
                lease_hash,
                false,
                "mcp.invoke",
                &Value::Object(first),
                "cancel",
            ),
            workspace_dispatch_digest(
                "lease-id",
                lease_hash,
                false,
                "mcp.invoke",
                &Value::Object(second),
                "cancel",
            )
        );
    }

    #[tokio::test]
    async fn replacement_is_the_only_action_sent_to_data() {
        let data = MockData::default();
        let logic = ToolExecutionLogic::new(
            data.clone(),
            policy(
                pipeline(Some(Arc::new(ReplacePath))),
                crate::permission::PermissionEffect::Allow,
            ),
        );
        let prepared = logic.prepare(command()).expect("prepare");
        let ConsequentialAction::ToolCall(original) = &prepared.original.action else {
            panic!("original tool")
        };
        assert_eq!(original.arguments["path"], "unsafe.txt");

        let authorized = logic.authorize_prepared(prepared).await.expect("authorize");
        logic
            .execute_authorized(authorized, false)
            .await
            .expect("execute");

        let requests = data.requests.lock().expect("requests");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].tool, "filesystem.read");
        assert_eq!(requests[0].arguments["path"], "safe.txt");
    }

    #[tokio::test]
    async fn immutable_host_configuration_failure_remains_distinct_in_logic() {
        let logic = ToolExecutionLogic::new(
            InvalidConfigurationData,
            policy(pipeline(None), crate::permission::PermissionEffect::Allow),
        );
        let prepared = logic.prepare(command()).expect("prepare");
        let authorized = logic.authorize_prepared(prepared).await.expect("authorize");

        assert_eq!(
            logic.execute_authorized(authorized, false).await,
            Err(ToolExecutionError::InvalidConfiguration)
        );
    }

    #[test]
    fn shared_read_only_lease_rejects_mutating_runtime_boundaries() {
        let workspace = PathBuf::from("workspace");
        let logic = ToolExecutionLogic::new(
            MockData::default(),
            policy(pipeline(None), crate::permission::PermissionEffect::Allow),
        )
        .with_workspace_lease(shared_read_only_lease(workspace))
        .expect("lease");
        for tool in [
            "filesystem.write",
            "process.run",
            "browser.download",
            "mcp.invoke",
        ] {
            let mut candidate = command();
            candidate.tool = String::from(tool);
            assert!(
                matches!(
                    logic.prepare(candidate),
                    Err(ToolExecutionError::WorkspaceAuthorization)
                ),
                "{tool}"
            );
        }
    }

    #[tokio::test]
    async fn interceptor_cannot_replace_read_with_workspace_write() {
        let data = MockData::default();
        let logic = ToolExecutionLogic::new(
            data.clone(),
            policy(
                pipeline(Some(Arc::new(ReplaceReadWithWrite))),
                crate::permission::PermissionEffect::Allow,
            ),
        )
        .with_workspace_lease(shared_read_only_lease(PathBuf::from("workspace")))
        .expect("lease");
        let prepared = logic.prepare(command()).expect("read prepare");
        assert_eq!(
            logic.authorize_prepared(prepared).await,
            Err(ToolExecutionError::WorkspaceAuthorization)
        );
        assert!(data.requests.lock().expect("requests").is_empty());
    }

    #[tokio::test]
    async fn read_dispatch_carries_exact_workspace_authorization() {
        let data = MockData::default();
        let lease = shared_read_only_lease(PathBuf::from("workspace"));
        let logic = ToolExecutionLogic::new(
            data.clone(),
            policy(pipeline(None), crate::permission::PermissionEffect::Allow),
        )
        .with_workspace_lease(lease.clone())
        .expect("lease");
        let prepared = logic.prepare(command()).expect("read prepare");
        let authorized = logic.authorize_prepared(prepared).await.expect("authorize");
        logic
            .execute_authorized(authorized, false)
            .await
            .expect("execute");
        let requests = data.requests.lock().expect("requests");
        let authorization = requests[0]
            .workspace_authorization
            .as_ref()
            .expect("workspace authorization");
        assert_eq!(authorization.lease_id, lease.lease_id);
        assert_eq!(authorization.lease_hash, lease.lease_hash);
        assert!(authorization.read_only);
        assert_eq!(
            authorization.dispatch_digest,
            workspace_dispatch_digest(
                &lease.lease_id,
                lease.lease_hash,
                true,
                &requests[0].tool,
                &requests[0].arguments,
                &requests[0].cancellation_id,
            )
            .expect("dispatch digest")
        );
    }

    #[tokio::test]
    async fn denied_action_never_reaches_data() {
        let data = MockData::default();
        let logic = ToolExecutionLogic::new(
            data.clone(),
            policy(pipeline(None), crate::permission::PermissionEffect::Deny),
        );
        let prepared = logic.prepare(command()).expect("prepare");

        assert!(matches!(
            logic.authorize_prepared(prepared).await,
            Err(ToolExecutionError::Rejected(_))
        ));
        assert!(data.requests.lock().expect("requests").is_empty());
    }

    #[tokio::test]
    async fn continuation_resume_runs_the_blocking_pipeline() {
        let logic = ToolExecutionLogic::new(
            MockData::default(),
            policy(
                pipeline(Some(Arc::new(RejectResume))),
                crate::permission::PermissionEffect::Allow,
            ),
        );
        assert!(matches!(
            logic
                .authorize_continuation_resume(
                    "session",
                    "workspace",
                    "persistent-chat",
                    "continuation"
                )
                .await,
            Err(ToolExecutionError::Rejected(reason)) if reason == "resume blocked"
        ));
    }

    #[test]
    fn git_tools_receive_the_git_security_group() {
        let logic = ToolExecutionLogic::new(
            MockData::default(),
            policy(pipeline(None), crate::permission::PermissionEffect::Allow),
        );
        let mut git = command();
        git.tool = "git.status".into();
        git.arguments = serde_json::json!({"path":"."});
        let prepared = logic.prepare(git).expect("Git tool is supported");
        let ConsequentialAction::ToolCall(action) = prepared.original.action else {
            panic!("tool proposal")
        };
        assert_eq!(action.tool, "git.status");
        assert_eq!(action.group, "git");
    }
}
