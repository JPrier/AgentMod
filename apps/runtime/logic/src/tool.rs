//! Runtime-owned tool interception and execution coordination.
#![allow(
    missing_docs,
    reason = "logic-local tool records are intentionally boundary-specific"
)]

use std::{path::PathBuf, sync::Arc};

use agentmod_event_pipeline::{ActionCapabilities, BlockingPipeline};
use agentmod_runtime_data::tool as data;
use serde_json::Value;
use thiserror::Error;

use crate::{
    action::{ActionProposal, ConsequentialAction, ProposalId, ToolCallAction},
    interception::{InterceptionOutcome, intercept_action},
    permission::{PermissionEffect, PermissionPolicy, revalidate_mandatory_after_approval},
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
}

#[derive(Clone, Debug, PartialEq)]
pub struct AuthorizedToolRequest {
    pub original: ActionProposal,
    pub executable: ActionProposal,
    session_id: String,
    workspace: PathBuf,
    call_id: String,
    cancellation_id: String,
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
}

impl<D> ToolExecutionLogic<D> {
    #[must_use]
    pub const fn new(data: D, policy: ToolExecutionPolicy) -> Self {
        Self { data, policy }
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
        let authorized = AuthorizedToolRequest {
            original: prepared.original,
            executable,
            session_id: prepared.session_id,
            workspace: prepared.workspace,
            call_id: prepared.call_id,
            cancellation_id: prepared.cancellation_id,
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
            session_id: prepared.session_id,
            workspace: prepared.workspace,
            call_id: prepared.call_id,
            cancellation_id: prepared.cancellation_id,
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
            })
            .await
            .map_err(|error| match error {
                data::ToolDataError::ReceiptUnavailable => ToolExecutionError::ReceiptUnavailable,
                data::ToolDataError::Unavailable => ToolExecutionError::Unavailable,
            })?
            .into_iter()
            .map(map_event)
            .collect();
        Ok(events)
    }
}

fn canonical_tool(tool: &str) -> Result<(&'static str, &'static str), ToolExecutionError> {
    match tool {
        "read_file" | "filesystem.read" => Ok(("filesystem.read", "filesystem")),
        "list_files" | "filesystem.list" => Ok(("filesystem.list", "filesystem")),
        "glob" | "filesystem.glob" => Ok(("filesystem.glob", "filesystem")),
        "grep" | "filesystem.grep" => Ok(("filesystem.grep", "filesystem")),
        "write_file" | "filesystem.write" => Ok(("filesystem.write", "filesystem")),
        "edit_file" | "filesystem.edit" => Ok(("filesystem.edit", "filesystem")),
        "apply_patch" | "filesystem.apply_patch" => Ok(("filesystem.apply_patch", "filesystem")),
        "run_command" | "process.run" => Ok(("process.run", "process")),
        "start_process" | "process.start" => Ok(("process.start", "process")),
        "process.run_pty" => Ok(("process.run_pty", "process")),
        "process.start_pty" => Ok(("process.start_pty", "process")),
        "process.read" => Ok(("process.read", "process")),
        "process.input" => Ok(("process.input", "process")),
        "process.resize" => Ok(("process.resize", "process")),
        "process.wait" => Ok(("process.wait", "process")),
        "process.interrupt" => Ok(("process.interrupt", "process")),
        "process.kill" => Ok(("process.kill", "process")),
        "process.detach" => Ok(("process.detach", "process")),
        "process.reattach" => Ok(("process.reattach", "process")),
        "process.list" => Ok(("process.list", "process")),
        "git.discover" => Ok(("git.discover", "git")),
        "git.status" => Ok(("git.status", "git")),
        "git.diff" => Ok(("git.diff", "git")),
        "git.changed_files" => Ok(("git.changed_files", "git")),
        "git.branch" => Ok(("git.branch", "git")),
        "git.dirty" => Ok(("git.dirty", "git")),
        "git.worktree_create" => Ok(("git.worktree_create", "git")),
        "git.worktree_cleanup" => Ok(("git.worktree_cleanup", "git")),
        "git.checkpoint_create" => Ok(("git.checkpoint_create", "git")),
        "git.checkpoint_restore" => Ok(("git.checkpoint_restore", "git")),
        "git.export_patch" => Ok(("git.export_patch", "git")),
        "http.request" => Ok(("http.request", "web")),
        "web.fetch" => Ok(("web.fetch", "web")),
        "web.search" => Ok(("web.search", "web")),
        "lsp.project_root" => Ok(("lsp.project_root", "lsp")),
        "lsp.diagnostics" => Ok(("lsp.diagnostics", "lsp")),
        "lsp.document_symbols" => Ok(("lsp.document_symbols", "lsp")),
        "lsp.workspace_symbols" => Ok(("lsp.workspace_symbols", "lsp")),
        "lsp.definition" => Ok(("lsp.definition", "lsp")),
        "lsp.references" => Ok(("lsp.references", "lsp")),
        "lsp.hover" => Ok(("lsp.hover", "lsp")),
        "lsp.signature_help" => Ok(("lsp.signature_help", "lsp")),
        "lsp.rename" => Ok(("lsp.rename", "lsp")),
        "lsp.formatting" => Ok(("lsp.formatting", "lsp")),
        "lsp.code_actions" => Ok(("lsp.code_actions", "lsp")),
        "mcp.server.list" => Ok(("mcp.server.list", "mcp")),
        "mcp.capabilities" => Ok(("mcp.capabilities", "mcp")),
        "mcp.invoke" => Ok(("mcp.invoke", "mcp")),
        "browser.start" => Ok(("browser.start", "browser")),
        "browser.navigate" => Ok(("browser.navigate", "browser")),
        "browser.inspect" => Ok(("browser.inspect", "browser")),
        "browser.screenshot" => Ok(("browser.screenshot", "browser")),
        "browser.click" => Ok(("browser.click", "browser")),
        "browser.type" => Ok(("browser.type", "browser")),
        "browser.submit" => Ok(("browser.submit", "browser")),
        "browser.download" => Ok(("browser.download", "browser")),
        "browser.close" => Ok(("browser.close", "browser")),
        _ => Err(ToolExecutionError::UnsupportedTool),
    }
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
