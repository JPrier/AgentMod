//! Runtime-owned consequential-action interception and permission coordination.

use agentmod_event_pipeline::{
    ActionCapabilities, BlockingPipeline, Decision, ExecutionOutcome, ExecutionReport,
    ExecutionStepResult,
};

use crate::{
    action::ActionProposal,
    permission::{PermissionDecision, PermissionEffect, PermissionPolicy},
};

/// Mandatory broad stage of a blocking handler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InterceptorScope {
    /// Session-style handlers always run first.
    SessionStyle,
    /// Activated plugin handlers run after the style.
    Plugin,
}

/// Logic-owned audit record for one blocking handler.
#[derive(Clone, Debug, PartialEq)]
pub struct InterceptorAuditStep {
    /// Mandatory broad stage.
    pub scope: InterceptorScope,
    /// Stable handler identifier.
    pub handler: String,
    /// Exact proposal supplied to the handler.
    pub input: ActionProposal,
    /// Normalized decision or failure.
    pub result: InterceptorAuditResult,
}

/// Logic-owned interceptor decision record.
#[allow(
    clippy::large_enum_variant,
    reason = "audit records intentionally retain the exact proposal inline for deterministic replay"
)]
#[derive(Clone, Debug, PartialEq)]
pub enum InterceptorAuditResult {
    /// Handler continued, potentially with a replacement.
    Continue {
        /// Proposal returned by the handler.
        output: ActionProposal,
        /// Whether the handler explicitly selected replacement semantics.
        replaced: bool,
    },
    /// Handler rejected the action.
    Reject {
        /// Safe reason.
        reason: String,
    },
    /// Handler requested durable approval.
    RequireApproval {
        /// Safe summary.
        summary: String,
        /// Opaque continuation text.
        continuation: String,
    },
    /// Handler deferred execution.
    Defer {
        /// Opaque continuation text.
        continuation: String,
    },
    /// Handler cancelled execution.
    Cancel {
        /// Safe reason.
        reason: String,
    },
    /// Handler requested execution branches.
    Fork {
        /// Exact branch proposals.
        branches: Vec<ActionProposal>,
    },
    /// Handler invocation failed under its configured policy.
    Failure {
        /// Classified readable detail.
        message: String,
    },
}

/// Complete runtime decision before any side effect.
#[derive(Clone, Debug, PartialEq)]
pub struct InterceptionResult {
    /// Original immutable proposal retained for canonical audit.
    pub original: ActionProposal,
    /// Ordered style then plugin audit trace.
    pub audit: Vec<InterceptorAuditStep>,
    /// Terminal result.
    pub outcome: InterceptionOutcome,
}

/// Terminal runtime interception outcome.
#[derive(Clone, Debug, PartialEq)]
pub enum InterceptionOutcome {
    /// Side effect may execute using exactly this final proposal.
    Approved {
        /// Interceptor-modified proposal bound to execution.
        executable: ActionProposal,
        /// User then mandatory permission evaluations.
        permission: PermissionDecision,
    },
    /// Runtime must create or await durable approval.
    RequireApproval {
        /// Interceptor-modified proposal pending approval.
        proposal: ActionProposal,
        /// Safe reason or summary.
        reason: String,
        /// Optional interceptor-supplied continuation.
        continuation: Option<String>,
    },
    /// No side effect may execute.
    Rejected {
        /// Safe reason.
        reason: String,
    },
    /// Execution is deferred.
    Deferred {
        /// Opaque continuation text.
        continuation: String,
    },
    /// Execution was cancelled.
    Cancelled {
        /// Safe reason.
        reason: String,
    },
    /// Execution fork requires runtime graph coordination.
    Forked {
        /// Exact proposals to coordinate.
        branches: Vec<ActionProposal>,
    },
    /// An interceptor failure policy aborted evaluation.
    Aborted {
        /// Handler that aborted.
        handler: String,
        /// Classified readable detail.
        reason: String,
    },
}

/// Runs the mandatory action order through both interceptor stages and both policies.
///
/// This function performs no side effect. Only [`InterceptionOutcome::Approved`]
/// conveys an executable proposal, and it always contains the final transformed
/// proposal rather than the original.
pub async fn intercept_action(
    original: ActionProposal,
    style_pipeline: &BlockingPipeline<ActionProposal>,
    plugin_pipeline: &BlockingPipeline<ActionProposal>,
    capabilities: ActionCapabilities,
    user_policy: &PermissionPolicy,
    mandatory_policy: &PermissionPolicy,
) -> InterceptionResult {
    intercept_action_with_user_policies(
        original,
        style_pipeline,
        plugin_pipeline,
        capabilities,
        &[user_policy],
        mandatory_policy,
    )
    .await
}

/// Runs both interceptor stages, every non-relaxing user-policy layer, and the
/// final mandatory policy gate.
pub async fn intercept_action_with_user_policies(
    original: ActionProposal,
    style_pipeline: &BlockingPipeline<ActionProposal>,
    plugin_pipeline: &BlockingPipeline<ActionProposal>,
    capabilities: ActionCapabilities,
    user_policies: &[&PermissionPolicy],
    mandatory_policy: &PermissionPolicy,
) -> InterceptionResult {
    let style_report = style_pipeline.execute(original.clone(), capabilities).await;
    let mut audit = audit_report(InterceptorScope::SessionStyle, &style_report);
    let after_style = match terminal_from_report(style_report) {
        PipelineTerminal::Continue(proposal) => proposal,
        terminal => {
            return InterceptionResult {
                original,
                audit,
                outcome: terminal.into_outcome(),
            };
        }
    };

    let plugin_report = plugin_pipeline.execute(after_style, capabilities).await;
    audit.extend(audit_report(InterceptorScope::Plugin, &plugin_report));
    let final_proposal = match terminal_from_report(plugin_report) {
        PipelineTerminal::Continue(proposal) => proposal,
        terminal => {
            return InterceptionResult {
                original,
                audit,
                outcome: terminal.into_outcome(),
            };
        }
    };

    let permission = crate::permission::evaluate_layered_permissions(
        &final_proposal,
        user_policies,
        mandatory_policy,
    );
    let outcome = match permission.effect {
        PermissionEffect::Allow => InterceptionOutcome::Approved {
            executable: final_proposal,
            permission,
        },
        PermissionEffect::Ask => InterceptionOutcome::RequireApproval {
            proposal: final_proposal,
            reason: permission.reason,
            continuation: None,
        },
        PermissionEffect::Deny => InterceptionOutcome::Rejected {
            reason: permission.reason,
        },
    };
    InterceptionResult {
        original,
        audit,
        outcome,
    }
}

fn audit_report(
    scope: InterceptorScope,
    report: &ExecutionReport<ActionProposal>,
) -> Vec<InterceptorAuditStep> {
    report
        .steps
        .iter()
        .map(|step| InterceptorAuditStep {
            scope,
            handler: step.handler.as_str().to_owned(),
            input: step.input.clone(),
            result: match &step.result {
                ExecutionStepResult::Decision(Decision::Continue(output)) => {
                    InterceptorAuditResult::Continue {
                        output: output.clone(),
                        replaced: false,
                    }
                }
                ExecutionStepResult::Decision(Decision::Replace(output)) => {
                    InterceptorAuditResult::Continue {
                        output: output.clone(),
                        replaced: true,
                    }
                }
                ExecutionStepResult::Decision(Decision::Reject { reason }) => {
                    InterceptorAuditResult::Reject {
                        reason: reason.clone(),
                    }
                }
                ExecutionStepResult::Decision(Decision::RequireApproval {
                    request,
                    continuation,
                }) => InterceptorAuditResult::RequireApproval {
                    summary: request.summary.clone(),
                    continuation: continuation.0.clone(),
                },
                ExecutionStepResult::Decision(Decision::Defer { continuation, .. }) => {
                    InterceptorAuditResult::Defer {
                        continuation: continuation.0.clone(),
                    }
                }
                ExecutionStepResult::Decision(Decision::Cancel { reason }) => {
                    InterceptorAuditResult::Cancel {
                        reason: reason.clone(),
                    }
                }
                ExecutionStepResult::Decision(Decision::Fork { branches, .. }) => {
                    InterceptorAuditResult::Fork {
                        branches: branches.clone(),
                    }
                }
                ExecutionStepResult::Failure(failure) => InterceptorAuditResult::Failure {
                    message: failure.message.clone(),
                },
            },
        })
        .collect()
}

enum PipelineTerminal {
    Continue(ActionProposal),
    RequireApproval {
        proposal: ActionProposal,
        reason: String,
        continuation: String,
    },
    Rejected(String),
    Deferred(String),
    Cancelled(String),
    Forked(Vec<ActionProposal>),
    Aborted {
        handler: String,
        reason: String,
    },
}

impl PipelineTerminal {
    fn into_outcome(self) -> InterceptionOutcome {
        match self {
            Self::Continue(_) => unreachable!("continuation is handled before terminal mapping"),
            Self::RequireApproval {
                proposal,
                reason,
                continuation,
            } => InterceptionOutcome::RequireApproval {
                proposal,
                reason,
                continuation: Some(continuation),
            },
            Self::Rejected(reason) => InterceptionOutcome::Rejected { reason },
            Self::Deferred(continuation) => InterceptionOutcome::Deferred { continuation },
            Self::Cancelled(reason) => InterceptionOutcome::Cancelled { reason },
            Self::Forked(branches) => InterceptionOutcome::Forked { branches },
            Self::Aborted { handler, reason } => InterceptionOutcome::Aborted { handler, reason },
        }
    }
}

fn terminal_from_report(report: ExecutionReport<ActionProposal>) -> PipelineTerminal {
    let last_input = report.steps.last().map(|step| step.input.clone());
    match report.outcome {
        ExecutionOutcome::Decision(Decision::Continue(proposal) | Decision::Replace(proposal)) => {
            PipelineTerminal::Continue(proposal)
        }
        ExecutionOutcome::Decision(Decision::Reject { reason }) => {
            PipelineTerminal::Rejected(reason)
        }
        ExecutionOutcome::Decision(Decision::RequireApproval {
            request,
            continuation,
        }) => PipelineTerminal::RequireApproval {
            proposal: last_input.expect("terminal interceptor has an input"),
            reason: request.summary,
            continuation: continuation.0,
        },
        ExecutionOutcome::Decision(Decision::Defer { continuation, .. }) => {
            PipelineTerminal::Deferred(continuation.0)
        }
        ExecutionOutcome::Decision(Decision::Cancel { reason }) => {
            PipelineTerminal::Cancelled(reason)
        }
        ExecutionOutcome::Decision(Decision::Fork { branches, .. }) => {
            PipelineTerminal::Forked(branches)
        }
        ExecutionOutcome::Aborted { handler, failure } => PipelineTerminal::Aborted {
            handler: handler.to_string(),
            reason: failure.message,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use agentmod_event_pipeline::{
        BlockingInterceptor, BlockingPipelineBuilder, FailurePolicy, InterceptorError,
        InterceptorRegistration, OrderingSpec,
    };
    use agentmod_primitives::ContentHash;
    use async_trait::async_trait;

    use crate::{
        action::{ConsequentialAction, FilesystemWriteAction, ProposalId},
        permission::{PermissionMatcher, PermissionRule},
    };

    use super::*;

    struct ReplacePath(&'static str);

    #[async_trait]
    impl BlockingInterceptor<ActionProposal> for ReplacePath {
        async fn intercept(
            &self,
            mut proposal: ActionProposal,
        ) -> Result<Decision<ActionProposal>, InterceptorError> {
            let ConsequentialAction::FilesystemWrite(write) = &mut proposal.action else {
                return Ok(Decision::Continue(proposal));
            };
            write.path = self.0.into();
            Ok(Decision::Replace(proposal))
        }
    }

    struct Continue;

    #[async_trait]
    impl BlockingInterceptor<ActionProposal> for Continue {
        async fn intercept(
            &self,
            proposal: ActionProposal,
        ) -> Result<Decision<ActionProposal>, InterceptorError> {
            Ok(Decision::Continue(proposal))
        }
    }

    fn proposal(path: &str) -> ActionProposal {
        ActionProposal {
            id: ProposalId("proposal-1".into()),
            action: ConsequentialAction::FilesystemWrite(FilesystemWriteAction {
                path: path.into(),
                expected_hash: None,
                content_hash: ContentHash::digest(b"content"),
                overwrite: false,
            }),
            style: "persistent-chat".into(),
            workspace: "repo".into(),
            origin: "runtime".into(),
        }
    }

    fn pipeline(
        handler: &'static str,
        interceptor: Arc<dyn BlockingInterceptor<ActionProposal>>,
    ) -> BlockingPipeline<ActionProposal> {
        let mut builder = BlockingPipelineBuilder::new();
        builder.register(InterceptorRegistration::new(
            OrderingSpec::new(handler, "fixture"),
            Duration::from_secs(1),
            FailurePolicy::Abort,
            interceptor,
        ));
        builder.compile().expect("pipeline")
    }

    fn policy(id: &str, effect: PermissionEffect, matcher: PermissionMatcher) -> PermissionPolicy {
        PermissionPolicy::new(
            id,
            vec![PermissionRule {
                id: format!("{id}-rule"),
                priority: 1,
                matcher,
                effect,
                reason: format!("{id} reason"),
            }],
            effect,
            format!("{id} default"),
        )
    }

    #[tokio::test]
    async fn records_original_and_executes_only_modified_proposal() {
        let style = pipeline("style-rewrite", Arc::new(ReplacePath("safe/output.txt")));
        let plugins = pipeline("plugin-observe", Arc::new(Continue));
        let allow = policy(
            "allow",
            PermissionEffect::Allow,
            PermissionMatcher::default(),
        );
        let result = intercept_action(
            proposal("unsafe/output.txt"),
            &style,
            &plugins,
            ActionCapabilities::all(),
            &allow,
            &allow,
        )
        .await;

        let ConsequentialAction::FilesystemWrite(original) = &result.original.action else {
            panic!("filesystem proposal")
        };
        assert_eq!(original.path, "unsafe/output.txt");
        assert_eq!(result.audit.len(), 2);
        assert!(matches!(
            result.audit[0].result,
            InterceptorAuditResult::Continue { replaced: true, .. }
        ));
        let InterceptionOutcome::Approved { executable, .. } = result.outcome else {
            panic!("approved")
        };
        let ConsequentialAction::FilesystemWrite(executable) = executable.action else {
            panic!("filesystem execution")
        };
        assert_eq!(executable.path, "safe/output.txt");
    }

    #[tokio::test]
    async fn mandatory_policy_evaluates_final_proposal_and_cannot_be_bypassed() {
        let style = pipeline("style-rewrite", Arc::new(ReplacePath("blocked/output.txt")));
        let plugins = pipeline("plugin-continue", Arc::new(Continue));
        let allow = policy(
            "user",
            PermissionEffect::Allow,
            PermissionMatcher::default(),
        );
        let mandatory = policy(
            "mandatory",
            PermissionEffect::Deny,
            PermissionMatcher {
                path_prefix: Some("blocked".into()),
                ..PermissionMatcher::default()
            },
        );
        let result = intercept_action(
            proposal("safe/input.txt"),
            &style,
            &plugins,
            ActionCapabilities::all(),
            &allow,
            &mandatory,
        )
        .await;
        assert!(matches!(
            result.outcome,
            InterceptionOutcome::Rejected { .. }
        ));
        assert_eq!(result.audit[0].scope, InterceptorScope::SessionStyle);
        assert_eq!(result.audit[1].scope, InterceptorScope::Plugin);
    }
}
