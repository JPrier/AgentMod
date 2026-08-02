//! Production policy and reviewer application boundary for generic child nodes.
//!
//! This module performs no journal writes and no child-session effects. It
//! converts exact generic-node requests into normal runtime action proposals,
//! applies the existing style → plugin → user → mandatory policy order, and
//! validates typed reviewer receipts. Child cancellation dispatch remains a
//! Turn-owned seam: an accepted cancellation returns a proposal reference
//! bound to the exact action digest and sorted child set.

use agentmod_event_pipeline::ActionCapabilities;
use agentmod_primitives::ContentHash;
use async_trait::async_trait;
use serde::Serialize;

use crate::{
    action::{ActionProposal, ChildAgentCancellationAction, ConsequentialAction, ProposalId},
    child_graph_continuation::{
        ChildGraphAncillaryApplicationContext, ChildGraphAncillaryApplicationError,
        ChildGraphAncillaryApplicationOutcome, ChildGraphAncillaryApplicationPhase,
        ChildGraphAncillaryApplicationPort,
    },
    child_graph_execution::ReviewRoutingProposal,
    child_graph_turn::{
        ChildCancellationProposalRequest, ChildCreationAuthorizationRequest,
        ChildReviewEvidenceRequest,
    },
    harness::ProviderExecutionPolicy,
    interception::{InterceptionOutcome, intercept_action, intercept_action_with_user_policies},
    permission::{
        PermissionEffect, PermissionMatcher, PermissionPolicy, PermissionRule,
        revalidate_mandatory_after_approval,
    },
    session::{SessionPermissionDefaults, session_mcp_binding_hash},
};

/// Consequential policy decision retained at this logic-owned boundary.
#[derive(Clone, Debug, PartialEq)]
pub enum ChildGraphConsequentialPolicyOutcome {
    /// The exact proposal is permitted.
    Approved {
        /// Exact executable proposal returned by the policy pipeline.
        proposal: ActionProposal,
    },
    /// The exact proposal requires durable approval.
    ApprovalRequired {
        /// Exact proposal to bind into the approval request.
        proposal: ActionProposal,
    },
    /// Policy terminally denied the proposal.
    Denied {
        /// Stable redacted denial code.
        code: String,
    },
    /// Policy evaluation cannot prove whether processing completed safely.
    Ambiguous {
        /// Stable bounded ambiguity code.
        code: String,
    },
}

/// Logic-owned use-case port for consequential child-node actions.
#[async_trait]
pub trait ChildGraphConsequentialPolicyPort: Send + Sync + 'static {
    /// Runs initial interception and permission evaluation.
    async fn evaluate(
        &self,
        proposal: ActionProposal,
    ) -> Result<ChildGraphConsequentialPolicyOutcome, ChildGraphAncillaryApplicationError>;

    /// Revalidates only mandatory policy after exact durable approval.
    ///
    /// # Errors
    ///
    /// Returns a stable bounded failure without performing the action.
    fn revalidate_after_approval(
        &self,
        proposal: ActionProposal,
    ) -> Result<ChildGraphConsequentialPolicyOutcome, ChildGraphAncillaryApplicationError>;
}

/// Existing runtime interception/permission pipeline exposed as a child-node
/// policy use case.
#[derive(Clone)]
pub struct InterceptionChildGraphConsequentialPolicy {
    policy: ProviderExecutionPolicy,
    style_user_policy: Option<PermissionPolicy>,
}

impl InterceptionChildGraphConsequentialPolicy {
    /// Wraps the same immutable policy assembled for normal runtime actions.
    #[must_use]
    pub const fn new(policy: ProviderExecutionPolicy) -> Self {
        Self {
            policy,
            style_user_policy: None,
        }
    }

    /// Adds the exact immutable style approval gate without relaxing the
    /// runtime-injected user policy.
    ///
    /// # Errors
    ///
    /// Returns a bounded validation error when persisted approval metadata is
    /// not one of the compiler-owned decisions.
    pub fn with_style_defaults(
        policy: ProviderExecutionPolicy,
        defaults: &SessionPermissionDefaults,
        allowed_tool_groups: &[String],
    ) -> Result<Self, ChildGraphAncillaryApplicationError> {
        Ok(Self {
            policy,
            style_user_policy: Some(immutable_style_permission_policy(
                defaults,
                allowed_tool_groups,
            )?),
        })
    }
}

#[async_trait]
impl ChildGraphConsequentialPolicyPort for InterceptionChildGraphConsequentialPolicy {
    async fn evaluate(
        &self,
        proposal: ActionProposal,
    ) -> Result<ChildGraphConsequentialPolicyOutcome, ChildGraphAncillaryApplicationError> {
        let result = if let Some(style_user_policy) = self.style_user_policy.as_ref() {
            intercept_action_with_user_policies(
                proposal,
                &self.policy.style_pipeline,
                &self.policy.plugin_pipeline,
                ActionCapabilities::all(),
                &[&self.policy.user_policy, style_user_policy],
                &self.policy.mandatory_policy,
            )
            .await
        } else {
            intercept_action(
                proposal,
                &self.policy.style_pipeline,
                &self.policy.plugin_pipeline,
                ActionCapabilities::all(),
                &self.policy.user_policy,
                &self.policy.mandatory_policy,
            )
            .await
        };
        Ok(match result.outcome {
            InterceptionOutcome::Approved { executable, .. } => {
                ChildGraphConsequentialPolicyOutcome::Approved {
                    proposal: executable,
                }
            }
            InterceptionOutcome::RequireApproval { proposal, .. } => {
                ChildGraphConsequentialPolicyOutcome::ApprovalRequired { proposal }
            }
            InterceptionOutcome::Rejected { .. } => ChildGraphConsequentialPolicyOutcome::Denied {
                code: String::from("policy_rejected"),
            },
            InterceptionOutcome::Cancelled { .. } => ChildGraphConsequentialPolicyOutcome::Denied {
                code: String::from("policy_cancelled"),
            },
            InterceptionOutcome::Deferred { .. } => {
                ChildGraphConsequentialPolicyOutcome::Ambiguous {
                    code: String::from("policy_deferred"),
                }
            }
            InterceptionOutcome::Forked { .. } => ChildGraphConsequentialPolicyOutcome::Denied {
                code: String::from("policy_fork_forbidden"),
            },
            InterceptionOutcome::Aborted { .. } => {
                ChildGraphConsequentialPolicyOutcome::Ambiguous {
                    code: String::from("policy_aborted"),
                }
            }
        })
    }

    fn revalidate_after_approval(
        &self,
        proposal: ActionProposal,
    ) -> Result<ChildGraphConsequentialPolicyOutcome, ChildGraphAncillaryApplicationError> {
        let decision =
            revalidate_mandatory_after_approval(&proposal, &self.policy.mandatory_policy);
        Ok(if decision.effect == PermissionEffect::Deny {
            ChildGraphConsequentialPolicyOutcome::Denied {
                code: String::from("mandatory_policy_rejected_after_approval"),
            }
        } else {
            ChildGraphConsequentialPolicyOutcome::Approved { proposal }
        })
    }
}

/// Compiles immutable style approval metadata into a separate restrictive
/// user-policy layer.
///
/// # Errors
///
/// Returns a bounded application error when a persisted decision is invalid
/// or an override names neither a consequential action nor an allowed tool
/// group.
pub fn immutable_style_permission_policy(
    defaults: &SessionPermissionDefaults,
    allowed_tool_groups: &[String],
) -> Result<PermissionPolicy, ChildGraphAncillaryApplicationError> {
    let rules = defaults
        .groups
        .iter()
        .map(|(group, effect)| {
            let (matcher, effect) = if is_consequential_action_kind(group) {
                (
                    PermissionMatcher {
                        action: Some(group.clone()),
                        ..PermissionMatcher::default()
                    },
                    permission_effect(effect)?,
                )
            } else if allowed_tool_groups.iter().any(|allowed| allowed == group) {
                (
                    PermissionMatcher {
                        tool_group: Some(group.clone()),
                        ..PermissionMatcher::default()
                    },
                    permission_effect(effect)?,
                )
            } else if let Ok((_, tool_group)) = crate::tool::canonical_tool(group) {
                (
                    PermissionMatcher {
                        tool_group: Some(tool_group.to_owned()),
                        ..PermissionMatcher::default()
                    },
                    if allowed_tool_groups
                        .iter()
                        .any(|allowed| allowed == tool_group)
                    {
                        permission_effect(effect)?
                    } else {
                        PermissionEffect::Deny
                    },
                )
            } else {
                return Err(application_error("unknown_style_approval_group"));
            };
            Ok(PermissionRule {
                id: format!("immutable-style-approval:{group}"),
                priority: 0,
                matcher,
                effect,
                reason: format!("immutable style approval override for {group}"),
            })
        })
        .collect::<Result<Vec<_>, ChildGraphAncillaryApplicationError>>()?;
    Ok(PermissionPolicy::new(
        "immutable-style-approvals",
        rules,
        permission_effect(&defaults.default)?,
        "immutable style approval default",
    ))
}

fn permission_effect(value: &str) -> Result<PermissionEffect, ChildGraphAncillaryApplicationError> {
    match value {
        "allow" => Ok(PermissionEffect::Allow),
        "ask" => Ok(PermissionEffect::Ask),
        "deny" => Ok(PermissionEffect::Deny),
        _ => Err(application_error("invalid_style_approval_decision")),
    }
}

fn is_consequential_action_kind(value: &str) -> bool {
    matches!(
        value,
        "context_construction"
            | "context_replacement"
            | "model_request"
            | "model_retry"
            | "provider_switch"
            | "tool_call"
            | "process_start"
            | "process_input"
            | "filesystem_write"
            | "http_request"
            | "web_search"
            | "memory_write"
            | "artifact_persistence"
            | "compaction"
            | "child_agent_creation"
            | "child_agent_message"
            | "child_agent_cancellation"
            | "plugin_state_change"
            | "plugin_node_invocation"
            | "continuation_resume"
            | "schedule_creation"
            | "checkpoint_restoration"
    )
}

/// Exact reviewer invocation command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildGraphReviewerCommand {
    /// Complete runtime-validated review request.
    pub request: ChildReviewEvidenceRequest,
    /// Hash of that complete request.
    pub request_hash: ContentHash,
    /// Stable effect idempotency key.
    pub idempotency_key: ContentHash,
    /// Initial invocation or post-approval invocation.
    pub phase: ChildGraphAncillaryApplicationPhase,
}

/// Typed reviewer/model receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildGraphReviewerOutcome {
    /// Reviewer produced a terminal, typed routing result.
    Completed {
        /// Exact routing result returned by the reviewer use case.
        routing: ReviewRoutingProposal,
        /// Echo of the exact input request hash.
        request_hash: ContentHash,
        /// Hash of the complete typed result.
        result_hash: ContentHash,
    },
    /// Reviewer/model invocation requires durable user approval.
    ApprovalRequired,
    /// Reviewer use case terminally denied execution.
    Denied {
        /// Stable redacted denial code.
        code: String,
    },
    /// Reviewer/model effect may have happened without a terminal receipt.
    Ambiguous {
        /// Stable bounded ambiguity code.
        code: String,
    },
}

/// Typed reviewer/model application use case.
#[async_trait]
pub trait ChildGraphReviewerUseCasePort: Send + Sync + 'static {
    /// Executes or recovers one exact hash-bound reviewer request.
    async fn review(
        &self,
        command: ChildGraphReviewerCommand,
    ) -> Result<ChildGraphReviewerOutcome, ChildGraphAncillaryApplicationError>;
}

/// Production ancillary application over injected logic-owned use cases.
pub struct RuntimeChildGraphAncillaryApplication<P, R> {
    policy: P,
    reviewer: R,
    style: String,
    workspace: String,
}

impl<P, R> RuntimeChildGraphAncillaryApplication<P, R> {
    /// Binds immutable style/workspace policy context and typed use cases.
    #[must_use]
    pub fn new(
        policy: P,
        reviewer: R,
        style: impl Into<String>,
        workspace: impl Into<String>,
    ) -> Self {
        Self {
            policy,
            reviewer,
            style: style.into(),
            workspace: workspace.into(),
        }
    }
}

#[async_trait]
impl<P, R> ChildGraphAncillaryApplicationPort for RuntimeChildGraphAncillaryApplication<P, R>
where
    P: ChildGraphConsequentialPolicyPort,
    R: ChildGraphReviewerUseCasePort,
{
    async fn apply_creation(
        &self,
        request: &ChildCreationAuthorizationRequest,
        context: ChildGraphAncillaryApplicationContext,
    ) -> Result<ChildGraphAncillaryApplicationOutcome, ChildGraphAncillaryApplicationError> {
        let request_hash = creation_request_hash(request)?;
        validate_context(request_hash, context)?;
        let proposal = ActionProposal {
            id: ProposalId(format!("child-creation:{request_hash}")),
            action: ConsequentialAction::ChildAgentCreation {
                style: request.child_style.clone(),
                workspace_mode: workspace_mode(&request.contract.workspace)?,
                token_budget: request.token_budget,
                inherited_provider: request.contract.inherited_provider.clone(),
                inherited_model: request.contract.inherited_model.clone(),
                inherited_mcp_binding_hash: request
                    .contract
                    .inherited_mcp
                    .as_ref()
                    .map(session_mcp_binding_hash)
                    .transpose()
                    .map_err(|_| application_error("invalid_child_mcp_binding"))?,
            },
            style: self.style.clone(),
            workspace: self.workspace.clone(),
            origin: String::from("runtime.child_graph"),
        };
        self.apply_policy(proposal, context)
            .await
            .map(|outcome| match outcome {
                ChildGraphAncillaryApplicationOutcome::Applied { .. } => {
                    ChildGraphAncillaryApplicationOutcome::Applied { reference: None }
                }
                other => other,
            })
    }

    async fn apply_cancellation(
        &self,
        request: &ChildCancellationProposalRequest,
        context: ChildGraphAncillaryApplicationContext,
    ) -> Result<ChildGraphAncillaryApplicationOutcome, ChildGraphAncillaryApplicationError> {
        let request_hash = cancellation_request_hash(request)?;
        validate_context(request_hash, context)?;
        let mut child_session_ids = request
            .child_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        child_session_ids.sort();
        if child_session_ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(application_error("duplicate_cancellation_child"));
        }
        let proposal = ActionProposal {
            id: ProposalId(format!("child-cancellation:{request_hash}")),
            action: ConsequentialAction::ChildAgentCancellation(ChildAgentCancellationAction {
                parent_session_id: request.session_id.to_string(),
                run_id: request.work.run_id.clone(),
                node_id: request.work.node_id.clone(),
                branch_path: request.work.branch_path.clone(),
                attempt: request.work.attempt,
                loop_iteration: request.work.loop_iteration,
                step: request.work.step,
                execution_plan_hash: request.execution_plan_hash,
                configuration_hash: request.configuration_hash,
                projection_hash: request.projection_hash,
                reason_hash: ContentHash::digest(request.reason.as_bytes()),
                child_session_ids,
            }),
            style: self.style.clone(),
            workspace: self.workspace.clone(),
            origin: String::from("runtime.child_graph"),
        };
        self.apply_policy(proposal, context).await
    }

    async fn apply_review(
        &self,
        request: &ChildReviewEvidenceRequest,
        context: ChildGraphAncillaryApplicationContext,
    ) -> Result<ChildGraphAncillaryApplicationOutcome, ChildGraphAncillaryApplicationError> {
        let request_hash = review_request_hash(request)?;
        validate_context(request_hash, context)?;
        match self
            .reviewer
            .review(ChildGraphReviewerCommand {
                request: request.clone(),
                request_hash,
                idempotency_key: context.idempotency_key,
                phase: context.phase,
            })
            .await?
        {
            ChildGraphReviewerOutcome::Completed {
                routing,
                request_hash: receipt_request_hash,
                result_hash,
            } => {
                if receipt_request_hash != request_hash {
                    return Ok(denied("review_request_hash_substitution"));
                }
                if routing != request.routing {
                    return Ok(denied("review_routing_substitution"));
                }
                if routing.evidence_hash != request.routing.evidence_hash {
                    return Ok(denied("review_evidence_hash_substitution"));
                }
                if result_hash != reviewer_result_hash(request_hash, &routing)? {
                    return Ok(denied("review_result_hash_substitution"));
                }
                Ok(ChildGraphAncillaryApplicationOutcome::Applied { reference: None })
            }
            ChildGraphReviewerOutcome::ApprovalRequired => {
                Ok(ChildGraphAncillaryApplicationOutcome::ApprovalRequired)
            }
            ChildGraphReviewerOutcome::Denied { code } => {
                validate_code(&code)?;
                Ok(denied(&code))
            }
            ChildGraphReviewerOutcome::Ambiguous { code } => {
                validate_code(&code)?;
                Ok(ChildGraphAncillaryApplicationOutcome::Ambiguous { code })
            }
        }
    }
}

impl<P, R> RuntimeChildGraphAncillaryApplication<P, R>
where
    P: ChildGraphConsequentialPolicyPort,
{
    async fn apply_policy(
        &self,
        proposal: ActionProposal,
        context: ChildGraphAncillaryApplicationContext,
    ) -> Result<ChildGraphAncillaryApplicationOutcome, ChildGraphAncillaryApplicationError> {
        let original = proposal.clone();
        let outcome = match context.phase {
            ChildGraphAncillaryApplicationPhase::Initial => self.policy.evaluate(proposal).await?,
            ChildGraphAncillaryApplicationPhase::AfterApproval => {
                self.policy.revalidate_after_approval(proposal)?
            }
        };
        match outcome {
            ChildGraphConsequentialPolicyOutcome::Approved { proposal } if proposal == original => {
                let digest = proposal
                    .digest()
                    .map_err(|_| application_error("proposal_encoding"))?;
                Ok(ChildGraphAncillaryApplicationOutcome::Applied {
                    reference: Some(format!(
                        "{}:{digest}",
                        proposal.action.kind().replace('_', "-")
                    )),
                })
            }
            ChildGraphConsequentialPolicyOutcome::ApprovalRequired { proposal }
                if proposal == original
                    && context.phase == ChildGraphAncillaryApplicationPhase::Initial =>
            {
                Ok(ChildGraphAncillaryApplicationOutcome::ApprovalRequired)
            }
            ChildGraphConsequentialPolicyOutcome::Approved { .. }
            | ChildGraphConsequentialPolicyOutcome::ApprovalRequired { .. } => {
                Ok(denied("policy_proposal_replacement_forbidden"))
            }
            ChildGraphConsequentialPolicyOutcome::Denied { code } => {
                validate_code(&code)?;
                Ok(denied(&code))
            }
            ChildGraphConsequentialPolicyOutcome::Ambiguous { code } => {
                validate_code(&code)?;
                Ok(ChildGraphAncillaryApplicationOutcome::Ambiguous { code })
            }
        }
    }
}

/// Computes the receipt hash a reviewer use case must return.
///
/// # Errors
///
/// Fails when bounded typed review fields cannot be deterministically encoded.
pub fn reviewer_result_hash(
    request_hash: ContentHash,
    routing: &ReviewRoutingProposal,
) -> Result<ContentHash, ChildGraphAncillaryApplicationError> {
    hash_value(&(
        "agentmod.child-graph.reviewer-result.v1",
        request_hash,
        routing.disposition,
        &routing.destination_node_id,
        routing.current_revision,
        routing.next_revision,
        &routing.rejected_task_ids,
        &routing.findings,
        routing.evidence_hash,
    ))
}

fn creation_request_hash(
    request: &ChildCreationAuthorizationRequest,
) -> Result<ContentHash, ChildGraphAncillaryApplicationError> {
    hash_value(&(
        "agentmod.child-graph.creation-approval.v1",
        &request.identity,
        &request.contract,
        &request.child_style,
        request.token_budget,
        request.action_digest,
        request.proposed_at.get(),
    ))
}

fn cancellation_request_hash(
    request: &ChildCancellationProposalRequest,
) -> Result<ContentHash, ChildGraphAncillaryApplicationError> {
    let mut child_ids = request
        .child_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    child_ids.sort();
    if child_ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(application_error("duplicate_cancellation_child"));
    }
    hash_value(&(
        "agentmod.child-graph.cancellation-approval.v1",
        request.session_id,
        &request.work,
        request.execution_plan_hash,
        request.configuration_hash,
        request.projection_hash,
        &request.reason,
        child_ids,
    ))
}

fn review_request_hash(
    request: &ChildReviewEvidenceRequest,
) -> Result<ContentHash, ChildGraphAncillaryApplicationError> {
    hash_value(&(
        "agentmod.child-graph.review-evidence.v1",
        request.session_id,
        &request.work,
        request.execution_plan_hash,
        request.configuration_hash,
        request.routing.disposition,
        &request.routing.destination_node_id,
        request.routing.current_revision,
        request.routing.next_revision,
        &request.routing.rejected_task_ids,
        &request.routing.findings,
        request.routing.evidence_hash,
    ))
}

fn workspace_mode(
    workspace: &serde_json::Value,
) -> Result<String, ChildGraphAncillaryApplicationError> {
    let mode = workspace
        .get("mode")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| application_error("workspace_mode_missing"))?;
    validate_code(mode)?;
    Ok(mode.to_owned())
}

fn validate_context(
    request_hash: ContentHash,
    context: ChildGraphAncillaryApplicationContext,
) -> Result<(), ChildGraphAncillaryApplicationError> {
    if request_hash != context.idempotency_key {
        return Err(application_error("idempotency_key_substitution"));
    }
    Ok(())
}

fn hash_value(value: &impl Serialize) -> Result<ContentHash, ChildGraphAncillaryApplicationError> {
    serde_json::to_vec(value)
        .map(|encoded| ContentHash::digest(&encoded))
        .map_err(|_| application_error("request_encoding"))
}

fn denied(code: &str) -> ChildGraphAncillaryApplicationOutcome {
    ChildGraphAncillaryApplicationOutcome::Denied {
        code: code.to_owned(),
    }
}

fn application_error(code: &str) -> ChildGraphAncillaryApplicationError {
    ChildGraphAncillaryApplicationError {
        code: code.to_owned(),
    }
}

fn validate_code(code: &str) -> Result<(), ChildGraphAncillaryApplicationError> {
    if code.is_empty()
        || code.len() > 128
        || !code
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(application_error("invalid_boundary_code"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet, VecDeque},
        sync::{Arc, Mutex},
        time::Duration,
    };

    use agentmod_event_pipeline::{
        BlockingInterceptor, BlockingPipeline, BlockingPipelineBuilder, Decision, FailurePolicy,
        InterceptorError, InterceptorRegistration, OrderingSpec,
    };
    use agentmod_primitives::{Sequence, SessionId};
    use uuid::Uuid;

    use crate::{
        child_graph_execution::{ReviewDisposition, ReviewFinding},
        node_execution::NodeWorkIdentity,
        permission::{PermissionMatcher, PermissionPolicy, PermissionRule},
        session::{GenericChildExecutionIdentity, GenericChildSpawnContract},
    };

    use super::*;

    #[derive(Default)]
    struct PolicyState {
        initial: Vec<ActionProposal>,
        revalidation: Vec<ActionProposal>,
        outcomes: VecDeque<ChildGraphConsequentialPolicyOutcome>,
    }

    #[derive(Clone, Default)]
    struct RecordingPolicy(Arc<Mutex<PolicyState>>);

    #[async_trait]
    impl ChildGraphConsequentialPolicyPort for RecordingPolicy {
        async fn evaluate(
            &self,
            proposal: ActionProposal,
        ) -> Result<ChildGraphConsequentialPolicyOutcome, ChildGraphAncillaryApplicationError>
        {
            let mut state = self.0.lock().expect("policy");
            state.initial.push(proposal.clone());
            Ok(state
                .outcomes
                .pop_front()
                .unwrap_or(ChildGraphConsequentialPolicyOutcome::Approved { proposal }))
        }

        fn revalidate_after_approval(
            &self,
            proposal: ActionProposal,
        ) -> Result<ChildGraphConsequentialPolicyOutcome, ChildGraphAncillaryApplicationError>
        {
            let mut state = self.0.lock().expect("policy");
            state.revalidation.push(proposal.clone());
            Ok(state
                .outcomes
                .pop_front()
                .unwrap_or(ChildGraphConsequentialPolicyOutcome::Approved { proposal }))
        }
    }

    #[derive(Clone, Default)]
    struct RecordingReviewer {
        commands: Arc<Mutex<Vec<ChildGraphReviewerCommand>>>,
        outcome: Arc<Mutex<Option<ChildGraphReviewerOutcome>>>,
    }

    #[async_trait]
    impl ChildGraphReviewerUseCasePort for RecordingReviewer {
        async fn review(
            &self,
            command: ChildGraphReviewerCommand,
        ) -> Result<ChildGraphReviewerOutcome, ChildGraphAncillaryApplicationError> {
            self.commands
                .lock()
                .expect("commands")
                .push(command.clone());
            self.outcome
                .lock()
                .expect("outcome")
                .clone()
                .ok_or_else(|| application_error("review_fixture_missing"))
        }
    }

    fn session(value: u128) -> SessionId {
        SessionId::from_uuid(Uuid::from_u128(value))
    }

    fn work(node: &str) -> NodeWorkIdentity {
        NodeWorkIdentity {
            run_id: String::from("run"),
            node_id: node.to_owned(),
            branch_path: vec![String::from("branch")],
            attempt: 1,
            loop_iteration: 2,
            step: 3,
        }
    }

    fn creation() -> ChildCreationAuthorizationRequest {
        let task = serde_json::json!({"instruction": "inspect"});
        ChildCreationAuthorizationRequest {
            identity: GenericChildExecutionIdentity {
                execution_id: String::from("execution"),
                work: work("spawn"),
                execution_plan_hash: ContentHash::digest(b"plan"),
                configuration_hash: ContentHash::digest(b"config"),
                proposal_hash: ContentHash::digest(b"proposal"),
                task_id: String::from("task"),
            },
            contract: GenericChildSpawnContract {
                parent_session_id: session(1),
                task_hash: ContentHash::digest(&serde_json::to_vec(&task).expect("task encoding")),
                task,
                inherited_provider: None,
                inherited_model: None,
                inherited_mcp: None,
                tool_groups: BTreeSet::new(),
                depth: 1,
                context_budget_tokens: 100,
                cost_budget_micros: 200,
                workspace: serde_json::json!({"mode": "shared_read_only"}),
                artifact_references: BTreeSet::new(),
                security_classification: String::from("internal"),
                approval_required: true,
                proposal_zero_json: String::from("{}"),
            },
            child_style: String::from("worker@1"),
            token_budget: 300,
            action_digest: ContentHash::digest(b"action"),
            proposed_at: Sequence::FIRST,
        }
    }

    fn cancellation() -> ChildCancellationProposalRequest {
        ChildCancellationProposalRequest {
            session_id: session(1),
            work: work("wait"),
            execution_plan_hash: ContentHash::digest(b"plan"),
            configuration_hash: ContentHash::digest(b"config"),
            projection_hash: ContentHash::digest(b"projection"),
            reason: String::from("wait_timeout"),
            child_ids: vec![session(12), session(10), session(11)],
        }
    }

    fn routing(disposition: ReviewDisposition) -> ReviewRoutingProposal {
        ReviewRoutingProposal {
            disposition,
            destination_node_id: match disposition {
                ReviewDisposition::Approved => "complete",
                ReviewDisposition::Revision => "revise",
                ReviewDisposition::Failed => "failed",
            }
            .to_owned(),
            current_revision: 1,
            next_revision: (disposition == ReviewDisposition::Revision).then_some(2),
            rejected_task_ids: if disposition == ReviewDisposition::Revision {
                vec![String::from("task")]
            } else {
                Vec::new()
            },
            findings: vec![ReviewFinding {
                code: String::from("revision_required"),
                message: String::from("evidence needs revision"),
                artifact_references: BTreeSet::new(),
            }],
            evidence_hash: ContentHash::digest(b"evidence"),
        }
    }

    fn review(disposition: ReviewDisposition) -> ChildReviewEvidenceRequest {
        ChildReviewEvidenceRequest {
            session_id: session(1),
            work: work("review"),
            execution_plan_hash: ContentHash::digest(b"plan"),
            configuration_hash: ContentHash::digest(b"config"),
            routing: routing(disposition),
        }
    }

    fn context(
        request_hash: ContentHash,
        phase: ChildGraphAncillaryApplicationPhase,
    ) -> ChildGraphAncillaryApplicationContext {
        ChildGraphAncillaryApplicationContext {
            phase,
            idempotency_key: request_hash,
        }
    }

    struct RecordStage {
        stage: &'static str,
        order: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl BlockingInterceptor<ActionProposal> for RecordStage {
        async fn intercept(
            &self,
            proposal: ActionProposal,
        ) -> Result<Decision<ActionProposal>, InterceptorError> {
            self.order.lock().expect("order").push(self.stage);
            Ok(Decision::Continue(proposal))
        }
    }

    fn pipeline(
        name: &'static str,
        order: Arc<Mutex<Vec<&'static str>>>,
    ) -> Arc<BlockingPipeline<ActionProposal>> {
        let mut builder = BlockingPipelineBuilder::new();
        builder.register(InterceptorRegistration::new(
            OrderingSpec::new(name, "child-graph-policy-test"),
            Duration::from_secs(1),
            FailurePolicy::Abort,
            Arc::new(RecordStage { stage: name, order }),
        ));
        Arc::new(builder.compile().expect("pipeline"))
    }

    fn permission(id: &str, effect: PermissionEffect) -> PermissionPolicy {
        PermissionPolicy::new(
            id,
            vec![PermissionRule {
                id: format!("{id}-rule"),
                priority: 1,
                matcher: PermissionMatcher::default(),
                effect,
                reason: format!("{id}-reason"),
            }],
            effect,
            format!("{id}-default"),
        )
    }

    #[tokio::test]
    async fn production_policy_runs_style_then_plugin_and_mandatory_denial_wins_last() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let policy = InterceptionChildGraphConsequentialPolicy::new(ProviderExecutionPolicy {
            style_pipeline: pipeline("style", order.clone()),
            plugin_pipeline: pipeline("plugin", order.clone()),
            user_policy: permission("user", PermissionEffect::Allow),
            mandatory_policy: permission("mandatory", PermissionEffect::Deny),
        });
        let request = cancellation();
        let request_hash = cancellation_request_hash(&request).expect("request hash");
        let application = RuntimeChildGraphAncillaryApplication::new(
            policy,
            RecordingReviewer::default(),
            "graph",
            "repo",
        );
        assert_eq!(
            application
                .apply_cancellation(
                    &request,
                    context(request_hash, ChildGraphAncillaryApplicationPhase::Initial),
                )
                .await
                .expect("policy decision"),
            denied("policy_rejected")
        );
        assert_eq!(*order.lock().expect("order"), vec!["style", "plugin"]);
    }

    #[tokio::test]
    async fn immutable_style_creation_gate_is_durable_and_cannot_relax_base_policy() {
        let application = |base_effect: PermissionEffect, style_effect: &str| {
            let order = Arc::new(Mutex::new(Vec::new()));
            let policy = ProviderExecutionPolicy {
                style_pipeline: pipeline("style", order.clone()),
                plugin_pipeline: pipeline("plugin", order),
                user_policy: permission("user", base_effect),
                mandatory_policy: permission("mandatory", PermissionEffect::Allow),
            };
            let defaults = SessionPermissionDefaults {
                default: String::from("allow"),
                groups: BTreeMap::from([(
                    String::from("child_agent_creation"),
                    style_effect.to_string(),
                )]),
            };
            RuntimeChildGraphAncillaryApplication::new(
                InterceptionChildGraphConsequentialPolicy::with_style_defaults(
                    policy,
                    &defaults,
                    &[],
                )
                .expect("style gate"),
                RecordingReviewer::default(),
                "graph",
                "repo",
            )
        };
        let request = creation();
        let hash = creation_request_hash(&request).expect("request hash");

        assert_eq!(
            application(PermissionEffect::Allow, "ask")
                .apply_creation(
                    &request,
                    context(hash, ChildGraphAncillaryApplicationPhase::Initial),
                )
                .await
                .expect("ask"),
            ChildGraphAncillaryApplicationOutcome::ApprovalRequired
        );
        assert_eq!(
            application(PermissionEffect::Deny, "allow")
                .apply_creation(
                    &request,
                    context(hash, ChildGraphAncillaryApplicationPhase::Initial),
                )
                .await
                .expect("base deny"),
            denied("policy_rejected")
        );
        assert_eq!(
            application(PermissionEffect::Allow, "deny")
                .apply_creation(
                    &request,
                    context(hash, ChildGraphAncillaryApplicationPhase::Initial),
                )
                .await
                .expect("style deny"),
            denied("policy_rejected")
        );
    }

    #[test]
    fn immutable_style_rejects_unknown_approval_groups() {
        let defaults = SessionPermissionDefaults {
            default: String::from("allow"),
            groups: BTreeMap::from([(String::from("unknown_action"), String::from("ask"))]),
        };
        let result = InterceptionChildGraphConsequentialPolicy::with_style_defaults(
            ProviderExecutionPolicy {
                style_pipeline: pipeline("style", Arc::new(Mutex::new(Vec::new()))),
                plugin_pipeline: pipeline("plugin", Arc::new(Mutex::new(Vec::new()))),
                user_policy: permission("user", PermissionEffect::Allow),
                mandatory_policy: permission("mandatory", PermissionEffect::Allow),
            },
            &defaults,
            &[],
        );
        assert!(matches!(
            result,
            Err(ChildGraphAncillaryApplicationError { code })
                if code == "unknown_style_approval_group"
        ));
    }

    #[test]
    fn immutable_style_known_tool_outside_capability_compiles_as_fail_closed() {
        let defaults = SessionPermissionDefaults {
            default: String::from("ask"),
            groups: BTreeMap::from([(String::from("filesystem.read"), String::from("allow"))]),
        };
        assert!(
            InterceptionChildGraphConsequentialPolicy::with_style_defaults(
                ProviderExecutionPolicy {
                    style_pipeline: pipeline("style", Arc::new(Mutex::new(Vec::new()))),
                    plugin_pipeline: pipeline("plugin", Arc::new(Mutex::new(Vec::new()))),
                    user_policy: permission("user", PermissionEffect::Allow),
                    mandatory_policy: permission("mandatory", PermissionEffect::Allow),
                },
                &defaults,
                &[],
            )
            .is_ok(),
            "a known tool outside the immutable capability set is represented by a deny rule"
        );
    }

    #[tokio::test]
    async fn approval_revalidates_only_mandatory_and_forbids_replacement() {
        let request = creation();
        let hash = creation_request_hash(&request).expect("request hash");
        let policy = RecordingPolicy::default();
        policy.0.lock().expect("policy").outcomes.push_back(
            ChildGraphConsequentialPolicyOutcome::ApprovalRequired {
                proposal: ActionProposal {
                    id: ProposalId(format!("child-creation:{hash}")),
                    action: ConsequentialAction::ChildAgentCreation {
                        style: request.child_style.clone(),
                        workspace_mode: String::from("shared_read_only"),
                        token_budget: request.token_budget,
                        inherited_provider: request.contract.inherited_provider.clone(),
                        inherited_model: request.contract.inherited_model.clone(),
                        inherited_mcp_binding_hash: None,
                    },
                    style: String::from("graph"),
                    workspace: String::from("repo"),
                    origin: String::from("runtime.child_graph"),
                },
            },
        );
        let application = RuntimeChildGraphAncillaryApplication::new(
            policy.clone(),
            RecordingReviewer::default(),
            "graph",
            "repo",
        );
        assert_eq!(
            application
                .apply_creation(
                    &request,
                    context(hash, ChildGraphAncillaryApplicationPhase::Initial),
                )
                .await
                .expect("initial"),
            ChildGraphAncillaryApplicationOutcome::ApprovalRequired
        );
        assert!(matches!(
            application
                .apply_creation(
                    &request,
                    context(hash, ChildGraphAncillaryApplicationPhase::AfterApproval),
                )
                .await
                .expect("approved"),
            ChildGraphAncillaryApplicationOutcome::Applied { .. }
        ));
        {
            let state = policy.0.lock().expect("policy");
            assert_eq!(state.initial.len(), 1);
            assert_eq!(state.revalidation.len(), 1);
        }

        let replacement_policy = RecordingPolicy::default();
        let mut replacement = policy
            .0
            .lock()
            .expect("policy")
            .initial
            .first()
            .expect("proposal")
            .clone();
        replacement.style = String::from("substituted");
        replacement_policy
            .0
            .lock()
            .expect("replacement policy")
            .outcomes
            .push_back(ChildGraphConsequentialPolicyOutcome::Approved {
                proposal: replacement,
            });
        let replaced = RuntimeChildGraphAncillaryApplication::new(
            replacement_policy,
            RecordingReviewer::default(),
            "graph",
            "repo",
        )
        .apply_creation(
            &request,
            context(hash, ChildGraphAncillaryApplicationPhase::Initial),
        )
        .await
        .expect("replacement classification");
        assert_eq!(replaced, denied("policy_proposal_replacement_forbidden"));
    }

    #[tokio::test]
    async fn cancellation_proposal_binds_sorted_exact_child_identity_and_is_stable() {
        let request = cancellation();
        let hash = cancellation_request_hash(&request).expect("request hash");
        let policy = RecordingPolicy::default();
        let application = RuntimeChildGraphAncillaryApplication::new(
            policy.clone(),
            RecordingReviewer::default(),
            "graph",
            "repo",
        );
        let first = application
            .apply_cancellation(
                &request,
                context(hash, ChildGraphAncillaryApplicationPhase::Initial),
            )
            .await
            .expect("cancellation");
        let second = application
            .apply_cancellation(
                &request,
                context(hash, ChildGraphAncillaryApplicationPhase::Initial),
            )
            .await
            .expect("duplicate cancellation");
        assert_eq!(first, second);
        let state = policy.0.lock().expect("policy");
        let ConsequentialAction::ChildAgentCancellation(action) = &state.initial[0].action else {
            panic!("cancellation action")
        };
        let mut sorted = action.child_session_ids.clone();
        sorted.sort();
        assert_eq!(action.child_session_ids, sorted);
        assert_eq!(action.parent_session_id, request.session_id.to_string());
        assert!(matches!(
            first,
            ChildGraphAncillaryApplicationOutcome::Applied {
                reference: Some(reference)
            } if reference.starts_with("child-agent-cancellation:")
        ));
    }

    #[tokio::test]
    async fn reviewer_acceptance_revision_failure_and_ambiguity_are_typed_and_hash_bound() {
        for disposition in [
            ReviewDisposition::Approved,
            ReviewDisposition::Revision,
            ReviewDisposition::Failed,
        ] {
            let request = review(disposition);
            let request_hash = review_request_hash(&request).expect("request hash");
            let reviewer = RecordingReviewer::default();
            *reviewer.outcome.lock().expect("outcome") =
                Some(ChildGraphReviewerOutcome::Completed {
                    routing: request.routing.clone(),
                    request_hash,
                    result_hash: reviewer_result_hash(request_hash, &request.routing)
                        .expect("result hash"),
                });
            let application = RuntimeChildGraphAncillaryApplication::new(
                RecordingPolicy::default(),
                reviewer.clone(),
                "graph",
                "repo",
            );
            assert_eq!(
                application
                    .apply_review(
                        &request,
                        context(request_hash, ChildGraphAncillaryApplicationPhase::Initial),
                    )
                    .await
                    .expect("review"),
                ChildGraphAncillaryApplicationOutcome::Applied { reference: None }
            );
            assert_eq!(
                reviewer.commands.lock().expect("commands")[0].request,
                request
            );
        }

        let request = review(ReviewDisposition::Revision);
        let request_hash = review_request_hash(&request).expect("request hash");
        let reviewer = RecordingReviewer::default();
        *reviewer.outcome.lock().expect("outcome") = Some(ChildGraphReviewerOutcome::Completed {
            routing: request.routing.clone(),
            request_hash,
            result_hash: ContentHash::digest(b"substituted"),
        });
        let application = RuntimeChildGraphAncillaryApplication::new(
            RecordingPolicy::default(),
            reviewer.clone(),
            "graph",
            "repo",
        );
        assert_eq!(
            application
                .apply_review(
                    &request,
                    context(request_hash, ChildGraphAncillaryApplicationPhase::Initial),
                )
                .await
                .expect("substitution"),
            denied("review_result_hash_substitution")
        );
        *reviewer.outcome.lock().expect("outcome") = Some(ChildGraphReviewerOutcome::Ambiguous {
            code: String::from("review_receipt_missing"),
        });
        assert_eq!(
            application
                .apply_review(
                    &request,
                    context(request_hash, ChildGraphAncillaryApplicationPhase::Initial),
                )
                .await
                .expect("ambiguity"),
            ChildGraphAncillaryApplicationOutcome::Ambiguous {
                code: String::from("review_receipt_missing")
            }
        );
    }

    #[tokio::test]
    async fn idempotency_substitution_and_duplicate_child_fail_before_policy() {
        let request = cancellation();
        let hash = cancellation_request_hash(&request).expect("request hash");
        let policy = RecordingPolicy::default();
        let application = RuntimeChildGraphAncillaryApplication::new(
            policy.clone(),
            RecordingReviewer::default(),
            "graph",
            "repo",
        );
        assert_eq!(
            application
                .apply_cancellation(
                    &request,
                    context(
                        ContentHash::digest(b"substituted"),
                        ChildGraphAncillaryApplicationPhase::Initial,
                    ),
                )
                .await
                .expect_err("substitution"),
            application_error("idempotency_key_substitution")
        );
        let mut duplicate = request;
        duplicate.child_ids = vec![session(10), session(10)];
        assert_eq!(
            application
                .apply_cancellation(
                    &duplicate,
                    context(hash, ChildGraphAncillaryApplicationPhase::Initial),
                )
                .await
                .expect_err("duplicate"),
            application_error("duplicate_cancellation_child")
        );
        assert!(policy.0.lock().expect("policy").initial.is_empty());
    }
}
