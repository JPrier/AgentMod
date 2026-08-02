//! Durable ancillary application for generic child-graph execution.
//!
//! The coordinator owns canonical graph events. This adapter owns only the
//! restart-safe decision boundary for child creation, cancellation proposals,
//! and reviewer evidence. It persists one deterministic continuation per exact
//! request and never creates, cancels, or fabricates a child-session message.

use agentmod_primitives::{ContentHash, ContinuationId};
use async_trait::async_trait;
use serde::Serialize;
use std::future::Future;
use thiserror::Error;

use crate::{
    child_graph_turn::{
        ChildCancellationProposalOutcome, ChildCancellationProposalRequest,
        ChildCreationAuthorizationOutcome, ChildCreationAuthorizationRequest,
        ChildGraphAncillaryEffectPort, ChildGraphEffectError, ChildReviewEvidenceOutcome,
        ChildReviewEvidenceRequest,
    },
    continuation::{
        ChildGraphApprovalContinuation, ChildGraphApprovalOperation, ContinuationLogicError,
        ContinuationLogicPort, ContinuationPayload, ContinuationState, CreateContinuationCommand,
        LoadContinuationQuery,
    },
};

/// Invocation phase supplied to the consequential application boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildGraphAncillaryApplicationPhase {
    /// No durable approval has been requested yet.
    Initial,
    /// The exact durable continuation was approved and must be revalidated.
    AfterApproval,
}

/// Typed result from the normal policy/reviewer application pipeline.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildGraphAncillaryApplicationOutcome {
    /// The exact request was accepted.
    Applied {
        /// Required only for cancellation proposals; ignored otherwise.
        reference: Option<String>,
    },
    /// The exact request requires durable manual resolution.
    ApprovalRequired,
    /// Policy or reviewer validation terminally rejected the request.
    Denied {
        /// Stable redacted reason.
        code: String,
    },
    /// An external action may have happened without terminal evidence.
    Ambiguous {
        /// Stable bounded ambiguity reason.
        code: String,
    },
}

/// Exact idempotency and approval context for one ancillary application.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChildGraphAncillaryApplicationContext {
    /// Initial evaluation or post-approval revalidation.
    pub phase: ChildGraphAncillaryApplicationPhase,
    /// Hash of the complete bounded request.
    pub idempotency_key: ContentHash,
}

/// Turn-owned consequential policy and reviewer application seam.
///
/// Implementations must route consequential actions through existing runtime
/// use cases. Repeated calls with the same idempotency key must return the same
/// terminal receipt and may not repeat an external effect.
#[async_trait]
pub trait ChildGraphAncillaryApplicationPort: Send + Sync + 'static {
    /// Evaluates or revalidates one exact child-creation proposal.
    ///
    /// # Errors
    ///
    /// Returns a stable bounded failure without mutating graph state.
    async fn apply_creation(
        &self,
        request: &ChildCreationAuthorizationRequest,
        context: ChildGraphAncillaryApplicationContext,
    ) -> Result<ChildGraphAncillaryApplicationOutcome, ChildGraphAncillaryApplicationError>;

    /// Proposes cancellation through the normal consequential action path.
    ///
    /// # Errors
    ///
    /// Returns a stable bounded failure without mutating graph state.
    async fn apply_cancellation(
        &self,
        request: &ChildCancellationProposalRequest,
        context: ChildGraphAncillaryApplicationContext,
    ) -> Result<ChildGraphAncillaryApplicationOutcome, ChildGraphAncillaryApplicationError>;

    /// Validates exact reviewer evidence through its selected runtime boundary.
    ///
    /// # Errors
    ///
    /// Returns a stable bounded failure without mutating graph state.
    async fn apply_review(
        &self,
        request: &ChildReviewEvidenceRequest,
        context: ChildGraphAncillaryApplicationContext,
    ) -> Result<ChildGraphAncillaryApplicationOutcome, ChildGraphAncillaryApplicationError>;
}

/// Stable ancillary application failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("child graph ancillary application failed: {code}")]
pub struct ChildGraphAncillaryApplicationError {
    /// Stable bounded diagnostic code.
    pub code: String,
}

/// Production continuation-backed ancillary effect adapter.
pub struct ContinuationChildGraphAncillaryEffects<C, A> {
    continuations: C,
    application: A,
}

impl<C, A> ContinuationChildGraphAncillaryEffects<C, A> {
    /// Composes the existing durable continuation use case with the normal
    /// consequential/reviewer application boundary.
    #[must_use]
    pub const fn new(continuations: C, application: A) -> Self {
        Self {
            continuations,
            application,
        }
    }
}

#[async_trait]
impl<C, A> ChildGraphAncillaryEffectPort for ContinuationChildGraphAncillaryEffects<C, A>
where
    C: ContinuationLogicPort + Send + Sync + 'static,
    A: ChildGraphAncillaryApplicationPort,
{
    async fn authorize_creation(
        &self,
        request: ChildCreationAuthorizationRequest,
    ) -> Result<ChildCreationAuthorizationOutcome, ChildGraphEffectError> {
        let request_hash = creation_request_hash(&request)?;
        let payload = approval_payload(
            request.identity.work.clone(),
            request.identity.execution_plan_hash,
            request.identity.configuration_hash,
            ChildGraphApprovalOperation::CreateChild,
            request_hash,
            request.action_digest,
            request.contract.parent_session_id.to_string(),
        );
        match self
            .apply_or_recover(&payload, |context| {
                self.application.apply_creation(&request, context)
            })
            .await?
        {
            RecoveredAncillaryOutcome::Applied { .. } => {
                Ok(ChildCreationAuthorizationOutcome::Approved {
                    action_digest: request.action_digest,
                })
            }
            RecoveredAncillaryOutcome::Waiting(reference) => {
                Ok(ChildCreationAuthorizationOutcome::Waiting {
                    continuation_reference: reference,
                })
            }
            RecoveredAncillaryOutcome::Denied(code) => {
                Ok(ChildCreationAuthorizationOutcome::Denied { code })
            }
            RecoveredAncillaryOutcome::Ambiguous(code) => {
                Err(effect_error(&format!("creation_ambiguous:{code}")))
            }
        }
    }

    async fn propose_cancellation(
        &self,
        request: ChildCancellationProposalRequest,
    ) -> Result<ChildCancellationProposalOutcome, ChildGraphEffectError> {
        let request_hash = cancellation_request_hash(&request)?;
        let payload = approval_payload(
            request.work.clone(),
            request.execution_plan_hash,
            request.configuration_hash,
            ChildGraphApprovalOperation::CancelChildren,
            request_hash,
            request.projection_hash,
            request.session_id.to_string(),
        );
        match self
            .apply_or_recover(&payload, |context| {
                self.application.apply_cancellation(&request, context)
            })
            .await?
        {
            RecoveredAncillaryOutcome::Applied {
                reference: Some(reference),
            } => Ok(ChildCancellationProposalOutcome::Proposed {
                proposal_reference: reference,
            }),
            RecoveredAncillaryOutcome::Applied { reference: None } => {
                Err(effect_error("cancellation_receipt_missing"))
            }
            RecoveredAncillaryOutcome::Waiting(reference) => {
                Ok(ChildCancellationProposalOutcome::Waiting {
                    continuation_reference: reference,
                })
            }
            RecoveredAncillaryOutcome::Denied(code) => {
                Ok(ChildCancellationProposalOutcome::Denied { code })
            }
            RecoveredAncillaryOutcome::Ambiguous(code) => {
                Ok(ChildCancellationProposalOutcome::Ambiguous { code })
            }
        }
    }

    async fn validate_review_evidence(
        &self,
        request: ChildReviewEvidenceRequest,
    ) -> Result<ChildReviewEvidenceOutcome, ChildGraphEffectError> {
        let request_hash = review_request_hash(&request)?;
        let payload = approval_payload(
            request.work.clone(),
            request.execution_plan_hash,
            request.configuration_hash,
            ChildGraphApprovalOperation::ReviewEvidence,
            request_hash,
            request.routing.evidence_hash,
            request.session_id.to_string(),
        );
        match self
            .apply_or_recover(&payload, |context| {
                self.application.apply_review(&request, context)
            })
            .await?
        {
            RecoveredAncillaryOutcome::Applied { .. } => {
                Ok(ChildReviewEvidenceOutcome::Validated {
                    evidence_hash: request.routing.evidence_hash,
                })
            }
            RecoveredAncillaryOutcome::Waiting(reference) => {
                Ok(ChildReviewEvidenceOutcome::Waiting {
                    continuation_reference: reference,
                })
            }
            RecoveredAncillaryOutcome::Denied(code) => {
                Ok(ChildReviewEvidenceOutcome::Rejected { code })
            }
            RecoveredAncillaryOutcome::Ambiguous(code) => {
                Ok(ChildReviewEvidenceOutcome::Ambiguous { code })
            }
        }
    }
}

impl<C, A> ContinuationChildGraphAncillaryEffects<C, A>
where
    C: ContinuationLogicPort,
{
    async fn apply_or_recover<F, Fut>(
        &self,
        payload: &ChildGraphApprovalContinuation,
        apply: F,
    ) -> Result<RecoveredAncillaryOutcome, ChildGraphEffectError>
    where
        F: FnOnce(ChildGraphAncillaryApplicationContext) -> Fut,
        Fut: Future<
            Output = Result<
                ChildGraphAncillaryApplicationOutcome,
                ChildGraphAncillaryApplicationError,
            >,
        >,
    {
        validate_approval_payload(payload)?;
        let continuation_id = child_graph_continuation_id(payload);
        let session_id = payload.session_id.clone();
        let existing = self
            .continuations
            .load_optional_continuation(LoadContinuationQuery {
                session_id: session_id.clone(),
                id: continuation_id,
            })
            .map_err(continuation_error)?;
        let phase = match existing {
            None => ChildGraphAncillaryApplicationPhase::Initial,
            Some(existing) => {
                if existing.payload
                    != ContinuationPayload::ChildGraphApproval(Box::new(payload.clone()))
                {
                    return Err(effect_error("continuation_payload_substitution"));
                }
                match existing.state {
                    ContinuationState::Pending => {
                        return Ok(RecoveredAncillaryOutcome::Waiting(
                            continuation_id.to_string(),
                        ));
                    }
                    ContinuationState::Resumed => {
                        ChildGraphAncillaryApplicationPhase::AfterApproval
                    }
                    ContinuationState::Cancelled => {
                        return Ok(RecoveredAncillaryOutcome::Denied(String::from(
                            "child_graph_approval_denied",
                        )));
                    }
                    ContinuationState::Expired => {
                        return Ok(RecoveredAncillaryOutcome::Denied(String::from(
                            "child_graph_approval_expired",
                        )));
                    }
                }
            }
        };
        let outcome = apply(ChildGraphAncillaryApplicationContext {
            phase,
            idempotency_key: payload.request_hash,
        })
        .await
        .map_err(|error| application_error(&error))?;
        match outcome {
            ChildGraphAncillaryApplicationOutcome::Applied { reference } => {
                Ok(RecoveredAncillaryOutcome::Applied { reference })
            }
            ChildGraphAncillaryApplicationOutcome::ApprovalRequired
                if phase == ChildGraphAncillaryApplicationPhase::Initial =>
            {
                self.continuations
                    .create_continuation(CreateContinuationCommand {
                        session_id,
                        id: continuation_id,
                        wake_condition: crate::continuation::ContinuationWakeCondition::Manual,
                        payload: ContinuationPayload::ChildGraphApproval(Box::new(payload.clone())),
                        expires_at: None,
                    })
                    .map_err(continuation_error)?;
                Ok(RecoveredAncillaryOutcome::Waiting(
                    continuation_id.to_string(),
                ))
            }
            ChildGraphAncillaryApplicationOutcome::ApprovalRequired => {
                Err(effect_error("approval_cycle_after_resolution"))
            }
            ChildGraphAncillaryApplicationOutcome::Denied { code } => {
                validate_code(&code)?;
                Ok(RecoveredAncillaryOutcome::Denied(code))
            }
            ChildGraphAncillaryApplicationOutcome::Ambiguous { code } => {
                validate_code(&code)?;
                Ok(RecoveredAncillaryOutcome::Ambiguous(code))
            }
        }
    }
}

enum RecoveredAncillaryOutcome {
    Applied { reference: Option<String> },
    Waiting(String),
    Denied(String),
    Ambiguous(String),
}

fn approval_payload(
    work: crate::node_execution::NodeWorkIdentity,
    execution_plan_hash: ContentHash,
    adapter_configuration_reference: ContentHash,
    operation: ChildGraphApprovalOperation,
    request_hash: ContentHash,
    subject_hash: ContentHash,
    session_id: String,
) -> ChildGraphApprovalContinuation {
    ChildGraphApprovalContinuation {
        session_id,
        operation,
        run_id: work.run_id,
        node_id: work.node_id,
        branch_path: work.branch_path,
        attempt: work.attempt,
        loop_iteration: work.loop_iteration,
        step: work.step,
        execution_plan_hash,
        adapter_configuration_reference,
        request_hash,
        subject_hash,
    }
}

fn creation_request_hash(
    request: &ChildCreationAuthorizationRequest,
) -> Result<ContentHash, ChildGraphEffectError> {
    hash_request(&(
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
) -> Result<ContentHash, ChildGraphEffectError> {
    let mut child_ids = request
        .child_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    child_ids.sort();
    if child_ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(effect_error("duplicate_cancellation_child"));
    }
    hash_request(&(
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
) -> Result<ContentHash, ChildGraphEffectError> {
    hash_request(&(
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

fn hash_request(value: &impl Serialize) -> Result<ContentHash, ChildGraphEffectError> {
    serde_json::to_vec(value)
        .map(|encoded| ContentHash::digest(&encoded))
        .map_err(|_| effect_error("request_encoding"))
}

pub(crate) fn child_graph_continuation_id(
    payload: &ChildGraphApprovalContinuation,
) -> ContinuationId {
    let operation = match payload.operation {
        ChildGraphApprovalOperation::CreateChild => "create_child",
        ChildGraphApprovalOperation::CancelChildren => "cancel_children",
        ChildGraphApprovalOperation::ReviewEvidence => "review_evidence",
    };
    let digest = ContentHash::digest(
        format!(
            "agentmod.child-graph.continuation.v1:{}:{operation}:{}",
            payload.session_id, payload.request_hash
        )
        .as_bytes(),
    );
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    ContinuationId::from_uuid(uuid::Uuid::from_bytes(bytes))
}

fn validate_approval_payload(
    payload: &ChildGraphApprovalContinuation,
) -> Result<(), ChildGraphEffectError> {
    let invalid_reference = |value: &str, maximum: usize| {
        value.trim().is_empty() || value.len() > maximum || value.chars().any(char::is_control)
    };
    if invalid_reference(&payload.session_id, 128)
        || invalid_reference(&payload.run_id, 256)
        || invalid_reference(&payload.node_id, 256)
        || payload.branch_path.len() > 64
        || payload
            .branch_path
            .iter()
            .any(|branch| invalid_reference(branch, 128))
        || payload.attempt == 0
        || payload.step == 0
        || payload.execution_plan_hash == ContentHash::from_bytes([0; 32])
        || payload.adapter_configuration_reference == ContentHash::from_bytes([0; 32])
        || payload.request_hash == ContentHash::from_bytes([0; 32])
        || payload.subject_hash == ContentHash::from_bytes([0; 32])
    {
        Err(effect_error("invalid_approval_payload"))
    } else {
        Ok(())
    }
}

fn validate_code(code: &str) -> Result<(), ChildGraphEffectError> {
    if code.trim().is_empty() || code.len() > 1_024 || code.chars().any(char::is_control) {
        Err(effect_error("invalid_application_code"))
    } else {
        Ok(())
    }
}

fn continuation_error(_error: ContinuationLogicError) -> ChildGraphEffectError {
    effect_error("continuation_boundary_failure")
}

fn application_error(error: &ChildGraphAncillaryApplicationError) -> ChildGraphEffectError {
    effect_error(&format!("application:{}", error.code))
}

fn effect_error(code: &str) -> ChildGraphEffectError {
    let bounded =
        if code.trim().is_empty() || code.len() > 1_024 || code.chars().any(char::is_control) {
            String::from("child_graph_ancillary_failure")
        } else {
            code.to_owned()
        };
    ChildGraphEffectError { code: bounded }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        str::FromStr,
        sync::{Arc, Mutex},
    };

    use agentmod_primitives::{Sequence, SessionId};
    use agentmod_runtime_data::continuation::{ContinuationDataPort, local_continuation_data};
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::{
        child_graph_execution::{ReviewDisposition, ReviewRoutingProposal},
        continuation::{
            ApprovalDisposition, ContinuationLogic, ContinuationTerminalDisposition,
            ResolveApprovalCommand, TransitionContinuationTerminalCommand,
        },
        node_execution::NodeWorkIdentity,
        session::{GenericChildExecutionIdentity, GenericChildSpawnContract},
    };

    use super::*;

    #[derive(Default)]
    struct ApplicationState {
        initial: Vec<ContentHash>,
        post_approval_attempts: Vec<ContentHash>,
        applied_once: BTreeSet<ContentHash>,
    }

    #[derive(Clone, Default)]
    struct RecordingApplication {
        state: Arc<Mutex<ApplicationState>>,
    }

    impl RecordingApplication {
        fn apply(
            &self,
            context: ChildGraphAncillaryApplicationContext,
            reference: Option<String>,
        ) -> ChildGraphAncillaryApplicationOutcome {
            let mut state = self.state.lock().expect("application state");
            match context.phase {
                ChildGraphAncillaryApplicationPhase::Initial => {
                    state.initial.push(context.idempotency_key);
                    ChildGraphAncillaryApplicationOutcome::ApprovalRequired
                }
                ChildGraphAncillaryApplicationPhase::AfterApproval => {
                    state.post_approval_attempts.push(context.idempotency_key);
                    state.applied_once.insert(context.idempotency_key);
                    ChildGraphAncillaryApplicationOutcome::Applied { reference }
                }
            }
        }
    }

    #[async_trait]
    impl ChildGraphAncillaryApplicationPort for RecordingApplication {
        async fn apply_creation(
            &self,
            _request: &ChildCreationAuthorizationRequest,
            context: ChildGraphAncillaryApplicationContext,
        ) -> Result<ChildGraphAncillaryApplicationOutcome, ChildGraphAncillaryApplicationError>
        {
            Ok(self.apply(context, None))
        }

        async fn apply_cancellation(
            &self,
            _request: &ChildCancellationProposalRequest,
            context: ChildGraphAncillaryApplicationContext,
        ) -> Result<ChildGraphAncillaryApplicationOutcome, ChildGraphAncillaryApplicationError>
        {
            Ok(self.apply(
                context,
                Some(format!("cancellation:{}", context.idempotency_key)),
            ))
        }

        async fn apply_review(
            &self,
            _request: &ChildReviewEvidenceRequest,
            context: ChildGraphAncillaryApplicationContext,
        ) -> Result<ChildGraphAncillaryApplicationOutcome, ChildGraphAncillaryApplicationError>
        {
            Ok(self.apply(context, None))
        }
    }

    fn session_id(value: u128) -> SessionId {
        SessionId::from_uuid(Uuid::from_u128(value))
    }

    fn work(node_id: &str) -> NodeWorkIdentity {
        NodeWorkIdentity {
            run_id: String::from("run_immutable"),
            node_id: node_id.to_owned(),
            branch_path: vec![String::from("fanout"), String::from("member_a")],
            attempt: 2,
            loop_iteration: 3,
            step: 4,
        }
    }

    fn creation_request() -> ChildCreationAuthorizationRequest {
        let task = serde_json::json!({"instruction": "inspect"});
        ChildCreationAuthorizationRequest {
            identity: GenericChildExecutionIdentity {
                execution_id: String::from("generic-child:proposal"),
                work: work("spawn"),
                execution_plan_hash: ContentHash::digest(b"plan"),
                configuration_hash: ContentHash::digest(b"spawn-config"),
                proposal_hash: ContentHash::digest(b"proposal"),
                task_id: String::from("task-a"),
            },
            contract: GenericChildSpawnContract {
                parent_session_id: session_id(1),
                task_hash: ContentHash::digest(&serde_json::to_vec(&task).expect("encode task")),
                task,
                inherited_provider: None,
                inherited_model: None,
                inherited_mcp: None,
                tool_groups: BTreeSet::new(),
                depth: 1,
                context_budget_tokens: 500,
                cost_budget_micros: 10_000,
                workspace: serde_json::json!({"mode": "shared_read_only"}),
                artifact_references: BTreeSet::new(),
                security_classification: String::from("internal"),
                approval_required: true,
                proposal_zero_json: String::from("{\"proposal_hash\":\"zero\"}"),
            },
            child_style: String::from("worker@1.0.0"),
            token_budget: 1_000,
            action_digest: ContentHash::digest(b"create-action"),
            proposed_at: Sequence::new(7).expect("sequence"),
        }
    }

    fn cancellation_request() -> ChildCancellationProposalRequest {
        ChildCancellationProposalRequest {
            session_id: session_id(1),
            work: work("wait"),
            execution_plan_hash: ContentHash::digest(b"plan"),
            configuration_hash: ContentHash::digest(b"wait-config"),
            projection_hash: ContentHash::digest(b"projection"),
            reason: String::from("child_wait_timeout"),
            child_ids: vec![session_id(10), session_id(11)],
        }
    }

    fn review_request() -> ChildReviewEvidenceRequest {
        ChildReviewEvidenceRequest {
            session_id: session_id(1),
            work: work("review"),
            execution_plan_hash: ContentHash::digest(b"plan"),
            configuration_hash: ContentHash::digest(b"review-config"),
            routing: ReviewRoutingProposal {
                disposition: ReviewDisposition::Revision,
                destination_node_id: String::from("revise"),
                current_revision: 0,
                next_revision: Some(1),
                rejected_task_ids: vec![String::from("task-a")],
                findings: Vec::new(),
                evidence_hash: ContentHash::digest(b"review-evidence"),
            },
        }
    }

    fn logic(directory: &TempDir) -> ContinuationLogic<impl ContinuationDataPort + use<>> {
        ContinuationLogic::new(local_continuation_data(directory.path().to_path_buf()))
    }

    fn approve(
        logic: &impl ContinuationLogicPort,
        continuation_reference: &str,
        approved: bool,
    ) -> ApprovalDisposition {
        logic
            .resolve_approval(ResolveApprovalCommand {
                session_id: session_id(1).to_string(),
                id: ContinuationId::from_str(continuation_reference).expect("continuation id"),
                approved,
            })
            .expect("resolve approval")
            .disposition
    }

    #[tokio::test]
    async fn creation_approval_survives_restart_and_reuses_exact_idempotency_key() {
        let directory = TempDir::new().expect("temporary continuation store");
        let application = RecordingApplication::default();
        let request = creation_request();
        let first =
            ContinuationChildGraphAncillaryEffects::new(logic(&directory), application.clone())
                .authorize_creation(request.clone())
                .await
                .expect("initial decision");
        let ChildCreationAuthorizationOutcome::Waiting {
            continuation_reference,
        } = first
        else {
            panic!("approval continuation")
        };

        let restarted =
            ContinuationChildGraphAncillaryEffects::new(logic(&directory), application.clone());
        assert_eq!(
            restarted
                .authorize_creation(request.clone())
                .await
                .expect("pending recovery"),
            ChildCreationAuthorizationOutcome::Waiting {
                continuation_reference: continuation_reference.clone()
            }
        );
        assert_eq!(
            approve(&logic(&directory), &continuation_reference, true),
            ApprovalDisposition::Approved
        );
        assert_eq!(
            restarted
                .authorize_creation(request.clone())
                .await
                .expect("approved recovery"),
            ChildCreationAuthorizationOutcome::Approved {
                action_digest: request.action_digest
            }
        );
        assert_eq!(
            restarted
                .authorize_creation(request)
                .await
                .expect("duplicate approved recovery"),
            ChildCreationAuthorizationOutcome::Approved {
                action_digest: ContentHash::digest(b"create-action")
            }
        );

        let state = application.state.lock().expect("application state");
        assert_eq!(state.initial.len(), 1);
        assert_eq!(state.post_approval_attempts.len(), 2);
        assert_eq!(state.applied_once.len(), 1);
        assert_eq!(state.initial[0], state.post_approval_attempts[0]);
    }

    #[tokio::test]
    async fn cancellation_and_review_resume_through_exact_durable_decisions() {
        let directory = TempDir::new().expect("temporary continuation store");
        let application = RecordingApplication::default();
        let adapter =
            ContinuationChildGraphAncillaryEffects::new(logic(&directory), application.clone());

        let cancellation = cancellation_request();
        let ChildCancellationProposalOutcome::Waiting {
            continuation_reference: cancellation_id,
        } = adapter
            .propose_cancellation(cancellation.clone())
            .await
            .expect("cancellation decision")
        else {
            panic!("cancellation continuation")
        };
        approve(&logic(&directory), &cancellation_id, true);
        assert!(matches!(
            adapter
                .propose_cancellation(cancellation)
                .await
                .expect("cancellation recovery"),
            ChildCancellationProposalOutcome::Proposed { proposal_reference }
                if proposal_reference.starts_with("cancellation:")
        ));

        let review = review_request();
        let ChildReviewEvidenceOutcome::Waiting {
            continuation_reference: review_id,
        } = adapter
            .validate_review_evidence(review.clone())
            .await
            .expect("review decision")
        else {
            panic!("review continuation")
        };
        approve(&logic(&directory), &review_id, true);
        assert_eq!(
            adapter
                .validate_review_evidence(review.clone())
                .await
                .expect("review recovery"),
            ChildReviewEvidenceOutcome::Validated {
                evidence_hash: review.routing.evidence_hash
            }
        );

        let state = application.state.lock().expect("application state");
        assert_eq!(state.initial.len(), 2);
        assert_eq!(state.post_approval_attempts.len(), 2);
        assert_eq!(state.applied_once.len(), 2);
    }

    #[tokio::test]
    async fn denial_is_terminal_and_never_enters_post_approval_application() {
        let directory = TempDir::new().expect("temporary continuation store");
        let application = RecordingApplication::default();
        let adapter =
            ContinuationChildGraphAncillaryEffects::new(logic(&directory), application.clone());
        let request = creation_request();
        let ChildCreationAuthorizationOutcome::Waiting {
            continuation_reference,
        } = adapter
            .authorize_creation(request.clone())
            .await
            .expect("initial decision")
        else {
            panic!("approval continuation")
        };
        assert_eq!(
            approve(&logic(&directory), &continuation_reference, false),
            ApprovalDisposition::Denied
        );
        assert_eq!(
            adapter
                .authorize_creation(request)
                .await
                .expect("denied recovery"),
            ChildCreationAuthorizationOutcome::Denied {
                code: String::from("child_graph_approval_denied")
            }
        );
        let state = application.state.lock().expect("application state");
        assert_eq!(state.initial.len(), 1);
        assert!(state.post_approval_attempts.is_empty());
        assert!(state.applied_once.is_empty());
    }

    #[tokio::test]
    async fn expiry_is_terminal_and_never_reenters_application() {
        let directory = TempDir::new().expect("temporary continuation store");
        let application = RecordingApplication::default();
        let adapter =
            ContinuationChildGraphAncillaryEffects::new(logic(&directory), application.clone());
        let request = creation_request();
        let ChildCreationAuthorizationOutcome::Waiting {
            continuation_reference,
        } = adapter
            .authorize_creation(request.clone())
            .await
            .expect("initial decision")
        else {
            panic!("approval continuation")
        };
        logic(&directory)
            .transition_terminal(TransitionContinuationTerminalCommand {
                session_id: session_id(1).to_string(),
                id: ContinuationId::from_str(&continuation_reference).expect("continuation id"),
                disposition: ContinuationTerminalDisposition::Expired,
            })
            .expect("expire continuation");
        assert_eq!(
            adapter
                .authorize_creation(request)
                .await
                .expect("expired recovery"),
            ChildCreationAuthorizationOutcome::Denied {
                code: String::from("child_graph_approval_expired")
            }
        );
        let state = application.state.lock().expect("application state");
        assert_eq!(state.initial.len(), 1);
        assert!(state.post_approval_attempts.is_empty());
        assert!(state.applied_once.is_empty());
    }

    #[tokio::test]
    async fn persisted_payload_substitution_fails_before_policy_or_effect_application() {
        let directory = TempDir::new().expect("temporary continuation store");
        let application = RecordingApplication::default();
        let request = creation_request();
        let request_hash = creation_request_hash(&request).expect("request hash");
        let expected = approval_payload(
            request.identity.work.clone(),
            request.identity.execution_plan_hash,
            request.identity.configuration_hash,
            ChildGraphApprovalOperation::CreateChild,
            request_hash,
            request.action_digest,
            request.contract.parent_session_id.to_string(),
        );
        let mut substituted = expected.clone();
        substituted.subject_hash = ContentHash::digest(b"substituted-action");
        logic(&directory)
            .create_continuation(CreateContinuationCommand {
                session_id: expected.session_id.clone(),
                id: child_graph_continuation_id(&expected),
                wake_condition: crate::continuation::ContinuationWakeCondition::Manual,
                payload: ContinuationPayload::ChildGraphApproval(Box::new(substituted)),
                expires_at: None,
            })
            .expect("persist substituted fixture");

        let error =
            ContinuationChildGraphAncillaryEffects::new(logic(&directory), application.clone())
                .authorize_creation(request)
                .await
                .expect_err("substitution must fail");
        assert_eq!(error.code, "continuation_payload_substitution");
        let state = application.state.lock().expect("application state");
        assert!(state.initial.is_empty());
        assert!(state.post_approval_attempts.is_empty());
        assert!(state.applied_once.is_empty());
    }
}
