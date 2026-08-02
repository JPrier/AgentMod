//! Pure canonical application planning for style-independent child graph nodes.
//!
//! The planner consumes replay state and the bounded output of
//! `child_graph_execution`. It returns typed canonical event proposals only;
//! it never invokes child sessions, providers, continuations, or journals.

use std::collections::{BTreeMap, BTreeSet};

use agentmod_primitives::{ContentHash, Sequence, SessionId};
use thiserror::Error;

use crate::{
    child_graph_execution::{
        ChildGraphNodeOutcome, ChildSpawnProposal, ChildWaitFailureDisposition,
        ChildWaitProjection, ReviewDisposition, ReviewRoutingProposal,
    },
    node_execution::{NativeExecutorKey, NodeWorkIdentity, native_executor_key},
    session::{
        ChildAgentState, ChildWaitProjectedEvent, GenericChildCreatedEvent,
        GenericChildCreationApprovedEvent, GenericChildCreationDispatchedEvent,
        GenericChildCreationProposedEvent, GenericChildExecutionIdentity,
        GenericChildSpawnContract, GenericChildTerminalDisposition, GenericChildTerminalEvent,
        GenericChildTerminalReceipt, GenericChildWaitDisposition, GenericChildWaitFailure,
        GenericChildWaitSuccess, GenericReviewRoutedEvent, GenericReviewRoutingEvidence,
        GenericReviewerFinding, RuntimeCommittedEvent, SessionReducerError, SessionState,
        generic_child_action_digest, generic_child_dispatch_hash, generic_child_link_hash,
        generic_child_terminal_receipt_hash, generic_child_wait_projection_hash,
        generic_review_application_hash,
    },
    workspace::WorkspaceLeaseContract,
};

/// External receipts already obtained by normal runtime use cases.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChildGraphApplicationEvidence {
    /// Exact policy-approved action digests keyed by task ID.
    pub approvals: BTreeMap<String, ContentHash>,
    /// Atomic child-creation receipts keyed by task ID.
    pub creations: BTreeMap<String, ChildCreationEvidence>,
    /// Verified terminal child receipts keyed by task ID.
    pub terminals: BTreeMap<String, ChildTerminalEvidence>,
}

/// Atomic child creation evidence supplied after the creation outbox.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildCreationEvidence {
    /// Runtime-managed child session.
    pub child_session_id: SessionId,
    /// Parent proposal sequence retained by the child link.
    pub parent_action_sequence: Sequence,
    /// Exact immutable parent/child link hash.
    pub child_link_hash: ContentHash,
    /// Exact immutable workspace lease committed in both journals.
    pub workspace_lease: WorkspaceLeaseContract,
}

/// Verified terminal child evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildTerminalEvidence {
    /// Runtime-managed child session.
    pub child_session_id: SessionId,
    /// Verified terminal child journal head.
    pub child_head_sequence: Sequence,
    /// Completed, failed, or cancelled disposition.
    pub disposition: GenericChildTerminalDisposition,
    /// Stable result reference on success.
    pub result_reference: Option<String>,
    /// Immutable result artifacts.
    pub artifact_references: BTreeSet<String>,
    /// Stable redacted failure code.
    pub failure_code: Option<String>,
    /// Hash of the exact terminal receipt.
    pub receipt_hash: ContentHash,
}

/// Pure application command bound to one immutable Graph-B node.
#[derive(Clone, Debug, PartialEq)]
pub struct PlanChildGraphApplicationCommand {
    /// Canonical parent session.
    pub session_id: SessionId,
    /// Exact node work producing the outcome.
    pub work: NodeWorkIdentity,
    /// Immutable execution-plan hash.
    pub execution_plan_hash: ContentHash,
    /// Complete compiled-node/configuration hash.
    pub configuration_hash: ContentHash,
    /// Pure style-independent Graph-B outcome.
    pub outcome: ChildGraphNodeOutcome,
    /// Already obtained policy or child-boundary evidence.
    pub evidence: ChildGraphApplicationEvidence,
}

/// Stable next canonical events. An empty vector means exact idempotent replay.
#[derive(Clone, Debug, PartialEq)]
pub struct ChildGraphApplicationPlan {
    /// Stable next canonical events, empty for exact idempotent replay.
    pub events: Vec<RuntimeCommittedEvent>,
}

/// Plans the next canonical application phase without performing effects.
///
/// # Errors
///
/// Fails closed when the session/executor identity, pure proposal, replayed
/// phase, or supplied external receipt differs from the immutable contract.
pub fn plan_child_graph_application(
    state: &SessionState,
    command: &PlanChildGraphApplicationCommand,
) -> Result<ChildGraphApplicationPlan, ChildGraphApplicationError> {
    let expected_executor = match &command.outcome {
        ChildGraphNodeOutcome::Spawn { .. } => NativeExecutorKey::ChildSpawn,
        ChildGraphNodeOutcome::Wait(_) => NativeExecutorKey::ChildWait,
        ChildGraphNodeOutcome::Review(_) => NativeExecutorKey::Review,
    };
    validate_command(state, command, expected_executor)?;
    let events = match &command.outcome {
        ChildGraphNodeOutcome::Spawn { proposals } => plan_spawn(state, command, proposals)?,
        ChildGraphNodeOutcome::Wait(projection) => plan_wait(state, command, projection)?,
        ChildGraphNodeOutcome::Review(routing) => plan_review(state, command, routing)?,
    };
    Ok(ChildGraphApplicationPlan { events })
}

fn validate_command(
    state: &SessionState,
    command: &PlanChildGraphApplicationCommand,
    expected_executor: NativeExecutorKey,
) -> Result<(), ChildGraphApplicationError> {
    let execution = state
        .style_execution
        .as_ref()
        .ok_or(ChildGraphApplicationError::Identity)?;
    let contract = execution
        .execution_contract
        .as_deref()
        .ok_or(ChildGraphApplicationError::Identity)?;
    let resolution = contract
        .node_executors
        .iter()
        .find(|resolution| resolution.node_id == command.work.node_id)
        .ok_or(ChildGraphApplicationError::Identity)?;
    if state.id != command.session_id
        || contract.run_id != command.work.run_id
        || contract.execution_plan_hash != command.execution_plan_hash
        || resolution.adapter_configuration_reference != command.configuration_hash
        || native_executor_key(resolution) != Ok(expected_executor)
        || command.execution_plan_hash == ContentHash::from_bytes([0; 32])
        || command.configuration_hash == ContentHash::from_bytes([0; 32])
        || (expected_executor != NativeExecutorKey::ChildSpawn
            && !command_work_is_active(state, &command.work))
    {
        return Err(ChildGraphApplicationError::Identity);
    }
    Ok(())
}

fn command_work_is_active(state: &SessionState, work: &NodeWorkIdentity) -> bool {
    state
        .style_execution
        .as_ref()
        .is_some_and(|execution| crate::session::node_work_is_active(execution, work))
}

#[allow(
    clippy::too_many_lines,
    reason = "the pure planner keeps each recoverable child outbox phase explicit"
)]
fn plan_spawn(
    state: &SessionState,
    command: &PlanChildGraphApplicationCommand,
    proposals: &[ChildSpawnProposal],
) -> Result<Vec<RuntimeCommittedEvent>, ChildGraphApplicationError> {
    let mut ordered = proposals.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.task_id.cmp(&right.task_id));
    if ordered.is_empty()
        || ordered
            .windows(2)
            .any(|pair| pair[0].task_id == pair[1].task_id)
        || ordered.iter().any(|proposal| {
            proposal.parent_session_id != command.session_id
                || proposal.work != command.work
                || !proposal.approval_required
        })
    {
        return Err(ChildGraphApplicationError::Proposal);
    }
    let mut proposed = Vec::new();
    let mut approved = Vec::new();
    let mut dispatched = Vec::new();
    let mut created = Vec::new();
    let mut terminal = Vec::new();
    for proposal in ordered {
        let (identity, contract) = generic_spawn_event_parts(command, proposal)?;
        let existing = state.child_agents.get(&identity.execution_id);
        let Some(record) = existing else {
            if !command_work_is_active(state, &command.work) {
                return Err(ChildGraphApplicationError::Identity);
            }
            proposed.push(RuntimeCommittedEvent::GenericChildCreationProposed(
                Box::new(GenericChildCreationProposedEvent {
                    identity,
                    proposal: Box::new(contract),
                    child_style: proposal.child_style.clone(),
                    token_budget: proposal.token_budget,
                }),
            ));
            continue;
        };
        if record.generic_identity.as_deref() != Some(&identity)
            || record.generic.as_deref() != Some(&contract)
            || record.child_style != proposal.child_style
            || record.token_budget != proposal.token_budget
        {
            return Err(ChildGraphApplicationError::Substitution);
        }
        let action_digest = generic_child_action_digest(
            &identity,
            &contract,
            &proposal.child_style,
            proposal.token_budget,
        )
        .map_err(ChildGraphApplicationError::Reducer)?;
        match record.state {
            ChildAgentState::Proposed => {
                if !command_work_is_active(state, &command.work) {
                    return Err(ChildGraphApplicationError::Identity);
                }
                if let Some(observed) = command.evidence.approvals.get(&proposal.task_id) {
                    if *observed != action_digest {
                        return Err(ChildGraphApplicationError::Substitution);
                    }
                    approved.push(RuntimeCommittedEvent::GenericChildCreationApproved(
                        GenericChildCreationApprovedEvent {
                            identity,
                            action_digest,
                        },
                    ));
                }
            }
            ChildAgentState::Approved => {
                if !command_work_is_active(state, &command.work) {
                    return Err(ChildGraphApplicationError::Identity);
                }
                let dispatch_hash = generic_child_dispatch_hash(&identity, action_digest)
                    .map_err(ChildGraphApplicationError::Reducer)?;
                dispatched.push(RuntimeCommittedEvent::GenericChildCreationDispatched(
                    GenericChildCreationDispatchedEvent {
                        identity,
                        action_digest,
                        dispatch_hash,
                    },
                ));
            }
            ChildAgentState::Dispatched => {
                if !command_work_is_active(state, &command.work) {
                    return Err(ChildGraphApplicationError::Identity);
                }
                if let Some(receipt) = command.evidence.creations.get(&proposal.task_id) {
                    let dispatch_hash = generic_child_dispatch_hash(&identity, action_digest)
                        .map_err(ChildGraphApplicationError::Reducer)?;
                    let expected_link = generic_child_link_hash(
                        &identity,
                        receipt.child_session_id,
                        receipt.parent_action_sequence,
                        &proposal.child_style,
                    )
                    .map_err(ChildGraphApplicationError::Reducer)?;
                    if receipt.child_link_hash != expected_link {
                        return Err(ChildGraphApplicationError::Substitution);
                    }
                    created.push(RuntimeCommittedEvent::GenericChildCreated(
                        GenericChildCreatedEvent {
                            identity,
                            action_digest,
                            dispatch_hash,
                            child_session_id: receipt.child_session_id,
                            parent_action_sequence: receipt.parent_action_sequence,
                            child_style: proposal.child_style.clone(),
                            child_link_hash: receipt.child_link_hash,
                            workspace_lease: Box::new(receipt.workspace_lease.clone()),
                        },
                    ));
                }
            }
            ChildAgentState::Active => {
                if let Some(receipt) = command.evidence.terminals.get(&proposal.task_id) {
                    let mut canonical = GenericChildTerminalReceipt {
                        disposition: receipt.disposition,
                        result_reference: receipt.result_reference.clone(),
                        artifact_references: receipt.artifact_references.clone(),
                        failure_code: receipt.failure_code.clone(),
                        receipt_hash: ContentHash::from_bytes([0; 32]),
                    };
                    canonical.receipt_hash = generic_child_terminal_receipt_hash(
                        &identity,
                        receipt.child_session_id,
                        receipt.child_head_sequence,
                        &canonical,
                    )
                    .map_err(ChildGraphApplicationError::Reducer)?;
                    if canonical.receipt_hash != receipt.receipt_hash
                        || record.child_session_id != Some(receipt.child_session_id)
                    {
                        return Err(ChildGraphApplicationError::Substitution);
                    }
                    terminal.push(RuntimeCommittedEvent::GenericChildTerminal(Box::new(
                        GenericChildTerminalEvent {
                            identity,
                            child_session_id: receipt.child_session_id,
                            child_head_sequence: receipt.child_head_sequence,
                            receipt: Box::new(canonical),
                        },
                    )));
                }
            }
            ChildAgentState::Completed | ChildAgentState::Failed | ChildAgentState::Cancelled => {}
        }
    }
    Ok(if !proposed.is_empty() {
        proposed
    } else if !approved.is_empty() {
        approved
    } else if !dispatched.is_empty() {
        dispatched
    } else if !created.is_empty() {
        created
    } else {
        terminal
    })
}

fn generic_spawn_event_parts(
    command: &PlanChildGraphApplicationCommand,
    proposal: &ChildSpawnProposal,
) -> Result<(GenericChildExecutionIdentity, GenericChildSpawnContract), ChildGraphApplicationError>
{
    let mut zeroed = proposal.clone();
    zeroed.proposal_hash = ContentHash::from_bytes([0; 32]);
    let proposal_zero_json =
        serde_json::to_string(&zeroed).map_err(|_| ChildGraphApplicationError::Proposal)?;
    if ContentHash::digest(proposal_zero_json.as_bytes()) != proposal.proposal_hash {
        return Err(ChildGraphApplicationError::Proposal);
    }
    let workspace = serde_json::to_value(&proposal.workspace)
        .map_err(|_| ChildGraphApplicationError::Proposal)?;
    let security = serde_json::to_value(proposal.security_classification)
        .map_err(|_| ChildGraphApplicationError::Proposal)?
        .as_str()
        .ok_or(ChildGraphApplicationError::Proposal)?
        .to_owned();
    Ok((
        GenericChildExecutionIdentity {
            execution_id: format!("generic-child:{}", proposal.proposal_hash),
            work: proposal.work.clone(),
            execution_plan_hash: command.execution_plan_hash,
            configuration_hash: command.configuration_hash,
            proposal_hash: proposal.proposal_hash,
            task_id: proposal.task_id.clone(),
        },
        GenericChildSpawnContract {
            parent_session_id: proposal.parent_session_id,
            task: proposal.task.clone(),
            task_hash: proposal.task_hash,
            inherited_provider: proposal.inherited_provider.clone(),
            inherited_model: proposal.inherited_model.clone(),
            inherited_mcp: proposal.inherited_mcp.clone(),
            tool_groups: proposal.tool_groups.clone(),
            depth: proposal.depth,
            context_budget_tokens: proposal.context_budget_tokens,
            cost_budget_micros: proposal.cost_budget_micros,
            workspace,
            artifact_references: proposal.artifact_references.clone(),
            security_classification: security,
            approval_required: proposal.approval_required,
            proposal_zero_json,
        },
    ))
}

#[allow(
    clippy::too_many_lines,
    reason = "one pure planner keeps every wait disposition and the exact replay/substitution rules adjacent"
)]
fn plan_wait(
    state: &SessionState,
    command: &PlanChildGraphApplicationCommand,
    projection: &ChildWaitProjection,
) -> Result<Vec<RuntimeCommittedEvent>, ChildGraphApplicationError> {
    let terminals = plan_wait_terminals(state, command)?;
    if !terminals.is_empty() {
        return Ok(terminals);
    }
    let mut event = match projection {
        ChildWaitProjection::Waiting {
            successful,
            pending,
            remaining_ms,
            cancellation_recorded,
        } => ChildWaitProjectedEvent {
            work: command.work.clone(),
            execution_plan_hash: command.execution_plan_hash,
            configuration_hash: command.configuration_hash,
            disposition: GenericChildWaitDisposition::Waiting,
            successful: map_successes(successful),
            unsuccessful: Vec::new(),
            pending: pending.clone(),
            remaining_ms: Some(*remaining_ms),
            cancellation_recorded: *cancellation_recorded,
            failure_code: None,
            cancel_children: Vec::new(),
            detached: false,
            result_hash: None,
            projection_hash: ContentHash::from_bytes([0; 32]),
        },
        ChildWaitProjection::Completed {
            successful,
            unsuccessful,
            result_hash,
        } => ChildWaitProjectedEvent {
            work: command.work.clone(),
            execution_plan_hash: command.execution_plan_hash,
            configuration_hash: command.configuration_hash,
            disposition: GenericChildWaitDisposition::Completed,
            successful: map_successes(successful),
            unsuccessful: unsuccessful
                .iter()
                .map(|failure| GenericChildWaitFailure {
                    child_id: failure.child_id,
                    task_id: failure.task_id.clone(),
                    disposition: match failure.disposition {
                        ChildWaitFailureDisposition::Failed => {
                            GenericChildTerminalDisposition::Failed
                        }
                        ChildWaitFailureDisposition::Cancelled => {
                            GenericChildTerminalDisposition::Cancelled
                        }
                    },
                    code: failure.code.clone(),
                    completion_sequence: failure.completion_sequence,
                })
                .collect(),
            pending: Vec::new(),
            remaining_ms: None,
            cancellation_recorded: false,
            failure_code: None,
            cancel_children: Vec::new(),
            detached: false,
            result_hash: Some(*result_hash),
            projection_hash: ContentHash::from_bytes([0; 32]),
        },
        ChildWaitProjection::Failed {
            code,
            cancel_children,
            detached,
            result_hash,
        } => ChildWaitProjectedEvent {
            work: command.work.clone(),
            execution_plan_hash: command.execution_plan_hash,
            configuration_hash: command.configuration_hash,
            disposition: GenericChildWaitDisposition::Failed,
            successful: Vec::new(),
            unsuccessful: Vec::new(),
            pending: Vec::new(),
            remaining_ms: None,
            cancellation_recorded: code == "parent_cancelled",
            failure_code: Some(code.clone()),
            cancel_children: cancel_children.clone(),
            detached: *detached,
            result_hash: Some(*result_hash),
            projection_hash: ContentHash::from_bytes([0; 32]),
        },
    };
    event.projection_hash =
        generic_child_wait_projection_hash(&event).map_err(ChildGraphApplicationError::Reducer)?;
    let existing = state
        .planner_worker
        .child_waits
        .values()
        .find(|record| record.projection.work == command.work);
    match existing {
        Some(existing) if existing.projection == event => Ok(Vec::new()),
        Some(existing)
            if existing.projection.disposition == GenericChildWaitDisposition::Waiting =>
        {
            Ok(vec![RuntimeCommittedEvent::ChildWaitProjected(Box::new(
                event,
            ))])
        }
        Some(_) => Err(ChildGraphApplicationError::Substitution),
        None => Ok(vec![RuntimeCommittedEvent::ChildWaitProjected(Box::new(
            event,
        ))]),
    }
}

fn plan_wait_terminals(
    state: &SessionState,
    command: &PlanChildGraphApplicationCommand,
) -> Result<Vec<RuntimeCommittedEvent>, ChildGraphApplicationError> {
    command
        .evidence
        .terminals
        .iter()
        .map(|(task_id, receipt)| {
            let record = state
                .child_agents
                .values()
                .find(|record| {
                    record.generic_identity.as_deref().is_some_and(|identity| {
                        identity.execution_plan_hash == command.execution_plan_hash
                            && identity.task_id == *task_id
                            && record.child_session_id == Some(receipt.child_session_id)
                    })
                })
                .ok_or(ChildGraphApplicationError::Substitution)?;
            let identity = record
                .generic_identity
                .as_deref()
                .ok_or(ChildGraphApplicationError::Substitution)?;
            if record.state != ChildAgentState::Active
                || record.child_session_id != Some(receipt.child_session_id)
            {
                return Err(ChildGraphApplicationError::Substitution);
            }
            let mut canonical = GenericChildTerminalReceipt {
                disposition: receipt.disposition,
                result_reference: receipt.result_reference.clone(),
                artifact_references: receipt.artifact_references.clone(),
                failure_code: receipt.failure_code.clone(),
                receipt_hash: ContentHash::from_bytes([0; 32]),
            };
            canonical.receipt_hash = generic_child_terminal_receipt_hash(
                identity,
                receipt.child_session_id,
                receipt.child_head_sequence,
                &canonical,
            )
            .map_err(ChildGraphApplicationError::Reducer)?;
            if canonical.receipt_hash != receipt.receipt_hash {
                return Err(ChildGraphApplicationError::Substitution);
            }
            Ok(RuntimeCommittedEvent::GenericChildTerminal(Box::new(
                GenericChildTerminalEvent {
                    identity: identity.clone(),
                    child_session_id: receipt.child_session_id,
                    child_head_sequence: receipt.child_head_sequence,
                    receipt: Box::new(canonical),
                },
            )))
        })
        .collect()
}

fn map_successes(
    successes: &[crate::child_graph_execution::ChildWaitSuccess],
) -> Vec<GenericChildWaitSuccess> {
    successes
        .iter()
        .map(|success| GenericChildWaitSuccess {
            child_id: success.child_id,
            task_id: success.task_id.clone(),
            result_reference: success.result_reference.clone(),
            artifact_references: success.artifact_references.clone(),
            completion_sequence: success.completion_sequence,
        })
        .collect()
}

fn plan_review(
    state: &SessionState,
    command: &PlanChildGraphApplicationCommand,
    routing: &ReviewRoutingProposal,
) -> Result<Vec<RuntimeCommittedEvent>, ChildGraphApplicationError> {
    let disposition = match routing.disposition {
        ReviewDisposition::Approved => "approved",
        ReviewDisposition::Revision => "revision",
        ReviewDisposition::Failed => "failed",
    };
    let findings = routing
        .findings
        .iter()
        .map(|finding| GenericReviewerFinding {
            code: finding.code.clone(),
            message: finding.message.clone(),
            artifact_references: finding.artifact_references.clone(),
        })
        .collect::<Vec<_>>();
    let mut event = GenericReviewRoutedEvent {
        approved: routing.disposition == ReviewDisposition::Approved,
        rejected_task_ids: routing.rejected_task_ids.clone(),
        findings: findings
            .iter()
            .map(|finding| finding.message.clone())
            .collect(),
        evidence: Box::new(GenericReviewRoutingEvidence {
            work: command.work.clone(),
            execution_plan_hash: command.execution_plan_hash,
            configuration_hash: command.configuration_hash,
            evidence_hash: routing.evidence_hash,
            disposition: disposition.to_owned(),
            destination_node_id: routing.destination_node_id.clone(),
            current_revision: routing.current_revision,
            next_revision: routing.next_revision,
            structured_findings: findings,
            application_hash: ContentHash::from_bytes([0; 32]),
        }),
    };
    event.evidence.application_hash =
        generic_review_application_hash(&event).map_err(ChildGraphApplicationError::Reducer)?;
    let existing = state.planner_worker.reviews.iter().find_map(|record| {
        record
            .generic
            .as_deref()
            .filter(|evidence| evidence.work == command.work)
    });
    match existing {
        Some(existing) if existing == event.evidence.as_ref() => Ok(Vec::new()),
        Some(_) => Err(ChildGraphApplicationError::Substitution),
        None => Ok(vec![RuntimeCommittedEvent::GenericReviewRouted(Box::new(
            event,
        ))]),
    }
}

/// Pure application planning failure.
#[derive(Debug, Error)]
pub enum ChildGraphApplicationError {
    /// Session, work, plan, configuration, or executor differs.
    #[error("child graph application identity is invalid")]
    Identity,
    /// The pure proposal is empty, unordered, or internally inconsistent.
    #[error("child graph proposal is invalid")]
    Proposal,
    /// Replayed state or supplied receipt substitutes exact canonical evidence.
    #[error("child graph application substituted canonical evidence")]
    Substitution,
    /// The shared session reducer contract rejected a derived digest.
    #[error("child graph application reducer contract failed: {0}")]
    Reducer(SessionReducerError),
}

#[cfg(test)]
mod tests {
    use agentmod_event_model::{
        EventClassification, EventEnvelope, EventMetadata, EventOrigin, EventScope,
    };
    use agentmod_graph_engine::{
        CompilerLimits, GraphCacheInputs, NodeConfiguration, ParallelJoinPolicy, compile,
    };
    use agentmod_primitives::{CausationId, CorrelationId, EventId, TimestampMillis, Version};
    use uuid::Uuid;

    use super::*;
    use crate::{
        parallel_execution::{ParallelBranchSpec, ParallelExecutionState},
        session::{
            CanonicalParallelExecutionState, ParallelBranchControlState,
            ParallelBranchNodeEnteredEvent, ParallelBranchRegionBinding,
            ParallelBranchReplayRecord, RuntimeCommittedEvent, SessionCreatedEvent,
            SessionNodeExecutorBoundary, SessionNodeExecutorResolution, SessionNodeExecutorSource,
            SessionStyleBudgets, StyleExecutionContract, StyleExecutionInitializedEvent, reduce,
        },
    };

    const WAIT_GRAPH: &str = r#"
format_version = 1
entry = "wait-a"
[budget]
max_steps = 8
max_tokens = 100
max_cost_micros = 100
max_duration_ms = 10000
[declarations]
capabilities = ["agents"]

[[nodes]]
id = "wait-a"
kind = "wait_for_agents"
configuration = { type = "wait_for_agents", children = { kind = "exact", child_ids = ["00000000-0000-0000-0000-000000000077"] }, maximum_children = 1, minimum_successes = 1, timeout_ms = 1000, cancellation = "cascade" }

[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "wait-a"
to = "done"
"#;

    fn session_id() -> SessionId {
        SessionId::from_uuid(Uuid::from_u128(0x71))
    }

    fn envelope(
        sequence: u64,
        payload: RuntimeCommittedEvent,
    ) -> EventEnvelope<RuntimeCommittedEvent> {
        EventEnvelope::seal(
            EventMetadata {
                event_id: EventId::from_uuid(Uuid::from_u128(100 + u128::from(sequence))),
                scope: EventScope::Session(session_id()),
                sequence: Sequence::new(sequence).expect("sequence"),
                timestamp: TimestampMillis::new(1_700_000_000_000),
                event_type: payload.event_type().to_owned(),
                event_version: Version::new(1, 0),
                correlation_id: CorrelationId::from_uuid(Uuid::from_u128(200)),
                causation_id: CausationId::from_uuid(Uuid::from_u128(300)),
                parent_graph_node_id: None,
                origin: EventOrigin {
                    subsystem: String::from("runtime"),
                    plugin: None,
                },
                schema_version: Version::new(1, 0),
                artifacts: Vec::new(),
                classification: EventClassification::Committed,
            },
            payload,
        )
        .expect("event")
    }

    fn resolution(
        node_id: &str,
        node_kind: &str,
        executor_id: &str,
        configuration_hash: ContentHash,
    ) -> SessionNodeExecutorResolution {
        SessionNodeExecutorResolution {
            node_id: node_id.to_owned(),
            node_kind: node_kind.to_owned(),
            executor_id: executor_id.to_owned(),
            executor_version: String::from("1.0.0"),
            source: SessionNodeExecutorSource::Runtime,
            boundary: SessionNodeExecutorBoundary::RuntimeLogic,
            required_capabilities: vec![String::from("agents")],
            resolved_capabilities: vec![String::from("agents")],
            runtime_api_requirement: String::from("^1.0.0"),
            executor_declaration_hash: ContentHash::digest(executor_id.as_bytes()),
            adapter_configuration_reference: configuration_hash,
        }
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "the regression keeps the exact immutable plan, canonical parallel branch state, and fail-closed substitution assertion together"
    )]
    fn parallel_branch_wait_work_is_accepted_by_application_planner() {
        let graph = compile(
            WAIT_GRAPH,
            &GraphCacheInputs {
                plugin_set_hash: ContentHash::digest(b"plugins"),
                runtime_api_version: String::from("1.0.0"),
                capability_set: BTreeSet::from([String::from("agents")]),
            },
            CompilerLimits::default(),
        )
        .expect("wait graph");
        let wait_node = graph
            .nodes
            .iter()
            .find(|node| node.id == "wait-a")
            .expect("wait node");
        let configuration_hash =
            ContentHash::digest(&serde_json::to_vec(wait_node).expect("wait node serialization"));
        let wait_executor = resolution(
            "wait-a",
            "wait_for_agents",
            "runtime.child-wait",
            configuration_hash,
        );
        let plan_hash = ContentHash::digest(b"parallel-child-application-plan");
        let run_id = String::from("parallel-child-application-run");
        let contract = StyleExecutionContract {
            style_binding_hash: ContentHash::digest(b"binding"),
            execution_plan_hash: plan_hash,
            registry_hash: ContentHash::digest(b"registry"),
            node_executors: vec![wait_executor.clone()],
            initial_node_id: String::from("wait-a"),
            initial_variables_json: String::from("{}"),
            invocation_provider: Some(String::from("mock")),
            invocation_model: Some(String::from("mock-model")),
            invocation_options_json: None,
            initial_budgets: SessionStyleBudgets {
                max_iterations: 2,
                max_steps: 8,
                max_tokens: 100,
                max_cost_micros: 100,
                max_duration_ms: 10_000,
            },
            run_id: run_id.clone(),
        };
        let mut state = reduce(
            None,
            &envelope(
                1,
                RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                    workspace: String::from("fixture"),
                    style: String::from("user-child-graph"),
                    style_binding: None,
                }),
            ),
        )
        .expect("created");
        state = reduce(
            Some(state),
            &envelope(
                2,
                RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                    StyleExecutionInitializedEvent {
                        graph: Box::new(graph),
                        input_reference: None,
                        execution_contract: None,
                    },
                )),
            ),
        )
        .expect("initialized");
        state
            .style_execution
            .as_mut()
            .expect("execution")
            .execution_contract = Some(Box::new(contract));

        let owner = NodeWorkIdentity {
            run_id: run_id.clone(),
            node_id: String::from("fanout"),
            branch_path: Vec::new(),
            attempt: 1,
            loop_iteration: 0,
            step: 1,
        };
        let parallel_configuration = NodeConfiguration::ParallelBranch {
            max_parallelism: 2,
            max_queue_depth: 2,
            join_target: String::from("join"),
            join_policy: ParallelJoinPolicy::All,
            variable_merge_policies: BTreeMap::new(),
            serialization_policy: None,
        };
        let specs = [
            ParallelBranchSpec {
                member_reference: String::from("wait-result"),
                target_node_id: String::from("wait-a"),
                write_variables: BTreeSet::new(),
                workspace_resources: BTreeSet::new(),
            },
            ParallelBranchSpec {
                member_reference: String::from("other-result"),
                target_node_id: String::from("other"),
                write_variables: BTreeSet::new(),
                workspace_resources: BTreeSet::new(),
            },
        ];
        let pure = ParallelExecutionState::new(owner.clone(), &parallel_configuration, &specs, &[])
            .expect("parallel state");
        let active_member = pure
            .member_bindings()
            .iter()
            .find(|member| member.target_node_id == "wait-a")
            .expect("wait member")
            .clone();
        let active_branch = pure
            .branches()
            .get(&active_member.branch_id)
            .expect("wait branch");
        let work = NodeWorkIdentity {
            run_id,
            node_id: String::from("wait-a"),
            branch_path: vec![active_member.branch_id.clone()],
            attempt: 1,
            loop_iteration: 0,
            step: 2,
        };
        let entered = ParallelBranchNodeEnteredEvent {
            owner: owner.clone(),
            branch_id: active_member.branch_id.clone(),
            dispatch_id: active_branch.dispatch_id.clone(),
            work: work.clone(),
            executor: wait_executor,
            configuration_hash,
        };
        let branches = pure
            .member_bindings()
            .iter()
            .map(|member| {
                let control = if member.branch_id == active_member.branch_id {
                    ParallelBranchControlState::Active(entered.clone())
                } else {
                    ParallelBranchControlState::Queued
                };
                (
                    member.branch_id.clone(),
                    ParallelBranchReplayRecord {
                        region: ParallelBranchRegionBinding {
                            region_id: format!("region:{}", member.branch_id),
                            member: member.clone(),
                            node_ids: [member.target_node_id.clone()].into_iter().collect(),
                            write_variables: BTreeSet::new(),
                            workspace_resources: BTreeSet::new(),
                            variable_base_versions: BTreeMap::new(),
                            workspace_base_versions: BTreeMap::new(),
                        },
                        control,
                        entered_at: (member.branch_id == active_member.branch_id)
                            .then(|| Sequence::new(3).expect("sequence")),
                        effect: None,
                        last_result: None,
                        terminal_at: None,
                        cancellation_requested_at: None,
                        suppression_at: None,
                    },
                )
            })
            .collect();
        let parallel_executor = resolution(
            "fanout",
            "parallel_branch",
            "runtime.parallel",
            ContentHash::digest(b"parallel-configuration"),
        );
        state
            .style_execution
            .as_mut()
            .expect("execution")
            .parallel_executions
            .insert(
                String::from("parallel-fixture"),
                CanonicalParallelExecutionState {
                    owner,
                    executor: parallel_executor,
                    configuration_hash: ContentHash::digest(b"parallel-configuration"),
                    execution: pure,
                    branches,
                    variable_contributions: BTreeMap::new(),
                    initialized_at: Sequence::new(2).expect("sequence"),
                    last_allocated_step: 2,
                    cancellation_code: None,
                    cancellation_requested_at: None,
                    cancellation_completed_at: None,
                },
            );

        let command = PlanChildGraphApplicationCommand {
            session_id: session_id(),
            work: work.clone(),
            execution_plan_hash: plan_hash,
            configuration_hash,
            outcome: ChildGraphNodeOutcome::Wait(ChildWaitProjection::Waiting {
                successful: Vec::new(),
                pending: vec![SessionId::from_uuid(Uuid::from_u128(0x77))],
                remaining_ms: 1_000,
                cancellation_recorded: false,
            }),
            evidence: ChildGraphApplicationEvidence::default(),
        };
        let plan = plan_child_graph_application(&state, &command).expect("branch wait plan");
        assert!(matches!(
            plan.events.as_slice(),
            [RuntimeCommittedEvent::ChildWaitProjected(event)] if event.work == work
        ));

        let mut substituted = command;
        substituted.work.branch_path = vec![String::from("other-branch")];
        assert!(matches!(
            plan_child_graph_application(&state, &substituted),
            Err(ChildGraphApplicationError::Identity)
        ));
    }
}
