//! Outcome-class validation against compiled node kinds.
//!
//! A typed [`NodeExecutionOutcome`] is only meaningful when it is consistent
//! with the compiled node kind that produced it. This module owns that pure
//! validation so dispatch and recovery share one compatibility contract.

use agentmod_graph_engine::NodeKind;

use super::{NodeExecutionOutcome, NodeFailure};

/// Outcome-consistency validation result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutcomeCompatibility {
    /// The outcome is legal for the node kind.
    Consistent,
    /// The outcome class is illegal for the node kind.
    Inconsistent,
}

/// Whether a node kind may wait on a durable continuation.
#[must_use]
pub const fn can_wait_on_continuation(kind: NodeKind) -> bool {
    matches!(
        kind,
        NodeKind::ToolExecutionGate | NodeKind::UserApproval | NodeKind::Schedule
    )
}

/// Whether a node kind may wait on child sessions.
#[must_use]
pub const fn can_wait_on_children(kind: NodeKind) -> bool {
    matches!(
        kind,
        NodeKind::SpawnChildAgent
            | NodeKind::SendChildAgentMessage
            | NodeKind::WaitForAgents
            | NodeKind::JoinResults
    )
}

/// Whether a node kind may wait on parallel branches.
#[must_use]
pub const fn can_wait_on_parallel_branches(kind: NodeKind) -> bool {
    matches!(kind, NodeKind::ParallelBranch)
}

/// Whether a node kind may request a retry.
#[must_use]
pub const fn can_retry(kind: NodeKind) -> bool {
    matches!(
        kind,
        NodeKind::ModelCall
            | NodeKind::ToolExecutionGate
            | NodeKind::UserApproval
            | NodeKind::Review
            | NodeKind::SpawnChildAgent
            | NodeKind::PersistArtifact
            | NodeKind::ContextTransform
    )
}

/// Whether a node kind may fail with a structured failure.
#[must_use]
pub const fn can_fail(kind: NodeKind) -> bool {
    !matches!(kind, NodeKind::CompleteTurn | NodeKind::CompleteSession)
}

/// Whether restart recovery must not infer this node's externally observable
/// work from graph control events alone.
#[must_use]
pub const fn requires_effect_evidence(kind: NodeKind) -> bool {
    !matches!(
        kind,
        NodeKind::Loop
            | NodeKind::ConditionalBranch
            | NodeKind::ParallelBranch
            | NodeKind::CompleteTurn
            | NodeKind::CompleteSession
            | NodeKind::Fail
    )
}

/// Whether the node kind is a terminal graph node.
#[must_use]
pub const fn is_terminal_kind(kind: NodeKind) -> bool {
    matches!(
        kind,
        NodeKind::CompleteTurn | NodeKind::CompleteSession | NodeKind::Fail
    )
}

/// Validates an outcome against the compiled node kind.
///
/// Rules:
/// - `Completed` is legal for every non-terminal kind and for every terminal
///   kind (terminal nodes may also report bounded output on completion).
/// - `WaitingOnContinuation` is legal only for nodes that durably wait.
/// - `WaitingOnChildren` is legal only for child-session nodes.
/// - `WaitingOnParallelBranches` is legal only for parallel-branch nodes.
/// - `Retry` is legal only for retry-capable kinds.
/// - `Failed` is legal for every kind except turn/session terminals.
/// - `CompleteTurn` is legal only on a `CompleteTurn` node.
/// - `CompleteSession` is legal only on a `CompleteSession` node.
#[must_use]
pub fn validate_outcome_for_kind(
    kind: NodeKind,
    outcome: &NodeExecutionOutcome,
) -> OutcomeCompatibility {
    let consistent = match outcome {
        NodeExecutionOutcome::Completed { .. } => !is_terminal_kind(kind),
        NodeExecutionOutcome::WaitingOnContinuation { .. } => can_wait_on_continuation(kind),
        NodeExecutionOutcome::WaitingOnChildren { .. } => can_wait_on_children(kind),
        NodeExecutionOutcome::WaitingOnParallelBranches { .. } => {
            can_wait_on_parallel_branches(kind)
        }
        NodeExecutionOutcome::Retry { .. } => can_retry(kind),
        NodeExecutionOutcome::Failed {
            failure: NodeFailure { .. },
        } => can_fail(kind),
        NodeExecutionOutcome::CompleteTurn { .. } => kind == NodeKind::CompleteTurn,
        NodeExecutionOutcome::CompleteSession { .. } => kind == NodeKind::CompleteSession,
    };
    if consistent {
        OutcomeCompatibility::Consistent
    } else {
        OutcomeCompatibility::Inconsistent
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_execution::{BoundedNodeOutput, NodeExecutionOutcome};

    fn completed() -> NodeExecutionOutcome {
        NodeExecutionOutcome::Completed {
            output: BoundedNodeOutput::reference("out"),
        }
    }

    #[test]
    fn terminal_outcomes_require_matching_terminal_kinds() {
        assert_eq!(
            validate_outcome_for_kind(
                NodeKind::CompleteTurn,
                &NodeExecutionOutcome::CompleteTurn {
                    output: BoundedNodeOutput::reference("out"),
                }
            ),
            OutcomeCompatibility::Consistent
        );
        assert_eq!(
            validate_outcome_for_kind(
                NodeKind::ModelCall,
                &NodeExecutionOutcome::CompleteTurn {
                    output: BoundedNodeOutput::reference("out"),
                }
            ),
            OutcomeCompatibility::Inconsistent
        );
        assert_eq!(
            validate_outcome_for_kind(
                NodeKind::CompleteSession,
                &NodeExecutionOutcome::CompleteSession {
                    output: BoundedNodeOutput::reference("out"),
                }
            ),
            OutcomeCompatibility::Consistent
        );
        assert_eq!(
            validate_outcome_for_kind(
                NodeKind::CompleteTurn,
                &NodeExecutionOutcome::CompleteSession {
                    output: BoundedNodeOutput::reference("out"),
                }
            ),
            OutcomeCompatibility::Inconsistent
        );
    }

    #[test]
    fn completed_is_rejected_for_terminal_kinds() {
        for kind in [
            NodeKind::CompleteTurn,
            NodeKind::CompleteSession,
            NodeKind::Fail,
        ] {
            assert_eq!(
                validate_outcome_for_kind(kind, &completed()),
                OutcomeCompatibility::Inconsistent,
                "{kind:?}"
            );
        }
    }

    #[test]
    fn waiting_classes_require_matching_kinds() {
        assert_eq!(
            validate_outcome_for_kind(
                NodeKind::ToolExecutionGate,
                &NodeExecutionOutcome::WaitingOnContinuation {
                    evidence: super::super::ContinuationEvidence {
                        continuation_id: String::from("c"),
                        resume_state: String::from("approval_owned"),
                        terminal: false,
                    },
                },
            ),
            OutcomeCompatibility::Consistent
        );
        assert_eq!(
            validate_outcome_for_kind(
                NodeKind::ModelCall,
                &NodeExecutionOutcome::WaitingOnContinuation {
                    evidence: super::super::ContinuationEvidence {
                        continuation_id: String::from("c"),
                        resume_state: String::from("approval_owned"),
                        terminal: false,
                    },
                },
            ),
            OutcomeCompatibility::Inconsistent
        );
        assert_eq!(
            validate_outcome_for_kind(
                NodeKind::WaitForAgents,
                &NodeExecutionOutcome::WaitingOnChildren {
                    evidence: super::super::ChildSessionEvidence::default(),
                },
            ),
            OutcomeCompatibility::Consistent
        );
        assert_eq!(
            validate_outcome_for_kind(
                NodeKind::ModelCall,
                &NodeExecutionOutcome::WaitingOnChildren {
                    evidence: super::super::ChildSessionEvidence::default(),
                },
            ),
            OutcomeCompatibility::Inconsistent
        );
        assert_eq!(
            validate_outcome_for_kind(
                NodeKind::ParallelBranch,
                &NodeExecutionOutcome::WaitingOnParallelBranches {
                    evidence: super::super::ParallelBranchEvidence::default(),
                },
            ),
            OutcomeCompatibility::Consistent
        );
    }

    #[test]
    fn retry_and_failure_class_rules() {
        assert_eq!(
            validate_outcome_for_kind(
                NodeKind::ModelCall,
                &NodeExecutionOutcome::Retry {
                    reason: super::super::RetryReason {
                        code: String::from("rate_limited"),
                        message: String::from("provider rate limit"),
                    },
                    next_attempt: 2,
                },
            ),
            OutcomeCompatibility::Consistent
        );
        assert_eq!(
            validate_outcome_for_kind(
                NodeKind::Loop,
                &NodeExecutionOutcome::Retry {
                    reason: super::super::RetryReason {
                        code: String::from("rate_limited"),
                        message: String::from("provider rate limit"),
                    },
                    next_attempt: 2,
                },
            ),
            OutcomeCompatibility::Inconsistent
        );
        assert_eq!(
            validate_outcome_for_kind(
                NodeKind::Fail,
                &NodeExecutionOutcome::Failed {
                    failure: NodeFailure {
                        code: String::from("graph_failed"),
                        message: String::from("declared failure"),
                        artifact_reference: None,
                    },
                },
            ),
            OutcomeCompatibility::Consistent
        );
        assert_eq!(
            validate_outcome_for_kind(
                NodeKind::CompleteSession,
                &NodeExecutionOutcome::Failed {
                    failure: NodeFailure {
                        code: String::from("graph_failed"),
                        message: String::from("declared failure"),
                        artifact_reference: None,
                    },
                },
            ),
            OutcomeCompatibility::Inconsistent
        );
    }
}
