//! Replay-derived node recovery classification.
//!
//! The dispatcher consumes replay-derived node state and classifies the
//! current node without inferring external completion from graph control
//! events alone. Waiting classes require durable evidence (continuation,
//! child, or scheduler state); ambiguous in-flight effects fail closed.

use agentmod_graph_engine::NodeKind;

use super::outcome::requires_effect_evidence;

/// Replay-derived classification of one node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeRecoveryClass {
    /// The node has no canonical entry event.
    NotStarted,
    /// The node was entered but dispatch never started.
    EnteredNotDispatched,
    /// The node is waiting on a durable continuation.
    WaitingOnContinuation,
    /// The node is waiting on child sessions.
    WaitingOnChildren,
    /// The node is waiting on parallel branches.
    WaitingOnParallelBranches,
    /// The node produced a canonical completion outcome.
    Completed,
    /// The node produced a canonical failure outcome.
    Failed,
    /// Dispatch started but no terminal or waiting evidence exists; the
    /// external effect is ambiguous and must fail closed without redispatch.
    AmbiguousExternalEffect,
    /// The graph reached a terminal node.
    Terminal,
}

impl NodeRecoveryClass {
    /// Stable diagnostic label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::EnteredNotDispatched => "entered_not_dispatched",
            Self::WaitingOnContinuation => "waiting_on_continuation",
            Self::WaitingOnChildren => "waiting_on_children",
            Self::WaitingOnParallelBranches => "waiting_on_parallel_branches",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::AmbiguousExternalEffect => "ambiguous_external_effect",
            Self::Terminal => "terminal",
        }
    }
}

/// Durable external-effect evidence recovery is allowed to consume.
#[allow(
    clippy::struct_excessive_bools,
    reason = "recovery evidence is a bounded boolean snapshot"
)]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EffectEvidence {
    /// A durable continuation exists and is not yet terminal.
    pub pending_continuation: bool,
    /// Child sessions are still pending.
    pub pending_children: bool,
    /// Parallel branches are still pending.
    pub pending_parallel_branches: bool,
    /// A scheduler claim is pending without a terminal worker marker.
    pub pending_scheduler_claim: bool,
    /// Whether any evidence is ambiguous (dispatch started, no outcome).
    pub ambiguous: bool,
}

/// Canonical node-state evidence reconstructed from replay.
#[allow(
    clippy::struct_excessive_bools,
    reason = "recovery evidence is a bounded boolean snapshot"
)]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NodeStateEvidence {
    /// Whether a canonical entry event exists for the node.
    pub entered: bool,
    /// Whether dispatch started for the node (outbox/proposal evidence).
    pub dispatched: bool,
    /// Whether a canonical completion outcome exists for the node.
    pub completed: bool,
    /// Whether a canonical failure outcome exists for the node.
    pub failed: bool,
    /// Whether the node is the terminal graph node.
    pub terminal_kind: bool,
    /// Durable external-effect evidence.
    pub effects: EffectEvidence,
}

/// Classifies one node from replay-derived evidence.
///
/// Priority: canonical outcomes first (completed/failed), then terminal
/// state, then ambiguous in-flight effects (fail closed), then durable
/// waiting classes, then entered-not-dispatched, then not started.
#[must_use]
pub fn classify_node(kind: NodeKind, evidence: &NodeStateEvidence) -> NodeRecoveryClass {
    if evidence.completed {
        return if evidence.terminal_kind {
            NodeRecoveryClass::Terminal
        } else {
            NodeRecoveryClass::Completed
        };
    }
    if evidence.failed {
        return NodeRecoveryClass::Failed;
    }
    if !evidence.entered {
        return NodeRecoveryClass::NotStarted;
    }
    if evidence.effects.ambiguous {
        return NodeRecoveryClass::AmbiguousExternalEffect;
    }
    if evidence.effects.pending_continuation {
        return NodeRecoveryClass::WaitingOnContinuation;
    }
    if evidence.effects.pending_children {
        return NodeRecoveryClass::WaitingOnChildren;
    }
    if evidence.effects.pending_parallel_branches {
        return NodeRecoveryClass::WaitingOnParallelBranches;
    }
    if !evidence.dispatched {
        return NodeRecoveryClass::EnteredNotDispatched;
    }
    if requires_effect_evidence(kind) {
        // Dispatch started on an effectful node with no terminal outcome and
        // no durable waiting evidence: the external effect is ambiguous.
        NodeRecoveryClass::AmbiguousExternalEffect
    } else {
        // Control-only node started but never produced an outcome: treat as
        // not dispatched for deterministic repair.
        NodeRecoveryClass::EnteredNotDispatched
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_and_failed_precede_waiting_classes() {
        let mut evidence = NodeStateEvidence {
            entered: true,
            dispatched: true,
            completed: true,
            effects: EffectEvidence {
                pending_continuation: true,
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(
            classify_node(NodeKind::ToolExecutionGate, &evidence),
            NodeRecoveryClass::Completed
        );
        evidence.completed = false;
        evidence.failed = true;
        assert_eq!(
            classify_node(NodeKind::ModelCall, &evidence),
            NodeRecoveryClass::Failed
        );
    }

    #[test]
    fn terminal_kind_completion_is_terminal() {
        let evidence = NodeStateEvidence {
            entered: true,
            completed: true,
            terminal_kind: true,
            ..Default::default()
        };
        assert_eq!(
            classify_node(NodeKind::CompleteTurn, &evidence),
            NodeRecoveryClass::Terminal
        );
    }

    #[test]
    fn waiting_classes_require_durable_evidence() {
        let base = NodeStateEvidence {
            entered: true,
            dispatched: true,
            ..Default::default()
        };
        let continuation = NodeStateEvidence {
            effects: EffectEvidence {
                pending_continuation: true,
                ..Default::default()
            },
            ..base.clone()
        };
        assert_eq!(
            classify_node(NodeKind::UserApproval, &continuation),
            NodeRecoveryClass::WaitingOnContinuation
        );
        let children = NodeStateEvidence {
            effects: EffectEvidence {
                pending_children: true,
                ..Default::default()
            },
            ..base.clone()
        };
        assert_eq!(
            classify_node(NodeKind::WaitForAgents, &children),
            NodeRecoveryClass::WaitingOnChildren
        );
        let parallel = NodeStateEvidence {
            effects: EffectEvidence {
                pending_parallel_branches: true,
                ..Default::default()
            },
            ..base
        };
        assert_eq!(
            classify_node(NodeKind::ParallelBranch, &parallel),
            NodeRecoveryClass::WaitingOnParallelBranches
        );
    }

    #[test]
    fn entered_but_never_dispatched_is_distinct() {
        let evidence = NodeStateEvidence {
            entered: true,
            dispatched: false,
            ..Default::default()
        };
        assert_eq!(
            classify_node(NodeKind::ModelCall, &evidence),
            NodeRecoveryClass::EnteredNotDispatched
        );
    }

    #[test]
    fn unentered_node_is_not_started() {
        assert_eq!(
            classify_node(NodeKind::ModelCall, &NodeStateEvidence::default()),
            NodeRecoveryClass::NotStarted
        );
    }

    #[test]
    fn ambiguous_effect_evidence_fails_closed() {
        // Dispatch started on an effectful node with no outcome: ambiguous.
        let evidence = NodeStateEvidence {
            entered: true,
            dispatched: true,
            ..Default::default()
        };
        assert_eq!(
            classify_node(NodeKind::ToolExecutionGate, &evidence),
            NodeRecoveryClass::AmbiguousExternalEffect
        );
        // Ambiguity is explicit and wins over the generic inference.
        let explicit = NodeStateEvidence {
            entered: true,
            dispatched: true,
            effects: EffectEvidence {
                ambiguous: true,
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(
            classify_node(NodeKind::ModelCall, &explicit),
            NodeRecoveryClass::AmbiguousExternalEffect
        );
    }

    #[test]
    fn control_only_nodes_are_repairable_without_effect_evidence() {
        let evidence = NodeStateEvidence {
            entered: true,
            dispatched: true,
            ..Default::default()
        };
        assert_eq!(
            classify_node(NodeKind::Loop, &evidence),
            NodeRecoveryClass::EnteredNotDispatched
        );
        assert_eq!(
            classify_node(NodeKind::ConditionalBranch, &evidence),
            NodeRecoveryClass::EnteredNotDispatched
        );
    }
}
