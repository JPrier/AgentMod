//! Generic style-execution dispatch reducer.
//!
//! The reducer validates a typed [`NodeExecutionOutcome`] against the compiled
//! node and produces the canonical decision the runtime commits: node
//! completed/failed, transition selected, execution waiting/resumed, retry, or
//! terminal turn/session completion. It never fabricates external-effect
//! completion and never mutates canonical state; it only reports evidence the
//! runtime already committed.

use agentmod_graph_engine::ExecutableGraph;
use serde_json::Value;

use super::{
    DispatchError, ExecuteNodeCommand, NodeCursor, NodeExecutionOutcome, NodeFailure,
    NodeRecoveryClass,
    outcome::validate_outcome_for_kind,
    transition::{LoopState, NodePlan, TransitionSelection, select_transition},
};

/// Stable lifecycle event kinds the generic dispatcher may emit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeDispatchEventKind {
    /// The executor dispatch was proposed.
    DispatchProposed,
    /// The executor dispatch started (where the runtime records it).
    DispatchStarted,
    /// The node completed with bounded output.
    NodeCompleted,
    /// The node failed with a structured failure.
    NodeFailed,
    /// A transition was selected deterministically.
    TransitionSelected,
    /// Execution waits on durable evidence.
    ExecutionWaiting,
    /// Execution resumed from a durable wait.
    ExecutionResumed,
    /// The turn reached its terminal node.
    TerminalTurn,
    /// The session reached its terminal node.
    TerminalSession,
}

/// One canonical lifecycle evidence entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeDispatchEvent {
    /// Stable event kind.
    pub kind: NodeDispatchEventKind,
    /// Compiled node ID.
    pub node_id: String,
    /// One-based attempt.
    pub attempt: u32,
    /// Zero-based loop iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Redacted structured detail (result reference, reason, transition).
    pub detail: Value,
}

impl NodeDispatchEvent {
    fn new(kind: NodeDispatchEventKind, command: &ExecuteNodeCommand, detail: Value) -> Self {
        Self {
            kind,
            node_id: command.node.id.clone(),
            attempt: command.attempt,
            loop_iteration: command.loop_iteration,
            step: command.step,
            detail,
        }
    }
}

/// Validated dispatch decision the caller commits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeDispatchDecision {
    /// The node completed and a deterministic transition (or terminal) follows.
    Completed {
        /// Selected transition, when the source is not terminal.
        transition: Option<TransitionSelection>,
        /// Whether the caller must advance the loop iteration counter.
        advance_loop: bool,
        /// Lifecycle evidence.
        trace: Vec<NodeDispatchEvent>,
    },
    /// The node waits on durable evidence.
    Waiting {
        /// Replay-derived waiting class.
        class: NodeRecoveryClass,
        /// Lifecycle evidence.
        trace: Vec<NodeDispatchEvent>,
    },
    /// A retry is requested within the compiled budget.
    Retry {
        /// Proposed next attempt.
        next_attempt: u32,
        /// Redacted reason detail.
        detail: Value,
        /// Lifecycle evidence.
        trace: Vec<NodeDispatchEvent>,
    },
    /// The node failed.
    Failed {
        /// Structured failure.
        failure: NodeFailure,
        /// Whether the failure terminates style execution.
        terminal: bool,
        /// Lifecycle evidence.
        trace: Vec<NodeDispatchEvent>,
    },
    /// Terminal turn completion.
    TerminalTurn {
        /// Bounded terminal output reference.
        reference: String,
        /// Lifecycle evidence.
        trace: Vec<NodeDispatchEvent>,
    },
    /// Terminal session completion.
    TerminalSession {
        /// Bounded terminal output reference.
        reference: String,
        /// Lifecycle evidence.
        trace: Vec<NodeDispatchEvent>,
    },
}

/// Pure dispatch reducer.
#[allow(
    clippy::too_many_lines,
    reason = "each outcome class keeps its canonical reduction adjacent"
)]
#[derive(Clone, Copy, Debug, Default)]
pub struct NodeDispatchReducer;

impl NodeDispatchReducer {
    /// Reduces one validated outcome into the canonical decision.
    ///
    /// # Errors
    ///
    /// Returns [`DispatchError`] when the outcome is inconsistent with the
    /// node kind or the transition selection rejects the graph.
    #[allow(
        clippy::too_many_lines,
        reason = "each outcome class keeps its canonical reduction adjacent"
    )]
    pub fn reduce(
        &self,
        graph: &ExecutableGraph,
        command: &ExecuteNodeCommand,
        outcome: &NodeExecutionOutcome,
        variables: &Value,
        loop_state: &LoopState,
        plan: &NodePlan,
    ) -> Result<NodeDispatchDecision, DispatchError> {
        if validate_outcome_for_kind(command.node.kind, outcome)
            != super::outcome::OutcomeCompatibility::Consistent
        {
            return Err(DispatchError::OutcomeInconsistent {
                node: command.node.id.clone(),
                kind: super::serialized_kind(command.node.kind),
                outcome: outcome.class_name().to_owned(),
            });
        }
        match outcome {
            NodeExecutionOutcome::Completed { .. } => {
                let selection = select_transition(
                    graph,
                    command.node.index,
                    variables,
                    Some(outcome),
                    loop_state,
                    plan,
                )?;
                let (transition, advance_loop) = match selection {
                    super::transition::TransitionSelectionOutcome::Selected(selected) => {
                        (Some(selected.clone()), selected.advance_loop)
                    }
                    super::transition::TransitionSelectionOutcome::Parallel(parallel) => {
                        // Deterministic single-path completion cannot reduce a
                        // parallel fan-out; the parallel branch owns waiting.
                        let mut trace = Vec::new();
                        trace.push(NodeDispatchEvent::new(
                            NodeDispatchEventKind::NodeCompleted,
                            command,
                            serde_json::json!({
                                "branches": parallel.branches.len(),
                            }),
                        ));
                        return Ok(NodeDispatchDecision::Waiting {
                            class: NodeRecoveryClass::WaitingOnParallelBranches,
                            trace,
                        });
                    }
                    super::transition::TransitionSelectionOutcome::Terminal => (None, false),
                };
                let mut trace = vec![NodeDispatchEvent::new(
                    NodeDispatchEventKind::NodeCompleted,
                    command,
                    serde_json::json!({
                        "result_reference": outcome_reference(outcome),
                    }),
                )];
                if let Some(selected) = &transition {
                    trace.push(NodeDispatchEvent::new(
                        NodeDispatchEventKind::TransitionSelected,
                        command,
                        serde_json::json!({
                            "to_node_id": selected.to.id,
                            "label": selected.label,
                            "advance_loop": selected.advance_loop,
                        }),
                    ));
                }
                Ok(NodeDispatchDecision::Completed {
                    transition,
                    advance_loop,
                    trace,
                })
            }
            NodeExecutionOutcome::WaitingOnContinuation { evidence } => {
                Ok(NodeDispatchDecision::Waiting {
                    class: NodeRecoveryClass::WaitingOnContinuation,
                    trace: vec![NodeDispatchEvent::new(
                        NodeDispatchEventKind::ExecutionWaiting,
                        command,
                        serde_json::json!({
                            "continuation_id": evidence.continuation_id,
                            "resume_state": evidence.resume_state,
                            "terminal": evidence.terminal,
                        }),
                    )],
                })
            }
            NodeExecutionOutcome::WaitingOnChildren { evidence } => {
                Ok(NodeDispatchDecision::Waiting {
                    class: NodeRecoveryClass::WaitingOnChildren,
                    trace: vec![NodeDispatchEvent::new(
                        NodeDispatchEventKind::ExecutionWaiting,
                        command,
                        serde_json::json!({
                            "pending": evidence.pending,
                            "joined": evidence.joined,
                        }),
                    )],
                })
            }
            NodeExecutionOutcome::WaitingOnParallelBranches { evidence } => {
                Ok(NodeDispatchDecision::Waiting {
                    class: NodeRecoveryClass::WaitingOnParallelBranches,
                    trace: vec![NodeDispatchEvent::new(
                        NodeDispatchEventKind::ExecutionWaiting,
                        command,
                        serde_json::json!({
                            "pending": evidence.branch_ids.len(),
                            "completed": evidence.completed.len(),
                        }),
                    )],
                })
            }
            NodeExecutionOutcome::Retry {
                reason,
                next_attempt,
            } => {
                if *next_attempt <= command.attempt {
                    return Err(DispatchError::InvalidCommand {
                        node: command.node.id.clone(),
                        reason: format!("retry must advance beyond attempt {}", command.attempt),
                    });
                }
                if command.attempt >= command.node.retry_limit {
                    return Err(DispatchError::RetryBudgetExceeded {
                        node: command.node.id.clone(),
                        attempt: command.attempt,
                        retry_limit: command.node.retry_limit,
                    });
                }
                Ok(NodeDispatchDecision::Retry {
                    next_attempt: *next_attempt,
                    detail: serde_json::json!({
                        "code": reason.code,
                        "message": reason.message,
                    }),
                    trace: vec![NodeDispatchEvent::new(
                        NodeDispatchEventKind::DispatchProposed,
                        command,
                        serde_json::json!({
                            "retry_code": reason.code,
                            "next_attempt": next_attempt,
                        }),
                    )],
                })
            }
            NodeExecutionOutcome::Failed { failure } => {
                let terminal = command.node.is_terminal_kind();
                Ok(NodeDispatchDecision::Failed {
                    failure: failure.clone(),
                    terminal,
                    trace: vec![NodeDispatchEvent::new(
                        NodeDispatchEventKind::NodeFailed,
                        command,
                        serde_json::json!({
                            "code": failure.code,
                            "terminal": terminal,
                        }),
                    )],
                })
            }
            NodeExecutionOutcome::CompleteTurn { output } => {
                let transition = select_transition(
                    graph,
                    command.node.index,
                    variables,
                    Some(outcome),
                    loop_state,
                    plan,
                )?;
                if !matches!(
                    transition,
                    super::transition::TransitionSelectionOutcome::Terminal
                ) {
                    return Err(DispatchError::OutcomeInconsistent {
                        node: command.node.id.clone(),
                        kind: super::serialized_kind(command.node.kind),
                        outcome: outcome.class_name().to_owned(),
                    });
                }
                Ok(NodeDispatchDecision::TerminalTurn {
                    reference: output.reference.clone(),
                    trace: vec![
                        NodeDispatchEvent::new(
                            NodeDispatchEventKind::NodeCompleted,
                            command,
                            serde_json::json!({ "result_reference": output.reference }),
                        ),
                        NodeDispatchEvent::new(
                            NodeDispatchEventKind::TerminalTurn,
                            command,
                            serde_json::json!({ "result_reference": output.reference }),
                        ),
                    ],
                })
            }
            NodeExecutionOutcome::CompleteSession { output } => {
                let transition = select_transition(
                    graph,
                    command.node.index,
                    variables,
                    Some(outcome),
                    loop_state,
                    plan,
                )?;
                if !matches!(
                    transition,
                    super::transition::TransitionSelectionOutcome::Terminal
                ) {
                    return Err(DispatchError::OutcomeInconsistent {
                        node: command.node.id.clone(),
                        kind: super::serialized_kind(command.node.kind),
                        outcome: outcome.class_name().to_owned(),
                    });
                }
                Ok(NodeDispatchDecision::TerminalSession {
                    reference: output.reference.clone(),
                    trace: vec![
                        NodeDispatchEvent::new(
                            NodeDispatchEventKind::NodeCompleted,
                            command,
                            serde_json::json!({ "result_reference": output.reference }),
                        ),
                        NodeDispatchEvent::new(
                            NodeDispatchEventKind::TerminalSession,
                            command,
                            serde_json::json!({ "result_reference": output.reference }),
                        ),
                    ],
                })
            }
        }
    }

    /// Builds the loop state for the current command.
    #[must_use]
    pub fn loop_state(command: &ExecuteNodeCommand) -> LoopState {
        LoopState {
            loop_iteration: command.loop_iteration,
            max_iterations: command.node.max_iterations,
        }
    }
}

fn outcome_reference(outcome: &NodeExecutionOutcome) -> Option<&str> {
    match outcome {
        NodeExecutionOutcome::Completed { output }
        | NodeExecutionOutcome::CompleteTurn { output }
        | NodeExecutionOutcome::CompleteSession { output } => Some(&output.reference),
        _ => None,
    }
}

/// Cursor access helper for tests and callers.
#[must_use]
pub fn command_node(command: &ExecuteNodeCommand) -> &NodeCursor {
    &command.node
}
