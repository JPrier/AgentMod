//! Deterministic generic transition selection.
//!
//! Selection consumes the compiled edges, canonical variable input, the
//! completed node outcome, and loop/retry state. It rejects:
//!
//! - zero eligible outgoing edges from a nonterminal node;
//! - multiple eligible edges where the graph has no explicit parallel
//!   semantics;
//! - an outcome inconsistent with the node kind;
//! - a transition to a node absent from the persisted execution plan;
//! - variable writes not declared by the graph;
//! - executor output exceeding bounds;
//! - a repeat transition beyond the compiled loop bound.

use std::collections::BTreeSet;

use agentmod_graph_engine::{ExecutableGraph, NodeKind};
use serde_json::Value;
use thiserror::Error;

use super::{
    MAX_NODE_OUTPUT_BYTES, NodeCursor, NodeExecutionOutcome,
    outcome::{OutcomeCompatibility, validate_outcome_for_kind},
};

/// Exact set of node IDs known to the persisted execution plan.
///
/// Task 1 persists the immutable execution plan; this engine consumes it as
/// the destination membership check so a transition can never enter a node the
/// resolved contract does not contain.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NodePlan {
    /// Known compiled node IDs.
    pub node_ids: BTreeSet<String>,
}

impl NodePlan {
    /// Derives the plan from a compiled graph (the temporary integration port
    /// used until Task 1's persisted plan is available).
    #[must_use]
    pub fn from_graph(graph: &ExecutableGraph) -> Self {
        Self {
            node_ids: graph.nodes.iter().map(|node| node.id.clone()).collect(),
        }
    }

    /// Derives the plan from exact per-node executor resolutions.
    #[must_use]
    pub fn from_resolutions<'a>(
        resolutions: impl IntoIterator<Item = &'a super::NodeExecutorIdentity>,
    ) -> Self {
        Self {
            node_ids: resolutions
                .into_iter()
                .map(|identity| identity.node_id.clone())
                .collect(),
        }
    }

    /// Whether the plan contains the exact node ID.
    #[must_use]
    pub fn contains(&self, node_id: &str) -> bool {
        self.node_ids.contains(node_id)
    }
}

/// Loop iteration state supplied to transition selection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LoopState {
    /// Zero-based completed loop iterations for the current node.
    pub loop_iteration: u32,
    /// Compiled static loop bound, when declared.
    pub max_iterations: Option<u32>,
}

impl LoopState {
    /// Whether the compiled loop bound is exhausted.
    #[must_use]
    pub const fn bound_exhausted(self) -> bool {
        match self.max_iterations {
            Some(limit) => self.loop_iteration >= limit,
            None => false,
        }
    }
}

/// One deterministic selected transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransitionSelection {
    /// Compiled source node.
    pub from: NodeCursor,
    /// Compiled destination node.
    pub to: NodeCursor,
    /// Optional stable edge label.
    pub label: Option<String>,
    /// Whether the caller must advance the loop iteration counter.
    pub advance_loop: bool,
}

/// Parallel fan-out selected by an explicit parallel-branch node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParallelSelection {
    /// Compiled source node.
    pub from: NodeCursor,
    /// Eligible destinations in compiled edge order.
    pub branches: Vec<BranchTarget>,
}

/// One parallel destination.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchTarget {
    /// Compiled destination node.
    pub to: NodeCursor,
    /// Optional stable edge label.
    pub label: Option<String>,
}

/// Generic transition selection result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransitionSelectionOutcome {
    /// A deterministic single-path transition.
    Selected(TransitionSelection),
    /// Explicit parallel fan-out with eligible edges.
    Parallel(ParallelSelection),
    /// The source is terminal and has no outgoing transition.
    Terminal,
}

/// Pure transition-selection failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TransitionError {
    /// The compiled node index is invalid.
    #[error("compiled graph node index {0} is invalid")]
    InvalidNodeIndex(usize),
    /// The compiled node ID was not found.
    #[error("compiled graph node `{0}` was not found")]
    UnknownNode(String),
    /// A condition could not be evaluated.
    #[error("condition evaluation failed at graph node `{node}`")]
    ConditionEvaluation {
        /// Compiled node ID.
        node: String,
    },
    /// A nonterminal node has no eligible outgoing edge.
    #[error("nonterminal graph node `{node}` has no eligible transition")]
    MissingTransition {
        /// Compiled node ID.
        node: String,
    },
    /// More than one edge is eligible without explicit parallel semantics.
    #[error("graph node `{node}` has more than one eligible transition without parallel semantics")]
    AmbiguousTransition {
        /// Compiled node ID.
        node: String,
    },
    /// The outcome is inconsistent with the compiled node kind.
    #[error("outcome `{outcome}` is inconsistent with node `{node}` of kind `{kind}`")]
    OutcomeInconsistent {
        /// Compiled node ID.
        node: String,
        /// Serialized node kind.
        kind: String,
        /// Outcome class label.
        outcome: String,
    },
    /// The selected destination is absent from the persisted execution plan.
    #[error(
        "transition from node `{node}` targets `{to}`, which is absent from the execution plan"
    )]
    UnknownDestination {
        /// Compiled source node ID.
        node: String,
        /// Destination node ID absent from the plan.
        to: String,
    },
    /// The outcome declared a variable write not covered by the graph.
    #[error("node `{node}` declared a write to undeclared scope `{scope}`")]
    UndeclaredVariableWrite {
        /// Compiled node ID.
        node: String,
        /// Scope name not covered by the node's declared write scopes.
        scope: String,
    },
    /// The outcome declared output above the engine bound.
    #[error("node `{node}` declared {bytes} output bytes above the {limit}-byte bound")]
    OutputExceededBounds {
        /// Compiled node ID.
        node: String,
        /// Declared output bytes.
        bytes: u64,
        /// Effective bound.
        limit: u64,
    },
    /// A repeat transition would exceed the compiled loop bound.
    #[error("node `{node}` would repeat iteration {iteration} beyond its compiled bound {limit}")]
    LoopBudgetExceeded {
        /// Compiled node ID.
        node: String,
        /// Completed loop iterations.
        iteration: u32,
        /// Compiled static bound.
        limit: u32,
    },
}

/// Selects the outgoing transition for one completed node.
///
/// `parallel_semantics` is derived from the compiled node kind: only
/// `ParallelBranch` nodes may fan out across multiple eligible edges.
///
/// # Errors
///
/// Returns [`TransitionError`] for every rejected case listed in the module
/// documentation.
pub fn select_transition(
    graph: &ExecutableGraph,
    from_index: usize,
    variables: &Value,
    outcome: Option<&NodeExecutionOutcome>,
    loop_state: &LoopState,
    plan: &NodePlan,
) -> Result<TransitionSelectionOutcome, TransitionError> {
    let from = graph
        .nodes
        .get(from_index)
        .filter(|node| node.index == from_index)
        .map(NodeCursor::from_executable)
        .ok_or(TransitionError::InvalidNodeIndex(from_index))?;

    if let Some(outcome) = outcome {
        if validate_outcome_for_kind(from.kind, outcome) == OutcomeCompatibility::Inconsistent {
            return Err(TransitionError::OutcomeInconsistent {
                node: from.id.clone(),
                kind: super::serialized_kind(from.kind),
                outcome: outcome.class_name().to_owned(),
            });
        }
        validate_declared_writes(&from, outcome)?;
        validate_output_bounds(&from, outcome)?;
    }

    let mut eligible = Vec::new();
    for edge in graph.edges.iter().filter(|edge| edge.from == from_index) {
        let selected = edge
            .condition
            .as_ref()
            .map_or(Ok(true), |condition| condition.evaluate(variables))
            .map_err(|_| TransitionError::ConditionEvaluation {
                node: from.id.clone(),
            })?;
        if selected {
            eligible.push(edge);
        }
    }

    let terminal = from.is_terminal_kind();
    if eligible.is_empty() {
        return if terminal {
            Ok(TransitionSelectionOutcome::Terminal)
        } else {
            Err(TransitionError::MissingTransition {
                node: from.id.clone(),
            })
        };
    }

    if from.kind == NodeKind::ParallelBranch {
        let mut branches = Vec::new();
        for edge in eligible {
            let to = cursor(graph, edge.to)?;
            plan_membership(&from, &to, plan)?;
            branches.push(BranchTarget {
                to,
                label: edge.label.clone(),
            });
        }
        return Ok(TransitionSelectionOutcome::Parallel(ParallelSelection {
            from,
            branches,
        }));
    }

    if eligible.len() > 1 {
        return Err(TransitionError::AmbiguousTransition {
            node: from.id.clone(),
        });
    }

    let edge = eligible[0];
    let to = cursor(graph, edge.to)?;
    plan_membership(&from, &to, plan)?;

    let advance_loop = from.kind == NodeKind::Loop && !to.is_terminal_kind();
    if advance_loop && loop_state.bound_exhausted() {
        return Err(TransitionError::LoopBudgetExceeded {
            node: from.id.clone(),
            iteration: loop_state.loop_iteration,
            limit: loop_state.max_iterations.unwrap_or(0),
        });
    }

    Ok(TransitionSelectionOutcome::Selected(TransitionSelection {
        from,
        to,
        label: edge.label.clone(),
        advance_loop,
    }))
}

fn cursor(graph: &ExecutableGraph, index: usize) -> Result<NodeCursor, TransitionError> {
    graph
        .nodes
        .get(index)
        .filter(|node| node.index == index)
        .map(NodeCursor::from_executable)
        .ok_or(TransitionError::InvalidNodeIndex(index))
}

fn plan_membership(
    from: &NodeCursor,
    to: &NodeCursor,
    plan: &NodePlan,
) -> Result<(), TransitionError> {
    if plan.contains(&to.id) {
        Ok(())
    } else {
        Err(TransitionError::UnknownDestination {
            node: from.id.clone(),
            to: to.id.clone(),
        })
    }
}

fn validate_declared_writes(
    from: &NodeCursor,
    outcome: &NodeExecutionOutcome,
) -> Result<(), TransitionError> {
    let (NodeExecutionOutcome::Completed { output }
    | NodeExecutionOutcome::CompleteTurn { output }
    | NodeExecutionOutcome::CompleteSession { output }) = outcome
    else {
        return Ok(());
    };
    for scope in &output.variable_writes {
        if !from_kind_allows_write(from.kind, scope) {
            return Err(TransitionError::UndeclaredVariableWrite {
                node: from.id.clone(),
                scope: scope.clone(),
            });
        }
    }
    Ok(())
}

/// Whether the compiled node may write the scope.
///
/// This mirrors the compiled graph contract: a node with declared write
/// scopes may only write those scopes; a node without declared write scopes
/// declares no variable writes at all. Condition-evaluation input variables
/// are read-only and are governed by the Task 4 variable interface instead.
fn from_kind_allows_write(kind: NodeKind, _scope: &str) -> bool {
    // Legacy compiled styles declare read scopes only. The engine therefore
    // requires outcomes to declare writes only when a future graph declares
    // write scopes; today no compiled node declares writes, so any declared
    // write is rejected as undeclared.
    //
    // A resolved write-scope contract arrives with Task 4; until then the
    // fail-closed rule is: declared writes are rejected unconditionally. No
    // runtime adapter currently declares writes, so this never permits a
    // write for live styles.
    let _ = kind;
    false
}

fn validate_output_bounds(
    from: &NodeCursor,
    outcome: &NodeExecutionOutcome,
) -> Result<(), TransitionError> {
    if let Some(bytes) = outcome
        .declared_bytes()
        .filter(|bytes| *bytes > MAX_NODE_OUTPUT_BYTES)
    {
        return Err(TransitionError::OutputExceededBounds {
            node: from.id.clone(),
            bytes,
            limit: MAX_NODE_OUTPUT_BYTES,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use agentmod_graph_engine::{ExecutableEdge, ExecutableGraph, ExecutableNode};
    use serde_json::json;

    use super::*;

    fn graph(nodes: Vec<ExecutableNode>, edges: Vec<ExecutableEdge>) -> ExecutableGraph {
        let hash = |salt: &[u8]| {
            let mut bytes = salt.to_vec();
            bytes.extend_from_slice(b"graph-test");
            agentmod_primitives::ContentHash::digest(&bytes)
        };
        ExecutableGraph {
            format_version: 1,
            entry_index: 0,
            budget: agentmod_graph_engine::GraphBudget {
                max_steps: 100,
                max_tokens: 100_000,
                max_cost_micros: 0,
                max_duration_ms: 0,
            },
            declarations: agentmod_graph_engine::GraphDeclarations::default(),
            nodes,
            edges,
            cache_key: agentmod_graph_engine::GraphCacheKey {
                graph_content_hash: hash(b"graph"),
                plugin_set_hash: hash(b"plugin"),
                capability_set_hash: hash(b"capability"),
                runtime_api_hash: hash(b"api"),
                combined_hash: hash(b"combined"),
            },
        }
    }

    fn node(index: usize, id: &str, kind: NodeKind) -> ExecutableNode {
        ExecutableNode {
            index,
            id: id.to_owned(),
            kind,
            condition: None,
            tool: None,
            provider: None,
            required_capabilities: BTreeSet::new(),
            read_scopes: BTreeSet::new(),
            write_scopes: BTreeSet::new(),
            retry_limit: 0,
            max_iterations: None,
        }
    }

    fn edge(from: usize, to: usize) -> ExecutableEdge {
        ExecutableEdge {
            from,
            to,
            condition: None,
            label: None,
        }
    }

    #[test]
    fn terminal_node_without_edges_is_terminal() {
        let g = graph(vec![node(0, "done", NodeKind::CompleteTurn)], Vec::new());
        assert_eq!(
            select_transition(
                &g,
                0,
                &json!({}),
                None,
                &LoopState::default(),
                &NodePlan::from_graph(&g)
            )
            .expect("selection"),
            TransitionSelectionOutcome::Terminal
        );
    }

    #[test]
    fn nonterminal_node_without_edges_is_rejected() {
        let g = graph(vec![node(0, "respond", NodeKind::ModelCall)], Vec::new());
        assert_eq!(
            select_transition(
                &g,
                0,
                &json!({}),
                None,
                &LoopState::default(),
                &NodePlan::from_graph(&g)
            ),
            Err(TransitionError::MissingTransition {
                node: String::from("respond")
            })
        );
    }

    #[test]
    fn multiple_eligible_edges_are_ambiguous_without_parallel_semantics() {
        let g = graph(
            vec![
                node(0, "branch", NodeKind::ConditionalBranch),
                node(1, "a", NodeKind::ModelCall),
                node(2, "b", NodeKind::ModelCall),
            ],
            vec![edge(0, 1), edge(0, 2)],
        );
        assert_eq!(
            select_transition(
                &g,
                0,
                &json!({}),
                None,
                &LoopState::default(),
                &NodePlan::from_graph(&g)
            ),
            Err(TransitionError::AmbiguousTransition {
                node: String::from("branch")
            })
        );
    }

    #[test]
    fn parallel_branch_fans_out_across_eligible_edges() {
        let g = graph(
            vec![
                node(0, "parallel", NodeKind::ParallelBranch),
                node(1, "a", NodeKind::ModelCall),
                node(2, "b", NodeKind::ModelCall),
            ],
            vec![edge(0, 1), edge(0, 2)],
        );
        let selection = select_transition(
            &g,
            0,
            &json!({}),
            None,
            &LoopState::default(),
            &NodePlan::from_graph(&g),
        )
        .expect("selection");
        let TransitionSelectionOutcome::Parallel(parallel) = selection else {
            panic!("expected parallel selection");
        };
        assert_eq!(parallel.branches.len(), 2);
        assert_eq!(parallel.branches[0].to.id, "a");
        assert_eq!(parallel.branches[1].to.id, "b");
    }

    #[test]
    fn destination_absent_from_plan_is_rejected() {
        let g = graph(
            vec![
                node(0, "respond", NodeKind::ModelCall),
                node(1, "tool", NodeKind::ToolExecutionGate),
            ],
            vec![edge(0, 1)],
        );
        let plan = NodePlan {
            node_ids: BTreeSet::from([String::from("respond")]),
        };
        assert_eq!(
            select_transition(&g, 0, &json!({}), None, &LoopState::default(), &plan),
            Err(TransitionError::UnknownDestination {
                node: String::from("respond"),
                to: String::from("tool")
            })
        );
    }

    #[test]
    fn loop_bound_rejects_repeat_after_exhaustion() {
        // Declarative-style loop: the repeat edge is conditional on
        // `iteration.remaining`, the exit edge on its negation.
        let mut repeat = node(0, "repeat", NodeKind::Loop);
        repeat.max_iterations = Some(2);
        let g = graph(
            vec![
                repeat,
                node(1, "work", NodeKind::ModelCall),
                node(2, "done", NodeKind::CompleteSession),
            ],
            vec![
                ExecutableEdge {
                    from: 0,
                    to: 1,
                    condition: None,
                    label: Some(String::from("repeat")),
                },
                ExecutableEdge {
                    from: 0,
                    to: 2,
                    condition: None,
                    label: Some(String::from("done")),
                },
            ],
        );
        // Both edges unconditional: ambiguous before the loop check.
        assert_eq!(
            select_transition(
                &g,
                0,
                &json!({}),
                None,
                &LoopState {
                    loop_iteration: 2,
                    max_iterations: Some(2),
                },
                &NodePlan::from_graph(&g)
            ),
            Err(TransitionError::AmbiguousTransition {
                node: String::from("repeat")
            })
        );

        // A single unconditional repeat edge at the bound is rejected by the
        // loop check (nonterminal destination).
        let g_single = graph(
            vec![
                {
                    let mut repeat = node(0, "repeat", NodeKind::Loop);
                    repeat.max_iterations = Some(2);
                    repeat
                },
                node(1, "work", NodeKind::ModelCall),
            ],
            vec![edge(0, 1)],
        );
        assert_eq!(
            select_transition(
                &g_single,
                0,
                &json!({}),
                None,
                &LoopState {
                    loop_iteration: 2,
                    max_iterations: Some(2),
                },
                &NodePlan::from_graph(&g_single)
            ),
            Err(TransitionError::LoopBudgetExceeded {
                node: String::from("repeat"),
                iteration: 2,
                limit: 2
            })
        );
    }

    #[test]
    fn terminal_destination_at_loop_bound_is_allowed() {
        let g = graph(
            vec![
                {
                    let mut repeat = node(0, "repeat", NodeKind::Loop);
                    repeat.max_iterations = Some(2);
                    repeat
                },
                node(1, "done", NodeKind::CompleteSession),
            ],
            vec![edge(0, 1)],
        );
        let selection = select_transition(
            &g,
            0,
            &json!({}),
            None,
            &LoopState {
                loop_iteration: 2,
                max_iterations: Some(2),
            },
            &NodePlan::from_graph(&g),
        )
        .expect("selection");
        let TransitionSelectionOutcome::Selected(selected) = selection else {
            panic!("expected selected");
        };
        assert_eq!(selected.to.id, "done");
        assert!(!selected.advance_loop);
    }

    #[test]
    fn advance_loop_is_computed_from_kind_and_destination() {
        let g = graph(
            vec![
                {
                    let mut repeat = node(0, "repeat", NodeKind::Loop);
                    repeat.max_iterations = Some(3);
                    repeat
                },
                node(1, "work", NodeKind::ModelCall),
            ],
            vec![edge(0, 1)],
        );
        let selection = select_transition(
            &g,
            0,
            &json!({}),
            None,
            &LoopState {
                loop_iteration: 0,
                max_iterations: Some(3),
            },
            &NodePlan::from_graph(&g),
        )
        .expect("selection");
        let TransitionSelectionOutcome::Selected(selected) = selection else {
            panic!("expected selected");
        };
        assert!(selected.advance_loop);
    }
}
