//! Focused regression tests: built-in styles execute through the generic node
//! dispatch path, style ID does not affect compiled semantics, structurally
//! different compatible graphs dispatch, and deterministic transition
//! selection is order-independent (property tested).

use std::collections::BTreeSet;

use agentmod_graph_engine::{ExecutableEdge, ExecutableGraph, ExecutableNode, NodeKind};
use agentmod_runtime_data::node_executor::RuntimeNodeExecutorData;
use agentmod_session_style_sdk::BuiltInStyle;
use serde_json::{Value, json};

use crate::{
    node_execution::{
        BoundedNodeOutput, DispatchError, ExecuteNodeCommand, NodeExecutionInput,
        NodeExecutionOutcome, NodeExecutorIdentity, NodeExecutorPort, NodePlan, dispatch_node,
        reducer::{NodeDispatchDecision, NodeDispatchReducer},
        select_transition,
        transition::{LoopState, TransitionError, TransitionSelectionOutcome},
    },
    node_executor::{dispatch_plan, inspect_runtime_executability},
    style_executor::CompiledStyleExecutor,
    style_executor::tests::binding,
};

/// Mock port that completes every node with the typed outcome its kind
/// requires. It never fabricates external-effect completion beyond the typed
/// outcome contract.
struct BuiltInWalkPort;

impl NodeExecutorPort for BuiltInWalkPort {
    fn can_execute(&self, _identity: &NodeExecutorIdentity) -> bool {
        true
    }

    fn execute(&self, command: &ExecuteNodeCommand) -> Result<NodeExecutionOutcome, DispatchError> {
        let output = BoundedNodeOutput::reference(format!("result:{}", command.step));
        Ok(match command.node.kind {
            NodeKind::CompleteTurn => NodeExecutionOutcome::CompleteTurn { output },
            NodeKind::CompleteSession => NodeExecutionOutcome::CompleteSession { output },
            _ => NodeExecutionOutcome::Completed { output },
        })
    }
}

/// Variables that force a terminating walk through each built-in graph.
fn terminating_variables(style: BuiltInStyle, node_id: &str) -> Value {
    match (style, node_id) {
        (BuiltInStyle::ResearchLoop, "repeat") => {
            json!({"completion":{"criteria_met":true}})
        }
        (BuiltInStyle::DeclarativeGraph, "branch") => {
            json!({"request":{"requires_approval":false}})
        }
        (BuiltInStyle::DeclarativeGraph, "repeat") => {
            json!({"iteration":{"remaining":false}})
        }
        (BuiltInStyle::PlannerWorker, "revision") => json!({"review":{"approved":true}}),
        _ => json!({}),
    }
}

/// Walks one compiled style through the generic dispatch engine: entry to
/// terminal, following selected transitions. Returns the visited node IDs in
/// order.
fn walk_built_in(style: BuiltInStyle) -> Vec<String> {
    let binding = binding(style);
    let executor = CompiledStyleExecutor::from_binding(&binding).expect("executor");
    let registry = RuntimeNodeExecutorData::native().expect("registry");
    let report = inspect_runtime_executability(&registry, &binding).expect("executability");
    assert!(report.executable, "{style:?}: {:?}", report.diagnostics);
    let plan = dispatch_plan(&report);
    let mut visited = Vec::new();
    let mut cursor = executor.entry().expect("entry");
    let mut step = 1u64;
    let mut loop_iteration = 0u32;
    for _ in 0..64 {
        visited.push(cursor.id.clone());
        let identity = plan
            .get(&cursor.id)
            .unwrap_or_else(|| panic!("{style:?}: no resolved executor for {}", cursor.id))
            .clone();
        let variables = terminating_variables(style, &cursor.id);
        let command = executor
            .dispatch_command(
                &cursor,
                identity.clone(),
                NodeExecutionInput {
                    variables: variables.clone(),
                    ..Default::default()
                },
                1,
                loop_iteration,
                step,
                256,
            )
            .expect("dispatch command");
        let outcome = dispatch_node(&BuiltInWalkPort, &command).expect("dispatch");
        // The compiled-style dispatch seam validates the same typed outcome.
        executor
            .validate_outcome(&cursor, &outcome)
            .expect("validated outcome");
        // The compiled-style dispatch entry executes through the same port.
        let seam_outcome = executor
            .dispatch(
                &BuiltInWalkPort,
                &cursor,
                identity.clone(),
                NodeExecutionInput {
                    variables: variables.clone(),
                    ..Default::default()
                },
                1,
                loop_iteration,
                step,
                256,
            )
            .expect("seam dispatch");
        assert_eq!(seam_outcome, outcome);
        let decision = NodeDispatchReducer
            .reduce(
                &executor.compiled().graph,
                &command,
                &outcome,
                &variables,
                &LoopState {
                    loop_iteration,
                    max_iterations: command.node.max_iterations,
                },
                &executor.dispatch_plan(),
            )
            .expect("reduce");
        match decision {
            NodeDispatchDecision::Completed {
                transition: Some(transition),
                advance_loop,
                ..
            } => {
                cursor = executor.node(&transition.to.id).expect("destination");
                step = step.saturating_add(1);
                if advance_loop {
                    loop_iteration = loop_iteration.saturating_add(1);
                }
            }
            NodeDispatchDecision::TerminalTurn { .. }
            | NodeDispatchDecision::TerminalSession { .. } => {
                return visited;
            }
            NodeDispatchDecision::Completed {
                transition: None, ..
            } => {
                panic!("{style:?}: nonterminal graph ended without a transition");
            }
            other => panic!("{style:?}: unexpected dispatch decision: {other:?}"),
        }
    }
    panic!("{style:?}: walk did not terminate within 64 steps");
}

#[test]
fn every_built_in_style_executes_through_the_generic_dispatch_path() {
    for (style, expected_terminal) in [
        (BuiltInStyle::PersistentChat, "done"),
        (BuiltInStyle::EphemeralTurn, "done"),
        (BuiltInStyle::ResearchLoop, "done"),
        (BuiltInStyle::PlannerWorker, "done"),
        (BuiltInStyle::DeclarativeGraph, "done"),
    ] {
        let visited = walk_built_in(style);
        assert_eq!(
            visited.last().map(String::as_str),
            Some(expected_terminal),
            "{style:?}: {visited:?}"
        );
        // Every visited node resolved through the exact identity plan: no
        // topology adapter profile was consulted during the walk.
        let binding = binding(style);
        let registry = RuntimeNodeExecutorData::native().expect("registry");
        let report = inspect_runtime_executability(&registry, &binding).expect("executability");
        let plan = dispatch_plan(&report);
        for node_id in &visited {
            assert!(
                plan.contains_key(node_id),
                "{style:?}: {node_id} missing from the identity plan"
            );
        }
    }
}

#[test]
fn style_id_does_not_affect_identical_compiled_semantics() {
    // Recompile the persistent-chat manifest under a different style ID and
    // assert the generic dispatch behavior is identical.
    let canonical = binding(BuiltInStyle::PersistentChat);
    let plugin_set_hash = agentmod_primitives::ContentHash::digest(b"plugins");
    let context = crate::style_executor::tests::compile_context(plugin_set_hash);
    let mut disguised_manifest =
        agentmod_session_style_sdk::built_in_manifest(BuiltInStyle::PersistentChat);
    disguised_manifest.identity.id = String::from("project.custom-chat");
    disguised_manifest.identity.version = String::from("9.9.9");
    let compiled = agentmod_session_style_sdk::compile_style(
        &disguised_manifest,
        &context,
        agentmod_session_style_sdk::StyleCompilerLimits::default(),
    )
    .expect("compile disguised");
    let mut disguised = canonical.clone();
    disguised.id = compiled.style_id.clone();
    disguised.version = compiled.style_version.clone();
    disguised.content_hash = compiled.cache_key.style_content_hash;
    disguised.compiled_cache_key = compiled.cache_key.combined_hash;
    disguised.compiled_style_hash = agentmod_primitives::ContentHash::digest(
        serde_json::to_string(&compiled)
            .expect("compiled json")
            .as_bytes(),
    );
    disguised.capability_set_hash = compiled.cache_key.capability_set_hash;
    disguised.compiled_style_json = serde_json::to_string(&compiled).expect("compiled json");
    let executor_canonical =
        CompiledStyleExecutor::from_binding(&canonical).expect("canonical executor");
    let executor_disguised =
        CompiledStyleExecutor::from_binding(&disguised).expect("disguised executor");
    assert_eq!(
        executor_canonical.entry_kind().expect("entry kind"),
        executor_disguised.entry_kind().expect("entry kind")
    );
    let canonical_entry = executor_canonical.entry().expect("entry");
    let disguised_entry = executor_disguised.entry().expect("entry");
    assert_eq!(canonical_entry.id, disguised_entry.id);
    assert_eq!(
        executor_canonical.compiled().graph,
        executor_disguised.compiled().graph
    );
    // Both graphs resolve through the exact same executor identities.
    let registry = RuntimeNodeExecutorData::native().expect("registry");
    let canonical_report =
        inspect_runtime_executability(&registry, &canonical).expect("canonical report");
    let disguised_report =
        inspect_runtime_executability(&registry, &disguised).expect("disguised report");
    assert!(canonical_report.executable && disguised_report.executable);
    assert_eq!(
        dispatch_plan(&canonical_report),
        dispatch_plan(&disguised_report)
    );
}

#[test]
fn structurally_different_compatible_graphs_dispatch_identically() {
    // A persistent-chat-shaped graph with a different node count and IDs
    // dispatches through the generic engine exactly like the built-in shape.
    let g = graph(
        vec![
            node(0, "ask", NodeKind::ModelCall),
            node(1, "act", NodeKind::ToolExecutionGate),
            node(2, "finish", NodeKind::CompleteTurn),
        ],
        vec![edge(0, 1), edge(1, 2)],
    );
    let plan = NodePlan::from_graph(&g);
    let engine = NodeDispatchReducer;

    let mut cursor = cursor(&g, 0);
    let mut step = 1u64;
    loop {
        let command = ExecuteNodeCommand {
            node: cursor.clone(),
            executor: NodeExecutorIdentity {
                node_id: cursor.id.clone(),
                node_kind: String::from("model_call"),
                implementation_id: String::from("runtime.model_call"),
                implementation_version: String::from("1.0.0"),
                boundary: crate::node_execution::ExecutorBoundary::RuntimeLogic,
            },
            input: NodeExecutionInput::default(),
            attempt: 1,
            loop_iteration: 0,
            step,
            max_steps: 64,
        };
        let outcome = dispatch_node(&BuiltInWalkPort, &command).expect("dispatch");
        let decision = engine
            .reduce(
                &g,
                &command,
                &outcome,
                &json!({}),
                &LoopState::default(),
                &plan,
            )
            .expect("reduce");
        match decision {
            NodeDispatchDecision::Completed {
                transition: Some(transition),
                ..
            } => {
                cursor = transition.to;
                step = step.saturating_add(1);
            }
            NodeDispatchDecision::TerminalTurn { .. } => break,
            other => panic!("unexpected decision: {other:?}"),
        }
    }
    assert_eq!(step, 3);
}

fn graph(nodes: Vec<ExecutableNode>, edges: Vec<ExecutableEdge>) -> ExecutableGraph {
    let hash = |salt: &[u8]| {
        let mut bytes = salt.to_vec();
        bytes.extend_from_slice(b"dispatch-test");
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

fn cursor(graph: &ExecutableGraph, index: usize) -> crate::node_execution::NodeCursor {
    crate::node_execution::NodeCursor::from_executable(
        graph
            .nodes
            .get(index)
            .filter(|node| node.index == index)
            .expect("node"),
    )
}

/// Property tests: deterministic transition selection is repeatable and
/// independent of edge insertion order.
mod deterministic {
    use proptest::prelude::*;

    use super::*;

    fn destination_kind() -> impl Strategy<Value = NodeKind> {
        prop_oneof![
            Just(NodeKind::ModelCall),
            Just(NodeKind::ToolExecutionGate),
            Just(NodeKind::CompleteTurn),
            Just(NodeKind::CompleteSession),
            Just(NodeKind::Loop),
        ]
    }

    /// Deterministic Fisher-Yates with a fixed seed: no external rng crate.
    fn shuffle<T: Clone>(mut items: Vec<T>, seed: u64) -> Vec<T> {
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let len = items.len();
        for index in (1..len).rev() {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let swap = (state >> 33) as usize % (index + 1);
            items.swap(index, swap);
        }
        items
    }

    proptest! {
        #[test]
        fn single_eligible_selection_is_order_and_repeat_independent(
            dests in prop::collection::vec(destination_kind(), 1..4),
            shuffle_seed in any::<u64>(),
        ) {
            let mut nodes = vec![node(0, "source", NodeKind::ModelCall)];
            let mut edges = Vec::new();
            for (index, kind) in dests.iter().enumerate() {
                nodes.push(node(index + 1, &format!("d{index}"), *kind));
                edges.push(ExecutableEdge {
                    from: 0,
                    to: index + 1,
                    condition: None,
                    label: Some(format!("edge-{index}")),
                });
            }
            let plan = NodePlan::from_graph(&graph(nodes.clone(), edges.clone()));

            // Every single-edge graph selects the same destination on every
            // repeat, independent of edge label and order.
            for (index, _) in dests.iter().enumerate() {
                let single_graph = graph(nodes.clone(), vec![edges[index].clone()]);
                let selected = select_transition(
                    &single_graph, 0, &json!({}), None, &LoopState::default(), &plan,
                );
                for _ in 0..3 {
                    let again = select_transition(
                        &single_graph, 0, &json!({}), None, &LoopState::default(), &plan,
                    );
                    prop_assert_eq!(&selected, &again);
                }
                if let Ok(TransitionSelectionOutcome::Selected(selection)) = &selected {
                    let expected = String::from("d") + &index.to_string();
                    prop_assert!(selection.to.id.as_str() == expected.as_str());
                }
            }

            // Multi-edge ambiguity is detected regardless of edge order.
            let shuffled = shuffle(edges.clone(), shuffle_seed);
            let mut shuffled_graph = graph(nodes.clone(), shuffled);
            shuffled_graph.entry_index = 0;
            let ambiguous = select_transition(
                &shuffled_graph, 0, &json!({}), None, &LoopState::default(), &plan,
            );
            if dests.len() > 1 {
                let ambiguous_rejected = matches!(
                    &ambiguous,
                    Err(TransitionError::AmbiguousTransition { .. })
                );
                prop_assert!(ambiguous_rejected);
            }
        }
    }

    #[test]
    fn repeated_selection_never_diverges_on_built_in_graphs() {
        use crate::style_executor::tests::binding as style_binding;
        for style in [
            BuiltInStyle::PersistentChat,
            BuiltInStyle::EphemeralTurn,
            BuiltInStyle::ResearchLoop,
            BuiltInStyle::PlannerWorker,
            BuiltInStyle::DeclarativeGraph,
        ] {
            let executor =
                CompiledStyleExecutor::from_binding(&style_binding(style)).expect("executor");
            let graph = &executor.compiled().graph;
            for node in &graph.nodes {
                let first = executor.transition(node.index, &json!({}));
                for _ in 0..5 {
                    assert_eq!(
                        first,
                        executor.transition(node.index, &json!({})),
                        "{style:?}: {} diverged",
                        node.id
                    );
                }
            }
        }
    }
}
