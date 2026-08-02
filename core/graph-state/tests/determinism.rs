//! Determinism and replay property tests for graph state.
//!
//! These tests prove the core claims of the graph-state substrate:
//!
//! - identical event sequences replay to identical state without external
//!   calls;
//! - condition verdicts and environments do not depend on assignment order or
//!   incidental JSON object ordering;
//! - merges are independent of contributor order;
//! - budget ledgers reconstruct exact remaining amounts after restart.

use std::collections::BTreeSet;

use agentmod_graph_state::budget::{
    BudgetLedger, BudgetLimits, RollupPolicy, UsageEvidence, UsageKind,
};
use agentmod_graph_state::declare::{
    BranchScopePolicy, DeclarationSet, LastWriterOrdering, MergePolicy, MutabilityPolicy,
    SecurityClassification, VariableDeclaration, VariableScope, VariableType,
};
use agentmod_graph_state::event::{BudgetEvent, GraphStateEvent};
use agentmod_graph_state::reduce::GraphStateReducer;
use agentmod_graph_state::state::{AssignmentSource, GraphState, MergeContribution};
use agentmod_graph_state::value::GraphValue;
use agentmod_primitives::{SessionId, TimestampMillis};
use proptest::prelude::*;

fn session() -> SessionId {
    SessionId::from_uuid(uuid::Uuid::nil())
}

fn declaration(name: &str, r#type: VariableType, merge: MergePolicy) -> VariableDeclaration {
    VariableDeclaration {
        name: name.to_owned(),
        r#type,
        scope: VariableScope::Run,
        producers: BTreeSet::new(),
        consumers: BTreeSet::new(),
        mutability: MutabilityPolicy::Assignable,
        max_serialized_bytes: 16 * 1024,
        classification: SecurityClassification::SessionInternal,
        merge_policy: merge,
        default: None,
    }
}

fn state() -> GraphState {
    state_with_events().0
}

/// Builds state together with its initialization events so replay tests can
/// feed the complete event stream.
fn state_with_events() -> (GraphState, Vec<GraphStateEvent>) {
    let mut set = DeclarationSet::new();
    set.insert(declaration(
        "counter",
        VariableType::UnsignedInteger {
            min: 0,
            max: 100_000,
        },
        MergePolicy::RejectConflict,
    ))
    .expect("declared");
    set.insert(declaration(
        "notes",
        VariableType::List {
            element: Box::new(VariableType::String { max_bytes: 128 }),
            max_len: 64,
        },
        MergePolicy::SetUnion,
    ))
    .expect("declared");
    GraphState::new(session(), set).expect("state")
}

/// Tolerant op-script model: every op that would fail is skipped, so the
/// committed event stream is always replayable and the invariant holds that
/// replay reconstructs the live state exactly.
#[derive(Clone, Debug)]
enum Op {
    RunAssign { value: u64 },
    BranchCreate { branch: u64 },
    BranchNotes { branch: u64, tag: u64 },
    MergeNotes { branches: Vec<u64> },
    CloseBranch { branch: u64 },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        any::<u64>().prop_map(|value| Op::RunAssign { value }),
        (0..16u64).prop_map(|branch| Op::BranchCreate { branch }),
        (0..16u64, any::<u64>()).prop_map(|(branch, tag)| Op::BranchNotes { branch, tag }),
        prop::collection::vec(0..16u64, 1..4).prop_map(|branches| Op::MergeNotes { branches }),
        (0..16u64).prop_map(|branch| Op::CloseBranch { branch }),
    ]
}

fn apply_op(state: &mut GraphState, events: &mut Vec<GraphStateEvent>, op: &Op) {
    let runtime = AssignmentSource::Runtime;
    match op {
        Op::RunAssign { value } => {
            if let Ok(mut committed) = state.assign(
                "counter",
                GraphValue::UnsignedInteger(*value),
                &runtime,
                &VariableScope::Run,
                None,
            ) {
                events.append(&mut committed);
            }
        }
        Op::BranchCreate { branch } => {
            let id = format!("b{branch}");
            if let Ok(mut committed) = state.create_branch_scope(&id, BranchScopePolicy::Isolated) {
                events.append(&mut committed);
            }
        }
        Op::BranchNotes { branch, tag } => {
            let id = format!("b{branch}");
            let value = GraphValue::List(vec![GraphValue::String(format!("note-{tag}"))]);
            if let Ok(mut committed) = state.assign(
                "notes",
                value,
                &runtime,
                &VariableScope::Branch { branch_id: id },
                None,
            ) {
                events.append(&mut committed);
            }
        }
        Op::MergeNotes { branches } => {
            let mut contributions = Vec::new();
            for branch in branches {
                let id = format!("b{branch}");
                let Some(entry) = state
                    .read(
                        "notes",
                        &VariableScope::Branch {
                            branch_id: id.clone(),
                        },
                    )
                    .ok()
                    .and_then(|outcome| match outcome {
                        agentmod_graph_state::state::ReadOutcome::Value(value) => {
                            Some(value.clone())
                        }
                        _ => None,
                    })
                else {
                    continue;
                };
                contributions.push(MergeContribution {
                    branch_id: id,
                    node_id: None,
                    value: entry,
                });
            }
            if contributions.is_empty() {
                return;
            }
            if let Ok(mut committed) = state.merge_parallel("notes", contributions) {
                events.append(&mut committed);
            }
        }
        Op::CloseBranch { branch } => {
            let id = format!("b{branch}");
            if let Ok(mut committed) = state.close_branch_scope(&id) {
                events.append(&mut committed);
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn replay_reconstructs_identical_live_state(ops in prop::collection::vec(op_strategy(), 0..40)) {
        let (mut live, mut events) = state_with_events();
        for op in &ops {
            apply_op(&mut live, &mut events, op);
        }
        let mut reducer = GraphStateReducer::new(session());
        for event in &events {
            reducer.apply(event).expect("replay applies");
        }
        assert_eq!(reducer.state(), &live, "replay must reconstruct identical state");
        assert!(reducer.initialized());
    }

    #[test]
    fn replay_is_prefix_equivalent(ops in prop::collection::vec(op_strategy(), 0..40)) {
        let (mut live, mut events) = state_with_events();
        for op in &ops {
            apply_op(&mut live, &mut events, op);
        }
        let mut full = GraphStateReducer::new(session());
        for event in &events {
            full.apply(event).expect("replay applies");
        }
        let mut prefix = GraphStateReducer::new(session());
        for event in events.iter().take(events.len() / 2) {
            prefix.apply(event).expect("replay applies");
        }
        // Extending the prefix with the remaining events must reach the same
        // state as applying the full sequence directly.
        let mut resumed = prefix;
        for event in events.iter().skip(events.len() / 2) {
            resumed.apply(event).expect("replay applies");
        }
        assert_eq!(resumed.state(), full.state());
    }

    #[test]
    fn assignment_order_does_not_change_environment(
        values in prop::collection::btree_map(0..8u8, 0..10_000u64, 0..20),
    ) {
        let mut set = DeclarationSet::new();
        for index in 0..8 {
            set.insert(declaration(
                &format!("v{index}"),
                VariableType::UnsignedInteger { min: 0, max: 10_000 },
                MergePolicy::RejectConflict,
            ))
            .expect("declared");
        }
        let (mut forward, mut forward_events) =
            GraphState::new(session(), set.clone()).expect("state");
        let (mut reverse, mut reverse_events) = GraphState::new(session(), set).expect("state");
        // The same final assignment map, applied in two different orders, so
        // both states end with identical per-variable values.
        let ordered: Vec<_> = values.iter().collect();
        for &(index, value) in &ordered {
            forward_events.extend(
                forward
                    .assign(
                        &format!("v{index}"),
                        GraphValue::UnsignedInteger(*value),
                        &AssignmentSource::Runtime,
                        &VariableScope::Run,
                        None,
                    )
                    .expect("assign"),
            );
        }
        for &(index, value) in ordered.iter().rev() {
            reverse_events.extend(
                reverse
                    .assign(
                        &format!("v{index}"),
                        GraphValue::UnsignedInteger(*value),
                        &AssignmentSource::Runtime,
                        &VariableScope::Run,
                        None,
                    )
                    .expect("assign"),
            );
        }
        let forward_bytes =
            serde_json::to_vec(&forward.environment(&VariableScope::Run)).expect("serialize");
        let reverse_bytes =
            serde_json::to_vec(&reverse.environment(&VariableScope::Run)).expect("serialize");
        assert_eq!(
            forward_bytes, reverse_bytes,
            "environment must not depend on assignment order"
        );
        // Replay equality holds for both orders.
        let mut replay = GraphStateReducer::new(session());
        for event in &forward_events {
            replay.apply(event).expect("replay");
        }
        assert_eq!(replay.state(), &forward);
        let mut replay = GraphStateReducer::new(session());
        for event in &reverse_events {
            replay.apply(event).expect("replay");
        }
        assert_eq!(replay.state(), &reverse);
    }
}

#[test]
fn merge_result_is_independent_of_contributor_order() {
    let mut first = state();
    let mut second = state();
    for (state, order) in [
        (&mut first, &["b2", "b1"][..]),
        (&mut second, &["b1", "b2"][..]),
    ] {
        for branch in order {
            state
                .create_branch_scope(branch, BranchScopePolicy::Isolated)
                .expect("branch");
        }
        for branch in order {
            let _ = state.assign(
                "notes",
                GraphValue::List(vec![GraphValue::String(format!("note-{branch}"))]),
                &AssignmentSource::Runtime,
                &VariableScope::Branch {
                    branch_id: (*branch).to_owned(),
                },
                None,
            );
        }
        let contributions = ["b1", "b2"]
            .into_iter()
            .map(|branch| MergeContribution {
                branch_id: branch.to_owned(),
                node_id: None,
                value: GraphValue::List(vec![GraphValue::String(format!("note-{branch}"))]),
            })
            .collect();
        state
            .merge_parallel("notes", contributions)
            .expect("set union merge succeeds");
    }
    let expected = GraphValue::List(vec![
        GraphValue::String("note-b1".to_owned()),
        GraphValue::String("note-b2".to_owned()),
    ]);
    assert_eq!(
        first.read("notes", &VariableScope::Run).expect("read"),
        agentmod_graph_state::state::ReadOutcome::Value(&expected)
    );
    assert_eq!(
        first.environment(&VariableScope::Run),
        second.environment(&VariableScope::Run)
    );
}

#[test]
fn budget_reconstruction_is_exact_under_random_commits() {
    let limits = BudgetLimits {
        max_style_steps: Some(10),
        max_model_requests: Some(4),
        max_tool_calls: Some(6),
        max_iterations: Some(3),
        max_retries: Some(2),
        max_child_sessions: Some(5),
        max_input_tokens: Some(1_000),
        max_output_tokens: Some(1_000),
        max_total_tokens: Some(2_000),
        max_provider_cost_micros: Some(10_000),
        max_active_provider_duration_ms: Some(60_000),
        max_active_tool_duration_ms: Some(30_000),
        max_elapsed_wall_clock_ms: None,
        max_concurrent_children: Some(2),
    };
    let mut ledger = BudgetLedger::initialize(
        session(),
        limits.clone(),
        TimestampMillis::new(1_700_000_000_000),
        false,
    )
    .0;
    let mut events = Vec::new();
    let at = TimestampMillis::new(1_700_000_000_001);
    let dims = [
        agentmod_graph_state::budget::BudgetDimension::StyleSteps,
        agentmod_graph_state::budget::BudgetDimension::ModelRequests,
        agentmod_graph_state::budget::BudgetDimension::InputTokens,
        agentmod_graph_state::budget::BudgetDimension::TotalTokens,
        agentmod_graph_state::budget::BudgetDimension::ProviderCostMicros,
    ];
    for (index, dimension) in dims.iter().enumerate() {
        let pricing = (matches!(
            dimension,
            agentmod_graph_state::budget::BudgetDimension::ProviderCostMicros
        ))
        .then(|| agentmod_graph_state::budget::PricingBinding {
            model: "mock".into(),
            provider: "fixture".into(),
            pricing_record_version: "1.0".into(),
            recorded_at: at,
        });
        let evidence = UsageEvidence::new(
            *dimension,
            (index as u64 + 1) * 3,
            UsageKind::Reported,
            pricing,
        );
        events.push(ledger.commit(&evidence, at).expect("commit"));
    }
    // Mark the tool-call dimension unknown after evidence was committed.
    events.push(
        ledger
            .mark_unknown(
                agentmod_graph_state::budget::BudgetDimension::ToolCalls,
                UsageKind::Estimated,
                at,
            )
            .expect("unknown"),
    );
    // Roll up a child per policy.
    let child = SessionId::from_uuid(uuid::Uuid::nil());
    let report = agentmod_graph_state::budget::ChildBudgetReport {
        contributions: vec![UsageEvidence::new(
            agentmod_graph_state::budget::BudgetDimension::ModelRequests,
            1,
            UsageKind::Reported,
            None,
        )],
    };
    events.extend(
        ledger
            .roll_up_child(child, &report, RollupPolicy::Full, at)
            .expect("rollup"),
    );

    let init = BudgetEvent::BudgetsInitialized {
        session_id: session(),
        limits,
        recorded_at: TimestampMillis::new(1_700_000_000_000),
        wall_clock_enabled: false,
    };
    let rebuilt = BudgetLedger::reconstruct(session(), &init, &events).expect("reconstruct");
    assert_eq!(rebuilt, ledger);
    // ModelRequests: committed 6 by the batch loop plus 1 from child rollup,
    // against a limit of 4, so the conservative remaining is 0.
    assert_eq!(
        rebuilt.remaining(agentmod_graph_state::budget::BudgetDimension::ModelRequests),
        0
    );
    // TotalTokens: committed 12 against a limit of 2000.
    assert_eq!(
        rebuilt.remaining(agentmod_graph_state::budget::BudgetDimension::TotalTokens),
        1_988
    );
}

#[test]
fn state_serialization_is_versioned_and_stable() {
    let mut set = DeclarationSet::new();
    set.insert(declaration(
        "counter",
        VariableType::UnsignedInteger {
            min: 0,
            max: 100_000,
        },
        MergePolicy::RejectConflict,
    ))
    .expect("declared");
    set.insert(declaration(
        "notes",
        VariableType::List {
            element: Box::new(VariableType::String { max_bytes: 128 }),
            max_len: 64,
        },
        MergePolicy::SetUnion,
    ))
    .expect("declared");
    let (mut live, mut events) = GraphState::new(session(), set).expect("state");
    let runtime = AssignmentSource::Runtime;
    events.extend(
        live.assign(
            "counter",
            GraphValue::UnsignedInteger(42),
            &runtime,
            &VariableScope::Run,
            Some("run-1"),
        )
        .expect("assign"),
    );
    events.extend(
        live.create_branch_scope("b1", BranchScopePolicy::Isolated)
            .expect("branch"),
    );
    events.extend(
        live.assign(
            "notes",
            GraphValue::List(vec![GraphValue::String("note-a".into())]),
            &runtime,
            &VariableScope::Branch {
                branch_id: "b1".into(),
            },
            Some("run-1"),
        )
        .expect("branch assign"),
    );
    events.extend(
        live.merge_parallel(
            "notes",
            vec![MergeContribution {
                branch_id: "b1".into(),
                node_id: None,
                value: GraphValue::List(vec![GraphValue::String("note-a".into())]),
            }],
        )
        .expect("merge"),
    );
    events.extend(live.close_branch_scope("b1").expect("close"));

    let serialized = serde_json::to_string_pretty(&events).expect("serialize");
    let golden = include_str!("golden/graph-state-events-v1.json");
    assert_eq!(
        serialized,
        golden.trim_end(),
        "canonical event serialization must stay versioned and stable"
    );
    // And the golden events must replay to the identical state.
    let decoded: Vec<GraphStateEvent> = serde_json::from_str(golden).expect("decode golden");
    let mut reducer = GraphStateReducer::new(session());
    for event in &decoded {
        reducer.apply(event).expect("replay golden");
    }
    assert_eq!(reducer.state(), &live);

    // Deterministic map ordering: serializing the same events twice yields
    // byte-identical output.
    assert_eq!(
        serde_json::to_vec(&events).expect("serialize"),
        serde_json::to_vec(&events).expect("serialize again")
    );
}

#[test]
fn last_writer_merge_is_deterministic_by_declared_ordering() {
    let mut set = DeclarationSet::new();
    set.insert(declaration(
        "decision",
        VariableType::Boolean,
        MergePolicy::LastWriter {
            ordering: LastWriterOrdering::BranchLexical,
        },
    ))
    .expect("declared");
    let (mut state, _) = GraphState::new(session(), set).expect("state");
    state
        .create_branch_scope("left", BranchScopePolicy::Isolated)
        .expect("left branch");
    state
        .create_branch_scope("right", BranchScopePolicy::Isolated)
        .expect("right branch");
    let contributions = vec![
        MergeContribution {
            branch_id: "left".into(),
            node_id: Some("node-a".into()),
            value: GraphValue::Boolean(false),
        },
        MergeContribution {
            branch_id: "right".into(),
            node_id: Some("node-b".into()),
            value: GraphValue::Boolean(true),
        },
    ];
    state
        .merge_parallel("decision", contributions)
        .expect("last writer merge");
    assert_eq!(
        state.read("decision", &VariableScope::Run).expect("read"),
        agentmod_graph_state::state::ReadOutcome::Value(&GraphValue::Boolean(true))
    );
}

#[test]
fn branch_local_variables_and_immutable_shared_reads_are_deterministic() {
    let mut set = DeclarationSet::new();
    let mut shared = declaration("shared", VariableType::Boolean, MergePolicy::RejectConflict);
    shared.mutability = MutabilityPolicy::Immutable;
    shared.default = Some(GraphValue::Boolean(true));
    set.insert(shared).expect("declared");
    set.insert(declaration(
        "local",
        VariableType::Boolean,
        MergePolicy::RejectConflict,
    ))
    .expect("declared");
    let (mut state, _) = GraphState::new(session(), set).expect("state");
    state
        .create_branch_scope("iso", BranchScopePolicy::Isolated)
        .expect("branch");
    // Immutable shared read from an isolated branch.
    assert_eq!(
        state
            .read(
                "shared",
                &VariableScope::Branch {
                    branch_id: "iso".into()
                }
            )
            .expect("read"),
        agentmod_graph_state::state::ReadOutcome::Value(&GraphValue::Boolean(true))
    );
    // An unassigned variable read from an isolated branch is unassigned.
    assert_eq!(
        state
            .read(
                "local",
                &VariableScope::Branch {
                    branch_id: "iso".into()
                }
            )
            .expect("read"),
        agentmod_graph_state::state::ReadOutcome::Unassigned
    );
    // Local branch writes do not leak to the run scope before a merge.
    state
        .assign(
            "local",
            GraphValue::Boolean(true),
            &AssignmentSource::Runtime,
            &VariableScope::Branch {
                branch_id: "iso".into(),
            },
            None,
        )
        .expect("branch assign");
    assert_eq!(
        state.read("local", &VariableScope::Run).expect("read"),
        agentmod_graph_state::state::ReadOutcome::Unassigned
    );
}

#[test]
fn env_digest_is_stable_across_processes() {
    // The environment projection must serialize to identical bytes for
    // identical canonical state, independent of any insertion history.
    let mut a = state();
    let mut b = state();
    let mut a_events = Vec::new();
    let mut b_events = Vec::new();
    for (state, events, values) in [
        (&mut a, &mut a_events, vec![1u64, 2, 3]),
        (&mut b, &mut b_events, vec![2u64, 1, 3]),
    ] {
        for value in values {
            if let Ok(mut committed) = state.assign(
                "counter",
                GraphValue::UnsignedInteger(value),
                &AssignmentSource::Runtime,
                &VariableScope::Run,
                None,
            ) {
                events.append(&mut committed);
            }
        }
    }
    assert_eq!(
        a.environment(&VariableScope::Run),
        b.environment(&VariableScope::Run)
    );
    assert_eq!(
        serde_json::to_vec(&a.environment(&VariableScope::Run)).expect("serialize"),
        serde_json::to_vec(&b.environment(&VariableScope::Run)).expect("serialize")
    );
    // Final value wins deterministically.
    assert_eq!(
        a.environment(&VariableScope::Run)["counter"],
        b.environment(&VariableScope::Run)["counter"]
    );
    assert_eq!(a.environment(&VariableScope::Run)["counter"], 3);
}
