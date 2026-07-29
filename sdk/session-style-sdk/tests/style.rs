//! Golden, acceptance, and property tests for session-style compilation.

use std::collections::{BTreeMap, BTreeSet};

use agentmod_graph_engine::{GRAPH_FORMAT_VERSION, NodeKind};
use agentmod_primitives::ContentHash;
use agentmod_session_style_sdk::{
    BuiltInStyle, CompactionStrategy, CompileContext, CompiledSessionStyle, DecisionCapability,
    ExecutionBudgetOverrides, GraphSource, MemoryInjectionLocation, MemoryRetrievalTiming,
    MemoryWritePolicy, StyleCompilerLimits, built_in_manifest, built_in_manifest_for_version,
    compile_style, compile_style_set, declarative_graph_manifest, parse_json, parse_toml,
    select_compaction_strategy, select_execution_budgets, select_memory_provider, to_json, to_toml,
};
use proptest::prelude::*;

const GOLDEN_TOML: &str = include_str!("golden/custom-style.toml");
const GOLDEN_JSON: &str = include_str!("golden/custom-style.json");

fn context() -> CompileContext {
    CompileContext {
        runtime_api_version: "1.0.0".to_owned(),
        plugin_set_hash: ContentHash::digest(b"plugins"),
        capabilities: [
            "agents",
            "approval",
            "artifacts",
            "context",
            "continuations",
            "events",
            "fork",
            "model",
            "tools",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect(),
        tool_groups: BTreeMap::from([(
            "filesystem".to_owned(),
            BTreeSet::from(["filesystem.read".to_owned()]),
        )]),
        providers: BTreeSet::from(["mock".to_owned()]),
        plugins: BTreeSet::from(["runtime.security".to_owned()]),
        memory_providers: BTreeSet::from(["file".to_owned()]),
        compaction_strategies: [
            "artifact_handoff",
            "none",
            "sliding_window",
            "summary",
            "tool_output_eviction",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect(),
        supported_decisions: [
            DecisionCapability::Continue,
            DecisionCapability::Replace,
            DecisionCapability::Reject,
            DecisionCapability::RequireApproval,
            DecisionCapability::Defer,
            DecisionCapability::Cancel,
            DecisionCapability::Fork,
        ]
        .into_iter()
        .collect(),
        graph_references: BTreeMap::new(),
    }
}

fn codes(error: &agentmod_session_style_sdk::StyleCompileError) -> Vec<&'static str> {
    error
        .diagnostics()
        .iter()
        .map(|diagnostic| diagnostic.code)
        .collect()
}

#[test]
fn golden_toml_and_json_are_equivalent_and_round_trip() {
    let from_toml = parse_toml(GOLDEN_TOML).expect("TOML golden");
    let from_json = parse_json(GOLDEN_JSON).expect("JSON golden");
    assert_eq!(from_toml, from_json);
    let encoded_toml = to_toml(&from_toml).expect("serialize TOML");
    let encoded_json = to_json(&from_json).expect("serialize JSON");
    assert_eq!(
        parse_toml(&encoded_toml).expect("round-trip TOML"),
        from_toml
    );
    assert_eq!(
        parse_json(&encoded_json).expect("round-trip JSON"),
        from_json
    );
    assert_eq!(encoded_json.trim(), GOLDEN_JSON.trim());
    compile_style(&from_toml, &context(), StyleCompilerLimits::default())
        .expect("golden style compiles");
}

#[test]
fn component_overrides_normalize_disabled_profiles_and_recompile_through_the_sdk() {
    let mut manifest = built_in_manifest(BuiltInStyle::EphemeralTurn);
    select_memory_provider(&mut manifest, "sqlite-fts");
    select_compaction_strategy(&mut manifest, "sliding_window").expect("known strategy");
    let mut runtime = context();
    runtime.memory_providers.insert(String::from("sqlite-fts"));

    let compiled = compile_style(&manifest, &runtime, StyleCompilerLimits::default())
        .expect("normalized override");

    assert_eq!(compiled.memory.provider, "sqlite-fts");
    assert_eq!(
        compiled.memory.retrieval_timing,
        MemoryRetrievalTiming::TurnStart
    );
    assert_eq!(
        compiled.memory.injection_location,
        MemoryInjectionLocation::BeforeCurrentInput
    );
    assert_eq!(
        compiled.compaction.strategy,
        CompactionStrategy::SlidingWindow
    );
    assert!(compiled.compaction.trigger_tokens.is_some());

    select_memory_provider(&mut manifest, "none");
    select_compaction_strategy(&mut manifest, "none").expect("known strategy");
    let disabled = compile_style(&manifest, &runtime, StyleCompilerLimits::default())
        .expect("disabled override");
    assert_eq!(disabled.memory.provider, "none");
    assert_eq!(disabled.memory.max_items, 0);
    assert_eq!(disabled.compaction.strategy, CompactionStrategy::None);
    assert_eq!(disabled.compaction.trigger_tokens, None);
}

#[test]
fn budget_overrides_narrow_all_subordinate_bounds_and_recompile() {
    let mut manifest = built_in_manifest(BuiltInStyle::PlannerWorker);
    select_execution_budgets(
        &mut manifest,
        ExecutionBudgetOverrides {
            max_iterations: Some(2),
            max_steps: Some(40),
            max_tokens: Some(1_000),
            max_cost_micros: Some(1_000),
            max_duration_ms: Some(10_000),
        },
    )
    .expect("valid inline graph");

    let compiled = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect("narrowed budgets compile");

    assert_eq!(compiled.budgets.max_iterations, 2);
    assert_eq!(compiled.budgets.max_steps, 40);
    assert_eq!(compiled.budgets.max_tokens, 1_000);
    assert_eq!(compiled.budgets.max_cost_micros, 1_000);
    assert_eq!(compiled.budgets.max_duration_ms, 10_000);
    assert!(compiled.graph.budget.max_steps <= 40);
    assert!(compiled.graph.budget.max_tokens <= 1_000);
    assert!(compiled.graph.budget.max_cost_micros <= 1_000);
    assert!(compiled.graph.budget.max_duration_ms <= 10_000);
    assert!(compiled.child_agents.per_child_token_budget <= 1_000);
    assert!(
        compiled
            .child_agents
            .per_child_cost_budget_micros
            .is_some_and(|value| value <= 1_000)
    );
    assert!(compiled.retry.max_backoff_ms < 10_000);
}

#[test]
fn all_five_built_ins_compile_with_hard_limits_and_explicit_completion() {
    for semantic in [
        BuiltInStyle::PersistentChat,
        BuiltInStyle::EphemeralTurn,
        BuiltInStyle::ResearchLoop,
        BuiltInStyle::PlannerWorker,
        BuiltInStyle::DeclarativeGraph,
    ] {
        let manifest = built_in_manifest(semantic);
        let compiled = compile_style(&manifest, &context(), StyleCompilerLimits::default())
            .unwrap_or_else(|error| panic!("{semantic:?} failed: {error}"));
        assert!(compiled.budgets.max_iterations > 0);
        assert!(compiled.budgets.max_steps > 0);
        assert!(compiled.budgets.max_tokens > 0);
        assert!(compiled.budgets.max_cost_micros > 0);
        assert!(compiled.budgets.max_duration_ms > 0);
        assert!(compiled.termination.require_explicit_terminal_node);
        assert_eq!(
            compiled.memory.provider == "none",
            compiled.memory.retrieval_timing == MemoryRetrievalTiming::Never
        );
        assert_eq!(
            compiled.memory.provider == "none",
            compiled.memory.write_policy == MemoryWritePolicy::Never
        );
        assert_eq!(
            compiled.memory.provider == "none",
            compiled.memory.injection_location == MemoryInjectionLocation::None
        );
        assert_eq!(
            compiled.compaction.strategy == CompactionStrategy::None,
            compiled.compaction.trigger_tokens.is_none()
        );
        if compiled.compaction.strategy == CompactionStrategy::None {
            assert_eq!(compiled.compaction.reserved_context_tokens, 0);
            assert_eq!(compiled.compaction.max_provider_projection_tokens, 0);
        } else {
            assert!(compiled.compaction.reserved_context_tokens > 0);
            assert!(
                compiled.compaction.reserved_context_tokens
                    < compiled.compaction.max_provider_projection_tokens
            );
        }
        assert!(!compiled.compaction.preservation_requirements.is_empty());
        assert!(compiled.graph.nodes.iter().any(|node| matches!(
            node.kind,
            agentmod_graph_engine::NodeKind::CompleteTurn
                | agentmod_graph_engine::NodeKind::CompleteSession
                | agentmod_graph_engine::NodeKind::Fail
        )));
        assert!(
            compiled
                .inspect_json()
                .expect("inspection")
                .contains("cache_key")
        );
    }
}

#[test]
fn ephemeral_turn_1_1_is_an_exact_tool_capable_fresh_turn_graph() {
    let manifest = built_in_manifest(BuiltInStyle::EphemeralTurn);
    assert_eq!(manifest.identity.id, "ephemeral-turn");
    assert_eq!(manifest.identity.version, "1.1.0");
    assert_eq!(manifest.compaction.strategy, CompactionStrategy::None);
    assert!(manifest.compaction.trigger_tokens.is_none());
    assert!(
        manifest
            .required_capabilities
            .iter()
            .any(|capability| capability == "tools")
    );
    assert_eq!(manifest.allowed_tool_groups, ["filesystem"]);

    let compiled = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect("ephemeral-turn 1.1 compiles");
    let nodes = &compiled.graph.nodes;
    let node = |id: &str| {
        nodes
            .iter()
            .find(|node| node.id == id)
            .unwrap_or_else(|| panic!("missing node {id}"))
    };
    assert_eq!(node("fresh-context").kind, NodeKind::ContextTransform);
    assert_eq!(node("respond").kind, NodeKind::ModelCall);
    assert_eq!(node("tool").kind, NodeKind::ToolExecutionGate);
    assert_eq!(node("tool").tool.as_deref(), Some("filesystem.read"));
    assert_eq!(node("done").kind, NodeKind::CompleteTurn);
    assert_eq!(
        compiled
            .graph
            .nodes
            .iter()
            .filter(|node| matches!(
                node.kind,
                NodeKind::CompleteTurn | NodeKind::CompleteSession | NodeKind::Fail
            ))
            .map(|node| (node.id.as_str(), node.kind))
            .collect::<Vec<_>>(),
        [("done", NodeKind::CompleteTurn)]
    );
    assert_eq!(
        compiled.graph.declarations.capabilities,
        ["context", "model", "tools"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    );
    assert_eq!(
        compiled.graph.declarations.tools,
        ["filesystem.read"].into_iter().map(str::to_owned).collect()
    );

    let mut current = compiled.graph.entry_index;
    let mut traversal = vec![compiled.graph.nodes[current].id.as_str()];
    while let Some(edge) = compiled
        .graph
        .edges
        .iter()
        .find(|edge| edge.from == current)
    {
        assert_eq!(
            compiled
                .graph
                .edges
                .iter()
                .filter(|candidate| candidate.from == current)
                .count(),
            1,
            "ephemeral turn control flow must be linear"
        );
        current = edge.to;
        traversal.push(compiled.graph.nodes[current].id.as_str());
    }
    assert_eq!(traversal, ["fresh-context", "respond", "tool", "done"]);
    assert_eq!(compiled.graph.nodes[current].kind, NodeKind::CompleteTurn);
}

#[test]
fn ephemeral_turn_version_selection_and_cache_identity_are_exact_and_deterministic() {
    assert!(
        built_in_manifest_for_version(BuiltInStyle::EphemeralTurn, "1.0.0").is_none(),
        "persisted 1.0.0 selectors must not bind to the incompatible 1.1.0 descriptor"
    );
    let manifest = built_in_manifest_for_version(BuiltInStyle::EphemeralTurn, "1.1.0")
        .expect("exact current version");
    assert_eq!(manifest, built_in_manifest(BuiltInStyle::EphemeralTurn));

    for semantic in [BuiltInStyle::PersistentChat, BuiltInStyle::PlannerWorker] {
        assert!(built_in_manifest_for_version(semantic, "1.0.0").is_none());
        assert!(built_in_manifest_for_version(semantic, "1.1.0").is_some());
    }

    let first = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect("first deterministic compile");
    let second = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect("second deterministic compile");
    assert_eq!(first, second);
    assert_eq!(first.style_id, "ephemeral-turn");
    assert_eq!(first.style_version, "1.1.0");
    assert_eq!(
        first.cache_key.style_content_hash,
        second.cache_key.style_content_hash
    );
    assert_eq!(
        first.cache_key.combined_hash,
        second.cache_key.combined_hash
    );
    assert_eq!(
        first.cache_key.style_content_hash.to_hex(),
        "c28950785dcadaffd12861df0b02bac671f82a7a0d9f65a3cda9451def5feeda"
    );
    assert_eq!(
        first.cache_key.combined_hash.to_hex(),
        "941929db2f0536e7624467a0ade02c9ccd95ea2edc02bc4285f18a309d92fb93"
    );

    let json = to_json(&manifest).expect("ephemeral JSON");
    let toml = to_toml(&manifest).expect("ephemeral TOML");
    assert_eq!(parse_json(&json).expect("JSON round trip"), manifest);
    assert_eq!(parse_toml(&toml).expect("TOML round trip"), manifest);
}

#[test]
fn planner_worker_1_1_compiles_complete_child_execution_policy() {
    let manifest = built_in_manifest(BuiltInStyle::PlannerWorker);
    assert_eq!(manifest.identity.version, "1.1.0");
    let compiled = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect("planner worker compile");

    assert_eq!(
        compiled.child_agents.child_style.as_deref(),
        Some("ephemeral-turn@1.1.0")
    );
    assert!(compiled.child_agents.workspace_mode.is_some());
    assert_eq!(compiled.child_agents.inherit_provider, Some(true));
    assert_eq!(compiled.child_agents.inherit_model, Some(true));
    assert!(compiled.child_agents.context_budget_tokens.is_some());
    assert!(compiled.child_agents.per_child_cost_budget_micros.is_some());
    assert!(compiled.child_agents.memory_access.is_some());
    assert!(compiled.child_agents.join_behavior.is_some());
    assert!(compiled.child_agents.cancellation_behavior.is_some());
    assert_eq!(compiled.child_agents.reviewer_max_attempts, Some(8));
}

#[test]
fn research_loop_1_1_declares_bounded_context_and_capabilities() {
    let manifest = built_in_manifest(BuiltInStyle::ResearchLoop);
    assert_eq!(manifest.identity.id, "research-loop");
    assert_eq!(manifest.identity.version, "1.1.0");
    assert_eq!(manifest.budgets.max_iterations, 16);
    assert_eq!(manifest.compaction.strategy, CompactionStrategy::None);
    assert_eq!(manifest.child_agents.max_children, 0);
    assert_eq!(
        manifest.required_capabilities,
        ["approval", "artifacts", "context", "model", "tools"]
    );
    assert_eq!(manifest.allowed_tool_groups, ["filesystem"]);
    assert_eq!(
        manifest.memory.retrieval_timing,
        MemoryRetrievalTiming::IterationStart
    );
    assert_eq!(
        manifest.memory.injection_location,
        MemoryInjectionLocation::BeforeCurrentInput
    );
}

#[test]
fn research_loop_1_1_compiles_deterministic_iteration_control() {
    let manifest = built_in_manifest(BuiltInStyle::ResearchLoop);
    let compiled = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect("research-loop 1.1 compiles");
    let nodes = &compiled.graph.nodes;
    let node = |id: &str| {
        nodes
            .iter()
            .find(|node| node.id == id)
            .unwrap_or_else(|| panic!("missing node {id}"))
    };
    assert_eq!(node("fresh-context").kind, NodeKind::ContextTransform);
    assert_eq!(node("research").kind, NodeKind::ModelCall);
    assert_eq!(node("tool").kind, NodeKind::ToolExecutionGate);
    assert_eq!(node("tool").tool.as_deref(), Some("filesystem.read"));
    assert_eq!(node("persist").kind, NodeKind::PersistArtifact);
    assert_eq!(node("repeat").kind, NodeKind::Loop);
    assert_eq!(node("repeat").max_iterations, Some(16));
    assert_eq!(node("done").kind, NodeKind::CompleteSession);
    assert!(
        nodes.iter().all(|node| node.kind != NodeKind::Review),
        "the built-in must not invent a reviewer-enable control input"
    );
    assert_eq!(compiled.graph.budget.max_steps, 500);
    assert_eq!(compiled.graph.budget.max_tokens, 750_000);
    assert_eq!(compiled.graph.budget.max_cost_micros, 75_000_000);
    assert_eq!(compiled.graph.budget.max_duration_ms, 2_700_000);
    assert_eq!(
        compiled
            .graph
            .nodes
            .iter()
            .filter(|node| matches!(
                node.kind,
                NodeKind::CompleteTurn | NodeKind::CompleteSession | NodeKind::Fail
            ))
            .map(|node| (node.id.as_str(), node.kind))
            .collect::<Vec<_>>(),
        [("done", NodeKind::CompleteSession)]
    );

    let edge = |from: &str, variables: &serde_json::Value| {
        let from = node(from);
        let eligible = compiled
            .graph
            .edges
            .iter()
            .filter(|edge| edge.from == from.index)
            .filter(|edge| {
                edge.condition
                    .as_ref()
                    .is_none_or(|condition| condition.evaluate(variables).unwrap())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            eligible.len(),
            1,
            "expected exactly one transition from {from:?}"
        );
        (
            nodes[eligible[0].to].id.as_str(),
            eligible[0].label.as_deref(),
        )
    };
    assert_eq!(edge("fresh-context", &serde_json::json!({})).0, "research");
    assert_eq!(edge("research", &serde_json::json!({})).0, "tool");
    assert_eq!(edge("tool", &serde_json::json!({})).0, "persist");
    assert_eq!(edge("persist", &serde_json::json!({})).0, "repeat");
    assert_eq!(
        edge(
            "repeat",
            &serde_json::json!({"completion":{"criteria_met":false}})
        ),
        ("fresh-context", Some("continue"))
    );
    assert_eq!(
        edge(
            "repeat",
            &serde_json::json!({"completion":{"criteria_met":true}})
        ),
        ("done", Some("complete"))
    );
    assert!(
        compiled
            .graph
            .edges
            .iter()
            .filter(|edge| edge.from == node("repeat").index)
            .all(|edge| edge
                .condition
                .as_ref()
                .expect("completion condition")
                .evaluate(&serde_json::json!({}))
                .is_err()),
        "completion must be an explicit runtime-owned input"
    );
}

#[test]
fn research_loop_version_and_cache_identity_are_exact_and_deterministic() {
    assert!(
        built_in_manifest_for_version(BuiltInStyle::ResearchLoop, "1.0.0").is_none(),
        "persisted 1.0.0 selectors must not bind to research-loop 1.1.0"
    );
    let manifest = built_in_manifest_for_version(BuiltInStyle::ResearchLoop, "1.1.0")
        .expect("exact current research-loop version");
    assert_eq!(manifest, built_in_manifest(BuiltInStyle::ResearchLoop));

    let first = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect("first deterministic compile");
    let second = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect("second deterministic compile");
    assert_eq!(first, second);
    assert_eq!(first.style_version, "1.1.0");
    assert_eq!(
        first.cache_key.style_content_hash.to_hex(),
        "4df10d7ea07306da14dc6735af520d37f3fb8ccf6136ebeca99dd378817a1bf2"
    );
    assert_eq!(
        first.cache_key.combined_hash.to_hex(),
        "f7d30ae18e812512ccce77e379fec38b289dcd321d5e584c1ec50fa53f998276"
    );

    let json = to_json(&manifest).expect("research JSON");
    let toml = to_toml(&manifest).expect("research TOML");
    assert_eq!(parse_json(&json).expect("JSON round trip"), manifest);
    assert_eq!(parse_toml(&toml).expect("TOML round trip"), manifest);
}

#[test]
fn declarative_graph_1_1_declares_the_acceptance_fixture_capabilities() {
    let manifest = built_in_manifest(BuiltInStyle::DeclarativeGraph);
    assert_eq!(manifest.identity.id, "declarative-graph");
    assert_eq!(manifest.identity.version, "1.1.0");
    assert_eq!(manifest.budgets.max_iterations, 3);
    assert_eq!(manifest.required_capabilities, ["approval", "tools"]);
    assert_eq!(manifest.allowed_tool_groups, ["filesystem"]);
    assert_eq!(manifest.compaction.strategy, CompactionStrategy::None);
    assert_eq!(manifest.memory.provider, "none");
    assert_eq!(manifest.child_agents.max_children, 0);

    let GraphSource::Inline { source } = &manifest.graph else {
        panic!("built-in graph must be inline");
    };
    assert_eq!(
        declarative_graph_manifest(source),
        manifest,
        "the user-graph wrapper must retain the exact acceptance fixture"
    );
}

#[test]
fn declarative_graph_1_1_compiles_branch_approval_tool_loop_and_terminal_nodes() {
    let manifest = built_in_manifest(BuiltInStyle::DeclarativeGraph);
    let compiled = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect("declarative-graph 1.1 compiles");
    let nodes = &compiled.graph.nodes;
    let node = |id: &str| {
        nodes
            .iter()
            .find(|node| node.id == id)
            .unwrap_or_else(|| panic!("missing node {id}"))
    };
    assert_eq!(node("branch").kind, NodeKind::ConditionalBranch);
    assert_eq!(node("approval").kind, NodeKind::UserApproval);
    assert_eq!(node("tool").kind, NodeKind::ToolExecutionGate);
    assert_eq!(node("tool").tool.as_deref(), Some("filesystem.read"));
    assert_eq!(node("repeat").kind, NodeKind::Loop);
    assert_eq!(node("repeat").max_iterations, Some(3));
    assert_eq!(node("done").kind, NodeKind::CompleteSession);
    assert_eq!(compiled.graph.budget.max_steps, 64);
    assert_eq!(compiled.graph.budget.max_tokens, 1_000);
    assert_eq!(compiled.graph.budget.max_cost_micros, 1_000);
    assert_eq!(compiled.graph.budget.max_duration_ms, 10_000);

    let approval = node("approval");
    assert!(approval.tool.is_none());
    assert!(approval.provider.is_none());
    assert!(approval.read_scopes.is_empty());
    assert!(approval.write_scopes.is_empty());

    let edge = |from: &str, variables: &serde_json::Value| {
        let from = node(from);
        compiled
            .graph
            .edges
            .iter()
            .filter(|edge| edge.from == from.index)
            .filter(|edge| {
                edge.condition
                    .as_ref()
                    .is_none_or(|condition| condition.evaluate(variables).unwrap())
            })
            .map(|edge| (nodes[edge.to].id.as_str(), edge.label.as_deref()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        edge(
            "branch",
            &serde_json::json!({"request":{"requires_approval":true}})
        ),
        [("approval", Some("require-approval"))]
    );
    assert_eq!(
        edge(
            "branch",
            &serde_json::json!({"request":{"requires_approval":false}})
        ),
        [("tool", Some("skip-approval"))]
    );
    assert_eq!(edge("approval", &serde_json::json!({})), [("tool", None)]);
    assert_eq!(edge("tool", &serde_json::json!({})), [("repeat", None)]);
    assert_eq!(
        edge(
            "repeat",
            &serde_json::json!({"iteration":{"remaining":true}})
        ),
        [("tool", Some("continue"))]
    );
    assert_eq!(
        edge(
            "repeat",
            &serde_json::json!({"iteration":{"remaining":false}})
        ),
        [("done", Some("complete"))]
    );
    for control in ["branch", "repeat"] {
        assert!(
            compiled
                .graph
                .edges
                .iter()
                .filter(|edge| edge.from == node(control).index)
                .all(|edge| edge
                    .condition
                    .as_ref()
                    .expect("control edge condition")
                    .evaluate(&serde_json::json!({}))
                    .is_err()),
            "{control} must fail closed when its runtime control input is absent"
        );
    }
    assert!(
        compiled
            .graph
            .edges
            .iter()
            .all(|edge| edge.from != node("done").index)
    );
}

#[test]
fn declarative_graph_version_and_cache_identity_are_exact_and_deterministic() {
    assert!(
        built_in_manifest_for_version(BuiltInStyle::DeclarativeGraph, "1.0.0").is_none(),
        "persisted 1.0.0 selectors must not bind to declarative-graph 1.1.0"
    );
    let manifest = built_in_manifest_for_version(BuiltInStyle::DeclarativeGraph, "1.1.0")
        .expect("exact current declarative-graph version");
    assert_eq!(manifest, built_in_manifest(BuiltInStyle::DeclarativeGraph));

    let first = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect("first deterministic compile");
    let second = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect("second deterministic compile");
    assert_eq!(first, second);
    assert_eq!(first.style_version, "1.1.0");
    assert_eq!(
        first.cache_key.style_content_hash.to_hex(),
        "4771b38f94121592175bff6ff9766f725e6a72efd493bd43e0ee3168ea4034b7"
    );
    assert_eq!(
        first.cache_key.combined_hash.to_hex(),
        "579af94d1a645cbeccabf59b174232fa874deeef94c6a4311e9e087202a266cf"
    );

    let json = to_json(&manifest).expect("declarative JSON");
    let toml = to_toml(&manifest).expect("declarative TOML");
    assert_eq!(parse_json(&json).expect("JSON round trip"), manifest);
    assert_eq!(parse_toml(&toml).expect("TOML round trip"), manifest);
}

#[test]
fn schema_v1_manifests_without_context_controls_receive_safe_explicit_defaults() {
    let manifest = built_in_manifest(BuiltInStyle::DeclarativeGraph);
    let mut legacy = serde_json::to_value(manifest).expect("manifest JSON");
    let memory = legacy["memory"].as_object_mut().expect("memory object");
    for field in [
        "retrieval_timing",
        "query",
        "write_policy",
        "injection_location",
    ] {
        memory.remove(field);
    }
    let compaction = legacy["compaction"]
        .as_object_mut()
        .expect("compaction object");
    for field in [
        "reserved_context_tokens",
        "max_provider_projection_tokens",
        "preservation_requirements",
    ] {
        compaction.remove(field);
    }

    let parsed = parse_json(&serde_json::to_string(&legacy).expect("legacy JSON"))
        .expect("schema-v1 manifest remains compatible");
    assert_eq!(parsed.memory.retrieval_timing, MemoryRetrievalTiming::Never);
    assert_eq!(parsed.memory.write_policy, MemoryWritePolicy::Never);
    assert_eq!(
        parsed.memory.injection_location,
        MemoryInjectionLocation::None
    );
    assert_eq!(parsed.compaction.reserved_context_tokens, 0);
    assert_eq!(parsed.compaction.max_provider_projection_tokens, 0);
    assert!(!parsed.compaction.preservation_requirements.is_empty());

    let canonical = to_json(&parsed).expect("canonical JSON");
    for field in [
        "\"retrieval_timing\"",
        "\"query\"",
        "\"write_policy\"",
        "\"injection_location\"",
        "\"reserved_context_tokens\"",
        "\"max_provider_projection_tokens\"",
        "\"preservation_requirements\"",
    ] {
        assert!(canonical.contains(field), "missing canonical field {field}");
    }
    compile_style(&parsed, &context(), StyleCompilerLimits::default())
        .expect("defaulted schema-v1 manifest compiles");
}

#[test]
fn inconsistent_memory_and_compaction_controls_are_rejected() {
    let mut manifest = built_in_manifest(BuiltInStyle::PersistentChat);
    manifest.memory.provider = "none".to_owned();
    manifest.memory.scopes.clear();
    manifest.memory.max_items = 0;
    manifest.memory.max_injected_bytes = 0;
    manifest.compaction.strategy = CompactionStrategy::None;
    manifest.compaction.trigger_tokens = None;

    let error = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect_err("disabled selections must not retain live controls");
    assert!(codes(&error).contains(&"STYLE030"), "{error}");
    assert!(codes(&error).contains(&"STYLE031"), "{error}");

    let mut manifest = built_in_manifest(BuiltInStyle::PersistentChat);
    manifest.memory.query.max_query_bytes = 0;
    manifest.compaction.reserved_context_tokens =
        manifest.compaction.max_provider_projection_tokens;
    let error = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect_err("active controls must be bounded");
    assert!(codes(&error).contains(&"STYLE030"), "{error}");
    assert!(codes(&error).contains(&"STYLE031"), "{error}");
}

#[test]
fn compiled_built_in_style_json_round_trips_and_rejects_invalid_cache_data() {
    let manifest = built_in_manifest(BuiltInStyle::PersistentChat);
    let compiled = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect("built-in style compiles");
    let encoded = serde_json::to_string(&compiled).expect("compiled style serializes");
    let reloaded: CompiledSessionStyle =
        serde_json::from_str(&encoded).expect("compiled style cache reloads");

    assert_eq!(reloaded, compiled);
    assert_eq!(
        serde_json::to_string(&reloaded).expect("reloaded style serializes"),
        encoded
    );

    let cached: serde_json::Value = serde_json::from_str(&encoded).expect("compiled cache is JSON");
    let mut unsupported_version = cached.clone();
    unsupported_version["graph"]["format_version"] =
        serde_json::Value::from(GRAPH_FORMAT_VERSION + 1);
    assert!(
        serde_json::from_value::<CompiledSessionStyle>(unsupported_version).is_err(),
        "compiled cache data with an unsupported graph version must be rejected"
    );

    let mut invalid = cached;
    invalid["graph"]["unexpected_cache_field"] = serde_json::Value::Null;
    assert!(
        serde_json::from_value::<CompiledSessionStyle>(invalid).is_err(),
        "compiled cache data with unknown graph fields must be rejected"
    );
}

#[test]
fn missing_runtime_inputs_and_unsupported_decisions_are_rejected() {
    let mut manifest = built_in_manifest(BuiltInStyle::PersistentChat);
    manifest
        .required_capabilities
        .push("missing.capability".to_owned());
    manifest.allowed_tool_groups.push("process".to_owned());
    manifest.allowed_providers.push("absent".to_owned());
    manifest.allowed_plugins.push("absent.plugin".to_owned());
    manifest.interceptors[0]
        .supported_decisions
        .push(DecisionCapability::Fork);
    let mut compile_context = context();
    compile_context
        .supported_decisions
        .remove(&DecisionCapability::Fork);

    let error = compile_style(&manifest, &compile_context, StyleCompilerLimits::default())
        .expect_err("availability failures");
    let actual = codes(&error);
    for expected in ["STYLE010", "STYLE011", "STYLE012", "STYLE013", "STYLE015"] {
        assert!(actual.contains(&expected), "missing {expected}: {error}");
    }
}

#[test]
fn interceptor_cycle_is_rejected_by_event_pipeline() {
    let mut manifest = built_in_manifest(BuiltInStyle::PersistentChat);
    let mut second = manifest.interceptors[0].clone();
    second.id = "project-policy".to_owned();
    manifest.interceptors[0].before = vec![second.id.clone()];
    second.before = vec![manifest.interceptors[0].id.clone()];
    manifest.interceptors.push(second);

    let error = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect_err("cycle must fail");
    assert!(codes(&error).contains(&"STYLE016"));
    assert!(error.to_string().contains("cycle"));
}

#[test]
fn graph_engine_rejects_unbounded_loop_and_parallel_write_conflict() {
    let invalid_loop = r#"
format_version = 1
entry = "loop"
[budget]
max_steps = 10
max_tokens = 100
max_cost_micros = 100
max_duration_ms = 1000
[[nodes]]
id = "loop"
kind = "loop"
[[nodes]]
id = "done"
kind = "complete_session"
[[edges]]
from = "loop"
to = "done"
"#;
    let mut manifest = built_in_manifest(BuiltInStyle::DeclarativeGraph);
    manifest.graph = GraphSource::Inline {
        source: invalid_loop.to_owned(),
    };
    let error = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect_err("loop must be bounded");
    assert!(codes(&error).contains(&"STYLE025"));

    let parallel = r#"
format_version = 1
entry = "parallel"
[budget]
max_steps = 20
max_tokens = 100
max_cost_micros = 100
max_duration_ms = 1000
[declarations]
capabilities = ["agents", "events"]
[[nodes]]
id = "parallel"
kind = "parallel_branch"
[[nodes]]
id = "left"
kind = "emit_event"
write_scopes = ["session"]
[[nodes]]
id = "right"
kind = "emit_event"
write_scopes = ["session"]
[[nodes]]
id = "join"
kind = "join_results"
[[nodes]]
id = "done"
kind = "complete_session"
[[edges]]
from = "parallel"
to = "left"
[[edges]]
from = "parallel"
to = "right"
[[edges]]
from = "left"
to = "join"
[[edges]]
from = "right"
to = "join"
[[edges]]
from = "join"
to = "done"
"#;
    manifest.graph = GraphSource::Inline {
        source: parallel.to_owned(),
    };
    manifest.required_capabilities.push("agents".to_owned());
    let error = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect_err("parallel writes must conflict");
    assert!(codes(&error).contains(&"STYLE025"));
    assert!(error.to_string().contains("both write"));
}

#[test]
fn style_cache_key_binds_every_required_input() {
    let manifest = built_in_manifest(BuiltInStyle::PersistentChat);
    let baseline = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect("baseline")
        .cache_key;

    let mut changed_manifest = manifest.clone();
    changed_manifest.budgets.max_steps += 1;
    assert_ne!(
        baseline.combined_hash,
        compile_style(
            &changed_manifest,
            &context(),
            StyleCompilerLimits::default()
        )
        .expect("changed style")
        .cache_key
        .combined_hash
    );

    let mut changed_manifest = manifest.clone();
    changed_manifest.memory.query.include_style_context =
        !changed_manifest.memory.query.include_style_context;
    assert_ne!(
        baseline.combined_hash,
        compile_style(
            &changed_manifest,
            &context(),
            StyleCompilerLimits::default()
        )
        .expect("changed memory query construction")
        .cache_key
        .combined_hash
    );

    let mut changed = context();
    changed.plugin_set_hash = ContentHash::digest(b"other plugins");
    assert_ne!(
        baseline.combined_hash,
        compile_style(&manifest, &changed, StyleCompilerLimits::default())
            .expect("changed plugins")
            .cache_key
            .combined_hash
    );
    let mut changed = context();
    changed.runtime_api_version = "1.1.0".to_owned();
    assert_ne!(
        baseline.combined_hash,
        compile_style(&manifest, &changed, StyleCompilerLimits::default())
            .expect("changed runtime")
            .cache_key
            .combined_hash
    );
    let mut changed = context();
    changed.capabilities.insert("extra".to_owned());
    assert_ne!(
        baseline.combined_hash,
        compile_style(&manifest, &changed, StyleCompilerLimits::default())
            .expect("changed capabilities")
            .cache_key
            .combined_hash
    );
}

#[test]
fn top_level_model_selection_and_unbounded_retries_are_rejected() {
    let mut manifest = built_in_manifest(BuiltInStyle::PersistentChat);
    manifest.selection.model_may_select = true;
    manifest.retry.max_attempts = u32::MAX;
    let error = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect_err("unsafe policy");
    assert!(codes(&error).contains(&"STYLE022"));
    assert!(codes(&error).contains(&"STYLE024"));
}

#[test]
fn catalog_style_ids_are_unique_and_graph_references_are_content_addressed() {
    let manifest = built_in_manifest(BuiltInStyle::PersistentChat);
    let error = compile_style_set(
        &[manifest.clone(), manifest],
        &context(),
        StyleCompilerLimits::default(),
    )
    .expect_err("duplicate style ID");
    assert!(codes(&error).contains(&"STYLE008"));

    let mut referenced = parse_toml(GOLDEN_TOML).expect("golden");
    let GraphSource::Inline { source } = &referenced.graph else {
        panic!("inline source");
    };
    let source = source.clone();
    let content_hash = ContentHash::digest(source.as_bytes()).to_hex();
    referenced.graph = GraphSource::Reference {
        id: "project.style-graph".to_owned(),
        content_hash,
    };
    let mut compile_context = context();
    compile_context
        .graph_references
        .insert("project.style-graph".to_owned(), source);
    compile_style(
        &referenced,
        &compile_context,
        StyleCompilerLimits::default(),
    )
    .expect("matching content-addressed reference");

    let GraphSource::Reference { content_hash, .. } = &mut referenced.graph else {
        panic!("reference");
    };
    *content_hash = content_hash.to_uppercase();
    let error = compile_style(
        &referenced,
        &compile_context,
        StyleCompilerLimits::default(),
    )
    .expect_err("noncanonical hash");
    assert!(codes(&error).contains(&"STYLE027"));
}

#[test]
fn harness_selection_is_compiled_and_invalid_identifiers_fail_closed() {
    let mut manifest = built_in_manifest(BuiltInStyle::PersistentChat);
    manifest.harness.id = String::from("fixture");
    manifest.harness.required_capabilities = vec![
        String::from("streaming"),
        String::from("structured_context_replacement"),
    ];
    let compiled = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect("harness selection compiles");
    assert_eq!(compiled.harness, manifest.harness);

    manifest.harness.id = String::from("../fixture");
    manifest.harness.required_capabilities = vec![String::from("not valid")];
    let error = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect_err("unsafe harness identifiers");
    assert!(codes(&error).contains(&"STYLE030"));
    assert!(codes(&error).contains(&"STYLE031"));
}

proptest! {
    #[test]
    fn parsers_never_panic_on_arbitrary_bounded_text(input in ".{0,4096}") {
        let _ = parse_toml(&input);
        let _ = parse_json(&input);
    }

    #[test]
    fn iteration_budget_bound_is_exact(iterations in any::<u32>()) {
        let mut manifest = built_in_manifest(BuiltInStyle::PersistentChat);
        manifest.budgets.max_iterations = iterations;
        let result = compile_style(&manifest, &context(), StyleCompilerLimits::default());
        let has_budget_error = result
            .as_ref()
            .err()
            .is_some_and(|error| codes(error).contains(&"STYLE020"));
        prop_assert_eq!(has_budget_error, iterations == 0 || iterations > 10_000);
    }
}
