//! Golden, acceptance, and property tests for session-style compilation.

use std::collections::{BTreeMap, BTreeSet};

use agentmod_graph_engine::GRAPH_FORMAT_VERSION;
use agentmod_primitives::ContentHash;
use agentmod_session_style_sdk::{
    BuiltInStyle, CompileContext, CompiledSessionStyle, DecisionCapability, GraphSource,
    StyleCompilerLimits, built_in_manifest, compile_style, compile_style_set, parse_json,
    parse_toml, to_json, to_toml,
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
