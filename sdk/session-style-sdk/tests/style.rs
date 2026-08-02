//! Golden, acceptance, and property tests for session-style compilation.

use std::collections::{BTreeMap, BTreeSet};

use agentmod_graph_engine::{
    ArtifactContentSource, ArtifactRetentionPolicy, ArtifactSecurityClassification, ChildSetSource,
    ChildWaitCancellation, ChildWorkspaceConfiguration, CompleteTurnCleanup,
    ContextTransformStrategy, GRAPH_FORMAT_VERSION, JoinArtifactCollection, JoinFailurePolicy,
    JoinOrderingPolicy, JoinResultProjection, NodeConfiguration, NodeKind, NodeValueSource,
    ParallelJoinPolicy, ReviewResultSchema, ReviewRoutes, SecurityClassification,
    VariableMergePolicy, VariableMutability, VariableScope, VariableValueType,
};
use agentmod_primitives::ContentHash;
use agentmod_session_style_sdk::{
    AvailableContextTransform, AvailablePluginCompactor, AvailablePluginMemoryProvider,
    BuiltInStyle, ChildMemoryAccess, CompactionStrategy, CompileContext, CompiledSessionStyle,
    ContextTransformLifecycle, ContextTransformSelection, DecisionCapability,
    ExecutionBudgetOverrides, GraphSource, MemoryInjectionLocation, MemoryRetrievalTiming,
    MemoryWritePolicy, PluginCompactorSelection, PluginMemorySelection, StyleCompilerLimits,
    built_in_manifest, built_in_manifest_for_version, built_in_versions, compile_style,
    compile_style_set, declarative_graph_manifest, parse_json, parse_toml,
    select_child_session_restrictions, select_compaction_strategy, select_execution_budgets,
    select_memory_provider, to_json, to_toml,
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
        context_transforms: Vec::new(),
        plugin_memory_providers: Vec::new(),
        plugin_compactors: Vec::new(),
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

fn context_transform_selection(
    plugin_id: &str,
    transform_id: &str,
    version: &str,
    declaration_hash: ContentHash,
    configuration: &[u8],
) -> ContextTransformSelection {
    ContextTransformSelection {
        plugin_id: plugin_id.to_owned(),
        transform_id: transform_id.to_owned(),
        version: version.to_owned(),
        declaration_hash,
        lifecycle: ContextTransformLifecycle::BeforeModelRequest,
        configuration_reference: ContentHash::digest(configuration),
    }
}

fn plugin_memory_selection(declaration_hash: ContentHash) -> PluginMemorySelection {
    PluginMemorySelection {
        plugin_id: String::from("fixture.context"),
        plugin_version: String::from("2.3.4"),
        provider_id: String::from("fixture.memory"),
        provider_version: String::from("1.4.0"),
        declaration_hash,
        configuration_reference: ContentHash::digest(b"memory configuration"),
    }
}

fn plugin_compactor_selection(declaration_hash: ContentHash) -> PluginCompactorSelection {
    PluginCompactorSelection {
        plugin_id: String::from("fixture.context"),
        plugin_version: String::from("2.3.4"),
        compactor_id: String::from("fixture.compactor"),
        compactor_version: String::from("3.1.0"),
        declaration_hash,
        configuration_reference: ContentHash::digest(b"compactor configuration"),
    }
}

fn plugin_context_style() -> (
    agentmod_session_style_sdk::SessionStyleManifest,
    CompileContext,
) {
    let memory_hash = ContentHash::digest(b"exact memory declaration");
    let compactor_hash = ContentHash::digest(b"exact compactor declaration");
    let mut manifest = built_in_manifest(BuiltInStyle::PersistentChat);
    manifest
        .allowed_plugins
        .push(String::from("fixture.context"));
    manifest.memory.provider = String::from("fixture.memory");
    manifest.memory.plugin = Some(plugin_memory_selection(memory_hash));
    manifest.compaction.strategy = CompactionStrategy::Plugin;
    manifest.compaction.plugin = Some(plugin_compactor_selection(compactor_hash));

    let mut runtime = current_builtin_context();
    runtime.plugins.insert(String::from("fixture.context"));
    runtime.plugin_memory_providers = vec![AvailablePluginMemoryProvider {
        plugin_id: String::from("fixture.context"),
        plugin_version: String::from("2.3.4"),
        provider_id: String::from("fixture.memory"),
        provider_version: String::from("1.4.0"),
        declaration_hash: memory_hash,
        configuration_reference: ContentHash::digest(b"memory configuration"),
        has_retrieve: true,
        has_write: true,
    }];
    runtime.plugin_compactors = vec![AvailablePluginCompactor {
        plugin_id: String::from("fixture.context"),
        plugin_version: String::from("2.3.4"),
        compactor_id: String::from("fixture.compactor"),
        compactor_version: String::from("3.1.0"),
        declaration_hash: compactor_hash,
        configuration_reference: ContentHash::digest(b"compactor configuration"),
    }];
    (manifest, runtime)
}

#[allow(
    clippy::too_many_lines,
    reason = "the test oracle spells out the complete runtime-owned native tool catalog"
)]
fn current_builtin_context() -> CompileContext {
    let mut runtime = context();
    runtime.providers.insert(String::from("deterministic-mock"));
    runtime.tool_groups = BTreeMap::from([
        (
            String::from("browser"),
            [
                "browser.start",
                "browser.navigate",
                "browser.inspect",
                "browser.screenshot",
                "browser.click",
                "browser.type",
                "browser.submit",
                "browser.download",
                "browser.close",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        ),
        (
            String::from("filesystem"),
            [
                "filesystem.read",
                "filesystem.list",
                "filesystem.glob",
                "filesystem.grep",
                "filesystem.write",
                "filesystem.edit",
                "filesystem.apply_patch",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        ),
        (
            String::from("git"),
            [
                "git.discover",
                "git.status",
                "git.diff",
                "git.changed_files",
                "git.branch",
                "git.dirty",
                "git.worktree_create",
                "git.worktree_cleanup",
                "git.checkpoint_create",
                "git.checkpoint_restore",
                "git.export_patch",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        ),
        (
            String::from("lsp"),
            [
                "lsp.project_root",
                "lsp.diagnostics",
                "lsp.document_symbols",
                "lsp.workspace_symbols",
                "lsp.definition",
                "lsp.references",
                "lsp.hover",
                "lsp.signature_help",
                "lsp.rename",
                "lsp.formatting",
                "lsp.code_actions",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        ),
        (
            String::from("mcp"),
            ["mcp.server.list", "mcp.capabilities", "mcp.invoke"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
        ),
        (
            String::from("process"),
            [
                "process.run",
                "process.start",
                "process.run_pty",
                "process.start_pty",
                "process.read",
                "process.input",
                "process.resize",
                "process.wait",
                "process.interrupt",
                "process.kill",
                "process.detach",
                "process.reattach",
                "process.list",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        ),
        (
            String::from("web"),
            ["http.request", "web.fetch", "web.search"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
        ),
    ]);
    runtime
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
    let mut runtime = current_builtin_context();
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
    let mut manifest = built_in_manifest_for_version(BuiltInStyle::PlannerWorker, "1.1.0")
        .expect("frozen planner-worker 1.1");
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
fn child_provider_inheritance_rewrites_manifest_graph_and_recompiles_exactly() {
    let mut manifest = built_in_manifest(BuiltInStyle::EphemeralTurn);
    select_child_session_restrictions(
        &mut manifest,
        &BTreeSet::from([String::from("filesystem")]),
        ChildMemoryAccess::None,
        ExecutionBudgetOverrides::default(),
        Some("inherited-mock"),
    )
    .expect("child restrictions");
    let unavailable = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect_err("unavailable inherited provider");
    assert!(codes(&unavailable).contains(&"STYLE012"));
    let mut runtime = context();
    runtime.providers.insert(String::from("inherited-mock"));
    runtime.memory_providers.insert(String::from("none"));
    let compiled = compile_style(&manifest, &runtime, StyleCompilerLimits::default())
        .expect("inherited provider compiles");

    assert_eq!(compiled.allowed_providers, ["inherited-mock"]);
    assert_eq!(
        compiled.graph.declarations.providers,
        BTreeSet::from([String::from("inherited-mock")])
    );
    assert!(
        compiled
            .graph
            .nodes
            .iter()
            .filter(|node| matches!(node.kind, NodeKind::ModelCall | NodeKind::Review))
            .all(|node| node.provider.as_deref() == Some("inherited-mock"))
    );

    let error = select_child_session_restrictions(
        &mut manifest,
        &BTreeSet::from([String::from("filesystem")]),
        ChildMemoryAccess::None,
        ExecutionBudgetOverrides::default(),
        Some("Invalid Provider"),
    )
    .expect_err("invalid inherited provider");
    assert!(error.to_string().contains("is invalid"));
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
        let runtime = if matches!(
            semantic,
            BuiltInStyle::PersistentChat
                | BuiltInStyle::EphemeralTurn
                | BuiltInStyle::ResearchLoop
                | BuiltInStyle::PlannerWorker
        ) {
            current_builtin_context()
        } else {
            context()
        };
        let compiled = compile_style(&manifest, &runtime, StyleCompilerLimits::default())
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
    let manifest = built_in_manifest_for_version(BuiltInStyle::EphemeralTurn, "1.1.0")
        .expect("retained ephemeral-turn 1.1");
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
#[allow(
    clippy::too_many_lines,
    reason = "one acceptance test audits the complete immutable generic turn contract as a single graph"
)]
fn ephemeral_turn_1_2_binds_typed_generic_provider_tool_and_cleanup_contracts() {
    let manifest = built_in_manifest(BuiltInStyle::EphemeralTurn);
    assert_eq!(manifest.identity.version, "1.2.0");
    let compiled = compile_style(
        &manifest,
        &current_builtin_context(),
        StyleCompilerLimits::default(),
    )
    .expect("ephemeral-turn 1.2 compiles");
    let node = |id: &str| {
        compiled
            .graph
            .nodes
            .iter()
            .find(|node| node.id == id)
            .unwrap_or_else(|| panic!("missing node {id}"))
    };

    assert_eq!(compiled.graph.variables.len(), 3);
    let disposition = compiled
        .graph
        .variables
        .iter()
        .find(|variable| variable.name == "model_disposition")
        .expect("model disposition");
    assert_eq!(disposition.scope, VariableScope::Run);
    assert_eq!(disposition.producer, "respond");
    assert_eq!(disposition.consumers, BTreeSet::from(["tool-batch".into()]));
    assert_eq!(disposition.mutability, VariableMutability::Mutable);
    assert_eq!(
        disposition.value_type,
        VariableValueType::Enum {
            values: BTreeSet::from(["response_complete".into(), "tool_requests".into()])
        }
    );
    for (name, producer, consumer) in [
        ("model_result", "respond", "tool-batch"),
        ("turn_result", "tool-batch", "done"),
    ] {
        let variable = compiled
            .graph
            .variables
            .iter()
            .find(|variable| variable.name == name)
            .unwrap_or_else(|| panic!("missing variable {name}"));
        assert_eq!(variable.value_type, VariableValueType::NodeResultReference);
        assert_eq!(variable.scope, VariableScope::Run);
        assert_eq!(variable.producer, producer);
        assert_eq!(variable.consumers, BTreeSet::from([consumer.into()]));
        assert_eq!(variable.mutability, VariableMutability::Mutable);
    }

    assert_eq!(
        node("respond").configuration,
        Some(NodeConfiguration::ModelRequest {
            disposition_output: "model_disposition".into(),
            result_output: "model_result".into(),
            provider_options: BTreeMap::new(),
            json_outputs: BTreeMap::new(),
            inputs: BTreeMap::new(),
        })
    );
    assert_eq!(
        node("respond").write_variables,
        BTreeSet::from(["model_disposition".into(), "model_result".into()])
    );
    assert_eq!(node("tool-batch").tool, None);
    assert_eq!(
        node("tool-batch").configuration,
        Some(NodeConfiguration::ProviderToolBatchExecution {
            request_reference_variable: "model_result".into(),
            disposition_variable: "model_disposition".into(),
            maximum_calls: 32,
            allowed_tools: BTreeSet::from(["filesystem.read".into()]),
        })
    );
    assert_eq!(
        node("tool-batch").read_variables,
        BTreeSet::from(["model_disposition".into(), "model_result".into()])
    );
    assert_eq!(
        node("tool-batch").write_variables,
        BTreeSet::from(["turn_result".into()])
    );
    assert_eq!(
        node("done").configuration,
        Some(NodeConfiguration::CompleteTurn {
            result_reference_variable: "turn_result".into(),
            cleanup: CompleteTurnCleanup::DiscardProjection,
        })
    );
    assert_eq!(
        node("done").read_variables,
        BTreeSet::from(["turn_result".into()])
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
            "provider batch lifecycle must have one deterministic transition"
        );
        current = edge.to;
        traversal.push(compiled.graph.nodes[current].id.as_str());
    }
    assert_eq!(
        traversal,
        ["fresh-context", "respond", "tool-batch", "done"]
    );
}

#[test]
fn ephemeral_turn_version_selection_and_cache_identity_are_exact_and_deterministic() {
    assert_eq!(
        built_in_versions(BuiltInStyle::EphemeralTurn),
        ["1.1.0", "1.2.0"]
    );
    assert_eq!(
        built_in_versions(BuiltInStyle::PersistentChat),
        ["1.1.0", "1.2.0"]
    );
    assert!(
        built_in_manifest_for_version(BuiltInStyle::EphemeralTurn, "1.0.0").is_none(),
        "persisted 1.0.0 selectors must not bind to the incompatible 1.1.0 descriptor"
    );
    let legacy = built_in_manifest_for_version(BuiltInStyle::EphemeralTurn, "1.1.0")
        .expect("exact retained version");
    let manifest = built_in_manifest_for_version(BuiltInStyle::EphemeralTurn, "1.2.0")
        .expect("exact current version");
    assert_ne!(legacy, manifest);
    assert_eq!(manifest, built_in_manifest(BuiltInStyle::EphemeralTurn));

    for semantic in [BuiltInStyle::PersistentChat, BuiltInStyle::PlannerWorker] {
        assert!(built_in_manifest_for_version(semantic, "1.0.0").is_none());
        assert!(built_in_manifest_for_version(semantic, "1.1.0").is_some());
    }
    assert_eq!(
        built_in_manifest_for_version(BuiltInStyle::PersistentChat, "1.1.0")
            .expect("persistent-chat 1.1")
            .child_agents
            .child_style
            .as_deref(),
        Some("ephemeral-turn@1.1.0"),
        "persistent-chat 1.1 must not silently adopt the new ephemeral executor contract"
    );

    let legacy_first = compile_style(&legacy, &context(), StyleCompilerLimits::default())
        .expect("first deterministic legacy compile");
    let legacy_second = compile_style(&legacy, &context(), StyleCompilerLimits::default())
        .expect("second deterministic legacy compile");
    assert_eq!(legacy_first, legacy_second);
    assert_eq!(legacy_first.style_version, "1.1.0");
    assert_eq!(
        legacy_first.cache_key.style_content_hash.to_hex(),
        "c28950785dcadaffd12861df0b02bac671f82a7a0d9f65a3cda9451def5feeda"
    );
    assert_eq!(
        legacy_first.cache_key.combined_hash.to_hex(),
        "941929db2f0536e7624467a0ade02c9ccd95ea2edc02bc4285f18a309d92fb93"
    );

    let current_context = current_builtin_context();
    let first = compile_style(&manifest, &current_context, StyleCompilerLimits::default())
        .expect("first deterministic compile");
    let second = compile_style(&manifest, &current_context, StyleCompilerLimits::default())
        .expect("second deterministic compile");
    assert_eq!(first, second);
    assert_eq!(first.style_id, "ephemeral-turn");
    assert_eq!(first.style_version, "1.2.0");
    assert_eq!(
        first.cache_key.style_content_hash,
        second.cache_key.style_content_hash
    );
    assert_eq!(
        first.cache_key.combined_hash,
        second.cache_key.combined_hash
    );
    assert_ne!(
        first.cache_key.style_content_hash,
        legacy_first.cache_key.style_content_hash
    );
    assert_ne!(
        first.cache_key.combined_hash,
        legacy_first.cache_key.combined_hash
    );

    let json = to_json(&manifest).expect("ephemeral JSON");
    let toml = to_toml(&manifest).expect("ephemeral TOML");
    assert_eq!(parse_json(&json).expect("JSON round trip"), manifest);
    assert_eq!(parse_toml(&toml).expect("TOML round trip"), manifest);
    let legacy_json = to_json(&legacy).expect("legacy ephemeral JSON");
    let legacy_toml = to_toml(&legacy).expect("legacy ephemeral TOML");
    assert_eq!(parse_json(&legacy_json).expect("legacy JSON"), legacy);
    assert_eq!(parse_toml(&legacy_toml).expect("legacy TOML"), legacy);
}

#[test]
fn persistent_chat_1_2_binds_generic_provider_tools_and_preserved_projection() {
    let legacy = built_in_manifest_for_version(BuiltInStyle::PersistentChat, "1.1.0")
        .expect("frozen persistent 1.1");
    assert_eq!(legacy.identity.version, "1.1.0");
    assert_eq!(legacy.allowed_providers, ["mock"]);
    assert_eq!(legacy.allowed_tool_groups, ["filesystem"]);
    assert!(
        !legacy
            .required_capabilities
            .contains(&String::from("context"))
    );
    assert_eq!(
        legacy.child_agents.child_style.as_deref(),
        Some("ephemeral-turn@1.1.0")
    );
    let legacy_compiled = compile_style(&legacy, &context(), StyleCompilerLimits::default())
        .expect("legacy persistent compile");
    assert_eq!(legacy_compiled.graph.nodes.len(), 3);
    assert!(matches!(
        legacy_compiled
            .graph
            .nodes
            .iter()
            .find(|node| node.id == "tool")
            .and_then(|node| node.configuration.as_ref()),
        Some(NodeConfiguration::ToolExecution {
            arguments: NodeValueSource::Static { value }
        }) if value == &serde_json::json!({})
    ));

    let current = built_in_manifest_for_version(BuiltInStyle::PersistentChat, "1.2.0")
        .expect("current persistent 1.2");
    assert_eq!(current, built_in_manifest(BuiltInStyle::PersistentChat));
    assert_eq!(current.allowed_providers, ["deterministic-mock"]);
    assert!(
        current
            .required_capabilities
            .contains(&String::from("context"))
    );
    let compiled = compile_style(
        &current,
        &current_builtin_context(),
        StyleCompilerLimits::default(),
    )
    .expect("current persistent compile");
    assert_eq!(compiled.graph.variables.len(), 3);
    let node = |id: &str| {
        compiled
            .graph
            .nodes
            .iter()
            .find(|node| node.id == id)
            .unwrap_or_else(|| panic!("missing node {id}"))
    };
    assert!(matches!(
        node("prepare-context").configuration.as_ref(),
        Some(NodeConfiguration::ContextTransform {
            strategy: agentmod_graph_engine::ContextTransformStrategy::PreserveHistory
        })
    ));
    assert!(matches!(
        node("respond").configuration.as_ref(),
        Some(NodeConfiguration::ModelRequest { .. })
    ));
    assert!(matches!(
        node("tool-batch").configuration.as_ref(),
        Some(NodeConfiguration::ProviderToolBatchExecution {
            maximum_calls: 32,
            ..
        })
    ));
    assert!(matches!(
        node("done").configuration.as_ref(),
        Some(NodeConfiguration::CompleteTurn {
            cleanup: CompleteTurnCleanup::PreserveProjection,
            ..
        })
    ));
}

#[test]
fn shipped_built_in_version_table_is_complete_sorted_and_exact() {
    for style in [
        BuiltInStyle::PersistentChat,
        BuiltInStyle::EphemeralTurn,
        BuiltInStyle::ResearchLoop,
        BuiltInStyle::PlannerWorker,
        BuiltInStyle::DeclarativeGraph,
    ] {
        let versions = built_in_versions(style);
        assert!(!versions.is_empty());
        let parsed = versions
            .iter()
            .map(|version| semver::Version::parse(version).expect("shipped semantic version"))
            .collect::<Vec<_>>();
        assert!(
            parsed.windows(2).all(|pair| pair[0] < pair[1]),
            "shipped versions must be unique and oldest-to-newest"
        );
        for version in versions {
            let manifest = built_in_manifest_for_version(style, version)
                .expect("every listed version has one exact complete descriptor");
            assert_eq!(manifest.identity.version, *version);
            assert_eq!(manifest.built_in_semantic, Some(style));
        }
        assert_eq!(
            built_in_manifest(style),
            built_in_manifest_for_version(style, versions.last().expect("latest version"))
                .expect("latest exact descriptor")
        );
        assert!(built_in_manifest_for_version(style, "0.0.0").is_none());
    }
}

#[test]
fn shipped_child_mcp_inheritance_defaults_false_without_changing_canonical_documents() {
    for style in [
        BuiltInStyle::PersistentChat,
        BuiltInStyle::EphemeralTurn,
        BuiltInStyle::ResearchLoop,
        BuiltInStyle::PlannerWorker,
        BuiltInStyle::DeclarativeGraph,
    ] {
        for version in built_in_versions(style) {
            let manifest =
                built_in_manifest_for_version(style, version).expect("listed built-in manifest");
            assert_eq!(manifest.child_agents.inherit_mcp, None);
            assert!(
                !to_json(&manifest)
                    .expect("canonical JSON")
                    .contains("inherit_mcp")
            );
            assert!(
                !to_toml(&manifest)
                    .expect("canonical TOML")
                    .contains("inherit_mcp")
            );
        }
    }

    let manifest = built_in_manifest(BuiltInStyle::PersistentChat);
    let runtime = current_builtin_context();
    let baseline = compile_style(&manifest, &runtime, StyleCompilerLimits::default())
        .expect("baseline style")
        .cache_key;
    let mut explicit_false = manifest;
    explicit_false.child_agents.inherit_mcp = Some(false);
    let explicit = compile_style(&explicit_false, &runtime, StyleCompilerLimits::default())
        .expect("explicit false remains valid")
        .cache_key;
    assert_ne!(baseline.style_content_hash, explicit.style_content_hash);
    assert_ne!(baseline.combined_hash, explicit.combined_hash);
}

#[test]
fn child_mcp_inheritance_requires_the_child_mcp_tool_group() {
    let mut disabled = built_in_manifest(BuiltInStyle::ResearchLoop);
    disabled.child_agents.inherit_mcp = Some(false);
    let error = compile_style(
        &disabled,
        &current_builtin_context(),
        StyleCompilerLimits::default(),
    )
    .expect_err("disabled child policies must omit MCP inheritance");
    assert!(codes(&error).contains(&"STYLE021"));

    let mut manifest = built_in_manifest(BuiltInStyle::PersistentChat);
    manifest.child_agents.inherit_mcp = Some(true);
    let error = compile_style(
        &manifest,
        &current_builtin_context(),
        StyleCompilerLimits::default(),
    )
    .expect_err("MCP inheritance without child MCP authority");
    assert!(codes(&error).contains(&"STYLE021"));

    manifest.child_agents.tool_groups.push(String::from("mcp"));
    manifest.child_agents.tool_groups.sort();
    let compiled = compile_style(
        &manifest,
        &current_builtin_context(),
        StyleCompilerLimits::default(),
    )
    .expect("MCP-enabled child policy");
    assert_eq!(compiled.child_agents.inherit_mcp, Some(true));
}

#[test]
fn planner_worker_1_1_compiles_complete_child_execution_policy() {
    let manifest = built_in_manifest_for_version(BuiltInStyle::PlannerWorker, "1.1.0")
        .expect("frozen planner-worker 1.1");
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
#[allow(
    clippy::too_many_lines,
    reason = "the typed planner-worker oracle keeps each exact executor configuration adjacent"
)]
fn planner_worker_1_2_compiles_exact_generic_executor_contracts() {
    let manifest = built_in_manifest_for_version(BuiltInStyle::PlannerWorker, "1.2.0")
        .expect("planner-worker 1.2");
    assert_eq!(manifest.allowed_providers, ["deterministic-mock"]);
    assert_eq!(
        manifest.child_agents.child_style.as_deref(),
        Some("ephemeral-turn@1.2.0")
    );
    assert_eq!(manifest.child_agents.max_concurrent, 2);
    assert_eq!(manifest.child_agents.reviewer_max_attempts, Some(2));

    let compiled = compile_style(
        &manifest,
        &current_builtin_context(),
        StyleCompilerLimits::default(),
    )
    .expect("planner-worker 1.2 compiles");
    let node = |id: &str| {
        compiled
            .graph
            .nodes
            .iter()
            .find(|node| node.id == id)
            .unwrap_or_else(|| panic!("missing node {id}"))
    };
    assert_eq!(
        node("plan").configuration,
        Some(NodeConfiguration::ModelRequest {
            disposition_output: String::from("plan_disposition"),
            result_output: String::from("plan_result"),
            provider_options: BTreeMap::from([(
                String::from("mock_planner_phase"),
                String::from("plan"),
            )]),
            json_outputs: BTreeMap::from([
                (
                    String::from("evidence_task"),
                    String::from("/tasks/1/description"),
                ),
                (
                    String::from("planner_task"),
                    String::from("/tasks/0/description"),
                ),
            ]),
            inputs: BTreeMap::new(),
        })
    );
    assert_eq!(node("plan-route").kind, NodeKind::ConditionalBranch);
    for (id, task_prefix, task_variable) in [
        ("spawn-planner", "planner-task", "planner_task"),
        ("spawn-evidence", "evidence-task", "evidence_task"),
    ] {
        assert!(matches!(
            node(id).configuration.as_ref(),
            Some(NodeConfiguration::SpawnChildAgent {
                task_input: NodeValueSource::Variable { variable },
                task_id_prefix,
                child_style,
                maximum_children: 1,
                maximum_depth: 2,
                token_budget: 100_000,
                context_budget_tokens: 64_000,
                cost_budget_micros: 10_000_000,
                workspace: ChildWorkspaceConfiguration::SharedReadOnly,
                approval_required: true,
                ..
            }) if variable == task_variable
                && task_id_prefix == task_prefix
                && child_style == "ephemeral-turn@1.2.0"
        ));
    }
    assert!(matches!(
        node("wait-fanout").configuration.as_ref(),
        Some(NodeConfiguration::ParallelBranch {
            max_parallelism: 2,
            max_queue_depth: 2,
            join_target,
            join_policy: ParallelJoinPolicy::All,
            ..
        }) if join_target == "join-workers"
    ));
    for (id, variable) in [
        ("wait-planner", "planner_child"),
        ("wait-evidence", "evidence_child"),
    ] {
        assert!(matches!(
            node(id).configuration.as_ref(),
            Some(NodeConfiguration::WaitForAgents {
                children: ChildSetSource::Variable { variable: selected },
                maximum_children: 1,
                minimum_successes: 1,
                timeout_ms: 600_000,
                cancellation: ChildWaitCancellation::Cascade,
            }) if selected == variable
        ));
    }
    assert_eq!(
        node("join-workers").configuration,
        Some(NodeConfiguration::JoinResults {
            required: BTreeSet::from([String::from("evidence"), String::from("planner")]),
            optional: BTreeSet::new(),
            minimum_successes: 2,
            failure_policy: JoinFailurePolicy::WaitRequired,
            ordering_policy: JoinOrderingPolicy::MemberId,
            timeout_ms: 600_000,
            cancellation_propagates: true,
            result_projection: JoinResultProjection::NodeReferences,
            artifact_collection: JoinArtifactCollection::All,
        })
    );
    assert_eq!(
        node("integrate").configuration,
        Some(NodeConfiguration::ModelRequest {
            disposition_output: String::from("integration_disposition"),
            result_output: String::from("integration_result"),
            provider_options: BTreeMap::from([(
                String::from("mock_planner_phase"),
                String::from("integrate"),
            )]),
            json_outputs: BTreeMap::new(),
            inputs: BTreeMap::new(),
        })
    );
    assert_eq!(node("integration-route").kind, NodeKind::ConditionalBranch);
    assert_eq!(
        node("persist-integration").configuration,
        Some(NodeConfiguration::PersistArtifact {
            content: ArtifactContentSource::ProviderResultText {
                reference_variable: String::from("integration_result"),
            },
            mime_type: String::from("text/markdown"),
            security: ArtifactSecurityClassification::Private,
            retention: ArtifactRetentionPolicy::Session,
        })
    );
    assert_eq!(
        node("review").configuration,
        Some(NodeConfiguration::Review {
            input: NodeValueSource::Variable {
                variable: String::from("integration_result"),
            },
            artifact_references: BTreeSet::new(),
            artifact_reference_variables: BTreeSet::new(),
            result_schema: ReviewResultSchema {
                maximum_findings: 16,
                maximum_finding_bytes: 1024,
                maximum_rejections: 2,
                require_artifact_evidence: false,
            },
            routes: ReviewRoutes {
                approved: String::from("done"),
                revision: String::from("revision"),
                failure: String::from("structured-failure"),
            },
            maximum_revisions: 2,
        })
    );
    assert_eq!(node("revision").kind, NodeKind::Loop);
    assert_eq!(node("revision").max_iterations, Some(2));
    assert_eq!(node("done").kind, NodeKind::CompleteSession);
    assert_eq!(node("structured-failure").kind, NodeKind::Fail);
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the planner-worker variable oracle keeps the exact producer, consumer, type, bound, and security contract together"
)]
fn planner_worker_1_2_declares_exact_canonical_variables() {
    let manifest = built_in_manifest_for_version(BuiltInStyle::PlannerWorker, "1.2.0")
        .expect("planner-worker 1.2");
    let compiled = compile_style(
        &manifest,
        &current_builtin_context(),
        StyleCompilerLimits::default(),
    )
    .expect("planner-worker 1.2 compiles");
    let variable = |name: &str| {
        compiled
            .graph
            .variables
            .iter()
            .find(|variable| variable.name == name)
            .unwrap_or_else(|| panic!("missing variable {name}"))
    };
    for (name, producer, consumers) in [
        (
            "planner_child",
            "spawn-planner",
            BTreeSet::from([String::from("join-workers"), String::from("wait-planner")]),
        ),
        (
            "evidence_child",
            "spawn-evidence",
            BTreeSet::from([String::from("join-workers"), String::from("wait-evidence")]),
        ),
    ] {
        let child = variable(name);
        assert_eq!(child.value_type, VariableValueType::ChildId);
        assert_eq!(child.producer, producer);
        assert_eq!(child.consumers, consumers);
    }
    for (name, consumer) in [
        ("planner_task", "spawn-planner"),
        ("evidence_task", "spawn-evidence"),
    ] {
        let task = variable(name);
        assert_eq!(task.value_type, VariableValueType::String);
        assert_eq!(task.producer, "plan");
        assert_eq!(task.consumers, BTreeSet::from([String::from(consumer)]));
        assert_eq!(task.max_size_bytes, 8192);
    }
    for (name, producer, consumers) in [
        (
            "plan_result",
            "plan",
            BTreeSet::from([String::from("plan-route")]),
        ),
        (
            "joined_results",
            "join-workers",
            BTreeSet::from([String::from("integrate")]),
        ),
        (
            "integration_result",
            "integrate",
            BTreeSet::from([
                String::from("done"),
                String::from("persist-integration"),
                String::from("review"),
            ]),
        ),
    ] {
        let result = variable(name);
        assert_eq!(result.value_type, VariableValueType::NodeResultReference);
        assert_eq!(result.producer, producer);
        assert_eq!(result.consumers, consumers);
    }
    for (name, producer, consumer) in [
        ("plan_disposition", "plan", "plan-route"),
        ("integration_disposition", "integrate", "integration-route"),
    ] {
        let disposition = variable(name);
        assert_eq!(
            disposition.value_type,
            VariableValueType::Enum {
                values: BTreeSet::from([
                    String::from("response_complete"),
                    String::from("tool_requests"),
                ]),
            }
        );
        assert_eq!(disposition.producer, producer);
        assert_eq!(
            disposition.consumers,
            BTreeSet::from([String::from(consumer)])
        );
    }
    let artifact = variable("integration_artifact");
    assert_eq!(artifact.value_type, VariableValueType::ArtifactReference);
    assert_eq!(artifact.producer, "persist-integration");
    assert_eq!(
        artifact.consumers,
        BTreeSet::from([String::from("done"), String::from("review")])
    );
    assert_eq!(
        artifact.security_classification,
        SecurityClassification::Confidential
    );
    assert_eq!(
        variable("iteration").value_type,
        VariableValueType::Map {
            value_type: Box::new(VariableValueType::Boolean),
            max_entries: 1,
        }
    );
    for variable in &compiled.graph.variables {
        assert_eq!(variable.scope, VariableScope::Run);
        assert_eq!(variable.mutability, VariableMutability::Mutable);
        assert_eq!(variable.merge_policy, None);
        if variable.name != "integration_artifact" {
            assert_eq!(
                variable.security_classification,
                SecurityClassification::Internal
            );
        }
    }
    assert_eq!(compiled.graph.variables.len(), 11);
}

#[test]
fn planner_worker_1_2_routes_only_from_canonical_outputs() {
    let manifest = built_in_manifest_for_version(BuiltInStyle::PlannerWorker, "1.2.0")
        .expect("planner-worker 1.2");
    let compiled = compile_style(
        &manifest,
        &current_builtin_context(),
        StyleCompilerLimits::default(),
    )
    .expect("planner-worker 1.2 compiles");
    let node = |id: &str| {
        compiled
            .graph
            .nodes
            .iter()
            .find(|node| node.id == id)
            .unwrap_or_else(|| panic!("missing node {id}"))
    };
    let destinations = |from: &str, variables: &serde_json::Value| {
        compiled
            .graph
            .edges
            .iter()
            .filter(|edge| edge.from == node(from).index)
            .filter(|edge| {
                edge.condition
                    .as_ref()
                    .is_none_or(|condition| condition.evaluate(variables).unwrap())
            })
            .map(|edge| compiled.graph.nodes[edge.to].id.as_str())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        destinations(
            "plan-route",
            &serde_json::json!({"plan_disposition":"response_complete"})
        ),
        ["spawn-planner"]
    );
    assert_eq!(
        destinations(
            "plan-route",
            &serde_json::json!({"plan_disposition":"tool_requests"})
        ),
        ["structured-failure"]
    );
    assert_eq!(
        destinations(
            "integration-route",
            &serde_json::json!({"integration_disposition":"response_complete"})
        ),
        ["persist-integration"]
    );
    assert_eq!(
        destinations(
            "integration-route",
            &serde_json::json!({"integration_disposition":"tool_requests"})
        ),
        ["structured-failure"]
    );
    assert_eq!(
        destinations(
            "revision",
            &serde_json::json!({"iteration":{"remaining":true}})
        ),
        ["spawn-planner"]
    );
    assert_eq!(
        destinations(
            "revision",
            &serde_json::json!({"iteration":{"remaining":false}})
        ),
        ["structured-failure"]
    );
    for control in ["plan-route", "integration-route", "revision"] {
        assert!(
            compiled
                .graph
                .edges
                .iter()
                .filter(|edge| edge.from == node(control).index)
                .all(|edge| edge
                    .condition
                    .as_ref()
                    .expect("control condition")
                    .evaluate(&serde_json::json!({}))
                    .is_err())
        );
    }
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the exact historical planner contract keeps parallel, review, and failure routes visible together"
)]
fn planner_worker_1_3_compiles_parallel_child_spawns_and_exact_merges() {
    let manifest = built_in_manifest_for_version(BuiltInStyle::PlannerWorker, "1.3.0")
        .expect("planner-worker 1.3");
    let current = built_in_manifest(BuiltInStyle::PlannerWorker);
    assert_eq!(manifest.identity.id, "planner-worker");
    assert_eq!(manifest.identity.version, "1.3.0");
    assert_eq!(current.identity.version, "1.4.0");
    assert_ne!(manifest, current);
    let compiled = compile_style(
        &manifest,
        &current_builtin_context(),
        StyleCompilerLimits::default(),
    )
    .expect("planner-worker 1.3 compiles");
    let node = |id: &str| {
        compiled
            .graph
            .nodes
            .iter()
            .find(|node| node.id == id)
            .unwrap_or_else(|| panic!("missing node {id}"))
    };
    assert!(matches!(
        node("worker-fanout").configuration.as_ref(),
        Some(NodeConfiguration::ParallelBranch {
            max_parallelism: 2,
            max_queue_depth: 2,
            join_target,
            join_policy: ParallelJoinPolicy::All,
            variable_merge_policies,
            ..
        }) if join_target == "join-workers"
            && variable_merge_policies
                == &BTreeMap::from([
                    (String::from("evidence_child"), VariableMergePolicy::FirstBranch),
                    (String::from("planner_child"), VariableMergePolicy::FirstBranch),
                ])
    ));
    for variable in ["planner_child", "evidence_child"] {
        assert_eq!(
            compiled
                .graph
                .variables
                .iter()
                .find(|declaration| declaration.name == variable)
                .expect("child variable")
                .merge_policy,
            Some(VariableMergePolicy::FirstBranch)
        );
    }
    let destinations = |from: &str| {
        compiled
            .graph
            .edges
            .iter()
            .filter(|edge| edge.from == node(from).index)
            .map(|edge| compiled.graph.nodes[edge.to].id.as_str())
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(
        destinations("worker-fanout"),
        BTreeSet::from(["spawn-evidence", "spawn-planner"])
    );
    assert_eq!(
        destinations("spawn-planner"),
        BTreeSet::from(["wait-planner"])
    );
    assert_eq!(
        destinations("spawn-evidence"),
        BTreeSet::from(["wait-evidence"])
    );
    assert!(matches!(
        node("review").configuration.as_ref(),
        Some(NodeConfiguration::Review {
            artifact_references,
            result_schema: ReviewResultSchema {
                require_artifact_evidence: true,
                ..
            },
            routes,
            maximum_revisions: 2,
            ..
        }) if artifact_references == &BTreeSet::from([String::from("integration_artifact")])
            && routes.approved == "done"
            && routes.revision == "revision"
            && routes.failure == "structured-failure"
    ));
    assert_eq!(node("structured-failure").kind, NodeKind::Fail);
    assert_eq!(
        destinations("review"),
        BTreeSet::from(["done", "revision", "structured-failure"])
    );
    assert_eq!(
        destinations("revision"),
        BTreeSet::from(["structured-failure", "worker-fanout"])
    );
    let revision_limit = compiled
        .graph
        .edges
        .iter()
        .find(|edge| {
            edge.from == node("revision").index && edge.to == node("structured-failure").index
        })
        .and_then(|edge| edge.condition.as_ref())
        .expect("compiled revision-limit condition");
    assert!(
        revision_limit
            .evaluate(&serde_json::json!({"iteration": {"remaining": false}}))
            .expect("revision limit is deterministic")
    );
    assert!(
        !revision_limit
            .evaluate(&serde_json::json!({"iteration": {"remaining": true}}))
            .expect("revision continuation is deterministic")
    );
}

#[test]
fn planner_worker_versions_preserve_history_and_select_exact_1_4() {
    assert_eq!(
        built_in_versions(BuiltInStyle::PlannerWorker),
        ["1.1.0", "1.2.0", "1.3.0", "1.4.0"]
    );
    let legacy = built_in_manifest_for_version(BuiltInStyle::PlannerWorker, "1.1.0")
        .expect("frozen planner-worker 1.1");
    let typed = built_in_manifest_for_version(BuiltInStyle::PlannerWorker, "1.2.0")
        .expect("typed planner-worker 1.2");
    let parallel = built_in_manifest_for_version(BuiltInStyle::PlannerWorker, "1.3.0")
        .expect("parallel planner-worker 1.3");
    let current = built_in_manifest_for_version(BuiltInStyle::PlannerWorker, "1.4.0")
        .expect("evidence-backed planner-worker 1.4");
    assert_ne!(legacy, typed);
    assert_ne!(typed, parallel);
    assert_ne!(parallel, current);
    assert_eq!(current, built_in_manifest(BuiltInStyle::PlannerWorker));
    assert!(built_in_manifest_for_version(BuiltInStyle::PlannerWorker, "1.5.0").is_none());

    let legacy_compiled = compile_style(&legacy, &context(), StyleCompilerLimits::default())
        .expect("legacy planner-worker compiles");
    let current_context = current_builtin_context();
    let typed_compiled = compile_style(&typed, &current_context, StyleCompilerLimits::default())
        .expect("typed planner-worker compiles");
    let parallel_compiled =
        compile_style(&parallel, &current_context, StyleCompilerLimits::default())
            .expect("parallel planner-worker compiles");
    let first = compile_style(&current, &current_context, StyleCompilerLimits::default())
        .expect("current planner-worker compiles");
    let second = compile_style(&current, &current_context, StyleCompilerLimits::default())
        .expect("current planner-worker recompiles");
    assert_eq!(
        legacy_compiled.cache_key.style_content_hash.to_hex(),
        "91a12b954dc0c4da922ae0469f9bc641148a7368a335c4eff94df01bc8177e73"
    );
    assert_eq!(
        legacy_compiled.cache_key.combined_hash.to_hex(),
        "e623481aa0d373d6271cb0982b5ee2adc2497f7df7f85d81e16b4147f14b0ea1"
    );
    assert_eq!(first, second);
    assert_eq!(legacy_compiled.style_version, "1.1.0");
    assert_eq!(typed_compiled.style_version, "1.2.0");
    assert_eq!(parallel_compiled.style_version, "1.3.0");
    assert_eq!(first.style_version, "1.4.0");
    assert!(legacy_compiled.graph.variables.is_empty());
    assert_eq!(typed_compiled.graph.variables.len(), 11);
    assert_eq!(first.graph.variables.len(), 11);
    assert_ne!(
        legacy_compiled.cache_key.style_content_hash,
        first.cache_key.style_content_hash
    );
}

#[test]
fn research_loop_1_1_declares_bounded_context_and_capabilities() {
    let manifest = built_in_manifest_for_version(BuiltInStyle::ResearchLoop, "1.1.0")
        .expect("frozen research-loop 1.1");
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
    let manifest = built_in_manifest_for_version(BuiltInStyle::ResearchLoop, "1.1.0")
        .expect("frozen research-loop 1.1");
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
fn research_loop_1_2_compiles_typed_generic_execution_contracts() {
    let manifest = built_in_manifest_for_version(BuiltInStyle::ResearchLoop, "1.2.0")
        .expect("research-loop 1.2");
    let current = built_in_manifest(BuiltInStyle::ResearchLoop);
    assert_eq!(manifest.identity.id, "research-loop");
    assert_eq!(manifest.identity.version, "1.2.0");
    assert_eq!(current.identity.version, "1.3.0");
    assert_ne!(manifest, current);
    assert_eq!(manifest.allowed_providers, ["deterministic-mock"]);
    assert_eq!(
        manifest.required_capabilities,
        ["approval", "artifacts", "context", "model", "tools"]
    );
    assert_eq!(
        manifest.memory.retrieval_timing,
        MemoryRetrievalTiming::IterationStart
    );
    assert_eq!(
        manifest.memory.injection_location,
        MemoryInjectionLocation::BeforeCurrentInput
    );

    let compiled = compile_style(
        &manifest,
        &current_builtin_context(),
        StyleCompilerLimits::default(),
    )
    .expect("research-loop 1.2 compiles");
    let node = |id: &str| {
        compiled
            .graph
            .nodes
            .iter()
            .find(|node| node.id == id)
            .unwrap_or_else(|| panic!("missing node {id}"))
    };
    assert_eq!(
        node("fresh-context").configuration,
        Some(NodeConfiguration::ContextTransform {
            strategy: ContextTransformStrategy::Fresh,
        })
    );
    assert_eq!(
        node("research").configuration,
        Some(NodeConfiguration::ModelRequest {
            disposition_output: String::from("model_disposition"),
            result_output: String::from("model_result"),
            provider_options: BTreeMap::new(),
            json_outputs: BTreeMap::new(),
            inputs: BTreeMap::new(),
        })
    );
    assert_eq!(
        node("tool-batch").configuration,
        Some(NodeConfiguration::ProviderToolBatchExecution {
            request_reference_variable: String::from("model_result"),
            disposition_variable: String::from("model_disposition"),
            maximum_calls: 32,
            allowed_tools: BTreeSet::from([String::from("filesystem.read")]),
        })
    );
    assert_eq!(
        node("persist").configuration,
        Some(NodeConfiguration::PersistArtifact {
            content: ArtifactContentSource::ProviderResultText {
                reference_variable: String::from("research_receipt"),
            },
            mime_type: String::from("text/markdown"),
            security: ArtifactSecurityClassification::Private,
            retention: ArtifactRetentionPolicy::Session,
        })
    );
    assert_eq!(
        node("persist").read_variables,
        BTreeSet::from([String::from("research_receipt")])
    );
    assert_eq!(
        node("persist").write_variables,
        BTreeSet::from([String::from("receipt_artifact")])
    );
    assert_eq!(node("repeat").kind, NodeKind::Loop);
    assert_eq!(node("repeat").max_iterations, Some(3));
    assert_eq!(
        node("repeat").read_variables,
        BTreeSet::from([String::from("iteration")])
    );
    assert_eq!(
        node("repeat").write_variables,
        BTreeSet::from([String::from("iteration")])
    );
    assert_eq!(node("done").kind, NodeKind::CompleteSession);
    assert_eq!(
        node("done").read_variables,
        BTreeSet::from([String::from("receipt_artifact")])
    );
}

#[test]
fn research_loop_1_2_routes_from_canonical_loop_output_and_fails_closed() {
    let manifest = built_in_manifest_for_version(BuiltInStyle::ResearchLoop, "1.2.0")
        .expect("research-loop 1.2");
    let compiled = compile_style(
        &manifest,
        &current_builtin_context(),
        StyleCompilerLimits::default(),
    )
    .expect("research-loop 1.2 compiles");
    let node = |id: &str| {
        compiled
            .graph
            .nodes
            .iter()
            .find(|node| node.id == id)
            .unwrap_or_else(|| panic!("missing node {id}"))
    };
    let transition = |variables: &serde_json::Value| {
        compiled
            .graph
            .edges
            .iter()
            .filter(|edge| edge.from == node("repeat").index)
            .filter(|edge| {
                edge.condition
                    .as_ref()
                    .is_none_or(|condition| condition.evaluate(variables).unwrap())
            })
            .map(|edge| compiled.graph.nodes[edge.to].id.as_str())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        transition(&serde_json::json!({"iteration":{"remaining":true}})),
        ["fresh-context"]
    );
    assert_eq!(
        transition(&serde_json::json!({"iteration":{"remaining":false}})),
        ["done"]
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
                .expect("loop transition condition")
                .evaluate(&serde_json::json!({}))
                .is_err()),
        "generic loop routing must fail closed without its canonical output"
    );
}

#[test]
fn research_loop_1_2_declares_exact_canonical_variable_contracts() {
    let manifest = built_in_manifest_for_version(BuiltInStyle::ResearchLoop, "1.2.0")
        .expect("research-loop 1.2");
    let compiled = compile_style(
        &manifest,
        &current_builtin_context(),
        StyleCompilerLimits::default(),
    )
    .expect("research-loop 1.2 compiles");
    let variable = |name: &str| {
        compiled
            .graph
            .variables
            .iter()
            .find(|variable| variable.name == name)
            .unwrap_or_else(|| panic!("missing variable {name}"))
    };

    let disposition = variable("model_disposition");
    assert_eq!(
        disposition.value_type,
        VariableValueType::Enum {
            values: BTreeSet::from([
                String::from("response_complete"),
                String::from("tool_requests"),
            ]),
        }
    );
    assert_eq!(disposition.producer, "research");
    assert_eq!(
        disposition.consumers,
        BTreeSet::from([String::from("tool-batch")])
    );

    for (name, producer, consumer) in [
        ("model_result", "research", "tool-batch"),
        ("research_receipt", "tool-batch", "persist"),
    ] {
        let reference = variable(name);
        assert_eq!(reference.value_type, VariableValueType::NodeResultReference);
        assert_eq!(reference.producer, producer);
        assert_eq!(reference.consumers, BTreeSet::from([consumer.to_owned()]));
    }

    let artifact = variable("receipt_artifact");
    assert_eq!(artifact.value_type, VariableValueType::ArtifactReference);
    assert_eq!(artifact.producer, "persist");
    assert_eq!(artifact.consumers, BTreeSet::from([String::from("done")]));
    assert_eq!(
        artifact.security_classification,
        SecurityClassification::Confidential
    );

    let completion = variable("iteration");
    assert_eq!(
        completion.value_type,
        VariableValueType::Map {
            value_type: Box::new(VariableValueType::Boolean),
            max_entries: 1,
        }
    );
    assert_eq!(completion.producer, "repeat");
    assert_eq!(
        completion.consumers,
        BTreeSet::from([String::from("repeat")])
    );

    for variable in &compiled.graph.variables {
        assert_eq!(variable.scope, VariableScope::Run);
        assert_eq!(variable.mutability, VariableMutability::Mutable);
        assert_eq!(variable.merge_policy, None);
    }
    assert_eq!(compiled.graph.variables.len(), 5);
}

#[test]
fn research_loop_versions_and_cache_identities_are_exact_and_deterministic() {
    assert_eq!(
        built_in_versions(BuiltInStyle::ResearchLoop),
        ["1.1.0", "1.2.0", "1.3.0"]
    );
    assert!(
        built_in_manifest_for_version(BuiltInStyle::ResearchLoop, "1.0.0").is_none(),
        "persisted 1.0.0 selectors must not bind to research-loop 1.1.0"
    );
    assert!(built_in_manifest_for_version(BuiltInStyle::ResearchLoop, "1.4.0").is_none());
    let legacy = built_in_manifest_for_version(BuiltInStyle::ResearchLoop, "1.1.0")
        .expect("exact frozen research-loop version");
    let typed = built_in_manifest_for_version(BuiltInStyle::ResearchLoop, "1.2.0")
        .expect("exact typed research-loop version");
    let manifest = built_in_manifest_for_version(BuiltInStyle::ResearchLoop, "1.3.0")
        .expect("exact current research-loop version");
    assert_ne!(legacy, typed);
    assert_ne!(typed, manifest);
    assert_eq!(manifest, built_in_manifest(BuiltInStyle::ResearchLoop));

    let legacy_first = compile_style(&legacy, &context(), StyleCompilerLimits::default())
        .expect("first deterministic legacy compile");
    let legacy_second = compile_style(&legacy, &context(), StyleCompilerLimits::default())
        .expect("second deterministic legacy compile");
    assert_eq!(legacy_first, legacy_second);
    assert_eq!(legacy_first.style_version, "1.1.0");
    assert!(legacy_first.graph.variables.is_empty());
    assert_eq!(
        legacy_first.cache_key.style_content_hash.to_hex(),
        "4df10d7ea07306da14dc6735af520d37f3fb8ccf6136ebeca99dd378817a1bf2"
    );
    assert_eq!(
        legacy_first.cache_key.combined_hash.to_hex(),
        "f7d30ae18e812512ccce77e379fec38b289dcd321d5e584c1ec50fa53f998276"
    );

    let current_context = current_builtin_context();
    let first = compile_style(&manifest, &current_context, StyleCompilerLimits::default())
        .expect("first deterministic current compile");
    let second = compile_style(&manifest, &current_context, StyleCompilerLimits::default())
        .expect("second deterministic current compile");
    assert_eq!(first, second);
    assert_eq!(first.style_version, "1.3.0");
    assert_ne!(
        first.cache_key.style_content_hash,
        legacy_first.cache_key.style_content_hash
    );
    assert_ne!(
        first.cache_key.combined_hash,
        legacy_first.cache_key.combined_hash
    );

    let json = to_json(&manifest).expect("research JSON");
    let toml = to_toml(&manifest).expect("research TOML");
    assert_eq!(parse_json(&json).expect("JSON round trip"), manifest);
    assert_eq!(parse_toml(&toml).expect("TOML round trip"), manifest);
    let legacy_json = to_json(&legacy).expect("legacy research JSON");
    let legacy_toml = to_toml(&legacy).expect("legacy research TOML");
    assert_eq!(parse_json(&legacy_json).expect("legacy JSON"), legacy);
    assert_eq!(parse_toml(&legacy_toml).expect("legacy TOML"), legacy);
}

#[test]
fn declarative_graph_1_2_declares_the_generic_execution_capabilities() {
    let manifest = built_in_manifest(BuiltInStyle::DeclarativeGraph);
    assert_eq!(manifest.identity.id, "declarative-graph");
    assert_eq!(manifest.identity.version, "1.2.0");
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
        "the user-graph wrapper must retain the exact typed built-in graph"
    );
}

#[test]
fn declarative_graph_1_2_compiles_typed_node_inputs_outputs_and_bounded_loop() {
    let manifest = built_in_manifest(BuiltInStyle::DeclarativeGraph);
    let compiled = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect("declarative-graph 1.2 compiles");
    let nodes = &compiled.graph.nodes;
    let node = |id: &str| {
        nodes
            .iter()
            .find(|node| node.id == id)
            .unwrap_or_else(|| panic!("missing node {id}"))
    };
    assert_eq!(node("branch").kind, NodeKind::ConditionalBranch);
    assert_eq!(node("branch").read_variables, ["request".to_owned()].into());
    assert!(node("branch").write_variables.is_empty());
    assert_eq!(node("approval").kind, NodeKind::UserApproval);
    assert_eq!(node("tool").kind, NodeKind::ToolExecutionGate);
    assert_eq!(node("tool").tool.as_deref(), Some("filesystem.read"));
    assert_eq!(
        node("tool").read_variables,
        ["tool_arguments".to_owned()].into()
    );
    assert!(node("tool").write_variables.is_empty());
    assert_eq!(
        node("tool").configuration,
        Some(NodeConfiguration::ToolExecution {
            arguments: NodeValueSource::Variable {
                variable: String::from("tool_arguments"),
            },
        })
    );
    assert_eq!(node("repeat").kind, NodeKind::Loop);
    assert_eq!(
        node("repeat").read_variables,
        ["iteration".to_owned()].into()
    );
    assert_eq!(
        node("repeat").write_variables,
        ["iteration".to_owned()].into()
    );
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
}

#[test]
fn declarative_graph_1_2_conditions_are_deterministic_and_fail_closed() {
    let manifest = built_in_manifest(BuiltInStyle::DeclarativeGraph);
    let compiled = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect("declarative-graph 1.2 compiles");
    let nodes = &compiled.graph.nodes;
    let node = |id: &str| {
        nodes
            .iter()
            .find(|node| node.id == id)
            .unwrap_or_else(|| panic!("missing node {id}"))
    };
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
fn declarative_graph_1_2_declares_exact_canonical_variable_contracts() {
    let manifest = built_in_manifest(BuiltInStyle::DeclarativeGraph);
    let compiled = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect("declarative-graph 1.2 compiles");
    let variable = |name: &str| {
        compiled
            .graph
            .variables
            .iter()
            .find(|variable| variable.name == name)
            .unwrap_or_else(|| panic!("missing variable {name}"))
    };

    let request = variable("request");
    assert_eq!(
        request.value_type,
        VariableValueType::Map {
            value_type: Box::new(VariableValueType::Boolean),
            max_entries: 1,
        }
    );
    assert_eq!(request.scope, VariableScope::Run);
    assert_eq!(request.producer, "runtime");
    assert_eq!(request.consumers, ["branch".to_owned()].into());
    assert_eq!(request.mutability, VariableMutability::Immutable);
    assert_eq!(request.merge_policy, None);
    assert_eq!(request.max_size_bytes, 128);
    assert_eq!(
        request.security_classification,
        SecurityClassification::Internal
    );

    let tool_arguments = variable("tool_arguments");
    assert_eq!(
        tool_arguments.value_type,
        VariableValueType::Map {
            value_type: Box::new(VariableValueType::String),
            max_entries: 8,
        }
    );
    assert_eq!(tool_arguments.scope, VariableScope::Run);
    assert_eq!(tool_arguments.producer, "runtime");
    assert_eq!(tool_arguments.consumers, ["tool".to_owned()].into());
    assert_eq!(tool_arguments.mutability, VariableMutability::Immutable);
    assert_eq!(tool_arguments.merge_policy, None);
    assert_eq!(tool_arguments.max_size_bytes, 4096);
    assert_eq!(
        tool_arguments.security_classification,
        SecurityClassification::Internal
    );

    let iteration = variable("iteration");
    assert_eq!(
        iteration.value_type,
        VariableValueType::Map {
            value_type: Box::new(VariableValueType::Boolean),
            max_entries: 1,
        }
    );
    assert_eq!(iteration.scope, VariableScope::Run);
    assert_eq!(iteration.producer, "repeat");
    assert_eq!(iteration.consumers, ["repeat".to_owned()].into());
    assert_eq!(iteration.mutability, VariableMutability::Mutable);
    assert_eq!(iteration.merge_policy, None);
    assert_eq!(iteration.max_size_bytes, 128);
    assert_eq!(
        iteration.security_classification,
        SecurityClassification::Internal
    );
    assert_eq!(compiled.graph.variables.len(), 3);
}

#[test]
fn declarative_graph_version_and_cache_identity_are_exact_and_deterministic() {
    assert!(
        built_in_manifest_for_version(BuiltInStyle::DeclarativeGraph, "1.0.0").is_none(),
        "persisted 1.0.0 selectors must not bind to a newer descriptor"
    );
    let legacy = built_in_manifest_for_version(BuiltInStyle::DeclarativeGraph, "1.1.0")
        .expect("legacy declarative-graph remains available for exact recovery fixtures");
    assert_eq!(legacy.identity.version, "1.1.0");
    assert_ne!(legacy, built_in_manifest(BuiltInStyle::DeclarativeGraph));
    let legacy_compiled = compile_style(&legacy, &context(), StyleCompilerLimits::default())
        .expect("legacy declarative graph compiles");
    assert!(legacy_compiled.graph.variables.is_empty());
    assert_eq!(
        legacy_compiled.cache_key.style_content_hash.to_hex(),
        "4771b38f94121592175bff6ff9766f725e6a72efd493bd43e0ee3168ea4034b7"
    );
    assert_eq!(
        legacy_compiled.cache_key.combined_hash.to_hex(),
        "579af94d1a645cbeccabf59b174232fa874deeef94c6a4311e9e087202a266cf"
    );
    assert_eq!(
        legacy_compiled
            .graph
            .nodes
            .iter()
            .find(|node| node.id == "tool")
            .and_then(|node| node.configuration.clone()),
        Some(NodeConfiguration::ToolExecution {
            arguments: NodeValueSource::Static {
                value: serde_json::json!({}),
            },
        })
    );

    let manifest = built_in_manifest_for_version(BuiltInStyle::DeclarativeGraph, "1.2.0")
        .expect("exact current declarative-graph version");
    assert_eq!(manifest, built_in_manifest(BuiltInStyle::DeclarativeGraph));

    let first = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect("first deterministic compile");
    let second = compile_style(&manifest, &context(), StyleCompilerLimits::default())
        .expect("second deterministic compile");
    assert_eq!(first, second);
    assert_eq!(first.style_version, "1.2.0");
    assert_eq!(
        first.cache_key.style_content_hash.to_hex(),
        "56ef6bb3df9e70f3b6239ae31b331fb19c3f55fdf58ebefb462f1d9ba7745d54"
    );
    assert_eq!(
        first.cache_key.combined_hash.to_hex(),
        "163c91ce942603d3b067053371e2e88fe085a1cdbe1659bf658ff1d349a7b24d"
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
    let compiled = compile_style(
        &manifest,
        &current_builtin_context(),
        StyleCompilerLimits::default(),
    )
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
configuration = { type = "parallel_branch", max_parallelism = 2, max_queue_depth = 2, join_target = "join", join_policy = "all" }
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
configuration = { type = "join_results", required = ["left-result", "right-result"], minimum_successes = 2, failure_policy = "wait_required", ordering_policy = "member_id", timeout_ms = 1000, cancellation_propagates = true, result_projection = "node_references", artifact_collection = "none" }
[[nodes]]
id = "done"
kind = "complete_session"
[[edges]]
from = "parallel"
to = "left"
label = "left-result"
[[edges]]
from = "parallel"
to = "right"
label = "right-result"
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
    let runtime = current_builtin_context();
    let baseline = compile_style(&manifest, &runtime, StyleCompilerLimits::default())
        .expect("baseline")
        .cache_key;

    let mut changed_manifest = manifest.clone();
    changed_manifest.budgets.max_steps += 1;
    assert_ne!(
        baseline.combined_hash,
        compile_style(&changed_manifest, &runtime, StyleCompilerLimits::default())
            .expect("changed style")
            .cache_key
            .combined_hash
    );

    let mut changed_manifest = manifest.clone();
    changed_manifest.memory.query.include_style_context =
        !changed_manifest.memory.query.include_style_context;
    assert_ne!(
        baseline.combined_hash,
        compile_style(&changed_manifest, &runtime, StyleCompilerLimits::default())
            .expect("changed memory query construction")
            .cache_key
            .combined_hash
    );

    let mut changed = runtime.clone();
    changed.plugin_set_hash = ContentHash::digest(b"other plugins");
    assert_ne!(
        baseline.combined_hash,
        compile_style(&manifest, &changed, StyleCompilerLimits::default())
            .expect("changed plugins")
            .cache_key
            .combined_hash
    );
    let mut changed = runtime.clone();
    changed.runtime_api_version = "1.1.0".to_owned();
    assert_ne!(
        baseline.combined_hash,
        compile_style(&manifest, &changed, StyleCompilerLimits::default())
            .expect("changed runtime")
            .cache_key
            .combined_hash
    );
    let mut changed = runtime;
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
    let runtime = current_builtin_context();
    let compiled = compile_style(&manifest, &runtime, StyleCompilerLimits::default())
        .expect("harness selection compiles");
    assert_eq!(compiled.harness, manifest.harness);

    manifest.harness.id = String::from("../fixture");
    manifest.harness.required_capabilities = vec![String::from("not valid")];
    let error = compile_style(&manifest, &runtime, StyleCompilerLimits::default())
        .expect_err("unsafe harness identifiers");
    assert!(codes(&error).contains(&"STYLE030"));
    assert!(codes(&error).contains(&"STYLE031"));
}

#[test]
fn ordered_context_transform_selection_is_exact_cache_bound_and_restart_stable() {
    let first_hash = ContentHash::digest(b"redact declaration");
    let second_hash = ContentHash::digest(b"annotate declaration");
    let mut manifest = built_in_manifest(BuiltInStyle::PersistentChat);
    manifest.allowed_plugins.extend([
        String::from("fixture.redactor"),
        String::from("fixture.annotator"),
    ]);
    manifest.context_transforms = vec![
        context_transform_selection(
            "fixture.redactor",
            "fixture.redact",
            "1.2.3",
            first_hash,
            b"redact config",
        ),
        context_transform_selection(
            "fixture.annotator",
            "fixture.annotate",
            "2.0.0",
            second_hash,
            b"annotate config",
        ),
    ];
    let mut runtime = current_builtin_context();
    runtime.plugins.extend([
        String::from("fixture.redactor"),
        String::from("fixture.annotator"),
    ]);
    runtime.context_transforms = vec![
        AvailableContextTransform {
            plugin_id: String::from("fixture.redactor"),
            transform_id: String::from("fixture.redact"),
            version: String::from("1.2.3"),
            declaration_hash: first_hash,
            lifecycle: ContextTransformLifecycle::BeforeModelRequest,
        },
        AvailableContextTransform {
            plugin_id: String::from("fixture.annotator"),
            transform_id: String::from("fixture.annotate"),
            version: String::from("2.0.0"),
            declaration_hash: second_hash,
            lifecycle: ContextTransformLifecycle::BeforeModelRequest,
        },
    ];

    let without_transforms = {
        let mut base = manifest.clone();
        base.context_transforms.clear();
        compile_style(&base, &runtime, StyleCompilerLimits::default()).expect("base style")
    };
    let compiled = compile_style(&manifest, &runtime, StyleCompilerLimits::default())
        .expect("exact ordered transforms");
    assert_eq!(compiled.context_transforms, manifest.context_transforms);
    assert_eq!(
        parse_toml(&to_toml(&manifest).expect("transform TOML")).expect("transform TOML roundtrip"),
        manifest
    );
    assert_ne!(
        compiled.cache_key.combined_hash,
        without_transforms.cache_key.combined_hash
    );
    let reordered = {
        let mut reordered = manifest.clone();
        reordered.context_transforms.reverse();
        compile_style(&reordered, &runtime, StyleCompilerLimits::default())
            .expect("reordered exact transforms")
    };
    assert_ne!(
        reordered.cache_key.combined_hash, compiled.cache_key.combined_hash,
        "execution order is part of immutable cache identity"
    );

    let cached = serde_json::to_string(&compiled).expect("compiled cache");
    let restarted: CompiledSessionStyle =
        serde_json::from_str(&cached).expect("restart reloads exact selection");
    assert_eq!(restarted.context_transforms, manifest.context_transforms);
    assert_eq!(restarted.cache_key, compiled.cache_key);
}

#[test]
fn context_transform_version_hash_availability_and_allowlist_fail_closed() {
    let declaration_hash = ContentHash::digest(b"exact declaration");
    let mut runtime = current_builtin_context();
    runtime.plugins.insert(String::from("fixture.redactor"));
    runtime.context_transforms = vec![AvailableContextTransform {
        plugin_id: String::from("fixture.redactor"),
        transform_id: String::from("fixture.redact"),
        version: String::from("1.2.3"),
        declaration_hash,
        lifecycle: ContextTransformLifecycle::BeforeModelRequest,
    }];
    let exact = context_transform_selection(
        "fixture.redactor",
        "fixture.redact",
        "1.2.3",
        declaration_hash,
        b"configuration",
    );

    let mut not_allowed = built_in_manifest(BuiltInStyle::PersistentChat);
    not_allowed.context_transforms = vec![exact.clone()];
    let error = compile_style(&not_allowed, &runtime, StyleCompilerLimits::default())
        .expect_err("plugin allowlist is mandatory");
    assert!(codes(&error).contains(&"STYLE034"));

    let mut selected = not_allowed;
    selected
        .allowed_plugins
        .push(String::from("fixture.redactor"));
    for substituted in [
        {
            let mut value = exact.clone();
            value.version = String::from("1.2.4");
            value
        },
        {
            let mut value = exact.clone();
            value.declaration_hash = ContentHash::digest(b"substituted declaration");
            value
        },
    ] {
        selected.context_transforms = vec![substituted];
        let error = compile_style(&selected, &runtime, StyleCompilerLimits::default())
            .expect_err("compatible-looking substitution is prohibited");
        assert!(codes(&error).contains(&"STYLE035"));
    }

    selected.context_transforms = vec![exact];
    runtime.context_transforms.clear();
    let error = compile_style(&selected, &runtime, StyleCompilerLimits::default())
        .expect_err("unavailable exact transform is prohibited");
    assert!(codes(&error).contains(&"STYLE035"));
}

#[test]
fn context_transform_selection_enforces_uniqueness_bounds_and_lifecycle_encoding() {
    let declaration_hash = ContentHash::digest(b"declaration");
    let selection = context_transform_selection(
        "runtime.security",
        "fixture.redact",
        "1.0.0",
        declaration_hash,
        b"configuration",
    );
    let mut manifest = built_in_manifest(BuiltInStyle::PersistentChat);
    manifest.context_transforms = vec![selection.clone(), selection.clone()];
    let mut runtime = current_builtin_context();
    runtime.context_transforms = vec![AvailableContextTransform {
        plugin_id: selection.plugin_id.clone(),
        transform_id: selection.transform_id.clone(),
        version: selection.version.clone(),
        declaration_hash,
        lifecycle: selection.lifecycle,
    }];
    let limits = StyleCompilerLimits {
        max_context_transforms: 1,
        ..StyleCompilerLimits::default()
    };
    let error = compile_style(&manifest, &runtime, limits)
        .expect_err("duplicates and collection overflow are rejected");
    assert!(codes(&error).contains(&"STYLE009"));
    assert!(codes(&error).contains(&"STYLE033"));

    let mut encoded = serde_json::to_value(&manifest).expect("manifest JSON");
    encoded["context_transforms"][0]["lifecycle"] =
        serde_json::Value::String(String::from("after_model_request"));
    assert!(
        parse_json(&serde_json::to_string(&encoded).expect("encoded manifest")).is_err(),
        "unsupported lifecycle must fail strict manifest parsing"
    );
}

#[test]
fn exact_plugin_memory_and_compactor_selections_compile_round_trip_and_restart() {
    let (manifest, runtime) = plugin_context_style();
    let compiled = compile_style(&manifest, &runtime, StyleCompilerLimits::default())
        .expect("exact plugin context selections");
    assert_eq!(compiled.memory.plugin, manifest.memory.plugin);
    assert_eq!(compiled.compaction.plugin, manifest.compaction.plugin);
    assert_eq!(compiled.compaction.strategy, CompactionStrategy::Plugin);

    let json = to_json(&manifest).expect("plugin context JSON");
    let toml = to_toml(&manifest).expect("plugin context TOML");
    assert_eq!(parse_json(&json).expect("plugin JSON roundtrip"), manifest);
    assert_eq!(parse_toml(&toml).expect("plugin TOML roundtrip"), manifest);

    let cached = serde_json::to_string(&compiled).expect("compiled plugin context");
    let restarted: CompiledSessionStyle =
        serde_json::from_str(&cached).expect("reload exact compiled plugin context");
    assert_eq!(restarted.memory.plugin, manifest.memory.plugin);
    assert_eq!(restarted.compaction.plugin, manifest.compaction.plugin);
    assert_eq!(restarted.cache_key, compiled.cache_key);
}

#[test]
fn plugin_memory_resolution_rejects_drift_ambiguity_and_forbidden_plugins_deterministically() {
    let (manifest, mut runtime) = plugin_context_style();

    let mut forbidden = manifest.clone();
    forbidden
        .allowed_plugins
        .retain(|plugin| plugin != "fixture.context");
    let first = compile_style(&forbidden, &runtime, StyleCompilerLimits::default())
        .expect_err("plugin allowlist is mandatory");
    let second = compile_style(&forbidden, &runtime, StyleCompilerLimits::default())
        .expect_err("diagnostics remain stable");
    assert_eq!(first.diagnostics(), second.diagnostics());
    assert!(codes(&first).contains(&"STYLE037"));
    assert!(codes(&first).contains(&"STYLE042"));

    let mut drifted = manifest.clone();
    drifted
        .memory
        .plugin
        .as_mut()
        .expect("plugin memory")
        .declaration_hash = ContentHash::digest(b"compatible-looking replacement");
    let error = compile_style(&drifted, &runtime, StyleCompilerLimits::default())
        .expect_err("declaration drift cannot substitute");
    assert!(codes(&error).contains(&"STYLE038"));

    let mut configuration_drift = runtime.clone();
    configuration_drift.plugin_memory_providers[0].configuration_reference =
        ContentHash::digest(b"replacement memory configuration");
    let error = compile_style(
        &manifest,
        &configuration_drift,
        StyleCompilerLimits::default(),
    )
    .expect_err("memory configuration drift cannot substitute on creation or restart");
    assert!(codes(&error).contains(&"STYLE038"));

    runtime
        .plugin_memory_providers
        .push(runtime.plugin_memory_providers[0].clone());
    let error = compile_style(&manifest, &runtime, StyleCompilerLimits::default())
        .expect_err("duplicate exact declarations are ambiguous");
    assert!(codes(&error).contains(&"STYLE038"));
}

#[test]
fn plugin_compactor_resolution_rejects_version_drift_and_ambiguity() {
    let (manifest, mut runtime) = plugin_context_style();
    let mut drifted = manifest.clone();
    drifted
        .compaction
        .plugin
        .as_mut()
        .expect("plugin compactor")
        .compactor_version = String::from("3.1.1");
    let error = compile_style(&drifted, &runtime, StyleCompilerLimits::default())
        .expect_err("compatible compactor version substitution is prohibited");
    assert!(codes(&error).contains(&"STYLE043"));

    let mut configuration_drift = runtime.clone();
    configuration_drift.plugin_compactors[0].configuration_reference =
        ContentHash::digest(b"replacement compactor configuration");
    let error = compile_style(
        &manifest,
        &configuration_drift,
        StyleCompilerLimits::default(),
    )
    .expect_err("compactor configuration drift cannot substitute on creation or restart");
    assert!(codes(&error).contains(&"STYLE043"));

    runtime
        .plugin_compactors
        .push(runtime.plugin_compactors[0].clone());
    let error = compile_style(&manifest, &runtime, StyleCompilerLimits::default())
        .expect_err("duplicate exact compactors are ambiguous");
    assert!(codes(&error).contains(&"STYLE043"));
}

#[test]
fn plugin_memory_lifecycle_requires_declared_retrieve_and_write_operations() {
    let (manifest, mut runtime) = plugin_context_style();
    runtime.plugin_memory_providers[0].has_retrieve = false;
    let error = compile_style(&manifest, &runtime, StyleCompilerLimits::default())
        .expect_err("retrieval policy requires retrieve operation");
    assert!(codes(&error).contains(&"STYLE039"));

    runtime.plugin_memory_providers[0].has_retrieve = true;
    runtime.plugin_memory_providers[0].has_write = false;
    let error = compile_style(&manifest, &runtime, StyleCompilerLimits::default())
        .expect_err("write policy requires write operation");
    assert!(codes(&error).contains(&"STYLE039"));

    let mut read_only = manifest;
    read_only.memory.write_policy = MemoryWritePolicy::Never;
    compile_style(&read_only, &runtime, StyleCompilerLimits::default())
        .expect("read-only lifecycle does not require write");
}

#[test]
fn plugin_selection_shape_must_match_normal_memory_and_compaction_selection() {
    let (manifest, runtime) = plugin_context_style();

    let mut mismatched_memory = manifest.clone();
    mismatched_memory.memory.provider = String::from("fixture.other-memory");
    let error = compile_style(&mismatched_memory, &runtime, StyleCompilerLimits::default())
        .expect_err("normal provider and exact selection must agree");
    assert!(codes(&error).contains(&"STYLE036"));

    let mut missing_compactor = manifest.clone();
    missing_compactor.compaction.plugin = None;
    let error = compile_style(&missing_compactor, &runtime, StyleCompilerLimits::default())
        .expect_err("plugin strategy requires exact compactor");
    assert!(codes(&error).contains(&"STYLE040"));

    let mut stale_compactor = manifest;
    stale_compactor.compaction.strategy = CompactionStrategy::Summary;
    let error = compile_style(&stale_compactor, &runtime, StyleCompilerLimits::default())
        .expect_err("built-in strategy cannot retain plugin selection");
    assert!(codes(&error).contains(&"STYLE040"));
}

#[test]
fn legacy_manifests_and_builtins_default_plugin_context_selections_to_none() {
    let from_toml = parse_toml(GOLDEN_TOML).expect("legacy TOML");
    let from_json = parse_json(GOLDEN_JSON).expect("legacy JSON");
    assert_eq!(from_toml.memory.plugin, None);
    assert_eq!(from_toml.compaction.plugin, None);
    assert_eq!(from_json.memory.plugin, None);
    assert_eq!(from_json.compaction.plugin, None);

    for semantic in [
        BuiltInStyle::PersistentChat,
        BuiltInStyle::EphemeralTurn,
        BuiltInStyle::ResearchLoop,
        BuiltInStyle::PlannerWorker,
        BuiltInStyle::DeclarativeGraph,
    ] {
        let manifest = built_in_manifest(semantic);
        assert_eq!(manifest.memory.plugin, None);
        assert_eq!(manifest.compaction.plugin, None);
    }
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
