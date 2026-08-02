//! Compiles the TASK-05 context fixture styles through the SDK exactly as the
//! runtime E2E would, proving the manifest schema is accepted before the
//! process test runs.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use agentmod_session_style_sdk::{
    CompileContext, DecisionCapability, StyleCompilerLimits, compile_style, parse_toml,
};

fn context() -> CompileContext {
    CompileContext {
        runtime_api_version: String::from("1.0.0"),
        plugin_set_hash: agentmod_primitives::ContentHash::digest(b"fixture"),
        capabilities: [
            "agents",
            "approval",
            "artifacts",
            "context",
            "continuations",
            "events",
            "model",
            "scheduling",
            "tools",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect(),
        tool_groups: BTreeMap::from([(
            String::from("filesystem"),
            BTreeSet::from([String::from("filesystem.read")]),
        )]),
        providers: BTreeSet::from([
            String::from("mock"),
            String::from("deterministic-mock"),
        ]),
        plugins: BTreeSet::from([String::from("runtime.security")]),
        memory_providers: ["none", "file", "sqlite-fts"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        compaction_strategies: BTreeSet::from([
            String::from("artifact_handoff"),
            String::from("none"),
            String::from("sliding_window"),
            String::from("summary"),
            String::from("tool_output_eviction"),
        ]),
        supported_decisions: BTreeSet::from([
            DecisionCapability::Continue,
            DecisionCapability::Replace,
            DecisionCapability::Reject,
            DecisionCapability::RequireApproval,
            DecisionCapability::Defer,
            DecisionCapability::Cancel,
            DecisionCapability::Fork,
        ]),
        graph_references: BTreeMap::new(),
    }
}

#[test]
fn task05_context_fixtures_compile() {
    for name in [
        "persistent-file-summary.toml",
        "persistent-file-artifact.toml",
        "persistent-file-auto-write.toml",
    ] {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let source = std::fs::read_to_string(
            root.join("../../tests/fixtures/styles").join(name),
        )
        .expect("fixture exists");
        let manifest = parse_toml(&source).unwrap_or_else(|error| panic!("{name}: {error}"));
        let compiled = compile_style(&manifest, &context(), StyleCompilerLimits::default())
            .unwrap_or_else(|error| panic!("{name} failed to compile: {error}"));
        assert_eq!(compiled.compaction.strategy == agentmod_session_style_sdk::CompactionStrategy::Summary, name.contains("summary"));
        assert!(!compiled.memory.provider.is_empty());
    }
}
