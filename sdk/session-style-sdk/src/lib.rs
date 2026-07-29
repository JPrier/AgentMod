//! Versioned session-style manifests and deterministic compilation.
//!
//! The SDK describes and validates execution styles. It does not execute
//! graphs, call providers, invoke tools, or select a top-level style.

mod builtins;
mod model;
mod parsing;
mod selection;
mod validation;

pub use builtins::{built_in_manifest, built_in_manifest_for_version, declarative_graph_manifest};
pub use model::{
    ApprovalDecision, ApprovalDefaults, BuiltInStyle, ChildAgentLimits, ChildCancellationBehavior,
    ChildJoinBehavior, ChildMemoryAccess, ChildWorkspaceMode, CompactionPreservationRequirement,
    CompactionSelection, CompactionStrategy, DecisionCapability, ExecutionBudgets, GraphSource,
    HarnessSelection, InterceptorDeclaration, MemoryInjectionLocation, MemoryQueryConstruction,
    MemoryQuerySource, MemoryRetrievalTiming, MemoryScope, MemorySelection, MemoryWritePolicy,
    RetryPolicy, SessionStyleManifest, StyleIdentity, StyleKind, TerminationOutcome,
    TerminationPolicy, TopLevelSelection,
};
pub use parsing::{ManifestFormat, ManifestParseError, parse_json, parse_toml, to_json, to_toml};
pub use selection::{ComponentSelectionError, select_compaction_strategy, select_memory_provider};
pub use validation::{
    CURRENT_STYLE_SCHEMA_VERSION, CompileContext, CompiledSessionStyle, Diagnostic,
    DiagnosticSeverity, StyleCacheKey, StyleCompileError, StyleCompilerLimits, compile_style,
    compile_style_set,
};
