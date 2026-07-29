//! SDK-owned component selection transforms for per-session style bindings.

use crate::{
    CompactionStrategy, MemoryInjectionLocation, MemoryQueryConstruction, MemoryRetrievalTiming,
    MemoryScope, MemoryWritePolicy, SessionStyleManifest,
};

/// Applies a memory-provider override while retaining the style's lifecycle
/// policy whenever it already selected active memory.
pub fn select_memory_provider(manifest: &mut SessionStyleManifest, provider: &str) {
    if provider == "none" {
        manifest.memory.provider = String::from("none");
        manifest.memory.scopes.clear();
        manifest.memory.retrieval_timing = MemoryRetrievalTiming::Never;
        manifest.memory.query = MemoryQueryConstruction::default();
        manifest.memory.max_items = 0;
        manifest.memory.max_injected_bytes = 0;
        manifest.memory.write_policy = MemoryWritePolicy::Never;
        manifest.memory.injection_location = MemoryInjectionLocation::None;
        return;
    }
    if manifest.memory.provider == "none" {
        manifest.memory.scopes = vec![MemoryScope::Session];
        manifest.memory.retrieval_timing = MemoryRetrievalTiming::TurnStart;
        manifest.memory.query = MemoryQueryConstruction::default();
        manifest.memory.max_items = 32;
        manifest.memory.max_injected_bytes = 256 * 1024;
        manifest.memory.write_policy = MemoryWritePolicy::ExplicitOnly;
        manifest.memory.injection_location = MemoryInjectionLocation::BeforeCurrentInput;
    }
    provider.clone_into(&mut manifest.memory.provider);
}

/// Applies a compaction-strategy override while retaining active style limits
/// or installing conservative bounded controls when enabling compaction.
///
/// # Errors
///
/// Returns [`ComponentSelectionError`] for an identifier outside the SDK's
/// current typed strategy model.
pub fn select_compaction_strategy(
    manifest: &mut SessionStyleManifest,
    strategy: &str,
) -> Result<(), ComponentSelectionError> {
    let strategy = match strategy {
        "none" => CompactionStrategy::None,
        "sliding_window" => CompactionStrategy::SlidingWindow,
        "summary" => CompactionStrategy::Summary,
        "artifact_handoff" => CompactionStrategy::ArtifactHandoff,
        "tool_output_eviction" => CompactionStrategy::ToolOutputEviction,
        _ => {
            return Err(ComponentSelectionError::UnknownCompaction(
                strategy.to_owned(),
            ));
        }
    };
    if strategy == CompactionStrategy::None {
        manifest.compaction.strategy = strategy;
        manifest.compaction.trigger_tokens = None;
        manifest.compaction.reserved_context_tokens = 0;
        manifest.compaction.max_provider_projection_tokens = 0;
        return Ok(());
    }
    if manifest.compaction.strategy == CompactionStrategy::None {
        let maximum = manifest.budgets.max_tokens;
        manifest.compaction.trigger_tokens = Some((maximum.saturating_mul(3) / 4).max(1));
        manifest.compaction.max_provider_projection_tokens = maximum;
        manifest.compaction.reserved_context_tokens = (maximum / 16).min(32_000);
        manifest.compaction.preserve_unresolved_tasks = true;
        manifest.compaction.preserve_active_processes = true;
    }
    manifest.compaction.strategy = strategy;
    Ok(())
}

/// Invalid typed component selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ComponentSelectionError {
    /// Compaction remains a closed SDK enum until plugin strategies gain a
    /// versioned protocol representation.
    UnknownCompaction(String),
}

impl std::fmt::Display for ComponentSelectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownCompaction(strategy) => {
                write!(formatter, "unknown compaction strategy `{strategy}`")
            }
        }
    }
}

impl std::error::Error for ComponentSelectionError {}
