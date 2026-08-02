//! Stable bounded runtime ports for plugin-provided context behavior.
//!
//! This module owns the typed contracts and the runtime-side validation
//! guarantees for plugin memory, plugin compaction, and context transforms.
//! Task 7 adapts these ports to plugin-host transport; no transport is
//! implemented here. Plugins cannot bypass the canonical proposal/policy
//! pipeline or write canonical state directly.

use agentmod_primitives::{ArtifactId, ContentHash, TimestampMillis};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::conversation::{ConversationEntry, ConversationState, ProjectionProvenance};

/// One lifecycle boundary where a plugin context transform may run.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextTransformBoundary {
    /// Before runtime memory retrieval for this boundary.
    BeforeMemoryRetrieval,
    /// After runtime memory retrieval for this boundary.
    AfterMemoryRetrieval,
    /// Before compaction planning for this boundary.
    BeforeCompaction,
    /// After compaction planning for this boundary.
    AfterCompaction,
    /// Immediately before the provider projection is finalized.
    BeforeProviderProjection,
    /// Immediately before turn completion.
    BeforeTurnCompletion,
}

impl ContextTransformBoundary {
    /// Stable boundary label used in canonical provenance.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::BeforeMemoryRetrieval => "before_memory_retrieval",
            Self::AfterMemoryRetrieval => "after_memory_retrieval",
            Self::BeforeCompaction => "before_compaction",
            Self::AfterCompaction => "after_compaction",
            Self::BeforeProviderProjection => "before_provider_projection",
            Self::BeforeTurnCompletion => "before_turn_completion",
        }
    }
}

/// One bounded plugin memory retrieval request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginMemoryRetrieveRequest {
    /// Canonical session identifier.
    pub session_id: String,
    /// Canonical workspace label.
    pub workspace: String,
    /// Normalized scope key (`session:...`, `project:...`, or `runtime`).
    pub scope: String,
    /// Bounded natural-language query.
    pub query: String,
    /// Strict result bound.
    pub limit: usize,
}

/// One retrieved plugin memory item with full provenance.
#[derive(Clone, Debug, PartialEq)]
pub struct PluginMemoryItem {
    /// Provider-local stable reference.
    pub reference: String,
    /// Bounded content.
    pub content: String,
    /// Optional relevance score.
    pub score: Option<f64>,
    /// Original provider creation time.
    pub created_at: TimestampMillis,
    /// Byte contribution.
    pub size: u64,
}

/// Proposal for one policy-approved plugin memory write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginMemoryWriteProposal {
    /// Canonical session identifier.
    pub session_id: String,
    /// Canonical workspace label.
    pub workspace: String,
    /// Normalized scope key.
    pub scope: String,
    /// Provenance label.
    pub source: String,
    /// Hash of the exact content.
    pub content_hash: ContentHash,
    /// Exact bounded content.
    pub content: String,
    /// Canonical duplicate-prevention key.
    pub deduplication_key: Option<String>,
}

/// Receipt that the runtime accepted a write proposal for policy evaluation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginMemoryWriteProposalReceipt {
    /// Runtime-owned proposal identifier.
    pub proposal_id: String,
}

/// Commit decision for a previously proposed plugin memory write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginMemoryWriteCommit {
    /// Runtime-owned proposal identifier.
    pub proposal_id: String,
    /// Whether the mandatory pipeline approved the write.
    pub approved: bool,
}

/// Terminal plugin memory-write receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginMemoryWriteReceipt {
    /// Provider-local stable reference.
    pub reference: String,
    /// Whether the provider retained the content.
    pub retained: bool,
    /// Whether an identical canonical write was already retained.
    pub deduplicated: bool,
}

/// Plugin memory health state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginMemoryHealth {
    /// Whether the provider is usable.
    pub available: bool,
    /// Safe diagnostic label.
    pub label: String,
}

/// Hard bounds a plugin memory implementation must honor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PluginMemoryBounds {
    /// Maximum retrieved items.
    pub max_items: usize,
    /// Maximum bytes per item.
    pub max_bytes_per_item: u64,
    /// Maximum total retrieved bytes.
    pub max_total_bytes: u64,
    /// Maximum query bytes.
    pub max_query_bytes: usize,
}

/// Stable plugin memory port. Transport is Task 7's responsibility.
pub trait PluginMemoryPort: Send + Sync {
    /// Retrieves bounded memory with complete provenance.
    ///
    /// # Errors
    ///
    /// Returns [`PluginContextError`] for invalid input or provider failure.
    fn retrieve(
        &self,
        request: PluginMemoryRetrieveRequest,
    ) -> Result<Vec<PluginMemoryItem>, PluginContextError>;

    /// Proposes one write; the runtime evaluates the proposal through the
    /// normal interceptor and policy chain before commit.
    ///
    /// # Errors
    ///
    /// Returns [`PluginContextError`] for invalid input or provider failure.
    fn propose_write(
        &self,
        request: PluginMemoryWriteProposal,
    ) -> Result<PluginMemoryWriteProposalReceipt, PluginContextError>;

    /// Commits or rejects a previously proposed write.
    ///
    /// # Errors
    ///
    /// Returns [`PluginContextError`] for an unknown proposal or failure.
    fn commit_write(
        &self,
        request: PluginMemoryWriteCommit,
    ) -> Result<PluginMemoryWriteReceipt, PluginContextError>;

    /// Returns provider health.
    fn health(&self) -> PluginMemoryHealth;

    /// Returns supported scope keys in deterministic order.
    fn supported_scopes(&self) -> Vec<String>;

    /// Returns the hard bounds this provider enforces.
    fn bounds(&self) -> PluginMemoryBounds;
}

/// One plugin-proposed compaction plan.
#[derive(Clone, Debug, PartialEq)]
pub struct PluginCompactionPlan {
    /// Bounded replacement projection entries.
    pub replacement: Vec<ConversationEntry>,
    /// Complete source/method/artifact provenance.
    pub provenance: ProjectionProvenance,
    /// Immutable artifacts the plan references.
    pub artifacts: Vec<PluginArtifactReference>,
    /// Typed state the plan declares preserved.
    pub preserved_state: Vec<PluginPreservedState>,
}

/// One immutable artifact a plugin plan references.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginArtifactReference {
    /// Content-addressed artifact ID.
    pub artifact_id: ArtifactId,
    /// Exact content hash.
    pub content_hash: ContentHash,
    /// Media type.
    pub mime_type: String,
    /// Safe label.
    pub label: String,
    /// Inclusive source range the artifact captures.
    pub source_range: Option<(u64, u64)>,
}

/// Typed runtime state a plugin plan declares preserved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginPreservedState {
    /// Stable requirement label (`pending_control_state`, `current_input`, ...).
    pub requirement: String,
    /// Whether the plan retains it.
    pub retained: bool,
}

/// Plugin-proposed compaction request.
#[derive(Clone, Debug, PartialEq)]
pub struct PluginCompactionProposal {
    /// Canonical session identifier.
    pub session_id: String,
    /// Canonical workspace label.
    pub workspace: String,
    /// Current provider projection the plugin may replace.
    pub projection: Vec<ConversationEntry>,
    /// Inclusive source event range of the projection.
    pub source_range: Option<(u64, u64)>,
    /// Hash of the exact serialized projection.
    pub source_hash: ContentHash,
    /// Effective provider-projection token bound.
    pub projection_token_limit: Option<u64>,
    /// Hard serialized-byte bound.
    pub serialized_byte_limit: u64,
}

/// Stable plugin compaction port.
pub trait PluginCompactionPort: Send + Sync {
    /// Proposes a bounded replacement projection.
    ///
    /// # Errors
    ///
    /// Returns [`PluginContextError`] when the plan violates runtime bounds.
    fn propose_replacement_projection(
        &self,
        request: PluginCompactionProposal,
    ) -> Result<PluginCompactionPlan, PluginContextError>;

    /// Reports the exact source range and hash the provider is compacting.
    ///
    /// # Errors
    ///
    /// Returns [`PluginContextError`] when no compacted range is active.
    fn report_source_range_hash(
        &self,
    ) -> Result<Option<(u64, u64, ContentHash)>, PluginContextError>;

    /// Provides immutable artifacts for an optional source range.
    ///
    /// # Errors
    ///
    /// Returns [`PluginContextError`] when the range is unavailable.
    fn provide_artifacts(
        &self,
        range: Option<(u64, u64)>,
    ) -> Result<Vec<PluginArtifactReference>, PluginContextError>;

    /// Declares the typed runtime state the plan preserves.
    fn declare_preserved_state(&self, plan: &PluginCompactionPlan) -> Vec<PluginPreservedState>;
}

/// One bounded plugin context transform result.
#[derive(Clone, Debug, PartialEq)]
pub struct ContextTransformResult {
    /// Replacement provider projection, identical when unchanged.
    pub projection: Vec<ConversationEntry>,
    /// Transform boundary that produced this result.
    pub boundary: ContextTransformBoundary,
    /// Stable transform identity.
    pub transform_id: String,
}

/// Runtime-owned validation of one plugin context effect.
///
/// Plugins cannot mutate canonical history, change session/style/workspace
/// identity, expose undeclared secrets, remove required pending state,
/// exceed context limits, or fabricate roles.
///
/// # Errors
///
/// Returns [`PluginContextError`] for any of those violations.
#[allow(clippy::too_many_arguments)]
pub fn validate_plugin_context_effect(
    session_id: &str,
    workspace: &str,
    style: &str,
    state: &ConversationState,
    proposed: &[ConversationEntry],
    preservation_requirements: &[String],
    byte_limit: u64,
    token_limit: Option<u64>,
    declared_secrets: &[String],
) -> Result<(), PluginContextError> {
    if session_id.trim().is_empty() || workspace.trim().is_empty() || style.trim().is_empty() {
        return Err(PluginContextError::IdentityChange);
    }
    let existing_ids = state
        .provider_projection()
        .iter()
        .map(ConversationEntry::id)
        .collect::<std::collections::BTreeSet<_>>();
    let mut seen = std::collections::BTreeSet::new();
    for entry in proposed {
        if !seen.insert(entry.id()) {
            return Err(PluginContextError::DuplicateEntry(entry.id().0.clone()));
        }
        // Plugins may retain existing typed entries but never fabricate roles.
        if matches!(
            entry,
            ConversationEntry::UserMessage(_) | ConversationEntry::UserInstruction(_)
        ) && !existing_ids.contains(entry.id())
        {
            return Err(PluginContextError::RoleFabrication(entry.id().0.clone()));
        }
        if entry_contains_declared_secret(entry, declared_secrets) {
            return Err(PluginContextError::UndeclaredSecret);
        }
    }
    // Required pending state must survive any replacement.
    for requirement in preservation_requirements {
        let missing = state.provider_projection().iter().any(|entry| {
            required_entry_for(entry, requirement)
                && !proposed
                    .iter()
                    .any(|candidate| candidate.id() == entry.id())
        });
        if missing {
            return Err(PluginContextError::RequiredStateRemoved(
                requirement.clone(),
            ));
        }
    }
    let serialized = serde_json::to_vec(proposed).map_err(|_| PluginContextError::InvalidPlan)?;
    if serialized.len() as u64 > byte_limit {
        return Err(PluginContextError::ContextLimitExceeded {
            serialized_bytes: serialized.len() as u64,
            limit: byte_limit,
        });
    }
    if token_limit.is_some_and(|limit| (serialized.len() as u64 / 4) > limit) {
        return Err(PluginContextError::ContextLimitExceeded {
            serialized_bytes: serialized.len() as u64,
            limit: byte_limit,
        });
    }
    Ok(())
}

/// Validates one proposed plugin memory write before policy evaluation.
///
/// # Errors
///
/// Returns [`PluginContextError`] when the write is invalid, oversized, or
/// exposes an undeclared secret.
#[allow(clippy::too_many_arguments)]
pub fn validate_plugin_memory_write(
    request: &PluginMemoryWriteProposal,
    declared_secrets: &[String],
    max_bytes: u64,
) -> Result<(), PluginContextError> {
    if request.session_id.trim().is_empty()
        || request.workspace.trim().is_empty()
        || request.scope.trim().is_empty()
        || request.source.trim().is_empty()
        || request.content.trim().is_empty()
        || request.content_hash != ContentHash::digest(request.content.as_bytes())
    {
        return Err(PluginContextError::InvalidWrite);
    }
    if request.content.len() as u64 > max_bytes {
        return Err(PluginContextError::WriteTooLarge {
            bytes: request.content.len() as u64,
            limit: max_bytes,
        });
    }
    for secret in declared_secrets {
        if !secret.trim().is_empty() && request.content.contains(secret) {
            return Err(PluginContextError::UndeclaredSecret);
        }
    }
    Ok(())
}

fn entry_contains_declared_secret(entry: &ConversationEntry, secrets: &[String]) -> bool {
    let text = match entry {
        ConversationEntry::SystemInstruction(entry)
        | ConversationEntry::UserMessage(entry)
        | ConversationEntry::AssistantMessage(entry)
        | ConversationEntry::ProjectInstruction(entry)
        | ConversationEntry::UserInstruction(entry)
        | ConversationEntry::RuntimeAnnotation(entry) => &entry.text,
        ConversationEntry::ToolResult(result) => &result.content,
        ConversationEntry::RetrievedMemory(memory) => &memory.content,
        ConversationEntry::ContextSummary(summary) => &summary.text,
        _ => return false,
    };
    secrets
        .iter()
        .any(|secret| !secret.trim().is_empty() && text.contains(secret))
}

fn required_entry_for(entry: &ConversationEntry, requirement: &str) -> bool {
    match requirement {
        "system_instructions" => matches!(
            entry,
            ConversationEntry::SystemInstruction(_)
                | ConversationEntry::ProjectInstruction(_)
                | ConversationEntry::UserInstruction(_)
        ),
        "current_input" => matches!(entry, ConversationEntry::UserMessage(_)),
        "pending_control_state" => matches!(
            entry,
            ConversationEntry::PendingTask(_)
                | ConversationEntry::ActiveProcessSummary(_)
                | ConversationEntry::ChildAgentHandoff(_)
        ),
        "artifact_references" => matches!(
            entry,
            ConversationEntry::ArtifactReference(_)
                | ConversationEntry::Attachment(_)
                | ConversationEntry::Image(_)
        ),
        "memory_provenance" => matches!(entry, ConversationEntry::RetrievedMemory(_)),
        "active_graph_state" => matches!(entry, ConversationEntry::ProviderVisibleMetadata(_)),
        "tool_call_correlation" => matches!(
            entry,
            ConversationEntry::ToolCallRequest(_) | ConversationEntry::ToolResult(_)
        ),
        _ => false,
    }
}

/// Plugin context contract failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PluginContextError {
    /// A plugin changed canonical session/style/workspace identity.
    #[error("plugin context effect changed immutable session identity")]
    IdentityChange,
    /// A plugin fabricated a user role message.
    #[error("plugin context effect fabricated user role entry {0}")]
    RoleFabrication(String),
    /// A plugin exposed a secret not declared for its scope.
    #[error("plugin context effect exposed an undeclared secret")]
    UndeclaredSecret,
    /// A plugin removed required pending runtime state.
    #[error("plugin context effect removed required state {0}")]
    RequiredStateRemoved(String),
    /// A plugin exceeded the configured context byte/token limit.
    #[error(
        "plugin context effect exceeds the context limit ({serialized_bytes} bytes > {limit} bound)"
    )]
    ContextLimitExceeded {
        /// Serialized replacement bytes.
        serialized_bytes: u64,
        /// Configured bound.
        limit: u64,
    },
    /// A plugin produced duplicate projection entries.
    #[error("plugin context effect produced duplicate entry {0}")]
    DuplicateEntry(String),
    /// A plugin produced an invalid replacement plan.
    #[error("plugin context effect produced an invalid replacement plan")]
    InvalidPlan,
    /// A plugin memory write is invalid.
    #[error("plugin memory write is invalid")]
    InvalidWrite,
    /// A plugin memory write exceeds the configured byte bound.
    #[error("plugin memory write is {bytes} bytes, exceeding the {limit} bound")]
    WriteTooLarge {
        /// Write bytes.
        bytes: u64,
        /// Configured bound.
        limit: u64,
    },
    /// Provider or transport failure.
    #[error("plugin context provider failed: {0}")]
    Provider(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::{ConversationEntryId, PendingTaskEntry, TextEntry};
    use agentmod_primitives::Sequence;

    fn sequence(value: u64) -> Sequence {
        Sequence::new(value).expect("sequence")
    }

    fn user(id: &str, at: u64, text: &str) -> ConversationEntry {
        ConversationEntry::UserMessage(TextEntry {
            id: ConversationEntryId(id.into()),
            text: text.into(),
            source_sequence: sequence(at),
        })
    }

    fn state_with(entries: Vec<ConversationEntry>) -> ConversationState {
        let mut state = ConversationState::new();
        for entry in entries {
            state.append(entry).expect("append");
        }
        state
    }

    #[test]
    fn plugin_cannot_fabricate_user_role_or_duplicate_entries() {
        let state = state_with(vec![user("u1", 1, "hello")]);
        let fabricated = vec![user("u2", 2, "plugin says")];
        assert_eq!(
            validate_plugin_context_effect(
                "s1",
                "repo",
                "style",
                &state,
                &fabricated,
                &[],
                1024,
                None,
                &[],
            ),
            Err(PluginContextError::RoleFabrication("u2".into()))
        );
        let duplicated = vec![user("u1", 1, "hello"), user("u1", 1, "hello")];
        assert_eq!(
            validate_plugin_context_effect(
                "s1",
                "repo",
                "style",
                &state,
                &duplicated,
                &[],
                1024,
                None,
                &[],
            ),
            Err(PluginContextError::DuplicateEntry("u1".into()))
        );
    }

    #[test]
    fn plugin_cannot_remove_required_pending_state_or_exceed_limits() {
        let task = ConversationEntry::PendingTask(PendingTaskEntry {
            id: ConversationEntryId("task".into()),
            task_id: "t1".into(),
            description: "finish".into(),
            state: "pending".into(),
            source_sequence: sequence(1),
        });
        let state = state_with(vec![user("u1", 1, "hello"), task.clone()]);
        let removed = vec![user("u1", 1, "hello")];
        assert_eq!(
            validate_plugin_context_effect(
                "s1",
                "repo",
                "style",
                &state,
                &removed,
                &[String::from("pending_control_state")],
                1024,
                None,
                &[],
            ),
            Err(PluginContextError::RequiredStateRemoved(
                "pending_control_state".into()
            ))
        );
        let oversized = vec![user("u1", 1, &"x".repeat(2048))];
        assert_eq!(
            validate_plugin_context_effect(
                "s1",
                "repo",
                "style",
                &state,
                &oversized,
                &[],
                1024,
                None,
                &[],
            ),
            Err(PluginContextError::ContextLimitExceeded {
                serialized_bytes: 2125,
                limit: 1024,
            })
        );
    }

    #[test]
    fn plugin_cannot_expose_undeclared_secrets_or_write_invalid_memory() {
        let state = state_with(vec![user("u1", 1, "hello")]);
        let leaking = vec![user("u1", 1, "my token is sk-abcdef")];
        assert_eq!(
            validate_plugin_context_effect(
                "s1",
                "repo",
                "style",
                &state,
                &leaking,
                &[],
                1024,
                None,
                &["sk-abcdef".into()],
            ),
            Err(PluginContextError::UndeclaredSecret)
        );
        let write = PluginMemoryWriteProposal {
            session_id: "s1".into(),
            workspace: "repo".into(),
            scope: "session:s1".into(),
            source: "plugin".into(),
            content_hash: ContentHash::digest(b"fact"),
            content: "fact".into(),
            deduplication_key: None,
        };
        assert_eq!(
            validate_plugin_memory_write(&write, &["sk-abcdef".into()], 1024),
            Ok(())
        );
        let mut bad = write.clone();
        bad.content_hash = ContentHash::digest(b"different");
        assert_eq!(
            validate_plugin_memory_write(&bad, &[], 1024),
            Err(PluginContextError::InvalidWrite)
        );
    }

    #[test]
    fn legitimate_retained_projection_passes_validation() {
        let state = state_with(vec![user("u1", 1, "hello")]);
        let retained = vec![user("u1", 1, "hello")];
        assert_eq!(
            validate_plugin_context_effect(
                "s1",
                "repo",
                "style",
                &state,
                &retained,
                &[String::from("current_input")],
                1024,
                None,
                &[],
            ),
            Ok(())
        );
    }
}
