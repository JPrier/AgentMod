//! Canonical structured conversation state and provider projections.

use std::collections::BTreeMap;

use agentmod_primitives::{ArtifactId, ContentHash, EventId, Sequence};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Logic-local stable conversation-entry identifier.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ConversationEntryId(pub String);

/// Shared typed text content.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TextEntry {
    /// Entry ID.
    pub id: ConversationEntryId,
    /// Exact visible text.
    pub text: String,
    /// Event sequence that introduced the entry.
    pub source_sequence: Sequence,
}

/// Approved tool-call request.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ToolCallEntry {
    /// Entry ID.
    pub id: ConversationEntryId,
    /// Provider-independent call ID.
    pub call_id: String,
    /// Stable tool name.
    pub tool: String,
    /// Validated arguments.
    pub arguments: Value,
    /// Event sequence that introduced the entry.
    pub source_sequence: Sequence,
}

/// Bounded projection of a tool result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ToolResultEntry {
    /// Entry ID.
    pub id: ConversationEntryId,
    /// Matching tool-call ID.
    pub call_id: String,
    /// Bounded model-visible content.
    pub content: String,
    /// Optional full-output artifact.
    pub artifact_id: Option<ArtifactId>,
    /// Whether the bounded content omits bytes.
    pub truncated: bool,
    /// Event sequence that introduced the entry.
    pub source_sequence: Sequence,
}

/// Immutable artifact reference in conversation state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactEntry {
    /// Entry ID.
    pub id: ConversationEntryId,
    /// Artifact ID.
    pub artifact_id: ArtifactId,
    /// Hash of exact artifact bytes.
    pub content_hash: ContentHash,
    /// MIME type.
    pub mime_type: String,
    /// Safe label.
    pub label: String,
    /// Event sequence that introduced the entry.
    pub source_sequence: Sequence,
}

/// Typed context summary with immutable source provenance.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextSummaryEntry {
    /// Entry ID.
    pub id: ConversationEntryId,
    /// Summary content.
    pub text: String,
    /// Inclusive source event range.
    pub source_start: Sequence,
    /// Inclusive source event range.
    pub source_end: Sequence,
    /// Stable compaction/transformation strategy.
    pub method: String,
    /// Optional full context snapshot.
    pub artifact_id: Option<ArtifactId>,
}

/// Retrieved memory injected into a provider projection.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RetrievedMemoryEntry {
    /// Entry ID.
    pub id: ConversationEntryId,
    /// Memory provider ID.
    pub provider: String,
    /// Query used for retrieval.
    pub query: String,
    /// Session/project/user/runtime scope.
    pub scope: String,
    /// Source reference.
    pub source: String,
    /// Provider-local stable reference.
    #[serde(default)]
    pub reference: String,
    /// Optional relevance score.
    pub score: Option<f64>,
    /// Bounded content.
    pub content: String,
    /// Event sequence that injected the record.
    pub injection_sequence: Sequence,
    /// Canonical projection event that performed the injection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub injection_event: Option<EventId>,
    /// Original provider record creation time in Unix milliseconds.
    #[serde(default)]
    pub created_at_millis: i64,
    /// Byte contribution to context.
    pub size_bytes: u64,
}

/// Provider-visible structured metadata.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MetadataEntry {
    /// Entry ID.
    pub id: ConversationEntryId,
    /// Stable metadata key.
    pub key: String,
    /// Structured value.
    pub value: Value,
    /// Event sequence that introduced the entry.
    pub source_sequence: Sequence,
}

/// Pending runtime task projected to a model.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PendingTaskEntry {
    /// Entry ID.
    pub id: ConversationEntryId,
    /// Runtime task ID.
    pub task_id: String,
    /// Bounded task description.
    pub description: String,
    /// Stable task state.
    pub state: String,
    /// Event sequence that introduced or updated the task.
    pub source_sequence: Sequence,
}

/// Active supervised process projected to a model.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActiveProcessEntry {
    /// Entry ID.
    pub id: ConversationEntryId,
    /// Runtime process ID.
    pub process_id: String,
    /// Redacted command label.
    pub label: String,
    /// Stable process state.
    pub state: String,
    /// Event sequence for the projection.
    pub source_sequence: Sequence,
}

/// Child-agent handoff projected explicitly rather than as fake user input.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildHandoffEntry {
    /// Entry ID.
    pub id: ConversationEntryId,
    /// Child session identifier rendered at the logic boundary.
    pub child_session: String,
    /// Bounded handoff summary.
    pub summary: String,
    /// Optional immutable result-package reference.
    pub artifact_id: Option<String>,
    /// Event sequence that committed the handoff.
    pub source_sequence: Sequence,
}

/// Canonical structured conversation content.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", content = "content", rename_all = "snake_case")]
pub enum ConversationEntry {
    /// System-level instruction.
    SystemInstruction(TextEntry),
    /// User-authored message.
    UserMessage(TextEntry),
    /// Visible assistant message.
    AssistantMessage(TextEntry),
    /// Tool-call request.
    ToolCallRequest(ToolCallEntry),
    /// Tool result.
    ToolResult(ToolResultEntry),
    /// User/runtime attachment.
    Attachment(ArtifactEntry),
    /// Image artifact.
    Image(ArtifactEntry),
    /// Typed context summary.
    ContextSummary(ContextSummaryEntry),
    /// Retrieved memory record.
    RetrievedMemory(RetrievedMemoryEntry),
    /// Project instruction discovered through policy.
    ProjectInstruction(TextEntry),
    /// Explicit user instruction with precedence.
    UserInstruction(TextEntry),
    /// Runtime annotation that may or may not be provider-visible by projection.
    RuntimeAnnotation(TextEntry),
    /// Provider-visible metadata.
    ProviderVisibleMetadata(MetadataEntry),
    /// Generic immutable artifact reference.
    ArtifactReference(ArtifactEntry),
    /// Pending task.
    PendingTask(PendingTaskEntry),
    /// Active process summary.
    ActiveProcessSummary(ActiveProcessEntry),
    /// Explicit child-agent handoff.
    ChildAgentHandoff(ChildHandoffEntry),
}

impl ConversationEntry {
    /// Returns the stable entry ID.
    #[must_use]
    pub fn id(&self) -> &ConversationEntryId {
        match self {
            Self::SystemInstruction(entry)
            | Self::UserMessage(entry)
            | Self::AssistantMessage(entry)
            | Self::ProjectInstruction(entry)
            | Self::UserInstruction(entry)
            | Self::RuntimeAnnotation(entry) => &entry.id,
            Self::ToolCallRequest(entry) => &entry.id,
            Self::ToolResult(entry) => &entry.id,
            Self::Attachment(entry) | Self::Image(entry) | Self::ArtifactReference(entry) => {
                &entry.id
            }
            Self::ContextSummary(entry) => &entry.id,
            Self::RetrievedMemory(entry) => &entry.id,
            Self::ProviderVisibleMetadata(entry) => &entry.id,
            Self::PendingTask(entry) => &entry.id,
            Self::ActiveProcessSummary(entry) => &entry.id,
            Self::ChildAgentHandoff(entry) => &entry.id,
        }
    }
}

/// Provenance for the current provider-visible projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProjectionProvenance {
    /// Stable projection ID.
    pub projection_id: String,
    /// Source history/event range when derived from a replacement.
    pub source_range: Option<(Sequence, Sequence)>,
    /// Stable context construction or compaction method.
    pub method: String,
    /// Event sequence that committed the projection.
    pub committed_at: Sequence,
    /// Optional complete projection artifact.
    pub artifact_id: Option<ArtifactId>,
}

/// Canonical content history plus replaceable provider projection.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ConversationState {
    history: Vec<ConversationEntry>,
    provider_projection: Vec<ConversationEntry>,
    projection_provenance: Option<ProjectionProvenance>,
    entry_positions: BTreeMap<ConversationEntryId, usize>,
}

impl ConversationState {
    /// Creates empty structured state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            history: Vec::new(),
            provider_projection: Vec::new(),
            projection_provenance: None,
            entry_positions: BTreeMap::new(),
        }
    }

    /// Appends a unique canonical entry and includes it in the current projection.
    ///
    /// # Errors
    ///
    /// Returns [`ConversationError::DuplicateEntry`] for a reused entry ID.
    pub fn append(&mut self, entry: ConversationEntry) -> Result<(), ConversationError> {
        if self.entry_positions.contains_key(entry.id()) {
            return Err(ConversationError::DuplicateEntry(entry.id().0.clone()));
        }
        self.entry_positions
            .insert(entry.id().clone(), self.history.len());
        self.history.push(entry.clone());
        self.provider_projection.push(entry);
        Ok(())
    }

    /// Replaces only the provider-visible projection, preserving canonical history.
    ///
    /// # Errors
    ///
    /// Returns [`ConversationError`] when provenance is invalid or replacement entry
    /// IDs are duplicated.
    pub fn replace_projection(
        &mut self,
        replacement: Vec<ConversationEntry>,
        provenance: ProjectionProvenance,
    ) -> Result<(), ConversationError> {
        if let Some((start, end)) = provenance.source_range
            && start > end
        {
            return Err(ConversationError::InvalidSourceRange {
                start: start.get(),
                end: end.get(),
            });
        }
        let mut ids = std::collections::BTreeSet::new();
        for entry in &replacement {
            if !ids.insert(entry.id()) {
                return Err(ConversationError::DuplicateProjectionEntry(
                    entry.id().0.clone(),
                ));
            }
        }
        self.provider_projection = replacement;
        self.projection_provenance = Some(provenance);
        Ok(())
    }

    /// Immutable canonical content history.
    #[must_use]
    pub fn history(&self) -> &[ConversationEntry] {
        &self.history
    }

    /// Current approved provider-visible projection.
    #[must_use]
    pub fn provider_projection(&self) -> &[ConversationEntry] {
        &self.provider_projection
    }

    /// Current projection provenance.
    #[must_use]
    pub const fn projection_provenance(&self) -> Option<&ProjectionProvenance> {
        self.projection_provenance.as_ref()
    }
}

impl Default for ConversationState {
    fn default() -> Self {
        Self::new()
    }
}

/// Canonical conversation invariant failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ConversationError {
    /// Canonical IDs cannot be reused.
    #[error("duplicate canonical conversation entry ID: {0}")]
    DuplicateEntry(String),
    /// A provider projection cannot contain duplicate entries.
    #[error("duplicate provider projection entry ID: {0}")]
    DuplicateProjectionEntry(String),
    /// Source range is reversed.
    #[error("invalid projection source range {start}..={end}")]
    InvalidSourceRange {
        /// First source sequence.
        start: u64,
        /// Last source sequence.
        end: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(id: &str, sequence: u64, text: &str) -> ConversationEntry {
        ConversationEntry::UserMessage(TextEntry {
            id: ConversationEntryId(id.into()),
            text: text.into(),
            source_sequence: Sequence::new(sequence).expect("sequence"),
        })
    }

    #[test]
    fn replacement_preserves_original_history_without_fake_message() {
        let mut state = ConversationState::new();
        state.append(text("u1", 1, "original")).expect("append");
        let history = state.history().to_vec();
        let summary = ConversationEntry::ContextSummary(ContextSummaryEntry {
            id: ConversationEntryId("summary-1".into()),
            text: "bounded summary".into(),
            source_start: Sequence::FIRST,
            source_end: Sequence::FIRST,
            method: "summary".into(),
            artifact_id: None,
        });
        state
            .replace_projection(
                vec![summary.clone()],
                ProjectionProvenance {
                    projection_id: "projection-2".into(),
                    source_range: Some((Sequence::FIRST, Sequence::FIRST)),
                    method: "summary".into(),
                    committed_at: Sequence::new(2).expect("sequence"),
                    artifact_id: None,
                },
            )
            .expect("replace");
        assert_eq!(state.history(), history);
        assert_eq!(state.provider_projection(), [summary]);
        assert!(matches!(
            state.provider_projection()[0],
            ConversationEntry::ContextSummary(_)
        ));
    }

    #[test]
    fn duplicate_canonical_entry_is_rejected() {
        let mut state = ConversationState::new();
        state.append(text("same", 1, "one")).expect("first");
        assert_eq!(
            state.append(text("same", 2, "two")),
            Err(ConversationError::DuplicateEntry("same".into()))
        );
    }
}
