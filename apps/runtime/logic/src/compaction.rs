//! Deterministic provider-projection compaction strategies.

use std::collections::BTreeSet;

use agentmod_primitives::{ArtifactId, ContentHash, Sequence};
use thiserror::Error;

use crate::conversation::{
    ArtifactEntry, ContextSummaryEntry, ConversationEntry, ConversationEntryId, ConversationState,
    ProjectionProvenance,
};

/// Logic-owned deterministic compaction selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompactionStrategy {
    /// Preserve the approved projection unchanged.
    None,
    /// Retain all protected runtime state plus a bounded number of recent entries.
    SlidingWindow {
        /// Number of most-recent entries, in addition to protected entries.
        max_recent_entries: usize,
    },
    /// Replace ordinary history with an approved typed summary.
    Summary {
        /// Unique projection-local summary ID.
        summary_id: String,
        /// Approved bounded summary text.
        summary: String,
        /// Optional immutable full-context artifact.
        artifact_id: Option<ArtifactId>,
    },
    /// Replace ordinary history with an immutable artifact handoff.
    ArtifactHandoff {
        /// Projection entry ID.
        entry_id: String,
        /// Immutable context artifact.
        artifact_id: ArtifactId,
        /// Exact artifact content hash.
        content_hash: ContentHash,
        /// Safe label.
        label: String,
    },
    /// Keep structure while replacing oversized tool-result content with artifact references.
    ToolOutputEviction {
        /// Maximum visible UTF-8 bytes per tool result.
        max_visible_bytes: usize,
    },
}

/// Metadata supplied by the committing runtime action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionContext {
    /// Unique provider projection ID.
    pub projection_id: String,
    /// Sequence reserved for the replacement commit.
    pub committed_at: Sequence,
}

/// Deterministic replacement ready to become a committed context event.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactionPlan {
    /// Structured provider-visible replacement.
    pub replacement: Vec<ConversationEntry>,
    /// Complete source/method/artifact provenance.
    pub provenance: ProjectionProvenance,
}

/// Constructs a deterministic replacement without mutating canonical history.
///
/// Summary text is supplied as already approved input; this pure function never
/// calls a model or fabricates a user message.
///
/// # Errors
///
/// Returns [`CompactionError`] for invalid bounds, empty generated identifiers,
/// empty summaries, unavailable source ranges, or unsafe tool-output eviction.
pub fn compact_projection(
    conversation: &ConversationState,
    strategy: CompactionStrategy,
    context: CompactionContext,
) -> Result<CompactionPlan, CompactionError> {
    if context.projection_id.trim().is_empty() {
        return Err(CompactionError::EmptyProjectionId);
    }
    let source = conversation.provider_projection();
    let source_range = projection_range(source);
    let (replacement, method, artifact_id) = match strategy {
        CompactionStrategy::None => (source.to_vec(), "none".to_owned(), None),
        CompactionStrategy::SlidingWindow { max_recent_entries } => {
            if max_recent_entries == 0 {
                return Err(CompactionError::ZeroWindow);
            }
            (
                sliding_window(source, max_recent_entries),
                "sliding_window".to_owned(),
                None,
            )
        }
        CompactionStrategy::Summary {
            summary_id,
            summary,
            artifact_id,
        } => {
            if summary_id.trim().is_empty() || summary.trim().is_empty() {
                return Err(CompactionError::InvalidSummary);
            }
            let (source_start, source_end) =
                source_range.ok_or(CompactionError::MissingSourceRange)?;
            let mut replacement = protected_entries(source);
            replacement.push(ConversationEntry::ContextSummary(ContextSummaryEntry {
                id: ConversationEntryId(summary_id),
                text: summary,
                source_start,
                source_end,
                method: "summary".into(),
                artifact_id,
            }));
            (replacement, "summary".to_owned(), artifact_id)
        }
        CompactionStrategy::ArtifactHandoff {
            entry_id,
            artifact_id,
            content_hash,
            label,
        } => {
            if entry_id.trim().is_empty() || label.trim().is_empty() {
                return Err(CompactionError::InvalidArtifactHandoff);
            }
            let mut replacement = protected_entries(source);
            replacement.push(ConversationEntry::ArtifactReference(ArtifactEntry {
                id: ConversationEntryId(entry_id),
                artifact_id,
                content_hash,
                mime_type: "application/vnd.agentmod.context+json".into(),
                label,
                source_sequence: context.committed_at,
            }));
            (
                replacement,
                "artifact_handoff".to_owned(),
                Some(artifact_id),
            )
        }
        CompactionStrategy::ToolOutputEviction { max_visible_bytes } => {
            if max_visible_bytes == 0 {
                return Err(CompactionError::ZeroToolOutputBound);
            }
            (
                evict_tool_outputs(source, max_visible_bytes)?,
                "tool_output_eviction".to_owned(),
                None,
            )
        }
    };
    ensure_unique_ids(&replacement)?;
    Ok(CompactionPlan {
        replacement,
        provenance: ProjectionProvenance {
            projection_id: context.projection_id,
            source_range,
            method,
            committed_at: context.committed_at,
            artifact_id,
        },
    })
}

fn sliding_window(
    source: &[ConversationEntry],
    max_recent_entries: usize,
) -> Vec<ConversationEntry> {
    let recent_start = source.len().saturating_sub(max_recent_entries);
    source
        .iter()
        .enumerate()
        .filter(|(index, entry)| *index >= recent_start || is_protected(entry))
        .map(|(_, entry)| entry.clone())
        .collect()
}

fn protected_entries(source: &[ConversationEntry]) -> Vec<ConversationEntry> {
    source
        .iter()
        .filter(|entry| is_protected(entry))
        .cloned()
        .collect()
}

fn is_protected(entry: &ConversationEntry) -> bool {
    match entry {
        ConversationEntry::SystemInstruction(_)
        | ConversationEntry::ProjectInstruction(_)
        | ConversationEntry::UserInstruction(_)
        | ConversationEntry::PendingTask(_)
        | ConversationEntry::ActiveProcessSummary(_)
        | ConversationEntry::ChildAgentHandoff(_)
        | ConversationEntry::ToolCallRequest(_) => true,
        ConversationEntry::ToolResult(result) => result.artifact_id.is_none(),
        _ => false,
    }
}

fn evict_tool_outputs(
    source: &[ConversationEntry],
    max_visible_bytes: usize,
) -> Result<Vec<ConversationEntry>, CompactionError> {
    source
        .iter()
        .cloned()
        .map(|entry| match entry {
            ConversationEntry::ToolResult(mut result)
                if result.content.len() > max_visible_bytes =>
            {
                let artifact_id = result.artifact_id.ok_or_else(|| {
                    CompactionError::ToolOutputMissingArtifact(result.id.0.clone())
                })?;
                let placeholder =
                    format!("[tool output evicted; full content is artifact {artifact_id}]");
                truncate_utf8(&placeholder, max_visible_bytes).clone_into(&mut result.content);
                result.truncated = true;
                Ok(ConversationEntry::ToolResult(result))
            }
            other => Ok(other),
        })
        .collect()
}

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut boundary = max_bytes;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    &value[..boundary]
}

fn projection_range(source: &[ConversationEntry]) -> Option<(Sequence, Sequence)> {
    source
        .iter()
        .filter_map(entry_range)
        .fold(None, |range, (start, end)| {
            Some(match range {
                None => (start, end),
                Some((minimum, maximum)) => (minimum.min(start), maximum.max(end)),
            })
        })
}

fn entry_range(entry: &ConversationEntry) -> Option<(Sequence, Sequence)> {
    let one = |sequence| Some((sequence, sequence));
    match entry {
        ConversationEntry::SystemInstruction(entry)
        | ConversationEntry::UserMessage(entry)
        | ConversationEntry::AssistantMessage(entry)
        | ConversationEntry::ProjectInstruction(entry)
        | ConversationEntry::UserInstruction(entry)
        | ConversationEntry::RuntimeAnnotation(entry) => one(entry.source_sequence),
        ConversationEntry::ToolCallRequest(entry) => one(entry.source_sequence),
        ConversationEntry::ToolResult(entry) => one(entry.source_sequence),
        ConversationEntry::Attachment(entry)
        | ConversationEntry::Image(entry)
        | ConversationEntry::ArtifactReference(entry) => one(entry.source_sequence),
        ConversationEntry::ContextSummary(entry) => Some((entry.source_start, entry.source_end)),
        ConversationEntry::RetrievedMemory(entry) => one(entry.injection_sequence),
        ConversationEntry::ProviderVisibleMetadata(entry) => one(entry.source_sequence),
        ConversationEntry::PendingTask(entry) => one(entry.source_sequence),
        ConversationEntry::ActiveProcessSummary(entry) => one(entry.source_sequence),
        ConversationEntry::ChildAgentHandoff(entry) => one(entry.source_sequence),
    }
}

fn ensure_unique_ids(replacement: &[ConversationEntry]) -> Result<(), CompactionError> {
    let mut ids = BTreeSet::new();
    for entry in replacement {
        if !ids.insert(entry.id()) {
            return Err(CompactionError::DuplicateProjectionEntry(
                entry.id().0.clone(),
            ));
        }
    }
    Ok(())
}

/// Deterministic compaction failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CompactionError {
    /// Projection commit must have a stable ID.
    #[error("compaction projection ID is empty")]
    EmptyProjectionId,
    /// Sliding window must retain at least one recent entry.
    #[error("sliding window recent-entry bound must be positive")]
    ZeroWindow,
    /// Summary ID and content must be non-empty.
    #[error("summary compaction requires a non-empty ID and summary")]
    InvalidSummary,
    /// Summary needs a source event range.
    #[error("summary compaction has no source event range")]
    MissingSourceRange,
    /// Artifact handoff metadata is incomplete.
    #[error("artifact handoff requires a non-empty entry ID and label")]
    InvalidArtifactHandoff,
    /// Tool-output visible bound must be positive.
    #[error("tool-output visible bound must be positive")]
    ZeroToolOutputBound,
    /// Large tool output cannot be discarded without its immutable full artifact.
    #[error("tool result {0:?} exceeds the bound but has no full-output artifact")]
    ToolOutputMissingArtifact(String),
    /// Generated replacement entry IDs must be unique.
    #[error("compaction generated duplicate projection entry ID: {0}")]
    DuplicateProjectionEntry(String),
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::conversation::{ActiveProcessEntry, PendingTaskEntry, TextEntry, ToolResultEntry};

    use super::*;

    fn sequence(value: u64) -> Sequence {
        Sequence::new(value).expect("sequence")
    }

    fn user(id: &str, at: u64) -> ConversationEntry {
        ConversationEntry::UserMessage(TextEntry {
            id: ConversationEntryId(id.into()),
            text: id.into(),
            source_sequence: sequence(at),
        })
    }

    fn state(entries: Vec<ConversationEntry>) -> ConversationState {
        let mut state = ConversationState::new();
        for entry in entries {
            state.append(entry).expect("unique");
        }
        state
    }

    fn context() -> CompactionContext {
        CompactionContext {
            projection_id: "projection-10".into(),
            committed_at: sequence(10),
        }
    }

    #[test]
    fn sliding_window_preserves_tasks_processes_and_history() {
        let entries = vec![
            user("old", 1),
            ConversationEntry::PendingTask(PendingTaskEntry {
                id: ConversationEntryId("task".into()),
                task_id: "t1".into(),
                description: "finish".into(),
                state: "pending".into(),
                source_sequence: sequence(2),
            }),
            ConversationEntry::ActiveProcessSummary(ActiveProcessEntry {
                id: ConversationEntryId("process".into()),
                process_id: "p1".into(),
                label: "tests".into(),
                state: "running".into(),
                source_sequence: sequence(3),
            }),
            user("new", 4),
        ];
        let state = state(entries.clone());
        let plan = compact_projection(
            &state,
            CompactionStrategy::SlidingWindow {
                max_recent_entries: 1,
            },
            context(),
        )
        .expect("compact");
        assert_eq!(state.history(), entries);
        assert_eq!(plan.replacement.len(), 3);
        assert!(plan.replacement.iter().any(|entry| entry.id().0 == "task"));
        assert!(
            plan.replacement
                .iter()
                .any(|entry| entry.id().0 == "process")
        );
        assert!(plan.replacement.iter().any(|entry| entry.id().0 == "new"));
    }

    #[test]
    fn summary_is_typed_and_never_a_user_message() {
        let state = state(vec![user("u1", 1), user("u2", 2)]);
        let plan = compact_projection(
            &state,
            CompactionStrategy::Summary {
                summary_id: "summary".into(),
                summary: "bounded findings".into(),
                artifact_id: None,
            },
            context(),
        )
        .expect("summary");
        assert!(matches!(
            plan.replacement.as_slice(),
            [ConversationEntry::ContextSummary(_)]
        ));
        assert_eq!(
            plan.provenance.source_range,
            Some((Sequence::FIRST, sequence(2)))
        );
        assert_eq!(state.history().len(), 2);
    }

    #[test]
    fn tool_output_eviction_requires_and_preserves_full_artifact() {
        let artifact_id = ArtifactId::from_uuid(Uuid::from_u128(8));
        let state = state(vec![ConversationEntry::ToolResult(ToolResultEntry {
            id: ConversationEntryId("result".into()),
            call_id: "call".into(),
            content: "a".repeat(1_000),
            artifact_id: Some(artifact_id),
            truncated: false,
            source_sequence: Sequence::FIRST,
        })]);
        let plan = compact_projection(
            &state,
            CompactionStrategy::ToolOutputEviction {
                max_visible_bytes: 32,
            },
            context(),
        )
        .expect("evict");
        let ConversationEntry::ToolResult(result) = &plan.replacement[0] else {
            panic!("tool result")
        };
        assert_eq!(result.artifact_id, Some(artifact_id));
        assert!(result.truncated);
        assert!(result.content.len() <= 32);

        let mut missing = state.clone();
        missing
            .replace_projection(
                vec![ConversationEntry::ToolResult(ToolResultEntry {
                    id: ConversationEntryId("missing".into()),
                    call_id: "call".into(),
                    content: "b".repeat(100),
                    artifact_id: None,
                    truncated: false,
                    source_sequence: Sequence::FIRST,
                })],
                ProjectionProvenance {
                    projection_id: "fixture".into(),
                    source_range: Some((Sequence::FIRST, Sequence::FIRST)),
                    method: "fixture".into(),
                    committed_at: sequence(2),
                    artifact_id: None,
                },
            )
            .expect("replace");
        assert_eq!(
            compact_projection(
                &missing,
                CompactionStrategy::ToolOutputEviction {
                    max_visible_bytes: 32
                },
                context(),
            ),
            Err(CompactionError::ToolOutputMissingArtifact("missing".into()))
        );
    }
}
