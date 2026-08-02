//! Pure provider-projection mapping and measurement.

use agentmod_primitives::ContentHash;
use serde_json::{Map, Value, json};
use thiserror::Error;

use crate::{conversation::ConversationEntry, harness::ProviderEntry};

pub(crate) const PROVIDER_ENTRY_TOKEN_OVERHEAD: u64 = 8;
pub(crate) const APPROX_BYTES_PER_TOKEN: u64 = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProjectionMeasure {
    pub(crate) estimated_tokens: u64,
    pub(crate) serialized_bytes: u64,
    pub(crate) projection_hash: ContentHash,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ProjectionMeasureError {
    #[error("provider projection serialization failed")]
    Serialization,
    #[error("provider projection size overflowed")]
    Overflow,
}

pub(crate) fn canonical_json_bytes(value: &Value) -> Result<Vec<u8>, ProjectionMeasureError> {
    serde_json::to_vec(&canonicalize_json(value)).map_err(|_| ProjectionMeasureError::Serialization)
}

pub(crate) fn measure_projection(
    entries: &[ConversationEntry],
) -> Result<ProjectionMeasure, ProjectionMeasureError> {
    let provider_entries = project(entries);
    let bytes =
        serde_json::to_vec(&provider_entries).map_err(|_| ProjectionMeasureError::Serialization)?;
    let serialized_bytes =
        u64::try_from(bytes.len()).map_err(|_| ProjectionMeasureError::Overflow)?;
    let mut estimated_tokens = 0_u64;
    for entry in &provider_entries {
        let entry_bytes = u64::try_from(
            serde_json::to_vec(entry)
                .map_err(|_| ProjectionMeasureError::Serialization)?
                .len(),
        )
        .map_err(|_| ProjectionMeasureError::Overflow)?;
        // Provider-independent approximation: three serialized UTF-8 bytes
        // per token, rounded upward, plus explicit per-entry framing overhead.
        // It is stable and intentionally independent of provider tokenizers.
        let content_tokens = entry_bytes
            .checked_add(APPROX_BYTES_PER_TOKEN - 1)
            .ok_or(ProjectionMeasureError::Overflow)?
            / APPROX_BYTES_PER_TOKEN;
        estimated_tokens = estimated_tokens
            .checked_add(content_tokens)
            .and_then(|value| value.checked_add(PROVIDER_ENTRY_TOKEN_OVERHEAD))
            .ok_or(ProjectionMeasureError::Overflow)?;
    }
    Ok(ProjectionMeasure {
        estimated_tokens,
        serialized_bytes,
        projection_hash: ContentHash::digest(&bytes),
    })
}

pub(crate) fn project(entries: &[ConversationEntry]) -> Vec<ProviderEntry> {
    entries
        .iter()
        .map(|entry| match entry {
            ConversationEntry::SystemInstruction(value)
            | ConversationEntry::ProjectInstruction(value)
            | ConversationEntry::UserInstruction(value) => {
                ProviderEntry::System(value.text.clone())
            }
            ConversationEntry::UserMessage(value) => ProviderEntry::User(value.text.clone()),
            ConversationEntry::AssistantMessage(value) => {
                ProviderEntry::Assistant(value.text.clone())
            }
            ConversationEntry::ToolCallRequest(value) => ProviderEntry::ToolCall {
                call_id: value.call_id.clone(),
                tool: value.tool.clone(),
                arguments: value.arguments.clone(),
            },
            ConversationEntry::ToolResult(value) => ProviderEntry::ToolResult {
                call_id: value.call_id.clone(),
                content: value.content.clone(),
                truncated: value.truncated,
            },
            ConversationEntry::ContextSummary(value) => ProviderEntry::Summary {
                text: value.text.clone(),
                start: value.source_start.get(),
                end: value.source_end.get(),
            },
            ConversationEntry::ProviderVisibleMetadata(value) => ProviderEntry::Metadata {
                key: value.key.clone(),
                value: value.value.clone(),
            },
            ConversationEntry::RetrievedMemory(value) => ProviderEntry::Metadata {
                key: format!("memory:{}", value.provider),
                value: json!({
                    "scope": value.scope,
                    "source": value.source,
                    "score": value.score,
                    "content": value.content
                }),
            },
            ConversationEntry::RuntimeAnnotation(value) => ProviderEntry::Metadata {
                key: "runtime_annotation".into(),
                value: Value::String(value.text.clone()),
            },
            ConversationEntry::Attachment(value)
            | ConversationEntry::Image(value)
            | ConversationEntry::ArtifactReference(value) => ProviderEntry::Metadata {
                key: "artifact".into(),
                value: json!({
                    "id": value.artifact_id,
                    "reference": value.artifact_reference,
                    "hash": value.content_hash,
                    "mime_type": value.mime_type,
                    "label": value.label
                }),
            },
            ConversationEntry::PendingTask(value) => ProviderEntry::Metadata {
                key: "pending_task".into(),
                value: json!({
                    "id": value.task_id,
                    "description": value.description,
                    "state": value.state
                }),
            },
            ConversationEntry::ActiveProcessSummary(value) => ProviderEntry::Metadata {
                key: "active_process".into(),
                value: json!({
                    "id": value.process_id,
                    "label": value.label,
                    "state": value.state
                }),
            },
            ConversationEntry::ChildAgentHandoff(value) => ProviderEntry::Metadata {
                key: "child_agent_handoff".into(),
                value: json!({
                    "session": value.child_session,
                    "summary": value.summary,
                    "artifact_id": value.artifact_id
                }),
            },
        })
        .collect()
}

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonicalize_json).collect()),
        Value::Object(values) => {
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            let mut canonical = Map::new();
            for (key, value) in entries {
                canonical.insert(key.clone(), canonicalize_json(value));
            }
            Value::Object(canonical)
        }
        scalar => scalar.clone(),
    }
}
