//! Typed consequential-action proposals and digest binding.

use agentmod_primitives::ContentHash;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Runtime-logic proposal identifier.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ProposalId(pub String);

/// Structured filesystem write proposal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FilesystemWriteAction {
    /// Normalized workspace-relative or approved-root-relative path.
    pub path: String,
    /// Hash expected before mutation.
    pub expected_hash: Option<ContentHash>,
    /// Hash of proposed content; content itself may live in an artifact.
    pub content_hash: ContentHash,
    /// Whether an existing target may be replaced.
    pub overwrite: bool,
}

/// Structured process start proposal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessStartAction {
    /// Executable after dependency-independent normalization.
    pub executable: String,
    /// Exact argument vector.
    pub arguments: Vec<String>,
    /// Normalized working-directory selection.
    pub working_directory: String,
    /// Environment variable names only; secret values remain references.
    pub environment_names: Vec<String>,
    /// Secret reference names requested by the process.
    pub secret_references: Vec<String>,
}

/// Structured network request proposal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HttpRequestAction {
    /// Uppercase HTTP method.
    pub method: String,
    /// Normalized URL.
    pub url: String,
    /// Header names; sensitive values remain dependency-side references.
    pub header_names: Vec<String>,
    /// Optional body artifact/hash.
    pub body_hash: Option<ContentHash>,
}

/// Structured provider request proposal.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ModelRequestAction {
    /// Provider adapter ID.
    pub provider: String,
    /// Provider model ID.
    pub model: String,
    /// Approved conversation projection content hash.
    pub projection_hash: ContentHash,
    /// Provider-specific options already constrained by runtime configuration.
    pub options: Value,
}

/// Structured tool call proposal.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ToolCallAction {
    /// Stable tool ID.
    pub tool: String,
    /// Capability group.
    pub group: String,
    /// Validated arguments.
    pub arguments: Value,
    /// Optional originating plugin/MCP server.
    pub source: Option<String>,
}

/// Structured immutable artifact persistence proposal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactPersistenceAction {
    /// Hash of the exact bytes to persist.
    pub content_hash: ContentHash,
    /// Valid media type.
    pub mime_type: String,
    /// Exact byte count.
    pub byte_size: u64,
    /// Stable retention selection.
    pub retention: String,
}

/// Every consequential runtime action class.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub enum ConsequentialAction {
    /// Construct provider-visible context.
    ContextConstruction {
        /// Stable context strategy.
        strategy: String,
    },
    /// Replace provider-visible structured context.
    ContextReplacement {
        /// Replacement projection hash.
        projection_hash: ContentHash,
    },
    /// Call a model provider.
    ModelRequest(ModelRequestAction),
    /// Retry a failed model request.
    ModelRetry {
        /// Original proposal.
        original_proposal: ProposalId,
        /// Retry attempt.
        attempt: u32,
    },
    /// Change provider/model.
    ProviderSwitch {
        /// Provider adapter ID.
        provider: String,
        /// Model ID.
        model: String,
    },
    /// Execute a normalized tool call.
    ToolCall(ToolCallAction),
    /// Start a supervised process.
    ProcessStart(ProcessStartAction),
    /// Send bytes to a supervised process.
    ProcessInput {
        /// Runtime process ID.
        process_id: String,
        /// Hash of exact bytes.
        input_hash: ContentHash,
    },
    /// Write/replace a file.
    FilesystemWrite(FilesystemWriteAction),
    /// Execute an HTTP operation.
    HttpRequest(HttpRequestAction),
    /// Execute web search.
    WebSearch {
        /// Search query.
        query: String,
        /// Result bound.
        result_count: u32,
    },
    /// Persist a memory record.
    MemoryWrite {
        /// Memory scope.
        scope: String,
        /// Hash of bounded content.
        content_hash: ContentHash,
    },
    /// Persist approved bytes as an immutable content-addressed artifact.
    ArtifactPersistence(ArtifactPersistenceAction),
    /// Compact provider context.
    Compaction {
        /// Stable strategy.
        strategy: String,
    },
    /// Create a child session.
    ChildAgentCreation {
        /// Child style.
        style: String,
        /// Workspace mode.
        workspace_mode: String,
        /// Token budget.
        token_budget: u64,
    },
    /// Change plugin activation/configuration/state.
    PluginStateChange {
        /// Plugin ID.
        plugin: String,
        /// Stable operation.
        operation: String,
    },
    /// Resume a durable continuation.
    ContinuationResume {
        /// Continuation rendered at this logic boundary.
        continuation: String,
    },
    /// Create a durable schedule.
    ScheduleCreation {
        /// Stable schedule expression.
        schedule: String,
        /// Session style.
        style: String,
    },
    /// Restore a Git/runtime checkpoint.
    CheckpointRestoration {
        /// Checkpoint ID.
        checkpoint: String,
    },
}

impl ConsequentialAction {
    /// Stable action type used by permission matching and event names.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::ContextConstruction { .. } => "context_construction",
            Self::ContextReplacement { .. } => "context_replacement",
            Self::ModelRequest(_) => "model_request",
            Self::ModelRetry { .. } => "model_retry",
            Self::ProviderSwitch { .. } => "provider_switch",
            Self::ToolCall(_) => "tool_call",
            Self::ProcessStart(_) => "process_start",
            Self::ProcessInput { .. } => "process_input",
            Self::FilesystemWrite(_) => "filesystem_write",
            Self::HttpRequest(_) => "http_request",
            Self::WebSearch { .. } => "web_search",
            Self::MemoryWrite { .. } => "memory_write",
            Self::ArtifactPersistence(_) => "artifact_persistence",
            Self::Compaction { .. } => "compaction",
            Self::ChildAgentCreation { .. } => "child_agent_creation",
            Self::PluginStateChange { .. } => "plugin_state_change",
            Self::ContinuationResume { .. } => "continuation_resume",
            Self::ScheduleCreation { .. } => "schedule_creation",
            Self::CheckpointRestoration { .. } => "checkpoint_restoration",
        }
    }
}

/// Original or interceptor-modified proposal.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ActionProposal {
    /// Stable proposal ID.
    pub id: ProposalId,
    /// Typed proposed side effect.
    pub action: ConsequentialAction,
    /// Explicit session style.
    pub style: String,
    /// Normalized workspace label.
    pub workspace: String,
    /// Originating subsystem/plugin, for audit and policy.
    pub origin: String,
}

impl ActionProposal {
    /// Computes the digest to which a dependency authorization grant must be bound.
    ///
    /// # Errors
    ///
    /// Returns [`ActionProposalError`] if deterministic JSON serialization fails.
    pub fn digest(&self) -> Result<ContentHash, ActionProposalError> {
        serde_json::to_vec(self)
            .map(|bytes| ContentHash::digest(&bytes))
            .map_err(|error| ActionProposalError::Serialization(error.to_string()))
    }
}

/// Proposal construction failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ActionProposalError {
    /// Typed action could not be serialized for digest binding.
    #[error("action proposal serialization failed: {0}")]
    Serialization(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proposal(path: &str) -> ActionProposal {
        ActionProposal {
            id: ProposalId("proposal-1".into()),
            action: ConsequentialAction::FilesystemWrite(FilesystemWriteAction {
                path: path.into(),
                expected_hash: None,
                content_hash: ContentHash::digest(b"content"),
                overwrite: false,
            }),
            style: "persistent-chat".into(),
            workspace: "fixture".into(),
            origin: "runtime".into(),
        }
    }

    #[test]
    fn digest_binds_exact_modified_action() {
        assert_ne!(
            proposal("safe.txt").digest().expect("digest"),
            proposal("other.txt").digest().expect("digest")
        );
        assert_eq!(
            proposal("safe.txt").digest().expect("digest"),
            proposal("safe.txt").digest().expect("digest")
        );
    }
}
