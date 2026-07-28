//! Provider-independent tool-host wire contracts.

use agentmod_primitives::{ArtifactId, CancellationId, Version};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Current tool-host wire protocol version.
pub const PROTOCOL_VERSION: Version = Version::new(1, 0);

/// Tool schema advertised lazily by a host.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ToolDescriptor {
    /// Namespaced stable tool ID.
    pub id: String,
    /// Capability/security group.
    pub group: String,
    /// Safe model-visible description.
    pub description: String,
    /// JSON Schema for arguments.
    pub input_schema: Value,
    /// Decisions supported by this action.
    pub supported_decisions: Vec<String>,
}

/// Runtime request to a capability host.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "command", content = "value", rename_all = "snake_case")]
pub enum ToolHostCommand {
    /// Report available tool groups, without all schemas.
    DiscoverGroups,
    /// Load schemas for selected groups.
    DiscoverTools {
        /// Requested capability groups.
        groups: Vec<String>,
    },
    /// Execute exactly the action covered by an authorization grant.
    Execute {
        /// Runtime tool-call ID.
        call_id: String,
        /// Stable tool name.
        tool: String,
        /// Validated arguments.
        arguments: Value,
        /// Digest of the exact normalized request.
        normalized_digest: String,
        /// Short-lived authorization grant.
        authorization_grant: String,
        /// Cancellation token.
        cancellation_id: CancellationId,
    },
    /// Cancel an active tool call.
    Cancel {
        /// Request to stop.
        cancellation_id: CancellationId,
    },
    /// Report host health.
    Health,
}

/// Capability-host response or stream item.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "event", content = "value", rename_all = "snake_case")]
pub enum ToolHostEvent {
    /// Available tool groups.
    Groups {
        /// Available group names.
        groups: Vec<String>,
    },
    /// Requested tool schemas.
    Tools {
        /// Requested schemas.
        tools: Vec<ToolDescriptor>,
    },
    /// Side effect began after grant validation.
    Started {
        /// Tool-call ID.
        call_id: String,
    },
    /// Bounded progress update.
    Progress {
        /// Tool-call ID.
        call_id: String,
        /// Bounded progress description.
        message: String,
        /// Completed units, when meaningful.
        completed: Option<u64>,
        /// Total units, when known.
        total: Option<u64>,
    },
    /// Bounded output chunk.
    Output {
        /// Tool-call ID.
        call_id: String,
        /// Logical output stream.
        stream: OutputStream,
        /// Bounded output fragment.
        content: String,
    },
    /// Tool completed with bounded projection and optional full artifact.
    Completed {
        /// Tool-call ID.
        call_id: String,
        /// Bounded structured result.
        result: Value,
        /// Optional full-output artifact.
        artifact: Option<ArtifactId>,
        /// Whether the inline projection is incomplete.
        truncated: bool,
    },
    /// Tool did not complete.
    Failed {
        /// Tool-call ID.
        call_id: String,
        /// Stable failure class.
        code: String,
        /// Redacted message.
        message: String,
        /// Whether policy may retry.
        retryable: bool,
    },
    /// Cancellation reached the host.
    Cancelled {
        /// Tool-call ID.
        call_id: String,
    },
}

/// Originating output stream.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    /// Ordinary standard output or content.
    Standard,
    /// Standard error.
    Error,
}
