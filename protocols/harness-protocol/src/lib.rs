//! Harness wire contracts. Runtime and harness internals must not import each other.

use agentmod_primitives::{CancellationId, ContinuationId, SessionId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Approved provider-visible conversation entry.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ProjectedEntry {
    /// System instruction.
    System {
        /// Instruction text.
        text: String,
    },
    /// User-authored content.
    User {
        /// User-authored text.
        text: String,
    },
    /// Visible assistant content.
    Assistant {
        /// Visible assistant text.
        text: String,
    },
    /// Approved tool request.
    ToolCall {
        /// Provider-independent call identifier.
        call_id: String,
        /// Stable tool name.
        tool: String,
        /// Validated tool arguments.
        arguments: Value,
    },
    /// Bounded tool result projection.
    ToolResult {
        /// Matching tool call.
        call_id: String,
        /// Bounded provider-visible result.
        content: String,
        /// Whether complete content lives in an artifact.
        truncated: bool,
    },
    /// Typed summary, never fabricated as user input.
    ContextSummary {
        /// Approved summary text.
        text: String,
        /// First source event sequence.
        source_start: u64,
        /// Last source event sequence.
        source_end: u64,
    },
    /// Provider-visible metadata approved by runtime.
    Metadata {
        /// Provider-visible metadata key.
        key: String,
        /// Approved structured value.
        value: Value,
    },
}

/// Runtime-to-harness command.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "command", content = "value", rename_all = "snake_case")]
pub enum HarnessCommand {
    /// Begin one approved provider request.
    Execute {
        /// Owning runtime session.
        session_id: SessionId,
        /// Selected provider adapter ID.
        provider: String,
        /// Selected model ID.
        model: String,
        /// Approved provider projection.
        entries: Vec<ProjectedEntry>,
        /// Provider-specific options permitted by runtime.
        options: Value,
        /// Short-lived grant bound to this request.
        authorization_grant: String,
        /// Cross-process cancellation token.
        cancellation_id: CancellationId,
    },
    /// Explicitly continue after a runtime decision.
    Continue {
        /// Pending lifecycle continuation.
        continuation_id: ContinuationId,
        /// Runtime's interceptible decision.
        decision: HarnessContinuationDecision,
    },
    /// Cancel provider generation.
    Cancel {
        /// Request to stop.
        cancellation_id: CancellationId,
    },
    /// Read harness health/capabilities.
    Health,
}

/// Decision returned only after runtime interception.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "decision", content = "value", rename_all = "snake_case")]
pub enum HarnessContinuationDecision {
    /// Continue with the current normalized state.
    Continue,
    /// Continue with replacement provider-visible entries.
    ReplaceContext {
        /// Replacement approved provider projection.
        entries: Vec<ProjectedEntry>,
    },
    /// Reject the proposed lifecycle action.
    Reject {
        /// Safe rejection reason.
        reason: String,
    },
    /// Cancel the execution.
    Cancel {
        /// Safe cancellation reason.
        reason: String,
    },
}

/// Harness-to-runtime stream event.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "event", content = "value", rename_all = "snake_case")]
pub enum HarnessEvent {
    /// Provider request began after authorization.
    Started,
    /// Visible text delta.
    TextDelta {
        /// Visible provider output fragment.
        text: String,
    },
    /// Provider tool-call delta.
    ToolCallDelta {
        /// Provider-independent call identifier.
        call_id: String,
        /// Partial tool-name fragment.
        name_fragment: String,
        /// Partial argument JSON fragment.
        arguments_fragment: String,
    },
    /// Completed tool-call proposal; execution is not authorized by this event.
    ToolCallProposed {
        /// Continuation awaiting runtime decision.
        continuation_id: ContinuationId,
        /// Provider-independent call identifier.
        call_id: String,
        /// Proposed stable tool name.
        tool: String,
        /// Decoded arguments.
        arguments: Value,
    },
    /// Normalized provider completion.
    Completed {
        /// Provider-neutral finish reason.
        finish_reason: String,
        /// Provider-reported usage.
        usage: Usage,
    },
    /// Partial output stopped by cancellation.
    Cancelled,
    /// Classified provider failure.
    Failed {
        /// Stable failure class.
        code: String,
        /// Redacted diagnostic.
        message: String,
        /// Whether policy may retry.
        retryable: bool,
    },
}

/// Provider-neutral usage metadata.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Usage {
    /// Input tokens reported by provider.
    pub input_tokens: u64,
    /// Output tokens reported by provider.
    pub output_tokens: u64,
    /// Provider-reported cache read tokens.
    pub cache_read_tokens: u64,
    /// Provider-reported cache write tokens.
    pub cache_write_tokens: u64,
}

/// One bounded harness-process reply frame.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "reply", content = "value", rename_all = "snake_case")]
pub enum HarnessReply {
    /// Health/capability projection.
    Health {
        /// Stable status.
        status: String,
        /// Ready providers.
        ready_provider_count: u32,
        /// Capabilities.
        capabilities: Vec<String>,
    },
    /// Events resulting from one command.
    Events {
        /// Ordered bounded lifecycle events.
        events: Vec<HarnessEvent>,
    },
    /// One incrementally delivered lifecycle event.
    Event {
        /// Provider event in observation order.
        event: HarnessEvent,
        /// Whether this is the final frame for the command.
        terminal: bool,
    },
    /// Redacted command failure.
    Failed {
        /// Stable class.
        code: String,
        /// Safe message.
        message: String,
        /// Retry classification.
        retryable: bool,
    },
}
