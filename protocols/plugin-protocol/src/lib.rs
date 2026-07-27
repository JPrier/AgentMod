//! Versioned wire contracts between the runtime and isolated plugin hosts.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Current plugin-host wire protocol.
pub const CURRENT_PROTOCOL_VERSION: u16 = 1;

/// Plugin execution classification.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginClass {
    /// Synchronous proposal interceptor.
    Blocking,
    /// Non-authoritative committed-event observer.
    Observer,
    /// Dynamically provided tool.
    Tool,
    /// Other declared extension category.
    Extension,
}

/// Out-of-process entrypoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginEntrypoint {
    /// Executable path or name selected by the composition root policy.
    pub program: String,
    /// Fixed launch arguments.
    #[serde(default)]
    pub arguments: Vec<String>,
}

/// Configuration-schema declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginConfigurationSchema {
    /// Stable schema ID.
    pub id: String,
    /// Positive schema version.
    pub version: u32,
    /// Whether configuration is mandatory.
    pub required: bool,
    /// Bounded inline JSON Schema object.
    pub inline_json: String,
}

/// Wire form of a plugin manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginManifest {
    /// Manifest schema version.
    pub schema_version: u16,
    /// Globally unique plugin ID.
    pub id: String,
    /// Plugin semantic version.
    pub version: String,
    /// Compatible runtime API requirement.
    pub runtime_api: String,
    /// Extension category.
    pub category: String,
    /// Invocation/model/turn/session/project/user/runtime scope.
    pub scope: String,
    /// Blocking/observer/tool/extension class.
    pub class: PluginClass,
    /// Isolated executable.
    pub entrypoint: PluginEntrypoint,
    /// Requested and provided capabilities.
    #[serde(default)]
    pub required_capabilities: BTreeSet<String>,
    /// Capabilities made available.
    #[serde(default)]
    pub provided_capabilities: BTreeSet<String>,
    /// Canonical event or proposal names.
    #[serde(default)]
    pub subscribed_events: BTreeSet<String>,
    /// Readable state scopes.
    #[serde(default)]
    pub read_authority: BTreeSet<String>,
    /// Proposed state writes; observers must leave canonical writes absent.
    #[serde(default)]
    pub proposed_write_authority: BTreeSet<String>,
    /// Allowed tool or tool-group names.
    #[serde(default)]
    pub tool_permissions: BTreeSet<String>,
    /// Allowed exact domains or wildcard subdomains.
    #[serde(default)]
    pub network_permissions: BTreeSet<String>,
    /// Stable handlers that must precede this plugin.
    #[serde(default)]
    pub after: BTreeSet<String>,
    /// Stable handlers that must follow this plugin.
    #[serde(default)]
    pub before: BTreeSet<String>,
    /// Ordering stage.
    #[serde(default)]
    pub stage: u16,
    /// Priority within the stage.
    #[serde(default)]
    pub priority: i32,
    /// Execution deadline.
    pub timeout_ms: u64,
    /// Failure policy: reject, cancel, disable, continue, or retry.
    pub failure_policy: String,
    /// Maximum attempts for retry policy.
    #[serde(default = "one")]
    pub max_attempts: u8,
    /// Delay between retries.
    #[serde(default)]
    pub retry_backoff_ms: u64,
    /// Plugin-owned state migration version.
    pub state_migration_version: u32,
    /// Configuration schema.
    pub configuration_schema: PluginConfigurationSchema,
}

const fn one() -> u8 {
    1
}

/// Short-lived authorization attached to consequential calls.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginAuthorization {
    /// Authenticated local owner.
    pub owner_id: String,
    /// Runtime session.
    pub session_id: String,
    /// Unique call ID.
    pub call_id: String,
    /// Digest of the exact normalized operation.
    pub normalized_digest: String,
    /// Shared-key authorization grant.
    pub grant: String,
    /// Opaque cancellation ID.
    pub cancellation_id: String,
}

/// Runtime/plugin-host command.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "command", content = "value", rename_all = "snake_case")]
pub enum PluginCommand {
    /// Negotiate protocol/API capabilities before activation.
    Negotiate {
        /// Requested wire version.
        protocol_version: u16,
        /// Runtime plugin API version.
        runtime_api_version: String,
        /// Runtime capabilities.
        capabilities: BTreeSet<String>,
    },
    /// Validate an activation set, including ordering constraints.
    ValidateSet {
        /// Complete candidate set.
        manifests: Vec<PluginManifest>,
    },
    /// Validate, migrate, and load a plugin.
    Load {
        /// Declared plugin authority and compatibility.
        manifest: Box<PluginManifest>,
        /// Schema-validated configuration.
        configuration: Value,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Run one declared interceptor.
    Intercept {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Unique invocation ID.
        invocation_id: String,
        /// Stable handler ID.
        handler: String,
        /// Stable proposal class.
        proposal_type: String,
        /// Current proposal.
        proposal: Value,
        /// Explicitly scoped readable state.
        readable_state: Value,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Deliver an event to an observer queue.
    Observe {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Unique invocation ID.
        invocation_id: String,
        /// Stable observer handler ID.
        handler: String,
        /// Stable committed event type.
        event_type: String,
        /// Bounded committed event projection.
        event: Value,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Invoke a declared plugin tool.
    InvokeTool {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Unique invocation ID.
        invocation_id: String,
        /// Stable tool name.
        tool: String,
        /// Normalized tool arguments.
        arguments: Value,
        /// Explicit readable state.
        readable_state: Value,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Cancel a running plugin invocation.
    Cancel {
        /// Plugin invocation to stop.
        invocation_id: String,
    },
    /// Disable without deleting persisted state.
    Disable {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Quarantine a plugin after a policy or crash finding.
    Quarantine {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Redacted reason code.
        reason_code: String,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Report plugin-host health and bounded audit state.
    Health,
}

/// Auditable invocation metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginAudit {
    /// Plugin ID.
    pub plugin_id: String,
    /// Invocation ID, if any.
    pub invocation_id: Option<String>,
    /// Stable operation name.
    pub operation: String,
    /// Stable outcome code.
    pub outcome: String,
    /// Attempt count.
    pub attempts: u8,
}

/// Plugin-host response.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "result", content = "value", rename_all = "snake_case")]
pub enum PluginResponse {
    /// Negotiation succeeded.
    Negotiated {
        /// Selected protocol.
        protocol_version: u16,
        /// Host API version.
        runtime_api_version: String,
        /// Mutually available capabilities.
        capabilities: BTreeSet<String>,
    },
    /// Candidate activation set is valid.
    SetValidated {
        /// Deterministically ordered plugin IDs.
        plugin_ids: Vec<String>,
    },
    /// Manifest/configuration accepted.
    Loaded {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Active state version.
        state_version: u32,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Proposal is unchanged.
    Continue {
        /// Unchanged or explicitly normalized proposal.
        proposal: Value,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Proposal was replaced.
    Replace {
        /// Replacement proposal.
        proposal: Value,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Proposal was rejected.
    Reject {
        /// Safe rejection explanation.
        reason: String,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Plugin tool completed.
    ToolResult {
        /// Bounded normalized result.
        value: Value,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Observation was accepted or dropped by the bounded queue.
    Observation {
        /// Whether it entered the queue.
        accepted: bool,
        /// Current bounded queue depth.
        queue_depth: usize,
        /// Total dropped events for this plugin.
        dropped: u64,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Plugin was disabled or quarantined.
    StateChanged {
        /// Plugin ID.
        plugin_id: String,
        /// `disabled` or `quarantined`.
        state: String,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Health projection.
    Health {
        /// Loaded plugin count.
        loaded: usize,
        /// Running invocation count.
        running: usize,
        /// Observer drops.
        observer_dropped: u64,
    },
    /// Structured plugin failure.
    Failed {
        /// Stable failure class.
        code: String,
        /// Redacted diagnostic.
        message: String,
        /// Whether runtime policy may retry.
        retryable: bool,
    },
}
