//! Canonical graph-state and budget events.
//!
//! Events are the only mutation surface: every accepted assignment, scope
//! change, and merge is committed as an event, and replay applies only events.
//! Values are embedded in events (bounded by their declared size), so
//! reconstruction never calls external systems.

use agentmod_event_model::ArtifactReference;
use agentmod_primitives::{ContentHash, SessionId, TimestampMillis};
use serde::{Deserialize, Serialize};

use crate::{
    declare::{BranchScopePolicy, MergePolicy, VariableDeclaration, VariableScope},
    value::GraphValue,
};

/// Canonical graph-state event.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "event", content = "value", rename_all = "snake_case")]
pub enum GraphStateEvent {
    /// Execution initialized with the exact declaration set.
    VariablesInitialized {
        /// Session owning the state.
        session_id: SessionId,
        /// Hash of the canonical declaration-set bytes.
        declarations_hash: ContentHash,
        /// Complete sorted declarations.
        declarations: Vec<VariableDeclaration>,
    },
    /// An accepted assignment bound to a session, style run, and node.
    VariableAssigned {
        /// Declared variable name.
        name: String,
        /// Owning scope.
        scope: VariableScope,
        /// Style run identity, when bound to one run.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        style_run: Option<String>,
        /// Producing node, when node-produced.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        producer_node: Option<String>,
        /// Version before the assignment.
        prior_version: u64,
        /// New version.
        version: u64,
        /// Accepted canonical value.
        value: GraphValue,
        /// Hash of the exact canonical value bytes.
        value_hash: ContentHash,
        /// Artifact reference when the value is artifact-backed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        artifact_reference: Option<ArtifactReference>,
    },
    /// A recorded rejected assignment; carries no state change.
    VariableValidationRejected {
        /// Declared variable name.
        name: String,
        /// Owning scope.
        scope: VariableScope,
        /// Attempting node, when node-produced.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        node: Option<String>,
        /// Stable rejection reason.
        reason: String,
    },
    /// A branch-local scope was created.
    BranchScopeCreated {
        /// Stable branch identifier.
        branch_id: String,
        /// Scope policy.
        policy: BranchScopePolicy,
    },
    /// A branch-local scope was closed and its writes merged.
    BranchScopeClosed {
        /// Stable branch identifier.
        branch_id: String,
    },
    /// A variable was merged from deterministic contributors.
    VariableMerged {
        /// Declared variable name.
        name: String,
        /// Target scope after merge.
        scope: VariableScope,
        /// Policy applied.
        policy: MergePolicy,
        /// Deterministically ordered contributor identities.
        contributors: Vec<String>,
        /// New version after the merge.
        version: u64,
        /// Merged canonical value.
        value: GraphValue,
        /// Hash of the merged canonical value.
        value_hash: ContentHash,
    },
}

/// Canonical budget accounting event.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "event", content = "value", rename_all = "snake_case")]
pub enum BudgetEvent {
    /// Execution initialized with exact limits.
    BudgetsInitialized {
        /// Session owning the ledger.
        session_id: SessionId,
        /// Immutable limits.
        limits: crate::budget::BudgetLimits,
        /// Clock-supplied initialization timestamp.
        recorded_at: TimestampMillis,
        /// Whether the wall-clock ceiling is explicitly selected.
        wall_clock_enabled: bool,
    },
    /// Usage committed after a completed action with exact evidence.
    BudgetCommitted {
        /// Accounting dimension.
        dimension: crate::budget::BudgetDimension,
        /// Exact positive delta.
        delta: u64,
        /// Whether the value is provider-reported or estimated.
        kind: crate::budget::UsageKind,
        /// Hash of the canonical evidence bytes.
        evidence_hash: ContentHash,
        /// Pricing binding required for cost dimensions.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pricing: Option<crate::budget::PricingBinding>,
        /// Clock-supplied commit timestamp.
        recorded_at: TimestampMillis,
    },
    /// A cost dimension is marked unknown; never recorded as zero.
    BudgetMarkedUnknown {
        /// Accounting dimension.
        dimension: crate::budget::BudgetDimension,
        /// Whether the reported or estimated slot is unknown.
        kind: crate::budget::UsageKind,
        /// Clock-supplied timestamp.
        recorded_at: TimestampMillis,
    },
    /// A pre-dispatch check was blocked.
    BudgetCheckBlocked {
        /// Accounting dimension.
        dimension: crate::budget::BudgetDimension,
        /// Requested delta.
        requested: u64,
        /// Conservative remaining amount.
        remaining: u64,
        /// Clock-supplied timestamp.
        recorded_at: TimestampMillis,
    },
    /// Concurrent-children gauge changed.
    ConcurrentChildrenChanged {
        /// Signed gauge delta (`1` open, `-1` close).
        delta: i64,
        /// Current gauge.
        current: u64,
        /// Observed peak.
        peak: u64,
        /// Declared ceiling.
        limit: Option<u64>,
        /// Clock-supplied timestamp.
        recorded_at: TimestampMillis,
    },
    /// A child session's usage rolled up per explicit policy.
    ChildUsageRolledUp {
        /// Child session identity.
        child_session: SessionId,
        /// Rollup policy applied.
        policy: crate::budget::RollupPolicy,
        /// Dimensions actually rolled up, in stable order.
        dimensions: Vec<crate::budget::BudgetDimension>,
        /// Clock-supplied timestamp.
        recorded_at: TimestampMillis,
    },
}
