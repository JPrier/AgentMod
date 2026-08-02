//! Versioned wire contracts for the isolated scheduler worker.

use serde::{Deserialize, Serialize};

/// Current compatible wire version.
pub const CURRENT_PROTOCOL_VERSION: u16 = 2;

/// Durable trigger.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ScheduleTrigger {
    /// Fire once at a Unix timestamp.
    AtMillis(i64),
    /// Fire repeatedly from an initial timestamp.
    Interval {
        /// Initial occurrence.
        starts_at_ms: i64,
        /// Positive recurrence.
        every_ms: u64,
    },
    /// Fire when runtime commits the exact event type.
    RuntimeEvent {
        /// Canonical event type.
        event_type: String,
    },
    /// Fire when bounded process output contains a literal.
    ProcessOutput {
        /// Runtime process ID.
        process_id: String,
        /// Literal bounded pattern.
        contains: String,
    },
}

/// Deferred work payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum SchedulePayload {
    /// Start a background model turn.
    Prompt {
        /// User-authored task.
        prompt: String,
    },
    /// Wake a durable continuation.
    Continuation {
        /// Opaque runtime continuation.
        continuation_id: String,
    },
    /// Record a runtime-owned graph trigger without synthesizing a user turn.
    GraphTrigger {
        /// Immutable graph run identity.
        run_id: String,
        /// Owning graph node.
        node_id: String,
    },
}

/// Complete runtime-owned execution policy supplied to a schedule.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleSpec {
    /// Stable schedule ID.
    pub schedule_id: String,
    /// Owning runtime session.
    pub session_id: String,
    /// Explicit idempotency key for create/update.
    pub idempotency_id: String,
    /// Explicit session style.
    pub style: String,
    /// Workspace.
    pub workspace: String,
    /// Permission policy name.
    pub permission_policy: String,
    /// Provider.
    pub provider: String,
    /// Model.
    pub model: String,
    /// Token budget.
    pub token_budget: u64,
    /// Maximum cost in micros.
    pub cost_budget_micros: u64,
    /// Trigger.
    pub trigger: ScheduleTrigger,
    /// Work.
    pub payload: SchedulePayload,
    /// Whether firing is enabled.
    pub active: bool,
}

/// Scheduler command.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "command", content = "value", rename_all = "snake_case")]
pub enum SchedulerCommand {
    /// Negotiate before stateful commands.
    Negotiate {
        /// Requested protocol.
        protocol_version: u16,
        /// Runtime capabilities.
        capabilities: Vec<String>,
        /// Local bootstrap authentication token.
        authentication_token: String,
    },
    /// Create or idempotently replace.
    Upsert {
        /// Schedule.
        schedule: Box<ScheduleSpec>,
    },
    /// Disable and remove.
    Remove {
        /// Schedule ID.
        schedule_id: String,
    },
    /// List bounded schedules.
    List {
        /// Maximum rows.
        limit: u32,
    },
    /// Atomically claim due time schedules.
    ClaimDue {
        /// Maximum executions.
        limit: u32,
    },
    /// List durable nonterminal claims for restart reconciliation.
    ListPendingExecutions {
        /// Maximum executions.
        limit: u32,
    },
    /// Match a committed runtime event.
    FireRuntimeEvent {
        /// Runtime session that committed the observation.
        source_session_id: String,
        /// Canonical event ID used for idempotency.
        event_id: String,
        /// Event type.
        event_type: String,
    },
    /// Match bounded process output.
    FireProcessOutput {
        /// Runtime session that committed the observation.
        source_session_id: String,
        /// Stable output event ID used for idempotency.
        output_id: String,
        /// Process ID.
        process_id: String,
        /// Bounded output.
        output: String,
    },
    /// Persist execution completion.
    CompleteExecution {
        /// Stable execution ID.
        execution_id: String,
        /// Whether runtime work completed.
        succeeded: bool,
    },
    /// Health.
    Health,
}

/// Exact trigger observation bound into a durable occurrence claim.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ScheduleObservation {
    /// Canonical runtime event that caused the claim.
    RuntimeEvent {
        /// Exact committed event identity.
        event_id: String,
    },
    /// Exact bounded process-output observation that caused the claim.
    ProcessOutput {
        /// Stable output observation identity.
        output_id: String,
    },
}

/// Claimed execution.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledExecution {
    /// Stable occurrence ID.
    pub execution_id: String,
    /// Scheduled time or trigger observation time.
    pub scheduled_for_ms: i64,
    /// Unix timestamp when the worker durably claimed this occurrence.
    #[serde(default)]
    pub claimed_at_ms: i64,
    /// Exact non-time observation that caused this claim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation: Option<ScheduleObservation>,
    /// Complete schedule snapshot.
    pub schedule: ScheduleSpec,
}

/// Scheduler response.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "result", content = "value", rename_all = "snake_case")]
pub enum SchedulerResponse {
    /// Negotiated capabilities.
    Negotiated {
        /// Selected protocol.
        protocol_version: u16,
        /// Worker capabilities.
        capabilities: Vec<String>,
    },
    /// Schedule stored.
    Stored {
        /// Schedule ID.
        schedule_id: String,
        /// Whether this was an idempotent replay.
        replayed: bool,
    },
    /// Schedule removed.
    Removed {
        /// Whether a schedule existed.
        existed: bool,
    },
    /// Bounded schedules.
    Schedules {
        /// Records.
        schedules: Vec<ScheduleSpec>,
    },
    /// Newly claimed executions.
    Executions {
        /// Claims.
        executions: Vec<ScheduledExecution>,
    },
    /// Completion persisted.
    ExecutionCompleted {
        /// Whether this call changed terminal state.
        changed: bool,
    },
    /// Health.
    Health {
        /// Stable status.
        status: String,
    },
    /// Safe incompatibility or request failure.
    Error {
        /// Stable code.
        code: String,
        /// Redacted message.
        message: String,
    },
}
