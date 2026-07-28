//! Runtime wire contracts. Receiving services must map these into service-owned types.

use agentmod_primitives::{ArtifactId, CancellationId, EventId, Sequence, SessionId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Runtime request payload.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "operation", content = "arguments", rename_all = "snake_case")]
pub enum RuntimeRequest {
    /// Report process health and negotiated API version.
    Health,
    /// Create a durable session.
    CreateSession {
        /// User-supplied workspace text, validated by service and logic.
        workspace: String,
        /// Explicit top-level execution style.
        style: String,
    },
    /// List sessions without loading their conversations.
    ListSessions {
        /// Maximum results.
        limit: u32,
    },
    /// Inspect purely replayed state at the journal head or an earlier sequence.
    InspectSession {
        /// Durable session.
        session_id: SessionId,
        /// Inclusive target; absent means the verified head.
        at: Option<Sequence>,
    },
    /// Replay session state without repeating external effects.
    ReplaySession {
        /// Durable session.
        session_id: SessionId,
        /// Inclusive target; absent means the verified head.
        at: Option<Sequence>,
    },
    /// Create an independently appendable child at a source sequence.
    BranchSession {
        /// Immutable parent.
        session_id: SessionId,
        /// Inclusive parent fork point.
        at: Sequence,
        /// Optional explicit child style.
        style: Option<String>,
    },
    /// Create or update one durable schedule through the runtime.
    UpsertSchedule {
        /// Runtime-owned schedule contract.
        schedule: Box<RuntimeScheduleSpec>,
    },
    /// Remove one durable schedule.
    RemoveSchedule {
        /// Stable schedule identifier.
        schedule_id: String,
    },
    /// List durable schedules without claiming them.
    ListSchedules {
        /// Maximum results.
        limit: u32,
    },
    /// Claim due occurrences for runtime execution.
    ClaimDueSchedules {
        /// Maximum claims.
        limit: u32,
    },
    /// Record the terminal result of a claimed occurrence.
    CompleteScheduledExecution {
        /// Deterministic execution identifier returned by a claim.
        execution_id: String,
        /// Whether normal runtime execution completed successfully.
        succeeded: bool,
    },
    /// Claim and execute due prompt schedules through the normal runtime turn path.
    RunDueSchedules {
        /// Maximum claims processed by this invocation.
        limit: u32,
    },
    /// Persist one schedule-bound turn continuation before storing its schedule.
    CreateDeferredTurn {
        /// Existing durable session.
        session_id: SessionId,
        /// Opaque resume-once continuation identifier.
        continuation_id: String,
        /// Exact schedule allowed to wake this continuation.
        schedule_id: String,
        /// User-authored prompt to execute after a valid wake.
        prompt: String,
        /// Explicit workspace retained for provenance validation.
        workspace: String,
        /// Explicit provider.
        provider: String,
        /// Explicit model.
        model: String,
        /// Provider and scheduled-execution policy options.
        options: Value,
        /// Explicit session style retained for provenance validation.
        style: String,
        /// Stable cancellation identifier for the eventual provider request.
        cancellation_id: CancellationId,
        /// Exact wake condition.
        trigger: RuntimeScheduleTrigger,
        /// Optional absolute expiration timestamp.
        expires_at_ms: Option<i64>,
    },
    /// Run one durable user turn through the selected provider.
    RunTurn {
        /// Existing durable session.
        session_id: SessionId,
        /// Exact user-authored input.
        prompt: String,
        /// Explicit provider adapter.
        provider: String,
        /// Explicit model.
        model: String,
        /// Provider-specific options subject to runtime policy.
        options: Value,
        /// Stable cancellation identifier.
        cancellation_id: CancellationId,
    },
    /// Subscribe from a committed sequence.
    Subscribe {
        /// Session to observe.
        session_id: SessionId,
        /// First sequence the client still needs.
        after: Option<Sequence>,
        /// Maximum verified events in this catch-up page.
        limit: u32,
    },
    /// Approve or deny a durable continuation.
    ResolveApproval {
        /// Session containing the durable continuation.
        session_id: SessionId,
        /// Opaque continuation string at the wire boundary.
        continuation_id: String,
        /// Approval choice.
        approved: bool,
        /// Continue the provider loop after recording the decision.
        ///
        /// ACP cancellation resolves the durable continuation without starting
        /// a replacement provider request.
        #[serde(default = "default_true")]
        resume_after_resolution: bool,
    },
    /// Cancel an active request/session operation.
    Cancel {
        /// Exact active operation to stop.
        cancellation_id: CancellationId,
        /// Stable cancellation reason for audit.
        reason: String,
    },
    /// Transport-level acknowledgement granting more stream items.
    ///
    /// The local RPC transport consumes this request directly; it never enters
    /// runtime business logic.
    StreamWindowUpdate {
        /// Number of additional nonterminal items the receiver can buffer.
        credits: u32,
        /// Highest contiguous stream sequence already accepted.
        last_received_sequence: u64,
    },
}

const fn default_true() -> bool {
    true
}

/// Runtime response payload.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "result", content = "value", rename_all = "snake_case")]
pub enum RuntimeResponse {
    /// Runtime health.
    Health {
        /// Stable status (`ok`, `degraded`, or `unavailable`).
        status: String,
        /// Runtime application version.
        version: String,
    },
    /// Session was durably created.
    SessionCreated {
        /// Canonical session ID.
        session_id: SessionId,
    },
    /// Bounded session listing.
    Sessions {
        /// Lightweight rows.
        sessions: Vec<SessionSummary>,
    },
    /// Pure replay/inspection result.
    SessionInspected {
        /// Selected session.
        session_id: SessionId,
        /// Verified journal head.
        head_sequence: Sequence,
        /// Inclusive replay target represented by `state`.
        inspected_sequence: Sequence,
        /// Number of events reduced.
        event_count: u64,
        /// Structured replay-derived state.
        state: Value,
    },
    /// Atomic branch result.
    SessionBranched {
        /// Fresh child session.
        session_id: SessionId,
        /// Immutable parent.
        parent_session_id: SessionId,
        /// Inclusive parent fork point.
        fork_sequence: Sequence,
        /// Materialized child journal head.
        child_head_sequence: Sequence,
    },
    /// Schedule storage result.
    ScheduleStored {
        /// Stable schedule identifier.
        schedule_id: String,
        /// Whether this was an exact idempotent replay.
        replayed: bool,
    },
    /// Schedule removal result.
    ScheduleRemoved {
        /// Whether a schedule existed.
        existed: bool,
    },
    /// Durable schedule listing.
    Schedules {
        /// Runtime-owned schedule projections.
        schedules: Vec<RuntimeScheduleSpec>,
    },
    /// Newly claimed occurrences.
    ScheduledExecutions {
        /// Deterministic durable claims.
        executions: Vec<RuntimeScheduledExecution>,
    },
    /// Scheduled execution completion result.
    ScheduledExecutionCompleted {
        /// Whether this request performed the terminal transition.
        changed: bool,
    },
    /// Results from one bounded scheduled-work cycle.
    ScheduledRuns {
        /// Per-occurrence outcomes.
        runs: Vec<RuntimeScheduledRun>,
    },
    /// A schedule-bound resume-once continuation was persisted.
    DeferredTurnCreated {
        /// Opaque persisted continuation identifier.
        continuation_id: String,
    },
    /// One turn reached a provider pause or completion boundary.
    Turn {
        /// Ordered provider lifecycle projection.
        events: Vec<RuntimeProviderEvent>,
        /// First event committed by the turn.
        first_committed_sequence: Sequence,
        /// Last event committed by the turn.
        last_committed_sequence: Sequence,
        /// Harness continuation awaiting runtime tool handling, if any.
        awaiting_continuation: Option<String>,
    },
    /// One provider lifecycle item committed before delivery.
    TurnEvent {
        /// Frontend-safe provider event.
        event: RuntimeProviderEvent,
        /// Canonical sequence committed for this item.
        committed_sequence: Sequence,
    },
    /// Terminal metadata for an incremental turn stream.
    TurnComplete {
        /// First canonical sequence committed by the turn.
        first_committed_sequence: Sequence,
        /// Last canonical sequence committed by the turn.
        last_committed_sequence: Sequence,
        /// Harness continuation awaiting runtime tool handling, if any.
        awaiting_continuation: Option<String>,
    },
    /// One verified canonical session event delivered after a reconnect cursor.
    SessionEvent {
        /// Canonical event identity used for durable trigger deduplication.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_id: Option<EventId>,
        /// Canonical session sequence.
        sequence: Sequence,
        /// Stable typed event name.
        event_type: String,
        /// Typed event payload.
        payload: Value,
    },
    /// Terminal metadata for one bounded session catch-up page.
    SubscriptionComplete {
        /// Verified journal head at scan time.
        head_sequence: Sequence,
        /// Last sequence delivered, or the original cursor for an empty page.
        last_delivered_sequence: Option<Sequence>,
        /// Whether the frontend should request another immediate page.
        has_more: bool,
    },
    /// Aggregated compatibility representation of a bounded subscription page.
    SessionEvents {
        /// Strictly ordered verified event projections.
        events: Vec<RuntimeSessionEvent>,
        /// Verified journal head at scan time.
        head_sequence: Sequence,
        /// Last delivered sequence.
        last_delivered_sequence: Option<Sequence>,
        /// Whether another immediate page exists.
        has_more: bool,
    },
    /// Approval resolution was accepted or was an idempotent duplicate.
    ApprovalResolved {
        /// Whether this request performed the transition.
        transitioned: bool,
        /// Provider events produced while the approved/denied turn continued.
        events: Vec<RuntimeProviderEvent>,
        /// Last committed sequence when turn execution was performed.
        last_committed_sequence: Option<Sequence>,
        /// A subsequent approval required by the resumed turn.
        awaiting_continuation: Option<String>,
    },
    /// Operation cancellation was accepted.
    Cancelled,
}

/// Runtime frontend schedule trigger.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum RuntimeScheduleTrigger {
    /// One-time Unix timestamp in milliseconds.
    AtMillis(i64),
    /// Fixed interval with an explicit initial occurrence.
    Interval {
        /// First Unix timestamp in milliseconds.
        starts_at_ms: i64,
        /// Interval duration.
        every_ms: u64,
    },
    /// Match a canonical runtime event type.
    RuntimeEvent {
        /// Stable event type.
        event_type: String,
    },
    /// Match bounded process output.
    ProcessOutput {
        /// Stable process identifier.
        process_id: String,
        /// Required literal text.
        contains: String,
    },
}

/// Runtime frontend schedule payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum RuntimeSchedulePayload {
    /// Run a normal intercepted user turn.
    Prompt {
        /// User-authored scheduled prompt.
        prompt: String,
    },
    /// Wake a durable runtime continuation.
    Continuation {
        /// Opaque continuation identifier.
        continuation_id: String,
    },
}

/// Runtime-owned schedule projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RuntimeScheduleSpec {
    /// Stable schedule identifier.
    pub schedule_id: String,
    /// Target canonical session.
    pub session_id: SessionId,
    /// Stable request idempotency identifier.
    pub idempotency_id: String,
    /// Explicit session style.
    pub style: String,
    /// Explicit workspace.
    pub workspace: String,
    /// Explicit permission policy.
    pub permission_policy: String,
    /// Explicit provider.
    pub provider: String,
    /// Explicit model.
    pub model: String,
    /// Hard token budget.
    pub token_budget: u64,
    /// Hard cost budget in micro-units.
    pub cost_budget_micros: u64,
    /// Durable trigger.
    pub trigger: RuntimeScheduleTrigger,
    /// Deferred work payload.
    pub payload: RuntimeSchedulePayload,
    /// Whether new occurrences may be claimed.
    pub active: bool,
}

/// One occurrence durably claimed by the runtime.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RuntimeScheduledExecution {
    /// Deterministic occurrence identifier.
    pub execution_id: String,
    /// Trigger occurrence timestamp.
    pub scheduled_for_ms: i64,
    /// Unix timestamp when the scheduler durably claimed this occurrence.
    #[serde(default)]
    pub claimed_at_ms: i64,
    /// Immutable schedule projection at claim time.
    pub schedule: RuntimeScheduleSpec,
}

/// Runtime result for one claimed scheduled occurrence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RuntimeScheduledRun {
    /// Deterministic occurrence identifier.
    pub execution_id: String,
    /// Owning schedule.
    pub schedule_id: String,
    /// Whether the occurrence reached a terminal scheduler state.
    pub terminal: bool,
    /// Whether normal runtime execution succeeded.
    pub succeeded: bool,
    /// Last canonical sequence committed by the turn, when started.
    pub last_committed_sequence: Option<Sequence>,
    /// Durable approval continuation, when execution paused.
    pub awaiting_continuation: Option<String>,
    /// Sanitized failure text.
    pub error: Option<String>,
}

/// One verified canonical event in a reconnect page.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RuntimeSessionEvent {
    /// Canonical event identity when supplied by a compatible runtime.
    #[serde(default)]
    pub event_id: Option<EventId>,
    /// Canonical sequence.
    pub sequence: Sequence,
    /// Stable typed event name.
    pub event_type: String,
    /// Typed payload.
    pub payload: Value,
}

/// Provider lifecycle event projected to a runtime frontend.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "event", content = "value", rename_all = "snake_case")]
pub enum RuntimeProviderEvent {
    /// Provider request started.
    Started,
    /// Visible output delta.
    Text {
        /// Visible text fragment.
        text: String,
    },
    /// Partial tool call.
    ToolDelta {
        /// Stable call identifier.
        call_id: String,
        /// Tool-name fragment.
        name: String,
        /// JSON argument fragment.
        arguments: String,
    },
    /// Complete tool proposal awaiting runtime interception.
    ToolProposed {
        /// Harness continuation.
        continuation_id: String,
        /// Stable call identifier.
        call_id: String,
        /// Tool ID.
        tool: String,
        /// Structured arguments.
        arguments: Value,
    },
    /// Provider response completed.
    Completed {
        /// Provider-neutral finish reason.
        reason: String,
        /// Provider-reported input tokens.
        input_tokens: u64,
        /// Provider-reported output tokens.
        output_tokens: u64,
    },
    /// Provider request was cancelled after any visible partial output.
    Cancelled,
    /// Provider emitted a classified failure.
    Failed {
        /// Stable failure code.
        code: String,
        /// Redacted message.
        message: String,
        /// Whether business policy may retry.
        retryable: bool,
    },
}

/// Lightweight wire session row.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionSummary {
    /// Canonical ID.
    pub id: SessionId,
    /// Display label; not interpreted as a path by the frontend.
    pub workspace_label: String,
    /// Explicit style ID.
    pub style: String,
    /// Last committed sequence.
    pub sequence: Sequence,
    /// Durable lifecycle state.
    pub state: String,
}

/// One stream item for a frontend subscription.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "event", content = "value", rename_all = "snake_case")]
pub enum RuntimeStreamItem {
    /// Renderable assistant text delta.
    AssistantText {
        /// Visible text only.
        text: String,
    },
    /// Canonical event summary.
    Event {
        /// Sequence number.
        sequence: Sequence,
        /// Stable event type.
        event_type: String,
    },
    /// Immutable artifact became available.
    Artifact {
        /// Artifact ID.
        artifact_id: ArtifactId,
        /// Safe display label.
        label: String,
    },
    /// Endpoint requests user approval.
    Approval {
        /// Continuation token.
        continuation_id: String,
        /// Redacted action description.
        description: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_approval_request_defaults_to_resuming() {
        let request: RuntimeRequest = serde_json::from_value(serde_json::json!({
            "operation": "resolve_approval",
            "arguments": {
                "session_id": "00000000-0000-0000-0000-000000000001",
                "continuation_id": "continuation",
                "approved": false
            }
        }))
        .expect("legacy approval request");
        assert!(matches!(
            request,
            RuntimeRequest::ResolveApproval {
                approved: false,
                resume_after_resolution: true,
                ..
            }
        ));
    }

    #[test]
    fn approval_cancellation_round_trips_without_resuming() {
        let request = RuntimeRequest::ResolveApproval {
            session_id: "00000000-0000-0000-0000-000000000001"
                .parse()
                .expect("session"),
            continuation_id: String::from("continuation"),
            approved: false,
            resume_after_resolution: false,
        };
        let encoded = serde_json::to_vec(&request).expect("encode");
        assert_eq!(
            serde_json::from_slice::<RuntimeRequest>(&encoded).expect("decode"),
            request
        );
    }
}
