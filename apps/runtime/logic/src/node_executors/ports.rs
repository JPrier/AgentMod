//! Narrow logic-owned ports consumed by native control-flow node executors.
//!
//! Executors never touch dependency or SDK types. Every external capability
//! (child-session delivery, scheduler-worker upsert, durable continuation
//! creation) is behind one of these ports. The composition root binds real
//! implementations over the existing `RuntimeScheduleLogicPort`,
//! `ChildSessionLogicPort`, and continuation logic; tests bind deterministic
//! mocks that record every call for recovery-cut assertions.

use agentmod_primitives::ContentHash;
use thiserror::Error;

use crate::node_executors::events::GraphScheduleTrigger;

/// Delivery command for one child-agent message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliverChildMessageCommand {
    /// Exact parent session.
    pub parent_session_id: String,
    /// Exact child session.
    pub child_session_id: String,
    /// Stable per-session message identity.
    pub message_id: String,
    /// Caller-supplied idempotency key.
    pub idempotency_key: String,
    /// Monotonic per-child message sequence.
    pub sequence: u64,
    /// Bounded typed content.
    pub content: String,
    /// Exact content hash.
    pub content_hash: ContentHash,
    /// Security classification.
    pub classification: crate::node_executors::events::ChildMessageClassification,
    /// Optional expiration wall-clock millis.
    pub expires_at_ms: Option<i64>,
}

/// Delivery receipt returned by the child-session boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildMessageReceipt {
    /// Whether the child accepted delivery.
    pub delivered: bool,
    /// Bounded receipt summary.
    pub summary: String,
    /// Stable rejection reason when not delivered.
    pub rejection_reason: Option<String>,
}

/// Child lifecycle view used for child-side validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildLifecycleView {
    /// Whether the exact child session exists.
    pub exists: bool,
    /// Whether the child is active (not archived/terminated).
    pub active: bool,
}

/// Child-session message delivery port.
pub trait ChildSessionMessagePort: Send + Sync {
    /// Delivers one child-agent message with create-once idempotency by
    /// `message_id`. The child style decides how and whether the content
    /// enters its provider projection; this port never fabricates a user
    /// message in the parent session.
    ///
    /// # Errors
    ///
    /// Returns [`ChildSessionMessagePortError`] for an unknown child,
    /// invalid content, or an unavailable boundary.
    fn deliver_child_message(
        &self,
        command: DeliverChildMessageCommand,
    ) -> Result<ChildMessageReceipt, ChildSessionMessagePortError>;

    /// Returns the exact child lifecycle for validation.
    ///
    /// # Errors
    ///
    /// Returns [`ChildSessionMessagePortError`] when the child catalog is
    /// unavailable.
    fn child_lifecycle(
        &self,
        child_session_id: &str,
    ) -> Result<ChildLifecycleView, ChildSessionMessagePortError>;
}

/// Port boundary failure classification.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ChildSessionMessagePortError {
    /// The exact child session does not exist or is not active.
    #[error("child session is unavailable")]
    Unavailable,
    /// The message exceeds a hard bound.
    #[error("child message exceeds its bound")]
    BoundExceeded,
    /// The child boundary is unreachable; delivery may or may not have
    /// occurred and must never be blindly retried.
    #[error("child message delivery is externally uncertain")]
    ExternallyUncertain,
}

/// Upsert command for one graph-owned schedule.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpsertGraphScheduleCommand {
    /// Stable schedule identity.
    pub schedule_id: String,
    /// Canonical session owning the schedule.
    pub session_id: String,
    /// Schedule idempotency key.
    pub idempotency_key: String,
    /// Trigger description.
    pub trigger: GraphScheduleTrigger,
    /// Exact style binding.
    pub style: String,
    /// Exact workspace binding.
    pub workspace: String,
    /// Permission policy binding.
    pub permission_policy: String,
    /// Provider binding.
    pub provider: String,
    /// Model binding.
    pub model: String,
    /// Hard token budget.
    pub token_budget: u64,
    /// Hard cost budget micros.
    pub cost_budget_micros: u64,
}

/// Schedule store receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduleStoreReceipt {
    /// Stable schedule identity.
    pub schedule_id: String,
    /// True when an identical schedule already existed (create-once replay).
    pub replayed: bool,
}

/// Graph-owned schedule port backed by the scheduler worker.
pub trait GraphSchedulePort: Send + Sync {
    /// Creates or reconciles one schedule with create-once semantics.
    ///
    /// # Errors
    ///
    /// Returns [`GraphSchedulePortError`] for an invalid or externally
    /// uncertain schedule operation.
    fn upsert_schedule(
        &self,
        command: UpsertGraphScheduleCommand,
    ) -> Result<ScheduleStoreReceipt, GraphSchedulePortError>;

    /// Removes one schedule.
    ///
    /// # Errors
    ///
    /// Returns [`GraphSchedulePortError`] when the scheduler worker is
    /// unavailable.
    fn remove_schedule(&self, schedule_id: &str) -> Result<bool, GraphSchedulePortError>;
}

/// Schedule port boundary failure classification.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum GraphSchedulePortError {
    /// The schedule request is invalid.
    #[error("graph-owned schedule request is invalid")]
    Invalid,
    /// The scheduler worker is unavailable.
    #[error("scheduler worker is unavailable")]
    Unavailable,
    /// The schedule operation may or may not have committed; never redispatch.
    #[error("schedule operation is externally uncertain")]
    ExternallyUncertain,
}

/// Create command for one durable delay continuation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateDelayContinuationCommand {
    /// Canonical session containing the delay.
    pub session_id: String,
    /// Deterministic continuation identity bound to run/node/transition.
    pub continuation_id: String,
    /// Exact resolved wake time in wall-clock millis.
    pub wake_time_ms: i64,
    /// Optional expiration wall-clock millis.
    pub expires_at_ms: Option<i64>,
    /// Graph node that owns the delay.
    pub node_id: String,
}

/// Wake claim for one durable delay continuation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimDelayWakeCommand {
    /// Canonical session containing the delay.
    pub session_id: String,
    /// Durable continuation identity.
    pub continuation_id: String,
    /// Exact canonical wake time.
    pub wake_time_ms: i64,
}

/// Exactly-once wake claim result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DelayWakeResult {
    /// True only for the claim that won the resume-once transition.
    pub transitioned: bool,
    /// Stable claim classification.
    pub proof: String,
}

/// Cancel command for one durable delay continuation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CancelDelayContinuationCommand {
    /// Canonical session containing the delay.
    pub session_id: String,
    /// Durable continuation identity.
    pub continuation_id: String,
}

/// Durable delay continuation port.
pub trait DurableDelayPort: Send + Sync {
    /// Creates a durable continuation with create-once semantics by
    /// `continuation_id`, surviving runtime restart.
    ///
    /// # Errors
    ///
    /// Returns [`DurableDelayPortError`] for an invalid or externally
    /// uncertain continuation operation.
    fn create_delay_continuation(
        &self,
        command: CreateDelayContinuationCommand,
    ) -> Result<(), DurableDelayPortError>;

    /// Claims the exact wake transition exactly once.
    ///
    /// # Errors
    ///
    /// Returns [`DurableDelayPortError`] when the continuation is unknown or
    /// the boundary is unavailable.
    fn claim_delay_wake(
        &self,
        command: ClaimDelayWakeCommand,
    ) -> Result<DelayWakeResult, DurableDelayPortError>;

    /// Cancels a pending delay continuation.
    ///
    /// # Errors
    ///
    /// Returns [`DurableDelayPortError`] when the boundary is unavailable.
    fn cancel_delay_continuation(
        &self,
        command: CancelDelayContinuationCommand,
    ) -> Result<bool, DurableDelayPortError>;
}

/// Delay port boundary failure classification.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum DurableDelayPortError {
    /// The delay request is invalid.
    #[error("durable delay request is invalid")]
    Invalid,
    /// The continuation store is unavailable.
    #[error("delay continuation store is unavailable")]
    Unavailable,
    /// The continuation operation may or may not have committed; never
    /// redispatch an ambiguous wake.
    #[error("delay continuation operation is externally uncertain")]
    ExternallyUncertain,
}

/// Facade over every narrow port a native node executor may consume.
pub trait NodeExecutorPorts: Send + Sync {
    /// Child-session message delivery.
    fn child_messages(&self) -> &dyn ChildSessionMessagePort;
    /// Graph-owned schedule creation/removal.
    fn schedules(&self) -> &dyn GraphSchedulePort;
    /// Durable delay continuation lifecycle.
    fn delays(&self) -> &dyn DurableDelayPort;
}
