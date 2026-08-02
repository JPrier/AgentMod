//! Runtime ownership of schedule policy and worker coordination.
#![allow(
    missing_docs,
    reason = "logic-local schedule records are exhaustively mapped at the layer boundary"
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the schedule logic port exposes one documented closed error taxonomy"
)]

use agentmod_graph_engine::{
    DelayCancellation, DelayResolution, NodeConfiguration, ScheduleCancellation,
    ScheduleTrigger as GraphScheduleTrigger,
};
use agentmod_primitives::{ContentHash, ContinuationId, SessionId, TimestampMillis};
use agentmod_runtime_data::scheduler::{
    RuntimeScheduleDataError, RuntimeScheduleDataPort, RuntimeScheduleDataRecord,
    ScheduleDataObservation, ScheduleDataPayload, ScheduleDataTrigger,
    ScheduledExecutionDataRecord,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{
    action::{ActionProposal, ConsequentialAction, ProposalId},
    node_execution::{NativeExecutorKey, NodeWorkIdentity, native_executor_key},
    session::{SessionNodeExecutorResolution, SessionNodeExecutorSource},
};

/// Immutable trigger retained in canonical graph-schedule events.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CanonicalGraphScheduleTrigger {
    /// One-time wall-clock wake.
    AtMillis {
        /// Exact Unix timestamp in milliseconds.
        timestamp_ms: i64,
    },
    /// Recurring interval.
    Interval {
        /// Exact first occurrence.
        starts_at_ms: i64,
        /// Positive recurring interval.
        every_ms: u64,
    },
    /// Matching committed runtime event.
    RuntimeEvent {
        /// Declared event type.
        event_type: String,
    },
    /// Matching supervised process output.
    ProcessOutput {
        /// Canonical process identity.
        process_id: String,
        /// Bounded literal match.
        contains: String,
    },
}

/// Immutable cancellation behavior selected by the compiled graph node.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphScheduleCancellationPolicy {
    /// Cancel the durable delay continuation.
    CancelContinuation,
    /// Keep the delay claim durable but suppress all later graph effects.
    SuppressEffects,
    /// Remove the scheduler trigger and cancel the current wait.
    CancelTrigger,
    /// Preserve the scheduler trigger while cancelling only the current wait.
    CancelWaitOnly,
}

impl CanonicalGraphScheduleTrigger {
    /// Converts the canonical trigger into the scheduler use-case command type.
    #[must_use]
    pub fn to_schedule_trigger(&self) -> ScheduleTrigger {
        match self {
            Self::AtMillis { timestamp_ms } => ScheduleTrigger::AtMillis(*timestamp_ms),
            Self::Interval {
                starts_at_ms,
                every_ms,
            } => ScheduleTrigger::Interval {
                starts_at_ms: *starts_at_ms,
                every_ms: *every_ms,
            },
            Self::RuntimeEvent { event_type } => ScheduleTrigger::RuntimeEvent {
                event_type: event_type.clone(),
            },
            Self::ProcessOutput {
                process_id,
                contains,
            } => ScheduleTrigger::ProcessOutput {
                process_id: process_id.clone(),
                contains: contains.clone(),
            },
        }
    }

    /// Returns the durable continuation wake condition for a waiting node.
    ///
    /// Recurring schedules deliberately have no single-wake continuation
    /// representation and are rejected when `wait_for_trigger` is requested.
    pub fn to_wake_condition(
        &self,
    ) -> Result<crate::continuation::ContinuationWakeCondition, GraphScheduleError> {
        match self {
            Self::AtMillis { timestamp_ms } => {
                Ok(crate::continuation::ContinuationWakeCondition::At(
                    TimestampMillis::new(*timestamp_ms),
                ))
            }
            Self::RuntimeEvent { event_type } => Ok(
                crate::continuation::ContinuationWakeCondition::RuntimeEvent {
                    event_type: event_type.clone(),
                    selector: None,
                },
            ),
            Self::ProcessOutput {
                process_id,
                contains,
            } => Ok(
                crate::continuation::ContinuationWakeCondition::ProcessOutput {
                    process_id: process_id.clone(),
                    pattern: contains.clone(),
                },
            ),
            Self::Interval { .. } => Err(GraphScheduleError::RecurringWaitUnsupported),
        }
    }

    /// Stable text bound into `ConsequentialAction::ScheduleCreation`.
    pub fn canonical_text(&self) -> Result<String, GraphScheduleError> {
        serde_json::to_string(self).map_err(|_| GraphScheduleError::InvalidConfiguration)
    }
}

/// Fully resolved immutable graph schedule identity and trigger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedGraphSchedule {
    /// Exact graph work owning the schedule.
    pub work: NodeWorkIdentity,
    /// Exact immutable execution-plan hash.
    pub execution_plan_hash: ContentHash,
    /// Exact compiled node/configuration hash.
    pub configuration_hash: ContentHash,
    /// Deterministic scheduler identity.
    pub schedule_id: String,
    /// Deterministic durable continuation identity when waiting.
    pub continuation_id: Option<ContinuationId>,
    /// Deterministic scheduler idempotency key.
    pub idempotency_id: String,
    /// Canonical resolved trigger.
    pub trigger: CanonicalGraphScheduleTrigger,
    /// Exact runtime timestamp used to resolve relative schedule values.
    pub resolution_timestamp_ms: Option<i64>,
    /// Exact nonnegative duration resolved at the same canonical timestamp.
    pub resolved_duration_ms: Option<u64>,
    /// Optional exact expiration.
    pub expires_at: Option<TimestampMillis>,
    /// Whether the node waits for a trigger.
    pub wait_for_trigger: bool,
    /// Whether schedule creation must traverse consequential-action policy.
    pub consequential: bool,
    /// Exact compiled cancellation behavior.
    pub cancellation: GraphScheduleCancellationPolicy,
}

/// Builds the exact consequential action proposal for a graph `schedule` node.
///
/// Interceptor replacement is intentionally rejected by orchestration because
/// the resolved trigger and deterministic scheduler identity are immutable.
pub fn graph_schedule_action_proposal(
    resolved: &ResolvedGraphSchedule,
    style: &str,
    workspace: &str,
) -> Result<ActionProposal, GraphScheduleError> {
    if !resolved.consequential || style.trim().is_empty() || workspace.trim().is_empty() {
        return Err(GraphScheduleError::InvalidConfiguration);
    }
    Ok(ActionProposal {
        id: ProposalId(format!("graph-schedule:{}", resolved.idempotency_id)),
        action: ConsequentialAction::ScheduleCreation {
            schedule: resolved.trigger.canonical_text()?,
            style: style.to_owned(),
        },
        style: style.to_owned(),
        workspace: workspace.to_owned(),
        origin: String::from("runtime.node-executor"),
    })
}

/// Resolves a delay or schedule exactly once from runtime-owned canonical time.
///
/// The returned IDs bind the complete node-work identity, immutable execution
/// plan, compiled node configuration, and resolved trigger.
#[allow(
    clippy::too_many_lines,
    reason = "one auditable resolver keeps kind validation, canonical time resolution, and deterministic identity derivation inseparable"
)]
pub fn resolve_graph_schedule(
    session_id: SessionId,
    work: NodeWorkIdentity,
    executor: &SessionNodeExecutorResolution,
    execution_plan_hash: ContentHash,
    configuration: Option<&NodeConfiguration>,
    now: TimestampMillis,
) -> Result<ResolvedGraphSchedule, GraphScheduleError> {
    resolve_graph_schedule_with_identity_version(
        session_id,
        work,
        executor,
        execution_plan_hash,
        configuration,
        now,
        true,
    )
}

/// Reconstructs the identity used by graph-schedule resolution events written
/// before the canonical resolution timestamp and duration were payload fields.
///
/// This is replay-only compatibility. Live execution always uses
/// [`resolve_graph_schedule`] and therefore binds the persisted runtime values
/// into every derived scheduler identity.
pub(crate) fn resolve_legacy_graph_schedule(
    session_id: SessionId,
    work: NodeWorkIdentity,
    executor: &SessionNodeExecutorResolution,
    execution_plan_hash: ContentHash,
    configuration: Option<&NodeConfiguration>,
    now: TimestampMillis,
) -> Result<ResolvedGraphSchedule, GraphScheduleError> {
    resolve_graph_schedule_with_identity_version(
        session_id,
        work,
        executor,
        execution_plan_hash,
        configuration,
        now,
        false,
    )
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "one auditable resolver keeps live and legacy identity derivation beside the shared configuration validation"
)]
fn resolve_graph_schedule_with_identity_version(
    session_id: SessionId,
    work: NodeWorkIdentity,
    executor: &SessionNodeExecutorResolution,
    execution_plan_hash: ContentHash,
    configuration: Option<&NodeConfiguration>,
    now: TimestampMillis,
    bind_runtime_resolution: bool,
) -> Result<ResolvedGraphSchedule, GraphScheduleError> {
    if executor.node_id != work.node_id
        || executor.source != SessionNodeExecutorSource::Runtime
        || !matches!(
            native_executor_key(executor),
            Ok(NativeExecutorKey::Delay | NativeExecutorKey::Schedule)
        )
    {
        return Err(GraphScheduleError::InvalidExecutor);
    }
    let (trigger, resolved_duration_ms, expires_at, wait_for_trigger, consequential, cancellation) =
        match configuration {
            Some(NodeConfiguration::Delay {
                resolution,
                expiration_timestamp,
                cancellation,
            }) if native_executor_key(executor) == Ok(NativeExecutorKey::Delay) => {
                let (trigger, resolved_duration_ms) = match resolution {
                    DelayResolution::Duration { duration_ms } => {
                        let duration = i64::try_from(*duration_ms)
                            .map_err(|_| GraphScheduleError::InvalidConfiguration)?;
                        (
                            CanonicalGraphScheduleTrigger::AtMillis {
                                timestamp_ms: now
                                    .get()
                                    .checked_add(duration)
                                    .ok_or(GraphScheduleError::InvalidConfiguration)?,
                            },
                            Some(*duration_ms),
                        )
                    }
                    DelayResolution::WakeTimestamp { timestamp } => {
                        let timestamp_ms = parse_timestamp(timestamp)?;
                        let resolved_duration_ms = timestamp_ms
                            .checked_sub(now.get())
                            .and_then(|duration| u64::try_from(duration).ok());
                        (
                            CanonicalGraphScheduleTrigger::AtMillis { timestamp_ms },
                            resolved_duration_ms,
                        )
                    }
                };
                let expiration = expiration_timestamp
                    .as_deref()
                    .map(parse_timestamp)
                    .transpose()?
                    .map(TimestampMillis::new);
                let cancellation = match cancellation {
                    DelayCancellation::CancelContinuation => {
                        GraphScheduleCancellationPolicy::CancelContinuation
                    }
                    DelayCancellation::SuppressEffects => {
                        GraphScheduleCancellationPolicy::SuppressEffects
                    }
                };
                (
                    trigger,
                    resolved_duration_ms,
                    expiration,
                    true,
                    false,
                    cancellation,
                )
            }
            Some(NodeConfiguration::Schedule {
                trigger,
                wait_for_trigger,
                cancellation,
            }) if native_executor_key(executor) == Ok(NativeExecutorKey::Schedule) => {
                let (trigger, resolved_duration_ms) = match trigger {
                    GraphScheduleTrigger::At { timestamp } => {
                        let timestamp_ms = parse_timestamp(timestamp)?;
                        (
                            CanonicalGraphScheduleTrigger::AtMillis { timestamp_ms },
                            timestamp_ms
                                .checked_sub(now.get())
                                .and_then(|duration| u64::try_from(duration).ok()),
                        )
                    }
                    GraphScheduleTrigger::Interval {
                        interval_ms,
                        start_timestamp,
                    } => {
                        if *wait_for_trigger {
                            return Err(GraphScheduleError::RecurringWaitUnsupported);
                        }
                        let starts_at_ms = start_timestamp
                            .as_deref()
                            .map(parse_timestamp)
                            .transpose()?
                            .unwrap_or(now.get());
                        (
                            CanonicalGraphScheduleTrigger::Interval {
                                starts_at_ms,
                                every_ms: *interval_ms,
                            },
                            Some(*interval_ms),
                        )
                    }
                    GraphScheduleTrigger::RuntimeEvent { event_type } => (
                        CanonicalGraphScheduleTrigger::RuntimeEvent {
                            event_type: event_type.clone(),
                        },
                        None,
                    ),
                    GraphScheduleTrigger::ProcessOutput {
                        process_reference,
                        pattern,
                    } => (
                        CanonicalGraphScheduleTrigger::ProcessOutput {
                            process_id: process_reference.clone(),
                            contains: pattern.clone(),
                        },
                        None,
                    ),
                };
                let cancellation = match cancellation {
                    ScheduleCancellation::CancelTrigger => {
                        GraphScheduleCancellationPolicy::CancelTrigger
                    }
                    ScheduleCancellation::CancelWaitOnly => {
                        GraphScheduleCancellationPolicy::CancelWaitOnly
                    }
                };
                (
                    trigger,
                    resolved_duration_ms,
                    None,
                    *wait_for_trigger,
                    true,
                    cancellation,
                )
            }
            _ => return Err(GraphScheduleError::InvalidConfiguration),
        };
    if let (CanonicalGraphScheduleTrigger::AtMillis { timestamp_ms }, Some(expires_at)) =
        (&trigger, expires_at)
        && *timestamp_ms > expires_at.get()
    {
        return Err(GraphScheduleError::InvalidConfiguration);
    }
    let identity_material = if bind_runtime_resolution {
        serde_json::to_vec(&(
            session_id.to_string(),
            &work,
            execution_plan_hash,
            executor.adapter_configuration_reference,
            &trigger,
            now.get(),
            resolved_duration_ms,
            cancellation,
        ))
    } else {
        serde_json::to_vec(&(
            session_id.to_string(),
            &work,
            execution_plan_hash,
            executor.adapter_configuration_reference,
            &trigger,
            cancellation,
        ))
    }
    .map_err(|_| GraphScheduleError::InvalidConfiguration)?;
    let digest = ContentHash::digest(&identity_material);
    let schedule_id = format!("graph-schedule:{digest}");
    let continuation_id = wait_for_trigger.then(|| {
        let continuation_digest =
            ContentHash::digest(&[identity_material.as_slice(), b":continuation"].concat());
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&continuation_digest.as_bytes()[..16]);
        ContinuationId::from_uuid(Uuid::from_bytes(bytes))
    });
    Ok(ResolvedGraphSchedule {
        work,
        execution_plan_hash,
        configuration_hash: executor.adapter_configuration_reference,
        schedule_id,
        continuation_id,
        idempotency_id: digest.to_hex(),
        trigger,
        resolution_timestamp_ms: Some(now.get()),
        resolved_duration_ms,
        expires_at,
        wait_for_trigger,
        consequential,
        cancellation,
    })
}

fn parse_timestamp(value: &str) -> Result<i64, GraphScheduleError> {
    let value = OffsetDateTime::parse(value, &Rfc3339)
        .map_err(|_| GraphScheduleError::InvalidConfiguration)?;
    let millis = value.unix_timestamp_nanos() / 1_000_000;
    i64::try_from(millis).map_err(|_| GraphScheduleError::InvalidConfiguration)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScheduleTrigger {
    AtMillis(i64),
    Interval {
        starts_at_ms: i64,
        every_ms: u64,
    },
    RuntimeEvent {
        event_type: String,
    },
    ProcessOutput {
        process_id: String,
        contains: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchedulePayload {
    Prompt {
        prompt: String,
    },
    Continuation {
        continuation_id: String,
    },
    /// Runtime-owned graph trigger registration that does not synthesize a user turn.
    GraphTrigger {
        run_id: String,
        node_id: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpsertScheduleCommand {
    pub schedule_id: String,
    pub session_id: SessionId,
    pub idempotency_id: String,
    pub style: String,
    pub workspace: String,
    pub permission_policy: String,
    pub provider: String,
    pub model: String,
    pub token_budget: u64,
    pub cost_budget_micros: u64,
    pub trigger: ScheduleTrigger,
    pub payload: SchedulePayload,
    pub active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeSchedule {
    pub schedule_id: String,
    pub session_id: SessionId,
    pub idempotency_id: String,
    pub style: String,
    pub workspace: String,
    pub permission_policy: String,
    pub provider: String,
    pub model: String,
    pub token_budget: u64,
    pub cost_budget_micros: u64,
    pub trigger: ScheduleTrigger,
    pub payload: SchedulePayload,
    pub active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledExecution {
    pub execution_id: String,
    pub scheduled_for_ms: i64,
    pub claimed_at_ms: i64,
    pub observation: Option<ScheduleObservation>,
    pub schedule: RuntimeSchedule,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScheduleObservation {
    RuntimeEvent { event_id: String },
    ProcessOutput { output_id: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduleStoreResult {
    pub schedule_id: String,
    pub replayed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FireRuntimeEventCommand {
    pub source_session_id: SessionId,
    pub event_id: String,
    pub event_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FireProcessOutputCommand {
    pub source_session_id: SessionId,
    pub output_id: String,
    pub process_id: String,
    pub output: String,
}

pub trait RuntimeScheduleLogicPort: Send + Sync {
    fn upsert_schedule(
        &self,
        command: UpsertScheduleCommand,
    ) -> Result<ScheduleStoreResult, RuntimeScheduleLogicError>;
    fn remove_schedule(&self, schedule_id: &str) -> Result<bool, RuntimeScheduleLogicError>;
    fn list_schedules(&self, limit: u32)
    -> Result<Vec<RuntimeSchedule>, RuntimeScheduleLogicError>;
    fn claim_due_schedules(
        &self,
        limit: u32,
    ) -> Result<Vec<ScheduledExecution>, RuntimeScheduleLogicError>;
    fn list_pending_executions(
        &self,
        _limit: u32,
    ) -> Result<Vec<ScheduledExecution>, RuntimeScheduleLogicError> {
        Err(RuntimeScheduleLogicError::CorruptData)
    }
    fn fire_runtime_event(
        &self,
        command: FireRuntimeEventCommand,
    ) -> Result<Vec<ScheduledExecution>, RuntimeScheduleLogicError>;
    fn fire_process_output(
        &self,
        command: FireProcessOutputCommand,
    ) -> Result<Vec<ScheduledExecution>, RuntimeScheduleLogicError>;
    fn complete_scheduled_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
    ) -> Result<bool, RuntimeScheduleLogicError>;
}

impl<D: RuntimeScheduleDataPort> RuntimeScheduleLogicPort for crate::RuntimeLogic<D> {
    fn upsert_schedule(
        &self,
        command: UpsertScheduleCommand,
    ) -> Result<ScheduleStoreResult, RuntimeScheduleLogicError> {
        validate_schedule(&command)?;
        self.data
            .upsert_schedule(to_data(command))
            .map(|value| ScheduleStoreResult {
                schedule_id: value.schedule_id,
                replayed: value.replayed,
            })
            .map_err(RuntimeScheduleLogicError::Data)
    }

    fn remove_schedule(&self, schedule_id: &str) -> Result<bool, RuntimeScheduleLogicError> {
        validate_id(schedule_id)?;
        self.data
            .remove_schedule(schedule_id)
            .map_err(RuntimeScheduleLogicError::Data)
    }

    fn list_schedules(
        &self,
        limit: u32,
    ) -> Result<Vec<RuntimeSchedule>, RuntimeScheduleLogicError> {
        validate_limit(limit)?;
        self.data
            .list_schedules(limit)
            .map(|values| {
                values
                    .into_iter()
                    .map(from_data_schedule)
                    .collect::<Result<_, _>>()
            })
            .map_err(RuntimeScheduleLogicError::Data)?
    }

    fn claim_due_schedules(
        &self,
        limit: u32,
    ) -> Result<Vec<ScheduledExecution>, RuntimeScheduleLogicError> {
        validate_limit(limit)?;
        self.data
            .claim_due_schedules(limit)
            .map_err(RuntimeScheduleLogicError::Data)?
            .into_iter()
            .map(from_data_execution)
            .collect()
    }

    fn list_pending_executions(
        &self,
        limit: u32,
    ) -> Result<Vec<ScheduledExecution>, RuntimeScheduleLogicError> {
        validate_limit(limit)?;
        self.data
            .list_pending_executions(limit)
            .map_err(RuntimeScheduleLogicError::Data)?
            .into_iter()
            .map(from_data_execution)
            .collect()
    }

    fn fire_runtime_event(
        &self,
        command: FireRuntimeEventCommand,
    ) -> Result<Vec<ScheduledExecution>, RuntimeScheduleLogicError> {
        validate_id(&command.event_id)?;
        validate_text(&command.event_type, 256)?;
        self.data
            .fire_runtime_event(
                &command.source_session_id.to_string(),
                &command.event_id,
                &command.event_type,
            )
            .map_err(RuntimeScheduleLogicError::Data)?
            .into_iter()
            .map(from_data_execution)
            .collect()
    }

    fn fire_process_output(
        &self,
        command: FireProcessOutputCommand,
    ) -> Result<Vec<ScheduledExecution>, RuntimeScheduleLogicError> {
        validate_id(&command.output_id)?;
        validate_id(&command.process_id)?;
        if command.output.len() > 64 * 1024 {
            return Err(RuntimeScheduleLogicError::Invalid);
        }
        self.data
            .fire_process_output(
                &command.source_session_id.to_string(),
                &command.output_id,
                &command.process_id,
                &command.output,
            )
            .map_err(RuntimeScheduleLogicError::Data)?
            .into_iter()
            .map(from_data_execution)
            .collect()
    }

    fn complete_scheduled_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
    ) -> Result<bool, RuntimeScheduleLogicError> {
        validate_hash(execution_id)?;
        self.data
            .complete_scheduled_execution(execution_id, succeeded)
            .map_err(RuntimeScheduleLogicError::Data)
    }
}

fn validate_schedule(value: &UpsertScheduleCommand) -> Result<(), RuntimeScheduleLogicError> {
    validate_id(&value.schedule_id)?;
    validate_id(&value.idempotency_id)?;
    for text in [
        &value.style,
        &value.workspace,
        &value.permission_policy,
        &value.provider,
        &value.model,
    ] {
        validate_text(text, 4096)?;
    }
    match &value.trigger {
        ScheduleTrigger::AtMillis(value) if *value >= 0 => {}
        ScheduleTrigger::Interval {
            starts_at_ms,
            every_ms,
        } if *starts_at_ms >= 0 && *every_ms >= 1_000 => {}
        ScheduleTrigger::RuntimeEvent { event_type } => validate_text(event_type, 256)?,
        ScheduleTrigger::ProcessOutput {
            process_id,
            contains,
        } => {
            validate_id(process_id)?;
            validate_text(contains, 4096)?;
        }
        ScheduleTrigger::AtMillis(_) | ScheduleTrigger::Interval { .. } => {
            return Err(RuntimeScheduleLogicError::Invalid);
        }
    }
    match &value.payload {
        SchedulePayload::Prompt { prompt } => validate_text(prompt, 256 * 1024)?,
        SchedulePayload::Continuation { continuation_id } => validate_id(continuation_id)?,
        SchedulePayload::GraphTrigger { run_id, node_id } => {
            validate_id(run_id)?;
            validate_id(node_id)?;
        }
    }
    Ok(())
}

fn validate_limit(limit: u32) -> Result<(), RuntimeScheduleLogicError> {
    if (1..=1_000).contains(&limit) {
        Ok(())
    } else {
        Err(RuntimeScheduleLogicError::Invalid)
    }
}

fn validate_id(value: &str) -> Result<(), RuntimeScheduleLogicError> {
    if !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
    {
        Ok(())
    } else {
        Err(RuntimeScheduleLogicError::Invalid)
    }
}

fn validate_hash(value: &str) -> Result<(), RuntimeScheduleLogicError> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(RuntimeScheduleLogicError::Invalid)
    }
}

fn validate_text(value: &str, maximum: usize) -> Result<(), RuntimeScheduleLogicError> {
    if !value.trim().is_empty() && value.len() <= maximum && !value.contains('\0') {
        Ok(())
    } else {
        Err(RuntimeScheduleLogicError::Invalid)
    }
}

fn to_data(value: UpsertScheduleCommand) -> RuntimeScheduleDataRecord {
    RuntimeScheduleDataRecord {
        schedule_id: value.schedule_id,
        session_id: value.session_id.to_string(),
        idempotency_id: value.idempotency_id,
        style: value.style,
        workspace: value.workspace,
        permission_policy: value.permission_policy,
        provider: value.provider,
        model: value.model,
        token_budget: value.token_budget,
        cost_budget_micros: value.cost_budget_micros,
        trigger: match value.trigger {
            ScheduleTrigger::AtMillis(value) => ScheduleDataTrigger::AtMillis(value),
            ScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => ScheduleDataTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            ScheduleTrigger::RuntimeEvent { event_type } => {
                ScheduleDataTrigger::RuntimeEvent { event_type }
            }
            ScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            } => ScheduleDataTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            SchedulePayload::Prompt { prompt } => ScheduleDataPayload::Prompt { prompt },
            SchedulePayload::Continuation { continuation_id } => {
                ScheduleDataPayload::Continuation { continuation_id }
            }
            SchedulePayload::GraphTrigger { run_id, node_id } => {
                ScheduleDataPayload::GraphTrigger { run_id, node_id }
            }
        },
        active: value.active,
    }
}

fn from_data_schedule(
    value: RuntimeScheduleDataRecord,
) -> Result<RuntimeSchedule, RuntimeScheduleLogicError> {
    Ok(RuntimeSchedule {
        schedule_id: value.schedule_id,
        session_id: value
            .session_id
            .parse()
            .map_err(|_| RuntimeScheduleLogicError::CorruptData)?,
        idempotency_id: value.idempotency_id,
        style: value.style,
        workspace: value.workspace,
        permission_policy: value.permission_policy,
        provider: value.provider,
        model: value.model,
        token_budget: value.token_budget,
        cost_budget_micros: value.cost_budget_micros,
        trigger: match value.trigger {
            ScheduleDataTrigger::AtMillis(value) => ScheduleTrigger::AtMillis(value),
            ScheduleDataTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => ScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            ScheduleDataTrigger::RuntimeEvent { event_type } => {
                ScheduleTrigger::RuntimeEvent { event_type }
            }
            ScheduleDataTrigger::ProcessOutput {
                process_id,
                contains,
            } => ScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            ScheduleDataPayload::Prompt { prompt } => SchedulePayload::Prompt { prompt },
            ScheduleDataPayload::Continuation { continuation_id } => {
                SchedulePayload::Continuation { continuation_id }
            }
            ScheduleDataPayload::GraphTrigger { run_id, node_id } => {
                SchedulePayload::GraphTrigger { run_id, node_id }
            }
        },
        active: value.active,
    })
}

fn from_data_execution(
    value: ScheduledExecutionDataRecord,
) -> Result<ScheduledExecution, RuntimeScheduleLogicError> {
    Ok(ScheduledExecution {
        execution_id: value.execution_id,
        scheduled_for_ms: value.scheduled_for_ms,
        claimed_at_ms: value.claimed_at_ms,
        observation: value.observation.map(|observation| match observation {
            ScheduleDataObservation::RuntimeEvent { event_id } => {
                ScheduleObservation::RuntimeEvent { event_id }
            }
            ScheduleDataObservation::ProcessOutput { output_id } => {
                ScheduleObservation::ProcessOutput { output_id }
            }
        }),
        schedule: from_data_schedule(value.schedule)?,
    })
}

#[derive(Debug, Eq, Error, PartialEq)]
pub enum RuntimeScheduleLogicError {
    #[error("invalid schedule request")]
    Invalid,
    #[error("scheduler data is unavailable: {0}")]
    Data(#[source] RuntimeScheduleDataError),
    #[error("scheduler returned corrupt business data")]
    CorruptData,
}

/// Generic graph schedule resolution failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum GraphScheduleError {
    /// The persisted executor identity is not the exact native delay/schedule implementation.
    #[error("graph schedule executor identity is invalid")]
    InvalidExecutor,
    /// The compiled trigger cannot be represented by the durable scheduler.
    #[error("graph schedule configuration is invalid")]
    InvalidConfiguration,
    /// A recurring schedule cannot own a resume-once graph continuation.
    #[error("recurring graph schedules cannot wait for one trigger")]
    RecurringWaitUnsupported,
}

#[cfg(test)]
mod graph_schedule_tests {
    use agentmod_graph_engine::{DelayCancellation, ScheduleCancellation};

    use super::*;
    use crate::session::{
        SessionNodeExecutorBoundary, SessionNodeExecutorResolution, SessionNodeExecutorSource,
    };

    fn work() -> NodeWorkIdentity {
        NodeWorkIdentity {
            run_id: String::from("run-1"),
            node_id: String::from("pause"),
            branch_path: Vec::new(),
            attempt: 1,
            loop_iteration: 0,
            step: 1,
        }
    }

    fn executor(id: &str, kind: &str) -> SessionNodeExecutorResolution {
        SessionNodeExecutorResolution {
            node_id: String::from("pause"),
            node_kind: kind.to_owned(),
            executor_id: id.to_owned(),
            executor_version: String::from("1.0.0"),
            source: SessionNodeExecutorSource::Runtime,
            boundary: SessionNodeExecutorBoundary::RuntimeLogic,
            required_capabilities: vec![String::from("scheduling")],
            resolved_capabilities: vec![String::from("scheduling")],
            runtime_api_requirement: String::from("^1.0"),
            executor_declaration_hash: ContentHash::digest(id.as_bytes()),
            adapter_configuration_reference: ContentHash::digest(b"pause-config"),
        }
    }

    #[test]
    fn relative_delay_is_resolved_once_from_canonical_time() {
        let first = resolve_graph_schedule(
            SessionId::from_uuid(Uuid::from_u128(1)),
            work(),
            &executor("runtime.delay", "delay"),
            ContentHash::digest(b"plan"),
            Some(&NodeConfiguration::Delay {
                resolution: DelayResolution::Duration { duration_ms: 250 },
                expiration_timestamp: Some(String::from("1970-01-01T00:00:02Z")),
                cancellation: DelayCancellation::CancelContinuation,
            }),
            TimestampMillis::new(1_000),
        )
        .expect("resolve delay");
        let second = resolve_graph_schedule(
            SessionId::from_uuid(Uuid::from_u128(1)),
            work(),
            &executor("runtime.delay", "delay"),
            ContentHash::digest(b"plan"),
            Some(&NodeConfiguration::Delay {
                resolution: DelayResolution::Duration { duration_ms: 250 },
                expiration_timestamp: Some(String::from("1970-01-01T00:00:02Z")),
                cancellation: DelayCancellation::CancelContinuation,
            }),
            TimestampMillis::new(1_000),
        )
        .expect("resolve delay");
        assert_eq!(first, second);
        assert_eq!(
            first.trigger,
            CanonicalGraphScheduleTrigger::AtMillis {
                timestamp_ms: 1_250
            }
        );
        assert_eq!(first.expires_at, Some(TimestampMillis::new(2_000)));
        assert!(first.continuation_id.is_some());
        assert!(!first.consequential);
    }

    #[test]
    fn recurring_wait_is_rejected_but_nonwaiting_registration_is_supported() {
        let configuration = NodeConfiguration::Schedule {
            trigger: GraphScheduleTrigger::Interval {
                interval_ms: 1_000,
                start_timestamp: None,
            },
            wait_for_trigger: true,
            cancellation: ScheduleCancellation::CancelTrigger,
        };
        assert_eq!(
            resolve_graph_schedule(
                SessionId::from_uuid(Uuid::from_u128(1)),
                work(),
                &executor("runtime.schedule", "schedule"),
                ContentHash::digest(b"plan"),
                Some(&configuration),
                TimestampMillis::new(1_000),
            ),
            Err(GraphScheduleError::RecurringWaitUnsupported)
        );
        let NodeConfiguration::Schedule {
            trigger,
            cancellation,
            ..
        } = configuration
        else {
            unreachable!()
        };
        let resolved = resolve_graph_schedule(
            SessionId::from_uuid(Uuid::from_u128(1)),
            work(),
            &executor("runtime.schedule", "schedule"),
            ContentHash::digest(b"plan"),
            Some(&NodeConfiguration::Schedule {
                trigger,
                wait_for_trigger: false,
                cancellation,
            }),
            TimestampMillis::new(1_000),
        )
        .expect("nonwaiting recurring");
        assert!(resolved.continuation_id.is_none());
        assert!(resolved.consequential);
    }

    #[test]
    fn runtime_event_and_process_output_triggers_bind_exact_wait_conditions() {
        let cases = [
            (
                NodeConfiguration::Schedule {
                    trigger: GraphScheduleTrigger::RuntimeEvent {
                        event_type: String::from("user.build.ready"),
                    },
                    wait_for_trigger: true,
                    cancellation: ScheduleCancellation::CancelTrigger,
                },
                CanonicalGraphScheduleTrigger::RuntimeEvent {
                    event_type: String::from("user.build.ready"),
                },
                crate::continuation::ContinuationWakeCondition::RuntimeEvent {
                    event_type: String::from("user.build.ready"),
                    selector: None,
                },
            ),
            (
                NodeConfiguration::Schedule {
                    trigger: GraphScheduleTrigger::ProcessOutput {
                        process_reference: String::from("process-7"),
                        pattern: String::from("READY"),
                    },
                    wait_for_trigger: true,
                    cancellation: ScheduleCancellation::CancelWaitOnly,
                },
                CanonicalGraphScheduleTrigger::ProcessOutput {
                    process_id: String::from("process-7"),
                    contains: String::from("READY"),
                },
                crate::continuation::ContinuationWakeCondition::ProcessOutput {
                    process_id: String::from("process-7"),
                    pattern: String::from("READY"),
                },
            ),
        ];

        for (configuration, expected_trigger, expected_wake) in cases {
            let resolved = resolve_graph_schedule(
                SessionId::from_uuid(Uuid::from_u128(1)),
                work(),
                &executor("runtime.schedule", "schedule"),
                ContentHash::digest(b"plan"),
                Some(&configuration),
                TimestampMillis::new(1_000),
            )
            .expect("resolve waiting trigger");

            assert_eq!(resolved.trigger, expected_trigger);
            assert_eq!(
                resolved
                    .trigger
                    .to_wake_condition()
                    .expect("supported wait condition"),
                expected_wake
            );
            assert!(resolved.continuation_id.is_some());
            assert!(resolved.consequential);
        }
    }
}
