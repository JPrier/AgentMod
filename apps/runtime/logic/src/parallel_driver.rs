//! Pure coordination driver for canonical parallel branches and generic joins.
//!
//! This module never writes the journal. It validates replayed projections and
//! returns ordered canonical payloads plus typed effect requests to the single
//! outer journal coordinator.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use agentmod_graph_engine::{ExecutableGraph, ExecutableNode, NodeConfiguration, NodeKind};
use agentmod_primitives::{ContentHash, SessionId, TimestampMillis};
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;
use tokio::task::JoinSet;

use crate::{
    node_execution::{
        CanonicalBudgetState, CanonicalGraphState, ExecuteNodeCommand, NativeExecutorKey,
        NodeExecutionOutcome, NodeWorkIdentity, execute_native_node, native_executor_key,
    },
    parallel_execution::{
        JoinDecision, JoinMemberResult, ParallelBranchSpec, ParallelBranchState,
        ParallelBranchTransition, ParallelExecutionError, ParallelExecutionState,
        evaluate_bound_parallel_join,
    },
    session::{
        CanonicalParallelExecutionState, GenericJoinExecutionState, GenericJoinFailedEvent,
        GenericJoinInitializedEvent, GenericJoinLifecycleState, GenericJoinReadyEvent,
        GenericJoinTimedOutEvent, ParallelBranchControlState, ParallelBranchDispatchedEvent,
        ParallelBranchNodeCompletedEvent, ParallelBranchNodeEnteredEvent,
        ParallelBranchNodeFailedEvent, ParallelBranchRegionBinding, ParallelBranchStartedEvent,
        ParallelBranchTerminalDisposition, ParallelBranchTerminatedEvent,
        ParallelBranchTransitionSelectedEvent, ParallelCancellationCompletedEvent,
        ParallelCancellationRequestedEvent, ParallelExecutionInitializedEvent,
        RuntimeCommittedEvent, SessionNodeExecutorBoundary, SessionNodeExecutorResolution,
        SessionNodeExecutorSource, StyleExecutionContract,
    },
};

const MAX_DRIVER_BRANCHES: usize = 1_024;
const MAX_FAILURE_CODE_BYTES: usize = 256;

/// Exact initialization input supplied by the outer journal coordinator.
#[derive(Clone, Debug)]
pub struct InitializeParallelDriverCommand {
    /// Immutable compiled graph.
    pub graph: ExecutableGraph,
    /// Exact parallel-node work.
    pub owner: NodeWorkIdentity,
    /// Persisted executor selected for the parallel node.
    pub executor: SessionNodeExecutorResolution,
}

/// One replay-driven parallel advancement.
#[derive(Clone, Debug)]
pub struct DriveParallelCommand {
    /// Owning session.
    pub session_id: SessionId,
    /// Immutable compiled graph.
    pub graph: ExecutableGraph,
    /// Immutable execution contract; live registry lookup is prohibited.
    pub contract: StyleExecutionContract,
    /// Current canonical projection.
    pub parallel: CanonicalParallelExecutionState,
    /// Canonical variables used by pure nodes and transition conditions.
    pub variables: Value,
    /// Branch-authorized fresh canonical input keyed by stable branch ID.
    pub branch_variables: BTreeMap<String, Value>,
    /// Effective globally serialized graph-step ceiling.
    pub max_steps: u64,
    /// Optional canonical cancellation request.
    pub cancellation_code: Option<String>,
}

/// Generic join initialization input.
#[derive(Clone, Debug)]
pub struct InitializeJoinDriverCommand {
    /// Immutable compiled graph.
    pub graph: ExecutableGraph,
    /// Exact join work.
    pub owner: NodeWorkIdentity,
    /// Persisted join executor.
    pub executor: SessionNodeExecutorResolution,
    /// Canonical source fan-out.
    pub parallel: CanonicalParallelExecutionState,
    /// Canonical event timestamp used to resolve the timeout once.
    pub timestamp: TimestampMillis,
}

/// One replay-driven join evaluation.
#[derive(Clone, Debug)]
pub struct DriveJoinCommand {
    /// Immutable compiled graph.
    pub graph: ExecutableGraph,
    /// Current source fan-out.
    pub parallel: CanonicalParallelExecutionState,
    /// Current canonical join projection.
    pub join: GenericJoinExecutionState,
    /// Whether a durable timeout receipt is canonical.
    pub timeout_elapsed: bool,
}

/// Effect class delegated to the outer runtime coordinator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BranchEffectKind {
    /// Constrained user-space event emission.
    EmitEvent,
    /// Durable delay continuation.
    Delay,
    /// Consequential or waiting schedule.
    Schedule,
    /// Tool-host effect.
    Tool,
    /// User approval.
    Approval,
    /// Runtime-managed child orchestration.
    Child,
    /// Provider/context/artifact/review or another non-pure runtime boundary.
    OtherRuntimeEffect,
}

/// Exact dispatch boundary derived from the immutable executor resolution.
///
/// This is intentionally separate from [`BranchEffectKind`]: the latter is a
/// compatibility classification consumed by existing native adapters, while
/// this identity decides whether dispatch must cross the plugin-host boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BranchEffectDispatchClass {
    /// Runtime-owned adapter selected by the persisted plan.
    Runtime(BranchEffectKind),
    /// Isolated plugin-host executor selected by the persisted plan.
    Plugin,
}

/// Exact branch effect request. The driver does not execute or commit it.
#[derive(Clone, Debug, PartialEq)]
pub struct BranchEffectRequest {
    /// Stable branch identity.
    pub branch_id: String,
    /// Stable exactly-once dispatch identity.
    pub dispatch_id: String,
    /// Stable compiled member order used for branch-scoped writes.
    pub stable_order: u32,
    /// Exact active node work.
    pub work: NodeWorkIdentity,
    /// Exact persisted executor.
    pub executor: SessionNodeExecutorResolution,
    /// Immutable typed node configuration.
    pub configuration: Option<NodeConfiguration>,
    /// Typed external boundary.
    pub kind: BranchEffectKind,
}

impl BranchEffectRequest {
    /// Derives the authoritative dispatch boundary from the complete persisted
    /// executor identity; labels and graph topology are never consulted.
    ///
    /// # Errors
    ///
    /// Fails closed when source and process boundary disagree.
    pub fn dispatch_class(&self) -> Result<BranchEffectDispatchClass, ParallelDriverError> {
        match (&self.executor.source, self.executor.boundary) {
            (SessionNodeExecutorSource::Plugin { .. }, SessionNodeExecutorBoundary::PluginHost) => {
                Ok(BranchEffectDispatchClass::Plugin)
            }
            (SessionNodeExecutorSource::Runtime, SessionNodeExecutorBoundary::RuntimeLogic) => {
                Ok(BranchEffectDispatchClass::Runtime(self.kind))
            }
            (SessionNodeExecutorSource::Runtime, SessionNodeExecutorBoundary::PluginHost)
            | (
                SessionNodeExecutorSource::Plugin { .. },
                SessionNodeExecutorBoundary::RuntimeLogic,
            ) => Err(ParallelDriverError::ExecutorIdentity),
        }
    }
}

/// Recovery classification derived only from canonical state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BranchRecoveryClass {
    /// Never dispatched; safe to emit one dispatch intent.
    NotStarted,
    /// Dispatch is canonical; safe to acknowledge start once.
    Dispatched,
    /// Start is canonical; safe to enter the exact cursor.
    ReadyForEntry,
    /// Pure node can be recomputed; effect node must use its existing outbox.
    Active,
    /// Completion exists; safe to select the same compiled transition.
    AwaitingTransition,
    /// Transition exists; safe to enter or terminalize its exact destination.
    AwaitingDestination,
    /// Failure evidence exists; safe to terminalize without redispatch.
    AwaitingFailure,
    /// Canonically terminal.
    Terminal,
}

/// One inspectable recovery classification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchRecovery {
    /// Stable branch identity.
    pub branch_id: String,
    /// Replay-only classification.
    pub class: BranchRecoveryClass,
}

/// Validated driver output for the single journal coordinator.
#[derive(Clone, Debug, PartialEq)]
pub struct ParallelDriverOutput {
    /// Canonical payloads in the only legal commit order.
    pub events: Vec<RuntimeCommittedEvent>,
    /// Exact effect requests; the driver performed no effect.
    pub effect_requests: Vec<BranchEffectRequest>,
    /// Current stable recovery classification.
    pub recovery: Vec<BranchRecovery>,
    /// Whether queued work remains behind the concurrency bound.
    pub backpressured: bool,
}

/// Result returned by a pure branch executor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PureBranchExecutionError {
    /// Stable bounded diagnostic.
    pub code: String,
}

/// Injectable pure-node executor used for bounded concurrent evaluation.
#[async_trait]
pub trait PureBranchExecutor: Send + Sync + 'static {
    /// Executes one already validated, effect-free node.
    async fn execute(
        &self,
        command: ExecuteNodeCommand,
    ) -> Result<NodeExecutionOutcome, PureBranchExecutionError>;
}

/// Production executor for native effect-free runtime nodes.
#[derive(Clone, Copy, Debug, Default)]
pub struct NativePureBranchExecutor;

#[async_trait]
impl PureBranchExecutor for NativePureBranchExecutor {
    async fn execute(
        &self,
        command: ExecuteNodeCommand,
    ) -> Result<NodeExecutionOutcome, PureBranchExecutionError> {
        execute_native_node(&command).map_err(|error| PureBranchExecutionError {
            code: error.to_string(),
        })
    }
}

/// Builds the immutable parallel initialization payload.
///
/// # Errors
///
/// Fails when topology, write declarations, executor identity, or bounds do not
/// reproduce the compiler-owned fan-out exactly.
pub fn initialize_parallel(
    command: &InitializeParallelDriverCommand,
) -> Result<RuntimeCommittedEvent, ParallelDriverError> {
    let node = graph_node(&command.graph, &command.owner.node_id)?;
    if node.kind != NodeKind::ParallelBranch
        || native_executor_key(&command.executor) != Ok(NativeExecutorKey::Parallel)
        || command.executor.node_id != command.owner.node_id
    {
        return Err(ParallelDriverError::ExecutorIdentity);
    }
    let NodeConfiguration::ParallelBranch { join_target, .. } = node
        .configuration
        .as_ref()
        .ok_or(ParallelDriverError::Configuration)?
    else {
        return Err(ParallelDriverError::Configuration);
    };
    let outgoing = command
        .graph
        .edges
        .iter()
        .filter(|edge| edge.from == node.index)
        .collect::<Vec<_>>();
    if outgoing.len() < 2 || outgoing.len() > MAX_DRIVER_BRANCHES {
        return Err(ParallelDriverError::Configuration);
    }
    let mut specs = Vec::with_capacity(outgoing.len());
    let mut material = Vec::with_capacity(outgoing.len());
    for edge in outgoing {
        let target = command
            .graph
            .nodes
            .get(edge.to)
            .ok_or(ParallelDriverError::Topology)?;
        let reference = edge.label.clone().ok_or(ParallelDriverError::Topology)?;
        let nodes = branch_region(&command.graph, &target.id, join_target)?;
        let writes = region_writes(&command.graph, &nodes, |node| &node.write_variables);
        let resources = region_writes(&command.graph, &nodes, |node| &node.write_scopes);
        specs.push(ParallelBranchSpec {
            member_reference: reference,
            target_node_id: target.id.clone(),
            write_variables: writes.clone(),
            workspace_resources: resources.clone(),
        });
        material.push((nodes, writes, resources));
    }
    let pure = ParallelExecutionState::new(
        command.owner.clone(),
        node.configuration
            .as_ref()
            .ok_or(ParallelDriverError::Configuration)?,
        &specs,
        &command.graph.variables,
    )?;
    let regions = pure
        .member_bindings()
        .iter()
        .cloned()
        .zip(material)
        .map(
            |(member, (node_ids, write_variables, workspace_resources))| {
                let bytes = serde_json::to_vec(&(&command.owner, &member, &node_ids))
                    .map_err(ParallelDriverError::Serialization)?;
                Ok(ParallelBranchRegionBinding {
                    region_id: format!("parallel-region:{}", ContentHash::digest(&bytes)),
                    member,
                    node_ids,
                    variable_base_versions: write_variables
                        .iter()
                        .cloned()
                        .map(|name| (name, 0))
                        .collect(),
                    workspace_base_versions: workspace_resources
                        .iter()
                        .cloned()
                        .map(|name| (name, 0))
                        .collect(),
                    write_variables,
                    workspace_resources,
                })
            },
        )
        .collect::<Result<Vec<_>, ParallelDriverError>>()?;
    Ok(RuntimeCommittedEvent::ParallelExecutionInitialized(
        ParallelExecutionInitializedEvent {
            owner: command.owner.clone(),
            executor: command.executor.clone(),
            configuration_hash: configuration_hash(node.configuration.as_ref())?,
            regions,
        },
    ))
}

/// Advances one parallel projection without committing events or performing
/// external effects.
///
/// # Errors
///
/// Fails closed for substituted projections, exhausted budgets, illegal
/// recovery states, ambiguous transitions, or invalid pure outcomes.
pub async fn drive_parallel<E>(
    command: DriveParallelCommand,
    executor: Arc<E>,
) -> Result<ParallelDriverOutput, ParallelDriverError>
where
    E: PureBranchExecutor,
{
    validate_parallel_command(&command)?;
    let recovery = classify_recovery(&command.parallel);
    if let Some(code) = command.cancellation_code.as_deref() {
        return cancellation_output(&command.parallel, code, recovery);
    }

    let decision = command.parallel.execution.dispatch_decision();
    let repaired = recovery_events(&command)?;
    if !repaired.is_empty() {
        return Ok(ParallelDriverOutput {
            events: repaired,
            effect_requests: Vec::new(),
            recovery,
            backpressured: decision.backpressured,
        });
    }

    if !decision.ready.is_empty() {
        let mut events = Vec::with_capacity(decision.ready.len() * 3);
        let mut next_step = command.parallel.last_allocated_step;
        for ready in &decision.ready {
            next_step = checked_step(next_step, command.max_steps)?;
            events.push(RuntimeCommittedEvent::ParallelBranchDispatched(
                ParallelBranchDispatchedEvent {
                    owner: command.parallel.owner.clone(),
                    configuration_hash: command.parallel.configuration_hash,
                    branch_id: ready.branch_id.clone(),
                    dispatch_id: ready.dispatch_id.clone(),
                },
            ));
            events.push(RuntimeCommittedEvent::ParallelBranchStarted(
                ParallelBranchStartedEvent {
                    owner: command.parallel.owner.clone(),
                    configuration_hash: command.parallel.configuration_hash,
                    branch_id: ready.branch_id.clone(),
                    dispatch_id: ready.dispatch_id.clone(),
                    first_step: next_step,
                },
            ));
            events.push(RuntimeCommittedEvent::ParallelBranchNodeEntered(
                // A fan-out reached through a bounded revision loop belongs to
                // that exact revision; zero would alias the original branch.
                entered_event(
                    &command,
                    &ready.branch_id,
                    &ready.dispatch_id,
                    &ready.target_node_id,
                    1,
                    command.parallel.owner.loop_iteration,
                    next_step,
                )?,
            ));
        }
        return Ok(ParallelDriverOutput {
            events,
            effect_requests: Vec::new(),
            recovery,
            backpressured: decision.backpressured,
        });
    }

    let mut pure = Vec::new();
    let mut effects = Vec::new();
    for branch in ordered_branches(&command.parallel) {
        let ParallelBranchControlState::Active(entered) = &branch.control else {
            continue;
        };
        let node = graph_node(&command.graph, &entered.work.node_id)?;
        match pure_key(&entered.executor)? {
            Some(_) => pure.push((branch.region.member.branch_index, entered.clone())),
            None => effects.push(effect_request(branch, entered, node)?),
        }
    }
    let events = execute_pure_batch(&command, executor, pure).await?;
    Ok(ParallelDriverOutput {
        events,
        effect_requests: effects,
        recovery,
        backpressured: decision.backpressured,
    })
}

/// Builds the exact generic-join initialization payload.
///
/// # Errors
///
/// Fails for substituted owner/executor/configuration, mismatched fan-out
/// target, or timeout overflow.
pub fn initialize_join(
    command: &InitializeJoinDriverCommand,
) -> Result<RuntimeCommittedEvent, ParallelDriverError> {
    let node = graph_node(&command.graph, &command.owner.node_id)?;
    if node.kind != NodeKind::JoinResults
        || native_executor_key(&command.executor) != Ok(NativeExecutorKey::Join)
        || command.parallel.execution.join_target() != command.owner.node_id
    {
        return Err(ParallelDriverError::ExecutorIdentity);
    }
    let NodeConfiguration::JoinResults { timeout_ms, .. } = node
        .configuration
        .as_ref()
        .ok_or(ParallelDriverError::Configuration)?
    else {
        return Err(ParallelDriverError::Configuration);
    };
    let configuration_hash = configuration_hash(node.configuration.as_ref())?;
    let timeout_bytes = serde_json::to_vec(&(&command.owner, configuration_hash))
        .map_err(ParallelDriverError::Serialization)?;
    let delta = i64::try_from(*timeout_ms).map_err(|_| ParallelDriverError::Budget)?;
    let deadline_ms = command
        .timestamp
        .get()
        .checked_add(delta)
        .ok_or(ParallelDriverError::Budget)?;
    Ok(RuntimeCommittedEvent::GenericJoinInitialized(
        GenericJoinInitializedEvent {
            owner: command.owner.clone(),
            executor: command.executor.clone(),
            configuration_hash,
            parallel_owner: command.parallel.owner.clone(),
            member_bindings: command.parallel.execution.member_bindings().to_vec(),
            timeout_id: format!(
                "generic-join-timeout:{}",
                ContentHash::digest(&timeout_bytes)
            ),
            deadline_ms,
        },
    ))
}

/// Evaluates and, when terminal, proposes the exact canonical join event.
///
/// # Errors
///
/// Fails for mismatched projections, member bindings, policies, or timeout
/// identity.
pub fn drive_join(
    command: &DriveJoinCommand,
) -> Result<(JoinDecision, Vec<RuntimeCommittedEvent>), ParallelDriverError> {
    if command.join.parallel_owner != command.parallel.owner
        || command.join.member_bindings != command.parallel.execution.member_bindings()
        || command.join.lifecycle != GenericJoinLifecycleState::Waiting
    {
        return Err(ParallelDriverError::JoinProjection);
    }
    let parallel_node = graph_node(&command.graph, &command.parallel.owner.node_id)?;
    let join_node = graph_node(&command.graph, &command.join.owner.node_id)?;
    if configuration_hash(join_node.configuration.as_ref())? != command.join.configuration_hash {
        return Err(ParallelDriverError::JoinProjection);
    }
    let decision = evaluate_bound_parallel_join(
        parallel_node
            .configuration
            .as_ref()
            .ok_or(ParallelDriverError::Configuration)?,
        join_node
            .configuration
            .as_ref()
            .ok_or(ParallelDriverError::Configuration)?,
        &command.join.member_bindings,
        &command.parallel.execution.join_members(),
        command.timeout_elapsed,
    )?;
    let event = match &decision {
        JoinDecision::Waiting { .. } => None,
        JoinDecision::Ready(ready) => Some(RuntimeCommittedEvent::GenericJoinReady(
            GenericJoinReadyEvent {
                owner: command.join.owner.clone(),
                configuration_hash: command.join.configuration_hash,
                decision: ready.clone(),
            },
        )),
        JoinDecision::Failed(failure)
            if failure.reason == crate::parallel_execution::JoinTerminalReason::TimedOut =>
        {
            Some(RuntimeCommittedEvent::GenericJoinTimedOut(
                GenericJoinTimedOutEvent {
                    owner: command.join.owner.clone(),
                    configuration_hash: command.join.configuration_hash,
                    timeout_id: command.join.timeout.timeout_id.clone(),
                },
            ))
        }
        JoinDecision::Failed(failure) => Some(RuntimeCommittedEvent::GenericJoinFailed(
            GenericJoinFailedEvent {
                owner: command.join.owner.clone(),
                configuration_hash: command.join.configuration_hash,
                decision: failure.clone(),
            },
        )),
    };
    Ok((decision, event.into_iter().collect()))
}

/// Classifies every branch solely from replayed canonical control.
#[must_use]
pub fn classify_recovery(parallel: &CanonicalParallelExecutionState) -> Vec<BranchRecovery> {
    ordered_branches(parallel)
        .into_iter()
        .map(|branch| BranchRecovery {
            branch_id: branch.region.member.branch_id.clone(),
            class: match branch.control {
                ParallelBranchControlState::Queued => BranchRecoveryClass::NotStarted,
                ParallelBranchControlState::Dispatched => BranchRecoveryClass::Dispatched,
                ParallelBranchControlState::ReadyForEntry(_) => BranchRecoveryClass::ReadyForEntry,
                ParallelBranchControlState::Active(_) => BranchRecoveryClass::Active,
                ParallelBranchControlState::AwaitingTransition(_) => {
                    BranchRecoveryClass::AwaitingTransition
                }
                ParallelBranchControlState::AwaitingDestinationEntry(_) => {
                    BranchRecoveryClass::AwaitingDestination
                }
                ParallelBranchControlState::AwaitingFailure(_) => {
                    BranchRecoveryClass::AwaitingFailure
                }
                ParallelBranchControlState::Terminal { .. } => BranchRecoveryClass::Terminal,
            },
        })
        .collect()
}

fn validate_parallel_command(command: &DriveParallelCommand) -> Result<(), ParallelDriverError> {
    let node = graph_node(&command.graph, &command.parallel.owner.node_id)?;
    let expected = command
        .contract
        .node_executors
        .iter()
        .find(|resolution| resolution.node_id == node.id)
        .ok_or(ParallelDriverError::ExecutorIdentity)?;
    if command.parallel.owner.run_id != command.contract.run_id
        || expected != &command.parallel.executor
        || native_executor_key(expected) != Ok(NativeExecutorKey::Parallel)
        || configuration_hash(node.configuration.as_ref())? != command.parallel.configuration_hash
        || command.parallel.execution.work() != &command.parallel.owner
        || command.parallel.branches.len() > MAX_DRIVER_BRANCHES
    {
        return Err(ParallelDriverError::ExecutorIdentity);
    }
    Ok(())
}

fn recovery_events(
    command: &DriveParallelCommand,
) -> Result<Vec<RuntimeCommittedEvent>, ParallelDriverError> {
    let mut events = Vec::new();
    let mut next_step = command.parallel.last_allocated_step;
    for branch in ordered_branches(&command.parallel) {
        let pure_branch = command
            .parallel
            .execution
            .branches()
            .get(&branch.region.member.branch_id)
            .ok_or(ParallelDriverError::Projection)?;
        append_branch_recovery(command, branch, pure_branch, &mut next_step, &mut events)?;
    }
    Ok(events)
}

// Keeping the replay control cases together makes their legal event ordering
// auditable against `ParallelBranchControlState`.
#[allow(clippy::too_many_lines)]
fn append_branch_recovery(
    command: &DriveParallelCommand,
    branch: &crate::session::ParallelBranchReplayRecord,
    pure_branch: &crate::parallel_execution::ParallelBranchRecord,
    next_step: &mut u64,
    events: &mut Vec<RuntimeCommittedEvent>,
) -> Result<(), ParallelDriverError> {
    match &branch.control {
        ParallelBranchControlState::Dispatched => {
            *next_step = checked_step(*next_step, command.max_steps)?;
            events.push(RuntimeCommittedEvent::ParallelBranchStarted(
                ParallelBranchStartedEvent {
                    owner: command.parallel.owner.clone(),
                    configuration_hash: command.parallel.configuration_hash,
                    branch_id: pure_branch.branch_id.clone(),
                    dispatch_id: pure_branch.dispatch_id.clone(),
                    first_step: *next_step,
                },
            ));
            events.push(RuntimeCommittedEvent::ParallelBranchNodeEntered(
                // Crash-gap repair must reproduce the same inherited
                // revision as the original branch dispatch.
                entered_event(
                    command,
                    &pure_branch.branch_id,
                    &pure_branch.dispatch_id,
                    &pure_branch.target_node_id,
                    1,
                    command.parallel.owner.loop_iteration,
                    *next_step,
                )?,
            ));
        }
        ParallelBranchControlState::ReadyForEntry(cursor) => {
            events.push(RuntimeCommittedEvent::ParallelBranchNodeEntered(
                entered_event(
                    command,
                    &pure_branch.branch_id,
                    &pure_branch.dispatch_id,
                    &cursor.node_id,
                    cursor.attempt,
                    cursor.loop_iteration,
                    cursor.step,
                )?,
            ));
        }
        ParallelBranchControlState::AwaitingTransition(completed) => {
            *next_step = checked_step(*next_step, command.max_steps)?;
            let transition = select_transition(
                &command.graph,
                &completed.entered.work.node_id,
                completed
                    .result
                    .inline_value
                    .as_ref()
                    .or_else(|| command.branch_variables.get(&pure_branch.branch_id))
                    .unwrap_or(&command.variables),
            )?;
            events.push(RuntimeCommittedEvent::ParallelBranchTransitionSelected(
                ParallelBranchTransitionSelectedEvent {
                    owner: command.parallel.owner.clone(),
                    branch_id: pure_branch.branch_id.clone(),
                    from_node_id: completed.entered.work.node_id.clone(),
                    to_node_id: transition.clone(),
                    attempt: completed.entered.work.attempt,
                    loop_iteration: completed.entered.work.loop_iteration,
                    step: completed.entered.work.step,
                    next_step: *next_step,
                },
            ));
            if transition == command.parallel.execution.join_target() {
                events.push(branch_terminal_event(
                    command,
                    &pure_branch.branch_id,
                    &pure_branch.dispatch_id,
                    ParallelBranchTerminalDisposition::Completed,
                    Some(completed.result.clone()),
                    None,
                ));
            }
        }
        ParallelBranchControlState::AwaitingDestinationEntry(selected) => {
            if selected.to_node_id == command.parallel.execution.join_target() {
                events.push(branch_terminal_event(
                    command,
                    &pure_branch.branch_id,
                    &pure_branch.dispatch_id,
                    ParallelBranchTerminalDisposition::Completed,
                    branch.last_result.clone(),
                    None,
                ));
            } else {
                events.push(RuntimeCommittedEvent::ParallelBranchNodeEntered(
                    entered_event(
                        command,
                        &pure_branch.branch_id,
                        &pure_branch.dispatch_id,
                        &selected.to_node_id,
                        selected.attempt,
                        selected.loop_iteration,
                        selected.next_step,
                    )?,
                ));
            }
        }
        ParallelBranchControlState::AwaitingFailure(failed) => {
            events.push(branch_terminal_event(
                command,
                &pure_branch.branch_id,
                &pure_branch.dispatch_id,
                ParallelBranchTerminalDisposition::Failed,
                None,
                Some(failed.code.clone()),
            ));
        }
        ParallelBranchControlState::Queued
        | ParallelBranchControlState::Active(_)
        | ParallelBranchControlState::Terminal { .. } => {}
    }
    Ok(())
}

async fn execute_pure_batch<E>(
    command: &DriveParallelCommand,
    executor: Arc<E>,
    pure: Vec<(u32, ParallelBranchNodeEnteredEvent)>,
) -> Result<Vec<RuntimeCommittedEvent>, ParallelDriverError>
where
    E: PureBranchExecutor,
{
    let max_parallelism = parallel_limit(command)?;
    if pure.len() > max_parallelism {
        return Err(ParallelDriverError::ConcurrencyBound);
    }
    let mut tasks = JoinSet::new();
    for (index, entered) in pure {
        let executor = Arc::clone(&executor);
        let node = graph_node(&command.graph, &entered.work.node_id)?.clone();
        let session_id = command.session_id;
        let variables = command
            .branch_variables
            .get(&entered.branch_id)
            .cloned()
            .unwrap_or_else(|| command.variables.clone());
        let completed_node_ids = completed_branch_node_ids(&command.parallel);
        let max_steps = command.max_steps;
        tasks.spawn(async move {
            let remaining_steps = max_steps
                .checked_sub(entered.work.step)
                .and_then(|remaining| remaining.checked_add(1))
                .ok_or_else(|| PureBranchExecutionError {
                    code: String::from("step_budget_exhausted"),
                })?;
            let max_iterations = node.max_iterations;
            let result = executor
                .execute(ExecuteNodeCommand {
                    session_id,
                    work: entered.work.clone(),
                    executor: entered.executor.clone(),
                    configuration: node.configuration.clone(),
                    input: crate::node_execution::NodeExecutionInput {
                        transition_variables: variables,
                    },
                    graph_state: CanonicalGraphState {
                        attempt: entered.work.attempt,
                        loop_iteration: entered.work.loop_iteration,
                        step: entered.work.step,
                        completed_node_ids,
                    },
                    budget_state: CanonicalBudgetState {
                        max_steps,
                        remaining_steps,
                        max_iterations,
                        remaining_iterations: max_iterations
                            .map(|limit| limit.saturating_sub(entered.work.loop_iteration)),
                    },
                })
                .await?;
            Ok::<_, PureBranchExecutionError>((index, entered, result))
        });
    }
    let mut completed = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        completed.push(joined.map_err(|_| ParallelDriverError::WorkerJoin)??);
    }
    completed.sort_by_key(|(index, _, _)| *index);

    let mut next_step = command.parallel.last_allocated_step;
    let mut events = Vec::new();
    for (_, entered, outcome) in completed {
        append_pure_outcome(command, &entered, outcome, &mut next_step, &mut events)?;
    }
    Ok(events)
}

fn append_pure_outcome(
    command: &DriveParallelCommand,
    entered: &ParallelBranchNodeEnteredEvent,
    outcome: NodeExecutionOutcome,
    next_step: &mut u64,
    events: &mut Vec<RuntimeCommittedEvent>,
) -> Result<(), ParallelDriverError> {
    match outcome {
        NodeExecutionOutcome::Completed { output } => {
            let result = member_result(
                output.transition_variables.clone(),
                output.result_reference,
                output.artifact_reference,
            );
            events.push(RuntimeCommittedEvent::ParallelBranchNodeCompleted(
                ParallelBranchNodeCompletedEvent {
                    entered: entered.clone(),
                    result: result.clone(),
                },
            ));
            *next_step = checked_step(*next_step, command.max_steps)?;
            let destination = select_transition(
                &command.graph,
                &entered.work.node_id,
                &output.transition_variables,
            )?;
            events.push(RuntimeCommittedEvent::ParallelBranchTransitionSelected(
                ParallelBranchTransitionSelectedEvent {
                    owner: command.parallel.owner.clone(),
                    branch_id: entered.branch_id.clone(),
                    from_node_id: entered.work.node_id.clone(),
                    to_node_id: destination.clone(),
                    attempt: entered.work.attempt,
                    loop_iteration: entered.work.loop_iteration,
                    step: entered.work.step,
                    next_step: *next_step,
                },
            ));
            if destination == command.parallel.execution.join_target() {
                events.push(branch_terminal_event(
                    command,
                    &entered.branch_id,
                    &entered.dispatch_id,
                    ParallelBranchTerminalDisposition::Completed,
                    Some(result),
                    None,
                ));
            }
        }
        NodeExecutionOutcome::Terminal {
            outcome: crate::node_execution::SessionTermination::Failed,
        } => {
            let code = String::from("structured_failure");
            events.push(RuntimeCommittedEvent::ParallelBranchNodeFailed(
                ParallelBranchNodeFailedEvent {
                    entered: entered.clone(),
                    code: code.clone(),
                },
            ));
            events.push(branch_terminal_event(
                command,
                &entered.branch_id,
                &entered.dispatch_id,
                ParallelBranchTerminalDisposition::Failed,
                None,
                Some(code),
            ));
        }
        _ => return Err(ParallelDriverError::InvalidPureOutcome),
    }
    Ok(())
}

fn branch_terminal_event(
    command: &DriveParallelCommand,
    branch_id: &str,
    dispatch_id: &str,
    disposition: ParallelBranchTerminalDisposition,
    result: Option<JoinMemberResult>,
    code: Option<String>,
) -> RuntimeCommittedEvent {
    RuntimeCommittedEvent::ParallelBranchTerminated(ParallelBranchTerminatedEvent {
        owner: command.parallel.owner.clone(),
        configuration_hash: command.parallel.configuration_hash,
        branch_id: branch_id.to_owned(),
        dispatch_id: dispatch_id.to_owned(),
        disposition,
        result,
        code,
    })
}

fn cancellation_output(
    parallel: &CanonicalParallelExecutionState,
    code: &str,
    recovery: Vec<BranchRecovery>,
) -> Result<ParallelDriverOutput, ParallelDriverError> {
    validate_code(code)?;
    let transitions = parallel.execution.cancellation_transitions(code)?;
    let cancelled = transitions
        .iter()
        .map(|transition| match transition {
            ParallelBranchTransition::Cancel { branch_id, .. } => branch_id.clone(),
            _ => unreachable!("pure engine returns cancellation transitions"),
        })
        .collect::<Vec<_>>();
    let suppressed = cancelled
        .iter()
        .filter(|branch_id| {
            parallel
                .execution
                .branches()
                .get(*branch_id)
                .is_some_and(|branch| {
                    matches!(
                        branch.state,
                        ParallelBranchState::Dispatched | ParallelBranchState::Running
                    )
                })
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut events = Vec::new();
    if parallel.cancellation_requested_at.is_none() {
        events.push(RuntimeCommittedEvent::ParallelCancellationRequested(
            ParallelCancellationRequestedEvent {
                owner: parallel.owner.clone(),
                configuration_hash: parallel.configuration_hash,
                code: code.to_owned(),
                branch_ids: cancelled.clone(),
            },
        ));
    } else if parallel.cancellation_code.as_deref() != Some(code) {
        return Err(ParallelDriverError::CancellationConflict);
    }
    if parallel.cancellation_completed_at.is_none() {
        events.push(RuntimeCommittedEvent::ParallelCancellationCompleted(
            ParallelCancellationCompletedEvent {
                owner: parallel.owner.clone(),
                configuration_hash: parallel.configuration_hash,
                code: code.to_owned(),
                cancelled_branch_ids: cancelled,
                suppressed_effect_branch_ids: suppressed,
            },
        ));
    }
    Ok(ParallelDriverOutput {
        events,
        effect_requests: Vec::new(),
        recovery,
        backpressured: false,
    })
}

fn entered_event(
    command: &DriveParallelCommand,
    branch_id: &str,
    dispatch_id: &str,
    node_id: &str,
    attempt: u32,
    loop_iteration: u32,
    step: u64,
) -> Result<ParallelBranchNodeEnteredEvent, ParallelDriverError> {
    let node = graph_node(&command.graph, node_id)?;
    let loop_iteration = loop_iteration.max(command.parallel.owner.loop_iteration);
    let resolution = command
        .contract
        .node_executors
        .iter()
        .find(|resolution| resolution.node_id == node_id)
        .ok_or(ParallelDriverError::ExecutorIdentity)?;
    let mut branch_path = command.parallel.owner.branch_path.clone();
    branch_path.push(branch_id.to_owned());
    Ok(ParallelBranchNodeEnteredEvent {
        owner: command.parallel.owner.clone(),
        branch_id: branch_id.to_owned(),
        dispatch_id: dispatch_id.to_owned(),
        work: NodeWorkIdentity {
            run_id: command.contract.run_id.clone(),
            node_id: node_id.to_owned(),
            branch_path,
            attempt,
            loop_iteration,
            step,
        },
        executor: resolution.clone(),
        configuration_hash: configuration_hash(node.configuration.as_ref())?,
    })
}

fn effect_request(
    branch: &crate::session::ParallelBranchReplayRecord,
    entered: &ParallelBranchNodeEnteredEvent,
    node: &ExecutableNode,
) -> Result<BranchEffectRequest, ParallelDriverError> {
    let kind = match (&entered.executor.source, entered.executor.boundary) {
        (SessionNodeExecutorSource::Plugin { .. }, SessionNodeExecutorBoundary::PluginHost) => {
            BranchEffectKind::OtherRuntimeEffect
        }
        (SessionNodeExecutorSource::Runtime, SessionNodeExecutorBoundary::RuntimeLogic) => {
            let key = native_executor_key(&entered.executor)
                .map_err(|_| ParallelDriverError::ExecutorIdentity)?;
            match key {
                NativeExecutorKey::EventEmission => BranchEffectKind::EmitEvent,
                NativeExecutorKey::Delay => BranchEffectKind::Delay,
                NativeExecutorKey::Schedule => BranchEffectKind::Schedule,
                NativeExecutorKey::ToolGate => BranchEffectKind::Tool,
                NativeExecutorKey::UserApproval => BranchEffectKind::Approval,
                NativeExecutorKey::ChildSpawn
                | NativeExecutorKey::ChildMessage
                | NativeExecutorKey::ChildWait => BranchEffectKind::Child,
                NativeExecutorKey::Join | NativeExecutorKey::Parallel => {
                    BranchEffectKind::OtherRuntimeEffect
                }
                NativeExecutorKey::ContextConstruction
                | NativeExecutorKey::ModelRequest
                | NativeExecutorKey::Review
                | NativeExecutorKey::ArtifactPersistence
                | NativeExecutorKey::TurnCompletion
                | NativeExecutorKey::SessionCompletion => BranchEffectKind::OtherRuntimeEffect,
                NativeExecutorKey::Conditional
                | NativeExecutorKey::Loop
                | NativeExecutorKey::StructuredFailure => {
                    return Err(ParallelDriverError::Projection);
                }
            }
        }
        (SessionNodeExecutorSource::Runtime, SessionNodeExecutorBoundary::PluginHost)
        | (SessionNodeExecutorSource::Plugin { .. }, SessionNodeExecutorBoundary::RuntimeLogic) => {
            return Err(ParallelDriverError::ExecutorIdentity);
        }
    };
    Ok(BranchEffectRequest {
        branch_id: branch.region.member.branch_id.clone(),
        dispatch_id: entered.dispatch_id.clone(),
        stable_order: branch.region.member.branch_index,
        work: entered.work.clone(),
        executor: entered.executor.clone(),
        configuration: node.configuration.clone(),
        kind,
    })
}

fn pure_key(
    resolution: &SessionNodeExecutorResolution,
) -> Result<Option<NativeExecutorKey>, ParallelDriverError> {
    if matches!(
        (&resolution.source, resolution.boundary),
        (
            SessionNodeExecutorSource::Plugin { .. },
            SessionNodeExecutorBoundary::PluginHost
        )
    ) {
        return Ok(None);
    }
    let key = native_executor_key(resolution).map_err(|_| ParallelDriverError::ExecutorIdentity)?;
    Ok(matches!(
        key,
        NativeExecutorKey::Conditional
            | NativeExecutorKey::Loop
            | NativeExecutorKey::StructuredFailure
    )
    .then_some(key))
}

fn select_transition(
    graph: &ExecutableGraph,
    from_node_id: &str,
    variables: &Value,
) -> Result<String, ParallelDriverError> {
    let node = graph_node(graph, from_node_id)?;
    let mut eligible = graph
        .edges
        .iter()
        .filter(|edge| edge.from == node.index)
        .filter_map(|edge| {
            let eligible = edge
                .condition
                .as_ref()
                .map_or(Ok(true), |condition| condition.evaluate(variables));
            match eligible {
                Ok(true) => Some(Ok(edge)),
                Ok(false) => None,
                Err(error) => Some(Err(error)),
            }
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ParallelDriverError::Condition)?;
    if eligible.len() != 1 {
        return Err(ParallelDriverError::Transition);
    }
    let edge = eligible.pop().ok_or(ParallelDriverError::Transition)?;
    Ok(graph
        .nodes
        .get(edge.to)
        .ok_or(ParallelDriverError::Topology)?
        .id
        .clone())
}

fn graph_node<'a>(
    graph: &'a ExecutableGraph,
    node_id: &str,
) -> Result<&'a ExecutableNode, ParallelDriverError> {
    graph
        .nodes
        .iter()
        .find(|node| node.id == node_id)
        .ok_or(ParallelDriverError::Topology)
}

fn configuration_hash(
    configuration: Option<&NodeConfiguration>,
) -> Result<ContentHash, ParallelDriverError> {
    serde_json::to_vec(&configuration)
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(ParallelDriverError::Serialization)
}

fn branch_region(
    graph: &ExecutableGraph,
    entry: &str,
    join: &str,
) -> Result<BTreeSet<String>, ParallelDriverError> {
    let mut region = BTreeSet::new();
    let mut pending = vec![entry.to_owned()];
    while let Some(node_id) = pending.pop() {
        if node_id == join || !region.insert(node_id.clone()) {
            continue;
        }
        let node = graph_node(graph, &node_id)?;
        pending.extend(
            graph
                .edges
                .iter()
                .filter(|edge| edge.from == node.index)
                .map(|edge| graph.nodes[edge.to].id.clone()),
        );
    }
    if region.is_empty() {
        return Err(ParallelDriverError::Topology);
    }
    Ok(region)
}

fn region_writes<F>(
    graph: &ExecutableGraph,
    region: &BTreeSet<String>,
    select: F,
) -> BTreeSet<String>
where
    F: Fn(&ExecutableNode) -> &BTreeSet<String>,
{
    region
        .iter()
        .filter_map(|node_id| graph.nodes.iter().find(|node| &node.id == node_id))
        .flat_map(select)
        .cloned()
        .collect()
}

fn ordered_branches(
    parallel: &CanonicalParallelExecutionState,
) -> Vec<&crate::session::ParallelBranchReplayRecord> {
    let mut branches = parallel.branches.values().collect::<Vec<_>>();
    branches.sort_by_key(|branch| branch.region.member.branch_index);
    branches
}

fn parallel_limit(command: &DriveParallelCommand) -> Result<usize, ParallelDriverError> {
    let NodeConfiguration::ParallelBranch {
        max_parallelism,
        serialization_policy,
        ..
    } = graph_node(&command.graph, &command.parallel.owner.node_id)?
        .configuration
        .as_ref()
        .ok_or(ParallelDriverError::Configuration)?
    else {
        return Err(ParallelDriverError::Configuration);
    };
    Ok(if serialization_policy.is_some() {
        1
    } else {
        usize::try_from(*max_parallelism).unwrap_or(usize::MAX)
    })
}

fn completed_branch_node_ids(parallel: &CanonicalParallelExecutionState) -> Vec<String> {
    ordered_branches(parallel)
        .into_iter()
        .filter_map(|branch| match &branch.control {
            ParallelBranchControlState::AwaitingTransition(completed) => {
                Some(completed.entered.work.node_id.clone())
            }
            ParallelBranchControlState::AwaitingDestinationEntry(selected) => {
                Some(selected.from_node_id.clone())
            }
            _ => None,
        })
        .collect()
}

fn member_result(
    transition_variables: Value,
    result_reference: Option<String>,
    artifact_reference: Option<String>,
) -> JoinMemberResult {
    let artifact_references = artifact_reference.into_iter().collect::<BTreeSet<_>>();
    JoinMemberResult {
        inline_value: Some(transition_variables),
        node_result_reference: result_reference,
        declared_artifact_references: artifact_references.clone(),
        artifact_references,
    }
}

fn checked_step(current: u64, maximum: u64) -> Result<u64, ParallelDriverError> {
    let next = current.checked_add(1).ok_or(ParallelDriverError::Budget)?;
    (next <= maximum)
        .then_some(next)
        .ok_or(ParallelDriverError::Budget)
}

fn validate_code(code: &str) -> Result<(), ParallelDriverError> {
    if code.trim().is_empty()
        || code.len() > MAX_FAILURE_CODE_BYTES
        || code.chars().any(char::is_control)
    {
        return Err(ParallelDriverError::CancellationConflict);
    }
    Ok(())
}

/// Stable pure-driver failure.
#[derive(Debug, Error)]
pub enum ParallelDriverError {
    /// Compiled graph topology was not the retained structured fan-out.
    #[error("parallel driver topology is invalid")]
    Topology,
    /// Typed node configuration was absent or inconsistent.
    #[error("parallel driver configuration is invalid")]
    Configuration,
    /// Exact persisted executor identity was substituted.
    #[error("parallel driver executor identity is invalid")]
    ExecutorIdentity,
    /// Canonical projection was internally inconsistent.
    #[error("parallel driver projection is invalid")]
    Projection,
    /// Globally serialized graph-step budget was exhausted.
    #[error("parallel driver step budget is exhausted")]
    Budget,
    /// Concurrent pure work exceeded the compiled limit.
    #[error("parallel driver concurrency bound was exceeded")]
    ConcurrencyBound,
    /// Pure worker task did not return normally.
    #[error("parallel pure worker failed to join")]
    WorkerJoin,
    /// Pure node returned an effectful or otherwise illegal outcome.
    #[error("parallel pure node returned an invalid outcome")]
    InvalidPureOutcome,
    /// A branch condition could not be evaluated from canonical values.
    #[error("parallel branch condition evaluation failed")]
    Condition,
    /// A branch had zero or multiple eligible transitions.
    #[error("parallel branch transition is ambiguous or missing")]
    Transition,
    /// Cancellation did not match the canonical request.
    #[error("parallel cancellation conflicts with canonical state")]
    CancellationConflict,
    /// Generic join projection did not bind the source fan-out exactly.
    #[error("generic join projection is invalid")]
    JoinProjection,
    /// Pure parallel/join engine rejected the request.
    #[error("parallel engine rejected driver state: {0}")]
    Parallel(#[from] ParallelExecutionError),
    /// Canonical identity material could not serialize.
    #[error("parallel driver serialization failed: {0}")]
    Serialization(serde_json::Error),
    /// Pure executor returned a stable failure.
    #[error("parallel pure executor failed: {0:?}")]
    PureExecution(PureBranchExecutionError),
}

impl From<PureBranchExecutionError> for ParallelDriverError {
    fn from(value: PureBranchExecutionError) -> Self {
        Self::PureExecution(value)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use agentmod_graph_engine::{CompilerLimits, GraphCacheInputs, compile};
    use agentmod_primitives::{ContentHash, Sequence};
    use serde_json::json;
    use tokio::sync::Barrier;
    use uuid::Uuid;

    use crate::{
        node_execution::NodeExecutionOutput,
        session::{
            GenericJoinTimeoutState, ParallelBranchReplayRecord, SessionNodeExecutorBoundary,
            SessionNodeExecutorSource, SessionStyleBudgets,
        },
    };

    use super::*;

    fn session_id() -> SessionId {
        SessionId::from_uuid(Uuid::from_u128(7))
    }

    fn graph(right_delay: bool, max_parallelism: u32) -> ExecutableGraph {
        let right = if right_delay {
            r#"
[[nodes]]
id = "right"
kind = "delay"
configuration = { type = "delay", resolution = { kind = "duration", duration_ms = 1 }, cancellation = "cancel_continuation" }
"#
        } else {
            r#"
[[nodes]]
id = "right"
kind = "conditional_branch"
"#
        };
        compile(
            &format!(
                r#"
format_version = 1
entry = "parallel"
[budget]
max_steps = 100
max_tokens = 100
max_cost_micros = 100
max_duration_ms = 10000
[declarations]
capabilities = ["agents", "scheduling"]

[[nodes]]
id = "parallel"
kind = "parallel_branch"
configuration = {{ type = "parallel_branch", max_parallelism = {max_parallelism}, max_queue_depth = 2, join_target = "join", join_policy = "all" }}
[[nodes]]
id = "left"
kind = "conditional_branch"
{right}
[[nodes]]
id = "join"
kind = "join_results"
configuration = {{ type = "join_results", required = ["left-result", "right-result"], minimum_successes = 2, failure_policy = "wait_required", ordering_policy = "member_id", timeout_ms = 1000, cancellation_propagates = true, result_projection = "node_references", artifact_collection = "none" }}
[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "parallel"
to = "left"
label = "left-result"
[[edges]]
from = "parallel"
to = "right"
label = "right-result"
[[edges]]
from = "left"
to = "join"
[[edges]]
from = "right"
to = "join"
[[edges]]
from = "join"
to = "done"
"#
            ),
            &GraphCacheInputs {
                plugin_set_hash: ContentHash::digest(b"plugins"),
                runtime_api_version: String::from("1.0.0"),
                capability_set: BTreeSet::from([
                    String::from("agents"),
                    String::from("scheduling"),
                ]),
            },
            CompilerLimits::default(),
        )
        .expect("parallel graph")
    }

    fn resolution(node: &ExecutableNode) -> SessionNodeExecutorResolution {
        let executor_id = match node.kind {
            NodeKind::ParallelBranch => "runtime.parallel",
            NodeKind::JoinResults => "runtime.join",
            NodeKind::ConditionalBranch => "runtime.conditional",
            NodeKind::Delay => "runtime.delay",
            NodeKind::CompleteSession => "runtime.session-completion",
            _ => panic!("unexpected fixture node"),
        };
        SessionNodeExecutorResolution {
            node_id: node.id.clone(),
            node_kind: serde_json::to_value(node.kind)
                .expect("kind")
                .as_str()
                .expect("kind string")
                .to_owned(),
            executor_id: executor_id.to_owned(),
            executor_version: String::from("1.0.0"),
            source: SessionNodeExecutorSource::Runtime,
            boundary: SessionNodeExecutorBoundary::RuntimeLogic,
            required_capabilities: node.required_capabilities.iter().cloned().collect(),
            resolved_capabilities: vec![String::from("agents"), String::from("scheduling")],
            runtime_api_requirement: String::from("^1.0.0"),
            executor_declaration_hash: ContentHash::digest(executor_id.as_bytes()),
            adapter_configuration_reference: ContentHash::digest(
                &serde_json::to_vec(node).expect("node"),
            ),
        }
    }

    fn contract(graph: &ExecutableGraph) -> StyleExecutionContract {
        StyleExecutionContract {
            style_binding_hash: ContentHash::digest(b"binding"),
            execution_plan_hash: ContentHash::digest(b"plan"),
            registry_hash: ContentHash::digest(b"registry"),
            node_executors: graph.nodes.iter().map(resolution).collect(),
            initial_node_id: String::from("parallel"),
            initial_variables_json: String::from("{}"),
            invocation_provider: Some(String::from("mock")),
            invocation_model: Some(String::from("mock-model")),
            invocation_options_json: None,
            initial_budgets: SessionStyleBudgets {
                max_iterations: 10,
                max_steps: 100,
                max_tokens: 100,
                max_cost_micros: 100,
                max_duration_ms: 10_000,
            },
            run_id: String::from("run:parallel"),
        }
    }

    fn owner() -> NodeWorkIdentity {
        NodeWorkIdentity {
            run_id: String::from("run:parallel"),
            node_id: String::from("parallel"),
            branch_path: Vec::new(),
            attempt: 1,
            loop_iteration: 0,
            step: 1,
        }
    }

    fn initialized_state(graph: &ExecutableGraph) -> CanonicalParallelExecutionState {
        initialized_state_for_owner(graph, owner())
    }

    fn initialized_state_for_owner(
        graph: &ExecutableGraph,
        owner: NodeWorkIdentity,
    ) -> CanonicalParallelExecutionState {
        let contract = contract(graph);
        let RuntimeCommittedEvent::ParallelExecutionInitialized(initialized) =
            initialize_parallel(&InitializeParallelDriverCommand {
                graph: graph.clone(),
                owner: owner.clone(),
                executor: contract
                    .node_executors
                    .iter()
                    .find(|item| item.node_id == "parallel")
                    .expect("parallel executor")
                    .clone(),
            })
            .expect("initialize")
        else {
            panic!("parallel event");
        };
        let parallel_node = graph_node(graph, "parallel").expect("parallel");
        let specs = initialized
            .regions
            .iter()
            .map(|region| ParallelBranchSpec {
                member_reference: region.member.configured_reference.clone(),
                target_node_id: region.member.target_node_id.clone(),
                write_variables: region.write_variables.clone(),
                workspace_resources: region.workspace_resources.clone(),
            })
            .collect::<Vec<_>>();
        let execution = ParallelExecutionState::new(
            owner.clone(),
            parallel_node.configuration.as_ref().expect("config"),
            &specs,
            &graph.variables,
        )
        .expect("parallel state");
        CanonicalParallelExecutionState {
            owner,
            executor: initialized.executor,
            configuration_hash: initialized.configuration_hash,
            execution,
            branches: initialized
                .regions
                .into_iter()
                .map(|region| {
                    (
                        region.member.branch_id.clone(),
                        ParallelBranchReplayRecord {
                            region,
                            control: ParallelBranchControlState::Queued,
                            entered_at: None,
                            effect: None,
                            last_result: None,
                            terminal_at: None,
                            cancellation_requested_at: None,
                            suppression_at: None,
                        },
                    )
                })
                .collect(),
            variable_contributions: BTreeMap::default(),
            initialized_at: Sequence::new(4).expect("sequence"),
            last_allocated_step: 1,
            cancellation_code: None,
            cancellation_requested_at: None,
            cancellation_completed_at: None,
        }
    }

    fn activate_all(
        graph: &ExecutableGraph,
        mut state: CanonicalParallelExecutionState,
    ) -> CanonicalParallelExecutionState {
        let contract = contract(graph);
        let mut step = state.last_allocated_step;
        let bindings = state.execution.member_bindings().to_vec();
        for binding in bindings {
            let pure = state
                .execution
                .branches()
                .get(&binding.branch_id)
                .expect("branch")
                .clone();
            state
                .execution
                .apply(ParallelBranchTransition::Dispatch {
                    branch_id: pure.branch_id.clone(),
                    dispatch_id: pure.dispatch_id.clone(),
                })
                .expect("dispatch");
            state
                .execution
                .apply(ParallelBranchTransition::Start {
                    branch_id: pure.branch_id.clone(),
                    dispatch_id: pure.dispatch_id.clone(),
                })
                .expect("start");
            step += 1;
            let node = graph_node(graph, &binding.target_node_id).expect("node");
            let branch = state.branches.get_mut(&binding.branch_id).expect("branch");
            branch.control = ParallelBranchControlState::Active(ParallelBranchNodeEnteredEvent {
                owner: owner(),
                branch_id: binding.branch_id.clone(),
                dispatch_id: pure.dispatch_id,
                work: NodeWorkIdentity {
                    run_id: String::from("run:parallel"),
                    node_id: binding.target_node_id,
                    branch_path: vec![binding.branch_id],
                    attempt: 1,
                    loop_iteration: 0,
                    step,
                },
                executor: contract
                    .node_executors
                    .iter()
                    .find(|item| item.node_id == node.id)
                    .expect("node executor")
                    .clone(),
                configuration_hash: configuration_hash(node.configuration.as_ref()).expect("hash"),
            });
        }
        state.last_allocated_step = step;
        state
    }

    fn command(
        graph: &ExecutableGraph,
        parallel: CanonicalParallelExecutionState,
    ) -> DriveParallelCommand {
        DriveParallelCommand {
            session_id: session_id(),
            graph: graph.clone(),
            contract: contract(graph),
            parallel,
            variables: json!({}),
            branch_variables: BTreeMap::new(),
            max_steps: 100,
            cancellation_code: None,
        }
    }

    #[derive(Clone)]
    struct BarrierExecutor {
        barrier: Arc<Barrier>,
        current: Arc<AtomicUsize>,
        maximum: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl PureBranchExecutor for BarrierExecutor {
        async fn execute(
            &self,
            _command: ExecuteNodeCommand,
        ) -> Result<NodeExecutionOutcome, PureBranchExecutionError> {
            let active = self.current.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(active, Ordering::SeqCst);
            self.barrier.wait().await;
            self.current.fetch_sub(1, Ordering::SeqCst);
            Ok(NodeExecutionOutcome::Completed {
                output: NodeExecutionOutput {
                    result_reference: Some(String::from("pure-result")),
                    artifact_reference: None,
                    transition_variables: json!({}),
                },
            })
        }
    }

    #[tokio::test]
    async fn pure_nodes_overlap_with_bound_and_commit_in_stable_branch_order() {
        let graph = graph(false, 2);
        let state = activate_all(&graph, initialized_state(&graph));
        let maximum = Arc::new(AtomicUsize::new(0));
        let output = drive_parallel(
            command(&graph, state),
            Arc::new(BarrierExecutor {
                barrier: Arc::new(Barrier::new(2)),
                current: Arc::new(AtomicUsize::new(0)),
                maximum: Arc::clone(&maximum),
            }),
        )
        .await
        .expect("drive");
        assert_eq!(maximum.load(Ordering::SeqCst), 2);
        let completed = output
            .events
            .iter()
            .filter_map(|event| match event {
                RuntimeCommittedEvent::ParallelBranchNodeCompleted(event) => {
                    Some(event.entered.work.node_id.as_str())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(completed, ["left", "right"]);
        assert!(output.effect_requests.is_empty());
    }

    #[tokio::test]
    async fn queued_dispatch_is_bounded_ordered_and_restart_suppresses_duplicate_intent() {
        let graph = graph(false, 1);
        let state = initialized_state(&graph);
        let output = drive_parallel(
            command(&graph, state.clone()),
            Arc::new(NativePureBranchExecutor),
        )
        .await
        .expect("dispatch");
        assert_eq!(
            output
                .events
                .iter()
                .filter(|event| matches!(event, RuntimeCommittedEvent::ParallelBranchDispatched(_)))
                .count(),
            1
        );
        assert!(output.backpressured);

        let first = state.execution.dispatch_decision().ready[0].clone();
        let mut restarted = state;
        restarted
            .execution
            .apply(ParallelBranchTransition::Dispatch {
                branch_id: first.branch_id.clone(),
                dispatch_id: first.dispatch_id.clone(),
            })
            .expect("dispatch");
        restarted
            .branches
            .get_mut(&first.branch_id)
            .expect("branch")
            .control = ParallelBranchControlState::Dispatched;
        let recovered = drive_parallel(
            command(&graph, restarted),
            Arc::new(NativePureBranchExecutor),
        )
        .await
        .expect("recover");
        assert!(
            recovered
                .events
                .iter()
                .all(|event| !matches!(event, RuntimeCommittedEvent::ParallelBranchDispatched(_)))
        );
        assert!(matches!(
            recovered.events.first(),
            Some(RuntimeCommittedEvent::ParallelBranchStarted(_))
        ));
    }

    #[tokio::test]
    async fn initial_and_recovered_branch_work_inherit_parallel_owner_loop_iteration() {
        let graph = graph(false, 1);
        let mut revised_owner = owner();
        revised_owner.loop_iteration = 3;
        let state = initialized_state_for_owner(&graph, revised_owner);

        let output = drive_parallel(
            command(&graph, state.clone()),
            Arc::new(NativePureBranchExecutor),
        )
        .await
        .expect("dispatch revised branch");
        let entered = output
            .events
            .iter()
            .find_map(|event| match event {
                RuntimeCommittedEvent::ParallelBranchNodeEntered(entered) => Some(entered),
                _ => None,
            })
            .expect("initial branch entry");
        assert_eq!(entered.work.loop_iteration, 3);

        let first = state.execution.dispatch_decision().ready[0].clone();
        let mut restarted = state;
        restarted
            .execution
            .apply(ParallelBranchTransition::Dispatch {
                branch_id: first.branch_id.clone(),
                dispatch_id: first.dispatch_id.clone(),
            })
            .expect("dispatch");
        restarted
            .branches
            .get_mut(&first.branch_id)
            .expect("branch")
            .control = ParallelBranchControlState::Dispatched;
        let recovered = drive_parallel(
            command(&graph, restarted),
            Arc::new(NativePureBranchExecutor),
        )
        .await
        .expect("recover revised branch");
        let entered = recovered
            .events
            .iter()
            .find_map(|event| match event {
                RuntimeCommittedEvent::ParallelBranchNodeEntered(entered) => Some(entered),
                _ => None,
            })
            .expect("recovered branch entry");
        assert_eq!(entered.work.loop_iteration, 3);
    }

    #[tokio::test]
    async fn cancellation_is_stable_and_completed_receipt_suppresses_duplicates() {
        let graph = graph(false, 2);
        let mut state = activate_all(&graph, initialized_state(&graph));
        let mut cancel = command(&graph, state.clone());
        cancel.cancellation_code = Some(String::from("parent_cancelled"));
        let output = drive_parallel(cancel, Arc::new(NativePureBranchExecutor))
            .await
            .expect("cancel");
        assert!(matches!(
            output.events.as_slice(),
            [
                RuntimeCommittedEvent::ParallelCancellationRequested(_),
                RuntimeCommittedEvent::ParallelCancellationCompleted(_)
            ]
        ));
        state.cancellation_code = Some(String::from("parent_cancelled"));
        state.cancellation_requested_at = Some(Sequence::new(10).expect("sequence"));
        state.cancellation_completed_at = Some(Sequence::new(11).expect("sequence"));
        for branch in state
            .execution
            .cancellation_transitions("parent_cancelled")
            .expect("cancel")
        {
            state.execution.apply(branch).expect("apply cancellation");
        }
        let mut duplicate = command(&graph, state);
        duplicate.cancellation_code = Some(String::from("parent_cancelled"));
        let output = drive_parallel(duplicate, Arc::new(NativePureBranchExecutor))
            .await
            .expect("duplicate");
        assert!(output.events.is_empty());
    }

    #[tokio::test]
    async fn step_budget_fails_before_dispatch_and_mixed_work_returns_effect_request() {
        let pure_graph = graph(false, 2);
        let mut exhausted = command(&pure_graph, initialized_state(&pure_graph));
        exhausted.max_steps = 1;
        assert!(matches!(
            drive_parallel(exhausted, Arc::new(NativePureBranchExecutor)).await,
            Err(ParallelDriverError::Budget)
        ));

        let mixed_graph = graph(true, 2);
        let mixed = activate_all(&mixed_graph, initialized_state(&mixed_graph));
        let output = drive_parallel(
            command(&mixed_graph, mixed),
            Arc::new(NativePureBranchExecutor),
        )
        .await
        .expect("mixed");
        assert_eq!(output.effect_requests.len(), 1);
        assert_eq!(output.effect_requests[0].kind, BranchEffectKind::Delay);
        assert!(
            output.events.iter().any(|event| matches!(
                event,
                RuntimeCommittedEvent::ParallelBranchNodeCompleted(_)
            ))
        );
    }

    #[tokio::test]
    async fn plugin_branch_is_classified_only_from_exact_persisted_source_and_boundary() {
        let graph = graph(true, 2);
        let mut state = activate_all(&graph, initialized_state(&graph));
        let right = state
            .branches
            .values_mut()
            .find(|branch| branch.region.member.target_node_id == "right")
            .expect("right branch");
        let ParallelBranchControlState::Active(entered) = &mut right.control else {
            panic!("right active");
        };
        entered.executor.executor_id = String::from("fixture.plugin-delay");
        entered.executor.executor_version = String::from("7.4.0");
        entered.executor.source = SessionNodeExecutorSource::Plugin {
            plugin_id: String::from("fixture.plugin"),
        };
        entered.executor.boundary = SessionNodeExecutorBoundary::PluginHost;
        entered.executor.executor_declaration_hash = ContentHash::digest(b"plugin-declaration");
        let plugin_resolution = entered.executor.clone();
        let stable_order = right.region.member.branch_index;

        let mut command = command(&graph, state);
        *command
            .contract
            .node_executors
            .iter_mut()
            .find(|resolution| resolution.node_id == "right")
            .expect("right resolution") = plugin_resolution;
        let output = drive_parallel(command, Arc::new(NativePureBranchExecutor))
            .await
            .expect("plugin request");
        let request = output.effect_requests.first().expect("plugin effect");
        assert_eq!(request.kind, BranchEffectKind::OtherRuntimeEffect);
        assert_eq!(
            request.dispatch_class().expect("dispatch class"),
            BranchEffectDispatchClass::Plugin
        );
        assert_eq!(request.stable_order, stable_order);

        let mut drifted = request.clone();
        drifted.executor.boundary = SessionNodeExecutorBoundary::RuntimeLogic;
        assert!(matches!(
            drifted.dispatch_class(),
            Err(ParallelDriverError::ExecutorIdentity)
        ));
    }

    #[test]
    fn join_ready_and_timeout_are_derived_from_exact_persisted_bindings() {
        let graph = graph(false, 2);
        let mut parallel = activate_all(&graph, initialized_state(&graph));
        let bindings = parallel.execution.member_bindings().to_vec();
        for (index, binding) in bindings.iter().enumerate() {
            let result = JoinMemberResult {
                inline_value: None,
                node_result_reference: Some(format!("result:{index}")),
                artifact_references: BTreeSet::new(),
                declared_artifact_references: BTreeSet::new(),
            };
            let transition = parallel
                .execution
                .completion_transition(&binding.branch_id, result)
                .expect("complete");
            parallel.execution.apply(transition).expect("apply");
        }
        let contract = contract(&graph);
        let join_owner = NodeWorkIdentity {
            run_id: String::from("run:parallel"),
            node_id: String::from("join"),
            branch_path: Vec::new(),
            attempt: 1,
            loop_iteration: 0,
            step: 8,
        };
        let RuntimeCommittedEvent::GenericJoinInitialized(initialized) =
            initialize_join(&InitializeJoinDriverCommand {
                graph: graph.clone(),
                owner: join_owner.clone(),
                executor: contract
                    .node_executors
                    .iter()
                    .find(|item| item.node_id == "join")
                    .expect("join executor")
                    .clone(),
                parallel: parallel.clone(),
                timestamp: TimestampMillis::new(1_000),
            })
            .expect("join initialization")
        else {
            panic!("join init event");
        };
        let join = GenericJoinExecutionState {
            owner: join_owner,
            executor: initialized.executor,
            configuration_hash: initialized.configuration_hash,
            parallel_owner: owner(),
            member_bindings: initialized.member_bindings,
            timeout: GenericJoinTimeoutState {
                timeout_id: initialized.timeout_id,
                deadline_ms: initialized.deadline_ms,
                elapsed_at: None,
            },
            lifecycle: GenericJoinLifecycleState::Waiting,
            initialized_at: Sequence::new(20).expect("sequence"),
            terminal_at: None,
        };
        let (decision, events) = drive_join(&DriveJoinCommand {
            graph,
            parallel,
            join,
            timeout_elapsed: false,
        })
        .expect("join");
        assert!(matches!(decision, JoinDecision::Ready(_)));
        assert!(matches!(
            events.as_slice(),
            [RuntimeCommittedEvent::GenericJoinReady(_)]
        ));
    }

    #[test]
    fn initialization_is_deterministic_and_preserves_exact_member_bindings() {
        let graph = graph(false, 2);
        let contract = contract(&graph);
        let command = InitializeParallelDriverCommand {
            graph,
            owner: owner(),
            executor: contract
                .node_executors
                .iter()
                .find(|item| item.node_id == "parallel")
                .expect("parallel executor")
                .clone(),
        };
        let left = initialize_parallel(&command).expect("left");
        let right = initialize_parallel(&command).expect("right");
        assert_eq!(left, right);
        let RuntimeCommittedEvent::ParallelExecutionInitialized(initialized) = left else {
            panic!("parallel init");
        };
        let references = initialized
            .regions
            .iter()
            .map(|region| region.member.configured_reference.as_str())
            .collect::<Vec<_>>();
        assert_eq!(references, ["left-result", "right-result"]);
        assert_eq!(
            initialized
                .regions
                .iter()
                .map(|region| region.member.branch_id.as_str())
                .collect::<BTreeSet<_>>()
                .len(),
            2
        );
    }

    #[test]
    fn recovery_classification_is_projection_only_and_stably_ordered() {
        let graph = graph(false, 2);
        let state = initialized_state(&graph);
        let recovery = classify_recovery(&state);
        let expected = state
            .execution
            .member_bindings()
            .iter()
            .map(|binding| binding.branch_id.clone())
            .collect::<Vec<_>>();
        assert_eq!(recovery.len(), 2);
        assert!(
            recovery
                .iter()
                .all(|item| item.class == BranchRecoveryClass::NotStarted)
        );
        assert_eq!(
            recovery
                .iter()
                .map(|item| item.branch_id.clone())
                .collect::<Vec<_>>(),
            expected
        );
    }
}
