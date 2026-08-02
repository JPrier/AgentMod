//! Live canonical-variable operation preparation and recovery classification.
//!
//! This module is deliberately journal-agnostic. It validates one exact graph
//! work identity against the compiled graph, delegates value and event
//! canonicalization to [`CanonicalVariableEventReducer`], and returns typed
//! runtime payloads for an outer coordinator to commit.

use std::collections::{BTreeMap, BTreeSet};

use agentmod_graph_engine::{
    ExecutableGraph, ExecutableNode, VariableDeclaration, VariableValueType,
};
use serde_json::Value;
use thiserror::Error;

use crate::{
    canonical_variables::{
        BranchVariableValue, BranchWriteContext, CanonicalVariableEvent,
        CanonicalVariableEventReducer, CanonicalVariableValue, InitialAssignmentAuditState,
        VariableValidationAttempt, VariableWriter, canonical_value_from_json,
    },
    node_execution::NodeWorkIdentity,
    session::RuntimeCommittedEvent,
};

const MAX_BRANCH_DEPTH: usize = 64;
const MAX_IDENTITY_BYTES: usize = 512;
const MAX_NODE_OUTPUT_BYTES: usize = 256 * 1024;

/// One validated canonical variable operation bound to exact graph work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoordinatedVariableOperation {
    /// Runtime-owned graph declaration.
    Declare {
        /// Exact compiled declaration.
        declaration: VariableDeclaration,
    },
    /// Runtime input or node output assignment.
    Assign {
        /// Declared variable.
        variable: String,
        /// Optimistic current version, absent for the first assignment.
        expected_version: Option<u64>,
        /// Bounded typed value.
        value: CanonicalVariableValue,
        /// Exact branch write context for branch work.
        branch: Option<BranchWriteContext>,
    },
    /// Runtime-recorded timestamp or duration attributed to this exact node.
    AssignRuntimeRecorded {
        /// Declared variable.
        variable: String,
        /// Optimistic current version.
        expected_version: Option<u64>,
        /// Exact runtime-recorded timestamp or duration.
        value: CanonicalVariableValue,
        /// Exact branch write context for branch work.
        branch: Option<BranchWriteContext>,
    },
    /// Deterministic shared-branch merge.
    Merge {
        /// Declared shared variable.
        variable: String,
        /// Optimistic current version, absent for the first merge.
        expected_version: Option<u64>,
        /// Complete branch contributions.
        branches: Vec<BranchVariableValue>,
    },
    /// Terminal removal of node/branch-scoped state.
    Remove {
        /// Declared live variable.
        variable: String,
        /// Exact current version.
        expected_version: u64,
        /// Exact branch write context for branch-scoped state.
        branch: Option<BranchWriteContext>,
    },
}

/// Runtime payload plus the complete work identity validated before creation.
///
/// The outer journal coordinator should use `work` for correlation/causation
/// metadata and commit only `payload`.
#[derive(Clone, Debug, PartialEq)]
pub struct PreparedVariableEvent {
    /// Exact immutable graph work that owns this operation.
    pub work: NodeWorkIdentity,
    /// Reducer-prepared variable payload ready for canonical commitment.
    pub payload: RuntimeCommittedEvent,
}

/// Completeness policy for strict canonical node output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeOutputCompleteness {
    /// Every compiled write must be present in JSON or explicit runtime input.
    RequireAll,
    /// Only this explicit subset of compiled writes may be absent.
    AllowMissing(BTreeSet<String>),
}

/// Pure bounded node-output planning command.
#[derive(Clone, Debug, PartialEq)]
pub struct PlanNodeOutputCommand {
    /// Exact bounded JSON object returned by the node executor.
    pub output: Value,
    /// Explicit completeness policy.
    pub completeness: NodeOutputCompleteness,
    /// Runtime-recorded timestamp/duration values keyed by declared variable.
    pub recorded_runtime_values: BTreeMap<String, CanonicalVariableValue>,
    /// Exact branch context, required for branch work.
    pub branch: Option<BranchWriteContext>,
}

impl PreparedVariableEvent {
    /// Reconstructs the exact coordinator operation carried by this receipt.
    ///
    /// The derived writer must match the complete work identity; callers cannot
    /// use a receipt to substitute another node or branch writer.
    ///
    /// # Errors
    ///
    /// Returns [`CanonicalVariableCoordinatorError::InvalidReceipt`] when the
    /// payload is not a variable event or its writer does not match `work`.
    pub fn operation(
        &self,
    ) -> Result<CoordinatedVariableOperation, CanonicalVariableCoordinatorError> {
        let event = self
            .payload
            .canonical_variable_event()
            .ok_or(CanonicalVariableCoordinatorError::InvalidReceipt)?;
        let attempt = match event {
            CanonicalVariableEvent::Declared(event) => VariableValidationAttempt::Declare {
                declaration: event.declaration,
            },
            CanonicalVariableEvent::Assigned(event) => VariableValidationAttempt::Assign {
                variable: event.binding.variable,
                writer: event.writer,
                expected_version: event.binding.prior_version,
                value: event.value,
            },
            CanonicalVariableEvent::Merged(event) => VariableValidationAttempt::Merge {
                variable: event.binding.variable,
                writer: event.writer,
                expected_version: event.binding.prior_version,
                branches: event.branches,
            },
            CanonicalVariableEvent::Removed(event) => VariableValidationAttempt::Remove {
                variable: event.binding.variable,
                writer: event.writer,
                expected_version: event
                    .binding
                    .prior_version
                    .ok_or(CanonicalVariableCoordinatorError::InvalidReceipt)?,
            },
            CanonicalVariableEvent::ValidationFailed(event) => event.attempt,
        };
        operation_from_attempt(&self.work, attempt)
    }
}

/// Fail-closed recovery decision for one intended variable operation.
#[derive(Clone, Debug, PartialEq)]
pub enum VariableRecoveryDecision {
    /// An exact retained receipt supplies the payload that remains to commit.
    CompleteFromReceipt(PreparedVariableEvent),
    /// No effect or receipt exists and the deterministic payload is safe to commit.
    SafeToCommit(PreparedVariableEvent),
    /// Replay already contains the exact intended terminal state.
    AlreadyApplied,
    /// Replay or a supplied receipt conflicts with the intended operation.
    Conflict,
}

/// Pure coordinator over one replay cut and one exact graph work identity.
pub struct CanonicalVariableCoordinator<'a> {
    replayed: &'a CanonicalVariableEventReducer,
    graph: &'a ExecutableGraph,
    work: &'a NodeWorkIdentity,
}

impl<'a> CanonicalVariableCoordinator<'a> {
    /// Creates a coordinator after validating replay and exact work identity.
    ///
    /// # Errors
    ///
    /// Returns [`CanonicalVariableCoordinatorError`] for corrupt replay,
    /// mismatched run/node identity, or unbounded branch identity.
    pub fn new(
        replayed: &'a CanonicalVariableEventReducer,
        graph: &'a ExecutableGraph,
        work: &'a NodeWorkIdentity,
    ) -> Result<Self, CanonicalVariableCoordinatorError> {
        replayed
            .validate_replayed()
            .map_err(|_| CanonicalVariableCoordinatorError::InvalidReplay)?;
        validate_work(replayed, graph, work)?;
        Ok(Self {
            replayed,
            graph,
            work,
        })
    }

    /// Prepares one exact runtime variable payload without mutating replay.
    ///
    /// Ordinary type, access, security, size, version-CAS, or merge failures
    /// become canonical `VariableValidationFailed` payloads. Corrupt graph/work
    /// contracts remain coordinator errors and are never journaled as user
    /// failures.
    ///
    /// # Errors
    ///
    /// Returns [`CanonicalVariableCoordinatorError`] for graph/work contract
    /// mismatch or canonical serialization failure.
    pub fn prepare(
        &self,
        operation: &CoordinatedVariableOperation,
    ) -> Result<PreparedVariableEvent, CanonicalVariableCoordinatorError> {
        prepare_against(self.replayed, self.graph, self.work, operation)
    }

    /// Prepares a sequential node-output batch against a staged reducer clone.
    ///
    /// Each returned event is applied only to the private staged reducer so
    /// later events receive exact prior/new versions. The caller remains the
    /// sole journal authority.
    ///
    /// # Errors
    ///
    /// Returns [`CanonicalVariableCoordinatorError`] if any operation violates
    /// the compiled work contract or cannot be canonically prepared.
    pub fn prepare_batch(
        &self,
        operations: &[CoordinatedVariableOperation],
    ) -> Result<Vec<PreparedVariableEvent>, CanonicalVariableCoordinatorError> {
        let mut staged = self.replayed.clone();
        let mut prepared = Vec::with_capacity(operations.len());
        for operation in operations {
            let event = prepare_against(&staged, self.graph, self.work, operation)?;
            apply_prepared(&mut staged, &event)?;
            prepared.push(event);
        }
        Ok(prepared)
    }

    /// Converts one bounded executor JSON object into exact ordered variable
    /// receipts without reading artifacts or writing the journal.
    ///
    /// Timestamp and duration values are never accepted from executor JSON.
    /// They must be supplied through `recorded_runtime_values`, which produces
    /// an explicit runtime-recorded writer in the canonical event.
    ///
    /// # Errors
    ///
    /// Returns [`CanonicalVariableCoordinatorError`] for an oversized or
    /// non-object output, extra/missing writes, invalid optional policy,
    /// forged runtime values, type conversion, branch mismatch, version drift,
    /// or any canonical validation failure.
    pub fn plan_node_output(
        &self,
        command: &PlanNodeOutputCommand,
    ) -> Result<Vec<PreparedVariableEvent>, CanonicalVariableCoordinatorError> {
        if self.work.node_id == "runtime" {
            return Err(CanonicalVariableCoordinatorError::NodeWorkRequired);
        }
        validate_branch_context(self.work, command.branch.as_ref())?;
        let encoded = serde_json::to_vec(&command.output)
            .map_err(|_| CanonicalVariableCoordinatorError::InvalidNodeOutput)?;
        if encoded.len() > MAX_NODE_OUTPUT_BYTES {
            return Err(CanonicalVariableCoordinatorError::NodeOutputTooLarge);
        }
        let output = command
            .output
            .as_object()
            .ok_or(CanonicalVariableCoordinatorError::InvalidNodeOutput)?;
        let node = graph_node(self.graph, &self.work.node_id)?;
        let optional = match &command.completeness {
            NodeOutputCompleteness::RequireAll => BTreeSet::new(),
            NodeOutputCompleteness::AllowMissing(optional) => optional.clone(),
        };
        if !optional.is_subset(&node.write_variables) {
            return Err(CanonicalVariableCoordinatorError::InvalidOptionalOutput);
        }
        if output
            .keys()
            .any(|name| !node.write_variables.contains(name))
            || command
                .recorded_runtime_values
                .keys()
                .any(|name| !node.write_variables.contains(name))
        {
            return Err(CanonicalVariableCoordinatorError::ExtraNodeOutput);
        }

        let mut operations = Vec::with_capacity(node.write_variables.len());
        for name in &node.write_variables {
            let declaration = declaration(self.graph, name)?;
            let expected_version = self
                .replayed
                .environment()
                .canonical_entries()
                .get(name)
                .map(|entry| entry.version);
            let runtime_recorded = matches!(
                declaration.value_type,
                VariableValueType::Timestamp | VariableValueType::Duration
            );
            if runtime_recorded {
                if output.contains_key(name) {
                    return Err(CanonicalVariableCoordinatorError::InvalidRecordedRuntimeValue);
                }
                let Some(value) = command.recorded_runtime_values.get(name) else {
                    if optional.contains(name) {
                        continue;
                    }
                    return Err(CanonicalVariableCoordinatorError::MissingNodeOutput(
                        name.clone(),
                    ));
                };
                operations.push(CoordinatedVariableOperation::AssignRuntimeRecorded {
                    variable: name.clone(),
                    expected_version,
                    value: value.clone(),
                    branch: command.branch.clone(),
                });
                continue;
            }
            if command.recorded_runtime_values.contains_key(name) {
                return Err(CanonicalVariableCoordinatorError::InvalidRecordedRuntimeValue);
            }
            let Some(value) = output.get(name) else {
                if optional.contains(name) {
                    continue;
                }
                return Err(CanonicalVariableCoordinatorError::MissingNodeOutput(
                    name.clone(),
                ));
            };
            let value =
                canonical_value_from_json(value, &declaration.value_type).map_err(|_| {
                    CanonicalVariableCoordinatorError::InvalidNodeOutputValue {
                        variable: name.clone(),
                    }
                })?;
            operations.push(CoordinatedVariableOperation::Assign {
                variable: name.clone(),
                expected_version,
                value,
                branch: command.branch.clone(),
            });
        }
        let prepared = self.prepare_batch(&operations)?;
        for event in &prepared {
            ensure_success_event(event)?;
        }
        Ok(prepared)
    }

    /// Prepares missing declaration and runtime-initialization events.
    ///
    /// Exact declaration/initial values already represented by replay are
    /// omitted. This supports both an empty reducer and the current
    /// style-initialization projection that seeds immutable initial values.
    ///
    /// # Errors
    ///
    /// Returns [`CanonicalVariableCoordinatorError`] for non-runtime work,
    /// unknown/non-runtime initial values, conflicting replay, or invalid
    /// canonical values.
    pub fn prepare_initialization(
        &self,
        initial_values: &BTreeMap<String, CanonicalVariableValue>,
    ) -> Result<Vec<PreparedVariableEvent>, CanonicalVariableCoordinatorError> {
        if self.work.node_id != "runtime" || !self.work.branch_path.is_empty() {
            return Err(CanonicalVariableCoordinatorError::RuntimeWorkRequired);
        }
        for name in initial_values.keys() {
            let declaration = declaration(self.graph, name)?;
            if declaration.producer != "runtime" {
                return Err(CanonicalVariableCoordinatorError::InvalidInitialProducer(
                    name.clone(),
                ));
            }
        }

        let mut staged = self.replayed.clone();
        let mut prepared = Vec::new();
        for declaration in &self.graph.variables {
            if staged.declaration_was_observed(&declaration.name) {
                if staged.environment().declarations().get(&declaration.name) != Some(declaration) {
                    return Err(CanonicalVariableCoordinatorError::Conflict);
                }
            } else {
                let operation = CoordinatedVariableOperation::Declare {
                    declaration: declaration.clone(),
                };
                let event = prepare_against(&staged, self.graph, self.work, &operation)?;
                ensure_success_event(&event)?;
                apply_prepared(&mut staged, &event)?;
                prepared.push(event);
            }
        }
        for (name, value) in initial_values {
            match staged
                .initial_assignment_audit_state(name, value)
                .map_err(|_| CanonicalVariableCoordinatorError::InvalidReplay)?
            {
                Some(InitialAssignmentAuditState::Observed) => continue,
                Some(InitialAssignmentAuditState::Pending) => {}
                None if staged.environment().canonical_entries().contains_key(name) => {
                    return Err(CanonicalVariableCoordinatorError::Conflict);
                }
                None => ensure_expected_version(&staged, name, None)?,
            }
            let operation = CoordinatedVariableOperation::Assign {
                variable: name.clone(),
                expected_version: None,
                value: value.clone(),
                branch: None,
            };
            let event = prepare_against(&staged, self.graph, self.work, &operation)?;
            ensure_success_event(&event)?;
            apply_prepared(&mut staged, &event)?;
            prepared.push(event);
        }
        Ok(prepared)
    }

    /// Projects exact declared transition inputs after consumer/scope checks.
    ///
    /// # Errors
    ///
    /// Returns [`CanonicalVariableCoordinatorError`] for undeclared node reads,
    /// missing values, unauthorized consumers, or branch mismatch.
    pub fn transition_environment(
        &self,
        required_variables: &BTreeSet<String>,
    ) -> Result<Value, CanonicalVariableCoordinatorError> {
        let node = graph_node(self.graph, &self.work.node_id)?;
        for variable in required_variables {
            if !node.read_variables.contains(variable) {
                return Err(CanonicalVariableCoordinatorError::VariableNotDeclaredRead {
                    node: node.id.clone(),
                    variable: variable.clone(),
                });
            }
        }
        self.replayed
            .environment()
            .transition_environment(
                &crate::canonical_variables::VariableReader {
                    node_id: node.id.clone(),
                    branch_id: self.work.branch_path.last().cloned(),
                },
                required_variables,
            )
            .map_err(|_| CanonicalVariableCoordinatorError::InvalidVariableAccess)
    }

    /// Projects exact declared pre-execution inputs while permitting an
    /// unassigned variable that this same node will produce.
    ///
    /// # Errors
    ///
    /// Returns [`CanonicalVariableCoordinatorError`] for undeclared reads or
    /// writes, unauthorized consumers, missing non-output values, or branch
    /// mismatch.
    pub fn node_input_environment(
        &self,
        read_variables: &BTreeSet<String>,
        write_variables: &BTreeSet<String>,
    ) -> Result<Value, CanonicalVariableCoordinatorError> {
        let node = graph_node(self.graph, &self.work.node_id)?;
        if read_variables != &node.read_variables || write_variables != &node.write_variables {
            return Err(CanonicalVariableCoordinatorError::NodeInputContractMismatch);
        }
        self.replayed
            .environment()
            .node_input_environment(
                &crate::canonical_variables::VariableReader {
                    node_id: node.id.clone(),
                    branch_id: self.work.branch_path.last().cloned(),
                },
                read_variables,
                write_variables,
            )
            .map_err(|_| CanonicalVariableCoordinatorError::InvalidVariableAccess)
    }

    /// Classifies restart recovery without redispatching an ambiguous action.
    ///
    /// A supplied receipt must bind the exact work and equal the payload
    /// independently prepared from replay. Without a receipt, a prior-version
    /// mismatch is a conflict unless replay already contains the exact intended
    /// terminal state.
    ///
    /// # Errors
    ///
    /// Returns [`CanonicalVariableCoordinatorError`] only for an invalid graph
    /// or work contract. Operation/replay disagreement is returned as
    /// [`VariableRecoveryDecision::Conflict`].
    pub fn recover(
        &self,
        operation: &CoordinatedVariableOperation,
        receipt: Option<&PreparedVariableEvent>,
    ) -> Result<VariableRecoveryDecision, CanonicalVariableCoordinatorError> {
        validate_operation(self.replayed, self.graph, self.work, operation)?;
        if let Some(receipt) = receipt {
            if receipt.work != *self.work || receipt.payload.canonical_variable_event().is_none() {
                return Ok(VariableRecoveryDecision::Conflict);
            }
            if event_is_reflected(self.replayed, &receipt.payload) {
                return Ok(VariableRecoveryDecision::AlreadyApplied);
            }
            if has_version_conflict(self.replayed, operation) {
                return Ok(VariableRecoveryDecision::Conflict);
            }
            let expected = self.prepare(operation)?;
            return Ok(if expected.payload == receipt.payload {
                VariableRecoveryDecision::CompleteFromReceipt(receipt.clone())
            } else {
                VariableRecoveryDecision::Conflict
            });
        }

        if operation_is_reflected(self.replayed, self.graph, self.work, operation)? {
            return Ok(VariableRecoveryDecision::AlreadyApplied);
        }
        if has_version_conflict(self.replayed, operation) {
            return Ok(VariableRecoveryDecision::Conflict);
        }
        let prepared = self.prepare(operation)?;
        if validation_failure_is_reflected(self.replayed, &prepared.payload) {
            Ok(VariableRecoveryDecision::AlreadyApplied)
        } else {
            Ok(VariableRecoveryDecision::SafeToCommit(prepared))
        }
    }
}

/// Purely applies an exact prepared receipt prefix and projects the
/// destination node's declared transition inputs.
///
/// This helper never commits events. It is intended for deterministic
/// pre-commit planning and restart-equivalence tests; authoritative live state
/// still comes only from replay after journal commitment.
///
/// # Errors
///
/// Returns [`CanonicalVariableCoordinatorError`] when any receipt/work binding
/// is invalid, reducer application fails, or the destination read contract is
/// not satisfied.
pub fn transition_environment_after_receipts(
    replayed: &CanonicalVariableEventReducer,
    graph: &ExecutableGraph,
    receipts: &[PreparedVariableEvent],
    destination_work: &NodeWorkIdentity,
    required_variables: &BTreeSet<String>,
) -> Result<Value, CanonicalVariableCoordinatorError> {
    let mut staged = replayed.clone();
    for receipt in receipts {
        receipt.operation()?;
        apply_prepared(&mut staged, receipt)?;
    }
    CanonicalVariableCoordinator::new(&staged, graph, destination_work)?
        .transition_environment(required_variables)
}

fn prepare_against(
    replayed: &CanonicalVariableEventReducer,
    graph: &ExecutableGraph,
    work: &NodeWorkIdentity,
    operation: &CoordinatedVariableOperation,
) -> Result<PreparedVariableEvent, CanonicalVariableCoordinatorError> {
    validate_operation(replayed, graph, work, operation)?;
    let attempt = operation_attempt(work, operation);
    let event = replayed
        .prepare_event(&work.node_id, attempt)
        .map_err(|_| CanonicalVariableCoordinatorError::Canonicalization)?;
    Ok(PreparedVariableEvent {
        work: work.clone(),
        payload: event.into(),
    })
}

fn validate_work(
    replayed: &CanonicalVariableEventReducer,
    graph: &ExecutableGraph,
    work: &NodeWorkIdentity,
) -> Result<(), CanonicalVariableCoordinatorError> {
    if work.run_id != replayed.run_id() {
        return Err(CanonicalVariableCoordinatorError::RunMismatch);
    }
    if work.attempt == 0 || work.step == 0 {
        return Err(CanonicalVariableCoordinatorError::InvalidWorkIdentity);
    }
    if work.branch_path.len() > MAX_BRANCH_DEPTH
        || work.branch_path.iter().any(|branch| {
            branch.is_empty()
                || branch.len() > MAX_IDENTITY_BYTES
                || branch.chars().any(char::is_control)
        })
    {
        return Err(CanonicalVariableCoordinatorError::InvalidWorkIdentity);
    }
    if work.node_id != "runtime" {
        graph_node(graph, &work.node_id)?;
    }
    Ok(())
}

fn validate_operation(
    replayed: &CanonicalVariableEventReducer,
    graph: &ExecutableGraph,
    work: &NodeWorkIdentity,
    operation: &CoordinatedVariableOperation,
) -> Result<(), CanonicalVariableCoordinatorError> {
    validate_work(replayed, graph, work)?;
    match operation {
        CoordinatedVariableOperation::Declare {
            declaration: candidate,
        } => {
            if work.node_id != "runtime" || !work.branch_path.is_empty() {
                return Err(CanonicalVariableCoordinatorError::RuntimeWorkRequired);
            }
            let compiled = declaration(graph, &candidate.name)?;
            if compiled != candidate {
                return Err(CanonicalVariableCoordinatorError::DeclarationMismatch(
                    candidate.name.clone(),
                ));
            }
        }
        CoordinatedVariableOperation::Assign {
            variable, branch, ..
        }
        | CoordinatedVariableOperation::Remove {
            variable, branch, ..
        } => {
            validate_write_contract(graph, work, variable)?;
            validate_branch_context(work, branch.as_ref())?;
        }
        CoordinatedVariableOperation::AssignRuntimeRecorded {
            variable,
            value,
            branch,
            ..
        } => {
            validate_write_contract(graph, work, variable)?;
            validate_branch_context(work, branch.as_ref())?;
            if !matches!(
                (declaration(graph, variable)?.value_type.clone(), value),
                (
                    VariableValueType::Timestamp,
                    CanonicalVariableValue::TimestampMillis(_)
                ) | (
                    VariableValueType::Duration,
                    CanonicalVariableValue::DurationMillis(_)
                )
            ) {
                return Err(CanonicalVariableCoordinatorError::InvalidRecordedRuntimeValue);
            }
        }
        CoordinatedVariableOperation::Merge { variable, .. } => {
            if !work.branch_path.is_empty() {
                return Err(CanonicalVariableCoordinatorError::AmbiguousBranchMerge);
            }
            if validate_write_contract(graph, work, variable).is_err() {
                validate_parallel_join_merge_contract(graph, work, variable)?;
            }
        }
    }
    Ok(())
}

fn validate_parallel_join_merge_contract(
    graph: &ExecutableGraph,
    work: &NodeWorkIdentity,
    variable: &str,
) -> Result<(), CanonicalVariableCoordinatorError> {
    let declaration = declaration(graph, variable)?;
    let node = graph_node(graph, &work.node_id)?;
    if node.kind != agentmod_graph_engine::NodeKind::JoinResults
        || declaration.merge_policy.is_none()
        || !graph.nodes.iter().any(|candidate| {
            matches!(
                candidate.configuration.as_ref(),
                Some(agentmod_graph_engine::NodeConfiguration::ParallelBranch {
                    join_target,
                    variable_merge_policies,
                    ..
                }) if join_target == &node.id
                    && variable_merge_policies.get(variable) == declaration.merge_policy.as_ref()
            )
        })
    {
        return Err(
            CanonicalVariableCoordinatorError::VariableNotDeclaredWrite {
                node: node.id.clone(),
                variable: variable.to_owned(),
            },
        );
    }
    Ok(())
}

fn validate_write_contract(
    graph: &ExecutableGraph,
    work: &NodeWorkIdentity,
    variable: &str,
) -> Result<(), CanonicalVariableCoordinatorError> {
    let declaration = declaration(graph, variable)?;
    if work.node_id == "runtime" {
        if declaration.producer == "runtime" {
            return Ok(());
        }
        return Err(CanonicalVariableCoordinatorError::InvalidInitialProducer(
            variable.to_owned(),
        ));
    }
    let node = graph_node(graph, &work.node_id)?;
    if !node.write_variables.contains(variable) || declaration.producer != node.id {
        return Err(
            CanonicalVariableCoordinatorError::VariableNotDeclaredWrite {
                node: node.id.clone(),
                variable: variable.to_owned(),
            },
        );
    }
    Ok(())
}

fn validate_branch_context(
    work: &NodeWorkIdentity,
    branch: Option<&BranchWriteContext>,
) -> Result<(), CanonicalVariableCoordinatorError> {
    match (work.branch_path.last(), branch) {
        (None, None) => Ok(()),
        (Some(expected), Some(actual)) if expected == &actual.branch_id => Ok(()),
        _ => Err(CanonicalVariableCoordinatorError::BranchIdentityMismatch),
    }
}

fn operation_attempt(
    work: &NodeWorkIdentity,
    operation: &CoordinatedVariableOperation,
) -> VariableValidationAttempt {
    match operation {
        CoordinatedVariableOperation::Declare { declaration } => {
            VariableValidationAttempt::Declare {
                declaration: declaration.clone(),
            }
        }
        CoordinatedVariableOperation::Assign {
            variable,
            expected_version,
            value,
            branch,
        } => VariableValidationAttempt::Assign {
            variable: variable.clone(),
            writer: writer(work, branch.clone()),
            expected_version: *expected_version,
            value: value.clone(),
        },
        CoordinatedVariableOperation::AssignRuntimeRecorded {
            variable,
            expected_version,
            value,
            branch,
        } => VariableValidationAttempt::Assign {
            variable: variable.clone(),
            writer: VariableWriter::RuntimeRecorded {
                node_id: work.node_id.clone(),
                branch: branch.clone(),
            },
            expected_version: *expected_version,
            value: value.clone(),
        },
        CoordinatedVariableOperation::Merge {
            variable,
            expected_version,
            branches,
        } => VariableValidationAttempt::Merge {
            variable: variable.clone(),
            writer: writer(work, None),
            expected_version: *expected_version,
            branches: branches.clone(),
        },
        CoordinatedVariableOperation::Remove {
            variable,
            expected_version,
            branch,
        } => VariableValidationAttempt::Remove {
            variable: variable.clone(),
            writer: writer(work, branch.clone()),
            expected_version: *expected_version,
        },
    }
}

fn operation_from_attempt(
    work: &NodeWorkIdentity,
    attempt: VariableValidationAttempt,
) -> Result<CoordinatedVariableOperation, CanonicalVariableCoordinatorError> {
    match attempt {
        VariableValidationAttempt::Declare { declaration } => {
            if work.node_id != "runtime" || !work.branch_path.is_empty() {
                return Err(CanonicalVariableCoordinatorError::InvalidReceipt);
            }
            Ok(CoordinatedVariableOperation::Declare { declaration })
        }
        VariableValidationAttempt::Assign {
            variable,
            writer,
            expected_version,
            value,
        } => match writer {
            VariableWriter::RuntimeRecorded { node_id, branch } if node_id == work.node_id => {
                validate_branch_context(work, branch.as_ref())?;
                Ok(CoordinatedVariableOperation::AssignRuntimeRecorded {
                    variable,
                    expected_version,
                    value,
                    branch,
                })
            }
            writer => Ok(CoordinatedVariableOperation::Assign {
                variable,
                expected_version,
                value,
                branch: receipt_branch(work, writer)?,
            }),
        },
        VariableValidationAttempt::Merge {
            variable,
            writer,
            expected_version,
            branches,
        } => {
            if writer
                != (crate::canonical_variables::VariableWriter::Node {
                    node_id: work.node_id.clone(),
                    branch: None,
                })
            {
                return Err(CanonicalVariableCoordinatorError::InvalidReceipt);
            }
            Ok(CoordinatedVariableOperation::Merge {
                variable,
                expected_version,
                branches,
            })
        }
        VariableValidationAttempt::Remove {
            variable,
            writer,
            expected_version,
        } => Ok(CoordinatedVariableOperation::Remove {
            variable,
            expected_version,
            branch: receipt_branch(work, writer)?,
        }),
    }
}

fn receipt_branch(
    work: &NodeWorkIdentity,
    writer: VariableWriter,
) -> Result<Option<BranchWriteContext>, CanonicalVariableCoordinatorError> {
    match writer {
        VariableWriter::Runtime if work.node_id == "runtime" && work.branch_path.is_empty() => {
            Ok(None)
        }
        VariableWriter::Node { node_id, branch } if node_id == work.node_id => {
            validate_branch_context(work, branch.as_ref())?;
            Ok(branch)
        }
        VariableWriter::Runtime
        | VariableWriter::RuntimeRecorded { .. }
        | VariableWriter::Node { .. } => Err(CanonicalVariableCoordinatorError::InvalidReceipt),
    }
}

fn writer(work: &NodeWorkIdentity, branch: Option<BranchWriteContext>) -> VariableWriter {
    if work.node_id == "runtime" {
        VariableWriter::Runtime
    } else {
        VariableWriter::Node {
            node_id: work.node_id.clone(),
            branch,
        }
    }
}

fn graph_node<'a>(
    graph: &'a ExecutableGraph,
    node_id: &str,
) -> Result<&'a ExecutableNode, CanonicalVariableCoordinatorError> {
    graph
        .nodes
        .iter()
        .find(|node| node.id == node_id)
        .ok_or_else(|| CanonicalVariableCoordinatorError::UnknownNode(node_id.to_owned()))
}

fn declaration<'a>(
    graph: &'a ExecutableGraph,
    variable: &str,
) -> Result<&'a VariableDeclaration, CanonicalVariableCoordinatorError> {
    graph
        .variables
        .iter()
        .find(|declaration| declaration.name == variable)
        .ok_or_else(|| CanonicalVariableCoordinatorError::UnknownVariable(variable.to_owned()))
}

fn apply_prepared(
    reducer: &mut CanonicalVariableEventReducer,
    prepared: &PreparedVariableEvent,
) -> Result<(), CanonicalVariableCoordinatorError> {
    let event = prepared
        .payload
        .canonical_variable_event()
        .ok_or(CanonicalVariableCoordinatorError::InvalidReceipt)?;
    reducer
        .apply(event)
        .map(|_| ())
        .map_err(|_| CanonicalVariableCoordinatorError::Canonicalization)
}

fn ensure_success_event(
    prepared: &PreparedVariableEvent,
) -> Result<(), CanonicalVariableCoordinatorError> {
    match prepared.payload.canonical_variable_event() {
        Some(CanonicalVariableEvent::ValidationFailed(_)) | None => {
            Err(CanonicalVariableCoordinatorError::Conflict)
        }
        Some(_) => Ok(()),
    }
}

fn ensure_expected_version(
    replayed: &CanonicalVariableEventReducer,
    variable: &str,
    expected: Option<u64>,
) -> Result<(), CanonicalVariableCoordinatorError> {
    if replayed
        .environment()
        .canonical_entries()
        .get(variable)
        .map(|entry| entry.version)
        == expected
    {
        Ok(())
    } else {
        Err(CanonicalVariableCoordinatorError::Conflict)
    }
}

fn has_version_conflict(
    replayed: &CanonicalVariableEventReducer,
    operation: &CoordinatedVariableOperation,
) -> bool {
    if let CoordinatedVariableOperation::Assign {
        variable,
        expected_version: None,
        value,
        branch: None,
    } = operation
        && matches!(
            replayed.initial_assignment_audit_state(variable, value),
            Ok(Some(InitialAssignmentAuditState::Pending))
        )
    {
        // Style initialization seeds the authoritative value before its
        // explicit assignment audit is appended. That exact pending audit is
        // not a version conflict and must remain recoverable after any prefix.
        return false;
    }
    let (variable, expected) = match operation {
        CoordinatedVariableOperation::Assign {
            variable,
            expected_version,
            ..
        }
        | CoordinatedVariableOperation::AssignRuntimeRecorded {
            variable,
            expected_version,
            ..
        }
        | CoordinatedVariableOperation::Merge {
            variable,
            expected_version,
            ..
        } => (variable, *expected_version),
        CoordinatedVariableOperation::Remove {
            variable,
            expected_version,
            ..
        } => (variable, Some(*expected_version)),
        CoordinatedVariableOperation::Declare { .. } => return false,
    };
    replayed
        .environment()
        .canonical_entries()
        .get(variable)
        .map(|entry| entry.version)
        != expected
}

fn operation_is_reflected(
    replayed: &CanonicalVariableEventReducer,
    graph: &ExecutableGraph,
    work: &NodeWorkIdentity,
    operation: &CoordinatedVariableOperation,
) -> Result<bool, CanonicalVariableCoordinatorError> {
    match operation {
        CoordinatedVariableOperation::Declare { declaration } => Ok(replayed
            .declaration_was_observed(&declaration.name)
            && replayed.environment().declarations().get(&declaration.name) == Some(declaration)),
        CoordinatedVariableOperation::Assign {
            variable,
            expected_version,
            value,
            branch,
        } => Ok(assignment_already_applied(
            replayed,
            variable,
            *expected_version,
            value,
            &writer(work, branch.clone()),
        )),
        CoordinatedVariableOperation::AssignRuntimeRecorded {
            variable,
            expected_version,
            value,
            branch,
        } => Ok(assignment_already_applied(
            replayed,
            variable,
            *expected_version,
            value,
            &VariableWriter::RuntimeRecorded {
                node_id: work.node_id.clone(),
                branch: branch.clone(),
            },
        )),
        CoordinatedVariableOperation::Remove {
            variable,
            expected_version,
            branch,
        } => Ok(replayed.removed().get(variable).is_some_and(|removed| {
            removed.version == expected_version.saturating_add(1)
                && removed.writer == writer(work, branch.clone())
        })),
        CoordinatedVariableOperation::Merge {
            variable,
            expected_version,
            branches,
        } => {
            let Some(entry) = replayed.environment().canonical_entries().get(variable) else {
                return Ok(false);
            };
            if entry.version != expected_version.unwrap_or(0).saturating_add(1)
                || entry.writer != writer(work, None)
            {
                return Ok(false);
            }
            let declaration = declaration(graph, variable)?.clone();
            let scratch_work = NodeWorkIdentity {
                run_id: String::from("merge-preview"),
                node_id: work.node_id.clone(),
                branch_path: Vec::new(),
                attempt: 1,
                loop_iteration: 0,
                step: 1,
            };
            let mut scratch = CanonicalVariableEventReducer::new(
                &scratch_work.run_id,
                crate::canonical_variables::VariableEnvironmentLimits::default(),
            )
            .map_err(|_| CanonicalVariableCoordinatorError::Canonicalization)?;
            let declared = scratch
                .prepare_event(
                    "runtime",
                    VariableValidationAttempt::Declare { declaration },
                )
                .map_err(|_| CanonicalVariableCoordinatorError::Canonicalization)?;
            scratch
                .apply(declared)
                .map_err(|_| CanonicalVariableCoordinatorError::Canonicalization)?;
            let preview = scratch
                .prepare_event(
                    &scratch_work.node_id,
                    VariableValidationAttempt::Merge {
                        variable: variable.clone(),
                        writer: writer(&scratch_work, None),
                        expected_version: None,
                        branches: branches.clone(),
                    },
                )
                .map_err(|_| CanonicalVariableCoordinatorError::Canonicalization)?;
            Ok(matches!(
                preview,
                CanonicalVariableEvent::Merged(event)
                    if event.value == entry.value && event.binding.value_hash == Some(entry.value_hash)
            ))
        }
    }
}

fn assignment_already_applied(
    replayed: &CanonicalVariableEventReducer,
    variable: &str,
    expected_version: Option<u64>,
    value: &CanonicalVariableValue,
    writer: &VariableWriter,
) -> bool {
    if writer == &VariableWriter::Runtime && expected_version.is_none() {
        match replayed.initial_assignment_audit_state(variable, value) {
            Ok(Some(InitialAssignmentAuditState::Observed)) => return true,
            Ok(Some(InitialAssignmentAuditState::Pending)) => return false,
            Ok(None) | Err(_) => {}
        }
    }
    replayed
        .environment()
        .canonical_entries()
        .get(variable)
        .is_some_and(|entry| {
            entry.version == expected_version.unwrap_or(0).saturating_add(1)
                && &entry.writer == writer
                && &entry.value == value
        })
}

fn event_is_reflected(
    replayed: &CanonicalVariableEventReducer,
    payload: &RuntimeCommittedEvent,
) -> bool {
    let Some(event) = payload.canonical_variable_event() else {
        return false;
    };
    match event {
        CanonicalVariableEvent::Declared(event) => {
            replayed.declaration_was_observed(&event.declaration.name)
                && replayed
                    .environment()
                    .declarations()
                    .get(&event.declaration.name)
                    == Some(&event.declaration)
        }
        CanonicalVariableEvent::Assigned(event) => {
            if event.writer == VariableWriter::Runtime && event.binding.prior_version.is_none() {
                matches!(
                    replayed.initial_assignment_audit_state(&event.binding.variable, &event.value),
                    Ok(Some(InitialAssignmentAuditState::Observed))
                )
            } else {
                replayed
                    .environment()
                    .canonical_entries()
                    .get(&event.binding.variable)
                    .is_some_and(|entry| {
                        Some(entry.version) == event.binding.new_version
                            && Some(entry.value_hash) == event.binding.value_hash
                            && entry.writer == event.writer
                            && entry.value == event.value
                    })
            }
        }
        CanonicalVariableEvent::Merged(event) => replayed
            .environment()
            .canonical_entries()
            .get(&event.binding.variable)
            .is_some_and(|entry| {
                Some(entry.version) == event.binding.new_version
                    && Some(entry.value_hash) == event.binding.value_hash
                    && entry.writer == event.writer
                    && entry.value == event.value
            }),
        CanonicalVariableEvent::Removed(event) => replayed
            .removed()
            .get(&event.binding.variable)
            .is_some_and(|removed| {
                Some(removed.version) == event.binding.new_version
                    && Some(removed.removed_value_hash) == event.binding.value_hash
                    && removed.writer == event.writer
            }),
        CanonicalVariableEvent::ValidationFailed(event) => {
            replayed.validation_failures().contains(event.as_ref())
        }
    }
}

fn validation_failure_is_reflected(
    replayed: &CanonicalVariableEventReducer,
    payload: &RuntimeCommittedEvent,
) -> bool {
    matches!(
        payload.canonical_variable_event(),
        Some(CanonicalVariableEvent::ValidationFailed(event))
            if replayed.validation_failures().contains(event.as_ref())
    )
}

/// Canonical-variable coordinator contract failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum CanonicalVariableCoordinatorError {
    /// Replayed variable projection failed integrity validation.
    #[error("canonical variable replay is invalid")]
    InvalidReplay,
    /// Work belongs to a different graph run.
    #[error("canonical variable work run does not match replay")]
    RunMismatch,
    /// Work counters or branch identity are invalid.
    #[error("canonical variable work identity is invalid")]
    InvalidWorkIdentity,
    /// Runtime initialization was attempted from graph-node work.
    #[error("canonical variable initialization requires runtime work")]
    RuntimeWorkRequired,
    /// Node-output planning was attempted with runtime initialization work.
    #[error("canonical node output planning requires graph-node work")]
    NodeWorkRequired,
    /// Compiled node is absent.
    #[error("compiled graph node `{0}` is unavailable")]
    UnknownNode(String),
    /// Compiled declaration is absent.
    #[error("compiled graph variable `{0}` is unavailable")]
    UnknownVariable(String),
    /// Declaration differs from the compiled graph.
    #[error("compiled declaration for `{0}` does not match")]
    DeclarationMismatch(String),
    /// Runtime initialization targeted a node-produced variable.
    #[error("initial variable `{0}` is not runtime-produced")]
    InvalidInitialProducer(String),
    /// Runtime-only output was absent, forged in JSON, or had the wrong type.
    #[error("node runtime-recorded variable value is invalid")]
    InvalidRecordedRuntimeValue,
    /// Node output was not a bounded JSON object.
    #[error("canonical node output must be a bounded JSON object")]
    InvalidNodeOutput,
    /// Serialized node output exceeded the hard logic-layer bound.
    #[error("canonical node output exceeds its byte bound")]
    NodeOutputTooLarge,
    /// Output contained a key outside the node's exact compiled write set.
    #[error("canonical node output contains an undeclared write")]
    ExtraNodeOutput,
    /// Required compiled output was absent.
    #[error("canonical node output is missing `{0}`")]
    MissingNodeOutput(String),
    /// Optional-output policy named a variable outside the compiled write set.
    #[error("canonical node optional-output policy is invalid")]
    InvalidOptionalOutput,
    /// JSON could not be converted to the exact declared variable type.
    #[error("canonical node output value for `{variable}` is invalid")]
    InvalidNodeOutputValue {
        /// Declared variable.
        variable: String,
    },
    /// Node did not declare this exact write.
    #[error("node `{node}` did not declare variable write `{variable}`")]
    VariableNotDeclaredWrite {
        /// Compiled node.
        node: String,
        /// Requested variable.
        variable: String,
    },
    /// Node did not declare this exact read.
    #[error("node `{node}` did not declare variable read `{variable}`")]
    VariableNotDeclaredRead {
        /// Compiled node.
        node: String,
        /// Requested variable.
        variable: String,
    },
    /// Requested pre-execution inputs differ from the exact compiled node contract.
    #[error("canonical node input contract does not match the compiled graph")]
    NodeInputContractMismatch,
    /// Branch context does not match exact work.
    #[error("variable branch identity does not match node work")]
    BranchIdentityMismatch,
    /// Nested merge ownership is not representable without an explicit parent merge policy.
    #[error("nested branch merge requires an explicit outer serialization boundary")]
    AmbiguousBranchMerge,
    /// Consumer, scope, or branch access failed.
    #[error("canonical variable access is invalid")]
    InvalidVariableAccess,
    /// Reducer could not prepare or apply the canonical payload.
    #[error("canonical variable payload could not be prepared")]
    Canonicalization,
    /// Supplied receipt is not a canonical variable payload.
    #[error("canonical variable receipt is invalid")]
    InvalidReceipt,
    /// Existing replay conflicts with the intended operation.
    #[error("canonical variable replay conflicts with the intended operation")]
    Conflict,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use agentmod_expression_engine::{Expression, ExpressionLimits};
    use agentmod_graph_engine::{
        ExecutableGraph, ExecutableNode, GraphBudget, GraphCacheKey, GraphDeclarations, NodeKind,
        SecurityClassification, VariableDeclaration, VariableMergePolicy, VariableMutability,
        VariableScope, VariableValueType,
    };
    use agentmod_primitives::ContentHash;

    use super::{
        CanonicalVariableCoordinator, CanonicalVariableCoordinatorError,
        CoordinatedVariableOperation, NodeOutputCompleteness, PlanNodeOutputCommand,
        PreparedVariableEvent, VariableRecoveryDecision, transition_environment_after_receipts,
    };
    use crate::{
        canonical_variables::{
            BranchVariableValue, BranchWriteContext, CanonicalVariableEvent,
            CanonicalVariableEventReducer, CanonicalVariableValue, ConditionEligibility,
            VariableEnvironmentLimits, VariableReader, VariableValidationFailureCode,
        },
        node_execution::NodeWorkIdentity,
        session::RuntimeCommittedEvent,
    };

    fn declaration(
        name: &str,
        value_type: VariableValueType,
        producer: &str,
        scope: VariableScope,
        merge_policy: Option<VariableMergePolicy>,
        classification: SecurityClassification,
    ) -> VariableDeclaration {
        VariableDeclaration {
            name: name.to_owned(),
            value_type,
            scope,
            producer: producer.to_owned(),
            merge_contributors: BTreeSet::new(),
            consumers: BTreeSet::from([String::from("compute")]),
            mutability: VariableMutability::Mutable,
            merge_policy,
            max_size_bytes: 4_096,
            security_classification: classification,
        }
    }

    fn node(index: usize, id: &str, reads: &[&str], writes: &[&str]) -> ExecutableNode {
        ExecutableNode {
            index,
            id: id.to_owned(),
            kind: if id == "done" {
                NodeKind::CompleteTurn
            } else {
                NodeKind::ConditionalBranch
            },
            configuration: None,
            condition: None,
            tool: None,
            provider: None,
            required_capabilities: BTreeSet::new(),
            read_scopes: BTreeSet::new(),
            write_scopes: BTreeSet::new(),
            read_variables: reads.iter().map(|value| (*value).to_owned()).collect(),
            write_variables: writes.iter().map(|value| (*value).to_owned()).collect(),
            retry_limit: 0,
            max_iterations: None,
        }
    }

    fn graph() -> ExecutableGraph {
        let hash = ContentHash::digest(b"canonical-variable-coordinator");
        ExecutableGraph {
            format_version: 1,
            entry_index: 0,
            budget: GraphBudget {
                max_steps: 64,
                max_tokens: 1_024,
                max_cost_micros: 1_000,
                max_duration_ms: 60_000,
            },
            declarations: GraphDeclarations::default(),
            variables: vec![
                declaration(
                    "artifact",
                    VariableValueType::ArtifactReference,
                    "compute",
                    VariableScope::Run,
                    None,
                    SecurityClassification::Internal,
                ),
                declaration(
                    "branch_local",
                    VariableValueType::String,
                    "compute",
                    VariableScope::Branch,
                    None,
                    SecurityClassification::Internal,
                ),
                declaration(
                    "input",
                    VariableValueType::Boolean,
                    "runtime",
                    VariableScope::Run,
                    None,
                    SecurityClassification::Public,
                ),
                declaration(
                    "output",
                    VariableValueType::String,
                    "compute",
                    VariableScope::Run,
                    None,
                    SecurityClassification::Internal,
                ),
                declaration(
                    "secret",
                    VariableValueType::SecretReference,
                    "compute",
                    VariableScope::Run,
                    None,
                    SecurityClassification::SecretReference,
                ),
                declaration(
                    "shared",
                    VariableValueType::List {
                        item_type: Box::new(VariableValueType::Integer),
                        max_items: 8,
                    },
                    "compute",
                    VariableScope::Run,
                    Some(VariableMergePolicy::Append),
                    SecurityClassification::Internal,
                ),
            ],
            nodes: vec![
                node(
                    0,
                    "compute",
                    &["input"],
                    &["artifact", "branch_local", "output", "secret", "shared"],
                ),
                node(1, "done", &[], &[]),
            ],
            edges: Vec::new(),
            cache_key: GraphCacheKey {
                graph_content_hash: hash,
                plugin_set_hash: hash,
                capability_set_hash: hash,
                runtime_api_hash: hash,
                combined_hash: hash,
            },
        }
    }

    fn work(node_id: &str, branch_path: Vec<String>) -> NodeWorkIdentity {
        NodeWorkIdentity {
            run_id: String::from("run-variable"),
            node_id: node_id.to_owned(),
            branch_path,
            attempt: 1,
            loop_iteration: 0,
            step: 1,
        }
    }

    fn fresh() -> CanonicalVariableEventReducer {
        CanonicalVariableEventReducer::new("run-variable", VariableEnvironmentLimits::default())
            .expect("reducer")
    }

    fn apply(reducer: &mut CanonicalVariableEventReducer, event: &PreparedVariableEvent) {
        reducer
            .apply(
                event
                    .payload
                    .canonical_variable_event()
                    .expect("variable payload"),
            )
            .expect("apply");
    }

    fn initialized() -> CanonicalVariableEventReducer {
        let graph = graph();
        initialize_graph(&graph)
    }

    fn initialize_graph(graph: &ExecutableGraph) -> CanonicalVariableEventReducer {
        let runtime = work("runtime", Vec::new());
        let mut reducer = fresh();
        let coordinator =
            CanonicalVariableCoordinator::new(&reducer, graph, &runtime).expect("coordinator");
        let events = coordinator
            .prepare_initialization(&BTreeMap::from([(
                String::from("input"),
                CanonicalVariableValue::Boolean(true),
            )]))
            .expect("initialization");
        for event in &events {
            apply(&mut reducer, event);
        }
        reducer
    }

    fn branch_context(id: &str, order: u32) -> BranchWriteContext {
        BranchWriteContext {
            branch_id: id.to_owned(),
            stable_order: order,
            serialized_shared_write: false,
        }
    }

    #[test]
    fn transition_environment_and_condition_are_identical_after_restart() {
        let graph = graph();
        let reducer = initialized();
        let compute = work("compute", Vec::new());
        let required = BTreeSet::from([String::from("input")]);
        let before = CanonicalVariableCoordinator::new(&reducer, &graph, &compute)
            .expect("coordinator")
            .transition_environment(&required)
            .expect("environment");
        let replayed: CanonicalVariableEventReducer =
            serde_json::from_slice(&serde_json::to_vec(&reducer).expect("serialize"))
                .expect("replay");
        let after = CanonicalVariableCoordinator::new(&replayed, &graph, &compute)
            .expect("coordinator")
            .transition_environment(&required)
            .expect("environment");

        assert_eq!(before, after);
        let expression =
            Expression::parse("input == true", ExpressionLimits::default()).expect("expression");
        assert_eq!(
            replayed.environment().classify_compiled_condition(
                &expression,
                &VariableReader {
                    node_id: String::from("compute"),
                    branch_id: None,
                },
                &required,
            ),
            ConditionEligibility::Eligible
        );
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one recovery matrix keeps assignment, merge, scoped removal, receipt, and tamper assertions on the same replay progression"
    )]
    fn output_merge_removal_and_recovery_are_exact_and_fail_closed() {
        let graph = graph();
        let mut reducer = initialized();
        let compute = work("compute", Vec::new());
        let assignment = CoordinatedVariableOperation::Assign {
            variable: String::from("output"),
            expected_version: None,
            value: CanonicalVariableValue::String(String::from("ready")),
            branch: None,
        };
        let coordinator =
            CanonicalVariableCoordinator::new(&reducer, &graph, &compute).expect("coordinator");
        let receipt = match coordinator.recover(&assignment, None).expect("recovery") {
            VariableRecoveryDecision::SafeToCommit(event) => event,
            other => panic!("unexpected recovery: {other:?}"),
        };
        assert!(matches!(
            coordinator
                .recover(&assignment, Some(&receipt))
                .expect("receipt"),
            VariableRecoveryDecision::CompleteFromReceipt(_)
        ));
        apply(&mut reducer, &receipt);
        let coordinator =
            CanonicalVariableCoordinator::new(&reducer, &graph, &compute).expect("coordinator");
        assert_eq!(
            coordinator.recover(&assignment, None).expect("recovery"),
            VariableRecoveryDecision::AlreadyApplied
        );

        let changed = CoordinatedVariableOperation::Assign {
            variable: String::from("output"),
            expected_version: None,
            value: CanonicalVariableValue::String(String::from("changed")),
            branch: None,
        };
        assert_eq!(
            coordinator.recover(&changed, None).expect("recovery"),
            VariableRecoveryDecision::Conflict
        );

        let merge = CoordinatedVariableOperation::Merge {
            variable: String::from("shared"),
            expected_version: None,
            branches: vec![
                BranchVariableValue {
                    branch_id: String::from("branch-b"),
                    stable_order: 1,
                    value: CanonicalVariableValue::List(vec![CanonicalVariableValue::Integer(2)]),
                },
                BranchVariableValue {
                    branch_id: String::from("branch-a"),
                    stable_order: 0,
                    value: CanonicalVariableValue::List(vec![CanonicalVariableValue::Integer(1)]),
                },
            ],
        };
        let merged = coordinator.prepare(&merge).expect("merge");
        let RuntimeCommittedEvent::VariableMerged(event) = &merged.payload else {
            panic!("expected merge");
        };
        assert_eq!(event.branches[0].branch_id, "branch-a");
        assert_eq!(
            event.value,
            CanonicalVariableValue::List(vec![
                CanonicalVariableValue::Integer(1),
                CanonicalVariableValue::Integer(2),
            ])
        );
        apply(&mut reducer, &merged);

        let branch_work = work("compute", vec![String::from("branch-a")]);
        let branch = Some(branch_context("branch-a", 0));
        let branch_assignment = CoordinatedVariableOperation::Assign {
            variable: String::from("branch_local"),
            expected_version: None,
            value: CanonicalVariableValue::String(String::from("temporary")),
            branch: branch.clone(),
        };
        let coordinator =
            CanonicalVariableCoordinator::new(&reducer, &graph, &branch_work).expect("coordinator");
        let assigned = coordinator
            .prepare(&branch_assignment)
            .expect("branch assign");
        apply(&mut reducer, &assigned);
        let coordinator =
            CanonicalVariableCoordinator::new(&reducer, &graph, &branch_work).expect("coordinator");
        let removed = coordinator
            .prepare(&CoordinatedVariableOperation::Remove {
                variable: String::from("branch_local"),
                expected_version: 1,
                branch,
            })
            .expect("remove");
        let RuntimeCommittedEvent::VariableRemoved(event) = &removed.payload else {
            panic!("expected removal");
        };
        assert_eq!(event.binding.prior_version, Some(1));
        assert_eq!(event.binding.new_version, Some(2));
        apply(&mut reducer, &removed);
        assert_eq!(
            reducer
                .removed()
                .get("branch_local")
                .expect("removed")
                .version,
            2
        );

        let mut tampered = receipt;
        let RuntimeCommittedEvent::VariableAssigned(event) = &mut tampered.payload else {
            panic!("expected assignment");
        };
        event.value = CanonicalVariableValue::String(String::from("tampered"));
        let pristine = initialized();
        let coordinator =
            CanonicalVariableCoordinator::new(&pristine, &graph, &compute).expect("coordinator");
        assert_eq!(
            coordinator
                .recover(&assignment, Some(&tampered))
                .expect("recovery"),
            VariableRecoveryDecision::Conflict
        );
    }

    #[test]
    fn invalid_value_and_shared_branch_write_become_canonical_failures() {
        let graph = graph();
        let mut reducer = initialized();
        let compute = work("compute", Vec::new());
        let coordinator =
            CanonicalVariableCoordinator::new(&reducer, &graph, &compute).expect("coordinator");
        let invalid = coordinator
            .prepare(&CoordinatedVariableOperation::Assign {
                variable: String::from("output"),
                expected_version: None,
                value: CanonicalVariableValue::Integer(7),
                branch: None,
            })
            .expect("failure payload");
        let RuntimeCommittedEvent::VariableValidationFailed(event) = &invalid.payload else {
            panic!("expected validation failure");
        };
        assert_eq!(event.code, VariableValidationFailureCode::InvalidValue);
        assert_eq!(event.binding.prior_version, None);
        assert_eq!(event.binding.new_version, None);
        apply(&mut reducer, &invalid);
        let coordinator =
            CanonicalVariableCoordinator::new(&reducer, &graph, &compute).expect("coordinator");
        assert_eq!(
            coordinator
                .recover(
                    &CoordinatedVariableOperation::Assign {
                        variable: String::from("output"),
                        expected_version: None,
                        value: CanonicalVariableValue::Integer(7),
                        branch: None,
                    },
                    None,
                )
                .expect("recovery"),
            VariableRecoveryDecision::AlreadyApplied
        );

        let branch_work = work("compute", vec![String::from("branch-a")]);
        let coordinator =
            CanonicalVariableCoordinator::new(&reducer, &graph, &branch_work).expect("coordinator");
        let shared = coordinator
            .prepare(&CoordinatedVariableOperation::Assign {
                variable: String::from("shared"),
                expected_version: None,
                value: CanonicalVariableValue::List(vec![CanonicalVariableValue::Integer(1)]),
                branch: Some(branch_context("branch-a", 0)),
            })
            .expect("failure payload");
        assert!(matches!(
            shared.payload,
            RuntimeCommittedEvent::VariableValidationFailed(_)
        ));

        assert_eq!(
            coordinator
                .prepare(&CoordinatedVariableOperation::Assign {
                    variable: String::from("input"),
                    expected_version: Some(1),
                    value: CanonicalVariableValue::Boolean(false),
                    branch: Some(branch_context("branch-a", 0)),
                })
                .expect_err("undeclared node write"),
            CanonicalVariableCoordinatorError::VariableNotDeclaredWrite {
                node: String::from("compute"),
                variable: String::from("input"),
            }
        );
    }

    #[test]
    fn artifact_and_secret_references_are_preserved_without_inline_secret_acceptance() {
        let graph = graph();
        let mut reducer = initialized();
        let compute = work("compute", Vec::new());
        let coordinator =
            CanonicalVariableCoordinator::new(&reducer, &graph, &compute).expect("coordinator");
        let events = coordinator
            .prepare_batch(&[
                CoordinatedVariableOperation::Assign {
                    variable: String::from("artifact"),
                    expected_version: None,
                    value: CanonicalVariableValue::ArtifactReference(String::from(
                        "blake3:artifact",
                    )),
                    branch: None,
                },
                CoordinatedVariableOperation::Assign {
                    variable: String::from("secret"),
                    expected_version: None,
                    value: CanonicalVariableValue::SecretReference(String::from(
                        "vault:item:version",
                    )),
                    branch: None,
                },
            ])
            .expect("reference events");
        let RuntimeCommittedEvent::VariableAssigned(artifact) = &events[0].payload else {
            panic!("expected artifact assignment");
        };
        assert_eq!(
            artifact.binding.artifact_references,
            BTreeSet::from([String::from("blake3:artifact")])
        );
        for event in &events {
            apply(&mut reducer, event);
        }
        assert_eq!(
            reducer
                .environment()
                .canonical_entries()
                .get("secret")
                .expect("secret reference")
                .value,
            CanonicalVariableValue::SecretReference(String::from("vault:item:version"))
        );

        let coordinator =
            CanonicalVariableCoordinator::new(&reducer, &graph, &compute).expect("coordinator");
        let inline = coordinator
            .prepare(&CoordinatedVariableOperation::Assign {
                variable: String::from("secret"),
                expected_version: Some(1),
                value: CanonicalVariableValue::String(String::from("inline secret")),
                branch: None,
            })
            .expect("failure payload");
        let Some(CanonicalVariableEvent::ValidationFailed(failure)) =
            inline.payload.canonical_variable_event()
        else {
            panic!("expected secret validation failure");
        };
        assert_eq!(
            failure.code,
            VariableValidationFailureCode::SecurityViolation
        );
    }

    #[test]
    fn seeded_initialization_audits_recover_every_prefix_and_reject_tampering() {
        let graph = graph();
        let runtime = work("runtime", Vec::new());
        let initial =
            BTreeMap::from([(String::from("input"), CanonicalVariableValue::Boolean(true))]);
        let seeded = CanonicalVariableEventReducer::initialize(
            "run-variable",
            VariableEnvironmentLimits::default(),
            graph.variables.clone(),
            initial.clone(),
        )
        .expect("seeded reducer");
        let events = CanonicalVariableCoordinator::new(&seeded, &graph, &runtime)
            .expect("coordinator")
            .prepare_initialization(&initial)
            .expect("audit events");
        assert_eq!(events.len(), graph.variables.len() + 1);
        assert!(matches!(
            events.last().expect("assignment").payload,
            RuntimeCommittedEvent::VariableAssigned(_)
        ));

        for prefix in 0..=events.len() {
            let mut replayed = seeded.clone();
            for event in &events[..prefix] {
                apply(&mut replayed, event);
            }
            let remaining = CanonicalVariableCoordinator::new(&replayed, &graph, &runtime)
                .expect("coordinator")
                .prepare_initialization(&initial)
                .expect("remaining audits");
            assert_eq!(remaining, events[prefix..]);
        }

        let mut complete = seeded.clone();
        for event in &events {
            apply(&mut complete, event);
        }
        assert!(
            CanonicalVariableCoordinator::new(&complete, &graph, &runtime)
                .expect("coordinator")
                .prepare_initialization(&initial)
                .expect("duplicate no-op")
                .is_empty()
        );
        assert!(
            complete
                .apply(
                    events[0]
                        .payload
                        .canonical_variable_event()
                        .expect("declaration")
                )
                .is_err()
        );
        assert!(
            complete
                .apply(
                    events
                        .last()
                        .expect("assignment")
                        .payload
                        .canonical_variable_event()
                        .expect("assignment")
                )
                .is_err()
        );

        let declarations = events.len() - 1;
        let mut before_assignment = seeded.clone();
        for event in &events[..declarations] {
            apply(&mut before_assignment, event);
        }
        let assignment = events.last().expect("assignment");
        for mutation in 0..4 {
            let mut tampered = assignment
                .payload
                .canonical_variable_event()
                .expect("assignment");
            let CanonicalVariableEvent::Assigned(event) = &mut tampered else {
                panic!("assignment audit");
            };
            match mutation {
                0 => event.value = CanonicalVariableValue::Boolean(false),
                1 => event.binding.value_hash = Some(ContentHash::digest(b"tampered")),
                2 => event.binding.new_version = Some(2),
                3 => {
                    event.writer = crate::canonical_variables::VariableWriter::Node {
                        node_id: String::from("runtime"),
                        branch: None,
                    };
                }
                _ => unreachable!(),
            }
            assert!(before_assignment.clone().apply(tampered).is_err());
        }
    }

    #[test]
    fn initialization_audit_serde_defaults_only_for_legacy_projection() {
        let graph = graph();
        let seeded = CanonicalVariableEventReducer::initialize(
            "run-variable",
            VariableEnvironmentLimits::default(),
            graph.variables,
            [(String::from("input"), CanonicalVariableValue::Boolean(true))],
        )
        .expect("seeded reducer");
        let mut legacy = serde_json::to_value(&seeded).expect("serialize");
        let object = legacy.as_object_mut().expect("object");
        object.remove("initial_audit_schema_version");
        object.remove("seeded_initial_assignment_hashes");
        object.remove("observed_initial_assignment_hashes");
        object.remove("initial_assignment_audit_state_hash");
        let legacy: CanonicalVariableEventReducer =
            serde_json::from_value(legacy).expect("legacy defaults");
        legacy.validate_replayed().expect("legacy remains valid");

        let mut erased = serde_json::to_value(&seeded).expect("serialize");
        erased
            .as_object_mut()
            .expect("object")
            .remove("seeded_initial_assignment_hashes");
        let erased: CanonicalVariableEventReducer =
            serde_json::from_value(erased).expect("deserialize tampering");
        assert!(erased.validate_replayed().is_err());
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one planner contract test keeps success, ordering, strict-key, runtime-provenance, and completeness assertions on one fixture"
    )]
    fn node_output_planner_is_strict_typed_ordered_and_runtime_recorded() {
        let mut graph = graph();
        graph.variables.extend([
            declaration(
                "duration",
                VariableValueType::Duration,
                "compute",
                VariableScope::Run,
                None,
                SecurityClassification::Internal,
            ),
            declaration(
                "timestamp",
                VariableValueType::Timestamp,
                "compute",
                VariableScope::Run,
                None,
                SecurityClassification::Internal,
            ),
        ]);
        graph.nodes[0]
            .write_variables
            .extend([String::from("duration"), String::from("timestamp")]);
        let reducer = initialize_graph(&graph);
        let compute = work("compute", Vec::new());
        let coordinator =
            CanonicalVariableCoordinator::new(&reducer, &graph, &compute).expect("coordinator");
        let optional = BTreeSet::from([String::from("branch_local"), String::from("shared")]);
        let command = PlanNodeOutputCommand {
            output: serde_json::json!({
                "artifact": "blake3:artifact",
                "output": "ready",
                "secret": "vault:item:version"
            }),
            completeness: NodeOutputCompleteness::AllowMissing(optional),
            recorded_runtime_values: BTreeMap::from([
                (
                    String::from("duration"),
                    CanonicalVariableValue::DurationMillis(250),
                ),
                (
                    String::from("timestamp"),
                    CanonicalVariableValue::TimestampMillis(1_700_000_000_000),
                ),
            ]),
            branch: None,
        };
        let events = coordinator
            .plan_node_output(&command)
            .expect("planned output");
        let names = events
            .iter()
            .map(|event| {
                event
                    .payload
                    .canonical_variable_event()
                    .expect("variable event")
                    .binding()
                    .variable
                    .clone()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec!["artifact", "duration", "output", "secret", "timestamp"]
        );
        assert!(matches!(
            events[1].payload,
            RuntimeCommittedEvent::VariableAssigned(ref event)
                if matches!(
                    event.writer,
                    crate::canonical_variables::VariableWriter::RuntimeRecorded { .. }
                )
        ));

        let branch_work = work("compute", vec![String::from("branch-a")]);
        let branch_coordinator =
            CanonicalVariableCoordinator::new(&reducer, &graph, &branch_work).expect("branch");
        let branch_events = branch_coordinator
            .plan_node_output(&PlanNodeOutputCommand {
                output: serde_json::json!({"branch_local": "branch value"}),
                completeness: NodeOutputCompleteness::AllowMissing(BTreeSet::from([
                    String::from("artifact"),
                    String::from("duration"),
                    String::from("output"),
                    String::from("secret"),
                    String::from("shared"),
                    String::from("timestamp"),
                ])),
                recorded_runtime_values: BTreeMap::new(),
                branch: Some(branch_context("branch-a", 0)),
            })
            .expect("branch output");
        assert_eq!(branch_events.len(), 1);
        let RuntimeCommittedEvent::VariableAssigned(branch_event) = &branch_events[0].payload
        else {
            panic!("branch assignment");
        };
        assert!(matches!(
            &branch_event.writer,
            crate::canonical_variables::VariableWriter::Node {
                branch: Some(branch),
                ..
            } if branch.branch_id == "branch-a"
        ));

        let mut extra = command.clone();
        extra.output["forged"] = serde_json::json!(true);
        assert_eq!(
            coordinator
                .plan_node_output(&extra)
                .expect_err("extra rejected"),
            CanonicalVariableCoordinatorError::ExtraNodeOutput
        );
        let mut forged_time = command.clone();
        forged_time.output["timestamp"] = serde_json::json!(1_700_000_000_000_i64);
        assert_eq!(
            coordinator
                .plan_node_output(&forged_time)
                .expect_err("forged time rejected"),
            CanonicalVariableCoordinatorError::InvalidRecordedRuntimeValue
        );
        let mut missing = command;
        missing
            .output
            .as_object_mut()
            .expect("object")
            .remove("output");
        assert_eq!(
            coordinator
                .plan_node_output(&missing)
                .expect_err("missing rejected"),
            CanonicalVariableCoordinatorError::MissingNodeOutput(String::from("output"))
        );
    }

    #[test]
    fn output_receipts_drive_restart_identical_transition_environment_and_versions() {
        let mut graph = graph();
        graph.nodes[1].read_variables.insert(String::from("output"));
        graph
            .variables
            .iter_mut()
            .find(|declaration| declaration.name == "output")
            .expect("output declaration")
            .consumers
            .insert(String::from("done"));
        let mut reducer = initialize_graph(&graph);
        let producer = work("compute", Vec::new());
        let optional = BTreeSet::from([
            String::from("artifact"),
            String::from("branch_local"),
            String::from("secret"),
            String::from("shared"),
        ]);
        let receipts = CanonicalVariableCoordinator::new(&reducer, &graph, &producer)
            .expect("producer")
            .plan_node_output(&PlanNodeOutputCommand {
                output: serde_json::json!({"output": "ready"}),
                completeness: NodeOutputCompleteness::AllowMissing(optional),
                recorded_runtime_values: BTreeMap::new(),
                branch: None,
            })
            .expect("output");
        let destination = work("done", Vec::new());
        let required = BTreeSet::from([String::from("output")]);
        let before = transition_environment_after_receipts(
            &reducer,
            &graph,
            &receipts,
            &destination,
            &required,
        )
        .expect("staged environment");
        for receipt in &receipts {
            apply(&mut reducer, receipt);
        }
        let restarted: CanonicalVariableEventReducer =
            serde_json::from_slice(&serde_json::to_vec(&reducer).expect("serialize"))
                .expect("restart");
        let after = CanonicalVariableCoordinator::new(&restarted, &graph, &destination)
            .expect("destination")
            .transition_environment(&required)
            .expect("environment");
        assert_eq!(before, after);
        assert_eq!(after["output"], "ready");
        let expression = Expression::parse("output == \"ready\"", ExpressionLimits::default())
            .expect("expression");
        assert_eq!(
            restarted.environment().classify_compiled_condition(
                &expression,
                &VariableReader {
                    node_id: String::from("done"),
                    branch_id: None,
                },
                &required,
            ),
            ConditionEligibility::Eligible
        );

        let second = CanonicalVariableCoordinator::new(&restarted, &graph, &producer)
            .expect("producer")
            .plan_node_output(&PlanNodeOutputCommand {
                output: serde_json::json!({"output": "again"}),
                completeness: NodeOutputCompleteness::AllowMissing(BTreeSet::from([
                    String::from("artifact"),
                    String::from("branch_local"),
                    String::from("secret"),
                    String::from("shared"),
                ])),
                recorded_runtime_values: BTreeMap::new(),
                branch: None,
            })
            .expect("second output");
        let RuntimeCommittedEvent::VariableAssigned(second) = &second[0].payload else {
            panic!("assignment");
        };
        assert_eq!(second.binding.prior_version, Some(1));
        assert_eq!(second.binding.new_version, Some(2));
    }
}
