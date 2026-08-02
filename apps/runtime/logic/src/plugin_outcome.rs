//! Runtime-owned validation of non-authoritative plugin-node outcomes.
//!
//! Validation is pure with respect to canonical runtime state. Artifact
//! references are inspected, never persisted, and canonical variable payloads
//! are prepared for an outer journal coordinator rather than committed here.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    str::FromStr,
};

use agentmod_expression_engine::{Expression, Operand, PathSegment};
use agentmod_graph_engine::{
    ExecutableEdge, ExecutableGraph, ExecutableNode, VariableScope, VariableValueType,
};
use agentmod_primitives::{ContentHash, SessionId};
use agentmod_runtime_data::{
    artifact::{ArtifactDataPort, InspectArtifactDataRequest},
    plugin::PluginNodeExecutorDataRecord,
};
use serde_json::Value;
use thiserror::Error;

use crate::{
    canonical_variable_coordinator::{
        CanonicalVariableCoordinator, NodeOutputCompleteness, PlanNodeOutputCommand,
        PreparedVariableEvent,
    },
    canonical_variables::{
        BranchWriteContext, CanonicalVariableEventReducer, CanonicalVariableValue,
        ConditionEligibility, VariableReader,
    },
    node_execution::NodeWorkIdentity,
    session::{
        CanonicalPluginNodeOutcomeProposal, PluginNodeInvocationIdentity,
        PluginNodeInvocationRecord, PluginNodeInvocationState, SessionNodeExecutorBoundary,
        SessionNodeExecutorResolution, SessionNodeExecutorSource, StyleExecutionContract,
        plugin_node_action_hash, plugin_node_actions_hash, plugin_node_value_hash,
    },
};

const MAX_PLUGIN_VALUE_BYTES: usize = 1024 * 1024;
const MAX_ARTIFACT_REFERENCES: usize = 128;
const MAX_STATE_KEYS: usize = 256;

/// Replay-derived remaining budget at the exact validation cut.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalPluginBudgetState {
    /// Graph steps remaining, including the active node.
    pub remaining_steps: u64,
    /// Provider tokens remaining.
    pub remaining_tokens: u64,
    /// Cost micros remaining.
    pub remaining_cost_micros: u64,
    /// Wall-clock duration remaining in milliseconds.
    pub remaining_duration_ms: u64,
}

/// Exact runtime-recorded values and graph context needed for validation.
#[derive(Clone, Debug)]
pub struct ValidatePluginNodeOutcomeCommand<'a> {
    /// Canonical session owning the result.
    pub session_id: SessionId,
    /// Exact immutable node-work identity.
    pub work: &'a NodeWorkIdentity,
    /// Exact compiled graph retained by replay.
    pub graph: &'a ExecutableGraph,
    /// Immutable persisted execution contract.
    pub execution_contract: &'a StyleExecutionContract,
    /// Exact persisted executor selected for this node.
    pub executor: &'a SessionNodeExecutorResolution,
    /// Exact live declaration matched to the persisted executor.
    pub declaration: &'a PluginNodeExecutorDataRecord,
    /// Replay-owned canonical variable projection.
    pub variables: &'a CanonicalVariableEventReducer,
    /// Non-authoritative plugin proposal.
    pub proposal: &'a CanonicalPluginNodeOutcomeProposal,
    /// Exact invocation identity carried by the terminal plugin receipt.
    pub receipt_identity: &'a PluginNodeInvocationIdentity,
    /// Canonical replay record to which the receipt and proposal must bind.
    pub canonical_invocation: &'a PluginNodeInvocationRecord,
    /// Artifact store selected by runtime configuration.
    pub artifact_store_root: PathBuf,
    /// Exact branch write context, when executing inside a parallel branch.
    pub branch: Option<BranchWriteContext>,
    /// Runtime-resolved timestamp/duration values, never plugin supplied.
    pub recorded_runtime_values: BTreeMap<String, CanonicalVariableValue>,
    /// State fields the invocation contract requires the plugin to preserve.
    pub required_preserved_state_keys: BTreeSet<String>,
    /// Canonical remaining budget.
    pub budget: CanonicalPluginBudgetState,
}

/// Runtime-owned transition proposal, still awaiting canonical commitment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedPluginTransition {
    /// Exact source node.
    pub from_node_id: String,
    /// Exact compiled destination.
    pub to_node_id: String,
    /// Stable optional compiled edge label.
    pub label: Option<String>,
}

/// Immutable artifact metadata verified through the runtime data boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedPluginArtifact {
    /// Portable immutable reference.
    pub artifact_reference: String,
    /// Exact content digest supplied by storage metadata.
    pub content_hash: String,
    /// Exact stored bytes.
    pub byte_size: u64,
    /// Exact media type.
    pub mime_type: String,
}

/// Runtime-understood proposed action class.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidatedPluginActionKind {
    /// Proposed tool action bounded by the declaration's exact tool permission.
    Tool {
        /// Exact declared tool.
        tool: String,
    },
    /// Proposed network action bounded by the declaration's exact network permission.
    Network {
        /// Exact declared network permission.
        permission: String,
    },
}

/// One runtime action proposal that must still cross normal action policy.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedPluginRuntimeActionProposal {
    /// Runtime-understood class and declared permission implication.
    pub kind: ValidatedPluginActionKind,
    /// Original bounded payload.
    pub payload: Value,
    /// Exact plugin proposal digest.
    pub action_hash: ContentHash,
}

/// Validated budget charge proposed by the plugin node.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ValidatedPluginBudgetUsage {
    /// Graph steps consumed by the proposal.
    pub steps: u64,
    /// Provider tokens consumed by the proposal.
    pub tokens: u64,
    /// Cost micros consumed by the proposal.
    pub cost_micros: u64,
    /// Wall-clock milliseconds consumed by the proposal.
    pub duration_ms: u64,
}

/// Complete logic-owned proposal set produced after validation.
///
/// No field is a committed event or a dependency handle.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedPluginNodeOutcome {
    /// Canonical session.
    pub session_id: SessionId,
    /// Exact work that produced the proposal.
    pub work: NodeWorkIdentity,
    /// Exact persisted executor identity.
    pub executor: SessionNodeExecutorResolution,
    /// Original bounded output digest.
    pub output_hash: ContentHash,
    /// Runtime-prepared canonical variable proposals.
    pub variable_events: Vec<PreparedVariableEvent>,
    /// Runtime-validated next transition proposal.
    pub transition: Option<ValidatedPluginTransition>,
    /// Immutable artifact references verified through data.
    pub artifacts: Vec<VerifiedPluginArtifact>,
    /// Consequential actions requiring ordinary runtime policy processing.
    pub runtime_actions: Vec<ValidatedPluginRuntimeActionProposal>,
    /// Validated canonical budget charge.
    pub budget_usage: ValidatedPluginBudgetUsage,
    /// Exact bounded plugin-owned state proposal.
    pub preserved_state: Value,
    /// Exact preserved state hash.
    pub preserved_state_hash: ContentHash,
    /// Declaration scope under which state may later be retained.
    pub state_scope: String,
}

/// Runtime outcome validator over the artifact data boundary.
#[derive(Clone)]
pub struct PluginNodeOutcomeValidator<A> {
    artifacts: A,
}

impl<A> PluginNodeOutcomeValidator<A> {
    /// Creates a validator. The data dependency is used only for immutable
    /// artifact inspection.
    #[must_use]
    pub const fn new(artifacts: A) -> Self {
        Self { artifacts }
    }
}

impl<A> PluginNodeOutcomeValidator<A>
where
    A: ArtifactDataPort,
{
    /// Validates one exact proposal and returns runtime proposals only.
    ///
    /// # Errors
    ///
    /// Fails closed for any identity, declaration, schema, variable,
    /// transition, artifact, state, permission, action, or budget mismatch.
    pub fn validate(
        &self,
        command: ValidatePluginNodeOutcomeCommand<'_>,
    ) -> Result<ValidatedPluginNodeOutcome, PluginNodeOutcomeValidationError> {
        let node = validate_exact_contract(&command)?;
        validate_branch_contract(&command, node)?;
        validate_proposal_hashes(command.proposal)?;
        validate_json_schema(&command.declaration.output_schema, &command.proposal.output)?;
        validate_bounded_value(&command.proposal.preserved_state)?;
        validate_preserved_state(
            &command.proposal.preserved_state,
            &command.declaration.state_scope,
            &command.required_preserved_state_keys,
        )?;

        let envelope = OutputEnvelope::parse(&command.proposal.output)?;
        validate_version_intent(node, command.variables, envelope.variable_versions)?;
        let coordinator =
            CanonicalVariableCoordinator::new(command.variables, command.graph, command.work)
                .map_err(|_| PluginNodeOutcomeValidationError::InvalidVariableWrite)?;
        let variable_events = coordinator
            .plan_node_output(&PlanNodeOutputCommand {
                output: Value::Object(envelope.variables.clone()),
                completeness: NodeOutputCompleteness::RequireAll,
                recorded_runtime_values: command.recorded_runtime_values,
                branch: command.branch,
            })
            .map_err(|_| PluginNodeOutcomeValidationError::InvalidVariableWrite)?;
        let transition = validate_transition(
            command.graph,
            command.variables,
            command.work,
            &variable_events,
            envelope.transition,
        )?;
        let artifact_references = collect_artifact_references(
            command.graph,
            envelope.variables,
            envelope.artifact_references,
        )?;
        let artifacts =
            self.validate_artifacts(&command.artifact_store_root, artifact_references)?;
        let runtime_actions = validate_actions(command.proposal, command.declaration)?;
        let budget_usage = validate_budget(envelope.budget_usage, command.budget)?;

        Ok(ValidatedPluginNodeOutcome {
            session_id: command.session_id,
            work: command.work.clone(),
            executor: command.executor.clone(),
            output_hash: command.proposal.output_hash,
            variable_events,
            transition,
            artifacts,
            runtime_actions,
            budget_usage,
            preserved_state: command.proposal.preserved_state.clone(),
            preserved_state_hash: command.proposal.preserved_state_hash,
            state_scope: command.declaration.state_scope.clone(),
        })
    }

    fn validate_artifacts(
        &self,
        store_root: &std::path::Path,
        references: Vec<String>,
    ) -> Result<Vec<VerifiedPluginArtifact>, PluginNodeOutcomeValidationError> {
        if references.len() > MAX_ARTIFACT_REFERENCES {
            return Err(PluginNodeOutcomeValidationError::InvalidArtifact);
        }
        let mut unique = BTreeSet::new();
        let mut verified = Vec::with_capacity(references.len());
        for artifact_reference in references {
            if artifact_reference.is_empty() || !unique.insert(artifact_reference.clone()) {
                return Err(PluginNodeOutcomeValidationError::InvalidArtifact);
            }
            let record = self
                .artifacts
                .inspect_artifact(InspectArtifactDataRequest {
                    store_root: store_root.to_owned(),
                    artifact_reference: artifact_reference.clone(),
                })
                .map_err(|_| PluginNodeOutcomeValidationError::InvalidArtifact)?;
            if record.artifact_reference != artifact_reference
                || record.content_hash.is_empty()
                || record.mime_type.is_empty()
            {
                return Err(PluginNodeOutcomeValidationError::InvalidArtifact);
            }
            verified.push(VerifiedPluginArtifact {
                artifact_reference,
                content_hash: record.content_hash,
                byte_size: record.byte_size,
                mime_type: record.mime_type,
            });
        }
        Ok(verified)
    }
}

fn validate_branch_contract(
    command: &ValidatePluginNodeOutcomeCommand<'_>,
    node: &ExecutableNode,
) -> Result<(), PluginNodeOutcomeValidationError> {
    match (command.work.branch_path.last(), command.branch.as_ref()) {
        (None, None) => return Ok(()),
        (Some(expected), Some(branch))
            if !branch.branch_id.is_empty()
                && branch.branch_id == *expected
                && !branch.serialized_shared_write => {}
        _ => {
            return Err(PluginNodeOutcomeValidationError::InvalidIdentity);
        }
    }
    if node.write_variables.iter().any(|name| {
        command.graph.variables.iter().any(|declaration| {
            declaration.name == *name
                && matches!(
                    declaration.scope,
                    VariableScope::Run | VariableScope::Session
                )
        })
    }) {
        return Err(PluginNodeOutcomeValidationError::InvalidVariableWrite);
    }
    Ok(())
}

struct OutputEnvelope<'a> {
    variables: &'a serde_json::Map<String, Value>,
    variable_versions: &'a serde_json::Map<String, Value>,
    transition: Option<&'a str>,
    artifact_references: Vec<String>,
    budget_usage: ValidatedPluginBudgetUsage,
}

impl<'a> OutputEnvelope<'a> {
    fn parse(output: &'a Value) -> Result<Self, PluginNodeOutcomeValidationError> {
        let object = output
            .as_object()
            .ok_or(PluginNodeOutcomeValidationError::InvalidSchema)?;
        let variables = object
            .get("variables")
            .and_then(Value::as_object)
            .ok_or(PluginNodeOutcomeValidationError::InvalidVariableWrite)?;
        let variable_versions = object
            .get("variable_versions")
            .and_then(Value::as_object)
            .ok_or(PluginNodeOutcomeValidationError::InvalidVariableWrite)?;
        let transition = object.get("transition").map_or(Ok(None), |value| {
            if value.is_null() {
                Ok(None)
            } else {
                value
                    .as_str()
                    .map(Some)
                    .ok_or(PluginNodeOutcomeValidationError::InvalidTransition)
            }
        })?;
        let artifact_references = object
            .get("artifact_references")
            .map_or(Ok(Vec::new()), parse_string_array)?;
        let budget_usage = object.get("budget_usage").map_or(
            Ok(ValidatedPluginBudgetUsage::default()),
            parse_budget_usage,
        )?;
        Ok(Self {
            variables,
            variable_versions,
            transition,
            artifact_references,
            budget_usage,
        })
    }
}

fn validate_exact_contract<'a>(
    command: &ValidatePluginNodeOutcomeCommand<'a>,
) -> Result<&'a ExecutableNode, PluginNodeOutcomeValidationError> {
    let invocation = command.receipt_identity;
    let canonical = command.canonical_invocation;
    let budgets = command.execution_contract.initial_budgets;
    let expected_remaining_steps = budgets
        .max_steps
        .checked_sub(command.work.step.saturating_sub(1));
    if command.work.run_id != command.execution_contract.run_id
        || command.work.node_id != command.executor.node_id
        || command.work.attempt == 0
        || command.work.step == 0
        || command.variables.run_id() != command.work.run_id
        || command
            .execution_contract
            .node_executors
            .iter()
            .find(|resolution| resolution.node_id == command.work.node_id)
            != Some(command.executor)
        || canonical.state != PluginNodeInvocationState::Completed
        || canonical.identity != *invocation
        || canonical.proposal.as_deref() != Some(command.proposal)
        || canonical.attempts == 0
        || canonical.terminal_at.is_none()
        || invocation.work != *command.work
        || invocation.executor != *command.executor
        || invocation.configuration_hash != command.executor.adapter_configuration_reference
        || invocation.invocation_id.is_empty()
        || is_zero_hash(invocation.invocation_digest)
        || is_zero_hash(invocation.input_hash)
        || is_zero_hash(invocation.readable_state_hash)
        || expected_remaining_steps != Some(command.budget.remaining_steps)
        || command.budget.remaining_tokens > budgets.max_tokens
        || command.budget.remaining_cost_micros > budgets.max_cost_micros
        || command.budget.remaining_duration_ms > budgets.max_duration_ms
    {
        return Err(PluginNodeOutcomeValidationError::InvalidIdentity);
    }
    let node = command
        .graph
        .nodes
        .iter()
        .find(|node| node.id == command.work.node_id)
        .ok_or(PluginNodeOutcomeValidationError::InvalidIdentity)?;
    let serialized_kind = serde_json::to_value(node.kind)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or(PluginNodeOutcomeValidationError::InvalidExecutor)?;
    let SessionNodeExecutorSource::Plugin { plugin_id } = &command.executor.source else {
        return Err(PluginNodeOutcomeValidationError::InvalidExecutor);
    };
    let capabilities = command
        .executor
        .resolved_capabilities
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let configuration_hash = serde_json::to_vec(node)
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| PluginNodeOutcomeValidationError::InvalidExecutor)?;
    if command.executor.boundary != SessionNodeExecutorBoundary::PluginHost
        || command.executor.node_kind != serialized_kind
        || command.declaration.node_kind != serialized_kind
        || command.executor.executor_id != command.declaration.executor_id
        || command.executor.executor_version != command.declaration.version
        || command.executor.runtime_api_requirement != command.declaration.runtime_api
        || command.executor.executor_declaration_hash != command.declaration.declaration_hash
        || command.executor.adapter_configuration_reference != configuration_hash
        || capabilities != command.declaration.capabilities
        || invocation.plugin_id != *plugin_id
        || !command
            .executor
            .required_capabilities
            .iter()
            .all(|capability| capabilities.contains(capability))
    {
        return Err(PluginNodeOutcomeValidationError::InvalidExecutor);
    }
    Ok(node)
}

fn is_zero_hash(hash: ContentHash) -> bool {
    hash == ContentHash::from_bytes([0; 32])
}

fn collect_artifact_references(
    graph: &ExecutableGraph,
    values: &serde_json::Map<String, Value>,
    explicit: Vec<String>,
) -> Result<Vec<String>, PluginNodeOutcomeValidationError> {
    let mut references = explicit.into_iter().collect::<BTreeSet<_>>();
    for (name, value) in values {
        let declaration = graph
            .variables
            .iter()
            .find(|declaration| declaration.name == *name)
            .ok_or(PluginNodeOutcomeValidationError::InvalidVariableWrite)?;
        collect_typed_artifact_references(value, &declaration.value_type, &mut references)?;
    }
    Ok(references.into_iter().collect())
}

fn collect_typed_artifact_references(
    value: &Value,
    value_type: &VariableValueType,
    references: &mut BTreeSet<String>,
) -> Result<(), PluginNodeOutcomeValidationError> {
    match value_type {
        VariableValueType::ArtifactReference => {
            let reference = value
                .as_str()
                .filter(|reference| !reference.is_empty())
                .ok_or(PluginNodeOutcomeValidationError::InvalidArtifact)?;
            references.insert(reference.to_owned());
        }
        VariableValueType::List { item_type, .. } => {
            for item in value
                .as_array()
                .ok_or(PluginNodeOutcomeValidationError::InvalidVariableWrite)?
            {
                collect_typed_artifact_references(item, item_type, references)?;
            }
        }
        VariableValueType::Map { value_type, .. } => {
            for item in value
                .as_object()
                .ok_or(PluginNodeOutcomeValidationError::InvalidVariableWrite)?
                .values()
            {
                collect_typed_artifact_references(item, value_type, references)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_proposal_hashes(
    proposal: &CanonicalPluginNodeOutcomeProposal,
) -> Result<(), PluginNodeOutcomeValidationError> {
    validate_bounded_value(&proposal.output)?;
    if proposal.output_hash
        != plugin_node_value_hash(&proposal.output)
            .map_err(|_| PluginNodeOutcomeValidationError::InvalidProposalHash)?
        || proposal.preserved_state_hash
            != plugin_node_value_hash(&proposal.preserved_state)
                .map_err(|_| PluginNodeOutcomeValidationError::InvalidProposalHash)?
        || proposal.proposed_actions_hash
            != plugin_node_actions_hash(&proposal.proposed_actions)
                .map_err(|_| PluginNodeOutcomeValidationError::InvalidProposalHash)?
        || proposal.proposed_actions.iter().any(|action| {
            plugin_node_action_hash(&action.kind, &action.payload)
                .map_or(true, |hash| hash != action.action_hash)
        })
    {
        return Err(PluginNodeOutcomeValidationError::InvalidProposalHash);
    }
    Ok(())
}

fn validate_version_intent(
    node: &ExecutableNode,
    variables: &CanonicalVariableEventReducer,
    versions: &serde_json::Map<String, Value>,
) -> Result<(), PluginNodeOutcomeValidationError> {
    if versions.keys().cloned().collect::<BTreeSet<_>>() != node.write_variables {
        return Err(PluginNodeOutcomeValidationError::InvalidVariableVersion);
    }
    for (name, value) in versions {
        let expected = variables
            .environment()
            .canonical_entries()
            .get(name)
            .map(|entry| entry.version);
        let supplied = if value.is_null() {
            None
        } else {
            value.as_u64()
        };
        if supplied != expected {
            return Err(PluginNodeOutcomeValidationError::InvalidVariableVersion);
        }
    }
    Ok(())
}

fn validate_transition(
    graph: &ExecutableGraph,
    variables: &CanonicalVariableEventReducer,
    work: &NodeWorkIdentity,
    variable_events: &[PreparedVariableEvent],
    target: Option<&str>,
) -> Result<Option<ValidatedPluginTransition>, PluginNodeOutcomeValidationError> {
    let source = graph
        .nodes
        .iter()
        .find(|node| node.id == work.node_id)
        .ok_or(PluginNodeOutcomeValidationError::InvalidTransition)?;
    let outgoing = graph
        .edges
        .iter()
        .filter(|edge| edge.from == source.index)
        .collect::<Vec<_>>();
    let Some(target) = target else {
        return if outgoing.is_empty() {
            Ok(None)
        } else {
            Err(PluginNodeOutcomeValidationError::InvalidTransition)
        };
    };
    let destination = graph
        .nodes
        .iter()
        .find(|node| node.id == target)
        .ok_or(PluginNodeOutcomeValidationError::InvalidTransition)?;
    let edge = outgoing
        .into_iter()
        .find(|edge| edge.to == destination.index)
        .ok_or(PluginNodeOutcomeValidationError::InvalidTransition)?;
    let mut staged = variables.clone();
    for prepared in variable_events {
        staged
            .apply(
                prepared
                    .payload
                    .canonical_variable_event()
                    .ok_or(PluginNodeOutcomeValidationError::InvalidVariableWrite)?,
            )
            .map_err(|_| PluginNodeOutcomeValidationError::InvalidVariableWrite)?;
    }
    if classify_edge(&staged, source, edge, work) != ConditionEligibility::Eligible {
        return Err(PluginNodeOutcomeValidationError::InvalidTransition);
    }
    Ok(Some(ValidatedPluginTransition {
        from_node_id: source.id.clone(),
        to_node_id: destination.id.clone(),
        label: edge.label.clone(),
    }))
}

fn classify_edge(
    variables: &CanonicalVariableEventReducer,
    source: &ExecutableNode,
    edge: &ExecutableEdge,
    work: &NodeWorkIdentity,
) -> ConditionEligibility {
    let Some(condition) = edge.condition.as_ref() else {
        return ConditionEligibility::Eligible;
    };
    let mut roots = BTreeSet::new();
    collect_expression_roots(condition, &mut roots);
    let required = roots
        .into_iter()
        .filter(|root| variables.environment().declarations().contains_key(root))
        .collect();
    variables.environment().classify_compiled_condition(
        condition,
        &VariableReader {
            node_id: source.id.clone(),
            branch_id: work.branch_path.last().cloned(),
        },
        &required,
    )
}

fn collect_expression_roots(expression: &Expression, roots: &mut BTreeSet<String>) {
    match expression {
        Expression::Value(operand) => collect_operand_root(operand, roots),
        Expression::Not(inner) => collect_expression_roots(inner, roots),
        Expression::And(left, right) | Expression::Or(left, right) => {
            collect_expression_roots(left, roots);
            collect_expression_roots(right, roots);
        }
        Expression::Compare { left, right, .. } => {
            collect_operand_root(left, roots);
            collect_operand_root(right, roots);
        }
        Expression::Exists(path) => collect_path_root(path.segments(), roots),
    }
}

fn collect_operand_root(operand: &Operand, roots: &mut BTreeSet<String>) {
    if let Operand::Path(path) = operand {
        collect_path_root(path.segments(), roots);
    }
}

fn collect_path_root(path: &[PathSegment], roots: &mut BTreeSet<String>) {
    if let Some(PathSegment::Key(root)) = path.first() {
        roots.insert(root.clone());
    }
}

fn validate_actions(
    proposal: &CanonicalPluginNodeOutcomeProposal,
    declaration: &PluginNodeExecutorDataRecord,
) -> Result<Vec<ValidatedPluginRuntimeActionProposal>, PluginNodeOutcomeValidationError> {
    if !proposal.proposed_actions.is_empty() && !declaration.external_effects {
        return Err(PluginNodeOutcomeValidationError::ExternalEffectProhibited);
    }
    proposal
        .proposed_actions
        .iter()
        .map(|action| {
            validate_bounded_value(&action.payload)
                .map_err(|_| PluginNodeOutcomeValidationError::InvalidAction)?;
            let payload = action
                .payload
                .as_object()
                .ok_or(PluginNodeOutcomeValidationError::InvalidAction)?;
            let kind = validate_action_kind(action.kind.as_str(), payload, declaration)?;
            Ok(ValidatedPluginRuntimeActionProposal {
                kind,
                payload: action.payload.clone(),
                action_hash: action.action_hash,
            })
        })
        .collect()
}

fn validate_action_kind(
    kind: &str,
    payload: &serde_json::Map<String, Value>,
    declaration: &PluginNodeExecutorDataRecord,
) -> Result<ValidatedPluginActionKind, PluginNodeOutcomeValidationError> {
    match kind {
        "tool.call" => validate_tool_action_payload(payload, declaration),
        "network.request" => validate_network_action_payload(payload, declaration),
        _ => Err(PluginNodeOutcomeValidationError::InvalidAction),
    }
}

fn validate_tool_action_payload(
    payload: &serde_json::Map<String, Value>,
    declaration: &PluginNodeExecutorDataRecord,
) -> Result<ValidatedPluginActionKind, PluginNodeOutcomeValidationError> {
    if payload
        .keys()
        .any(|key| !matches!(key.as_str(), "tool" | "arguments"))
    {
        return Err(PluginNodeOutcomeValidationError::InvalidAction);
    }
    let tool = payload
        .get("tool")
        .and_then(Value::as_str)
        .ok_or(PluginNodeOutcomeValidationError::InvalidAction)?;
    let arguments = payload
        .get("arguments")
        .and_then(Value::as_object)
        .ok_or(PluginNodeOutcomeValidationError::InvalidAction)?;
    if !declaration.tool_permissions.contains(tool) {
        return Err(PluginNodeOutcomeValidationError::PermissionNotDeclared);
    }
    if contains_forbidden_action_arguments(&Value::Object(arguments.clone())) {
        return Err(PluginNodeOutcomeValidationError::InvalidAction);
    }
    Ok(ValidatedPluginActionKind::Tool {
        tool: tool.to_owned(),
    })
}

fn validate_network_action_payload(
    payload: &serde_json::Map<String, Value>,
    declaration: &PluginNodeExecutorDataRecord,
) -> Result<ValidatedPluginActionKind, PluginNodeOutcomeValidationError> {
    if payload.keys().any(|key| {
        !matches!(
            key.as_str(),
            "permission"
                | "method"
                | "url"
                | "header_names"
                | "body_artifact_reference"
                | "body_hash"
        )
    }) {
        return Err(PluginNodeOutcomeValidationError::InvalidAction);
    }
    let permission = payload
        .get("permission")
        .and_then(Value::as_str)
        .ok_or(PluginNodeOutcomeValidationError::InvalidAction)?;
    if !declaration.network_permissions.contains(permission) {
        return Err(PluginNodeOutcomeValidationError::PermissionNotDeclared);
    }
    let method = payload
        .get("method")
        .and_then(Value::as_str)
        .ok_or(PluginNodeOutcomeValidationError::InvalidAction)?;
    let url = payload
        .get("url")
        .and_then(Value::as_str)
        .ok_or(PluginNodeOutcomeValidationError::InvalidAction)?;
    let header_names = payload
        .get("header_names")
        .and_then(Value::as_array)
        .ok_or(PluginNodeOutcomeValidationError::InvalidAction)?;
    if !matches!(
        method,
        "GET" | "HEAD" | "POST" | "PUT" | "PATCH" | "DELETE" | "OPTIONS"
    ) || url.len() > 8 * 1024
        || !(url.starts_with("https://") || url.starts_with("http://"))
        || url.contains('@')
        || url.chars().any(char::is_control)
        || header_names.len() > 64
        || header_names.iter().any(|name| {
            name.as_str().is_none_or(|name| {
                name.is_empty() || name.len() > 128 || name.chars().any(char::is_control)
            })
        })
        || payload
            .get("body_artifact_reference")
            .is_some_and(|reference| {
                !reference.is_null()
                    && reference.as_str().is_none_or(|reference| {
                        reference.is_empty()
                            || reference.len() > 1024
                            || reference.chars().any(char::is_control)
                    })
            })
        || payload.get("body_hash").is_some_and(|hash| {
            !hash.is_null()
                && hash
                    .as_str()
                    .is_none_or(|hash| ContentHash::from_str(hash).is_err())
        })
    {
        return Err(PluginNodeOutcomeValidationError::InvalidAction);
    }
    Ok(ValidatedPluginActionKind::Network {
        permission: permission.to_owned(),
    })
}

fn contains_forbidden_action_arguments(value: &Value) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, value)| {
            let key = key.to_ascii_lowercase();
            matches!(
                key.as_str(),
                "authorization"
                    | "cookie"
                    | "password"
                    | "passwd"
                    | "private_key"
                    | "secret"
                    | "secret_key"
                    | "token"
                    | "access_token"
                    | "refresh_token"
                    | "api_key"
            ) || contains_forbidden_action_arguments(value)
        }),
        Value::Array(values) => values.iter().any(contains_forbidden_action_arguments),
        Value::String(value) => {
            value.len() > MAX_PLUGIN_VALUE_BYTES
                || value.contains('\0')
                || value.starts_with("sk-")
                || value.starts_with("ghp_")
                || value.starts_with("github_pat_")
                || value.starts_with("AKIA")
                || value.contains("-----BEGIN PRIVATE KEY-----")
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn validate_preserved_state(
    state: &Value,
    state_scope: &str,
    required_keys: &BTreeSet<String>,
) -> Result<(), PluginNodeOutcomeValidationError> {
    if !matches!(
        state_scope,
        "invocation" | "model_call" | "turn" | "session" | "project" | "user" | "runtime"
    ) {
        return Err(PluginNodeOutcomeValidationError::InvalidPreservedState);
    }
    let object = state
        .as_object()
        .ok_or(PluginNodeOutcomeValidationError::InvalidPreservedState)?;
    if object.len() > MAX_STATE_KEYS
        || !required_keys.iter().all(|key| object.contains_key(key))
        || contains_forbidden_state(state)
    {
        return Err(PluginNodeOutcomeValidationError::InvalidPreservedState);
    }
    Ok(())
}

fn contains_forbidden_state(value: &Value) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, value)| {
            let normalized = key.to_ascii_lowercase();
            ((normalized.contains("secret")
                && value
                    .as_str()
                    .is_some_and(|text| !text.starts_with("secret://")))
                || normalized.contains("handle")
                || normalized.contains("socket")
                || normalized.contains("process_id"))
                || contains_forbidden_state(value)
        }),
        Value::Array(values) => values.iter().any(contains_forbidden_state),
        _ => false,
    }
}

fn validate_budget(
    usage: ValidatedPluginBudgetUsage,
    budget: CanonicalPluginBudgetState,
) -> Result<ValidatedPluginBudgetUsage, PluginNodeOutcomeValidationError> {
    if usage.steps == 0
        || usage.steps > budget.remaining_steps
        || usage.tokens > budget.remaining_tokens
        || usage.cost_micros > budget.remaining_cost_micros
        || usage.duration_ms > budget.remaining_duration_ms
    {
        return Err(PluginNodeOutcomeValidationError::BudgetExceeded);
    }
    Ok(usage)
}

fn parse_budget_usage(
    value: &Value,
) -> Result<ValidatedPluginBudgetUsage, PluginNodeOutcomeValidationError> {
    let object = value
        .as_object()
        .ok_or(PluginNodeOutcomeValidationError::InvalidBudget)?;
    if object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "steps" | "tokens" | "cost_micros" | "duration_ms"
        )
    }) {
        return Err(PluginNodeOutcomeValidationError::InvalidBudget);
    }
    Ok(ValidatedPluginBudgetUsage {
        steps: required_u64(object, "steps")?,
        tokens: required_u64(object, "tokens")?,
        cost_micros: required_u64(object, "cost_micros")?,
        duration_ms: required_u64(object, "duration_ms")?,
    })
}

fn required_u64(
    object: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<u64, PluginNodeOutcomeValidationError> {
    object
        .get(key)
        .and_then(Value::as_u64)
        .ok_or(PluginNodeOutcomeValidationError::InvalidBudget)
}

fn parse_string_array(value: &Value) -> Result<Vec<String>, PluginNodeOutcomeValidationError> {
    value
        .as_array()
        .ok_or(PluginNodeOutcomeValidationError::InvalidArtifact)?
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or(PluginNodeOutcomeValidationError::InvalidArtifact)
        })
        .collect()
}

fn validate_bounded_value(value: &Value) -> Result<(), PluginNodeOutcomeValidationError> {
    if serde_json::to_vec(value)
        .map_err(|_| PluginNodeOutcomeValidationError::InvalidSchema)?
        .len()
        > MAX_PLUGIN_VALUE_BYTES
    {
        return Err(PluginNodeOutcomeValidationError::InvalidSchema);
    }
    Ok(())
}

fn validate_json_schema(
    schema: &str,
    value: &Value,
) -> Result<(), PluginNodeOutcomeValidationError> {
    let schema: Value = serde_json::from_str(schema)
        .map_err(|_| PluginNodeOutcomeValidationError::InvalidSchema)?;
    validate_schema_value(&schema, value, 0)
}

fn validate_schema_value(
    schema: &Value,
    value: &Value,
    depth: usize,
) -> Result<(), PluginNodeOutcomeValidationError> {
    if depth > 32 {
        return Err(PluginNodeOutcomeValidationError::InvalidSchema);
    }
    if let Some(expected) = schema.get("type").and_then(Value::as_str) {
        let valid = match expected {
            "null" => value.is_null(),
            "boolean" => value.is_boolean(),
            "number" => value.is_number(),
            "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
            "string" => value.is_string(),
            "array" => value.is_array(),
            "object" => value.is_object(),
            _ => false,
        };
        if !valid {
            return Err(PluginNodeOutcomeValidationError::InvalidSchema);
        }
    }
    if let Some(allowed) = schema.get("enum").and_then(Value::as_array)
        && !allowed.contains(value)
    {
        return Err(PluginNodeOutcomeValidationError::InvalidSchema);
    }
    if let Some(object) = value.as_object() {
        if let Some(required) = schema.get("required").and_then(Value::as_array)
            && required
                .iter()
                .filter_map(Value::as_str)
                .any(|field| !object.contains_key(field))
        {
            return Err(PluginNodeOutcomeValidationError::InvalidSchema);
        }
        let properties = schema.get("properties").and_then(Value::as_object);
        if schema.get("additionalProperties") == Some(&Value::Bool(false))
            && object
                .keys()
                .any(|key| properties.is_none_or(|properties| !properties.contains_key(key)))
        {
            return Err(PluginNodeOutcomeValidationError::InvalidSchema);
        }
        if let Some(properties) = properties {
            for (key, nested_schema) in properties {
                if let Some(nested_value) = object.get(key) {
                    validate_schema_value(nested_schema, nested_value, depth + 1)?;
                }
            }
        }
    }
    if let (Some(values), Some(items)) = (value.as_array(), schema.get("items")) {
        for item in values {
            validate_schema_value(items, item, depth + 1)?;
        }
    }
    Ok(())
}

/// Stable fail-closed proposal validation classification.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PluginNodeOutcomeValidationError {
    /// Session/run/work identity did not match the immutable contract.
    #[error("plugin outcome identity does not match persisted work")]
    InvalidIdentity,
    /// Persisted executor and exact declaration did not match.
    #[error("plugin outcome executor declaration does not match persisted resolution")]
    InvalidExecutor,
    /// Proposal content hashes were invalid.
    #[error("plugin outcome proposal hash is invalid")]
    InvalidProposalHash,
    /// Output or declaration schema was invalid.
    #[error("plugin outcome does not match its output schema")]
    InvalidSchema,
    /// Variable set, type, security, or write policy was invalid.
    #[error("plugin outcome variable write is invalid")]
    InvalidVariableWrite,
    /// Optimistic variable-version intent was absent or stale.
    #[error("plugin outcome variable version intent is invalid")]
    InvalidVariableVersion,
    /// Proposed transition was absent, ineligible, or not compiled.
    #[error("plugin outcome transition is invalid")]
    InvalidTransition,
    /// Artifact reference was absent, duplicate, malformed, or unavailable.
    #[error("plugin outcome artifact reference is invalid")]
    InvalidArtifact,
    /// Preserved state exceeded or violated its exact declaration.
    #[error("plugin outcome preserved state is invalid")]
    InvalidPreservedState,
    /// Proposed action class or payload was invalid.
    #[error("plugin outcome action is invalid")]
    InvalidAction,
    /// Proposed action required an undeclared permission.
    #[error("plugin outcome action permission is not declared")]
    PermissionNotDeclared,
    /// Declaration prohibits external effects.
    #[error("plugin outcome proposed a prohibited external effect")]
    ExternalEffectProhibited,
    /// Budget usage shape was invalid.
    #[error("plugin outcome budget usage is invalid")]
    InvalidBudget,
    /// Proposed budget usage exceeded canonical remaining budget.
    #[error("plugin outcome exceeds remaining budget")]
    BudgetExceeded,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use agentmod_graph_engine::{
        ExecutableEdge, ExecutableGraph, ExecutableNode, GraphBudget, GraphCacheKey,
        GraphDeclarations, NodeKind, SecurityClassification, VariableDeclaration,
        VariableMutability, VariableScope, VariableValueType,
    };
    use agentmod_primitives::{EventId, Sequence};
    use agentmod_runtime_data::artifact::{
        ArtifactDataError, PersistArtifactDataRequest, PersistedArtifactDataRecord,
    };
    use serde_json::json;
    use uuid::Uuid;

    use crate::{
        canonical_variables::VariableEnvironmentLimits,
        session::{
            CanonicalPluginNodeActionProposal, SessionNodeExecutorBoundary,
            SessionNodeExecutorSource, SessionStyleBudgets,
        },
    };

    use super::*;

    const ARTIFACT_REFERENCE: &str =
        "artifact://blake3/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[derive(Clone, Copy)]
    struct MockArtifacts;

    impl ArtifactDataPort for MockArtifacts {
        fn persist_artifact(
            &self,
            _request: PersistArtifactDataRequest,
        ) -> Result<PersistedArtifactDataRecord, ArtifactDataError> {
            Err(ArtifactDataError::InvalidRequest)
        }

        fn inspect_artifact(
            &self,
            request: InspectArtifactDataRequest,
        ) -> Result<PersistedArtifactDataRecord, ArtifactDataError> {
            if request.artifact_reference != ARTIFACT_REFERENCE {
                return Err(ArtifactDataError::NotFound);
            }
            Ok(PersistedArtifactDataRecord {
                artifact_id: String::from("artifact"),
                artifact_reference: request.artifact_reference,
                mime_type: String::from("application/json"),
                byte_size: 12,
                creation_event: String::from("event"),
                producer: String::from("fixture.plugin"),
                security: agentmod_runtime_data::artifact::ArtifactSecurityRecord::Private,
                retention: agentmod_runtime_data::artifact::ArtifactRetentionRecord::Session,
                content_hash: String::from(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                ),
                deduplicated: false,
            })
        }
    }

    struct Fixture {
        graph: ExecutableGraph,
        work: NodeWorkIdentity,
        executor: SessionNodeExecutorResolution,
        declaration: PluginNodeExecutorDataRecord,
        contract: StyleExecutionContract,
        variables: CanonicalVariableEventReducer,
        proposal: CanonicalPluginNodeOutcomeProposal,
        receipt_identity: PluginNodeInvocationIdentity,
        canonical_invocation: PluginNodeInvocationRecord,
        branch: Option<BranchWriteContext>,
    }

    impl Fixture {
        fn command(&self) -> ValidatePluginNodeOutcomeCommand<'_> {
            ValidatePluginNodeOutcomeCommand {
                session_id: SessionId::from_uuid(Uuid::from_u128(1)),
                work: &self.work,
                graph: &self.graph,
                execution_contract: &self.contract,
                executor: &self.executor,
                declaration: &self.declaration,
                variables: &self.variables,
                proposal: &self.proposal,
                receipt_identity: &self.receipt_identity,
                canonical_invocation: &self.canonical_invocation,
                artifact_store_root: PathBuf::from("fixture-artifacts"),
                branch: self.branch.clone(),
                recorded_runtime_values: BTreeMap::new(),
                required_preserved_state_keys: BTreeSet::from([String::from("cursor")]),
                budget: CanonicalPluginBudgetState {
                    remaining_steps: 10,
                    remaining_tokens: 100,
                    remaining_cost_micros: 100,
                    remaining_duration_ms: 1_000,
                },
            }
        }

        fn reseal(&mut self) {
            seal(&mut self.proposal);
            self.canonical_invocation.proposal = Some(Box::new(self.proposal.clone()));
        }
    }

    fn graph() -> ExecutableGraph {
        let hash = ContentHash::digest(b"plugin-outcome-graph");
        ExecutableGraph {
            format_version: 1,
            entry_index: 0,
            budget: GraphBudget {
                max_steps: 10,
                max_tokens: 100,
                max_cost_micros: 100,
                max_duration_ms: 1_000,
            },
            declarations: GraphDeclarations::default(),
            variables: vec![VariableDeclaration {
                name: String::from("result"),
                value_type: VariableValueType::String,
                scope: VariableScope::Run,
                producer: String::from("plugin"),
                merge_contributors: BTreeSet::new(),
                consumers: BTreeSet::from([String::from("done")]),
                mutability: VariableMutability::Immutable,
                merge_policy: None,
                max_size_bytes: 128,
                security_classification: SecurityClassification::Internal,
            }],
            nodes: vec![
                ExecutableNode {
                    index: 0,
                    id: String::from("plugin"),
                    kind: NodeKind::ModelCall,
                    configuration: None,
                    condition: None,
                    tool: None,
                    provider: None,
                    required_capabilities: BTreeSet::from([String::from("model")]),
                    read_scopes: BTreeSet::new(),
                    write_scopes: BTreeSet::new(),
                    read_variables: BTreeSet::new(),
                    write_variables: BTreeSet::from([String::from("result")]),
                    retry_limit: 0,
                    max_iterations: None,
                },
                ExecutableNode {
                    index: 1,
                    id: String::from("done"),
                    kind: NodeKind::CompleteSession,
                    configuration: None,
                    condition: None,
                    tool: None,
                    provider: None,
                    required_capabilities: BTreeSet::new(),
                    read_scopes: BTreeSet::new(),
                    write_scopes: BTreeSet::new(),
                    read_variables: BTreeSet::from([String::from("result")]),
                    write_variables: BTreeSet::new(),
                    retry_limit: 0,
                    max_iterations: None,
                },
            ],
            edges: vec![ExecutableEdge {
                from: 0,
                to: 1,
                condition: None,
                label: Some(String::from("complete")),
            }],
            cache_key: GraphCacheKey {
                graph_content_hash: hash,
                plugin_set_hash: hash,
                capability_set_hash: hash,
                runtime_api_hash: hash,
                combined_hash: hash,
            },
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the exact persisted graph, executor, declaration, invocation, and proposal fixture is intentionally explicit"
    )]
    fn fixture() -> Fixture {
        let graph = graph();
        let work = NodeWorkIdentity {
            run_id: String::from("run-plugin"),
            node_id: String::from("plugin"),
            branch_path: vec![],
            attempt: 1,
            loop_iteration: 0,
            step: 1,
        };
        let declaration_hash = ContentHash::digest(b"declaration");
        let executor = SessionNodeExecutorResolution {
            node_id: work.node_id.clone(),
            node_kind: String::from("model_call"),
            executor_id: String::from("fixture.echo"),
            executor_version: String::from("1.0.0"),
            source: SessionNodeExecutorSource::Plugin {
                plugin_id: String::from("fixture.plugin"),
            },
            boundary: SessionNodeExecutorBoundary::PluginHost,
            required_capabilities: vec![String::from("model")],
            resolved_capabilities: vec![String::from("model")],
            runtime_api_requirement: String::from("^1"),
            executor_declaration_hash: declaration_hash,
            adapter_configuration_reference: ContentHash::digest(
                &serde_json::to_vec(&graph.nodes[0]).expect("compiled node"),
            ),
        };
        let declaration = PluginNodeExecutorDataRecord {
            plugin_version: String::from("1.0.0"),
            executor_id: executor.executor_id.clone(),
            version: executor.executor_version.clone(),
            runtime_api: executor.runtime_api_requirement.clone(),
            node_kind: executor.node_kind.clone(),
            handler: String::from("echo"),
            capabilities: BTreeSet::from([String::from("model")]),
            input_schema: String::from(r#"{"type":"object"}"#),
            output_schema: String::from(
                r#"{"type":"object","required":["variables","variable_versions","transition","artifact_references","budget_usage"]}"#,
            ),
            timeout_ms: 100,
            failure_policy: String::from("reject"),
            max_attempts: 1,
            retry_backoff_ms: 0,
            idempotent: true,
            tool_permissions: BTreeSet::from([String::from("fixture.tool")]),
            network_permissions: BTreeSet::from([String::from("api.example")]),
            state_scope: String::from("invocation"),
            external_effects: true,
            declaration_hash,
        };
        let contract = StyleExecutionContract {
            style_binding_hash: ContentHash::digest(b"binding"),
            execution_plan_hash: ContentHash::digest(b"plan"),
            registry_hash: ContentHash::digest(b"registry"),
            node_executors: vec![executor.clone()],
            initial_node_id: work.node_id.clone(),
            initial_variables_json: String::from("{}"),
            invocation_provider: Some(String::from("mock")),
            invocation_model: Some(String::from("mock-model")),
            invocation_options_json: None,
            initial_budgets: SessionStyleBudgets {
                max_iterations: 4,
                max_steps: 10,
                max_tokens: 100,
                max_cost_micros: 100,
                max_duration_ms: 1_000,
            },
            run_id: work.run_id.clone(),
        };
        let variables = CanonicalVariableEventReducer::initialize(
            work.run_id.clone(),
            VariableEnvironmentLimits::default(),
            graph.variables.clone(),
            [],
        )
        .expect("variables");
        let action = CanonicalPluginNodeActionProposal {
            kind: String::from("tool.call"),
            payload: json!({"tool":"fixture.tool","arguments":{"value":1}}),
            action_hash: ContentHash::from_bytes([0; 32]),
        };
        let mut proposal = CanonicalPluginNodeOutcomeProposal {
            output: json!({
                "variables":{"result":"ok"},
                "variable_versions":{"result":null},
                "transition":"done",
                "artifact_references":[ARTIFACT_REFERENCE],
                "budget_usage":{"steps":1,"tokens":2,"cost_micros":3,"duration_ms":4}
            }),
            output_hash: ContentHash::from_bytes([0; 32]),
            preserved_state: json!({"cursor":1}),
            preserved_state_hash: ContentHash::from_bytes([0; 32]),
            proposed_actions: vec![action],
            proposed_actions_hash: ContentHash::from_bytes([0; 32]),
        };
        seal(&mut proposal);
        let receipt_identity = PluginNodeInvocationIdentity {
            work: work.clone(),
            executor: executor.clone(),
            configuration_hash: executor.adapter_configuration_reference,
            plugin_id: String::from("fixture.plugin"),
            invocation_id: String::from("invocation-1"),
            invocation_digest: ContentHash::digest(b"invocation-1"),
            input_hash: ContentHash::digest(b"input-1"),
            readable_state_hash: ContentHash::digest(b"readable-state-1"),
            causation_event_id: EventId::from_uuid(Uuid::from_u128(2)),
        };
        let canonical_invocation = PluginNodeInvocationRecord {
            identity: receipt_identity.clone(),
            state: PluginNodeInvocationState::Completed,
            latest_event_id: EventId::from_uuid(Uuid::from_u128(3)),
            authorization_digest: Some(ContentHash::digest(b"authorization")),
            dispatch_digest: Some(ContentHash::digest(b"dispatch")),
            proposal: Some(Box::new(proposal.clone())),
            outcome_application: None,
            failure_code: None,
            diagnostic: None,
            attempts: 1,
            proposed_at: Sequence::new(1).expect("sequence"),
            authorized_at: Some(Sequence::new(2).expect("sequence")),
            dispatched_at: Some(Sequence::new(3).expect("sequence")),
            terminal_at: Some(Sequence::new(4).expect("sequence")),
        };
        Fixture {
            graph,
            work,
            executor,
            declaration,
            contract,
            variables,
            proposal,
            receipt_identity,
            canonical_invocation,
            branch: None,
        }
    }

    fn seal(proposal: &mut CanonicalPluginNodeOutcomeProposal) {
        for action in &mut proposal.proposed_actions {
            action.action_hash =
                plugin_node_action_hash(&action.kind, &action.payload).expect("action hash");
        }
        proposal.output_hash = plugin_node_value_hash(&proposal.output).expect("output hash");
        proposal.preserved_state_hash =
            plugin_node_value_hash(&proposal.preserved_state).expect("state hash");
        proposal.proposed_actions_hash =
            plugin_node_actions_hash(&proposal.proposed_actions).expect("actions hash");
    }

    #[test]
    fn validates_exact_schema_variables_transition_artifact_action_budget_and_state() {
        let fixture = fixture();
        let validated = PluginNodeOutcomeValidator::new(MockArtifacts)
            .validate(fixture.command())
            .expect("validated");
        assert_eq!(validated.work, fixture.work);
        assert_eq!(validated.variable_events.len(), 1);
        assert_eq!(
            validated
                .transition
                .as_ref()
                .map(|edge| edge.to_node_id.as_str()),
            Some("done")
        );
        assert_eq!(validated.artifacts.len(), 1);
        assert_eq!(
            validated.runtime_actions[0].kind,
            ValidatedPluginActionKind::Tool {
                tool: String::from("fixture.tool")
            }
        );
        assert_eq!(validated.budget_usage.steps, 1);
    }

    #[test]
    fn rejects_invalid_schema_transition_variable_and_artifact() {
        let validator = PluginNodeOutcomeValidator::new(MockArtifacts);

        let mut schema = fixture();
        schema.proposal.output = json!("not-an-object");
        schema.reseal();
        assert_eq!(
            validator.validate(schema.command()).expect_err("schema"),
            PluginNodeOutcomeValidationError::InvalidSchema
        );

        let mut transition = fixture();
        transition.proposal.output["transition"] = json!("missing");
        transition.reseal();
        assert_eq!(
            validator
                .validate(transition.command())
                .expect_err("transition"),
            PluginNodeOutcomeValidationError::InvalidTransition
        );

        let mut variable = fixture();
        variable.proposal.output["variables"]["result"] = json!(false);
        variable.reseal();
        assert_eq!(
            validator
                .validate(variable.command())
                .expect_err("variable"),
            PluginNodeOutcomeValidationError::InvalidVariableWrite
        );

        let mut artifact = fixture();
        artifact.proposal.output["artifact_references"] = json!(["artifact://blake3/missing"]);
        artifact.reseal();
        assert_eq!(
            validator
                .validate(artifact.command())
                .expect_err("artifact"),
            PluginNodeOutcomeValidationError::InvalidArtifact
        );
    }

    #[test]
    fn rejects_invalid_action_permission_external_effect_budget_and_preserved_state() {
        let validator = PluginNodeOutcomeValidator::new(MockArtifacts);

        let mut action = fixture();
        action.proposal.proposed_actions[0].kind = String::from("runtime.lifecycle");
        action.reseal();
        assert_eq!(
            validator.validate(action.command()).expect_err("action"),
            PluginNodeOutcomeValidationError::InvalidAction
        );

        let mut permission = fixture();
        permission.proposal.proposed_actions[0].payload["tool"] = json!("other.tool");
        permission.reseal();
        assert_eq!(
            validator
                .validate(permission.command())
                .expect_err("permission"),
            PluginNodeOutcomeValidationError::PermissionNotDeclared
        );

        let mut external = fixture();
        external.declaration.external_effects = false;
        assert_eq!(
            validator
                .validate(external.command())
                .expect_err("external"),
            PluginNodeOutcomeValidationError::ExternalEffectProhibited
        );

        let mut budget = fixture();
        budget.proposal.output["budget_usage"]["tokens"] = json!(101);
        budget.reseal();
        assert_eq!(
            validator.validate(budget.command()).expect_err("budget"),
            PluginNodeOutcomeValidationError::BudgetExceeded
        );

        let mut state = fixture();
        state.proposal.preserved_state = json!({"secret":"plaintext"});
        state.reseal();
        assert_eq!(
            validator.validate(state.command()).expect_err("state"),
            PluginNodeOutcomeValidationError::InvalidPreservedState
        );

        let mut extra_field = fixture();
        extra_field.proposal.proposed_actions[0].payload["forged_lifecycle"] = json!("completed");
        extra_field.reseal();
        assert_eq!(
            validator
                .validate(extra_field.command())
                .expect_err("forged action field"),
            PluginNodeOutcomeValidationError::InvalidAction
        );

        let mut secret = fixture();
        secret.proposal.proposed_actions[0].payload["arguments"] =
            json!({"api_key":"sk-fixture-secret"});
        secret.reseal();
        assert_eq!(
            validator
                .validate(secret.command())
                .expect_err("secret action argument"),
            PluginNodeOutcomeValidationError::InvalidAction
        );
    }

    #[test]
    fn validates_typed_network_action_and_rejects_userinfo_or_header_values() {
        let validator = PluginNodeOutcomeValidator::new(MockArtifacts);
        let mut network = fixture();
        network.proposal.proposed_actions[0].kind = String::from("network.request");
        network.proposal.proposed_actions[0].payload = json!({
            "permission":"api.example",
            "method":"GET",
            "url":"https://api.example/items",
            "header_names":["accept"],
            "body_artifact_reference":null,
            "body_hash":null
        });
        network.reseal();
        assert_eq!(
            validator
                .validate(network.command())
                .expect("network action")
                .runtime_actions[0]
                .kind,
            ValidatedPluginActionKind::Network {
                permission: String::from("api.example")
            }
        );

        network.proposal.proposed_actions[0].payload["url"] =
            json!("https://user:secret@api.example/items");
        network.reseal();
        assert_eq!(
            validator.validate(network.command()).expect_err("userinfo"),
            PluginNodeOutcomeValidationError::InvalidAction
        );

        network.proposal.proposed_actions[0].payload["url"] = json!("https://api.example/items");
        network.proposal.proposed_actions[0].payload["headers"] = json!({"authorization":"secret"});
        network.reseal();
        assert_eq!(
            validator
                .validate(network.command())
                .expect_err("header values"),
            PluginNodeOutcomeValidationError::InvalidAction
        );
    }

    #[test]
    fn rejects_stale_variable_version_and_executor_substitution() {
        let validator = PluginNodeOutcomeValidator::new(MockArtifacts);
        let mut version = fixture();
        version.proposal.output["variable_versions"]["result"] = json!(7);
        version.reseal();
        assert_eq!(
            validator.validate(version.command()).expect_err("version"),
            PluginNodeOutcomeValidationError::InvalidVariableVersion
        );

        let mut executor = fixture();
        executor.executor.executor_version = String::from("2.0.0");
        assert_eq!(
            validator
                .validate(executor.command())
                .expect_err("executor"),
            PluginNodeOutcomeValidationError::InvalidIdentity
        );

        let mut invocation = fixture();
        invocation.receipt_identity.input_hash = ContentHash::digest(b"substituted-input");
        assert_eq!(
            validator
                .validate(invocation.command())
                .expect_err("invocation identity"),
            PluginNodeOutcomeValidationError::InvalidIdentity
        );
    }

    #[test]
    fn branch_outcome_binds_identity_transition_and_artifact_and_rejects_shared_writes() {
        let validator = PluginNodeOutcomeValidator::new(MockArtifacts);
        let mut branch = fixture();
        branch.graph.variables[0].scope = VariableScope::Branch;
        branch.variables = CanonicalVariableEventReducer::initialize(
            branch.work.run_id.clone(),
            VariableEnvironmentLimits::default(),
            branch.graph.variables.clone(),
            [],
        )
        .expect("branch variables");
        branch.work.branch_path = vec![String::from("branch-a")];
        branch.receipt_identity.work = branch.work.clone();
        branch.canonical_invocation.identity = branch.receipt_identity.clone();
        branch.branch = Some(BranchWriteContext {
            branch_id: String::from("branch-a"),
            stable_order: 1,
            serialized_shared_write: false,
        });
        let validated = validator
            .validate(branch.command())
            .expect("branch outcome");
        assert_eq!(validated.work.branch_path, ["branch-a"]);
        assert_eq!(
            validated.artifacts[0].artifact_reference,
            ARTIFACT_REFERENCE
        );
        assert_eq!(
            validated
                .transition
                .as_ref()
                .map(|edge| edge.to_node_id.as_str()),
            Some("done")
        );

        let mut substituted = branch;
        substituted.branch.as_mut().expect("branch").branch_id = String::from("branch-b");
        assert_eq!(
            validator
                .validate(substituted.command())
                .expect_err("substituted branch"),
            PluginNodeOutcomeValidationError::InvalidIdentity
        );

        let mut shared = fixture();
        shared.work.branch_path = vec![String::from("branch-a")];
        shared.receipt_identity.work = shared.work.clone();
        shared.canonical_invocation.identity = shared.receipt_identity.clone();
        shared.branch = Some(BranchWriteContext {
            branch_id: String::from("branch-a"),
            stable_order: 1,
            serialized_shared_write: false,
        });
        assert_eq!(
            validator
                .validate(shared.command())
                .expect_err("shared branch write"),
            PluginNodeOutcomeValidationError::InvalidVariableWrite
        );
    }
}
