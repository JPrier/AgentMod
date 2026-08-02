//! Runtime-owned generic node execution contract and native dispatch.
//!
//! Executors receive bounded canonical state and return proposals describing
//! graph outcomes. They cannot persist events or mutate session state.

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use agentmod_event_model::{ArtifactIdentifier, ArtifactReference};
use agentmod_graph_engine::NodeConfiguration;
use agentmod_primitives::{ContentHash, ContinuationId, SessionId};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::session::{
    SessionNodeExecutorBoundary, SessionNodeExecutorResolution, SessionNodeExecutorSource,
};

/// Runtime implementation key derived from one exact persisted native
/// executor identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeExecutorKey {
    ContextConstruction,
    ModelRequest,
    ToolGate,
    UserApproval,
    ChildSpawn,
    ChildMessage,
    ChildWait,
    Join,
    Review,
    Loop,
    Conditional,
    Parallel,
    Delay,
    Schedule,
    EventEmission,
    ArtifactPersistence,
    TurnCompletion,
    SessionCompletion,
    StructuredFailure,
}

/// Resolves a runtime handler key from the complete exact first-party identity.
pub(crate) fn native_executor_key(
    resolution: &SessionNodeExecutorResolution,
) -> Result<NativeExecutorKey, NativeNodeExecutionError> {
    if resolution.source != SessionNodeExecutorSource::Runtime
        || resolution.boundary != SessionNodeExecutorBoundary::RuntimeLogic
    {
        return Err(NativeNodeExecutionError::UnsupportedExecutorIdentity);
    }
    let key = match (
        resolution.executor_id.as_str(),
        resolution.node_kind.as_str(),
    ) {
        ("runtime.context-construction", "context_transform") => {
            Ok(NativeExecutorKey::ContextConstruction)
        }
        ("runtime.model-request", "model_call") => Ok(NativeExecutorKey::ModelRequest),
        ("runtime.tool-gate", "tool_execution_gate") => Ok(NativeExecutorKey::ToolGate),
        ("runtime.user-approval", "user_approval") => Ok(NativeExecutorKey::UserApproval),
        ("runtime.child-spawn", "spawn_child_agent") => Ok(NativeExecutorKey::ChildSpawn),
        ("runtime.child-message", "send_child_agent_message") => {
            Ok(NativeExecutorKey::ChildMessage)
        }
        ("runtime.child-wait", "wait_for_agents") => Ok(NativeExecutorKey::ChildWait),
        ("runtime.join", "join_results") => Ok(NativeExecutorKey::Join),
        ("runtime.review", "review") => Ok(NativeExecutorKey::Review),
        ("runtime.loop", "loop") => Ok(NativeExecutorKey::Loop),
        ("runtime.conditional", "conditional_branch") => Ok(NativeExecutorKey::Conditional),
        ("runtime.parallel", "parallel_branch") => Ok(NativeExecutorKey::Parallel),
        ("runtime.delay", "delay") => Ok(NativeExecutorKey::Delay),
        ("runtime.schedule", "schedule") => Ok(NativeExecutorKey::Schedule),
        ("runtime.event-emission", "emit_event") => Ok(NativeExecutorKey::EventEmission),
        ("runtime.artifact-persistence", "persist_artifact") => {
            Ok(NativeExecutorKey::ArtifactPersistence)
        }
        ("runtime.turn-completion", "complete_turn") => Ok(NativeExecutorKey::TurnCompletion),
        ("runtime.session-completion", "complete_session") => {
            Ok(NativeExecutorKey::SessionCompletion)
        }
        ("runtime.structured-failure", "fail") => Ok(NativeExecutorKey::StructuredFailure),
        _ => return Err(NativeNodeExecutionError::UnsupportedExecutorIdentity),
    }?;
    if resolution.executor_version == "1.0.0"
        || (resolution.executor_version == "1.1.0"
            && matches!(
                key,
                NativeExecutorKey::ToolGate
                    | NativeExecutorKey::ModelRequest
                    | NativeExecutorKey::ChildSpawn
                    | NativeExecutorKey::Review
                    | NativeExecutorKey::ArtifactPersistence
            ))
    {
        Ok(key)
    } else {
        Err(NativeNodeExecutionError::UnsupportedExecutorIdentity)
    }
}

/// Bounded input supplied to one exact node implementation.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeExecutionInput {
    /// Canonical variables available to compiled transition expressions.
    pub transition_variables: Value,
}

/// Replay-derived graph state visible to a node implementation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalGraphState {
    /// One-based attempt of the active node.
    pub attempt: u32,
    /// Zero-based enclosing loop iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Stable completed-node IDs in canonical order.
    pub completed_node_ids: Vec<String>,
}

/// Runtime-enforced budgets visible to a node implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalBudgetState {
    /// Effective maximum graph step.
    pub max_steps: u64,
    /// Graph steps remaining, including the active node.
    pub remaining_steps: u64,
    /// Static iteration limit declared by the active node, when applicable.
    pub max_iterations: Option<u32>,
    /// Iterations remaining after the current iteration.
    pub remaining_iterations: Option<u32>,
}

/// Immutable identity of one node-work attempt within a graph run.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct NodeWorkIdentity {
    /// Immutable execution-contract run identity.
    pub run_id: String,
    /// Stable compiled node ID.
    pub node_id: String,
    /// Stable runtime-owned nested parallel branch path, empty for the root graph.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub branch_path: Vec<String>,
    /// One-based execution attempt for this node work.
    pub attempt: u32,
    /// Zero-based enclosing loop iteration.
    pub loop_iteration: u32,
    /// One-based graph step for this node work.
    pub step: u64,
}

/// Logic-owned command for one exact persisted node implementation.
#[derive(Clone, Debug, PartialEq)]
pub struct ExecuteNodeCommand {
    /// Canonical owning session.
    pub session_id: SessionId,
    /// Exact immutable node-work identity.
    pub work: NodeWorkIdentity,
    /// Exact executor resolution copied from the immutable execution plan.
    pub executor: SessionNodeExecutorResolution,
    /// Exact validated configuration retained by the compiled graph.
    pub configuration: Option<NodeConfiguration>,
    /// Bounded typed node input.
    pub input: NodeExecutionInput,
    /// Replay-derived graph state.
    pub graph_state: CanonicalGraphState,
    /// Runtime-enforced budget state.
    pub budget_state: CanonicalBudgetState,
}

/// Successful node output proposed to runtime orchestration.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeExecutionOutput {
    /// Bounded durable result identity, when produced.
    pub result_reference: Option<String>,
    /// Immutable artifact reference for large output, when produced.
    pub artifact_reference: Option<String>,
    /// Canonical variables used only for compiled transition evaluation.
    pub transition_variables: Value,
}

/// Runtime-validated content for one constrained user-space event.
///
/// This is deliberately not an event envelope: runtime orchestration retains
/// authority over event IDs, scope, sequence, correlation, causation, and
/// commitment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserSpaceEventProposal {
    /// Declared user-space event type carried inside the static runtime event.
    pub declared_event_type: String,
    /// Bounded typed payload.
    pub payload: Value,
    /// Declared immutable artifact references.
    pub artifact_references: BTreeSet<String>,
    /// Bounded metadata after runtime secret-key validation and normalization.
    pub metadata: BTreeMap<String, String>,
}

/// Child execution proposed by an executor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildExecutionReference {
    /// Stable runtime-owned child identity.
    pub child_id: String,
}

/// Parallel branch proposed by an executor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchExecutionReference {
    /// Stable runtime-owned branch identity.
    pub branch_id: String,
}

/// Structured retry proposal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryReason {
    /// Stable redacted retry code.
    pub code: String,
}

/// Structured node failure proposed to runtime orchestration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StructuredNodeFailure {
    /// Stable redacted failure code.
    pub code: String,
    /// Optional immutable artifact containing bounded details.
    pub artifact_reference: Option<String>,
}

/// Runtime-owned terminal disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionTermination {
    /// Complete only the current turn.
    CompleteTurn,
    /// Complete the owning session.
    CompleteSession,
    /// Fail the owning style execution.
    Failed,
}

/// Typed outcome returned by a node implementation.
#[derive(Clone, Debug, PartialEq)]
pub enum NodeExecutionOutcome {
    /// The node completed and runtime may validate a compiled transition.
    Completed {
        /// Runtime-validated bounded output.
        output: NodeExecutionOutput,
    },
    /// The node proposed one constrained user-space event for runtime commit.
    Emitted {
        /// Runtime-validated event content without canonical envelope fields.
        event: UserSpaceEventProposal,
        /// Transition output applied only after the event is canonical.
        output: NodeExecutionOutput,
    },
    /// The node is durably waiting for an existing continuation.
    Waiting {
        /// Existing durable continuation identity.
        continuation: ContinuationId,
    },
    /// The node proposed child executions.
    Spawned {
        /// Proposed runtime-owned children.
        children: Vec<ChildExecutionReference>,
    },
    /// The node proposed bounded parallel branches.
    Parallel {
        /// Proposed runtime-owned branches.
        branches: Vec<BranchExecutionReference>,
    },
    /// The node requested a policy-validated retry.
    Retry {
        /// Structured retry reason.
        reason: RetryReason,
    },
    /// The node failed with structured bounded details.
    Failed {
        /// Structured bounded failure.
        failure: StructuredNodeFailure,
    },
    /// The node reached a compiled terminal disposition.
    Terminal {
        /// Requested compiled terminal disposition.
        outcome: SessionTermination,
    },
}

/// Dispatches one implemented first-party node by its exact persisted
/// implementation identity.
///
/// This is implementation dispatch, not a capability registry: availability
/// and selection remain owned by the single immutable registry and execution
/// plan.
///
/// # Errors
///
/// Fails closed when the command does not name an exact supported first-party
/// implementation or when bounded graph state is invalid.
pub fn execute_native_node(
    command: &ExecuteNodeCommand,
) -> Result<NodeExecutionOutcome, NativeNodeExecutionError> {
    validate_command(command)?;
    match native_executor_key(&command.executor)? {
        NativeExecutorKey::Conditional => Ok(completed(
            command.input.transition_variables.clone(),
            "conditional",
        )),
        NativeExecutorKey::Loop => {
            let limit = command
                .budget_state
                .max_iterations
                .ok_or(NativeNodeExecutionError::InvalidLoopBudget)?;
            let completed_iterations = command
                .graph_state
                .loop_iteration
                .checked_add(1)
                .ok_or(NativeNodeExecutionError::InvalidLoopBudget)?;
            let remaining = completed_iterations < limit;
            Ok(completed(
                serde_json::json!({"iteration":{"remaining":remaining}}),
                &format!("loop:remaining:{remaining}"),
            ))
        }
        NativeExecutorKey::TurnCompletion => Ok(NodeExecutionOutcome::Terminal {
            outcome: SessionTermination::CompleteTurn,
        }),
        NativeExecutorKey::SessionCompletion => Ok(NodeExecutionOutcome::Terminal {
            outcome: SessionTermination::CompleteSession,
        }),
        NativeExecutorKey::StructuredFailure => Ok(NodeExecutionOutcome::Terminal {
            outcome: SessionTermination::Failed,
        }),
        NativeExecutorKey::EventEmission => {
            let event = user_space_event_proposal(command.configuration.as_ref())?;
            Ok(NodeExecutionOutcome::Emitted {
                output: NodeExecutionOutput {
                    result_reference: Some(format!(
                        "user-space-event:{}:{}",
                        event.declared_event_type, command.work.step
                    )),
                    artifact_reference: None,
                    transition_variables: command.input.transition_variables.clone(),
                },
                event,
            })
        }
        _ => Err(NativeNodeExecutionError::UnsupportedExecutorIdentity),
    }
}

const MAX_USER_EVENT_PAYLOAD_BYTES: usize = 64 * 1024;
const MAX_USER_EVENT_VALUE_DEPTH: usize = 32;
const MAX_USER_EVENT_ARTIFACTS: usize = 128;
const MAX_USER_EVENT_METADATA_ENTRIES: usize = 128;
const MAX_USER_EVENT_METADATA_BYTES: usize = 16 * 1024;
const MAX_USER_EVENT_NAME_BYTES: usize = 256;
const MAX_INITIAL_VARIABLE_BYTES: usize = 64 * 1024;
const MAX_INITIAL_VARIABLE_DEPTH: usize = 32;
const MAX_INITIAL_VARIABLE_COLLECTION_ITEMS: usize = 1024;
const MAX_INITIAL_VARIABLE_TOTAL_VALUES: usize = 8192;
const MAX_INITIAL_VARIABLE_STRING_BYTES: usize = 16 * 1024;

/// Validates and canonically serializes the immutable initial generic variable
/// environment stored in the execution contract.
pub(crate) fn canonical_initial_variables_json(
    variables: &Value,
) -> Result<String, NativeNodeExecutionError> {
    if !variables.is_object() {
        return Err(NativeNodeExecutionError::InvalidInitialVariables);
    }
    let mut total_values = 0_usize;
    validate_initial_variable_shape(variables, 1, &mut total_values)?;
    let bytes = serde_json::to_vec(&canonicalize_json(variables))
        .map_err(|_| NativeNodeExecutionError::InvalidInitialVariables)?;
    if bytes.len() > MAX_INITIAL_VARIABLE_BYTES {
        return Err(NativeNodeExecutionError::InvalidInitialVariables);
    }
    String::from_utf8(bytes).map_err(|_| NativeNodeExecutionError::InvalidInitialVariables)
}

fn validate_initial_variable_shape(
    value: &Value,
    depth: usize,
    total_values: &mut usize,
) -> Result<(), NativeNodeExecutionError> {
    *total_values = total_values
        .checked_add(1)
        .ok_or(NativeNodeExecutionError::InvalidInitialVariables)?;
    if depth > MAX_INITIAL_VARIABLE_DEPTH || *total_values > MAX_INITIAL_VARIABLE_TOTAL_VALUES {
        return Err(NativeNodeExecutionError::InvalidInitialVariables);
    }
    match value {
        Value::Array(values) => {
            if values.len() > MAX_INITIAL_VARIABLE_COLLECTION_ITEMS {
                return Err(NativeNodeExecutionError::InvalidInitialVariables);
            }
            for value in values {
                validate_initial_variable_shape(value, depth + 1, total_values)?;
            }
        }
        Value::Object(values) => {
            if values.len() > MAX_INITIAL_VARIABLE_COLLECTION_ITEMS
                || values.keys().any(|key| {
                    key.is_empty()
                        || key.len() > MAX_USER_EVENT_NAME_BYTES
                        || key.chars().any(char::is_control)
                })
            {
                return Err(NativeNodeExecutionError::InvalidInitialVariables);
            }
            for value in values.values() {
                validate_initial_variable_shape(value, depth + 1, total_values)?;
            }
        }
        Value::String(value) if value.len() > MAX_INITIAL_VARIABLE_STRING_BYTES => {
            return Err(NativeNodeExecutionError::InvalidInitialVariables);
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonicalize_json).collect()),
        Value::Object(values) => {
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            let mut canonical = Map::new();
            for (key, value) in entries {
                canonical.insert(key.clone(), canonicalize_json(value));
            }
            Value::Object(canonical)
        }
        scalar => scalar.clone(),
    }
}

/// Validates the exact compiled event configuration and returns the canonical
/// bounded content runtime may commit.
pub(crate) fn user_space_event_proposal(
    configuration: Option<&NodeConfiguration>,
) -> Result<UserSpaceEventProposal, NativeNodeExecutionError> {
    let Some(NodeConfiguration::EmitEvent {
        event_type,
        payload,
        artifact_references,
        metadata,
    }) = configuration
    else {
        return Err(NativeNodeExecutionError::InvalidEventConfiguration);
    };
    if invalid_user_event_type(event_type)
        || serde_json::to_vec(payload)
            .map_or(true, |bytes| bytes.len() > MAX_USER_EVENT_PAYLOAD_BYTES)
        || json_depth(payload) > MAX_USER_EVENT_VALUE_DEPTH
        || artifact_references.len() > MAX_USER_EVENT_ARTIFACTS
        || metadata.len() > MAX_USER_EVENT_METADATA_ENTRIES
    {
        return Err(NativeNodeExecutionError::InvalidEventConfiguration);
    }
    canonical_user_event_artifacts(artifact_references)?;
    let mut sanitized_metadata = BTreeMap::new();
    let mut metadata_bytes = 0_usize;
    for (key, value) in metadata {
        let normalized_key = key.trim();
        let normalized_value = value.trim();
        let lower_key = normalized_key.to_ascii_lowercase();
        if normalized_key.is_empty()
            || normalized_key.len() > MAX_USER_EVENT_NAME_BYTES
            || normalized_value.len() > MAX_USER_EVENT_NAME_BYTES
            || normalized_key.chars().any(char::is_control)
            || normalized_value.chars().any(char::is_control)
            || secret_metadata_key(&lower_key)
            || envelope_metadata_key(&lower_key)
        {
            return Err(NativeNodeExecutionError::InvalidEventConfiguration);
        }
        metadata_bytes = metadata_bytes
            .checked_add(normalized_key.len())
            .and_then(|size| size.checked_add(normalized_value.len()))
            .ok_or(NativeNodeExecutionError::InvalidEventConfiguration)?;
        sanitized_metadata.insert(normalized_key.to_owned(), normalized_value.to_owned());
    }
    if metadata_bytes > MAX_USER_EVENT_METADATA_BYTES {
        return Err(NativeNodeExecutionError::InvalidEventConfiguration);
    }
    Ok(UserSpaceEventProposal {
        declared_event_type: event_type.clone(),
        payload: payload.clone(),
        artifact_references: artifact_references.clone(),
        metadata: sanitized_metadata,
    })
}

/// Converts declared portable content-addressed references into the exact
/// canonical event-envelope artifact identities they bind.
pub(crate) fn canonical_user_event_artifacts(
    references: &BTreeSet<String>,
) -> Result<Vec<ArtifactReference>, NativeNodeExecutionError> {
    references
        .iter()
        .map(|reference| {
            if reference.len() > MAX_USER_EVENT_NAME_BYTES {
                return Err(NativeNodeExecutionError::InvalidEventConfiguration);
            }
            let identifier = reference
                .strip_prefix("artifact:")
                .ok_or(NativeNodeExecutionError::InvalidEventConfiguration)?;
            let hash = identifier
                .strip_prefix("blake3:")
                .ok_or(NativeNodeExecutionError::InvalidEventConfiguration)?;
            Ok(ArtifactReference {
                id: ArtifactIdentifier::parse(identifier)
                    .map_err(|_| NativeNodeExecutionError::InvalidEventConfiguration)?,
                content_hash: ContentHash::from_str(hash)
                    .map_err(|_| NativeNodeExecutionError::InvalidEventConfiguration)?,
            })
        })
        .collect()
}

fn invalid_user_event_type(event_type: &str) -> bool {
    const RESERVED_PREFIXES: &[&str] = &[
        "approval.",
        "artifact.",
        "child_agent.",
        "child_session.",
        "context.",
        "conversation.",
        "model.",
        "permission.",
        "plugin.",
        "process.",
        "provider.",
        "runtime.",
        "scheduler.",
        "security.",
        "session.",
        "style.",
        "tool.",
    ];
    let normalized = event_type.trim();
    let lower = normalized.to_ascii_lowercase();
    normalized.is_empty()
        || normalized.len() > MAX_USER_EVENT_NAME_BYTES
        || normalized.chars().any(char::is_control)
        || RESERVED_PREFIXES
            .iter()
            .any(|prefix| lower.starts_with(prefix))
}

fn secret_metadata_key(lower_key: &str) -> bool {
    [
        "authorization",
        "cookie",
        "credential",
        "password",
        "private_key",
        "secret",
        "token",
        "api_key",
    ]
    .iter()
    .any(|sensitive| lower_key.contains(sensitive))
}

fn envelope_metadata_key(lower_key: &str) -> bool {
    matches!(
        lower_key,
        "artifacts"
            | "causation_id"
            | "classification"
            | "correlation_id"
            | "event_id"
            | "event_type"
            | "event_version"
            | "origin"
            | "parent_graph_node_id"
            | "schema_version"
            | "scope"
            | "sequence"
            | "timestamp"
    )
}

fn json_depth(value: &Value) -> usize {
    match value {
        Value::Array(values) => 1 + values.iter().map(json_depth).max().unwrap_or(0),
        Value::Object(values) => 1 + values.values().map(json_depth).max().unwrap_or(0),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => 1,
    }
}

fn completed(transition_variables: Value, reference: &str) -> NodeExecutionOutcome {
    NodeExecutionOutcome::Completed {
        output: NodeExecutionOutput {
            result_reference: Some(reference.to_owned()),
            artifact_reference: None,
            transition_variables,
        },
    }
}

fn validate_command(command: &ExecuteNodeCommand) -> Result<(), NativeNodeExecutionError> {
    if command.work.run_id.trim().is_empty()
        || command.work.node_id.trim().is_empty()
        || command.work.branch_path.len() > 32
        || command
            .work
            .branch_path
            .iter()
            .any(|branch| branch.trim().is_empty() || branch.len() > MAX_USER_EVENT_NAME_BYTES)
        || command.executor.node_id != command.work.node_id
        || command.work.attempt == 0
        || command.work.attempt != command.graph_state.attempt
        || command.work.loop_iteration != command.graph_state.loop_iteration
        || command.work.step == 0
        || command.work.step != command.graph_state.step
        || command.graph_state.attempt == 0
        || command.graph_state.step == 0
        || command.graph_state.step > command.budget_state.max_steps
        || command.budget_state.remaining_steps
            != command
                .budget_state
                .max_steps
                .saturating_sub(command.graph_state.step)
                .saturating_add(1)
    {
        return Err(NativeNodeExecutionError::InvalidCommand);
    }
    Ok(())
}

/// Native generic node dispatch failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum NativeNodeExecutionError {
    /// The command does not match canonical graph state or budgets.
    #[error("generic node execution command is invalid")]
    InvalidCommand,
    /// The exact persisted executor identity has no runtime-owned handler.
    #[error("exact persisted node executor has no native runtime handler")]
    UnsupportedExecutorIdentity,
    /// A loop executor did not receive a valid static bound.
    #[error("loop node execution budget is invalid")]
    InvalidLoopBudget,
    /// Event emission did not carry an exact bounded user-space configuration.
    #[error("user-space event node configuration is invalid")]
    InvalidEventConfiguration,
    /// Initial generic variables were not a bounded canonical object.
    #[error("generic initial variables are invalid")]
    InvalidInitialVariables,
}

#[cfg(test)]
mod tests {
    use agentmod_primitives::ContentHash;

    use super::*;

    fn command(id: &str, kind: &str) -> ExecuteNodeCommand {
        ExecuteNodeCommand {
            session_id: SessionId::from_uuid(uuid::Uuid::from_u128(1)),
            work: NodeWorkIdentity {
                run_id: String::from("run"),
                node_id: String::from("renamed-node"),
                branch_path: Vec::new(),
                attempt: 1,
                loop_iteration: 0,
                step: 1,
            },
            executor: SessionNodeExecutorResolution {
                node_id: String::from("renamed-node"),
                node_kind: kind.to_owned(),
                executor_id: id.to_owned(),
                executor_version: String::from("1.0.0"),
                source: SessionNodeExecutorSource::Runtime,
                boundary: SessionNodeExecutorBoundary::RuntimeLogic,
                required_capabilities: Vec::new(),
                resolved_capabilities: Vec::new(),
                runtime_api_requirement: String::from("^1.0"),
                executor_declaration_hash: ContentHash::digest(id.as_bytes()),
                adapter_configuration_reference: ContentHash::digest(b"node"),
            },
            configuration: None,
            input: NodeExecutionInput {
                transition_variables: serde_json::json!({"route":"right"}),
            },
            graph_state: CanonicalGraphState {
                attempt: 1,
                loop_iteration: 0,
                step: 1,
                completed_node_ids: Vec::new(),
            },
            budget_state: CanonicalBudgetState {
                max_steps: 4,
                remaining_steps: 4,
                max_iterations: None,
                remaining_iterations: None,
            },
        }
    }

    #[test]
    fn renamed_conditional_dispatches_only_by_exact_executor_identity() {
        let outcome = execute_native_node(&command("runtime.conditional", "conditional_branch"))
            .expect("dispatch");
        assert!(matches!(
            outcome,
            NodeExecutionOutcome::Completed {
                output: NodeExecutionOutput {
                    transition_variables,
                    ..
                }
            } if transition_variables == serde_json::json!({"route":"right"})
        ));
    }

    #[test]
    fn wrong_id_or_version_fails_closed() {
        let mut wrong = command("runtime.conditional", "conditional_branch");
        wrong.executor.executor_version = String::from("1.0.1");
        assert_eq!(
            execute_native_node(&wrong),
            Err(NativeNodeExecutionError::UnsupportedExecutorIdentity)
        );
        wrong.executor.executor_version = String::from("1.0.0");
        wrong.executor.executor_id = String::from("runtime.other");
        assert_eq!(
            execute_native_node(&wrong),
            Err(NativeNodeExecutionError::UnsupportedExecutorIdentity)
        );
    }

    #[test]
    fn alias_bound_tool_gate_version_is_exactly_scoped() {
        let mut tool_gate = command("runtime.tool-gate", "tool_execution_gate");
        tool_gate.executor.executor_version = String::from("1.1.0");
        assert_eq!(
            native_executor_key(&tool_gate.executor),
            Ok(NativeExecutorKey::ToolGate)
        );

        let mut conditional = command("runtime.conditional", "conditional_branch");
        conditional.executor.executor_version = String::from("1.1.0");
        assert_eq!(
            native_executor_key(&conditional.executor),
            Err(NativeNodeExecutionError::UnsupportedExecutorIdentity)
        );
    }

    #[test]
    fn event_emission_returns_bounded_proposal_without_envelope_authority() {
        let mut command = command("runtime.event-emission", "emit_event");
        let artifact_reference = format!("artifact:blake3:{}", "a5".repeat(32));
        command.configuration = Some(NodeConfiguration::EmitEvent {
            event_type: String::from("user.progress"),
            payload: serde_json::json!({"percent": 25}),
            artifact_references: BTreeSet::from([artifact_reference.clone()]),
            metadata: BTreeMap::from([(String::from(" label "), String::from(" checkpoint "))]),
        });
        assert_eq!(
            execute_native_node(&command),
            Ok(NodeExecutionOutcome::Emitted {
                event: UserSpaceEventProposal {
                    declared_event_type: String::from("user.progress"),
                    payload: serde_json::json!({"percent": 25}),
                    artifact_references: BTreeSet::from([artifact_reference]),
                    metadata: BTreeMap::from([
                        (String::from("label"), String::from("checkpoint"),)
                    ]),
                },
                output: NodeExecutionOutput {
                    result_reference: Some(String::from("user-space-event:user.progress:1")),
                    artifact_reference: None,
                    transition_variables: serde_json::json!({"route":"right"}),
                },
            })
        );
    }

    #[test]
    fn event_emission_rejects_reserved_types_and_secret_metadata() {
        for (event_type, metadata) in [
            (
                "session.created",
                BTreeMap::from([(String::from("label"), String::from("safe"))]),
            ),
            (
                "Runtime.forged",
                BTreeMap::from([(String::from("label"), String::from("safe"))]),
            ),
            (
                "user.progress",
                BTreeMap::from([(String::from("api_token"), String::from("hidden"))]),
            ),
            (
                "user.progress",
                BTreeMap::from([(String::from("sequence"), String::from("42"))]),
            ),
        ] {
            let mut command = command("runtime.event-emission", "emit_event");
            command.configuration = Some(NodeConfiguration::EmitEvent {
                event_type: event_type.to_owned(),
                payload: serde_json::json!({}),
                artifact_references: BTreeSet::new(),
                metadata,
            });
            assert_eq!(
                execute_native_node(&command),
                Err(NativeNodeExecutionError::InvalidEventConfiguration)
            );
        }
    }

    #[test]
    fn initial_variables_are_canonical_bounded_and_work_identity_is_exact() {
        assert_eq!(
            canonical_initial_variables_json(&serde_json::json!({
                "z": 1,
                "a": {"second": 2, "first": 1}
            })),
            Ok(String::from(r#"{"a":{"first":1,"second":2},"z":1}"#))
        );
        assert_eq!(
            canonical_initial_variables_json(&Value::String(String::from("not-an-object"))),
            Err(NativeNodeExecutionError::InvalidInitialVariables)
        );
        assert_eq!(
            canonical_initial_variables_json(&serde_json::json!({
                "oversized": "x".repeat(MAX_INITIAL_VARIABLE_STRING_BYTES + 1)
            })),
            Err(NativeNodeExecutionError::InvalidInitialVariables)
        );

        let mut mismatched = command("runtime.conditional", "conditional_branch");
        mismatched.work.loop_iteration = 1;
        assert_eq!(
            execute_native_node(&mismatched),
            Err(NativeNodeExecutionError::InvalidCommand)
        );
    }
}
