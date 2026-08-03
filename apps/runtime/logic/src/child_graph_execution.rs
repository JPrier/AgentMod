//! Pure, style-agnostic child spawn/wait/review node coordination.
//!
//! This module validates one exact persisted native executor and returns
//! bounded proposals. It does not create sessions, invoke providers, commit
//! events, or otherwise perform effects.

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use agentmod_graph_engine::{
    ChildSetSource, ChildWaitCancellation, ChildWorkspaceConfiguration, ChildWorkspaceMergePolicy,
    ExecutableNode, NodeConfiguration, NodeTextSource, NodeValueSource, ReviewResultSchema,
    SecurityClassification,
};
use agentmod_primitives::{ContentHash, SessionId};
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

use crate::{
    information_flow::{
        InformationFlowClassification, InformationFlowDecision, InformationFlowSink,
        InformationFlowSource, evaluate_information_flow, is_exact_secret_reference,
    },
    node_execution::{NativeExecutorKey, NodeWorkIdentity, native_executor_key},
    session::{SessionMcpBinding, SessionNodeExecutorResolution},
};

const MAX_COMMAND_VARIABLE_BYTES: usize = 1024 * 1024;
const MAX_TASK_BYTES: usize = 64 * 1024;
const MAX_REFERENCE_BYTES: usize = 1024;
const MAX_FAILURE_CODE_BYTES: usize = 256;
const MAX_JSON_DEPTH: usize = 32;

/// Parent-owned immutable bounds authorizing proposals from one graph run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParentChildAuthorization {
    /// Canonical parent session.
    pub parent_session_id: SessionId,
    /// Exact immutable graph run.
    pub run_id: String,
    /// Complete immutable execution-plan hash.
    pub execution_plan_hash: ContentHash,
    /// Exact persisted resolution authorized by that immutable plan.
    pub executor: SessionNodeExecutorResolution,
    /// Graph nodes allowed to propose child orchestration.
    pub node_ids: BTreeSet<String>,
    /// Child styles allowed by the immutable parent style.
    pub child_styles: BTreeSet<String>,
    /// Tool groups allowed by the immutable parent style.
    pub tool_groups: BTreeSet<String>,
    /// Workspace modes allowed by the immutable parent style.
    pub workspace_modes: BTreeSet<AuthorizedWorkspaceMode>,
    /// Exact allowed custom workspace locators.
    pub custom_workspaces: BTreeSet<String>,
    /// Immutable artifact references the parent may delegate.
    pub artifact_references: BTreeSet<String>,
    /// Maximum children proposed by one node.
    pub maximum_children: u32,
    /// Maximum recursive child depth.
    pub maximum_depth: u32,
    /// Current parent depth.
    pub current_depth: u32,
    /// Maximum token budget per child.
    pub maximum_token_budget: u64,
    /// Maximum context budget per child.
    pub maximum_context_budget_tokens: u64,
    /// Maximum cost budget per child.
    pub maximum_cost_budget_micros: u64,
    /// Exact parent provider inherited by each child when enabled.
    pub inherited_provider: Option<String>,
    /// Exact parent model inherited by each child when enabled.
    pub inherited_model: Option<String>,
    /// Exact sanitized parent MCP binding inherited by each child when enabled.
    pub inherited_mcp: Option<SessionMcpBinding>,
}

/// Workspace modes independently authorized by the parent style.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AuthorizedWorkspaceMode {
    /// Shared read-only workspace.
    SharedReadOnly,
    /// Shared workspace with runtime serialization.
    SharedSerializedWrites,
    /// Independent Git worktree.
    IndependentGitWorktree,
    /// Temporary workspace copy.
    TemporaryCopy,
    /// Exact custom workspace.
    ExplicitCustomWorkspace,
}

/// Pure command for one exact persisted Graph-B executor.
#[derive(Clone, Debug, PartialEq)]
pub struct CoordinateChildGraphNodeCommand {
    /// Canonical parent session.
    pub session_id: SessionId,
    /// Exact graph work attempt.
    pub work: NodeWorkIdentity,
    /// Exact immutable plan selected for this run.
    pub execution_plan_hash: ContentHash,
    /// Exact persisted executor resolution.
    pub executor: SessionNodeExecutorResolution,
    /// Exact compiled node whose hash is bound by the resolution.
    pub node: ExecutableNode,
    /// Replay-derived canonical variable values.
    pub variables: BTreeMap<String, Value>,
    /// Immutable parent authorization.
    pub authorization: ParentChildAuthorization,
    /// Replay-derived node-specific input.
    pub input: ChildGraphNodeInput,
}

/// Replay-derived input supplied to the pure coordinator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildGraphNodeInput {
    /// Child-spawn proposal uses only compiled configuration and variables.
    Spawn,
    /// Canonical child projection used to evaluate a wait.
    Wait {
        /// Elapsed duration recorded by the runtime.
        elapsed_ms: u64,
        /// Whether parent cancellation is canonical.
        cancellation_requested: bool,
        /// Known exact child states in arbitrary input order.
        children: Vec<CanonicalChildState>,
    },
    /// Runtime-bounded reviewer candidate.
    Review {
        /// Zero-based completed revision count.
        revision: u32,
        /// Exact known task or child IDs eligible for rejection.
        known_task_ids: BTreeSet<String>,
        /// Candidate structured reviewer result.
        candidate: ReviewCandidate,
    },
}

/// Canonical child state used for pure wait projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalChildState {
    /// Exact runtime-managed child identity.
    pub child_id: SessionId,
    /// Exact graph-owned task identity.
    pub task_id: String,
    /// Replay-derived lifecycle state.
    pub status: CanonicalChildStatus,
}

/// Replay-derived child lifecycle state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CanonicalChildStatus {
    /// Child exists but has no terminal receipt.
    Pending,
    /// Child completed with exact canonical references.
    Completed {
        /// Stable node-result reference.
        result_reference: String,
        /// Immutable artifact references.
        artifact_references: BTreeSet<String>,
        /// Canonical parent-observed completion sequence.
        completion_sequence: u64,
    },
    /// Child terminally failed.
    Failed {
        /// Stable redacted code.
        code: String,
        /// Canonical parent-observed completion sequence.
        completion_sequence: u64,
    },
    /// Child was canonically cancelled.
    Cancelled {
        /// Canonical parent-observed completion sequence.
        completion_sequence: u64,
    },
}

/// Structured bounded reviewer candidate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReviewCandidate {
    /// Whether the integration is accepted.
    pub approved: bool,
    /// Task or child identities requiring revision.
    pub rejected_task_ids: BTreeSet<String>,
    /// Structured findings.
    pub findings: Vec<ReviewFinding>,
    /// Exact provider or plugin terminal-result hash.
    pub source_result_hash: ContentHash,
}

/// One bounded reviewer finding.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReviewFinding {
    /// Stable machine-readable finding code.
    pub code: String,
    /// Bounded human-readable detail.
    pub message: String,
    /// Immutable evidence artifacts.
    pub artifact_references: BTreeSet<String>,
}

/// Pure result from one Graph-B node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildGraphNodeOutcome {
    /// Consequential child-creation proposals awaiting normal policy handling.
    Spawn {
        /// Stable task-ordered child proposals.
        proposals: Vec<ChildSpawnProposal>,
    },
    /// Replay-derived child wait projection.
    Wait(ChildWaitProjection),
    /// Runtime-validated reviewer routing proposal.
    Review(ReviewRoutingProposal),
}

/// One bounded child-session creation proposal.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ChildSpawnProposal {
    /// Canonical parent session.
    pub parent_session_id: SessionId,
    /// Exact owning graph work.
    pub work: NodeWorkIdentity,
    /// Stable task identity.
    pub task_id: String,
    /// Bounded typed task.
    pub task: Value,
    /// Hash of the exact canonical task.
    pub task_hash: ContentHash,
    /// Exact child style selector.
    pub child_style: String,
    /// Exact parent provider inherited by the child when enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inherited_provider: Option<String>,
    /// Exact parent model inherited by the child when enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inherited_model: Option<String>,
    /// Exact sanitized parent MCP binding inherited by the child when enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inherited_mcp: Option<SessionMcpBinding>,
    /// Sorted tool groups.
    pub tool_groups: BTreeSet<String>,
    /// One-based proposed child depth.
    pub depth: u32,
    /// Hard token budget.
    pub token_budget: u64,
    /// Hard provider-context budget.
    pub context_budget_tokens: u64,
    /// Hard cost budget.
    pub cost_budget_micros: u64,
    /// Runtime-enforced workspace policy.
    pub workspace: ResolvedChildWorkspace,
    /// Immutable task artifact references.
    pub artifact_references: BTreeSet<String>,
    /// Task security classification.
    pub security_classification: SecurityClassification,
    /// Must remain true through proposal/interceptor/policy execution.
    pub approval_required: bool,
    /// Digest of the complete proposal excluding this digest.
    pub proposal_hash: ContentHash,
}

/// Resolved child workspace proposal without operating-system handles.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum ResolvedChildWorkspace {
    /// Shared read-only workspace.
    SharedReadOnly,
    /// Shared writes serialized by the runtime.
    SharedSerializedWrites {
        /// Stable serialization key.
        serialization_key: String,
    },
    /// Independent Git worktree.
    IndependentGitWorktree,
    /// Temporary workspace copy.
    TemporaryCopy,
    /// Bounded runtime-owned filesystem copy.
    IsolatedCopy,
    /// Owned branch workspace with explicit integration policy.
    BranchWorkspace {
        /// Policy retained by the immutable workspace contract.
        merge_policy: ChildWorkspaceMergePolicy,
    },
    /// Exact custom workspace locator.
    ExplicitCustomWorkspace {
        /// Bounded locator; dependency resolves operating-system types later.
        path: String,
    },
}

/// Pure child-wait state reconstructed from canonical inputs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildWaitProjection {
    /// Threshold has not yet been met.
    Waiting {
        /// Stable successful-child receipts.
        successful: Vec<ChildWaitSuccess>,
        /// Stable missing or pending-child order.
        pending: Vec<SessionId>,
        /// Canonical remaining timeout.
        remaining_ms: u64,
        /// Cancellation is recorded but wait policy suppresses later effects.
        cancellation_recorded: bool,
    },
    /// Minimum success threshold is satisfied.
    Completed {
        /// Stable successful-child receipts.
        successful: Vec<ChildWaitSuccess>,
        /// Terminal unsuccessful child receipts in stable order.
        unsuccessful: Vec<ChildWaitFailure>,
        /// Hash of the exact stable projection.
        result_hash: ContentHash,
    },
    /// Timeout, cancellation, or impossible threshold.
    Failed {
        /// Stable structured failure.
        code: String,
        /// Incomplete children for which cancellation may be proposed.
        cancel_children: Vec<SessionId>,
        /// Whether existing children remain independently active.
        detached: bool,
        /// Hash of the exact stable projection.
        result_hash: ContentHash,
    },
}

/// Exact successful child receipt retained by the wait projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ChildWaitSuccess {
    /// Exact child identity.
    pub child_id: SessionId,
    /// Exact graph-owned task.
    pub task_id: String,
    /// Canonical result reference.
    pub result_reference: String,
    /// Immutable artifact references.
    pub artifact_references: BTreeSet<String>,
    /// Canonical completion sequence.
    pub completion_sequence: u64,
}

/// Exact unsuccessful terminal child receipt retained by the wait projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ChildWaitFailure {
    /// Exact child identity.
    pub child_id: SessionId,
    /// Exact graph-owned task.
    pub task_id: String,
    /// Failure or cancellation disposition.
    pub disposition: ChildWaitFailureDisposition,
    /// Stable redacted failure code when failed.
    pub code: Option<String>,
    /// Canonical completion sequence.
    pub completion_sequence: u64,
}

/// Terminal unsuccessful child disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildWaitFailureDisposition {
    /// Child terminally failed.
    Failed,
    /// Child was canonically cancelled.
    Cancelled,
}

/// Reviewer route disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewDisposition {
    /// Integration accepted.
    Approved,
    /// Another bounded revision is required.
    Revision,
    /// Structured terminal failure route.
    Failed,
}

/// Pure reviewer routing proposal with exact evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReviewRoutingProposal {
    /// Runtime-validated disposition.
    pub disposition: ReviewDisposition,
    /// Exact configured graph destination.
    pub destination_node_id: String,
    /// Current completed revision count.
    pub current_revision: u32,
    /// Next revision when revision is selected.
    pub next_revision: Option<u32>,
    /// Stable rejected task identities.
    pub rejected_task_ids: Vec<String>,
    /// Bounded findings.
    pub findings: Vec<ReviewFinding>,
    /// Exact runtime routing-evidence hash.
    pub evidence_hash: ContentHash,
}

/// Coordinates one exact child-spawn, child-wait, or review node without effects.
///
/// # Errors
///
/// Fails closed on executor, compiled-node, parent authorization, configuration,
/// canonical input, or bound mismatch.
pub fn coordinate_child_graph_node(
    command: &CoordinateChildGraphNodeCommand,
) -> Result<ChildGraphNodeOutcome, ChildGraphExecutionError> {
    validate_common(command)?;
    match native_executor_key(&command.executor)
        .map_err(|_| ChildGraphExecutionError::UnsupportedExecutorIdentity)?
    {
        NativeExecutorKey::ChildSpawn => coordinate_spawn(command),
        NativeExecutorKey::ChildWait => coordinate_wait(command),
        NativeExecutorKey::Review => coordinate_review(command),
        _ => Err(ChildGraphExecutionError::UnsupportedExecutorIdentity),
    }
}

fn validate_common(
    command: &CoordinateChildGraphNodeCommand,
) -> Result<(), ChildGraphExecutionError> {
    if command.work.node_id != command.node.id
        || command.executor.node_id != command.node.id
        || command.work.run_id != command.authorization.run_id
        || command.session_id != command.authorization.parent_session_id
        || command.execution_plan_hash != command.authorization.execution_plan_hash
        || command.executor != command.authorization.executor
        || command.execution_plan_hash == ContentHash::from_bytes([0; 32])
        || !command.authorization.node_ids.contains(&command.node.id)
        || command.work.run_id.trim().is_empty()
        || command.work.run_id.len() > MAX_REFERENCE_BYTES
        || command.work.run_id.chars().any(char::is_control)
        || command.work.attempt == 0
        || command.work.step == 0
    {
        return Err(ChildGraphExecutionError::IdentityMismatch);
    }
    let node_bytes = serde_json::to_vec(&command.node)
        .map_err(|_| ChildGraphExecutionError::InvalidConfiguration)?;
    if ContentHash::digest(&node_bytes) != command.executor.adapter_configuration_reference {
        return Err(ChildGraphExecutionError::CompiledNodeMismatch);
    }
    let variable_bytes = serde_json::to_vec(&command.variables)
        .map_err(|_| ChildGraphExecutionError::InvalidVariables)?;
    if variable_bytes.len() > MAX_COMMAND_VARIABLE_BYTES
        || command
            .variables
            .iter()
            .any(|(name, value)| !valid_reference(name) || json_depth(value) > MAX_JSON_DEPTH)
    {
        return Err(ChildGraphExecutionError::InvalidVariables);
    }
    Ok(())
}

fn coordinate_spawn(
    command: &CoordinateChildGraphNodeCommand,
) -> Result<ChildGraphNodeOutcome, ChildGraphExecutionError> {
    let Some(NodeConfiguration::SpawnChildAgent {
        task_input,
        task_id_prefix,
        child_style,
        tool_groups,
        maximum_children,
        maximum_depth,
        token_budget,
        context_budget_tokens,
        cost_budget_micros,
        workspace,
        artifact_references,
        artifact_reference_variables,
        security_classification,
        approval_required,
    }) = command.node.configuration.as_ref()
    else {
        return Err(ChildGraphExecutionError::ConfigurationRequired);
    };
    if !*approval_required
        || *maximum_children == 0
        || *maximum_children > command.authorization.maximum_children
        || *maximum_depth == 0
        || *maximum_depth > command.authorization.maximum_depth
        || command.authorization.current_depth >= *maximum_depth
        || *token_budget == 0
        || *token_budget > command.authorization.maximum_token_budget
        || *context_budget_tokens == 0
        || *context_budget_tokens > *token_budget
        || *context_budget_tokens > command.authorization.maximum_context_budget_tokens
        || *cost_budget_micros == 0
        || *cost_budget_micros > command.authorization.maximum_cost_budget_micros
        || !command.authorization.child_styles.contains(child_style)
        || !valid_inherited_selection(&command.authorization)
        || !tool_groups.is_subset(&command.authorization.tool_groups)
    {
        return Err(ChildGraphExecutionError::ParentAuthorization);
    }
    let artifact_references =
        resolve_artifact_references(command, artifact_references, artifact_reference_variables)?;
    if !artifact_references.is_subset(&command.authorization.artifact_references) {
        return Err(ChildGraphExecutionError::ParentAuthorization);
    }
    let task_value = resolve_value_source(command, task_input)?;
    if *security_classification == SecurityClassification::SecretReference {
        if !task_value.as_str().is_some_and(is_exact_secret_reference) {
            return Err(ChildGraphExecutionError::InvalidTask);
        }
    } else if contains_forbidden_inline_secret(task_value)
        || contains_exact_secret_reference(task_value)
    {
        return Err(ChildGraphExecutionError::InvalidTask);
    }
    validate_child_task_information_flow(
        command,
        task_value,
        &artifact_references,
        *security_classification,
    )?;
    let tasks = canonical_tasks(task_value, task_id_prefix, *maximum_children)?;
    let workspace = resolve_workspace(command, workspace)?;
    let depth = command
        .authorization
        .current_depth
        .checked_add(1)
        .ok_or(ChildGraphExecutionError::ParentAuthorization)?;
    let mut proposals = Vec::with_capacity(tasks.len());
    for (task_id, task) in tasks {
        let task_bytes =
            serde_json::to_vec(&task).map_err(|_| ChildGraphExecutionError::InvalidTask)?;
        if task_bytes.len() > MAX_TASK_BYTES {
            return Err(ChildGraphExecutionError::InvalidTask);
        }
        let mut proposal = ChildSpawnProposal {
            parent_session_id: command.session_id,
            work: command.work.clone(),
            task_id,
            task,
            task_hash: ContentHash::digest(&task_bytes),
            child_style: child_style.clone(),
            inherited_provider: command.authorization.inherited_provider.clone(),
            inherited_model: command.authorization.inherited_model.clone(),
            inherited_mcp: command.authorization.inherited_mcp.clone(),
            tool_groups: tool_groups.clone(),
            depth,
            token_budget: *token_budget,
            context_budget_tokens: *context_budget_tokens,
            cost_budget_micros: *cost_budget_micros,
            workspace: workspace.clone(),
            artifact_references: artifact_references.clone(),
            security_classification: *security_classification,
            approval_required: true,
            proposal_hash: ContentHash::from_bytes([0; 32]),
        };
        proposal.proposal_hash = hash_serializable(&proposal)?;
        proposals.push(proposal);
    }
    Ok(ChildGraphNodeOutcome::Spawn { proposals })
}

fn validate_child_task_information_flow(
    command: &CoordinateChildGraphNodeCommand,
    task: &Value,
    artifact_references: &BTreeSet<String>,
    declared: SecurityClassification,
) -> Result<(), ChildGraphExecutionError> {
    if artifact_references
        .iter()
        .any(|reference| is_exact_secret_reference(reference))
    {
        return Err(ChildGraphExecutionError::InvalidTask);
    }
    let classification = information_flow_classification(declared);
    let task_bytes = serde_json::to_vec(task).map_err(|_| ChildGraphExecutionError::InvalidTask)?;
    let dedicated_secret_reference = (declared == SecurityClassification::SecretReference)
        .then(|| task.as_str())
        .flatten();
    let mut sources = Vec::with_capacity(artifact_references.len() + 1);
    sources.push(
        InformationFlowSource::from_bytes(
            "task",
            classification,
            &task_bytes,
            dedicated_secret_reference,
        )
        .map_err(|_| ChildGraphExecutionError::InvalidTask)?,
    );
    for (index, reference) in artifact_references.iter().enumerate() {
        sources.push(
            InformationFlowSource::from_bytes(
                format!("artifact:{index}"),
                classification,
                reference.as_bytes(),
                None,
            )
            .map_err(|_| ChildGraphExecutionError::InvalidTask)?,
        );
    }
    let identity = format!(
        "child-task:{}",
        command.executor.adapter_configuration_reference.to_hex()
    );
    let (_, decision) = evaluate_information_flow(
        identity,
        InformationFlowSink::ChildMessage,
        classification,
        &sources,
    )
    .map_err(|_| ChildGraphExecutionError::InvalidTask)?;
    if matches!(decision, InformationFlowDecision::Allowed { .. }) {
        Ok(())
    } else {
        Err(ChildGraphExecutionError::InvalidTask)
    }
}

const fn information_flow_classification(
    classification: SecurityClassification,
) -> InformationFlowClassification {
    match classification {
        SecurityClassification::Public => InformationFlowClassification::Public,
        SecurityClassification::Internal => InformationFlowClassification::Internal,
        SecurityClassification::Confidential | SecurityClassification::SecretReference => {
            InformationFlowClassification::Confidential
        }
    }
}

fn valid_inherited_selection(authorization: &ParentChildAuthorization) -> bool {
    authorization
        .inherited_provider
        .as_deref()
        .is_none_or(valid_reference)
        && authorization
            .inherited_model
            .as_deref()
            .is_none_or(valid_reference)
        && authorization.inherited_mcp.as_ref().is_none_or(|binding| {
            authorization.tool_groups.contains("mcp")
                && binding
                    .configuration_reference
                    .as_deref()
                    .is_some_and(valid_reference)
                && !binding.servers.is_empty()
        })
}

fn coordinate_wait(
    command: &CoordinateChildGraphNodeCommand,
) -> Result<ChildGraphNodeOutcome, ChildGraphExecutionError> {
    let Some(NodeConfiguration::WaitForAgents {
        children,
        maximum_children,
        minimum_successes,
        timeout_ms,
        cancellation,
    }) = command.node.configuration.as_ref()
    else {
        return Err(ChildGraphExecutionError::ConfigurationRequired);
    };
    let ChildGraphNodeInput::Wait {
        elapsed_ms,
        cancellation_requested,
        children: states,
    } = &command.input
    else {
        return Err(ChildGraphExecutionError::InputKindMismatch);
    };
    if *maximum_children == 0
        || *maximum_children > command.authorization.maximum_children
        || *minimum_successes == 0
        || *minimum_successes > *maximum_children
        || *timeout_ms == 0
    {
        return Err(ChildGraphExecutionError::ParentAuthorization);
    }
    let expected = resolve_child_set(command, children, *maximum_children)?;
    if *minimum_successes as usize > expected.len() {
        return Err(ChildGraphExecutionError::InvalidChildSet);
    }
    let mut by_id = BTreeMap::new();
    let mut task_ids = BTreeSet::new();
    for state in states {
        validate_child_state(state)?;
        if !expected.contains(&state.child_id)
            || !task_ids.insert(&state.task_id)
            || by_id.insert(state.child_id, state).is_some()
        {
            return Err(ChildGraphExecutionError::InvalidChildSet);
        }
    }
    let (successful, unsuccessful, pending) = classify_child_sets(&expected, &by_id);
    let projection = if *cancellation_requested && *cancellation != ChildWaitCancellation::Wait {
        let cancel_children = if *cancellation == ChildWaitCancellation::Cascade {
            pending.clone()
        } else {
            Vec::new()
        };
        failed_wait(
            "parent_cancelled",
            cancel_children,
            *cancellation == ChildWaitCancellation::Detach,
        )?
    } else if successful.len() >= *minimum_successes as usize {
        completed_wait(successful, unsuccessful)?
    } else if *elapsed_ms >= *timeout_ms {
        failed_wait("child_wait_timeout", pending, false)?
    } else if successful.len() + pending.len() < *minimum_successes as usize {
        failed_wait("minimum_success_impossible", pending, false)?
    } else {
        ChildWaitProjection::Waiting {
            successful,
            pending,
            remaining_ms: timeout_ms.saturating_sub(*elapsed_ms),
            cancellation_recorded: *cancellation_requested,
        }
    };
    Ok(ChildGraphNodeOutcome::Wait(projection))
}

fn classify_child_sets(
    expected: &BTreeSet<SessionId>,
    by_id: &BTreeMap<SessionId, &CanonicalChildState>,
) -> (Vec<ChildWaitSuccess>, Vec<ChildWaitFailure>, Vec<SessionId>) {
    let mut successful = Vec::new();
    let mut unsuccessful = Vec::new();
    let mut pending = Vec::new();
    for child_id in expected {
        match by_id.get(child_id).map(|state| &state.status) {
            Some(CanonicalChildStatus::Completed {
                result_reference,
                artifact_references,
                completion_sequence,
            }) => successful.push(ChildWaitSuccess {
                child_id: *child_id,
                task_id: by_id[child_id].task_id.clone(),
                result_reference: result_reference.clone(),
                artifact_references: artifact_references.clone(),
                completion_sequence: *completion_sequence,
            }),
            Some(CanonicalChildStatus::Failed {
                code,
                completion_sequence,
            }) => unsuccessful.push(ChildWaitFailure {
                child_id: *child_id,
                task_id: by_id[child_id].task_id.clone(),
                disposition: ChildWaitFailureDisposition::Failed,
                code: Some(code.clone()),
                completion_sequence: *completion_sequence,
            }),
            Some(CanonicalChildStatus::Cancelled {
                completion_sequence,
            }) => unsuccessful.push(ChildWaitFailure {
                child_id: *child_id,
                task_id: by_id[child_id].task_id.clone(),
                disposition: ChildWaitFailureDisposition::Cancelled,
                code: None,
                completion_sequence: *completion_sequence,
            }),
            Some(CanonicalChildStatus::Pending) | None => pending.push(*child_id),
        }
    }
    (successful, unsuccessful, pending)
}

fn coordinate_review(
    command: &CoordinateChildGraphNodeCommand,
) -> Result<ChildGraphNodeOutcome, ChildGraphExecutionError> {
    let Some(NodeConfiguration::Review {
        input,
        artifact_references,
        artifact_reference_variables,
        result_schema,
        routes,
        maximum_revisions,
    }) = command.node.configuration.as_ref()
    else {
        return Err(ChildGraphExecutionError::ConfigurationRequired);
    };
    let ChildGraphNodeInput::Review {
        revision,
        known_task_ids,
        candidate,
    } = &command.input
    else {
        return Err(ChildGraphExecutionError::InputKindMismatch);
    };
    let resolved_input = resolve_value_source(command, input)?;
    validate_bounded_value(resolved_input)?;
    let artifact_references =
        resolve_artifact_references(command, artifact_references, artifact_reference_variables)?;
    if !artifact_references.is_subset(&command.authorization.artifact_references)
        || *maximum_revisions == 0
        || *revision > *maximum_revisions
    {
        return Err(ChildGraphExecutionError::ParentAuthorization);
    }
    validate_review_candidate(candidate, known_task_ids, result_schema)?;
    if candidate
        .findings
        .iter()
        .any(|finding| !finding.artifact_references.is_subset(&artifact_references))
    {
        return Err(ChildGraphExecutionError::ParentAuthorization);
    }
    let (disposition, destination_node_id, next_revision) = if candidate.approved {
        (ReviewDisposition::Approved, routes.approved.clone(), None)
    } else if *revision < *maximum_revisions {
        (
            ReviewDisposition::Revision,
            routes.revision.clone(),
            revision.checked_add(1),
        )
    } else {
        (ReviewDisposition::Failed, routes.failure.clone(), None)
    };
    let rejected_task_ids = candidate
        .rejected_task_ids
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let evidence_hash = hash_serializable(&(
        &command.work,
        command.execution_plan_hash,
        ContentHash::digest(
            &serde_json::to_vec(resolved_input)
                .map_err(|_| ChildGraphExecutionError::InvalidReview)?,
        ),
        &artifact_references,
        candidate,
        disposition,
        &destination_node_id,
        revision,
        next_revision,
    ))?;
    Ok(ChildGraphNodeOutcome::Review(ReviewRoutingProposal {
        disposition,
        destination_node_id,
        current_revision: *revision,
        next_revision,
        rejected_task_ids,
        findings: candidate.findings.clone(),
        evidence_hash,
    }))
}

fn resolve_artifact_references(
    command: &CoordinateChildGraphNodeCommand,
    static_references: &BTreeSet<String>,
    variables: &BTreeSet<String>,
) -> Result<BTreeSet<String>, ChildGraphExecutionError> {
    let mut resolved = static_references.clone();
    for variable in variables {
        let reference = command
            .variables
            .get(variable)
            .and_then(serde_json::Value::as_str)
            .filter(|value| valid_reference(value))
            .ok_or(ChildGraphExecutionError::InvalidVariables)?;
        resolved.insert(reference.to_owned());
    }
    Ok(resolved)
}

fn resolve_value_source<'a>(
    command: &'a CoordinateChildGraphNodeCommand,
    source: &'a NodeValueSource,
) -> Result<&'a Value, ChildGraphExecutionError> {
    match source {
        NodeValueSource::Static { value } => Ok(value),
        NodeValueSource::Variable { variable } => {
            if !command.node.read_variables.contains(variable) {
                return Err(ChildGraphExecutionError::InvalidVariables);
            }
            command
                .variables
                .get(variable)
                .ok_or(ChildGraphExecutionError::InvalidVariables)
        }
    }
}

fn canonical_tasks(
    value: &Value,
    task_id_prefix: &str,
    maximum_children: u32,
) -> Result<Vec<(String, Value)>, ChildGraphExecutionError> {
    if !valid_reference(task_id_prefix) || maximum_children == 0 {
        return Err(ChildGraphExecutionError::InvalidTask);
    }
    let tasks = match value {
        Value::String(task) if !task.trim().is_empty() => {
            vec![(format!("{task_id_prefix}-0"), Value::String(task.clone()))]
        }
        Value::Array(tasks) => tasks
            .iter()
            .enumerate()
            .map(|(index, task)| {
                task.as_str()
                    .filter(|task| !task.trim().is_empty())
                    .map(|task| {
                        (
                            format!("{task_id_prefix}-{index}"),
                            Value::String(task.to_owned()),
                        )
                    })
                    .ok_or(ChildGraphExecutionError::InvalidTask)
            })
            .collect::<Result<Vec<_>, _>>()?,
        Value::Object(tasks) => {
            let mut tasks = tasks
                .iter()
                .map(|(task_id, task)| {
                    task.as_str()
                        .filter(|task| !task.trim().is_empty() && valid_reference(task_id))
                        .map(|task| (task_id.clone(), Value::String(task.to_owned())))
                        .ok_or(ChildGraphExecutionError::InvalidTask)
                })
                .collect::<Result<Vec<_>, _>>()?;
            tasks.sort_by(|left, right| left.0.cmp(&right.0));
            tasks
        }
        _ => return Err(ChildGraphExecutionError::InvalidTask),
    };
    if tasks.is_empty() || tasks.len() > maximum_children as usize {
        return Err(ChildGraphExecutionError::InvalidTask);
    }
    Ok(tasks)
}

fn resolve_workspace(
    command: &CoordinateChildGraphNodeCommand,
    workspace: &ChildWorkspaceConfiguration,
) -> Result<ResolvedChildWorkspace, ChildGraphExecutionError> {
    let (mode, resolved) = match workspace {
        ChildWorkspaceConfiguration::SharedReadOnly => (
            AuthorizedWorkspaceMode::SharedReadOnly,
            ResolvedChildWorkspace::SharedReadOnly,
        ),
        ChildWorkspaceConfiguration::SharedSerializedWrites { serialization_key }
            if valid_reference(serialization_key) =>
        {
            (
                AuthorizedWorkspaceMode::SharedSerializedWrites,
                ResolvedChildWorkspace::SharedSerializedWrites {
                    serialization_key: serialization_key.clone(),
                },
            )
        }
        ChildWorkspaceConfiguration::IndependentGitWorktree => (
            AuthorizedWorkspaceMode::IndependentGitWorktree,
            ResolvedChildWorkspace::IndependentGitWorktree,
        ),
        ChildWorkspaceConfiguration::TemporaryCopy => (
            AuthorizedWorkspaceMode::TemporaryCopy,
            ResolvedChildWorkspace::TemporaryCopy,
        ),
        ChildWorkspaceConfiguration::IsolatedCopy => (
            AuthorizedWorkspaceMode::TemporaryCopy,
            ResolvedChildWorkspace::IsolatedCopy,
        ),
        ChildWorkspaceConfiguration::BranchWorkspace { merge_policy } => (
            AuthorizedWorkspaceMode::IndependentGitWorktree,
            ResolvedChildWorkspace::BranchWorkspace {
                merge_policy: *merge_policy,
            },
        ),
        ChildWorkspaceConfiguration::ExplicitCustomWorkspace { path } => {
            let path = resolve_text_source(command, path)?;
            if !command.authorization.custom_workspaces.contains(&path) {
                return Err(ChildGraphExecutionError::ParentAuthorization);
            }
            (
                AuthorizedWorkspaceMode::ExplicitCustomWorkspace,
                ResolvedChildWorkspace::ExplicitCustomWorkspace { path },
            )
        }
        ChildWorkspaceConfiguration::SharedSerializedWrites { .. } => {
            return Err(ChildGraphExecutionError::InvalidConfiguration);
        }
    };
    if command.authorization.workspace_modes.contains(&mode) {
        Ok(resolved)
    } else {
        Err(ChildGraphExecutionError::ParentAuthorization)
    }
}

fn resolve_text_source(
    command: &CoordinateChildGraphNodeCommand,
    source: &NodeTextSource,
) -> Result<String, ChildGraphExecutionError> {
    let value = match source {
        NodeTextSource::Static { value } => value.clone(),
        NodeTextSource::Variable { variable } => {
            if !command.node.read_variables.contains(variable) {
                return Err(ChildGraphExecutionError::InvalidVariables);
            }
            command
                .variables
                .get(variable)
                .and_then(Value::as_str)
                .ok_or(ChildGraphExecutionError::InvalidVariables)?
                .to_owned()
        }
    };
    if value.trim().is_empty()
        || value.len() > MAX_REFERENCE_BYTES
        || value.chars().any(char::is_control)
    {
        Err(ChildGraphExecutionError::InvalidConfiguration)
    } else {
        Ok(value)
    }
}

fn resolve_child_set(
    command: &CoordinateChildGraphNodeCommand,
    source: &ChildSetSource,
    maximum_children: u32,
) -> Result<BTreeSet<SessionId>, ChildGraphExecutionError> {
    resolve_child_set_input(&command.node, &command.variables, source, maximum_children)
}

pub(crate) fn resolve_child_set_input(
    node: &ExecutableNode,
    variables: &BTreeMap<String, Value>,
    source: &ChildSetSource,
    maximum_children: u32,
) -> Result<BTreeSet<SessionId>, ChildGraphExecutionError> {
    let values = match source {
        ChildSetSource::Exact { child_ids } => child_ids.iter().cloned().collect::<Vec<_>>(),
        ChildSetSource::Variable { variable } => {
            if !node.read_variables.contains(variable) {
                return Err(ChildGraphExecutionError::InvalidVariables);
            }
            let value = variables
                .get(variable)
                .ok_or(ChildGraphExecutionError::InvalidChildSet)?;
            match value {
                Value::String(value) => vec![value.clone()],
                Value::Array(values) => values
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .map(str::to_owned)
                            .ok_or(ChildGraphExecutionError::InvalidChildSet)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                _ => return Err(ChildGraphExecutionError::InvalidChildSet),
            }
        }
    };
    if values.is_empty() || values.len() > maximum_children as usize {
        return Err(ChildGraphExecutionError::InvalidChildSet);
    }
    let value_count = values.len();
    let parsed = values
        .into_iter()
        .map(|value| {
            SessionId::from_str(&value).map_err(|_| ChildGraphExecutionError::InvalidChildSet)
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if parsed.len() != value_count || parsed.len() > maximum_children as usize {
        Err(ChildGraphExecutionError::InvalidChildSet)
    } else {
        Ok(parsed)
    }
}

fn validate_child_state(state: &CanonicalChildState) -> Result<(), ChildGraphExecutionError> {
    if !valid_reference(&state.task_id) {
        return Err(ChildGraphExecutionError::InvalidChildState);
    }
    match &state.status {
        CanonicalChildStatus::Pending => Ok(()),
        CanonicalChildStatus::Completed {
            result_reference,
            artifact_references,
            completion_sequence,
        } => {
            if *completion_sequence == 0
                || !valid_reference(result_reference)
                || artifact_references
                    .iter()
                    .any(|value| !valid_reference(value))
            {
                Err(ChildGraphExecutionError::InvalidChildState)
            } else {
                Ok(())
            }
        }
        CanonicalChildStatus::Failed {
            code,
            completion_sequence,
        } => {
            if *completion_sequence == 0 || !valid_failure_code(code) {
                Err(ChildGraphExecutionError::InvalidChildState)
            } else {
                Ok(())
            }
        }
        CanonicalChildStatus::Cancelled {
            completion_sequence,
        } if *completion_sequence > 0 => Ok(()),
        CanonicalChildStatus::Cancelled { .. } => Err(ChildGraphExecutionError::InvalidChildState),
    }
}

fn validate_review_candidate(
    candidate: &ReviewCandidate,
    known_task_ids: &BTreeSet<String>,
    schema: &ReviewResultSchema,
) -> Result<(), ChildGraphExecutionError> {
    if candidate.source_result_hash == ContentHash::from_bytes([0; 32])
        || candidate.findings.len() > schema.maximum_findings as usize
        || candidate.rejected_task_ids.len() > schema.maximum_rejections as usize
        || !candidate.rejected_task_ids.is_subset(known_task_ids)
        || candidate.approved != candidate.rejected_task_ids.is_empty()
        || candidate.findings.iter().any(|finding| {
            !valid_reference(&finding.code)
                || finding.message.trim().is_empty()
                || finding.message.len() > schema.maximum_finding_bytes as usize
                || finding.message.chars().any(char::is_control)
                || finding
                    .artifact_references
                    .iter()
                    .any(|reference| !valid_reference(reference))
                || (schema.require_artifact_evidence && finding.artifact_references.is_empty())
        })
    {
        Err(ChildGraphExecutionError::InvalidReview)
    } else {
        Ok(())
    }
}

fn completed_wait(
    successful: Vec<ChildWaitSuccess>,
    unsuccessful: Vec<ChildWaitFailure>,
) -> Result<ChildWaitProjection, ChildGraphExecutionError> {
    let result_hash = hash_serializable(&("completed", &successful, &unsuccessful))?;
    Ok(ChildWaitProjection::Completed {
        successful,
        unsuccessful,
        result_hash,
    })
}

fn failed_wait(
    code: &str,
    cancel_children: Vec<SessionId>,
    detached: bool,
) -> Result<ChildWaitProjection, ChildGraphExecutionError> {
    let result_hash = hash_serializable(&(code, &cancel_children, detached))?;
    Ok(ChildWaitProjection::Failed {
        code: code.to_owned(),
        cancel_children,
        detached,
        result_hash,
    })
}

fn validate_bounded_value(value: &Value) -> Result<(), ChildGraphExecutionError> {
    let bytes =
        serde_json::to_vec(value).map_err(|_| ChildGraphExecutionError::InvalidVariables)?;
    if bytes.len() > MAX_TASK_BYTES
        || json_depth(value) > MAX_JSON_DEPTH
        || contains_forbidden_inline_secret(value)
    {
        Err(ChildGraphExecutionError::InvalidVariables)
    } else {
        Ok(())
    }
}

fn contains_forbidden_inline_secret(value: &Value) -> bool {
    match value {
        Value::Object(values) => values.iter().any(|(key, value)| {
            matches!(
                key.to_ascii_lowercase().as_str(),
                "secret" | "password" | "token" | "api_key" | "private_key"
            ) || contains_forbidden_inline_secret(value)
        }),
        Value::Array(values) => values.iter().any(contains_forbidden_inline_secret),
        _ => false,
    }
}

fn contains_exact_secret_reference(value: &Value) -> bool {
    match value {
        Value::String(value) => is_exact_secret_reference(value),
        Value::Array(values) => values.iter().any(contains_exact_secret_reference),
        Value::Object(values) => values.values().any(contains_exact_secret_reference),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn json_depth(value: &Value) -> usize {
    match value {
        Value::Array(values) => 1 + values.iter().map(json_depth).max().unwrap_or(0),
        Value::Object(values) => 1 + values.values().map(json_depth).max().unwrap_or(0),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => 1,
    }
}

fn valid_reference(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_REFERENCE_BYTES
        && !value.chars().any(char::is_control)
}

fn valid_failure_code(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_FAILURE_CODE_BYTES
        && !value.chars().any(char::is_control)
}

fn hash_serializable(value: &impl Serialize) -> Result<ContentHash, ChildGraphExecutionError> {
    serde_json::to_vec(value)
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| ChildGraphExecutionError::Hash)
}

/// Pure Graph-B node validation failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ChildGraphExecutionError {
    /// Work, run, plan, session, or parent authorization identity differs.
    #[error("child graph work identity does not match immutable parent authorization")]
    IdentityMismatch,
    /// Persisted executor does not name the exact supported implementation.
    #[error("child graph executor identity is unsupported")]
    UnsupportedExecutorIdentity,
    /// Compiled node bytes differ from the persisted resolution reference.
    #[error("compiled child graph node does not match persisted executor resolution")]
    CompiledNodeMismatch,
    /// Legacy or invalid graph omitted the typed configuration.
    #[error("child graph node requires an explicit versioned configuration migration")]
    ConfigurationRequired,
    /// Configuration is malformed.
    #[error("child graph node configuration is invalid")]
    InvalidConfiguration,
    /// Replay-derived input kind does not match the exact executor.
    #[error("child graph input does not match executor kind")]
    InputKindMismatch,
    /// Canonical variable environment is missing, malformed, or unbounded.
    #[error("child graph canonical variables are invalid")]
    InvalidVariables,
    /// Child proposal exceeds immutable parent authorization.
    #[error("child graph proposal exceeds immutable parent authorization")]
    ParentAuthorization,
    /// Task input is malformed, unbounded, or contains inline secrets.
    #[error("child task input is invalid")]
    InvalidTask,
    /// Exact wait child set is malformed or substituted.
    #[error("child wait set is invalid")]
    InvalidChildSet,
    /// Canonical child state is malformed.
    #[error("canonical child state is invalid")]
    InvalidChildState,
    /// Reviewer result is inconsistent, unbounded, or references unknown work.
    #[error("reviewer result is invalid")]
    InvalidReview,
    /// Logic-owned proposal hashing failed.
    #[error("child graph proposal could not be hashed")]
    Hash,
}

#[cfg(test)]
mod tests {
    use agentmod_graph_engine::{
        ChildSetSource, ChildWaitCancellation, ChildWorkspaceConfiguration, ExecutableNode,
        NodeConfiguration, NodeKind, NodeValueSource, ReviewResultSchema, ReviewRoutes,
        SecurityClassification,
    };
    use agentmod_primitives::SessionId;
    use uuid::Uuid;

    use super::*;
    use crate::session::{SessionNodeExecutorBoundary, SessionNodeExecutorSource};

    fn session_id(value: u128) -> SessionId {
        SessionId::from_uuid(Uuid::from_u128(value))
    }

    fn node(
        id: &str,
        kind: NodeKind,
        configuration: Option<NodeConfiguration>,
        read_variables: BTreeSet<String>,
    ) -> ExecutableNode {
        ExecutableNode {
            index: 0,
            id: id.to_owned(),
            kind,
            configuration,
            condition: None,
            tool: None,
            provider: (kind == NodeKind::Review).then(|| String::from("mock")),
            required_capabilities: BTreeSet::new(),
            read_scopes: BTreeSet::new(),
            write_scopes: BTreeSet::new(),
            read_variables,
            write_variables: BTreeSet::new(),
            retry_limit: 2,
            max_iterations: None,
        }
    }

    fn executor(node: &ExecutableNode) -> SessionNodeExecutorResolution {
        let (executor_id, node_kind) = match node.kind {
            NodeKind::SpawnChildAgent => ("runtime.child-spawn", "spawn_child_agent"),
            NodeKind::WaitForAgents => ("runtime.child-wait", "wait_for_agents"),
            NodeKind::Review => ("runtime.review", "review"),
            _ => panic!("unsupported test node"),
        };
        SessionNodeExecutorResolution {
            node_id: node.id.clone(),
            node_kind: node_kind.into(),
            executor_id: executor_id.into(),
            executor_version: "1.0.0".into(),
            source: SessionNodeExecutorSource::Runtime,
            boundary: SessionNodeExecutorBoundary::RuntimeLogic,
            required_capabilities: Vec::new(),
            resolved_capabilities: vec!["agents".into()],
            runtime_api_requirement: ">=0.1.0".into(),
            executor_declaration_hash: ContentHash::digest(executor_id.as_bytes()),
            adapter_configuration_reference: ContentHash::digest(
                &serde_json::to_vec(node).expect("node"),
            ),
        }
    }

    fn authorization(node: &ExecutableNode) -> ParentChildAuthorization {
        ParentChildAuthorization {
            parent_session_id: session_id(1),
            run_id: "renamed-graph-run".into(),
            execution_plan_hash: ContentHash::digest(b"plan"),
            executor: executor(node),
            node_ids: [node.id.clone()].into_iter().collect(),
            child_styles: ["worker-v1".to_owned()].into_iter().collect(),
            tool_groups: ["filesystem.read".to_owned()].into_iter().collect(),
            workspace_modes: [
                AuthorizedWorkspaceMode::SharedReadOnly,
                AuthorizedWorkspaceMode::TemporaryCopy,
            ]
            .into_iter()
            .collect(),
            custom_workspaces: BTreeSet::new(),
            artifact_references: [
                "artifact-brief".to_owned(),
                "artifact-evidence".to_owned(),
                "secret-ref:vault_record_17".to_owned(),
            ]
            .into_iter()
            .collect(),
            maximum_children: 4,
            maximum_depth: 3,
            current_depth: 0,
            maximum_token_budget: 1_000,
            maximum_context_budget_tokens: 500,
            maximum_cost_budget_micros: 10_000,
            inherited_provider: None,
            inherited_model: None,
            inherited_mcp: None,
        }
    }

    fn command(
        node: ExecutableNode,
        variables: BTreeMap<String, Value>,
        input: ChildGraphNodeInput,
    ) -> CoordinateChildGraphNodeCommand {
        let authorization = authorization(&node);
        CoordinateChildGraphNodeCommand {
            session_id: authorization.parent_session_id,
            work: NodeWorkIdentity {
                run_id: authorization.run_id.clone(),
                node_id: node.id.clone(),
                branch_path: Vec::new(),
                attempt: 1,
                loop_iteration: 0,
                step: 3,
            },
            execution_plan_hash: authorization.execution_plan_hash,
            executor: authorization.executor.clone(),
            node,
            variables,
            authorization,
            input,
        }
    }

    fn spawn_node(task_input: NodeValueSource) -> ExecutableNode {
        let read_variables = match &task_input {
            NodeValueSource::Variable { variable } => [variable.clone()].into_iter().collect(),
            NodeValueSource::Static { .. } => BTreeSet::new(),
        };
        node(
            "commission-renamed",
            NodeKind::SpawnChildAgent,
            Some(NodeConfiguration::SpawnChildAgent {
                task_input,
                task_id_prefix: "work".into(),
                child_style: "worker-v1".into(),
                tool_groups: ["filesystem.read".to_owned()].into_iter().collect(),
                maximum_children: 4,
                maximum_depth: 2,
                token_budget: 1_000,
                context_budget_tokens: 500,
                cost_budget_micros: 10_000,
                workspace: ChildWorkspaceConfiguration::SharedReadOnly,
                artifact_references: ["artifact-brief".to_owned()].into_iter().collect(),
                artifact_reference_variables: BTreeSet::new(),
                security_classification: SecurityClassification::Internal,
                approval_required: true,
            }),
            read_variables,
        )
    }

    fn wait_node(child_ids: BTreeSet<String>, minimum_successes: u32) -> ExecutableNode {
        node(
            "rendezvous-renamed",
            NodeKind::WaitForAgents,
            Some(NodeConfiguration::WaitForAgents {
                children: ChildSetSource::Exact { child_ids },
                maximum_children: 4,
                minimum_successes,
                timeout_ms: 1_000,
                cancellation: ChildWaitCancellation::Cascade,
            }),
            BTreeSet::new(),
        )
    }

    fn variable_wait_node(variable: &str, minimum_successes: u32) -> ExecutableNode {
        node(
            "rendezvous-renamed",
            NodeKind::WaitForAgents,
            Some(NodeConfiguration::WaitForAgents {
                children: ChildSetSource::Variable {
                    variable: variable.to_owned(),
                },
                maximum_children: 4,
                minimum_successes,
                timeout_ms: 1_000,
                cancellation: ChildWaitCancellation::Cascade,
            }),
            [variable.to_owned()].into_iter().collect(),
        )
    }

    fn review_node() -> ExecutableNode {
        node(
            "quality-gate-renamed",
            NodeKind::Review,
            Some(NodeConfiguration::Review {
                input: NodeValueSource::Static {
                    value: serde_json::json!({"result_reference": "node-result-1"}),
                },
                artifact_references: ["artifact-evidence".to_owned()].into_iter().collect(),
                artifact_reference_variables: BTreeSet::new(),
                result_schema: ReviewResultSchema {
                    maximum_findings: 4,
                    maximum_finding_bytes: 256,
                    maximum_rejections: 4,
                    require_artifact_evidence: true,
                },
                routes: ReviewRoutes {
                    approved: "accepted-renamed".into(),
                    revision: "revise-renamed".into(),
                    failure: "failed-renamed".into(),
                },
                maximum_revisions: 2,
            }),
            BTreeSet::new(),
        )
    }

    #[test]
    fn renamed_spawn_uses_static_and_variable_tasks_with_exact_authorization() {
        let static_node = spawn_node(NodeValueSource::Static {
            value: serde_json::json!({
                "task-b": "test the change",
                "task-a": "inspect the change"
            }),
        });
        let mut static_command = command(static_node, BTreeMap::new(), ChildGraphNodeInput::Spawn);
        static_command.authorization.inherited_provider = Some(String::from("deterministic-mock"));
        static_command.authorization.inherited_model = Some(String::from("mock-model"));
        let static_outcome = coordinate_child_graph_node(&static_command).expect("static spawn");
        let ChildGraphNodeOutcome::Spawn { proposals } = static_outcome else {
            panic!("spawn")
        };
        assert_eq!(
            proposals
                .iter()
                .map(|proposal| proposal.task_id.as_str())
                .collect::<Vec<_>>(),
            vec!["task-a", "task-b"]
        );
        assert!(proposals.iter().all(|proposal| {
            proposal.approval_required
                && proposal.child_style == "worker-v1"
                && proposal.inherited_provider.as_deref() == Some("deterministic-mock")
                && proposal.inherited_model.as_deref() == Some("mock-model")
                && proposal.proposal_hash != ContentHash::from_bytes([0; 32])
        }));
        let without_inheritance = coordinate_child_graph_node(&command(
            spawn_node(NodeValueSource::Static {
                value: serde_json::json!({
                    "task-b": "test the change",
                    "task-a": "inspect the change"
                }),
            }),
            BTreeMap::new(),
            ChildGraphNodeInput::Spawn,
        ))
        .expect("spawn without inheritance");
        let ChildGraphNodeOutcome::Spawn {
            proposals: without_inheritance,
        } = without_inheritance
        else {
            panic!("spawn")
        };
        assert_ne!(
            proposals[0].proposal_hash,
            without_inheritance[0].proposal_hash
        );

        let variable_node = spawn_node(NodeValueSource::Variable {
            variable: "assignments".into(),
        });
        let reverse_ordered_variables = BTreeMap::from([(
            "assignments".into(),
            serde_json::json!({
                "task-b": "test the change",
                "task-a": "inspect the change"
            }),
        )]);
        let variable_object_outcome = coordinate_child_graph_node(&command(
            variable_node.clone(),
            reverse_ordered_variables,
            ChildGraphNodeInput::Spawn,
        ))
        .expect("variable object spawn");
        let ChildGraphNodeOutcome::Spawn {
            proposals: variable_object_proposals,
        } = variable_object_outcome
        else {
            panic!("spawn")
        };
        assert_eq!(
            variable_object_proposals
                .iter()
                .map(|proposal| (proposal.task_id.as_str(), proposal.task.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("task-a", Some("inspect the change")),
                ("task-b", Some("test the change"))
            ]
        );
        let variables =
            BTreeMap::from([("assignments".into(), serde_json::json!(["inspect", "test"]))]);
        let variable_outcome = coordinate_child_graph_node(&command(
            variable_node,
            variables,
            ChildGraphNodeInput::Spawn,
        ))
        .expect("variable spawn");
        let ChildGraphNodeOutcome::Spawn { proposals } = variable_outcome else {
            panic!("spawn")
        };
        assert_eq!(
            proposals
                .iter()
                .map(|proposal| proposal.task_id.as_str())
                .collect::<Vec<_>>(),
            vec!["work-0", "work-1"]
        );
    }

    #[test]
    fn child_task_flow_accepts_only_exact_dedicated_secret_references() {
        let mut secret_node = spawn_node(NodeValueSource::Static {
            value: Value::String(String::from("secret-ref:vault_record_17")),
        });
        let Some(NodeConfiguration::SpawnChildAgent {
            security_classification,
            ..
        }) = secret_node.configuration.as_mut()
        else {
            panic!("spawn configuration")
        };
        *security_classification = SecurityClassification::SecretReference;
        let secret = coordinate_child_graph_node(&command(
            secret_node,
            BTreeMap::new(),
            ChildGraphNodeInput::Spawn,
        ))
        .expect("exact dedicated secret reference");
        assert!(matches!(secret, ChildGraphNodeOutcome::Spawn { .. }));

        let mut near_miss_node = spawn_node(NodeValueSource::Static {
            value: Value::String(String::from("secret:vault_record_17")),
        });
        let Some(NodeConfiguration::SpawnChildAgent {
            security_classification,
            ..
        }) = near_miss_node.configuration.as_mut()
        else {
            panic!("spawn configuration")
        };
        *security_classification = SecurityClassification::SecretReference;
        assert_eq!(
            coordinate_child_graph_node(&command(
                near_miss_node,
                BTreeMap::new(),
                ChildGraphNodeInput::Spawn,
            )),
            Err(ChildGraphExecutionError::InvalidTask)
        );

        let ordinary_capability = spawn_node(NodeValueSource::Static {
            value: Value::String(String::from("secret-ref:vault_record_17")),
        });
        assert_eq!(
            coordinate_child_graph_node(&command(
                ordinary_capability,
                BTreeMap::new(),
                ChildGraphNodeInput::Spawn,
            )),
            Err(ChildGraphExecutionError::InvalidTask)
        );

        let mut artifact_capability = spawn_node(NodeValueSource::Static {
            value: Value::String(String::from("inspect")),
        });
        let Some(NodeConfiguration::SpawnChildAgent {
            artifact_references,
            ..
        }) = artifact_capability.configuration.as_mut()
        else {
            panic!("spawn configuration")
        };
        artifact_references.insert(String::from("secret-ref:vault_record_17"));
        assert_eq!(
            coordinate_child_graph_node(&command(
                artifact_capability,
                BTreeMap::new(),
                ChildGraphNodeInput::Spawn,
            )),
            Err(ChildGraphExecutionError::InvalidTask)
        );
    }

    #[test]
    fn exact_plan_work_executor_and_bounded_task_substitution_fail_closed() {
        let node = spawn_node(NodeValueSource::Static {
            value: Value::String("inspect".into()),
        });
        let base = command(node, BTreeMap::new(), ChildGraphNodeInput::Spawn);

        let mut wrong_plan = base.clone();
        wrong_plan.execution_plan_hash = ContentHash::digest(b"other");
        assert_eq!(
            coordinate_child_graph_node(&wrong_plan),
            Err(ChildGraphExecutionError::IdentityMismatch)
        );

        let mut wrong_executor = base.clone();
        wrong_executor.executor.executor_version = "2.0.0".into();
        assert_eq!(
            coordinate_child_graph_node(&wrong_executor),
            Err(ChildGraphExecutionError::IdentityMismatch)
        );

        let mut forged_plan_executor = base.clone();
        forged_plan_executor.executor.executor_version = "2.0.0".into();
        forged_plan_executor.authorization.executor = forged_plan_executor.executor.clone();
        assert_eq!(
            coordinate_child_graph_node(&forged_plan_executor),
            Err(ChildGraphExecutionError::UnsupportedExecutorIdentity)
        );

        let mut substituted_node = base.clone();
        let Some(NodeConfiguration::SpawnChildAgent { child_style, .. }) =
            substituted_node.node.configuration.as_mut()
        else {
            panic!("spawn")
        };
        *child_style = "substituted".into();
        assert_eq!(
            coordinate_child_graph_node(&substituted_node),
            Err(ChildGraphExecutionError::CompiledNodeMismatch)
        );

        let oversized_node = spawn_node(NodeValueSource::Static {
            value: Value::String("x".repeat(MAX_TASK_BYTES + 1)),
        });
        assert_eq!(
            coordinate_child_graph_node(&command(
                oversized_node,
                BTreeMap::new(),
                ChildGraphNodeInput::Spawn,
            )),
            Err(ChildGraphExecutionError::InvalidTask)
        );

        let secret_node = spawn_node(NodeValueSource::Static {
            value: serde_json::json!({"task-a": "inspect", "token": "inline-secret"}),
        });
        assert_eq!(
            coordinate_child_graph_node(&command(
                secret_node,
                BTreeMap::new(),
                ChildGraphNodeInput::Spawn,
            )),
            Err(ChildGraphExecutionError::InvalidTask)
        );
    }

    #[test]
    fn wait_projection_is_stably_ordered_and_restart_identical() {
        let child_a = session_id(10);
        let child_b = session_id(11);
        let node = wait_node(
            [child_b.to_string(), child_a.to_string()]
                .into_iter()
                .collect(),
            2,
        );
        let completed_a = CanonicalChildState {
            child_id: child_a,
            task_id: "task-a".into(),
            status: CanonicalChildStatus::Completed {
                result_reference: "result-a".into(),
                artifact_references: BTreeSet::new(),
                completion_sequence: 10,
            },
        };
        let pending_b = CanonicalChildState {
            child_id: child_b,
            task_id: "task-b".into(),
            status: CanonicalChildStatus::Pending,
        };
        let first = coordinate_child_graph_node(&command(
            node.clone(),
            BTreeMap::new(),
            ChildGraphNodeInput::Wait {
                elapsed_ms: 100,
                cancellation_requested: false,
                children: vec![pending_b.clone(), completed_a.clone()],
            },
        ))
        .expect("first projection");
        let restarted = coordinate_child_graph_node(&command(
            node.clone(),
            BTreeMap::new(),
            ChildGraphNodeInput::Wait {
                elapsed_ms: 100,
                cancellation_requested: false,
                children: vec![completed_a.clone(), pending_b],
            },
        ))
        .expect("restart projection");
        assert_eq!(first, restarted);
        assert!(matches!(
            first,
            ChildGraphNodeOutcome::Wait(ChildWaitProjection::Waiting {
                successful,
                pending,
                remaining_ms: 900,
                ..
            }) if successful.iter().map(|receipt| receipt.child_id).collect::<Vec<_>>()
                    == vec![child_a]
                && pending == vec![child_b]
        ));

        let completed_b = CanonicalChildState {
            child_id: child_b,
            task_id: "task-b".into(),
            status: CanonicalChildStatus::Completed {
                result_reference: "result-b".into(),
                artifact_references: BTreeSet::new(),
                completion_sequence: 11,
            },
        };
        let done = coordinate_child_graph_node(&command(
            node,
            BTreeMap::new(),
            ChildGraphNodeInput::Wait {
                elapsed_ms: 120,
                cancellation_requested: false,
                children: vec![completed_b, completed_a],
            },
        ))
        .expect("completed projection");
        assert!(matches!(
            done,
            ChildGraphNodeOutcome::Wait(ChildWaitProjection::Completed {
                successful,
                result_hash,
                ..
            }) if successful.iter().map(|receipt| receipt.child_id).collect::<Vec<_>>()
                    == vec![child_a, child_b]
                && result_hash != ContentHash::from_bytes([0; 32])
        ));
    }

    #[test]
    fn wait_variable_accepts_one_child_id_or_a_bounded_child_id_list() {
        let child_a = session_id(12);
        let completed = CanonicalChildState {
            child_id: child_a,
            task_id: "task-a".into(),
            status: CanonicalChildStatus::Completed {
                result_reference: "result-a".into(),
                artifact_references: BTreeSet::new(),
                completion_sequence: 12,
            },
        };
        for value in [
            Value::String(child_a.to_string()),
            serde_json::json!([child_a.to_string()]),
        ] {
            let outcome = coordinate_child_graph_node(&command(
                variable_wait_node("worker_id", 1),
                BTreeMap::from([("worker_id".into(), value)]),
                ChildGraphNodeInput::Wait {
                    elapsed_ms: 12,
                    cancellation_requested: false,
                    children: vec![completed.clone()],
                },
            ))
            .expect("singleton child variable");
            assert!(matches!(
                outcome,
                ChildGraphNodeOutcome::Wait(ChildWaitProjection::Completed {
                    successful,
                    ..
                }) if successful.len() == 1 && successful[0].child_id == child_a
            ));
        }
    }

    #[test]
    fn wait_cancellation_timeout_and_child_substitution_fail_closed() {
        let child_a = session_id(20);
        let child_b = session_id(21);
        let node = wait_node(
            [child_a.to_string(), child_b.to_string()]
                .into_iter()
                .collect(),
            2,
        );
        let pending = |child_id, task_id: &str| CanonicalChildState {
            child_id,
            task_id: task_id.to_owned(),
            status: CanonicalChildStatus::Pending,
        };
        let cancelled = coordinate_child_graph_node(&command(
            node.clone(),
            BTreeMap::new(),
            ChildGraphNodeInput::Wait {
                elapsed_ms: 1,
                cancellation_requested: true,
                children: vec![pending(child_b, "task-b"), pending(child_a, "task-a")],
            },
        ))
        .expect("cancel projection");
        assert!(matches!(
            cancelled,
            ChildGraphNodeOutcome::Wait(ChildWaitProjection::Failed {
                ref code,
                cancel_children,
                detached: false,
                ..
            }) if code == "parent_cancelled"
                && cancel_children == vec![child_a, child_b]
        ));

        let timed_out = coordinate_child_graph_node(&command(
            node.clone(),
            BTreeMap::new(),
            ChildGraphNodeInput::Wait {
                elapsed_ms: 1_000,
                cancellation_requested: false,
                children: vec![pending(child_a, "task-a"), pending(child_b, "task-b")],
            },
        ))
        .expect("timeout projection");
        assert!(matches!(
            timed_out,
            ChildGraphNodeOutcome::Wait(ChildWaitProjection::Failed { ref code, .. })
                if code == "child_wait_timeout"
        ));

        let substituted = coordinate_child_graph_node(&command(
            node,
            BTreeMap::new(),
            ChildGraphNodeInput::Wait {
                elapsed_ms: 1,
                cancellation_requested: false,
                children: vec![pending(session_id(99), "task-x")],
            },
        ));
        assert_eq!(substituted, Err(ChildGraphExecutionError::InvalidChildSet));
    }

    #[test]
    fn reviewer_rejection_revision_approval_and_structured_failure_are_exact() {
        let node = review_node();
        let finding = ReviewFinding {
            code: "missing-test".into(),
            message: "add restart evidence".into(),
            artifact_references: ["artifact-evidence".to_owned()].into_iter().collect(),
        };
        let rejected = ReviewCandidate {
            approved: false,
            rejected_task_ids: ["task-a".to_owned()].into_iter().collect(),
            findings: vec![finding],
            source_result_hash: ContentHash::digest(b"review"),
        };
        let revision = coordinate_child_graph_node(&command(
            node.clone(),
            BTreeMap::new(),
            ChildGraphNodeInput::Review {
                revision: 0,
                known_task_ids: ["task-a".to_owned()].into_iter().collect(),
                candidate: rejected.clone(),
            },
        ))
        .expect("revision");
        assert!(matches!(
            revision,
            ChildGraphNodeOutcome::Review(ReviewRoutingProposal {
                disposition: ReviewDisposition::Revision,
                ref destination_node_id,
                next_revision: Some(1),
                ..
            }) if destination_node_id == "revise-renamed"
        ));

        let failed = coordinate_child_graph_node(&command(
            node.clone(),
            BTreeMap::new(),
            ChildGraphNodeInput::Review {
                revision: 2,
                known_task_ids: ["task-a".to_owned()].into_iter().collect(),
                candidate: rejected,
            },
        ))
        .expect("structured failure");
        assert!(matches!(
            failed,
            ChildGraphNodeOutcome::Review(ReviewRoutingProposal {
                disposition: ReviewDisposition::Failed,
                ref destination_node_id,
                next_revision: None,
                ..
            }) if destination_node_id == "failed-renamed"
        ));

        let approved = coordinate_child_graph_node(&command(
            node.clone(),
            BTreeMap::new(),
            ChildGraphNodeInput::Review {
                revision: 1,
                known_task_ids: ["task-a".to_owned()].into_iter().collect(),
                candidate: ReviewCandidate {
                    approved: true,
                    rejected_task_ids: BTreeSet::new(),
                    findings: Vec::new(),
                    source_result_hash: ContentHash::digest(b"approved"),
                },
            },
        ))
        .expect("approved");
        assert!(matches!(
            approved,
            ChildGraphNodeOutcome::Review(ReviewRoutingProposal {
                disposition: ReviewDisposition::Approved,
                ref destination_node_id,
                ..
            }) if destination_node_id == "accepted-renamed"
        ));

        let invalid = coordinate_child_graph_node(&command(
            node,
            BTreeMap::new(),
            ChildGraphNodeInput::Review {
                revision: 0,
                known_task_ids: ["task-a".to_owned()].into_iter().collect(),
                candidate: ReviewCandidate {
                    approved: false,
                    rejected_task_ids: ["unknown".to_owned()].into_iter().collect(),
                    findings: Vec::new(),
                    source_result_hash: ContentHash::digest(b"invalid"),
                },
            },
        ));
        assert_eq!(invalid, Err(ChildGraphExecutionError::InvalidReview));
    }

    #[test]
    fn planless_legacy_node_requires_explicit_branch_migration() {
        let node = node(
            "legacy-spawn",
            NodeKind::SpawnChildAgent,
            None,
            BTreeSet::new(),
        );
        assert_eq!(
            coordinate_child_graph_node(&command(
                node,
                BTreeMap::new(),
                ChildGraphNodeInput::Spawn,
            )),
            Err(ChildGraphExecutionError::ConfigurationRequired)
        );
    }
}
