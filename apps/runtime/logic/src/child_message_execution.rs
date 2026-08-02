//! Pure business contract for generic child-message node execution.
//!
//! This module resolves and validates one exact child-message operation, but it
//! deliberately does not append journals, allocate canonical envelope fields,
//! run policy, or call a dependency. Runtime orchestration remains responsible
//! for those consequential boundaries.

use std::collections::{BTreeMap, BTreeSet};

use agentmod_event_model::ArtifactReference;
use agentmod_graph_engine::{
    ChildMessageCancellation, ChildSelector, ExecutableNode, NodeConfiguration, NodeKind,
    SecurityClassification, VariableDeclaration, VariableValueType,
};
use agentmod_primitives::{ByteCount, ContentHash, EventId, Sequence, SessionId};
use agentmod_runtime_data::child_message::{
    AppendedChildMessageDataRecord, ChildMessageDataError, ChildMessageDependencyFailure,
    ChildMessageJournalHeadData,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::{
    action::{ActionProposal, ChildAgentMessageAction, ConsequentialAction, ProposalId},
    information_flow::{
        InformationFlowClassification, InformationFlowDecision, InformationFlowSink,
        InformationFlowSource, evaluate_information_flow, is_exact_secret_reference,
    },
    node_execution::NodeWorkIdentity,
    session::{
        SessionLifecycle, SessionNodeExecutorBoundary, SessionNodeExecutorResolution,
        SessionNodeExecutorSource,
    },
};

const CHILD_MESSAGE_EXECUTOR_ID: &str = "runtime.child-message";
const CHILD_MESSAGE_EXECUTOR_VERSION: &str = "1.0.0";
const CHILD_MESSAGE_NODE_KIND: &str = "send_child_agent_message";
const CHILD_MESSAGE_EVENT_TYPE: &str = "child_agent.message_received";
const CHILD_MESSAGE_ACTION_KIND: &str = "child_agent.message_delivery";
const MAX_MESSAGE_BYTES: usize = 256 * 1024;
const MAX_PAYLOAD_DEPTH: usize = 32;
const MAX_PAYLOAD_VALUES: usize = 8_192;
const MAX_COLLECTION_ITEMS: usize = 1_024;
const MAX_STRING_BYTES: usize = 16 * 1024;
const MAX_ARTIFACT_REFERENCES: usize = 64;
const MAX_ARTIFACT_REFERENCE_BYTES: usize = 256;
const MAX_IDENTITY_BYTES: usize = 256;
const MAX_BRANCH_DEPTH: usize = 32;

/// Exact replay-derived parent ownership expected for the selected child.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildMessageParentLink {
    /// Parent session that owns the child.
    pub parent_session_id: SessionId,
    /// Canonical child-creation proposal sequence.
    pub parent_action_sequence: Sequence,
    /// Parent graph node that created the child.
    pub parent_graph_node_id: String,
    /// Runtime-owned task identity.
    pub task_id: String,
}

/// Replay-derived target state supplied to the pure child-message contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildMessageTarget {
    /// Exact target child session.
    pub child_session_id: SessionId,
    /// Current canonical child lifecycle.
    pub lifecycle: SessionLifecycle,
    /// Whether cancellation has begun but may not yet be terminal.
    pub cancellation_started: bool,
    /// Immutable child ownership reconstructed from sequence two.
    pub parent_link: ChildMessageParentLink,
}

/// Canonical variable declarations and current typed values visible to a node.
#[derive(Clone, Debug, PartialEq)]
pub struct ChildMessageVariableEnvironment {
    /// Immutable graph declarations keyed by exact variable name.
    pub declarations: BTreeMap<String, VariableDeclaration>,
    /// Replay-derived live values keyed by exact variable name.
    pub values: BTreeMap<String, Value>,
}

/// Exact active execution binding against which a request is checked.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveChildMessageNodeBinding {
    /// Active immutable work identity reconstructed by replay.
    pub work: NodeWorkIdentity,
    /// Execution-plan hash committed at run initialization.
    pub execution_plan_hash: ContentHash,
}

/// Logic-owned command that prepares one child-message proposal.
#[derive(Clone, Debug, PartialEq)]
pub struct PrepareChildMessageCommand {
    /// Canonical parent session.
    pub parent_session_id: SessionId,
    /// Requested immutable work identity.
    pub work: NodeWorkIdentity,
    /// Active work and plan binding reconstructed from canonical events.
    pub active: ActiveChildMessageNodeBinding,
    /// Requested execution-plan hash.
    pub execution_plan_hash: ContentHash,
    /// Exact persisted executor resolution for the compiled node.
    pub executor: SessionNodeExecutorResolution,
    /// Exact retained compiled node.
    pub compiled_node: ExecutableNode,
    /// Canonical variables visible to the node.
    pub variables: ChildMessageVariableEnvironment,
    /// Exact immutable artifact records resolved through runtime data before
    /// policy evaluation, keyed by the graph-declared reference.
    pub resolved_artifacts: BTreeMap<String, ArtifactReference>,
    /// Exact expected ownership link selected by runtime orchestration.
    pub expected_parent_link: ChildMessageParentLink,
    /// Replay-derived child target.
    pub target: ChildMessageTarget,
    /// Whether cancellation has begun for the parent run.
    pub parent_cancellation_started: bool,
}

/// Runtime-policy proposal for a bounded child-message delivery.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildMessageProposal {
    /// Consequential action kind passed through runtime policy.
    pub action_kind: String,
    /// Stable deterministic business message identity.
    pub message_identity: ContentHash,
    /// Stable exact-operation idempotency digest.
    pub idempotency_digest: ContentHash,
    /// Digest of the complete consequential proposal.
    pub action_digest: ContentHash,
    /// Exact parent session.
    pub parent_session_id: SessionId,
    /// Exact child session.
    pub child_session_id: SessionId,
    /// Exact graph work owning the operation.
    pub work: NodeWorkIdentity,
    /// Exact immutable execution plan.
    pub execution_plan_hash: ContentHash,
    /// Exact compiled-node configuration reference.
    pub configuration_hash: ContentHash,
    /// Immutable ownership link.
    pub parent_link: ChildMessageParentLink,
    /// Canonical bounded typed payload.
    pub payload: Value,
    /// Hash of canonical payload bytes.
    pub payload_hash: ContentHash,
    /// Canonical payload byte count.
    pub message_bytes: ByteCount,
    /// Ordered declared artifact references.
    pub declared_artifact_references: BTreeSet<String>,
    /// Runtime-resolved immutable artifact records that must be copied into
    /// canonical child event metadata in this exact order.
    pub artifact_references: Vec<ArtifactReference>,
    /// Hash of canonical artifact-reference bytes.
    pub artifact_references_hash: ContentHash,
    /// Declared information-flow classification.
    pub security_classification: SecurityClassification,
    /// Exact configured message-size bound.
    pub max_message_bytes: u64,
    /// Exact compiled cancellation behavior.
    pub cancellation: ChildMessageCancellation,
}

/// Typed child-journal payload that runtime logic asks the event model to seal.
///
/// This is not an event envelope. In particular it contains no sequence,
/// correlation, causation, origin, or commitment metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildMessageReceivedPayload {
    /// Stable message identity.
    pub message_id: EventId,
    /// Deterministic business message identity.
    pub message_identity: ContentHash,
    /// Exact parent session.
    pub parent_session_id: SessionId,
    /// Exact parent ownership proposal.
    pub parent_action_sequence: Sequence,
    /// Parent node that owns the target child.
    pub parent_graph_node_id: String,
    /// Runtime-owned child task identity.
    pub task_id: String,
    /// Exact graph work that sent the message.
    pub work: NodeWorkIdentity,
    /// Exact immutable execution plan.
    pub execution_plan_hash: ContentHash,
    /// Exact compiled-node configuration.
    pub configuration_hash: ContentHash,
    /// Bounded typed message body.
    pub payload: Value,
    /// Canonical message-body hash.
    pub payload_hash: ContentHash,
    /// Canonical message-body byte count.
    pub message_bytes: ByteCount,
    /// Graph-declared artifact references.
    pub declared_artifact_references: BTreeSet<String>,
    /// Runtime-resolved immutable artifact records.
    pub artifact_references: Vec<ArtifactReference>,
    /// Canonical artifact-reference hash.
    pub artifact_references_hash: ContentHash,
    /// Declared information-flow classification.
    pub security_classification: SecurityClassification,
    /// Stable exact-operation idempotency digest.
    pub idempotency_digest: ContentHash,
}

/// Sequence/head-bound dispatch specification produced after policy approval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildMessageDispatchSpecification {
    /// Static canonical event type that runtime must seal.
    pub event_type: String,
    /// Canonical event identity allocated and committed by runtime
    /// orchestration before dispatch.
    pub message_id: EventId,
    /// Exact proposal accepted by policy.
    pub proposal: ChildMessageProposal,
    /// Typed payload to place in the child journal.
    pub child_payload: ChildMessageReceivedPayload,
    /// Observed exact child tail.
    pub expected_head: ChildMessageJournalHeadData,
    /// Required next child-journal sequence.
    pub message_sequence: Sequence,
    /// Checksum of the runtime-sealed event envelope.
    pub envelope_checksum: ContentHash,
}

/// Canonical parent-side receipt payload after an exact data receipt validates.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildMessageParentReceipt {
    /// Stable message identity.
    pub message_id: EventId,
    /// Deterministic business message identity.
    pub message_identity: ContentHash,
    /// Exact child session.
    pub child_session_id: SessionId,
    /// Exact graph work that sent the message.
    pub work: NodeWorkIdentity,
    /// Exact immutable execution plan.
    pub execution_plan_hash: ContentHash,
    /// Canonical child-journal receipt sequence.
    pub child_sequence: Sequence,
    /// Canonical child event checksum.
    pub envelope_checksum: ContentHash,
    /// Canonical child journal-frame checksum.
    pub journal_checksum: ContentHash,
    /// Previous child journal-frame checksum.
    pub previous_journal_checksum: ContentHash,
    /// Receipt frame offset.
    pub offset: ByteCount,
    /// Child journal bytes after the receipt.
    pub journal_bytes: ByteCount,
    /// Canonical payload hash.
    pub payload_hash: ContentHash,
    /// Canonical payload byte count.
    pub message_bytes: ByteCount,
    /// Canonical artifact-reference hash.
    pub artifact_references_hash: ContentHash,
    /// Declared information-flow classification of the delivered payload.
    pub security_classification: SecurityClassification,
    /// Exact compiled maximum for the delivered payload.
    pub max_message_bytes: u64,
    /// True only for an exact previously committed receipt.
    pub replayed: bool,
}

/// Valid terminal receipt classification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildMessageReceiptDisposition {
    /// This dispatch appended the canonical child event.
    Fresh(ChildMessageParentReceipt),
    /// Storage proved the exact canonical child event already existed.
    Replayed(ChildMessageParentReceipt),
}

/// Recovery disposition for a failed append attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildMessageFailureDisposition {
    /// Request was rejected before an ambiguous storage boundary.
    Rejected,
    /// The stable identity is already bound to different content.
    Conflict,
    /// Storage may have committed; reconciliation is required and redispatch
    /// is prohibited until a terminal receipt is recovered.
    AmbiguousNoRedispatch,
}

/// Prepares a bounded, exact child-message proposal without causing effects.
///
/// # Errors
///
/// Returns [`ChildMessageExecutionError`] when the executor, active work,
/// configuration, variable, ownership, lifecycle, or payload is invalid.
pub fn prepare_child_message(
    command: &PrepareChildMessageCommand,
) -> Result<ChildMessageProposal, ChildMessageExecutionError> {
    validate_active_binding(command)?;
    let configuration_hash = validate_executor_and_node(command)?;
    validate_parent_link(&command.expected_parent_link)?;
    validate_parent_link(&command.target.parent_link)?;
    if command.expected_parent_link != command.target.parent_link
        || command.expected_parent_link.parent_session_id != command.parent_session_id
    {
        return Err(ChildMessageExecutionError::OwnershipMismatch);
    }

    let NodeConfiguration::SendChildAgentMessage {
        child,
        payload,
        artifact_references,
        security_classification,
        max_message_bytes,
        cancellation,
    } = command
        .compiled_node
        .configuration
        .as_ref()
        .ok_or(ChildMessageExecutionError::InvalidConfiguration)?
    else {
        return Err(ChildMessageExecutionError::InvalidConfiguration);
    };
    let selected_child = resolve_child(child, &command.compiled_node, &command.variables)?;
    if selected_child != command.target.child_session_id {
        return Err(ChildMessageExecutionError::ChildIdentityMismatch);
    }
    validate_lifecycle_and_cancellation(
        command.target.lifecycle,
        command.parent_cancellation_started,
        command.target.cancellation_started,
        *cancellation,
    )?;

    let prepared = prepare_child_message_payload(
        configuration_hash,
        payload,
        artifact_references,
        &command.resolved_artifacts,
        *security_classification,
        *max_message_bytes,
    )?;

    let identity = ChildMessageIdentityMaterial {
        parent_session_id: command.parent_session_id,
        child_session_id: selected_child,
        work: command.work.clone(),
        execution_plan_hash: command.execution_plan_hash,
        configuration_hash,
        parent_link: command.expected_parent_link.clone(),
        payload_hash: prepared.payload_hash,
        artifact_references_hash: prepared.artifact_references_hash,
        security_classification: *security_classification,
        max_message_bytes: *max_message_bytes,
        cancellation: *cancellation,
    };
    let identity_bytes =
        serde_json::to_vec(&identity).map_err(|_| ChildMessageExecutionError::InvalidPayload)?;
    let message_identity = ContentHash::digest(&identity_bytes);
    let idempotency_digest = ContentHash::digest(
        &[b"child-message-idempotency@1\0".as_slice(), &identity_bytes].concat(),
    );
    let action_digest = ContentHash::digest(
        &serde_json::to_vec(&ChildMessageActionMaterial {
            action_kind: CHILD_MESSAGE_ACTION_KIND,
            message_identity,
            identity,
        })
        .map_err(|_| ChildMessageExecutionError::InvalidPayload)?,
    );

    Ok(ChildMessageProposal {
        action_kind: CHILD_MESSAGE_ACTION_KIND.to_owned(),
        message_identity,
        idempotency_digest,
        action_digest,
        parent_session_id: command.parent_session_id,
        child_session_id: selected_child,
        work: command.work.clone(),
        execution_plan_hash: command.execution_plan_hash,
        configuration_hash,
        parent_link: command.expected_parent_link.clone(),
        payload: prepared.payload,
        payload_hash: prepared.payload_hash,
        message_bytes: prepared.message_bytes,
        declared_artifact_references: artifact_references.clone(),
        artifact_references: prepared.artifact_references,
        artifact_references_hash: prepared.artifact_references_hash,
        security_classification: *security_classification,
        max_message_bytes: *max_message_bytes,
        cancellation: *cancellation,
    })
}

/// Builds the exact typed consequential action evaluated by interceptors and
/// permission policy for a canonical child-message proposal.
#[must_use]
pub fn child_message_action_proposal(
    proposal: &ChildMessageProposal,
    style: &str,
    workspace: &str,
) -> ActionProposal {
    ActionProposal {
        id: ProposalId(format!("child-message:{}", proposal.message_identity)),
        action: ConsequentialAction::ChildAgentMessage(ChildAgentMessageAction {
            child_session_id: proposal.child_session_id.to_string(),
            message_identity: proposal.message_identity,
            payload_hash: proposal.payload_hash,
            artifact_references_hash: proposal.artifact_references_hash,
            security_classification: serde_json::to_value(proposal.security_classification)
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
                .unwrap_or_else(|| String::from("invalid")),
        }),
        style: style.to_owned(),
        workspace: workspace.to_owned(),
        origin: String::from("runtime.child-message"),
    }
}

/// Binds an approved proposal to an exact child journal head and sealed event.
///
/// Runtime must run interceptors, user policy, and mandatory policy before
/// calling this function. The supplied checksum is the checksum of the
/// runtime-owned canonical envelope containing [`ChildMessageReceivedPayload`].
///
/// # Errors
///
/// Returns [`ChildMessageExecutionError`] if child sequence arithmetic
/// overflows.
pub fn bind_child_message_dispatch(
    proposal: &ChildMessageProposal,
    expected_head: ChildMessageJournalHeadData,
    message_id: EventId,
    envelope_checksum: ContentHash,
) -> Result<ChildMessageDispatchSpecification, ChildMessageExecutionError> {
    let message_sequence = expected_head
        .sequence
        .checked_next()
        .map_err(|_| ChildMessageExecutionError::SequenceOverflow)?;
    Ok(ChildMessageDispatchSpecification {
        event_type: CHILD_MESSAGE_EVENT_TYPE.to_owned(),
        message_id,
        child_payload: ChildMessageReceivedPayload {
            message_id,
            message_identity: proposal.message_identity,
            parent_session_id: proposal.parent_session_id,
            parent_action_sequence: proposal.parent_link.parent_action_sequence,
            parent_graph_node_id: proposal.parent_link.parent_graph_node_id.clone(),
            task_id: proposal.parent_link.task_id.clone(),
            work: proposal.work.clone(),
            execution_plan_hash: proposal.execution_plan_hash,
            configuration_hash: proposal.configuration_hash,
            payload: proposal.payload.clone(),
            payload_hash: proposal.payload_hash,
            message_bytes: proposal.message_bytes,
            declared_artifact_references: proposal.declared_artifact_references.clone(),
            artifact_references: proposal.artifact_references.clone(),
            artifact_references_hash: proposal.artifact_references_hash,
            security_classification: proposal.security_classification,
            idempotency_digest: proposal.idempotency_digest,
        },
        proposal: proposal.clone(),
        expected_head,
        message_sequence,
        envelope_checksum,
    })
}

/// Validates an append receipt against the complete exact dispatch.
///
/// # Errors
///
/// Returns [`ChildMessageExecutionError::ReceiptMismatch`] for every receipt
/// that cannot prove a fresh append or exact replay.
pub fn validate_child_message_receipt(
    dispatch: &ChildMessageDispatchSpecification,
    receipt: &AppendedChildMessageDataRecord,
) -> Result<ChildMessageReceiptDisposition, ChildMessageExecutionError> {
    if receipt.message_id != dispatch.message_id
        || receipt.sequence != dispatch.message_sequence
        || receipt.envelope_checksum != dispatch.envelope_checksum
        || receipt.previous_journal_checksum != Some(dispatch.expected_head.checksum)
        || receipt.offset.get() > receipt.journal_bytes.get()
    {
        return Err(ChildMessageExecutionError::ReceiptMismatch);
    }
    let parent_receipt = ChildMessageParentReceipt {
        message_id: receipt.message_id,
        message_identity: dispatch.proposal.message_identity,
        child_session_id: dispatch.proposal.child_session_id,
        work: dispatch.proposal.work.clone(),
        execution_plan_hash: dispatch.proposal.execution_plan_hash,
        child_sequence: receipt.sequence,
        envelope_checksum: receipt.envelope_checksum,
        journal_checksum: receipt.journal_checksum,
        previous_journal_checksum: dispatch.expected_head.checksum,
        offset: receipt.offset,
        journal_bytes: receipt.journal_bytes,
        payload_hash: dispatch.proposal.payload_hash,
        message_bytes: dispatch.proposal.message_bytes,
        artifact_references_hash: dispatch.proposal.artifact_references_hash,
        security_classification: dispatch.proposal.security_classification,
        max_message_bytes: dispatch.proposal.max_message_bytes,
        replayed: receipt.replayed,
    };
    Ok(if receipt.replayed {
        ChildMessageReceiptDisposition::Replayed(parent_receipt)
    } else {
        ChildMessageReceiptDisposition::Fresh(parent_receipt)
    })
}

/// Requires a recovered proposal to be byte-for-byte the same operation.
///
/// # Errors
///
/// Returns [`ChildMessageExecutionError::ConflictingOperation`] rather than
/// permitting a changed payload or target to reuse an existing operation.
pub fn validate_recovered_child_message(
    expected: &ChildMessageProposal,
    recovered: &ChildMessageProposal,
) -> Result<(), ChildMessageExecutionError> {
    if expected == recovered {
        Ok(())
    } else {
        Err(ChildMessageExecutionError::ConflictingOperation)
    }
}

/// Classifies data-boundary failures without authorizing an automatic retry.
#[must_use]
pub const fn classify_child_message_failure(
    error: &ChildMessageDataError,
) -> ChildMessageFailureDisposition {
    match error {
        ChildMessageDataError::Dependency {
            category: ChildMessageDependencyFailure::ConflictingDuplicate,
            ..
        } => ChildMessageFailureDisposition::Conflict,
        ChildMessageDataError::Dependency {
            category: ChildMessageDependencyFailure::Journal | ChildMessageDependencyFailure::Access,
            ..
        } => ChildMessageFailureDisposition::AmbiguousNoRedispatch,
        ChildMessageDataError::Dependency {
            category:
                ChildMessageDependencyFailure::InvalidRequest
                | ChildMessageDependencyFailure::Identity
                | ChildMessageDependencyFailure::Lifecycle
                | ChildMessageDependencyFailure::StaleHead
                | ChildMessageDependencyFailure::Sequence,
            ..
        }
        | ChildMessageDataError::InvalidRequest
        | ChildMessageDataError::SequenceMismatch { .. }
        | ChildMessageDataError::SequenceOverflow
        | ChildMessageDataError::EventIntegrity { .. }
        | ChildMessageDataError::EventSerialization { .. }
        | ChildMessageDataError::EventTooLarge
        | ChildMessageDataError::ProjectionHashMismatch => ChildMessageFailureDisposition::Rejected,
        ChildMessageDataError::InvalidReceipt => {
            ChildMessageFailureDisposition::AmbiguousNoRedispatch
        }
    }
}

fn validate_active_binding(
    command: &PrepareChildMessageCommand,
) -> Result<(), ChildMessageExecutionError> {
    if command.work != command.active.work
        || command.execution_plan_hash != command.active.execution_plan_hash
        || command.work.run_id.trim().is_empty()
        || command.work.node_id.trim().is_empty()
        || command.work.node_id != command.compiled_node.id
        || command.work.attempt == 0
        || command.work.step == 0
        || command.work.run_id.len() > MAX_IDENTITY_BYTES
        || command.work.node_id.len() > MAX_IDENTITY_BYTES
        || command.work.branch_path.len() > MAX_BRANCH_DEPTH
        || command
            .work
            .branch_path
            .iter()
            .any(|branch| branch.trim().is_empty() || branch.len() > MAX_IDENTITY_BYTES)
    {
        return Err(ChildMessageExecutionError::ActiveWorkMismatch);
    }
    Ok(())
}

fn validate_executor_and_node(
    command: &PrepareChildMessageCommand,
) -> Result<ContentHash, ChildMessageExecutionError> {
    let serialized_kind = serde_json::to_value(command.compiled_node.kind)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or(ChildMessageExecutionError::InvalidConfiguration)?;
    let configuration_hash = ContentHash::digest(
        &serde_json::to_vec(&command.compiled_node)
            .map_err(|_| ChildMessageExecutionError::InvalidConfiguration)?,
    );
    if command.compiled_node.kind != NodeKind::SendChildAgentMessage
        || serialized_kind != CHILD_MESSAGE_NODE_KIND
        || command.executor.node_id != command.compiled_node.id
        || command.executor.node_kind != serialized_kind
        || command.executor.executor_id != CHILD_MESSAGE_EXECUTOR_ID
        || command.executor.executor_version != CHILD_MESSAGE_EXECUTOR_VERSION
        || command.executor.source != SessionNodeExecutorSource::Runtime
        || command.executor.boundary != SessionNodeExecutorBoundary::RuntimeLogic
        || command.executor.adapter_configuration_reference != configuration_hash
    {
        return Err(ChildMessageExecutionError::UnsupportedExecutorBinding);
    }
    Ok(configuration_hash)
}

fn resolve_child(
    selector: &ChildSelector,
    node: &ExecutableNode,
    variables: &ChildMessageVariableEnvironment,
) -> Result<SessionId, ChildMessageExecutionError> {
    match selector {
        ChildSelector::Exact { child_id } => child_id
            .parse()
            .map_err(|_| ChildMessageExecutionError::InvalidChildIdentity),
        ChildSelector::Variable { variable } => {
            let declaration = variables
                .declarations
                .get(variable)
                .ok_or_else(|| ChildMessageExecutionError::UndeclaredVariable(variable.clone()))?;
            if !node.read_variables.contains(variable) || !declaration.consumers.contains(&node.id)
            {
                return Err(ChildMessageExecutionError::UnauthorizedVariable(
                    variable.clone(),
                ));
            }
            if declaration.name != *variable || declaration.value_type != VariableValueType::ChildId
            {
                return Err(ChildMessageExecutionError::VariableTypeMismatch(
                    variable.clone(),
                ));
            }
            let value = variables
                .values
                .get(variable)
                .ok_or_else(|| ChildMessageExecutionError::MissingVariable(variable.clone()))?;
            let child_id = value
                .as_str()
                .ok_or_else(|| ChildMessageExecutionError::VariableTypeMismatch(variable.clone()))?
                .parse()
                .map_err(|_| ChildMessageExecutionError::InvalidChildIdentity)?;
            let value_bytes = serde_json::to_vec(value)
                .map_err(|_| ChildMessageExecutionError::VariableTypeMismatch(variable.clone()))?;
            if value_bytes.len() as u64 > declaration.max_size_bytes {
                return Err(ChildMessageExecutionError::VariableTypeMismatch(
                    variable.clone(),
                ));
            }
            Ok(child_id)
        }
    }
}

fn validate_parent_link(link: &ChildMessageParentLink) -> Result<(), ChildMessageExecutionError> {
    if link.parent_graph_node_id.trim().is_empty()
        || link.parent_graph_node_id.len() > MAX_IDENTITY_BYTES
        || link.task_id.trim().is_empty()
        || link.task_id.len() > MAX_IDENTITY_BYTES
    {
        return Err(ChildMessageExecutionError::InvalidParentLink);
    }
    Ok(())
}

fn validate_lifecycle_and_cancellation(
    lifecycle: SessionLifecycle,
    parent_cancellation_started: bool,
    child_cancellation_started: bool,
    policy: ChildMessageCancellation,
) -> Result<(), ChildMessageExecutionError> {
    if lifecycle != SessionLifecycle::Active {
        return Err(ChildMessageExecutionError::ChildNotRunning);
    }
    if policy == ChildMessageCancellation::Reject
        && (parent_cancellation_started || child_cancellation_started)
    {
        return Err(ChildMessageExecutionError::CancellationRejected);
    }
    Ok(())
}

fn bounded_payload_bytes(
    value: &Value,
    configured_max: u64,
    classification: SecurityClassification,
) -> Result<Vec<u8>, ChildMessageExecutionError> {
    if configured_max == 0
        || configured_max > MAX_MESSAGE_BYTES as u64
        || !validate_json_bounds(value, 0, &mut 0)
        || (classification == SecurityClassification::SecretReference
            && !validate_secret_reference_payload(value))
    {
        return Err(ChildMessageExecutionError::InvalidPayload);
    }
    let bytes =
        serde_json::to_vec(value).map_err(|_| ChildMessageExecutionError::InvalidPayload)?;
    let byte_count =
        u64::try_from(bytes.len()).map_err(|_| ChildMessageExecutionError::MessageTooLarge)?;
    if byte_count > configured_max || bytes.len() > MAX_MESSAGE_BYTES {
        return Err(ChildMessageExecutionError::MessageTooLarge);
    }
    Ok(bytes)
}

fn prepare_child_message_payload(
    configuration_hash: ContentHash,
    payload: &Value,
    declared_artifact_references: &BTreeSet<String>,
    resolved_artifacts: &BTreeMap<String, ArtifactReference>,
    security_classification: SecurityClassification,
    max_message_bytes: u64,
) -> Result<PreparedChildMessagePayload, ChildMessageExecutionError> {
    let payload = canonicalize_json(payload);
    let payload_bytes =
        bounded_payload_bytes(&payload, max_message_bytes, security_classification)?;
    let artifact_references = validate_artifacts(declared_artifact_references, resolved_artifacts)?;
    validate_child_message_information_flow(
        configuration_hash,
        &payload,
        &payload_bytes,
        declared_artifact_references,
        security_classification,
    )?;
    let artifact_bytes = serde_json::to_vec(&artifact_references)
        .map_err(|_| ChildMessageExecutionError::InvalidPayload)?;
    Ok(PreparedChildMessagePayload {
        payload,
        payload_hash: ContentHash::digest(&payload_bytes),
        message_bytes: ByteCount::new(
            u64::try_from(payload_bytes.len())
                .map_err(|_| ChildMessageExecutionError::MessageTooLarge)?,
        ),
        artifact_references,
        artifact_references_hash: ContentHash::digest(&artifact_bytes),
    })
}

fn validate_json_bounds(value: &Value, depth: usize, values: &mut usize) -> bool {
    if depth > MAX_PAYLOAD_DEPTH {
        return false;
    }
    *values = match values.checked_add(1) {
        Some(values) => values,
        None => return false,
    };
    if *values > MAX_PAYLOAD_VALUES {
        return false;
    }
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => true,
        Value::String(value) => value.len() <= MAX_STRING_BYTES,
        Value::Array(items) => {
            items.len() <= MAX_COLLECTION_ITEMS
                && items
                    .iter()
                    .all(|item| validate_json_bounds(item, depth + 1, values))
        }
        Value::Object(entries) => {
            entries.len() <= MAX_COLLECTION_ITEMS
                && entries.iter().all(|(key, value)| {
                    key.len() <= MAX_IDENTITY_BYTES
                        && validate_json_bounds(value, depth + 1, values)
                })
        }
    }
}

fn validate_secret_reference_payload(value: &Value) -> bool {
    match value {
        Value::String(reference) => is_exact_secret_reference(reference),
        Value::Array(values) => {
            !values.is_empty() && values.iter().all(validate_secret_reference_payload)
        }
        Value::Object(values) => {
            !values.is_empty() && values.values().all(validate_secret_reference_payload)
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn validate_child_message_information_flow(
    configuration_hash: ContentHash,
    payload: &Value,
    payload_bytes: &[u8],
    artifact_references: &BTreeSet<String>,
    declared: SecurityClassification,
) -> Result<(), ChildMessageExecutionError> {
    if declared != SecurityClassification::SecretReference
        && contains_exact_secret_reference(payload)
    {
        return Err(ChildMessageExecutionError::InvalidPayload);
    }
    if artifact_references
        .iter()
        .any(|reference| is_exact_secret_reference(reference))
    {
        return Err(ChildMessageExecutionError::InvalidArtifactReference);
    }
    let classification = information_flow_classification(declared);
    let dedicated_secret_reference = if declared == SecurityClassification::SecretReference {
        first_secret_reference(payload)
    } else {
        None
    };
    let mut sources = Vec::with_capacity(artifact_references.len() + 1);
    sources.push(
        InformationFlowSource::from_bytes(
            "payload",
            classification,
            payload_bytes,
            dedicated_secret_reference,
        )
        .map_err(|_| ChildMessageExecutionError::InvalidPayload)?,
    );
    for (index, reference) in artifact_references.iter().enumerate() {
        sources.push(
            InformationFlowSource::from_bytes(
                format!("artifact:{index}"),
                classification,
                reference.as_bytes(),
                None,
            )
            .map_err(|_| ChildMessageExecutionError::InvalidArtifactReference)?,
        );
    }
    let (_, decision) = evaluate_information_flow(
        format!("child-message:{}", configuration_hash.to_hex()),
        InformationFlowSink::ChildMessage,
        classification,
        &sources,
    )
    .map_err(|_| ChildMessageExecutionError::InvalidPayload)?;
    if matches!(decision, InformationFlowDecision::Allowed { .. }) {
        Ok(())
    } else {
        Err(ChildMessageExecutionError::InvalidPayload)
    }
}

fn contains_exact_secret_reference(value: &Value) -> bool {
    match value {
        Value::String(reference) => is_exact_secret_reference(reference),
        Value::Array(values) => values.iter().any(contains_exact_secret_reference),
        Value::Object(values) => values.values().any(contains_exact_secret_reference),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn first_secret_reference(value: &Value) -> Option<&str> {
    match value {
        Value::String(reference) => Some(reference),
        Value::Array(values) => values.iter().find_map(first_secret_reference),
        Value::Object(values) => values.values().find_map(first_secret_reference),
        Value::Null | Value::Bool(_) | Value::Number(_) => None,
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

fn validate_artifacts(
    artifacts: &BTreeSet<String>,
    resolved: &BTreeMap<String, ArtifactReference>,
) -> Result<Vec<ArtifactReference>, ChildMessageExecutionError> {
    if artifacts.len() > MAX_ARTIFACT_REFERENCES
        || resolved.len() != artifacts.len()
        || resolved.keys().ne(artifacts.iter())
        || artifacts.iter().any(|reference| {
            reference.trim() != reference
                || reference.is_empty()
                || reference.len() > MAX_ARTIFACT_REFERENCE_BYTES
                || reference.contains("..")
                || reference.starts_with('/')
                || reference.starts_with('\\')
                || !reference.chars().all(|character| {
                    character.is_ascii_alphanumeric() || "_.:/-".contains(character)
                })
        })
    {
        return Err(ChildMessageExecutionError::InvalidArtifactReference);
    }
    let resolved = artifacts
        .iter()
        .map(|reference| {
            resolved
                .get(reference)
                .cloned()
                .ok_or(ChildMessageExecutionError::InvalidArtifactReference)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let unique = resolved
        .iter()
        .map(|artifact| (artifact.id.clone(), artifact.content_hash))
        .collect::<BTreeSet<_>>();
    if unique.len() != resolved.len() {
        return Err(ChildMessageExecutionError::InvalidArtifactReference);
    }
    Ok(resolved)
}

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonicalize_json).collect()),
        Value::Object(values) => {
            let mut canonical = Map::new();
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            for (key, value) in entries {
                canonical.insert(key.clone(), canonicalize_json(value));
            }
            Value::Object(canonical)
        }
        scalar => scalar.clone(),
    }
}

struct PreparedChildMessagePayload {
    payload: Value,
    payload_hash: ContentHash,
    message_bytes: ByteCount,
    artifact_references: Vec<ArtifactReference>,
    artifact_references_hash: ContentHash,
}

#[derive(Serialize)]
struct ChildMessageIdentityMaterial {
    parent_session_id: SessionId,
    child_session_id: SessionId,
    work: NodeWorkIdentity,
    execution_plan_hash: ContentHash,
    configuration_hash: ContentHash,
    parent_link: ChildMessageParentLink,
    payload_hash: ContentHash,
    artifact_references_hash: ContentHash,
    security_classification: SecurityClassification,
    max_message_bytes: u64,
    cancellation: ChildMessageCancellation,
}

#[derive(Serialize)]
struct ChildMessageActionMaterial {
    action_kind: &'static str,
    message_identity: ContentHash,
    identity: ChildMessageIdentityMaterial,
}

/// Stable pure child-message validation or receipt failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ChildMessageExecutionError {
    /// Requested work does not equal the replay-owned active node work.
    #[error("child-message active node work or execution-plan binding differs")]
    ActiveWorkMismatch,
    /// Persisted executor or compiled-node binding is not the exact supported
    /// first-party implementation.
    #[error("child-message executor binding is unsupported or inconsistent")]
    UnsupportedExecutorBinding,
    /// Compiled node configuration is absent or has the wrong kind.
    #[error("child-message node configuration is invalid")]
    InvalidConfiguration,
    /// Exact or variable-selected child identity is invalid.
    #[error("child-message target identity is invalid")]
    InvalidChildIdentity,
    /// Selected child differs from the replay-owned target.
    #[error("child-message selected child differs from the target projection")]
    ChildIdentityMismatch,
    /// A configured variable has no declaration.
    #[error("child-message variable `{0}` is undeclared")]
    UndeclaredVariable(String),
    /// The compiled node is not authorized to read the configured variable.
    #[error("child-message variable `{0}` is not an authorized node input")]
    UnauthorizedVariable(String),
    /// Declared or live variable type is not a child identity.
    #[error("child-message variable `{0}` is not a child-id value")]
    VariableTypeMismatch(String),
    /// A declared child-ID variable has no live canonical value.
    #[error("child-message variable `{0}` has no live value")]
    MissingVariable(String),
    /// Supplied parent ownership fields are malformed.
    #[error("child-message parent ownership link is invalid")]
    InvalidParentLink,
    /// Target child is not owned by the exact supplied parent link.
    #[error("child-message target ownership differs from the expected parent link")]
    OwnershipMismatch,
    /// Target child is not canonically running.
    #[error("child-message target child is not running")]
    ChildNotRunning,
    /// Configured cancellation policy rejects this delivery.
    #[error("child-message delivery is rejected after cancellation began")]
    CancellationRejected,
    /// Typed payload is malformed, unbounded, or violates secret-reference
    /// representation.
    #[error("child-message typed payload is invalid")]
    InvalidPayload,
    /// Canonical payload exceeds its exact configured or runtime hard bound.
    #[error("child-message payload exceeds its configured bound")]
    MessageTooLarge,
    /// A declared artifact reference is malformed or the collection is too
    /// large.
    #[error("child-message artifact references are invalid")]
    InvalidArtifactReference,
    /// Child journal sequence arithmetic overflowed.
    #[error("child-message sequence overflow")]
    SequenceOverflow,
    /// Data receipt does not prove the exact requested append or replay.
    #[error("child-message append receipt does not match the exact dispatch")]
    ReceiptMismatch,
    /// Recovery attempted to bind changed content to a stable operation.
    #[error("child-message recovery operation conflicts with canonical content")]
    ConflictingOperation,
}

#[cfg(test)]
mod tests {
    use agentmod_event_model::ArtifactIdentifier;
    use agentmod_graph_engine::{VariableMutability, VariableScope};
    use uuid::Uuid;

    use super::*;

    fn session(value: u128) -> SessionId {
        SessionId::from_uuid(Uuid::from_u128(value))
    }

    fn sequence(value: u64) -> Sequence {
        Sequence::new(value).expect("positive sequence")
    }

    fn parent_link(parent: SessionId) -> ChildMessageParentLink {
        ChildMessageParentLink {
            parent_session_id: parent,
            parent_action_sequence: sequence(17),
            parent_graph_node_id: String::from("spawn-workers"),
            task_id: String::from("task-1"),
        }
    }

    fn configuration(child: ChildSelector) -> NodeConfiguration {
        NodeConfiguration::SendChildAgentMessage {
            child,
            payload: serde_json::json!({"kind":"instruction","body":"continue"}),
            artifact_references: BTreeSet::from([String::from("artifact:report")]),
            security_classification: SecurityClassification::Internal,
            max_message_bytes: 4_096,
            cancellation: ChildMessageCancellation::Reject,
        }
    }

    fn command(child_selector: ChildSelector) -> PrepareChildMessageCommand {
        let parent = session(1);
        let child = session(2);
        let compiled_node = ExecutableNode {
            index: 0,
            id: String::from("message-child"),
            kind: NodeKind::SendChildAgentMessage,
            configuration: Some(configuration(child_selector)),
            condition: None,
            tool: None,
            provider: None,
            required_capabilities: BTreeSet::new(),
            read_scopes: BTreeSet::new(),
            write_scopes: BTreeSet::new(),
            read_variables: BTreeSet::new(),
            write_variables: BTreeSet::new(),
            retry_limit: 0,
            max_iterations: None,
        };
        let configuration_hash =
            ContentHash::digest(&serde_json::to_vec(&compiled_node).expect("node"));
        let work = NodeWorkIdentity {
            run_id: String::from("run-1"),
            node_id: compiled_node.id.clone(),
            branch_path: vec![String::from("branch-a")],
            attempt: 1,
            loop_iteration: 0,
            step: 3,
        };
        let execution_plan_hash = ContentHash::digest(b"plan");
        PrepareChildMessageCommand {
            parent_session_id: parent,
            work: work.clone(),
            active: ActiveChildMessageNodeBinding {
                work,
                execution_plan_hash,
            },
            execution_plan_hash,
            executor: SessionNodeExecutorResolution {
                node_id: compiled_node.id.clone(),
                node_kind: String::from(CHILD_MESSAGE_NODE_KIND),
                executor_id: String::from(CHILD_MESSAGE_EXECUTOR_ID),
                executor_version: String::from(CHILD_MESSAGE_EXECUTOR_VERSION),
                source: SessionNodeExecutorSource::Runtime,
                boundary: SessionNodeExecutorBoundary::RuntimeLogic,
                required_capabilities: vec![],
                resolved_capabilities: vec![String::from("child.message")],
                runtime_api_requirement: String::from("^1.0"),
                executor_declaration_hash: ContentHash::digest(b"runtime.child-message@1.0.0"),
                adapter_configuration_reference: configuration_hash,
            },
            compiled_node,
            variables: ChildMessageVariableEnvironment {
                declarations: BTreeMap::new(),
                values: BTreeMap::new(),
            },
            resolved_artifacts: BTreeMap::from([(
                String::from("artifact:report"),
                ArtifactReference {
                    id: ArtifactIdentifier::parse("00000000-0000-0000-0000-00000000000a")
                        .expect("artifact identifier"),
                    content_hash: ContentHash::digest(b"report"),
                },
            )]),
            expected_parent_link: parent_link(parent),
            target: ChildMessageTarget {
                child_session_id: child,
                lifecycle: SessionLifecycle::Active,
                cancellation_started: false,
                parent_link: parent_link(parent),
            },
            parent_cancellation_started: false,
        }
    }

    fn child_variable_declaration() -> VariableDeclaration {
        VariableDeclaration {
            name: String::from("worker"),
            value_type: VariableValueType::ChildId,
            scope: VariableScope::Run,
            producer: String::from("spawn-workers"),
            merge_contributors: BTreeSet::new(),
            consumers: BTreeSet::from([String::from("message-child")]),
            mutability: VariableMutability::Immutable,
            merge_policy: None,
            max_size_bytes: 64,
            security_classification: SecurityClassification::Internal,
        }
    }

    fn dispatch(proposal: &ChildMessageProposal) -> ChildMessageDispatchSpecification {
        bind_child_message_dispatch(
            proposal,
            ChildMessageJournalHeadData {
                sequence: sequence(2),
                checksum: ContentHash::digest(b"head"),
            },
            EventId::from_uuid(Uuid::from_u128(3)),
            ContentHash::digest(b"envelope"),
        )
        .expect("dispatch")
    }

    fn receipt(
        dispatch: &ChildMessageDispatchSpecification,
        replayed: bool,
    ) -> AppendedChildMessageDataRecord {
        AppendedChildMessageDataRecord {
            replayed,
            sequence: dispatch.message_sequence,
            message_id: dispatch.message_id,
            envelope_checksum: dispatch.envelope_checksum,
            journal_checksum: ContentHash::digest(b"journal"),
            previous_journal_checksum: Some(dispatch.expected_head.checksum),
            offset: ByteCount::new(100),
            journal_bytes: ByteCount::new(200),
        }
    }

    #[test]
    fn exact_child_produces_a_bounded_non_event_proposal() {
        let proposal = prepare_child_message(&command(ChildSelector::Exact {
            child_id: session(2).to_string(),
        }))
        .expect("proposal");
        assert_eq!(proposal.child_session_id, session(2));
        assert_eq!(proposal.action_kind, CHILD_MESSAGE_ACTION_KIND);
        assert_eq!(
            proposal.payload_hash,
            ContentHash::digest(&serde_json::to_vec(&proposal.payload).expect("payload"),)
        );
        assert_eq!(proposal.artifact_references.len(), 1);
    }

    #[test]
    fn declared_typed_child_variable_resolves_exact_target() {
        let mut command = command(ChildSelector::Variable {
            variable: String::from("worker"),
        });
        command
            .compiled_node
            .read_variables
            .insert(String::from("worker"));
        command
            .variables
            .declarations
            .insert(String::from("worker"), child_variable_declaration());
        command.variables.values.insert(
            String::from("worker"),
            Value::String(session(2).to_string()),
        );
        command.executor.adapter_configuration_reference =
            ContentHash::digest(&serde_json::to_vec(&command.compiled_node).expect("node"));
        assert_eq!(
            prepare_child_message(&command)
                .expect("variable proposal")
                .child_session_id,
            session(2)
        );
    }

    #[test]
    fn variable_must_be_declared_authorized_typed_and_present() {
        let base = command(ChildSelector::Variable {
            variable: String::from("worker"),
        });
        assert_eq!(
            prepare_child_message(&base),
            Err(ChildMessageExecutionError::UndeclaredVariable(
                String::from("worker")
            ))
        );

        let mut unauthorized = base.clone();
        unauthorized
            .variables
            .declarations
            .insert(String::from("worker"), child_variable_declaration());
        assert_eq!(
            prepare_child_message(&unauthorized),
            Err(ChildMessageExecutionError::UnauthorizedVariable(
                String::from("worker")
            ))
        );

        let mut mistyped = unauthorized.clone();
        mistyped
            .compiled_node
            .read_variables
            .insert(String::from("worker"));
        mistyped
            .variables
            .declarations
            .get_mut("worker")
            .expect("declaration")
            .value_type = VariableValueType::String;
        mistyped.executor.adapter_configuration_reference =
            ContentHash::digest(&serde_json::to_vec(&mistyped.compiled_node).expect("node"));
        assert_eq!(
            prepare_child_message(&mistyped),
            Err(ChildMessageExecutionError::VariableTypeMismatch(
                String::from("worker")
            ))
        );

        let mut missing = unauthorized;
        missing
            .compiled_node
            .read_variables
            .insert(String::from("worker"));
        missing.executor.adapter_configuration_reference =
            ContentHash::digest(&serde_json::to_vec(&missing.compiled_node).expect("node"));
        assert_eq!(
            prepare_child_message(&missing),
            Err(ChildMessageExecutionError::MissingVariable(String::from(
                "worker"
            )))
        );
    }

    #[test]
    fn exact_executor_active_work_plan_and_ownership_are_required() {
        let base = command(ChildSelector::Exact {
            child_id: session(2).to_string(),
        });
        let mut changed_work = base.clone();
        changed_work.work.attempt = 2;
        assert_eq!(
            prepare_child_message(&changed_work),
            Err(ChildMessageExecutionError::ActiveWorkMismatch)
        );

        let mut changed_executor = base.clone();
        changed_executor.executor.executor_version = String::from("1.1.0");
        assert_eq!(
            prepare_child_message(&changed_executor),
            Err(ChildMessageExecutionError::UnsupportedExecutorBinding)
        );

        let mut changed_owner = base;
        changed_owner.target.parent_link.task_id = String::from("task-2");
        assert_eq!(
            prepare_child_message(&changed_owner),
            Err(ChildMessageExecutionError::OwnershipMismatch)
        );
    }

    #[test]
    fn terminal_children_and_reject_cancellation_fail_closed() {
        for lifecycle in [
            SessionLifecycle::Suspended,
            SessionLifecycle::Completed,
            SessionLifecycle::Failed,
            SessionLifecycle::Cancelled,
            SessionLifecycle::Archived,
        ] {
            let mut command = command(ChildSelector::Exact {
                child_id: session(2).to_string(),
            });
            command.target.lifecycle = lifecycle;
            assert_eq!(
                prepare_child_message(&command),
                Err(ChildMessageExecutionError::ChildNotRunning)
            );
        }

        let mut command = command(ChildSelector::Exact {
            child_id: session(2).to_string(),
        });
        command.parent_cancellation_started = true;
        assert_eq!(
            prepare_child_message(&command),
            Err(ChildMessageExecutionError::CancellationRejected)
        );
        let NodeConfiguration::SendChildAgentMessage { cancellation, .. } = command
            .compiled_node
            .configuration
            .as_mut()
            .expect("configuration")
        else {
            panic!("child message configuration");
        };
        *cancellation = ChildMessageCancellation::DeliverIfRunning;
        command.executor.adapter_configuration_reference =
            ContentHash::digest(&serde_json::to_vec(&command.compiled_node).expect("node"));
        assert!(prepare_child_message(&command).is_ok());
    }

    #[test]
    fn payload_depth_size_secret_and_artifact_bounds_are_enforced() {
        let base = command(ChildSelector::Exact {
            child_id: session(2).to_string(),
        });
        let mut oversized = base.clone();
        let NodeConfiguration::SendChildAgentMessage {
            payload,
            max_message_bytes,
            ..
        } = oversized
            .compiled_node
            .configuration
            .as_mut()
            .expect("configuration")
        else {
            panic!("child message configuration");
        };
        *payload = Value::String(String::from("too large"));
        *max_message_bytes = 2;
        oversized.executor.adapter_configuration_reference =
            ContentHash::digest(&serde_json::to_vec(&oversized.compiled_node).expect("node"));
        assert_eq!(
            prepare_child_message(&oversized),
            Err(ChildMessageExecutionError::MessageTooLarge)
        );

        let mut deep = base.clone();
        let mut payload = Value::String(String::from("leaf"));
        for _ in 0..=MAX_PAYLOAD_DEPTH {
            payload = Value::Array(vec![payload]);
        }
        let NodeConfiguration::SendChildAgentMessage {
            payload: configured,
            max_message_bytes,
            ..
        } = deep
            .compiled_node
            .configuration
            .as_mut()
            .expect("configuration")
        else {
            panic!("child message configuration");
        };
        *configured = payload;
        *max_message_bytes = MAX_MESSAGE_BYTES as u64;
        deep.executor.adapter_configuration_reference =
            ContentHash::digest(&serde_json::to_vec(&deep.compiled_node).expect("node"));
        assert_eq!(
            prepare_child_message(&deep),
            Err(ChildMessageExecutionError::InvalidPayload)
        );

        let mut inline_secret = base.clone();
        let NodeConfiguration::SendChildAgentMessage {
            payload,
            security_classification,
            ..
        } = inline_secret
            .compiled_node
            .configuration
            .as_mut()
            .expect("configuration")
        else {
            panic!("child message configuration");
        };
        *payload = serde_json::json!({"credential":"not-a-reference"});
        *security_classification = SecurityClassification::SecretReference;
        inline_secret.executor.adapter_configuration_reference =
            ContentHash::digest(&serde_json::to_vec(&inline_secret.compiled_node).expect("node"));
        assert_eq!(
            prepare_child_message(&inline_secret),
            Err(ChildMessageExecutionError::InvalidPayload)
        );

        let mut artifacts = base;
        let NodeConfiguration::SendChildAgentMessage {
            artifact_references,
            ..
        } = artifacts
            .compiled_node
            .configuration
            .as_mut()
            .expect("configuration")
        else {
            panic!("child message configuration");
        };
        artifact_references.insert(String::from("../escape"));
        artifacts.executor.adapter_configuration_reference =
            ContentHash::digest(&serde_json::to_vec(&artifacts.compiled_node).expect("node"));
        assert_eq!(
            prepare_child_message(&artifacts),
            Err(ChildMessageExecutionError::InvalidArtifactReference)
        );
    }

    #[test]
    fn child_message_flow_accepts_only_exact_dedicated_secret_references() {
        let exact_reference = "secret-ref:vault_record_17";
        let mut exact = command(ChildSelector::Exact {
            child_id: session(2).to_string(),
        });
        let NodeConfiguration::SendChildAgentMessage {
            payload,
            security_classification,
            ..
        } = exact
            .compiled_node
            .configuration
            .as_mut()
            .expect("configuration")
        else {
            panic!("child message configuration")
        };
        *payload = Value::String(String::from(exact_reference));
        *security_classification = SecurityClassification::SecretReference;
        exact.executor.adapter_configuration_reference =
            ContentHash::digest(&serde_json::to_vec(&exact.compiled_node).expect("node"));
        assert!(prepare_child_message(&exact).is_ok());

        let mut near_miss = exact.clone();
        let NodeConfiguration::SendChildAgentMessage { payload, .. } = near_miss
            .compiled_node
            .configuration
            .as_mut()
            .expect("configuration")
        else {
            panic!("child message configuration")
        };
        *payload = Value::String(String::from("secret:vault_record_17"));
        near_miss.executor.adapter_configuration_reference =
            ContentHash::digest(&serde_json::to_vec(&near_miss.compiled_node).expect("node"));
        assert_eq!(
            prepare_child_message(&near_miss),
            Err(ChildMessageExecutionError::InvalidPayload)
        );

        let mut ordinary = exact;
        let NodeConfiguration::SendChildAgentMessage {
            security_classification,
            ..
        } = ordinary
            .compiled_node
            .configuration
            .as_mut()
            .expect("configuration")
        else {
            panic!("child message configuration")
        };
        *security_classification = SecurityClassification::Internal;
        ordinary.executor.adapter_configuration_reference =
            ContentHash::digest(&serde_json::to_vec(&ordinary.compiled_node).expect("node"));
        assert_eq!(
            prepare_child_message(&ordinary),
            Err(ChildMessageExecutionError::InvalidPayload)
        );

        let mut artifact = command(ChildSelector::Exact {
            child_id: session(2).to_string(),
        });
        let NodeConfiguration::SendChildAgentMessage {
            artifact_references,
            ..
        } = artifact
            .compiled_node
            .configuration
            .as_mut()
            .expect("configuration")
        else {
            panic!("child message configuration")
        };
        *artifact_references = BTreeSet::from([String::from(exact_reference)]);
        artifact.resolved_artifacts = BTreeMap::from([(
            String::from(exact_reference),
            ArtifactReference {
                id: ArtifactIdentifier::parse("00000000-0000-0000-0000-00000000000b")
                    .expect("artifact identifier"),
                content_hash: ContentHash::digest(b"secret reference masquerading as artifact"),
            },
        )]);
        artifact.executor.adapter_configuration_reference =
            ContentHash::digest(&serde_json::to_vec(&artifact.compiled_node).expect("node"));
        assert_eq!(
            prepare_child_message(&artifact),
            Err(ChildMessageExecutionError::InvalidArtifactReference)
        );
    }

    #[test]
    fn identity_is_deterministic_and_changed_payload_conflicts() {
        let command = command(ChildSelector::Exact {
            child_id: session(2).to_string(),
        });
        let first = prepare_child_message(&command).expect("first");
        let second = prepare_child_message(&command).expect("second");
        assert_eq!(first, second);

        let mut changed = command;
        let NodeConfiguration::SendChildAgentMessage { payload, .. } = changed
            .compiled_node
            .configuration
            .as_mut()
            .expect("configuration")
        else {
            panic!("child message configuration");
        };
        *payload = serde_json::json!({"kind":"instruction","body":"different"});
        changed.executor.adapter_configuration_reference =
            ContentHash::digest(&serde_json::to_vec(&changed.compiled_node).expect("node"));
        let changed = prepare_child_message(&changed).expect("changed");
        assert_ne!(first.message_identity, changed.message_identity);
        assert_eq!(
            validate_recovered_child_message(&first, &changed),
            Err(ChildMessageExecutionError::ConflictingOperation)
        );
    }

    #[test]
    fn exact_fresh_and_replayed_receipts_produce_parent_specs() {
        let proposal = prepare_child_message(&command(ChildSelector::Exact {
            child_id: session(2).to_string(),
        }))
        .expect("proposal");
        let dispatch = dispatch(&proposal);
        assert_eq!(dispatch.event_type, CHILD_MESSAGE_EVENT_TYPE);
        assert_eq!(dispatch.child_payload.payload_hash, proposal.payload_hash);

        let fresh =
            validate_child_message_receipt(&dispatch, &receipt(&dispatch, false)).expect("fresh");
        let replayed =
            validate_child_message_receipt(&dispatch, &receipt(&dispatch, true)).expect("replayed");
        assert!(matches!(fresh, ChildMessageReceiptDisposition::Fresh(_)));
        assert!(matches!(
            replayed,
            ChildMessageReceiptDisposition::Replayed(_)
        ));
    }

    #[test]
    fn wrong_receipt_is_rejected_and_ambiguous_storage_never_redispatches() {
        let proposal = prepare_child_message(&command(ChildSelector::Exact {
            child_id: session(2).to_string(),
        }))
        .expect("proposal");
        let dispatch = dispatch(&proposal);
        let mut wrong = receipt(&dispatch, false);
        wrong.envelope_checksum = ContentHash::digest(b"other-envelope");
        assert_eq!(
            validate_child_message_receipt(&dispatch, &wrong),
            Err(ChildMessageExecutionError::ReceiptMismatch)
        );

        assert_eq!(
            classify_child_message_failure(&ChildMessageDataError::Dependency {
                category: ChildMessageDependencyFailure::Access,
                message: String::from("redacted"),
            }),
            ChildMessageFailureDisposition::AmbiguousNoRedispatch
        );
        assert_eq!(
            classify_child_message_failure(&ChildMessageDataError::Dependency {
                category: ChildMessageDependencyFailure::ConflictingDuplicate,
                message: String::from("redacted"),
            }),
            ChildMessageFailureDisposition::Conflict
        );
        assert_eq!(
            classify_child_message_failure(&ChildMessageDataError::InvalidRequest),
            ChildMessageFailureDisposition::Rejected
        );
    }

    #[test]
    fn sequence_overflow_is_stable() {
        let proposal = prepare_child_message(&command(ChildSelector::Exact {
            child_id: session(2).to_string(),
        }))
        .expect("proposal");
        assert_eq!(
            bind_child_message_dispatch(
                &proposal,
                ChildMessageJournalHeadData {
                    sequence: Sequence::new(u64::MAX).expect("max sequence"),
                    checksum: ContentHash::digest(b"head"),
                },
                EventId::from_uuid(Uuid::from_u128(3)),
                ContentHash::digest(b"envelope"),
            ),
            Err(ChildMessageExecutionError::SequenceOverflow)
        );
    }
}
