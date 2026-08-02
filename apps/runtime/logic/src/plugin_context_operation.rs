//! Replay-safe coordination for plugin-provided memory retrieval and context
//! compaction.
//!
//! This module deliberately does not append journal events. It returns one
//! reducer-valid canonical event at a time. The caller must append and replay
//! that event before asking for the next transition. In particular, an
//! unforgeable dispatch ticket is returned with the dispatch-intent event and
//! may be consumed only against replay state containing that exact committed
//! dispatch.

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
};

use agentmod_event_model::{
    EventClassification, EventEnvelope, EventMetadata, EventOrigin, EventScope,
};
use agentmod_primitives::{
    ArtifactId, CausationId, ContentHash, CorrelationId, EventId, Sequence, SessionId,
    TimestampMillis, Version,
};
use agentmod_runtime_data::{
    plugin::{
        CompactPluginContextDataRequest, PluginArtifactReferenceDataRecord,
        PluginCanonicalReferenceDataRecord, PluginCanonicalReferenceKindData,
        PluginCompactionInputDataRecord, PluginCompactionProposalDataRecord,
        PluginCompactorDataRecord, PluginDataError, PluginDataPort,
        PluginMemoryItemProposalDataRecord, PluginMemoryProviderDataRecord,
        PluginMemoryRetrieveInputDataRecord, PluginMemoryRetrieveProposalDataRecord,
        PluginMemoryScopeData, PluginOperationBindingDataRecord, PluginSecurityClassificationData,
        RetrievePluginMemoryDataRequest,
    },
    plugin_receipt::{
        PluginInvocationReceiptDataIdentity, PluginNodeReceiptDataError, PluginNodeReceiptDataPort,
        StorePluginInvocationReceiptDataRequest,
    },
};
use async_trait::async_trait;
use semver::{Version as SemanticVersion, VersionReq};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use crate::{
    action::ActionProposal,
    conversation::{
        ConversationEntry, ConversationEntryId, ProjectionProvenance,
        RetrievedMemoryArtifactProvenance, RetrievedMemoryEntry,
        RetrievedMemoryReferenceProvenance, RetrievedMemoryTypedProvenance,
    },
    projection::{canonical_json_bytes, measure_projection, project},
    session::{
        ContextPhaseIdentity, PluginContextOperationAmbiguousEvent,
        PluginContextOperationApplicationApprovedEvent, PluginContextOperationAppliedEvent,
        PluginContextOperationAuthorizedEvent, PluginContextOperationCompletedEvent,
        PluginContextOperationDispatchedEvent, PluginContextOperationFailedEvent,
        PluginContextOperationIdentity, PluginContextOperationKind, PluginContextOperationProposal,
        PluginContextOperationProposedEvent, PluginContextOperationRecovery,
        PluginContextOperationRequest, PluginContextOperationState, RuntimeCommittedEvent,
        SealedPluginContextReceipt, SessionReducerError, SessionState,
        classify_plugin_context_operation_recovery, context_replacement_action_proposal,
        plugin_context_operation_action_proposal, plugin_context_operation_application_hash,
        plugin_context_operation_authorization_digest, plugin_context_operation_identity,
        plugin_context_operation_identity_with_implementation,
        plugin_context_operation_proposal_hash, plugin_context_operation_replacement_hash, reduce,
    },
};

const MAX_OPERATION_BYTES: usize = 512 * 1024;
const MAX_RESOURCE_COUNT: usize = 256;
const MAX_IDENTIFIER_BYTES: usize = 512;
const MAX_METADATA_ITEMS: usize = 64;
const MAX_METADATA_BYTES: usize = 8 * 1024;
const MAX_MEMORY_ITEM_ID_BYTES: usize = 80;
const ZERO_HASH: ContentHash = ContentHash::from_bytes([0; 32]);
/// Runtime-owned schema identifier required for plugin-context readable state.
pub const PLUGIN_CONTEXT_READABLE_STATE_SCHEMA_V1: &str =
    "agentmod.plugin_context.readable_state.v1";
const READABLE_STATE_FIELDS: &[&str] = &[
    "approval_result_references",
    "canonical_counters",
    "canonical_variables",
    "context_metadata",
    "node_result_references",
    "recorded_runtime_values",
    "schema",
];

#[derive(Debug, Default)]
struct DispatchGateState {
    claimed: bool,
}

type DispatchGate = AsyncMutex<DispatchGateState>;
type DispatchGateKey = (SessionId, String);
type DispatchGateMap = BTreeMap<DispatchGateKey, Arc<DispatchGate>>;

async fn release_dispatch_gate(
    gates: &AsyncMutex<DispatchGateMap>,
    key: &DispatchGateKey,
    gate: &Arc<DispatchGate>,
) {
    let mut gates = gates.lock().await;
    if gates
        .get(key)
        .is_some_and(|registered| Arc::ptr_eq(registered, gate))
    {
        gates.remove(key);
    }
}

/// Logic-owned runtime security classification for plugin-visible resources.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginContextSecurityClassification {
    /// Public information.
    Public,
    /// Runtime-internal information.
    Internal,
    /// User-private information.
    Private,
    /// Confidential information.
    Confidential,
}

/// Logic-owned canonical reference kind visible to a plugin operation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginContextReferenceKind {
    /// Immutable artifact.
    Artifact,
    /// Canonical node result.
    NodeResult,
    /// Canonical tool result.
    ToolResult,
    /// Canonical approval result.
    ApprovalResult,
    /// Durable continuation.
    Continuation,
    /// Runtime-managed child session.
    ChildSession,
}

/// One exact runtime-owned reference made readable to the plugin.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PluginContextReference {
    /// Reference kind.
    pub kind: PluginContextReferenceKind,
    /// Opaque canonical identity.
    pub id: String,
    /// Exact immutable content hash.
    pub content_hash: ContentHash,
}

/// One exact immutable artifact made readable to the plugin.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PluginContextArtifact {
    /// Runtime-owned artifact UUID.
    pub artifact_id: String,
    /// Portable artifact-store reference.
    pub artifact_reference: String,
    /// Hash of exact artifact bytes.
    pub content_hash: ContentHash,
    /// Stable media type.
    pub media_type: String,
    /// Exact artifact byte count.
    pub size_bytes: u64,
    /// Runtime-owned security classification.
    pub security_classification: PluginContextSecurityClassification,
}

/// Exact bounded auxiliary inputs bound into the immutable invocation.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PluginContextOperationInputs {
    /// Explicit state projection readable by the selected plugin.
    pub readable_state: Value,
    /// Exact immutable artifacts exposed by runtime policy.
    pub artifacts: Vec<PluginContextArtifact>,
    /// Exact canonical references exposed by runtime policy.
    pub references: Vec<PluginContextReference>,
    /// Schema-validated implementation configuration parameters.
    pub parameters: Value,
    /// Hard serialized replacement limit for compaction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_replacement_bytes: Option<u64>,
}

/// Exact runtime-policy authorization for the invocation itself.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PluginContextOperationAuthorization {
    /// Digest of the exact consequential action.
    pub action_digest: ContentHash,
    /// Digest of the keyed-grant authorization decision.
    pub authorization_digest: ContentHash,
}

/// Exact runtime-policy authorization for applying a completed proposal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PluginContextApplicationAuthorization {
    /// Digest of the exact provider-projection replacement action.
    pub action_digest: ContentHash,
}

/// One live or recovering plugin memory-retrieve/compaction drive.
#[derive(Clone, Debug, PartialEq)]
pub struct DrivePluginContextOperationCommand {
    /// Pure canonical session projection reconstructed by replay.
    pub state: SessionState,
    /// Owning recoverable context phase.
    pub phase: ContextPhaseIdentity,
    /// Complete runtime-owned request.
    pub request: PluginContextOperationRequest,
    /// Auxiliary inputs whose hash is persisted in the proposed event.
    pub inputs: PluginContextOperationInputs,
    /// Runtime-owned cancellation identity for a fresh dispatch.
    pub cancellation_id: String,
    /// Invocation policy evidence, when policy has approved.
    pub authorization: Option<PluginContextOperationAuthorization>,
    /// Application policy evidence, when replacement policy has approved.
    pub application_authorization: Option<PluginContextApplicationAuthorization>,
    /// Event identity reserved before a memory retrieval crosses the plugin
    /// boundary. It becomes the exact terminal-completion event identity and
    /// therefore cannot be displaced by later approval continuations.
    pub reserved_completion_event_id: Option<EventId>,
    /// Optional exact receipt already loaded by the caller. Production drive
    /// still verifies it against the durable receipt store.
    pub terminal_receipt: Option<PluginContextOperationTerminalReceipt>,
}

/// Terminal replay state that requires no live plugin query.
#[derive(Clone, Debug, PartialEq)]
pub enum PluginContextOperationTerminalState {
    /// The validated proposal is already canonical.
    Applied {
        /// Exact immutable invocation.
        identity: PluginContextOperationIdentity,
        /// Exact applied proposal.
        proposal: PluginContextOperationProposal,
    },
    /// A definite terminal failure is already canonical.
    Failed {
        /// Exact immutable invocation.
        identity: PluginContextOperationIdentity,
        /// Stable failure code.
        code: String,
    },
    /// Ambiguous execution is already canonical and cannot be retried.
    Ambiguous {
        /// Exact immutable invocation.
        identity: PluginContextOperationIdentity,
        /// Stable ambiguity code.
        code: String,
    },
}

/// Result of one coordinator drive. At most one canonical event is returned.
#[allow(
    clippy::large_enum_variant,
    reason = "the private single-use ticket deliberately owns the complete immutable bounded invocation"
)]
#[derive(Debug, PartialEq)]
pub enum DrivePluginContextOperationResult {
    /// Caller must append and replay this event before driving again.
    Emit {
        /// Exact runtime-owned canonical event payload.
        event: RuntimeCommittedEvent,
        /// Required event identity for a completed memory proposal. Other
        /// transitions may use the ordinary runtime allocator.
        required_event_id: Option<EventId>,
    },
    /// Invocation policy must evaluate this exact proposal.
    AwaitAuthorization {
        /// Exact consequential action proposal.
        proposal: ActionProposal,
    },
    /// Caller must first append the dispatch event, then consume the private
    /// ticket against the replayed Dispatched state.
    Dispatch {
        /// Exact durable dispatch intent.
        event: RuntimeCommittedEvent,
        /// Unforgeable in-process ticket for the single external call.
        ticket: PluginContextOperationDispatchTicket,
    },
    /// Replacement policy must evaluate this exact proposal.
    AwaitApplicationAuthorization {
        /// Exact provider-projection replacement proposal.
        proposal: ActionProposal,
    },
    /// No further transition or live query is legal.
    Terminal(PluginContextOperationTerminalState),
}

/// In-process capability for one exact authorized dispatch.
///
/// All fields are private and the type is not serializable. A ticket can only
/// be created by [`PluginContextOperationCoordinatorPort::drive`] from a
/// reducer-owned Authorized record.
#[derive(Debug, PartialEq)]
pub struct PluginContextOperationDispatchTicket {
    identity: PluginContextOperationIdentity,
    request: PluginContextOperationRequest,
    inputs: PluginContextOperationInputs,
    action_digest: ContentHash,
    cancellation_id: String,
    reserved_completion_event_id: Option<EventId>,
    declaration: PluginContextOperationDeclaration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PluginContextOperationDeclaration {
    Memory(PluginMemoryProviderDataRecord),
    Compaction(PluginCompactorDataRecord),
}

/// Durable logic-owned terminal receipt content.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PluginContextOperationTerminalReceipt {
    identity: PluginContextOperationIdentity,
    outcome: PluginContextOperationTerminalOutcome,
    receipt_hash: ContentHash,
    receipt_reference: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
enum PluginContextOperationTerminalOutcome {
    Completed {
        proposal: PluginContextOperationProposal,
        proposal_hash: ContentHash,
    },
    Failed {
        code: String,
    },
    Ambiguous {
        code: String,
    },
}

impl PluginContextOperationTerminalReceipt {
    /// Returns exact pure-recovery evidence for the session classifier.
    #[must_use]
    pub fn evidence(&self) -> crate::session::PluginContextOperationReceiptEvidence {
        crate::session::PluginContextOperationReceiptEvidence {
            invocation_id: self.identity.invocation_id.clone(),
            invocation_digest: self.identity.invocation_digest,
            terminal_receipt: self.sealed_reference(),
        }
    }

    /// Returns the complete immutable invocation identity.
    #[must_use]
    pub const fn identity(&self) -> &PluginContextOperationIdentity {
        &self.identity
    }

    fn sealed_reference(&self) -> SealedPluginContextReceipt {
        SealedPluginContextReceipt {
            receipt_hash: self.receipt_hash,
            receipt_reference: self.receipt_reference.clone(),
        }
    }
}

/// Logic-owned memory/compaction coordinator seam.
#[async_trait]
pub trait PluginContextOperationCoordinatorPort: Send + Sync {
    /// Plans the next replay-safe transition without committing it.
    ///
    /// # Errors
    ///
    /// Fails closed for identity substitution, invalid inputs, corrupt receipt
    /// evidence, or an impossible replay prefix.
    fn drive(
        &self,
        command: DrivePluginContextOperationCommand,
    ) -> Result<DrivePluginContextOperationResult, PluginContextOperationError>;

    /// Crosses the plugin-host boundary once, but only after the caller has
    /// committed and replayed the exact dispatch event paired with `ticket`.
    ///
    /// The returned receipt has already been atomically stored. If receipt
    /// storage fails after invocation, the method returns an ambiguity error;
    /// replay sees Dispatched without a receipt and permanently fails closed.
    ///
    /// # Errors
    ///
    /// Fails for a mismatched replay head, receipt conflict, or the
    /// post-invocation receipt-storage ambiguity.
    async fn dispatch(
        &self,
        dispatched_state: &SessionState,
        ticket: PluginContextOperationDispatchTicket,
    ) -> Result<PluginContextOperationTerminalReceipt, PluginContextOperationError>;

    /// Loads and validates one exact durable terminal receipt.
    ///
    /// # Errors
    ///
    /// Fails for corrupt, substituted, or unavailable receipt storage.
    fn load_terminal_receipt(
        &self,
        session_id: SessionId,
        invocation_id: &str,
    ) -> Result<Option<PluginContextOperationTerminalReceipt>, PluginContextOperationError>;
}

/// Production runtime-logic coordinator over runtime data ports.
///
/// The composition root must construct this coordinator once and distribute
/// clones. The clone family shares the in-process one-shot dispatch registry.
/// Independently constructed coordinators require durable receipt-store claim
/// CAS before they may dispatch against the same session.
#[derive(Clone, Debug)]
pub struct ProductionPluginContextOperationCoordinator<D> {
    data: D,
    dispatch_gates: Arc<AsyncMutex<DispatchGateMap>>,
}

impl<D> ProductionPluginContextOperationCoordinator<D> {
    /// Creates the coordinator.
    #[must_use]
    pub fn new(data: D) -> Self {
        Self {
            data,
            dispatch_gates: Arc::new(AsyncMutex::new(BTreeMap::new())),
        }
    }
}

#[async_trait]
impl<D> PluginContextOperationCoordinatorPort for ProductionPluginContextOperationCoordinator<D>
where
    D: Send + Sync + PluginDataPort + PluginNodeReceiptDataPort,
{
    #[allow(
        clippy::too_many_lines,
        reason = "the exhaustive replay classification is intentionally adjacent to each exact canonical transition"
    )]
    fn drive(
        &self,
        command: DrivePluginContextOperationCommand,
    ) -> Result<DrivePluginContextOperationResult, PluginContextOperationError> {
        validate_drive_command(&command)?;
        let persisted_inputs = serde_json::to_value(&command.inputs)
            .map_err(|_| PluginContextOperationError::Serialization)?;
        validate_bounded_json(&persisted_inputs, SchemaUse::Input)?;
        let existing = command
            .state
            .plugin_context_operations
            .values()
            .find(|record| record.identity.phase == command.phase)
            .cloned();
        let identity = if let Some(record) = existing.as_ref() {
            if record.request != command.request
                || record.readable_state != persisted_inputs
                || record.identity.kind != request_kind(&command.request)
            {
                return Err(PluginContextOperationError::InvalidCommand);
            }
            record.identity.clone()
        } else {
            let provisional = plugin_context_operation_identity(
                &command.state,
                command.phase.clone(),
                &command.request,
                &persisted_inputs,
            )?;
            let declaration = self
                .exact_declaration(&command.state, &provisional, false)
                .map_err(|_| PluginContextOperationError::DeclarationDrift)?;
            let (handler, timeout_ms, idempotent) = match &declaration {
                PluginContextOperationDeclaration::Memory(declaration) => (
                    declaration.retrieve.handler.as_str(),
                    declaration.retrieve.timeout_ms,
                    declaration.retrieve.idempotent,
                ),
                PluginContextOperationDeclaration::Compaction(declaration) => (
                    declaration.handler.as_str(),
                    declaration.timeout_ms,
                    declaration.idempotent,
                ),
            };
            plugin_context_operation_identity_with_implementation(
                &command.state,
                command.phase.clone(),
                &command.request,
                &persisted_inputs,
                handler,
                timeout_ms,
                idempotent,
            )?
        };

        if command
            .state
            .plugin_context_operations
            .values()
            .any(|record| {
                record.identity.phase == command.phase
                    && record.identity.invocation_id != identity.invocation_id
            })
        {
            return Err(PluginContextOperationError::IdentitySubstitution);
        }

        let supplied_receipt = command.terminal_receipt.as_ref();
        if existing
            .as_ref()
            .is_none_or(|record| record.state != PluginContextOperationState::Dispatched)
            && supplied_receipt.is_some()
        {
            return Err(PluginContextOperationError::InvalidReceipt);
        }

        let receipt = if existing
            .as_ref()
            .is_some_and(|record| record.state == PluginContextOperationState::Dispatched)
        {
            let stored = self.load_terminal_receipt(command.state.id, &identity.invocation_id)?;
            match (stored, supplied_receipt) {
                (Some(stored), Some(supplied)) if &stored == supplied => Some(stored),
                (Some(stored), None) => Some(stored),
                (None, None) => None,
                (Some(_) | None, Some(_)) => {
                    return Err(PluginContextOperationError::InvalidReceipt);
                }
            }
        } else {
            None
        };
        let evidence = receipt
            .as_ref()
            .map(PluginContextOperationTerminalReceipt::evidence);

        match classify_plugin_context_operation_recovery(
            &command.state,
            &identity.invocation_id,
            evidence.as_ref(),
        ) {
            PluginContextOperationRecovery::NotStarted => {
                let event = RuntimeCommittedEvent::PluginContextOperationProposed(Box::new(
                    PluginContextOperationProposedEvent {
                        identity,
                        request: command.request,
                        readable_state: persisted_inputs,
                    },
                ));
                preflight_event(&command.state, &event, None)?;
                Ok(emit(event, None))
            }
            PluginContextOperationRecovery::AwaitPolicy => {
                let proposal = plugin_context_operation_action_proposal(
                    &command.state,
                    &identity,
                    &command.request,
                )?;
                let action_digest = proposal
                    .digest()
                    .map_err(|_| PluginContextOperationError::InvalidAuthorization)?;
                let Some(authorization) = command.authorization else {
                    return Ok(DrivePluginContextOperationResult::AwaitAuthorization { proposal });
                };
                if authorization.action_digest != action_digest
                    || authorization.authorization_digest
                        != plugin_context_operation_authorization_digest(&identity, action_digest)
                {
                    return Err(PluginContextOperationError::InvalidAuthorization);
                }
                let event = RuntimeCommittedEvent::PluginContextOperationAuthorized(Box::new(
                    PluginContextOperationAuthorizedEvent {
                        identity,
                        action_digest,
                        authorization_digest: authorization.authorization_digest,
                    },
                ));
                preflight_event(&command.state, &event, None)?;
                Ok(emit(event, None))
            }
            PluginContextOperationRecovery::SafeToDispatchOnce => {
                if command.authorization.is_some() {
                    let record = command
                        .state
                        .plugin_context_operations
                        .get(&identity.invocation_id)
                        .ok_or(PluginContextOperationError::InvalidCommand)?;
                    let exact = PluginContextOperationAuthorization {
                        action_digest: record
                            .action_digest
                            .ok_or(PluginContextOperationError::InvalidCommand)?,
                        authorization_digest: record
                            .authorization_digest
                            .ok_or(PluginContextOperationError::InvalidCommand)?,
                    };
                    if command.authorization != Some(exact) {
                        return Err(PluginContextOperationError::InvalidAuthorization);
                    }
                }
                let record = command
                    .state
                    .plugin_context_operations
                    .get(&identity.invocation_id)
                    .ok_or(PluginContextOperationError::InvalidCommand)?;
                let action_digest = record
                    .action_digest
                    .ok_or(PluginContextOperationError::InvalidCommand)?;
                let declaration = match self.exact_declaration(&command.state, &identity, true) {
                    Ok(declaration) => declaration,
                    Err(code) => {
                        let event = RuntimeCommittedEvent::PluginContextOperationFailed(Box::new(
                            PluginContextOperationFailedEvent {
                                identity,
                                action_digest: None,
                                code,
                                terminal_receipt: None,
                            },
                        ));
                        preflight_event(&command.state, &event, None)?;
                        return Ok(emit(event, None));
                    }
                };
                validate_operation_input_schema(&declaration, &command.request, &command.inputs)
                    .map_err(|error| match error {
                        PluginContextOperationError::InvalidInput => {
                            PluginContextOperationError::InvalidInput
                        }
                        _ => PluginContextOperationError::DeclarationDrift,
                    })?;
                let event = RuntimeCommittedEvent::PluginContextOperationDispatched(Box::new(
                    PluginContextOperationDispatchedEvent {
                        identity: identity.clone(),
                        action_digest,
                    },
                ));
                preflight_event(&command.state, &event, None)?;
                let ticket = PluginContextOperationDispatchTicket {
                    identity,
                    request: command.request,
                    inputs: command.inputs,
                    action_digest,
                    cancellation_id: command.cancellation_id,
                    reserved_completion_event_id: command.reserved_completion_event_id,
                    declaration,
                };
                Ok(DrivePluginContextOperationResult::Dispatch { event, ticket })
            }
            PluginContextOperationRecovery::SafeToCompleteFromReceipt => {
                let receipt = receipt.ok_or(PluginContextOperationError::InvalidReceipt)?;
                receipt.validate()?;
                if receipt.identity != identity {
                    return Err(PluginContextOperationError::IdentitySubstitution);
                }
                let required_event_id = completed_memory_event_id(&receipt)?;
                let event = receipt.into_terminal_event();
                preflight_event(&command.state, &event, required_event_id)?;
                Ok(emit(event, required_event_id))
            }
            PluginContextOperationRecovery::AmbiguousFailClosed => {
                if let Some(record) = existing {
                    if record.state == PluginContextOperationState::Ambiguous {
                        return Ok(DrivePluginContextOperationResult::Terminal(
                            PluginContextOperationTerminalState::Ambiguous {
                                identity: record.identity,
                                code: record.failure_code.unwrap_or_else(|| {
                                    String::from("plugin_context_operation_ambiguous")
                                }),
                            },
                        ));
                    }
                    if record.state == PluginContextOperationState::Dispatched {
                        let event = RuntimeCommittedEvent::PluginContextOperationAmbiguous(
                            Box::new(PluginContextOperationAmbiguousEvent {
                                identity,
                                code: String::from(
                                    "plugin_context_operation_missing_terminal_receipt",
                                ),
                                terminal_receipt: None,
                            }),
                        );
                        preflight_event(&command.state, &event, None)?;
                        return Ok(emit(event, None));
                    }
                }
                Err(PluginContextOperationError::AmbiguousFailClosed)
            }
            PluginContextOperationRecovery::AwaitApplicationAuthorization => {
                let record = command
                    .state
                    .plugin_context_operations
                    .get(&identity.invocation_id)
                    .ok_or(PluginContextOperationError::InvalidCommand)?;
                let proposal_hash = record
                    .proposal_hash
                    .ok_or(PluginContextOperationError::InvalidCommand)?;
                let proposal_record = record
                    .proposal
                    .as_ref()
                    .ok_or(PluginContextOperationError::InvalidCommand)?;
                let replacement_hash = plugin_context_operation_replacement_hash(proposal_record)?;
                let binding = command
                    .state
                    .style_binding
                    .as_ref()
                    .ok_or(PluginContextOperationError::InvalidCommand)?;
                let proposal = context_replacement_action_proposal(
                    &binding.id,
                    &command.state.workspace,
                    &identity.phase.boundary.run_id,
                    &identity.phase.phase,
                    replacement_hash,
                );
                let action_digest = proposal
                    .digest()
                    .map_err(|_| PluginContextOperationError::InvalidAuthorization)?;
                let Some(authorization) = command.application_authorization else {
                    return Ok(
                        DrivePluginContextOperationResult::AwaitApplicationAuthorization {
                            proposal,
                        },
                    );
                };
                if authorization.action_digest != action_digest {
                    return Err(PluginContextOperationError::InvalidAuthorization);
                }
                let event = RuntimeCommittedEvent::PluginContextOperationApplicationApproved(
                    Box::new(PluginContextOperationApplicationApprovedEvent {
                        identity,
                        proposal_hash,
                        replacement_hash,
                        action_digest,
                    }),
                );
                preflight_event(&command.state, &event, None)?;
                Ok(emit(event, None))
            }
            PluginContextOperationRecovery::SafeToApply => {
                let record = command
                    .state
                    .plugin_context_operations
                    .get(&identity.invocation_id)
                    .ok_or(PluginContextOperationError::InvalidCommand)?;
                let proposal = record
                    .proposal
                    .as_ref()
                    .ok_or(PluginContextOperationError::InvalidCommand)?;
                let proposal_hash = record
                    .proposal_hash
                    .ok_or(PluginContextOperationError::InvalidCommand)?;
                let replacement_hash = record
                    .replacement_hash
                    .ok_or(PluginContextOperationError::InvalidCommand)?;
                let sequence = command
                    .state
                    .last_sequence
                    .checked_next()
                    .map_err(|_| PluginContextOperationError::Sequence)?;
                let replacement = proposal_replacement(proposal).to_vec();
                let event = RuntimeCommittedEvent::PluginContextOperationApplied(Box::new(
                    PluginContextOperationAppliedEvent {
                        identity: identity.clone(),
                        proposal_hash,
                        replacement,
                        replacement_hash,
                        application_hash: plugin_context_operation_application_hash(
                            &identity,
                            proposal_hash,
                            replacement_hash,
                        ),
                        provenance: ProjectionProvenance {
                            projection_id: format!(
                                "plugin-context-operation:{}:{}",
                                identity.invocation_id,
                                sequence.get()
                            ),
                            source_range: None,
                            method: match identity.kind {
                                PluginContextOperationKind::MemoryRetrieve => {
                                    String::from("plugin_memory_retrieve")
                                }
                                PluginContextOperationKind::Compaction => {
                                    String::from("plugin_compaction")
                                }
                            },
                            committed_at: sequence,
                            artifact_id: None,
                        },
                    },
                ));
                preflight_event(&command.state, &event, None)?;
                Ok(emit(event, None))
            }
            PluginContextOperationRecovery::Applied => {
                let record = command
                    .state
                    .plugin_context_operations
                    .get(&identity.invocation_id)
                    .ok_or(PluginContextOperationError::InvalidCommand)?;
                Ok(DrivePluginContextOperationResult::Terminal(
                    PluginContextOperationTerminalState::Applied {
                        identity,
                        proposal: record
                            .proposal
                            .clone()
                            .ok_or(PluginContextOperationError::InvalidCommand)?,
                    },
                ))
            }
            PluginContextOperationRecovery::TerminallyFailed => {
                let record = command
                    .state
                    .plugin_context_operations
                    .get(&identity.invocation_id)
                    .ok_or(PluginContextOperationError::InvalidCommand)?;
                Ok(DrivePluginContextOperationResult::Terminal(
                    PluginContextOperationTerminalState::Failed {
                        identity,
                        code: record
                            .failure_code
                            .clone()
                            .ok_or(PluginContextOperationError::InvalidCommand)?,
                    },
                ))
            }
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the one-shot gate, call, validation, sealing, and durable receipt write form one auditable dispatch transaction"
    )]
    async fn dispatch(
        &self,
        dispatched_state: &SessionState,
        ticket: PluginContextOperationDispatchTicket,
    ) -> Result<PluginContextOperationTerminalReceipt, PluginContextOperationError> {
        let record = dispatched_state
            .plugin_context_operations
            .get(&ticket.identity.invocation_id)
            .filter(|record| {
                record.identity == ticket.identity
                    && record.request == ticket.request
                    && record.readable_state
                        == serde_json::to_value(&ticket.inputs).unwrap_or(Value::Null)
                    && record.state == PluginContextOperationState::Dispatched
                    && record.action_digest == Some(ticket.action_digest)
                    && record.last_sequence == dispatched_state.last_sequence
            })
            .ok_or(PluginContextOperationError::DispatchNotCommitted)?;
        if ticket.cancellation_id.trim().is_empty() {
            return Err(PluginContextOperationError::InvalidCommand);
        }
        let dispatch_key = (dispatched_state.id, ticket.identity.invocation_id.clone());
        let dispatch_gate = {
            let mut gates = self.dispatch_gates.lock().await;
            Arc::clone(
                gates
                    .entry(dispatch_key.clone())
                    .or_insert_with(|| Arc::new(DispatchGate::default())),
            )
        };
        let mut dispatch_guard = dispatch_gate.lock().await;
        if let Some(receipt) =
            self.load_terminal_receipt(dispatched_state.id, &ticket.identity.invocation_id)?
        {
            if receipt.identity != ticket.identity {
                return Err(PluginContextOperationError::IdentitySubstitution);
            }
            drop(dispatch_guard);
            release_dispatch_gate(&self.dispatch_gates, &dispatch_key, &dispatch_gate).await;
            return Ok(receipt);
        }
        if dispatch_guard.claimed {
            return Err(PluginContextOperationError::AmbiguousFailClosed);
        }
        dispatch_guard.claimed = true;
        let completion_sequence = record
            .last_sequence
            .checked_next()
            .map_err(|_| PluginContextOperationError::Sequence)?;
        let mut receipt = match &ticket.declaration {
            PluginContextOperationDeclaration::Memory(declaration) => {
                let completion_event_id = ticket
                    .reserved_completion_event_id
                    .ok_or(PluginContextOperationError::MissingCompletionEventIdentity)?;
                self.invoke_memory(
                    dispatched_state,
                    &ticket,
                    declaration,
                    completion_event_id,
                    completion_sequence,
                )
                .await?
            }
            PluginContextOperationDeclaration::Compaction(declaration) => {
                self.invoke_compaction(dispatched_state, &ticket, declaration)
                    .await?
            }
        };
        let completion_event_id = completed_memory_event_id(&receipt)?;
        if matches!(
            &receipt.outcome,
            PluginContextOperationTerminalOutcome::Completed { .. }
        ) && preflight_event(
            dispatched_state,
            &receipt.clone().into_terminal_event(),
            completion_event_id,
        )
        .is_err()
        {
            receipt = PluginContextOperationTerminalReceipt::seal(
                ticket.identity.clone(),
                PluginContextOperationTerminalOutcome::Failed {
                    code: match ticket.identity.kind {
                        PluginContextOperationKind::MemoryRetrieve => {
                            String::from("plugin_memory_retrieve_invalid_output")
                        }
                        PluginContextOperationKind::Compaction => {
                            String::from("plugin_compaction_invalid_output")
                        }
                    },
                },
            )?;
        }
        receipt.validate()?;
        let encoded = serde_json::to_string(&receipt)
            .map_err(|_| PluginContextOperationError::Serialization)?;
        let stored = self
            .data
            .store_plugin_invocation_receipt(StorePluginInvocationReceiptDataRequest {
                identity: PluginInvocationReceiptDataIdentity {
                    session_id: dispatched_state.id,
                    invocation_id: ticket.identity.invocation_id,
                },
                receipt_json: encoded.clone(),
            })
            .map_err(PluginContextOperationError::ReceiptPersistenceAmbiguous)?;
        if stored.receipt_json != encoded {
            return Err(PluginContextOperationError::ReceiptPersistenceAmbiguous(
                PluginNodeReceiptDataError::Conflict,
            ));
        }
        drop(dispatch_guard);
        release_dispatch_gate(&self.dispatch_gates, &dispatch_key, &dispatch_gate).await;
        Ok(receipt)
    }

    fn load_terminal_receipt(
        &self,
        session_id: SessionId,
        invocation_id: &str,
    ) -> Result<Option<PluginContextOperationTerminalReceipt>, PluginContextOperationError> {
        if !valid_identifier(invocation_id, MAX_IDENTIFIER_BYTES) {
            return Err(PluginContextOperationError::InvalidReceipt);
        }
        self.data
            .load_plugin_invocation_receipt(PluginInvocationReceiptDataIdentity {
                session_id,
                invocation_id: invocation_id.to_owned(),
            })
            .map_err(PluginContextOperationError::ReceiptData)?
            .map(|record| {
                let receipt: PluginContextOperationTerminalReceipt =
                    serde_json::from_str(&record.receipt_json)
                        .map_err(|_| PluginContextOperationError::InvalidReceipt)?;
                receipt.validate()?;
                if receipt.identity.invocation_id != invocation_id {
                    return Err(PluginContextOperationError::IdentitySubstitution);
                }
                Ok(receipt)
            })
            .transpose()
    }
}

impl<D> ProductionPluginContextOperationCoordinator<D>
where
    D: Send + Sync + PluginDataPort + PluginNodeReceiptDataPort,
{
    #[allow(
        clippy::too_many_lines,
        reason = "exact immutable declaration comparison is exhaustive and kept in one fail-closed audit point"
    )]
    fn exact_declaration(
        &self,
        state: &SessionState,
        identity: &PluginContextOperationIdentity,
        validate_invocation_semantics: bool,
    ) -> Result<PluginContextOperationDeclaration, String> {
        let binding = state
            .style_binding
            .as_ref()
            .ok_or_else(|| String::from("plugin_context_operation_missing_style_binding"))?;
        if identity.attempt != 1
            || !state
                .plugins
                .activated_plugin_ids
                .contains(&identity.plugin_id)
        {
            return Err(String::from(
                "plugin_context_operation_immutable_selection_unavailable",
            ));
        }
        let runtime = SemanticVersion::parse(&binding.runtime_api_version)
            .map_err(|_| String::from("plugin_context_operation_runtime_api_incompatible"))?;
        match identity.kind {
            PluginContextOperationKind::MemoryRetrieve => {
                let selected = binding.memory.plugin.as_ref().ok_or_else(|| {
                    String::from("plugin_context_operation_immutable_selection_unavailable")
                })?;
                if selected.plugin_id != identity.plugin_id
                    || selected.plugin_version != identity.plugin_version
                    || selected.provider_id != identity.implementation_id
                    || selected.provider_version != identity.implementation_version
                    || selected.declaration_hash != identity.declaration_hash
                    || selected.configuration_reference != identity.configuration_reference
                {
                    return Err(String::from(
                        "plugin_context_operation_immutable_selection_drift",
                    ));
                }
                let declaration = self
                    .data
                    .memory_provider_declaration(
                        &identity.plugin_id,
                        &identity.implementation_id,
                        &identity.implementation_version,
                    )
                    .map_err(|error| declaration_error_code(&error).to_owned())?;
                let operation = &declaration.retrieve;
                if declaration.provider_id != identity.implementation_id
                    || declaration.version != identity.implementation_version
                    || declaration.declaration_hash != identity.declaration_hash
                    || SemanticVersion::parse(&declaration.version).is_err()
                    || VersionReq::parse(&declaration.runtime_api)
                        .map_or(true, |requirement| !requirement.matches(&runtime))
                    || !valid_capabilities(&declaration.capabilities)
                    || !valid_handler(&operation.handler)
                    || (validate_invocation_semantics && operation.handler != identity.handler)
                    || operation.timeout_ms == 0
                    || (validate_invocation_semantics
                        && operation.timeout_ms != identity.timeout_ms)
                    || operation.max_attempts == 0
                    || !operation.idempotent
                    || (validate_invocation_semantics
                        && operation.idempotent != identity.idempotent)
                    || operation.external_effects
                    || !valid_failure_policy(&operation.failure_policy)
                    || !valid_state_scope(&operation.state_scope)
                {
                    return Err(String::from("plugin_context_operation_declaration_drift"));
                }
                Ok(PluginContextOperationDeclaration::Memory(declaration))
            }
            PluginContextOperationKind::Compaction => {
                let selected = binding.compaction.plugin.as_ref().ok_or_else(|| {
                    String::from("plugin_context_operation_immutable_selection_unavailable")
                })?;
                if selected.plugin_id != identity.plugin_id
                    || selected.plugin_version != identity.plugin_version
                    || selected.compactor_id != identity.implementation_id
                    || selected.compactor_version != identity.implementation_version
                    || selected.declaration_hash != identity.declaration_hash
                    || selected.configuration_reference != identity.configuration_reference
                {
                    return Err(String::from(
                        "plugin_context_operation_immutable_selection_drift",
                    ));
                }
                let declaration = self
                    .data
                    .compactor_declaration(
                        &identity.plugin_id,
                        &identity.implementation_id,
                        &identity.implementation_version,
                    )
                    .map_err(|error| declaration_error_code(&error).to_owned())?;
                if declaration.compactor_id != identity.implementation_id
                    || declaration.version != identity.implementation_version
                    || declaration.declaration_hash != identity.declaration_hash
                    || SemanticVersion::parse(&declaration.version).is_err()
                    || VersionReq::parse(&declaration.runtime_api)
                        .map_or(true, |requirement| !requirement.matches(&runtime))
                    || !valid_capabilities(&declaration.capabilities)
                    || !valid_handler(&declaration.handler)
                    || (validate_invocation_semantics && declaration.handler != identity.handler)
                    || declaration.timeout_ms == 0
                    || (validate_invocation_semantics
                        && declaration.timeout_ms != identity.timeout_ms)
                    || declaration.max_attempts == 0
                    || !declaration.idempotent
                    || (validate_invocation_semantics
                        && declaration.idempotent != identity.idempotent)
                    || declaration.external_effects
                    || !valid_failure_policy(&declaration.failure_policy)
                    || !valid_state_scope(&declaration.state_scope)
                {
                    return Err(String::from("plugin_context_operation_declaration_drift"));
                }
                Ok(PluginContextOperationDeclaration::Compaction(declaration))
            }
        }
    }

    async fn invoke_memory(
        &self,
        state: &SessionState,
        ticket: &PluginContextOperationDispatchTicket,
        declaration: &PluginMemoryProviderDataRecord,
        completion_event_id: EventId,
        completion_sequence: Sequence,
    ) -> Result<PluginContextOperationTerminalReceipt, PluginContextOperationError> {
        let PluginContextOperationRequest::MemoryRetrieve {
            query,
            scopes,
            max_items,
            max_injected_bytes,
        } = &ticket.request
        else {
            return Err(PluginContextOperationError::InvalidCommand);
        };
        let mut request = RetrievePluginMemoryDataRequest {
            binding: operation_binding(state, &ticket.identity),
            provider_id: declaration.provider_id.clone(),
            provider_version: declaration.version.clone(),
            handler: declaration.retrieve.handler.clone(),
            max_attempts: 1,
            retry_backoff_ms: declaration.retrieve.retry_backoff_ms,
            timeout_ms: declaration.retrieve.timeout_ms,
            input: PluginMemoryRetrieveInputDataRecord {
                query: query.clone(),
                scopes: map_memory_scopes(scopes)?,
                max_items: *max_items,
                max_bytes: *max_injected_bytes,
                artifacts: ticket.inputs.artifacts.iter().map(map_artifact).collect(),
                references: ticket.inputs.references.iter().map(map_reference).collect(),
                parameters: ticket.inputs.parameters.clone(),
            },
            readable_state: ticket.inputs.readable_state.clone(),
            cancellation_id: ticket.cancellation_id.clone(),
        };
        request.binding.request_hash =
            agentmod_runtime_data::plugin::plugin_memory_retrieve_request_hash(&request)
                .map_err(|_| PluginContextOperationError::Serialization)?;
        let expected_binding = request.binding.clone();
        let outcome = match self.data.retrieve_memory(request).await {
            Ok(raw) => {
                match validate_memory_output(
                    state,
                    &ticket.identity,
                    &expected_binding,
                    &ticket.request,
                    &ticket.inputs,
                    declaration,
                    raw,
                    completion_event_id,
                    completion_sequence,
                ) {
                    Ok(proposal) => PluginContextOperationTerminalOutcome::Completed {
                        proposal,
                        proposal_hash: ZERO_HASH,
                    },
                    Err(_) => PluginContextOperationTerminalOutcome::Failed {
                        code: String::from("plugin_memory_retrieve_invalid_output"),
                    },
                }
            }
            Err(error) if plugin_data_error_is_ambiguous(&error) => {
                PluginContextOperationTerminalOutcome::Ambiguous {
                    code: String::from("plugin_memory_retrieve_ambiguous"),
                }
            }
            Err(error) => PluginContextOperationTerminalOutcome::Failed {
                code: plugin_data_failure_code(&error, PluginContextOperationKind::MemoryRetrieve)
                    .to_owned(),
            },
        };
        PluginContextOperationTerminalReceipt::seal(ticket.identity.clone(), outcome)
    }

    async fn invoke_compaction(
        &self,
        state: &SessionState,
        ticket: &PluginContextOperationDispatchTicket,
        declaration: &PluginCompactorDataRecord,
    ) -> Result<PluginContextOperationTerminalReceipt, PluginContextOperationError> {
        let PluginContextOperationRequest::Compaction {
            projection,
            projection_hash,
            max_projection_tokens,
            preservation_requirements,
            ..
        } = &ticket.request
        else {
            return Err(PluginContextOperationError::InvalidCommand);
        };
        let max_replacement_bytes = ticket
            .inputs
            .max_replacement_bytes
            .ok_or(PluginContextOperationError::InvalidInput)?;
        let projection = serde_json::to_value(project(projection))
            .map_err(|_| PluginContextOperationError::Serialization)?;
        let mut request = CompactPluginContextDataRequest {
            binding: operation_binding(state, &ticket.identity),
            compactor_id: declaration.compactor_id.clone(),
            compactor_version: declaration.version.clone(),
            handler: declaration.handler.clone(),
            max_attempts: 1,
            retry_backoff_ms: declaration.retry_backoff_ms,
            timeout_ms: declaration.timeout_ms,
            input: PluginCompactionInputDataRecord {
                projection,
                projection_hash: *projection_hash,
                required_references: ticket.inputs.references.iter().map(map_reference).collect(),
                required_artifacts: ticket.inputs.artifacts.iter().map(map_artifact).collect(),
                preservation_requirements: preservation_requirements.iter().cloned().collect(),
                max_replacement_bytes,
                max_projection_tokens: *max_projection_tokens,
                parameters: ticket.inputs.parameters.clone(),
            },
            readable_state: ticket.inputs.readable_state.clone(),
            cancellation_id: ticket.cancellation_id.clone(),
        };
        request.binding.request_hash =
            agentmod_runtime_data::plugin::plugin_compaction_request_hash(&request)
                .map_err(|_| PluginContextOperationError::Serialization)?;
        let expected_binding = request.binding.clone();
        let outcome = match self.data.compact_context(request).await {
            Ok(raw) => match validate_compaction_output(
                &ticket.identity,
                &expected_binding,
                &ticket.request,
                &ticket.inputs,
                declaration,
                raw,
            ) {
                Ok(proposal) => PluginContextOperationTerminalOutcome::Completed {
                    proposal,
                    proposal_hash: ZERO_HASH,
                },
                Err(_) => PluginContextOperationTerminalOutcome::Failed {
                    code: String::from("plugin_compaction_invalid_output"),
                },
            },
            Err(error) if plugin_data_error_is_ambiguous(&error) => {
                PluginContextOperationTerminalOutcome::Ambiguous {
                    code: String::from("plugin_compaction_ambiguous"),
                }
            }
            Err(error) => PluginContextOperationTerminalOutcome::Failed {
                code: plugin_data_failure_code(&error, PluginContextOperationKind::Compaction)
                    .to_owned(),
            },
        };
        PluginContextOperationTerminalReceipt::seal(ticket.identity.clone(), outcome)
    }
}

impl PluginContextOperationTerminalReceipt {
    fn seal(
        identity: PluginContextOperationIdentity,
        mut outcome: PluginContextOperationTerminalOutcome,
    ) -> Result<Self, PluginContextOperationError> {
        let receipt_hash = terminal_receipt_hash(&identity, &outcome)?;
        if let PluginContextOperationTerminalOutcome::Completed {
            proposal,
            proposal_hash,
        } = &mut outcome
        {
            bind_memory_receipt_hash(proposal, receipt_hash);
            *proposal_hash = plugin_context_operation_proposal_hash(proposal)?;
        }
        let receipt_reference = format!("plugin-receipt:{receipt_hash}");
        let receipt = Self {
            identity,
            outcome,
            receipt_hash,
            receipt_reference,
        };
        receipt.validate()?;
        Ok(receipt)
    }

    fn validate(&self) -> Result<(), PluginContextOperationError> {
        if self.identity.attempt != 1
            || self.receipt_hash == ZERO_HASH
            || self.receipt_hash != terminal_receipt_hash(&self.identity, &self.outcome)?
            || self.receipt_reference != format!("plugin-receipt:{}", self.receipt_hash)
            || !valid_identifier(&self.receipt_reference, MAX_IDENTIFIER_BYTES)
        {
            return Err(PluginContextOperationError::InvalidReceipt);
        }
        match &self.outcome {
            PluginContextOperationTerminalOutcome::Completed {
                proposal,
                proposal_hash,
            } => {
                if proposal_kind(proposal) != self.identity.kind
                    || *proposal_hash != plugin_context_operation_proposal_hash(proposal)?
                    || memory_receipt_hashes(proposal)
                        .is_some_and(|hashes| hashes.iter().any(|hash| *hash != self.receipt_hash))
                {
                    return Err(PluginContextOperationError::InvalidReceipt);
                }
            }
            PluginContextOperationTerminalOutcome::Failed { code }
            | PluginContextOperationTerminalOutcome::Ambiguous { code } => {
                if !valid_diagnostic(code) {
                    return Err(PluginContextOperationError::InvalidReceipt);
                }
            }
        }
        Ok(())
    }

    fn into_terminal_event(self) -> RuntimeCommittedEvent {
        let terminal = self.sealed_reference();
        match self.outcome {
            PluginContextOperationTerminalOutcome::Completed {
                proposal,
                proposal_hash,
            } => RuntimeCommittedEvent::PluginContextOperationCompleted(Box::new(
                PluginContextOperationCompletedEvent {
                    identity: self.identity,
                    proposal,
                    proposal_hash,
                    terminal_receipt: terminal,
                },
            )),
            PluginContextOperationTerminalOutcome::Failed { code } => {
                RuntimeCommittedEvent::PluginContextOperationFailed(Box::new(
                    PluginContextOperationFailedEvent {
                        identity: self.identity,
                        action_digest: None,
                        code,
                        terminal_receipt: Some(terminal),
                    },
                ))
            }
            PluginContextOperationTerminalOutcome::Ambiguous { code } => {
                RuntimeCommittedEvent::PluginContextOperationAmbiguous(Box::new(
                    PluginContextOperationAmbiguousEvent {
                        identity: self.identity,
                        code,
                        terminal_receipt: Some(terminal),
                    },
                ))
            }
        }
    }
}

fn terminal_receipt_hash(
    identity: &PluginContextOperationIdentity,
    outcome: &PluginContextOperationTerminalOutcome,
) -> Result<ContentHash, PluginContextOperationError> {
    let mut normalized = outcome.clone();
    if let PluginContextOperationTerminalOutcome::Completed {
        proposal,
        proposal_hash,
    } = &mut normalized
    {
        bind_memory_receipt_hash(proposal, ZERO_HASH);
        *proposal_hash = ZERO_HASH;
    }
    serde_json::to_vec(&(
        "agentmod.plugin-context-operation-terminal-receipt.v1",
        identity,
        normalized,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
    .map_err(|_| PluginContextOperationError::Serialization)
}

fn bind_memory_receipt_hash(
    proposal: &mut PluginContextOperationProposal,
    receipt_hash: ContentHash,
) {
    let PluginContextOperationProposal::MemoryRetrieve {
        replacement,
        retrieved_entry_ids,
    } = proposal
    else {
        return;
    };
    for entry in replacement {
        if retrieved_entry_ids.iter().any(|id| id == entry.id())
            && let ConversationEntry::RetrievedMemory(memory) = entry
            && let Some(provenance) = memory.typed_provenance.as_deref_mut()
        {
            provenance.plugin_terminal_receipt_hash = receipt_hash;
        }
    }
}

fn memory_receipt_hashes(proposal: &PluginContextOperationProposal) -> Option<Vec<ContentHash>> {
    let PluginContextOperationProposal::MemoryRetrieve {
        replacement,
        retrieved_entry_ids,
    } = proposal
    else {
        return None;
    };
    Some(
        replacement
            .iter()
            .filter(|entry| retrieved_entry_ids.iter().any(|id| id == entry.id()))
            .filter_map(|entry| match entry {
                ConversationEntry::RetrievedMemory(memory) => memory
                    .typed_provenance
                    .as_deref()
                    .map(|provenance| provenance.plugin_terminal_receipt_hash),
                _ => None,
            })
            .collect(),
    )
}

fn validate_drive_command(
    command: &DrivePluginContextOperationCommand,
) -> Result<(), PluginContextOperationError> {
    if command.cancellation_id.trim().is_empty()
        || command.cancellation_id.len() > MAX_IDENTIFIER_BYTES
        || command.cancellation_id.chars().any(char::is_control)
        || request_kind(&command.request) != command.phase_kind()?
    {
        return Err(PluginContextOperationError::InvalidCommand);
    }
    validate_inputs(&command.request, &command.inputs)
}

trait DriveCommandExt {
    fn phase_kind(&self) -> Result<PluginContextOperationKind, PluginContextOperationError>;
}

impl DriveCommandExt for DrivePluginContextOperationCommand {
    fn phase_kind(&self) -> Result<PluginContextOperationKind, PluginContextOperationError> {
        match self.phase.phase.as_str() {
            "memory" => Ok(PluginContextOperationKind::MemoryRetrieve),
            "compaction" => Ok(PluginContextOperationKind::Compaction),
            _ => Err(PluginContextOperationError::InvalidCommand),
        }
    }
}

fn validate_inputs(
    request: &PluginContextOperationRequest,
    inputs: &PluginContextOperationInputs,
) -> Result<(), PluginContextOperationError> {
    validate_bounded_json(&inputs.readable_state, SchemaUse::Input)?;
    validate_readable_state(&inputs.readable_state)?;
    validate_bounded_json(&inputs.parameters, SchemaUse::Input)?;
    validate_readable_state_value(&inputs.parameters, false)?;
    if !inputs.readable_state.is_object()
        || !inputs.parameters.is_object()
        || inputs.artifacts.len() > MAX_RESOURCE_COUNT
        || inputs.references.len() > MAX_RESOURCE_COUNT
        || !inputs.artifacts.windows(2).all(|pair| pair[0] < pair[1])
        || !inputs.references.windows(2).all(|pair| pair[0] < pair[1])
    {
        return Err(PluginContextOperationError::InvalidInput);
    }
    for artifact in &inputs.artifacts {
        if ArtifactId::from_str(&artifact.artifact_id).is_err()
            || !valid_identifier(&artifact.artifact_reference, MAX_IDENTIFIER_BYTES)
            || artifact.content_hash == ZERO_HASH
            || !valid_identifier(&artifact.media_type, 128)
            || artifact.size_bytes == 0
        {
            return Err(PluginContextOperationError::InvalidArtifact);
        }
    }
    for reference in &inputs.references {
        if !valid_identifier(&reference.id, MAX_IDENTIFIER_BYTES)
            || reference.content_hash == ZERO_HASH
        {
            return Err(PluginContextOperationError::InvalidReference);
        }
    }
    match request {
        PluginContextOperationRequest::MemoryRetrieve { scopes, .. } => {
            if inputs.max_replacement_bytes.is_some() {
                return Err(PluginContextOperationError::InvalidInput);
            }
            map_memory_scopes(scopes)?;
        }
        PluginContextOperationRequest::Compaction {
            artifact_references,
            references,
            ..
        } => {
            let max = inputs
                .max_replacement_bytes
                .ok_or(PluginContextOperationError::InvalidInput)?;
            if max == 0 || max > MAX_OPERATION_BYTES as u64 {
                return Err(PluginContextOperationError::InvalidInput);
            }
            if artifact_references
                != &inputs
                    .artifacts
                    .iter()
                    .map(|artifact| artifact.artifact_reference.clone())
                    .collect::<Vec<_>>()
                || references
                    != &inputs
                        .references
                        .iter()
                        .map(|reference| reference.id.clone())
                        .collect::<Vec<_>>()
            {
                return Err(PluginContextOperationError::InvalidReference);
            }
        }
    }
    Ok(())
}

fn validate_readable_state(readable_state: &Value) -> Result<(), PluginContextOperationError> {
    let object = readable_state
        .as_object()
        .ok_or(PluginContextOperationError::InvalidInput)?;
    if object.get("schema").and_then(Value::as_str) != Some(PLUGIN_CONTEXT_READABLE_STATE_SCHEMA_V1)
        || object.keys().any(|field| {
            READABLE_STATE_FIELDS
                .binary_search(&field.as_str())
                .is_err()
        })
    {
        return Err(PluginContextOperationError::InvalidInput);
    }
    for field in [
        "canonical_variables",
        "canonical_counters",
        "context_metadata",
        "recorded_runtime_values",
    ] {
        if object.get(field).is_some_and(|value| !value.is_object()) {
            return Err(PluginContextOperationError::InvalidInput);
        }
    }
    for field in ["node_result_references", "approval_result_references"] {
        if let Some(value) = object.get(field) {
            let references = value
                .as_array()
                .ok_or(PluginContextOperationError::InvalidInput)?;
            if references.iter().any(|reference| {
                reference
                    .as_str()
                    .is_none_or(|reference| !valid_identifier(reference, MAX_IDENTIFIER_BYTES))
            }) {
                return Err(PluginContextOperationError::InvalidInput);
            }
        }
    }
    validate_readable_state_value(readable_state, true)
}

fn validate_readable_state_value(
    value: &Value,
    is_root: bool,
) -> Result<(), PluginContextOperationError> {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                if !is_root && forbidden_readable_state_key(key) {
                    return Err(PluginContextOperationError::InvalidInput);
                }
                validate_readable_state_value(value, false)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                validate_readable_state_value(value, false)?;
            }
        }
        Value::String(value) if forbidden_readable_state_string(value) => {
            return Err(PluginContextOperationError::InvalidInput);
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

fn forbidden_readable_state_key(key: &str) -> bool {
    let normalized = key
        .bytes()
        .filter(u8::is_ascii_alphanumeric)
        .map(|byte| byte.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let normalized = String::from_utf8_lossy(&normalized);
    [
        "secret",
        "password",
        "passwd",
        "credential",
        "authorization",
        "bearer",
        "apikey",
        "accesstoken",
        "refreshtoken",
        "privatekey",
        "clientsecret",
        "cookie",
        "processhandle",
        "oshandle",
        "rawhandle",
        "filedescriptor",
    ]
    .iter()
    .any(|forbidden| normalized.contains(forbidden))
        || matches!(
            normalized.as_ref(),
            "fd" | "handle"
                | "pid"
                | "process"
                | "processid"
                | "stdin"
                | "stdout"
                | "stderr"
                | "token"
        )
        || normalized.ends_with("token")
}

fn forbidden_readable_state_string(value: &str) -> bool {
    let trimmed = value.trim();
    let lower = trimmed.to_ascii_lowercase();
    lower.starts_with("bearer ")
        || lower.starts_with("basic ")
        || lower.starts_with("file://")
        || lower.starts_with("pipe://")
        || lower.starts_with("fd:")
        || lower.starts_with("handle:")
        || lower.starts_with("pid:")
        || lower.starts_with("process:")
        || lower.starts_with("/proc/")
        || lower.starts_with(r"\\.\pipe\")
        || lower.starts_with("ghp_")
        || lower.starts_with("github_pat_")
        || lower.starts_with("sk-")
        || lower.starts_with("xoxb-")
        || lower.contains("-----begin private key-----")
        || lower.contains("-----begin rsa private key-----")
        || lower.contains("-----begin ec private key-----")
}

fn validate_operation_input_schema(
    declaration: &PluginContextOperationDeclaration,
    request: &PluginContextOperationRequest,
    inputs: &PluginContextOperationInputs,
) -> Result<(), PluginContextOperationError> {
    let input = operation_input_json(request, inputs)?;
    validate_bounded_json(&input, SchemaUse::Input)?;
    let schema = match declaration {
        PluginContextOperationDeclaration::Memory(declaration) => {
            &declaration.retrieve.input_schema
        }
        PluginContextOperationDeclaration::Compaction(declaration) => &declaration.input_schema,
    };
    validate_json_schema(schema, &input, SchemaUse::Input)
}

fn operation_input_json(
    request: &PluginContextOperationRequest,
    inputs: &PluginContextOperationInputs,
) -> Result<Value, PluginContextOperationError> {
    match request {
        PluginContextOperationRequest::MemoryRetrieve {
            query,
            scopes,
            max_items,
            max_injected_bytes,
        } => Ok(json!({
            "query": query,
            "scopes": scopes.iter().map(|scope| scope_family(scope)).collect::<Result<Vec<_>, _>>()?,
            "max_items": max_items,
            "max_bytes": max_injected_bytes,
            "artifacts": inputs.artifacts.iter().map(|artifact| {
                artifact_data_json(&map_artifact(artifact))
            }).collect::<Vec<_>>(),
            "references": inputs.references.iter().map(|reference| {
                reference_data_json(&map_reference(reference))
            }).collect::<Vec<_>>(),
            "parameters": inputs.parameters,
        })),
        PluginContextOperationRequest::Compaction {
            projection,
            projection_hash,
            max_projection_tokens,
            preservation_requirements,
            ..
        } => Ok(json!({
            "projection": project(projection),
            "projection_hash": projection_hash,
            "required_references": inputs.references.iter().map(|reference| {
                reference_data_json(&map_reference(reference))
            }).collect::<Vec<_>>(),
            "required_artifacts": inputs.artifacts.iter().map(|artifact| {
                artifact_data_json(&map_artifact(artifact))
            }).collect::<Vec<_>>(),
            "preservation_requirements": preservation_requirements,
            "max_replacement_bytes": inputs.max_replacement_bytes,
            "max_projection_tokens": max_projection_tokens,
            "parameters": inputs.parameters,
        })),
    }
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "every exact invocation, bounded resource, and application identity is exhaustively validated at this trust boundary"
)]
fn validate_memory_output(
    state: &SessionState,
    identity: &PluginContextOperationIdentity,
    expected_binding: &PluginOperationBindingDataRecord,
    request: &PluginContextOperationRequest,
    inputs: &PluginContextOperationInputs,
    declaration: &PluginMemoryProviderDataRecord,
    raw: PluginMemoryRetrieveProposalDataRecord,
    completion_event_id: EventId,
    completion_sequence: Sequence,
) -> Result<PluginContextOperationProposal, PluginContextOperationError> {
    let PluginContextOperationRequest::MemoryRetrieve {
        query,
        scopes,
        max_items,
        max_injected_bytes,
    } = request
    else {
        return Err(PluginContextOperationError::InvalidOutput);
    };
    if raw.binding != *expected_binding
        || raw.provider_id != identity.implementation_id
        || raw.provider_version != identity.implementation_version
        || raw.items.is_empty()
        || raw.items.len()
            > usize::try_from(*max_items).map_err(|_| PluginContextOperationError::InvalidOutput)?
    {
        return Err(PluginContextOperationError::IdentitySubstitution);
    }
    let output = memory_output_json(&raw.items);
    validate_bounded_json(&output, SchemaUse::Output)?;
    validate_json_schema(
        &declaration.retrieve.output_schema,
        &output,
        SchemaUse::Output,
    )?;
    let mut item_ids = BTreeSet::new();
    let known_artifacts = inputs
        .artifacts
        .iter()
        .map(|artifact| (artifact.artifact_id.as_str(), artifact))
        .collect::<BTreeMap<_, _>>();
    let known_references = inputs
        .references
        .iter()
        .map(|reference| ((reference.kind, reference.id.as_str()), reference))
        .collect::<BTreeMap<_, _>>();
    let mut introduced = Vec::with_capacity(raw.items.len());
    let mut introduced_ids = Vec::with_capacity(raw.items.len());
    let mut value_bytes = 0_u64;
    for item in raw.items {
        if !valid_identifier(&item.item_id, MAX_MEMORY_ITEM_ID_BYTES)
            || !item_ids.insert(item.item_id.clone())
            || item.metadata.len() > MAX_METADATA_ITEMS
            || metadata_bytes(&item.metadata)? > MAX_METADATA_BYTES
            || item.metadata.iter().any(|(key, value)| {
                !valid_identifier(key, 128)
                    || value.len() > 1024
                    || value.chars().any(char::is_control)
                    || is_secret_metadata_key(key)
            })
            || item.value_hash != hash_json(&item.value)?
            || item.artifacts.len() > 1
            || has_duplicate_artifacts(&item.artifacts)
            || has_duplicate_references(&item.references)
        {
            return Err(PluginContextOperationError::InvalidOutput);
        }
        let encoded_value = canonical_json_bytes(&item.value)
            .map_err(|_| PluginContextOperationError::InvalidOutput)?;
        let encoded_len = u64::try_from(encoded_value.len())
            .map_err(|_| PluginContextOperationError::InvalidOutput)?;
        value_bytes = value_bytes
            .checked_add(encoded_len)
            .ok_or(PluginContextOperationError::BudgetExceeded)?;
        if value_bytes > *max_injected_bytes {
            return Err(PluginContextOperationError::BudgetExceeded);
        }
        let scope = exact_scope_label(item.scope, scopes)?;
        let effective_security = map_security_from_data(item.security_classification);
        let artifact = item
            .artifacts
            .first()
            .map(|artifact| {
                let known = known_artifacts
                    .get(artifact.artifact_id.as_str())
                    .copied()
                    .filter(|known| artifact_matches_known(artifact, known))
                    .ok_or(PluginContextOperationError::InvalidArtifact)?;
                if effective_security < known.security_classification {
                    return Err(PluginContextOperationError::InvalidSecurityProvenance);
                }
                Ok(RetrievedMemoryArtifactProvenance {
                    artifact_id: ArtifactId::from_str(&known.artifact_id)
                        .map_err(|_| PluginContextOperationError::InvalidArtifact)?,
                    artifact_reference: known.artifact_reference.clone(),
                    content_hash: known.content_hash,
                    mime_type: known.media_type.clone(),
                    byte_size: known.size_bytes,
                })
            })
            .transpose()?;
        if encoded_value.len() > 32 * 1024 {
            return Err(PluginContextOperationError::BudgetExceeded);
        }
        let references = item
            .references
            .iter()
            .map(|reference| {
                let kind = unmap_reference_kind(reference.kind);
                let known = known_references
                    .get(&(kind, reference.id.as_str()))
                    .copied()
                    .filter(|known| {
                        reference.content_hash == Some(known.content_hash)
                            && reference.id == known.id
                    })
                    .ok_or(PluginContextOperationError::InvalidReference)?;
                Ok(RetrievedMemoryReferenceProvenance {
                    kind: reference_kind_label(known.kind).to_owned(),
                    reference: known.id.clone(),
                    reference_hash: known.content_hash,
                })
            })
            .collect::<Result<Vec<_>, PluginContextOperationError>>()?;
        let entry_id = ConversationEntryId(format!(
            "plugin-memory:{}:{}",
            &identity.invocation_digest.to_hex()[..16],
            item.item_id
        ));
        let content = match &item.value {
            Value::String(value) => value.clone(),
            _ => String::from_utf8(encoded_value.clone())
                .map_err(|_| PluginContextOperationError::InvalidOutput)?,
        };
        let entry = ConversationEntry::RetrievedMemory(RetrievedMemoryEntry {
            id: entry_id.clone(),
            provider: identity.implementation_id.clone(),
            query: query.clone(),
            scope,
            source: identity.plugin_id.clone(),
            reference: item.item_id,
            score: None,
            content,
            injection_sequence: completion_sequence,
            injection_event: Some(completion_event_id),
            created_at_millis: 0,
            size_bytes: encoded_len,
            typed_provenance: Some(Box::new(RetrievedMemoryTypedProvenance {
                value: Some(item.value),
                value_hash: item.value_hash,
                artifact,
                references,
                security_classification: security_label(effective_security).to_owned(),
                plugin_invocation_id: identity.invocation_id.clone(),
                plugin_terminal_receipt_hash: ZERO_HASH,
            })),
        });
        introduced_ids.push(entry_id);
        introduced.push(entry);
    }
    let mut replacement = state.conversation.provider_projection().to_vec();
    inject_plugin_memory(
        &mut replacement,
        introduced,
        state
            .style_binding
            .as_ref()
            .ok_or(PluginContextOperationError::InvalidCommand)?
            .memory
            .injection_location
            .as_str(),
    )?;
    let proposal = PluginContextOperationProposal::MemoryRetrieve {
        replacement,
        retrieved_entry_ids: introduced_ids,
    };
    if hash_json(&proposal)?.as_bytes().is_empty() {
        return Err(PluginContextOperationError::InvalidOutput);
    }
    Ok(proposal)
}

fn validate_compaction_output(
    identity: &PluginContextOperationIdentity,
    expected_binding: &PluginOperationBindingDataRecord,
    request: &PluginContextOperationRequest,
    inputs: &PluginContextOperationInputs,
    declaration: &PluginCompactorDataRecord,
    raw: PluginCompactionProposalDataRecord,
) -> Result<PluginContextOperationProposal, PluginContextOperationError> {
    let PluginContextOperationRequest::Compaction {
        max_projection_tokens,
        preservation_requirements: _,
        artifact_references,
        references,
        ..
    } = request
    else {
        return Err(PluginContextOperationError::InvalidOutput);
    };
    if raw.binding != *expected_binding
        || raw.compactor_id != identity.implementation_id
        || raw.compactor_version != identity.implementation_version
        || raw.replacement_hash != hash_json(&raw.replacement)?
    {
        return Err(PluginContextOperationError::IdentitySubstitution);
    }
    let output = compaction_output_json(&raw);
    validate_bounded_json(&output, SchemaUse::Output)?;
    validate_json_schema(&declaration.output_schema, &output, SchemaUse::Output)?;
    let replacement_bytes = u64::try_from(
        canonical_json_bytes(&raw.replacement)
            .map_err(|_| PluginContextOperationError::InvalidOutput)?
            .len(),
    )
    .map_err(|_| PluginContextOperationError::BudgetExceeded)?;
    if replacement_bytes
        > inputs
            .max_replacement_bytes
            .ok_or(PluginContextOperationError::InvalidInput)?
    {
        return Err(PluginContextOperationError::BudgetExceeded);
    }
    let replacement: Vec<ConversationEntry> = serde_json::from_value(raw.replacement)
        .map_err(|_| PluginContextOperationError::InvalidOutput)?;
    if replacement
        .iter()
        .map(ConversationEntry::id)
        .collect::<BTreeSet<_>>()
        .len()
        != replacement.len()
    {
        return Err(PluginContextOperationError::InvalidOutput);
    }
    let measurement =
        measure_projection(&replacement).map_err(|_| PluginContextOperationError::InvalidOutput)?;
    if measurement.estimated_tokens > *max_projection_tokens {
        return Err(PluginContextOperationError::BudgetExceeded);
    }
    let preserved_artifacts = validate_preserved_artifacts(&raw.preserved_artifacts, inputs)?;
    let preserved_references = raw
        .preserved_references
        .iter()
        .map(unmap_reference)
        .collect::<Result<Vec<_>, _>>()?;
    if preserved_artifacts != inputs.artifacts
        || preserved_references != inputs.references
        || artifact_references
            != &preserved_artifacts
                .iter()
                .map(|artifact| artifact.artifact_reference.clone())
                .collect::<Vec<_>>()
        || references
            != &preserved_references
                .iter()
                .map(|reference| reference.id.clone())
                .collect::<Vec<_>>()
    {
        return Err(PluginContextOperationError::PreservationViolation);
    }
    Ok(PluginContextOperationProposal::Compaction {
        replacement,
        preserved_artifact_references: artifact_references.clone(),
        preserved_references: references.clone(),
    })
}

fn inject_plugin_memory(
    replacement: &mut Vec<ConversationEntry>,
    entries: Vec<ConversationEntry>,
    injection_location: &str,
) -> Result<(), PluginContextOperationError> {
    match injection_location {
        "before_current_input" | "beforecurrentinput" => {
            let position = replacement
                .iter()
                .rposition(|entry| matches!(entry, ConversationEntry::UserMessage(_)))
                .unwrap_or(replacement.len());
            replacement.splice(position..position, entries);
            Ok(())
        }
        "before_model_request" | "beforemodelrequest" => {
            replacement.extend(entries);
            Ok(())
        }
        "none" | "context_artifact" | "contextartifact" => {
            Err(PluginContextOperationError::InvalidInput)
        }
        _ => Err(PluginContextOperationError::InvalidInput),
    }
}

fn memory_output_json(items: &[PluginMemoryItemProposalDataRecord]) -> Value {
    json!({
        "items": items.iter().map(|item| json!({
            "item_id": item.item_id,
            "scope": memory_scope_label(item.scope),
            "value": item.value,
            "value_hash": item.value_hash,
            "artifacts": item.artifacts.iter().map(artifact_data_json).collect::<Vec<_>>(),
            "references": item.references.iter().map(reference_data_json).collect::<Vec<_>>(),
            "security_classification": security_data_label(item.security_classification),
            "metadata": item.metadata,
        })).collect::<Vec<_>>()
    })
}

fn compaction_output_json(raw: &PluginCompactionProposalDataRecord) -> Value {
    json!({
        "replacement": raw.replacement,
        "replacement_hash": raw.replacement_hash,
        "preserved_references": raw.preserved_references.iter().map(reference_data_json).collect::<Vec<_>>(),
        "preserved_artifacts": raw.preserved_artifacts.iter().map(artifact_data_json).collect::<Vec<_>>(),
    })
}

fn artifact_data_json(artifact: &PluginArtifactReferenceDataRecord) -> Value {
    json!({
        "artifact_id": artifact.artifact_id,
        "content_hash": artifact.content_hash,
        "media_type": artifact.media_type,
        "size_bytes": artifact.size_bytes,
        "security_classification": security_data_label(artifact.security_classification),
    })
}

fn reference_data_json(reference: &PluginCanonicalReferenceDataRecord) -> Value {
    json!({
        "kind": reference_kind_data_label(reference.kind),
        "id": reference.id,
        "content_hash": reference.content_hash,
    })
}

fn operation_binding(
    state: &SessionState,
    identity: &PluginContextOperationIdentity,
) -> PluginOperationBindingDataRecord {
    PluginOperationBindingDataRecord {
        plugin_id: identity.plugin_id.clone(),
        plugin_version: identity.plugin_version.clone(),
        invocation_id: identity.invocation_id.clone(),
        operation_id: identity.implementation_id.clone(),
        session_id: state.id.to_string(),
        run_id: identity.phase.boundary.run_id.clone(),
        node_id: Some(identity.phase.boundary.node_id.clone()),
        declaration_hash: identity.declaration_hash,
        configuration_reference: identity.configuration_reference,
        request_hash: identity.request_hash,
        idempotency_key: identity.idempotency_key.clone(),
        attempt: 1,
    }
}

fn map_artifact(value: &PluginContextArtifact) -> PluginArtifactReferenceDataRecord {
    PluginArtifactReferenceDataRecord {
        artifact_id: value.artifact_id.clone(),
        content_hash: value.content_hash,
        media_type: value.media_type.clone(),
        size_bytes: value.size_bytes,
        security_classification: map_security_to_data(value.security_classification),
    }
}

fn artifact_matches_known(
    value: &PluginArtifactReferenceDataRecord,
    known: &PluginContextArtifact,
) -> bool {
    value.artifact_id == known.artifact_id
        && value.content_hash == known.content_hash
        && value.media_type == known.media_type
        && value.size_bytes == known.size_bytes
        && map_security_from_data(value.security_classification) == known.security_classification
}

fn map_reference(value: &PluginContextReference) -> PluginCanonicalReferenceDataRecord {
    PluginCanonicalReferenceDataRecord {
        kind: map_reference_kind(value.kind),
        id: value.id.clone(),
        content_hash: Some(value.content_hash),
    }
}

fn unmap_reference(
    value: &PluginCanonicalReferenceDataRecord,
) -> Result<PluginContextReference, PluginContextOperationError> {
    Ok(PluginContextReference {
        kind: unmap_reference_kind(value.kind),
        id: value.id.clone(),
        content_hash: value
            .content_hash
            .ok_or(PluginContextOperationError::InvalidReference)?,
    })
}

const fn map_reference_kind(value: PluginContextReferenceKind) -> PluginCanonicalReferenceKindData {
    match value {
        PluginContextReferenceKind::Artifact => PluginCanonicalReferenceKindData::Artifact,
        PluginContextReferenceKind::NodeResult => PluginCanonicalReferenceKindData::NodeResult,
        PluginContextReferenceKind::ToolResult => PluginCanonicalReferenceKindData::ToolResult,
        PluginContextReferenceKind::ApprovalResult => {
            PluginCanonicalReferenceKindData::ApprovalResult
        }
        PluginContextReferenceKind::Continuation => PluginCanonicalReferenceKindData::Continuation,
        PluginContextReferenceKind::ChildSession => PluginCanonicalReferenceKindData::ChildSession,
    }
}

const fn unmap_reference_kind(
    value: PluginCanonicalReferenceKindData,
) -> PluginContextReferenceKind {
    match value {
        PluginCanonicalReferenceKindData::Artifact => PluginContextReferenceKind::Artifact,
        PluginCanonicalReferenceKindData::NodeResult => PluginContextReferenceKind::NodeResult,
        PluginCanonicalReferenceKindData::ToolResult => PluginContextReferenceKind::ToolResult,
        PluginCanonicalReferenceKindData::ApprovalResult => {
            PluginContextReferenceKind::ApprovalResult
        }
        PluginCanonicalReferenceKindData::Continuation => PluginContextReferenceKind::Continuation,
        PluginCanonicalReferenceKindData::ChildSession => PluginContextReferenceKind::ChildSession,
    }
}

const fn map_security_to_data(
    value: PluginContextSecurityClassification,
) -> PluginSecurityClassificationData {
    match value {
        PluginContextSecurityClassification::Public => PluginSecurityClassificationData::Public,
        PluginContextSecurityClassification::Internal => PluginSecurityClassificationData::Internal,
        PluginContextSecurityClassification::Private => PluginSecurityClassificationData::Private,
        PluginContextSecurityClassification::Confidential => {
            PluginSecurityClassificationData::Confidential
        }
    }
}

const fn map_security_from_data(
    value: PluginSecurityClassificationData,
) -> PluginContextSecurityClassification {
    match value {
        PluginSecurityClassificationData::Public => PluginContextSecurityClassification::Public,
        PluginSecurityClassificationData::Internal => PluginContextSecurityClassification::Internal,
        PluginSecurityClassificationData::Private => PluginContextSecurityClassification::Private,
        PluginSecurityClassificationData::Confidential => {
            PluginContextSecurityClassification::Confidential
        }
    }
}

fn map_memory_scopes(
    scopes: &[String],
) -> Result<BTreeSet<PluginMemoryScopeData>, PluginContextOperationError> {
    scopes.iter().map(|scope| map_memory_scope(scope)).collect()
}

fn map_memory_scope(scope: &str) -> Result<PluginMemoryScopeData, PluginContextOperationError> {
    match scope_family(scope)? {
        "session" => Ok(PluginMemoryScopeData::Session),
        "project" => Ok(PluginMemoryScopeData::Project),
        "user" => Ok(PluginMemoryScopeData::User),
        "runtime" => Ok(PluginMemoryScopeData::Runtime),
        _ => Err(PluginContextOperationError::InvalidScope),
    }
}

fn scope_family(scope: &str) -> Result<&'static str, PluginContextOperationError> {
    let (family, identity) = scope
        .split_once(':')
        .ok_or(PluginContextOperationError::InvalidScope)?;
    if !valid_identifier(identity, MAX_IDENTIFIER_BYTES) {
        return Err(PluginContextOperationError::InvalidScope);
    }
    match family {
        "session" => Ok("session"),
        "project" => Ok("project"),
        "user" => Ok("user"),
        "runtime" => Ok("runtime"),
        _ => Err(PluginContextOperationError::InvalidScope),
    }
}

fn exact_scope_label(
    scope: PluginMemoryScopeData,
    allowed: &[String],
) -> Result<String, PluginContextOperationError> {
    let family = memory_scope_label(scope);
    let mut matches = allowed
        .iter()
        .filter(|candidate| scope_family(candidate).is_ok_and(|value| value == family));
    let exact = matches
        .next()
        .ok_or(PluginContextOperationError::InvalidScope)?;
    if matches.next().is_some() {
        return Err(PluginContextOperationError::InvalidScope);
    }
    Ok(exact.clone())
}

const fn memory_scope_label(scope: PluginMemoryScopeData) -> &'static str {
    match scope {
        PluginMemoryScopeData::Session => "session",
        PluginMemoryScopeData::Project => "project",
        PluginMemoryScopeData::User => "user",
        PluginMemoryScopeData::Runtime => "runtime",
    }
}

const fn security_data_label(value: PluginSecurityClassificationData) -> &'static str {
    match value {
        PluginSecurityClassificationData::Public => "public",
        PluginSecurityClassificationData::Internal => "internal",
        PluginSecurityClassificationData::Private => "private",
        PluginSecurityClassificationData::Confidential => "confidential",
    }
}

const fn security_label(value: PluginContextSecurityClassification) -> &'static str {
    match value {
        PluginContextSecurityClassification::Public => "public",
        // Canonical conversation state historically names the internal class
        // `standard`; the typed data/protocol boundary still uses `internal`.
        PluginContextSecurityClassification::Internal => "standard",
        PluginContextSecurityClassification::Private => "private",
        PluginContextSecurityClassification::Confidential => "confidential",
    }
}

const fn reference_kind_label(value: PluginContextReferenceKind) -> &'static str {
    match value {
        PluginContextReferenceKind::Artifact => "artifact",
        PluginContextReferenceKind::NodeResult => "node_result",
        PluginContextReferenceKind::ToolResult => "tool_result",
        PluginContextReferenceKind::ApprovalResult => "approval_result",
        PluginContextReferenceKind::Continuation => "continuation",
        PluginContextReferenceKind::ChildSession => "child_session",
    }
}

const fn reference_kind_data_label(value: PluginCanonicalReferenceKindData) -> &'static str {
    reference_kind_label(unmap_reference_kind(value))
}

fn validate_preserved_artifacts(
    proposed: &[PluginArtifactReferenceDataRecord],
    inputs: &PluginContextOperationInputs,
) -> Result<Vec<PluginContextArtifact>, PluginContextOperationError> {
    if proposed.len() != inputs.artifacts.len()
        || proposed
            .iter()
            .zip(&inputs.artifacts)
            .any(|(value, known)| !artifact_matches_known(value, known))
    {
        return Err(PluginContextOperationError::PreservationViolation);
    }
    Ok(inputs.artifacts.clone())
}

fn proposal_kind(proposal: &PluginContextOperationProposal) -> PluginContextOperationKind {
    match proposal {
        PluginContextOperationProposal::MemoryRetrieve { .. } => {
            PluginContextOperationKind::MemoryRetrieve
        }
        PluginContextOperationProposal::Compaction { .. } => PluginContextOperationKind::Compaction,
    }
}

fn proposal_replacement(proposal: &PluginContextOperationProposal) -> &[ConversationEntry] {
    match proposal {
        PluginContextOperationProposal::MemoryRetrieve { replacement, .. }
        | PluginContextOperationProposal::Compaction { replacement, .. } => replacement,
    }
}

fn request_kind(request: &PluginContextOperationRequest) -> PluginContextOperationKind {
    match request {
        PluginContextOperationRequest::MemoryRetrieve { .. } => {
            PluginContextOperationKind::MemoryRetrieve
        }
        PluginContextOperationRequest::Compaction { .. } => PluginContextOperationKind::Compaction,
    }
}

fn completed_memory_event_id(
    receipt: &PluginContextOperationTerminalReceipt,
) -> Result<Option<EventId>, PluginContextOperationError> {
    let PluginContextOperationTerminalOutcome::Completed {
        proposal:
            PluginContextOperationProposal::MemoryRetrieve {
                replacement,
                retrieved_entry_ids,
            },
        ..
    } = &receipt.outcome
    else {
        return Ok(None);
    };
    let ids = retrieved_entry_ids.iter().collect::<BTreeSet<_>>();
    let event_ids = replacement
        .iter()
        .filter(|entry| ids.contains(entry.id()))
        .map(|entry| match entry {
            ConversationEntry::RetrievedMemory(memory) => memory
                .injection_event
                .ok_or(PluginContextOperationError::InvalidReceipt),
            _ => Err(PluginContextOperationError::InvalidReceipt),
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if event_ids.len() != 1 {
        return Err(PluginContextOperationError::InvalidReceipt);
    }
    Ok(event_ids.into_iter().next())
}

fn has_duplicate_artifacts(values: &[PluginArtifactReferenceDataRecord]) -> bool {
    values
        .iter()
        .map(|value| value.artifact_id.as_str())
        .collect::<BTreeSet<_>>()
        .len()
        != values.len()
}

fn has_duplicate_references(values: &[PluginCanonicalReferenceDataRecord]) -> bool {
    values
        .iter()
        .map(|value| (reference_kind_data_label(value.kind), value.id.as_str()))
        .collect::<BTreeSet<_>>()
        .len()
        != values.len()
}

fn metadata_bytes(
    metadata: &BTreeMap<String, String>,
) -> Result<usize, PluginContextOperationError> {
    serde_json::to_vec(metadata)
        .map(|bytes| bytes.len())
        .map_err(|_| PluginContextOperationError::InvalidOutput)
}

fn is_secret_metadata_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    ["secret", "token", "password", "credential", "authorization"]
        .iter()
        .any(|candidate| normalized.contains(candidate))
}

fn valid_handler(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'.')
}

fn valid_capabilities(values: &BTreeSet<String>) -> bool {
    values
        .iter()
        .all(|value| valid_identifier(value, 128) && !value.chars().any(char::is_whitespace))
}

fn valid_failure_policy(value: &str) -> bool {
    matches!(
        value,
        "reject" | "cancel" | "disable" | "continue" | "retry"
    )
}

fn valid_state_scope(value: &str) -> bool {
    matches!(
        value,
        "invocation" | "model_call" | "turn" | "session" | "project" | "user"
    )
}

fn valid_identifier(value: &str, maximum: usize) -> bool {
    !value.trim().is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:/@+-".contains(&byte))
}

fn valid_diagnostic(value: &str) -> bool {
    valid_identifier(value, 128)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn hash_json(value: &impl Serialize) -> Result<ContentHash, PluginContextOperationError> {
    serde_json::to_vec(value)
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| PluginContextOperationError::Serialization)
}

#[derive(Clone, Copy)]
enum SchemaUse {
    Input,
    Output,
}

fn validate_bounded_json(
    value: &Value,
    usage: SchemaUse,
) -> Result<(), PluginContextOperationError> {
    crate::plugin_schema::validate_bounded_json(value).map_err(|_| match usage {
        SchemaUse::Input => PluginContextOperationError::InvalidInput,
        SchemaUse::Output => PluginContextOperationError::InvalidOutput,
    })
}

fn validate_json_schema(
    schema: &str,
    value: &Value,
    usage: SchemaUse,
) -> Result<(), PluginContextOperationError> {
    crate::plugin_schema::validate_json_schema(schema, value).map_err(|error| match error {
        crate::plugin_schema::PluginSchemaValidationError::InvalidDeclaration => {
            PluginContextOperationError::DeclarationDrift
        }
        crate::plugin_schema::PluginSchemaValidationError::InvalidValue => match usage {
            SchemaUse::Input => PluginContextOperationError::InvalidInput,
            SchemaUse::Output => PluginContextOperationError::InvalidOutput,
        },
    })
}

fn emit(
    event: RuntimeCommittedEvent,
    required_event_id: Option<EventId>,
) -> DrivePluginContextOperationResult {
    DrivePluginContextOperationResult::Emit {
        event,
        required_event_id,
    }
}

fn preflight_event(
    state: &SessionState,
    event: &RuntimeCommittedEvent,
    required_event_id: Option<EventId>,
) -> Result<(), PluginContextOperationError> {
    let sequence = state
        .last_sequence
        .checked_next()
        .map_err(|_| PluginContextOperationError::Sequence)?;
    let digest = hash_json(&(
        "agentmod.plugin-context-operation-preflight.v1",
        state.id,
        sequence,
        event,
    ))?;
    let event_id = required_event_id.unwrap_or_else(|| id_from_hash(digest));
    let correlation_id = CorrelationId::from_uuid(uuid_from_hash(ContentHash::digest(
        &[digest.as_bytes().as_slice(), b"correlation"].concat(),
    )));
    let envelope = EventEnvelope::seal(
        EventMetadata {
            event_id,
            scope: EventScope::Session(state.id),
            sequence,
            timestamp: TimestampMillis::new(0),
            event_type: event.event_type().to_owned(),
            event_version: Version::new(1, 0),
            correlation_id,
            causation_id: CausationId::from_uuid(uuid_from_hash(ContentHash::digest(
                &[digest.as_bytes().as_slice(), b"causation"].concat(),
            ))),
            parent_graph_node_id: None,
            origin: EventOrigin {
                subsystem: String::from("runtime"),
                plugin: None,
            },
            schema_version: Version::new(1, 0),
            artifacts: Vec::new(),
            classification: EventClassification::Committed,
        },
        event.clone(),
    )
    .map_err(|_| PluginContextOperationError::Event)?;
    reduce(Some(state.clone()), &envelope)?;
    Ok(())
}

fn id_from_hash(hash: ContentHash) -> EventId {
    EventId::from_uuid(uuid_from_hash(hash))
}

fn uuid_from_hash(hash: ContentHash) -> Uuid {
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hash.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

const fn declaration_error_code(error: &PluginDataError) -> &'static str {
    match error {
        PluginDataError::Inactive => "plugin_context_operation_plugin_inactive",
        PluginDataError::Unavailable => "plugin_context_operation_plugin_unavailable",
        _ => "plugin_context_operation_declaration_unavailable",
    }
}

const fn plugin_data_error_is_ambiguous(error: &PluginDataError) -> bool {
    matches!(
        error,
        PluginDataError::Ambiguous { .. }
            | PluginDataError::AmbiguousContextTransform { .. }
            | PluginDataError::AmbiguousMemoryWrite { .. }
            | PluginDataError::AmbiguousStatePersistence { .. }
            | PluginDataError::AmbiguousStateRead { .. }
    )
}

const fn plugin_data_failure_code(
    error: &PluginDataError,
    kind: PluginContextOperationKind,
) -> &'static str {
    match (kind, error) {
        (_, PluginDataError::Inactive) => "plugin_context_operation_plugin_inactive",
        (_, PluginDataError::Unavailable) => "plugin_context_operation_plugin_unavailable",
        (_, PluginDataError::Cancelled) => "plugin_context_operation_cancelled",
        (_, PluginDataError::Rejected { .. }) => "plugin_context_operation_rejected",
        (
            PluginContextOperationKind::MemoryRetrieve,
            PluginDataError::MemoryOperationUnsupported,
        ) => "plugin_memory_retrieve_unsupported",
        (PluginContextOperationKind::Compaction, PluginDataError::MemoryOperationUnsupported) => {
            "plugin_compaction_unsupported"
        }
        (PluginContextOperationKind::MemoryRetrieve, _) => "plugin_memory_retrieve_failed",
        (PluginContextOperationKind::Compaction, _) => "plugin_compaction_failed",
    }
}

/// Stable runtime-logic coordinator failure.
#[derive(Debug, Error)]
pub enum PluginContextOperationError {
    /// Command does not match the replay-owned context phase.
    #[error("plugin context operation command is invalid")]
    InvalidCommand,
    /// An immutable invocation or plugin-returned identity was substituted.
    #[error("plugin context operation identity was substituted")]
    IdentitySubstitution,
    /// Exact declaration disappeared or drifted.
    #[error("plugin context operation declaration drifted")]
    DeclarationDrift,
    /// Runtime-owned input is invalid or unbounded.
    #[error("plugin context operation input is invalid")]
    InvalidInput,
    /// Plugin proposal is invalid, unbounded, or schema-incompatible.
    #[error("plugin context operation output is invalid")]
    InvalidOutput,
    /// Memory scope is undeclared or ambiguous.
    #[error("plugin context operation scope is invalid")]
    InvalidScope,
    /// Artifact provenance is unknown or inconsistent.
    #[error("plugin context operation artifact provenance is invalid")]
    InvalidArtifact,
    /// Canonical reference provenance is unknown or inconsistent.
    #[error("plugin context operation reference provenance is invalid")]
    InvalidReference,
    /// Proposed security classification is weaker than referenced content.
    #[error("plugin context operation security provenance is invalid")]
    InvalidSecurityProvenance,
    /// Proposal violates its hard item, byte, or token budget.
    #[error("plugin context operation budget was exceeded")]
    BudgetExceeded,
    /// Compaction did not preserve the exact required references or records.
    #[error("plugin context operation preservation contract was violated")]
    PreservationViolation,
    /// Policy evidence does not bind the exact action.
    #[error("plugin context operation authorization is invalid")]
    InvalidAuthorization,
    /// Ticket was not presented against its exact committed dispatch.
    #[error("plugin context operation dispatch was not committed")]
    DispatchNotCommitted,
    /// A memory completion event identity was not reserved.
    #[error("plugin context operation completion event identity is missing")]
    MissingCompletionEventIdentity,
    /// Dispatched execution lacks a receipt and is permanently ambiguous.
    #[error("plugin context operation is ambiguous and fail-closed")]
    AmbiguousFailClosed,
    /// Durable receipt is corrupt or substituted.
    #[error("plugin context operation receipt is invalid")]
    InvalidReceipt,
    /// Read-only receipt lookup failed.
    #[error("plugin context operation receipt data failed: {0}")]
    ReceiptData(PluginNodeReceiptDataError),
    /// The plugin returned but its terminal receipt could not be durably
    /// stored; replay must classify the dispatch as ambiguous.
    #[error("plugin context operation receipt persistence is ambiguous: {0}")]
    ReceiptPersistenceAmbiguous(PluginNodeReceiptDataError),
    /// Pure session reducer rejected a planned event.
    #[error("plugin context operation reducer rejected the transition: {0}")]
    Reducer(#[from] SessionReducerError),
    /// Canonical event envelope construction failed.
    #[error("plugin context operation event construction failed")]
    Event,
    /// Sequence arithmetic overflowed.
    #[error("plugin context operation sequence overflowed")]
    Sequence,
    /// JSON serialization failed.
    #[error("plugin context operation serialization failed")]
    Serialization,
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use agentmod_runtime_data::plugin::{
        ActivatePluginsDataRequest, ActivatedPluginsDataRecord, CompactPluginContextDataRequest,
        InvokePluginDataRequest, ObservePluginDataRequest, PluginDecisionDataRecord,
        PluginMemoryOperationDataRecord, PluginObservationDataRecord,
        RetrievePluginMemoryDataRequest,
    };
    use agentmod_session_style_sdk::{BuiltInStyle, CompiledSessionStyle};

    use crate::{
        conversation::TextEntry,
        session::{
            ContextBoundaryCompletedEvent, ContextBoundaryIdentity, ContextBoundaryOrigin,
            ContextBoundaryStartedEvent, ContextPhaseCompletedEvent, ContextPhaseStartedEvent,
            ConversationEntryCommittedEvent, PluginSetActivatedEvent, SessionCreatedEvent,
            SessionPluginCompactorConfiguration, SessionPluginMemoryConfiguration,
            StyleExecutionInitializedEvent, StyleNodeEnteredEvent,
        },
        style_executor::tests::binding,
    };

    use super::*;

    #[derive(Clone)]
    struct MockData {
        state: Arc<MockState>,
    }

    struct MockState {
        memory_declaration: Mutex<Option<PluginMemoryProviderDataRecord>>,
        compaction_declaration: Mutex<Option<PluginCompactorDataRecord>>,
        memory_result: Mutex<Result<PluginMemoryRetrieveProposalDataRecord, PluginDataError>>,
        compaction_result: Mutex<Result<PluginCompactionProposalDataRecord, PluginDataError>>,
        receipts: Mutex<BTreeMap<String, String>>,
        memory_pause: Mutex<Option<(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>)>>,
        declaration_queries: AtomicUsize,
        invocations: AtomicUsize,
    }

    impl MockData {
        fn new() -> Self {
            Self {
                state: Arc::new(MockState {
                    memory_declaration: Mutex::new(Some(memory_declaration())),
                    compaction_declaration: Mutex::new(Some(compaction_declaration())),
                    memory_result: Mutex::new(Err(PluginDataError::Invalid)),
                    compaction_result: Mutex::new(Err(PluginDataError::Invalid)),
                    receipts: Mutex::new(BTreeMap::new()),
                    memory_pause: Mutex::new(None),
                    declaration_queries: AtomicUsize::new(0),
                    invocations: AtomicUsize::new(0),
                }),
            }
        }

        fn invocations(&self) -> usize {
            self.state.invocations.load(Ordering::SeqCst)
        }

        fn declaration_queries(&self) -> usize {
            self.state.declaration_queries.load(Ordering::SeqCst)
        }

        fn set_memory_result(
            &self,
            result: Result<PluginMemoryRetrieveProposalDataRecord, PluginDataError>,
        ) {
            *self.state.memory_result.lock().expect("memory result") = result;
        }

        fn remove_declarations(&self) {
            *self
                .state
                .memory_declaration
                .lock()
                .expect("memory declaration") = None;
            *self
                .state
                .compaction_declaration
                .lock()
                .expect("compaction declaration") = None;
        }

        fn pause_memory_invocation(
            &self,
            started: Arc<tokio::sync::Barrier>,
            release: Arc<tokio::sync::Barrier>,
        ) {
            *self.state.memory_pause.lock().expect("memory pause") = Some((started, release));
        }
    }

    #[async_trait]
    impl PluginDataPort for MockData {
        fn memory_provider_declaration(
            &self,
            plugin_id: &str,
            provider_id: &str,
            provider_version: &str,
        ) -> Result<PluginMemoryProviderDataRecord, PluginDataError> {
            self.state
                .declaration_queries
                .fetch_add(1, Ordering::SeqCst);
            self.state
                .memory_declaration
                .lock()
                .expect("memory declaration")
                .clone()
                .filter(|declaration| {
                    plugin_id == "fixture.context"
                        && declaration.provider_id == provider_id
                        && declaration.version == provider_version
                })
                .ok_or(PluginDataError::Invalid)
        }

        fn compactor_declaration(
            &self,
            plugin_id: &str,
            compactor_id: &str,
            compactor_version: &str,
        ) -> Result<PluginCompactorDataRecord, PluginDataError> {
            self.state
                .declaration_queries
                .fetch_add(1, Ordering::SeqCst);
            self.state
                .compaction_declaration
                .lock()
                .expect("compaction declaration")
                .clone()
                .filter(|declaration| {
                    plugin_id == "fixture.context"
                        && declaration.compactor_id == compactor_id
                        && declaration.version == compactor_version
                })
                .ok_or(PluginDataError::Invalid)
        }

        async fn activate_plugins(
            &self,
            request: ActivatePluginsDataRequest,
        ) -> Result<ActivatedPluginsDataRecord, PluginDataError> {
            Ok(ActivatedPluginsDataRecord {
                plugin_ids: request.plugin_ids,
                plugins: Vec::new(),
            })
        }

        async fn invoke_plugin(
            &self,
            _request: InvokePluginDataRequest,
        ) -> Result<PluginDecisionDataRecord, PluginDataError> {
            Err(PluginDataError::Invalid)
        }

        async fn observe_event(
            &self,
            _request: ObservePluginDataRequest,
        ) -> Result<PluginObservationDataRecord, PluginDataError> {
            Err(PluginDataError::Invalid)
        }

        async fn retrieve_memory(
            &self,
            request: RetrievePluginMemoryDataRequest,
        ) -> Result<PluginMemoryRetrieveProposalDataRecord, PluginDataError> {
            self.state.invocations.fetch_add(1, Ordering::SeqCst);
            let pause = self
                .state
                .memory_pause
                .lock()
                .expect("memory pause")
                .clone();
            if let Some((started, release)) = pause {
                started.wait().await;
                release.wait().await;
            }
            tokio::task::yield_now().await;
            let mut result = self
                .state
                .memory_result
                .lock()
                .expect("memory result")
                .clone();
            if let Ok(proposal) = &mut result
                && proposal.binding.invocation_id == "replace-at-dispatch"
            {
                proposal.binding = request.binding;
                proposal.provider_id = request.provider_id;
                proposal.provider_version = request.provider_version;
            }
            result
        }

        async fn compact_context(
            &self,
            request: CompactPluginContextDataRequest,
        ) -> Result<PluginCompactionProposalDataRecord, PluginDataError> {
            self.state.invocations.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            let mut result = self
                .state
                .compaction_result
                .lock()
                .expect("compaction result")
                .clone();
            if let Ok(proposal) = &mut result
                && proposal.binding.invocation_id == "replace-at-dispatch"
            {
                proposal.binding = request.binding;
                proposal.compactor_id = request.compactor_id;
                proposal.compactor_version = request.compactor_version;
            }
            result
        }
    }

    impl PluginNodeReceiptDataPort for MockData {
        fn load_plugin_node_receipt(
            &self,
            identity: agentmod_runtime_data::plugin_receipt::PluginNodeReceiptDataIdentity,
        ) -> Result<
            Option<agentmod_runtime_data::plugin_receipt::PluginNodeReceiptDataRecord>,
            PluginNodeReceiptDataError,
        > {
            let key = format!("{}:{}", identity.session_id, identity.invocation_id);
            Ok(self
                .state
                .receipts
                .lock()
                .expect("receipts")
                .get(&key)
                .cloned()
                .map(|receipt_json| {
                    agentmod_runtime_data::plugin_receipt::PluginNodeReceiptDataRecord {
                        identity,
                        receipt_json,
                    }
                }))
        }

        fn store_plugin_node_receipt(
            &self,
            request: agentmod_runtime_data::plugin_receipt::StorePluginNodeReceiptDataRequest,
        ) -> Result<
            agentmod_runtime_data::plugin_receipt::PluginNodeReceiptDataRecord,
            PluginNodeReceiptDataError,
        > {
            let key = format!(
                "{}:{}",
                request.identity.session_id, request.identity.invocation_id
            );
            let mut receipts = self.state.receipts.lock().expect("receipts");
            if let Some(existing) = receipts.get(&key) {
                if existing != &request.receipt_json {
                    return Err(PluginNodeReceiptDataError::Conflict);
                }
            } else {
                receipts.insert(key, request.receipt_json.clone());
            }
            Ok(
                agentmod_runtime_data::plugin_receipt::PluginNodeReceiptDataRecord {
                    identity: request.identity,
                    receipt_json: request.receipt_json,
                },
            )
        }
    }

    fn session_id() -> SessionId {
        SessionId::from_uuid(
            Uuid::from_str("01900000-0000-7000-8000-000000000061").expect("session UUID"),
        )
    }

    fn completion_event_id() -> EventId {
        EventId::from_uuid(
            Uuid::from_str("01900000-0000-7000-8000-000000000062").expect("event UUID"),
        )
    }

    fn memory_declaration() -> PluginMemoryProviderDataRecord {
        PluginMemoryProviderDataRecord {
            provider_id: String::from("fixture.memory"),
            version: String::from("1.4.0"),
            runtime_api: String::from("^1.0"),
            capabilities: BTreeSet::from([String::from("memory.semantic")]),
            retrieve: PluginMemoryOperationDataRecord {
                handler: String::from("retrieve_memory"),
                input_schema: String::from(
                    r#"{"type":"object","required":["query","scopes","max_items","max_bytes","artifacts","references","parameters"],"additionalProperties":false,"properties":{"query":{"type":"string"},"scopes":{"type":"array","items":{"type":"string"}},"max_items":{"type":"integer"},"max_bytes":{"type":"integer"},"artifacts":{"type":"array"},"references":{"type":"array"},"parameters":{"type":"object"}}}"#,
                ),
                output_schema: String::from(
                    r#"{"type":"object","required":["items"],"additionalProperties":false,"properties":{"items":{"type":"array"}}}"#,
                ),
                timeout_ms: 500,
                failure_policy: String::from("retry"),
                max_attempts: 3,
                retry_backoff_ms: 5,
                idempotent: true,
                tool_permissions: BTreeSet::new(),
                network_permissions: BTreeSet::new(),
                state_scope: String::from("session"),
                external_effects: false,
            },
            write: None,
            declaration_hash: ContentHash::digest(b"memory declaration"),
        }
    }

    fn compaction_declaration() -> PluginCompactorDataRecord {
        PluginCompactorDataRecord {
            compactor_id: String::from("fixture.compactor"),
            version: String::from("3.1.0"),
            runtime_api: String::from("^1.0"),
            handler: String::from("compact_context"),
            capabilities: BTreeSet::from([String::from("compaction.semantic")]),
            input_schema: String::from(r#"{"type":"object"}"#),
            output_schema: String::from(
                r#"{"type":"object","required":["replacement","replacement_hash","preserved_references","preserved_artifacts"],"additionalProperties":false,"properties":{"replacement":{"type":"array"},"replacement_hash":{"type":"string"},"preserved_references":{"type":"array"},"preserved_artifacts":{"type":"array"}}}"#,
            ),
            timeout_ms: 500,
            failure_policy: String::from("retry"),
            max_attempts: 2,
            retry_backoff_ms: 5,
            idempotent: true,
            tool_permissions: BTreeSet::new(),
            network_permissions: BTreeSet::new(),
            state_scope: String::from("session"),
            external_effects: false,
            declaration_hash: ContentHash::digest(b"compactor declaration"),
        }
    }

    fn envelope(
        state: Option<&SessionState>,
        event: RuntimeCommittedEvent,
        event_id: Option<EventId>,
    ) -> EventEnvelope<RuntimeCommittedEvent> {
        let sequence = state.map_or(Sequence::FIRST, |state| {
            state.last_sequence.checked_next().expect("sequence")
        });
        EventEnvelope::seal(
            EventMetadata {
                event_id: event_id.unwrap_or_else(|| {
                    EventId::from_uuid(Uuid::from_u128(1000 + u128::from(sequence.get())))
                }),
                scope: EventScope::Session(session_id()),
                sequence,
                timestamp: TimestampMillis::new(1_700_000_000_000),
                event_type: event.event_type().to_owned(),
                event_version: Version::new(1, 0),
                correlation_id: CorrelationId::from_uuid(Uuid::from_u128(2000)),
                causation_id: CausationId::from_uuid(Uuid::from_u128(3000)),
                parent_graph_node_id: None,
                origin: EventOrigin {
                    subsystem: String::from("runtime"),
                    plugin: None,
                },
                schema_version: Version::new(1, 0),
                artifacts: Vec::new(),
                classification: EventClassification::Committed,
            },
            event,
        )
        .expect("event")
    }

    fn apply(
        state: SessionState,
        event: RuntimeCommittedEvent,
        event_id: Option<EventId>,
    ) -> SessionState {
        let event = envelope(Some(&state), event, event_id);
        reduce(Some(state), &event).expect("reduce event")
    }

    fn context_style(
        kind: PluginContextOperationKind,
    ) -> (
        crate::session::SessionStyleBinding,
        agentmod_graph_engine::ExecutableGraph,
        String,
    ) {
        let mut style_binding = binding(BuiltInStyle::PersistentChat);
        let plugin_id = String::from("fixture.context");
        match kind {
            PluginContextOperationKind::MemoryRetrieve => {
                style_binding.memory.provider = String::from("fixture.memory");
                style_binding.memory.plugin = Some(SessionPluginMemoryConfiguration {
                    plugin_id: plugin_id.clone(),
                    plugin_version: String::from("2.3.4"),
                    provider_id: String::from("fixture.memory"),
                    provider_version: String::from("1.4.0"),
                    declaration_hash: ContentHash::digest(b"memory declaration"),
                    configuration_reference: ContentHash::digest(b"memory configuration"),
                });
                style_binding.memory.scopes = vec![format!("session:{}", session_id())];
                style_binding.memory.retrieval_timing = String::from("turn_start");
                style_binding.memory.max_items = 4;
                style_binding.memory.max_injected_bytes = 16 * 1024;
                style_binding.memory.injection_location = String::from("before_current_input");
            }
            PluginContextOperationKind::Compaction => {
                style_binding.compaction.strategy = String::from("plugin");
                style_binding.compaction.plugin = Some(SessionPluginCompactorConfiguration {
                    plugin_id: plugin_id.clone(),
                    plugin_version: String::from("2.3.4"),
                    compactor_id: String::from("fixture.compactor"),
                    compactor_version: String::from("3.1.0"),
                    declaration_hash: ContentHash::digest(b"compactor declaration"),
                    configuration_reference: ContentHash::digest(b"compactor configuration"),
                });
                style_binding.compaction.max_provider_projection_tokens = 4_096;
                style_binding.compaction.preservation_requirements = Vec::new();
            }
        }
        let compiled: CompiledSessionStyle =
            serde_json::from_str(&style_binding.compiled_style_json).expect("compiled style");
        let mut graph = compiled.graph;
        graph.entry_index = graph
            .nodes
            .iter()
            .position(|node| node.id == "respond")
            .expect("persistent-chat model node");
        style_binding.execution_plan = None;
        style_binding.execution_plan_hash = None;
        (style_binding, graph, plugin_id)
    }

    fn enter_context_phase(
        mut state: SessionState,
        kind: PluginContextOperationKind,
        mut boundary: ContextBoundaryIdentity,
        mut phase: ContextPhaseIdentity,
    ) -> (SessionState, ContextPhaseIdentity) {
        if kind == PluginContextOperationKind::Compaction {
            let turn = ContextBoundaryIdentity {
                node_id: String::from("respond"),
                boundary: String::from("turn_start"),
                run_id: boundary.run_id.clone(),
                origin: boundary.origin,
                request_hash: boundary.request_hash,
                source_head: state.last_sequence,
            };
            let turn_memory = ContextPhaseIdentity {
                boundary: turn.clone(),
                phase: String::from("memory"),
            };
            for event in [
                RuntimeCommittedEvent::ContextBoundaryStarted(ContextBoundaryStartedEvent {
                    identity: turn.clone(),
                }),
                RuntimeCommittedEvent::ContextPhaseStarted(ContextPhaseStartedEvent {
                    identity: turn_memory.clone(),
                }),
                RuntimeCommittedEvent::ContextPhaseCompleted(ContextPhaseCompletedEvent {
                    identity: turn_memory,
                }),
            ] {
                state = apply(state, event, None);
            }
            let measurement = measure_projection(state.conversation.provider_projection())
                .expect("provider projection");
            state = apply(
                state,
                RuntimeCommittedEvent::ContextBoundaryCompleted(ContextBoundaryCompletedEvent {
                    identity: turn,
                    projection_hash: measurement.projection_hash,
                    estimated_tokens: measurement.estimated_tokens,
                    serialized_bytes: measurement.serialized_bytes,
                }),
                None,
            );
            boundary.source_head = state.last_sequence;
            phase.boundary.clone_from(&boundary);
        }
        state = apply(
            state,
            RuntimeCommittedEvent::ContextBoundaryStarted(ContextBoundaryStartedEvent {
                identity: boundary.clone(),
            }),
            None,
        );
        if kind == PluginContextOperationKind::Compaction {
            let before_memory = ContextPhaseIdentity {
                boundary,
                phase: String::from("memory"),
            };
            state = apply(
                state,
                RuntimeCommittedEvent::ContextPhaseStarted(ContextPhaseStartedEvent {
                    identity: before_memory.clone(),
                }),
                None,
            );
            state = apply(
                state,
                RuntimeCommittedEvent::ContextPhaseCompleted(ContextPhaseCompletedEvent {
                    identity: before_memory,
                }),
                None,
            );
        }
        state = apply(
            state,
            RuntimeCommittedEvent::ContextPhaseStarted(ContextPhaseStartedEvent {
                identity: phase.clone(),
            }),
            None,
        );
        (state, phase)
    }

    fn context_state(kind: PluginContextOperationKind) -> (SessionState, ContextPhaseIdentity) {
        let (style_binding, graph, plugin_id) = context_style(kind);
        let boundary = ContextBoundaryIdentity {
            node_id: String::from("respond"),
            boundary: match kind {
                PluginContextOperationKind::MemoryRetrieve => String::from("turn_start"),
                PluginContextOperationKind::Compaction => String::from("before_model_request"),
            },
            run_id: String::from("run-1"),
            origin: ContextBoundaryOrigin::UserTurn,
            request_hash: ContentHash::digest(b"request"),
            source_head: Sequence::new(5).expect("source head"),
        };
        let phase = ContextPhaseIdentity {
            boundary: boundary.clone(),
            phase: match kind {
                PluginContextOperationKind::MemoryRetrieve => String::from("memory"),
                PluginContextOperationKind::Compaction => String::from("compaction"),
            },
        };
        let user = ConversationEntry::UserMessage(TextEntry {
            id: ConversationEntryId(String::from("user:1")),
            text: String::from("remember this"),
            source_sequence: Sequence::new(2).expect("source sequence"),
        });
        let mut state = reduce(
            None,
            &envelope(
                None,
                RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                    workspace: String::from("fixture"),
                    style: style_binding.id.clone(),
                    style_binding: Some(Box::new(style_binding.clone())),
                }),
                None,
            ),
        )
        .expect("session");
        state = apply(
            state,
            RuntimeCommittedEvent::ConversationEntryCommitted(ConversationEntryCommittedEvent {
                entry: user,
            }),
            None,
        );
        state = apply(
            state,
            RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                StyleExecutionInitializedEvent {
                    graph: Box::new(graph),
                    input_reference: None,
                    execution_contract: None,
                },
            )),
            None,
        );
        state = apply(
            state,
            RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                node_id: String::from("respond"),
                attempt: 1,
                loop_iteration: 0,
                step: 1,
            }),
            None,
        );
        state = apply(
            state,
            RuntimeCommittedEvent::PluginSetActivated(PluginSetActivatedEvent {
                plugin_ids: vec![plugin_id],
                plugin_set_hash: style_binding.plugin_set_hash,
            }),
            None,
        );
        enter_context_phase(state, kind, boundary, phase)
    }

    fn memory_request() -> PluginContextOperationRequest {
        PluginContextOperationRequest::MemoryRetrieve {
            query: String::from("current goal"),
            scopes: vec![format!("session:{}", session_id())],
            max_items: 4,
            max_injected_bytes: 16 * 1024,
        }
    }

    fn inputs(kind: PluginContextOperationKind) -> PluginContextOperationInputs {
        PluginContextOperationInputs {
            readable_state: json!({
                "schema": PLUGIN_CONTEXT_READABLE_STATE_SCHEMA_V1,
                "canonical_variables": {"visible":"bounded"},
            }),
            artifacts: Vec::new(),
            references: Vec::new(),
            parameters: json!({}),
            max_replacement_bytes: (kind == PluginContextOperationKind::Compaction)
                .then_some(64 * 1024),
        }
    }

    fn command(
        state: SessionState,
        phase: ContextPhaseIdentity,
        request: PluginContextOperationRequest,
        inputs: PluginContextOperationInputs,
    ) -> DrivePluginContextOperationCommand {
        DrivePluginContextOperationCommand {
            state,
            phase,
            request,
            inputs,
            cancellation_id: String::from("cancel-1"),
            authorization: None,
            application_authorization: None,
            reserved_completion_event_id: None,
            terminal_receipt: None,
        }
    }

    fn memory_item() -> PluginMemoryItemProposalDataRecord {
        let value = json!({"memory":"typed"});
        PluginMemoryItemProposalDataRecord {
            item_id: String::from("item-1"),
            scope: PluginMemoryScopeData::Session,
            value_hash: hash_json(&value).expect("value hash"),
            value,
            artifacts: Vec::new(),
            references: Vec::new(),
            security_classification: PluginSecurityClassificationData::Private,
            metadata: BTreeMap::new(),
        }
    }

    fn sealed_memory_entry(id: &str, receipt_hash: ContentHash) -> ConversationEntry {
        ConversationEntry::RetrievedMemory(RetrievedMemoryEntry {
            id: ConversationEntryId(String::from(id)),
            provider: String::from("memory"),
            query: String::from("query"),
            scope: String::from("session"),
            source: String::from("plugin"),
            reference: String::from("item"),
            score: None,
            content: String::from("{}"),
            injection_sequence: Sequence::new(1).expect("sequence"),
            injection_event: Some(completion_event_id()),
            created_at_millis: 0,
            size_bytes: 2,
            typed_provenance: Some(Box::new(RetrievedMemoryTypedProvenance {
                value: Some(json!({})),
                value_hash: hash_json(&json!({})).expect("value hash"),
                artifact: None,
                references: Vec::new(),
                security_classification: String::from("private"),
                plugin_invocation_id: String::from("invocation"),
                plugin_terminal_receipt_hash: receipt_hash,
            })),
        })
    }

    #[test]
    fn receipt_sealing_binds_only_memory_introduced_by_this_invocation() {
        let prior_hash = ContentHash::digest(b"prior receipt");
        let next_hash = ContentHash::digest(b"next receipt");
        let introduced_id = ConversationEntryId(String::from("memory:new"));
        let mut proposal = PluginContextOperationProposal::MemoryRetrieve {
            replacement: vec![
                sealed_memory_entry("memory:prior", prior_hash),
                sealed_memory_entry(&introduced_id.0, ZERO_HASH),
            ],
            retrieved_entry_ids: vec![introduced_id],
        };

        bind_memory_receipt_hash(&mut proposal, next_hash);

        assert_eq!(
            memory_receipt_hashes(&proposal),
            Some(vec![next_hash]),
            "receipt validation must inspect only entries introduced by this invocation"
        );
        let PluginContextOperationProposal::MemoryRetrieve { replacement, .. } = proposal else {
            unreachable!();
        };
        let ConversationEntry::RetrievedMemory(prior) = &replacement[0] else {
            unreachable!();
        };
        assert_eq!(
            prior
                .typed_provenance
                .as_deref()
                .expect("prior provenance")
                .plugin_terminal_receipt_hash,
            prior_hash,
            "a later retrieval must preserve prior receipt provenance byte-for-byte"
        );
    }

    fn placeholder_binding() -> PluginOperationBindingDataRecord {
        PluginOperationBindingDataRecord {
            plugin_id: String::new(),
            plugin_version: String::new(),
            invocation_id: String::from("replace-at-dispatch"),
            operation_id: String::new(),
            session_id: String::new(),
            run_id: String::new(),
            node_id: None,
            declaration_hash: ZERO_HASH,
            configuration_reference: ZERO_HASH,
            request_hash: ZERO_HASH,
            idempotency_key: String::new(),
            attempt: 1,
        }
    }

    fn event_from(
        result: DrivePluginContextOperationResult,
    ) -> (RuntimeCommittedEvent, Option<EventId>) {
        match result {
            DrivePluginContextOperationResult::Emit {
                event,
                required_event_id,
            } => (event, required_event_id),
            other => panic!("expected event, got {other:?}"),
        }
    }

    fn propose_and_authorize_memory(
        coordinator: &ProductionPluginContextOperationCoordinator<MockData>,
        data: &MockData,
        state: SessionState,
        phase: &ContextPhaseIdentity,
        request: &PluginContextOperationRequest,
        operation_inputs: &PluginContextOperationInputs,
    ) -> (SessionState, PluginContextOperationIdentity) {
        let (event, _) = event_from(
            coordinator
                .drive(command(
                    state.clone(),
                    phase.clone(),
                    request.clone(),
                    operation_inputs.clone(),
                ))
                .expect("proposed"),
        );
        assert!(matches!(
            event,
            RuntimeCommittedEvent::PluginContextOperationProposed(_)
        ));
        let state = apply(state, event, None);
        assert_eq!(data.invocations(), 0);
        let proposal = match coordinator
            .drive(command(
                state.clone(),
                phase.clone(),
                request.clone(),
                operation_inputs.clone(),
            ))
            .expect("await authorization")
        {
            DrivePluginContextOperationResult::AwaitAuthorization { proposal } => proposal,
            other => panic!("expected authorization proposal, got {other:?}"),
        };
        let action_digest = proposal.digest().expect("action digest");
        let identity = state
            .plugin_context_operations
            .values()
            .next()
            .expect("operation")
            .identity
            .clone();
        let mut authorize = command(
            state.clone(),
            phase.clone(),
            request.clone(),
            operation_inputs.clone(),
        );
        authorize.authorization = Some(PluginContextOperationAuthorization {
            action_digest,
            authorization_digest: plugin_context_operation_authorization_digest(
                &identity,
                action_digest,
            ),
        });
        let (event, _) = event_from(coordinator.drive(authorize).expect("authorized transition"));
        (apply(state, event, None), identity)
    }

    fn recover_and_apply_memory(
        coordinator: &ProductionPluginContextOperationCoordinator<MockData>,
        data: &MockData,
        mut state: SessionState,
        phase: ContextPhaseIdentity,
        request: PluginContextOperationRequest,
        operation_inputs: PluginContextOperationInputs,
    ) {
        data.remove_declarations();
        let queries_before_recovery = data.declaration_queries();
        let (event, required_completion_id) = event_from(
            coordinator
                .drive(command(
                    state.clone(),
                    phase.clone(),
                    request.clone(),
                    operation_inputs.clone(),
                ))
                .expect("receipt completion"),
        );
        assert!(matches!(
            event,
            RuntimeCommittedEvent::PluginContextOperationCompleted(_)
        ));
        assert_eq!(data.invocations(), 1);
        assert_eq!(data.declaration_queries(), queries_before_recovery);
        assert_eq!(required_completion_id, Some(completion_event_id()));
        state = apply(state, event, required_completion_id);

        let proposal = match coordinator
            .drive(command(
                state.clone(),
                phase.clone(),
                request.clone(),
                operation_inputs.clone(),
            ))
            .expect("await replacement policy")
        {
            DrivePluginContextOperationResult::AwaitApplicationAuthorization { proposal } => {
                proposal
            }
            other => panic!("expected replacement proposal, got {other:?}"),
        };
        let mut approve = command(
            state.clone(),
            phase.clone(),
            request.clone(),
            operation_inputs.clone(),
        );
        approve.application_authorization = Some(PluginContextApplicationAuthorization {
            action_digest: proposal.digest().expect("replacement digest"),
        });
        let (event, _) = event_from(coordinator.drive(approve).expect("application approved"));
        state = apply(state, event, None);
        let (event, required_event_id) = event_from(
            coordinator
                .drive(command(
                    state.clone(),
                    phase.clone(),
                    request.clone(),
                    operation_inputs.clone(),
                ))
                .expect("application event"),
        );
        assert_eq!(required_event_id, None);
        state = apply(state, event, required_event_id);
        assert_eq!(state.conversation.provider_projection().len(), 2);
        assert!(matches!(
            coordinator
                .drive(command(state, phase, request, operation_inputs))
                .expect("applied recovery"),
            DrivePluginContextOperationResult::Terminal(
                PluginContextOperationTerminalState::Applied { .. }
            )
        ));
        assert_eq!(data.invocations(), 1);
        assert_eq!(data.declaration_queries(), queries_before_recovery);
    }

    #[tokio::test]
    async fn complete_memory_lifecycle_is_single_dispatch_and_receipt_recovery_is_live_free() {
        let data = MockData::new();
        data.set_memory_result(Ok(PluginMemoryRetrieveProposalDataRecord {
            binding: placeholder_binding(),
            provider_id: String::new(),
            provider_version: String::new(),
            items: vec![memory_item()],
        }));
        let coordinator = ProductionPluginContextOperationCoordinator::new(data.clone());
        let (state, phase) = context_state(PluginContextOperationKind::MemoryRetrieve);
        let request = memory_request();
        let operation_inputs = inputs(PluginContextOperationKind::MemoryRetrieve);
        let (mut state, identity) = propose_and_authorize_memory(
            &coordinator,
            &data,
            state,
            &phase,
            &request,
            &operation_inputs,
        );
        assert_eq!(data.invocations(), 0);

        let mut prepare = command(
            state.clone(),
            phase.clone(),
            request.clone(),
            operation_inputs.clone(),
        );
        prepare.reserved_completion_event_id = Some(completion_event_id());
        let (dispatch_event, ticket) = match coordinator.drive(prepare).expect("dispatch plan") {
            DrivePluginContextOperationResult::Dispatch { event, ticket } => (event, ticket),
            other => panic!("expected dispatch ticket, got {other:?}"),
        };
        assert!(matches!(
            coordinator.dispatch(&state, ticket).await,
            Err(PluginContextOperationError::DispatchNotCommitted)
        ));
        assert_eq!(data.invocations(), 0);

        let mut prepare = command(
            state.clone(),
            phase.clone(),
            request.clone(),
            operation_inputs.clone(),
        );
        prepare.reserved_completion_event_id = Some(completion_event_id());
        let ticket = match coordinator.drive(prepare).expect("replacement ticket") {
            DrivePluginContextOperationResult::Dispatch { ticket, .. } => ticket,
            other => panic!("expected dispatch ticket, got {other:?}"),
        };
        state = apply(state, dispatch_event, None);
        let receipt = coordinator
            .dispatch(&state, ticket)
            .await
            .expect("sealed terminal receipt");
        assert_eq!(data.invocations(), 1);
        assert_eq!(receipt.identity(), &identity);

        recover_and_apply_memory(&coordinator, &data, state, phase, request, operation_inputs);
    }

    #[tokio::test]
    async fn every_pre_dispatch_prefix_is_effect_free_and_missing_receipt_is_ambiguous_once() {
        let data = MockData::new();
        let coordinator = ProductionPluginContextOperationCoordinator::new(data.clone());
        let (mut state, phase) = context_state(PluginContextOperationKind::MemoryRetrieve);
        let request = memory_request();
        let operation_inputs = inputs(PluginContextOperationKind::MemoryRetrieve);
        let (event, _) = event_from(
            coordinator
                .drive(command(
                    state.clone(),
                    phase.clone(),
                    request.clone(),
                    operation_inputs.clone(),
                ))
                .expect("propose"),
        );
        state = apply(state, event, None);
        assert!(matches!(
            coordinator
                .drive(command(
                    state.clone(),
                    phase.clone(),
                    request.clone(),
                    operation_inputs.clone()
                ))
                .expect("policy wait"),
            DrivePluginContextOperationResult::AwaitAuthorization { .. }
        ));
        assert_eq!(data.invocations(), 0);
        let record = state
            .plugin_context_operations
            .values()
            .next()
            .expect("record");
        let proposal =
            plugin_context_operation_action_proposal(&state, &record.identity, &record.request)
                .expect("action");
        let digest = proposal.digest().expect("digest");
        let mut authorization = command(
            state.clone(),
            phase.clone(),
            request.clone(),
            operation_inputs.clone(),
        );
        authorization.authorization = Some(PluginContextOperationAuthorization {
            action_digest: digest,
            authorization_digest: plugin_context_operation_authorization_digest(
                &record.identity,
                digest,
            ),
        });
        let (event, _) = event_from(
            coordinator
                .drive(authorization)
                .expect("authorization event"),
        );
        state = apply(state, event, None);
        let mut prepare = command(
            state.clone(),
            phase.clone(),
            request.clone(),
            operation_inputs.clone(),
        );
        prepare.reserved_completion_event_id = Some(completion_event_id());
        let dispatch_event = match coordinator.drive(prepare).expect("dispatch") {
            DrivePluginContextOperationResult::Dispatch { event, .. } => event,
            other => panic!("expected dispatch, got {other:?}"),
        };
        assert_eq!(data.invocations(), 0);
        state = apply(state, dispatch_event, None);

        let (ambiguous, _) = event_from(
            coordinator
                .drive(command(
                    state.clone(),
                    phase.clone(),
                    request.clone(),
                    operation_inputs.clone(),
                ))
                .expect("missing receipt ambiguity"),
        );
        assert!(matches!(
            ambiguous,
            RuntimeCommittedEvent::PluginContextOperationAmbiguous(_)
        ));
        assert_eq!(data.invocations(), 0);
        assert_eq!(data.declaration_queries(), 2);
        state = apply(state, ambiguous, None);
        assert!(matches!(
            coordinator
                .drive(command(state, phase, request, operation_inputs))
                .expect("terminal ambiguity"),
            DrivePluginContextOperationResult::Terminal(
                PluginContextOperationTerminalState::Ambiguous { .. }
            )
        ));
        assert_eq!(data.invocations(), 0);
        assert_eq!(data.declaration_queries(), 2);
    }

    #[tokio::test]
    async fn concurrently_dispatched_tickets_reconcile_one_stored_receipt() {
        let data = MockData::new();
        data.set_memory_result(Ok(PluginMemoryRetrieveProposalDataRecord {
            binding: placeholder_binding(),
            provider_id: String::new(),
            provider_version: String::new(),
            items: vec![memory_item()],
        }));
        let coordinator = ProductionPluginContextOperationCoordinator::new(data.clone());
        let (state, phase) = context_state(PluginContextOperationKind::MemoryRetrieve);
        let request = memory_request();
        let operation_inputs = inputs(PluginContextOperationKind::MemoryRetrieve);
        let state = authorized_state(&coordinator, state, &phase, &request, &operation_inputs);
        let ticket = |state: &SessionState| {
            let mut command = command(
                state.clone(),
                phase.clone(),
                request.clone(),
                operation_inputs.clone(),
            );
            command.reserved_completion_event_id = Some(completion_event_id());
            match coordinator.drive(command).expect("ticket") {
                DrivePluginContextOperationResult::Dispatch { event, ticket } => (event, ticket),
                other => panic!("expected ticket, got {other:?}"),
            }
        };
        let (dispatch_event, first) = ticket(&state);
        let (_, second) = ticket(&state);
        let dispatched = apply(state, dispatch_event, None);
        let start = Arc::new(tokio::sync::Barrier::new(3));
        let first_start = Arc::clone(&start);
        let second_start = Arc::clone(&start);
        let first_coordinator = coordinator.clone();
        let second_coordinator = coordinator.clone();
        let first_state = dispatched.clone();
        let second_state = dispatched;
        let (_, first_receipt, second_receipt) = tokio::join!(
            start.wait(),
            async move {
                first_start.wait().await;
                tokio::task::yield_now().await;
                first_coordinator.dispatch(&first_state, first).await
            },
            async move {
                second_start.wait().await;
                tokio::task::yield_now().await;
                second_coordinator.dispatch(&second_state, second).await
            },
        );
        let first_receipt = first_receipt.expect("first receipt");
        let second_receipt = second_receipt.expect("reconciled receipt");
        assert_eq!(first_receipt, second_receipt);
        assert_eq!(data.invocations(), 1);
    }

    #[tokio::test]
    async fn cancelled_live_dispatch_retains_claim_and_cannot_be_redispatched() {
        let data = MockData::new();
        data.set_memory_result(Ok(PluginMemoryRetrieveProposalDataRecord {
            binding: placeholder_binding(),
            provider_id: String::new(),
            provider_version: String::new(),
            items: vec![memory_item()],
        }));
        let started = Arc::new(tokio::sync::Barrier::new(2));
        let release = Arc::new(tokio::sync::Barrier::new(2));
        data.pause_memory_invocation(Arc::clone(&started), release);
        let coordinator = ProductionPluginContextOperationCoordinator::new(data.clone());
        let (state, phase) = context_state(PluginContextOperationKind::MemoryRetrieve);
        let request = memory_request();
        let operation_inputs = inputs(PluginContextOperationKind::MemoryRetrieve);
        let state = authorized_state(&coordinator, state, &phase, &request, &operation_inputs);
        let ticket = |state: &SessionState| {
            let mut command = command(
                state.clone(),
                phase.clone(),
                request.clone(),
                operation_inputs.clone(),
            );
            command.reserved_completion_event_id = Some(completion_event_id());
            match coordinator.drive(command).expect("ticket") {
                DrivePluginContextOperationResult::Dispatch { event, ticket } => (event, ticket),
                other => panic!("expected ticket, got {other:?}"),
            }
        };
        let (dispatch_event, first) = ticket(&state);
        let (_, second) = ticket(&state);
        let dispatched = apply(state, dispatch_event, None);
        let invoking_coordinator = coordinator.clone();
        let invoking_state = dispatched.clone();
        let invocation =
            tokio::spawn(
                async move { invoking_coordinator.dispatch(&invoking_state, first).await },
            );
        started.wait().await;
        invocation.abort();
        assert!(
            invocation
                .await
                .expect_err("dispatch aborted")
                .is_cancelled()
        );
        assert!(matches!(
            coordinator.dispatch(&dispatched, second).await,
            Err(PluginContextOperationError::AmbiguousFailClosed)
        ));
        assert_eq!(data.invocations(), 1);
    }

    #[test]
    fn raw_secrets_credentials_and_external_handles_never_enter_proposed_state() {
        let data = MockData::new();
        let coordinator = ProductionPluginContextOperationCoordinator::new(data.clone());
        let (state, phase) = context_state(PluginContextOperationKind::MemoryRetrieve);
        let request = memory_request();
        let base = inputs(PluginContextOperationKind::MemoryRetrieve);
        let hostile_states = [
            json!({
                "schema": PLUGIN_CONTEXT_READABLE_STATE_SCHEMA_V1,
                "canonical_variables": {"password":"correct-horse"},
            }),
            json!({
                "schema": PLUGIN_CONTEXT_READABLE_STATE_SCHEMA_V1,
                "canonical_variables": {"api_token":"opaque"},
            }),
            json!({
                "schema": PLUGIN_CONTEXT_READABLE_STATE_SCHEMA_V1,
                "canonical_variables": {"token":"opaque"},
            }),
            json!({
                "schema": PLUGIN_CONTEXT_READABLE_STATE_SCHEMA_V1,
                "context_metadata": {"authorization":"Bearer credential"},
            }),
            json!({
                "schema": PLUGIN_CONTEXT_READABLE_STATE_SCHEMA_V1,
                "canonical_variables": {"visible":"Bearer credential"},
            }),
            json!({
                "schema": PLUGIN_CONTEXT_READABLE_STATE_SCHEMA_V1,
                "recorded_runtime_values": {"process_handle":1234},
            }),
            json!({
                "schema": PLUGIN_CONTEXT_READABLE_STATE_SCHEMA_V1,
                "recorded_runtime_values": {"visible":"file:///private/runtime.sock"},
            }),
            json!({
                "schema": PLUGIN_CONTEXT_READABLE_STATE_SCHEMA_V1,
                "recorded_runtime_values": {"visible":"HANDLE:0000000000000042"},
            }),
            json!({
                "schema": "unowned.schema.v1",
                "canonical_variables": {"visible":"bounded"},
            }),
            json!({
                "schema": PLUGIN_CONTEXT_READABLE_STATE_SCHEMA_V1,
                "undeclared_projection": {"visible":"bounded"},
            }),
        ];
        for readable_state in hostile_states {
            let mut operation_inputs = base.clone();
            operation_inputs.readable_state = readable_state;
            assert!(matches!(
                coordinator.drive(command(
                    state.clone(),
                    phase.clone(),
                    request.clone(),
                    operation_inputs,
                )),
                Err(PluginContextOperationError::InvalidInput)
            ));
        }
        let mut hostile_parameters = base;
        hostile_parameters.parameters = json!({"credential":"sk-private-material"});
        assert!(matches!(
            coordinator.drive(command(
                state.clone(),
                phase.clone(),
                request,
                hostile_parameters,
            )),
            Err(PluginContextOperationError::InvalidInput)
        ));
        assert!(state.plugin_context_operations.is_empty());
        assert_eq!(data.invocations(), 0);
        assert_eq!(data.declaration_queries(), 0);
    }

    #[test]
    fn invalid_input_schema_and_tampered_receipt_fail_closed_before_effect_or_replay() {
        let data = MockData::new();
        let coordinator = ProductionPluginContextOperationCoordinator::new(data.clone());
        let (state, phase) = context_state(PluginContextOperationKind::MemoryRetrieve);
        let request = memory_request();
        let operation_inputs = inputs(PluginContextOperationKind::MemoryRetrieve);
        let state = authorized_state(&coordinator, state, &phase, &request, &operation_inputs);
        data.state
            .memory_declaration
            .lock()
            .expect("declaration")
            .as_mut()
            .expect("declaration")
            .retrieve
            .input_schema = String::from(r#"{"type":"string"}"#);
        let mut drive = command(state, phase, request, operation_inputs);
        drive.reserved_completion_event_id = Some(completion_event_id());
        assert!(matches!(
            coordinator.drive(drive),
            Err(PluginContextOperationError::InvalidInput)
        ));
        assert_eq!(data.invocations(), 0);

        let identity = PluginContextOperationIdentity {
            kind: PluginContextOperationKind::MemoryRetrieve,
            phase: ContextPhaseIdentity {
                boundary: ContextBoundaryIdentity {
                    node_id: String::from("respond"),
                    boundary: String::from("turn_start"),
                    run_id: String::from("run"),
                    origin: ContextBoundaryOrigin::UserTurn,
                    request_hash: ContentHash::digest(b"request"),
                    source_head: Sequence::FIRST,
                },
                phase: String::from("memory"),
            },
            plugin_id: String::from("fixture.context"),
            plugin_version: String::from("2.3.4"),
            implementation_id: String::from("fixture.memory"),
            implementation_version: String::from("1.4.0"),
            declaration_hash: ContentHash::digest(b"declaration"),
            configuration_reference: ContentHash::digest(b"configuration"),
            handler: String::from("fixture.memory.retrieve"),
            timeout_ms: 1_000,
            idempotent: true,
            request_hash: ContentHash::digest(b"request"),
            readable_state_hash: ContentHash::digest(b"state"),
            invocation_digest: ContentHash::digest(b"invocation"),
            invocation_id: String::from("plugin-context-operation:tamper"),
            idempotency_key: String::from("plugin-context-operation-once:tamper"),
            attempt: 1,
        };
        let mut receipt = PluginContextOperationTerminalReceipt::seal(
            identity,
            PluginContextOperationTerminalOutcome::Failed {
                code: String::from("plugin_memory_retrieve_failed"),
            },
        )
        .expect("receipt");
        receipt.receipt_hash = ContentHash::digest(b"tampered");
        assert!(matches!(
            receipt.validate(),
            Err(PluginContextOperationError::InvalidReceipt)
        ));
    }

    #[test]
    fn authorization_and_input_identity_substitution_fail_before_dispatch() {
        let data = MockData::new();
        let coordinator = ProductionPluginContextOperationCoordinator::new(data);
        let (state, phase) = context_state(PluginContextOperationKind::MemoryRetrieve);
        let request = memory_request();
        let operation_inputs = inputs(PluginContextOperationKind::MemoryRetrieve);
        let (event, _) = event_from(
            coordinator
                .drive(command(
                    state.clone(),
                    phase.clone(),
                    request.clone(),
                    operation_inputs.clone(),
                ))
                .expect("proposed"),
        );
        let proposed = apply(state, event, None);
        let mut wrong_authorization = command(
            proposed.clone(),
            phase.clone(),
            request.clone(),
            operation_inputs.clone(),
        );
        wrong_authorization.authorization = Some(PluginContextOperationAuthorization {
            action_digest: ContentHash::digest(b"substituted action"),
            authorization_digest: ContentHash::digest(b"substituted authorization"),
        });
        assert!(matches!(
            coordinator.drive(wrong_authorization),
            Err(PluginContextOperationError::InvalidAuthorization)
        ));

        let mut changed_inputs = operation_inputs;
        changed_inputs.parameters = json!({"substituted":true});
        assert!(matches!(
            coordinator.drive(command(
                proposed.clone(),
                phase.clone(),
                request.clone(),
                changed_inputs
            )),
            Err(PluginContextOperationError::InvalidCommand)
        ));
        let mut changed_request = request;
        if let PluginContextOperationRequest::MemoryRetrieve { query, .. } = &mut changed_request {
            *query = String::from("substituted");
        }
        assert!(matches!(
            coordinator.drive(command(
                proposed,
                phase,
                changed_request,
                inputs(PluginContextOperationKind::MemoryRetrieve)
            )),
            Err(PluginContextOperationError::InvalidCommand)
        ));
    }

    fn prepare_dispatched(
        coordinator: &ProductionPluginContextOperationCoordinator<MockData>,
        mut state: SessionState,
        phase: &ContextPhaseIdentity,
        request: &PluginContextOperationRequest,
        operation_inputs: &PluginContextOperationInputs,
        completion_event_id: Option<EventId>,
    ) -> (SessionState, PluginContextOperationDispatchTicket) {
        let (event, _) = event_from(
            coordinator
                .drive(command(
                    state.clone(),
                    phase.clone(),
                    request.clone(),
                    operation_inputs.clone(),
                ))
                .expect("proposed"),
        );
        state = apply(state, event, None);
        let record = state
            .plugin_context_operations
            .values()
            .next()
            .expect("record");
        let proposal =
            plugin_context_operation_action_proposal(&state, &record.identity, &record.request)
                .expect("action proposal");
        let action_digest = proposal.digest().expect("action digest");
        let mut authorize = command(
            state.clone(),
            phase.clone(),
            request.clone(),
            operation_inputs.clone(),
        );
        authorize.authorization = Some(PluginContextOperationAuthorization {
            action_digest,
            authorization_digest: plugin_context_operation_authorization_digest(
                &record.identity,
                action_digest,
            ),
        });
        let (event, _) = event_from(coordinator.drive(authorize).expect("authorized event"));
        state = apply(state, event, None);
        let mut prepare = command(
            state.clone(),
            phase.clone(),
            request.clone(),
            operation_inputs.clone(),
        );
        prepare.reserved_completion_event_id = completion_event_id;
        let (event, ticket) = match coordinator.drive(prepare).expect("dispatch plan") {
            DrivePluginContextOperationResult::Dispatch { event, ticket } => (event, ticket),
            other => panic!("expected dispatch, got {other:?}"),
        };
        state = apply(state, event, None);
        (state, ticket)
    }

    fn authorized_state(
        coordinator: &ProductionPluginContextOperationCoordinator<MockData>,
        mut state: SessionState,
        phase: &ContextPhaseIdentity,
        request: &PluginContextOperationRequest,
        operation_inputs: &PluginContextOperationInputs,
    ) -> SessionState {
        let (event, _) = event_from(
            coordinator
                .drive(command(
                    state.clone(),
                    phase.clone(),
                    request.clone(),
                    operation_inputs.clone(),
                ))
                .expect("proposed"),
        );
        state = apply(state, event, None);
        let record = state
            .plugin_context_operations
            .values()
            .next()
            .expect("record");
        let proposal =
            plugin_context_operation_action_proposal(&state, &record.identity, &record.request)
                .expect("proposal");
        let action_digest = proposal.digest().expect("digest");
        let mut authorize = command(
            state.clone(),
            phase.clone(),
            request.clone(),
            operation_inputs.clone(),
        );
        authorize.authorization = Some(PluginContextOperationAuthorization {
            action_digest,
            authorization_digest: plugin_context_operation_authorization_digest(
                &record.identity,
                action_digest,
            ),
        });
        let (event, _) = event_from(coordinator.drive(authorize).expect("authorization event"));
        apply(state, event, None)
    }

    #[test]
    fn declaration_runtime_handler_effect_timeout_and_configuration_drift_fail_before_effect() {
        for variant in [
            "declaration",
            "runtime_api",
            "handler",
            "idempotency",
            "external_effects",
            "timeout",
            "configuration",
        ] {
            let data = MockData::new();
            let coordinator = ProductionPluginContextOperationCoordinator::new(data.clone());
            let (state, phase) = context_state(PluginContextOperationKind::MemoryRetrieve);
            let request = memory_request();
            let operation_inputs = inputs(PluginContextOperationKind::MemoryRetrieve);
            let mut state =
                authorized_state(&coordinator, state, &phase, &request, &operation_inputs);
            if variant == "configuration" {
                state
                    .style_binding
                    .as_mut()
                    .expect("binding")
                    .memory
                    .plugin
                    .as_mut()
                    .expect("plugin")
                    .configuration_reference = ContentHash::digest(b"substituted");
            } else {
                let mut guard = data.state.memory_declaration.lock().expect("declaration");
                let declaration = guard.as_mut().expect("declaration");
                match variant {
                    "declaration" => {
                        declaration.declaration_hash = ContentHash::digest(b"substituted");
                    }
                    "runtime_api" => declaration.runtime_api = String::from("^9"),
                    "handler" => declaration.retrieve.handler = String::from("bad-handler"),
                    "idempotency" => declaration.retrieve.idempotent = false,
                    "external_effects" => declaration.retrieve.external_effects = true,
                    "timeout" => declaration.retrieve.timeout_ms = 0,
                    _ => unreachable!(),
                }
            }
            let mut drive = command(
                state.clone(),
                phase.clone(),
                request.clone(),
                operation_inputs.clone(),
            );
            drive.reserved_completion_event_id = Some(completion_event_id());
            let (event, _) = event_from(coordinator.drive(drive).expect("fail-closed event"));
            assert!(
                matches!(
                    event,
                    RuntimeCommittedEvent::PluginContextOperationFailed(_)
                ),
                "variant {variant}"
            );
            assert_eq!(data.invocations(), 0);
            state = apply(state, event, None);
            assert!(matches!(
                coordinator
                    .drive(command(state, phase, request, operation_inputs))
                    .expect("terminal failure"),
                DrivePluginContextOperationResult::Terminal(
                    PluginContextOperationTerminalState::Failed { .. }
                )
            ));
        }
    }

    async fn memory_terminal_event(
        data: &MockData,
        request: PluginContextOperationRequest,
        operation_inputs: PluginContextOperationInputs,
    ) -> RuntimeCommittedEvent {
        let coordinator = ProductionPluginContextOperationCoordinator::new(data.clone());
        let (state, phase) = context_state(PluginContextOperationKind::MemoryRetrieve);
        let (state, ticket) = prepare_dispatched(
            &coordinator,
            state,
            &phase,
            &request,
            &operation_inputs,
            Some(completion_event_id()),
        );
        coordinator
            .dispatch(&state, ticket)
            .await
            .expect("terminal receipt");
        event_from(
            coordinator
                .drive(command(state, phase, request, operation_inputs))
                .expect("terminal event"),
        )
        .0
    }

    #[tokio::test]
    async fn hostile_memory_schema_hash_scope_identity_and_bounds_become_sealed_failures() {
        #[derive(Clone, Copy)]
        enum Mutation {
            Schema,
            Hash,
            Scope,
            Identity,
            Budget,
            DefiniteFailure,
        }
        for mutation in [
            Mutation::Schema,
            Mutation::Hash,
            Mutation::Scope,
            Mutation::Identity,
            Mutation::Budget,
            Mutation::DefiniteFailure,
        ] {
            let data = MockData::new();
            let mut request = memory_request();
            let mut item = memory_item();
            match mutation {
                Mutation::Schema => {
                    data.state
                        .memory_declaration
                        .lock()
                        .expect("declaration")
                        .as_mut()
                        .expect("declaration")
                        .retrieve
                        .output_schema = String::from(r#"{"type":"string"}"#);
                }
                Mutation::Hash => item.value_hash = ContentHash::digest(b"wrong"),
                Mutation::Scope => item.scope = PluginMemoryScopeData::Project,
                Mutation::Identity | Mutation::DefiniteFailure => {}
                Mutation::Budget => {
                    if let PluginContextOperationRequest::MemoryRetrieve {
                        max_injected_bytes,
                        ..
                    } = &mut request
                    {
                        *max_injected_bytes = 2;
                    }
                }
            }
            if matches!(mutation, Mutation::DefiniteFailure) {
                data.set_memory_result(Err(PluginDataError::Rejected {
                    operation: String::from("retrieve_memory"),
                    code: String::from("denied"),
                    retryable: false,
                }));
            } else {
                let mut binding = placeholder_binding();
                if matches!(mutation, Mutation::Identity) {
                    binding.invocation_id = String::from("substituted-invocation");
                }
                data.set_memory_result(Ok(PluginMemoryRetrieveProposalDataRecord {
                    binding,
                    provider_id: String::new(),
                    provider_version: String::new(),
                    items: vec![item],
                }));
            }
            let event = memory_terminal_event(
                &data,
                request,
                inputs(PluginContextOperationKind::MemoryRetrieve),
            )
            .await;
            assert!(
                matches!(
                    event,
                    RuntimeCommittedEvent::PluginContextOperationFailed(_)
                ),
                "hostile proposal must be a definite sealed failure"
            );
            assert_eq!(data.invocations(), 1);
        }
    }

    #[tokio::test]
    async fn unknown_artifact_reference_and_security_downgrade_are_rejected() {
        let known_artifact = PluginContextArtifact {
            artifact_id: String::from("01900000-0000-7000-8000-000000000070"),
            artifact_reference: String::from("artifact://known"),
            content_hash: ContentHash::digest(b"artifact"),
            media_type: String::from("application/json"),
            size_bytes: 10,
            security_classification: PluginContextSecurityClassification::Private,
        };
        let known_reference = PluginContextReference {
            kind: PluginContextReferenceKind::NodeResult,
            id: String::from("node-result:known"),
            content_hash: ContentHash::digest(b"node result"),
        };
        for variant in ["artifact", "reference", "security"] {
            let data = MockData::new();
            let mut item = memory_item();
            let mut expected_artifact = known_artifact.clone();
            if variant == "security" {
                expected_artifact.security_classification =
                    PluginContextSecurityClassification::Confidential;
            }
            let mut artifact = map_artifact(&expected_artifact);
            let mut reference = map_reference(&known_reference);
            match variant {
                "artifact" => artifact.content_hash = ContentHash::digest(b"unknown"),
                "reference" => reference.content_hash = Some(ContentHash::digest(b"unknown")),
                "security" => {
                    item.security_classification = PluginSecurityClassificationData::Public;
                }
                _ => unreachable!(),
            }
            item.artifacts = vec![artifact];
            item.references = vec![reference];
            data.set_memory_result(Ok(PluginMemoryRetrieveProposalDataRecord {
                binding: placeholder_binding(),
                provider_id: String::new(),
                provider_version: String::new(),
                items: vec![item],
            }));
            let mut operation_inputs = inputs(PluginContextOperationKind::MemoryRetrieve);
            operation_inputs.artifacts = vec![expected_artifact];
            operation_inputs.references = vec![known_reference.clone()];
            let event = memory_terminal_event(&data, memory_request(), operation_inputs).await;
            assert!(matches!(
                event,
                RuntimeCommittedEvent::PluginContextOperationFailed(_)
            ));
        }
    }

    fn compaction_request(
        state: &SessionState,
        operation_inputs: &PluginContextOperationInputs,
        max_projection_tokens: u64,
    ) -> PluginContextOperationRequest {
        let projection = state.conversation.provider_projection().to_vec();
        let measurement = measure_projection(&projection).expect("projection");
        PluginContextOperationRequest::Compaction {
            projection,
            projection_hash: measurement.projection_hash,
            estimated_tokens: measurement.estimated_tokens,
            serialized_bytes: measurement.serialized_bytes,
            max_projection_tokens,
            preservation_requirements: Vec::new(),
            artifact_references: operation_inputs
                .artifacts
                .iter()
                .map(|artifact| artifact.artifact_reference.clone())
                .collect(),
            references: operation_inputs
                .references
                .iter()
                .map(|reference| reference.id.clone())
                .collect(),
        }
    }

    async fn compaction_terminal_event(
        data: &MockData,
        operation_inputs: PluginContextOperationInputs,
        max_projection_tokens: u64,
    ) -> RuntimeCommittedEvent {
        let coordinator = ProductionPluginContextOperationCoordinator::new(data.clone());
        let (state, phase) = context_state(PluginContextOperationKind::Compaction);
        let request = compaction_request(&state, &operation_inputs, max_projection_tokens);
        let (state, ticket) = prepare_dispatched(
            &coordinator,
            state,
            &phase,
            &request,
            &operation_inputs,
            None,
        );
        coordinator
            .dispatch(&state, ticket)
            .await
            .expect("compaction receipt");
        event_from(
            coordinator
                .drive(command(state, phase, request, operation_inputs))
                .expect("compaction terminal event"),
        )
        .0
    }

    #[tokio::test]
    async fn compaction_hash_preservation_schema_and_budget_violations_are_sealed_failures() {
        for variant in ["hash", "preservation", "schema", "bytes", "tokens"] {
            let data = MockData::new();
            let mut operation_inputs = inputs(PluginContextOperationKind::Compaction);
            let mut preserved_artifacts = Vec::new();
            if variant == "preservation" {
                let artifact = PluginContextArtifact {
                    artifact_id: String::from("01900000-0000-7000-8000-000000000071"),
                    artifact_reference: String::from("artifact://required"),
                    content_hash: ContentHash::digest(b"required artifact"),
                    media_type: String::from("text/plain"),
                    size_bytes: 3,
                    security_classification: PluginContextSecurityClassification::Private,
                };
                operation_inputs.artifacts = vec![artifact];
                preserved_artifacts = Vec::new();
            }
            if variant == "bytes" {
                operation_inputs.max_replacement_bytes = Some(1);
            }
            if variant == "schema" {
                data.state
                    .compaction_declaration
                    .lock()
                    .expect("declaration")
                    .as_mut()
                    .expect("declaration")
                    .output_schema = String::from(r#"{"type":"string"}"#);
            }
            let (state, _) = context_state(PluginContextOperationKind::Compaction);
            let replacement = serde_json::to_value(state.conversation.provider_projection())
                .expect("replacement");
            let replacement_hash = if variant == "hash" {
                ContentHash::digest(b"wrong")
            } else {
                hash_json(&replacement).expect("replacement hash")
            };
            data.state
                .compaction_result
                .lock()
                .expect("result")
                .clone_from(&Ok(PluginCompactionProposalDataRecord {
                    binding: placeholder_binding(),
                    compactor_id: String::new(),
                    compactor_version: String::new(),
                    replacement,
                    replacement_hash,
                    preserved_references: Vec::new(),
                    preserved_artifacts,
                }));
            let event = compaction_terminal_event(
                &data,
                operation_inputs,
                if variant == "tokens" { 1 } else { 4_096 },
            )
            .await;
            assert!(
                matches!(
                    event,
                    RuntimeCommittedEvent::PluginContextOperationFailed(_)
                ),
                "variant {variant}"
            );
        }
    }
}
