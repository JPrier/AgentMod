//! Replay-safe coordination for immutable plugin context transforms.
//!
//! The coordinator owns canonical intent/dispatch/terminal events and durable
//! terminal receipts. Plugin output remains a proposal; applying it to the
//! provider projection is a separate runtime-policy step owned by turn logic.

use std::path::PathBuf;

use agentmod_event_model::{
    EventClassification, EventEnvelope, EventMetadata, EventOrigin, EventScope,
};
use agentmod_primitives::{CausationId, ContentHash, EventId, Sequence, SessionId, Version};
use agentmod_runtime_data::{
    identity::{AllocateEventIdentityDataRequest, EventIdentityDataPort},
    journal::JournalEventDataPort,
    plugin::{
        InvokePluginContextTransformDataRequest, PluginContextTransformDataRecord,
        PluginContextTransformLifecycleData, PluginContextTransformProposalDataRecord,
        PluginDataError, PluginDataPort,
    },
    plugin_receipt::{
        PluginInvocationReceiptDataIdentity, PluginNodeReceiptDataError, PluginNodeReceiptDataPort,
        StorePluginInvocationReceiptDataRequest,
    },
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    conversation::ConversationEntry,
    persistence::{
        CommitDurability, CompareAppendSessionEventCommand, CompareAppendSessionEventResult,
        LoadSessionCommand, SessionPersistenceLogic, SessionPersistenceLogicError,
        SessionPersistenceLogicPort,
    },
    plugin::{map_cancellation_target, plugin_invocation_cancellation_target},
    session::{
        ContextPhaseIdentity, PluginContextTransformAmbiguousEvent,
        PluginContextTransformCompletedEvent, PluginContextTransformDispatchedEvent,
        PluginContextTransformFailedEvent, PluginContextTransformIdentity,
        PluginContextTransformProposedEvent, PluginContextTransformRecovery,
        PluginContextTransformState, RuntimeCommittedEvent, SessionReducerError, SessionState,
        classify_plugin_context_transform_recovery, plugin_context_transform_identity,
        plugin_context_transform_input, reduce,
    },
};

/// One exact live or recovering plugin context-transform request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DrivePluginContextTransformCommand {
    /// Canonical owning session.
    pub session_id: SessionId,
    /// Exact session directory selected by runtime composition.
    pub session_directory: PathBuf,
    /// Owning immutable context phase.
    pub phase: ContextPhaseIdentity,
    /// Zero-based position in the compiled style selection.
    pub ordinal: u32,
    /// Runtime-owned cancellation identity.
    pub cancellation_id: String,
}

/// Successful live coordinator result. The replacement is still
/// non-authoritative until ordinary runtime replacement policy accepts it.
#[derive(Clone, Debug, PartialEq)]
pub struct PluginContextTransformTurnResult {
    /// Complete immutable invocation identity.
    pub identity: PluginContextTransformIdentity,
    /// Exact bounded typed replacement.
    pub replacement: Vec<ConversationEntry>,
    /// Hash of the exact serialized replacement.
    pub replacement_hash: ContentHash,
    /// Canonical journal head after terminal proposal reduction.
    pub last_sequence: Sequence,
}

/// Runtime-logic-owned context-transform seam consumed by generic turn
/// orchestration.
#[async_trait]
pub trait PluginContextTransformTurnPort: Send + Sync {
    /// Drives or safely recovers one exact transform through a canonical
    /// terminal proposal.
    ///
    /// # Errors
    ///
    /// Fails closed for immutable-selection drift, invalid schema/output,
    /// missing receipts after dispatch, ambiguity, or journal failures.
    async fn drive_plugin_context_transform(
        &self,
        command: DrivePluginContextTransformCommand,
    ) -> Result<PluginContextTransformTurnResult, PluginContextTransformTurnError>;
}

/// Production N-tier adapter over runtime data.
#[derive(Clone, Debug)]
pub struct ProductionPluginContextTransformTurn<D> {
    data: D,
}

impl<D> ProductionPluginContextTransformTurn<D> {
    /// Creates the production coordinator.
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}

#[async_trait]
#[allow(
    clippy::too_many_lines,
    reason = "the production drive keeps exact recovery classification, receipt handling, and terminal projection adjacent"
)]
impl<D> PluginContextTransformTurnPort for ProductionPluginContextTransformTurn<D>
where
    D: Clone
        + Send
        + Sync
        + EventIdentityDataPort
        + JournalEventDataPort
        + PluginDataPort
        + PluginNodeReceiptDataPort
        + 'static,
{
    async fn drive_plugin_context_transform(
        &self,
        command: DrivePluginContextTransformCommand,
    ) -> Result<PluginContextTransformTurnResult, PluginContextTransformTurnError> {
        if command.cancellation_id.trim().is_empty() {
            return Err(PluginContextTransformTurnError::InvalidCommand);
        }
        let persistence = SessionPersistenceLogic::new(self.data.clone());
        let loaded = persistence.load_session(LoadSessionCommand {
            session_directory: command.session_directory.clone(),
            expected_session_id: command.session_id,
        })?;
        let mut head = TransformHead {
            state: loaded.state,
            last_event_id: loaded.last_event_id,
        };
        let existing = head
            .state
            .plugin_context_transforms
            .values()
            .find(|record| record.identity.phase == command.phase)
            .cloned();
        let identity = if let Some(record) = existing {
            if record.identity.ordinal != command.ordinal {
                return Err(PluginContextTransformTurnError::InvalidCommand);
            }
            record.identity
        } else {
            let binding = head
                .state
                .style_binding
                .as_ref()
                .ok_or(PluginContextTransformTurnError::InvalidCommand)?;
            let compiled: agentmod_session_style_sdk::CompiledSessionStyle =
                serde_json::from_str(&binding.compiled_style_json)
                    .map_err(|_| PluginContextTransformTurnError::InvalidCommand)?;
            let selection = compiled
                .context_transforms
                .get(
                    usize::try_from(command.ordinal)
                        .map_err(|_| PluginContextTransformTurnError::InvalidCommand)?,
                )
                .ok_or(PluginContextTransformTurnError::InvalidCommand)?;
            let input =
                plugin_context_transform_input(&head.state, &command.phase, command.ordinal)?;
            let input_hash = hash_json(&input)?;
            let readable_state_hash = ContentHash::digest(b"{}");
            plugin_context_transform_identity(
                command.phase.clone(),
                command.ordinal,
                selection,
                input_hash,
                readable_state_hash,
            )?
        };

        if classify_plugin_context_transform_recovery(&head.state, &identity.invocation_id)
            == PluginContextTransformRecovery::NotStarted
        {
            head = self.commit(
                &persistence,
                &command.session_directory,
                head,
                RuntimeCommittedEvent::PluginContextTransformProposed(Box::new(
                    PluginContextTransformProposedEvent {
                        identity: identity.clone(),
                    },
                )),
            )?;
        }

        match classify_plugin_context_transform_recovery(&head.state, &identity.invocation_id) {
            PluginContextTransformRecovery::SafeToDispatchOnce => {
                head = self.commit(
                    &persistence,
                    &command.session_directory,
                    head,
                    RuntimeCommittedEvent::PluginContextTransformDispatched(Box::new(
                        PluginContextTransformDispatchedEvent {
                            identity: identity.clone(),
                        },
                    )),
                )?;
                let receipt = self
                    .invoke_and_seal_receipt(&head.state, &identity, &command.cancellation_id)
                    .await?;
                self.store_receipt(command.session_id, &receipt)?;
                head =
                    self.commit_receipt(&persistence, &command.session_directory, head, receipt)?;
            }
            PluginContextTransformRecovery::WaitingForTerminalReceipt => {
                let receipt = self
                    .load_receipt(command.session_id, &identity.invocation_id)?
                    .ok_or(PluginContextTransformTurnError::AmbiguousFailClosed)?;
                head =
                    self.commit_receipt(&persistence, &command.session_directory, head, receipt)?;
            }
            PluginContextTransformRecovery::AwaitingReplacementAuthorization
            | PluginContextTransformRecovery::SafeToApplyApprovedReplacement
            | PluginContextTransformRecovery::Applied => {}
            PluginContextTransformRecovery::TerminallyFailed => {
                return Err(PluginContextTransformTurnError::TerminalFailure);
            }
            PluginContextTransformRecovery::AmbiguousFailClosed => {
                return Err(PluginContextTransformTurnError::AmbiguousFailClosed);
            }
            PluginContextTransformRecovery::NotStarted => {
                return Err(PluginContextTransformTurnError::InvalidCommand);
            }
        }

        let record = head
            .state
            .plugin_context_transforms
            .get(&identity.invocation_id)
            .ok_or(PluginContextTransformTurnError::InvalidCommand)?;
        match record.state {
            PluginContextTransformState::Completed
            | PluginContextTransformState::ReplacementApproved
            | PluginContextTransformState::Applied => Ok(PluginContextTransformTurnResult {
                identity,
                replacement: record
                    .replacement
                    .clone()
                    .ok_or(PluginContextTransformTurnError::InvalidReceipt)?,
                replacement_hash: record
                    .replacement_hash
                    .ok_or(PluginContextTransformTurnError::InvalidReceipt)?,
                last_sequence: head.state.last_sequence,
            }),
            PluginContextTransformState::Failed => {
                Err(PluginContextTransformTurnError::TerminalFailure)
            }
            PluginContextTransformState::Ambiguous => {
                Err(PluginContextTransformTurnError::AmbiguousFailClosed)
            }
            PluginContextTransformState::Proposed | PluginContextTransformState::Dispatched => {
                Err(PluginContextTransformTurnError::InvalidCommand)
            }
        }
    }
}

impl<D> ProductionPluginContextTransformTurn<D>
where
    D: Clone
        + Send
        + Sync
        + EventIdentityDataPort
        + JournalEventDataPort
        + PluginDataPort
        + PluginNodeReceiptDataPort
        + 'static,
{
    async fn invoke_and_seal_receipt(
        &self,
        state: &SessionState,
        identity: &PluginContextTransformIdentity,
        cancellation_id: &str,
    ) -> Result<PluginContextTransformTerminalReceipt, PluginContextTransformTurnError> {
        let (declaration, request) = self.prepare_invocation(state, identity, cancellation_id)?;
        let outcome = Self::terminal_outcome(
            &declaration,
            self.data.invoke_context_transform(request).await,
        );
        PluginContextTransformTerminalReceipt::seal(identity.clone(), outcome)
    }

    fn prepare_invocation(
        &self,
        state: &SessionState,
        identity: &PluginContextTransformIdentity,
        cancellation_id: &str,
    ) -> Result<
        (
            PluginContextTransformDataRecord,
            InvokePluginContextTransformDataRequest,
        ),
        PluginContextTransformTurnError,
    > {
        let declaration = self.data.context_transform_declaration(
            &identity.plugin_id,
            &identity.transform_id,
            &identity.transform_version,
        )?;
        if declaration.declaration_hash != identity.declaration_hash
            || declaration.lifecycle != "before_model_request"
            || !declaration.idempotent
            || declaration.external_effects
        {
            return Err(PluginContextTransformTurnError::DeclarationDrift);
        }
        let input = plugin_context_transform_input(state, &identity.phase, identity.ordinal)?;
        if hash_json(&input)? != identity.input_hash {
            return Err(PluginContextTransformTurnError::InvalidCommand);
        }
        let readable_state = serde_json::json!({});
        validate_bounded_json(&input)?;
        validate_bounded_json(&readable_state)?;
        validate_json_schema(&declaration.input_schema, &input)?;
        let request_hash = serde_json::to_vec(&(
            "agentmod.plugin.context-transform.request.v1",
            &identity.plugin_id,
            &identity.invocation_id,
            &identity.transform_id,
            &identity.transform_version,
            "before_model_request",
            &declaration.handler,
            declaration.timeout_ms,
            identity.configuration_reference,
            &input,
            &readable_state,
        ))
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| PluginContextTransformTurnError::InvalidCommand)?;
        let cancellation_target = plugin_invocation_cancellation_target(
            &state.id.to_string(),
            &identity.phase.boundary.run_id,
            &identity.plugin_id,
            &declaration.plugin_version,
            &identity.invocation_id,
            &identity.transform_id,
            identity.declaration_hash,
            request_hash,
        )
        .map_err(|_| PluginContextTransformTurnError::InvalidCommand)?;
        let request = InvokePluginContextTransformDataRequest {
            cancellation_target: map_cancellation_target(&cancellation_target),
            session_id: state.id.to_string(),
            plugin_id: identity.plugin_id.clone(),
            invocation_id: identity.invocation_id.clone(),
            transform_id: identity.transform_id.clone(),
            transform_version: identity.transform_version.clone(),
            declaration_hash: identity.declaration_hash,
            timeout_ms: declaration.timeout_ms,
            configuration_reference: identity.configuration_reference,
            lifecycle: PluginContextTransformLifecycleData::BeforeModelRequest,
            handler: declaration.handler.clone(),
            input,
            readable_state,
            cancellation_id: cancellation_id.to_owned(),
        };
        Ok((declaration, request))
    }

    fn terminal_outcome(
        declaration: &PluginContextTransformDataRecord,
        result: Result<PluginContextTransformProposalDataRecord, PluginDataError>,
    ) -> PluginContextTransformTerminalOutcome {
        match result {
            Ok(proposal) => {
                let attempts = proposal.attempts;
                let validated = validate_bounded_json(&proposal.replacement)
                    .and_then(|()| {
                        validate_json_schema(&declaration.output_schema, &proposal.replacement)
                    })
                    .and_then(|()| {
                        serde_json::from_value::<Vec<ConversationEntry>>(proposal.replacement)
                            .map_err(|_| PluginContextTransformTurnError::InvalidOutput)
                    })
                    .and_then(|replacement| {
                        hash_json(&replacement).map(|replacement_hash| {
                            PluginContextTransformTerminalOutcome::Completed {
                                replacement,
                                replacement_hash,
                                attempts,
                            }
                        })
                    });
                match validated {
                    Ok(outcome) => outcome,
                    Err(_) => PluginContextTransformTerminalOutcome::Failed {
                        code: String::from("plugin_context_transform_invalid_output"),
                        attempts,
                    },
                }
            }
            Err(PluginDataError::AmbiguousContextTransform { .. }) => {
                PluginContextTransformTerminalOutcome::Ambiguous {
                    code: String::from("plugin_context_transform_ambiguous"),
                }
            }
            Err(error) => PluginContextTransformTerminalOutcome::Failed {
                code: plugin_data_error_code(&error).to_owned(),
                attempts: 0,
            },
        }
    }

    fn load_receipt(
        &self,
        session_id: SessionId,
        invocation_id: &str,
    ) -> Result<Option<PluginContextTransformTerminalReceipt>, PluginContextTransformTurnError>
    {
        self.data
            .load_plugin_invocation_receipt(PluginInvocationReceiptDataIdentity {
                session_id,
                invocation_id: invocation_id.to_owned(),
            })
            .map_err(PluginContextTransformTurnError::ReceiptData)?
            .map(|record| {
                let receipt: PluginContextTransformTerminalReceipt =
                    serde_json::from_str(&record.receipt_json)
                        .map_err(|_| PluginContextTransformTurnError::InvalidReceipt)?;
                receipt.validate()?;
                Ok(receipt)
            })
            .transpose()
    }

    fn store_receipt(
        &self,
        session_id: SessionId,
        receipt: &PluginContextTransformTerminalReceipt,
    ) -> Result<(), PluginContextTransformTurnError> {
        receipt.validate()?;
        self.data
            .store_plugin_invocation_receipt(StorePluginInvocationReceiptDataRequest {
                identity: PluginInvocationReceiptDataIdentity {
                    session_id,
                    invocation_id: receipt.identity.invocation_id.clone(),
                },
                receipt_json: serde_json::to_string(receipt)
                    .map_err(|_| PluginContextTransformTurnError::Serialization)?,
            })
            .map_err(PluginContextTransformTurnError::ReceiptData)?;
        Ok(())
    }

    fn commit_receipt(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_directory: &std::path::Path,
        head: TransformHead,
        receipt: PluginContextTransformTerminalReceipt,
    ) -> Result<TransformHead, PluginContextTransformTurnError> {
        receipt.validate()?;
        if receipt.identity.invocation_id.is_empty()
            || head
                .state
                .plugin_context_transforms
                .get(&receipt.identity.invocation_id)
                .is_none_or(|record| record.identity != receipt.identity)
        {
            return Err(PluginContextTransformTurnError::InvalidReceipt);
        }
        let payload = match receipt.outcome {
            PluginContextTransformTerminalOutcome::Completed {
                replacement,
                replacement_hash,
                attempts,
            } => RuntimeCommittedEvent::PluginContextTransformCompleted(Box::new(
                PluginContextTransformCompletedEvent {
                    identity: receipt.identity,
                    replacement,
                    replacement_hash,
                    attempts,
                    terminal_receipt_hash: receipt.receipt_hash,
                },
            )),
            PluginContextTransformTerminalOutcome::Failed { code, attempts } => {
                RuntimeCommittedEvent::PluginContextTransformFailed(Box::new(
                    PluginContextTransformFailedEvent {
                        identity: receipt.identity,
                        code,
                        attempts,
                        terminal_receipt_hash: receipt.receipt_hash,
                    },
                ))
            }
            PluginContextTransformTerminalOutcome::Ambiguous { code } => {
                RuntimeCommittedEvent::PluginContextTransformAmbiguous(Box::new(
                    PluginContextTransformAmbiguousEvent {
                        identity: receipt.identity,
                        code,
                        terminal_receipt_hash: receipt.receipt_hash,
                    },
                ))
            }
        };
        self.commit(persistence, session_directory, head, payload)
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "consuming the reducer-validated head prevents accidental reuse after append"
    )]
    fn commit(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_directory: &std::path::Path,
        head: TransformHead,
        payload: RuntimeCommittedEvent,
    ) -> Result<TransformHead, PluginContextTransformTurnError> {
        let allocated = self
            .data
            .allocate_event_identity(AllocateEventIdentityDataRequest)
            .map_err(|_| PluginContextTransformTurnError::Identity)?;
        let sequence = head
            .state
            .last_sequence
            .checked_next()
            .map_err(|_| PluginContextTransformTurnError::Sequence)?;
        let event = EventEnvelope::seal(
            EventMetadata {
                event_id: allocated.event_id,
                scope: EventScope::Session(head.state.id),
                sequence,
                timestamp: allocated.timestamp,
                event_type: payload.event_type().to_owned(),
                event_version: Version::new(1, 0),
                correlation_id: allocated.correlation_id,
                causation_id: CausationId::from_uuid(head.last_event_id.into_uuid()),
                parent_graph_node_id: None,
                origin: EventOrigin {
                    subsystem: String::from("runtime"),
                    plugin: None,
                },
                schema_version: Version::new(1, 0),
                artifacts: Vec::new(),
                classification: EventClassification::Committed,
            },
            payload,
        )
        .map_err(|_| PluginContextTransformTurnError::Event)?;
        let next_state = reduce(Some(head.state.clone()), &event)?;
        match persistence.compare_append_event(CompareAppendSessionEventCommand {
            session_directory: session_directory.to_owned(),
            expected_head_event_id: head.last_event_id,
            event,
            durability: CommitDurability::Data,
        })? {
            CompareAppendSessionEventResult::Appended(appended)
                if appended.event_id == allocated.event_id && appended.sequence == sequence =>
            {
                Ok(TransformHead {
                    state: next_state,
                    last_event_id: allocated.event_id,
                })
            }
            CompareAppendSessionEventResult::Appended(_)
            | CompareAppendSessionEventResult::Conflict => {
                Err(PluginContextTransformTurnError::Conflict)
            }
        }
    }
}

#[derive(Clone, Debug)]
struct TransformHead {
    state: SessionState,
    last_event_id: EventId,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
enum PluginContextTransformTerminalOutcome {
    Completed {
        replacement: Vec<ConversationEntry>,
        replacement_hash: ContentHash,
        attempts: u8,
    },
    Failed {
        code: String,
        attempts: u8,
    },
    Ambiguous {
        code: String,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct PluginContextTransformTerminalReceipt {
    identity: PluginContextTransformIdentity,
    outcome: PluginContextTransformTerminalOutcome,
    receipt_hash: ContentHash,
}

impl PluginContextTransformTerminalReceipt {
    fn seal(
        identity: PluginContextTransformIdentity,
        outcome: PluginContextTransformTerminalOutcome,
    ) -> Result<Self, PluginContextTransformTurnError> {
        let receipt_hash = receipt_hash(&identity, &outcome)?;
        Ok(Self {
            identity,
            outcome,
            receipt_hash,
        })
    }

    fn validate(&self) -> Result<(), PluginContextTransformTurnError> {
        if self.receipt_hash != receipt_hash(&self.identity, &self.outcome)? {
            return Err(PluginContextTransformTurnError::InvalidReceipt);
        }
        Ok(())
    }
}

fn receipt_hash(
    identity: &PluginContextTransformIdentity,
    outcome: &PluginContextTransformTerminalOutcome,
) -> Result<ContentHash, PluginContextTransformTurnError> {
    serde_json::to_vec(&(identity, outcome))
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| PluginContextTransformTurnError::Serialization)
}

fn hash_json(value: &impl Serialize) -> Result<ContentHash, PluginContextTransformTurnError> {
    serde_json::to_vec(value)
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| PluginContextTransformTurnError::Serialization)
}

fn validate_bounded_json(value: &serde_json::Value) -> Result<(), PluginContextTransformTurnError> {
    crate::plugin_schema::validate_bounded_json(value).map_err(map_plugin_schema_error)
}

fn validate_json_schema(
    schema: &str,
    value: &serde_json::Value,
) -> Result<(), PluginContextTransformTurnError> {
    crate::plugin_schema::validate_json_schema(schema, value).map_err(map_plugin_schema_error)
}

const fn map_plugin_schema_error(
    error: crate::plugin_schema::PluginSchemaValidationError,
) -> PluginContextTransformTurnError {
    match error {
        crate::plugin_schema::PluginSchemaValidationError::InvalidDeclaration => {
            PluginContextTransformTurnError::DeclarationDrift
        }
        crate::plugin_schema::PluginSchemaValidationError::InvalidValue => {
            PluginContextTransformTurnError::InvalidOutput
        }
    }
}

const fn plugin_data_error_code(error: &PluginDataError) -> &'static str {
    match error {
        PluginDataError::Invalid => "plugin_context_transform_invalid",
        PluginDataError::Unavailable => "plugin_context_transform_unavailable",
        PluginDataError::Inactive => "plugin_context_transform_inactive",
        PluginDataError::Rejected { .. } => "plugin_context_transform_rejected",
        PluginDataError::Ambiguous { .. }
        | PluginDataError::AmbiguousContextTransform { .. }
        | PluginDataError::AmbiguousMemoryWrite { .. }
        | PluginDataError::AmbiguousStatePersistence { .. }
        | PluginDataError::AmbiguousStateRead { .. } => "plugin_context_transform_ambiguous",
        PluginDataError::MemoryOperationUnsupported
        | PluginDataError::StatePersistenceUnsupported
        | PluginDataError::StateReadUnsupported
        | PluginDataError::UnsupportedStateScope
        | PluginDataError::StaleStateGeneration
        | PluginDataError::StateConflict
        | PluginDataError::Cancelled => "plugin_context_transform_dependency_failed",
    }
}

/// Stable plugin context-transform coordinator failure.
#[derive(Debug, Error)]
pub enum PluginContextTransformTurnError {
    /// Command or replay projection did not match the immutable style.
    #[error("plugin context-transform command is invalid")]
    InvalidCommand,
    /// Exact plugin declaration disappeared or changed.
    #[error("plugin context-transform declaration drifted")]
    DeclarationDrift,
    /// Plugin proposal was malformed, unbounded, or schema-invalid.
    #[error("plugin context-transform output is invalid")]
    InvalidOutput,
    /// A definite terminal plugin failure is canonical.
    #[error("plugin context-transform failed terminally")]
    TerminalFailure,
    /// The external boundary may have been crossed and no exact terminal
    /// receipt exists.
    #[error("plugin context-transform execution is ambiguous and fail-closed")]
    AmbiguousFailClosed,
    /// Durable receipt was corrupt, substituted, or mismatched.
    #[error("plugin context-transform terminal receipt is invalid")]
    InvalidReceipt,
    /// Receipt data boundary failed.
    #[error("plugin context-transform receipt data failed: {0}")]
    ReceiptData(PluginNodeReceiptDataError),
    /// Plugin data boundary failed before terminal classification.
    #[error("plugin context-transform data failed: {0}")]
    PluginData(#[from] PluginDataError),
    /// Canonical session persistence failed.
    #[error("plugin context-transform persistence failed: {0}")]
    Persistence(#[from] SessionPersistenceLogicError),
    /// Pure reducer rejected an event.
    #[error("plugin context-transform reducer failed: {0}")]
    Reducer(#[from] SessionReducerError),
    /// Event identity allocation failed.
    #[error("plugin context-transform event identity is unavailable")]
    Identity,
    /// Canonical event sealing failed.
    #[error("plugin context-transform event sealing failed")]
    Event,
    /// Journal head changed concurrently.
    #[error("plugin context-transform journal append conflicted")]
    Conflict,
    /// Sequence arithmetic overflowed.
    #[error("plugin context-transform sequence overflowed")]
    Sequence,
    /// Receipt serialization failed.
    #[error("plugin context-transform serialization failed")]
    Serialization,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        path::PathBuf,
        str::FromStr,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use agentmod_event_model::{EventMetadata, EventScope};
    use agentmod_graph_engine::{CompilerLimits, GraphCacheInputs, compile as compile_graph};
    use agentmod_primitives::{ByteCount, CorrelationId, TimestampMillis};
    use agentmod_runtime_data::{
        identity::{EventIdentityDataError, EventIdentityDataRecord},
        journal::{
            AppendEventDataRequest, AppendedEventDataRecord, JournalDataError,
            JournalDependencyFailureCode, JournalEventDataRecord, JournalRecoveryStatus,
            RecoverJournalDataRequest, RecoveredJournalDataRecord, ScanEventsDataRequest,
            ScannedEventsDataRecord,
        },
        plugin::{
            ActivatePluginsDataRequest, ActivatedPluginsDataRecord, InvokePluginDataRequest,
            InvokePluginNodeExecutorDataRequest, ObservePluginDataRequest,
            PluginContextTransformDataRecord, PluginContextTransformProposalDataRecord,
            PluginDecisionDataRecord, PluginNodeOutcomeDataRecord, PluginObservationDataRecord,
        },
        plugin_receipt::{
            PluginInvocationReceiptDataRecord, PluginNodeReceiptDataError,
            PluginNodeReceiptDataIdentity, PluginNodeReceiptDataRecord,
            StorePluginNodeReceiptDataRequest,
        },
    };
    use agentmod_session_style_sdk::{
        BuiltInStyle, CompiledSessionStyle, ContextTransformLifecycle, ContextTransformSelection,
    };
    use async_trait::async_trait;
    use serde_json::json;
    use uuid::Uuid;

    use crate::{
        conversation::{ConversationEntryId, TextEntry},
        persistence::{CommitSessionEventCommand, SessionPersistenceLogicPort},
        projection::measure_projection,
        session::{
            ContextBoundaryCompletedEvent, ContextBoundaryIdentity, ContextBoundaryOrigin,
            ContextBoundaryStartedEvent, ContextPhaseCompletedEvent, ContextPhaseStartedEvent,
            ConversationEntryCommittedEvent, SessionCreatedEvent, StyleExecutionInitializedEvent,
            StyleNodeEnteredEvent,
        },
        style_executor::tests::binding,
    };

    use super::*;

    struct MockDataState {
        events: Mutex<Vec<EventEnvelope<serde_json::Value>>>,
        receipts: Mutex<BTreeMap<String, String>>,
        declaration: Mutex<Option<PluginContextTransformDataRecord>>,
        outcome: Mutex<Result<PluginContextTransformProposalDataRecord, PluginDataError>>,
        calls: AtomicUsize,
        next_identity: AtomicUsize,
    }

    #[derive(Clone)]
    struct MockData {
        state: Arc<MockDataState>,
    }

    impl MockData {
        fn new(
            declaration: PluginContextTransformDataRecord,
            outcome: Result<PluginContextTransformProposalDataRecord, PluginDataError>,
        ) -> Self {
            Self {
                state: Arc::new(MockDataState {
                    events: Mutex::new(Vec::new()),
                    receipts: Mutex::new(BTreeMap::new()),
                    declaration: Mutex::new(Some(declaration)),
                    outcome: Mutex::new(outcome),
                    calls: AtomicUsize::new(0),
                    next_identity: AtomicUsize::new(10_000),
                }),
            }
        }

        fn calls(&self) -> usize {
            self.state.calls.load(Ordering::SeqCst)
        }

        fn receipt_count(&self) -> usize {
            self.state.receipts.lock().expect("receipts").len()
        }

        fn event_types(&self) -> Vec<String> {
            self.state
                .events
                .lock()
                .expect("events")
                .iter()
                .map(|event| event.metadata.event_type.clone())
                .collect()
        }

        fn replace_declaration(&self, declaration: Option<PluginContextTransformDataRecord>) {
            *self.state.declaration.lock().expect("declaration") = declaration;
        }
    }

    impl EventIdentityDataPort for MockData {
        fn allocate_event_identity(
            &self,
            _request: AllocateEventIdentityDataRequest,
        ) -> Result<EventIdentityDataRecord, EventIdentityDataError> {
            let value = self.state.next_identity.fetch_add(1, Ordering::SeqCst);
            Ok(EventIdentityDataRecord {
                event_id: EventId::from_uuid(Uuid::from_u128(value as u128)),
                correlation_id: CorrelationId::from_uuid(Uuid::from_u128(value as u128 + 20_000)),
                causation_id: CausationId::from_uuid(Uuid::from_u128(value as u128 + 40_000)),
                timestamp: TimestampMillis::new(i64::try_from(value).expect("timestamp")),
            })
        }
    }

    impl JournalEventDataPort for MockData {
        fn append_event(
            &self,
            request: AppendEventDataRequest,
        ) -> Result<AppendedEventDataRecord, JournalDataError> {
            let mut events = self.state.events.lock().expect("events");
            let expected_head = events.last().map(|event| event.metadata.event_id);
            if request.expected_head_event_id.is_some()
                && request.expected_head_event_id != expected_head
            {
                return Err(JournalDataError::Dependency {
                    code: JournalDependencyFailureCode::SequenceConflict,
                    message: String::from("fixture compare-append conflict"),
                });
            }
            if request.event.metadata.sequence.get() != events.len() as u64 + 1 {
                return Err(JournalDataError::Dependency {
                    code: JournalDependencyFailureCode::SequenceConflict,
                    message: String::from("fixture sequence conflict"),
                });
            }
            let sequence = request.event.metadata.sequence;
            let event_id = request.event.metadata.event_id;
            let envelope_checksum = request.event.integrity_checksum;
            events.push(request.event);
            Ok(AppendedEventDataRecord {
                event_id,
                sequence,
                envelope_checksum,
                journal_checksum: ContentHash::digest(
                    format!("context-journal-{}", sequence.get()).as_bytes(),
                ),
                offset: ByteCount::new((sequence.get() - 1) * 100),
                journal_bytes: ByteCount::new(sequence.get() * 100),
            })
        }

        fn scan_events(
            &self,
            _request: ScanEventsDataRequest,
        ) -> Result<ScannedEventsDataRecord, JournalDataError> {
            let events = self.state.events.lock().expect("events").clone();
            let event_count = events.len();
            let mut previous = None;
            let records = events
                .into_iter()
                .map(|event| {
                    let checksum = ContentHash::digest(
                        format!("context-journal-{}", event.metadata.sequence.get()).as_bytes(),
                    );
                    let record = JournalEventDataRecord {
                        offset: ByteCount::new((event.metadata.sequence.get() - 1) * 100),
                        event,
                        journal_checksum: checksum,
                        previous_journal_checksum: previous,
                    };
                    previous = Some(checksum);
                    record
                })
                .collect();
            Ok(ScannedEventsDataRecord {
                events: records,
                valid_bytes: ByteCount::new(event_count as u64 * 100),
            })
        }

        fn recover_journal(
            &self,
            _request: RecoverJournalDataRequest,
        ) -> Result<RecoveredJournalDataRecord, JournalDataError> {
            Ok(RecoveredJournalDataRecord {
                status: JournalRecoveryStatus::Clean,
                valid_bytes: ByteCount::new(0),
            })
        }
    }

    #[async_trait]
    impl PluginDataPort for MockData {
        fn context_transform_declaration(
            &self,
            plugin_id: &str,
            transform_id: &str,
            transform_version: &str,
        ) -> Result<PluginContextTransformDataRecord, PluginDataError> {
            self.state
                .declaration
                .lock()
                .expect("declaration")
                .clone()
                .filter(|declaration| {
                    plugin_id == "fixture.context"
                        && declaration.transform_id == transform_id
                        && declaration.version == transform_version
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

        async fn invoke_node_executor(
            &self,
            _request: InvokePluginNodeExecutorDataRequest,
        ) -> Result<PluginNodeOutcomeDataRecord, PluginDataError> {
            Err(PluginDataError::Invalid)
        }

        async fn invoke_context_transform(
            &self,
            _request: InvokePluginContextTransformDataRequest,
        ) -> Result<PluginContextTransformProposalDataRecord, PluginDataError> {
            self.state.calls.fetch_add(1, Ordering::SeqCst);
            self.state.outcome.lock().expect("outcome").clone()
        }
    }

    impl PluginNodeReceiptDataPort for MockData {
        fn load_plugin_node_receipt(
            &self,
            identity: PluginNodeReceiptDataIdentity,
        ) -> Result<Option<PluginNodeReceiptDataRecord>, PluginNodeReceiptDataError> {
            let key = format!("{}:{}", identity.session_id, identity.invocation_id);
            Ok(self
                .state
                .receipts
                .lock()
                .expect("receipts")
                .get(&key)
                .cloned()
                .map(|receipt_json| PluginInvocationReceiptDataRecord {
                    identity,
                    receipt_json,
                }))
        }

        fn store_plugin_node_receipt(
            &self,
            request: StorePluginNodeReceiptDataRequest,
        ) -> Result<PluginNodeReceiptDataRecord, PluginNodeReceiptDataError> {
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
            Ok(PluginNodeReceiptDataRecord {
                identity: request.identity,
                receipt_json: request.receipt_json,
            })
        }
    }

    struct Fixture {
        data: MockData,
        session_directory: PathBuf,
        command: DrivePluginContextTransformCommand,
        declaration: PluginContextTransformDataRecord,
    }

    fn session_id() -> SessionId {
        SessionId::from_uuid(
            Uuid::from_str("01900000-0000-7000-8000-000000000041").expect("session UUID"),
        )
    }

    fn graph() -> agentmod_graph_engine::ExecutableGraph {
        compile_graph(
            r#"
format_version = 1
entry = "respond"
[budget]
max_steps = 8
max_tokens = 100
max_cost_micros = 100
max_duration_ms = 1000
[declarations]
capabilities = ["model"]
providers = ["mock"]
[[nodes]]
id = "respond"
kind = "model_call"
provider = "mock"
[[nodes]]
id = "done"
kind = "complete_turn"
[[edges]]
from = "respond"
to = "done"
"#,
            &GraphCacheInputs {
                plugin_set_hash: ContentHash::digest(b"context-transform-test-plugins"),
                runtime_api_version: String::from("1.0.0"),
                capability_set: BTreeSet::from([String::from("model")]),
            },
            CompilerLimits::default(),
        )
        .expect("context graph")
    }

    fn declaration() -> PluginContextTransformDataRecord {
        PluginContextTransformDataRecord {
            plugin_version: String::from("1.0.0"),
            transform_id: String::from("fixture.redact"),
            version: String::from("1.0.0"),
            runtime_api: String::from("^1.0"),
            handler: String::from("redact_projection"),
            lifecycle: String::from("before_model_request"),
            capabilities: BTreeSet::from([String::from("context.redaction")]),
            input_schema: String::from(r#"{"type":"object","required":["projection"]}"#),
            output_schema: String::from(r#"{"type":"array"}"#),
            timeout_ms: 500,
            failure_policy: String::from("reject"),
            max_attempts: 1,
            retry_backoff_ms: 0,
            idempotent: true,
            tool_permissions: BTreeSet::new(),
            network_permissions: BTreeSet::new(),
            state_scope: String::from("model_call"),
            external_effects: false,
            declaration_hash: ContentHash::digest(b"fixture-context-declaration"),
        }
    }

    fn envelope(
        sequence: u64,
        payload: RuntimeCommittedEvent,
    ) -> EventEnvelope<RuntimeCommittedEvent> {
        EventEnvelope::seal(
            EventMetadata {
                event_id: EventId::from_uuid(Uuid::from_u128(800 + u128::from(sequence))),
                scope: EventScope::Session(session_id()),
                sequence: Sequence::new(sequence).expect("sequence"),
                timestamp: TimestampMillis::new(1_700_000_000_000),
                event_type: payload.event_type().to_owned(),
                event_version: Version::new(1, 0),
                correlation_id: CorrelationId::from_uuid(
                    Uuid::from_str("01900000-0000-7000-8000-000000000042")
                        .expect("correlation UUID"),
                ),
                causation_id: CausationId::from_uuid(
                    Uuid::from_str("01900000-0000-7000-8000-000000000043").expect("causation UUID"),
                ),
                parent_graph_node_id: None,
                origin: EventOrigin {
                    subsystem: String::from("runtime"),
                    plugin: None,
                },
                schema_version: Version::new(1, 0),
                artifacts: Vec::new(),
                classification: EventClassification::Committed,
            },
            payload,
        )
        .expect("event")
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the replay fixture keeps the complete ordered context boundary and phase journal visible"
    )]
    fn fixture(
        outcome: Result<PluginContextTransformProposalDataRecord, PluginDataError>,
    ) -> Fixture {
        let declaration = declaration();
        let mut style_binding = binding(BuiltInStyle::PersistentChat);
        let mut compiled: CompiledSessionStyle =
            serde_json::from_str(&style_binding.compiled_style_json).expect("compiled style");
        compiled.graph = graph();
        compiled.context_transforms = vec![ContextTransformSelection {
            plugin_id: String::from("fixture.context"),
            transform_id: declaration.transform_id.clone(),
            version: declaration.version.clone(),
            declaration_hash: declaration.declaration_hash,
            lifecycle: ContextTransformLifecycle::BeforeModelRequest,
            configuration_reference: ContentHash::digest(b"fixture-context-configuration"),
        }];
        style_binding.compiled_style_json =
            serde_json::to_string(&compiled).expect("compiled style JSON");
        style_binding.compiled_style_hash =
            ContentHash::digest(style_binding.compiled_style_json.as_bytes());
        style_binding.execution_plan = None;
        style_binding.execution_plan_hash = None;

        let data = MockData::new(declaration.clone(), outcome);
        let session_directory = PathBuf::from("fixture-context-session");
        let request_hash = ContentHash::digest(b"context-request");
        let turn_boundary = ContextBoundaryIdentity {
            node_id: String::from("respond"),
            boundary: String::from("turn_start"),
            run_id: String::from("context-run"),
            origin: ContextBoundaryOrigin::UserTurn,
            request_hash,
            source_head: Sequence::new(4).expect("source head"),
        };
        let memory_phase = ContextPhaseIdentity {
            boundary: turn_boundary.clone(),
            phase: String::from("memory"),
        };
        let boundary = ContextBoundaryIdentity {
            node_id: String::from("respond"),
            boundary: String::from("before_model_request"),
            run_id: String::from("context-run"),
            origin: ContextBoundaryOrigin::UserTurn,
            request_hash,
            source_head: Sequence::new(8).expect("source head"),
        };
        let before_memory_phase = ContextPhaseIdentity {
            boundary: boundary.clone(),
            phase: String::from("memory"),
        };
        let phase = ContextPhaseIdentity {
            boundary: boundary.clone(),
            phase: String::from("plugin_context_transform:0"),
        };
        let user = ConversationEntry::UserMessage(TextEntry {
            id: ConversationEntryId(String::from("user-1")),
            text: String::from("private context"),
            source_sequence: Sequence::new(2).expect("source sequence"),
        });
        let measurement = measure_projection(std::slice::from_ref(&user))
            .expect("provider projection measurement");
        let events = [
            envelope(
                1,
                RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                    workspace: String::from("fixture"),
                    style: style_binding.id.clone(),
                    style_binding: Some(Box::new(style_binding)),
                }),
            ),
            envelope(
                2,
                RuntimeCommittedEvent::ConversationEntryCommitted(
                    ConversationEntryCommittedEvent { entry: user },
                ),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                    StyleExecutionInitializedEvent {
                        graph: Box::new(compiled.graph),
                        input_reference: None,
                        execution_contract: None,
                    },
                )),
            ),
            envelope(
                4,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: String::from("respond"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                }),
            ),
            envelope(
                5,
                RuntimeCommittedEvent::ContextBoundaryStarted(ContextBoundaryStartedEvent {
                    identity: turn_boundary.clone(),
                }),
            ),
            envelope(
                6,
                RuntimeCommittedEvent::ContextPhaseStarted(ContextPhaseStartedEvent {
                    identity: memory_phase.clone(),
                }),
            ),
            envelope(
                7,
                RuntimeCommittedEvent::ContextPhaseCompleted(ContextPhaseCompletedEvent {
                    identity: memory_phase,
                }),
            ),
            envelope(
                8,
                RuntimeCommittedEvent::ContextBoundaryCompleted(ContextBoundaryCompletedEvent {
                    identity: turn_boundary,
                    projection_hash: measurement.projection_hash,
                    estimated_tokens: measurement.estimated_tokens,
                    serialized_bytes: measurement.serialized_bytes,
                }),
            ),
            envelope(
                9,
                RuntimeCommittedEvent::ContextBoundaryStarted(ContextBoundaryStartedEvent {
                    identity: boundary,
                }),
            ),
            envelope(
                10,
                RuntimeCommittedEvent::ContextPhaseStarted(ContextPhaseStartedEvent {
                    identity: before_memory_phase.clone(),
                }),
            ),
            envelope(
                11,
                RuntimeCommittedEvent::ContextPhaseCompleted(ContextPhaseCompletedEvent {
                    identity: before_memory_phase,
                }),
            ),
            envelope(
                12,
                RuntimeCommittedEvent::ContextPhaseStarted(ContextPhaseStartedEvent {
                    identity: phase.clone(),
                }),
            ),
        ];
        let persistence = SessionPersistenceLogic::new(data.clone());
        for event in events {
            persistence
                .commit_event(CommitSessionEventCommand {
                    session_directory: session_directory.clone(),
                    event,
                    durability: CommitDurability::Full,
                })
                .expect("seed context journal");
        }
        Fixture {
            data,
            session_directory: session_directory.clone(),
            command: DrivePluginContextTransformCommand {
                session_id: session_id(),
                session_directory,
                phase,
                ordinal: 0,
                cancellation_id: String::from("context-cancel"),
            },
            declaration,
        }
    }

    fn replacement() -> Vec<ConversationEntry> {
        vec![ConversationEntry::UserMessage(TextEntry {
            id: ConversationEntryId(String::from("user-redacted")),
            text: String::from("[redacted]"),
            source_sequence: Sequence::new(2).expect("source sequence"),
        })]
    }

    fn loaded_state(fixture: &Fixture) -> SessionState {
        SessionPersistenceLogic::new(fixture.data.clone())
            .load_session(LoadSessionCommand {
                session_directory: fixture.session_directory.clone(),
                expected_session_id: session_id(),
            })
            .expect("load state")
            .state
    }

    fn expected_identity(fixture: &Fixture) -> PluginContextTransformIdentity {
        let state = loaded_state(fixture);
        let input = plugin_context_transform_input(&state, &fixture.command.phase, 0)
            .expect("context input");
        plugin_context_transform_identity(
            fixture.command.phase.clone(),
            0,
            &ContextTransformSelection {
                plugin_id: String::from("fixture.context"),
                transform_id: fixture.declaration.transform_id.clone(),
                version: fixture.declaration.version.clone(),
                declaration_hash: fixture.declaration.declaration_hash,
                lifecycle: ContextTransformLifecycle::BeforeModelRequest,
                configuration_reference: ContentHash::digest(b"fixture-context-configuration"),
            },
            hash_json(&input).expect("input hash"),
            ContentHash::digest(b"{}"),
        )
        .expect("context identity")
    }

    fn append(fixture: &Fixture, payload: RuntimeCommittedEvent) {
        let loaded = SessionPersistenceLogic::new(fixture.data.clone())
            .load_session(LoadSessionCommand {
                session_directory: fixture.session_directory.clone(),
                expected_session_id: session_id(),
            })
            .expect("load state");
        let sequence = loaded
            .state
            .last_sequence
            .checked_next()
            .expect("next sequence");
        SessionPersistenceLogic::new(fixture.data.clone())
            .commit_event(CommitSessionEventCommand {
                session_directory: fixture.session_directory.clone(),
                event: envelope(sequence.get(), payload),
                durability: CommitDurability::Full,
            })
            .expect("append context event");
    }

    #[tokio::test]
    async fn success_recovers_from_terminal_receipt_without_redispatch() {
        let expected = replacement();
        let fixture = fixture(Ok(PluginContextTransformProposalDataRecord {
            replacement: serde_json::to_value(&expected).expect("replacement JSON"),
            attempts: 1,
        }));
        let turn = ProductionPluginContextTransformTurn::new(fixture.data.clone());
        let first = turn
            .drive_plugin_context_transform(fixture.command.clone())
            .await
            .expect("first drive");
        assert_eq!(first.replacement, expected);
        assert_eq!(fixture.data.calls(), 1);
        assert_eq!(
            &fixture.data.event_types()[12..],
            [
                "plugin.context_transform_proposed",
                "plugin.context_transform_dispatched",
                "plugin.context_transform_completed",
            ]
        );
        assert_eq!(
            classify_plugin_context_transform_recovery(
                &loaded_state(&fixture),
                &first.identity.invocation_id,
            ),
            PluginContextTransformRecovery::AwaitingReplacementAuthorization
        );

        let restarted = ProductionPluginContextTransformTurn::new(fixture.data.clone())
            .drive_plugin_context_transform(fixture.command.clone())
            .await
            .expect("pure replay restart");
        assert_eq!(restarted.replacement_hash, first.replacement_hash);
        assert_eq!(fixture.data.calls(), 1);
    }

    #[tokio::test]
    async fn dispatched_without_receipt_fails_closed_and_never_redispatches() {
        let fixture = fixture(Ok(PluginContextTransformProposalDataRecord {
            replacement: serde_json::to_value(replacement()).expect("replacement JSON"),
            attempts: 1,
        }));
        let identity = expected_identity(&fixture);
        append(
            &fixture,
            RuntimeCommittedEvent::PluginContextTransformProposed(Box::new(
                PluginContextTransformProposedEvent {
                    identity: identity.clone(),
                },
            )),
        );
        append(
            &fixture,
            RuntimeCommittedEvent::PluginContextTransformDispatched(Box::new(
                PluginContextTransformDispatchedEvent { identity },
            )),
        );

        assert!(matches!(
            ProductionPluginContextTransformTurn::new(fixture.data.clone())
                .drive_plugin_context_transform(fixture.command.clone())
                .await,
            Err(PluginContextTransformTurnError::AmbiguousFailClosed)
        ));
        assert_eq!(fixture.data.calls(), 0);
    }

    #[tokio::test]
    async fn invalid_output_is_a_durable_terminal_failure_after_dispatch() {
        let fixture = fixture(Ok(PluginContextTransformProposalDataRecord {
            replacement: json!({"not":"an array"}),
            attempts: 1,
        }));
        let turn = ProductionPluginContextTransformTurn::new(fixture.data.clone());
        assert!(matches!(
            turn.drive_plugin_context_transform(fixture.command.clone())
                .await,
            Err(PluginContextTransformTurnError::TerminalFailure)
        ));
        assert_eq!(fixture.data.calls(), 1);
        assert_eq!(fixture.data.receipt_count(), 1);
        assert!(matches!(
            turn.drive_plugin_context_transform(fixture.command.clone())
                .await,
            Err(PluginContextTransformTurnError::TerminalFailure)
        ));
        assert_eq!(fixture.data.calls(), 1);
        assert_eq!(fixture.data.receipt_count(), 1);
    }

    #[tokio::test]
    async fn declaration_unavailability_and_substitution_never_invoke_plugin() {
        let unavailable = fixture(Ok(PluginContextTransformProposalDataRecord {
            replacement: serde_json::to_value(replacement()).expect("replacement JSON"),
            attempts: 1,
        }));
        unavailable.data.replace_declaration(None);
        let unavailable_turn = ProductionPluginContextTransformTurn::new(unavailable.data.clone());
        assert!(matches!(
            unavailable_turn
                .drive_plugin_context_transform(unavailable.command.clone())
                .await,
            Err(PluginContextTransformTurnError::PluginData(
                PluginDataError::Invalid
            ))
        ));
        assert_eq!(unavailable.data.calls(), 0);

        let substituted = fixture(Ok(PluginContextTransformProposalDataRecord {
            replacement: serde_json::to_value(replacement()).expect("replacement JSON"),
            attempts: 1,
        }));
        let mut changed = substituted.declaration.clone();
        changed.declaration_hash = ContentHash::digest(b"substituted declaration");
        substituted.data.replace_declaration(Some(changed));
        let substituted_turn = ProductionPluginContextTransformTurn::new(substituted.data.clone());
        assert!(matches!(
            substituted_turn
                .drive_plugin_context_transform(substituted.command.clone())
                .await,
            Err(PluginContextTransformTurnError::DeclarationDrift)
        ));
        assert_eq!(substituted.data.calls(), 0);
    }

    #[tokio::test]
    async fn substituted_terminal_receipt_is_rejected_without_redispatch() {
        let fixture = fixture(Ok(PluginContextTransformProposalDataRecord {
            replacement: serde_json::to_value(replacement()).expect("replacement JSON"),
            attempts: 1,
        }));
        let identity = expected_identity(&fixture);
        append(
            &fixture,
            RuntimeCommittedEvent::PluginContextTransformProposed(Box::new(
                PluginContextTransformProposedEvent {
                    identity: identity.clone(),
                },
            )),
        );
        append(
            &fixture,
            RuntimeCommittedEvent::PluginContextTransformDispatched(Box::new(
                PluginContextTransformDispatchedEvent {
                    identity: identity.clone(),
                },
            )),
        );
        let mut substituted = identity.clone();
        substituted.declaration_hash = ContentHash::digest(b"receipt substitution");
        let receipt = PluginContextTransformTerminalReceipt::seal(
            substituted,
            PluginContextTransformTerminalOutcome::Completed {
                replacement: replacement(),
                replacement_hash: hash_json(&replacement()).expect("replacement hash"),
                attempts: 1,
            },
        )
        .expect("self-consistent receipt");
        fixture
            .data
            .store_plugin_invocation_receipt(StorePluginInvocationReceiptDataRequest {
                identity: PluginInvocationReceiptDataIdentity {
                    session_id: session_id(),
                    invocation_id: identity.invocation_id,
                },
                receipt_json: serde_json::to_string(&receipt).expect("receipt JSON"),
            })
            .expect("store substituted receipt");

        assert!(matches!(
            ProductionPluginContextTransformTurn::new(fixture.data.clone())
                .drive_plugin_context_transform(fixture.command.clone())
                .await,
            Err(PluginContextTransformTurnError::InvalidReceipt)
        ));
        assert_eq!(fixture.data.calls(), 0);
    }

    #[test]
    fn shared_schema_errors_preserve_context_transform_classification() {
        assert!(matches!(
            validate_json_schema("{", &json!([])),
            Err(PluginContextTransformTurnError::DeclarationDrift)
        ));
        assert!(matches!(
            validate_json_schema(r#"{"type":"array"}"#, &json!({})),
            Err(PluginContextTransformTurnError::InvalidOutput)
        ));
    }
}
