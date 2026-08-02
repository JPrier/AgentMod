//! Replay-safe observer delivery coordination.
//!
//! The coordinator emits one reducer-valid canonical event at a time. The
//! caller must append and replay `Proposed`, then append and replay
//! `Dispatched`, before invoking the isolated host. A replayed `Dispatched`
//! record may issue only the exact same request so the host can return its
//! durable terminal receipt; it never selects a new observer or identity.
#![allow(
    missing_docs,
    reason = "layer-owned observer records mirror individually documented canonical event fields"
)]

use std::str::FromStr;

use agentmod_primitives::ContentHash;
use agentmod_runtime_data::plugin::{
    ObservePluginDataRequest, PluginDataError, PluginDataPort, PluginObservationDataRecord,
    PluginObserverDeliveryStatusDataRecord,
};
use serde_json::{Value, json};
use thiserror::Error;

use crate::{
    plugin::CommittedPluginEvent,
    session::{
        PluginObserverDeliveryDispatchedEvent, PluginObserverDeliveryIdentity,
        PluginObserverDeliveryProposedEvent, PluginObserverDeliveryState,
        PluginObserverDeliveryTerminalEvent, RuntimeCommittedEvent, SessionState,
    },
};

/// Exact immutable observer declaration selected during activation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObserverDeclaration {
    pub plugin_id: String,
    pub plugin_version: String,
    pub handler: String,
    pub declaration_hash: ContentHash,
    pub configuration_reference: ContentHash,
}

/// Exact delivery input retained until its terminal receipt is canonical.
#[derive(Clone, Debug, PartialEq)]
pub struct PreparedObserverDelivery {
    pub session_id: String,
    pub identity: PluginObserverDeliveryIdentity,
    pub event_type: String,
    pub event: Value,
}

/// Replay classification for one canonical observer delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObserverDeliveryRecovery {
    /// Intent is canonical but dispatch intent is not; append `Dispatched`.
    AppendDispatchIntent,
    /// Dispatch intent is canonical; issue only the exact request to reconcile.
    ReconcileExactReceipt,
    /// A terminal receipt is canonical and no further action is allowed.
    Terminal(PluginObserverDeliveryState),
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ObserverDeliveryError {
    #[error("observer delivery identity or replay state is invalid")]
    InvalidState,
    #[error("observer delivery request could not be encoded canonically")]
    Encoding,
    #[error("observer host operation failed before an exact terminal receipt: {0}")]
    Data(PluginDataError),
    #[error("observer host receipt is malformed or does not match the exact request")]
    InvalidReceipt,
}

#[derive(Clone)]
pub struct ObserverDeliveryCoordinator<D> {
    data: D,
}

impl<D> ObserverDeliveryCoordinator<D> {
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}

impl<D> ObserverDeliveryCoordinator<D>
where
    D: PluginDataPort,
{
    /// Builds immutable intent for one observer and one committed runtime event.
    ///
    /// The returned event must be appended and replayed before
    /// [`Self::dispatch_intent`] is called.
    ///
    /// # Errors
    ///
    /// Returns [`ObserverDeliveryError`] when the declaration/event identity is
    /// invalid or the bounded canonical request cannot be encoded.
    pub fn propose(
        &self,
        session_id: String,
        cancellation_id: String,
        declaration: ObserverDeclaration,
        committed: &CommittedPluginEvent,
    ) -> Result<(RuntimeCommittedEvent, PreparedObserverDelivery), ObserverDeliveryError> {
        if session_id.trim().is_empty()
            || cancellation_id.trim().is_empty()
            || declaration.plugin_id.trim().is_empty()
            || declaration.plugin_version.trim().is_empty()
            || declaration.handler.trim().is_empty()
            || committed.event_id.trim().is_empty()
            || committed.sequence == 0
            || committed.event_type.trim().is_empty()
        {
            return Err(ObserverDeliveryError::InvalidState);
        }
        let invocation_id = format!("observer-{}-{}", committed.event_id, declaration.plugin_id);
        let event = json!({
            "event_id": committed.event_id,
            "sequence": committed.sequence,
            "event_type": committed.event_type,
            "payload": committed.payload,
        });
        let event_range_hash = canonical_hash(&(
            "agentmod.runtime.plugin.observer.event_range.v1",
            committed.sequence,
            committed.sequence,
            &event,
        ))?;
        let request_hash = canonical_hash(&(
            "agentmod.plugin.observer.delivery.request.v1",
            &declaration.plugin_id,
            &invocation_id,
            &declaration.handler,
            &committed.event_type,
            &event,
        ))?;
        let identity = PluginObserverDeliveryIdentity {
            invocation_id,
            plugin_id: declaration.plugin_id,
            plugin_version: declaration.plugin_version,
            handler: declaration.handler,
            declaration_hash: declaration.declaration_hash,
            configuration_reference: declaration.configuration_reference,
            first_sequence: committed.sequence,
            last_sequence: committed.sequence,
            event_range_hash,
            request_hash,
            cancellation_id,
        };
        let prepared = PreparedObserverDelivery {
            session_id,
            identity: identity.clone(),
            event_type: committed.event_type.clone(),
            event,
        };
        Ok((
            RuntimeCommittedEvent::PluginObserverDeliveryProposed(Box::new(
                PluginObserverDeliveryProposedEvent { identity },
            )),
            prepared,
        ))
    }

    /// Builds dispatch intent only for the exact replayed proposal.
    ///
    /// # Errors
    ///
    /// Returns [`ObserverDeliveryError::InvalidState`] unless replay contains
    /// the exact proposed identity and it has not already crossed dispatch.
    pub fn dispatch_intent(
        &self,
        state: &SessionState,
        prepared: &PreparedObserverDelivery,
    ) -> Result<RuntimeCommittedEvent, ObserverDeliveryError> {
        let record = exact_record(state, prepared)?;
        if record.state != PluginObserverDeliveryState::Proposed {
            return Err(ObserverDeliveryError::InvalidState);
        }
        Ok(RuntimeCommittedEvent::PluginObserverDeliveryDispatched(
            PluginObserverDeliveryDispatchedEvent {
                invocation_id: prepared.identity.invocation_id.clone(),
                request_hash: prepared.identity.request_hash,
            },
        ))
    }

    /// Calls the isolated host only after exact dispatch intent is canonical.
    ///
    /// On restart this sends the same request. The host either returns its
    /// durable exact receipt or seals a previously pending delivery as
    /// ambiguous; it does not enqueue a second worker for an existing identity.
    ///
    /// # Errors
    ///
    /// Returns [`ObserverDeliveryError`] when replay identity is not exactly
    /// dispatched, host I/O fails, or the terminal receipt is inconsistent.
    pub async fn reconcile_exact_receipt(
        &self,
        state: &SessionState,
        prepared: &PreparedObserverDelivery,
    ) -> Result<RuntimeCommittedEvent, ObserverDeliveryError> {
        let record = exact_record(state, prepared)?;
        if record.state != PluginObserverDeliveryState::Dispatched {
            return Err(ObserverDeliveryError::InvalidState);
        }
        let result = self
            .data
            .observe_event(ObservePluginDataRequest {
                session_id: prepared.session_id.clone(),
                plugin_id: prepared.identity.plugin_id.clone(),
                invocation_id: prepared.identity.invocation_id.clone(),
                handler: prepared.identity.handler.clone(),
                event_type: prepared.event_type.clone(),
                event: prepared.event.clone(),
                cancellation_id: prepared.identity.cancellation_id.clone(),
            })
            .await
            .map_err(ObserverDeliveryError::Data)?;
        terminal_event(&prepared.identity, result)
    }

    /// Classifies replay state without consulting live plugin components.
    ///
    /// # Errors
    ///
    /// Returns [`ObserverDeliveryError::InvalidState`] when replay has no
    /// canonical record for `invocation_id`.
    pub fn recovery(
        state: &SessionState,
        invocation_id: &str,
    ) -> Result<ObserverDeliveryRecovery, ObserverDeliveryError> {
        let record = state
            .plugins
            .observer_deliveries
            .get(invocation_id)
            .ok_or(ObserverDeliveryError::InvalidState)?;
        match record.state {
            PluginObserverDeliveryState::Proposed => {
                Ok(ObserverDeliveryRecovery::AppendDispatchIntent)
            }
            PluginObserverDeliveryState::Dispatched => {
                Ok(ObserverDeliveryRecovery::ReconcileExactReceipt)
            }
            terminal => Ok(ObserverDeliveryRecovery::Terminal(terminal)),
        }
    }
}

fn exact_record<'a>(
    state: &'a SessionState,
    prepared: &PreparedObserverDelivery,
) -> Result<&'a crate::session::PluginObserverDeliveryRecord, ObserverDeliveryError> {
    let record = state
        .plugins
        .observer_deliveries
        .get(&prepared.identity.invocation_id)
        .ok_or(ObserverDeliveryError::InvalidState)?;
    if record.identity != prepared.identity {
        return Err(ObserverDeliveryError::InvalidState);
    }
    Ok(record)
}

fn terminal_event(
    identity: &PluginObserverDeliveryIdentity,
    result: PluginObservationDataRecord,
) -> Result<RuntimeCommittedEvent, ObserverDeliveryError> {
    let request_hash = ContentHash::from_str(&result.request_hash)
        .map_err(|_| ObserverDeliveryError::InvalidReceipt)?;
    let receipt_digest = ContentHash::from_str(&result.receipt_digest)
        .map_err(|_| ObserverDeliveryError::InvalidReceipt)?;
    if request_hash != identity.request_hash || result.receipt_id.trim().is_empty() {
        return Err(ObserverDeliveryError::InvalidReceipt);
    }
    let terminal = PluginObserverDeliveryTerminalEvent {
        invocation_id: identity.invocation_id.clone(),
        request_hash,
        receipt_id: result.receipt_id,
        receipt_digest,
        queue_depth: result.queue_depth,
        dropped: result.dropped,
        replayed: result.replayed,
    };
    Ok(match result.status {
        PluginObserverDeliveryStatusDataRecord::Completed if result.accepted => {
            RuntimeCommittedEvent::PluginObserverDeliveryCompleted(terminal)
        }
        PluginObserverDeliveryStatusDataRecord::Rejected if !result.accepted => {
            RuntimeCommittedEvent::PluginObserverDeliveryRejected(terminal)
        }
        PluginObserverDeliveryStatusDataRecord::Failed if result.accepted => {
            RuntimeCommittedEvent::PluginObserverDeliveryFailed(terminal)
        }
        PluginObserverDeliveryStatusDataRecord::Ambiguous => {
            RuntimeCommittedEvent::PluginObserverDeliveryAmbiguous(terminal)
        }
        _ => return Err(ObserverDeliveryError::InvalidReceipt),
    })
}

fn canonical_hash<T: serde::Serialize>(value: &T) -> Result<ContentHash, ObserverDeliveryError> {
    serde_json::to_vec(value)
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| ObserverDeliveryError::Encoding)
}

#[cfg(test)]
mod tests {
    use agentmod_event_model::{
        EventClassification, EventEnvelope, EventMetadata, EventOrigin, EventScope,
    };
    use agentmod_primitives::{
        CausationId, CorrelationId, EventId, Sequence, SessionId, TimestampMillis, Version,
    };
    use agentmod_runtime_data::plugin::{
        ActivatePluginsDataRequest, ActivatedPluginsDataRecord, InvokePluginDataRequest,
        PluginDecisionDataRecord,
    };
    use async_trait::async_trait;
    use serde_json::json;
    use uuid::Uuid;

    use super::*;
    use crate::session::{SessionCreatedEvent, reduce};

    #[derive(Clone, Copy)]
    struct FixtureData {
        status: PluginObserverDeliveryStatusDataRecord,
        accepted: bool,
    }

    #[async_trait]
    impl PluginDataPort for FixtureData {
        async fn activate_plugins(
            &self,
            _request: ActivatePluginsDataRequest,
        ) -> Result<ActivatedPluginsDataRecord, PluginDataError> {
            Err(PluginDataError::Invalid)
        }

        async fn invoke_plugin(
            &self,
            _request: InvokePluginDataRequest,
        ) -> Result<PluginDecisionDataRecord, PluginDataError> {
            Err(PluginDataError::Invalid)
        }

        async fn observe_event(
            &self,
            request: ObservePluginDataRequest,
        ) -> Result<PluginObservationDataRecord, PluginDataError> {
            let request_hash = canonical_hash(&(
                "agentmod.plugin.observer.delivery.request.v1",
                &request.plugin_id,
                &request.invocation_id,
                &request.handler,
                &request.event_type,
                &request.event,
            ))
            .expect("request hash");
            let receipt_id = String::from("observer:fixture-receipt");
            let status = match self.status {
                PluginObserverDeliveryStatusDataRecord::Completed => "completed",
                PluginObserverDeliveryStatusDataRecord::Rejected => "rejected",
                PluginObserverDeliveryStatusDataRecord::Failed => "failed",
                PluginObserverDeliveryStatusDataRecord::Ambiguous => "ambiguous",
            };
            let receipt_digest = canonical_hash(&(
                "agentmod.plugin.observer.delivery.receipt.v1",
                &request.plugin_id,
                &request.invocation_id,
                request_hash,
                status,
                &receipt_id,
            ))
            .expect("receipt digest");
            Ok(PluginObservationDataRecord {
                accepted: self.accepted,
                queue_depth: 0,
                dropped: u64::from(!self.accepted),
                status: self.status,
                request_hash: request_hash.to_hex(),
                receipt_id,
                receipt_digest: receipt_digest.to_hex(),
                replayed: false,
            })
        }
    }

    fn session_id() -> SessionId {
        SessionId::from_uuid(Uuid::from_u128(1))
    }

    fn envelope(
        sequence: u64,
        payload: RuntimeCommittedEvent,
    ) -> EventEnvelope<RuntimeCommittedEvent> {
        EventEnvelope::seal(
            EventMetadata {
                event_id: EventId::from_uuid(Uuid::from_u128(100 + u128::from(sequence))),
                scope: EventScope::Session(session_id()),
                sequence: Sequence::new(sequence).expect("sequence"),
                timestamp: TimestampMillis::new(1_700_000_000_000),
                event_type: payload.event_type().into(),
                event_version: Version::new(1, 0),
                correlation_id: CorrelationId::from_uuid(Uuid::from_u128(2)),
                causation_id: CausationId::from_uuid(Uuid::from_u128(3)),
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
        .expect("sealed event")
    }

    fn initial_state() -> SessionState {
        let mut state = reduce(
            None,
            &envelope(
                1,
                RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                    workspace: String::from("fixture"),
                    style: String::from("fixture"),
                    style_binding: None,
                }),
            ),
        )
        .expect("created state");
        state
            .plugins
            .activated_plugin_ids
            .push(String::from("fixture.observer"));
        state
    }

    fn declaration() -> ObserverDeclaration {
        ObserverDeclaration {
            plugin_id: String::from("fixture.observer"),
            plugin_version: String::from("1.0.0"),
            handler: String::from("observe:tool.execution_completed"),
            declaration_hash: ContentHash::digest(b"declaration"),
            configuration_reference: ContentHash::digest(b"configuration"),
        }
    }

    fn committed() -> CommittedPluginEvent {
        CommittedPluginEvent {
            event_id: String::from("event-1"),
            sequence: 1,
            event_type: String::from("tool.execution_completed"),
            payload: json!({"result":"ok"}),
        }
    }

    #[tokio::test]
    async fn exact_dispatch_receipt_completes_once_and_replays_terminal_state() {
        let coordinator = ObserverDeliveryCoordinator::new(FixtureData {
            status: PluginObserverDeliveryStatusDataRecord::Completed,
            accepted: true,
        });
        let (proposed, prepared) = coordinator
            .propose(
                session_id().to_string(),
                String::from("cancel-1"),
                declaration(),
                &committed(),
            )
            .expect("proposal");
        let proposed_state =
            reduce(Some(initial_state()), &envelope(2, proposed)).expect("proposed state");
        assert_eq!(
            ObserverDeliveryCoordinator::<FixtureData>::recovery(
                &proposed_state,
                &prepared.identity.invocation_id
            ),
            Ok(ObserverDeliveryRecovery::AppendDispatchIntent)
        );
        let dispatched = coordinator
            .dispatch_intent(&proposed_state, &prepared)
            .expect("dispatch");
        let dispatched_state =
            reduce(Some(proposed_state), &envelope(3, dispatched)).expect("dispatched state");
        assert_eq!(
            ObserverDeliveryCoordinator::<FixtureData>::recovery(
                &dispatched_state,
                &prepared.identity.invocation_id
            ),
            Ok(ObserverDeliveryRecovery::ReconcileExactReceipt)
        );
        let completed = coordinator
            .reconcile_exact_receipt(&dispatched_state, &prepared)
            .await
            .expect("terminal receipt");
        let completed_state =
            reduce(Some(dispatched_state), &envelope(4, completed)).expect("completed state");
        assert_eq!(
            ObserverDeliveryCoordinator::<FixtureData>::recovery(
                &completed_state,
                &prepared.identity.invocation_id
            ),
            Ok(ObserverDeliveryRecovery::Terminal(
                PluginObserverDeliveryState::Completed
            ))
        );
        assert!(
            coordinator
                .reconcile_exact_receipt(&completed_state, &prepared)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn queue_rejection_is_terminal_and_never_reported_as_completion() {
        let coordinator = ObserverDeliveryCoordinator::new(FixtureData {
            status: PluginObserverDeliveryStatusDataRecord::Rejected,
            accepted: false,
        });
        let (proposed, prepared) = coordinator
            .propose(
                session_id().to_string(),
                String::from("cancel-2"),
                declaration(),
                &committed(),
            )
            .expect("proposal");
        let proposed_state =
            reduce(Some(initial_state()), &envelope(2, proposed)).expect("proposed state");
        let dispatched = coordinator
            .dispatch_intent(&proposed_state, &prepared)
            .expect("dispatch");
        let dispatched_state =
            reduce(Some(proposed_state), &envelope(3, dispatched)).expect("dispatched state");
        let terminal = coordinator
            .reconcile_exact_receipt(&dispatched_state, &prepared)
            .await
            .expect("rejected receipt");
        assert!(matches!(
            terminal,
            RuntimeCommittedEvent::PluginObserverDeliveryRejected(_)
        ));
    }

    #[test]
    fn changed_exact_identity_cannot_reuse_canonical_dispatch() {
        let coordinator = ObserverDeliveryCoordinator::new(FixtureData {
            status: PluginObserverDeliveryStatusDataRecord::Completed,
            accepted: true,
        });
        let (proposed, mut prepared) = coordinator
            .propose(
                session_id().to_string(),
                String::from("cancel-3"),
                declaration(),
                &committed(),
            )
            .expect("proposal");
        let state = reduce(Some(initial_state()), &envelope(2, proposed)).expect("proposed state");
        prepared.identity.configuration_reference = ContentHash::digest(b"changed");
        assert_eq!(
            coordinator.dispatch_intent(&state, &prepared),
            Err(ObserverDeliveryError::InvalidState)
        );
    }

    #[tokio::test]
    async fn timeout_ambiguous_receipt_is_terminal_and_not_redispatched() {
        let coordinator = ObserverDeliveryCoordinator::new(FixtureData {
            status: PluginObserverDeliveryStatusDataRecord::Ambiguous,
            accepted: true,
        });
        let (proposed, prepared) = coordinator
            .propose(
                session_id().to_string(),
                String::from("cancel-timeout"),
                declaration(),
                &committed(),
            )
            .expect("proposal");
        let proposed_state =
            reduce(Some(initial_state()), &envelope(2, proposed)).expect("proposed state");
        let dispatched = coordinator
            .dispatch_intent(&proposed_state, &prepared)
            .expect("dispatch");
        let dispatched_state =
            reduce(Some(proposed_state), &envelope(3, dispatched)).expect("dispatched state");
        let terminal = coordinator
            .reconcile_exact_receipt(&dispatched_state, &prepared)
            .await
            .expect("ambiguous receipt");
        assert!(matches!(
            terminal,
            RuntimeCommittedEvent::PluginObserverDeliveryAmbiguous(_)
        ));
        let terminal_state =
            reduce(Some(dispatched_state), &envelope(4, terminal)).expect("terminal state");
        assert!(
            coordinator
                .reconcile_exact_receipt(&terminal_state, &prepared)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn inconsistent_plugin_terminal_result_is_rejected() {
        let coordinator = ObserverDeliveryCoordinator::new(FixtureData {
            status: PluginObserverDeliveryStatusDataRecord::Completed,
            accepted: false,
        });
        let (proposed, prepared) = coordinator
            .propose(
                session_id().to_string(),
                String::from("cancel-invalid"),
                declaration(),
                &committed(),
            )
            .expect("proposal");
        let proposed_state =
            reduce(Some(initial_state()), &envelope(2, proposed)).expect("proposed state");
        let dispatched = coordinator
            .dispatch_intent(&proposed_state, &prepared)
            .expect("dispatch");
        let dispatched_state =
            reduce(Some(proposed_state), &envelope(3, dispatched)).expect("dispatched state");
        assert_eq!(
            coordinator
                .reconcile_exact_receipt(&dispatched_state, &prepared)
                .await,
            Err(ObserverDeliveryError::InvalidReceipt)
        );
    }
}
