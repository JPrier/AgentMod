//! Runtime business coordination over canonical event journal data.

use std::path::PathBuf;

use agentmod_event_model::{EventClassification, EventEnvelope, EventScope};
use agentmod_primitives::{ByteCount, ContentHash, EventId, Sequence, SessionId};
use agentmod_runtime_data::journal::{
    AppendEventDataRequest, AppendedEventDataRecord, JournalDataError, JournalDurability,
    JournalEventDataPort, JournalRecoveryStatus, RecoverJournalDataRequest, ScanEventsDataRequest,
};
use serde_json::Value;
use thiserror::Error;

use crate::session::{RuntimeCommittedEvent, SessionReducerError, SessionState, replay};

/// Logic-owned durability policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitDurability {
    /// Operating system may batch disk persistence.
    Buffered,
    /// File data is synchronized.
    Data,
    /// File data and metadata are synchronized.
    Full,
}

/// Logic-owned command to commit a canonical event.
#[derive(Clone, Debug, PartialEq)]
pub struct CommitSessionEventCommand {
    /// Session directory selected through session data.
    pub session_directory: PathBuf,
    /// Typed runtime-logic committed event.
    pub event: EventEnvelope<RuntimeCommittedEvent>,
    /// Business durability policy.
    pub durability: CommitDurability,
}

/// Logic-owned commit result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitSessionEventResult {
    /// Event ID.
    pub event_id: EventId,
    /// Committed sequence.
    pub sequence: Sequence,
    /// Journal frame checksum.
    pub journal_checksum: ContentHash,
    /// Journal bytes after commit.
    pub journal_bytes: ByteCount,
}

/// Logic-owned load command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadSessionCommand {
    /// Durable session directory.
    pub session_directory: PathBuf,
    /// Expected session ID from metadata/endpoint selection.
    pub expected_session_id: SessionId,
}

/// Logic-owned load/replay result.
#[derive(Clone, Debug, PartialEq)]
pub struct LoadSessionResult {
    /// Purely reconstructed state.
    pub state: SessionState,
    /// Last verified committed event identifier.
    pub last_event_id: EventId,
    /// Verified canonical journal bytes.
    pub journal_bytes: ByteCount,
}

/// Logic-owned recovery command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoverSessionJournalCommand {
    /// Durable session directory.
    pub session_directory: PathBuf,
    /// Injected stable recovery identifier rendered safely.
    pub recovery_id: String,
}

/// Logic-owned recovery result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoverSessionJournalResult {
    /// Journal required no repair.
    Clean {
        /// Verified bytes.
        valid_bytes: ByteCount,
    },
    /// Only an invalid tail was quarantined.
    TailQuarantined {
        /// Safe quarantine artifact label.
        quarantine_file: String,
        /// Retained verified bytes.
        valid_bytes: ByteCount,
    },
}

/// Session persistence business interface consumed by runtime service/use cases.
pub trait SessionPersistenceLogicPort {
    /// Commits one typed event.
    ///
    /// # Errors
    ///
    /// Returns [`SessionPersistenceLogicError`] for invalid event authority/mapping or
    /// translated data failures.
    fn commit_event(
        &self,
        command: CommitSessionEventCommand,
    ) -> Result<CommitSessionEventResult, SessionPersistenceLogicError>;

    /// Loads and purely replays a session.
    ///
    /// # Errors
    ///
    /// Returns [`SessionPersistenceLogicError`] for mapping, data, identity, or pure
    /// reducer failures. No side effect is dispatched during replay.
    fn load_session(
        &self,
        command: LoadSessionCommand,
    ) -> Result<LoadSessionResult, SessionPersistenceLogicError>;

    /// Recovers only invalid final journal bytes.
    ///
    /// # Errors
    ///
    /// Returns [`SessionPersistenceLogicError`] when the ID is invalid, data recovery
    /// fails, or the returned record violates business expectations.
    fn recover_session_journal(
        &self,
        command: RecoverSessionJournalCommand,
    ) -> Result<RecoverSessionJournalResult, SessionPersistenceLogicError>;
}

/// Runtime persistence coordinator using only a data interface.
#[derive(Clone, Debug)]
pub struct SessionPersistenceLogic<D> {
    data: D,
}

impl<D> SessionPersistenceLogic<D> {
    /// Creates logic over the injected journal data interface.
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}

impl<D> SessionPersistenceLogicPort for SessionPersistenceLogic<D>
where
    D: JournalEventDataPort,
{
    fn commit_event(
        &self,
        command: CommitSessionEventCommand,
    ) -> Result<CommitSessionEventResult, SessionPersistenceLogicError> {
        command
            .event
            .verify()
            .map_err(|error| SessionPersistenceLogicError::EventIntegrity(error.to_string()))?;
        if command.event.metadata.classification != EventClassification::Committed {
            return Err(SessionPersistenceLogicError::NotCommitted);
        }
        if command.event.metadata.event_type != command.event.payload.event_type() {
            return Err(SessionPersistenceLogicError::EventTypeMismatch {
                metadata: command.event.metadata.event_type.clone(),
                payload: command.event.payload.event_type(),
            });
        }
        if !matches!(command.event.metadata.scope, EventScope::Session(_)) {
            return Err(SessionPersistenceLogicError::NotSessionScoped);
        }
        let data_event = to_data_event(&command.event)?;
        let appended = self
            .data
            .append_event(AppendEventDataRequest {
                session_directory: command.session_directory,
                event: data_event,
                durability: match command.durability {
                    CommitDurability::Buffered => JournalDurability::Buffered,
                    CommitDurability::Data => JournalDurability::Data,
                    CommitDurability::Full => JournalDurability::Full,
                },
            })
            .map_err(SessionPersistenceLogicError::Data)?;
        Ok(map_append_result(&appended))
    }

    fn load_session(
        &self,
        command: LoadSessionCommand,
    ) -> Result<LoadSessionResult, SessionPersistenceLogicError> {
        let scanned = self
            .data
            .scan_events(ScanEventsDataRequest {
                session_directory: command.session_directory,
            })
            .map_err(SessionPersistenceLogicError::Data)?;
        let typed: Vec<_> = scanned
            .events
            .iter()
            .map(|record| from_data_event(&record.event))
            .collect::<Result<_, _>>()?;
        let state = replay(&typed).map_err(SessionPersistenceLogicError::Reducer)?;
        let last_event_id = typed
            .last()
            .map(|event| event.metadata.event_id)
            .ok_or(SessionPersistenceLogicError::EmptyJournal)?;
        if state.id != command.expected_session_id {
            return Err(SessionPersistenceLogicError::SessionIdentityMismatch {
                expected: command.expected_session_id,
                actual: state.id,
            });
        }
        Ok(LoadSessionResult {
            state,
            last_event_id,
            journal_bytes: scanned.valid_bytes,
        })
    }

    fn recover_session_journal(
        &self,
        command: RecoverSessionJournalCommand,
    ) -> Result<RecoverSessionJournalResult, SessionPersistenceLogicError> {
        if command.recovery_id.is_empty()
            || !command
                .recovery_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(SessionPersistenceLogicError::InvalidRecoveryId);
        }
        let result = self
            .data
            .recover_journal(RecoverJournalDataRequest {
                session_directory: command.session_directory,
                quarantine_label: command.recovery_id,
            })
            .map_err(SessionPersistenceLogicError::Data)?;
        Ok(match result.status {
            JournalRecoveryStatus::Clean => RecoverSessionJournalResult::Clean {
                valid_bytes: result.valid_bytes,
            },
            JournalRecoveryStatus::TailQuarantined {
                quarantine_file_name,
            } => RecoverSessionJournalResult::TailQuarantined {
                quarantine_file: quarantine_file_name,
                valid_bytes: result.valid_bytes,
            },
        })
    }
}

fn to_data_event(
    event: &EventEnvelope<RuntimeCommittedEvent>,
) -> Result<EventEnvelope<Value>, SessionPersistenceLogicError> {
    let payload = serde_json::to_value(&event.payload)
        .map_err(|error| SessionPersistenceLogicError::EventMapping(error.to_string()))?;
    let mapped = EventEnvelope::seal(event.metadata.clone(), payload)
        .map_err(|error| SessionPersistenceLogicError::EventMapping(error.to_string()))?;
    if mapped.integrity_checksum != event.integrity_checksum {
        return Err(SessionPersistenceLogicError::MappingChangedChecksum);
    }
    Ok(mapped)
}

fn from_data_event(
    event: &EventEnvelope<Value>,
) -> Result<EventEnvelope<RuntimeCommittedEvent>, SessionPersistenceLogicError> {
    let payload = serde_json::from_value(event.payload.clone())
        .map_err(|error| SessionPersistenceLogicError::EventMapping(error.to_string()))?;
    let mapped = EventEnvelope::seal(event.metadata.clone(), payload)
        .map_err(|error| SessionPersistenceLogicError::EventMapping(error.to_string()))?;
    if mapped.integrity_checksum != event.integrity_checksum {
        return Err(SessionPersistenceLogicError::MappingChangedChecksum);
    }
    Ok(mapped)
}

fn map_append_result(record: &AppendedEventDataRecord) -> CommitSessionEventResult {
    CommitSessionEventResult {
        event_id: record.event_id,
        sequence: record.sequence,
        journal_checksum: record.journal_checksum,
        journal_bytes: record.journal_bytes,
    }
}

/// Session persistence business error.
#[derive(Debug, Error)]
pub enum SessionPersistenceLogicError {
    /// A session journal must contain its creation event.
    #[error("session journal is empty")]
    EmptyJournal,
    /// Event envelope checksum failed.
    #[error("typed event integrity failed: {0}")]
    EventIntegrity(String),
    /// Only committed events may reach canonical persistence.
    #[error("canonical journal accepts committed events only")]
    NotCommitted,
    /// Envelope metadata does not identify its typed payload.
    #[error("event metadata type {metadata:?} does not match payload type {payload:?}")]
    EventTypeMismatch {
        /// Type carried in metadata.
        metadata: String,
        /// Type required by the typed payload.
        payload: &'static str,
    },
    /// Session journal accepts session scope only.
    #[error("session journal event is not session-scoped")]
    NotSessionScoped,
    /// Typed/value mapping failed.
    #[error("event boundary mapping failed: {0}")]
    EventMapping(String),
    /// Mapping changed canonical JSON/integrity.
    #[error("event boundary mapping changed canonical checksum")]
    MappingChangedChecksum,
    /// Data-layer operation failed.
    #[error("session journal data failed: {0}")]
    Data(JournalDataError),
    /// Pure reducer rejected history.
    #[error("session replay failed: {0}")]
    Reducer(SessionReducerError),
    /// Endpoint-selected and journal session IDs differ.
    #[error("session identity mismatch: expected {expected}, replayed {actual}")]
    SessionIdentityMismatch {
        /// Expected session.
        expected: SessionId,
        /// Replayed session.
        actual: SessionId,
    },
    /// Recovery labels must be safe opaque identifiers.
    #[error("recovery ID contains unsupported characters")]
    InvalidRecoveryId,
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, str::FromStr};

    use agentmod_event_model::{EventMetadata, EventOrigin};
    use agentmod_primitives::{CausationId, CorrelationId, EventId, TimestampMillis, Version};
    use agentmod_runtime_data::journal::{
        JournalEventDataRecord, RecoveredJournalDataRecord, ScannedEventsDataRecord,
    };
    use uuid::Uuid;

    use crate::session::SessionCreatedEvent;

    use super::*;

    struct MockData {
        append_requests: RefCell<Vec<AppendEventDataRequest>>,
        scan_result: RefCell<Option<Result<ScannedEventsDataRecord, JournalDataError>>>,
        recovery_result: RefCell<Option<Result<RecoveredJournalDataRecord, JournalDataError>>>,
    }

    impl JournalEventDataPort for MockData {
        fn append_event(
            &self,
            request: AppendEventDataRequest,
        ) -> Result<AppendedEventDataRecord, JournalDataError> {
            let event_id = request.event.metadata.event_id;
            let sequence = request.event.metadata.sequence;
            let envelope_checksum = request.event.integrity_checksum;
            self.append_requests.borrow_mut().push(request);
            Ok(AppendedEventDataRecord {
                event_id,
                sequence,
                envelope_checksum,
                journal_checksum: ContentHash::digest(b"journal"),
                offset: ByteCount::new(0),
                journal_bytes: ByteCount::new(100),
            })
        }

        fn scan_events(
            &self,
            _request: ScanEventsDataRequest,
        ) -> Result<ScannedEventsDataRecord, JournalDataError> {
            self.scan_result
                .borrow_mut()
                .take()
                .expect("configured scan")
        }

        fn recover_journal(
            &self,
            _request: RecoverJournalDataRequest,
        ) -> Result<RecoveredJournalDataRecord, JournalDataError> {
            self.recovery_result
                .borrow_mut()
                .take()
                .expect("configured recovery")
        }
    }

    fn session_id() -> SessionId {
        SessionId::from_str("018f6f83-7b80-7000-8000-000000000001").expect("ID")
    }

    fn created() -> EventEnvelope<RuntimeCommittedEvent> {
        EventEnvelope::seal(
            EventMetadata {
                event_id: EventId::from_uuid(Uuid::from_u128(1)),
                scope: EventScope::Session(session_id()),
                sequence: Sequence::FIRST,
                timestamp: TimestampMillis::new(1),
                event_type: "session.created".into(),
                event_version: Version::new(1, 0),
                correlation_id: CorrelationId::from_uuid(Uuid::from_u128(2)),
                causation_id: CausationId::from_uuid(Uuid::from_u128(3)),
                parent_graph_node_id: None,
                origin: EventOrigin {
                    subsystem: "runtime".into(),
                    plugin: None,
                },
                schema_version: Version::new(1, 0),
                artifacts: vec![],
                classification: EventClassification::Committed,
            },
            RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                workspace: "fixture".into(),
                style: "persistent-chat".into(),
                style_binding: None,
            }),
        )
        .expect("seal")
    }

    fn data_event() -> EventEnvelope<Value> {
        to_data_event(&created()).expect("map")
    }

    #[test]
    fn commit_maps_typed_event_and_durability() {
        let data = MockData {
            append_requests: RefCell::new(vec![]),
            scan_result: RefCell::new(None),
            recovery_result: RefCell::new(None),
        };
        let logic = SessionPersistenceLogic::new(data);
        let result = logic
            .commit_event(CommitSessionEventCommand {
                session_directory: PathBuf::from("session"),
                event: created(),
                durability: CommitDurability::Full,
            })
            .expect("commit");
        assert_eq!(result.sequence, Sequence::FIRST);
        assert_eq!(
            logic.data.append_requests.borrow()[0].durability,
            JournalDurability::Full
        );
    }

    #[test]
    fn commit_rejects_metadata_type_that_does_not_match_payload() {
        let data = MockData {
            append_requests: RefCell::new(vec![]),
            scan_result: RefCell::new(None),
            recovery_result: RefCell::new(None),
        };
        let logic = SessionPersistenceLogic::new(data);
        let event = created();
        let mut metadata = event.metadata;
        metadata.event_type = "tool.execution.completed".into();
        let mismatched =
            EventEnvelope::seal(metadata, event.payload).expect("seal valid but mismatched event");
        assert!(matches!(
            logic.commit_event(CommitSessionEventCommand {
                session_directory: PathBuf::from("session"),
                event: mismatched,
                durability: CommitDurability::Full,
            }),
            Err(SessionPersistenceLogicError::EventTypeMismatch {
                payload: "session.created",
                ..
            })
        ));
        assert!(logic.data.append_requests.borrow().is_empty());
    }

    #[test]
    fn load_maps_and_purely_replays_without_effects() {
        let data = MockData {
            append_requests: RefCell::new(vec![]),
            scan_result: RefCell::new(Some(Ok(ScannedEventsDataRecord {
                events: vec![JournalEventDataRecord {
                    event: data_event(),
                    journal_checksum: ContentHash::digest(b"journal"),
                    previous_journal_checksum: None,
                    offset: ByteCount::new(0),
                }],
                valid_bytes: ByteCount::new(100),
            }))),
            recovery_result: RefCell::new(None),
        };
        let result = SessionPersistenceLogic::new(data)
            .load_session(LoadSessionCommand {
                session_directory: PathBuf::from("session"),
                expected_session_id: session_id(),
            })
            .expect("load");
        assert_eq!(result.state.workspace, "fixture");
        assert_eq!(result.journal_bytes, ByteCount::new(100));
    }

    #[test]
    fn recovery_maps_safe_status() {
        let data = MockData {
            append_requests: RefCell::new(vec![]),
            scan_result: RefCell::new(None),
            recovery_result: RefCell::new(Some(Ok(RecoveredJournalDataRecord {
                status: JournalRecoveryStatus::TailQuarantined {
                    quarantine_file_name: "events-tail-r1.bin".into(),
                },
                valid_bytes: ByteCount::new(50),
            }))),
        };
        assert_eq!(
            SessionPersistenceLogic::new(data)
                .recover_session_journal(RecoverSessionJournalCommand {
                    session_directory: PathBuf::from("session"),
                    recovery_id: "r1".into(),
                })
                .expect("recover"),
            RecoverSessionJournalResult::TailQuarantined {
                quarantine_file: "events-tail-r1.bin".into(),
                valid_bytes: ByteCount::new(50)
            }
        );
    }
}
