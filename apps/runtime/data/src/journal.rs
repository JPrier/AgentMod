//! Business-facing canonical event journal data adapter.

use std::{path::PathBuf, str::FromStr};

use agentmod_event_model::{EventEnvelope, EventModelError};
use agentmod_primitives::{ByteCount, ContentHash, EventId, Sequence};
use agentmod_runtime_dependency::journal::{
    DependencyAppendJournalRequest, DependencyAppendJournalResponse, DependencyDurability,
    DependencyJournalRecord, DependencyRecoverJournalRequest, DependencyRecoverJournalResponse,
    DependencyScanJournalRequest, DependencyScanJournalResponse, JournalDependencyError,
    JournalDependencyPort,
};
use serde_json::Value;
use thiserror::Error;

/// Data-owned journal durability selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalDurability {
    /// Allow the operating system to batch persistence.
    Buffered,
    /// Synchronize file data before returning.
    Data,
    /// Synchronize file data and metadata before returning.
    Full,
}

/// Data-owned request to append one verified canonical event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendEventDataRequest {
    /// Durable session directory selected by runtime logic.
    pub session_directory: PathBuf,
    /// Generic canonical event whose integrity must already be sealed.
    pub event: EventEnvelope<Value>,
    /// Required persistence guarantee.
    pub durability: JournalDurability,
}

/// Data-owned result of a successful event append.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendedEventDataRecord {
    /// Canonical event identifier.
    pub event_id: EventId,
    /// Canonical sequence written to the journal.
    pub sequence: Sequence,
    /// Integrity checksum inside the canonical event envelope.
    pub envelope_checksum: ContentHash,
    /// Checksum of the dependency's complete journal frame.
    pub journal_checksum: ContentHash,
    /// Byte offset where the frame begins.
    pub offset: ByteCount,
    /// Journal size after the append.
    pub journal_bytes: ByteCount,
}

/// Data-owned request to scan a canonical session journal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanEventsDataRequest {
    /// Durable session directory selected by runtime logic.
    pub session_directory: PathBuf,
}

/// One normalized, independently verified journal event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalEventDataRecord {
    /// Verified generic canonical event.
    pub event: EventEnvelope<Value>,
    /// Checksum of the complete journal frame.
    pub journal_checksum: ContentHash,
    /// Previous frame checksum in the journal chain.
    pub previous_journal_checksum: Option<ContentHash>,
    /// Byte offset where the frame begins.
    pub offset: ByteCount,
}

/// Data-owned result of a complete verified scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScannedEventsDataRecord {
    /// Strictly ordered canonical events.
    pub events: Vec<JournalEventDataRecord>,
    /// Number of verified bytes in the journal.
    pub valid_bytes: ByteCount,
}

/// Data-owned request to recover an invalid final journal frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoverJournalDataRequest {
    /// Durable session directory selected by runtime logic.
    pub session_directory: PathBuf,
    /// Caller-generated safe label for the quarantine artifact.
    pub quarantine_label: String,
}

/// Data-owned recovery classification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JournalRecoveryStatus {
    /// Journal was already valid and unchanged.
    Clean,
    /// Invalid final bytes were removed and quarantined.
    TailQuarantined {
        /// Safe filename only; dependency paths never escape the data boundary.
        quarantine_file_name: String,
    },
}

/// Data-owned journal recovery result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredJournalDataRecord {
    /// Recovery classification.
    pub status: JournalRecoveryStatus,
    /// Bytes retained as the valid canonical journal.
    pub valid_bytes: ByteCount,
}

/// Stable category for a dependency failure translated at the data boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalDependencyFailureCode {
    /// Filesystem or locking failure.
    Access,
    /// JSON representation failure inside the dependency.
    Encoding,
    /// Data supplied invalid event JSON.
    InvalidEvent,
    /// Append sequence did not match the valid tail.
    SequenceConflict,
    /// Event identifier was already present.
    DuplicateEventId,
    /// Journal ended in an incomplete frame.
    InvalidTail,
    /// A complete journal frame was corrupt.
    CorruptRecord,
    /// Corruption occurred before the final frame.
    InteriorCorruption,
    /// Recovery quarantine label was unsafe.
    InvalidRecoveryLabel,
    /// Sequence arithmetic overflowed.
    SequenceOverflow,
    /// Byte length arithmetic overflowed.
    LengthOverflow,
}

/// Data-layer journal failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum JournalDataError {
    /// A canonical event failed its envelope checksum before persistence or after scan.
    #[error("canonical event integrity check failed during {operation}: {message}")]
    EventIntegrity {
        /// Stable operation name.
        operation: &'static str,
        /// Redacted integrity diagnostic.
        message: String,
    },
    /// A canonical event could not be serialized at the data boundary.
    #[error("canonical event serialization failed: {message}")]
    EventSerialization {
        /// Redacted serialization diagnostic.
        message: String,
    },
    /// A dependency frame carried an invalid opaque event identifier.
    #[error("journal frame contains invalid event ID `{event_id}`")]
    InvalidFrameEventId {
        /// Invalid dependency-owned text.
        event_id: String,
    },
    /// A dependency frame carried sequence zero.
    #[error("journal frame contains invalid sequence {sequence}")]
    InvalidFrameSequence {
        /// Invalid raw dependency sequence.
        sequence: u64,
    },
    /// Frame and envelope event identifiers differed.
    #[error("journal frame event ID {frame} does not match envelope event ID {envelope}")]
    EventIdMismatch {
        /// Normalized dependency frame identifier.
        frame: EventId,
        /// Canonical envelope identifier.
        envelope: EventId,
    },
    /// Frame and envelope event sequences differed.
    #[error("journal frame sequence {frame:?} does not match envelope sequence {envelope:?}")]
    EventSequenceMismatch {
        /// Normalized dependency frame sequence.
        frame: Sequence,
        /// Canonical envelope sequence.
        envelope: Sequence,
    },
    /// Frames were not returned in strict sequence order.
    #[error("journal scan expected sequence {expected}, received {actual}")]
    NonMonotonicSequence {
        /// Next required raw sequence.
        expected: u64,
        /// Actual raw sequence.
        actual: u64,
    },
    /// Dependency returned a checksum outside the canonical hash representation.
    #[error("journal frame contains invalid {field} checksum")]
    InvalidJournalChecksum {
        /// Stable checksum field name.
        field: &'static str,
    },
    /// Dependency frame checksum chain was internally inconsistent.
    #[error("journal checksum chain mismatch at sequence {sequence:?}")]
    ChecksumChainMismatch {
        /// Sequence whose previous checksum was inconsistent.
        sequence: Sequence,
    },
    /// Dependency returned an offset before a prior frame.
    #[error("journal offset did not advance beyond {previous:?}; received {actual:?}")]
    NonMonotonicOffset {
        /// Prior normalized byte offset.
        previous: ByteCount,
        /// Actual normalized byte offset.
        actual: ByteCount,
    },
    /// Scan claimed fewer valid bytes than the final record offset.
    #[error("journal valid byte count {valid_bytes:?} is not after final offset {last_offset:?}")]
    InvalidValidByteCount {
        /// Dependency-reported verified bytes.
        valid_bytes: ByteCount,
        /// Final normalized record offset.
        last_offset: ByteCount,
    },
    /// Recovery response combined incompatible status and quarantine fields.
    #[error("journal recovery dependency returned an inconsistent result")]
    InvalidRecoveryResult,
    /// External journal adapter failed, translated into a stable data category.
    #[error("journal dependency failed ({code:?}): {message}")]
    Dependency {
        /// Stable category independent of the concrete adapter.
        code: JournalDependencyFailureCode,
        /// Redacted dependency diagnostic.
        message: String,
    },
}

/// Business-facing canonical event journal interface.
pub trait JournalEventDataPort {
    /// Verifies and appends one canonical event.
    ///
    /// # Errors
    ///
    /// Returns [`JournalDataError`] for event-integrity, mapping, dependency, or
    /// dependency-contract failures.
    fn append_event(
        &self,
        request: AppendEventDataRequest,
    ) -> Result<AppendedEventDataRecord, JournalDataError>;

    /// Scans and independently verifies all canonical events.
    ///
    /// # Errors
    ///
    /// Returns [`JournalDataError`] when any frame, envelope, identity, ordering, or
    /// checksum invariant fails.
    fn scan_events(
        &self,
        request: ScanEventsDataRequest,
    ) -> Result<ScannedEventsDataRecord, JournalDataError>;

    /// Recovers an invalid final frame without exposing dependency paths.
    ///
    /// # Errors
    ///
    /// Returns [`JournalDataError`] when recovery fails or the dependency response is
    /// inconsistent.
    fn recover_journal(
        &self,
        request: RecoverJournalDataRequest,
    ) -> Result<RecoveredJournalDataRecord, JournalDataError>;
}

impl<D> JournalEventDataPort for super::RuntimeData<D>
where
    D: JournalDependencyPort,
{
    fn append_event(
        &self,
        request: AppendEventDataRequest,
    ) -> Result<AppendedEventDataRecord, JournalDataError> {
        request
            .event
            .verify()
            .map_err(|error| map_event_error("append", error))?;
        let event_json = serde_json::to_vec(&request.event).map_err(|error| {
            JournalDataError::EventSerialization {
                message: error.to_string(),
            }
        })?;
        let event_id = request.event.metadata.event_id;
        let sequence = request.event.metadata.sequence;
        let envelope_checksum = request.event.integrity_checksum;
        let dependency_request = DependencyAppendJournalRequest {
            session_directory: request.session_directory,
            sequence: sequence.get(),
            event_id: event_id.to_string(),
            event_json,
            durability: map_durability(request.durability),
        };
        let response = self
            .dependency
            .append(dependency_request)
            .map_err(|error| map_dependency_error(&error))?;
        map_append_response(event_id, sequence, envelope_checksum, response)
    }

    fn scan_events(
        &self,
        request: ScanEventsDataRequest,
    ) -> Result<ScannedEventsDataRecord, JournalDataError> {
        let response = self
            .dependency
            .scan(DependencyScanJournalRequest {
                session_directory: request.session_directory,
            })
            .map_err(|error| map_dependency_error(&error))?;
        map_scan_response(response)
    }

    fn recover_journal(
        &self,
        request: RecoverJournalDataRequest,
    ) -> Result<RecoveredJournalDataRecord, JournalDataError> {
        let response = self
            .dependency
            .recover_tail(DependencyRecoverJournalRequest {
                session_directory: request.session_directory,
                quarantine_label: request.quarantine_label,
            })
            .map_err(|error| map_dependency_error(&error))?;
        map_recovery_response(response)
    }
}

fn map_durability(durability: JournalDurability) -> DependencyDurability {
    match durability {
        JournalDurability::Buffered => DependencyDurability::Buffered,
        JournalDurability::Data => DependencyDurability::Data,
        JournalDurability::Full => DependencyDurability::Full,
    }
}

fn map_append_response(
    event_id: EventId,
    sequence: Sequence,
    envelope_checksum: ContentHash,
    response: DependencyAppendJournalResponse,
) -> Result<AppendedEventDataRecord, JournalDataError> {
    let DependencyAppendJournalResponse {
        offset,
        checksum,
        journal_bytes,
    } = response;
    let journal_checksum = parse_checksum(&checksum, "record")?;
    Ok(AppendedEventDataRecord {
        event_id,
        sequence,
        envelope_checksum,
        journal_checksum,
        offset: ByteCount::new(offset),
        journal_bytes: ByteCount::new(journal_bytes),
    })
}

fn map_scan_response(
    response: DependencyScanJournalResponse,
) -> Result<ScannedEventsDataRecord, JournalDataError> {
    let mut events = Vec::with_capacity(response.records.len());
    let mut expected_sequence = Sequence::FIRST;
    let mut expected_previous_checksum = None;
    let mut previous_offset = None;

    for dependency_record in response.records {
        let record = map_scan_record(
            dependency_record,
            expected_sequence,
            expected_previous_checksum,
            previous_offset,
        )?;
        expected_sequence = record
            .event
            .metadata
            .sequence
            .checked_next()
            .map_err(|error| JournalDataError::EventIntegrity {
                operation: "scan",
                message: error.to_string(),
            })?;
        expected_previous_checksum = Some(record.journal_checksum);
        previous_offset = Some(record.offset);
        events.push(record);
    }

    let valid_bytes = ByteCount::new(response.valid_bytes);
    if let Some(last_offset) = previous_offset
        && valid_bytes <= last_offset
    {
        return Err(JournalDataError::InvalidValidByteCount {
            valid_bytes,
            last_offset,
        });
    }
    Ok(ScannedEventsDataRecord {
        events,
        valid_bytes,
    })
}

fn map_scan_record(
    record: DependencyJournalRecord,
    expected_sequence: Sequence,
    expected_previous_checksum: Option<ContentHash>,
    previous_offset: Option<ByteCount>,
) -> Result<JournalEventDataRecord, JournalDataError> {
    let DependencyJournalRecord {
        sequence: raw_sequence,
        event_id: raw_event_id,
        checksum,
        previous_checksum,
        event_json,
        offset: raw_offset,
    } = record;
    if raw_sequence != expected_sequence.get() {
        return Err(JournalDataError::NonMonotonicSequence {
            expected: expected_sequence.get(),
            actual: raw_sequence,
        });
    }
    let sequence =
        Sequence::new(raw_sequence).map_err(|_| JournalDataError::InvalidFrameSequence {
            sequence: raw_sequence,
        })?;
    let event_id =
        EventId::from_str(&raw_event_id).map_err(|_| JournalDataError::InvalidFrameEventId {
            event_id: raw_event_id,
        })?;
    let journal_checksum = parse_checksum(&checksum, "record")?;
    let previous_journal_checksum = previous_checksum
        .as_deref()
        .map(|checksum| parse_checksum(checksum, "previous"))
        .transpose()?;
    if previous_journal_checksum != expected_previous_checksum {
        return Err(JournalDataError::ChecksumChainMismatch { sequence });
    }
    let offset = ByteCount::new(raw_offset);
    if let Some(previous) = previous_offset
        && offset <= previous
    {
        return Err(JournalDataError::NonMonotonicOffset {
            previous,
            actual: offset,
        });
    }
    let event = EventEnvelope::<Value>::from_verified_json(&event_json)
        .map_err(|error| map_event_error("scan", error))?;
    if event.metadata.event_id != event_id {
        return Err(JournalDataError::EventIdMismatch {
            frame: event_id,
            envelope: event.metadata.event_id,
        });
    }
    if event.metadata.sequence != sequence {
        return Err(JournalDataError::EventSequenceMismatch {
            frame: sequence,
            envelope: event.metadata.sequence,
        });
    }
    Ok(JournalEventDataRecord {
        event,
        journal_checksum,
        previous_journal_checksum,
        offset,
    })
}

fn map_recovery_response(
    response: DependencyRecoverJournalResponse,
) -> Result<RecoveredJournalDataRecord, JournalDataError> {
    let status = match (response.repaired, response.quarantine_path) {
        (false, None) => JournalRecoveryStatus::Clean,
        (true, Some(path)) => {
            let quarantine_file_name = path
                .file_name()
                .and_then(|value| value.to_str())
                .filter(|value| !value.is_empty())
                .ok_or(JournalDataError::InvalidRecoveryResult)?
                .to_owned();
            JournalRecoveryStatus::TailQuarantined {
                quarantine_file_name,
            }
        }
        (false, Some(_)) | (true, None) => return Err(JournalDataError::InvalidRecoveryResult),
    };
    Ok(RecoveredJournalDataRecord {
        status,
        valid_bytes: ByteCount::new(response.valid_bytes),
    })
}

fn parse_checksum(checksum: &str, field: &'static str) -> Result<ContentHash, JournalDataError> {
    ContentHash::from_str(checksum).map_err(|_| JournalDataError::InvalidJournalChecksum { field })
}

fn map_event_error(operation: &'static str, error: EventModelError) -> JournalDataError {
    match error {
        EventModelError::Serialization(error) => JournalDataError::EventSerialization {
            message: error.to_string(),
        },
        EventModelError::ChecksumMismatch { .. } => JournalDataError::EventIntegrity {
            operation,
            message: "envelope checksum mismatch".to_owned(),
        },
    }
}

fn map_dependency_error(error: &JournalDependencyError) -> JournalDataError {
    let message = error.to_string();
    let code = match error {
        JournalDependencyError::Io(_) => JournalDependencyFailureCode::Access,
        JournalDependencyError::Json(_) => JournalDependencyFailureCode::Encoding,
        JournalDependencyError::InvalidEventJson(_) => JournalDependencyFailureCode::InvalidEvent,
        JournalDependencyError::SequenceMismatch { .. } => {
            JournalDependencyFailureCode::SequenceConflict
        }
        JournalDependencyError::DuplicateEventId(_) => {
            JournalDependencyFailureCode::DuplicateEventId
        }
        JournalDependencyError::InvalidTrailingRecord { .. } => {
            JournalDependencyFailureCode::InvalidTail
        }
        JournalDependencyError::CorruptRecord { .. } => JournalDependencyFailureCode::CorruptRecord,
        JournalDependencyError::InteriorCorruption { .. } => {
            JournalDependencyFailureCode::InteriorCorruption
        }
        JournalDependencyError::InvalidQuarantineLabel => {
            JournalDependencyFailureCode::InvalidRecoveryLabel
        }
        JournalDependencyError::SequenceOverflow => JournalDependencyFailureCode::SequenceOverflow,
        JournalDependencyError::LengthOverflow => JournalDependencyFailureCode::LengthOverflow,
    };
    JournalDataError::Dependency { code, message }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, path::Path, str::FromStr};

    use agentmod_event_model::{EventClassification, EventMetadata, EventOrigin, EventScope};
    use agentmod_primitives::{CausationId, CorrelationId, TimestampMillis, Version};

    use super::*;

    struct MockJournalDependency {
        appends: RefCell<Vec<DependencyAppendJournalRequest>>,
        scans: RefCell<Vec<DependencyScanJournalRequest>>,
        recoveries: RefCell<Vec<DependencyRecoverJournalRequest>>,
        append_result: Result<DependencyAppendJournalResponse, JournalDependencyError>,
        scan_result: Result<DependencyScanJournalResponse, JournalDependencyError>,
        recovery_result: Result<DependencyRecoverJournalResponse, JournalDependencyError>,
    }

    impl JournalDependencyPort for MockJournalDependency {
        fn append(
            &self,
            request: DependencyAppendJournalRequest,
        ) -> Result<DependencyAppendJournalResponse, JournalDependencyError> {
            self.appends.borrow_mut().push(request);
            self.append_result.clone()
        }

        fn scan(
            &self,
            request: DependencyScanJournalRequest,
        ) -> Result<DependencyScanJournalResponse, JournalDependencyError> {
            self.scans.borrow_mut().push(request);
            self.scan_result.clone()
        }

        fn recover_tail(
            &self,
            request: DependencyRecoverJournalRequest,
        ) -> Result<DependencyRecoverJournalResponse, JournalDependencyError> {
            self.recoveries.borrow_mut().push(request);
            self.recovery_result.clone()
        }
    }

    fn fixture_dependency() -> MockJournalDependency {
        MockJournalDependency {
            appends: RefCell::new(Vec::new()),
            scans: RefCell::new(Vec::new()),
            recoveries: RefCell::new(Vec::new()),
            append_result: Ok(DependencyAppendJournalResponse {
                offset: 16,
                checksum: fixture_hash("journal").to_hex(),
                journal_bytes: 256,
            }),
            scan_result: Ok(DependencyScanJournalResponse {
                records: Vec::new(),
                valid_bytes: 0,
            }),
            recovery_result: Ok(DependencyRecoverJournalResponse {
                repaired: false,
                valid_bytes: 0,
                quarantine_path: None,
            }),
        }
    }

    fn fixture_event(sequence: u64, event_id: &str) -> EventEnvelope<Value> {
        EventEnvelope::seal(
            EventMetadata {
                event_id: EventId::from_str(event_id).expect("event ID fixture"),
                scope: EventScope::Runtime,
                sequence: Sequence::new(sequence).expect("sequence fixture"),
                timestamp: TimestampMillis::new(1_700_000_000_000),
                event_type: "runtime.fixture".to_owned(),
                event_version: Version::new(1, 0),
                correlation_id: CorrelationId::from_str("018f6f83-7b80-7000-8000-000000000101")
                    .expect("correlation fixture"),
                causation_id: CausationId::from_str("018f6f83-7b80-7000-8000-000000000102")
                    .expect("causation fixture"),
                parent_graph_node_id: None,
                origin: EventOrigin {
                    subsystem: "runtime".to_owned(),
                    plugin: None,
                },
                schema_version: Version::new(1, 0),
                artifacts: Vec::new(),
                classification: EventClassification::Committed,
            },
            serde_json::json!({"fixture": sequence}),
        )
        .expect("event fixture seals")
    }

    fn fixture_hash(value: &str) -> ContentHash {
        ContentHash::digest(value.as_bytes())
    }

    fn dependency_record(
        event: &EventEnvelope<Value>,
        checksum: ContentHash,
        previous: Option<ContentHash>,
        offset: u64,
    ) -> DependencyJournalRecord {
        DependencyJournalRecord {
            sequence: event.metadata.sequence.get(),
            event_id: event.metadata.event_id.to_string(),
            checksum: checksum.to_hex(),
            previous_checksum: previous.map(ContentHash::to_hex),
            event_json: serde_json::to_vec(event).expect("event fixture serializes"),
            offset,
        }
    }

    #[test]
    fn append_verifies_serializes_and_maps_durability() {
        let event = fixture_event(1, "018f6f83-7b80-7000-8000-000000000001");
        let expected_event = event.clone();
        let data = super::super::RuntimeData::new(fixture_dependency());

        let appended = data
            .append_event(AppendEventDataRequest {
                session_directory: PathBuf::from("session-a"),
                event,
                durability: JournalDurability::Full,
            })
            .expect("append succeeds");

        assert_eq!(appended.event_id, expected_event.metadata.event_id);
        assert_eq!(appended.sequence, Sequence::FIRST);
        assert_eq!(appended.offset, ByteCount::new(16));
        let requests = data.dependency.appends.borrow();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].sequence, 1);
        assert_eq!(
            requests[0].event_id,
            expected_event.metadata.event_id.to_string()
        );
        assert_eq!(requests[0].durability, DependencyDurability::Full);
        assert_eq!(
            EventEnvelope::<Value>::from_verified_json(&requests[0].event_json)
                .expect("mapped event verifies"),
            expected_event
        );
    }

    #[test]
    fn append_rejects_tampering_before_dependency_call() {
        let mut event = fixture_event(1, "018f6f83-7b80-7000-8000-000000000002");
        event.payload = serde_json::json!({"tampered": true});
        let data = super::super::RuntimeData::new(fixture_dependency());

        assert!(matches!(
            data.append_event(AppendEventDataRequest {
                session_directory: PathBuf::from("session-a"),
                event,
                durability: JournalDurability::Buffered,
            }),
            Err(JournalDataError::EventIntegrity {
                operation: "append",
                ..
            })
        ));
        assert!(data.dependency.appends.borrow().is_empty());
    }

    #[test]
    fn scan_normalizes_records_and_rechecks_identity_and_chain() {
        let first = fixture_event(1, "018f6f83-7b80-7000-8000-000000000011");
        let second = fixture_event(2, "018f6f83-7b80-7000-8000-000000000012");
        let first_hash = fixture_hash("first-frame");
        let second_hash = fixture_hash("second-frame");
        let mut dependency = fixture_dependency();
        dependency.scan_result = Ok(DependencyScanJournalResponse {
            records: vec![
                dependency_record(&first, first_hash, None, 0),
                dependency_record(&second, second_hash, Some(first_hash), 128),
            ],
            valid_bytes: 256,
        });
        let data = super::super::RuntimeData::new(dependency);

        let scanned = data
            .scan_events(ScanEventsDataRequest {
                session_directory: PathBuf::from("session-a"),
            })
            .expect("scan succeeds");

        assert_eq!(scanned.valid_bytes, ByteCount::new(256));
        assert_eq!(scanned.events.len(), 2);
        assert_eq!(scanned.events[0].event, first);
        assert_eq!(scanned.events[1].event, second);
        assert_eq!(
            scanned.events[1].previous_journal_checksum,
            Some(first_hash)
        );
        assert_eq!(
            data.dependency.scans.into_inner(),
            vec![DependencyScanJournalRequest {
                session_directory: PathBuf::from("session-a")
            }]
        );
    }

    #[test]
    fn scan_rejects_envelope_tampering() {
        let event = fixture_event(1, "018f6f83-7b80-7000-8000-000000000021");
        let mut record = dependency_record(&event, fixture_hash("frame"), None, 0);
        let mut tampered: EventEnvelope<Value> =
            serde_json::from_slice(&record.event_json).expect("decode fixture");
        tampered.payload = serde_json::json!({"tampered": true});
        record.event_json = serde_json::to_vec(&tampered).expect("encode tampering");
        let mut dependency = fixture_dependency();
        dependency.scan_result = Ok(DependencyScanJournalResponse {
            records: vec![record],
            valid_bytes: 128,
        });
        let data = super::super::RuntimeData::new(dependency);

        assert!(matches!(
            data.scan_events(ScanEventsDataRequest {
                session_directory: PathBuf::from("session-a")
            }),
            Err(JournalDataError::EventIntegrity {
                operation: "scan",
                ..
            })
        ));
    }

    #[test]
    fn scan_rejects_frame_and_envelope_sequence_mismatch() {
        let event = fixture_event(2, "018f6f83-7b80-7000-8000-000000000031");
        let mut record = dependency_record(&event, fixture_hash("frame"), None, 0);
        record.sequence = 1;
        let mut dependency = fixture_dependency();
        dependency.scan_result = Ok(DependencyScanJournalResponse {
            records: vec![record],
            valid_bytes: 128,
        });
        let data = super::super::RuntimeData::new(dependency);

        assert_eq!(
            data.scan_events(ScanEventsDataRequest {
                session_directory: PathBuf::from("session-a")
            }),
            Err(JournalDataError::EventSequenceMismatch {
                frame: Sequence::FIRST,
                envelope: Sequence::new(2).expect("sequence fixture"),
            })
        );
    }

    #[test]
    fn scan_rejects_frame_and_envelope_event_id_mismatch() {
        let event = fixture_event(1, "018f6f83-7b80-7000-8000-000000000041");
        let mut record = dependency_record(&event, fixture_hash("frame"), None, 0);
        let frame_id =
            EventId::from_str("018f6f83-7b80-7000-8000-000000000042").expect("frame ID fixture");
        record.event_id = frame_id.to_string();
        let mut dependency = fixture_dependency();
        dependency.scan_result = Ok(DependencyScanJournalResponse {
            records: vec![record],
            valid_bytes: 128,
        });
        let data = super::super::RuntimeData::new(dependency);

        assert_eq!(
            data.scan_events(ScanEventsDataRequest {
                session_directory: PathBuf::from("session-a")
            }),
            Err(JournalDataError::EventIdMismatch {
                frame: frame_id,
                envelope: event.metadata.event_id,
            })
        );
    }

    #[test]
    fn dependency_errors_are_translated_without_leaking_dependency_type() {
        let mut dependency = fixture_dependency();
        dependency.scan_result = Err(JournalDependencyError::DuplicateEventId(
            "duplicate".to_owned(),
        ));
        let data = super::super::RuntimeData::new(dependency);

        assert_eq!(
            data.scan_events(ScanEventsDataRequest {
                session_directory: PathBuf::from("session-a")
            }),
            Err(JournalDataError::Dependency {
                code: JournalDependencyFailureCode::DuplicateEventId,
                message: "duplicate event ID: duplicate".to_owned(),
            })
        );
    }

    #[test]
    fn recovery_maps_request_and_returns_only_safe_file_name() {
        let mut dependency = fixture_dependency();
        dependency.recovery_result = Ok(DependencyRecoverJournalResponse {
            repaired: true,
            valid_bytes: 512,
            quarantine_path: Some(
                Path::new("session-a")
                    .join("quarantine")
                    .join("events-tail-crash.bin"),
            ),
        });
        let data = super::super::RuntimeData::new(dependency);

        let recovered = data
            .recover_journal(RecoverJournalDataRequest {
                session_directory: PathBuf::from("session-a"),
                quarantine_label: "crash".to_owned(),
            })
            .expect("recovery succeeds");

        assert_eq!(recovered.valid_bytes, ByteCount::new(512));
        assert_eq!(
            recovered.status,
            JournalRecoveryStatus::TailQuarantined {
                quarantine_file_name: "events-tail-crash.bin".to_owned()
            }
        );
        assert_eq!(
            data.dependency.recoveries.into_inner(),
            vec![DependencyRecoverJournalRequest {
                session_directory: PathBuf::from("session-a"),
                quarantine_label: "crash".to_owned(),
            }]
        );
    }

    #[test]
    fn recovery_rejects_inconsistent_dependency_result() {
        let mut dependency = fixture_dependency();
        dependency.recovery_result = Ok(DependencyRecoverJournalResponse {
            repaired: false,
            valid_bytes: 0,
            quarantine_path: Some(PathBuf::from("unexpected.bin")),
        });
        let data = super::super::RuntimeData::new(dependency);

        assert_eq!(
            data.recover_journal(RecoverJournalDataRequest {
                session_directory: PathBuf::from("session-a"),
                quarantine_label: "crash".to_owned(),
            }),
            Err(JournalDataError::InvalidRecoveryResult)
        );
    }
}
