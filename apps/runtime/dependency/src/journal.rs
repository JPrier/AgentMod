//! Checksummed canonical JSONL journal adapter.

use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

const JOURNAL_FILE: &str = "events.jsonl";

/// Configurable persistence guarantee for an append.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyDurability {
    /// Flush userspace buffers but allow operating-system batching.
    Buffered,
    /// Flush and synchronize journal data before returning.
    Data,
    /// Flush and synchronize journal data and metadata before returning.
    Full,
}

/// Dependency-owned append request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyAppendJournalRequest {
    /// Durable session directory.
    pub session_directory: PathBuf,
    /// Sequence expected immediately after the current valid tail.
    pub sequence: u64,
    /// Event ID used for duplicate detection.
    pub event_id: String,
    /// Exact typed event envelope JSON supplied by runtime data.
    pub event_json: Vec<u8>,
    /// Configured durability.
    pub durability: DependencyDurability,
}

/// Dependency-owned successful append response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyAppendJournalResponse {
    /// Byte offset at which this frame starts.
    pub offset: u64,
    /// Checksum of the complete logical record.
    pub checksum: String,
    /// Total bytes in the journal after append.
    pub journal_bytes: u64,
}

/// Dependency-owned scan request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyScanJournalRequest {
    /// Durable session directory.
    pub session_directory: PathBuf,
}

/// Verified dependency record returned from a scan.
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyJournalRecord {
    /// Strictly monotonic sequence.
    pub sequence: u64,
    /// Event ID.
    pub event_id: String,
    /// Stored checksum.
    pub checksum: String,
    /// Previous checksum in the chain.
    pub previous_checksum: Option<String>,
    /// Parsed typed envelope without leaking serde JSON outside dependency.
    pub event_json: Vec<u8>,
    /// Starting byte offset.
    pub offset: u64,
}

/// Verified scan result.
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyScanJournalResponse {
    /// Valid ordered records.
    pub records: Vec<DependencyJournalRecord>,
    /// Total verified bytes.
    pub valid_bytes: u64,
}

/// Dependency-owned recovery request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyRecoverJournalRequest {
    /// Durable session directory.
    pub session_directory: PathBuf,
    /// Caller-supplied unique label from an injected ID dependency.
    pub quarantine_label: String,
}

/// Tail recovery result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyRecoverJournalResponse {
    /// Whether bytes were removed from the canonical tail.
    pub repaired: bool,
    /// Bytes retained in the valid journal.
    pub valid_bytes: u64,
    /// Quarantined invalid-tail file, when repaired.
    pub quarantine_path: Option<PathBuf>,
}

/// Runtime journal external dependency contract.
pub trait JournalDependencyPort {
    /// Appends a complete checksummed frame.
    ///
    /// # Errors
    ///
    /// Returns [`JournalDependencyError`] for invalid input, stale sequences,
    /// corruption, duplicate IDs, locking failures, or persistence failures.
    fn append(
        &self,
        request: DependencyAppendJournalRequest,
    ) -> Result<DependencyAppendJournalResponse, JournalDependencyError>;

    /// Reads and validates a complete journal without modifying it.
    ///
    /// # Errors
    ///
    /// Returns [`JournalDependencyError`] for an invalid record or checksum chain, or
    /// when the journal cannot be read safely.
    fn scan(
        &self,
        request: DependencyScanJournalRequest,
    ) -> Result<DependencyScanJournalResponse, JournalDependencyError>;

    /// Quarantines and truncates only an invalid final record.
    ///
    /// # Errors
    ///
    /// Returns [`JournalDependencyError`] for unsafe labels, interior corruption, or
    /// filesystem failures. Interior corruption is never truncated.
    fn recover_tail(
        &self,
        request: DependencyRecoverJournalRequest,
    ) -> Result<DependencyRecoverJournalResponse, JournalDependencyError>;
}

/// Filesystem-backed checksummed JSONL journal.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonlJournalDependency;

#[derive(Debug, Deserialize, Serialize)]
struct StoredFrame {
    sequence: u64,
    event_id: String,
    previous_checksum: Option<String>,
    checksum: String,
    payload_bytes: usize,
    event: Value,
}

#[derive(Serialize)]
struct ChecksumMaterial<'a> {
    sequence: u64,
    event_id: &'a str,
    previous_checksum: Option<&'a str>,
    event: &'a Value,
}

impl JournalDependencyPort for JsonlJournalDependency {
    fn append(
        &self,
        request: DependencyAppendJournalRequest,
    ) -> Result<DependencyAppendJournalResponse, JournalDependencyError> {
        validate_sequence(request.sequence)?;
        fs::create_dir_all(&request.session_directory).map_err(io_error)?;
        let journal_path = request.session_directory.join(JOURNAL_FILE);
        let mut file = open_journal(&journal_path)?;
        file.lock_exclusive().map_err(io_error)?;

        let result = (|| {
            file.seek(SeekFrom::Start(0)).map_err(io_error)?;
            let scan = scan_reader(&mut file)?;
            let expected = scan
                .records
                .last()
                .map_or(1, |record| record.sequence.saturating_add(1));
            if request.sequence != expected {
                return Err(JournalDependencyError::SequenceMismatch {
                    expected,
                    actual: request.sequence,
                });
            }
            if scan
                .records
                .iter()
                .any(|record| record.event_id == request.event_id)
            {
                return Err(JournalDependencyError::DuplicateEventId(request.event_id));
            }
            let event: Value = serde_json::from_slice(&request.event_json)
                .map_err(|error| JournalDependencyError::InvalidEventJson(error.to_string()))?;
            let previous_checksum = scan.records.last().map(|record| record.checksum.clone());
            let checksum = checksum_record(
                request.sequence,
                &request.event_id,
                previous_checksum.as_deref(),
                &event,
            )?;
            let frame = StoredFrame {
                sequence: request.sequence,
                event_id: request.event_id,
                previous_checksum,
                checksum: checksum.clone(),
                payload_bytes: request.event_json.len(),
                event,
            };
            let mut encoded = serde_json::to_vec(&frame).map_err(json_error)?;
            encoded.push(b'\n');

            let offset = scan.valid_bytes;
            file.seek(SeekFrom::Start(offset)).map_err(io_error)?;
            file.write_all(&encoded).map_err(io_error)?;
            file.flush().map_err(io_error)?;
            match request.durability {
                DependencyDurability::Buffered => {}
                DependencyDurability::Data => file.sync_data().map_err(io_error)?,
                DependencyDurability::Full => file.sync_all().map_err(io_error)?,
            }
            let journal_bytes = offset
                .checked_add(
                    u64::try_from(encoded.len())
                        .map_err(|_| JournalDependencyError::LengthOverflow)?,
                )
                .ok_or(JournalDependencyError::LengthOverflow)?;
            Ok(DependencyAppendJournalResponse {
                offset,
                checksum,
                journal_bytes,
            })
        })();

        let unlock_result = fs2::FileExt::unlock(&file).map_err(io_error);
        match (result, unlock_result) {
            (Ok(response), Ok(())) => Ok(response),
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        }
    }

    fn scan(
        &self,
        request: DependencyScanJournalRequest,
    ) -> Result<DependencyScanJournalResponse, JournalDependencyError> {
        let path = request.session_directory.join(JOURNAL_FILE);
        if !path.exists() {
            return Ok(DependencyScanJournalResponse {
                records: Vec::new(),
                valid_bytes: 0,
            });
        }
        let mut file = OpenOptions::new().read(true).open(path).map_err(io_error)?;
        file.lock_shared().map_err(io_error)?;
        let result = scan_reader(&mut file);
        let unlock_result = fs2::FileExt::unlock(&file).map_err(io_error);
        match (result, unlock_result) {
            (Ok(response), Ok(())) => Ok(response),
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        }
    }

    fn recover_tail(
        &self,
        request: DependencyRecoverJournalRequest,
    ) -> Result<DependencyRecoverJournalResponse, JournalDependencyError> {
        if request.quarantine_label.is_empty()
            || !request
                .quarantine_label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(JournalDependencyError::InvalidQuarantineLabel);
        }
        let journal_path = request.session_directory.join(JOURNAL_FILE);
        if !journal_path.exists() {
            return Ok(DependencyRecoverJournalResponse {
                repaired: false,
                valid_bytes: 0,
                quarantine_path: None,
            });
        }
        let mut file = open_journal(&journal_path)?;
        file.lock_exclusive().map_err(io_error)?;
        let result = (|| {
            file.seek(SeekFrom::Start(0)).map_err(io_error)?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).map_err(io_error)?;
            match locate_invalid_tail(&bytes)? {
                None => Ok(DependencyRecoverJournalResponse {
                    repaired: false,
                    valid_bytes: u64::try_from(bytes.len())
                        .map_err(|_| JournalDependencyError::LengthOverflow)?,
                    quarantine_path: None,
                }),
                Some(valid_length) => {
                    let invalid = &bytes[valid_length..];
                    let quarantine_directory = request.session_directory.join("quarantine");
                    fs::create_dir_all(&quarantine_directory).map_err(io_error)?;
                    let quarantine_path = quarantine_directory
                        .join(format!("events-tail-{}.bin", request.quarantine_label));
                    write_new_file(&quarantine_path, invalid)?;
                    file.set_len(
                        u64::try_from(valid_length)
                            .map_err(|_| JournalDependencyError::LengthOverflow)?,
                    )
                    .map_err(io_error)?;
                    file.sync_all().map_err(io_error)?;
                    Ok(DependencyRecoverJournalResponse {
                        repaired: true,
                        valid_bytes: u64::try_from(valid_length)
                            .map_err(|_| JournalDependencyError::LengthOverflow)?,
                        quarantine_path: Some(quarantine_path),
                    })
                }
            }
        })();
        let unlock_result = fs2::FileExt::unlock(&file).map_err(io_error);
        match (result, unlock_result) {
            (Ok(response), Ok(())) => Ok(response),
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        }
    }
}

impl JournalDependencyPort for crate::LocalRuntimeDependencies {
    fn append(
        &self,
        request: DependencyAppendJournalRequest,
    ) -> Result<DependencyAppendJournalResponse, JournalDependencyError> {
        JsonlJournalDependency.append(request)
    }

    fn scan(
        &self,
        request: DependencyScanJournalRequest,
    ) -> Result<DependencyScanJournalResponse, JournalDependencyError> {
        JsonlJournalDependency.scan(request)
    }

    fn recover_tail(
        &self,
        request: DependencyRecoverJournalRequest,
    ) -> Result<DependencyRecoverJournalResponse, JournalDependencyError> {
        JsonlJournalDependency.recover_tail(request)
    }
}

fn open_journal(path: &Path) -> Result<File, JournalDependencyError> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(io_error)
}

fn scan_reader(file: &mut File) -> Result<DependencyScanJournalResponse, JournalDependencyError> {
    let length = file.metadata().map_err(io_error)?.len();
    let mut reader = BufReader::new(file);
    let mut records = Vec::new();
    let mut valid_bytes = 0_u64;
    let mut expected_sequence = 1_u64;
    let mut previous_checksum: Option<String> = None;
    let mut event_ids = std::collections::BTreeSet::new();
    let mut line = Vec::new();

    loop {
        line.clear();
        let count = reader.read_until(b'\n', &mut line).map_err(io_error)?;
        if count == 0 {
            break;
        }
        let line_start = valid_bytes;
        if line.last() != Some(&b'\n') {
            return Err(JournalDependencyError::InvalidTrailingRecord { offset: line_start });
        }
        let content = &line[..line.len() - 1];
        let stored: StoredFrame = serde_json::from_slice(content).map_err(|error| {
            JournalDependencyError::CorruptRecord {
                offset: line_start,
                reason: error.to_string(),
            }
        })?;
        validate_stored_frame(
            &stored,
            expected_sequence,
            previous_checksum.as_deref(),
            line_start,
        )?;
        if !event_ids.insert(stored.event_id.clone()) {
            return Err(JournalDependencyError::DuplicateEventId(stored.event_id));
        }
        let event_json = serde_json::to_vec(&stored.event).map_err(json_error)?;
        records.push(DependencyJournalRecord {
            sequence: stored.sequence,
            event_id: stored.event_id,
            checksum: stored.checksum.clone(),
            previous_checksum: stored.previous_checksum,
            event_json,
            offset: line_start,
        });
        previous_checksum = Some(stored.checksum);
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or(JournalDependencyError::SequenceOverflow)?;
        valid_bytes = valid_bytes
            .checked_add(u64::try_from(count).map_err(|_| JournalDependencyError::LengthOverflow)?)
            .ok_or(JournalDependencyError::LengthOverflow)?;
    }

    if valid_bytes != length {
        return Err(JournalDependencyError::InvalidTrailingRecord {
            offset: valid_bytes,
        });
    }
    Ok(DependencyScanJournalResponse {
        records,
        valid_bytes,
    })
}

fn validate_stored_frame(
    stored: &StoredFrame,
    expected_sequence: u64,
    expected_previous: Option<&str>,
    offset: u64,
) -> Result<(), JournalDependencyError> {
    if stored.sequence != expected_sequence {
        return Err(JournalDependencyError::CorruptRecord {
            offset,
            reason: format!(
                "expected sequence {expected_sequence}, found {}",
                stored.sequence
            ),
        });
    }
    if stored.previous_checksum.as_deref() != expected_previous {
        return Err(JournalDependencyError::CorruptRecord {
            offset,
            reason: "previous checksum does not match the valid prefix".into(),
        });
    }
    let expected = checksum_record(
        stored.sequence,
        &stored.event_id,
        stored.previous_checksum.as_deref(),
        &stored.event,
    )?;
    if stored.checksum != expected {
        return Err(JournalDependencyError::CorruptRecord {
            offset,
            reason: "record checksum mismatch".into(),
        });
    }
    Ok(())
}

fn checksum_record(
    sequence: u64,
    event_id: &str,
    previous_checksum: Option<&str>,
    event: &Value,
) -> Result<String, JournalDependencyError> {
    let material = ChecksumMaterial {
        sequence,
        event_id,
        previous_checksum,
        event,
    };
    let bytes = serde_json::to_vec(&material).map_err(json_error)?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

fn locate_invalid_tail(bytes: &[u8]) -> Result<Option<usize>, JournalDependencyError> {
    let mut offset = 0_usize;
    let mut expected_sequence = 1_u64;
    let mut previous_checksum: Option<String> = None;
    let mut event_ids = std::collections::BTreeSet::new();
    let lines: Vec<&[u8]> = bytes.split_inclusive(|byte| *byte == b'\n').collect();

    for (index, line) in lines.iter().enumerate() {
        let is_last = index + 1 == lines.len();
        let terminated = line.last() == Some(&b'\n');
        let content = if terminated {
            &line[..line.len() - 1]
        } else {
            *line
        };
        let parsed = serde_json::from_slice::<StoredFrame>(content);
        let valid = parsed.as_ref().ok().is_some_and(|stored| {
            validate_stored_frame(
                stored,
                expected_sequence,
                previous_checksum.as_deref(),
                u64::try_from(offset).unwrap_or(u64::MAX),
            )
            .is_ok()
                && !event_ids.contains(&stored.event_id)
        });
        if !terminated || !valid {
            if is_last {
                return Ok(Some(offset));
            }
            return Err(JournalDependencyError::InteriorCorruption {
                offset: u64::try_from(offset)
                    .map_err(|_| JournalDependencyError::LengthOverflow)?,
            });
        }
        let stored = parsed.expect("valid was established without mutation");
        event_ids.insert(stored.event_id);
        previous_checksum = Some(stored.checksum);
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or(JournalDependencyError::SequenceOverflow)?;
        offset = offset
            .checked_add(line.len())
            .ok_or(JournalDependencyError::LengthOverflow)?;
    }
    Ok(None)
}

fn write_new_file(path: &Path, content: &[u8]) -> Result<(), JournalDependencyError> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(io_error)?;
    file.write_all(content).map_err(io_error)?;
    file.sync_all().map_err(io_error)
}

fn validate_sequence(sequence: u64) -> Result<(), JournalDependencyError> {
    if sequence == 0 {
        Err(JournalDependencyError::SequenceMismatch {
            expected: 1,
            actual: 0,
        })
    } else {
        Ok(())
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err callbacks receive owned external error values"
)]
fn io_error(error: std::io::Error) -> JournalDependencyError {
    JournalDependencyError::Io(error.to_string())
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err callbacks receive owned external error values"
)]
fn json_error(error: serde_json::Error) -> JournalDependencyError {
    JournalDependencyError::Json(error.to_string())
}

/// Journal adapter failure hidden below the data boundary.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum JournalDependencyError {
    /// Filesystem/locking operation failed.
    #[error("journal I/O failed: {0}")]
    Io(String),
    /// JSON encoding failed.
    #[error("journal JSON encoding failed: {0}")]
    Json(String),
    /// Typed event JSON supplied by data was invalid.
    #[error("event JSON is invalid: {0}")]
    InvalidEventJson(String),
    /// Requested sequence does not immediately follow the valid tail.
    #[error("journal sequence mismatch: expected {expected}, received {actual}")]
    SequenceMismatch {
        /// Next accepted sequence.
        expected: u64,
        /// Requested sequence.
        actual: u64,
    },
    /// An event ID already exists in the journal.
    #[error("duplicate event ID: {0}")]
    DuplicateEventId(String),
    /// Final bytes do not form a complete record.
    #[error("invalid trailing journal record at byte {offset}")]
    InvalidTrailingRecord {
        /// First invalid byte.
        offset: u64,
    },
    /// A completed record is corrupt.
    #[error("corrupt journal record at byte {offset}: {reason}")]
    CorruptRecord {
        /// Record start.
        offset: u64,
        /// Integrity reason.
        reason: String,
    },
    /// Corruption before the final record cannot be automatically repaired.
    #[error("interior journal corruption at byte {offset}; session must be quarantined")]
    InteriorCorruption {
        /// First invalid record.
        offset: u64,
    },
    /// Quarantine file label was unsafe.
    #[error("quarantine label must contain only ASCII letters, digits, '-' or '_'")]
    InvalidQuarantineLabel,
    /// Sequence arithmetic overflowed.
    #[error("journal sequence overflow")]
    SequenceOverflow,
    /// File length cannot be represented safely.
    #[error("journal length overflow")]
    LengthOverflow,
}

#[cfg(test)]
mod tests {
    use std::{
        fs::OpenOptions,
        io::Write,
        sync::{Arc, Barrier},
    };

    use super::*;

    fn append(
        journal: JsonlJournalDependency,
        root: &Path,
        sequence: u64,
        id: &str,
    ) -> Result<DependencyAppendJournalResponse, JournalDependencyError> {
        journal.append(DependencyAppendJournalRequest {
            session_directory: root.to_owned(),
            sequence,
            event_id: id.into(),
            event_json: serde_json::to_vec(&serde_json::json!({
                "metadata": {"sequence": sequence},
                "payload": {"fixture": id}
            }))
            .expect("fixture JSON"),
            durability: DependencyDurability::Full,
        })
    }

    #[test]
    fn appends_and_scans_checksum_chain() {
        let directory = tempfile::tempdir().expect("temp directory");
        let journal = JsonlJournalDependency;
        let first = append(journal, directory.path(), 1, "event-1").expect("first");
        let second = append(journal, directory.path(), 2, "event-2").expect("second");
        assert_ne!(first.checksum, second.checksum);

        let scan = journal
            .scan(DependencyScanJournalRequest {
                session_directory: directory.path().to_owned(),
            })
            .expect("scan");
        assert_eq!(scan.records.len(), 2);
        assert_eq!(
            scan.records[1].previous_checksum.as_deref(),
            Some(first.checksum.as_str())
        );
        assert_eq!(scan.valid_bytes, second.journal_bytes);
    }

    #[test]
    fn sequence_and_duplicate_event_id_are_rejected() {
        let directory = tempfile::tempdir().expect("temp directory");
        let journal = JsonlJournalDependency;
        append(journal, directory.path(), 1, "same").expect("first");
        assert_eq!(
            append(journal, directory.path(), 3, "later"),
            Err(JournalDependencyError::SequenceMismatch {
                expected: 2,
                actual: 3
            })
        );
        assert_eq!(
            append(journal, directory.path(), 2, "same"),
            Err(JournalDependencyError::DuplicateEventId("same".into()))
        );
    }

    #[test]
    fn partial_final_record_is_quarantined_and_truncated() {
        let directory = tempfile::tempdir().expect("temp directory");
        let journal = JsonlJournalDependency;
        let first = append(journal, directory.path(), 1, "event-1").expect("first");
        let path = directory.path().join(JOURNAL_FILE);
        let mut file = OpenOptions::new().append(true).open(&path).expect("open");
        file.write_all(b"{\"sequence\":2").expect("partial write");
        file.sync_all().expect("sync");

        assert!(matches!(
            journal.scan(DependencyScanJournalRequest {
                session_directory: directory.path().to_owned()
            }),
            Err(JournalDependencyError::InvalidTrailingRecord { .. })
        ));
        let recovered = journal
            .recover_tail(DependencyRecoverJournalRequest {
                session_directory: directory.path().to_owned(),
                quarantine_label: "crash-fixture".into(),
            })
            .expect("recover");
        assert!(recovered.repaired);
        assert_eq!(recovered.valid_bytes, first.journal_bytes);
        let quarantine = recovered.quarantine_path.expect("quarantine path");
        assert_eq!(
            fs::read(quarantine).expect("quarantine bytes"),
            b"{\"sequence\":2"
        );
        assert_eq!(
            journal
                .scan(DependencyScanJournalRequest {
                    session_directory: directory.path().to_owned()
                })
                .expect("valid after recovery")
                .records
                .len(),
            1
        );
    }

    #[test]
    fn interior_corruption_is_never_truncated() {
        let directory = tempfile::tempdir().expect("temp directory");
        let journal = JsonlJournalDependency;
        append(journal, directory.path(), 1, "event-1").expect("first");
        append(journal, directory.path(), 2, "event-2").expect("second");
        let path = directory.path().join(JOURNAL_FILE);
        let mut bytes = fs::read(&path).expect("read");
        let first_newline = bytes.iter().position(|byte| *byte == b'\n').expect("line");
        bytes[first_newline / 2] ^= 1;
        fs::write(&path, &bytes).expect("corrupt fixture");

        assert!(matches!(
            journal.recover_tail(DependencyRecoverJournalRequest {
                session_directory: directory.path().to_owned(),
                quarantine_label: "interior".into(),
            }),
            Err(JournalDependencyError::InteriorCorruption { .. })
        ));
        assert_eq!(fs::read(path).expect("unchanged bytes"), bytes);
    }

    #[test]
    fn concurrent_append_serializes_and_rejects_stale_sequence() {
        let directory = tempfile::tempdir().expect("temp directory");
        let root = Arc::new(directory.path().to_owned());
        let barrier = Arc::new(Barrier::new(3));
        let threads: Vec<_> = ["a", "b"]
            .into_iter()
            .map(|id| {
                let root = Arc::clone(&root);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    append(JsonlJournalDependency, &root, 1, id)
                })
            })
            .collect();
        barrier.wait();
        let results: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().expect("join"))
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(
                    result,
                    Err(JournalDependencyError::SequenceMismatch {
                        expected: 2,
                        actual: 1
                    })
                ))
                .count(),
            1
        );
    }
}
