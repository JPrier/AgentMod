//! Business-facing snapshot dataset construction and normalization.

use std::{collections::BTreeMap, path::PathBuf};

use agentmod_runtime_dependency::snapshot::{
    DependencyLoadSnapshotRequest, DependencyPersistSnapshotRequest,
    DependencyPersistSnapshotResponse, DependencyScanSnapshotsRequest, DependencySnapshotMetadata,
    DependencySnapshotRecord, SnapshotDependencyError, SnapshotDependencyPort,
};
use thiserror::Error;

use crate::RuntimeData;

const SNAPSHOT_SCHEMA_VERSION: u16 = 1;

/// Data-layer journal anchor accepted for snapshot restoration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataSnapshotAnchor {
    /// Event sequence.
    pub event_sequence: u64,
    /// Expected terminal event checksum.
    pub terminal_event_checksum: String,
}

/// Data-layer request to persist normalized reducer state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistSnapshotDataRequest {
    /// Session directory selected by session data.
    pub session_directory: PathBuf,
    /// Current pure reducer version.
    pub reducer_version: u32,
    /// Last event represented by state.
    pub anchor: DataSnapshotAnchor,
    /// JSON bytes supplied by runtime logic.
    pub state_json: Vec<u8>,
}

/// Data-layer request to select the latest compatible snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadLatestSnapshotDataRequest {
    /// Session directory selected by session data.
    pub session_directory: PathBuf,
    /// Reducer version capable of reading the state.
    pub expected_reducer_version: u32,
    /// Verified journal anchors available for restoration.
    pub valid_anchors: Vec<DataSnapshotAnchor>,
}

/// Normalized business snapshot metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataSnapshotMetadataRecord {
    /// Dependency-safe immutable name used only for later data access.
    pub snapshot_name: String,
    /// Snapshot format version.
    pub schema_version: u16,
    /// Reducer implementation version.
    pub reducer_version: u32,
    /// Verified terminal journal anchor.
    pub anchor: DataSnapshotAnchor,
    /// BLAKE3 digest of normalized JSON state.
    pub state_content_hash: String,
    /// Complete snapshot bytes.
    pub snapshot_bytes: u64,
}

/// Persisted snapshot business record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistSnapshotDataRecord {
    /// Normalized metadata.
    pub metadata: DataSnapshotMetadataRecord,
    /// Whether immutable storage already contained the identical snapshot.
    pub deduplicated: bool,
}

/// Loaded and verified normalized snapshot state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedSnapshotDataRecord {
    /// Normalized metadata.
    pub metadata: DataSnapshotMetadataRecord,
    /// Canonical compact JSON bytes.
    pub state_json: Vec<u8>,
    /// Number of corrupt dependency files ignored during selection.
    pub ignored_invalid_snapshots: u64,
}

/// Business-facing snapshot operations consumed by runtime logic.
pub trait SnapshotDataPort {
    /// Normalizes JSON, hashes it, and persists a versioned snapshot.
    ///
    /// # Errors
    ///
    /// Returns a data error for invalid JSON or translated dependency failure.
    fn persist_snapshot_data(
        &self,
        request: PersistSnapshotDataRequest,
    ) -> Result<PersistSnapshotDataRecord, SnapshotDataError>;

    /// Selects, loads, and verifies the latest schema/reducer/anchor-compatible
    /// snapshot.
    ///
    /// # Errors
    ///
    /// Returns a data error for invalid expectations, dependency failure,
    /// metadata mismatch, non-canonical JSON, or hash mismatch.
    fn load_latest_snapshot_data(
        &self,
        request: LoadLatestSnapshotDataRequest,
    ) -> Result<Option<LoadedSnapshotDataRecord>, SnapshotDataError>;
}

impl<D> SnapshotDataPort for RuntimeData<D>
where
    D: SnapshotDependencyPort,
{
    fn persist_snapshot_data(
        &self,
        request: PersistSnapshotDataRequest,
    ) -> Result<PersistSnapshotDataRecord, SnapshotDataError> {
        validate_anchor(&request.anchor)?;
        if request.reducer_version == 0 {
            return Err(SnapshotDataError::InvalidReducerVersion);
        }
        let state_json = normalize_json(&request.state_json)?;
        let state_content_hash = blake3::hash(&state_json).to_hex().to_string();
        let response = self
            .dependency
            .persist_snapshot(DependencyPersistSnapshotRequest {
                session_directory: request.session_directory,
                schema_version: SNAPSHOT_SCHEMA_VERSION,
                reducer_version: request.reducer_version,
                event_sequence: request.anchor.event_sequence,
                terminal_event_checksum: request.anchor.terminal_event_checksum,
                state_content_hash,
                state_bytes: state_json,
            })
            .map_err(|error| translate_dependency("persist", &error))?;
        map_persist_response(response)
    }

    fn load_latest_snapshot_data(
        &self,
        request: LoadLatestSnapshotDataRequest,
    ) -> Result<Option<LoadedSnapshotDataRecord>, SnapshotDataError> {
        if request.expected_reducer_version == 0 {
            return Err(SnapshotDataError::InvalidReducerVersion);
        }
        if request.valid_anchors.is_empty() {
            return Err(SnapshotDataError::EmptyAnchorSet);
        }
        for anchor in &request.valid_anchors {
            validate_anchor(anchor)?;
        }

        let scan = self
            .dependency
            .scan_snapshots(DependencyScanSnapshotsRequest {
                session_directory: request.session_directory.clone(),
            })
            .map_err(|error| translate_dependency("scan", &error))?;
        let selected = scan
            .valid
            .iter()
            .rev()
            .find(|metadata| {
                metadata.schema_version == SNAPSHOT_SCHEMA_VERSION
                    && metadata.reducer_version == request.expected_reducer_version
                    && anchor_matches(
                        &request.valid_anchors,
                        metadata.event_sequence,
                        &metadata.terminal_event_checksum,
                    )
            })
            .cloned();
        let Some(selected) = selected else {
            return Ok(None);
        };
        let loaded = self
            .dependency
            .load_snapshot(DependencyLoadSnapshotRequest {
                session_directory: request.session_directory,
                snapshot_name: selected.snapshot_name.clone(),
            })
            .map_err(|error| translate_dependency("load", &error))?;
        verify_loaded(
            loaded,
            &selected,
            request.expected_reducer_version,
            &request.valid_anchors,
            u64::try_from(scan.invalid.len()).map_err(|_| SnapshotDataError::CountOverflow)?,
        )
        .map(Some)
    }
}

fn map_persist_response(
    response: DependencyPersistSnapshotResponse,
) -> Result<PersistSnapshotDataRecord, SnapshotDataError> {
    Ok(PersistSnapshotDataRecord {
        metadata: map_metadata(response.metadata)?,
        deduplicated: response.deduplicated,
    })
}

fn map_metadata(
    metadata: DependencySnapshotMetadata,
) -> Result<DataSnapshotMetadataRecord, SnapshotDataError> {
    let anchor = DataSnapshotAnchor {
        event_sequence: metadata.event_sequence,
        terminal_event_checksum: metadata.terminal_event_checksum,
    };
    validate_anchor(&anchor)?;
    if metadata.schema_version == 0
        || metadata.reducer_version == 0
        || !is_hash(&metadata.state_content_hash)
    {
        return Err(SnapshotDataError::DependencyRecordInvalid);
    }
    Ok(DataSnapshotMetadataRecord {
        snapshot_name: metadata.snapshot_name,
        schema_version: metadata.schema_version,
        reducer_version: metadata.reducer_version,
        anchor,
        state_content_hash: metadata.state_content_hash,
        snapshot_bytes: metadata.snapshot_bytes,
    })
}

fn verify_loaded(
    loaded: DependencySnapshotRecord,
    selected: &DependencySnapshotMetadata,
    expected_reducer_version: u32,
    anchors: &[DataSnapshotAnchor],
    ignored_invalid_snapshots: u64,
) -> Result<LoadedSnapshotDataRecord, SnapshotDataError> {
    if loaded.metadata != *selected {
        return Err(SnapshotDataError::DependencyRecordChanged);
    }
    if loaded.metadata.schema_version != SNAPSHOT_SCHEMA_VERSION
        || loaded.metadata.reducer_version != expected_reducer_version
    {
        return Err(SnapshotDataError::IncompatibleVersion);
    }
    if !anchor_matches(
        anchors,
        loaded.metadata.event_sequence,
        &loaded.metadata.terminal_event_checksum,
    ) {
        return Err(SnapshotDataError::AnchorMismatch);
    }
    let normalized = normalize_json(&loaded.state_bytes)?;
    if normalized != loaded.state_bytes {
        return Err(SnapshotDataError::NonCanonicalState);
    }
    if blake3::hash(&normalized).to_hex().as_str() != loaded.metadata.state_content_hash {
        return Err(SnapshotDataError::StateHashMismatch);
    }
    Ok(LoadedSnapshotDataRecord {
        metadata: map_metadata(loaded.metadata)?,
        state_json: normalized,
        ignored_invalid_snapshots,
    })
}

fn normalize_json(bytes: &[u8]) -> Result<Vec<u8>, SnapshotDataError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| SnapshotDataError::InvalidStateJson)?;
    serde_json::to_vec(&normalize_json_value(&value))
        .map_err(|_| SnapshotDataError::InvalidStateJson)
}

fn normalize_json_value(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let sorted: BTreeMap<_, _> = map
                .iter()
                .map(|(key, value)| (key.clone(), normalize_json_value(value)))
                .collect();
            serde_json::to_value(sorted).unwrap_or(serde_json::Value::Null)
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(normalize_json_value).collect())
        }
        _ => value.clone(),
    }
}

fn validate_anchor(anchor: &DataSnapshotAnchor) -> Result<(), SnapshotDataError> {
    if anchor.event_sequence == 0 || !is_hash(&anchor.terminal_event_checksum) {
        Err(SnapshotDataError::InvalidAnchor)
    } else {
        Ok(())
    }
}

fn anchor_matches(anchors: &[DataSnapshotAnchor], sequence: u64, checksum: &str) -> bool {
    anchors.iter().any(|anchor| {
        anchor.event_sequence == sequence && anchor.terminal_event_checksum == checksum
    })
}

fn is_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn translate_dependency(
    operation: &'static str,
    error: &SnapshotDependencyError,
) -> SnapshotDataError {
    SnapshotDataError::Dependency {
        operation,
        message: error.to_string(),
    }
}

/// Snapshot data-layer error with dependency failures translated to owned text.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SnapshotDataError {
    /// State bytes are not valid JSON.
    #[error("snapshot state is not valid JSON")]
    InvalidStateJson,
    /// Reducer version zero is not restorable.
    #[error("snapshot reducer version is invalid")]
    InvalidReducerVersion,
    /// Journal anchor is malformed.
    #[error("snapshot journal anchor is invalid")]
    InvalidAnchor,
    /// No journal anchors were provided for safe restoration.
    #[error("snapshot restoration requires at least one verified journal anchor")]
    EmptyAnchorSet,
    /// Dependency returned malformed normalized metadata.
    #[error("snapshot dependency returned an invalid record")]
    DependencyRecordInvalid,
    /// Scanned metadata changed before named load.
    #[error("snapshot dependency record changed between scan and load")]
    DependencyRecordChanged,
    /// Loaded schema or reducer no longer matches.
    #[error("snapshot version is incompatible")]
    IncompatibleVersion,
    /// Loaded event anchor is absent from verified journal history.
    #[error("snapshot journal anchor does not match")]
    AnchorMismatch,
    /// Loaded state is valid JSON but not its canonical representation.
    #[error("snapshot state is not normalized JSON")]
    NonCanonicalState,
    /// Loaded state digest differs from metadata.
    #[error("snapshot state hash does not match")]
    StateHashMismatch,
    /// Invalid-entry count exceeded data representation.
    #[error("snapshot invalid-entry count overflow")]
    CountOverflow,
    /// Translated external-adapter failure.
    #[error("snapshot dependency `{operation}` failed: {message}")]
    Dependency {
        /// Failed dependency operation.
        operation: &'static str,
        /// Readable dependency-owned message.
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use agentmod_runtime_dependency::snapshot::{
        DependencyInvalidSnapshot, DependencyScanSnapshotsResponse, DependencySnapshotLimits,
        LocalSnapshotDependency,
    };

    use super::*;

    #[derive(Clone)]
    struct MockSnapshotDependency {
        persisted: RefCell<Vec<DependencyPersistSnapshotRequest>>,
        scan: Result<DependencyScanSnapshotsResponse, SnapshotDependencyError>,
        loaded: RefCell<Option<Result<DependencySnapshotRecord, SnapshotDependencyError>>>,
        loaded_names: RefCell<Vec<String>>,
    }

    impl MockSnapshotDependency {
        fn with_records(
            valid: Vec<DependencySnapshotMetadata>,
            loaded: DependencySnapshotRecord,
        ) -> Self {
            Self {
                persisted: RefCell::new(Vec::new()),
                scan: Ok(DependencyScanSnapshotsResponse {
                    valid,
                    invalid: vec![DependencyInvalidSnapshot {
                        snapshot_name: "broken.bin".into(),
                        reason: "corrupt".into(),
                    }],
                }),
                loaded: RefCell::new(Some(Ok(loaded))),
                loaded_names: RefCell::new(Vec::new()),
            }
        }
    }

    impl SnapshotDependencyPort for MockSnapshotDependency {
        fn persist_snapshot(
            &self,
            request: DependencyPersistSnapshotRequest,
        ) -> Result<DependencyPersistSnapshotResponse, SnapshotDependencyError> {
            self.persisted.borrow_mut().push(request.clone());
            Ok(DependencyPersistSnapshotResponse {
                metadata: metadata(
                    request.event_sequence,
                    request.reducer_version,
                    &request.terminal_event_checksum,
                    &request.state_bytes,
                ),
                deduplicated: false,
            })
        }

        fn scan_snapshots(
            &self,
            _request: DependencyScanSnapshotsRequest,
        ) -> Result<DependencyScanSnapshotsResponse, SnapshotDependencyError> {
            self.scan.clone()
        }

        fn load_snapshot(
            &self,
            request: DependencyLoadSnapshotRequest,
        ) -> Result<DependencySnapshotRecord, SnapshotDependencyError> {
            self.loaded_names.borrow_mut().push(request.snapshot_name);
            self.loaded
                .borrow_mut()
                .take()
                .unwrap_or(Err(SnapshotDependencyError::SnapshotNotFound))
        }

        fn load_latest_valid_snapshot(
            &self,
            _request: DependencyScanSnapshotsRequest,
        ) -> Result<Option<DependencySnapshotRecord>, SnapshotDependencyError> {
            self.loaded.borrow_mut().take().transpose()
        }
    }

    #[test]
    fn persist_maps_and_normalizes_json() {
        let dependency = MockSnapshotDependency {
            persisted: RefCell::new(Vec::new()),
            scan: Ok(DependencyScanSnapshotsResponse {
                valid: Vec::new(),
                invalid: Vec::new(),
            }),
            loaded: RefCell::new(None),
            loaded_names: RefCell::new(Vec::new()),
        };
        let data = RuntimeData::new(dependency);
        let anchor = anchor(7);
        let record = data
            .persist_snapshot_data(PersistSnapshotDataRequest {
                session_directory: PathBuf::from("session"),
                reducer_version: 2,
                anchor: anchor.clone(),
                state_json: br#"{ "z": 1, "a": 2 }"#.to_vec(),
            })
            .expect("persist mapping");
        let observed = data.dependency.persisted.borrow();
        assert_eq!(observed[0].schema_version, SNAPSHOT_SCHEMA_VERSION);
        assert_eq!(observed[0].state_bytes, br#"{"a":2,"z":1}"#);
        assert_eq!(
            observed[0].state_content_hash,
            blake3::hash(br#"{"a":2,"z":1}"#).to_hex().to_string()
        );
        assert_eq!(record.metadata.anchor, anchor);
    }

    #[test]
    fn selects_latest_compatible_anchor_and_translates_record() {
        let state = br#"{"turn":4}"#.to_vec();
        let compatible_three = metadata(3, 2, &anchor(3).terminal_event_checksum, b"old");
        let compatible_four = metadata(4, 2, &anchor(4).terminal_event_checksum, &state);
        let incompatible_five = metadata(5, 99, &anchor(5).terminal_event_checksum, b"future");
        let dependency = MockSnapshotDependency::with_records(
            vec![compatible_three, compatible_four.clone(), incompatible_five],
            DependencySnapshotRecord {
                metadata: compatible_four.clone(),
                state_bytes: state.clone(),
            },
        );
        let data = RuntimeData::new(dependency);
        let loaded = data
            .load_latest_snapshot_data(LoadLatestSnapshotDataRequest {
                session_directory: PathBuf::from("session"),
                expected_reducer_version: 2,
                valid_anchors: vec![anchor(3), anchor(4), anchor(5)],
            })
            .expect("selection")
            .expect("compatible snapshot");
        assert_eq!(loaded.metadata.anchor.event_sequence, 4);
        assert_eq!(loaded.state_json, state);
        assert_eq!(loaded.ignored_invalid_snapshots, 1);
        assert_eq!(
            *data.dependency.loaded_names.borrow(),
            [compatible_four.snapshot_name]
        );
    }

    #[test]
    fn rejects_loaded_hash_and_anchor_mismatch() {
        let state = br#"{"ok":true}"#.to_vec();
        let selected = metadata(2, 1, &anchor(2).terminal_event_checksum, &state);
        let mut changed = selected.clone();
        changed.terminal_event_checksum = anchor(3).terminal_event_checksum;
        let data = RuntimeData::new(MockSnapshotDependency::with_records(
            vec![selected],
            DependencySnapshotRecord {
                metadata: changed,
                state_bytes: state,
            },
        ));
        assert_eq!(
            data.load_latest_snapshot_data(LoadLatestSnapshotDataRequest {
                session_directory: PathBuf::from("session"),
                expected_reducer_version: 1,
                valid_anchors: vec![anchor(2)],
            }),
            Err(SnapshotDataError::DependencyRecordChanged)
        );

        let state = br#"{"ok":true}"#.to_vec();
        let selected = metadata(2, 1, &anchor(2).terminal_event_checksum, &state);
        let mut bad_hash = selected.clone();
        bad_hash.state_content_hash = "0".repeat(64);
        let data = RuntimeData::new(MockSnapshotDependency::with_records(
            vec![selected],
            DependencySnapshotRecord {
                metadata: bad_hash,
                state_bytes: state,
            },
        ));
        assert_eq!(
            data.load_latest_snapshot_data(LoadLatestSnapshotDataRequest {
                session_directory: PathBuf::from("session"),
                expected_reducer_version: 1,
                valid_anchors: vec![anchor(2)],
            }),
            Err(SnapshotDataError::DependencyRecordChanged)
        );
    }

    #[test]
    fn dependency_errors_are_translated() {
        let dependency = MockSnapshotDependency {
            persisted: RefCell::new(Vec::new()),
            scan: Err(SnapshotDependencyError::Storage("offline".into())),
            loaded: RefCell::new(None),
            loaded_names: RefCell::new(Vec::new()),
        };
        let data = RuntimeData::new(dependency);
        assert_eq!(
            data.load_latest_snapshot_data(LoadLatestSnapshotDataRequest {
                session_directory: PathBuf::from("session"),
                expected_reducer_version: 1,
                valid_anchors: vec![anchor(1)],
            }),
            Err(SnapshotDataError::Dependency {
                operation: "scan",
                message: "snapshot storage failed: offline".into(),
            })
        );
    }

    #[test]
    fn data_and_local_dependency_roundtrip() {
        let directory = tempfile::tempdir().expect("temp directory");
        let dependency = LocalSnapshotDependency::new(DependencySnapshotLimits {
            max_state_bytes: 1024,
            max_file_bytes: 2048,
        })
        .expect("local snapshots");
        let data = RuntimeData::new(dependency);
        let anchor = anchor(11);
        data.persist_snapshot_data(PersistSnapshotDataRequest {
            session_directory: directory.path().to_owned(),
            reducer_version: 4,
            anchor: anchor.clone(),
            state_json: br#"{ "tasks": [], "turn": 11 }"#.to_vec(),
        })
        .expect("persist");
        let loaded = data
            .load_latest_snapshot_data(LoadLatestSnapshotDataRequest {
                session_directory: directory.path().to_owned(),
                expected_reducer_version: 4,
                valid_anchors: vec![anchor],
            })
            .expect("load")
            .expect("snapshot");
        assert_eq!(loaded.state_json, br#"{"tasks":[],"turn":11}"#);
    }

    fn anchor(sequence: u64) -> DataSnapshotAnchor {
        DataSnapshotAnchor {
            event_sequence: sequence,
            terminal_event_checksum: blake3::hash(format!("event-{sequence}").as_bytes())
                .to_hex()
                .to_string(),
        }
    }

    fn metadata(
        sequence: u64,
        reducer_version: u32,
        checksum: &str,
        state: &[u8],
    ) -> DependencySnapshotMetadata {
        let hash = blake3::hash(state).to_hex().to_string();
        DependencySnapshotMetadata {
            snapshot_name: format!("snapshot-{sequence:020}-{hash}.bin"),
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            reducer_version,
            event_sequence: sequence,
            terminal_event_checksum: checksum.into(),
            state_content_hash: hash,
            snapshot_bytes: 200,
        }
    }
}
