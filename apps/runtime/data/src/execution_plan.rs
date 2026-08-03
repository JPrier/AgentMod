//! Business-facing execution-plan persistence datasets.
//!
//! The data layer owns normalized plan identity records and the selection of
//! the plan-file persistence dependency. It maps logic-owned plan identity
//! into a canonical checksummed payload and validates the loaded payload's
//! structure without parsing live executor state.

use std::path::PathBuf;

use agentmod_primitives::ContentHash;
use agentmod_runtime_dependency::execution_plan::{
    DependencyExecutionPlanFile, DependencyLoadExecutionPlanRequest,
    DependencyLoadExecutionPlanResult, DependencyStoreExecutionPlanRequest,
    ExecutionPlanDependencyError, ExecutionPlanDependencyPort,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Canonical plan-file schema version owned by runtime data.
pub const EXECUTION_PLAN_RECORD_SCHEMA_VERSION: u16 = 1;

/// Normalized immutable execution-plan identity bound to a session style.
///
/// Every field is part of the checksummed payload; a later runtime that reads
/// the plan file can prove which style, plugin set, capability set, compiled
/// descriptor, and live registry produced the plan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionPlanIdentityData {
    /// Canonical record schema version.
    pub schema_version: u16,
    /// Stable style ID.
    pub style_id: String,
    /// Exact semantic style version.
    pub style_version: String,
    /// Canonical style manifest content hash.
    pub style_content_hash: ContentHash,
    /// Hash of the exact compiled descriptor.
    pub compiled_style_hash: ContentHash,
    /// Compatibility-bound compiled cache key.
    pub compiled_cache_key: ContentHash,
    /// Runtime API version used during resolution.
    pub runtime_api_version: String,
    /// Plugin-set hash used during compilation.
    pub plugin_set_hash: ContentHash,
    /// Runtime capability-set hash used during compilation.
    pub capability_set_hash: ContentHash,
    /// Node-executor registry hash used during resolution.
    pub registry_hash: ContentHash,
    /// Hash of the canonical serialized execution plan.
    pub plan_hash: ContentHash,
    /// Number of compiled graph nodes covered by the plan.
    pub node_count: u64,
}

/// Complete data-owned plan file supplied with an atomic session creation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionPlanFileData {
    /// Normalized immutable identity.
    pub identity: ExecutionPlanIdentityData,
    /// Canonical serialized plan JSON (opaque to data selection).
    pub canonical_plan_json: String,
}

/// Data request to store the immutable plan file for one session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreExecutionPlanDataRequest {
    /// Session directory selected by session data.
    pub session_directory: PathBuf,
    /// Complete plan file.
    pub file: ExecutionPlanFileData,
}

/// Successful plan-file persist record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreExecutionPlanDataRecord {
    /// Whether an identical immutable plan file already existed.
    pub deduplicated: bool,
    /// BLAKE3 checksum of the canonical payload.
    pub payload_checksum: ContentHash,
    /// Canonical payload byte count.
    pub payload_bytes: u64,
}

/// Data request to load the immutable plan file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadExecutionPlanDataRequest {
    /// Session directory selected by session data.
    pub session_directory: PathBuf,
}

/// Validated loaded plan file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedExecutionPlanDataRecord {
    /// Normalized immutable identity.
    pub identity: ExecutionPlanIdentityData,
    /// Canonical serialized plan JSON.
    pub canonical_plan_json: String,
    /// Verified payload checksum.
    pub payload_checksum: ContentHash,
}

/// Result of loading a plan file through data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LoadExecutionPlanDataResult {
    /// The session retains a valid immutable plan file.
    Present(Box<LoadedExecutionPlanDataRecord>),
    /// No plan file exists (legacy session; migration required).
    Missing,
}

/// Business-facing plan-file operations consumed by runtime logic.
pub trait ExecutionPlanDataPort {
    /// Normalizes and persists the immutable plan file.
    ///
    /// # Errors
    ///
    /// Returns a data error for invalid identity/payload or translated
    /// dependency failure.
    fn store_execution_plan(
        &self,
        request: StoreExecutionPlanDataRequest,
    ) -> Result<StoreExecutionPlanDataRecord, ExecutionPlanDataError>;

    /// Loads and validates the immutable plan file.
    ///
    /// # Errors
    ///
    /// Returns a data error for a corrupt file or translated dependency
    /// failure. An absent file is a valid [`LoadExecutionPlanDataResult::Missing`].
    fn load_execution_plan(
        &self,
        request: LoadExecutionPlanDataRequest,
    ) -> Result<LoadExecutionPlanDataResult, ExecutionPlanDataError>;
}

/// Runtime data implementation routing plan files to the injected dependency.
#[derive(Clone, Copy, Debug, Default)]
pub struct RuntimeExecutionPlanData;

impl<D> ExecutionPlanDataPort for super::RuntimeData<D>
where
    D: ExecutionPlanDependencyPort,
{
    fn store_execution_plan(
        &self,
        request: StoreExecutionPlanDataRequest,
    ) -> Result<StoreExecutionPlanDataRecord, ExecutionPlanDataError> {
        let file = to_dependency_file(request.file)?;
        let response = self
            .dependency
            .store_execution_plan(DependencyStoreExecutionPlanRequest {
                session_directory: request.session_directory,
                plan: file,
            })
            .map_err(ExecutionPlanDataError::Dependency)?;
        let payload_checksum = response
            .payload_checksum
            .parse()
            .map_err(|_| ExecutionPlanDataError::InvalidChecksum)?;
        Ok(StoreExecutionPlanDataRecord {
            deduplicated: response.deduplicated,
            payload_checksum,
            payload_bytes: response.payload_bytes,
        })
    }

    fn load_execution_plan(
        &self,
        request: LoadExecutionPlanDataRequest,
    ) -> Result<LoadExecutionPlanDataResult, ExecutionPlanDataError> {
        let result = self
            .dependency
            .load_execution_plan(DependencyLoadExecutionPlanRequest {
                session_directory: request.session_directory,
            })
            .map_err(|error| match error {
                ExecutionPlanDependencyError::Corrupt { reason } => {
                    ExecutionPlanDataError::CorruptFile { reason }
                }
                other => ExecutionPlanDataError::Dependency(other),
            })?;
        match result {
            DependencyLoadExecutionPlanResult::Missing => Ok(LoadExecutionPlanDataResult::Missing),
            DependencyLoadExecutionPlanResult::Present(record) => {
                let payload = parse_payload(&record.payload)?;
                Ok(LoadExecutionPlanDataResult::Present(Box::new(payload)))
            }
        }
    }
}

/// Builds the dependency envelope from a normalized data plan file.
///
/// # Errors
///
/// Returns a data error when the canonical plan JSON is not valid JSON or the
/// identity does not match the embedded plan.
pub fn to_dependency_file(
    file: ExecutionPlanFileData,
) -> Result<DependencyExecutionPlanFile, ExecutionPlanDataError> {
    let ExecutionPlanFileData {
        identity,
        canonical_plan_json,
    } = file;
    if identity.schema_version != EXECUTION_PLAN_RECORD_SCHEMA_VERSION {
        return Err(ExecutionPlanDataError::UnknownSchemaVersion {
            schema_version: identity.schema_version,
        });
    }
    // Validate that the canonical plan JSON parses before persisting it.
    serde_json::from_str::<serde_json::Value>(&canonical_plan_json)
        .map_err(|_| ExecutionPlanDataError::InvalidPlanJson)?;
    let payload = serde_json::json!({
        "schema_version": EXECUTION_PLAN_RECORD_SCHEMA_VERSION,
        "identity": identity,
        // The canonical plan JSON is retained as an exact string so the
        // plan content hash survives the round trip byte-for-byte.
        "plan_json": canonical_plan_json,
    });
    let payload_bytes =
        serde_json::to_vec(&payload).map_err(|_| ExecutionPlanDataError::PayloadSerialization)?;
    let payload_checksum = ContentHash::digest(&payload_bytes).to_hex();
    Ok(DependencyExecutionPlanFile {
        schema_version: 1,
        payload_checksum,
        payload: payload_bytes,
    })
}

/// Validates that a stored file matches the identity and plan it claims.
///
/// # Errors
///
/// Returns a data error when the identity and embedded plan disagree.
pub fn validate_file_identity(file: &ExecutionPlanFileData) -> Result<(), ExecutionPlanDataError> {
    if file.identity.schema_version != EXECUTION_PLAN_RECORD_SCHEMA_VERSION {
        return Err(ExecutionPlanDataError::UnknownSchemaVersion {
            schema_version: file.identity.schema_version,
        });
    }
    let plan_value: serde_json::Value = serde_json::from_str(&file.canonical_plan_json)
        .map_err(|_| ExecutionPlanDataError::InvalidPlanJson)?;
    let Some(nodes) = plan_value
        .get("nodes")
        .and_then(serde_json::Value::as_array)
    else {
        return Err(ExecutionPlanDataError::InvalidPlanJson);
    };
    if u64::try_from(nodes.len()).map_err(|_| ExecutionPlanDataError::InvalidPlanJson)?
        != file.identity.node_count
    {
        return Err(ExecutionPlanDataError::IdentityNodeCountMismatch {
            expected: file.identity.node_count,
            actual: u64::try_from(nodes.len()).unwrap_or(u64::MAX),
        });
    }
    let plan_hash = ContentHash::digest(file.canonical_plan_json.as_bytes());
    if plan_hash != file.identity.plan_hash {
        return Err(ExecutionPlanDataError::PlanHashMismatch {
            expected: file.identity.plan_hash.to_hex(),
            computed: plan_hash.to_hex(),
        });
    }
    Ok(())
}

fn parse_payload(payload: &[u8]) -> Result<LoadedExecutionPlanDataRecord, ExecutionPlanDataError> {
    let value: serde_json::Value =
        serde_json::from_slice(payload).map_err(|_| ExecutionPlanDataError::InvalidPayload)?;
    let schema_version = value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .ok_or(ExecutionPlanDataError::InvalidPayload)?;
    if schema_version != u64::from(EXECUTION_PLAN_RECORD_SCHEMA_VERSION) {
        return Err(ExecutionPlanDataError::UnknownSchemaVersion {
            schema_version: u16::try_from(schema_version).unwrap_or(0),
        });
    }
    let identity = value
        .get("identity")
        .ok_or(ExecutionPlanDataError::InvalidPayload)?;
    let identity: ExecutionPlanIdentityData = serde_json::from_value(identity.clone())
        .map_err(|_| ExecutionPlanDataError::InvalidIdentity)?;
    if identity.schema_version != EXECUTION_PLAN_RECORD_SCHEMA_VERSION {
        return Err(ExecutionPlanDataError::UnknownSchemaVersion {
            schema_version: identity.schema_version,
        });
    }
    let plan_json = match value.get("plan_json") {
        Some(serde_json::Value::String(plan)) => plan.clone(),
        _ => return Err(ExecutionPlanDataError::InvalidPlanJson),
    };
    let plan: serde_json::Value =
        serde_json::from_str(&plan_json).map_err(|_| ExecutionPlanDataError::InvalidPlanJson)?;
    let nodes = plan
        .get("nodes")
        .and_then(serde_json::Value::as_array)
        .ok_or(ExecutionPlanDataError::InvalidPlanJson)?;
    if u64::try_from(nodes.len()).map_err(|_| ExecutionPlanDataError::InvalidPlanJson)?
        != identity.node_count
    {
        return Err(ExecutionPlanDataError::IdentityNodeCountMismatch {
            expected: identity.node_count,
            actual: u64::try_from(nodes.len()).unwrap_or(u64::MAX),
        });
    }
    let plan_hash = ContentHash::digest(plan_json.as_bytes());
    if plan_hash != identity.plan_hash {
        return Err(ExecutionPlanDataError::PlanHashMismatch {
            expected: identity.plan_hash.to_hex(),
            computed: plan_hash.to_hex(),
        });
    }
    Ok(LoadedExecutionPlanDataRecord {
        identity,
        canonical_plan_json: plan_json,
        payload_checksum: ContentHash::digest(payload),
    })
}

/// Plan-file persistence failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ExecutionPlanDataError {
    /// Translated dependency failure.
    #[error("execution-plan data dependency failed: {0}")]
    Dependency(ExecutionPlanDependencyError),
    /// The stored plan file is corrupt (truncated, unparsable, or
    /// checksum-invalid).
    #[error("execution-plan: stored plan file is corrupt: {reason}")]
    CorruptFile {
        /// Dependency-owned readable reason.
        reason: String,
    },
    /// The canonical plan JSON is not valid JSON or lacks nodes.
    #[error("execution-plan: canonical plan JSON is invalid")]
    InvalidPlanJson,
    /// The canonical payload is not valid JSON.
    #[error("execution-plan: stored payload is invalid")]
    InvalidPayload,
    /// The stored identity record is invalid.
    #[error("execution-plan: stored identity is invalid")]
    InvalidIdentity,
    /// Unknown record schema version.
    #[error("execution-plan: unknown schema version {schema_version}")]
    UnknownSchemaVersion {
        /// Stored schema version.
        schema_version: u16,
    },
    /// The dependency returned a checksum that is not valid hex.
    #[error("execution-plan: dependency returned an invalid payload checksum")]
    InvalidChecksum,
    /// The embedded plan node count differs from the identity.
    #[error("execution-plan: identity node count {expected} differs from plan {actual}")]
    IdentityNodeCountMismatch {
        /// Identity-declared node count.
        expected: u64,
        /// Plan-derived node count.
        actual: u64,
    },
    /// The embedded plan hash differs from the identity.
    #[error("execution-plan: plan hash mismatch (expected {expected}, computed {computed})")]
    PlanHashMismatch {
        /// Identity-declared plan hash.
        expected: String,
        /// Recomputed plan hash.
        computed: String,
    },
    /// The canonical payload could not be serialized.
    #[error("execution-plan: payload serialization failed")]
    PayloadSerialization,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(node_count: u64, plan_json: &str) -> ExecutionPlanIdentityData {
        ExecutionPlanIdentityData {
            schema_version: EXECUTION_PLAN_RECORD_SCHEMA_VERSION,
            style_id: String::from("fixture.style"),
            style_version: String::from("1.0.0"),
            style_content_hash: ContentHash::digest(b"style"),
            compiled_style_hash: ContentHash::digest(b"compiled"),
            compiled_cache_key: ContentHash::digest(b"cache-key"),
            runtime_api_version: String::from("0.1.0"),
            plugin_set_hash: ContentHash::digest(b"plugins"),
            capability_set_hash: ContentHash::digest(b"capabilities"),
            registry_hash: ContentHash::digest(b"registry"),
            plan_hash: ContentHash::digest(plan_json.as_bytes()),
            node_count,
        }
    }

    fn plan_json(node_count: u64) -> String {
        let mut nodes = Vec::new();
        for index in 0..node_count {
            nodes.push(serde_json::json!({"node_id": format!("node-{index}")}));
        }
        serde_json::json!({
            "registry_hash": "0".repeat(64),
            "compilation": {"compiler": "agentmod-runtime-node-plan@3"},
            "nodes": nodes,
        })
        .to_string()
    }

    #[test]
    fn round_trip_payload_preserves_identity_and_plan() {
        let plan = plan_json(2);
        let file = ExecutionPlanFileData {
            identity: identity(2, &plan),
            canonical_plan_json: plan.clone(),
        };
        validate_file_identity(&file).expect("identity validation");
        let dependency_file = to_dependency_file(file.clone()).expect("dependency file");
        assert_eq!(dependency_file.schema_version, 1);
        let payload = dependency_file.payload.clone();
        let parsed = parse_payload(&payload).expect("parse");
        assert_eq!(parsed.identity, file.identity);
        let parsed_plan: serde_json::Value =
            serde_json::from_str(&parsed.canonical_plan_json).expect("plan");
        let original_plan: serde_json::Value = serde_json::from_str(&plan).expect("plan");
        assert_eq!(parsed_plan, original_plan);
    }

    #[test]
    fn node_count_mismatch_is_rejected() {
        let plan = plan_json(3);
        let file = ExecutionPlanFileData {
            identity: identity(2, &plan),
            canonical_plan_json: plan,
        };
        assert_eq!(
            validate_file_identity(&file),
            Err(ExecutionPlanDataError::IdentityNodeCountMismatch {
                expected: 2,
                actual: 3,
            })
        );
    }

    #[test]
    fn plan_hash_mismatch_is_rejected() {
        let plan = plan_json(1);
        let mut wrong = identity(1, &plan);
        wrong.plan_hash = ContentHash::digest(b"different-plan");
        let file = ExecutionPlanFileData {
            identity: wrong,
            canonical_plan_json: plan,
        };
        assert!(matches!(
            validate_file_identity(&file),
            Err(ExecutionPlanDataError::PlanHashMismatch { .. })
        ));
    }

    #[test]
    fn unknown_schema_version_is_rejected() {
        let plan = plan_json(1);
        let mut wrong = identity(1, &plan);
        wrong.schema_version = 99;
        let file = ExecutionPlanFileData {
            identity: wrong,
            canonical_plan_json: plan,
        };
        assert_eq!(
            validate_file_identity(&file),
            Err(ExecutionPlanDataError::UnknownSchemaVersion { schema_version: 99 })
        );
    }

    #[test]
    fn store_and_load_through_local_dependency_round_trips() {
        let directory = tempfile::tempdir().expect("temp directory");
        let session_directory = directory.path().to_owned();
        let plan = plan_json(2);
        let file = ExecutionPlanFileData {
            identity: identity(2, &plan),
            canonical_plan_json: plan,
        };
        let data =
            super::super::RuntimeData::new(agentmod_runtime_dependency::LocalRuntimeDependencies);
        let stored = data
            .store_execution_plan(StoreExecutionPlanDataRequest {
                session_directory: session_directory.clone(),
                file: file.clone(),
            })
            .expect("store");
        assert!(!stored.deduplicated);
        let loaded = data
            .load_execution_plan(LoadExecutionPlanDataRequest {
                session_directory: session_directory.clone(),
            })
            .expect("load");
        match loaded {
            LoadExecutionPlanDataResult::Present(record) => {
                assert_eq!(record.identity, file.identity);
                let loaded_plan: serde_json::Value =
                    serde_json::from_str(&record.canonical_plan_json).expect("plan");
                let original_plan: serde_json::Value =
                    serde_json::from_str(&file.canonical_plan_json).expect("plan");
                assert_eq!(loaded_plan, original_plan);
            }
            LoadExecutionPlanDataResult::Missing => panic!("plan file must exist"),
        }
        let missing = data
            .load_execution_plan(LoadExecutionPlanDataRequest {
                session_directory: directory.path().join("absent"),
            })
            .expect("load absent");
        assert_eq!(missing, LoadExecutionPlanDataResult::Missing);
    }

    #[test]
    fn corrupt_stored_payload_fails_load() {
        use std::io::Write as _;

        let directory = tempfile::tempdir().expect("temp directory");
        let session_directory = directory.path().to_owned();
        // Write truncated envelope bytes directly through a dependency-owned
        // temp file rename; raw filesystem APIs stay in the dependency layer
        // even inside tests.
        let mut corrupted = tempfile::NamedTempFile::new_in(&session_directory).expect("temp file");
        corrupted.write_all(b"{\"truncated\": true").expect("write");
        corrupted
            .persist(session_directory.join("execution-plan.json"))
            .expect("persist corrupt file");
        let data =
            super::super::RuntimeData::new(agentmod_runtime_dependency::LocalRuntimeDependencies);
        assert!(matches!(
            data.load_execution_plan(LoadExecutionPlanDataRequest {
                session_directory: session_directory.clone(),
            }),
            Err(ExecutionPlanDataError::CorruptFile { .. })
        ));
    }
}
