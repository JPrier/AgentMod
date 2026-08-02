//! Checksummed, atomic, immutable execution-plan file persistence.
//!
//! The dependency owns the external storage representation of the per-node
//! execution plan: one bounded `execution-plan.json` envelope per session,
//! written atomically beside the session metadata, style files, and initial
//! journal event. The payload itself is opaque canonical JSON produced by the
//! runtime data boundary; this layer validates the envelope schema and the
//! exact BLAKE3 payload checksum and never parses business plan contents.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use thiserror::Error;

const EXECUTION_PLAN_FILE: &str = "execution-plan.json";
/// Stable binary schema of the dependency envelope.
pub const EXECUTION_PLAN_ENVELOPE_SCHEMA_VERSION: u16 = 1;
/// Hard bound for the complete plan payload (identity + canonical plan JSON).
const EXECUTION_PLAN_PAYLOAD_LIMIT_BYTES: u64 = 4 * 1024 * 1024;
/// Hard bound for the on-disk envelope (checksum + base64 overhead).
const EXECUTION_PLAN_FILE_LIMIT_BYTES: u64 = 8 * 1024 * 1024;
/// Stable envelope magic marker for corruption classification.
const EXECUTION_PLAN_ENVELOPE_MAGIC: &str = "agentmod.execution-plan@1";

/// Dependency-owned immutable execution-plan file.
///
/// `payload` is the canonical opaque JSON produced by runtime data; the
/// dependency treats it as bytes and verifies only its checksum.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyExecutionPlanFile {
    /// Stable envelope schema version.
    pub schema_version: u16,
    /// BLAKE3 hex checksum of the exact canonical payload bytes.
    pub payload_checksum: String,
    /// Canonical opaque payload bytes.
    pub payload: Vec<u8>,
}

/// Successful immutable plan-file persist result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStoreExecutionPlanResponse {
    /// Whether an identical immutable plan file already existed.
    pub deduplicated: bool,
    /// Exact BLAKE3 checksum of the retained payload.
    pub payload_checksum: String,
    /// Canonical payload byte count.
    pub payload_bytes: u64,
}

/// Dependency request to store one immutable plan file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStoreExecutionPlanRequest {
    /// Session directory containing the plan file.
    pub session_directory: PathBuf,
    /// Complete immutable plan file.
    pub plan: DependencyExecutionPlanFile,
}

/// Dependency request to load the immutable plan file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyLoadExecutionPlanRequest {
    /// Session directory containing the plan file.
    pub session_directory: PathBuf,
}

/// Loaded immutable plan file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyExecutionPlanRecord {
    /// Validated envelope schema version.
    pub schema_version: u16,
    /// Verified BLAKE3 payload checksum.
    pub payload_checksum: String,
    /// Exact canonical payload bytes.
    pub payload: Vec<u8>,
    /// Verified on-disk envelope byte count.
    pub file_bytes: u64,
}

/// Result of loading a plan file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DependencyLoadExecutionPlanResult {
    /// The session retains a valid immutable plan file.
    Present(DependencyExecutionPlanRecord),
    /// No plan file exists (legacy session or pre-feature creation).
    Missing,
}

/// Plan-file persistence abstraction consumed only by runtime data.
pub trait ExecutionPlanDependencyPort {
    /// Atomically persists an immutable plan file.
    ///
    /// # Errors
    ///
    /// Returns a dependency error for invalid schema, checksum mismatch,
    /// size overflow, or storage failure.
    fn store_execution_plan(
        &self,
        request: DependencyStoreExecutionPlanRequest,
    ) -> Result<DependencyStoreExecutionPlanResponse, ExecutionPlanDependencyError>;

    /// Loads and checksum-validates the immutable plan file.
    ///
    /// # Errors
    ///
    /// Returns a dependency error when the file exists but is truncated,
    /// unparsable, schema-unknown, or checksum-invalid. An absent file is a
    /// valid [`DependencyLoadExecutionPlanResult::Missing`], not an error.
    fn load_execution_plan(
        &self,
        request: DependencyLoadExecutionPlanRequest,
    ) -> Result<DependencyLoadExecutionPlanResult, ExecutionPlanDependencyError>;
}

/// Local filesystem plan-file implementation.
#[derive(Clone, Copy, Debug, Default)]
pub struct LocalExecutionPlanDependency;

impl ExecutionPlanDependencyPort for LocalExecutionPlanDependency {
    fn store_execution_plan(
        &self,
        request: DependencyStoreExecutionPlanRequest,
    ) -> Result<DependencyStoreExecutionPlanResponse, ExecutionPlanDependencyError> {
        validate_plan_file(&request.plan)?;
        if !request.session_directory.is_dir() {
            return Err(ExecutionPlanDependencyError::MissingSessionDirectory);
        }
        let path = request.session_directory.join(EXECUTION_PLAN_FILE);
        let envelope = envelope_json(&request.plan);
        let bytes = serde_json::to_vec(&envelope)
            .map_err(|_| ExecutionPlanDependencyError::EnvelopeSerialization)?;
        if u64::try_from(bytes.len()).map_err(|_| ExecutionPlanDependencyError::FileTooLarge)?
            > EXECUTION_PLAN_FILE_LIMIT_BYTES
        {
            return Err(ExecutionPlanDependencyError::FileTooLarge);
        }
        match read_envelope(&path)? {
            DependencyReadEnvelope::Missing => {}
            DependencyReadEnvelope::Present(existing) => {
                if existing.payload_checksum == request.plan.payload_checksum
                    && existing.payload == request.plan.payload
                {
                    return Ok(DependencyStoreExecutionPlanResponse {
                        deduplicated: true,
                        payload_checksum: request.plan.payload_checksum.clone(),
                        payload_bytes: u64::try_from(request.plan.payload.len())
                            .map_err(|_| ExecutionPlanDependencyError::FileTooLarge)?,
                    });
                }
                return Err(ExecutionPlanDependencyError::ExistingPlanMismatch);
            }
            DependencyReadEnvelope::Corrupt { reason } => {
                return Err(ExecutionPlanDependencyError::ExistingCorrupt { reason });
            }
        }
        atomic_write(&path, &bytes)?;
        Ok(DependencyStoreExecutionPlanResponse {
            deduplicated: false,
            payload_checksum: request.plan.payload_checksum,
            payload_bytes: u64::try_from(request.plan.payload.len())
                .map_err(|_| ExecutionPlanDependencyError::FileTooLarge)?,
        })
    }

    fn load_execution_plan(
        &self,
        request: DependencyLoadExecutionPlanRequest,
    ) -> Result<DependencyLoadExecutionPlanResult, ExecutionPlanDependencyError> {
        if !request.session_directory.is_dir() {
            return Ok(DependencyLoadExecutionPlanResult::Missing);
        }
        let path = request.session_directory.join(EXECUTION_PLAN_FILE);
        match read_envelope(&path)? {
            DependencyReadEnvelope::Missing => Ok(DependencyLoadExecutionPlanResult::Missing),
            DependencyReadEnvelope::Present(record) => {
                Ok(DependencyLoadExecutionPlanResult::Present(record))
            }
            DependencyReadEnvelope::Corrupt { reason } => {
                Err(ExecutionPlanDependencyError::Corrupt { reason })
            }
        }
    }
}

fn validate_plan_file(
    plan: &DependencyExecutionPlanFile,
) -> Result<(), ExecutionPlanDependencyError> {
    if plan.schema_version != EXECUTION_PLAN_ENVELOPE_SCHEMA_VERSION {
        return Err(ExecutionPlanDependencyError::UnknownSchemaVersion {
            schema_version: plan.schema_version,
        });
    }
    let payload_bytes = u64::try_from(plan.payload.len())
        .map_err(|_| ExecutionPlanDependencyError::FileTooLarge)?;
    if payload_bytes > EXECUTION_PLAN_PAYLOAD_LIMIT_BYTES {
        return Err(ExecutionPlanDependencyError::FileTooLarge);
    }
    let computed = blake3::hash(&plan.payload).to_hex().to_string();
    if computed != plan.payload_checksum {
        return Err(ExecutionPlanDependencyError::ChecksumMismatch {
            expected: plan.payload_checksum.clone(),
            computed,
        });
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DependencyReadEnvelope {
    Missing,
    Present(DependencyExecutionPlanRecord),
    Corrupt { reason: String },
}

#[allow(
    clippy::too_many_lines,
    reason = "envelope parsing keeps every corruption classification in one fail-closed audit"
)]
fn read_envelope(path: &Path) -> Result<DependencyReadEnvelope, ExecutionPlanDependencyError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(DependencyReadEnvelope::Missing);
        }
        Err(error) => return Err(map_io(error)),
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(map_io)?;
    let file_bytes =
        u64::try_from(bytes.len()).map_err(|_| ExecutionPlanDependencyError::FileTooLarge)?;
    if file_bytes > EXECUTION_PLAN_FILE_LIMIT_BYTES {
        return Ok(DependencyReadEnvelope::Corrupt {
            reason: format!("plan file exceeds the {EXECUTION_PLAN_FILE_LIMIT_BYTES} byte limit"),
        });
    }
    let envelope: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(_) => {
            return Ok(DependencyReadEnvelope::Corrupt {
                reason: String::from("plan file is not valid envelope JSON"),
            });
        }
    };
    let magic = envelope.get("magic").and_then(serde_json::Value::as_str);
    if magic != Some(EXECUTION_PLAN_ENVELOPE_MAGIC) {
        return Ok(DependencyReadEnvelope::Corrupt {
            reason: String::from("plan file envelope magic is missing or unknown"),
        });
    }
    let schema_version = match envelope
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
    {
        Some(version) => u16::try_from(version).unwrap_or(0),
        None => {
            return Ok(DependencyReadEnvelope::Corrupt {
                reason: String::from("plan file envelope schema version is missing"),
            });
        }
    };
    if schema_version != EXECUTION_PLAN_ENVELOPE_SCHEMA_VERSION {
        return Ok(DependencyReadEnvelope::Corrupt {
            reason: format!("plan file uses unknown schema version {schema_version}"),
        });
    }
    let payload_checksum = match envelope
        .get("payload_checksum")
        .and_then(serde_json::Value::as_str)
    {
        Some(checksum) => checksum.to_owned(),
        None => {
            return Ok(DependencyReadEnvelope::Corrupt {
                reason: String::from("plan file envelope payload checksum is missing"),
            });
        }
    };
    let Some(payload_base64) = envelope
        .get("payload_base64")
        .and_then(serde_json::Value::as_str)
    else {
        return Ok(DependencyReadEnvelope::Corrupt {
            reason: String::from("plan file envelope payload is missing"),
        });
    };
    let Ok(payload) = BASE64.decode(payload_base64) else {
        return Ok(DependencyReadEnvelope::Corrupt {
            reason: String::from("plan file envelope payload is not valid base64"),
        });
    };
    let payload_bytes =
        u64::try_from(payload.len()).map_err(|_| ExecutionPlanDependencyError::FileTooLarge)?;
    if payload_bytes > EXECUTION_PLAN_PAYLOAD_LIMIT_BYTES {
        return Ok(DependencyReadEnvelope::Corrupt {
            reason: format!(
                "plan file payload exceeds the {EXECUTION_PLAN_PAYLOAD_LIMIT_BYTES} byte limit"
            ),
        });
    }
    let computed = blake3::hash(&payload).to_hex().to_string();
    if computed != payload_checksum {
        return Ok(DependencyReadEnvelope::Corrupt {
            reason: format!(
                "plan file payload checksum mismatch (expected {payload_checksum}, computed {computed})"
            ),
        });
    }
    Ok(DependencyReadEnvelope::Present(
        DependencyExecutionPlanRecord {
            schema_version,
            payload_checksum,
            payload,
            file_bytes,
        },
    ))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct EnvelopeJson {
    magic: String,
    schema_version: u16,
    payload_checksum: String,
    payload_base64: String,
}

fn envelope_json(plan: &DependencyExecutionPlanFile) -> EnvelopeJson {
    EnvelopeJson {
        magic: String::from(EXECUTION_PLAN_ENVELOPE_MAGIC),
        schema_version: plan.schema_version,
        payload_checksum: plan.payload_checksum.clone(),
        payload_base64: BASE64.encode(&plan.payload),
    }
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), ExecutionPlanDependencyError> {
    let parent = path
        .parent()
        .ok_or(ExecutionPlanDependencyError::MissingSessionDirectory)?;
    let temporary = parent.join(format!(
        "{EXECUTION_PLAN_FILE}.tmp.{}",
        uuid::Uuid::now_v7()
    ));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(map_io)?;
    file.write_all(bytes).map_err(map_io)?;
    file.sync_all().map_err(map_io)?;
    set_private_permissions(&temporary)?;
    fs::rename(&temporary, path).map_err(map_io)?;
    sync_directory(parent)?;
    Ok(())
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "unix targets perform a real directory fsync; windows targets are no-ops"
)]
fn sync_directory(directory: &Path) -> Result<(), ExecutionPlanDependencyError> {
    #[cfg(unix)]
    {
        let handle = File::open(directory).map_err(map_io)?;
        handle.sync_all().map_err(map_io)?;
    }
    #[cfg(not(unix))]
    {
        let _ = directory;
    }
    Ok(())
}

#[cfg(unix)]
fn set_private_permissions(path: &Path) -> Result<(), ExecutionPlanDependencyError> {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(map_io)
}

#[cfg(windows)]
#[allow(clippy::unnecessary_wraps)]
fn set_private_permissions(_path: &Path) -> Result<(), ExecutionPlanDependencyError> {
    Ok(())
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the io::Error is consumed into a stable owned diagnostic string"
)]
fn map_io(error: std::io::Error) -> ExecutionPlanDependencyError {
    ExecutionPlanDependencyError::Io(error.to_string())
}

/// Plan-file persistence failure with stable diagnostic codes.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ExecutionPlanDependencyError {
    /// The selected session directory does not exist.
    #[error("execution-plan: session directory is missing")]
    MissingSessionDirectory,
    /// The supplied plan file uses an unknown schema version.
    #[error("execution-plan: unknown schema version {schema_version}")]
    UnknownSchemaVersion {
        /// Supplied schema version.
        schema_version: u16,
    },
    /// The supplied payload checksum does not match its bytes.
    #[error("execution-plan: payload checksum mismatch (expected {expected}, computed {computed})")]
    ChecksumMismatch {
        /// Declared checksum.
        expected: String,
        /// Computed checksum.
        computed: String,
    },
    /// The plan payload exceeds the bounded size.
    #[error("execution-plan: plan payload exceeds the storage limit")]
    FileTooLarge,
    /// The canonical envelope could not be serialized.
    #[error("execution-plan: envelope serialization failed")]
    EnvelopeSerialization,
    /// An existing plan file was rejected because it is corrupt.
    #[error("execution-plan: existing plan file is corrupt: {reason}")]
    ExistingCorrupt {
        /// Dependency-owned readable reason.
        reason: String,
    },
    /// The session already retains a different immutable plan.
    #[error("execution-plan: an existing plan file differs from the new plan")]
    ExistingPlanMismatch,
    /// A stored plan file is corrupt (truncated, unparsable, or checksum-invalid).
    #[error("execution-plan: stored plan file is corrupt: {reason}")]
    Corrupt {
        /// Dependency-owned readable reason.
        reason: String,
    },
    /// Underlying filesystem failure.
    #[error("execution-plan: filesystem failure: {0}")]
    Io(String),
}

use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests {
    use super::*;

    fn session_directory() -> (tempfile::TempDir, PathBuf) {
        let directory = tempfile::tempdir().expect("temp directory");
        let path = directory.path().join("session-1");
        fs::create_dir(&path).expect("session directory");
        (directory, path)
    }

    fn plan_file(payload: &[u8]) -> DependencyExecutionPlanFile {
        DependencyExecutionPlanFile {
            schema_version: EXECUTION_PLAN_ENVELOPE_SCHEMA_VERSION,
            payload_checksum: blake3::hash(payload).to_hex().to_string(),
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn store_then_load_returns_exact_checksummed_payload() {
        let (_root, directory) = session_directory();
        let plan = plan_file(br#"{"schema_version":1,"plan":{"nodes":[]}}"#);
        let dependency = LocalExecutionPlanDependency;
        let stored = dependency
            .store_execution_plan(DependencyStoreExecutionPlanRequest {
                session_directory: directory.clone(),
                plan: plan.clone(),
            })
            .expect("store");
        assert!(!stored.deduplicated);
        assert_eq!(stored.payload_checksum, plan.payload_checksum);
        assert_eq!(
            stored.payload_bytes,
            u64::try_from(plan.payload.len()).unwrap()
        );
        let loaded = dependency
            .load_execution_plan(DependencyLoadExecutionPlanRequest {
                session_directory: directory.clone(),
            })
            .expect("load");
        match loaded {
            DependencyLoadExecutionPlanResult::Present(record) => {
                assert_eq!(record.schema_version, 1);
                assert_eq!(record.payload_checksum, plan.payload_checksum);
                assert_eq!(record.payload, plan.payload);
                assert!(record.file_bytes > 0);
            }
            DependencyLoadExecutionPlanResult::Missing => panic!("plan file must exist"),
        }
    }

    #[test]
    fn identical_store_is_deduplicated() {
        let (_root, directory) = session_directory();
        let plan = plan_file(br#"{"plan":"same"}"#);
        let dependency = LocalExecutionPlanDependency;
        dependency
            .store_execution_plan(DependencyStoreExecutionPlanRequest {
                session_directory: directory.clone(),
                plan: plan.clone(),
            })
            .expect("first store");
        let second = dependency
            .store_execution_plan(DependencyStoreExecutionPlanRequest {
                session_directory: directory.clone(),
                plan,
            })
            .expect("second store");
        assert!(second.deduplicated);
    }

    #[test]
    fn differing_store_is_rejected() {
        let (_root, directory) = session_directory();
        let dependency = LocalExecutionPlanDependency;
        dependency
            .store_execution_plan(DependencyStoreExecutionPlanRequest {
                session_directory: directory.clone(),
                plan: plan_file(b"first"),
            })
            .expect("first store");
        let error = dependency
            .store_execution_plan(DependencyStoreExecutionPlanRequest {
                session_directory: directory.clone(),
                plan: plan_file(b"second"),
            })
            .expect_err("conflicting plan must be rejected");
        assert_eq!(error, ExecutionPlanDependencyError::ExistingPlanMismatch);
    }

    #[test]
    fn missing_file_loads_as_missing() {
        let (_root, directory) = session_directory();
        let result = LocalExecutionPlanDependency
            .load_execution_plan(DependencyLoadExecutionPlanRequest {
                session_directory: directory,
            })
            .expect("load");
        assert_eq!(result, DependencyLoadExecutionPlanResult::Missing);
    }

    #[test]
    fn truncated_file_is_corrupt() {
        let (_root, directory) = session_directory();
        let plan = plan_file(br#"{"plan":[]}"#);
        let dependency = LocalExecutionPlanDependency;
        dependency
            .store_execution_plan(DependencyStoreExecutionPlanRequest {
                session_directory: directory.clone(),
                plan,
            })
            .expect("store");
        let path = directory.join(EXECUTION_PLAN_FILE);
        let bytes = fs::read(&path).expect("read");
        fs::write(&path, &bytes[..bytes.len() / 2]).expect("truncate");
        let error = dependency
            .load_execution_plan(DependencyLoadExecutionPlanRequest {
                session_directory: directory,
            })
            .expect_err("truncated file must fail");
        match error {
            ExecutionPlanDependencyError::Corrupt { reason } => {
                assert!(
                    reason.contains("JSON")
                        || reason.contains("magic")
                        || reason.contains("base64")
                );
            }
            other => panic!("unexpected error {other:?}"),
        }
    }

    #[test]
    fn tampered_payload_is_corrupt() {
        let (_root, directory) = session_directory();
        let dependency = LocalExecutionPlanDependency;
        dependency
            .store_execution_plan(DependencyStoreExecutionPlanRequest {
                session_directory: directory.clone(),
                plan: plan_file(br#"{"plan":[]}"#),
            })
            .expect("store");
        let path = directory.join(EXECUTION_PLAN_FILE);
        let bytes = fs::read(&path).expect("read");
        let mut envelope: serde_json::Value = serde_json::from_slice(&bytes).expect("envelope");
        envelope["payload_base64"] =
            serde_json::Value::String(BASE64.encode(br#"{"tampered":true}"#));
        fs::write(
            &path,
            serde_json::to_vec(&envelope).expect("serialize tampered envelope"),
        )
        .expect("tamper");
        let error = dependency
            .load_execution_plan(DependencyLoadExecutionPlanRequest {
                session_directory: directory,
            })
            .expect_err("tampered payload must fail checksum");
        assert!(
            matches!(
                error,
                ExecutionPlanDependencyError::Corrupt { ref reason } if reason.contains("checksum mismatch")
            ),
            "unexpected error {error:?}"
        );
    }

    #[test]
    fn unknown_schema_version_is_corrupt() {
        let (_root, directory) = session_directory();
        let dependency = LocalExecutionPlanDependency;
        dependency
            .store_execution_plan(DependencyStoreExecutionPlanRequest {
                session_directory: directory.clone(),
                plan: plan_file(b"payload"),
            })
            .expect("store");
        let path = directory.join(EXECUTION_PLAN_FILE);
        let bytes = fs::read(&path).expect("read");
        let mut envelope: serde_json::Value = serde_json::from_slice(&bytes).expect("envelope");
        envelope["schema_version"] = serde_json::Value::from(99_u16);
        envelope["payload_checksum"] =
            serde_json::Value::String(blake3::hash(b"payload").to_hex().to_string());
        fs::write(
            &path,
            serde_json::to_vec(&envelope).expect("serialize envelope"),
        )
        .expect("rewrite");
        let error = dependency
            .load_execution_plan(DependencyLoadExecutionPlanRequest {
                session_directory: directory,
            })
            .expect_err("unknown schema must fail");
        assert!(
            matches!(
                error,
                ExecutionPlanDependencyError::Corrupt { ref reason } if reason.contains("unknown schema version 99")
            ),
            "unexpected error {error:?}"
        );
    }

    #[test]
    fn store_rejects_invalid_checksum_and_oversized_payload() {
        let (_root, directory) = session_directory();
        let invalid = DependencyExecutionPlanFile {
            schema_version: EXECUTION_PLAN_ENVELOPE_SCHEMA_VERSION,
            payload_checksum: blake3::hash(b"expected").to_hex().to_string(),
            payload: b"actual".to_vec(),
        };
        assert_eq!(
            LocalExecutionPlanDependency.store_execution_plan(
                DependencyStoreExecutionPlanRequest {
                    session_directory: directory.clone(),
                    plan: invalid,
                }
            ),
            Err(ExecutionPlanDependencyError::ChecksumMismatch {
                expected: blake3::hash(b"expected").to_hex().to_string(),
                computed: blake3::hash(b"actual").to_hex().to_string(),
            })
        );
        let oversized_bytes =
            usize::try_from(EXECUTION_PLAN_PAYLOAD_LIMIT_BYTES + 1).expect("usize width");
        let oversized = DependencyExecutionPlanFile {
            schema_version: EXECUTION_PLAN_ENVELOPE_SCHEMA_VERSION,
            payload_checksum: blake3::hash(&vec![0_u8; oversized_bytes])
                .to_hex()
                .to_string(),
            payload: vec![0_u8; oversized_bytes],
        };
        assert_eq!(
            LocalExecutionPlanDependency.store_execution_plan(
                DependencyStoreExecutionPlanRequest {
                    session_directory: directory,
                    plan: oversized,
                }
            ),
            Err(ExecutionPlanDependencyError::FileTooLarge)
        );
    }
}
