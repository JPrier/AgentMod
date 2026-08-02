//! Dependency-owned durable receipts for completed provider invocations.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};

use agentmod_primitives::ContentHash;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

const RECEIPT_VERSION: u32 = 1;
const MAX_RECEIPT_BYTES: usize = 2 * 1024 * 1024;

/// Dependency-owned exact provider-completion receipt identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyProviderCompletionReceiptIdentity {
    /// Canonical session UUID.
    pub session_id: String,
    /// Digest-backed provider invocation identity.
    pub invocation_id: String,
}

/// Dependency-owned provider-completion receipt storage request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStoreProviderCompletionReceiptRequest {
    /// Exact scoped identity.
    pub identity: DependencyProviderCompletionReceiptIdentity,
    /// Complete logic-owned serialized receipt.
    pub receipt_bytes: Vec<u8>,
}

/// Dependency-owned verified provider-completion receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyProviderCompletionReceiptRecord {
    /// Exact scoped identity.
    pub identity: DependencyProviderCompletionReceiptIdentity,
    /// Complete verified serialized receipt.
    pub receipt_bytes: Vec<u8>,
}

/// Narrow dependency port for durable provider-completion receipts.
pub trait ProviderCompletionReceiptDependencyPort: Send + Sync {
    /// Loads and checksum-verifies one exact receipt.
    ///
    /// # Errors
    ///
    /// Returns a classified dependency error for unsafe identity, corruption,
    /// or filesystem failure.
    fn load_provider_completion_receipt(
        &self,
        identity: &DependencyProviderCompletionReceiptIdentity,
    ) -> Result<
        Option<DependencyProviderCompletionReceiptRecord>,
        ProviderCompletionReceiptDependencyError,
    >;

    /// Atomically stores one exact receipt, accepting exact duplicates and
    /// rejecting substitutions.
    ///
    /// The implementation returns only after the file and containing
    /// directory have been durably synchronized. A configured post-persist
    /// delay creates a deterministic process-crash observation window.
    ///
    /// # Errors
    ///
    /// Returns a classified dependency error for unsafe identity, oversized
    /// content, conflicting content, or filesystem failure.
    fn store_provider_completion_receipt(
        &self,
        request: DependencyStoreProviderCompletionReceiptRequest,
    ) -> Result<DependencyProviderCompletionReceiptRecord, ProviderCompletionReceiptDependencyError>;
}

/// Filesystem provider-completion receipt store rooted beneath sessions.
#[derive(Clone, Debug)]
pub struct FileProviderCompletionReceiptDependency {
    sessions_root: PathBuf,
    post_persist_delay: Duration,
}

impl FileProviderCompletionReceiptDependency {
    /// Creates a filesystem store beneath the exact sessions root.
    ///
    /// # Errors
    ///
    /// Rejects an empty sessions root.
    pub fn new(sessions_root: PathBuf) -> Result<Self, ProviderCompletionReceiptDependencyError> {
        if sessions_root.as_os_str().is_empty() {
            return Err(ProviderCompletionReceiptDependencyError::InvalidRequest);
        }
        Ok(Self {
            sessions_root,
            post_persist_delay: Duration::ZERO,
        })
    }

    /// Configures a bounded crash-injection observation window after durable
    /// persistence and before control returns to runtime data.
    ///
    /// # Errors
    ///
    /// Rejects delays longer than ten seconds.
    pub fn with_post_persist_delay(
        mut self,
        delay: Duration,
    ) -> Result<Self, ProviderCompletionReceiptDependencyError> {
        if delay > Duration::from_secs(10) {
            return Err(ProviderCompletionReceiptDependencyError::InvalidRequest);
        }
        self.post_persist_delay = delay;
        Ok(self)
    }

    /// Returns the configured post-persist observation window.
    #[must_use]
    pub const fn post_persist_delay(&self) -> Duration {
        self.post_persist_delay
    }

    fn receipt_path(
        &self,
        identity: &DependencyProviderCompletionReceiptIdentity,
    ) -> Result<PathBuf, ProviderCompletionReceiptDependencyError> {
        validate_identity(identity)?;
        let session = Uuid::parse_str(&identity.session_id)
            .map_err(|_| ProviderCompletionReceiptDependencyError::InvalidRequest)?;
        let path_identity =
            serde_json::to_vec(&(RECEIPT_VERSION, session, &identity.invocation_id))
                .map_err(|_| ProviderCompletionReceiptDependencyError::Corrupt)?;
        Ok(self
            .sessions_root
            .join(session.to_string())
            .join("artifacts")
            .join("provider-completion-receipts")
            .join(format!(
                "{}.json",
                ContentHash::digest(&path_identity).to_hex()
            )))
    }
}

impl ProviderCompletionReceiptDependencyPort for FileProviderCompletionReceiptDependency {
    fn load_provider_completion_receipt(
        &self,
        identity: &DependencyProviderCompletionReceiptIdentity,
    ) -> Result<
        Option<DependencyProviderCompletionReceiptRecord>,
        ProviderCompletionReceiptDependencyError,
    > {
        let path = self.receipt_path(identity)?;
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(ProviderCompletionReceiptDependencyError::Storage),
        };
        if bytes.len() > MAX_RECEIPT_BYTES {
            return Err(ProviderCompletionReceiptDependencyError::TooLarge);
        }
        let stored: StoredProviderCompletionReceipt = serde_json::from_slice(&bytes)
            .map_err(|_| ProviderCompletionReceiptDependencyError::Corrupt)?;
        stored.verify(identity)?;
        Ok(Some(DependencyProviderCompletionReceiptRecord {
            identity: identity.clone(),
            receipt_bytes: stored.payload.receipt_json.into_bytes(),
        }))
    }

    fn store_provider_completion_receipt(
        &self,
        request: DependencyStoreProviderCompletionReceiptRequest,
    ) -> Result<DependencyProviderCompletionReceiptRecord, ProviderCompletionReceiptDependencyError>
    {
        validate_identity(&request.identity)?;
        if request.receipt_bytes.is_empty() || request.receipt_bytes.len() > MAX_RECEIPT_BYTES {
            return Err(ProviderCompletionReceiptDependencyError::TooLarge);
        }
        if let Some(existing) = self.load_provider_completion_receipt(&request.identity)? {
            return if existing.receipt_bytes == request.receipt_bytes {
                Ok(existing)
            } else {
                Err(ProviderCompletionReceiptDependencyError::Conflict)
            };
        }
        let path = self.receipt_path(&request.identity)?;
        let receipt_json = String::from_utf8(request.receipt_bytes)
            .map_err(|_| ProviderCompletionReceiptDependencyError::Corrupt)?;
        let payload = StoredProviderCompletionReceiptPayload {
            version: RECEIPT_VERSION,
            session_id: request.identity.session_id.clone(),
            invocation_id: request.identity.invocation_id.clone(),
            receipt_json,
        };
        let stored = StoredProviderCompletionReceipt {
            checksum: payload.checksum()?,
            payload,
        };
        let bytes = serde_json::to_vec(&stored)
            .map_err(|_| ProviderCompletionReceiptDependencyError::Corrupt)?;
        if bytes.len() > MAX_RECEIPT_BYTES {
            return Err(ProviderCompletionReceiptDependencyError::TooLarge);
        }
        let parent = path
            .parent()
            .ok_or(ProviderCompletionReceiptDependencyError::Storage)?;
        fs::create_dir_all(parent)
            .map_err(|_| ProviderCompletionReceiptDependencyError::Storage)?;
        let temporary = parent.join(format!(".{}.tmp", Uuid::now_v7()));
        let result = (|| {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .map_err(|_| ProviderCompletionReceiptDependencyError::Storage)?;
            file.write_all(&bytes)
                .map_err(|_| ProviderCompletionReceiptDependencyError::Storage)?;
            file.sync_all()
                .map_err(|_| ProviderCompletionReceiptDependencyError::Storage)?;
            match fs::hard_link(&temporary, &path) {
                Ok(()) => {
                    fs::remove_file(&temporary)
                        .map_err(|_| ProviderCompletionReceiptDependencyError::Storage)?;
                    sync_parent(parent)
                }
                Err(_) => match self.load_provider_completion_receipt(&request.identity)? {
                    Some(existing)
                        if existing.receipt_bytes == stored.payload.receipt_json.as_bytes() =>
                    {
                        Ok(())
                    }
                    Some(_) => Err(ProviderCompletionReceiptDependencyError::Conflict),
                    None => Err(ProviderCompletionReceiptDependencyError::Storage),
                },
            }
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result?;
        if !self.post_persist_delay.is_zero() {
            std::thread::sleep(self.post_persist_delay);
        }
        self.load_provider_completion_receipt(&request.identity)?
            .ok_or(ProviderCompletionReceiptDependencyError::Storage)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StoredProviderCompletionReceipt {
    payload: StoredProviderCompletionReceiptPayload,
    checksum: ContentHash,
}

impl StoredProviderCompletionReceipt {
    fn verify(
        &self,
        identity: &DependencyProviderCompletionReceiptIdentity,
    ) -> Result<(), ProviderCompletionReceiptDependencyError> {
        if self.payload.version != RECEIPT_VERSION
            || self.payload.session_id != identity.session_id
            || self.payload.invocation_id != identity.invocation_id
            || self.payload.receipt_json.is_empty()
            || self.payload.receipt_json.len() > MAX_RECEIPT_BYTES
            || self.checksum != self.payload.checksum()?
        {
            return Err(ProviderCompletionReceiptDependencyError::Corrupt);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StoredProviderCompletionReceiptPayload {
    version: u32,
    session_id: String,
    invocation_id: String,
    receipt_json: String,
}

impl StoredProviderCompletionReceiptPayload {
    fn checksum(&self) -> Result<ContentHash, ProviderCompletionReceiptDependencyError> {
        serde_json::to_vec(self)
            .map(|bytes| ContentHash::digest(&bytes))
            .map_err(|_| ProviderCompletionReceiptDependencyError::Corrupt)
    }
}

fn validate_identity(
    identity: &DependencyProviderCompletionReceiptIdentity,
) -> Result<(), ProviderCompletionReceiptDependencyError> {
    let digest = identity
        .invocation_id
        .strip_prefix("provider-completion:")
        .ok_or(ProviderCompletionReceiptDependencyError::InvalidRequest)?;
    if Uuid::parse_str(&identity.session_id).is_err()
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ProviderCompletionReceiptDependencyError::InvalidRequest);
    }
    Ok(())
}

#[cfg(not(windows))]
fn sync_parent(parent: &Path) -> Result<(), ProviderCompletionReceiptDependencyError> {
    OpenOptions::new()
        .read(true)
        .open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| ProviderCompletionReceiptDependencyError::Storage)
}

#[cfg(windows)]
#[allow(
    clippy::unnecessary_wraps,
    reason = "keeps one cross-platform durability helper signature"
)]
fn sync_parent(_parent: &Path) -> Result<(), ProviderCompletionReceiptDependencyError> {
    Ok(())
}

/// Stable provider-completion receipt dependency failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ProviderCompletionReceiptDependencyError {
    /// Identity or configuration was unsafe.
    #[error("provider-completion receipt request is invalid")]
    InvalidRequest,
    /// Receipt exceeds the bounded representation.
    #[error("provider-completion receipt exceeds its byte bound")]
    TooLarge,
    /// Filesystem operation failed.
    #[error("provider-completion receipt storage is unavailable")]
    Storage,
    /// Stored receipt failed checksum or representation validation.
    #[error("provider-completion receipt is corrupt")]
    Corrupt,
    /// Existing receipt differs for the same exact identity.
    #[error("provider-completion receipt conflicts with existing content")]
    Conflict,
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    fn identity() -> DependencyProviderCompletionReceiptIdentity {
        DependencyProviderCompletionReceiptIdentity {
            session_id: Uuid::from_u128(1).to_string(),
            invocation_id: format!(
                "provider-completion:{}",
                ContentHash::digest(b"invocation").to_hex()
            ),
        }
    }

    #[test]
    fn filesystem_receipt_is_idempotent_and_rejects_substitution_and_corruption() {
        let root = tempfile::tempdir().expect("root");
        let store =
            FileProviderCompletionReceiptDependency::new(root.path().to_owned()).expect("store");
        let request = DependencyStoreProviderCompletionReceiptRequest {
            identity: identity(),
            receipt_bytes: br#"{"schema_version":1,"outcome":"completed"}"#.to_vec(),
        };
        let first = store
            .store_provider_completion_receipt(request.clone())
            .expect("first");
        assert_eq!(
            store
                .store_provider_completion_receipt(request.clone())
                .expect("duplicate"),
            first
        );
        let reopened =
            FileProviderCompletionReceiptDependency::new(root.path().to_owned()).expect("reopen");
        assert_eq!(
            reopened
                .load_provider_completion_receipt(&request.identity)
                .expect("load"),
            Some(first)
        );
        let mut substituted = request.clone();
        substituted.receipt_bytes = br#"{"schema_version":1,"outcome":"different"}"#.to_vec();
        assert_eq!(
            reopened.store_provider_completion_receipt(substituted),
            Err(ProviderCompletionReceiptDependencyError::Conflict)
        );

        let path = reopened.receipt_path(&request.identity).expect("path");
        let mut bytes = fs::read(&path).expect("bytes");
        let index = bytes.len() / 2;
        bytes[index] ^= 1;
        fs::write(path, bytes).expect("corrupt");
        assert_eq!(
            reopened.load_provider_completion_receipt(&request.identity),
            Err(ProviderCompletionReceiptDependencyError::Corrupt)
        );
    }

    #[test]
    fn identity_size_and_delay_are_bounded() {
        let root = tempfile::tempdir().expect("root");
        let store = FileProviderCompletionReceiptDependency::new(root.path().to_owned())
            .expect("store")
            .with_post_persist_delay(Duration::from_millis(15))
            .expect("delay");
        let started = Instant::now();
        store
            .store_provider_completion_receipt(DependencyStoreProviderCompletionReceiptRequest {
                identity: identity(),
                receipt_bytes: b"{}".to_vec(),
            })
            .expect("store");
        assert!(started.elapsed() >= Duration::from_millis(15));

        let mut invalid = identity();
        invalid.invocation_id = String::from("provider-completion:not-a-digest");
        assert_eq!(
            store.load_provider_completion_receipt(&invalid),
            Err(ProviderCompletionReceiptDependencyError::InvalidRequest)
        );
        assert_eq!(
            store.store_provider_completion_receipt(
                DependencyStoreProviderCompletionReceiptRequest {
                    identity: DependencyProviderCompletionReceiptIdentity {
                        session_id: Uuid::from_u128(2).to_string(),
                        invocation_id: format!(
                            "provider-completion:{}",
                            ContentHash::digest(b"large").to_hex()
                        ),
                    },
                    receipt_bytes: vec![b'x'; MAX_RECEIPT_BYTES + 1],
                },
            ),
            Err(ProviderCompletionReceiptDependencyError::TooLarge)
        );
        assert!(matches!(
            FileProviderCompletionReceiptDependency::new(root.path().to_owned())
                .expect("store")
                .with_post_persist_delay(Duration::from_secs(11)),
            Err(ProviderCompletionReceiptDependencyError::InvalidRequest)
        ));
    }
}
