//! Dependency-owned durable receipts for isolated plugin invocations.
//!
//! The original physical namespace is retained for plugin-node receipts so
//! existing sessions remain recoverable. Other isolated plugin operations use
//! the same atomic, checksum-verified store beneath a separate generic
//! namespace.

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

/// Dependency-owned exact plugin-node receipt identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginNodeReceiptIdentity {
    /// Canonical session UUID.
    pub session_id: String,
    /// Digest-backed plugin invocation identity.
    pub invocation_id: String,
}

/// Dependency-owned receipt storage request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStorePluginNodeReceiptRequest {
    /// Exact scoped identity.
    pub identity: DependencyPluginNodeReceiptIdentity,
    /// Complete logic-owned serialized receipt.
    pub receipt_bytes: Vec<u8>,
}

/// Dependency-owned verified receipt record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginNodeReceiptRecord {
    /// Exact scoped identity.
    pub identity: DependencyPluginNodeReceiptIdentity,
    /// Complete verified serialized receipt.
    pub receipt_bytes: Vec<u8>,
}

/// Narrow dependency port for durable plugin-node terminal receipts.
pub trait PluginNodeReceiptDependencyPort: Send + Sync {
    /// Loads and checksum-verifies one exact receipt.
    ///
    /// # Errors
    ///
    /// Returns a classified dependency error for unsafe identity, corruption,
    /// or filesystem failure.
    fn load_plugin_node_receipt(
        &self,
        identity: &DependencyPluginNodeReceiptIdentity,
    ) -> Result<Option<DependencyPluginNodeReceiptRecord>, PluginNodeReceiptDependencyError>;

    /// Atomically stores one exact receipt, accepting exact duplicates and
    /// rejecting substitutions.
    ///
    /// # Errors
    ///
    /// Returns a classified dependency error for unsafe identity, oversized
    /// content, conflicting content, or filesystem failure.
    fn store_plugin_node_receipt(
        &self,
        request: DependencyStorePluginNodeReceiptRequest,
    ) -> Result<DependencyPluginNodeReceiptRecord, PluginNodeReceiptDependencyError>;
}

/// Filesystem receipt store rooted beneath canonical session directories.
#[derive(Clone, Debug)]
pub struct FilePluginNodeReceiptDependency {
    sessions_root: PathBuf,
    post_persist_delay: Duration,
}

impl FilePluginNodeReceiptDependency {
    /// Creates a filesystem store beneath the exact sessions root.
    ///
    /// # Errors
    ///
    /// Rejects an empty sessions root.
    pub fn new(sessions_root: PathBuf) -> Result<Self, PluginNodeReceiptDependencyError> {
        if sessions_root.as_os_str().is_empty() {
            return Err(PluginNodeReceiptDependencyError::InvalidRequest);
        }
        Ok(Self {
            sessions_root,
            post_persist_delay: Duration::ZERO,
        })
    }

    /// Adds a testable crash cut after durable persistence and before the
    /// terminal receipt is returned to runtime logic.
    #[must_use]
    pub fn with_post_persist_delay(mut self, delay: Duration) -> Self {
        self.post_persist_delay = delay;
        self
    }

    fn receipt_path(
        &self,
        identity: &DependencyPluginNodeReceiptIdentity,
    ) -> Result<PathBuf, PluginNodeReceiptDependencyError> {
        validate_identity(identity)?;
        let session = Uuid::parse_str(&identity.session_id)
            .map_err(|_| PluginNodeReceiptDependencyError::InvalidRequest)?;
        let path_identity =
            serde_json::to_vec(&(RECEIPT_VERSION, session, &identity.invocation_id))
                .map_err(|_| PluginNodeReceiptDependencyError::Corrupt)?;
        let receipt_directory = if identity.invocation_id.starts_with("plugin-node:") {
            "plugin-node-receipts"
        } else {
            "plugin-invocation-receipts"
        };
        Ok(self
            .sessions_root
            .join(session.to_string())
            .join("artifacts")
            .join(receipt_directory)
            .join(format!(
                "{}.json",
                ContentHash::digest(&path_identity).to_hex()
            )))
    }
}

impl PluginNodeReceiptDependencyPort for FilePluginNodeReceiptDependency {
    fn load_plugin_node_receipt(
        &self,
        identity: &DependencyPluginNodeReceiptIdentity,
    ) -> Result<Option<DependencyPluginNodeReceiptRecord>, PluginNodeReceiptDependencyError> {
        let path = self.receipt_path(identity)?;
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(PluginNodeReceiptDependencyError::Storage),
        };
        if bytes.len() > MAX_RECEIPT_BYTES {
            return Err(PluginNodeReceiptDependencyError::TooLarge);
        }
        let stored: StoredPluginNodeReceipt = serde_json::from_slice(&bytes)
            .map_err(|_| PluginNodeReceiptDependencyError::Corrupt)?;
        stored.verify(identity)?;
        Ok(Some(DependencyPluginNodeReceiptRecord {
            identity: identity.clone(),
            receipt_bytes: stored.payload.receipt_json.into_bytes(),
        }))
    }

    fn store_plugin_node_receipt(
        &self,
        request: DependencyStorePluginNodeReceiptRequest,
    ) -> Result<DependencyPluginNodeReceiptRecord, PluginNodeReceiptDependencyError> {
        validate_identity(&request.identity)?;
        if request.receipt_bytes.is_empty() || request.receipt_bytes.len() > MAX_RECEIPT_BYTES {
            return Err(PluginNodeReceiptDependencyError::TooLarge);
        }
        if let Some(existing) = self.load_plugin_node_receipt(&request.identity)? {
            return if existing.receipt_bytes == request.receipt_bytes {
                Ok(existing)
            } else {
                Err(PluginNodeReceiptDependencyError::Conflict)
            };
        }
        let path = self.receipt_path(&request.identity)?;
        let receipt_json = String::from_utf8(request.receipt_bytes)
            .map_err(|_| PluginNodeReceiptDependencyError::Corrupt)?;
        let payload = StoredPluginNodeReceiptPayload {
            version: RECEIPT_VERSION,
            session_id: request.identity.session_id.clone(),
            invocation_id: request.identity.invocation_id.clone(),
            receipt_json,
        };
        let stored = StoredPluginNodeReceipt {
            checksum: payload.checksum()?,
            payload,
        };
        let bytes =
            serde_json::to_vec(&stored).map_err(|_| PluginNodeReceiptDependencyError::Corrupt)?;
        if bytes.len() > MAX_RECEIPT_BYTES {
            return Err(PluginNodeReceiptDependencyError::TooLarge);
        }
        let parent = path
            .parent()
            .ok_or(PluginNodeReceiptDependencyError::Storage)?;
        fs::create_dir_all(parent).map_err(|_| PluginNodeReceiptDependencyError::Storage)?;
        let temporary = parent.join(format!(".{}.tmp", Uuid::now_v7()));
        let result = (|| {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .map_err(|_| PluginNodeReceiptDependencyError::Storage)?;
            file.write_all(&bytes)
                .map_err(|_| PluginNodeReceiptDependencyError::Storage)?;
            file.sync_all()
                .map_err(|_| PluginNodeReceiptDependencyError::Storage)?;
            match fs::hard_link(&temporary, &path) {
                Ok(()) => {
                    fs::remove_file(&temporary)
                        .map_err(|_| PluginNodeReceiptDependencyError::Storage)?;
                    sync_parent(parent)
                }
                Err(_) => match self.load_plugin_node_receipt(&request.identity)? {
                    Some(existing)
                        if existing.receipt_bytes == stored.payload.receipt_json.as_bytes() =>
                    {
                        Ok(())
                    }
                    Some(_) => Err(PluginNodeReceiptDependencyError::Conflict),
                    None => Err(PluginNodeReceiptDependencyError::Storage),
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
        self.load_plugin_node_receipt(&request.identity)?
            .ok_or(PluginNodeReceiptDependencyError::Storage)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StoredPluginNodeReceipt {
    payload: StoredPluginNodeReceiptPayload,
    checksum: ContentHash,
}

impl StoredPluginNodeReceipt {
    fn verify(
        &self,
        identity: &DependencyPluginNodeReceiptIdentity,
    ) -> Result<(), PluginNodeReceiptDependencyError> {
        if self.payload.version != RECEIPT_VERSION
            || self.payload.session_id != identity.session_id
            || self.payload.invocation_id != identity.invocation_id
            || self.payload.receipt_json.is_empty()
            || self.payload.receipt_json.len() > MAX_RECEIPT_BYTES
            || self.checksum != self.payload.checksum()?
        {
            return Err(PluginNodeReceiptDependencyError::Corrupt);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StoredPluginNodeReceiptPayload {
    version: u32,
    session_id: String,
    invocation_id: String,
    receipt_json: String,
}

impl StoredPluginNodeReceiptPayload {
    fn checksum(&self) -> Result<ContentHash, PluginNodeReceiptDependencyError> {
        serde_json::to_vec(self)
            .map(|bytes| ContentHash::digest(&bytes))
            .map_err(|_| PluginNodeReceiptDependencyError::Corrupt)
    }
}

fn validate_identity(
    identity: &DependencyPluginNodeReceiptIdentity,
) -> Result<(), PluginNodeReceiptDependencyError> {
    let digest = identity
        .invocation_id
        .strip_prefix("plugin-node:")
        .or_else(|| {
            identity
                .invocation_id
                .strip_prefix("plugin-automatic-memory-write:")
        })
        .or_else(|| {
            identity
                .invocation_id
                .strip_prefix("plugin-context-operation:")
        })
        .or_else(|| {
            identity
                .invocation_id
                .strip_prefix("plugin-context-transform:")
        })
        .ok_or(PluginNodeReceiptDependencyError::InvalidRequest)?;
    if Uuid::parse_str(&identity.session_id).is_err()
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(PluginNodeReceiptDependencyError::InvalidRequest);
    }
    Ok(())
}

#[cfg(not(windows))]
fn sync_parent(parent: &Path) -> Result<(), PluginNodeReceiptDependencyError> {
    OpenOptions::new()
        .read(true)
        .open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| PluginNodeReceiptDependencyError::Storage)
}

#[cfg(windows)]
#[allow(
    clippy::unnecessary_wraps,
    reason = "keeps one cross-platform durability helper signature"
)]
fn sync_parent(_parent: &Path) -> Result<(), PluginNodeReceiptDependencyError> {
    Ok(())
}

/// Stable dependency receipt failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum PluginNodeReceiptDependencyError {
    /// Identity or configuration was unsafe.
    #[error("plugin-node receipt request is invalid")]
    InvalidRequest,
    /// Receipt exceeds the bounded representation.
    #[error("plugin-node receipt exceeds its byte bound")]
    TooLarge,
    /// Filesystem operation failed.
    #[error("plugin-node receipt storage is unavailable")]
    Storage,
    /// Stored receipt failed checksum or representation validation.
    #[error("plugin-node receipt is corrupt")]
    Corrupt,
    /// Existing receipt differs for the same exact identity.
    #[error("plugin-node receipt conflicts with existing content")]
    Conflict,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> DependencyPluginNodeReceiptIdentity {
        DependencyPluginNodeReceiptIdentity {
            session_id: Uuid::from_u128(1).to_string(),
            invocation_id: format!(
                "plugin-node:{}",
                ContentHash::digest(b"invocation").to_hex()
            ),
        }
    }

    fn context_transform_identity() -> DependencyPluginNodeReceiptIdentity {
        DependencyPluginNodeReceiptIdentity {
            session_id: Uuid::from_u128(2).to_string(),
            invocation_id: format!(
                "plugin-context-transform:{}",
                ContentHash::digest(b"context-transform-invocation").to_hex()
            ),
        }
    }

    fn automatic_memory_identity() -> DependencyPluginNodeReceiptIdentity {
        DependencyPluginNodeReceiptIdentity {
            session_id: Uuid::from_u128(3).to_string(),
            invocation_id: format!(
                "plugin-automatic-memory-write:{}",
                ContentHash::digest(b"automatic-memory-invocation").to_hex()
            ),
        }
    }

    #[test]
    fn filesystem_receipt_survives_reopen_and_rejects_substitution_and_corruption() {
        let root = tempfile::tempdir().expect("root");
        let store = FilePluginNodeReceiptDependency::new(root.path().to_owned()).expect("store");
        let request = DependencyStorePluginNodeReceiptRequest {
            identity: identity(),
            receipt_bytes: br#"{"outcome":"completed"}"#.to_vec(),
        };
        let first = store
            .store_plugin_node_receipt(request.clone())
            .expect("first");
        assert_eq!(
            store
                .store_plugin_node_receipt(request.clone())
                .expect("duplicate"),
            first
        );
        let reopened =
            FilePluginNodeReceiptDependency::new(root.path().to_owned()).expect("reopened");
        assert_eq!(
            reopened
                .load_plugin_node_receipt(&request.identity)
                .expect("load"),
            Some(first)
        );
        let mut substituted = request.clone();
        substituted.receipt_bytes = br#"{"outcome":"failed"}"#.to_vec();
        assert_eq!(
            reopened.store_plugin_node_receipt(substituted),
            Err(PluginNodeReceiptDependencyError::Conflict)
        );

        let path = reopened.receipt_path(&request.identity).expect("path");
        let mut bytes = fs::read(&path).expect("bytes");
        let index = bytes.len() / 2;
        bytes[index] ^= 1;
        fs::write(path, bytes).expect("corrupt");
        assert_eq!(
            reopened.load_plugin_node_receipt(&request.identity),
            Err(PluginNodeReceiptDependencyError::Corrupt)
        );
    }

    #[test]
    fn context_transform_receipt_uses_generic_namespace_and_survives_reopen() {
        let root = tempfile::tempdir().expect("root");
        let store = FilePluginNodeReceiptDependency::new(root.path().to_owned()).expect("store");
        let request = DependencyStorePluginNodeReceiptRequest {
            identity: context_transform_identity(),
            receipt_bytes: br#"{"outcome":"completed","replacement":[]}"#.to_vec(),
        };
        let record = store
            .store_plugin_node_receipt(request.clone())
            .expect("store");
        assert!(
            store
                .receipt_path(&request.identity)
                .expect("path")
                .to_string_lossy()
                .contains("plugin-invocation-receipts")
        );

        let reopened =
            FilePluginNodeReceiptDependency::new(root.path().to_owned()).expect("reopened");
        assert_eq!(
            reopened
                .load_plugin_node_receipt(&request.identity)
                .expect("load"),
            Some(record)
        );
    }

    #[test]
    fn automatic_memory_receipt_uses_generic_namespace_and_survives_reopen() {
        let root = tempfile::tempdir().expect("root");
        let store = FilePluginNodeReceiptDependency::new(root.path().to_owned()).expect("store");
        let request = DependencyStorePluginNodeReceiptRequest {
            identity: automatic_memory_identity(),
            receipt_bytes: br#"{"outcome":"completed","retained":true}"#.to_vec(),
        };
        let record = store
            .store_plugin_node_receipt(request.clone())
            .expect("store");
        assert!(
            store
                .receipt_path(&request.identity)
                .expect("path")
                .to_string_lossy()
                .contains("plugin-invocation-receipts")
        );

        let reopened =
            FilePluginNodeReceiptDependency::new(root.path().to_owned()).expect("reopened");
        assert_eq!(
            reopened
                .load_plugin_node_receipt(&request.identity)
                .expect("load"),
            Some(record)
        );
    }
}
