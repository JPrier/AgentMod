//! Durable dependency-owned terminal receipts for supervised tool hosts.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};

use agentmod_primitives::ContentHash;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::tool::{
    DependencyToolCommand, DependencyToolEvent, DependencyToolReceipt,
    DependencyWorkspaceAuthorization, ToolHostDependencyError,
};

const RECEIPT_VERSION: u32 = 2;
const MAX_RECEIPT_BYTES: usize = 16 * 1024 * 1024;

/// Durable receipt store assembled into the runtime dependency layer.
#[derive(Clone, Debug)]
pub struct ToolReceiptDependency {
    sessions_root: PathBuf,
    post_persist_delay: Duration,
}

impl ToolReceiptDependency {
    /// Creates a receipt store rooted beneath canonical session directories.
    ///
    /// # Errors
    ///
    /// Rejects an empty sessions root.
    pub fn new(sessions_root: PathBuf) -> Result<Self, ToolHostDependencyError> {
        if sessions_root.as_os_str().is_empty() {
            return Err(ToolHostDependencyError::InvalidConfiguration);
        }
        Ok(Self {
            sessions_root,
            post_persist_delay: Duration::ZERO,
        })
    }

    /// Configures a bounded crash-injection observation window after durable
    /// receipt persistence and before the result returns to runtime logic.
    ///
    /// # Errors
    ///
    /// Rejects delays longer than ten seconds.
    pub fn with_post_persist_delay(
        mut self,
        delay: Duration,
    ) -> Result<Self, ToolHostDependencyError> {
        if delay > Duration::from_secs(10) {
            return Err(ToolHostDependencyError::InvalidConfiguration);
        }
        self.post_persist_delay = delay;
        Ok(self)
    }

    /// Returns the configured post-persist observation window.
    #[must_use]
    pub const fn post_persist_delay(&self) -> Duration {
        self.post_persist_delay
    }

    /// Returns the canonical sessions root for adjacent dependency-owned
    /// authorization indexes.
    pub(crate) fn sessions_root(&self) -> &Path {
        &self.sessions_root
    }

    /// Loads a verified terminal receipt matching the exact request.
    ///
    /// # Errors
    ///
    /// Returns a receipt error for malformed identities, corruption, binding
    /// mismatch, or filesystem failure.
    pub fn load(
        &self,
        command: &DependencyToolCommand,
    ) -> Result<Option<Vec<DependencyToolEvent>>, ToolHostDependencyError> {
        let path = self.receipt_path(command)?;
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(ToolHostDependencyError::ReceiptStorage),
        };
        if bytes.len() > MAX_RECEIPT_BYTES {
            return Err(ToolHostDependencyError::ReceiptCorrupt);
        }
        let stored: StoredReceipt =
            serde_json::from_slice(&bytes).map_err(|_| ToolHostDependencyError::ReceiptCorrupt)?;
        stored.verify(command)?;
        Ok(Some(stored.payload.events))
    }

    /// Atomically persists a complete terminal stream before it is returned to
    /// runtime logic.
    ///
    /// # Errors
    ///
    /// Returns a receipt error for invalid event streams, serialization, or
    /// durable filesystem failures.
    pub fn persist(
        &self,
        command: &DependencyToolCommand,
        events: &[DependencyToolEvent],
    ) -> Result<(), ToolHostDependencyError> {
        validate_terminal_events(command, events)?;
        let path = self.receipt_path(command)?;
        if let Some(existing) = self.load(command)? {
            return if existing == events {
                Ok(())
            } else {
                Err(ToolHostDependencyError::ReceiptConflict)
            };
        }
        let payload = ReceiptPayload::new(command, events.to_vec())?;
        let checksum = payload.checksum()?;
        let stored = StoredReceipt { payload, checksum };
        let bytes =
            serde_json::to_vec(&stored).map_err(|_| ToolHostDependencyError::ReceiptCorrupt)?;
        if bytes.len() > MAX_RECEIPT_BYTES {
            return Err(ToolHostDependencyError::ReceiptCorrupt);
        }
        let parent = path
            .parent()
            .ok_or(ToolHostDependencyError::ReceiptStorage)?;
        fs::create_dir_all(parent).map_err(|_| ToolHostDependencyError::ReceiptStorage)?;
        let temporary = parent.join(format!(".{}.tmp", Uuid::now_v7()));
        let write_result = (|| {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .map_err(|_| ToolHostDependencyError::ReceiptStorage)?;
            file.write_all(&bytes)
                .map_err(|_| ToolHostDependencyError::ReceiptStorage)?;
            file.sync_all()
                .map_err(|_| ToolHostDependencyError::ReceiptStorage)?;
            fs::rename(&temporary, &path).map_err(|_| ToolHostDependencyError::ReceiptStorage)?;
            sync_parent(parent)
        })();
        if write_result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        write_result
    }

    /// Enumerates checksum-verified terminal receipts for startup
    /// reconciliation without contacting a capability host.
    ///
    /// # Errors
    ///
    /// Returns an error when a receipt directory or receipt is malformed,
    /// corrupt, or cannot be read.
    pub fn list(&self) -> Result<Vec<DependencyToolReceipt>, ToolHostDependencyError> {
        let mut paths = Vec::new();
        let sessions = match fs::read_dir(&self.sessions_root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(_) => return Err(ToolHostDependencyError::ReceiptStorage),
        };
        for session in sessions {
            let session = session.map_err(|_| ToolHostDependencyError::ReceiptStorage)?;
            if !session
                .file_type()
                .map_err(|_| ToolHostDependencyError::ReceiptStorage)?
                .is_dir()
                || Uuid::parse_str(&session.file_name().to_string_lossy()).is_err()
            {
                continue;
            }
            let directory = session.path().join("artifacts").join("tool-receipts");
            let receipts = match fs::read_dir(directory) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(_) => return Err(ToolHostDependencyError::ReceiptStorage),
            };
            for receipt in receipts {
                let receipt = receipt.map_err(|_| ToolHostDependencyError::ReceiptStorage)?;
                if receipt
                    .file_type()
                    .map_err(|_| ToolHostDependencyError::ReceiptStorage)?
                    .is_file()
                    && receipt
                        .path()
                        .extension()
                        .is_some_and(|value| value == "json")
                {
                    paths.push(receipt.path());
                }
            }
        }
        paths.sort();
        paths
            .into_iter()
            .map(|path| {
                let bytes = fs::read(path).map_err(|_| ToolHostDependencyError::ReceiptStorage)?;
                if bytes.len() > MAX_RECEIPT_BYTES {
                    return Err(ToolHostDependencyError::ReceiptCorrupt);
                }
                let stored: StoredReceipt = serde_json::from_slice(&bytes)
                    .map_err(|_| ToolHostDependencyError::ReceiptCorrupt)?;
                let command = stored.payload.to_command()?;
                stored.verify(&command)?;
                Ok(DependencyToolReceipt { command })
            })
            .collect()
    }

    fn receipt_path(
        &self,
        command: &DependencyToolCommand,
    ) -> Result<PathBuf, ToolHostDependencyError> {
        let session = Uuid::parse_str(&command.session_id)
            .map_err(|_| ToolHostDependencyError::InvalidRequest)?;
        if command.execution_id.trim().is_empty() || command.execution_id.len() > 512 {
            return Err(ToolHostDependencyError::InvalidRequest);
        }
        let identity = serde_json::to_vec(&(
            RECEIPT_VERSION,
            session,
            &command.execution_id,
            &command.call_id,
        ))
        .map_err(|_| ToolHostDependencyError::ReceiptCorrupt)?;
        Ok(self
            .sessions_root
            .join(session.to_string())
            .join("artifacts")
            .join("tool-receipts")
            .join(format!("{}.json", ContentHash::digest(&identity).to_hex())))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StoredReceipt {
    payload: ReceiptPayload,
    checksum: ContentHash,
}

impl StoredReceipt {
    fn verify(&self, command: &DependencyToolCommand) -> Result<(), ToolHostDependencyError> {
        if self.checksum != self.payload.checksum()? {
            return Err(ToolHostDependencyError::ReceiptCorrupt);
        }
        self.payload.verify_binding(command)?;
        validate_terminal_events(command, &self.payload.events)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ReceiptPayload {
    version: u32,
    execution_id: String,
    session_id: String,
    call_id: String,
    tool: String,
    workspace: PathBuf,
    arguments: serde_json::Value,
    cancellation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    workspace_authorization: Option<DependencyWorkspaceAuthorization>,
    request_digest: ContentHash,
    events: Vec<DependencyToolEvent>,
}

impl ReceiptPayload {
    fn new(
        command: &DependencyToolCommand,
        events: Vec<DependencyToolEvent>,
    ) -> Result<Self, ToolHostDependencyError> {
        Ok(Self {
            version: RECEIPT_VERSION,
            execution_id: command.execution_id.clone(),
            session_id: command.session_id.clone(),
            call_id: command.call_id.clone(),
            tool: command.tool.clone(),
            workspace: command.workspace.clone(),
            arguments: command.arguments.clone(),
            cancellation_id: command.cancellation_id.clone(),
            workspace_authorization: command.workspace_authorization.clone(),
            request_digest: request_digest(command)?,
            events,
        })
    }

    fn checksum(&self) -> Result<ContentHash, ToolHostDependencyError> {
        serde_json::to_vec(self)
            .map(|bytes| ContentHash::digest(&bytes))
            .map_err(|_| ToolHostDependencyError::ReceiptCorrupt)
    }

    fn verify_binding(
        &self,
        command: &DependencyToolCommand,
    ) -> Result<(), ToolHostDependencyError> {
        if self.version != RECEIPT_VERSION
            || self.execution_id != command.execution_id
            || self.session_id != command.session_id
            || self.call_id != command.call_id
            || self.tool != command.tool
            || self.request_digest != request_digest(command)?
        {
            return Err(ToolHostDependencyError::ReceiptConflict);
        }
        Ok(())
    }

    fn to_command(&self) -> Result<DependencyToolCommand, ToolHostDependencyError> {
        if self.version != RECEIPT_VERSION
            || self.execution_id.trim().is_empty()
            || self.session_id.trim().is_empty()
            || self.call_id.trim().is_empty()
            || self.tool.trim().is_empty()
            || self.workspace.as_os_str().is_empty()
            || !self.arguments.is_object()
            || self.cancellation_id.trim().is_empty()
        {
            return Err(ToolHostDependencyError::ReceiptCorrupt);
        }
        Ok(DependencyToolCommand {
            execution_id: self.execution_id.clone(),
            receipt_only: true,
            session_id: self.session_id.clone(),
            workspace: self.workspace.clone(),
            call_id: self.call_id.clone(),
            tool: self.tool.clone(),
            arguments: self.arguments.clone(),
            cancellation_id: self.cancellation_id.clone(),
            workspace_authorization: self.workspace_authorization.clone(),
        })
    }
}

fn request_digest(command: &DependencyToolCommand) -> Result<ContentHash, ToolHostDependencyError> {
    let legacy = (
        &command.execution_id,
        &command.session_id,
        command.workspace.to_string_lossy(),
        &command.call_id,
        &command.tool,
        &command.arguments,
        &command.cancellation_id,
    );
    let bytes = match &command.workspace_authorization {
        Some(authorization) => serde_json::to_vec(&(legacy, authorization)),
        None => serde_json::to_vec(&legacy),
    };
    bytes
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| ToolHostDependencyError::ReceiptCorrupt)
}

fn validate_terminal_events(
    command: &DependencyToolCommand,
    events: &[DependencyToolEvent],
) -> Result<(), ToolHostDependencyError> {
    let Some(last) = events.last() else {
        return Err(ToolHostDependencyError::ReceiptCorrupt);
    };
    if !matches!(
        last,
        DependencyToolEvent::Completed { .. }
            | DependencyToolEvent::Failed { .. }
            | DependencyToolEvent::Cancelled { .. }
    ) || events
        .iter()
        .any(|event| event_call_id(event) != command.call_id)
        || events[..events.len() - 1].iter().any(|event| {
            matches!(
                event,
                DependencyToolEvent::Completed { .. }
                    | DependencyToolEvent::Failed { .. }
                    | DependencyToolEvent::Cancelled { .. }
            )
        })
    {
        return Err(ToolHostDependencyError::ReceiptCorrupt);
    }
    Ok(())
}

fn event_call_id(event: &DependencyToolEvent) -> &str {
    match event {
        DependencyToolEvent::Started { call_id }
        | DependencyToolEvent::Progress { call_id, .. }
        | DependencyToolEvent::Output { call_id, .. }
        | DependencyToolEvent::Completed { call_id, .. }
        | DependencyToolEvent::Failed { call_id, .. }
        | DependencyToolEvent::Cancelled { call_id } => call_id,
    }
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<(), ToolHostDependencyError> {
    OpenOptions::new()
        .read(true)
        .open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| ToolHostDependencyError::ReceiptStorage)
}

#[cfg(windows)]
#[allow(
    clippy::unnecessary_wraps,
    reason = "keeps one cross-platform durability helper signature"
)]
fn sync_parent(_path: &Path) -> Result<(), ToolHostDependencyError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    fn command(root: &Path) -> (ToolReceiptDependency, DependencyToolCommand) {
        let session = Uuid::from_u128(1).to_string();
        fs::create_dir_all(root.join(&session).join("artifacts")).expect("session artifacts");
        (
            ToolReceiptDependency::new(root.to_path_buf()).expect("store"),
            DependencyToolCommand {
                execution_id: "tool-call:one".into(),
                receipt_only: false,
                session_id: session,
                workspace: PathBuf::from("workspace"),
                call_id: "one".into(),
                tool: "filesystem.write".into(),
                arguments: serde_json::json!({"path":"a.txt","content":"done"}),
                cancellation_id: Uuid::from_u128(2).to_string(),
                workspace_authorization: None,
            },
        )
    }

    #[test]
    fn terminal_receipt_round_trips_and_binds_exact_request() {
        let root = tempdir().expect("root");
        let (store, command) = command(root.path());
        let events = vec![
            DependencyToolEvent::Started {
                call_id: "one".into(),
            },
            DependencyToolEvent::Completed {
                call_id: "one".into(),
                result: serde_json::json!({"written":true}),
                artifact: None,
                truncated: false,
            },
        ];
        store.persist(&command, &events).expect("persist");
        assert_eq!(store.load(&command).expect("load"), Some(events));

        let mut changed = command;
        changed.arguments = serde_json::json!({"path":"a.txt","content":"changed"});
        assert_eq!(
            store.load(&changed),
            Err(ToolHostDependencyError::ReceiptConflict)
        );
    }

    #[test]
    fn startup_listing_reconstructs_only_verified_exact_commands() {
        let root = tempdir().expect("root");
        let (store, command) = command(root.path());
        let events = vec![
            DependencyToolEvent::Started {
                call_id: "one".into(),
            },
            DependencyToolEvent::Completed {
                call_id: "one".into(),
                result: serde_json::json!({"written":true}),
                artifact: None,
                truncated: false,
            },
        ];
        store.persist(&command, &events).expect("persist");
        let receipts = store.list().expect("list");
        assert_eq!(receipts.len(), 1);
        let mut expected = command;
        expected.receipt_only = true;
        assert_eq!(receipts[0].command, expected);
    }

    #[test]
    fn checksum_tampering_is_rejected() {
        let root = tempdir().expect("root");
        let (store, command) = command(root.path());
        let events = vec![DependencyToolEvent::Failed {
            call_id: "one".into(),
            code: "denied".into(),
            message: "denied".into(),
            retryable: false,
        }];
        store.persist(&command, &events).expect("persist");
        let path = store.receipt_path(&command).expect("path");
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).expect("read")).expect("json");
        value["payload"]["events"][0]["Failed"]["message"] =
            serde_json::Value::String("tampered".into());
        fs::write(&path, serde_json::to_vec(&value).expect("encode")).expect("tamper");
        assert_eq!(
            store.load(&command),
            Err(ToolHostDependencyError::ReceiptCorrupt)
        );
    }
}
