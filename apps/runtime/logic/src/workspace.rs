//! Logic-owned immutable child workspace lease contracts.
//!
//! This coordinator selects no operating-system implementation. It derives an
//! exact stable contract and delegates materialization to runtime data.

use std::{collections::BTreeSet, path::PathBuf, str::FromStr};

use agentmod_primitives::{ContentHash, Sequence, SessionId};
use agentmod_runtime_data::workspace::{
    BindWorkspaceSessionDataRequest, EnsureWorkspaceLeaseDataRequest, WorkspaceLeaseDataError,
    WorkspaceLeaseDataMode, WorkspaceLeaseDataOwnership, WorkspaceLeaseDataPort,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Default maximum file count for one owned worker workspace.
pub const DEFAULT_MAXIMUM_WORKSPACE_FILES: u64 = 100_000;
/// Default maximum aggregate bytes for one owned worker workspace.
pub const DEFAULT_MAXIMUM_WORKSPACE_BYTES: u64 = 4 * 1024 * 1024 * 1024;
/// Default maximum recursive directory depth.
pub const DEFAULT_MAXIMUM_WORKSPACE_DEPTH: u32 = 32;

/// Security-sensitive path components excluded from owned worker snapshots.
#[must_use]
pub fn default_workspace_exclusions() -> BTreeSet<String> {
    [
        ".agentmod",
        ".env*",
        ".git",
        ".secrets",
        ".workspace-leases",
        "node_modules",
        "secrets",
        "target",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

/// Logic-owned workspace lease mode persisted in parent and child contracts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum WorkspaceLeaseMode {
    /// Borrow the parent workspace under enforced read-only authorization.
    SharedReadOnly,
    /// Create a bounded runtime-owned filesystem copy.
    IsolatedCopy,
    /// Create an independent Git worktree with explicit merge semantics.
    BranchWorkspace {
        /// Runtime-reviewed merge policy.
        merge_policy: WorkspaceMergePolicy,
    },
}

/// Explicit merge policy for an owned branch workspace.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceMergePolicy {
    /// Integration requires an explicit reviewed manual action.
    ManualReview,
    /// Integration may use a reviewed fast-forward action.
    ReviewedFastForward,
    /// Integration may use a reviewed three-way merge action.
    ReviewedThreeWay,
}

impl WorkspaceMergePolicy {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ManualReview => "manual_review",
            Self::ReviewedFastForward => "reviewed_fast_forward",
            Self::ReviewedThreeWay => "reviewed_three_way",
        }
    }
}

/// Logic-owned workspace ownership classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceLeaseOwnership {
    /// Parent workspace remains externally owned.
    BorrowedReadOnly,
    /// Runtime owns a bounded copy.
    RuntimeOwnedCopy,
    /// Runtime owns a Git worktree.
    RuntimeOwnedBranch,
}

/// Stable graph-owned lease owner identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceLeaseOwner {
    /// Parent session.
    pub parent_session_id: SessionId,
    /// Canonical parent creation proposal sequence.
    pub parent_action_sequence: Sequence,
    /// Parent graph node.
    pub parent_graph_node_id: String,
    /// Stable graph task identity.
    pub task_id: String,
}

/// Logic request to materialize or reconcile one exact child workspace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsureWorkspaceLeaseCommand {
    /// Canonical sessions storage root.
    pub sessions_root: PathBuf,
    /// Exact parent session workspace.
    pub source_workspace: PathBuf,
    /// Stable graph-owned owner.
    pub owner: WorkspaceLeaseOwner,
    /// Exact immutable mode.
    pub mode: WorkspaceLeaseMode,
    /// Maximum regular files copied or hashed.
    pub maximum_files: u64,
    /// Maximum aggregate regular-file bytes copied or hashed.
    pub maximum_bytes: u64,
    /// Maximum recursive directory depth.
    pub maximum_depth: u32,
    /// Exact security-sensitive path-component exclusions.
    pub excluded_names: BTreeSet<String>,
}

/// Canonical workspace lease contract suitable for event persistence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceLeaseContract {
    /// Stable lease identity.
    pub lease_id: String,
    /// Hash of this immutable lease contract.
    pub lease_hash: ContentHash,
    /// Exact dependency-normalized source root.
    pub source_root: PathBuf,
    /// Exact effective child workspace.
    pub effective_root: PathBuf,
    /// Exact lease mode.
    pub mode: WorkspaceLeaseMode,
    /// Runtime ownership classification.
    pub ownership: WorkspaceLeaseOwnership,
    /// Stable graph-owned owner.
    pub owner: WorkspaceLeaseOwner,
    /// Initial source snapshot hash.
    pub source_snapshot_hash: ContentHash,
    /// Initial materialized snapshot hash.
    pub materialized_snapshot_hash: ContentHash,
    /// Stable Git branch name, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch_name: Option<String>,
    /// Immutable maximum file count.
    pub maximum_files: u64,
    /// Immutable maximum aggregate bytes.
    pub maximum_bytes: u64,
    /// Immutable maximum recursive depth.
    pub maximum_depth: u32,
    /// Immutable excluded path-component names.
    pub excluded_names: BTreeSet<String>,
    /// Git revision from which a branch workspace was created.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_revision: Option<String>,
    /// Git revision observed immediately after materialization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub materialized_revision: Option<String>,
}

impl WorkspaceLeaseContract {
    /// Verifies that the persisted hash binds every immutable lease field.
    ///
    /// # Errors
    ///
    /// Returns an encoding or substitution error when the canonical contract
    /// cannot reproduce its recorded hash.
    pub fn validate_hash(&self) -> Result<(), WorkspaceLeaseLogicError> {
        if workspace_lease_hash(self)? == self.lease_hash {
            Ok(())
        } else {
            Err(WorkspaceLeaseLogicError::SubstitutedReceipt)
        }
    }
}

/// Logic coordinator for exact workspace leases.
#[derive(Clone, Debug)]
pub struct WorkspaceLeaseLogic<D> {
    data: D,
}

impl<D> WorkspaceLeaseLogic<D> {
    /// Constructs the coordinator over a runtime data boundary.
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}

impl<D: WorkspaceLeaseDataPort> WorkspaceLeaseLogic<D> {
    /// Materializes or reconciles one exact immutable workspace contract.
    ///
    /// # Errors
    ///
    /// Fails closed for invalid owners/bounds or dependency ambiguity.
    pub fn ensure_workspace_lease(
        &self,
        command: EnsureWorkspaceLeaseCommand,
    ) -> Result<WorkspaceLeaseContract, WorkspaceLeaseLogicError> {
        validate(&command)?;
        let owner_json =
            serde_json::to_vec(&command.owner).map_err(|_| WorkspaceLeaseLogicError::Encoding)?;
        let lease_id = format!("lease-{}", ContentHash::digest(&owner_json).to_hex());
        let contract_input = WorkspaceLeaseHashInput {
            lease_id: &lease_id,
            source_workspace: &command.source_workspace,
            owner: &command.owner,
            mode: &command.mode,
            maximum_files: command.maximum_files,
            maximum_bytes: command.maximum_bytes,
            maximum_depth: command.maximum_depth,
            excluded_names: &command.excluded_names,
        };
        let contract_json =
            serde_json::to_vec(&contract_input).map_err(|_| WorkspaceLeaseLogicError::Encoding)?;
        let request_hash = ContentHash::digest(&contract_json);
        let (data_mode, merge_policy) = map_mode(&command.mode);
        let owner =
            String::from_utf8(owner_json).map_err(|_| WorkspaceLeaseLogicError::Encoding)?;
        let record = self
            .data
            .ensure_workspace_lease(EnsureWorkspaceLeaseDataRequest {
                lease_root: command.sessions_root.join(".workspace-leases"),
                source_workspace: command.source_workspace,
                lease_id: lease_id.clone(),
                contract_hash: request_hash.to_hex(),
                mode: data_mode,
                owner,
                merge_policy,
                maximum_files: command.maximum_files,
                maximum_bytes: command.maximum_bytes,
                maximum_depth: command.maximum_depth,
                excluded_names: command.excluded_names.clone(),
            })
            .map_err(WorkspaceLeaseLogicError::Data)?;
        if record.lease_id != lease_id
            || record.contract_hash != request_hash.to_hex()
            || record.mode != map_mode(&command.mode).0
        {
            return Err(WorkspaceLeaseLogicError::SubstitutedReceipt);
        }
        let source_snapshot_hash = ContentHash::from_str(&record.source_snapshot_hash)
            .map_err(|_| WorkspaceLeaseLogicError::SubstitutedReceipt)?;
        let materialized_snapshot_hash = ContentHash::from_str(&record.materialized_snapshot_hash)
            .map_err(|_| WorkspaceLeaseLogicError::SubstitutedReceipt)?;
        let contract = WorkspaceLeaseContract {
            lease_id,
            lease_hash: ContentHash::from_bytes([0; 32]),
            source_root: record.source_root,
            effective_root: record.effective_root,
            mode: command.mode,
            ownership: match record.ownership {
                WorkspaceLeaseDataOwnership::BorrowedReadOnly => {
                    WorkspaceLeaseOwnership::BorrowedReadOnly
                }
                WorkspaceLeaseDataOwnership::RuntimeOwnedCopy => {
                    WorkspaceLeaseOwnership::RuntimeOwnedCopy
                }
                WorkspaceLeaseDataOwnership::RuntimeOwnedBranch => {
                    WorkspaceLeaseOwnership::RuntimeOwnedBranch
                }
            },
            owner: command.owner,
            source_snapshot_hash,
            materialized_snapshot_hash,
            branch_name: record.branch_name,
            maximum_files: command.maximum_files,
            maximum_bytes: command.maximum_bytes,
            maximum_depth: command.maximum_depth,
            excluded_names: command.excluded_names,
            base_revision: record.base_revision,
            materialized_revision: record.materialized_revision,
        };
        seal_workspace_lease_contract(contract)
    }

    /// Binds a prepared child-session identity before atomic session creation.
    ///
    /// # Errors
    ///
    /// Fails closed on a substituted lease hash or durable binding mismatch.
    pub fn bind_session(
        &self,
        sessions_root: &std::path::Path,
        session_id: SessionId,
        lease: &WorkspaceLeaseContract,
    ) -> Result<(), WorkspaceLeaseLogicError> {
        lease.validate_hash()?;
        self.data
            .bind_workspace_session(BindWorkspaceSessionDataRequest {
                lease_root: sessions_root.join(".workspace-leases"),
                session_id: session_id.to_string(),
                lease_id: lease.lease_id.clone(),
                lease_hash: lease.lease_hash.to_hex(),
                effective_root: lease.effective_root.clone(),
                read_only: lease.mode == WorkspaceLeaseMode::SharedReadOnly,
            })
            .map_err(WorkspaceLeaseLogicError::Data)
    }
}

#[derive(Serialize)]
struct WorkspaceLeaseHashInput<'a> {
    lease_id: &'a str,
    source_workspace: &'a PathBuf,
    owner: &'a WorkspaceLeaseOwner,
    mode: &'a WorkspaceLeaseMode,
    maximum_files: u64,
    maximum_bytes: u64,
    maximum_depth: u32,
    excluded_names: &'a BTreeSet<String>,
}

#[derive(Serialize)]
struct PersistedWorkspaceLeaseHashInput<'a> {
    lease_id: &'a str,
    source_root: &'a PathBuf,
    effective_root: &'a PathBuf,
    mode: &'a WorkspaceLeaseMode,
    ownership: WorkspaceLeaseOwnership,
    owner: &'a WorkspaceLeaseOwner,
    source_snapshot_hash: ContentHash,
    materialized_snapshot_hash: ContentHash,
    branch_name: &'a Option<String>,
    maximum_files: u64,
    maximum_bytes: u64,
    maximum_depth: u32,
    excluded_names: &'a BTreeSet<String>,
    base_revision: &'a Option<String>,
    materialized_revision: &'a Option<String>,
}

fn workspace_lease_hash(
    contract: &WorkspaceLeaseContract,
) -> Result<ContentHash, WorkspaceLeaseLogicError> {
    let input = PersistedWorkspaceLeaseHashInput {
        lease_id: &contract.lease_id,
        source_root: &contract.source_root,
        effective_root: &contract.effective_root,
        mode: &contract.mode,
        ownership: contract.ownership,
        owner: &contract.owner,
        source_snapshot_hash: contract.source_snapshot_hash,
        materialized_snapshot_hash: contract.materialized_snapshot_hash,
        branch_name: &contract.branch_name,
        maximum_files: contract.maximum_files,
        maximum_bytes: contract.maximum_bytes,
        maximum_depth: contract.maximum_depth,
        excluded_names: &contract.excluded_names,
        base_revision: &contract.base_revision,
        materialized_revision: &contract.materialized_revision,
    };
    serde_json::to_vec(&input)
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| WorkspaceLeaseLogicError::Encoding)
}

fn seal_workspace_lease_contract(
    mut contract: WorkspaceLeaseContract,
) -> Result<WorkspaceLeaseContract, WorkspaceLeaseLogicError> {
    contract.lease_hash = workspace_lease_hash(&contract)?;
    Ok(contract)
}

#[cfg(test)]
pub(crate) fn test_workspace_lease(
    owner: WorkspaceLeaseOwner,
    effective_root: PathBuf,
) -> WorkspaceLeaseContract {
    seal_workspace_lease_contract(WorkspaceLeaseContract {
        lease_id: format!(
            "lease-{}",
            ContentHash::digest(&serde_json::to_vec(&owner).expect("workspace owner serializes"))
                .to_hex()
        ),
        lease_hash: ContentHash::from_bytes([0; 32]),
        source_root: effective_root.clone(),
        effective_root,
        mode: WorkspaceLeaseMode::SharedReadOnly,
        ownership: WorkspaceLeaseOwnership::BorrowedReadOnly,
        owner,
        source_snapshot_hash: ContentHash::digest(b"fixture-source"),
        materialized_snapshot_hash: ContentHash::digest(b"fixture-source"),
        branch_name: None,
        maximum_files: 16,
        maximum_bytes: 4096,
        maximum_depth: 8,
        excluded_names: default_workspace_exclusions(),
        base_revision: None,
        materialized_revision: None,
    })
    .expect("fixture workspace lease")
}

fn map_mode(mode: &WorkspaceLeaseMode) -> (WorkspaceLeaseDataMode, Option<String>) {
    match mode {
        WorkspaceLeaseMode::SharedReadOnly => (WorkspaceLeaseDataMode::SharedReadOnly, None),
        WorkspaceLeaseMode::IsolatedCopy => (WorkspaceLeaseDataMode::IsolatedCopy, None),
        WorkspaceLeaseMode::BranchWorkspace { merge_policy } => (
            WorkspaceLeaseDataMode::BranchWorkspace,
            Some(String::from(merge_policy.as_str())),
        ),
    }
}

fn validate(command: &EnsureWorkspaceLeaseCommand) -> Result<(), WorkspaceLeaseLogicError> {
    if command.sessions_root.as_os_str().is_empty()
        || command.source_workspace.as_os_str().is_empty()
        || command.owner.parent_graph_node_id.trim().is_empty()
        || command.owner.parent_graph_node_id.len() > 1024
        || command.owner.task_id.trim().is_empty()
        || command.owner.task_id.len() > 1024
        || command.maximum_files == 0
        || command.maximum_files > DEFAULT_MAXIMUM_WORKSPACE_FILES
        || command.maximum_bytes == 0
        || command.maximum_bytes > DEFAULT_MAXIMUM_WORKSPACE_BYTES
        || command.maximum_depth == 0
        || command.maximum_depth > DEFAULT_MAXIMUM_WORKSPACE_DEPTH
        || command.excluded_names.is_empty()
    {
        Err(WorkspaceLeaseLogicError::Invalid)
    } else {
        Ok(())
    }
}

/// Workspace lease logic failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum WorkspaceLeaseLogicError {
    /// Contract is invalid or exceeds immutable runtime bounds.
    #[error("workspace lease contract is invalid")]
    Invalid,
    /// Logic-owned canonical encoding failed.
    #[error("workspace lease contract encoding failed")]
    Encoding,
    /// Data boundary rejected or could not reconcile the lease.
    #[error("workspace lease data failed: {0}")]
    Data(WorkspaceLeaseDataError),
    /// Data returned a different immutable lease identity.
    #[error("workspace lease receipt identity differs")]
    SubstitutedReceipt,
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentmod_runtime_data::local::local_runtime_data;
    use tempfile::tempdir;

    fn owner() -> WorkspaceLeaseOwner {
        WorkspaceLeaseOwner {
            parent_session_id: SessionId::from_str("018f1000-0000-7000-8000-000000000001")
                .expect("session"),
            parent_action_sequence: Sequence::new(7).expect("sequence"),
            parent_graph_node_id: String::from("worker-fanout/spawn-planner"),
            task_id: String::from("planner-task-0"),
        }
    }

    #[test]
    fn stable_contract_reconciles_and_isolated_copy_has_runtime_ownership() {
        let temporary = tempdir().expect("temporary");
        let source = tempdir().expect("source");
        let logic = WorkspaceLeaseLogic::new(local_runtime_data());
        let command = EnsureWorkspaceLeaseCommand {
            sessions_root: temporary.path().join("sessions"),
            source_workspace: source.path().to_path_buf(),
            owner: owner(),
            mode: WorkspaceLeaseMode::IsolatedCopy,
            maximum_files: 10,
            maximum_bytes: 1024,
            maximum_depth: 8,
            excluded_names: default_workspace_exclusions(),
        };
        let created = logic
            .ensure_workspace_lease(command.clone())
            .expect("create");
        let recovered = logic.ensure_workspace_lease(command).expect("recover");
        assert_eq!(created, recovered);
        assert_eq!(created.ownership, WorkspaceLeaseOwnership::RuntimeOwnedCopy);
        assert!(created.effective_root.is_dir());
    }
}
