//! Business-facing workspace lease dataset.

use std::{collections::BTreeSet, path::PathBuf};

use agentmod_runtime_dependency::workspace::{
    DependencyBindWorkspaceSessionRequest, DependencyEnsureWorkspaceLeaseRequest,
    DependencyWorkspaceLeaseMode, DependencyWorkspaceLeaseRecord, DependencyWorkspaceOwnership,
    WorkspaceLeaseDependencyError, WorkspaceLeaseDependencyPort,
};
use thiserror::Error;

/// Data-owned workspace lease mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkspaceLeaseDataMode {
    /// Borrow the parent workspace under read-only runtime authorization.
    SharedReadOnly,
    /// Materialize a bounded runtime-owned filesystem copy.
    IsolatedCopy,
    /// Materialize an independent Git worktree.
    BranchWorkspace,
}

/// Data-owned workspace ownership classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkspaceLeaseDataOwnership {
    /// Parent workspace remains externally owned.
    BorrowedReadOnly,
    /// Runtime owns a bounded copy.
    RuntimeOwnedCopy,
    /// Runtime owns a Git worktree.
    RuntimeOwnedBranch,
}

/// Data request to materialize or reconcile one exact lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsureWorkspaceLeaseDataRequest {
    /// Durable receipt and owned-workspace root.
    pub lease_root: PathBuf,
    /// Parent session workspace.
    pub source_workspace: PathBuf,
    /// Stable logic-derived identity.
    pub lease_id: String,
    /// Complete immutable contract hash.
    pub contract_hash: String,
    /// Requested materialization mode.
    pub mode: WorkspaceLeaseDataMode,
    /// Stable runtime ownership key.
    pub owner: String,
    /// Explicit branch merge policy.
    pub merge_policy: Option<String>,
    /// Maximum regular files copied or hashed.
    pub maximum_files: u64,
    /// Maximum aggregate regular-file bytes copied or hashed.
    pub maximum_bytes: u64,
    /// Maximum recursive directory depth.
    pub maximum_depth: u32,
    /// Exact excluded path-component names.
    pub excluded_names: BTreeSet<String>,
}

/// Data record returned to runtime logic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceLeaseDataRecord {
    /// Stable lease identity.
    pub lease_id: String,
    /// Complete immutable contract hash.
    pub contract_hash: String,
    /// Dependency-normalized parent root.
    pub source_root: PathBuf,
    /// Exact effective child workspace.
    pub effective_root: PathBuf,
    /// Exact materialization mode.
    pub mode: WorkspaceLeaseDataMode,
    /// Runtime ownership classification.
    pub ownership: WorkspaceLeaseDataOwnership,
    /// Stable ownership key.
    pub owner: String,
    /// Explicit branch merge policy.
    pub merge_policy: Option<String>,
    /// Initial source tree hash.
    pub source_snapshot_hash: String,
    /// Initial materialized tree hash.
    pub materialized_snapshot_hash: String,
    /// Stable Git branch name, if any.
    pub branch_name: Option<String>,
    /// Exact immutable traversal depth bound.
    pub maximum_depth: u32,
    /// Exact immutable excluded path-component names.
    pub excluded_names: BTreeSet<String>,
    /// Git revision from which a branch workspace was created.
    pub base_revision: Option<String>,
    /// Git revision observed after materialization.
    pub materialized_revision: Option<String>,
}

/// Data request to bind a prepared child session to one exact workspace lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BindWorkspaceSessionDataRequest {
    /// Durable workspace-lease storage root.
    pub lease_root: PathBuf,
    /// Prepared runtime-managed child session.
    pub session_id: String,
    /// Stable immutable lease identity.
    pub lease_id: String,
    /// Hash of the complete immutable lease contract.
    pub lease_hash: String,
    /// Exact dependency-normalized child workspace.
    pub effective_root: PathBuf,
    /// Whether workspace-mutating effects are prohibited.
    pub read_only: bool,
}

/// Narrow workspace dataset consumed by runtime logic.
pub trait WorkspaceLeaseDataPort {
    /// Materializes or reconciles one exact lease.
    ///
    /// # Errors
    ///
    /// Returns a stable data error when the dependency fails closed.
    fn ensure_workspace_lease(
        &self,
        request: EnsureWorkspaceLeaseDataRequest,
    ) -> Result<WorkspaceLeaseDataRecord, WorkspaceLeaseDataError>;

    /// Persists or reconciles an exact child-session lease binding.
    ///
    /// # Errors
    ///
    /// Returns a stable data error when the dependency fails closed.
    fn bind_workspace_session(
        &self,
        request: BindWorkspaceSessionDataRequest,
    ) -> Result<(), WorkspaceLeaseDataError>;
}

impl<D: WorkspaceLeaseDependencyPort> WorkspaceLeaseDataPort for super::RuntimeData<D> {
    fn ensure_workspace_lease(
        &self,
        request: EnsureWorkspaceLeaseDataRequest,
    ) -> Result<WorkspaceLeaseDataRecord, WorkspaceLeaseDataError> {
        self.dependency
            .ensure_workspace_lease(DependencyEnsureWorkspaceLeaseRequest {
                lease_root: request.lease_root,
                source_workspace: request.source_workspace,
                lease_id: request.lease_id,
                contract_hash: request.contract_hash,
                mode: map_mode(request.mode),
                owner: request.owner,
                merge_policy: request.merge_policy,
                maximum_files: request.maximum_files,
                maximum_bytes: request.maximum_bytes,
                maximum_depth: request.maximum_depth,
                excluded_names: request.excluded_names,
            })
            .map(map_record)
            .map_err(|error| map_error(&error))
    }

    fn bind_workspace_session(
        &self,
        request: BindWorkspaceSessionDataRequest,
    ) -> Result<(), WorkspaceLeaseDataError> {
        self.dependency
            .bind_workspace_session(DependencyBindWorkspaceSessionRequest {
                lease_root: request.lease_root,
                session_id: request.session_id,
                lease_id: request.lease_id,
                lease_hash: request.lease_hash,
                effective_root: request.effective_root,
                read_only: request.read_only,
            })
            .map(|_| ())
            .map_err(|error| map_error(&error))
    }
}

fn map_mode(mode: WorkspaceLeaseDataMode) -> DependencyWorkspaceLeaseMode {
    match mode {
        WorkspaceLeaseDataMode::SharedReadOnly => DependencyWorkspaceLeaseMode::SharedReadOnly,
        WorkspaceLeaseDataMode::IsolatedCopy => DependencyWorkspaceLeaseMode::IsolatedCopy,
        WorkspaceLeaseDataMode::BranchWorkspace => DependencyWorkspaceLeaseMode::BranchWorkspace,
    }
}

fn map_record(record: DependencyWorkspaceLeaseRecord) -> WorkspaceLeaseDataRecord {
    WorkspaceLeaseDataRecord {
        lease_id: record.lease_id,
        contract_hash: record.contract_hash,
        source_root: record.source_root,
        effective_root: record.effective_root,
        mode: match record.mode {
            DependencyWorkspaceLeaseMode::SharedReadOnly => WorkspaceLeaseDataMode::SharedReadOnly,
            DependencyWorkspaceLeaseMode::IsolatedCopy => WorkspaceLeaseDataMode::IsolatedCopy,
            DependencyWorkspaceLeaseMode::BranchWorkspace => {
                WorkspaceLeaseDataMode::BranchWorkspace
            }
        },
        ownership: match record.ownership {
            DependencyWorkspaceOwnership::BorrowedReadOnly => {
                WorkspaceLeaseDataOwnership::BorrowedReadOnly
            }
            DependencyWorkspaceOwnership::RuntimeOwnedCopy => {
                WorkspaceLeaseDataOwnership::RuntimeOwnedCopy
            }
            DependencyWorkspaceOwnership::RuntimeOwnedBranch => {
                WorkspaceLeaseDataOwnership::RuntimeOwnedBranch
            }
        },
        owner: record.owner,
        merge_policy: record.merge_policy,
        source_snapshot_hash: record.source_snapshot_hash,
        materialized_snapshot_hash: record.materialized_snapshot_hash,
        branch_name: record.branch_name,
        maximum_depth: record.maximum_depth,
        excluded_names: record.excluded_names,
        base_revision: record.base_revision,
        materialized_revision: record.materialized_revision,
    }
}

fn map_error(error: &WorkspaceLeaseDependencyError) -> WorkspaceLeaseDataError {
    match error {
        WorkspaceLeaseDependencyError::InvalidRequest
        | WorkspaceLeaseDependencyError::InvalidLeaseIdentity
        | WorkspaceLeaseDependencyError::InvalidSource
        | WorkspaceLeaseDependencyError::InvalidPathEncoding
        | WorkspaceLeaseDependencyError::InvalidEffectiveRoot
        | WorkspaceLeaseDependencyError::MergePolicyRequired
        | WorkspaceLeaseDependencyError::UnexpectedMergePolicy
        | WorkspaceLeaseDependencyError::SymlinkProhibited
        | WorkspaceLeaseDependencyError::UnsupportedFileType
        | WorkspaceLeaseDependencyError::PathTraversal
        | WorkspaceLeaseDependencyError::BoundsExceeded => WorkspaceLeaseDataError::Invalid,
        WorkspaceLeaseDependencyError::AmbiguousMaterialization => {
            WorkspaceLeaseDataError::Ambiguous
        }
        WorkspaceLeaseDependencyError::RecoveryMismatch
        | WorkspaceLeaseDependencyError::MissingMaterialization
        | WorkspaceLeaseDependencyError::MaterializationMismatch
        | WorkspaceLeaseDependencyError::SourceSnapshotChanged => {
            WorkspaceLeaseDataError::RecoveryMismatch
        }
        WorkspaceLeaseDependencyError::GitMaterialization
        | WorkspaceLeaseDependencyError::Io(_)
        | WorkspaceLeaseDependencyError::Encoding(_) => WorkspaceLeaseDataError::Unavailable,
    }
}

/// Workspace data failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum WorkspaceLeaseDataError {
    /// Request or bounded content is invalid.
    #[error("workspace lease data request is invalid")]
    Invalid,
    /// A partial materialization may exist.
    #[error("workspace lease materialization is ambiguous")]
    Ambiguous,
    /// Existing immutable receipt differs or is incomplete.
    #[error("workspace lease recovery identity differs")]
    RecoveryMismatch,
    /// Filesystem or Git dependency is unavailable.
    #[error("workspace lease dependency is unavailable")]
    Unavailable,
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentmod_runtime_dependency::LocalRuntimeDependencies;
    use tempfile::tempdir;

    #[test]
    fn maps_local_dependency_receipt_without_leaking_dependency_types() {
        let temporary = tempdir().expect("temporary");
        let source = tempdir().expect("source");
        let data = super::super::RuntimeData::new(LocalRuntimeDependencies);
        let record = data
            .ensure_workspace_lease(EnsureWorkspaceLeaseDataRequest {
                lease_root: temporary.path().join("leases"),
                source_workspace: source.path().to_path_buf(),
                lease_id: String::from("lease-0123456789abcdef"),
                contract_hash: "b".repeat(64),
                mode: WorkspaceLeaseDataMode::IsolatedCopy,
                owner: String::from("owner"),
                merge_policy: None,
                maximum_files: 4,
                maximum_bytes: 1024,
                maximum_depth: 8,
                excluded_names: [String::from(".git"), String::from(".env*")]
                    .into_iter()
                    .collect(),
            })
            .expect("lease");
        assert_eq!(record.mode, WorkspaceLeaseDataMode::IsolatedCopy);
        assert_eq!(
            record.ownership,
            WorkspaceLeaseDataOwnership::RuntimeOwnedCopy
        );
    }
}
