//! Runtime workspace lease materialization.
//!
//! This is the only runtime layer allowed to inspect or mutate workspace
//! files. Logic supplies a bounded immutable lease request; this adapter
//! normalizes paths, creates owned workspaces, and persists an exact receipt
//! for restart reconciliation.

use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::Command,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

const RECORD_VERSION: u32 = 1;
const COPY_BUFFER_BYTES: usize = 64 * 1024;

/// Dependency-owned workspace materialization mode.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyWorkspaceLeaseMode {
    /// Borrow the parent workspace while runtime authorization denies writes.
    SharedReadOnly,
    /// Create a bounded runtime-owned filesystem snapshot.
    IsolatedCopy,
    /// Create an independent Git worktree and retain an explicit merge policy.
    BranchWorkspace,
}

/// Dependency-owned workspace ownership classification.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyWorkspaceOwnership {
    /// The runtime does not own or remove the source workspace.
    BorrowedReadOnly,
    /// The runtime owns an isolated bounded copy.
    RuntimeOwnedCopy,
    /// The runtime owns an independent Git worktree.
    RuntimeOwnedBranch,
}

/// Exact dependency request for one immutable workspace lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyEnsureWorkspaceLeaseRequest {
    /// Durable root beneath which lease receipts and owned workspaces live.
    pub lease_root: PathBuf,
    /// Parent session workspace.
    pub source_workspace: PathBuf,
    /// Stable logic-derived lease identity.
    pub lease_id: String,
    /// Hash of the complete logic-owned lease contract.
    pub contract_hash: String,
    /// Exact requested materialization mode.
    pub mode: DependencyWorkspaceLeaseMode,
    /// Stable runtime ownership key.
    pub owner: String,
    /// Explicit merge policy required for branch workspaces.
    pub merge_policy: Option<String>,
    /// Maximum regular files copied or hashed.
    pub maximum_files: u64,
    /// Maximum aggregate regular-file bytes copied or hashed.
    pub maximum_bytes: u64,
    /// Maximum recursive directory depth.
    pub maximum_depth: u32,
    /// Exact path-component names excluded by immutable policy.
    pub excluded_names: BTreeSet<String>,
}

/// Durable dependency receipt for one workspace lease.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DependencyWorkspaceLeaseRecord {
    /// Receipt schema version.
    pub record_version: u32,
    /// Stable lease identity.
    pub lease_id: String,
    /// Complete immutable contract hash.
    pub contract_hash: String,
    /// Exact normalized source root.
    pub source_root: PathBuf,
    /// Effective child-session workspace.
    pub effective_root: PathBuf,
    /// Exact materialization mode.
    pub mode: DependencyWorkspaceLeaseMode,
    /// Runtime ownership classification.
    pub ownership: DependencyWorkspaceOwnership,
    /// Stable runtime ownership key.
    pub owner: String,
    /// Explicit merge policy, if any.
    pub merge_policy: Option<String>,
    /// Source tree hash observed at first materialization.
    pub source_snapshot_hash: String,
    /// Effective tree hash observed after owned materialization.
    pub materialized_snapshot_hash: String,
    /// Stable Git branch name for branch workspaces.
    pub branch_name: Option<String>,
    /// Exact immutable traversal depth bound.
    pub maximum_depth: u32,
    /// Exact immutable excluded path-component names.
    pub excluded_names: BTreeSet<String>,
    /// Git revision from which a branch workspace was created.
    pub base_revision: Option<String>,
    /// Git revision observed in the worktree after creation.
    pub materialized_revision: Option<String>,
}

/// Dependency request to bind one prepared child session to an exact lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyBindWorkspaceSessionRequest {
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

/// Durable dependency-owned child-session workspace binding.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DependencyWorkspaceSessionBinding {
    /// Binding receipt schema version.
    pub record_version: u32,
    /// Runtime-managed child session.
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

/// Dependency boundary consumed only by runtime data.
pub trait WorkspaceLeaseDependencyPort {
    /// Materializes or reconciles one exact immutable workspace lease.
    ///
    /// # Errors
    ///
    /// Fails closed for invalid paths or bounds, symlinks, changed immutable
    /// receipts, partial prior materialization, or unavailable Git support.
    fn ensure_workspace_lease(
        &self,
        request: DependencyEnsureWorkspaceLeaseRequest,
    ) -> Result<DependencyWorkspaceLeaseRecord, WorkspaceLeaseDependencyError>;

    /// Persists or reconciles the prepared child-session/lease binding.
    ///
    /// # Errors
    ///
    /// Fails closed for invalid identities, paths, substituted bindings, or
    /// storage failures.
    fn bind_workspace_session(
        &self,
        request: DependencyBindWorkspaceSessionRequest,
    ) -> Result<DependencyWorkspaceSessionBinding, WorkspaceLeaseDependencyError>;
}

impl WorkspaceLeaseDependencyPort for crate::LocalRuntimeDependencies {
    fn ensure_workspace_lease(
        &self,
        request: DependencyEnsureWorkspaceLeaseRequest,
    ) -> Result<DependencyWorkspaceLeaseRecord, WorkspaceLeaseDependencyError> {
        ensure_workspace_lease(request)
    }

    fn bind_workspace_session(
        &self,
        request: DependencyBindWorkspaceSessionRequest,
    ) -> Result<DependencyWorkspaceSessionBinding, WorkspaceLeaseDependencyError> {
        bind_workspace_session(request)
    }
}

fn bind_workspace_session(
    request: DependencyBindWorkspaceSessionRequest,
) -> Result<DependencyWorkspaceSessionBinding, WorkspaceLeaseDependencyError> {
    validate_lease_id(&request.lease_id)?;
    if uuid::Uuid::parse_str(&request.session_id).is_err()
        || request.lease_root.as_os_str().is_empty()
        || request.lease_hash.len() != 64
        || !request
            .lease_hash
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || request.effective_root.as_os_str().is_empty()
    {
        return Err(WorkspaceLeaseDependencyError::InvalidRequest);
    }
    fs::create_dir_all(&request.lease_root).map_err(WorkspaceLeaseDependencyError::Io)?;
    let lease_root = request
        .lease_root
        .canonicalize()
        .map_err(WorkspaceLeaseDependencyError::Io)?;
    let effective_root = request
        .effective_root
        .canonicalize()
        .map_err(WorkspaceLeaseDependencyError::Io)?;
    let bindings_root = lease_root.join("bindings");
    fs::create_dir_all(&bindings_root).map_err(WorkspaceLeaseDependencyError::Io)?;
    let path = bindings_root.join(format!("{}.json", request.session_id));
    let binding = DependencyWorkspaceSessionBinding {
        record_version: RECORD_VERSION,
        session_id: request.session_id,
        lease_id: request.lease_id,
        lease_hash: request.lease_hash,
        effective_root,
        read_only: request.read_only,
    };
    if path.exists() {
        let bytes = fs::read(path).map_err(WorkspaceLeaseDependencyError::Io)?;
        let existing: DependencyWorkspaceSessionBinding =
            serde_json::from_slice(&bytes).map_err(WorkspaceLeaseDependencyError::Encoding)?;
        return if existing == binding {
            Ok(existing)
        } else {
            Err(WorkspaceLeaseDependencyError::RecoveryMismatch)
        };
    }
    persist_binding(&path, &binding)?;
    Ok(binding)
}

/// Loads the exact durable session binding for independent dispatch checks.
///
/// # Errors
///
/// Fails closed for an invalid session identity, unreadable storage, or an
/// invalid persisted binding.
pub fn load_workspace_session_binding(
    lease_root: &Path,
    session_id: &str,
) -> Result<Option<DependencyWorkspaceSessionBinding>, WorkspaceLeaseDependencyError> {
    if lease_root.as_os_str().is_empty() || uuid::Uuid::parse_str(session_id).is_err() {
        return Err(WorkspaceLeaseDependencyError::InvalidRequest);
    }
    let path = lease_root
        .join("bindings")
        .join(format!("{session_id}.json"));
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(WorkspaceLeaseDependencyError::Io(error)),
    };
    let binding =
        serde_json::from_slice(&bytes).map_err(WorkspaceLeaseDependencyError::Encoding)?;
    Ok(Some(binding))
}

#[allow(
    clippy::too_many_lines,
    reason = "lease reconciliation keeps the security-sensitive create/recover decision linear and auditable"
)]
fn ensure_workspace_lease(
    request: DependencyEnsureWorkspaceLeaseRequest,
) -> Result<DependencyWorkspaceLeaseRecord, WorkspaceLeaseDependencyError> {
    validate_request(&request)?;
    let source_root = request
        .source_workspace
        .canonicalize()
        .map_err(WorkspaceLeaseDependencyError::Io)?;
    if !source_root.is_dir() {
        return Err(WorkspaceLeaseDependencyError::InvalidSource);
    }
    fs::create_dir_all(&request.lease_root).map_err(WorkspaceLeaseDependencyError::Io)?;
    let lease_root = request
        .lease_root
        .canonicalize()
        .map_err(WorkspaceLeaseDependencyError::Io)?;
    let records_root = lease_root.join("records");
    let workspaces_root = lease_root.join("workspaces");
    fs::create_dir_all(&records_root).map_err(WorkspaceLeaseDependencyError::Io)?;
    fs::create_dir_all(&workspaces_root).map_err(WorkspaceLeaseDependencyError::Io)?;
    let record_path = records_root.join(format!("{}.json", request.lease_id));
    if record_path.exists() {
        return reconcile_existing(&request, &source_root, &lease_root, &record_path);
    }

    reject_partial_materialization(&workspaces_root, &request.lease_id)?;
    reject_partial_materialization(&records_root, &request.lease_id)?;
    let source_snapshot_hash = bounded_tree_hash(
        &source_root,
        request.maximum_files,
        request.maximum_bytes,
        request.maximum_depth,
        &request.excluded_names,
    )?;
    let (
        effective_root,
        ownership,
        materialized_snapshot_hash,
        branch_name,
        base_revision,
        materialized_revision,
    ) = match request.mode {
        DependencyWorkspaceLeaseMode::SharedReadOnly => (
            source_root.clone(),
            DependencyWorkspaceOwnership::BorrowedReadOnly,
            source_snapshot_hash.clone(),
            None,
            None,
            None,
        ),
        DependencyWorkspaceLeaseMode::IsolatedCopy => {
            let target = workspaces_root.join(&request.lease_id);
            ensure_target_absent(&target)?;
            let temporary =
                workspaces_root.join(format!(".{}.{}.tmp", request.lease_id, Uuid::now_v7()));
            bounded_copy_tree(
                &source_root,
                &temporary,
                request.maximum_files,
                request.maximum_bytes,
                request.maximum_depth,
                &request.excluded_names,
            )?;
            fs::rename(&temporary, &target).map_err(WorkspaceLeaseDependencyError::Io)?;
            sync_directory(&workspaces_root)
                .map_err(|_| WorkspaceLeaseDependencyError::AmbiguousMaterialization)?;
            let effective_root = target
                .canonicalize()
                .map_err(WorkspaceLeaseDependencyError::Io)?;
            ensure_within(&effective_root, &lease_root)?;
            let hash = bounded_tree_hash(
                &effective_root,
                request.maximum_files,
                request.maximum_bytes,
                request.maximum_depth,
                &request.excluded_names,
            )?;
            if hash != source_snapshot_hash {
                return Err(WorkspaceLeaseDependencyError::MaterializationMismatch);
            }
            (
                effective_root,
                DependencyWorkspaceOwnership::RuntimeOwnedCopy,
                hash,
                None,
                None,
                None,
            )
        }
        DependencyWorkspaceLeaseMode::BranchWorkspace => {
            let merge_policy = request
                .merge_policy
                .as_deref()
                .ok_or(WorkspaceLeaseDependencyError::MergePolicyRequired)?;
            validate_label(merge_policy)?;
            let target = workspaces_root.join(&request.lease_id);
            ensure_target_absent(&target)?;
            let branch_name = format!("agentmod/{}", request.lease_id);
            let base_revision = git_revision(&source_root)?;
            let target_argument = git_cli_path(&target)?;
            let status = Command::new("git")
                .current_dir(&source_root)
                .args([
                    "worktree",
                    "add",
                    "-b",
                    branch_name.as_str(),
                    target_argument.as_str(),
                    "HEAD",
                ])
                .status()
                .map_err(WorkspaceLeaseDependencyError::Io)?;
            if !status.success() {
                // `git worktree add -b` can create the branch before failing
                // to finish the worktree. Without a durable receipt that is
                // an ambiguous external effect and must never be retried.
                return Err(WorkspaceLeaseDependencyError::AmbiguousMaterialization);
            }
            sync_directory(&workspaces_root)
                .map_err(|_| WorkspaceLeaseDependencyError::AmbiguousMaterialization)?;
            let effective_root = target
                .canonicalize()
                .map_err(WorkspaceLeaseDependencyError::Io)?;
            let hash = bounded_tree_hash(
                &effective_root,
                request.maximum_files,
                request.maximum_bytes,
                request.maximum_depth,
                &request.excluded_names,
            )?;
            let materialized_revision = git_revision(&effective_root)?;
            (
                effective_root,
                DependencyWorkspaceOwnership::RuntimeOwnedBranch,
                hash,
                Some(branch_name),
                Some(base_revision),
                Some(materialized_revision),
            )
        }
    };
    ensure_descendant_or_equal(&effective_root, &lease_root, &source_root)?;
    let record = DependencyWorkspaceLeaseRecord {
        record_version: RECORD_VERSION,
        lease_id: request.lease_id,
        contract_hash: request.contract_hash,
        source_root,
        effective_root,
        mode: request.mode,
        ownership,
        owner: request.owner,
        merge_policy: request.merge_policy,
        source_snapshot_hash,
        materialized_snapshot_hash,
        branch_name,
        maximum_depth: request.maximum_depth,
        excluded_names: request.excluded_names,
        base_revision,
        materialized_revision,
    };
    persist_record(&record_path, &record)?;
    Ok(record)
}

fn git_cli_path(path: &Path) -> Result<String, WorkspaceLeaseDependencyError> {
    let path = path
        .to_str()
        .ok_or(WorkspaceLeaseDependencyError::InvalidPathEncoding)?;
    #[cfg(windows)]
    {
        if let Some(network) = path.strip_prefix(r"\\?\UNC\") {
            return Ok(format!(r"\\{network}"));
        }
        if let Some(local) = path.strip_prefix(r"\\?\") {
            return Ok(local.to_owned());
        }
    }
    Ok(path.to_owned())
}

fn reconcile_existing(
    request: &DependencyEnsureWorkspaceLeaseRequest,
    source_root: &Path,
    lease_root: &Path,
    record_path: &Path,
) -> Result<DependencyWorkspaceLeaseRecord, WorkspaceLeaseDependencyError> {
    let bytes = fs::read(record_path).map_err(WorkspaceLeaseDependencyError::Io)?;
    let record: DependencyWorkspaceLeaseRecord =
        serde_json::from_slice(&bytes).map_err(WorkspaceLeaseDependencyError::Encoding)?;
    if record.record_version != RECORD_VERSION
        || record.lease_id != request.lease_id
        || record.contract_hash != request.contract_hash
        || record.source_root != source_root
        || record.mode != request.mode
        || record.owner != request.owner
        || record.merge_policy != request.merge_policy
        || record.maximum_depth != request.maximum_depth
        || record.excluded_names != request.excluded_names
    {
        return Err(WorkspaceLeaseDependencyError::RecoveryMismatch);
    }
    ensure_descendant_or_equal(&record.effective_root, lease_root, source_root)?;
    if !record.effective_root.is_dir() {
        return Err(WorkspaceLeaseDependencyError::MissingMaterialization);
    }
    match record.ownership {
        DependencyWorkspaceOwnership::BorrowedReadOnly => {
            if record.effective_root != source_root {
                return Err(WorkspaceLeaseDependencyError::RecoveryMismatch);
            }
            let current = bounded_tree_hash(
                source_root,
                request.maximum_files,
                request.maximum_bytes,
                request.maximum_depth,
                &request.excluded_names,
            )?;
            if current != record.source_snapshot_hash {
                return Err(WorkspaceLeaseDependencyError::SourceSnapshotChanged);
            }
        }
        DependencyWorkspaceOwnership::RuntimeOwnedCopy
        | DependencyWorkspaceOwnership::RuntimeOwnedBranch => {
            let current = bounded_tree_hash(
                &record.effective_root,
                request.maximum_files,
                request.maximum_bytes,
                request.maximum_depth,
                &request.excluded_names,
            )?;
            // Child work is expected to evolve after creation. The immutable
            // receipt binds the initial snapshot; recovery validates exact
            // ownership/root identity and the existence of that workspace.
            if current.is_empty() || record.materialized_snapshot_hash.is_empty() {
                return Err(WorkspaceLeaseDependencyError::RecoveryMismatch);
            }
        }
    }
    Ok(record)
}

fn validate_request(
    request: &DependencyEnsureWorkspaceLeaseRequest,
) -> Result<(), WorkspaceLeaseDependencyError> {
    if request.lease_root.as_os_str().is_empty()
        || request.source_workspace.as_os_str().is_empty()
        || request.contract_hash.len() != 64
        || request.owner.trim().is_empty()
        || request.owner.len() > 1024
        || request.maximum_files == 0
        || request.maximum_bytes == 0
        || request.maximum_bytes > 16 * 1024 * 1024 * 1024
        || request.maximum_depth == 0
        || request.maximum_depth > 128
        || request.excluded_names.is_empty()
        || request
            .excluded_names
            .iter()
            .any(|name| invalid_component(name))
    {
        return Err(WorkspaceLeaseDependencyError::InvalidRequest);
    }
    validate_lease_id(&request.lease_id)?;
    match request.mode {
        DependencyWorkspaceLeaseMode::BranchWorkspace => {
            let policy = request
                .merge_policy
                .as_deref()
                .ok_or(WorkspaceLeaseDependencyError::MergePolicyRequired)?;
            validate_label(policy)
        }
        DependencyWorkspaceLeaseMode::SharedReadOnly
        | DependencyWorkspaceLeaseMode::IsolatedCopy => {
            if request.merge_policy.is_some() {
                Err(WorkspaceLeaseDependencyError::UnexpectedMergePolicy)
            } else {
                Ok(())
            }
        }
    }
}

fn validate_lease_id(value: &str) -> Result<(), WorkspaceLeaseDependencyError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Err(WorkspaceLeaseDependencyError::InvalidLeaseIdentity)
    } else {
        Ok(())
    }
}

fn invalid_component(value: &str) -> bool {
    let literal = value.strip_suffix('*').unwrap_or(value);
    value.trim().is_empty()
        || literal.is_empty()
        || value.len() > 255
        || matches!(literal, "." | "..")
        || literal.contains(['/', '\\', '*'])
        || value.chars().any(char::is_control)
}

fn validate_label(value: &str) -> Result<(), WorkspaceLeaseDependencyError> {
    if value.trim().is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        Err(WorkspaceLeaseDependencyError::InvalidRequest)
    } else {
        Ok(())
    }
}

fn ensure_target_absent(target: &Path) -> Result<(), WorkspaceLeaseDependencyError> {
    if target.exists() {
        Err(WorkspaceLeaseDependencyError::AmbiguousMaterialization)
    } else {
        Ok(())
    }
}

fn reject_partial_materialization(
    root: &Path,
    lease_id: &str,
) -> Result<(), WorkspaceLeaseDependencyError> {
    let prefix = format!(".{lease_id}.");
    for entry in fs::read_dir(root)
        .map_err(WorkspaceLeaseDependencyError::Io)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(WorkspaceLeaseDependencyError::Io)?
    {
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or(WorkspaceLeaseDependencyError::InvalidPathEncoding)?;
        if name.starts_with(&prefix)
            && Path::new(name)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("tmp"))
        {
            return Err(WorkspaceLeaseDependencyError::AmbiguousMaterialization);
        }
    }
    Ok(())
}

fn ensure_descendant_or_equal(
    effective_root: &Path,
    lease_root: &Path,
    source_root: &Path,
) -> Result<(), WorkspaceLeaseDependencyError> {
    if effective_root == source_root || effective_root.starts_with(lease_root) {
        Ok(())
    } else {
        Err(WorkspaceLeaseDependencyError::InvalidEffectiveRoot)
    }
}

fn bounded_copy_tree(
    source: &Path,
    destination: &Path,
    maximum_files: u64,
    maximum_bytes: u64,
    maximum_depth: u32,
    excluded_names: &BTreeSet<String>,
) -> Result<(), WorkspaceLeaseDependencyError> {
    fs::create_dir(destination).map_err(WorkspaceLeaseDependencyError::Io)?;
    let mut counters = TreeCounters::default();
    copy_directory(
        source,
        source,
        destination,
        maximum_files,
        maximum_bytes,
        maximum_depth,
        excluded_names,
        0,
        &mut counters,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "bounded recursive traversal carries explicit immutable limits and mutable counters"
)]
fn copy_directory(
    source_root: &Path,
    source: &Path,
    destination: &Path,
    maximum_files: u64,
    maximum_bytes: u64,
    maximum_depth: u32,
    excluded_names: &BTreeSet<String>,
    depth: u32,
    counters: &mut TreeCounters,
) -> Result<(), WorkspaceLeaseDependencyError> {
    let mut entries = fs::read_dir(source)
        .map_err(WorkspaceLeaseDependencyError::Io)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(WorkspaceLeaseDependencyError::Io)?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let component = entry
            .file_name()
            .to_str()
            .ok_or(WorkspaceLeaseDependencyError::InvalidPathEncoding)?
            .to_owned();
        if is_excluded(&component, excluded_names) {
            continue;
        }
        let file_type = entry
            .file_type()
            .map_err(WorkspaceLeaseDependencyError::Io)?;
        if file_type.is_symlink() {
            return Err(WorkspaceLeaseDependencyError::SymlinkProhibited);
        }
        let source_entry = entry
            .path()
            .canonicalize()
            .map_err(WorkspaceLeaseDependencyError::Io)?;
        ensure_within(&source_entry, source_root)?;
        let target = destination.join(entry.file_name());
        if file_type.is_dir() {
            let next_depth = depth
                .checked_add(1)
                .ok_or(WorkspaceLeaseDependencyError::BoundsExceeded)?;
            if next_depth > maximum_depth {
                return Err(WorkspaceLeaseDependencyError::BoundsExceeded);
            }
            fs::create_dir(&target).map_err(WorkspaceLeaseDependencyError::Io)?;
            copy_directory(
                source_root,
                &source_entry,
                &target,
                maximum_files,
                maximum_bytes,
                maximum_depth,
                excluded_names,
                next_depth,
                counters,
            )?;
        } else if file_type.is_file() {
            counters.add_file(
                entry
                    .metadata()
                    .map_err(WorkspaceLeaseDependencyError::Io)?
                    .len(),
                maximum_files,
                maximum_bytes,
            )?;
            fs::copy(source_entry, target).map_err(WorkspaceLeaseDependencyError::Io)?;
        } else {
            return Err(WorkspaceLeaseDependencyError::UnsupportedFileType);
        }
    }
    Ok(())
}

fn bounded_tree_hash(
    root: &Path,
    maximum_files: u64,
    maximum_bytes: u64,
    maximum_depth: u32,
    excluded_names: &BTreeSet<String>,
) -> Result<String, WorkspaceLeaseDependencyError> {
    let mut hasher = blake3::Hasher::new();
    let mut counters = TreeCounters::default();
    hash_directory(
        root,
        root,
        maximum_files,
        maximum_bytes,
        maximum_depth,
        excluded_names,
        0,
        &mut counters,
        &mut hasher,
    )?;
    Ok(hasher.finalize().to_hex().to_string())
}

#[allow(
    clippy::too_many_arguments,
    reason = "bounded recursive hashing carries explicit immutable limits and mutable digest state"
)]
fn hash_directory(
    root: &Path,
    directory: &Path,
    maximum_files: u64,
    maximum_bytes: u64,
    maximum_depth: u32,
    excluded_names: &BTreeSet<String>,
    depth: u32,
    counters: &mut TreeCounters,
    hasher: &mut blake3::Hasher,
) -> Result<(), WorkspaceLeaseDependencyError> {
    let mut entries = fs::read_dir(directory)
        .map_err(WorkspaceLeaseDependencyError::Io)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(WorkspaceLeaseDependencyError::Io)?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let component = entry
            .file_name()
            .to_str()
            .ok_or(WorkspaceLeaseDependencyError::InvalidPathEncoding)?
            .to_owned();
        if is_excluded(&component, excluded_names) {
            continue;
        }
        let file_type = entry
            .file_type()
            .map_err(WorkspaceLeaseDependencyError::Io)?;
        if file_type.is_symlink() {
            return Err(WorkspaceLeaseDependencyError::SymlinkProhibited);
        }
        let path = entry
            .path()
            .canonicalize()
            .map_err(WorkspaceLeaseDependencyError::Io)?;
        ensure_within(&path, root)?;
        let relative = path
            .strip_prefix(root)
            .map_err(|_| WorkspaceLeaseDependencyError::InvalidEffectiveRoot)?;
        let relative = relative
            .to_str()
            .ok_or(WorkspaceLeaseDependencyError::InvalidPathEncoding)?;
        if file_type.is_dir() {
            let next_depth = depth
                .checked_add(1)
                .ok_or(WorkspaceLeaseDependencyError::BoundsExceeded)?;
            if next_depth > maximum_depth {
                return Err(WorkspaceLeaseDependencyError::BoundsExceeded);
            }
            hasher.update(b"d\0");
            hasher.update(relative.as_bytes());
            hasher.update(b"\0");
            hash_directory(
                root,
                &path,
                maximum_files,
                maximum_bytes,
                maximum_depth,
                excluded_names,
                next_depth,
                counters,
                hasher,
            )?;
        } else if file_type.is_file() {
            let length = entry
                .metadata()
                .map_err(WorkspaceLeaseDependencyError::Io)?
                .len();
            counters.add_file(length, maximum_files, maximum_bytes)?;
            hasher.update(b"f\0");
            hasher.update(relative.as_bytes());
            hasher.update(b"\0");
            hasher.update(&length.to_le_bytes());
            let mut file = File::open(&path).map_err(WorkspaceLeaseDependencyError::Io)?;
            let mut buffer = vec![0_u8; COPY_BUFFER_BYTES].into_boxed_slice();
            loop {
                let read = file
                    .read(&mut buffer)
                    .map_err(WorkspaceLeaseDependencyError::Io)?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
            }
        } else {
            return Err(WorkspaceLeaseDependencyError::UnsupportedFileType);
        }
    }
    Ok(())
}

fn ensure_within(path: &Path, root: &Path) -> Result<(), WorkspaceLeaseDependencyError> {
    if path.starts_with(root) {
        Ok(())
    } else {
        Err(WorkspaceLeaseDependencyError::PathTraversal)
    }
}

fn is_excluded(component: &str, rules: &BTreeSet<String>) -> bool {
    #[cfg(windows)]
    let component = component.to_lowercase();
    #[cfg(not(windows))]
    let component = component.to_owned();
    rules.iter().any(|rule| {
        #[cfg(windows)]
        let rule = rule.to_lowercase();
        #[cfg(not(windows))]
        let rule = rule.to_owned();
        rule.strip_suffix('*')
            .map_or(component == rule, |prefix| component.starts_with(prefix))
    })
}

fn git_revision(workspace: &Path) -> Result<String, WorkspaceLeaseDependencyError> {
    let output = Command::new("git")
        .current_dir(workspace)
        .args(["rev-parse", "--verify", "HEAD"])
        .output()
        .map_err(WorkspaceLeaseDependencyError::Io)?;
    if !output.status.success() {
        return Err(WorkspaceLeaseDependencyError::GitMaterialization);
    }
    let revision = String::from_utf8(output.stdout)
        .map_err(|_| WorkspaceLeaseDependencyError::InvalidPathEncoding)?
        .trim()
        .to_owned();
    if !(40..=64).contains(&revision.len())
        || !revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(WorkspaceLeaseDependencyError::GitMaterialization);
    }
    Ok(revision)
}

#[derive(Default)]
struct TreeCounters {
    files: u64,
    bytes: u64,
}

impl TreeCounters {
    fn add_file(
        &mut self,
        bytes: u64,
        maximum_files: u64,
        maximum_bytes: u64,
    ) -> Result<(), WorkspaceLeaseDependencyError> {
        self.files = self
            .files
            .checked_add(1)
            .ok_or(WorkspaceLeaseDependencyError::BoundsExceeded)?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or(WorkspaceLeaseDependencyError::BoundsExceeded)?;
        if self.files > maximum_files || self.bytes > maximum_bytes {
            Err(WorkspaceLeaseDependencyError::BoundsExceeded)
        } else {
            Ok(())
        }
    }
}

fn persist_record(
    path: &Path,
    record: &DependencyWorkspaceLeaseRecord,
) -> Result<(), WorkspaceLeaseDependencyError> {
    let parent = path
        .parent()
        .ok_or(WorkspaceLeaseDependencyError::InvalidEffectiveRoot)?;
    let temporary = parent.join(format!(".{}.{}.tmp", record.lease_id, Uuid::now_v7()));
    let bytes = serde_json::to_vec(record).map_err(WorkspaceLeaseDependencyError::Encoding)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(WorkspaceLeaseDependencyError::Io)?;
    file.write_all(&bytes)
        .map_err(WorkspaceLeaseDependencyError::Io)?;
    file.sync_all().map_err(WorkspaceLeaseDependencyError::Io)?;
    fs::rename(&temporary, path).map_err(WorkspaceLeaseDependencyError::Io)?;
    sync_directory(parent).map_err(|_| WorkspaceLeaseDependencyError::AmbiguousMaterialization)
}

fn persist_binding(
    path: &Path,
    binding: &DependencyWorkspaceSessionBinding,
) -> Result<(), WorkspaceLeaseDependencyError> {
    let parent = path
        .parent()
        .ok_or(WorkspaceLeaseDependencyError::InvalidEffectiveRoot)?;
    let temporary = parent.join(format!(".{}.{}.tmp", binding.session_id, Uuid::now_v7()));
    let bytes = serde_json::to_vec(binding).map_err(WorkspaceLeaseDependencyError::Encoding)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(WorkspaceLeaseDependencyError::Io)?;
    file.write_all(&bytes)
        .map_err(WorkspaceLeaseDependencyError::Io)?;
    file.sync_all().map_err(WorkspaceLeaseDependencyError::Io)?;
    fs::rename(&temporary, path).map_err(WorkspaceLeaseDependencyError::Io)?;
    sync_directory(parent).map_err(|_| WorkspaceLeaseDependencyError::AmbiguousMaterialization)
}

#[cfg(windows)]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the platform-specific helper preserves the fallible cross-platform call contract"
)]
fn sync_directory(directory: &Path) -> Result<(), std::io::Error> {
    // Rust's safe standard-library API cannot flush a Windows directory
    // handle (`unsafe_code` is forbidden workspace-wide). File contents
    // are flushed before rename, and receipt loss is recovered as an
    // ambiguous existing-target state rather than by deletion/retry.
    let _ = directory;
    Ok(())
}

#[cfg(not(windows))]
fn sync_directory(directory: &Path) -> Result<(), std::io::Error> {
    File::open(directory)?.sync_all()
}

/// Workspace lease dependency failure.
#[derive(Debug, Error)]
pub enum WorkspaceLeaseDependencyError {
    /// Invalid or unbounded request.
    #[error("workspace lease request is invalid")]
    InvalidRequest,
    /// Stable lease identity is unsafe for path construction.
    #[error("workspace lease identity is invalid")]
    InvalidLeaseIdentity,
    /// Source workspace is absent or not a directory.
    #[error("workspace lease source is invalid")]
    InvalidSource,
    /// Filesystem path cannot be represented in the bounded canonical record.
    #[error("workspace path encoding is invalid")]
    InvalidPathEncoding,
    /// Effective root escaped the selected source/lease roots.
    #[error("workspace lease effective root is invalid")]
    InvalidEffectiveRoot,
    /// A prior uncommitted materialization may exist.
    #[error("workspace materialization is ambiguous")]
    AmbiguousMaterialization,
    /// Existing immutable receipt differs from the request.
    #[error("workspace lease recovery identity differs")]
    RecoveryMismatch,
    /// Exact persisted materialization is absent.
    #[error("workspace lease materialization is missing")]
    MissingMaterialization,
    /// Borrowed read-only source changed after the immutable lease was bound.
    #[error("borrowed workspace source snapshot changed")]
    SourceSnapshotChanged,
    /// Copy or worktree content failed initial verification.
    #[error("workspace lease materialization hash differs")]
    MaterializationMismatch,
    /// A branch workspace omitted an explicit merge policy.
    #[error("branch workspace requires an explicit merge policy")]
    MergePolicyRequired,
    /// A non-branch workspace supplied a merge policy.
    #[error("workspace merge policy is not allowed for this mode")]
    UnexpectedMergePolicy,
    /// Symbolic links are excluded from bounded workspace materialization.
    #[error("workspace symbolic links are prohibited")]
    SymlinkProhibited,
    /// Special filesystem entries are excluded.
    #[error("workspace file type is unsupported")]
    UnsupportedFileType,
    /// A resolved filesystem entry escaped its declared root.
    #[error("workspace path traversal is prohibited")]
    PathTraversal,
    /// Bounded file or byte limit was exceeded.
    #[error("workspace materialization bound was exceeded")]
    BoundsExceeded,
    /// Git worktree creation failed.
    #[error("Git workspace materialization failed")]
    GitMaterialization,
    /// Filesystem operation failed.
    #[error("workspace lease filesystem operation failed: {0}")]
    Io(#[source] std::io::Error),
    /// Canonical receipt encoding failed.
    #[error("workspace lease receipt encoding failed: {0}")]
    Encoding(#[source] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn request(root: &Path, source: &Path) -> DependencyEnsureWorkspaceLeaseRequest {
        DependencyEnsureWorkspaceLeaseRequest {
            lease_root: root.to_owned(),
            source_workspace: source.to_owned(),
            lease_id: String::from("lease-0123456789abcdef"),
            contract_hash: "a".repeat(64),
            mode: DependencyWorkspaceLeaseMode::SharedReadOnly,
            owner: String::from("session/action/task"),
            merge_policy: None,
            maximum_files: 8,
            maximum_bytes: 1024,
            maximum_depth: 8,
            excluded_names: [String::from(".git"), String::from(".env*")]
                .into_iter()
                .collect(),
        }
    }

    #[test]
    fn shared_read_only_receipt_reconciles_exact_contract() {
        let temporary = tempdir().expect("temporary");
        let source = temporary.path().join("source");
        fs::create_dir(&source).expect("source");
        fs::write(source.join("input.txt"), b"input").expect("input");
        let lease_root = temporary.path().join("leases");
        let created = ensure_workspace_lease(request(&lease_root, &source)).expect("create");
        assert_eq!(
            created.ownership,
            DependencyWorkspaceOwnership::BorrowedReadOnly
        );
        let recovered = ensure_workspace_lease(request(&lease_root, &source)).expect("recover");
        assert_eq!(created, recovered);
    }

    #[test]
    fn isolated_copy_is_bounded_and_recovery_does_not_overwrite_child_work() {
        let temporary = tempdir().expect("temporary");
        let source = temporary.path().join("source");
        fs::create_dir(&source).expect("source");
        fs::write(source.join("input.txt"), b"input").expect("input");
        let lease_root = temporary.path().join("leases");
        let mut command = request(&lease_root, &source);
        command.mode = DependencyWorkspaceLeaseMode::IsolatedCopy;
        let created = ensure_workspace_lease(command.clone()).expect("create");
        assert_ne!(created.effective_root, created.source_root);
        fs::write(created.effective_root.join("worker.txt"), b"result").expect("worker");
        let recovered = ensure_workspace_lease(command).expect("recover");
        assert_eq!(created, recovered);
        assert_eq!(
            fs::read(recovered.effective_root.join("worker.txt")).expect("worker"),
            b"result"
        );
        assert!(!source.join("worker.txt").exists());
    }

    #[test]
    fn branch_workspace_isolated_changes_require_explicit_manual_review() {
        let temporary = tempdir().expect("temporary");
        let source = temporary.path().join("source");
        fs::create_dir(&source).expect("source");
        fs::write(source.join("worker.txt"), b"parent\n").expect("input");
        for arguments in [
            vec!["init"],
            vec!["config", "user.email", "agentmod@example.invalid"],
            vec!["config", "user.name", "AgentMod Fixture"],
            vec!["add", "worker.txt"],
            vec!["commit", "-m", "fixture base"],
        ] {
            let status = Command::new("git")
                .current_dir(&source)
                .args(arguments)
                .status()
                .expect("git fixture command");
            assert!(status.success(), "git fixture initialization failed");
        }
        let base_branch = Command::new("git")
            .current_dir(&source)
            .args(["branch", "--show-current"])
            .output()
            .expect("base branch");
        assert!(base_branch.status.success());

        let lease_root = temporary.path().join("leases");
        let mut command = request(&lease_root, &source);
        command.mode = DependencyWorkspaceLeaseMode::BranchWorkspace;
        command.merge_policy = Some(String::from("manual_review"));
        let created = ensure_workspace_lease(command.clone()).expect("create branch workspace");
        assert_eq!(
            created.ownership,
            DependencyWorkspaceOwnership::RuntimeOwnedBranch
        );
        assert_ne!(created.effective_root, created.source_root);
        assert!(
            created
                .branch_name
                .as_deref()
                .is_some_and(|name| { name.starts_with("agentmod/lease-0123456789abcdef") })
        );
        assert_eq!(created.base_revision, created.materialized_revision);

        fs::write(created.effective_root.join("worker.txt"), b"child\n").expect("child-only edit");
        assert_eq!(
            fs::read(source.join("worker.txt")).expect("parent"),
            b"parent\n"
        );
        let recovered = ensure_workspace_lease(command).expect("recover branch workspace");
        assert_eq!(recovered, created);
        assert_eq!(
            fs::read(recovered.effective_root.join("worker.txt")).expect("child"),
            b"child\n"
        );
        let current_branch = Command::new("git")
            .current_dir(&source)
            .args(["branch", "--show-current"])
            .output()
            .expect("current branch");
        assert!(current_branch.status.success());
        assert_eq!(current_branch.stdout, base_branch.stdout);
    }

    #[test]
    fn changed_contract_fails_closed() {
        let temporary = tempdir().expect("temporary");
        let source = temporary.path().join("source");
        fs::create_dir(&source).expect("source");
        fs::write(source.join("input.txt"), b"input").expect("input");
        let lease_root = temporary.path().join("leases");
        ensure_workspace_lease(request(&lease_root, &source)).expect("create");
        let mut changed = request(&lease_root, &source);
        changed.owner = String::from("substituted");
        assert!(matches!(
            ensure_workspace_lease(changed),
            Err(WorkspaceLeaseDependencyError::RecoveryMismatch)
        ));
    }

    #[test]
    fn borrowed_source_drift_fails_closed_before_later_effects() {
        let temporary = tempdir().expect("temporary");
        let source = temporary.path().join("source");
        fs::create_dir(&source).expect("source");
        fs::write(source.join("input.txt"), b"input").expect("input");
        let lease_root = temporary.path().join("leases");
        let command = request(&lease_root, &source);
        ensure_workspace_lease(command.clone()).expect("create");
        fs::write(source.join("input.txt"), b"changed").expect("change");
        assert!(matches!(
            ensure_workspace_lease(command),
            Err(WorkspaceLeaseDependencyError::SourceSnapshotChanged)
        ));
    }

    #[test]
    fn isolated_copy_excludes_declared_secret_and_runtime_state_patterns() {
        let temporary = tempdir().expect("temporary");
        let source = temporary.path().join("source");
        fs::create_dir(&source).expect("source");
        fs::write(source.join("input.txt"), b"input").expect("input");
        fs::write(source.join(".env.local"), b"secret").expect("secret");
        fs::create_dir(source.join(".git")).expect("git");
        fs::write(source.join(".git").join("config"), b"runtime").expect("runtime");
        let lease_root = temporary.path().join("leases");
        let mut command = request(&lease_root, &source);
        command.mode = DependencyWorkspaceLeaseMode::IsolatedCopy;
        let created = ensure_workspace_lease(command).expect("create");
        assert!(created.effective_root.join("input.txt").is_file());
        assert!(!created.effective_root.join(".env.local").exists());
        assert!(!created.effective_root.join(".git").exists());
    }

    #[test]
    fn receipt_loss_with_existing_owned_target_is_ambiguous_and_untouched() {
        let temporary = tempdir().expect("temporary");
        let source = temporary.path().join("source");
        fs::create_dir(&source).expect("source");
        fs::write(source.join("input.txt"), b"input").expect("input");
        let lease_root = temporary.path().join("leases");
        let target = lease_root.join("workspaces").join("lease-0123456789abcdef");
        fs::create_dir_all(&target).expect("partial target");
        fs::write(target.join("unowned.txt"), b"preserve").expect("unowned");
        let mut command = request(&lease_root, &source);
        command.mode = DependencyWorkspaceLeaseMode::IsolatedCopy;
        assert!(matches!(
            ensure_workspace_lease(command),
            Err(WorkspaceLeaseDependencyError::AmbiguousMaterialization)
        ));
        assert_eq!(
            fs::read(target.join("unowned.txt")).expect("preserved"),
            b"preserve"
        );
    }

    #[test]
    fn depth_bound_fails_before_owned_copy_is_committed() {
        let temporary = tempdir().expect("temporary");
        let source = temporary.path().join("source");
        fs::create_dir_all(source.join("one").join("two")).expect("source");
        fs::write(source.join("one").join("two").join("input.txt"), b"input").expect("input");
        let lease_root = temporary.path().join("leases");
        let mut command = request(&lease_root, &source);
        command.mode = DependencyWorkspaceLeaseMode::IsolatedCopy;
        command.maximum_depth = 1;
        assert!(matches!(
            ensure_workspace_lease(command),
            Err(WorkspaceLeaseDependencyError::BoundsExceeded)
        ));
    }

    #[test]
    fn symlink_or_windows_reparse_entry_is_rejected_when_supported() {
        let temporary = tempdir().expect("temporary");
        let source = temporary.path().join("source");
        let outside = temporary.path().join("outside.txt");
        fs::create_dir(&source).expect("source");
        fs::write(&outside, b"outside").expect("outside");
        let link = source.join("link.txt");
        #[cfg(unix)]
        let linked = std::os::unix::fs::symlink(&outside, &link);
        #[cfg(windows)]
        let linked = std::os::windows::fs::symlink_file(&outside, &link);
        if linked.is_err() {
            // Windows may deny symlink creation without Developer Mode. The
            // production path still checks the reparse/symlink file type.
            return;
        }
        let lease_root = temporary.path().join("leases");
        let mut command = request(&lease_root, &source);
        command.mode = DependencyWorkspaceLeaseMode::IsolatedCopy;
        assert!(matches!(
            ensure_workspace_lease(command),
            Err(WorkspaceLeaseDependencyError::SymlinkProhibited)
        ));
    }
}
