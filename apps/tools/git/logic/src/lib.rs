//! Git-host business validation and use cases.

use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use agentmod_git_host_data::{
    ChangeDataRecord, CheckpointDataRecord, ContentDataRecord, GitDataAuthorization, GitDataError,
    GitDataPort, RepositoryDataRecord, StatusDataRecord, WorktreeDataRecord,
};
use async_trait::async_trait;
use thiserror::Error;

/// Logic-owned repository selector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepositorySelection {
    /// Authorization.
    pub authorization: GitAuthorization,
    /// Workspace-contained path.
    pub path: PathBuf,
}

/// Logic-owned changed-file status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangedFile {
    /// Two-column status.
    pub status: String,
    /// Repository-relative path.
    pub path: PathBuf,
}

/// Logic-owned branch information.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchInfo {
    /// Current branch or detached `HEAD`.
    pub branch: String,
    /// Current commit.
    pub head: String,
    /// Configured upstream.
    pub upstream: Option<String>,
    /// Ahead count.
    pub ahead: Option<u64>,
    /// Behind count.
    pub behind: Option<u64>,
}

/// Logic-owned status result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepositoryStatus {
    /// Canonical root.
    pub repository_root: PathBuf,
    /// Branch information.
    pub branch: BranchInfo,
    /// Changed files.
    pub changes: Vec<ChangedFile>,
    /// Whether changes exist.
    pub dirty: bool,
}

/// Logic-owned bounded content.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundedContent {
    /// Inline bytes.
    pub inline: Vec<u8>,
    /// Total byte size.
    pub total_bytes: u64,
    /// Full host-owned artifact path.
    pub artifact: Option<PathBuf>,
    /// Whether inline content is incomplete.
    pub truncated: bool,
}

/// Logic-owned worktree creation command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateWorktreeCommand {
    /// Repository selection.
    pub repository: RepositorySelection,
    /// Workspace-relative or contained absolute destination.
    pub destination: PathBuf,
    /// Validated branch/tag/ref name.
    pub base: String,
}

/// Logic-owned worktree result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorktreeResult {
    /// Canonical worktree path.
    pub path: PathBuf,
    /// Resolved base.
    pub head: String,
}

/// Logic-owned worktree cleanup command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupWorktreeCommand {
    /// Repository selection.
    pub repository: RepositorySelection,
    /// Managed worktree path.
    pub destination: PathBuf,
}

/// Logic-owned checkpoint result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointResult {
    /// Stable checkpoint ID.
    pub checkpoint_id: String,
    /// Base commit.
    pub base_head: String,
    /// Immutable artifact directory.
    pub artifact_directory: PathBuf,
    /// Patch bytes.
    pub patch_bytes: u64,
    /// Untracked file count.
    pub untracked_files: u64,
}

/// Logic-owned restore command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreCheckpointCommand {
    /// Repository selection.
    pub repository: RepositorySelection,
    /// Stable checkpoint ID.
    pub checkpoint_id: String,
}

/// Git business configuration.
#[derive(Clone, Debug)]
pub struct GitLogicConfig {
    /// Workspace root.
    pub workspace_root: PathBuf,
}

/// Git business failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum GitLogicError {
    /// Requested path escapes the workspace.
    #[error("Git path escapes the configured workspace")]
    WorkspaceEscape,
    /// Path is empty or unsafe.
    #[error("invalid Git path")]
    InvalidPath,
    /// Git reference is unsafe.
    #[error("invalid Git reference")]
    InvalidRef,
    /// Checkpoint ID is malformed.
    #[error("invalid checkpoint ID")]
    InvalidCheckpointId,
    /// Data operation failed.
    #[error("Git data operation failed: {0}")]
    Data(String),
}

/// Git business interface exposed to service.
#[async_trait]
pub trait GitLogicPort: Send + Sync {
    /// Discovers and canonicalizes a repository.
    async fn discover(&self, selection: RepositorySelection) -> Result<PathBuf, GitLogicError>;
    /// Reads status, branch information, changed files, and dirty state.
    async fn status(
        &self,
        selection: RepositorySelection,
    ) -> Result<RepositoryStatus, GitLogicError>;
    /// Reads bounded diff.
    async fn diff(&self, selection: RepositorySelection) -> Result<BoundedContent, GitLogicError>;
    /// Exports a full patch artifact.
    async fn export_patch(
        &self,
        selection: RepositorySelection,
    ) -> Result<BoundedContent, GitLogicError>;
    /// Creates a detached independent worktree.
    async fn create_worktree(
        &self,
        command: CreateWorktreeCommand,
    ) -> Result<WorktreeResult, GitLogicError>;
    /// Removes a clean worktree.
    async fn cleanup_worktree(&self, command: CleanupWorktreeCommand) -> Result<(), GitLogicError>;
    /// Creates an artifact checkpoint without a commit.
    async fn create_checkpoint(
        &self,
        selection: RepositorySelection,
    ) -> Result<CheckpointResult, GitLogicError>;
    /// Restores only over a clean matching base.
    async fn restore_checkpoint(
        &self,
        command: RestoreCheckpointCommand,
    ) -> Result<CheckpointResult, GitLogicError>;
}

/// Git business implementation.
#[derive(Clone)]
pub struct GitLogic<D> {
    data: D,
    config: Arc<GitLogicConfig>,
}

impl<D> GitLogic<D> {
    /// Injects data and policy.
    #[must_use]
    pub fn new(data: D, config: GitLogicConfig) -> Self {
        Self {
            data,
            config: Arc::new(config),
        }
    }

    async fn repository(
        &self,
        selection: RepositorySelection,
    ) -> Result<RepositoryDataRecord, GitLogicError>
    where
        D: GitDataPort,
    {
        let path = resolve_contained(&self.config.workspace_root, selection.path)?;
        self.data
            .discover(map_authorization(selection.authorization), path)
            .await
            .map_err(map_error)
    }
}

fn map_authorization(value: GitAuthorization) -> GitDataAuthorization {
    GitDataAuthorization {
        owner_id: value.owner_id,
        session_id: value.session_id,
        call_id: value.call_id,
        action: value.action,
        normalized_digest: value.normalized_digest,
        grant: value.grant,
        canonical_operation: value.canonical_operation,
    }
}

#[async_trait]
impl<D> GitLogicPort for GitLogic<D>
where
    D: GitDataPort,
{
    async fn discover(&self, selection: RepositorySelection) -> Result<PathBuf, GitLogicError> {
        self.repository(selection).await.map(|record| record.root)
    }

    async fn status(
        &self,
        selection: RepositorySelection,
    ) -> Result<RepositoryStatus, GitLogicError> {
        let repository = self.repository(selection).await?;
        let root = repository.root.clone();
        let status = self.data.status(repository).await.map_err(map_error)?;
        Ok(map_status(root, status))
    }

    async fn diff(&self, selection: RepositorySelection) -> Result<BoundedContent, GitLogicError> {
        let repository = self.repository(selection).await?;
        self.data
            .diff(repository)
            .await
            .map(map_content)
            .map_err(map_error)
    }

    async fn export_patch(
        &self,
        selection: RepositorySelection,
    ) -> Result<BoundedContent, GitLogicError> {
        let repository = self.repository(selection).await?;
        self.data
            .export_patch(repository)
            .await
            .map(map_content)
            .map_err(map_error)
    }

    async fn create_worktree(
        &self,
        command: CreateWorktreeCommand,
    ) -> Result<WorktreeResult, GitLogicError> {
        validate_ref(&command.base)?;
        let destination = resolve_contained(&self.config.workspace_root, command.destination)?;
        let repository = self.repository(command.repository).await?;
        self.data
            .create_worktree(repository, destination, command.base)
            .await
            .map(map_worktree)
            .map_err(map_error)
    }

    async fn cleanup_worktree(&self, command: CleanupWorktreeCommand) -> Result<(), GitLogicError> {
        let destination = resolve_contained(&self.config.workspace_root, command.destination)?;
        let repository = self.repository(command.repository).await?;
        self.data
            .cleanup_worktree(repository, destination)
            .await
            .map_err(map_error)
    }

    async fn create_checkpoint(
        &self,
        selection: RepositorySelection,
    ) -> Result<CheckpointResult, GitLogicError> {
        let repository = self.repository(selection).await?;
        self.data
            .create_checkpoint(repository)
            .await
            .map(map_checkpoint)
            .map_err(map_error)
    }

    async fn restore_checkpoint(
        &self,
        command: RestoreCheckpointCommand,
    ) -> Result<CheckpointResult, GitLogicError> {
        validate_checkpoint_id(&command.checkpoint_id)?;
        let repository = self.repository(command.repository).await?;
        self.data
            .restore_checkpoint(repository, command.checkpoint_id)
            .await
            .map(map_checkpoint)
            .map_err(map_error)
    }
}

fn resolve_contained(workspace: &Path, requested: PathBuf) -> Result<PathBuf, GitLogicError> {
    if requested.as_os_str().is_empty() {
        return Err(GitLogicError::InvalidPath);
    }
    if requested.is_absolute() {
        return requested
            .starts_with(workspace)
            .then_some(requested)
            .ok_or(GitLogicError::WorkspaceEscape);
    }
    if requested.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(GitLogicError::WorkspaceEscape);
    }
    Ok(workspace.join(requested))
}

fn validate_ref(reference: &str) -> Result<(), GitLogicError> {
    if reference.is_empty()
        || reference.len() > 255
        || reference.starts_with('-')
        || reference.contains("..")
        || reference.contains("@{")
        || reference.contains("//")
        || reference.ends_with('/')
        || reference.ends_with('.')
        || Path::new(reference)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("lock"))
        || !reference
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._/-".contains(&byte))
    {
        Err(GitLogicError::InvalidRef)
    } else {
        Ok(())
    }
}

fn validate_checkpoint_id(value: &str) -> Result<(), GitLogicError> {
    if value.len() == 36
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
    {
        Ok(())
    } else {
        Err(GitLogicError::InvalidCheckpointId)
    }
}

fn map_status(root: PathBuf, record: StatusDataRecord) -> RepositoryStatus {
    let changes: Vec<_> = record.changes.into_iter().map(map_change).collect();
    RepositoryStatus {
        repository_root: root,
        branch: BranchInfo {
            branch: record.branch,
            head: record.head,
            upstream: record.upstream,
            ahead: record.ahead,
            behind: record.behind,
        },
        dirty: !changes.is_empty(),
        changes,
    }
}

fn map_change(record: ChangeDataRecord) -> ChangedFile {
    ChangedFile {
        status: record.status,
        path: record.path,
    }
}

fn map_content(record: ContentDataRecord) -> BoundedContent {
    let truncated = record.artifact.is_some() && record.total_bytes > record.inline.len() as u64;
    BoundedContent {
        inline: record.inline,
        total_bytes: record.total_bytes,
        artifact: record.artifact,
        truncated,
    }
}

fn map_worktree(record: WorktreeDataRecord) -> WorktreeResult {
    WorktreeResult {
        path: record.path,
        head: record.head,
    }
}

fn map_checkpoint(record: CheckpointDataRecord) -> CheckpointResult {
    CheckpointResult {
        checkpoint_id: record.checkpoint_id,
        base_head: record.base_head,
        artifact_directory: record.artifact_directory,
        patch_bytes: record.patch_bytes,
        untracked_files: record.untracked_files,
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err supplies ownership and logic redacts data-layer details"
)]
fn map_error(error: GitDataError) -> GitLogicError {
    GitLogicError::Data(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    #[derive(Clone)]
    struct MockData {
        discovers: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl GitDataPort for MockData {
        async fn discover(
            &self,
            _authorization: GitDataAuthorization,
            path: PathBuf,
        ) -> Result<RepositoryDataRecord, GitDataError> {
            self.discovers.fetch_add(1, Ordering::Relaxed);
            Ok(RepositoryDataRecord {
                root: path,
                authorization: _authorization,
            })
        }

        async fn status(
            &self,
            _repository: RepositoryDataRecord,
        ) -> Result<StatusDataRecord, GitDataError> {
            Ok(StatusDataRecord {
                branch: "main".to_owned(),
                head: "abc".to_owned(),
                upstream: None,
                ahead: None,
                behind: None,
                changes: Vec::new(),
            })
        }

        async fn diff(
            &self,
            _repository: RepositoryDataRecord,
        ) -> Result<ContentDataRecord, GitDataError> {
            unreachable!()
        }

        async fn export_patch(
            &self,
            _repository: RepositoryDataRecord,
        ) -> Result<ContentDataRecord, GitDataError> {
            unreachable!()
        }

        async fn create_worktree(
            &self,
            _repository: RepositoryDataRecord,
            _destination: PathBuf,
            _base: String,
        ) -> Result<WorktreeDataRecord, GitDataError> {
            unreachable!()
        }

        async fn cleanup_worktree(
            &self,
            _repository: RepositoryDataRecord,
            _destination: PathBuf,
        ) -> Result<(), GitDataError> {
            unreachable!()
        }

        async fn create_checkpoint(
            &self,
            _repository: RepositoryDataRecord,
        ) -> Result<CheckpointDataRecord, GitDataError> {
            unreachable!()
        }

        async fn restore_checkpoint(
            &self,
            _repository: RepositoryDataRecord,
            _checkpoint_id: String,
        ) -> Result<CheckpointDataRecord, GitDataError> {
            unreachable!()
        }
    }

    fn logic(data: MockData) -> GitLogic<MockData> {
        GitLogic::new(
            data,
            GitLogicConfig {
                workspace_root: PathBuf::from("workspace"),
            },
        )
    }

    fn authorization() -> GitAuthorization {
        GitAuthorization {
            owner_id: "owner".to_owned(),
            session_id: "session".to_owned(),
            call_id: "call".to_owned(),
            action: "git.status".to_owned(),
            normalized_digest: "0".repeat(64),
            grant: "grant".to_owned(),
            canonical_operation: Vec::new(),
        }
    }

    #[tokio::test]
    async fn maps_clean_status_and_dirty_semantics() {
        let result = logic(MockData {
            discovers: Arc::new(AtomicUsize::new(0)),
        })
        .status(RepositorySelection {
            path: PathBuf::from("repo"),
            authorization: authorization(),
        })
        .await
        .expect("status");
        assert_eq!(result.branch.branch, "main");
        assert!(!result.dirty);
    }

    #[tokio::test]
    async fn rejects_escape_before_data_access() {
        let calls = Arc::new(AtomicUsize::new(0));
        let result = logic(MockData {
            discovers: Arc::clone(&calls),
        })
        .discover(RepositorySelection {
            path: PathBuf::from("../outside"),
            authorization: authorization(),
        })
        .await;
        assert_eq!(result, Err(GitLogicError::WorkspaceEscape));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }
}
/// Logic-owned authorization context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitAuthorization {
    /// Owner.
    pub owner_id: String,
    /// Session.
    pub session_id: String,
    /// Call.
    pub call_id: String,
    /// Action.
    pub action: String,
    /// Digest.
    pub normalized_digest: String,
    /// Grant.
    pub grant: String,
    /// Canonical operation.
    pub canonical_operation: Vec<u8>,
}
