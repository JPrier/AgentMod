//! Business-facing Git datasets and dependency normalization.

use std::path::PathBuf;

use agentmod_git_host_dependency::{
    DependencyAuthorization, DependencyChange, DependencyCheckpoint, DependencyContent,
    DependencyRepository, DependencyStatus, DependencyWorktree, GitDependencyError,
    GitDependencyPort,
};
use async_trait::async_trait;
use thiserror::Error;

/// Data-owned authorization context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitDataAuthorization {
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

/// Data-owned repository record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepositoryDataRecord {
    /// Canonical repository root.
    pub root: PathBuf,
    /// Verified authorization context.
    pub authorization: GitDataAuthorization,
}

/// Data-owned changed-file record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeDataRecord {
    /// Two-column Git status.
    pub status: String,
    /// Repository-relative path.
    pub path: PathBuf,
}

/// Data-owned repository status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatusDataRecord {
    /// Branch or detached `HEAD`.
    pub branch: String,
    /// Commit object ID.
    pub head: String,
    /// Optional upstream.
    pub upstream: Option<String>,
    /// Ahead count.
    pub ahead: Option<u64>,
    /// Behind count.
    pub behind: Option<u64>,
    /// Changed paths.
    pub changes: Vec<ChangeDataRecord>,
}

/// Data-owned bounded content.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContentDataRecord {
    /// Inline prefix.
    pub inline: Vec<u8>,
    /// Total bytes.
    pub total_bytes: u64,
    /// Host artifact for full overflow/export.
    pub artifact: Option<PathBuf>,
}

/// Data-owned worktree record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorktreeDataRecord {
    /// Canonical worktree path.
    pub path: PathBuf,
    /// Resolved commit.
    pub head: String,
}

/// Data-owned checkpoint record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointDataRecord {
    /// Stable checkpoint ID.
    pub checkpoint_id: String,
    /// Base commit.
    pub base_head: String,
    /// Immutable artifact directory.
    pub artifact_directory: PathBuf,
    /// Patch bytes.
    pub patch_bytes: u64,
    /// Captured untracked files.
    pub untracked_files: u64,
}

/// Git data-layer failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum GitDataError {
    /// Dependency operation failed.
    #[error("Git dependency failed: {0}")]
    Dependency(String),
}

/// Git datasets exposed to logic.
#[async_trait]
pub trait GitDataPort: Send + Sync {
    /// Discovers a repository.
    async fn discover(
        &self,
        authorization: GitDataAuthorization,
        path: PathBuf,
    ) -> Result<RepositoryDataRecord, GitDataError>;
    /// Reads status.
    async fn status(
        &self,
        repository: RepositoryDataRecord,
    ) -> Result<StatusDataRecord, GitDataError>;
    /// Reads bounded diff.
    async fn diff(
        &self,
        repository: RepositoryDataRecord,
    ) -> Result<ContentDataRecord, GitDataError>;
    /// Exports patch.
    async fn export_patch(
        &self,
        repository: RepositoryDataRecord,
    ) -> Result<ContentDataRecord, GitDataError>;
    /// Creates worktree.
    async fn create_worktree(
        &self,
        repository: RepositoryDataRecord,
        destination: PathBuf,
        base: String,
    ) -> Result<WorktreeDataRecord, GitDataError>;
    /// Cleans up worktree.
    async fn cleanup_worktree(
        &self,
        repository: RepositoryDataRecord,
        destination: PathBuf,
    ) -> Result<(), GitDataError>;
    /// Creates checkpoint.
    async fn create_checkpoint(
        &self,
        repository: RepositoryDataRecord,
    ) -> Result<CheckpointDataRecord, GitDataError>;
    /// Restores checkpoint.
    async fn restore_checkpoint(
        &self,
        repository: RepositoryDataRecord,
        checkpoint_id: String,
    ) -> Result<CheckpointDataRecord, GitDataError>;
}

/// Git data implementation.
#[derive(Clone)]
pub struct GitData<D> {
    dependency: D,
}

impl<D> GitData<D> {
    /// Injects a Git dependency.
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

#[async_trait]
impl<D> GitDataPort for GitData<D>
where
    D: GitDependencyPort,
{
    async fn discover(
        &self,
        authorization: GitDataAuthorization,
        path: PathBuf,
    ) -> Result<RepositoryDataRecord, GitDataError> {
        self.dependency
            .discover(map_authorization(authorization), path)
            .await
            .map(map_repository)
            .map_err(map_error)
    }

    async fn status(
        &self,
        repository: RepositoryDataRecord,
    ) -> Result<StatusDataRecord, GitDataError> {
        self.dependency
            .status(map_repository_request(repository))
            .await
            .map(map_status)
            .map_err(map_error)
    }

    async fn diff(
        &self,
        repository: RepositoryDataRecord,
    ) -> Result<ContentDataRecord, GitDataError> {
        self.dependency
            .diff(map_repository_request(repository))
            .await
            .map(map_content)
            .map_err(map_error)
    }

    async fn export_patch(
        &self,
        repository: RepositoryDataRecord,
    ) -> Result<ContentDataRecord, GitDataError> {
        self.dependency
            .export_patch(map_repository_request(repository))
            .await
            .map(map_content)
            .map_err(map_error)
    }

    async fn create_worktree(
        &self,
        repository: RepositoryDataRecord,
        destination: PathBuf,
        base: String,
    ) -> Result<WorktreeDataRecord, GitDataError> {
        self.dependency
            .create_worktree(map_repository_request(repository), destination, base)
            .await
            .map(map_worktree)
            .map_err(map_error)
    }

    async fn cleanup_worktree(
        &self,
        repository: RepositoryDataRecord,
        destination: PathBuf,
    ) -> Result<(), GitDataError> {
        self.dependency
            .cleanup_worktree(map_repository_request(repository), destination)
            .await
            .map_err(map_error)
    }

    async fn create_checkpoint(
        &self,
        repository: RepositoryDataRecord,
    ) -> Result<CheckpointDataRecord, GitDataError> {
        self.dependency
            .create_checkpoint(map_repository_request(repository))
            .await
            .map(map_checkpoint)
            .map_err(map_error)
    }

    async fn restore_checkpoint(
        &self,
        repository: RepositoryDataRecord,
        checkpoint_id: String,
    ) -> Result<CheckpointDataRecord, GitDataError> {
        self.dependency
            .restore_checkpoint(map_repository_request(repository), checkpoint_id)
            .await
            .map(map_checkpoint)
            .map_err(map_error)
    }
}

fn map_authorization(value: GitDataAuthorization) -> DependencyAuthorization {
    DependencyAuthorization {
        owner_id: value.owner_id,
        session_id: value.session_id,
        call_id: value.call_id,
        action: value.action,
        normalized_digest: value.normalized_digest,
        grant: value.grant,
        canonical_operation: value.canonical_operation,
    }
}

fn map_repository(record: DependencyRepository) -> RepositoryDataRecord {
    RepositoryDataRecord {
        root: record.root,
        authorization: map_dependency_authorization(record.authorization),
    }
}

fn map_repository_request(record: RepositoryDataRecord) -> DependencyRepository {
    DependencyRepository {
        root: record.root,
        authorization: map_authorization(record.authorization),
    }
}

fn map_dependency_authorization(value: DependencyAuthorization) -> GitDataAuthorization {
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

fn map_status(record: DependencyStatus) -> StatusDataRecord {
    StatusDataRecord {
        branch: record.branch,
        head: record.head,
        upstream: record.upstream,
        ahead: record.ahead,
        behind: record.behind,
        changes: record.changes.into_iter().map(map_change).collect(),
    }
}

fn map_change(record: DependencyChange) -> ChangeDataRecord {
    ChangeDataRecord {
        status: record.status,
        path: record.path,
    }
}

fn map_content(record: DependencyContent) -> ContentDataRecord {
    ContentDataRecord {
        inline: record.inline,
        total_bytes: record.total_bytes,
        artifact: record.overflow_artifact,
    }
}

fn map_worktree(record: DependencyWorktree) -> WorktreeDataRecord {
    WorktreeDataRecord {
        path: record.path,
        head: record.head,
    }
}

fn map_checkpoint(record: DependencyCheckpoint) -> CheckpointDataRecord {
    CheckpointDataRecord {
        checkpoint_id: record.checkpoint_id,
        base_head: record.base_head,
        artifact_directory: record.artifact_directory,
        patch_bytes: record.patch_bytes,
        untracked_files: record.untracked_files,
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err supplies ownership and data redacts dependency details"
)]
fn map_error(error: GitDependencyError) -> GitDataError {
    GitDataError::Dependency(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct MockDependency {
        statuses: Mutex<Vec<DependencyRepository>>,
    }

    #[async_trait]
    impl GitDependencyPort for MockDependency {
        async fn discover(
            &self,
            _authorization: DependencyAuthorization,
            path: PathBuf,
        ) -> Result<DependencyRepository, GitDependencyError> {
            Ok(DependencyRepository {
                root: path,
                authorization: _authorization,
            })
        }

        async fn status(
            &self,
            repository: DependencyRepository,
        ) -> Result<DependencyStatus, GitDependencyError> {
            self.statuses.lock().expect("statuses").push(repository);
            Ok(DependencyStatus {
                branch: "main".to_owned(),
                head: "abc".to_owned(),
                upstream: None,
                ahead: None,
                behind: None,
                changes: vec![DependencyChange {
                    status: " M".to_owned(),
                    path: PathBuf::from("src/lib.rs"),
                }],
            })
        }

        async fn diff(
            &self,
            _repository: DependencyRepository,
        ) -> Result<DependencyContent, GitDependencyError> {
            unreachable!()
        }

        async fn export_patch(
            &self,
            _repository: DependencyRepository,
        ) -> Result<DependencyContent, GitDependencyError> {
            unreachable!()
        }

        async fn create_worktree(
            &self,
            _repository: DependencyRepository,
            _destination: PathBuf,
            _base: String,
        ) -> Result<DependencyWorktree, GitDependencyError> {
            unreachable!()
        }

        async fn cleanup_worktree(
            &self,
            _repository: DependencyRepository,
            _destination: PathBuf,
        ) -> Result<(), GitDependencyError> {
            unreachable!()
        }

        async fn create_checkpoint(
            &self,
            _repository: DependencyRepository,
        ) -> Result<DependencyCheckpoint, GitDependencyError> {
            unreachable!()
        }

        async fn restore_checkpoint(
            &self,
            _repository: DependencyRepository,
            _checkpoint_id: String,
        ) -> Result<DependencyCheckpoint, GitDependencyError> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn maps_status_across_data_boundary() {
        let data = GitData::new(MockDependency {
            statuses: Mutex::new(Vec::new()),
        });
        let status = data
            .status(RepositoryDataRecord {
                root: PathBuf::from("repo"),
                authorization: GitDataAuthorization {
                    owner_id: "owner".to_owned(),
                    session_id: "session".to_owned(),
                    call_id: "call".to_owned(),
                    action: "git.status".to_owned(),
                    normalized_digest: "0".repeat(64),
                    grant: "grant".to_owned(),
                    canonical_operation: Vec::new(),
                },
            })
            .await
            .expect("status");
        assert_eq!(status.branch, "main");
        assert_eq!(status.changes[0].path, PathBuf::from("src/lib.rs"));
        assert_eq!(
            data.dependency.statuses.lock().expect("statuses")[0].root,
            PathBuf::from("repo")
        );
    }
}
