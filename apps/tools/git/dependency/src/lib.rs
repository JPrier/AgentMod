//! External Git command execution and immutable checkpoint artifact storage.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentmod_primitives::{ContentHash, TimestampMillis};
use agentmod_protocol_support::authorization::{
    AuthorizationKey, ExpectedAuthorization, verify_authorization,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    fs::{self, File},
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
    time::timeout,
};
use uuid::Uuid;

/// Dependency-owned authorization context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyAuthorization {
    /// Owner.
    pub owner_id: String,
    /// Session.
    pub session_id: String,
    /// Call ID.
    pub call_id: String,
    /// Action.
    pub action: String,
    /// Supplied digest.
    pub normalized_digest: String,
    /// Signed grant.
    pub grant: String,
    /// Canonical operation bytes.
    pub canonical_operation: Vec<u8>,
}

/// Dependency-owned repository reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyRepository {
    /// Canonical repository root.
    pub root: PathBuf,
    /// Verified authorization capability.
    pub authorization: DependencyAuthorization,
}

/// Dependency-owned changed-file record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyChange {
    /// Two-column porcelain status.
    pub status: String,
    /// Repository-relative path.
    pub path: PathBuf,
}

/// Dependency-owned branch and worktree status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStatus {
    /// Current branch or `HEAD` when detached.
    pub branch: String,
    /// Current commit object ID.
    pub head: String,
    /// Configured upstream branch.
    pub upstream: Option<String>,
    /// Commits ahead of upstream.
    pub ahead: Option<u64>,
    /// Commits behind upstream.
    pub behind: Option<u64>,
    /// Structured changes.
    pub changes: Vec<DependencyChange>,
}

/// Dependency-owned bounded content response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyContent {
    /// Bounded inline prefix.
    pub inline: Vec<u8>,
    /// Total byte size.
    pub total_bytes: u64,
    /// Full host-owned artifact when the prefix was truncated.
    pub overflow_artifact: Option<PathBuf>,
}

/// Dependency-owned worktree record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyWorktree {
    /// Canonical worktree location.
    pub path: PathBuf,
    /// Resolved base commit.
    pub head: String,
}

/// Dependency-owned checkpoint record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyCheckpoint {
    /// Stable checkpoint ID.
    pub checkpoint_id: String,
    /// Base commit object ID.
    pub base_head: String,
    /// Immutable checkpoint directory.
    pub artifact_directory: PathBuf,
    /// Captured patch bytes.
    pub patch_bytes: u64,
    /// Count of captured untracked files.
    pub untracked_files: u64,
}

/// Git dependency configuration.
#[derive(Clone, Debug)]
pub struct GitDependencyConfig {
    /// Only repositories and worktrees under this root are accepted.
    pub workspace_root: PathBuf,
    /// Host-owned artifacts are persisted here.
    pub artifact_root: PathBuf,
    /// Maximum inline command output.
    pub output_limit_bytes: u64,
    /// Maximum checkpoint payload.
    pub checkpoint_limit_bytes: u64,
    /// Hard command timeout.
    pub command_timeout: Duration,
    /// Authorization secret reference.
    pub authorization_key_hex: String,
}

/// External Git dependency contract consumed only by data.
#[async_trait]
pub trait GitDependencyPort: Send + Sync {
    /// Discovers a containing repository.
    async fn discover(
        &self,
        authorization: DependencyAuthorization,
        path: PathBuf,
    ) -> Result<DependencyRepository, GitDependencyError>;
    /// Reads branch and changed-file state.
    async fn status(
        &self,
        repository: DependencyRepository,
    ) -> Result<DependencyStatus, GitDependencyError>;
    /// Reads a bounded binary-capable worktree diff.
    async fn diff(
        &self,
        repository: DependencyRepository,
    ) -> Result<DependencyContent, GitDependencyError>;
    /// Exports the full worktree patch to a host-owned artifact.
    async fn export_patch(
        &self,
        repository: DependencyRepository,
    ) -> Result<DependencyContent, GitDependencyError>;
    /// Creates a detached independent worktree.
    async fn create_worktree(
        &self,
        repository: DependencyRepository,
        destination: PathBuf,
        base: String,
    ) -> Result<DependencyWorktree, GitDependencyError>;
    /// Removes a clean managed worktree without force.
    async fn cleanup_worktree(
        &self,
        repository: DependencyRepository,
        destination: PathBuf,
    ) -> Result<(), GitDependencyError>;
    /// Captures an immutable patch plus untracked-file artifacts.
    async fn create_checkpoint(
        &self,
        repository: DependencyRepository,
    ) -> Result<DependencyCheckpoint, GitDependencyError>;
    /// Restores a checkpoint only over its clean matching base.
    async fn restore_checkpoint(
        &self,
        repository: DependencyRepository,
        checkpoint_id: String,
    ) -> Result<DependencyCheckpoint, GitDependencyError>;
}

/// Tokio Git adapter.
#[derive(Clone)]
pub struct TokioGitDependency {
    config: Arc<GitDependencyConfig>,
    authorization_key: Arc<AuthorizationKey>,
    permits: Arc<tokio::sync::Mutex<BTreeMap<String, PermitState>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PermitState {
    Pending,
    Ready(PathBuf),
    Spent,
}

impl TokioGitDependency {
    /// Validates and constructs the dependency.
    ///
    /// # Errors
    ///
    /// Rejects empty roots, zero bounds, or a zero timeout.
    pub fn new(mut config: GitDependencyConfig) -> Result<Self, GitDependencyError> {
        if config.workspace_root.as_os_str().is_empty()
            || config.artifact_root.as_os_str().is_empty()
            || config.output_limit_bytes == 0
            || config.checkpoint_limit_bytes == 0
            || config.command_timeout.is_zero()
            || config.authorization_key_hex.is_empty()
        {
            return Err(GitDependencyError::InvalidConfiguration);
        }
        let authorization_key = AuthorizationKey::from_hex(&config.authorization_key_hex)
            .map_err(|_| GitDependencyError::InvalidConfiguration)?;
        config.authorization_key_hex.clear();
        Ok(Self {
            config: Arc::new(config),
            authorization_key: Arc::new(authorization_key),
            permits: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
        })
    }

    async fn authorize(
        &self,
        authorization: &DependencyAuthorization,
    ) -> Result<String, GitDependencyError> {
        let digest = ContentHash::digest(&authorization.canonical_operation);
        if authorization.owner_id.is_empty()
            || authorization.session_id.is_empty()
            || authorization.call_id.is_empty()
            || authorization.action.is_empty()
            || digest.to_hex() != authorization.normalized_digest
        {
            return Err(GitDependencyError::AuthorizationDenied);
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| GitDependencyError::AuthorizationDenied)?
            .as_millis();
        let claims = verify_authorization(
            &authorization.grant,
            &self.authorization_key,
            ExpectedAuthorization {
                owner: &authorization.owner_id,
                session: &authorization.session_id,
                call_id: &authorization.call_id,
                action: &authorization.action,
                normalized_digest: digest,
            },
            TimestampMillis::new(
                i64::try_from(now).map_err(|_| GitDependencyError::AuthorizationDenied)?,
            ),
        )
        .map_err(|_| GitDependencyError::AuthorizationDenied)?;
        let permit_id = format!("{}:{}:{}", claims.owner, claims.session, claims.nonce);
        match self.permits.lock().await.entry(permit_id.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(PermitState::Pending);
                Ok(permit_id)
            }
            Entry::Occupied(_) => Err(GitDependencyError::AuthorizationReplay),
        }
    }

    async fn ensure_permit(
        &self,
        repository: &DependencyRepository,
        allowed_actions: &[&str],
    ) -> Result<(), GitDependencyError> {
        let authorization = &repository.authorization;
        if !allowed_actions.contains(&authorization.action.as_str()) {
            return Err(GitDependencyError::AuthorizationDenied);
        }
        let digest = ContentHash::digest(&authorization.canonical_operation);
        if digest.to_hex() != authorization.normalized_digest {
            return Err(GitDependencyError::AuthorizationDenied);
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| GitDependencyError::AuthorizationDenied)?
            .as_millis();
        let claims = verify_authorization(
            &authorization.grant,
            &self.authorization_key,
            ExpectedAuthorization {
                owner: &authorization.owner_id,
                session: &authorization.session_id,
                call_id: &authorization.call_id,
                action: &authorization.action,
                normalized_digest: digest,
            },
            TimestampMillis::new(
                i64::try_from(now).map_err(|_| GitDependencyError::AuthorizationDenied)?,
            ),
        )
        .map_err(|_| GitDependencyError::AuthorizationDenied)?;
        let permit_id = format!("{}:{}:{}", claims.owner, claims.session, claims.nonce);
        let expected_root = self.checked_repository(&repository.root).await?;
        let mut permits = self.permits.lock().await;
        let Some(state) = permits.get_mut(&permit_id) else {
            return Err(GitDependencyError::AuthorizationDenied);
        };
        if matches!(state, PermitState::Ready(root) if root == &expected_root) {
            *state = PermitState::Spent;
            Ok(())
        } else {
            Err(GitDependencyError::AuthorizationDenied)
        }
    }

    async fn checked_repository(&self, root: &Path) -> Result<PathBuf, GitDependencyError> {
        let workspace = fs::canonicalize(&self.config.workspace_root)
            .await
            .map_err(io_error)?;
        let repository = fs::canonicalize(root).await.map_err(io_error)?;
        if !repository.starts_with(workspace) {
            return Err(GitDependencyError::WorkspaceEscape);
        }
        Ok(repository)
    }

    async fn run(
        &self,
        cwd: &Path,
        arguments: &[&str],
        input: Option<&[u8]>,
    ) -> Result<CommandOutput, GitDependencyError> {
        let artifact_id = Uuid::now_v7().to_string();
        let command_directory = self.config.artifact_root.join("commands");
        fs::create_dir_all(&command_directory)
            .await
            .map_err(io_error)?;
        let output_path = command_directory.join(format!("{artifact_id}.stdout"));
        let output_file = File::create(&output_path).await.map_err(io_error)?;

        let mut command = Command::new("git");
        command
            .arg("-C")
            .arg(git_cli_path(cwd))
            .args(arguments)
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("LC_ALL", "C")
            .stdin(if input.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(io_error)?;
        if let Some(bytes) = input {
            let mut stdin = child
                .stdin
                .take()
                .ok_or(GitDependencyError::PipeUnavailable)?;
            stdin.write_all(bytes).await.map_err(io_error)?;
            drop(stdin);
        }
        let stdout = child
            .stdout
            .take()
            .ok_or(GitDependencyError::PipeUnavailable)?;
        let stderr = child
            .stderr
            .take()
            .ok_or(GitDependencyError::PipeUnavailable)?;
        let stdout_task = tokio::spawn(capture_output(
            stdout,
            output_file,
            self.config.output_limit_bytes,
        ));
        let stderr_limit = self.config.output_limit_bytes.min(64 * 1024);
        let stderr_task = tokio::spawn(capture_memory(stderr, stderr_limit));
        let status = timeout(self.config.command_timeout, child.wait())
            .await
            .map_err(|_| GitDependencyError::CommandTimeout)?
            .map_err(io_error)?;
        let stdout = stdout_task
            .await
            .map_err(|error| GitDependencyError::Io(error.to_string()))??;
        let stderr = stderr_task
            .await
            .map_err(|error| GitDependencyError::Io(error.to_string()))??;
        if !status.success() {
            return Err(GitDependencyError::CommandFailed {
                arguments: arguments.join(" "),
                message: String::from_utf8_lossy(&stderr.inline).trim().to_owned(),
            });
        }
        Ok(CommandOutput {
            content: DependencyContent {
                inline: stdout.inline,
                total_bytes: stdout.total_bytes,
                overflow_artifact: stdout.truncated.then_some(output_path.clone()),
            },
            full_path: output_path,
        })
    }

    async fn clean_status(&self, repository: &Path) -> Result<bool, GitDependencyError> {
        let output = self
            .run(
                repository,
                &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
                None,
            )
            .await?;
        require_complete(&output.content)?;
        Ok(output.content.inline.is_empty())
    }
}

#[async_trait]
impl GitDependencyPort for TokioGitDependency {
    async fn discover(
        &self,
        authorization: DependencyAuthorization,
        path: PathBuf,
    ) -> Result<DependencyRepository, GitDependencyError> {
        let permit_id = self.authorize(&authorization).await?;
        let candidate = fs::canonicalize(path).await.map_err(io_error)?;
        let workspace = fs::canonicalize(&self.config.workspace_root)
            .await
            .map_err(io_error)?;
        if !candidate.starts_with(&workspace) {
            return Err(GitDependencyError::WorkspaceEscape);
        }
        let output = self
            .run(&candidate, &["rev-parse", "--show-toplevel"], None)
            .await?;
        require_complete(&output.content)?;
        let root = PathBuf::from(text(&output.content.inline)?);
        let root = self.checked_repository(&root).await?;
        let mut permits = self.permits.lock().await;
        let Some(permit) = permits.get_mut(&permit_id) else {
            return Err(GitDependencyError::AuthorizationDenied);
        };
        *permit = PermitState::Ready(root.clone());
        Ok(DependencyRepository {
            root,
            authorization,
        })
    }

    async fn status(
        &self,
        repository: DependencyRepository,
    ) -> Result<DependencyStatus, GitDependencyError> {
        self.ensure_permit(
            &repository,
            &["git.status", "git.branch", "git.changed_files", "git.dirty"],
        )
        .await?;
        let root = self.checked_repository(&repository.root).await?;
        let branch = self
            .run(&root, &["rev-parse", "--abbrev-ref", "HEAD"], None)
            .await?;
        let head = self
            .run(&root, &["rev-parse", "--verify", "HEAD^{commit}"], None)
            .await?;
        let changes = self
            .run(
                &root,
                &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
                None,
            )
            .await?;
        require_complete(&branch.content)?;
        require_complete(&head.content)?;
        require_complete(&changes.content)?;
        let upstream = self
            .run(
                &root,
                &[
                    "rev-parse",
                    "--abbrev-ref",
                    "--symbolic-full-name",
                    "@{upstream}",
                ],
                None,
            )
            .await
            .ok()
            .and_then(|output| text(&output.content.inline).ok());
        let (ahead, behind) = if upstream.is_some() {
            self.run(
                &root,
                &["rev-list", "--left-right", "--count", "HEAD...@{upstream}"],
                None,
            )
            .await
            .ok()
            .and_then(|output| parse_counts(&output.content.inline).ok())
            .map_or((None, None), |(left, right)| (Some(left), Some(right)))
        } else {
            (None, None)
        };
        Ok(DependencyStatus {
            branch: text(&branch.content.inline)?,
            head: text(&head.content.inline)?,
            upstream,
            ahead,
            behind,
            changes: parse_porcelain(&changes.content.inline)?,
        })
    }

    async fn diff(
        &self,
        repository: DependencyRepository,
    ) -> Result<DependencyContent, GitDependencyError> {
        self.ensure_permit(&repository, &["git.diff"]).await?;
        let root = self.checked_repository(&repository.root).await?;
        self.run(&root, &["diff", "--binary", "HEAD", "--"], None)
            .await
            .map(|output| output.content)
    }

    async fn export_patch(
        &self,
        repository: DependencyRepository,
    ) -> Result<DependencyContent, GitDependencyError> {
        self.ensure_permit(&repository, &["git.export_patch"])
            .await?;
        let root = self.checked_repository(&repository.root).await?;
        let output = self
            .run(&root, &["diff", "--binary", "HEAD", "--"], None)
            .await?;
        Ok(DependencyContent {
            inline: output.content.inline,
            total_bytes: output.content.total_bytes,
            overflow_artifact: Some(output.full_path),
        })
    }

    async fn create_worktree(
        &self,
        repository: DependencyRepository,
        destination: PathBuf,
        base: String,
    ) -> Result<DependencyWorktree, GitDependencyError> {
        self.ensure_permit(&repository, &["git.worktree_create"])
            .await?;
        validate_ref(&base)?;
        let root = self.checked_repository(&repository.root).await?;
        let destination =
            validate_new_destination(&self.config.workspace_root, &root, &destination).await?;
        let resolved = self
            .run(
                &root,
                &["rev-parse", "--verify", &format!("{base}^{{commit}}")],
                None,
            )
            .await?;
        require_complete(&resolved.content)?;
        let destination_text = git_cli_path(&destination).to_string_lossy().into_owned();
        self.run(
            &root,
            &[
                "worktree",
                "add",
                "--detach",
                "--",
                &destination_text,
                &base,
            ],
            None,
        )
        .await?;
        Ok(DependencyWorktree {
            path: fs::canonicalize(destination).await.map_err(io_error)?,
            head: text(&resolved.content.inline)?,
        })
    }

    async fn cleanup_worktree(
        &self,
        repository: DependencyRepository,
        destination: PathBuf,
    ) -> Result<(), GitDependencyError> {
        self.ensure_permit(&repository, &["git.worktree_cleanup"])
            .await?;
        let root = self.checked_repository(&repository.root).await?;
        let destination = fs::canonicalize(destination).await.map_err(io_error)?;
        let workspace = fs::canonicalize(&self.config.workspace_root)
            .await
            .map_err(io_error)?;
        if !destination.starts_with(workspace) || destination == root {
            return Err(GitDependencyError::WorkspaceEscape);
        }
        if !self.clean_status(&destination).await? {
            return Err(GitDependencyError::DirtyWorktree);
        }
        let destination_text = git_cli_path(&destination).to_string_lossy().into_owned();
        self.run(
            &root,
            &["worktree", "remove", "--", &destination_text],
            None,
        )
        .await?;
        Ok(())
    }

    async fn create_checkpoint(
        &self,
        repository: DependencyRepository,
    ) -> Result<DependencyCheckpoint, GitDependencyError> {
        self.ensure_permit(&repository, &["git.checkpoint_create"])
            .await?;
        let root = self.checked_repository(&repository.root).await?;
        let head = self
            .run(&root, &["rev-parse", "--verify", "HEAD^{commit}"], None)
            .await?;
        require_complete(&head.content)?;
        let base_head = text(&head.content.inline)?;
        let patch = self
            .run(&root, &["diff", "--binary", "HEAD", "--"], None)
            .await?;
        let untracked = self
            .run(
                &root,
                &["ls-files", "--others", "--exclude-standard", "-z"],
                None,
            )
            .await?;
        require_complete(&untracked.content)?;
        let checkpoint_id = Uuid::now_v7().to_string();
        let directory = self
            .config
            .artifact_root
            .join("checkpoints")
            .join(&checkpoint_id);
        fs::create_dir_all(directory.join("untracked"))
            .await
            .map_err(io_error)?;
        fs::copy(&patch.full_path, directory.join("changes.patch"))
            .await
            .map_err(io_error)?;
        let paths = parse_nul_paths(&untracked.content.inline)?;
        let mut total = patch.content.total_bytes;
        let mut files = Vec::with_capacity(paths.len());
        for (index, relative) in paths.iter().enumerate() {
            validate_relative_path(relative)?;
            let source = root.join(relative);
            let metadata = fs::symlink_metadata(&source).await.map_err(io_error)?;
            if !metadata.is_file() {
                return Err(GitDependencyError::UnsupportedUntrackedType);
            }
            total = total
                .checked_add(metadata.len())
                .ok_or(GitDependencyError::CheckpointTooLarge)?;
            if total > self.config.checkpoint_limit_bytes {
                return Err(GitDependencyError::CheckpointTooLarge);
            }
            let artifact_name = format!("{index}.bin");
            fs::copy(&source, directory.join("untracked").join(&artifact_name))
                .await
                .map_err(io_error)?;
            files.push(CheckpointFile {
                relative_path: relative.clone(),
                artifact_name,
                content_hash: blake3_file(&source).await?,
            });
        }
        let metadata = CheckpointMetadata {
            checkpoint_id: checkpoint_id.clone(),
            repository_root: root,
            base_head: base_head.clone(),
            patch_bytes: patch.content.total_bytes,
            patch_hash: blake3_file(&patch.full_path).await?,
            files,
        };
        write_json_atomic(&directory.join("metadata.json"), &metadata).await?;
        Ok(checkpoint_record(&directory, metadata))
    }

    async fn restore_checkpoint(
        &self,
        repository: DependencyRepository,
        checkpoint_id: String,
    ) -> Result<DependencyCheckpoint, GitDependencyError> {
        self.ensure_permit(&repository, &["git.checkpoint_restore"])
            .await?;
        validate_checkpoint_id(&checkpoint_id)?;
        let root = self.checked_repository(&repository.root).await?;
        if !self.clean_status(&root).await? {
            return Err(GitDependencyError::DirtyWorktree);
        }
        let directory = self
            .config
            .artifact_root
            .join("checkpoints")
            .join(&checkpoint_id);
        let metadata_path = directory.join("metadata.json");
        let metadata_length = fs::metadata(&metadata_path).await.map_err(io_error)?.len();
        if metadata_length > self.config.output_limit_bytes {
            return Err(GitDependencyError::InvalidCheckpoint(
                "metadata exceeds configured bound".to_owned(),
            ));
        }
        let metadata_bytes = fs::read(metadata_path).await.map_err(io_error)?;
        let metadata: CheckpointMetadata = serde_json::from_slice(&metadata_bytes)
            .map_err(|error| GitDependencyError::InvalidCheckpoint(error.to_string()))?;
        if metadata.checkpoint_id != checkpoint_id
            || fs::canonicalize(&metadata.repository_root)
                .await
                .map_err(io_error)?
                != root
        {
            return Err(GitDependencyError::CheckpointRepositoryMismatch);
        }
        let current = self
            .run(&root, &["rev-parse", "--verify", "HEAD^{commit}"], None)
            .await?;
        require_complete(&current.content)?;
        if text(&current.content.inline)? != metadata.base_head {
            return Err(GitDependencyError::CheckpointBaseMismatch);
        }
        for file in &metadata.files {
            validate_relative_path(&file.relative_path)?;
            validate_artifact_name(&file.artifact_name)?;
            let target = root.join(&file.relative_path);
            if fs::try_exists(&target).await.map_err(io_error)? {
                return Err(GitDependencyError::RestoreCollision(
                    file.relative_path.clone(),
                ));
            }
            let artifact = directory.join("untracked").join(&file.artifact_name);
            if blake3_file(&artifact).await? != file.content_hash {
                return Err(GitDependencyError::CheckpointIntegrity);
            }
        }
        let patch_path = directory.join("changes.patch");
        let patch_length = fs::metadata(&patch_path).await.map_err(io_error)?.len();
        if patch_length != metadata.patch_bytes
            || patch_length > self.config.checkpoint_limit_bytes
            || blake3_file(&patch_path).await? != metadata.patch_hash
        {
            return Err(GitDependencyError::CheckpointIntegrity);
        }
        let patch = fs::read(patch_path).await.map_err(io_error)?;
        if !patch.is_empty() {
            self.run(
                &root,
                &["apply", "--check", "--whitespace=nowarn", "-"],
                Some(&patch),
            )
            .await?;
            self.run(&root, &["apply", "--whitespace=nowarn", "-"], Some(&patch))
                .await?;
        }
        for file in &metadata.files {
            let target = root.join(&file.relative_path);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).await.map_err(io_error)?;
            }
            fs::copy(
                directory.join("untracked").join(&file.artifact_name),
                target,
            )
            .await
            .map_err(io_error)?;
        }
        Ok(checkpoint_record(&directory, metadata))
    }
}

struct CommandOutput {
    content: DependencyContent,
    full_path: PathBuf,
}

struct Captured {
    inline: Vec<u8>,
    total_bytes: u64,
    truncated: bool,
}

async fn capture_output<R>(
    reader: R,
    mut full: File,
    inline_limit: u64,
) -> Result<Captured, GitDependencyError>
where
    R: AsyncRead + Unpin,
{
    let captured = capture(reader, Some(&mut full), inline_limit).await?;
    full.flush().await.map_err(io_error)?;
    full.sync_data().await.map_err(io_error)?;
    Ok(captured)
}

async fn capture_memory<R>(reader: R, inline_limit: u64) -> Result<Captured, GitDependencyError>
where
    R: AsyncRead + Unpin,
{
    capture::<R>(reader, None, inline_limit).await
}

async fn capture<R>(
    mut reader: R,
    mut full: Option<&mut File>,
    inline_limit: u64,
) -> Result<Captured, GitDependencyError>
where
    R: AsyncRead + Unpin,
{
    let mut inline = Vec::new();
    let mut total_bytes = 0_u64;
    let mut buffer = vec![0_u8; 8192];
    loop {
        let count = reader.read(&mut buffer).await.map_err(io_error)?;
        if count == 0 {
            break;
        }
        if let Some(file) = full.as_mut() {
            file.write_all(&buffer[..count]).await.map_err(io_error)?;
        }
        total_bytes = total_bytes
            .checked_add(u64::try_from(count).map_err(|_| GitDependencyError::LengthOverflow)?)
            .ok_or(GitDependencyError::LengthOverflow)?;
        let remaining = inline_limit.saturating_sub(
            u64::try_from(inline.len()).map_err(|_| GitDependencyError::LengthOverflow)?,
        );
        let retain = usize::try_from(
            remaining.min(u64::try_from(count).map_err(|_| GitDependencyError::LengthOverflow)?),
        )
        .map_err(|_| GitDependencyError::LengthOverflow)?;
        inline.extend_from_slice(&buffer[..retain]);
    }
    Ok(Captured {
        truncated: total_bytes > inline_limit,
        inline,
        total_bytes,
    })
}

fn require_complete(content: &DependencyContent) -> Result<(), GitDependencyError> {
    if content.overflow_artifact.is_some() {
        Err(GitDependencyError::OutputTooLarge)
    } else {
        Ok(())
    }
}

fn text(bytes: &[u8]) -> Result<String, GitDependencyError> {
    std::str::from_utf8(bytes)
        .map(str::trim)
        .map(str::to_owned)
        .map_err(|_| GitDependencyError::InvalidGitOutput)
}

fn parse_counts(bytes: &[u8]) -> Result<(u64, u64), GitDependencyError> {
    let content = text(bytes)?;
    let mut fields = content.split_whitespace();
    let left = fields
        .next()
        .ok_or(GitDependencyError::InvalidGitOutput)?
        .parse()
        .map_err(|_| GitDependencyError::InvalidGitOutput)?;
    let right = fields
        .next()
        .ok_or(GitDependencyError::InvalidGitOutput)?
        .parse()
        .map_err(|_| GitDependencyError::InvalidGitOutput)?;
    Ok((left, right))
}

fn parse_porcelain(bytes: &[u8]) -> Result<Vec<DependencyChange>, GitDependencyError> {
    let fields: Vec<&[u8]> = bytes
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty())
        .collect();
    let mut changes = Vec::new();
    let mut index = 0;
    while index < fields.len() {
        let field = fields[index];
        if field.len() < 4 || field[2] != b' ' {
            return Err(GitDependencyError::InvalidGitOutput);
        }
        let status = std::str::from_utf8(&field[..2])
            .map_err(|_| GitDependencyError::InvalidGitOutput)?
            .to_owned();
        let path = PathBuf::from(
            std::str::from_utf8(&field[3..]).map_err(|_| GitDependencyError::InvalidGitOutput)?,
        );
        validate_relative_path(&path)?;
        changes.push(DependencyChange { status, path });
        if field[0] == b'R' || field[0] == b'C' || field[1] == b'R' || field[1] == b'C' {
            index += 1;
        }
        index += 1;
    }
    Ok(changes)
}

fn parse_nul_paths(bytes: &[u8]) -> Result<Vec<PathBuf>, GitDependencyError> {
    bytes
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty())
        .map(|field| {
            std::str::from_utf8(field)
                .map(PathBuf::from)
                .map_err(|_| GitDependencyError::InvalidGitOutput)
        })
        .collect()
}

fn validate_relative_path(path: &Path) -> Result<(), GitDependencyError> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(GitDependencyError::InvalidPath);
    }
    Ok(())
}

fn validate_ref(reference: &str) -> Result<(), GitDependencyError> {
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
        return Err(GitDependencyError::InvalidRef);
    }
    Ok(())
}

async fn validate_new_destination(
    workspace_root: &Path,
    repository: &Path,
    destination: &Path,
) -> Result<PathBuf, GitDependencyError> {
    if destination.as_os_str().is_empty()
        || destination
            .components()
            .any(|part| part == Component::ParentDir)
    {
        return Err(GitDependencyError::InvalidPath);
    }
    if fs::try_exists(destination).await.map_err(io_error)? {
        return Err(GitDependencyError::DestinationExists);
    }
    let parent = destination
        .parent()
        .ok_or(GitDependencyError::InvalidPath)?;
    let parent = fs::canonicalize(parent).await.map_err(io_error)?;
    let workspace = fs::canonicalize(workspace_root).await.map_err(io_error)?;
    if !parent.starts_with(workspace) {
        return Err(GitDependencyError::WorkspaceEscape);
    }
    let name = destination
        .file_name()
        .ok_or(GitDependencyError::InvalidPath)?;
    let normalized = parent.join(name);
    if normalized.starts_with(repository) || repository.starts_with(&normalized) {
        return Err(GitDependencyError::InvalidWorktreeLocation);
    }
    Ok(normalized)
}

fn validate_checkpoint_id(value: &str) -> Result<(), GitDependencyError> {
    Uuid::parse_str(value)
        .map(|_| ())
        .map_err(|_| GitDependencyError::InvalidCheckpointId)
}

fn validate_artifact_name(value: &str) -> Result<(), GitDependencyError> {
    let path = Path::new(value);
    if value.is_empty()
        || path.components().count() != 1
        || path.file_name().is_none_or(|name| name != path.as_os_str())
    {
        Err(GitDependencyError::CheckpointIntegrity)
    } else {
        Ok(())
    }
}

fn git_cli_path(path: &Path) -> PathBuf {
    let value = path.to_string_lossy();
    if let Some(rest) = value.strip_prefix(r"\\?\UNC\") {
        PathBuf::from(format!(r"\\{rest}"))
    } else if let Some(rest) = value.strip_prefix(r"\\?\") {
        PathBuf::from(rest)
    } else {
        path.to_path_buf()
    }
}

#[derive(Deserialize, Serialize)]
struct CheckpointMetadata {
    checkpoint_id: String,
    repository_root: PathBuf,
    base_head: String,
    patch_bytes: u64,
    patch_hash: String,
    files: Vec<CheckpointFile>,
}

#[derive(Deserialize, Serialize)]
struct CheckpointFile {
    relative_path: PathBuf,
    artifact_name: String,
    content_hash: String,
}

fn checkpoint_record(directory: &Path, metadata: CheckpointMetadata) -> DependencyCheckpoint {
    DependencyCheckpoint {
        checkpoint_id: metadata.checkpoint_id,
        base_head: metadata.base_head,
        artifact_directory: directory.to_path_buf(),
        patch_bytes: metadata.patch_bytes,
        untracked_files: u64::try_from(metadata.files.len()).unwrap_or(u64::MAX),
    }
}

async fn write_json_atomic<T>(path: &Path, value: &T) -> Result<(), GitDependencyError>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| GitDependencyError::InvalidCheckpoint(error.to_string()))?;
    let temporary = path.with_extension(format!("tmp-{}", Uuid::now_v7()));
    let mut file = File::create(&temporary).await.map_err(io_error)?;
    file.write_all(&bytes).await.map_err(io_error)?;
    file.sync_all().await.map_err(io_error)?;
    drop(file);
    fs::rename(temporary, path).await.map_err(io_error)
}

async fn blake3_file(path: &Path) -> Result<String, GitDependencyError> {
    let bytes = fs::read(path).await.map_err(io_error)?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err provides owned errors and this boundary redacts external details"
)]
fn io_error(error: std::io::Error) -> GitDependencyError {
    GitDependencyError::Io(error.to_string())
}

/// Git dependency failure hidden below data.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum GitDependencyError {
    /// Dependency configuration is invalid.
    #[error("invalid Git dependency configuration")]
    InvalidConfiguration,
    /// Authorization denied.
    #[error("Git authorization denied")]
    AuthorizationDenied,
    /// Grant replay.
    #[error("Git authorization replay denied")]
    AuthorizationReplay,
    /// Requested path escaped the configured workspace.
    #[error("Git path escapes the configured workspace")]
    WorkspaceEscape,
    /// Path shape is unsafe.
    #[error("invalid Git path")]
    InvalidPath,
    /// Worktree location overlaps the repository.
    #[error("worktree location must be independent from the repository")]
    InvalidWorktreeLocation,
    /// Destination already exists.
    #[error("worktree destination already exists")]
    DestinationExists,
    /// Revision is unsafe.
    #[error("invalid Git reference")]
    InvalidRef,
    /// Git output was malformed or non-UTF-8 where text was required.
    #[error("invalid Git command output")]
    InvalidGitOutput,
    /// A parse-oriented command exceeded its bound.
    #[error("Git command output exceeds the configured bound")]
    OutputTooLarge,
    /// Git command exceeded its timeout.
    #[error("Git command timed out")]
    CommandTimeout,
    /// A child pipe was unavailable.
    #[error("Git command pipe unavailable")]
    PipeUnavailable,
    /// Git rejected the command.
    #[error("Git command `{arguments}` failed: {message}")]
    CommandFailed {
        /// Redacted argument labels.
        arguments: String,
        /// Bounded stderr.
        message: String,
    },
    /// Worktree is unexpectedly dirty.
    #[error("Git worktree is dirty")]
    DirtyWorktree,
    /// Checkpoint exceeded its configured bound.
    #[error("checkpoint exceeds the configured byte bound")]
    CheckpointTooLarge,
    /// Symlink or non-file untracked content is unsupported.
    #[error("checkpoint contains an unsupported untracked file type")]
    UnsupportedUntrackedType,
    /// Checkpoint ID is malformed.
    #[error("invalid checkpoint ID")]
    InvalidCheckpointId,
    /// Checkpoint metadata is invalid.
    #[error("invalid checkpoint metadata: {0}")]
    InvalidCheckpoint(String),
    /// Checkpoint belongs to another repository.
    #[error("checkpoint repository does not match")]
    CheckpointRepositoryMismatch,
    /// Current HEAD differs from checkpoint base.
    #[error("checkpoint base does not match current HEAD")]
    CheckpointBaseMismatch,
    /// Checkpoint content hash differs.
    #[error("checkpoint artifact integrity check failed")]
    CheckpointIntegrity,
    /// Restore would overwrite an unexpected path.
    #[error("checkpoint restore collides with existing path: {0}")]
    RestoreCollision(PathBuf),
    /// Byte length conversion overflowed.
    #[error("Git output length overflow")]
    LengthOverflow,
    /// OS or artifact I/O failed.
    #[error("Git dependency I/O failed: {0}")]
    Io(String),
}

#[cfg(test)]
mod authorization_tests {
    use agentmod_protocol_support::authorization::{AuthorizationClaims, seal_authorization};

    use super::*;

    fn config(root: &Path) -> GitDependencyConfig {
        GitDependencyConfig {
            workspace_root: root.to_path_buf(),
            artifact_root: root.join("artifacts"),
            output_limit_bytes: 4096,
            checkpoint_limit_bytes: 4096,
            command_timeout: Duration::from_secs(2),
            authorization_key_hex: "07".repeat(32),
        }
    }

    fn authorization(
        key: [u8; 32],
        owner: &str,
        nonce: &str,
        expired: bool,
    ) -> DependencyAuthorization {
        let operation = format!("operation-{nonce}").into_bytes();
        let digest = ContentHash::digest(&operation);
        let now = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_millis(),
        )
        .expect("time");
        let expiry = if expired { now - 1 } else { now + 30_000 };
        let claims = AuthorizationClaims {
            owner: owner.to_owned(),
            session: "session".to_owned(),
            call_id: nonce.to_owned(),
            action: "git.discover".to_owned(),
            normalized_digest: digest,
            issued_at: TimestampMillis::new(expiry - 1000),
            expires_at: TimestampMillis::new(expiry),
            nonce: nonce.to_owned(),
        };
        DependencyAuthorization {
            owner_id: owner.to_owned(),
            session_id: "session".to_owned(),
            call_id: nonce.to_owned(),
            action: "git.discover".to_owned(),
            normalized_digest: digest.to_hex(),
            grant: seal_authorization(&claims, &AuthorizationKey::from_bytes(key)).expect("seal"),
            canonical_operation: operation,
        }
    }

    #[tokio::test]
    async fn forged_expired_wrong_owner_digest_and_replay_have_no_side_effect() {
        let root = tempfile::tempdir().expect("root");
        let dependency = TokioGitDependency::new(config(root.path())).expect("dependency");
        let forged = authorization([8; 32], "owner", "forged", false);
        assert_eq!(
            dependency.discover(forged, root.path().to_path_buf()).await,
            Err(GitDependencyError::AuthorizationDenied)
        );
        let expired = authorization([7; 32], "owner", "expired", true);
        assert_eq!(
            dependency
                .discover(expired, root.path().to_path_buf())
                .await,
            Err(GitDependencyError::AuthorizationDenied)
        );
        let mut wrong_owner = authorization([7; 32], "owner", "owner", false);
        wrong_owner.owner_id = "other".to_owned();
        assert_eq!(
            dependency
                .discover(wrong_owner, root.path().to_path_buf())
                .await,
            Err(GitDependencyError::AuthorizationDenied)
        );
        let mut wrong_digest = authorization([7; 32], "owner", "digest", false);
        wrong_digest.normalized_digest = "0".repeat(64);
        assert_eq!(
            dependency
                .discover(wrong_digest, root.path().to_path_buf())
                .await,
            Err(GitDependencyError::AuthorizationDenied)
        );
        let replay = authorization([7; 32], "owner", "replay", false);
        dependency.authorize(&replay).await.expect("first use");
        assert_eq!(
            dependency.discover(replay, root.path().to_path_buf()).await,
            Err(GitDependencyError::AuthorizationReplay)
        );
        assert!(!root.path().join("artifacts").exists());
    }
}
