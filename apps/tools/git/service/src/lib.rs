//! Tool-protocol endpoints and authorization enforcement for Git operations.

use std::path::PathBuf;

use agentmod_git_host_logic::{
    BoundedContent, CheckpointResult, CleanupWorktreeCommand, CreateWorktreeCommand,
    GitAuthorization, GitLogicError, GitLogicPort, RepositorySelection, RepositoryStatus,
    RestoreCheckpointCommand, WorktreeResult,
};
use agentmod_tool_protocol::{OutputStream, ToolDescriptor, ToolHostCommand, ToolHostEvent};
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;

const GIT_GROUP: &str = "git";

/// Mandatory local identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitHostServiceConfig {
    /// Owner.
    pub owner_id: String,
    /// Session.
    pub session_id: String,
}

/// Service endpoint failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum GitServiceError {
    /// Tool call ID is empty.
    #[error("tool call ID is required")]
    InvalidCallId,
    /// Normalized request digest is empty.
    #[error("normalized request digest is required")]
    MissingDigest,
    /// Consequential action has no authorization.
    #[error("explicit authorization grant is required")]
    MissingAuthorization,
    /// Host identity is absent.
    #[error("Git host authorization configuration unavailable")]
    MissingConfiguration,
    /// Tool is unknown.
    #[error("unknown Git tool: {0}")]
    UnknownTool(String),
    /// Arguments are malformed.
    #[error("invalid Git tool arguments: {0}")]
    InvalidArguments(String),
    /// Generic cancellation needs runtime routing.
    #[error("generic cancellation requires runtime call routing")]
    UnsupportedCancellation,
    /// Logic operation failed.
    #[error("Git operation failed: {0}")]
    Logic(String),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RepositoryRequest {
    path: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorktreeCreateRequest {
    repository: PathBuf,
    destination: PathBuf,
    base: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorktreeCleanupRequest {
    repository: PathBuf,
    destination: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreRequest {
    repository: PathBuf,
    checkpoint_id: String,
}

/// Git tool-host service.
#[derive(Clone)]
pub struct GitHostService<L> {
    logic: L,
    config: GitHostServiceConfig,
}

impl<L> GitHostService<L> {
    /// Injects Git logic and mandatory local identity.
    ///
    /// # Errors
    ///
    /// Returns an error when owner or session identity is absent.
    pub fn new(logic: L, config: GitHostServiceConfig) -> Result<Self, GitServiceError> {
        if config.owner_id.is_empty() || config.session_id.is_empty() {
            Err(GitServiceError::MissingConfiguration)
        } else {
            Ok(Self { logic, config })
        }
    }
}

impl<L> GitHostService<L>
where
    L: GitLogicPort,
{
    /// Handles one tool-host command.
    ///
    /// # Errors
    ///
    /// Rejects malformed, unauthorized, unknown, or failed operations.
    pub async fn handle(
        &self,
        command: ToolHostCommand,
    ) -> Result<Vec<ToolHostEvent>, GitServiceError> {
        match command {
            ToolHostCommand::DiscoverGroups => Ok(vec![ToolHostEvent::Groups {
                groups: vec![GIT_GROUP.to_owned()],
            }]),
            ToolHostCommand::DiscoverTools { groups } => Ok(vec![ToolHostEvent::Tools {
                tools: groups
                    .iter()
                    .any(|group| group == GIT_GROUP)
                    .then(tool_descriptors)
                    .unwrap_or_default(),
            }]),
            ToolHostCommand::Health => Ok(vec![ToolHostEvent::Progress {
                call_id: "health".to_owned(),
                message: "Git host ready".to_owned(),
                completed: Some(1),
                total: Some(1),
            }]),
            ToolHostCommand::Cancel { .. } => Err(GitServiceError::UnsupportedCancellation),
            ToolHostCommand::Execute {
                call_id,
                tool,
                arguments,
                normalized_digest,
                authorization_grant,
                ..
            } => {
                validate_envelope(&call_id, &normalized_digest, &authorization_grant)?;
                let canonical_operation = canonical_operation(&tool, &arguments)?;
                let authorization = GitAuthorization {
                    owner_id: self.config.owner_id.clone(),
                    session_id: self.config.session_id.clone(),
                    call_id: call_id.clone(),
                    action: tool.clone(),
                    normalized_digest,
                    grant: authorization_grant,
                    canonical_operation,
                };
                self.execute(call_id, tool, arguments, authorization).await
            }
        }
    }

    async fn execute(
        &self,
        call_id: String,
        tool: String,
        arguments: Value,
        authorization: GitAuthorization,
    ) -> Result<Vec<ToolHostEvent>, GitServiceError> {
        let mut events = vec![ToolHostEvent::Started {
            call_id: call_id.clone(),
        }];
        match tool.as_str() {
            "git.discover" => {
                let request: RepositoryRequest = parse(arguments)?;
                let root = self
                    .logic
                    .discover(selection(request.path, authorization))
                    .await
                    .map_err(map_logic_error)?;
                events.push(completed(
                    &call_id,
                    json!({ "repository_root": root }),
                    false,
                ));
            }
            "git.status" | "git.branch" | "git.changed_files" | "git.dirty" => {
                let request: RepositoryRequest = parse(arguments)?;
                let status = self
                    .logic
                    .status(selection(request.path, authorization))
                    .await
                    .map_err(map_logic_error)?;
                let value = status_projection(&tool, status);
                events.push(completed(&call_id, value, false));
            }
            "git.diff" | "git.export_patch" => {
                let request: RepositoryRequest = parse(arguments)?;
                let repository = selection(request.path, authorization);
                let content = if tool == "git.diff" {
                    self.logic.diff(repository).await
                } else {
                    self.logic.export_patch(repository).await
                }
                .map_err(map_logic_error)?;
                append_content(&mut events, &call_id, &content);
            }
            "git.worktree_create" => {
                let request: WorktreeCreateRequest = parse(arguments)?;
                let worktree = self
                    .logic
                    .create_worktree(CreateWorktreeCommand {
                        repository: selection(request.repository, authorization),
                        destination: request.destination,
                        base: request.base,
                    })
                    .await
                    .map_err(map_logic_error)?;
                events.push(completed(&call_id, worktree_json(&worktree), false));
            }
            "git.worktree_cleanup" => {
                let request: WorktreeCleanupRequest = parse(arguments)?;
                self.logic
                    .cleanup_worktree(CleanupWorktreeCommand {
                        repository: selection(request.repository, authorization),
                        destination: request.destination,
                    })
                    .await
                    .map_err(map_logic_error)?;
                events.push(completed(&call_id, json!({ "removed": true }), false));
            }
            "git.checkpoint_create" => {
                let request: RepositoryRequest = parse(arguments)?;
                let checkpoint = self
                    .logic
                    .create_checkpoint(selection(request.path, authorization))
                    .await
                    .map_err(map_logic_error)?;
                events.push(completed(&call_id, checkpoint_json(&checkpoint), false));
            }
            "git.checkpoint_restore" => {
                let request: RestoreRequest = parse(arguments)?;
                let checkpoint = self
                    .logic
                    .restore_checkpoint(RestoreCheckpointCommand {
                        repository: selection(request.repository, authorization),
                        checkpoint_id: request.checkpoint_id,
                    })
                    .await
                    .map_err(map_logic_error)?;
                events.push(completed(&call_id, checkpoint_json(&checkpoint), false));
            }
            _ => return Err(GitServiceError::UnknownTool(tool)),
        }
        Ok(events)
    }
}

fn validate_envelope(
    call_id: &str,
    digest: &str,
    authorization: &str,
) -> Result<(), GitServiceError> {
    if call_id.trim().is_empty() {
        return Err(GitServiceError::InvalidCallId);
    }
    if digest.trim().is_empty() {
        return Err(GitServiceError::MissingDigest);
    }
    if authorization.trim().is_empty() {
        return Err(GitServiceError::MissingAuthorization);
    }
    Ok(())
}

fn selection(path: PathBuf, authorization: GitAuthorization) -> RepositorySelection {
    RepositorySelection {
        path,
        authorization,
    }
}

fn canonical_operation(tool: &str, arguments: &Value) -> Result<Vec<u8>, GitServiceError> {
    let normalized = normalize_json(arguments);
    serde_json::to_vec(&(tool, normalized))
        .map_err(|_| GitServiceError::InvalidArguments("canonicalization failed".to_owned()))
}

fn normalize_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted: std::collections::BTreeMap<_, _> = map
                .iter()
                .map(|(key, value)| (key.clone(), normalize_json(value)))
                .collect();
            serde_json::to_value(sorted).unwrap_or(Value::Null)
        }
        Value::Array(values) => Value::Array(values.iter().map(normalize_json).collect()),
        _ => value.clone(),
    }
}

fn parse<T>(arguments: Value) -> Result<T, GitServiceError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(arguments)
        .map_err(|error| GitServiceError::InvalidArguments(error.to_string()))
}

fn status_projection(tool: &str, status: RepositoryStatus) -> Value {
    match tool {
        "git.branch" => json!({
            "repository_root": status.repository_root,
            "branch": status.branch.branch,
            "head": status.branch.head,
            "upstream": status.branch.upstream,
            "ahead": status.branch.ahead,
            "behind": status.branch.behind,
        }),
        "git.changed_files" => json!({
            "repository_root": status.repository_root,
            "changes": status.changes.into_iter().map(|change| json!({
                "status": change.status,
                "path": change.path,
            })).collect::<Vec<_>>(),
        }),
        "git.dirty" => json!({
            "repository_root": status.repository_root,
            "dirty": status.dirty,
        }),
        _ => json!({
            "repository_root": status.repository_root,
            "branch": {
                "name": status.branch.branch,
                "head": status.branch.head,
                "upstream": status.branch.upstream,
                "ahead": status.branch.ahead,
                "behind": status.branch.behind,
            },
            "changes": status.changes.into_iter().map(|change| json!({
                "status": change.status,
                "path": change.path,
            })).collect::<Vec<_>>(),
            "dirty": status.dirty,
        }),
    }
}

fn append_content(events: &mut Vec<ToolHostEvent>, call_id: &str, content: &BoundedContent) {
    if !content.inline.is_empty() {
        events.push(ToolHostEvent::Output {
            call_id: call_id.to_owned(),
            stream: OutputStream::Standard,
            content: String::from_utf8_lossy(&content.inline).into_owned(),
        });
    }
    events.push(completed(
        call_id,
        json!({
            "total_bytes": content.total_bytes,
            "host_artifact": content.artifact,
        }),
        content.truncated,
    ));
}

fn worktree_json(result: &WorktreeResult) -> Value {
    json!({ "path": result.path, "head": result.head })
}

fn checkpoint_json(result: &CheckpointResult) -> Value {
    json!({
        "checkpoint_id": result.checkpoint_id,
        "base_head": result.base_head,
        "artifact_directory": result.artifact_directory,
        "patch_bytes": result.patch_bytes,
        "untracked_files": result.untracked_files,
    })
}

fn completed(call_id: &str, result: Value, truncated: bool) -> ToolHostEvent {
    ToolHostEvent::Completed {
        call_id: call_id.to_owned(),
        result,
        artifact: None,
        truncated,
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err supplies ownership and service redacts logic details"
)]
fn map_logic_error(error: GitLogicError) -> GitServiceError {
    GitServiceError::Logic(error.to_string())
}

fn tool_descriptors() -> Vec<ToolDescriptor> {
    [
        (
            "git.discover",
            "Discover a containing repository.",
            repo_schema(),
        ),
        (
            "git.status",
            "Read branch and changed-file status.",
            repo_schema(),
        ),
        (
            "git.branch",
            "Read branch, HEAD, and upstream counts.",
            repo_schema(),
        ),
        (
            "git.changed_files",
            "List structured changed files.",
            repo_schema(),
        ),
        (
            "git.dirty",
            "Detect whether a repository is dirty.",
            repo_schema(),
        ),
        (
            "git.diff",
            "Read a bounded binary-capable diff.",
            repo_schema(),
        ),
        (
            "git.export_patch",
            "Export a full patch artifact.",
            repo_schema(),
        ),
        (
            "git.worktree_create",
            "Create a detached independent worktree.",
            json!({
                "type":"object",
                "required":["repository","destination","base"],
                "properties":{
                    "repository":{"type":"string"},
                    "destination":{"type":"string"},
                    "base":{"type":"string"}
                },
                "additionalProperties":false
            }),
        ),
        (
            "git.worktree_cleanup",
            "Remove a clean managed worktree without force.",
            json!({
                "type":"object",
                "required":["repository","destination"],
                "properties":{
                    "repository":{"type":"string"},
                    "destination":{"type":"string"}
                },
                "additionalProperties":false
            }),
        ),
        (
            "git.checkpoint_create",
            "Capture an immutable patch and untracked-file checkpoint.",
            repo_schema(),
        ),
        (
            "git.checkpoint_restore",
            "Restore a checkpoint over its clean matching base.",
            json!({
                "type":"object",
                "required":["repository","checkpoint_id"],
                "properties":{
                    "repository":{"type":"string"},
                    "checkpoint_id":{"type":"string"}
                },
                "additionalProperties":false
            }),
        ),
    ]
    .into_iter()
    .map(|(id, description, input_schema)| ToolDescriptor {
        id: id.to_owned(),
        group: GIT_GROUP.to_owned(),
        description: description.to_owned(),
        input_schema,
        supported_decisions: vec![
            "continue".to_owned(),
            "replace".to_owned(),
            "reject".to_owned(),
            "require_approval".to_owned(),
            "defer".to_owned(),
            "cancel".to_owned(),
        ],
    })
    .collect()
}

fn repo_schema() -> Value {
    json!({
        "type":"object",
        "required":["path"],
        "properties":{"path":{"type":"string"}},
        "additionalProperties":false
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use async_trait::async_trait;

    use super::*;

    #[derive(Clone)]
    struct MockLogic {
        mutations: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl GitLogicPort for MockLogic {
        async fn discover(&self, selection: RepositorySelection) -> Result<PathBuf, GitLogicError> {
            Ok(selection.path)
        }

        async fn status(
            &self,
            _selection: RepositorySelection,
        ) -> Result<RepositoryStatus, GitLogicError> {
            unreachable!()
        }

        async fn diff(
            &self,
            _selection: RepositorySelection,
        ) -> Result<BoundedContent, GitLogicError> {
            unreachable!()
        }

        async fn export_patch(
            &self,
            _selection: RepositorySelection,
        ) -> Result<BoundedContent, GitLogicError> {
            unreachable!()
        }

        async fn create_worktree(
            &self,
            _command: CreateWorktreeCommand,
        ) -> Result<WorktreeResult, GitLogicError> {
            self.mutations.fetch_add(1, Ordering::Relaxed);
            Ok(WorktreeResult {
                path: PathBuf::from("worktree"),
                head: "abc".to_owned(),
            })
        }

        async fn cleanup_worktree(
            &self,
            _command: CleanupWorktreeCommand,
        ) -> Result<(), GitLogicError> {
            unreachable!()
        }

        async fn create_checkpoint(
            &self,
            _selection: RepositorySelection,
        ) -> Result<CheckpointResult, GitLogicError> {
            unreachable!()
        }

        async fn restore_checkpoint(
            &self,
            _command: RestoreCheckpointCommand,
        ) -> Result<CheckpointResult, GitLogicError> {
            unreachable!()
        }
    }

    fn command(authorization: &str) -> ToolHostCommand {
        serde_json::from_value(json!({
            "command":"execute",
            "value":{
                "call_id":"call-1",
                "tool":"git.worktree_create",
                "arguments":{
                    "repository":"repo",
                    "destination":"worktree",
                    "base":"HEAD"
                },
                "normalized_digest":"digest",
                "authorization_grant":authorization,
                "cancellation_id":"018f6f83-7b80-7000-8000-000000000002"
            }
        }))
        .expect("wire command")
    }

    fn service(mutations: Arc<AtomicUsize>) -> GitHostService<MockLogic> {
        GitHostService::new(
            MockLogic { mutations },
            GitHostServiceConfig {
                owner_id: "owner".to_owned(),
                session_id: "session".to_owned(),
            },
        )
        .expect("service")
    }

    #[tokio::test]
    async fn denied_mutation_never_reaches_logic() {
        let mutations = Arc::new(AtomicUsize::new(0));
        let service = service(Arc::clone(&mutations));
        assert_eq!(
            service.handle(command("")).await,
            Err(GitServiceError::MissingAuthorization)
        );
        assert_eq!(mutations.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn authorized_mutation_reaches_logic() {
        let mutations = Arc::new(AtomicUsize::new(0));
        let service = service(Arc::clone(&mutations));
        let events = service.handle(command("grant")).await.expect("authorized");
        assert!(matches!(
            events.first(),
            Some(ToolHostEvent::Started { .. })
        ));
        assert_eq!(mutations.load(Ordering::Relaxed), 1);
    }
}
