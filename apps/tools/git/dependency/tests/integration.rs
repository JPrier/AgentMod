//! Deterministic integration coverage against temporary local Git repositories.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use agentmod_git_host_dependency::{
    DependencyAuthorization, GitDependencyConfig, GitDependencyError, GitDependencyPort,
    TokioGitDependency,
};
use tempfile::TempDir;

struct Fixture {
    root: TempDir,
    repository: PathBuf,
    dependency: TokioGitDependency,
}

impl Fixture {
    fn new(output_limit_bytes: u64) -> Self {
        let root = TempDir::new().expect("workspace");
        let repository = root.path().join("repository");
        fs::create_dir(&repository).expect("repository directory");
        git(&repository, &["init", "-b", "main"]);
        git(&repository, &["config", "user.name", "AgentMod Test"]);
        git(
            &repository,
            &["config", "user.email", "agentmod@example.invalid"],
        );
        git(&repository, &["config", "core.autocrlf", "false"]);
        fs::write(repository.join("tracked.txt"), "base\n").expect("base file");
        git(&repository, &["add", "tracked.txt"]);
        git(&repository, &["commit", "-m", "base"]);
        let dependency = TokioGitDependency::new(GitDependencyConfig {
            workspace_root: root.path().to_path_buf(),
            artifact_root: root.path().join("artifacts"),
            output_limit_bytes,
            checkpoint_limit_bytes: 1024 * 1024,
            command_timeout: Duration::from_secs(10),
            authorization_key_hex: "07".repeat(32),
        })
        .expect("dependency");
        Self {
            root,
            repository,
            dependency,
        }
    }
}

fn git(directory: &Path, arguments: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(directory)
        .args(arguments)
        .output()
        .expect("run git fixture command");
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        arguments,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("Git fixture output is UTF-8")
        .trim()
        .to_owned()
}

#[tokio::test]
async fn discovers_statuses_diffs_and_exports_overflow() {
    let fixture = Fixture::new(64);
    let nested = fixture.repository.join("nested");
    fs::create_dir(&nested).expect("nested");
    let discovered = fixture
        .dependency
        .discover(test_authorization("git.discover", "discover"), nested)
        .await
        .expect("discover");
    assert_eq!(
        discovered.root,
        fixture.repository.canonicalize().expect("canonical repo")
    );

    fs::write(
        fixture.repository.join("tracked.txt"),
        "changed content that creates a bounded binary-capable patch projection\n",
    )
    .expect("modify");
    fs::write(fixture.repository.join("untracked.txt"), "new\n").expect("untracked");
    let status_repository = fixture
        .dependency
        .discover(
            test_authorization("git.status", "status"),
            fixture.repository.clone(),
        )
        .await
        .expect("discover for status");
    let status = fixture
        .dependency
        .status(status_repository)
        .await
        .expect("status");
    assert_eq!(status.branch, "main");
    let paths: BTreeSet<_> = status
        .changes
        .iter()
        .map(|change| change.path.clone())
        .collect();
    assert!(paths.contains(&PathBuf::from("tracked.txt")));
    assert!(paths.contains(&PathBuf::from("untracked.txt")));

    let diff_repository = fixture
        .dependency
        .discover(
            test_authorization("git.diff", "diff"),
            fixture.repository.clone(),
        )
        .await
        .expect("discover for diff");
    let diff = fixture
        .dependency
        .diff(diff_repository)
        .await
        .expect("diff");
    assert!(diff.total_bytes > 64);
    assert_eq!(diff.inline.len(), 64);
    assert!(
        diff.overflow_artifact
            .as_ref()
            .is_some_and(|path| path.is_file())
    );

    let export_repository = fixture
        .dependency
        .discover(
            test_authorization("git.export_patch", "export"),
            fixture.repository.clone(),
        )
        .await
        .expect("discover for export");
    let export = fixture
        .dependency
        .export_patch(export_repository)
        .await
        .expect("export");
    assert!(
        export
            .overflow_artifact
            .as_ref()
            .is_some_and(|path| path.is_file())
    );
}

fn test_authorization(action: &str, nonce: &str) -> DependencyAuthorization {
    use agentmod_primitives::{ContentHash, TimestampMillis};
    use agentmod_protocol_support::authorization::{
        AuthorizationClaims, AuthorizationKey, seal_authorization,
    };
    use std::time::{SystemTime, UNIX_EPOCH};
    let operation = format!("operation-{nonce}").into_bytes();
    let digest = ContentHash::digest(&operation);
    let now = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("time");
    let claims = AuthorizationClaims {
        owner: "owner".to_owned(),
        session: "session".to_owned(),
        call_id: nonce.to_owned(),
        action: action.to_owned(),
        normalized_digest: digest,
        issued_at: TimestampMillis::new(now - 1000),
        expires_at: TimestampMillis::new(now + 30_000),
        nonce: nonce.to_owned(),
    };
    DependencyAuthorization {
        owner_id: "owner".to_owned(),
        session_id: "session".to_owned(),
        call_id: nonce.to_owned(),
        action: action.to_owned(),
        normalized_digest: digest.to_hex(),
        grant: seal_authorization(&claims, &AuthorizationKey::from_bytes([7; 32])).expect("grant"),
        canonical_operation: operation,
    }
}

#[tokio::test]
async fn creates_and_cleans_up_detached_worktree() {
    let fixture = Fixture::new(64 * 1024);
    let create_repository = fixture
        .dependency
        .discover(
            test_authorization("git.worktree_create", "worktree-create"),
            fixture.repository.clone(),
        )
        .await
        .expect("discover");
    let replayed_create_repository = create_repository.clone();
    let destination = fixture.root.path().join("worker");
    let worktree = fixture
        .dependency
        .create_worktree(create_repository, destination.clone(), "HEAD".to_owned())
        .await
        .expect("create worktree");
    assert!(worktree.path.join("tracked.txt").is_file());
    assert_eq!(
        git(&worktree.path, &["rev-parse", "--abbrev-ref", "HEAD"]),
        "HEAD"
    );
    let replay_destination = fixture.root.path().join("replayed-worker");
    assert_eq!(
        fixture
            .dependency
            .create_worktree(
                replayed_create_repository,
                replay_destination.clone(),
                "HEAD".to_owned(),
            )
            .await,
        Err(GitDependencyError::AuthorizationDenied)
    );
    assert!(!replay_destination.exists());
    let cleanup_repository = fixture
        .dependency
        .discover(
            test_authorization("git.worktree_cleanup", "worktree-cleanup"),
            fixture.repository.clone(),
        )
        .await
        .expect("discover for cleanup");
    fixture
        .dependency
        .cleanup_worktree(cleanup_repository, destination.clone())
        .await
        .expect("cleanup worktree");
    assert!(!destination.exists());
}

#[tokio::test]
async fn mutating_operations_reject_wrong_action_and_repository_without_side_effects() {
    let fixture = Fixture::new(64 * 1024);
    let destination = fixture.root.path().join("unauthorized-worker");
    let wrong_action = fixture
        .dependency
        .discover(
            test_authorization("git.status", "wrong-action"),
            fixture.repository.clone(),
        )
        .await
        .expect("discover with read grant");
    assert_eq!(
        fixture
            .dependency
            .create_worktree(wrong_action, destination.clone(), "HEAD".to_owned())
            .await,
        Err(GitDependencyError::AuthorizationDenied)
    );
    assert!(!destination.exists());

    let other_repository = fixture.root.path().join("other-repository");
    fs::create_dir(&other_repository).expect("other repository directory");
    git(&other_repository, &["init", "-b", "main"]);
    git(&other_repository, &["config", "user.name", "AgentMod Test"]);
    git(
        &other_repository,
        &["config", "user.email", "agentmod@example.invalid"],
    );
    fs::write(other_repository.join("tracked.txt"), "other\n").expect("other file");
    git(&other_repository, &["add", "tracked.txt"]);
    git(&other_repository, &["commit", "-m", "other"]);

    let mut swapped_repository = fixture
        .dependency
        .discover(
            test_authorization("git.worktree_create", "swapped-repository"),
            fixture.repository.clone(),
        )
        .await
        .expect("discover create grant");
    swapped_repository.root = other_repository;
    assert_eq!(
        fixture
            .dependency
            .create_worktree(swapped_repository, destination.clone(), "HEAD".to_owned(),)
            .await,
        Err(GitDependencyError::AuthorizationDenied)
    );
    assert!(!destination.exists());
}

#[tokio::test]
async fn checkpoint_restore_preserves_changes_and_rejects_dirty_target() {
    let fixture = Fixture::new(64 * 1024);
    let create_repository = fixture
        .dependency
        .discover(
            test_authorization("git.checkpoint_create", "checkpoint-create"),
            fixture.repository.clone(),
        )
        .await
        .expect("discover");
    fs::write(
        fixture.repository.join("tracked.txt"),
        "checkpoint change\n",
    )
    .expect("modify");
    fs::create_dir(fixture.repository.join("notes")).expect("notes");
    fs::write(
        fixture.repository.join("notes").join("untracked.txt"),
        "checkpoint note\n",
    )
    .expect("untracked");
    let checkpoint = fixture
        .dependency
        .create_checkpoint(create_repository)
        .await
        .expect("checkpoint");
    assert_eq!(checkpoint.untracked_files, 1);
    assert!(
        checkpoint
            .artifact_directory
            .join("metadata.json")
            .is_file()
    );
    let dirty_restore_repository = fixture
        .dependency
        .discover(
            test_authorization("git.checkpoint_restore", "checkpoint-restore-dirty"),
            fixture.repository.clone(),
        )
        .await
        .expect("discover for dirty restore");
    assert_eq!(
        fixture
            .dependency
            .restore_checkpoint(dirty_restore_repository, checkpoint.checkpoint_id.clone(),)
            .await,
        Err(GitDependencyError::DirtyWorktree)
    );

    git(&fixture.repository, &["checkout", "--", "tracked.txt"]);
    fs::remove_dir_all(fixture.repository.join("notes")).expect("remove untracked fixture");
    let restore_repository = fixture
        .dependency
        .discover(
            test_authorization("git.checkpoint_restore", "checkpoint-restore"),
            fixture.repository.clone(),
        )
        .await
        .expect("discover for restore");
    fixture
        .dependency
        .restore_checkpoint(restore_repository, checkpoint.checkpoint_id)
        .await
        .expect("restore");
    assert_eq!(
        fs::read_to_string(fixture.repository.join("tracked.txt")).expect("tracked"),
        "checkpoint change\n"
    );
    assert_eq!(
        fs::read_to_string(fixture.repository.join("notes").join("untracked.txt"))
            .expect("untracked"),
        "checkpoint note\n"
    );
}

#[tokio::test]
async fn checkpoint_restore_rejects_base_mismatch() {
    let fixture = Fixture::new(64 * 1024);
    let create_repository = fixture
        .dependency
        .discover(
            test_authorization("git.checkpoint_create", "base-mismatch-create"),
            fixture.repository.clone(),
        )
        .await
        .expect("discover");
    fs::write(fixture.repository.join("tracked.txt"), "checkpoint\n").expect("modify");
    let checkpoint = fixture
        .dependency
        .create_checkpoint(create_repository)
        .await
        .expect("checkpoint");
    git(&fixture.repository, &["checkout", "--", "tracked.txt"]);
    fs::write(fixture.repository.join("second.txt"), "second\n").expect("second");
    git(&fixture.repository, &["add", "second.txt"]);
    git(&fixture.repository, &["commit", "-m", "second"]);
    let restore_repository = fixture
        .dependency
        .discover(
            test_authorization("git.checkpoint_restore", "base-mismatch-restore"),
            fixture.repository.clone(),
        )
        .await
        .expect("discover for restore");
    assert_eq!(
        fixture
            .dependency
            .restore_checkpoint(restore_repository, checkpoint.checkpoint_id)
            .await,
        Err(GitDependencyError::CheckpointBaseMismatch)
    );
}
