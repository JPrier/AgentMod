//! Authenticated process lifecycle, ownership, cancellation, and cleanup regressions.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentmod_primitives::{ContentHash, TimestampMillis};
use agentmod_process_host_dependency::{
    DependencyAuthorization, DependencyCancelRequest, DependencyCleanupPolicy,
    DependencyExecutablePolicy, DependencyIdentity, DependencyListRequest, DependencyOutputStream,
    DependencyProcessInputRequest, DependencyProcessRequest, DependencyProcessState,
    DependencyReadOutputRequest, DependencyStartProcessRequest, ProcessDependencyConfig,
    ProcessDependencyError, ProcessDependencyPort, TokioProcessDependency,
    canonical_control_operation, canonical_input_operation, canonical_list_operation,
    canonical_read_operation, canonical_start_operation,
};
use agentmod_protocol_support::authorization::{
    AuthorizationClaims, AuthorizationKey, seal_authorization,
};
use tempfile::TempDir;

const KEY: [u8; 32] = [7; 32];

fn compile_fixture(root: &TempDir) -> PathBuf {
    let executable = root.path().join(if cfg!(windows) {
        "fixture.exe"
    } else {
        "fixture"
    });
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("process_fixture.rs");
    let status = Command::new("rustc")
        .arg(source)
        .arg("-o")
        .arg(&executable)
        .status()
        .expect("run rustc");
    assert!(status.success());
    executable
}

fn dependency(root: &TempDir) -> TokioProcessDependency {
    TokioProcessDependency::new(ProcessDependencyConfig {
        storage_root: root.path().join("storage"),
        log_root: root.path().join("storage/logs"),
        authorization_key_hex: "07".repeat(32),
        owner_id: "owner".to_owned(),
        session_id: "session".to_owned(),
        inherited_environment_allowlist: BTreeSet::from([
            "PATH".to_owned(),
            "SYSTEMROOT".to_owned(),
            "WINDIR".to_owned(),
        ]),
        max_input_bytes: 1024,
        max_range_bytes: 4096,
        max_arguments: 16,
        max_argument_bytes: 4096,
        max_environment_entries: 8,
        max_environment_bytes: 4096,
        max_active_processes: 8,
        max_total_retained_bytes: 128 * 1024,
        drain_timeout: Duration::from_secs(3),
        input_write_timeout: Duration::from_millis(250),
        max_replay_entries: 128,
        max_completed_entries: 16,
        max_waiters_per_process: 8,
        executable_policy: BTreeMap::new(),
        default_executable_policy: DependencyExecutablePolicy::Allow,
    })
    .expect("dependency")
}

fn authorization(
    owner: &str,
    session: &str,
    call: &str,
    tool: &str,
    cancellation: &str,
    nonce: &str,
    operation: Vec<u8>,
) -> DependencyAuthorization {
    let digest = ContentHash::digest(&operation);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let now = i64::try_from(now).expect("timestamp");
    let claims = AuthorizationClaims {
        owner: owner.to_owned(),
        session: session.to_owned(),
        call_id: call.to_owned(),
        action: tool.to_owned(),
        normalized_digest: digest,
        issued_at: TimestampMillis::new(now - 1_000),
        expires_at: TimestampMillis::new(now + 30_000),
        nonce: nonce.to_owned(),
    };
    DependencyAuthorization {
        identity: DependencyIdentity {
            owner_id: owner.to_owned(),
            session_id: session.to_owned(),
        },
        call_id: call.to_owned(),
        tool: tool.to_owned(),
        normalized_digest: digest.to_hex(),
        grant: seal_authorization(&claims, &AuthorizationKey::from_bytes(KEY)).expect("grant"),
        cancellation_id: cancellation.to_owned(),
        canonical_operation: operation,
    }
}

fn start_request(
    root: &TempDir,
    executable: &Path,
    call: &str,
    cancellation: &str,
    nonce: &str,
    foreground: bool,
    cleanup: DependencyCleanupPolicy,
) -> DependencyStartProcessRequest {
    let tool = if foreground {
        "process.run"
    } else {
        "process.start"
    };
    let mut request = DependencyStartProcessRequest {
        authorization: authorization(
            "owner",
            "session",
            call,
            tool,
            cancellation,
            nonce,
            Vec::new(),
        ),
        workspace_root: root.path().to_path_buf(),
        requested_working_directory: Some(root.path().to_path_buf()),
        working_directory: root.path().to_path_buf(),
        executable: executable.to_string_lossy().into_owned(),
        arguments: Vec::new(),
        environment: BTreeMap::new(),
        timeout: Some(Duration::from_secs(10)),
        output_limit_bytes: 4096,
        cleanup,
        foreground,
    };
    let canonical = canonical_start_operation(&request).expect("canonical start");
    request.authorization = authorization(
        "owner",
        "session",
        call,
        tool,
        cancellation,
        nonce,
        canonical,
    );
    request
}

fn control(
    process_id: &str,
    owner: &str,
    call: &str,
    tool: &str,
    nonce: &str,
) -> DependencyProcessRequest {
    let canonical = canonical_control_operation(tool, call, process_id).expect("canonical control");
    DependencyProcessRequest {
        authorization: authorization(owner, "session", call, tool, call, nonce, canonical),
        process_id: process_id.to_owned(),
    }
}

#[tokio::test]
async fn foreground_captures_output_before_remove_always_cleanup() {
    let root = TempDir::new().expect("root");
    let executable = compile_fixture(&root);
    let dependency = dependency(&root);
    let auth_cancel = "cancel-run".to_owned();
    let dependency_for_input = dependency.clone();
    let start = tokio::spawn(async move {
        dependency
            .start(start_request(
                &root,
                &executable,
                "run",
                &auth_cancel,
                "n1",
                true,
                DependencyCleanupPolicy::RemoveLogsAlways,
            ))
            .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let records = dependency_for_input
        .list(DependencyListRequest {
            authorization: authorization(
                "owner",
                "session",
                "list",
                "process.list",
                "list-cancel",
                "n2",
                canonical_list_operation("list-cancel").expect("canonical list"),
            ),
        })
        .await
        .expect("list");
    let id = records[0].process_id.as_str().to_owned();
    let mut input_request = DependencyProcessInputRequest {
        authorization: authorization(
            "owner",
            "session",
            "input",
            "process.input",
            "input-cancel",
            "n3",
            Vec::new(),
        ),
        process_id: id,
        bytes: b"hello\n".to_vec(),
        close: true,
    };
    let canonical = canonical_input_operation(&input_request).expect("canonical input");
    input_request.authorization = authorization(
        "owner",
        "session",
        "input",
        "process.input",
        "input-cancel",
        "n3",
        canonical,
    );
    dependency_for_input
        .input(input_request)
        .await
        .expect("input");
    let completed = start.await.expect("join").expect("completed");
    assert_eq!(completed.state, DependencyProcessState::Exited);
    let stdout = String::from_utf8(completed.stdout_projection).expect("utf8");
    assert!(stdout.contains("fixture-stdout:hello"));
    assert!(stdout.contains("openai_present=false"));
    assert!(completed.logs_removed);
    assert!(!completed.cleanup_failed);
    assert!(
        dependency_for_input
            .cancel(DependencyCancelRequest {
                identity: DependencyIdentity {
                    owner_id: "owner".to_owned(),
                    session_id: "session".to_owned(),
                },
                cancellation_id: "cancel-run".to_owned(),
            })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn cancellation_token_kills_running_process_and_owner_is_enforced() {
    let root = TempDir::new().expect("root");
    let executable = compile_fixture(&root);
    let dependency = dependency(&root);
    let cancellation = "cancel-running";
    let started = dependency
        .start(start_request(
            &root,
            &executable,
            "start",
            cancellation,
            "s1",
            false,
            DependencyCleanupPolicy::Retain,
        ))
        .await
        .expect("start");
    let id = started.process_id.as_str().to_owned();
    assert_eq!(
        dependency
            .wait(control(&id, "other", "wrong", "process.wait", "s2"))
            .await,
        Err(ProcessDependencyError::AuthorizationDenied)
    );
    let cancelled = dependency
        .cancel(DependencyCancelRequest {
            identity: DependencyIdentity {
                owner_id: "owner".to_owned(),
                session_id: "session".to_owned(),
            },
            cancellation_id: cancellation.to_owned(),
        })
        .await
        .expect("cancel");
    assert_eq!(cancelled, id);
    let completed = dependency
        .wait(control(&id, "owner", "wait", "process.wait", "s3"))
        .await
        .expect("wait");
    assert!(completed.exit.is_some());
}

#[tokio::test]
async fn output_read_is_owner_scoped() {
    let root = TempDir::new().expect("root");
    let executable = compile_fixture(&root);
    let dependency = dependency(&root);
    let started = dependency
        .start(start_request(
            &root,
            &executable,
            "start2",
            "c2",
            "o1",
            false,
            DependencyCleanupPolicy::Retain,
        ))
        .await
        .expect("start");
    let mut read_request = DependencyReadOutputRequest {
        authorization: authorization(
            "other",
            "session",
            "read",
            "process.read",
            "read-cancel",
            "o2",
            Vec::new(),
        ),
        process_id: started.process_id.as_str().to_owned(),
        stream: DependencyOutputStream::Stdout,
        offset: 0,
        length: 10,
    };
    let canonical = canonical_read_operation(&read_request).expect("canonical read");
    read_request.authorization = authorization(
        "other",
        "session",
        "read",
        "process.read",
        "read-cancel",
        "o2",
        canonical,
    );
    assert_eq!(
        dependency.read_output(read_request).await,
        Err(ProcessDependencyError::AuthorizationDenied)
    );
    dependency
        .cancel(DependencyCancelRequest {
            identity: DependencyIdentity {
                owner_id: "owner".to_owned(),
                session_id: "session".to_owned(),
            },
            cancellation_id: "c2".to_owned(),
        })
        .await
        .expect("cleanup");
}
