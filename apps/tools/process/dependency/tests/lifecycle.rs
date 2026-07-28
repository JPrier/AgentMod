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
    DependencyReadOutputRequest, DependencyRecoveryState, DependencyResizeTerminalRequest,
    DependencyStartProcessRequest, DependencyTerminalSize, ProcessDependencyConfig,
    ProcessDependencyError, ProcessDependencyPort, TokioProcessDependency,
    canonical_control_operation, canonical_input_operation, canonical_list_operation,
    canonical_read_operation, canonical_resize_operation, canonical_start_operation,
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
        terminal_size: None,
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

fn terminal_start_request(root: &TempDir, executable: &Path) -> DependencyStartProcessRequest {
    let mut request = start_request(
        root,
        executable,
        "pty-start",
        "pty-cancel",
        "pty-start-nonce",
        false,
        DependencyCleanupPolicy::Retain,
    );
    request.terminal_size = Some(DependencyTerminalSize {
        columns: 80,
        rows: 24,
        pixel_width: 0,
        pixel_height: 0,
    });
    let canonical = canonical_start_operation(&request).expect("canonical PTY start");
    request.authorization = authorization(
        "owner",
        "session",
        "pty-start",
        "process.start_pty",
        "pty-cancel",
        "pty-start-nonce",
        canonical,
    );
    request
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

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the process-level PTY acceptance test keeps one exact grant per lifecycle action visible"
)]
async fn terminal_supports_input_resize_detach_reattach_and_durable_output() {
    let root = TempDir::new().expect("root");
    let executable = compile_fixture(&root);
    let dependency = dependency(&root);
    let started = dependency
        .start(terminal_start_request(&root, &executable))
        .await
        .expect("start PTY");
    assert!(started.terminal);
    assert!(started.os_process_id.is_some());
    assert_eq!(
        started.terminal_size,
        Some(DependencyTerminalSize {
            columns: 80,
            rows: 24,
            pixel_width: 0,
            pixel_height: 0,
        })
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    let process_id = started.process_id.as_str().to_owned();

    let resized_size = DependencyTerminalSize {
        columns: 100,
        rows: 40,
        pixel_width: 8,
        pixel_height: 16,
    };
    let mut resize = DependencyResizeTerminalRequest {
        authorization: authorization(
            "owner",
            "session",
            "pty-resize",
            "process.resize",
            "pty-resize-cancel",
            "pty-resize-nonce",
            Vec::new(),
        ),
        process_id: process_id.clone(),
        size: resized_size,
    };
    resize.authorization = authorization(
        "owner",
        "session",
        "pty-resize",
        "process.resize",
        "pty-resize-cancel",
        "pty-resize-nonce",
        canonical_resize_operation(&resize).expect("canonical resize"),
    );
    let resized = dependency.resize(resize).await.expect("resize");
    assert_eq!(resized.terminal_size, Some(resized_size));

    let detached = dependency
        .detach(control(
            &process_id,
            "owner",
            "pty-detach",
            "process.detach",
            "pty-detach-nonce",
        ))
        .await
        .expect("detach");
    assert!(detached.detached);
    let reattached = dependency
        .reattach(control(
            &process_id,
            "owner",
            "pty-reattach",
            "process.reattach",
            "pty-reattach-nonce",
        ))
        .await
        .expect("reattach");
    assert!(!reattached.detached);

    let mut input = DependencyProcessInputRequest {
        authorization: authorization(
            "owner",
            "session",
            "pty-input",
            "process.input",
            "pty-input-cancel",
            "pty-input-nonce",
            Vec::new(),
        ),
        process_id: process_id.clone(),
        bytes: b"terminal-input\r\n".to_vec(),
        close: false,
    };
    input.authorization = authorization(
        "owner",
        "session",
        "pty-input",
        "process.input",
        "pty-input-cancel",
        "pty-input-nonce",
        canonical_input_operation(&input).expect("canonical input"),
    );
    dependency.input(input).await.expect("input");
    let completed = dependency
        .wait(control(
            &process_id,
            "owner",
            "pty-wait",
            "process.wait",
            "pty-wait-nonce",
        ))
        .await
        .expect("wait");
    assert_eq!(completed.state, DependencyProcessState::Exited);
    assert!(
        completed.exit.as_ref().is_some_and(|exit| exit.success),
        "PTY process did not exit successfully: {completed:?}"
    );

    let mut read = DependencyReadOutputRequest {
        authorization: authorization(
            "owner",
            "session",
            "pty-read",
            "process.read",
            "pty-read-cancel",
            "pty-read-nonce",
            Vec::new(),
        ),
        process_id,
        stream: DependencyOutputStream::Terminal,
        offset: 0,
        length: 4096,
    };
    read.authorization = authorization(
        "owner",
        "session",
        "pty-read",
        "process.read",
        "pty-read-cancel",
        "pty-read-nonce",
        canonical_read_operation(&read).expect("canonical read"),
    );
    let output = dependency.read_output(read).await.expect("read terminal");
    let output = String::from_utf8_lossy(&output.bytes);
    assert!(output.contains("fixture-stderr"));
    assert!(output.contains("fixture-stdout:terminal-input"));
}

#[tokio::test]
async fn restart_reconciles_exact_live_identity_without_redispatch_or_reattachment() {
    let root = TempDir::new().expect("root");
    let executable = compile_fixture(&root);
    let original = dependency(&root);
    let started = original
        .start(start_request(
            &root,
            &executable,
            "recovery-start",
            "recovery-cancel",
            "recovery-start-nonce",
            false,
            DependencyCleanupPolicy::Retain,
        ))
        .await
        .expect("start");
    let process_id = started.process_id.as_str().to_owned();
    let os_process_id = started.os_process_id;
    let os_start_time = started.os_start_time;
    assert!(os_process_id.is_some());
    assert!(os_start_time.is_some());

    let recovered = dependency(&root);
    let records = recovered
        .list(DependencyListRequest {
            authorization: authorization(
                "owner",
                "session",
                "recovery-list",
                "process.list",
                "recovery-list-cancel",
                "recovery-list-nonce",
                canonical_list_operation("recovery-list-cancel").expect("canonical list"),
            ),
        })
        .await
        .expect("list recovered");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].process_id.as_str(), process_id);
    assert_eq!(records[0].os_process_id, os_process_id);
    assert_eq!(records[0].os_start_time, os_start_time);
    assert_eq!(
        records[0].recovery_state,
        DependencyRecoveryState::RecoveredRunningUnattached
    );
    assert_eq!(
        recovered
            .reattach(control(
                &process_id,
                "owner",
                "recovery-reattach",
                "process.reattach",
                "recovery-reattach-nonce",
            ))
            .await,
        Err(ProcessDependencyError::ReattachmentUnavailable)
    );

    original
        .cancel(DependencyCancelRequest {
            identity: DependencyIdentity {
                owner_id: "owner".to_owned(),
                session_id: "session".to_owned(),
            },
            cancellation_id: "recovery-cancel".to_owned(),
        })
        .await
        .expect("cleanup original");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let records = recovered
        .list(DependencyListRequest {
            authorization: authorization(
                "owner",
                "session",
                "recovery-list-after-exit",
                "process.list",
                "recovery-list-after-exit-cancel",
                "recovery-list-after-exit-nonce",
                canonical_list_operation("recovery-list-after-exit-cancel")
                    .expect("canonical list"),
            ),
        })
        .await
        .expect("list after exit");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].state, DependencyProcessState::Exited);
    assert_eq!(
        records[0].recovery_state,
        DependencyRecoveryState::RecoveredExited
    );
}

#[tokio::test]
async fn restart_recovers_completed_record_and_durable_output_ranges() {
    let root = TempDir::new().expect("root");
    let executable = compile_fixture(&root);
    let original = dependency(&root);
    let started = original
        .start(start_request(
            &root,
            &executable,
            "completed-start",
            "completed-cancel",
            "completed-start-nonce",
            false,
            DependencyCleanupPolicy::Retain,
        ))
        .await
        .expect("start");
    let process_id = started.process_id.as_str().to_owned();
    let mut input = DependencyProcessInputRequest {
        authorization: authorization(
            "owner",
            "session",
            "completed-input",
            "process.input",
            "completed-input-cancel",
            "completed-input-nonce",
            Vec::new(),
        ),
        process_id: process_id.clone(),
        bytes: b"persisted\n".to_vec(),
        close: true,
    };
    input.authorization = authorization(
        "owner",
        "session",
        "completed-input",
        "process.input",
        "completed-input-cancel",
        "completed-input-nonce",
        canonical_input_operation(&input).expect("canonical input"),
    );
    original.input(input).await.expect("input");
    original
        .wait(control(
            &process_id,
            "owner",
            "completed-wait",
            "process.wait",
            "completed-wait-nonce",
        ))
        .await
        .expect("wait");
    drop(original);

    let recovered = dependency(&root);
    let records = recovered
        .list(DependencyListRequest {
            authorization: authorization(
                "owner",
                "session",
                "completed-list",
                "process.list",
                "completed-list-cancel",
                "completed-list-nonce",
                canonical_list_operation("completed-list-cancel").expect("canonical list"),
            ),
        })
        .await
        .expect("list");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].state, DependencyProcessState::Exited);
    assert_eq!(
        records[0].recovery_state,
        DependencyRecoveryState::RecoveredExited
    );

    let mut read = DependencyReadOutputRequest {
        authorization: authorization(
            "owner",
            "session",
            "completed-read",
            "process.read",
            "completed-read-cancel",
            "completed-read-nonce",
            Vec::new(),
        ),
        process_id,
        stream: DependencyOutputStream::Stdout,
        offset: 0,
        length: 4096,
    };
    read.authorization = authorization(
        "owner",
        "session",
        "completed-read",
        "process.read",
        "completed-read-cancel",
        "completed-read-nonce",
        canonical_read_operation(&read).expect("canonical read"),
    );
    let output = recovered.read_output(read).await.expect("read");
    assert!(String::from_utf8_lossy(&output.bytes).contains("fixture-stdout:persisted"));
}
