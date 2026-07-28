//! Real binary recovery regression for a process-host crash after dispatch.

use std::{
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentmod_primitives::{ContentHash, TimestampMillis};
use agentmod_protocol_support::authorization::{
    AuthorizationClaims, AuthorizationKey, seal_authorization,
};
use serde_json::{Value, json};
use tempfile::TempDir;

const OWNER: &str = "owner";
const SESSION: &str = "session";
const AUTHORIZATION_KEY: [u8; 32] = [7; 32];

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the process-boundary recovery regression keeps dispatch, crash, restart, and cleanup explicit"
)]
fn host_restart_reconciles_without_redispatching_the_child() {
    let root = TempDir::new().expect("root");
    let marker = root.path().join("starts.log");
    let fixture = compile_fixture(root.path());
    let mut first_host = spawn_host(root.path(), &fixture);

    let start_arguments = json!({
        "executable": fixture,
        "arguments": [marker],
        "working_directory": null,
        "environment": {},
        "timeout_ms": null,
        "output_limit_bytes": 4096,
        "cleanup": "retain"
    });
    let start = execute(
        &mut first_host,
        "start-call",
        "process.start",
        &start_arguments,
        "018f6f83-7b80-7000-8000-000000000101",
        "start-nonce",
    );
    let process = &start["value"]["result"];
    let process_id = process["process_id"]
        .as_str()
        .expect("process id")
        .to_owned();
    let os_process_id = process["os_process_id"].as_u64().expect("OS process id");
    let _cleanup = ProcessCleanup::new(os_process_id);
    wait_for_start_marker(&marker);

    first_host.child.kill().expect("crash first host");
    first_host.child.wait().expect("wait for first host");
    drop(first_host.stdin);
    drop(first_host.stdout);

    let mut second_host = spawn_host(root.path(), &fixture);
    let list = execute(
        &mut second_host,
        "list-call",
        "process.list",
        &json!({}),
        "018f6f83-7b80-7000-8000-000000000102",
        "list-nonce",
    );
    let processes = list["value"]["result"]["processes"]
        .as_array()
        .expect("process list");
    assert_eq!(processes.len(), 1, "restart must load exactly one record");
    assert_eq!(processes[0]["process_id"], process_id);
    assert_eq!(processes[0]["os_process_id"], os_process_id);
    assert_eq!(
        processes[0]["recovery_status"],
        "recovered_running_unattached"
    );
    assert_eq!(
        start_count(&marker),
        1,
        "recovery must never redispatch the child"
    );

    let reattach = execute_failure(
        &mut second_host,
        "reattach-call",
        "process.reattach",
        &json!({"process_id":process_id}),
        "018f6f83-7b80-7000-8000-000000000103",
        "reattach-nonce",
    );
    assert_eq!(reattach["value"]["code"], "operation_rejected");
    assert_eq!(
        start_count(&marker),
        1,
        "failed-closed reattachment must not redispatch"
    );

    drop(second_host.stdin);
    assert!(
        second_host
            .child
            .wait()
            .expect("second host wait")
            .success()
    );
}

struct Host {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

fn spawn_host(root: &Path, fixture: &str) -> Host {
    let mut child = Command::new(env!("CARGO_BIN_EXE_agentmod-process-host"))
        .current_dir(root)
        .env("AGENTMOD_PROCESS_OWNER", OWNER)
        .env("AGENTMOD_PROCESS_SESSION", SESSION)
        .env("AGENTMOD_PROCESS_AUTH_KEY", "07".repeat(32))
        .env("AGENTMOD_PROCESS_ALLOWED_EXECUTABLES", fixture)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("host");
    let stdin = child.stdin.take().expect("host stdin");
    let stdout = BufReader::new(child.stdout.take().expect("host stdout"));
    Host {
        child,
        stdin,
        stdout,
    }
}

fn execute(
    host: &mut Host,
    call_id: &str,
    tool: &str,
    arguments: &Value,
    cancellation_id: &str,
    nonce: &str,
) -> Value {
    let event = send_and_read_terminal(host, call_id, tool, arguments, cancellation_id, nonce);
    assert_eq!(event["event"], "completed", "event={event}");
    event
}

fn execute_failure(
    host: &mut Host,
    call_id: &str,
    tool: &str,
    arguments: &Value,
    cancellation_id: &str,
    nonce: &str,
) -> Value {
    let event = send_and_read_terminal(host, call_id, tool, arguments, cancellation_id, nonce);
    assert_eq!(event["event"], "failed", "event={event}");
    event
}

fn send_and_read_terminal(
    host: &mut Host,
    call_id: &str,
    tool: &str,
    arguments: &Value,
    cancellation_id: &str,
    nonce: &str,
) -> Value {
    let canonical = canonical_operation(tool, arguments, cancellation_id);
    let digest = ContentHash::digest(&canonical);
    let grant = authorization(call_id, tool, digest, nonce);
    writeln!(
        host.stdin,
        "{}",
        json!({
            "command":"execute",
            "value":{
                "call_id":call_id,
                "tool":tool,
                "arguments":arguments,
                "normalized_digest":digest.to_hex(),
                "authorization_grant":grant,
                "cancellation_id":cancellation_id
            }
        })
    )
    .expect("request");
    host.stdin.flush().expect("request flush");
    loop {
        let mut line = String::new();
        assert_ne!(
            host.stdout.read_line(&mut line).expect("host output"),
            0,
            "host closed before a terminal event"
        );
        let event: Value = serde_json::from_str(&line).expect("event");
        if matches!(
            event["event"].as_str(),
            Some("completed" | "failed" | "cancelled")
        ) {
            return event;
        }
    }
}

fn authorization(call_id: &str, action: &str, digest: ContentHash, nonce: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let now = i64::try_from(now).expect("time");
    seal_authorization(
        &AuthorizationClaims {
            owner: OWNER.to_owned(),
            session: SESSION.to_owned(),
            call_id: call_id.to_owned(),
            action: action.to_owned(),
            normalized_digest: digest,
            issued_at: TimestampMillis::new(now - 1_000),
            expires_at: TimestampMillis::new(now + 30_000),
            nonce: nonce.to_owned(),
        },
        &AuthorizationKey::from_bytes(AUTHORIZATION_KEY),
    )
    .expect("grant")
}

fn wait_for_start_marker(marker: &Path) {
    for _ in 0..100 {
        if start_count(marker) == 1 {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("child never wrote its start marker");
}

fn start_count(marker: &Path) -> usize {
    std::fs::read_to_string(marker)
        .map(|contents| contents.lines().count())
        .unwrap_or(0)
}

fn compile_fixture(root: &Path) -> String {
    let source = root.join("survivor.rs");
    std::fs::write(
        &source,
        r#"use std::{fs::OpenOptions, io::Write, thread, time::Duration};
fn main() {
    let marker = std::env::args().nth(1).expect("marker");
    let mut file = OpenOptions::new().create(true).append(true).open(marker).expect("open");
    writeln!(file, "started").expect("write");
    file.sync_all().expect("sync");
    loop { thread::sleep(Duration::from_secs(60)); }
}"#,
    )
    .expect("source");
    let executable = root.join(if cfg!(windows) {
        "survivor.exe"
    } else {
        "survivor"
    });
    let status = Command::new("rustc")
        .arg(source)
        .arg("-o")
        .arg(&executable)
        .status()
        .expect("rustc");
    assert!(status.success());
    executable.to_string_lossy().into_owned()
}

struct ProcessCleanup {
    process_id: u64,
}

impl ProcessCleanup {
    const fn new(process_id: u64) -> Self {
        Self { process_id }
    }
}

impl Drop for ProcessCleanup {
    fn drop(&mut self) {
        kill_process(self.process_id);
    }
}

#[cfg(windows)]
fn kill_process(process_id: u64) {
    let _ = Command::new("taskkill")
        .args(["/PID", &process_id.to_string(), "/T", "/F"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

#[cfg(unix)]
fn kill_process(process_id: u64) {
    let _ = Command::new("kill")
        .args(["-KILL", &process_id.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

fn canonical_operation(tool: &str, arguments: &Value, cancellation_id: &str) -> Vec<u8> {
    serde_json::to_vec(&(tool, cancellation_id, normalize_json(arguments))).expect("canonical")
}

fn normalize_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted: std::collections::BTreeMap<_, _> = map
                .iter()
                .map(|(key, value)| (key.clone(), normalize_json(value)))
                .collect();
            serde_json::to_value(sorted).expect("normalize")
        }
        Value::Array(values) => Value::Array(values.iter().map(normalize_json).collect()),
        _ => value.clone(),
    }
}
