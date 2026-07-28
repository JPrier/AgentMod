//! Reconnectable endpoint E2E for a live PTY owned by a surviving host.

use std::{
    collections::BTreeSet,
    io,
    path::Path,
    process::{Child, Command, Stdio},
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentmod_primitives::{
    CancellationId, CausationId, ContentHash, CorrelationId, IdempotencyId, RequestId,
    TimestampMillis,
};
use agentmod_protocol_support::{
    FrameHeader, FrameKind, Handshake, Negotiated, WireFrame,
    authorization::{AuthorizationClaims, AuthorizationKey, seal_authorization},
    read_frame, write_frame,
};
use agentmod_tool_protocol::{PROTOCOL_VERSION, ToolHostCommand, ToolHostEvent};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::sleep,
};
use uuid::Uuid;

const OWNER: &str = "owner";
const SESSION: &str = "session";
const KEY: [u8; 32] = [7; 32];
const MAXIMUM_FRAME_BYTES: usize = 1024 * 1024;

trait LocalStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> LocalStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the E2E keeps host launch, disconnect, reconnect, PTY I/O, and cleanup explicit"
)]
async fn replacement_client_reattaches_to_the_same_live_pty() {
    let root = TempDir::new().expect("root");
    let fixture = compile_fixture(root.path());
    let endpoint = endpoint(root.path());
    let mut host = spawn_host(root.path(), &fixture, &endpoint);
    let mut first = connect_with_retry(&endpoint).await;
    handshake(&mut first).await;

    let start = execute(
        &mut first,
        "start",
        "process.start_pty",
        &json!({
            "executable":fixture,
            "arguments":[],
            "working_directory":null,
            "environment":{},
            "timeout_ms":null,
            "output_limit_bytes":65536,
            "cleanup":"retain",
            "terminal":{"columns":80,"rows":24,"pixel_width":0,"pixel_height":0}
        }),
        "018f6f83-7b80-7000-8000-000000000201",
        "start-nonce",
    )
    .await;
    let process_id = start["value"]["result"]["process_id"]
        .as_str()
        .expect("process id")
        .to_owned();
    assert_eq!(start["value"]["result"]["recovery_status"], "live");

    let detached = execute(
        &mut first,
        "detach",
        "process.detach",
        &json!({"process_id":process_id}),
        "018f6f83-7b80-7000-8000-000000000202",
        "detach-nonce",
    )
    .await;
    assert_eq!(detached["value"]["result"]["detached"], true);
    drop(first);
    sleep(Duration::from_millis(750)).await;
    assert!(
        host.child.try_wait().expect("host status").is_none(),
        "idle checks must retain a host that still owns a live child"
    );

    let mut replacement = connect_with_retry(&endpoint).await;
    handshake(&mut replacement).await;
    let reattached = execute(
        &mut replacement,
        "reattach",
        "process.reattach",
        &json!({"process_id":process_id}),
        "018f6f83-7b80-7000-8000-000000000203",
        "reattach-nonce",
    )
    .await;
    assert_eq!(reattached["value"]["result"]["process_id"], process_id);
    assert_eq!(reattached["value"]["result"]["recovery_status"], "live");
    assert_eq!(reattached["value"]["result"]["detached"], false);

    execute(
        &mut replacement,
        "input",
        "process.input",
        &json!({"process_id":process_id,"content":"hello-after-reconnect\r\n","close":false}),
        "018f6f83-7b80-7000-8000-000000000204",
        "input-nonce",
    )
    .await;

    let mut observed = String::new();
    for attempt in 0..50 {
        let read = execute(
            &mut replacement,
            &format!("read-{attempt}"),
            "process.read",
            &json!({"process_id":process_id,"stream":"terminal","offset":0,"length":4096}),
            &format!("018f6f83-7b80-7000-8000-{:012}", 205 + attempt),
            &format!("read-nonce-{attempt}"),
        )
        .await;
        observed.push_str(read["captured_output"].as_str().unwrap_or_default());
        if observed.contains("echo:hello-after-reconnect") {
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    assert!(
        observed.contains("echo:hello-after-reconnect"),
        "replacement client must retain access to the original PTY; output={observed:?}"
    );

    execute(
        &mut replacement,
        "exit-input",
        "process.input",
        &json!({"process_id":process_id,"content":"exit\r\n","close":false}),
        "018f6f83-7b80-7000-8000-000000000299",
        "exit-input-nonce",
    )
    .await;
    execute(
        &mut replacement,
        "wait",
        "process.wait",
        &json!({"process_id":process_id}),
        "018f6f83-7b80-7000-8000-000000000300",
        "wait-nonce",
    )
    .await;
    drop(replacement);
    assert!(
        host.wait_for_exit(Duration::from_secs(3)),
        "host must exit after the final connection and live child are gone"
    );
}

struct HostGuard {
    child: Child,
}

impl HostGuard {
    fn wait_for_exit(&mut self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if self.child.try_wait().ok().flatten().is_some() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        false
    }

    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for HostGuard {
    fn drop(&mut self) {
        self.stop();
    }
}

fn spawn_host(root: &Path, fixture: &str, endpoint: &str) -> HostGuard {
    let child = Command::new(env!("CARGO_BIN_EXE_agentmod-process-host"))
        .current_dir(root)
        .env("AGENTMOD_PROCESS_OWNER", OWNER)
        .env("AGENTMOD_PROCESS_SESSION", SESSION)
        .env("AGENTMOD_PROCESS_AUTH_KEY", "07".repeat(32))
        .env("AGENTMOD_PROCESS_ALLOWED_EXECUTABLES", fixture)
        .env("AGENTMOD_PROCESS_ENDPOINT", endpoint)
        .env("AGENTMOD_PROCESS_IDLE_TIMEOUT_MS", "500")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("host");
    HostGuard { child }
}

async fn connect_with_retry(endpoint: &str) -> Box<dyn LocalStream> {
    for _ in 0..100 {
        if let Ok(stream) = open_endpoint(endpoint).await {
            return stream;
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!("process host endpoint did not become ready");
}

#[cfg(unix)]
async fn open_endpoint(endpoint: &str) -> io::Result<Box<dyn LocalStream>> {
    tokio::net::UnixStream::connect(endpoint)
        .await
        .map(|stream| Box::new(stream) as Box<dyn LocalStream>)
}

#[cfg(windows)]
async fn open_endpoint(endpoint: &str) -> io::Result<Box<dyn LocalStream>> {
    tokio::task::yield_now().await;
    tokio::net::windows::named_pipe::ClientOptions::new()
        .open(endpoint)
        .map(|stream| Box::new(stream) as Box<dyn LocalStream>)
}

async fn handshake(stream: &mut Box<dyn LocalStream>) {
    let request = header(FrameKind::Handshake, None);
    write_frame(
        stream,
        &WireFrame {
            header: request.clone(),
            payload: Handshake {
                supported_versions: vec![PROTOCOL_VERSION],
                capabilities: BTreeSet::from([
                    String::from("request_response"),
                    String::from("streaming"),
                ]),
                authorization_token: "07".repeat(32),
            },
        },
        MAXIMUM_FRAME_BYTES,
    )
    .await
    .expect("handshake");
    let response: WireFrame<Negotiated> = read_frame(stream, MAXIMUM_FRAME_BYTES)
        .await
        .expect("negotiated");
    assert_eq!(response.header.kind, FrameKind::Response);
    assert_eq!(response.header.request_id, request.request_id);
    assert_eq!(response.payload.version, PROTOCOL_VERSION);
}

async fn execute(
    stream: &mut Box<dyn LocalStream>,
    call_id: &str,
    tool: &str,
    arguments: &Value,
    cancellation_id: &str,
    nonce: &str,
) -> Value {
    let cancellation_id = CancellationId::from_str(cancellation_id).expect("cancellation ID");
    let canonical = canonical_operation(tool, arguments, &cancellation_id.to_string());
    let digest = ContentHash::digest(&canonical);
    let request = header(FrameKind::Request, Some(cancellation_id));
    write_frame(
        stream,
        &WireFrame {
            header: request.clone(),
            payload: ToolHostCommand::Execute {
                call_id: call_id.to_owned(),
                tool: tool.to_owned(),
                arguments: arguments.clone(),
                normalized_digest: digest.to_hex(),
                authorization_grant: authorization(call_id, tool, digest, nonce),
                cancellation_id,
            },
        },
        MAXIMUM_FRAME_BYTES,
    )
    .await
    .expect("request");
    let mut expected_sequence = 1_u64;
    let mut captured_output = String::new();
    loop {
        let response: WireFrame<ToolHostEvent> = read_frame(stream, MAXIMUM_FRAME_BYTES)
            .await
            .expect("response");
        assert_eq!(response.header.request_id, request.request_id);
        assert_eq!(
            response.header.stream_sequence,
            Some(expected_sequence),
            "response sequence"
        );
        if matches!(
            response.header.kind,
            FrameKind::Response | FrameKind::StreamEnd
        ) {
            let mut value = serde_json::to_value(response.payload).expect("event");
            assert_eq!(value["event"], "completed", "event={value}");
            value["captured_output"] = Value::String(captured_output);
            return value;
        }
        assert_eq!(response.header.kind, FrameKind::StreamItem);
        if let ToolHostEvent::Output { content, .. } = response.payload {
            captured_output.push_str(&content);
        }
        expected_sequence += 1;
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
        &AuthorizationKey::from_bytes(KEY),
    )
    .expect("grant")
}

fn header(kind: FrameKind, cancellation_id: Option<CancellationId>) -> FrameHeader {
    FrameHeader {
        family: String::from("tool"),
        version: PROTOCOL_VERSION,
        kind,
        request_id: RequestId::from_uuid(Uuid::now_v7()),
        stream_sequence: None,
        correlation_id: CorrelationId::from_uuid(Uuid::now_v7()),
        causation_id: CausationId::from_uuid(Uuid::now_v7()),
        idempotency_id: IdempotencyId::from_uuid(Uuid::now_v7()),
        cancellation_id,
    }
}

#[cfg(unix)]
fn endpoint(root: &Path) -> String {
    root.join("process.sock").to_string_lossy().into_owned()
}

#[cfg(windows)]
fn endpoint(_root: &Path) -> String {
    format!(r"\\.\pipe\agentmod-process-test-{}", Uuid::now_v7())
}

fn compile_fixture(root: &Path) -> String {
    let source = root.join("interactive.rs");
    std::fs::write(
        &source,
        r#"use std::io::{self, BufRead, Write};
fn main() {
    std::thread::spawn(|| {
        std::thread::sleep(std::time::Duration::from_secs(15));
        std::process::exit(124);
    });
    println!("ready");
    io::stdout().flush().expect("flush");
    for line in io::stdin().lock().lines() {
        let line = line.expect("line");
        println!("echo:{line}");
        io::stdout().flush().expect("flush");
        if line == "exit" { break; }
    }
}"#,
    )
    .expect("source");
    let executable = root.join(if cfg!(windows) {
        "interactive.exe"
    } else {
        "interactive"
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
