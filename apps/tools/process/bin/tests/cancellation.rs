//! Real binary regression for cancellation while a foreground request is blocked.

use std::{
    io::{BufRead, BufReader, Read, Write},
    path::Path,
    process::{Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentmod_primitives::{ContentHash, TimestampMillis};
use agentmod_protocol_support::authorization::{
    AuthorizationClaims, AuthorizationKey, seal_authorization,
};
use serde_json::{Value, json};
use tempfile::TempDir;

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the real-process regression keeps the complete authenticated cancellation exchange explicit"
)]
fn foreground_request_can_be_cancelled_concurrently() {
    let root = TempDir::new().expect("root");
    let fixture = compile_fixture(root.path());
    let cancellation = "018f6f83-7b80-7000-8000-000000000099";
    let arguments = json!({
        "executable": fixture,
        "arguments": [],
        "working_directory": null,
        "environment": {},
        "timeout_ms": null,
        "output_limit_bytes": 4096,
        "cleanup": "retain"
    });
    let canonical = canonical_operation("process.run", &arguments, cancellation);
    let digest = ContentHash::digest(&canonical);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let now = i64::try_from(now).expect("time");
    let claims = AuthorizationClaims {
        owner: "owner".to_owned(),
        session: "session".to_owned(),
        call_id: "run-call".to_owned(),
        action: "process.run".to_owned(),
        normalized_digest: digest,
        issued_at: TimestampMillis::new(now - 1_000),
        expires_at: TimestampMillis::new(now + 30_000),
        nonce: "run-nonce".to_owned(),
    };
    let grant = seal_authorization(&claims, &AuthorizationKey::from_bytes([7; 32])).expect("grant");
    let mut host = Command::new(env!("CARGO_BIN_EXE_agentmod-process-host"))
        .current_dir(root.path())
        .env("AGENTMOD_PROCESS_OWNER", "owner")
        .env("AGENTMOD_PROCESS_SESSION", "session")
        .env("AGENTMOD_PROCESS_AUTH_KEY", "07".repeat(32))
        .env("AGENTMOD_PROCESS_ALLOWED_EXECUTABLES", &fixture)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("host");
    let mut stdin = host.stdin.take().expect("stdin");
    let stdout = host.stdout.take().expect("stdout");
    let mut stderr = host.stderr.take().expect("stderr");
    writeln!(
        stdin,
        "{}",
        json!({
            "command":"execute",
            "value":{
                "call_id":"run-call",
                "tool":"process.run",
                "arguments":arguments,
                "normalized_digest":digest.to_hex(),
                "authorization_grant":grant,
                "cancellation_id":cancellation
            }
        })
    )
    .expect("run request");
    stdin.flush().expect("flush");
    thread::sleep(Duration::from_millis(300));
    writeln!(
        stdin,
        "{}",
        json!({
            "command":"cancel",
            "value":{"cancellation_id":cancellation}
        })
    )
    .expect("cancel request");
    stdin.flush().expect("flush");

    let (event_sender, event_receiver) = mpsc::channel();
    let reader_thread = thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            if event_sender.send(line).is_err() {
                break;
            }
        }
    });
    let mut saw_cancelled = false;
    let mut saw_completed = false;
    let mut observed = Vec::new();
    for _ in 0..6 {
        let line = match event_receiver.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(line)) => line,
            Ok(Err(error)) => panic!("host output read failed: {error}"),
            Err(error) => {
                let _ = host.kill();
                let _ = host.wait();
                let mut diagnostics = String::new();
                let _ = stderr.read_to_string(&mut diagnostics);
                let _ = reader_thread.join();
                panic!(
                    "timed out waiting for process events: {error}; observed={observed:?}; stderr={diagnostics}"
                );
            }
        };
        let event: Value = serde_json::from_str(&line).expect("event");
        observed.push(event.clone());
        saw_cancelled |= event["event"] == "cancelled";
        saw_completed |= event["event"] == "completed";
        if saw_cancelled && saw_completed {
            break;
        }
    }
    assert!(
        saw_cancelled,
        "cancel command must be serviced concurrently"
    );
    assert!(
        saw_completed,
        "foreground request must terminate after cancellation"
    );
    drop(stdin);
    assert!(host.wait().expect("host wait").success());
    reader_thread.join().expect("reader");
}

fn compile_fixture(root: &Path) -> String {
    let source = root.join("blocking.rs");
    std::fs::write(
        &source,
        "fn main(){let mut s=String::new();std::io::stdin().read_line(&mut s).unwrap();}",
    )
    .expect("source");
    let executable = root.join(if cfg!(windows) {
        "blocking.exe"
    } else {
        "blocking"
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
