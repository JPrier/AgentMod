//! Real subprocess coverage for the filesystem host's JSONL protocol.

use std::{
    io::{BufRead, BufReader, Write},
    process::{Command, Stdio},
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use agentmod_filesystem_host_dependency::{
    DependencyRequest, ReadRange, ReadRequest, canonical_operation_digest,
};
use agentmod_primitives::{CancellationId, TimestampMillis};
use agentmod_protocol_support::authorization::{
    AuthorizationClaims, AuthorizationKey, seal_authorization,
};
use agentmod_tool_protocol::{ToolHostCommand, ToolHostEvent};

#[test]
fn authorized_read_crosses_the_real_process_protocol() {
    let workspace = tempfile::tempdir().expect("workspace");
    std::fs::write(workspace.path().join("fixture.txt"), "line one\nline two\n").expect("fixture");
    let owner = "runtime-test";
    let session = "session-test";
    let call_id = "call-1";
    let action = "filesystem.read";
    let operation = DependencyRequest::Read(ReadRequest {
        path: "fixture.txt".into(),
        range: ReadRange::All,
        max_projection_bytes: 64 * 1024,
    });
    let digest = canonical_operation_digest(&operation).expect("canonical digest");
    let key_bytes = [7_u8; 32];
    let key = AuthorizationKey::from_bytes(key_bytes);
    let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock");
    let issued_at = i64::try_from(now.as_millis()).expect("time");
    let grant = seal_authorization(
        &AuthorizationClaims {
            owner: owner.into(),
            session: session.into(),
            call_id: call_id.into(),
            action: action.into(),
            normalized_digest: digest,
            issued_at: TimestampMillis::new(issued_at),
            expires_at: TimestampMillis::new(issued_at + 30_000),
            nonce: "nonce-1".into(),
        },
        &key,
    )
    .expect("grant");
    let cancellation_id =
        CancellationId::from_str("018f6f83-7b80-7000-8000-000000000001").expect("cancellation");
    let command = ToolHostCommand::Execute {
        call_id: call_id.into(),
        tool: action.into(),
        arguments: serde_json::json!({"path":"fixture.txt"}),
        normalized_digest: digest.to_hex(),
        authorization_grant: grant,
        cancellation_id,
    };
    let mut child = Command::new(env!("CARGO_BIN_EXE_agentmod-filesystem-host"))
        .current_dir(workspace.path())
        .env_clear()
        .env("AGENTMOD_FILESYSTEM_AUTH_KEY_HEX", encode_hex(&key_bytes))
        .env("AGENTMOD_FILESYSTEM_AUTH_OWNER", owner)
        .env("AGENTMOD_FILESYSTEM_AUTH_SESSION", session)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn host");
    let mut stdin = child.stdin.take().expect("stdin");
    serde_json::to_writer(&mut stdin, &command).expect("encode command");
    stdin.write_all(b"\n").expect("newline");
    stdin.flush().expect("flush");
    drop(stdin);

    let stdout = child.stdout.take().expect("stdout");
    let events: Vec<ToolHostEvent> = BufReader::new(stdout)
        .lines()
        .map(|line| serde_json::from_str(&line.expect("line")).expect("tool event"))
        .collect();
    let status = child.wait().expect("wait");

    assert!(status.success());
    assert!(matches!(
        events.as_slice(),
        [
            ToolHostEvent::Started { call_id: started },
            ToolHostEvent::Completed {
                call_id: completed,
                result,
                truncated: false,
                ..
            }
        ] if started == call_id
            && completed == call_id
            && result["lines"][0]["text"] == "line one"
    ));
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
