//! Independent harness process conformance tests.
//!
//! These spawn the real `agentmod-harness-fixture` binary over bounded JSONL
//! stdio and prove the harness protocol is not hard-coded to the native
//! implementation: distinct identity, distinct capabilities, deterministic
//! streaming, tool-call continuation, cancellation, and negative capability
//! guards all work through the shared wire contract.

use std::{
    io::{BufRead, BufReader, Write},
    process::{Child, ChildStdin, Stdio},
    time::Duration,
};

use agentmod_harness_protocol::{
    CatalogProvider, HarnessCommand, HarnessContinuationDecision, HarnessEvent, HarnessReply,
    ProjectedEntry,
};

fn harness_binary() -> &'static str {
    env!("CARGO_BIN_EXE_agentmod-harness-fixture")
}

fn authorization_key() -> [u8; 32] {
    let mut key = [0_u8; 32];
    for (index, chunk) in "11".repeat(32).as_bytes().chunks_exact(2).enumerate() {
        key[index] = u8::from_str_radix(std::str::from_utf8(chunk).expect("hex"), 16).expect("hex");
    }
    key
}

fn signed_grant() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NONCE: AtomicU64 = AtomicU64::new(0);
    let counter = NONCE.fetch_add(1, Ordering::Relaxed);
    let expires = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis()
        + 120_000;
    let nonce = uuid::Uuid::from_u128(0x018f6f83_7b80_7000_8000_0000000000ff + u128::from(counter));
    let binding = "ef".repeat(32);
    let payload = format!("v1.{expires}.{nonce}.{binding}");
    let signature = blake3::keyed_hash(&authorization_key(), payload.as_bytes());
    format!("{payload}.{}", signature.to_hex())
}

struct FixtureProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<std::process::ChildStdout>,
}

impl FixtureProcess {
    fn spawn() -> Self {
        let mut child = std::process::Command::new(harness_binary())
            .env("AGENTMOD_HARNESS_AUTH_KEY", "11".repeat(32))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn fixture harness");
        let stdin = child.stdin.take().expect("fixture stdin");
        let stdout = child.stdout.take().expect("fixture stdout");
        Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
        }
    }

    fn send(&mut self, command: &HarnessCommand) {
        let mut bytes = serde_json::to_vec(command).expect("command json");
        bytes.push(b'\n');
        self.stdin.write_all(&bytes).expect("write command");
        self.stdin.flush().expect("flush command");
    }

    fn read_reply(&mut self) -> HarnessReply {
        let mut line = String::new();
        let bytes = self
            .stdout
            .read_line(&mut line)
            .expect("read fixture reply");
        assert!(bytes > 0, "fixture process closed stdout");
        serde_json::from_str(&line).expect("fixture reply json")
    }

    fn read_events(&mut self) -> Vec<HarnessEvent> {
        let mut events = Vec::new();
        loop {
            match self.read_reply() {
                HarnessReply::Event { event, terminal } => {
                    events.push(event);
                    if terminal {
                        return events;
                    }
                }
                HarnessReply::Failed { code, message, .. } => {
                    panic!("fixture failed: {code}: {message}")
                }
                other => panic!("unexpected fixture reply: {other:?}"),
            }
        }
    }
}

impl Drop for FixtureProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn session_id() -> agentmod_primitives::SessionId {
    "018f6f83-7b80-7000-8000-000000000001"
        .parse()
        .expect("session ID")
}

fn cancellation_id(value: u128) -> agentmod_primitives::CancellationId {
    format!("018f6f83-7b80-7000-8000-{value:012}")
        .parse()
        .expect("cancellation ID")
}

fn execute_command(
    scenario: &str,
    entries: Vec<ProjectedEntry>,
    extra_options: &[(&str, serde_json::Value)],
    cancellation: u128,
) -> HarnessCommand {
    let mut options = serde_json::json!({"fixture_scenario": scenario});
    for (key, value) in extra_options {
        options[key] = value.clone();
    }
    HarnessCommand::Execute {
        session_id: session_id(),
        provider: "fixture-deterministic".into(),
        model: "fixture-model".into(),
        entries,
        options,
        authorization_grant: signed_grant(),
        cancellation_id: cancellation_id(cancellation),
    }
}

#[test]
fn health_and_catalog_report_distinct_identity() {
    let mut fixture = FixtureProcess::spawn();
    fixture.send(&HarnessCommand::Health);
    match fixture.read_reply() {
        HarnessReply::Health {
            status,
            ready_provider_count,
            capabilities,
        } => {
            assert_eq!(status, "ok");
            assert_eq!(ready_provider_count, 1);
            assert!(capabilities.contains(&"streaming".to_owned()));
            assert!(!capabilities.contains(&"images".to_owned()));
            assert!(!capabilities.contains(&"structured_output".to_owned()));
        }
        other => panic!("unexpected health reply: {other:?}"),
    }

    fixture.send(&HarnessCommand::Catalog);
    match fixture.read_reply() {
        HarnessReply::Catalog { providers } => {
            assert_eq!(providers.len(), 1);
            let provider: &CatalogProvider = &providers[0];
            assert_eq!(provider.id, "independent-fixture");
            assert_eq!(provider.version, "2.0.0");
            assert_eq!(provider.models, ["fixture-model"]);
            assert!(!provider.image_support);
            assert!(!provider.structured_output_support);
            assert!(provider.streaming_support);
            assert!(provider.tool_support);
            assert!(provider.available);
        }
        other => panic!("unexpected catalog reply: {other:?}"),
    }
}

#[test]
fn streams_text_and_normalizes_usage() {
    let mut fixture = FixtureProcess::spawn();
    fixture.send(&execute_command(
        "streaming_text",
        vec![ProjectedEntry::User {
            text: "hello".into(),
        }],
        &[("fixture_text", serde_json::json!("independent"))],
        10,
    ));
    let events = fixture.read_events();
    assert!(matches!(events.first(), Some(HarnessEvent::Started)));
    let text: String = events
        .iter()
        .filter_map(|event| match event {
            HarnessEvent::TextDelta { text } => Some(text.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(text, "alpha beta independent");
    match events.last() {
        Some(HarnessEvent::Completed { usage, .. }) => {
            assert_eq!(usage.input_tokens, 5);
            assert_eq!(usage.output_tokens, 3);
        }
        other => panic!("expected completion, got {other:?}"),
    }
}

#[test]
fn explicit_non_streaming_behavior_emits_one_delta() {
    let mut fixture = FixtureProcess::spawn();
    fixture.send(&execute_command(
        "non_streaming",
        vec![ProjectedEntry::User {
            text: "hello".into(),
        }],
        &[("streaming", serde_json::json!(false))],
        11,
    ));
    let events = fixture.read_events();
    let deltas: Vec<_> = events
        .iter()
        .filter(|event| matches!(event, HarnessEvent::TextDelta { .. }))
        .collect();
    assert_eq!(deltas.len(), 1);
    assert!(matches!(
        events.last(),
        Some(HarnessEvent::Completed { .. })
    ));
}

#[test]
fn tool_call_waits_for_explicit_continuation_and_resolves_once() {
    let mut fixture = FixtureProcess::spawn();
    fixture.send(&execute_command(
        "one_tool_call",
        vec![ProjectedEntry::User {
            text: "read".into(),
        }],
        &[],
        12,
    ));
    let events = fixture.read_events();
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, HarnessEvent::Completed { .. }))
    );
    let continuation_id = events
        .iter()
        .find_map(|event| {
            if let HarnessEvent::ToolCallProposed {
                continuation_id, ..
            } = event
            {
                Some(*continuation_id)
            } else {
                None
            }
        })
        .expect("tool proposal continuation");
    assert!(events.iter().any(|event| matches!(
        event,
        HarnessEvent::ToolCallProposed {
            tool,
            call_id,
            ..
        } if tool == "read_file" && call_id == "fixture-call-1"
    )));

    fixture.send(&HarnessCommand::Continue {
        continuation_id,
        decision: HarnessContinuationDecision::ReplaceContext {
            entries: vec![
                ProjectedEntry::User {
                    text: "read".into(),
                },
                ProjectedEntry::ToolResult {
                    call_id: "fixture-call-1".into(),
                    content: "bounded result".into(),
                    truncated: false,
                },
            ],
        },
    });
    let resumed = fixture.read_events();
    assert!(matches!(resumed.first(), Some(HarnessEvent::Started)));
    assert!(matches!(
        resumed.last(),
        Some(HarnessEvent::Completed { .. })
    ));

    // Duplicate resolution is rejected.
    fixture.send(&HarnessCommand::Continue {
        continuation_id,
        decision: HarnessContinuationDecision::Continue,
    });
    assert!(matches!(
        fixture.read_reply(),
        HarnessReply::Failed { code, .. } if code == "continuation_failed"
    ));
}

#[test]
fn slow_stream_cancellation_emits_partial_output_then_cancelled() {
    let mut fixture = FixtureProcess::spawn();
    fixture.send(&execute_command(
        "slow_stream",
        vec![ProjectedEntry::User {
            text: "wait".into(),
        }],
        &[],
        13,
    ));
    // Give the fixture time to register the exchange before cancelling.
    std::thread::sleep(Duration::from_millis(200));
    fixture.send(&HarnessCommand::Cancel {
        cancellation_id: cancellation_id(13),
    });
    let events = fixture.read_events();
    assert!(events.iter().any(|event| matches!(
        event,
        HarnessEvent::TextDelta { text } if text == "partial before cancellation"
    )));
    assert!(matches!(events.last(), Some(HarnessEvent::Cancelled)));
}

#[test]
fn negative_capability_guards_are_enforced() {
    let mut fixture = FixtureProcess::spawn();
    fixture.send(&execute_command(
        "text",
        vec![ProjectedEntry::Image {
            media_type: "image/png".into(),
            data_base64: "aGVsbG8=".into(),
        }],
        &[],
        14,
    ));
    let events = fixture.read_events();
    assert!(matches!(
        events.last(),
        Some(HarnessEvent::Failed {
            code,
            retryable: false,
            ..
        }) if code == "unsupported_capability"
    ));

    fixture.send(&execute_command(
        "text",
        vec![ProjectedEntry::User {
            text: "json".into(),
        }],
        &[(
            "response_format",
            serde_json::json!({"type": "json_object"}),
        )],
        15,
    ));
    let events = fixture.read_events();
    assert!(matches!(
        events.last(),
        Some(HarnessEvent::Failed {
            code,
            retryable: false,
            ..
        }) if code == "unsupported_capability"
    ));
}

#[test]
fn oversize_frames_fail_closed() {
    let mut fixture = FixtureProcess::spawn();
    let mut oversized = vec![b' '; 20 * 1024 * 1024];
    oversized.push(b'\n');
    fixture
        .stdin
        .write_all(&oversized)
        .expect("write oversized frame");
    fixture.stdin.flush().expect("flush oversized frame");
    match fixture.read_reply() {
        HarnessReply::Failed { code, .. } => assert_eq!(code, "frame_too_large"),
        other => panic!("unexpected oversized reply: {other:?}"),
    }
}
