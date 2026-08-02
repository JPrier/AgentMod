//! Deterministic local HTTP fixtures for every live provider wire format.
//!
//! These tests never require network credentials or paid APIs. Each test
//! spawns a bounded local HTTP server that speaks the provider's documented
//! wire format and drives the live provider dependency through the public
//! execution interface. The server runs on a background thread so the
//! synchronous live provider stack is exercised end to end.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use agentmod_harness_dependency::execution::{
    DependencyConversationEntry, DependencyProviderEvent, DependencyProviderExecutionRequest,
    DependencyProviderFailureKind, DependencyProviderOption, DependencyRetryClassification,
    ProviderCancellationDependency, ProviderExecutionDependency,
};
use agentmod_harness_dependency::live::{
    LiveProviderCatalogDependency, PROVIDER_ANTHROPIC, PROVIDER_GEMINI, PROVIDER_LOCAL,
    PROVIDER_OPENAI, PROVIDER_OPENROUTER,
};
use serde_json::json;

/// Scripted HTTP response plan.
enum ResponsePlan {
    /// Single full response with a status line, headers, and body.
    Full {
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    },
    /// Chunked SSE response written incrementally; optionally holds the
    /// connection open after the final chunk to exercise cancellation.
    SseChunks {
        chunks: Vec<Vec<u8>>,
        hold_open_ms: u64,
    },
}

/// Binds a local HTTP server; each accepted request is scripted by `plan`.
fn spawn_server(
    plan: impl Fn(&str, &[u8]) -> ResponsePlan + Send + Sync + 'static,
) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture server");
    let address = listener.local_addr().expect("fixture address");
    let plan = Arc::new(plan);
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut socket) = stream else {
                break;
            };
            let plan = plan.clone();
            thread::spawn(move || {
                let Some((headers, body)) = read_request(&mut socket) else {
                    return;
                };
                match plan(&headers, &body) {
                    ResponsePlan::Full {
                        status,
                        headers: response_headers,
                        body: response_body,
                    } => {
                        write_full(&mut socket, status, &response_headers, &response_body).ok();
                    }
                    ResponsePlan::SseChunks {
                        chunks,
                        hold_open_ms,
                    } => {
                        let _ = write_chunked_start(&mut socket);
                        for chunk in chunks {
                            if write_chunk(&mut socket, &chunk).is_err() {
                                return;
                            }
                        }
                        if hold_open_ms > 0 {
                            // Abort the connection without a terminating chunk
                            // to exercise cancellation and ambiguous disconnects.
                            thread::sleep(Duration::from_millis(hold_open_ms));
                            drop(socket);
                        } else {
                            let _ = write_chunk_end(&mut socket);
                        }
                    }
                }
            });
        }
    });
    address
}

fn read_request(socket: &mut TcpStream) -> Option<(String, Vec<u8>)> {
    let mut buffer = Vec::new();
    let mut tmp = [0_u8; 4096];
    loop {
        let read = socket.read(&mut tmp).ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&tmp[..read]);
        if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if buffer.len() > 64 * 1024 {
            return None;
        }
    }
    let header_end = buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("header terminator");
    let headers = String::from_utf8_lossy(&buffer[..header_end]).into_owned();
    let body_start = header_end + 4;
    let content_length = headers
        .lines()
        .find_map(|line| {
            let lower = line.to_ascii_lowercase();
            lower
                .strip_prefix("content-length:")
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    let mut body = buffer[body_start..].to_vec();
    while body.len() < content_length {
        let read = socket.read(&mut tmp).ok()?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..read]);
    }
    Some((headers, body))
}

fn write_full(
    socket: &mut TcpStream,
    status: u16,
    headers: &[(String, String)],
    body: &[u8],
) -> std::io::Result<()> {
    write!(
        socket,
        "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\n",
        body.len()
    )?;
    for (name, value) in headers {
        write!(socket, "{name}: {value}\r\n")?;
    }
    socket.write_all(b"\r\n")?;
    socket.write_all(body)?;
    socket.flush()
}

fn write_chunked_start(socket: &mut TcpStream) -> std::io::Result<()> {
    write!(
        socket,
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n"
    )?;
    socket.flush()
}

fn write_chunk(socket: &mut TcpStream, chunk: &[u8]) -> std::io::Result<()> {
    write!(socket, "{:x}\r\n", chunk.len())?;
    socket.write_all(chunk)?;
    socket.write_all(b"\r\n")?;
    socket.flush()
}

fn write_chunk_end(socket: &mut TcpStream) -> std::io::Result<()> {
    socket.write_all(b"0\r\n\r\n")?;
    socket.flush()
}

fn sse(data: &str) -> Vec<u8> {
    format!("data: {data}\n\n").into_bytes()
}

fn sse_event(event: &str, data: &str) -> Vec<u8> {
    format!("event: {event}\ndata: {data}\n\n").into_bytes()
}

fn sse_keepalive() -> Vec<u8> {
    b": keepalive\n\n".to_vec()
}

fn option(key: &str, value: &str) -> DependencyProviderOption {
    DependencyProviderOption {
        key: key.into(),
        value: value.into(),
    }
}

fn live_request(
    provider: &str,
    model: &str,
    entries: Vec<DependencyConversationEntry>,
    options: Vec<DependencyProviderOption>,
    cancellation_reference: &str,
) -> DependencyProviderExecutionRequest {
    DependencyProviderExecutionRequest {
        provider_key: provider.into(),
        model_key: model.into(),
        entries,
        options,
        authorization_grant: "grant".into(),
        cancellation_reference: cancellation_reference.into(),
        resumed_after_continuation: false,
    }
}

fn options_with_base(base_url: &str) -> Vec<DependencyProviderOption> {
    vec![option("base_url", base_url)]
}

fn user(text: &str) -> DependencyConversationEntry {
    DependencyConversationEntry::User(text.into())
}

#[test]
fn openai_compatible_streams_text_with_fragmented_utf8_and_usage() {
    let base = spawn_server(|_headers, _body| {
        ResponsePlan::SseChunks {
            chunks: vec![
                sse(&json!({"choices": [{"delta": {"content": "hé"}, "finish_reason": null}]}).to_string()),
                sse(&json!({"choices": [{"delta": {"content": "llo"}, "finish_reason": null}]}).to_string()),
                sse(
                    &json!({
                        "choices": [{"delta": {}, "finish_reason": "stop"}],
                        "usage": {"prompt_tokens": 11, "completion_tokens": 7, "total_tokens": 18},
                    })
                    .to_string(),
                ),
                b"data: [DONE]\n\n".to_vec(),
            ],
            hold_open_ms: 0,
        }
    });
    let dependency = LiveProviderCatalogDependency::development();
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_LOCAL,
            "local-model",
            vec![user("hello")],
            options_with_base(&format!("http://{base}/v1")),
            "cancel-utf8",
        ))
        .expect("live execution");
    let text: String = response
        .events
        .iter()
        .filter_map(|event| match event {
            DependencyProviderEvent::TextDelta(text) => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(text, "héllo");
    assert!(response.events.iter().any(|event| matches!(
        event,
        DependencyProviderEvent::Completed { usage, .. }
            if usage.input_tokens == 11 && usage.output_tokens == 7
    )));
}

#[test]
fn openai_compatible_streams_tool_call_deltas_and_multiple_proposals() {
    let base = spawn_server(|_headers, _body| {
        ResponsePlan::SseChunks {
            chunks: vec![
                sse(
                    &json!({
                        "choices": [{
                            "delta": {"tool_calls": [{"index": 0, "id": "call-1", "function": {"name": "filesystem.read", "arguments": ""}}]},
                            "finish_reason": null
                        }]
                    })
                    .to_string(),
                ),
                sse(
                    &json!({
                        "choices": [{
                            "delta": {"tool_calls": [{"index": 0, "function": {"arguments": "{\"path\":\"/tmp/a.txt\"}"}}]},
                            "finish_reason": null
                        }]
                    })
                    .to_string(),
                ),
                sse(
                    &json!({
                        "choices": [{
                            "delta": {"tool_calls": [{"index": 1, "id": "call-2", "function": {"name": "filesystem.read", "arguments": "{\"path\":\"/tmp/b.txt\"}"}}]},
                            "finish_reason": "tool_calls"
                        }]
                    })
                    .to_string(),
                ),
                b"data: [DONE]\n\n".to_vec(),
            ],
            hold_open_ms: 0,
        }
    });
    let dependency = LiveProviderCatalogDependency::development();
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_LOCAL,
            "local-model",
            vec![user("read files")],
            options_with_base(&format!("http://{base}/v1")),
            "cancel-tools",
        ))
        .expect("live execution");
    let proposals: Vec<_> = response
        .events
        .iter()
        .filter_map(|event| match event {
            DependencyProviderEvent::ToolCallProposed {
                call_id,
                tool,
                arguments_json,
                ..
            } => Some((call_id.as_str(), tool.as_str(), arguments_json.as_str())),
            _ => None,
        })
        .collect();
    assert_eq!(proposals.len(), 2);
    assert_eq!(proposals[0].0, "call-1");
    assert_eq!(proposals[0].1, "filesystem.read");
    assert!(proposals[0].2.contains("\"path\""));
    assert_eq!(proposals[1].0, "call-2");
    assert_eq!(
        response.events.last(),
        Some(&DependencyProviderEvent::Completed {
            finish_reason: String::from("tool_calls"),
            usage: Default::default(),
            cost: None,
        })
    );
}

#[test]
fn openrouter_returns_usage_and_computed_cost_metadata() {
    let base = spawn_server(|_headers, _body| {
        ResponsePlan::SseChunks {
            chunks: vec![
                sse(&json!({"choices": [{"delta": {"content": "hi"}, "finish_reason": null}]}).to_string()),
                sse(
                    &json!({
                        "choices": [{"delta": {}, "finish_reason": "stop"}],
                        "usage": {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150},
                    })
                    .to_string(),
                ),
                b"data: [DONE]\n\n".to_vec(),
            ],
            hold_open_ms: 0,
        }
    });
    let dependency = LiveProviderCatalogDependency::development();
    let mut options = options_with_base(&format!("http://{base}/v1"));
    options.push(option(
        "pricing_json",
        r#"{"source":"openrouter-model-catalog","version":"2026-07","currency":"USD","models":{"openrouter-model":{"input_per_1k_micros":250,"output_per_1k_micros":1000}}}"#,
    ));
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_OPENROUTER,
            "openrouter-model",
            vec![user("cost")],
            options,
            "cancel-cost",
        ))
        .expect("live execution");
    let completed = response
        .events
        .iter()
        .find_map(|event| match event {
            DependencyProviderEvent::Completed { usage, cost, .. } => Some((usage, cost)),
            _ => None,
        })
        .expect("completed event");
    assert_eq!(completed.0.input_tokens, 100);
    assert_eq!(completed.0.output_tokens, 50);
    let cost = completed.1.as_ref().expect("cost metadata");
    assert_eq!(cost.currency, "USD");
    // 100 input tokens * 250 micros/1k = 25 micros; 50 output * 1000 = 50 micros.
    assert_eq!(cost.input_cost_micros, 25);
    assert_eq!(cost.output_cost_micros, 50);
}

#[test]
fn openai_non_streaming_response_is_normalized() {
    let base = spawn_server(|_headers, _body| {
        ResponsePlan::Full {
            status: 200,
            headers: vec![("Content-Type".into(), "application/json".into())],
            body: serde_json::to_vec(&json!({
                "choices": [{"message": {"content": "non-streaming reply"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5},
            }))
            .expect("response body"),
        }
    });
    let dependency = LiveProviderCatalogDependency::development();
    let mut options = options_with_base(&format!("http://{base}/v1"));
    options.push(option("streaming", "false"));
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_OPENAI,
            "gpt-4o-mini",
            vec![user("hi")],
            options,
            "cancel-nonstream",
        ))
        .expect("live execution");
    let text: String = response
        .events
        .iter()
        .filter_map(|event| match event {
            DependencyProviderEvent::TextDelta(text) => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(text, "non-streaming reply");
    assert!(response.events.iter().any(|event| matches!(
        event,
        DependencyProviderEvent::Completed { usage, .. }
            if usage.input_tokens == 3 && usage.output_tokens == 2
    )));
}

#[test]
fn anthropic_streams_text_and_tool_use_events() {
    let base = spawn_server(|_headers, _body| {
        ResponsePlan::SseChunks {
            chunks: vec![
                sse_event(
                    "message_start",
                    &json!({"message": {"usage": {"input_tokens": 12, "output_tokens": 1}}}).to_string(),
                ),
                sse_event(
                    "content_block_start",
                    &json!({"index": 0, "content_block": {"type": "text", "text": ""}}).to_string(),
                ),
                sse_event(
                    "content_block_delta",
                    &json!({"index": 0, "delta": {"type": "text_delta", "text": "hello"}}).to_string(),
                ),
                sse_event("content_block_stop", &json!({"index": 0}).to_string()),
                sse_event(
                    "content_block_start",
                    &json!({"index": 1, "content_block": {"type": "tool_use", "id": "tool-1", "name": "filesystem.read", "input": {}}}).to_string(),
                ),
                sse_event(
                    "content_block_delta",
                    &json!({"index": 1, "delta": {"type": "input_json_delta", "partial_json": "{\"path\":\"/tmp/x\"}"}}).to_string(),
                ),
                sse_event("content_block_stop", &json!({"index": 1}).to_string()),
                sse_event(
                    "message_delta",
                    &json!({"delta": {"stop_reason": "tool_use"}, "usage": {"output_tokens": 33}}).to_string(),
                ),
                sse_event("message_stop", &json!({}).to_string()),
            ],
            hold_open_ms: 0,
        }
    });
    let dependency = LiveProviderCatalogDependency::development();
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_ANTHROPIC,
            "claude-3-5-haiku-latest",
            vec![user("hi")],
            options_with_base(&format!("http://{base}")),
            "cancel-anthropic",
        ))
        .expect("live execution");
    let text: String = response
        .events
        .iter()
        .filter_map(|event| match event {
            DependencyProviderEvent::TextDelta(text) => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(text, "hello");
    assert!(response.events.iter().any(|event| matches!(
        event,
        DependencyProviderEvent::ToolCallProposed {
            call_id,
            tool,
            ..
        } if call_id == "tool-1" && tool == "filesystem.read"
    )));
    assert!(response.events.iter().any(|event| matches!(
        event,
        DependencyProviderEvent::Completed { usage, .. }
            if usage.input_tokens == 12 && usage.output_tokens == 33
    )));
}

#[test]
fn gemini_streams_text_and_function_calls() {
    let base = spawn_server(|_headers, _body| {
        ResponsePlan::SseChunks {
            chunks: vec![
                sse(
                    &json!({
                        "candidates": [{
                            "content": {"parts": [{"text": "gemini"}]},
                            "finishReason": "STOP"
                        }]
                    })
                    .to_string(),
                ),
                sse(
                    &json!({
                        "candidates": [{
                            "content": {"parts": [{"functionCall": {"name": "filesystem.read", "args": {"path": "/tmp/g"}}}]},
                            "finishReason": "STOP"
                        }]
                    })
                    .to_string(),
                ),
                sse(
                    &json!({
                        "usageMetadata": {
                            "promptTokenCount": 9,
                            "candidatesTokenCount": 4,
                            "totalTokenCount": 13,
                        }
                    })
                    .to_string(),
                ),
            ],
            hold_open_ms: 0,
        }
    });
    let dependency = LiveProviderCatalogDependency::development();
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_GEMINI,
            "gemini-2.0-flash",
            vec![user("hi")],
            options_with_base(&format!("http://{base}")),
            "cancel-gemini",
        ))
        .expect("live execution");
    let text: String = response
        .events
        .iter()
        .filter_map(|event| match event {
            DependencyProviderEvent::TextDelta(text) => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(text, "gemini");
    assert!(response.events.iter().any(|event| matches!(
        event,
        DependencyProviderEvent::ToolCallProposed { tool, .. } if tool == "filesystem.read"
    )));
    assert!(response.events.iter().any(|event| matches!(
        event,
        DependencyProviderEvent::Completed { usage, .. }
            if usage.input_tokens == 9 && usage.output_tokens == 4
    )));
}

#[test]
fn image_inputs_are_serialized_into_the_request_body() {
    let base = spawn_server(|_headers, body| {
        let parsed: serde_json::Value = serde_json::from_slice(body).expect("request body");
        let messages = parsed["messages"].as_array().expect("messages");
        let content = messages[0]["content"].as_array().expect("content array");
        assert!(content.iter().any(|part| {
            part["type"] == "image_url"
                && part["image_url"]["url"]
                    .as_str()
                    .is_some_and(|url| url.starts_with("data:image/png;base64,"))
        }));
        ResponsePlan::Full {
            status: 200,
            headers: vec![("Content-Type".into(), "application/json".into())],
            body: serde_json::to_vec(&json!({
                "choices": [{"message": {"content": "seen"}, "finish_reason": "stop"}],
            }))
            .expect("response body"),
        }
    });
    let dependency = LiveProviderCatalogDependency::development();
    let mut options = options_with_base(&format!("http://{base}/v1"));
    options.push(option("streaming", "false"));
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_OPENAI,
            "gpt-4o-mini",
            vec![DependencyConversationEntry::Image {
                media_type: "image/png".into(),
                data_base64: "aGVsbG8=".into(),
            }],
            options,
            "cancel-image",
        ))
        .expect("live execution");
    assert!(response
        .events
        .iter()
        .any(|event| matches!(event, DependencyProviderEvent::Completed { .. })));
}

#[test]
fn malformed_sse_fails_closed_without_stream_partial_claims() {
    let base = spawn_server(|_headers, _body| {
        ResponsePlan::SseChunks {
            chunks: vec![
                sse("not-json-{"),
                sse(&json!({"choices": [{"delta": {"content": "partial"}, "finish_reason": null}]}).to_string()),
                b"data: [DONE]\n\n".to_vec(),
            ],
            hold_open_ms: 0,
        }
    });
    let dependency = LiveProviderCatalogDependency::development();
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_LOCAL,
            "local-model",
            vec![user("malformed")],
            options_with_base(&format!("http://{base}/v1")),
            "cancel-malformed",
        ))
        .expect("live execution");
    assert!(matches!(
        response.events.last(),
        Some(DependencyProviderEvent::Failed {
            kind: DependencyProviderFailureKind::InvalidRequest,
            retry: DependencyRetryClassification::Never,
            ..
        })
    ));
}

#[test]
fn retryable_rate_limit_is_classified_with_delay() {
    let base = spawn_server(|_headers, _body| {
        ResponsePlan::Full {
            status: 429,
            headers: vec![("Retry-After".into(), "5".into())],
            body: br#"{"error":{"message":"rate limited"}}"#.to_vec(),
        }
    });
    let dependency = LiveProviderCatalogDependency::development();
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_OPENAI,
            "gpt-4o-mini",
            vec![user("hi")],
            options_with_base(&format!("http://{base}/v1")),
            "cancel-ratelimit",
        ))
        .expect("live execution");
    assert!(matches!(
        response.events.last(),
        Some(DependencyProviderEvent::Failed {
            kind: DependencyProviderFailureKind::RateLimited,
            retry: DependencyRetryClassification::AfterMilliseconds(5_000),
            ..
        })
    ));
}

#[test]
fn non_retryable_auth_failure_is_classified_never() {
    let base = spawn_server(|_headers, _body| {
        ResponsePlan::Full {
            status: 401,
            headers: vec![("Content-Type".into(), "application/json".into())],
            body: br#"{"error":{"message":"invalid api key"}}"#.to_vec(),
        }
    });
    let dependency = LiveProviderCatalogDependency::development();
    let mut options = options_with_base(&format!("http://{base}/v1"));
    options.push(option("api_key_ref", "AGENTMOD_TEST_OPENAI_API_KEY"));
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_OPENAI,
            "gpt-4o-mini",
            vec![user("hi")],
            options,
            "cancel-auth",
        ))
        .expect("live execution");
    assert!(matches!(
        response.events.last(),
        Some(DependencyProviderEvent::Failed {
            kind: DependencyProviderFailureKind::AuthenticationFailed,
            retry: DependencyRetryClassification::Never,
            ..
        })
    ));
}

#[test]
fn ambiguous_disconnect_fails_closed_without_completion() {
    let base = spawn_server(|_headers, _body| {
        ResponsePlan::SseChunks {
            chunks: vec![
                sse(&json!({"choices": [{"delta": {"content": "partial"}, "finish_reason": null}]}).to_string()),
            ],
            hold_open_ms: 5_000,
        }
    });
    let dependency = LiveProviderCatalogDependency::development();
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_LOCAL,
            "local-model",
            vec![user("disconnect")],
            options_with_base(&format!("http://{base}/v1")),
            "cancel-disconnect",
        ))
        .expect("live execution");
    assert!(matches!(
        response.events.last(),
        Some(DependencyProviderEvent::Failed {
            kind: DependencyProviderFailureKind::AmbiguousDisconnect,
            ..
        })
    ));
    assert!(!response.events.iter().any(|event| matches!(
        event,
        DependencyProviderEvent::Completed { .. }
    )));
}

#[test]
fn cancellation_during_stream_emits_cancelled_and_stops() {
    let base = spawn_server(move |_headers, _body| ResponsePlan::SseChunks {
        chunks: vec![sse(
            &json!({"choices": [{"delta": {"content": "partial"}, "finish_reason": null}]})
                .to_string(),
        )],
        hold_open_ms: 60_000,
    });
    let dependency = LiveProviderCatalogDependency::development();
    let handle = {
        let dependency = dependency.clone();
        thread::spawn(move || {
            dependency
                .execute_provider(live_request(
                    PROVIDER_LOCAL,
                    "local-model",
                    vec![user("stream forever")],
                    options_with_base(&format!("http://{base}/v1")),
                    "cancel-mid-stream",
                ))
                .expect("live execution")
        })
    };
    thread::sleep(Duration::from_millis(500));
    assert!(
        dependency
            .cancel_provider("cancel-mid-stream")
            .expect("cancel")
    );
    let response = handle.join().expect("task");
    assert!(response.events.iter().any(|event| matches!(
        event,
        DependencyProviderEvent::TextDelta(text) if text == "partial"
    )));
    assert_eq!(
        response.events.last(),
        Some(&DependencyProviderEvent::Cancelled)
    );
}

#[test]
fn authentication_headers_are_sent_and_never_leaked_in_errors() {
    let seen_auth = Arc::new(Mutex::new(None::<String>));
    let captured = seen_auth.clone();
    let base = spawn_server(move |headers, _body| {
        let auth = headers
            .lines()
            .find_map(|line| {
                line.strip_prefix("authorization:")
                    .map(|value| value.trim().to_owned())
            })
            .unwrap_or_default();
        *captured.lock().expect("capture") = Some(auth);
        ResponsePlan::Full {
            status: 401,
            headers: vec![("Content-Type".into(), "application/json".into())],
            body: br#"{"error":{"message":"invalid api key sk-secret-value"}}"#.to_vec(),
        }
    });
    let secret_dir = tempfile::tempdir().expect("temp dir");
    let secret_path = secret_dir.path().join("openai.key");
    std::fs::write(&secret_path, "sk-secret-value\n").expect("write secret fixture");
    let dependency = LiveProviderCatalogDependency::development();
    let mut options = options_with_base(&format!("http://{base}/v1"));
    options.push(option(
        "api_key_ref",
        &format!("file:{}", secret_path.display()),
    ));
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_OPENAI,
            "gpt-4o-mini",
            vec![user("hi")],
            options,
            "cancel-auth",
        ))
        .expect("live execution");
    let auth = seen_auth
        .lock()
        .expect("auth")
        .clone()
        .expect("auth header");
    assert_eq!(auth, "Bearer sk-secret-value");
    let message = match response.events.last() {
        Some(DependencyProviderEvent::Failed { message, .. }) => message.clone(),
        other => panic!("expected failure, got {other:?}"),
    };
    assert!(
        !message.contains("sk-secret-value"),
        "secret leaked into failure message: {message}"
    );
    let serialized = format!("{:?}", response.events);
    assert!(
        !serialized.contains("sk-secret-value"),
        "secret leaked into serialized events: {serialized}"
    );
}
