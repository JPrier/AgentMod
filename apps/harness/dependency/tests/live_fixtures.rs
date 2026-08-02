//! Deterministic local HTTP fixtures for every live provider wire format.
//!
//! These tests never require network credentials or paid APIs. Each test
//! spawns a bounded local HTTP server that speaks the provider's documented
//! wire format and drives the live provider dependency through the public
//! execution interface.

use std::sync::{Arc, Mutex};

use agentmod_harness_dependency::execution::{
    DependencyConversationEntry, DependencyProviderEvent, DependencyProviderExecutionRequest,
    DependencyProviderOption, DependencyProviderFailureKind, DependencyRetryClassification, ProviderCancellationDependency, ProviderExecutionDependency,
};
use agentmod_harness_dependency::live::{
    LiveProviderCatalogDependency, PROVIDER_ANTHROPIC, PROVIDER_GEMINI, PROVIDER_OPENAI,
    PROVIDER_OPENROUTER, PROVIDER_LOCAL,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
async fn spawn_server(
    plan: impl Fn(&str, &[u8]) -> ResponsePlan + Send + Sync + 'static,
) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture server");
    let address = listener.local_addr().expect("fixture address");
    let plan = Arc::new(plan);
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let plan = plan.clone();
            tokio::spawn(async move {
                let Some((headers, body)) = read_request(&mut socket).await else {
                    return;
                };
                match plan(&headers, &body) {
                    ResponsePlan::Full {
                        status,
                        headers: response_headers,
                        body: response_body,
                    } => {
                        write_full(&mut socket, status, &response_headers, &response_body)
                            .await
                            .ok();
                    }
                    ResponsePlan::SseChunks {
                        chunks,
                        hold_open_ms,
                    } => {
                        let _ = write_chunked_start(&mut socket).await;
                        for chunk in chunks {
                            if write_chunk(&mut socket, &chunk).await.is_err() {
                                return;
                            }
                        }
                        if hold_open_ms > 0 {
                            // Abort the connection without a terminating chunk
                            // to exercise cancellation and ambiguous disconnects.
                            tokio::time::sleep(std::time::Duration::from_millis(hold_open_ms)).await;
                            drop(socket);
                        } else {
                            let _ = write_chunk_end(&mut socket).await;
                        }
                    }
                }
            });
        }
    });
    address
}

async fn read_request(
    socket: &mut tokio::net::TcpStream,
) -> Option<(String, Vec<u8>)> {
    let mut buffer = Vec::new();
    let mut tmp = [0_u8; 4096];
    loop {
        let read = socket.read(&mut tmp).await.ok()?;
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
                .map(|value| value.trim().parse::<usize>().unwrap_or(0))
        })
        .unwrap_or(0);
    while buffer.len() < body_start + content_length {
        let read = socket.read(&mut tmp).await.ok()?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&tmp[..read]);
    }
    let body = buffer[body_start..body_start + content_length.min(buffer.len() - body_start)].to_vec();
    Some((headers, body))
}

async fn write_full(
    socket: &mut tokio::net::TcpStream,
    status: u16,
    headers: &[(String, String)],
    body: &[u8],
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        _ => "Status",
    };
    let mut response = format!("HTTP/1.1 {status} {reason}\r\n");
    for (key, value) in headers {
        response.push_str(key);
        response.push_str(": ");
        response.push_str(value);
        response.push_str("\r\n");
    }
    response.push_str("Content-Length: ");
    response.push_str(&body.len().to_string());
    response.push_str("\r\n\r\n");
    socket.write_all(response.as_bytes()).await?;
    socket.write_all(body).await?;
    socket.flush().await
}

async fn write_chunked_start(socket: &mut tokio::net::TcpStream) -> std::io::Result<()> {
    socket
        .write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nCache-Control: no-cache\r\n\r\n",
        )
        .await
}

async fn write_chunk(
    socket: &mut tokio::net::TcpStream,
    chunk: &[u8],
) -> std::io::Result<()> {
    socket
        .write_all(format!("{:X}\r\n", chunk.len()).as_bytes())
        .await?;
    socket.write_all(chunk).await?;
    socket.write_all(b"\r\n").await?;
    socket.flush().await
}

async fn write_chunk_end(socket: &mut tokio::net::TcpStream) -> std::io::Result<()> {
    socket.write_all(b"0\r\n\r\n").await?;
    socket.flush().await
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
        key: key.to_owned(),
        value: value.to_owned(),
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
        provider_key: provider.to_owned(),
        model_key: model.to_owned(),
        entries,
        options,
        authorization_grant: "grant".to_owned(),
        cancellation_reference: cancellation_reference.to_owned(),
        resumed_after_continuation: false,
    }
}

fn options_with_base(base_url: &str) -> Vec<DependencyProviderOption> {
    vec![option("base_url", base_url), option("timeout_ms", "30000")]
}

fn user(text: &str) -> DependencyConversationEntry {
    DependencyConversationEntry::User(text.to_owned())
}

#[tokio::test]
async fn openai_compatible_streams_text_with_fragmented_utf8_and_usage() {
    let base = spawn_server(move |_headers, _body| {
        // Event text is "h\u{e9}llo world"; the multi-byte character is split
        // across two HTTP chunks.
        let prefix = b"data: {\"choices\":[{\"delta\":{\"content\":\"h";
        let mut chunk_a = prefix.to_vec();
        chunk_a.push(0xC3); // first byte of the two-byte encoding of U+00E9
        let mut chunk_b = vec![0xA9]; // second byte of the two-byte encoding of U+00E9
        chunk_b.extend_from_slice(b"llo\"},\"finish_reason\":null}]}\n\n");
        ResponsePlan::SseChunks {
            chunks: vec![
                chunk_a,
                chunk_b,
                sse_keepalive(),
                sse(&json!({"choices": [{"delta": {"content": " world"}, "finish_reason": "stop"}],
                           "usage": {"prompt_tokens": 12, "completion_tokens": 7,
                                     "prompt_tokens_details": {"cached_tokens": 3}}}).to_string()),
                sse("[DONE]"),
            ],
            hold_open_ms: 0,
        }
    })
    .await;
    let dependency = LiveProviderCatalogDependency::development();
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_LOCAL,
            "local-model",
            vec![user("hi")],
            options_with_base(&format!("http://{base}/v1")),
            "cancel-openai",
        ))
        .await
        .expect("live execution");
    assert!(matches!(response.events.first(), Some(DependencyProviderEvent::Started)));
    let text: String = response
        .events
        .iter()
        .filter_map(|event| match event {
            DependencyProviderEvent::TextDelta(text) => Some(text.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(text, "h\u{e9}llo world");
    let completed = response.events.last().expect("completion");
    match completed {
        DependencyProviderEvent::Completed { usage, .. } => {
            assert_eq!(usage.input_tokens, 12);
            assert_eq!(usage.output_tokens, 7);
            assert_eq!(usage.cache_read_tokens, 3);
        }
        other => panic!("expected completion, got {other:?}"),
    }
}

#[tokio::test]
async fn openai_compatible_streams_tool_call_deltas_and_multiple_proposals() {
    let base = spawn_server(move |_headers, _body| {
        ResponsePlan::SseChunks {
            chunks: vec![
                sse(&json!({"choices": [{"delta": {"tool_calls": [{
                    "index": 0, "id": "call-1",
                    "function": {"name": "read_file", "arguments": "{\"pa"}
                }]}, "finish_reason": null}]}).to_string()),
                sse(&json!({"choices": [{"delta": {"tool_calls": [{
                    "index": 0,
                    "function": {"arguments": "th\":\"a\"}"}
                }]}, "finish_reason": null}]}).to_string()),
                sse(&json!({"choices": [{"delta": {"tool_calls": [{
                    "index": 1, "id": "call-2",
                    "function": {"name": "read_file", "arguments": "{\"path\":\"b\"}"}
                }]}, "finish_reason": null}]}).to_string()),
                sse(&json!({"choices": [{"delta": {}, "finish_reason": "tool_calls"}],
                           "usage": {"prompt_tokens": 5, "completion_tokens": 9}}).to_string()),
                sse("[DONE]"),
            ],
            hold_open_ms: 0,
        }
    })
    .await;
    let dependency = LiveProviderCatalogDependency::development();
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_LOCAL,
            "local-model",
            vec![user("read files")],
            options_with_base(&format!("http://{base}/v1")),
            "cancel-tools",
        ))
        .await
        .expect("live execution");
    let deltas = response
        .events
        .iter()
        .filter(|event| matches!(event, DependencyProviderEvent::ToolCallDelta { .. }))
        .count();
    assert!(deltas >= 3, "expected tool-call deltas, got {deltas}");
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
    assert_eq!(proposals[0], ("call-1", "read_file", r#"{"path":"a"}"#));
    assert_eq!(proposals[1], ("call-2", "read_file", r#"{"path":"b"}"#));
}

#[tokio::test]
async fn openrouter_returns_usage_and_computed_cost_metadata() {
    let base = spawn_server(move |_headers, _body| {
        ResponsePlan::SseChunks {
            chunks: vec![
                sse(&json!({"choices": [{"delta": {"content": "ok"}, "finish_reason": "stop"}],
                           "usage": {"prompt_tokens": 100, "completion_tokens": 50,
                                     "prompt_tokens_details": {"cached_tokens": 40}}}).to_string()),
                sse("[DONE]"),
            ],
            hold_open_ms: 0,
        }
    })
    .await;
    let dependency = LiveProviderCatalogDependency::development();
    let mut options = options_with_base(&format!("http://{base}/api/v1"));
    options.push(option(
        "pricing_json",
        r#"{"source":"fixture","version":"2026-07","currency":"USD","models":{"fixture-model":{"input_per_1k_micros":10,"output_per_1k_micros":30,"cache_read_per_1k_micros":5,"cache_write_per_1k_micros":5}}}"#,
    ));
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_OPENROUTER,
            "fixture-model",
            vec![user("hi")],
            options,
            "cancel-or",
        ))
        .await
        .expect("live execution");
    match response.events.last() {
        Some(DependencyProviderEvent::Completed { usage, cost, .. }) => {
            assert_eq!(usage.input_tokens, 100);
            assert_eq!(usage.output_tokens, 50);
            let cost = cost.as_ref().expect("pricing record present");
            assert_eq!(cost.source, "fixture");
            assert_eq!(cost.input_cost_micros, 1); // 100 * 10 / 1000
            assert_eq!(cost.output_cost_micros, 1); // 50 * 30 / 1000
            assert_eq!(cost.cache_read_cost_micros, 0); // 40 * 5 / 1000
        }
        other => panic!("expected completion, got {other:?}"),
    }
}

#[tokio::test]
async fn openai_non_streaming_response_is_normalized() {
    let base = spawn_server(move |_headers, _body| {
        ResponsePlan::Full {
            status: 200,
            headers: vec![("Content-Type".into(), "application/json".into())],
            body: json!({
                "id": "chatcmpl-fixture",
                "choices": [{
                    "message": {"role": "assistant", "content": "non-stream reply"},
                    "finish_reason": "stop"
                }],
                "usage": {"prompt_tokens": 3, "completion_tokens": 2}
            })
            .to_string()
            .into_bytes(),
        }
    })
    .await;
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
        .await
        .expect("live execution");
    let text: String = response
        .events
        .iter()
        .filter_map(|event| match event {
            DependencyProviderEvent::TextDelta(text) => Some(text.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(text, "non-stream reply");
    assert!(matches!(
        response.events.last(),
        Some(DependencyProviderEvent::Completed { usage, .. }) if usage.input_tokens == 3
    ));
}

#[tokio::test]
async fn anthropic_streams_text_and_tool_use_events() {
    let base = spawn_server(move |_headers, _body| {
        ResponsePlan::SseChunks {
            chunks: vec![
                sse_event("message_start", r#"{"type":"message_start","message":{"usage":{"input_tokens":4,"output_tokens":1}}}"#),
                sse_event("content_block_start", r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#),
                sse_event("content_block_delta", r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}"#),
                sse_event("content_block_stop", r#"{"type":"content_block_stop","index":0}"#),
                sse_event("content_block_start", r#"{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"tool-1","name":"read_file"}}"#),
                sse_event("content_block_delta", r#"{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"path\":"}}"#),
                sse_event("content_block_delta", r#"{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"\"src/lib.rs\"}"}}"#),
                sse_event("content_block_stop", r#"{"type":"content_block_stop","index":1}"#),
                sse_event("message_delta", r#"{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":7}}"#),
                sse_event("message_stop", r#"{"type":"message_stop"}"#),
            ],
            hold_open_ms: 0,
        }
    })
    .await;
    let dependency = LiveProviderCatalogDependency::development();
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_ANTHROPIC,
            "claude-3-5-haiku-latest",
            vec![user("read the file")],
            options_with_base(&format!("http://{base}")),
            "cancel-anthropic",
        ))
        .await
        .expect("live execution");
    let text: String = response
        .events
        .iter()
        .filter_map(|event| match event {
            DependencyProviderEvent::TextDelta(text) => Some(text.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(text, "hello");
    assert!(response.events.iter().any(|event| matches!(
        event,
        DependencyProviderEvent::ToolCallProposed {
            tool,
            call_id,
            arguments_json,
            ..
        } if tool == "read_file" && call_id == "tool-1"
            && arguments_json == r#"{"path":"src/lib.rs"}"#
    )));
    match response.events.last() {
        Some(DependencyProviderEvent::Completed { usage, .. }) => {
            assert_eq!(usage.input_tokens, 4);
            assert_eq!(usage.output_tokens, 7);
        }
        other => panic!("expected completion, got {other:?}"),
    }
}

#[tokio::test]
async fn gemini_streams_text_and_function_calls() {
    let base = spawn_server(move |_headers, _body| {
        ResponsePlan::SseChunks {
            chunks: vec![
                sse(&json!({
                    "candidates": [{"content": {"parts": [{"text": "look"}], "role": "model"}, "finishReason": null}],
                    "usageMetadata": {"promptTokenCount": 8, "candidatesTokenCount": 3}
                }).to_string()),
                sse(&json!({
                    "candidates": [{"content": {"parts": [{"functionCall": {
                        "name": "read_file", "args": {"path": "src/lib.rs"}
                    }}], "role": "model"}, "finishReason": "STOP"}],
                    "usageMetadata": {"promptTokenCount": 8, "candidatesTokenCount": 4,
                                       "cachedContentTokenCount": 2, "thoughtsTokenCount": 1}
                }).to_string()),
            ],
            hold_open_ms: 0,
        }
    })
    .await;
    let dependency = LiveProviderCatalogDependency::development();
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_GEMINI,
            "gemini-2.0-flash",
            vec![user("inspect")],
            options_with_base(&format!("http://{base}")),
            "cancel-gemini",
        ))
        .await
        .expect("live execution");
    assert!(response.events.iter().any(|event| matches!(
        event,
        DependencyProviderEvent::ToolCallProposed {
            tool,
            ..
        } if tool == "read_file"
    )));
    match response.events.last() {
        Some(DependencyProviderEvent::Completed { usage, .. }) => {
            assert_eq!(usage.input_tokens, 8);
            assert_eq!(usage.cache_read_tokens, 2);
            assert_eq!(usage.reasoning_tokens, 1);
        }
        other => panic!("expected completion, got {other:?}"),
    }
}

#[tokio::test]
async fn image_inputs_are_serialized_into_the_request_body() {
    let received = Arc::new(Mutex::new(Vec::<u8>::new()));
    let captured = received.clone();
    let base = spawn_server(move |_headers, body| {
        *captured.lock().expect("capture") = body.to_vec();
        ResponsePlan::SseChunks {
            chunks: vec![
                sse(&json!({"choices": [{"delta": {"content": "seen"}, "finish_reason": "stop"}],
                           "usage": {"prompt_tokens": 9, "completion_tokens": 1}}).to_string()),
                sse("[DONE]"),
            ],
            hold_open_ms: 0,
        }
    })
    .await;
    let dependency = LiveProviderCatalogDependency::development();
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_LOCAL,
            "local-model",
            vec![
                user("describe"),
                DependencyConversationEntry::Image {
                    media_type: "image/png".into(),
                    data_base64: "aGVsbG8=".into(),
                },
            ],
            options_with_base(&format!("http://{base}/v1")),
            "cancel-image",
        ))
        .await
        .expect("live execution");
    assert!(matches!(response.events.last(), Some(DependencyProviderEvent::Completed { .. })));
    let body = String::from_utf8(received.lock().expect("received").clone()).expect("utf8");
    let value: serde_json::Value = serde_json::from_str(&body).expect("request json");
    assert_eq!(
        value["messages"][1]["content"][0]["image_url"]["url"],
        "data:image/png;base64,aGVsbG8="
    );
}

#[tokio::test]
async fn cancellation_during_stream_emits_cancelled_and_stops() {
    let base = spawn_server(move |_headers, _body| {
        ResponsePlan::SseChunks {
            chunks: vec![
                sse(&json!({"choices": [{"delta": {"content": "partial"}, "finish_reason": null}]}).to_string()),
            ],
            hold_open_ms: 60_000,
        }
    })
    .await;
    let dependency = LiveProviderCatalogDependency::development();
    let task = tokio::spawn({
        let dependency = dependency.clone();
        async move {
            dependency
                .execute_provider(live_request(
                    PROVIDER_LOCAL,
                    "local-model",
                    vec![user("stream forever")],
                    options_with_base(&format!("http://{base}/v1")),
                    "cancel-mid-stream",
                ))
                .await
                .expect("live execution")
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        dependency
            .cancel_provider("cancel-mid-stream")
            .await
            .expect("cancel")
    );
    let response = task.await.expect("task");
    assert!(response.events.iter().any(|event| matches!(
        event,
        DependencyProviderEvent::TextDelta(text) if text == "partial"
    )));
    assert_eq!(
        response.events.last(),
        Some(&DependencyProviderEvent::Cancelled)
    );
}

#[tokio::test]
async fn authentication_headers_are_sent_and_never_leaked_in_errors() {
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
    })
    .await;
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
        .await
        .expect("live execution");
    let auth = seen_auth.lock().expect("auth").clone().expect("auth header");
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
        "secret leaked into collected events"
    );
}

#[tokio::test]
async fn http_statuses_are_classified_without_auto_retry_of_ambiguous_cases() {
    let cases: Vec<(u16, u64, DependencyProviderFailureKind, DependencyRetryClassification)> = vec![
        (401, 0, DependencyProviderFailureKind::AuthenticationFailed, DependencyRetryClassification::Never),
        (429, 2, DependencyProviderFailureKind::RateLimited, DependencyRetryClassification::AfterMilliseconds(2_000)),
        (500, 0, DependencyProviderFailureKind::ProviderOverloaded, DependencyRetryClassification::AfterMilliseconds(1_000)),
        (400, 0, DependencyProviderFailureKind::InvalidRequest, DependencyRetryClassification::Never),
        (404, 0, DependencyProviderFailureKind::UnsupportedCapability, DependencyRetryClassification::Never),
    ];
    for (index, (status, retry_after, expected_kind, expected_retry)) in cases.into_iter().enumerate() {
        let base = spawn_server(move |_headers, _body| {
            let mut headers = vec![("Content-Type".into(), "application/json".into())];
            if retry_after > 0 {
                headers.push(("Retry-After".into(), retry_after.to_string()));
            }
            ResponsePlan::Full {
                status,
                headers,
                body: br#"{"error":{"message":"fixture error"}}"#.to_vec(),
            }
        })
        .await;
        let dependency = LiveProviderCatalogDependency::development();
        let response = dependency
            .execute_provider(live_request(
                PROVIDER_LOCAL,
                "local-model",
                vec![user("hi")],
                options_with_base(&format!("http://{base}/v1")),
                &format!("cancel-status-{index}"),
            ))
            .await
            .expect("live execution");
        assert_eq!(response.events.len(), 1);
        match response.events.last() {
            Some(DependencyProviderEvent::Failed { kind, retry, .. }) => {
                assert_eq!(kind, &expected_kind, "status {status}");
                assert_eq!(retry, &expected_retry, "status {status}");
            }
            other => panic!("expected failure for status {status}, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn ambiguous_disconnect_after_partial_output_is_never_retried() {
    let base = spawn_server(move |_headers, _body| {
        ResponsePlan::SseChunks {
            chunks: vec![
                sse(&json!({"choices": [{"delta": {"content": "partial"}, "finish_reason": null}]}).to_string()),
            ],
            // Hold open briefly, then the fixture aborts the connection by
            // dropping the socket without a terminating chunk.
            hold_open_ms: 50,
        }
    })
    .await;
    let dependency = LiveProviderCatalogDependency::development();
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_LOCAL,
            "local-model",
            vec![user("hi")],
            options_with_base(&format!("http://{base}/v1")),
            "cancel-ambiguous",
        ))
        .await
        .expect("live execution");
    let last = response.events.last().expect("last event");
    match last {
        DependencyProviderEvent::Failed {
            kind,
            retry,
            ..
        } => {
            assert_eq!(kind, &DependencyProviderFailureKind::AmbiguousDisconnect);
            assert_eq!(retry, &DependencyRetryClassification::Never);
        }
        other => panic!("expected ambiguous disconnect, got {other:?}"),
    }
}

#[tokio::test]
async fn malformed_stream_events_fail_closed_bounded() {
    let base = spawn_server(move |_headers, _body| {
        ResponsePlan::SseChunks {
            chunks: vec![b"data: {not-json}\n\n".to_vec()],
            hold_open_ms: 0,
        }
    })
    .await;
    let dependency = LiveProviderCatalogDependency::development();
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_LOCAL,
            "local-model",
            vec![user("hi")],
            options_with_base(&format!("http://{base}/v1")),
            "cancel-malformed",
        ))
        .await
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

#[tokio::test]
async fn anthropic_keepalives_and_gemini_errors_are_normalized() {
    // Anthropic pings between deltas are ignored.
    let base = spawn_server(move |_headers, _body| {
        ResponsePlan::SseChunks {
            chunks: vec![
                sse_event("ping", r#"{"type":"ping"}"#),
                sse_event("message_start", r#"{"type":"message_start","message":{"usage":{"input_tokens":1,"output_tokens":0}}}"#),
                sse_event("content_block_start", r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#),
                sse_event("content_block_delta", r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"pong"}}"#),
                sse_event("content_block_stop", r#"{"type":"content_block_stop","index":0}"#),
                sse_event("message_delta", r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}"#),
                sse_event("message_stop", r#"{"type":"message_stop"}"#),
            ],
            hold_open_ms: 0,
        }
    })
    .await;
    let dependency = LiveProviderCatalogDependency::development();
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_ANTHROPIC,
            "claude-3-5-haiku-latest",
            vec![user("hi")],
            options_with_base(&format!("http://{base}")),
            "cancel-ping",
        ))
        .await
        .expect("live execution");
    let text: String = response
        .events
        .iter()
        .filter_map(|event| match event {
            DependencyProviderEvent::TextDelta(text) => Some(text.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(text, "pong");

    // Gemini overloaded_error is classified without retry ambiguity.
    let base = spawn_server(move |_headers, _body| {
        ResponsePlan::SseChunks {
            chunks: vec![
                sse(r#"{"error":{"code":503,"message":"overloaded","status":"UNAVAILABLE"}}"#),
            ],
            hold_open_ms: 0,
        }
    })
    .await;
    let dependency = LiveProviderCatalogDependency::development();
    let response = dependency
        .execute_provider(live_request(
            PROVIDER_GEMINI,
            "gemini-2.0-flash",
            vec![user("hi")],
            options_with_base(&format!("http://{base}")),
            "cancel-gemini-error",
        ))
        .await
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
