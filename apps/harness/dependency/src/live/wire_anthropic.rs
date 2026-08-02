//! Anthropic Messages API wire adapter.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use crate::execution::{DependencyConversationEntry, DependencyProviderEvent, DependencyUsage};

/// Accumulated Anthropic content-block tool call.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AnthropicToolBlock {
    /// Tool-use call id.
    pub call_id: String,
    /// Tool name.
    pub name: String,
    /// Accumulated input JSON.
    pub arguments: String,
}

/// Normalizes Anthropic `messages` SSE events into provider events.
#[derive(Clone, Debug)]
pub struct AnthropicStreamNormalizer {
    tool_blocks: BTreeMap<usize, AnthropicToolBlock>,
    finish_reason: Option<String>,
    usage: Option<DependencyUsage>,
    /// True once any text or tool delta was emitted.
    pub started: bool,
}

impl Default for AnthropicStreamNormalizer {
    fn default() -> Self {
        Self::new()
    }
}

impl AnthropicStreamNormalizer {
    /// Creates a fresh normalizer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tool_blocks: BTreeMap::new(),
            finish_reason: None,
            usage: None,
            started: false,
        }
    }

    /// Consumes one Anthropic SSE event payload and returns normalized
    /// events.
    ///
    /// # Errors
    ///
    /// Returns a redacted diagnostic when an event cannot be normalized.
    #[allow(
        clippy::too_many_lines,
        reason = "the event matrix is intentionally explicit for auditability"
    )]
    pub fn handle(
        &mut self,
        event_type: &str,
        payload: &Value,
    ) -> Result<Vec<DependencyProviderEvent>, String> {
        let mut events = Vec::new();
        match event_type {
            "message_start" => {
                if let Some(usage) = payload.pointer("/message/usage") {
                    self.usage = Some(merge_usage(
                        self.usage.unwrap_or_default(),
                        parse_usage(usage),
                    ));
                }
            }
            "content_block_start" => {
                let index =
                    usize::try_from(payload.get("index").and_then(Value::as_u64).unwrap_or(0))
                        .unwrap_or(0);
                if let Some(block) = payload.get("content_block")
                    && block.get("type").and_then(Value::as_str) == Some("tool_use")
                {
                    let buffer = self.tool_blocks.entry(index).or_default();
                    if let Some(id) = block.get("id").and_then(Value::as_str) {
                        id.clone_into(&mut buffer.call_id);
                    }
                    if let Some(name) = block.get("name").and_then(Value::as_str) {
                        name.clone_into(&mut buffer.name);
                    }
                }
            }
            "content_block_delta" => {
                let index =
                    usize::try_from(payload.get("index").and_then(Value::as_u64).unwrap_or(0))
                        .unwrap_or(0);
                if let Some(delta) = payload.get("delta") {
                    // Reasoning deltas are never surfaced as visible text.
                    match delta.get("type").and_then(Value::as_str) {
                        Some("text_delta") => {
                            if let Some(text) = delta.get("text").and_then(Value::as_str)
                                && !text.is_empty()
                            {
                                self.started = true;
                                events.push(DependencyProviderEvent::TextDelta(text.to_owned()));
                            }
                        }
                        Some("input_json_delta") => {
                            if let Some(fragment) =
                                delta.get("partial_json").and_then(Value::as_str)
                                && !fragment.is_empty()
                            {
                                self.started = true;
                                let buffer = self.tool_blocks.entry(index).or_default();
                                buffer.arguments.push_str(fragment);
                                events.push(DependencyProviderEvent::ToolCallDelta {
                                    call_id: if buffer.call_id.is_empty() {
                                        format!("tool-call-{index}")
                                    } else {
                                        buffer.call_id.clone()
                                    },
                                    name_fragment: String::new(),
                                    arguments_fragment: fragment.to_owned(),
                                });
                            }
                        }
                        _ => {}
                    }
                }
            }
            "content_block_stop" => {
                let index =
                    usize::try_from(payload.get("index").and_then(Value::as_u64).unwrap_or(0))
                        .unwrap_or(0);
                if let Some(buffer) = self.tool_blocks.get(&index)
                    && (!buffer.name.is_empty() || !buffer.arguments.is_empty())
                {
                    events.push(DependencyProviderEvent::ToolCallProposed {
                        continuation_reference: buffer.call_id.clone(),
                        call_id: if buffer.call_id.is_empty() {
                            format!("tool-call-{index}")
                        } else {
                            buffer.call_id.clone()
                        },
                        tool: buffer.name.clone(),
                        arguments_json: if buffer.arguments.is_empty() {
                            "{}".to_owned()
                        } else {
                            buffer.arguments.clone()
                        },
                    });
                }
            }
            "message_delta" => {
                if let Some(reason) = payload
                    .pointer("/delta/stop_reason")
                    .and_then(Value::as_str)
                {
                    self.finish_reason = Some(reason.to_owned());
                }
                if let Some(usage) = payload.get("usage") {
                    self.usage = Some(merge_usage(
                        self.usage.unwrap_or_default(),
                        parse_usage(usage),
                    ));
                }
            }
            // Terminal (`message_stop`) and keepalive (`ping`) events need no
            // further normalization; usage and stop reason are already captured.
            _ => {}
        }
        Ok(events)
    }

    /// Returns accumulated usage.
    #[must_use]
    pub fn usage(&self) -> Option<DependencyUsage> {
        self.usage
    }

    /// Returns the normalized finish reason.
    #[must_use]
    pub fn finish_reason(&self) -> Option<&str> {
        self.finish_reason.as_deref()
    }
}

fn parse_usage(usage: &Value) -> DependencyUsage {
    DependencyUsage {
        input_tokens: usage
            .get("input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        output_tokens: usage
            .get("output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_read_tokens: usage
            .get("cache_read_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_write_tokens: usage
            .get("cache_creation_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        reasoning_tokens: 0,
        estimated: false,
    }
}

/// Merges a later usage observation into the accumulated one. Anthropic splits
/// input/output usage across events, so fields are merged by maximum rather
/// than replaced.
fn merge_usage(current: DependencyUsage, incoming: DependencyUsage) -> DependencyUsage {
    DependencyUsage {
        input_tokens: current.input_tokens.max(incoming.input_tokens),
        output_tokens: current.output_tokens.max(incoming.output_tokens),
        cache_read_tokens: current.cache_read_tokens.max(incoming.cache_read_tokens),
        cache_write_tokens: current.cache_write_tokens.max(incoming.cache_write_tokens),
        reasoning_tokens: current.reasoning_tokens.max(incoming.reasoning_tokens),
        estimated: current.estimated || incoming.estimated,
    }
}

/// Builds the Anthropic Messages request body.
///
/// # Errors
///
/// Returns a redacted diagnostic when options are malformed or bounds exceeded.
#[allow(
    clippy::too_many_lines,
    reason = "the message builder maps every projection kind explicitly"
)]
pub fn build_request_body(
    model: &str,
    entries: &[DependencyConversationEntry],
    options: &BTreeMap<String, String>,
) -> Result<Value, String> {
    let system: Vec<String> = entries
        .iter()
        .filter_map(|entry| match entry {
            DependencyConversationEntry::System(text) => Some(text.clone()),
            _ => None,
        })
        .collect();
    let messages = build_messages(entries)?;
    let mut body = json!({
        "model": model,
        "messages": messages,
        "max_tokens": options
            .get("max_tokens")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(4_096),
        "stream": true,
    });
    if !system.is_empty() {
        body["system"] = Value::Array(
            system
                .into_iter()
                .map(|text| json!({"type": "text", "text": text}))
                .collect(),
        );
    }
    if let Some(value) = options.get("temperature") {
        body["temperature"] = parse_f64(value, "temperature")?;
    }
    if let Some(value) = options.get("top_p") {
        body["top_p"] = parse_f64(value, "top_p")?;
    }
    if let Some(value) = options.get("stop") {
        body["stop_sequences"] = parse_json(value, "stop")?;
    }
    if let Some(value) = options.get("tools") {
        body["tools"] = parse_json(value, "tools")?;
    }
    if let Some(value) = options.get("tool_choice") {
        body["tool_choice"] = parse_json(value, "tool_choice")?;
    }
    if let Some(value) = options.get("thinking") {
        body["thinking"] = parse_json(value, "thinking")?;
    }
    Ok(body)
}

#[allow(
    clippy::too_many_lines,
    reason = "the message builder maps every projection kind explicitly"
)]
fn build_messages(entries: &[DependencyConversationEntry]) -> Result<Vec<Value>, String> {
    let mut messages: Vec<Value> = Vec::new();
    let mut pending_tool_blocks: Vec<Value> = Vec::new();
    let mut pending_text: Vec<Value> = Vec::new();
    for entry in entries {
        let (role, content) = match entry {
            DependencyConversationEntry::System(_) => continue,
            DependencyConversationEntry::User(text) => (
                "user".to_owned(),
                vec![json!({"type": "text", "text": text})],
            ),
            DependencyConversationEntry::Image {
                media_type,
                data_base64,
            } => (
                "user".to_owned(),
                vec![json!({
                    "type": "image",
                    "source": {"type": "base64", "media_type": media_type, "data": data_base64}
                })],
            ),
            DependencyConversationEntry::Assistant(text) => (
                "assistant".to_owned(),
                vec![json!({"type": "text", "text": text})],
            ),
            DependencyConversationEntry::ToolCall {
                call_id,
                tool,
                arguments_json,
            } => {
                let arguments = serde_json::from_str::<Value>(arguments_json)
                    .map_err(|error| format!("tool arguments are not valid JSON: {error}"))?;
                (
                    "assistant".to_owned(),
                    vec![json!({
                        "type": "tool_use",
                        "id": call_id,
                        "name": tool,
                        "input": arguments,
                    })],
                )
            }
            DependencyConversationEntry::ToolResult {
                call_id,
                content,
                truncated,
            } => {
                let content = if *truncated {
                    format!("{content}\n[result truncated; full content is artifact-backed]")
                } else {
                    content.clone()
                };
                (
                    "user".to_owned(),
                    vec![json!({
                        "type": "tool_result",
                        "tool_use_id": call_id,
                        "content": content,
                    })],
                )
            }
            DependencyConversationEntry::ContextSummary {
                text,
                source_start,
                source_end,
            } => (
                "user".to_owned(),
                vec![json!({
                    "type": "text",
                    "text": format!(
                        "[context summary of sequences {source_start}..={source_end}]\n{text}"
                    ),
                })],
            ),
            DependencyConversationEntry::Metadata { key, value_json } => {
                let parsed = serde_json::from_str::<Value>(value_json)
                    .map_err(|error| format!("metadata `{key}` is not valid JSON: {error}"))?;
                (
                    "user".to_owned(),
                    vec![json!({
                        "type": "text",
                        "text": format!("[{key} metadata]\n{parsed}"),
                    })],
                )
            }
        };
        if role == "assistant" {
            // Merge consecutive assistant tool blocks into one message.
            if !pending_tool_blocks.is_empty() && pending_text.is_empty() {
                pending_tool_blocks.extend(content);
            } else {
                flush_message(&mut messages, &mut pending_text, &mut pending_tool_blocks);
                if content[0].get("type").and_then(Value::as_str) == Some("tool_use") {
                    pending_tool_blocks.extend(content);
                } else {
                    pending_text.extend(content);
                }
            }
        } else {
            flush_message(&mut messages, &mut pending_text, &mut pending_tool_blocks);
            messages.push(json!({"role": role, "content": content}));
        }
    }
    flush_message(&mut messages, &mut pending_text, &mut pending_tool_blocks);
    if messages.is_empty() || messages.len() > 256 {
        return Err("provider projection must contain 1..=256 messages".into());
    }
    Ok(messages)
}

fn flush_message(
    messages: &mut Vec<Value>,
    pending_text: &mut Vec<Value>,
    pending_tool_blocks: &mut Vec<Value>,
) {
    if pending_tool_blocks.is_empty() && pending_text.is_empty() {
        return;
    }
    let mut content = Vec::new();
    content.append(pending_text);
    content.append(pending_tool_blocks);
    messages.push(json!({"role": "assistant", "content": content}));
}

fn parse_json(value: &str, key: &str) -> Result<Value, String> {
    serde_json::from_str(value)
        .map_err(|error| format!("option `{key}` is not valid JSON: {error}"))
}

fn parse_f64(value: &str, key: &str) -> Result<Value, String> {
    value
        .parse::<f64>()
        .map(Value::from)
        .map_err(|_| format!("option `{key}` must be a number"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_anthropic_stream_events() {
        let mut normalizer = AnthropicStreamNormalizer::new();
        let mut events = Vec::new();
        events.extend(
            normalizer
                .handle(
                    "message_start",
                    &json!({"message": {"usage": {"input_tokens": 5, "output_tokens": 1}}}),
                )
                .expect("start"),
        );
        events.extend(
            normalizer
                .handle(
                    "content_block_start",
                    &json!({"index": 0, "content_block": {"type": "tool_use", "id": "call-1", "name": "read_file"}}),
                )
                .expect("block start"),
        );
        events.extend(
            normalizer
                .handle(
                    "content_block_delta",
                    &json!({"index": 0, "delta": {"type": "input_json_delta", "partial_json": "{\"pa"}}),
                )
                .expect("delta"),
        );
        events.extend(
            normalizer
                .handle(
                    "content_block_delta",
                    &json!({"index": 0, "delta": {"type": "input_json_delta", "partial_json": "th\":\"x\"}"}}),
                )
                .expect("delta"),
        );
        events.extend(
            normalizer
                .handle("content_block_stop", &json!({"index": 0}))
                .expect("block stop"),
        );
        assert!(matches!(
            events.last(),
            Some(DependencyProviderEvent::ToolCallProposed {
                tool,
                call_id,
                arguments_json,
                ..
            }) if tool == "read_file" && call_id == "call-1" && arguments_json == r#"{"path":"x"}"#
        ));
        assert!(normalizer.finish_reason().is_none());
    }

    #[test]
    fn message_delta_captures_stop_reason_and_usage() {
        let mut normalizer = AnthropicStreamNormalizer::new();
        normalizer
            .handle(
                "message_delta",
                &json!({"delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 7}}),
            )
            .expect("delta");
        assert_eq!(normalizer.finish_reason(), Some("end_turn"));
        assert_eq!(normalizer.usage().expect("usage").output_tokens, 7);
    }

    #[test]
    fn builds_request_with_system_images_and_tool_results() {
        let body = build_request_body(
            "claude-3-5-haiku-latest",
            &[
                DependencyConversationEntry::System("you are helpful".into()),
                DependencyConversationEntry::Image {
                    media_type: "image/png".into(),
                    data_base64: "aGVsbG8=".into(),
                },
                DependencyConversationEntry::ToolCall {
                    call_id: "call-1".into(),
                    tool: "read_file".into(),
                    arguments_json: r#"{"path":"x"}"#.into(),
                },
                DependencyConversationEntry::ToolResult {
                    call_id: "call-1".into(),
                    content: "content".into(),
                    truncated: false,
                },
            ],
            &BTreeMap::new(),
        )
        .expect("request body");
        assert_eq!(body["system"][0]["text"], "you are helpful");
        assert_eq!(body["messages"][0]["content"][0]["type"], "image");
        assert_eq!(body["messages"][1]["content"][0]["type"], "tool_use");
        assert_eq!(body["messages"][2]["content"][0]["type"], "tool_result");
    }
}
