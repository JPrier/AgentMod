//! OpenAI-compatible Chat Completions wire adapter.
//!
//! Shared by the generic OpenAI-compatible, OpenRouter, OpenAI, and local
//! endpoints. Provider-specific serialization stays in dependency.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use crate::execution::{
    DependencyConversationEntry, DependencyProviderEvent, DependencyUsage,
};

/// Bounded tool-call accumulator for one streamed tool call.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ToolCallBuffer {
    /// Provider call id.
    pub call_id: String,
    /// Accumulated function name.
    pub name: String,
    /// Accumulated argument JSON bytes.
    pub arguments: String,
}

/// Normalizes OpenAI-compatible stream chunks into provider events.
#[derive(Clone, Debug)]
pub struct OpenAiStreamNormalizer {
    tool_buffers: BTreeMap<usize, ToolCallBuffer>,
    finish_reason: Option<String>,
    usage: Option<DependencyUsage>,
    /// True once any text or tool delta was emitted.
    pub started: bool,
}

/// Accumulated final tool proposals.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalToolCall {
    /// Provider call id.
    pub call_id: String,
    /// Stable tool name.
    pub name: String,
    /// Complete argument JSON.
    pub arguments_json: String,
}

impl Default for OpenAiStreamNormalizer {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenAiStreamNormalizer {
    /// Creates a fresh normalizer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tool_buffers: BTreeMap::new(),
            finish_reason: None,
            usage: None,
            started: false,
        }
    }

    /// Consumes one chunk and returns normalized events.
    pub fn handle(&mut self, chunk: &Value) -> Result<Vec<DependencyProviderEvent>, String> {
        let mut events = Vec::new();
        if let Some(usage) = chunk.get("usage") {
            self.usage = Some(parse_usage(usage));
        }
        let Some(choices) = chunk.get("choices").and_then(Value::as_array) else {
            return Ok(events);
        };
        for choice in choices {
            if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
                self.finish_reason = Some(reason.to_owned());
            }
            let Some(delta) = choice.get("delta") else {
                continue;
            };
            if let Some(text) = delta.get("content").and_then(Value::as_str) {
                if !text.is_empty() {
                    self.started = true;
                    events.push(DependencyProviderEvent::TextDelta(text.to_owned()));
                }
            }
            if let Some(deltas) = delta.get("tool_calls").and_then(Value::as_array) {
                for tool_call in deltas {
                    let index = tool_call
                        .get("index")
                        .and_then(Value::as_u64)
                        .unwrap_or(0) as usize;
                    let buffer = self
                        .tool_buffers
                        .entry(index)
                        .or_insert_with(ToolCallBuffer::default);
                    if let Some(id) = tool_call.get("id").and_then(Value::as_str) {
                        buffer.call_id = id.to_owned();
                    }
                    if let Some(function) = tool_call.get("function") {
                        if let Some(name) = function.get("name").and_then(Value::as_str) {
                            buffer.name.push_str(name);
                        }
                        if let Some(arguments) =
                            function.get("arguments").and_then(Value::as_str)
                        {
                            if !arguments.is_empty() {
                                self.started = true;
                                buffer.arguments.push_str(arguments);
                                events.push(DependencyProviderEvent::ToolCallDelta {
                                    call_id: if buffer.call_id.is_empty() {
                                        format!("tool-call-{index}")
                                    } else {
                                        buffer.call_id.clone()
                                    },
                                    name_fragment: String::new(),
                                    arguments_fragment: arguments.to_owned(),
                                });
                            }
                        }
                    }
                }
            }
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

    /// Completes the stream, flushing any pending tool-call proposals.
    ///
    /// # Errors
    ///
    /// Returns a redacted diagnostic when accumulated tool arguments are not
    /// valid JSON.
    pub fn finish(&mut self) -> Result<Vec<FinalToolCall>, String> {
        let calls: Vec<FinalToolCall> = self
            .tool_buffers
            .values()
            .filter(|buffer| !buffer.name.is_empty() || !buffer.arguments.is_empty())
            .map(|buffer| FinalToolCall {
                call_id: if buffer.call_id.is_empty() {
                    format!("tool-call-{}", 0)
                } else {
                    buffer.call_id.clone()
                },
                name: buffer.name.clone(),
                arguments_json: if buffer.arguments.is_empty() {
                    "{}".to_owned()
                } else {
                    buffer.arguments.clone()
                },
            })
            .collect();
        for call in &calls {
            serde_json::from_str::<Value>(&call.arguments_json)
                .map_err(|error| format!("malformed tool arguments: {error}"))?;
        }
        self.tool_buffers.clear();
        Ok(calls)
    }
}

fn parse_usage(usage: &Value) -> DependencyUsage {
    let input_tokens = usage
        .get("prompt_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output_tokens = usage
        .get("completion_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cache_read_tokens = usage
        .pointer("/prompt_tokens_details/cached_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let reasoning_tokens = usage
        .pointer("/completion_tokens_details/reasoning_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    DependencyUsage {
        input_tokens,
        output_tokens,
        cache_read_tokens,
        cache_write_tokens: 0,
        reasoning_tokens,
        estimated: false,
    }
}

/// Builds the OpenAI-compatible request body.
///
/// # Errors
///
/// Returns a redacted diagnostic when options are malformed or bounds exceeded.
pub fn build_request_body(
    model: &str,
    entries: &[DependencyConversationEntry],
    options: &BTreeMap<String, String>,
) -> Result<Value, String> {
    let messages = build_messages(entries, options)?;
    let mut body = json!({
        "model": model,
        "messages": messages,
        "stream": parse_bool_option(options, "streaming").unwrap_or(true),
    });
    if let Some(value) = options.get("temperature") {
        body["temperature"] = parse_f64(value, "temperature")?;
    }
    if let Some(value) = options.get("max_tokens") {
        body["max_tokens"] = parse_u64(value, "max_tokens")?;
    }
    if let Some(value) = options.get("top_p") {
        body["top_p"] = parse_f64(value, "top_p")?;
    }
    if let Some(value) = options.get("stop") {
        body["stop"] = parse_json(value, "stop")?;
    }
    if let Some(value) = options.get("tools") {
        body["tools"] = parse_json(value, "tools")?;
    }
    if let Some(value) = options.get("tool_choice") {
        body["tool_choice"] = parse_json(value, "tool_choice")?;
    }
    if let Some(value) = options.get("response_format") {
        body["response_format"] = parse_json(value, "response_format")?;
    }
    if let Some(value) = options.get("reasoning_effort") {
        if value != "low" && value != "medium" && value != "high" && value != "minimal" {
            return Err(format!("unsupported reasoning_effort `{value}`"));
        }
        body["reasoning_effort"] = Value::String(value.clone());
    }
    if let Some(value) = options.get("max_completion_tokens") {
        body["max_completion_tokens"] = parse_u64(value, "max_completion_tokens")?;
    }
    Ok(body)
}

fn build_messages(
    entries: &[DependencyConversationEntry],
    _options: &BTreeMap<String, String>,
) -> Result<Vec<Value>, String> {
    let mut messages: Vec<Value> = Vec::new();
    for entry in entries {
        let message = match entry {
            DependencyConversationEntry::System(text) => json!({
                "role": "system",
                "content": text,
            }),
            DependencyConversationEntry::User(text) => json!({
                "role": "user",
                "content": text,
            }),
            DependencyConversationEntry::Image {
                media_type,
                data_base64,
            } => json!({
                "role": "user",
                "content": [{
                    "type": "image_url",
                    "image_url": {
                        "url": format!("data:{media_type};base64,{data_base64}")
                    }
                }],
            }),
            DependencyConversationEntry::Assistant(text) => json!({
                "role": "assistant",
                "content": text,
            }),
            DependencyConversationEntry::ToolCall {
                call_id,
                tool,
                arguments_json,
            } => json!({
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": call_id,
                    "type": "function",
                    "function": {
                        "name": tool,
                        "arguments": arguments_json,
                    },
                }],
            }),
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
                json!({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": content,
                })
            }
            DependencyConversationEntry::ContextSummary {
                text,
                source_start,
                source_end,
            } => json!({
                "role": "user",
                "content": format!(
                    "[context summary of sequences {source_start}..={source_end}]\n{text}"
                ),
            }),
            DependencyConversationEntry::Metadata { key, value_json } => {
                let parsed = serde_json::from_str::<Value>(value_json).map_err(|error| {
                    format!("metadata `{key}` is not valid JSON: {error}")
                })?;
                json!({
                    "role": "user",
                    "content": format!("[{key} metadata]\n{parsed}"),
                })
            }
        };
        messages.push(message);
    }
    if messages.is_empty() || messages.len() > 256 {
        return Err("provider projection must contain 1..=256 entries".into());
    }
    Ok(messages)
}

fn parse_json(value: &str, key: &str) -> Result<Value, String> {
    serde_json::from_str(value).map_err(|error| format!("option `{key}` is not valid JSON: {error}"))
}

fn parse_bool_option(options: &BTreeMap<String, String>, key: &str) -> Option<bool> {
    options.get(key).and_then(|value| match value.as_str() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    })
}

fn parse_f64(value: &str, key: &str) -> Result<Value, String> {
    value
        .parse::<f64>()
        .map(Value::from)
        .map_err(|_| format!("option `{key}` must be a number"))
}

fn parse_u64(value: &str, key: &str) -> Result<Value, String> {
    value
        .parse::<u64>()
        .map(Value::from)
        .map_err(|_| format!("option `{key}` must be a non-negative integer"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry_user(text: &str) -> DependencyConversationEntry {
        DependencyConversationEntry::User(text.to_owned())
    }

    #[test]
    fn normalizes_text_and_tool_stream_chunks() {
        let mut normalizer = OpenAiStreamNormalizer::new();
        let chunk = json!({
            "choices": [{"delta": {"content": "hello"}, "finish_reason": null}]
        });
        let events = normalizer.handle(&chunk).expect("chunk");
        assert_eq!(events, [DependencyProviderEvent::TextDelta("hello".into())]);

        let tool_chunk = json!({
            "choices": [{"delta": {"tool_calls": [{
                "index": 0,
                "id": "call-1",
                "function": {"name": "read_file", "arguments": "{\"pa"}
            }]}, "finish_reason": null}]
        });
        let events = normalizer.handle(&tool_chunk).expect("tool chunk");
        assert_eq!(
            events,
            [DependencyProviderEvent::ToolCallDelta {
                call_id: "call-1".into(),
                name_fragment: String::new(),
                arguments_fragment: "{\"pa".into(),
            }]
        );
        normalizer
            .handle(&json!({
                "choices": [{
                    "delta": {
                        "tool_calls": [{
                            "index": 0,
                            "function": {"arguments": "th\":\"src/lib.rs\"}"}
                        }]
                    }
                }]
            }))
            .expect("tool completion chunk");

        let finish = json!({
            "choices": [{"delta": {}, "finish_reason": "tool_calls"}],
            "usage": {"prompt_tokens": 12, "completion_tokens": 7,
                      "prompt_tokens_details": {"cached_tokens": 3},
                      "completion_tokens_details": {"reasoning_tokens": 2}}
        });
        let events = normalizer.handle(&finish).expect("finish chunk");
        assert!(events.is_empty());
        let calls = normalizer.finish().expect("proposals");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "read_file");
        assert_eq!(calls[0].call_id, "call-1");
        assert_eq!(calls[0].arguments_json, "{\"path\":\"src/lib.rs\"}");
        let usage = normalizer.usage().expect("usage");
        assert_eq!(usage.input_tokens, 12);
        assert_eq!(usage.output_tokens, 7);
        assert_eq!(usage.cache_read_tokens, 3);
        assert_eq!(usage.reasoning_tokens, 2);
    }

    #[test]
    fn multiple_tool_calls_are_accumulated_by_index() {
        let mut normalizer = OpenAiStreamNormalizer::new();
        normalizer
            .handle(&json!({"choices": [{"delta": {"tool_calls": [
                {"index": 0, "id": "a", "function": {"name": "read", "arguments": "{\"x\":1}"}},
                {"index": 1, "id": "b", "function": {"name": "write", "arguments": "{\"x\":2}"}}
            ]}}]}))
            .expect("chunk");
        normalizer
            .handle(&json!({"choices": [{"delta": {}, "finish_reason": "tool_calls"}]}))
            .expect("finish");
        let calls = normalizer.finish().expect("proposals");
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].name, "read");
        assert_eq!(calls[1].name, "write");
    }

    #[test]
    fn malformed_tool_arguments_fail_closed() {
        let mut normalizer = OpenAiStreamNormalizer::new();
        normalizer
            .handle(&json!({"choices": [{"delta": {"tool_calls": [
                {"index": 0, "id": "a", "function": {"name": "read", "arguments": "{not-json"}}
            ]}}]}))
            .expect("chunk");
        assert!(normalizer.finish().is_err());
    }

    #[test]
    fn builds_request_with_images_tools_and_structured_output() {
        let mut options = BTreeMap::new();
        options.insert("tools".into(), r#"[{"type":"function","function":{"name":"read_file"}}]"#.into());
        options.insert("response_format".into(), r#"{"type":"json_object"}"#.into());
        options.insert("reasoning_effort".into(), "medium".into());
        let body = build_request_body(
            "gpt-4o-mini",
            &[
                entry_user("look"),
                DependencyConversationEntry::Image {
                    media_type: "image/png".into(),
                    data_base64: "aGVsbG8=".into(),
                },
            ],
            &options,
        )
        .expect("request body");
        assert_eq!(body["model"], "gpt-4o-mini");
        assert_eq!(body["messages"][0]["content"], "look");
        assert_eq!(
            body["messages"][1]["content"][0]["image_url"]["url"],
            "data:image/png;base64,aGVsbG8="
        );
        assert_eq!(body["reasoning_effort"], "medium");
        assert!(body["tools"].is_array());
    }
}
