//! Google Gemini API wire adapter.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use crate::execution::{DependencyConversationEntry, DependencyProviderEvent, DependencyUsage};

/// Normalizes Gemini `streamGenerateContent` chunks into provider events.
#[derive(Clone, Debug, Default)]
pub struct GeminiStreamNormalizer {
    finish_reason: Option<String>,
    usage: Option<DependencyUsage>,
    /// True once any text or tool delta was emitted.
    pub started: bool,
}

impl GeminiStreamNormalizer {
    /// Creates a fresh normalizer.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Consumes one Gemini response chunk and returns normalized events.
    ///
    /// # Errors
    ///
    /// Returns a redacted diagnostic when a chunk cannot be normalized.
    pub fn handle(&mut self, chunk: &Value) -> Result<Vec<DependencyProviderEvent>, String> {
        let mut events = Vec::new();
        if let Some(metadata) = chunk.get("usageMetadata") {
            self.usage = Some(merge_usage(
                self.usage.unwrap_or_default(),
                parse_usage(metadata),
            ));
        }
        let Some(candidates) = chunk.get("candidates").and_then(Value::as_array) else {
            return Ok(events);
        };
        for candidate in candidates {
            if let Some(reason) = candidate.get("finishReason").and_then(Value::as_str) {
                self.finish_reason = Some(reason.to_owned());
            }
            let Some(content) = candidate.get("content") else {
                continue;
            };
            let Some(parts) = content.get("parts").and_then(Value::as_array) else {
                continue;
            };
            for part in parts {
                if let Some(text) = part.get("text").and_then(Value::as_str)
                    && !text.is_empty()
                {
                    self.started = true;
                    events.push(DependencyProviderEvent::TextDelta(text.to_owned()));
                }
                if let Some(call) = part.get("functionCall") {
                    let name = call.get("name").and_then(Value::as_str).unwrap_or("");
                    let arguments = call.get("args").cloned().unwrap_or_else(|| json!({}));
                    if !name.is_empty() {
                        self.started = true;
                        let arguments_json = arguments.to_string();
                        let call_id = format!("gemini-call-{}", events.len());
                        events.push(DependencyProviderEvent::ToolCallDelta {
                            call_id: call_id.clone(),
                            name_fragment: String::new(),
                            arguments_fragment: arguments_json.clone(),
                        });
                        events.push(DependencyProviderEvent::ToolCallProposed {
                            continuation_reference: call_id.clone(),
                            call_id,
                            tool: name.to_owned(),
                            arguments_json,
                        });
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
}

fn parse_usage(metadata: &Value) -> DependencyUsage {
    DependencyUsage {
        input_tokens: metadata
            .get("promptTokenCount")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        output_tokens: metadata
            .get("candidatesTokenCount")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_read_tokens: metadata
            .get("cachedContentTokenCount")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_write_tokens: 0,
        reasoning_tokens: metadata
            .get("thoughtsTokenCount")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        estimated: false,
    }
}

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

/// Builds the Gemini generateContent request body.
///
/// # Errors
///
/// Returns a redacted diagnostic when options are malformed or bounds exceeded.
pub fn build_request_body(
    _model: &str,
    entries: &[DependencyConversationEntry],
    options: &BTreeMap<String, String>,
) -> Result<Value, String> {
    let contents = build_contents(entries)?;
    let mut body = json!({
        "contents": contents,
    });
    let system: Vec<String> = entries
        .iter()
        .filter_map(|entry| match entry {
            DependencyConversationEntry::System(text) => Some(text.clone()),
            _ => None,
        })
        .collect();
    if !system.is_empty() {
        body["systemInstruction"] = json!({
            "parts": system.into_iter().map(|text| json!({"text": text})).collect::<Vec<_>>()
        });
    }
    let mut generation = serde_json::Map::new();
    if let Some(value) = options.get("temperature") {
        generation.insert("temperature".into(), parse_f64(value, "temperature")?);
    }
    if let Some(value) = options.get("max_tokens") {
        generation.insert("maxOutputTokens".into(), parse_u64(value, "max_tokens")?);
    }
    if let Some(value) = options.get("top_p") {
        generation.insert("topP".into(), parse_f64(value, "top_p")?);
    }
    if let Some(value) = options.get("stop") {
        generation.insert("stopSequences".into(), parse_json(value, "stop")?);
    }
    if let Some(value) = options.get("response_format") {
        let parsed = parse_json(value, "response_format")?;
        if parsed.get("type").and_then(Value::as_str) == Some("json_schema") {
            if let Some(schema) = parsed.get("json_schema") {
                generation.insert(
                    "responseMimeType".into(),
                    Value::String("application/json".into()),
                );
                generation.insert("responseSchema".into(), schema.clone());
            }
        } else if parsed.get("type").and_then(Value::as_str) == Some("json_object") {
            generation.insert(
                "responseMimeType".into(),
                Value::String("application/json".into()),
            );
        }
    }
    if let Some(value) = options.get("thinking")
        && value == "enabled"
    {
        generation.insert("thinkingConfig".into(), json!({"includeThoughts": false}));
    }
    if !generation.is_empty() {
        body["generationConfig"] = Value::Object(generation);
    }
    if let Some(value) = options.get("tools") {
        let parsed = parse_json(value, "tools")?;
        let functions = parsed
            .as_array()
            .map(|tools| {
                tools
                    .iter()
                    .filter_map(|tool| {
                        let function = tool
                            .get("function")
                            .or(Some(tool))?;
                        let name = function.get("name").and_then(Value::as_str)?;
                        Some(json!({
                            "name": name,
                            "description": function.get("description").cloned().unwrap_or(Value::Null),
                            "parameters": function.get("parameters").cloned().unwrap_or_else(|| json!({"type": "object", "properties": {}})),
                        }))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if !functions.is_empty() {
            body["tools"] = json!([{"functionDeclarations": functions}]);
        }
    }
    Ok(body)
}

fn build_contents(entries: &[DependencyConversationEntry]) -> Result<Vec<Value>, String> {
    let mut contents: Vec<Value> = Vec::new();
    for entry in entries {
        let (role, part) = match entry {
            DependencyConversationEntry::System(_) => continue,
            DependencyConversationEntry::User(text) => ("user", json!({"text": text})),
            DependencyConversationEntry::Image {
                media_type,
                data_base64,
            } => (
                "user",
                json!({"inline_data": {"mime_type": media_type, "data": data_base64}}),
            ),
            DependencyConversationEntry::Assistant(text) => ("model", json!({"text": text})),
            DependencyConversationEntry::ToolCall {
                call_id: _,
                tool,
                arguments_json,
            } => {
                let arguments = serde_json::from_str::<Value>(arguments_json)
                    .map_err(|error| format!("tool arguments are not valid JSON: {error}"))?;
                (
                    "model",
                    json!({"functionCall": {"name": tool, "args": arguments}}),
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
                    "function",
                    json!({"functionResponse": {
                        "name": call_id,
                        "response": {"result": content},
                    }}),
                )
            }
            DependencyConversationEntry::ContextSummary {
                text,
                source_start,
                source_end,
            } => (
                "user",
                json!({"text": format!(
                    "[context summary of sequences {source_start}..={source_end}]\n{text}"
                )}),
            ),
            DependencyConversationEntry::Metadata { key, value_json } => {
                let parsed = serde_json::from_str::<Value>(value_json)
                    .map_err(|error| format!("metadata `{key}` is not valid JSON: {error}"))?;
                (
                    "user",
                    json!({"text": format!("[{key} metadata]\n{parsed}")}),
                )
            }
        };
        if let Some(previous) = contents.last_mut()
            && previous.get("role").and_then(Value::as_str) == Some(role)
            && role != "function"
            && previous.get("role").and_then(Value::as_str) != Some("function")
        {
            previous["parts"]
                .as_array_mut()
                .expect("parts are an array")
                .push(part);
            continue;
        }
        contents.push(json!({"role": role, "parts": [part]}));
    }
    if contents.is_empty() || contents.len() > 256 {
        return Err("provider projection must contain 1..=256 contents".into());
    }
    Ok(contents)
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

fn parse_u64(value: &str, key: &str) -> Result<Value, String> {
    value
        .parse::<u64>()
        .map(Value::from)
        .map_err(|_| format!("option `{key}` must be a non-negative integer"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_text_and_function_calls() {
        let mut normalizer = GeminiStreamNormalizer::new();
        let mut events = normalizer
            .handle(&json!({
                "candidates": [{
                    "content": {"parts": [{"text": "hello"}], "role": "model"},
                    "finishReason": "STOP"
                }],
                "usageMetadata": {"promptTokenCount": 12, "candidatesTokenCount": 7,
                                   "cachedContentTokenCount": 3, "thoughtsTokenCount": 2}
            }))
            .expect("chunk");
        events.extend(
            normalizer
                .handle(&json!({
                    "candidates": [{
                        "content": {"parts": [{"functionCall": {
                            "name": "read_file", "args": {"path": "x"}
                        }}]},
                        "finishReason": "STOP"
                    }]
                }))
                .expect("chunk"),
        );
        assert_eq!(
            events[0],
            DependencyProviderEvent::TextDelta("hello".into())
        );
        assert!(matches!(
            events.last(),
            Some(DependencyProviderEvent::ToolCallProposed {
                tool,
                arguments_json,
                ..
            }) if tool == "read_file" && arguments_json.contains("\"path\"")
        ));
        assert_eq!(normalizer.finish_reason(), Some("STOP"));
        let usage = normalizer.usage().expect("usage");
        assert_eq!(usage.input_tokens, 12);
        assert_eq!(usage.output_tokens, 7);
        assert_eq!(usage.cache_read_tokens, 3);
        assert_eq!(usage.reasoning_tokens, 2);
    }

    #[test]
    fn builds_request_with_images_and_functions() {
        let mut options = BTreeMap::new();
        options.insert(
            "tools".into(),
            r#"[{"function":{"name":"read_file","parameters":{"type":"object"}}}]"#.into(),
        );
        options.insert(
            "response_format".into(),
            r#"{"type":"json_schema","json_schema":{"type":"object"}}"#.into(),
        );
        let body = build_request_body(
            "gemini-2.0-flash",
            &[DependencyConversationEntry::Image {
                media_type: "image/png".into(),
                data_base64: "aGVsbG8=".into(),
            }],
            &options,
        )
        .expect("request body");
        assert_eq!(
            body["contents"][0]["parts"][0]["inline_data"]["data"],
            "aGVsbG8="
        );
        assert_eq!(
            body["tools"][0]["functionDeclarations"][0]["name"],
            "read_file"
        );
        assert_eq!(
            body["generationConfig"]["responseMimeType"],
            "application/json"
        );
    }
}
