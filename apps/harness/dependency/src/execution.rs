//! Provider-neutral execution dependency and deterministic mock provider.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::StaticProviderCatalogDependency;
use base64::{Engine as _, engine::general_purpose::STANDARD};

const MAX_EVENTS: usize = 64;
const CONTINUATION_ONE: &str = "018f6f83-7b80-7000-8000-000000000101";
const CONTINUATION_TWO: &str = "018f6f83-7b80-7000-8000-000000000102";

/// Provider-neutral dependency conversation entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DependencyConversationEntry {
    /// System instruction.
    System(String),
    /// User content.
    User(String),
    /// Visible assistant content.
    Assistant(String),
    /// Approved tool request serialized as JSON text.
    ToolCall {
        /// Stable call ID.
        call_id: String,
        /// Stable tool name.
        tool: String,
        /// Provider-independent JSON.
        arguments_json: String,
    },
    /// Bounded tool result.
    ToolResult {
        /// Matching call.
        call_id: String,
        /// Bounded visible content.
        content: String,
        /// Whether full content is artifact-backed.
        truncated: bool,
    },
    /// Typed context summary.
    ContextSummary {
        /// Summary text.
        text: String,
        /// First source event sequence.
        source_start: u64,
        /// Last source event sequence.
        source_end: u64,
    },
    /// Approved provider-visible metadata serialized as JSON.
    Metadata {
        /// Stable metadata key.
        key: String,
        /// JSON value.
        value_json: String,
    },
}

/// Dependency-owned provider option.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyProviderOption {
    /// Provider option key.
    pub key: String,
    /// Provider-neutral textual representation.
    pub value: String,
}

/// Dependency-owned provider execution request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyProviderExecutionRequest {
    /// Adapter key.
    pub provider_key: String,
    /// Provider model key.
    pub model_key: String,
    /// Approved projected conversation.
    pub entries: Vec<DependencyConversationEntry>,
    /// Approved provider options.
    pub options: Vec<DependencyProviderOption>,
    /// Runtime-issued authorization grant.
    pub authorization_grant: String,
    /// Request cancellation reference.
    pub cancellation_reference: String,
    /// True only for a fresh provider request approved after a runtime continuation.
    pub resumed_after_continuation: bool,
}

/// Provider-neutral usage returned by a dependency.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DependencyUsage {
    /// Provider-reported input tokens.
    pub input_tokens: u64,
    /// Provider-reported output tokens.
    pub output_tokens: u64,
    /// Provider-reported cache-read tokens.
    pub cache_read_tokens: u64,
    /// Provider-reported cache-write tokens.
    pub cache_write_tokens: u64,
}

/// Provider failure classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyProviderFailureKind {
    /// Tool arguments could not be decoded.
    MalformedToolArguments,
    /// Provider deadline elapsed.
    Timeout,
    /// Provider rejected the request due to rate limiting.
    RateLimited,
    /// Stream failed after emitting visible output.
    PartialOutputFailure,
    /// Provider transport disconnected.
    Disconnected,
}

/// Provider-neutral retry classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyRetryClassification {
    /// Policy must not retry.
    Never,
    /// Policy may retry immediately.
    Immediate,
    /// Policy may retry after the supplied delay.
    AfterMilliseconds(u64),
}

/// Collected provider execution event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DependencyProviderEvent {
    /// Provider accepted the request.
    Started,
    /// Visible text fragment.
    TextDelta(String),
    /// Provider tool-call fragment.
    ToolCallDelta {
        /// Provider-independent call ID.
        call_id: String,
        /// Tool-name fragment.
        name_fragment: String,
        /// JSON argument fragment.
        arguments_fragment: String,
    },
    /// Complete tool call requiring runtime continuation.
    ToolCallProposed {
        /// Opaque continuation reference.
        continuation_reference: String,
        /// Provider-independent call ID.
        call_id: String,
        /// Stable tool name.
        tool: String,
        /// Complete argument JSON.
        arguments_json: String,
    },
    /// Normalized completion.
    Completed {
        /// Provider-neutral finish reason.
        finish_reason: String,
        /// Provider usage.
        usage: DependencyUsage,
    },
    /// Provider request was cancelled.
    Cancelled,
    /// Classified provider failure.
    Failed {
        /// Stable failure kind.
        kind: DependencyProviderFailureKind,
        /// Redacted diagnostic.
        message: String,
        /// Retry classification.
        retry: DependencyRetryClassification,
    },
}

/// Bounded collected provider output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyProviderExecutionResponse {
    /// Events in provider observation order.
    pub events: Vec<DependencyProviderEvent>,
}

/// Provider execution dependency failure before lifecycle events are available.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderExecutionDependencyError {
    /// Selected provider is not configured.
    ProviderNotConfigured,
    /// Request fields or bounds are invalid.
    InvalidRequest(String),
    /// Deterministic implementation exceeded its event bound.
    EventLimitExceeded,
}

impl std::fmt::Display for ProviderExecutionDependencyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProviderNotConfigured => formatter.write_str("provider is not configured"),
            Self::InvalidRequest(message) => {
                write!(formatter, "provider request is invalid: {message}")
            }
            Self::EventLimitExceeded => formatter.write_str("provider event limit exceeded"),
        }
    }
}

impl std::error::Error for ProviderExecutionDependencyError {}

/// External provider execution interface consumed only by harness data.
pub trait ProviderExecutionDependency {
    /// Executes one provider request into a bounded collected event stream.
    ///
    /// # Errors
    ///
    /// Returns a dependency error before execution for invalid selection,
    /// invalid bounds, or event overflow.
    fn execute_provider(
        &self,
        request: DependencyProviderExecutionRequest,
    ) -> Result<DependencyProviderExecutionResponse, ProviderExecutionDependencyError>;
}

impl ProviderExecutionDependency for StaticProviderCatalogDependency {
    fn execute_provider(
        &self,
        request: DependencyProviderExecutionRequest,
    ) -> Result<DependencyProviderExecutionResponse, ProviderExecutionDependencyError> {
        self.validate_grant(
            &request.authorization_grant,
            request.resumed_after_continuation,
        )?;
        validate_request(&request)?;
        let tool_result_count = request
            .entries
            .iter()
            .filter(|entry| matches!(entry, DependencyConversationEntry::ToolResult { .. }))
            .count();
        let options: BTreeMap<_, _> = request
            .options
            .into_iter()
            .map(|option| (option.key, option.value))
            .collect();
        if options
            .get("mock_scenario")
            .is_some_and(|value| matches!(value.as_str(), "approval_write" | "approval_multi"))
            && tool_result_count > 0
        {
            return Ok(DependencyProviderExecutionResponse {
                events: vec![
                    DependencyProviderEvent::Started,
                    DependencyProviderEvent::TextDelta(
                        "continued after durable approval decision".into(),
                    ),
                    completed(
                        "stop",
                        DependencyUsage {
                            input_tokens: 20,
                            output_tokens: 6,
                            cache_read_tokens: 0,
                            cache_write_tokens: 0,
                        },
                    ),
                ],
            });
        }
        if request.resumed_after_continuation {
            if options
                .get("mock_scenario")
                .is_some_and(|value| value == "coding_task")
            {
                return Ok(DependencyProviderExecutionResponse {
                    events: coding_task_events(tool_result_count),
                });
            }
            return Ok(DependencyProviderExecutionResponse {
                events: vec![
                    DependencyProviderEvent::Started,
                    DependencyProviderEvent::TextDelta(
                        "continued after approved runtime decision".into(),
                    ),
                    completed(
                        "stop",
                        DependencyUsage {
                            input_tokens: 18,
                            output_tokens: 5,
                            cache_read_tokens: 0,
                            cache_write_tokens: 0,
                        },
                    ),
                ],
            });
        }
        let scenario = options.get("mock_scenario").map_or("text", String::as_str);
        let text = options
            .get("mock_text")
            .cloned()
            .unwrap_or_else(|| "deterministic response".to_owned());
        let events = if scenario == "process_action" {
            process_action_events(&options)?
        } else if scenario == "planner_worker" {
            planner_worker_events(&options, &request.entries)?
        } else {
            scenario_events(scenario, text)?
        };
        let events = namespace_continuations(events, &request.cancellation_reference);
        if events.len() > MAX_EVENTS {
            return Err(ProviderExecutionDependencyError::EventLimitExceeded);
        }
        Ok(DependencyProviderExecutionResponse { events })
    }
}

pub(crate) fn validate_runtime_grant(
    grant: &str,
    key: &[u8; 32],
    uses: &Arc<Mutex<BTreeMap<uuid::Uuid, u8>>>,
    resumed: bool,
) -> Result<(), ProviderExecutionDependencyError> {
    let fields: Vec<_> = grant.split('.').collect();
    if fields.len() != 5
        || fields[0] != "v1"
        || fields[3].len() != 64
        || !fields[3].bytes().all(|byte| byte.is_ascii_hexdigit())
        || fields[4].len() != 64
        || !fields[4].bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(invalid_grant());
    }
    let expires = fields[1].parse::<u128>().map_err(|_| invalid_grant())?;
    let nonce = fields[2]
        .parse::<uuid::Uuid>()
        .map_err(|_| invalid_grant())?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| invalid_grant())?
        .as_millis();
    if expires < now || expires.saturating_sub(now) > 300_000 {
        return Err(invalid_grant());
    }
    let payload = fields[..4].join(".");
    let expected = blake3::keyed_hash(key, payload.as_bytes())
        .to_hex()
        .to_string();
    if !constant_time_equal(expected.as_bytes(), fields[4].as_bytes()) {
        return Err(invalid_grant());
    }
    let mut uses = uses.lock().map_err(|_| invalid_grant())?;
    match (uses.get_mut(&nonce), resumed) {
        (None, false) => {
            uses.insert(nonce, 1);
        }
        (Some(count), true) if *count < 16 => {
            *count += 1;
        }
        _ => return Err(invalid_grant()),
    }
    Ok(())
}

fn invalid_grant() -> ProviderExecutionDependencyError {
    ProviderExecutionDependencyError::InvalidRequest(
        "runtime authorization grant is invalid, expired, or replayed".into(),
    )
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let maximum = left.len().max(right.len());
    for index in 0..maximum {
        difference |= usize::from(
            left.get(index).copied().unwrap_or(0) ^ right.get(index).copied().unwrap_or(0),
        );
    }
    difference == 0
}

fn validate_request(
    request: &DependencyProviderExecutionRequest,
) -> Result<(), ProviderExecutionDependencyError> {
    if request.provider_key != "deterministic-mock" {
        return Err(ProviderExecutionDependencyError::ProviderNotConfigured);
    }
    if request.model_key.trim().is_empty()
        || request.cancellation_reference.trim().is_empty()
        || request.entries.len() > 256
        || request.options.len() > 64
    {
        return Err(ProviderExecutionDependencyError::InvalidRequest(
            "model, cancellation reference, entry count, or option count".into(),
        ));
    }
    Ok(())
}

fn planner_worker_events(
    options: &BTreeMap<String, String>,
    entries: &[DependencyConversationEntry],
) -> Result<Vec<DependencyProviderEvent>, ProviderExecutionDependencyError> {
    let phase = options.get("mock_planner_phase").map(String::as_str);
    let iteration = options
        .get("mock_planner_iteration")
        .map_or(Ok(0_u32), |value| value.parse::<u32>())
        .map_err(|_| {
            ProviderExecutionDependencyError::InvalidRequest(String::from(
                "mock_planner_iteration is invalid",
            ))
        })?;
    let pending_task = entries.iter().find_map(|entry| match entry {
        DependencyConversationEntry::Metadata { key, value_json } if key == "pending_task" => {
            Some(value_json.as_str())
        }
        _ => None,
    });
    let text = match (phase, pending_task) {
        (Some("plan"), _) => String::from(
            r#"{"tasks":[{"task_id":"task-1","description":"inspect runtime child recovery"},{"task_id":"task-2","description":"inspect planner join evidence"}]}"#,
        ),
        (None, Some(task)) => format!(
            r#"{{"worker_result":{task},"status":"completed","evidence":["canonical child journal"]}}"#
        ),
        (Some("integrate"), _) => format!(
            r#"{{"integration":"combined runtime-owned child handoffs","iteration":{iteration},"tests":"deterministic fixture passed"}}"#
        ),
        (Some("review"), _) if iteration == 0 => String::from(
            r#"{"approved":false,"rejected_task_ids":["task-2"],"findings":["task-2 requires one evidence-bound revision"]}"#,
        ),
        (Some("review"), _) => String::from(
            r#"{"approved":true,"rejected_task_ids":[],"findings":["child revision and integration evidence approved"]}"#,
        ),
        _ => {
            return Err(ProviderExecutionDependencyError::InvalidRequest(
                String::from("planner_worker requires a supported phase or pending task"),
            ));
        }
    };
    Ok(vec![
        DependencyProviderEvent::Started,
        DependencyProviderEvent::TextDelta(text),
        completed(
            "stop",
            DependencyUsage {
                input_tokens: 24,
                output_tokens: 16,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
            },
        ),
    ])
}

#[allow(
    clippy::too_many_lines,
    reason = "the deterministic provider keeps its complete scenario matrix together for auditability"
)]
fn scenario_events(
    scenario: &str,
    text: String,
) -> Result<Vec<DependencyProviderEvent>, ProviderExecutionDependencyError> {
    let usage = DependencyUsage {
        input_tokens: 12,
        output_tokens: 7,
        cache_read_tokens: 3,
        cache_write_tokens: 1,
    };
    let events = match scenario {
        "text" => vec![
            DependencyProviderEvent::Started,
            DependencyProviderEvent::TextDelta(text),
            completed("stop", usage),
        ],
        "streaming_text" => vec![
            DependencyProviderEvent::Started,
            DependencyProviderEvent::TextDelta("alpha ".into()),
            DependencyProviderEvent::TextDelta("beta ".into()),
            DependencyProviderEvent::TextDelta(text),
            completed("stop", usage),
        ],
        "one_tool_call" => {
            let mut events = vec![DependencyProviderEvent::Started];
            append_tool_call(
                &mut events,
                CONTINUATION_ONE,
                "call-1",
                "read_file",
                r#"{"path":"src/lib.rs"}"#,
            );
            events
        }
        "one_process_call" => {
            let mut events = vec![DependencyProviderEvent::Started];
            append_tool_call(
                &mut events,
                CONTINUATION_ONE,
                "process-call-1",
                "process.run",
                r#"{"executable":"cargo","arguments":["--version"],"output_limit_bytes":65536,"timeout_ms":30000,"cleanup":"remove_logs_always"}"#,
            );
            events
        }
        "git_status" => {
            let mut events = vec![DependencyProviderEvent::Started];
            append_tool_call(
                &mut events,
                CONTINUATION_ONE,
                "git-status-1",
                "git.status",
                r#"{"path":"."}"#,
            );
            events
        }
        "web_search" => {
            let mut events = vec![DependencyProviderEvent::Started];
            append_tool_call(
                &mut events,
                CONTINUATION_ONE,
                "web-search-1",
                "web.search",
                r#"{"query":"event driven Rust agents","count":5}"#,
            );
            events
        }
        "lsp_project_root" => {
            let mut events = vec![DependencyProviderEvent::Started];
            append_tool_call(
                &mut events,
                CONTINUATION_ONE,
                "lsp-project-root-1",
                "lsp.project_root",
                r#"{"document":"src/lib.rs"}"#,
            );
            events
        }
        "mcp_server_list" => {
            let mut events = vec![DependencyProviderEvent::Started];
            append_tool_call(
                &mut events,
                CONTINUATION_ONE,
                "mcp-server-list-1",
                "mcp.server.list",
                "{}",
            );
            events
        }
        "mcp_fixture_call" => {
            let mut events = vec![DependencyProviderEvent::Started];
            append_tool_call(
                &mut events,
                CONTINUATION_ONE,
                "mcp-fixture-call-1",
                "mcp.invoke",
                r#"{"server_id":"fixture","kind":"tool","name":"echo","arguments":{"value":"hello"}}"#,
            );
            events
        }
        "browser_fixture_flow" => {
            let mut events = vec![DependencyProviderEvent::Started];
            for (call_id, tool, arguments) in [
                ("browser-start-1", "browser.start", r"{}"),
                (
                    "browser-navigate-1",
                    "browser.navigate",
                    r#"{"url":"http://127.0.0.1/page"}"#,
                ),
                ("browser-inspect-1", "browser.inspect", r"{}"),
                (
                    "browser-click-1",
                    "browser.click",
                    r##"{"selector":"#button"}"##,
                ),
                (
                    "browser-type-1",
                    "browser.type",
                    r##"{"selector":"#input","text":"hello"}"##,
                ),
                (
                    "browser-submit-1",
                    "browser.submit",
                    r##"{"selector":"#input"}"##,
                ),
                ("browser-screenshot-1", "browser.screenshot", r"{}"),
                (
                    "browser-download-1",
                    "browser.download",
                    r#"{"url":"http://127.0.0.1/file","maximum_bytes":1024}"#,
                ),
                ("browser-close-1", "browser.close", r"{}"),
            ] {
                append_tool_call(&mut events, CONTINUATION_ONE, call_id, tool, arguments);
            }
            events
        }
        "approval_write" => {
            let mut events = vec![DependencyProviderEvent::Started];
            append_tool_call(
                &mut events,
                CONTINUATION_ONE,
                "approval-write-1",
                "filesystem.write",
                r#"{"path":"approved.txt","content":"executed once\n","mode":"create"}"#,
            );
            events
        }
        "approval_multi" => {
            let mut events = vec![DependencyProviderEvent::Started];
            append_tool_call(
                &mut events,
                CONTINUATION_ONE,
                "approval-multi-write",
                "filesystem.write",
                r#"{"path":"approved.txt","content":"batch approved\n","mode":"create"}"#,
            );
            append_tool_call(
                &mut events,
                CONTINUATION_TWO,
                "approval-multi-read",
                "filesystem.read",
                r#"{"path":"src/lib.rs"}"#,
            );
            events
        }
        "coding_task" => {
            let mut events = vec![DependencyProviderEvent::Started];
            append_tool_call(
                &mut events,
                CONTINUATION_ONE,
                "coding-read",
                "filesystem.read",
                r#"{"path":"src/lib.rs"}"#,
            );
            events
        }
        "multiple_tool_calls" => {
            let mut events = vec![DependencyProviderEvent::Started];
            append_tool_call(
                &mut events,
                CONTINUATION_ONE,
                "call-1",
                "read_file",
                r#"{"path":"src/lib.rs"}"#,
            );
            append_tool_call(
                &mut events,
                CONTINUATION_TWO,
                "call-2",
                "read_file",
                r#"{"path":"src/lib.rs"}"#,
            );
            events
        }
        "malformed_arguments" | "timeout" | "rate_limit" | "partial_failure" | "disconnected" => {
            failure_scenario(scenario)
        }
        "cancelled" => vec![
            DependencyProviderEvent::Started,
            DependencyProviderEvent::TextDelta("partial before cancellation".into()),
            DependencyProviderEvent::Cancelled,
        ],
        other => {
            return Err(ProviderExecutionDependencyError::InvalidRequest(format!(
                "unsupported deterministic mock scenario `{other}`"
            )));
        }
    };
    Ok(events)
}

fn process_action_events(
    options: &BTreeMap<String, String>,
) -> Result<Vec<DependencyProviderEvent>, ProviderExecutionDependencyError> {
    let tool = options
        .get("mock_process_tool")
        .filter(|value| {
            matches!(
                value.as_str(),
                "process.run"
                    | "process.start"
                    | "process.run_pty"
                    | "process.start_pty"
                    | "process.read"
                    | "process.input"
                    | "process.resize"
                    | "process.wait"
                    | "process.interrupt"
                    | "process.kill"
                    | "process.detach"
                    | "process.reattach"
                    | "process.list"
            )
        })
        .ok_or_else(|| {
            ProviderExecutionDependencyError::InvalidRequest(
                "process_action requires a supported mock_process_tool".into(),
            )
        })?;
    let decoded_arguments;
    let arguments = if let Some(arguments) = options.get("mock_process_arguments") {
        arguments.as_bytes()
    } else if let Some(arguments) = options.get("mock_process_arguments_base64") {
        decoded_arguments = STANDARD.decode(arguments).map_err(|_| {
            ProviderExecutionDependencyError::InvalidRequest(
                "mock_process_arguments_base64 must be valid base64".into(),
            )
        })?;
        decoded_arguments.as_slice()
    } else {
        return Err(ProviderExecutionDependencyError::InvalidRequest(
            "process_action requires mock_process_arguments".into(),
        ));
    };
    if arguments.len() > 64 * 1024 {
        return Err(ProviderExecutionDependencyError::InvalidRequest(
            "mock_process_arguments exceeds the deterministic fixture limit".into(),
        ));
    }
    let arguments: serde_json::Value = serde_json::from_slice(arguments).map_err(|_| {
        ProviderExecutionDependencyError::InvalidRequest(
            "mock_process_arguments must be valid JSON".into(),
        )
    })?;
    if !arguments.is_object() {
        return Err(ProviderExecutionDependencyError::InvalidRequest(
            "mock_process_arguments must be a JSON object".into(),
        ));
    }
    let arguments = serde_json::to_string(&arguments).map_err(|_| {
        ProviderExecutionDependencyError::InvalidRequest(
            "mock_process_arguments could not be normalized".into(),
        )
    })?;
    let call_id = options
        .get("mock_process_call_id")
        .map_or("process-action-1", String::as_str);
    if call_id.is_empty() || call_id.len() > 256 {
        return Err(ProviderExecutionDependencyError::InvalidRequest(
            "mock_process_call_id is invalid".into(),
        ));
    }
    let mut events = vec![DependencyProviderEvent::Started];
    append_tool_call(&mut events, CONTINUATION_ONE, call_id, tool, &arguments);
    Ok(events)
}

fn namespace_continuations(
    mut events: Vec<DependencyProviderEvent>,
    request_reference: &str,
) -> Vec<DependencyProviderEvent> {
    for event in &mut events {
        if let DependencyProviderEvent::ToolCallProposed {
            continuation_reference,
            ..
        } = event
        {
            *continuation_reference =
                scoped_continuation_reference(request_reference, continuation_reference);
        }
    }
    events
}

fn scoped_continuation_reference(request_reference: &str, provider_reference: &str) -> String {
    let mut input = Vec::with_capacity(request_reference.len() + provider_reference.len() + 1);
    input.extend_from_slice(request_reference.as_bytes());
    input.push(0);
    input.extend_from_slice(provider_reference.as_bytes());
    let digest = blake3::hash(&input);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    // RFC 9562 UUIDv8 is explicitly reserved for application-defined,
    // deterministic identifiers. The harness protocol exposes UUIDs even when
    // a provider's own continuation reference is not globally unique.
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes).to_string()
}

fn coding_task_events(tool_result_count: usize) -> Vec<DependencyProviderEvent> {
    let mut events = vec![DependencyProviderEvent::Started];
    match tool_result_count {
        1 => append_tool_call(
            &mut events,
            CONTINUATION_ONE,
            "coding-edit-incomplete",
            "filesystem.edit",
            r#"{"path":"src/lib.rs","replacements":[{"old":"left + right + 1","new":"left + right + 2"}]}"#,
        ),
        2 => append_tool_call(
            &mut events,
            CONTINUATION_ONE,
            "coding-test-failing",
            "process.run",
            r#"{"executable":"cargo","arguments":["test","--quiet"],"output_limit_bytes":1048576,"timeout_ms":120000,"cleanup":"retain"}"#,
        ),
        3 => append_tool_call(
            &mut events,
            CONTINUATION_ONE,
            "coding-edit-fix",
            "filesystem.edit",
            r#"{"path":"src/lib.rs","replacements":[{"old":"left + right + 2","new":"left + right"}]}"#,
        ),
        4 => append_tool_call(
            &mut events,
            CONTINUATION_ONE,
            "coding-test-passing",
            "process.run",
            r#"{"executable":"cargo","arguments":["test","--quiet"],"output_limit_bytes":1048576,"timeout_ms":120000,"cleanup":"retain"}"#,
        ),
        _ => {
            events.push(DependencyProviderEvent::TextDelta(
                "implemented the fix and verified the tests pass".into(),
            ));
            events.push(completed(
                "stop",
                DependencyUsage {
                    input_tokens: 48,
                    output_tokens: 9,
                    cache_read_tokens: 0,
                    cache_write_tokens: 0,
                },
            ));
        }
    }
    events
}

fn failure_scenario(scenario: &str) -> Vec<DependencyProviderEvent> {
    match scenario {
        "malformed_arguments" => vec![
            DependencyProviderEvent::Started,
            DependencyProviderEvent::ToolCallDelta {
                call_id: "call-bad".into(),
                name_fragment: "read_file".into(),
                arguments_fragment: "{\"path\":".into(),
            },
            failed(
                DependencyProviderFailureKind::MalformedToolArguments,
                "provider returned malformed tool arguments",
                DependencyRetryClassification::Never,
            ),
        ],
        "timeout" => vec![
            DependencyProviderEvent::Started,
            failed(
                DependencyProviderFailureKind::Timeout,
                "provider deadline elapsed",
                DependencyRetryClassification::Immediate,
            ),
        ],
        "rate_limit" => vec![
            DependencyProviderEvent::Started,
            failed(
                DependencyProviderFailureKind::RateLimited,
                "provider rate limited the request",
                DependencyRetryClassification::AfterMilliseconds(1_000),
            ),
        ],
        "partial_failure" => vec![
            DependencyProviderEvent::Started,
            DependencyProviderEvent::TextDelta("partial visible output".into()),
            failed(
                DependencyProviderFailureKind::PartialOutputFailure,
                "provider stream failed after partial output",
                DependencyRetryClassification::Immediate,
            ),
        ],
        "disconnected" => vec![
            DependencyProviderEvent::Started,
            failed(
                DependencyProviderFailureKind::Disconnected,
                "provider transport disconnected",
                DependencyRetryClassification::Immediate,
            ),
        ],
        _ => unreachable!("caller restricts failure scenario"),
    }
}

fn append_tool_call(
    events: &mut Vec<DependencyProviderEvent>,
    continuation_reference: &str,
    call_id: &str,
    tool: &str,
    arguments_json: &str,
) {
    events.push(DependencyProviderEvent::ToolCallDelta {
        call_id: call_id.into(),
        name_fragment: tool.into(),
        arguments_fragment: arguments_json.into(),
    });
    events.push(DependencyProviderEvent::ToolCallProposed {
        continuation_reference: continuation_reference.into(),
        call_id: call_id.into(),
        tool: tool.into(),
        arguments_json: arguments_json.into(),
    });
}

fn completed(reason: &str, usage: DependencyUsage) -> DependencyProviderEvent {
    DependencyProviderEvent::Completed {
        finish_reason: reason.into(),
        usage,
    }
}

fn failed(
    kind: DependencyProviderFailureKind,
    message: &str,
    retry: DependencyRetryClassification,
) -> DependencyProviderEvent {
    DependencyProviderEvent::Failed {
        kind,
        message: message.into(),
        retry,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_scenarios_cover_required_provider_behaviors() {
        let dependency = StaticProviderCatalogDependency::built_in();
        for scenario in [
            "text",
            "streaming_text",
            "one_tool_call",
            "one_process_call",
            "process_action",
            "git_status",
            "web_search",
            "lsp_project_root",
            "mcp_server_list",
            "mcp_fixture_call",
            "browser_fixture_flow",
            "approval_write",
            "approval_multi",
            "coding_task",
            "multiple_tool_calls",
            "malformed_arguments",
            "timeout",
            "rate_limit",
            "partial_failure",
            "cancelled",
            "disconnected",
        ] {
            let response = dependency
                .execute_provider(request(scenario))
                .expect("documented scenario");
            assert!(matches!(
                response.events.first(),
                Some(DependencyProviderEvent::Started)
            ));
            assert!(response.events.len() <= MAX_EVENTS);
        }
    }

    #[test]
    fn usage_retry_tool_calls_and_cancellation_are_normalized() {
        let dependency = StaticProviderCatalogDependency::built_in();
        let text = dependency.execute_provider(request("text")).expect("text");
        assert!(matches!(
            text.events.last(),
            Some(DependencyProviderEvent::Completed {
                usage: DependencyUsage {
                    cache_read_tokens: 3,
                    ..
                },
                ..
            })
        ));
        let tools = dependency
            .execute_provider(request("multiple_tool_calls"))
            .expect("tools");
        assert_eq!(
            tools
                .events
                .iter()
                .filter(|event| matches!(event, DependencyProviderEvent::ToolCallProposed { .. }))
                .count(),
            2
        );
        let rate_limit = dependency
            .execute_provider(request("rate_limit"))
            .expect("rate limit");
        assert!(matches!(
            rate_limit.events.last(),
            Some(DependencyProviderEvent::Failed {
                retry: DependencyRetryClassification::AfterMilliseconds(1_000),
                ..
            })
        ));
        let cancelled = dependency
            .execute_provider(request("cancelled"))
            .expect("cancelled");
        assert_eq!(
            cancelled.events.last(),
            Some(&DependencyProviderEvent::Cancelled)
        );
    }

    #[test]
    fn planner_worker_fixture_is_stateless_and_phase_driven() {
        let dependency = StaticProviderCatalogDependency::built_in();
        let mut plan = request("planner_worker");
        plan.options.extend([
            DependencyProviderOption {
                key: String::from("mock_planner_phase"),
                value: String::from("plan"),
            },
            DependencyProviderOption {
                key: String::from("mock_planner_iteration"),
                value: String::from("0"),
            },
        ]);
        let plan = dependency.execute_provider(plan).expect("planner response");
        assert!(plan.events.iter().any(|event| matches!(
            event,
            DependencyProviderEvent::TextDelta(text)
                if text.contains("\"task_id\":\"task-1\"")
                    && text.contains("\"task_id\":\"task-2\"")
        )));

        let mut worker = request("planner_worker");
        worker.entries = vec![DependencyConversationEntry::Metadata {
            key: String::from("pending_task"),
            value_json: String::from(
                r#"{"task_id":"task-2","description":"inspect joins","state":"assigned"}"#,
            ),
        }];
        let worker = dependency
            .execute_provider(worker)
            .expect("worker response");
        assert!(worker.events.iter().any(|event| matches!(
            event,
            DependencyProviderEvent::TextDelta(text)
                if text.contains("\"status\":\"completed\"")
                    && text.contains("\"task_id\":\"task-2\"")
        )));

        let mut rejected = request("planner_worker");
        rejected.options.extend([
            DependencyProviderOption {
                key: String::from("mock_planner_phase"),
                value: String::from("review"),
            },
            DependencyProviderOption {
                key: String::from("mock_planner_iteration"),
                value: String::from("0"),
            },
        ]);
        let rejected = dependency
            .execute_provider(rejected)
            .expect("reject once response");
        assert!(rejected.events.iter().any(|event| matches!(
            event,
            DependencyProviderEvent::TextDelta(text)
                if text.contains("\"approved\":false")
                    && text.contains("\"task-2\"")
        )));
    }

    #[test]
    fn provider_continuations_are_stable_within_and_unique_across_requests() {
        let dependency = StaticProviderCatalogDependency::built_in();
        let first = dependency
            .execute_provider(request("approval_multi"))
            .expect("first request");
        let repeated = dependency
            .execute_provider(request("approval_multi"))
            .expect("repeated request identity");
        let mut second_request = request("approval_multi");
        second_request.cancellation_reference = "cancel-2".into();
        let second = dependency
            .execute_provider(second_request)
            .expect("second request");
        let continuations = |response: &DependencyProviderExecutionResponse| {
            response
                .events
                .iter()
                .filter_map(|event| match event {
                    DependencyProviderEvent::ToolCallProposed {
                        continuation_reference,
                        ..
                    } => Some(continuation_reference.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        let first_ids = continuations(&first);
        assert_eq!(first_ids, continuations(&repeated));
        assert_ne!(first_ids, continuations(&second));
        assert_eq!(first_ids.len(), 2);
        assert_ne!(first_ids[0], first_ids[1]);
        assert!(first_ids.iter().all(|id| uuid::Uuid::parse_str(id).is_ok()));
    }

    fn request(scenario: &str) -> DependencyProviderExecutionRequest {
        let mut request = DependencyProviderExecutionRequest {
            provider_key: "deterministic-mock".into(),
            model_key: "mock-model".into(),
            entries: vec![DependencyConversationEntry::User("hello".into())],
            options: vec![DependencyProviderOption {
                key: "mock_scenario".into(),
                value: scenario.into(),
            }],
            authorization_grant: "grant".into(),
            cancellation_reference: "cancel-1".into(),
            resumed_after_continuation: false,
        };
        if scenario == "process_action" {
            request.options.extend([
                DependencyProviderOption {
                    key: "mock_process_tool".into(),
                    value: "process.list".into(),
                },
                DependencyProviderOption {
                    key: "mock_process_arguments".into(),
                    value: "{}".into(),
                },
            ]);
        }
        request
    }

    #[test]
    fn process_action_fixture_is_constrained_and_normalizes_json_arguments() {
        let dependency = StaticProviderCatalogDependency::built_in();
        let response = dependency
            .execute_provider(request("process_action"))
            .expect("valid process action");
        assert!(response.events.iter().any(|event| matches!(
            event,
            DependencyProviderEvent::ToolCallProposed {
                tool,
                arguments_json,
                ..
            } if tool == "process.list" && arguments_json == "{}"
        )));

        let mut unsupported = request("process_action");
        unsupported
            .options
            .iter_mut()
            .find(|option| option.key == "mock_process_tool")
            .expect("tool option")
            .value = "filesystem.write".into();
        assert!(matches!(
            dependency.execute_provider(unsupported),
            Err(ProviderExecutionDependencyError::InvalidRequest(_))
        ));

        let mut malformed = request("process_action");
        malformed
            .options
            .iter_mut()
            .find(|option| option.key == "mock_process_arguments")
            .expect("arguments option")
            .value = "[]".into();
        assert!(matches!(
            dependency.execute_provider(malformed),
            Err(ProviderExecutionDependencyError::InvalidRequest(_))
        ));
    }

    #[test]
    fn process_action_fixture_accepts_bounded_base64_json_arguments() {
        let dependency = StaticProviderCatalogDependency::built_in();
        let mut value = request("process_action");
        value
            .options
            .retain(|option| option.key != "mock_process_arguments");
        value.options.push(DependencyProviderOption {
            key: "mock_process_arguments_base64".into(),
            value: STANDARD.encode(br#"{"process_id":"process-1"}"#),
        });
        let response = dependency
            .execute_provider(value)
            .expect("base64 arguments");
        assert!(response.events.iter().any(|event| matches!(
            event,
            DependencyProviderEvent::ToolCallProposed {
                arguments_json,
                ..
            } if arguments_json == r#"{"process_id":"process-1"}"#
        )));
    }

    fn signed_grant(key: &[u8; 32], nonce: uuid::Uuid) -> String {
        let expires = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis()
            + 60_000;
        let binding = "ab".repeat(32);
        let payload = format!("v1.{expires}.{nonce}.{binding}");
        let signature = blake3::keyed_hash(key, payload.as_bytes());
        format!("{payload}.{}", signature.to_hex())
    }

    #[test]
    fn secure_catalog_rejects_tampering_and_initial_replay() {
        let key = [7_u8; 32];
        let dependency = StaticProviderCatalogDependency::secure(key);
        let mut first = request("text");
        first.authorization_grant = signed_grant(&key, uuid::Uuid::from_u128(101));
        dependency
            .execute_provider(first.clone())
            .expect("first authorized request");
        assert!(
            dependency.execute_provider(first).is_err(),
            "an initial provider request cannot replay its nonce"
        );

        let mut tampered = request("text");
        tampered.authorization_grant = signed_grant(&key, uuid::Uuid::from_u128(102));
        tampered.authorization_grant.push('0');
        assert!(dependency.execute_provider(tampered).is_err());
    }

    #[test]
    fn secure_catalog_allows_explicit_continuation_use_only_after_initial_use() {
        let key = [9_u8; 32];
        let dependency = StaticProviderCatalogDependency::secure(key);
        let grant = signed_grant(&key, uuid::Uuid::from_u128(103));
        let mut resumed_first = request("text");
        resumed_first.authorization_grant = grant.clone();
        resumed_first.resumed_after_continuation = true;
        assert!(dependency.execute_provider(resumed_first).is_err());

        let mut initial = request("text");
        initial.authorization_grant = grant.clone();
        dependency
            .execute_provider(initial)
            .expect("initial authorized request");
        let mut resumed = request("text");
        resumed.authorization_grant = grant;
        resumed.resumed_after_continuation = true;
        dependency
            .execute_provider(resumed)
            .expect("explicit continuation use");
    }
}
