//! Provider-neutral execution dependency and deterministic mock provider.

use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
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
    /// Provider-visible image input.
    Image {
        /// Image media type.
        media_type: String,
        /// Base64-encoded image bytes.
        data_base64: String,
    },
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
    /// Provider-reported reasoning/thinking tokens.
    pub reasoning_tokens: u64,
    /// True only when usage is estimated rather than provider-reported.
    pub estimated: bool,
}

/// Pricing-record identity and computed cost for one provider exchange.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DependencyCostMetadata {
    /// Stable pricing-record source.
    pub source: String,
    /// Pricing-record version.
    pub version: String,
    /// Computed input cost in micro-units of `currency`.
    pub input_cost_micros: u64,
    /// Computed output cost in micro-units of `currency`.
    pub output_cost_micros: u64,
    /// Computed cache-read cost in micro-units of `currency`.
    pub cache_read_cost_micros: u64,
    /// Computed cache-write cost in micro-units of `currency`.
    pub cache_write_cost_micros: u64,
    /// ISO-4217 currency code; empty when the pricing record is unknown.
    pub currency: String,
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
    /// Provider rejected the supplied credentials.
    AuthenticationFailed,
    /// Provider reported overload or transient server failure.
    ProviderOverloaded,
    /// Provider rejected the request as invalid.
    InvalidRequest,
    /// Provider does not support the requested capability or model.
    UnsupportedCapability,
    /// Transport failed safely before any provider response.
    TransportFailure,
    /// Disconnect after dispatch whose outcome is ambiguous.
    AmbiguousDisconnect,
    /// The caller cancelled the request.
    UserCancellation,
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
        /// Pricing-record identity and computed cost.
        cost: Option<DependencyCostMetadata>,
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

/// External provider cancellation interface consumed only by harness data.
pub trait ProviderCancellationDependency: Send + Sync {
    /// Requests cancellation of an in-flight provider exchange.
    ///
    /// Returns whether an active exchange for the reference was found.
    ///
    /// # Errors
    ///
    /// Returns a dependency error when the request is malformed.
    fn cancel_provider(
        &self,
        cancellation_reference: &str,
    ) -> Result<bool, ProviderExecutionDependencyError>;
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
        wait_for_test_gate(&options, &request.cancellation_reference)?;
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
                            reasoning_tokens: 0,
                            estimated: false,
                        },
                        None,
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
                            reasoning_tokens: 0,
                            estimated: false,
                        },
                        None,
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
        } else if scenario == "planner_worker_child" {
            planner_worker_child_events(&request.entries)?
        } else if scenario == "graph_b_review_sequence" {
            graph_b_review_sequence_events(&request.entries)?
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

impl ProviderCancellationDependency for StaticProviderCatalogDependency {
    fn cancel_provider(
        &self,
        _cancellation_reference: &str,
    ) -> Result<bool, ProviderExecutionDependencyError> {
        Ok(false)
    }
}

fn planner_worker_child_events(
    entries: &[DependencyConversationEntry],
) -> Result<Vec<DependencyProviderEvent>, ProviderExecutionDependencyError> {
    let work = entries.iter().find_map(|entry| match entry {
        DependencyConversationEntry::Metadata { key, value_json }
            if key == "agentmod.canonical_node_work" =>
        {
            Some(value_json.as_str())
        }
        _ => None,
    });
    let work: serde_json::Value = serde_json::from_str(work.ok_or_else(|| {
        ProviderExecutionDependencyError::InvalidRequest(String::from(
            "planner worker child canonical node work is missing",
        ))
    })?)
    .map_err(|_| {
        ProviderExecutionDependencyError::InvalidRequest(String::from(
            "planner worker child canonical node work is invalid",
        ))
    })?;
    let iteration = work
        .get("loop_iteration")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            ProviderExecutionDependencyError::InvalidRequest(String::from(
                "planner worker child loop iteration is invalid",
            ))
        })?;
    let mut events = vec![DependencyProviderEvent::Started];
    let (call_id, tool, arguments) = match iteration {
        0 => (
            "planner-worker-edit",
            "filesystem.edit",
            r#"{"path":"worker.txt","replacements":[{"old":"parent-owned","new":"child-owned\n","expected_occurrences":1}]}"#,
        ),
        1 => (
            "planner-worker-test",
            "process.run",
            r#"{"executable":"cargo","arguments":["test","--quiet"],"working_directory":".","output_limit_bytes":262144,"timeout_ms":30000,"cleanup":"remove_logs_on_success"}"#,
        ),
        2 => ("planner-worker-diff", "git.diff", r#"{"path":"."}"#),
        _ => {
            events.push(DependencyProviderEvent::TextDelta(String::from(
                "worker edit, test, and diff evidence completed",
            )));
            events.push(completed(
                "stop",
                DependencyUsage {
                    input_tokens: 24,
                    output_tokens: 8,
                    cache_read_tokens: 0,
                    cache_write_tokens: 0,
                    reasoning_tokens: 0,
                    estimated: false,
                },
                None,
            ));
            return Ok(events);
        }
    };
    append_tool_call(&mut events, CONTINUATION_ONE, call_id, tool, arguments);
    Ok(events)
}

fn wait_for_test_gate(
    options: &BTreeMap<String, String>,
    cancellation_reference: &str,
) -> Result<(), ProviderExecutionDependencyError> {
    let Some(gate_id) = options.get("mock_gate_id") else {
        return Ok(());
    };
    if gate_id.is_empty()
        || gate_id.len() > 64
        || !gate_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(invalid_gate("mock_gate_id is invalid"));
    }
    let root = std::env::var_os("AGENTMOD_HARNESS_TEST_GATE_ROOT")
        .ok_or_else(|| invalid_gate("mock gate root is unavailable"))?;
    let gate = Path::new(&root).join(gate_id);
    fs::create_dir_all(&gate).map_err(|_| invalid_gate("mock gate cannot be initialized"))?;
    let request_hash = blake3::hash(cancellation_reference.as_bytes()).to_hex();
    let started = gate.join(format!("started-{request_hash}"));
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&started)
    {
        Ok(mut marker) => marker
            .write_all(cancellation_reference.as_bytes())
            .map_err(|_| invalid_gate("mock gate start cannot be recorded"))?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(invalid_gate("mock gate start cannot be recorded")),
    }
    let timeout_ms = options
        .get("mock_gate_timeout_ms")
        .map_or(Ok(30_000_u64), |value| value.parse::<u64>())
        .map_err(|_| invalid_gate("mock gate timeout is invalid"))?;
    if !(10..=120_000).contains(&timeout_ms) {
        return Err(invalid_gate("mock gate timeout is invalid"));
    }
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    while !gate.join("release").is_file() {
        if Instant::now() >= deadline {
            return Err(invalid_gate("mock gate timed out"));
        }
        thread::sleep(Duration::from_millis(10));
    }
    fs::write(gate.join(format!("released-{request_hash}")), b"released")
        .map_err(|_| invalid_gate("mock gate release cannot be recorded"))?;
    Ok(())
}

fn invalid_gate(message: &str) -> ProviderExecutionDependencyError {
    ProviderExecutionDependencyError::InvalidRequest(message.to_owned())
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
    if !matches!(request.provider_key.as_str(), "deterministic-mock" | "mock") {
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

#[allow(
    clippy::too_many_lines,
    reason = "the deterministic planner fixture keeps every phase and exact structured review response visible together"
)]
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
    let generic_review = entries.iter().find_map(|entry| match entry {
        DependencyConversationEntry::Metadata { key, value_json }
            if key == "agentmod.generic_review_request" =>
        {
            Some(value_json.as_str())
        }
        _ => None,
    });
    let canonical_model_inputs = entries.iter().find_map(|entry| match entry {
        DependencyConversationEntry::Metadata { key, value_json }
            if key == "agentmod.canonical_model_inputs" =>
        {
            Some(value_json.as_str())
        }
        _ => None,
    });
    if phase.is_none()
        && pending_task.is_none()
        && let Some(review) = generic_review
    {
        let request: serde_json::Value = serde_json::from_str(review).map_err(|_| {
            ProviderExecutionDependencyError::InvalidRequest(String::from(
                "planner_worker generic review metadata is invalid",
            ))
        })?;
        let schema_version = request
            .get("schema_version")
            .and_then(serde_json::Value::as_u64);
        let artifact_reference = request
            .get("artifact_evidence")
            .and_then(serde_json::Value::as_array)
            .and_then(|evidence| evidence.first())
            .and_then(|evidence| evidence.get("artifact_reference"))
            .and_then(serde_json::Value::as_str);
        if schema_version == Some(2) && artifact_reference.is_none() {
            return Err(ProviderExecutionDependencyError::InvalidRequest(
                String::from("planner_worker generic review artifact evidence is missing"),
            ));
        }
        let revision = request
            .get("current_revision")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| {
                ProviderExecutionDependencyError::InvalidRequest(String::from(
                    "planner_worker generic review revision is invalid",
                ))
            })?;
        let known_task_ids = request
            .get("known_task_ids")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                ProviderExecutionDependencyError::InvalidRequest(String::from(
                    "planner_worker generic review task set is invalid",
                ))
            })?;
        let rejected = known_task_ids
            .iter()
            .filter_map(serde_json::Value::as_str)
            .find(|task| task.starts_with("evidence-task"))
            .or_else(|| known_task_ids.iter().find_map(serde_json::Value::as_str))
            .ok_or_else(|| {
                ProviderExecutionDependencyError::InvalidRequest(String::from(
                    "planner_worker generic review task set is empty",
                ))
            })?;
        let response = if revision == 0 {
            serde_json::json!({
                "approved": false,
                "rejected_task_ids": [rejected],
                "findings": [{
                    "code": "planner.evidence_revision",
                    "message": "evidence task requires one artifact-bound revision",
                    "artifact_references": [artifact_reference.unwrap_or("integration_artifact")],
                }],
            })
        } else {
            serde_json::json!({
                "approved": true,
                "rejected_task_ids": [],
                "findings": [],
            })
        };
        let text = serde_json::to_string(&response).map_err(|_| {
            ProviderExecutionDependencyError::InvalidRequest(String::from(
                "planner_worker generic review response is invalid",
            ))
        })?;
        return Ok(vec![
            DependencyProviderEvent::Started,
            DependencyProviderEvent::TextDelta(text),
            completed(
                "stop",
                DependencyUsage {
                    input_tokens: 24,
                    output_tokens: 16,
                    cache_read_tokens: 0,
                    cache_write_tokens: 0,
                    reasoning_tokens: 0,
                    estimated: false,
                },
                None,
            ),
        ]);
    }
    let mut integration_member_order = None;
    if phase == Some("integrate_v1_4") {
        let inputs = canonical_model_inputs.ok_or_else(|| {
            ProviderExecutionDependencyError::InvalidRequest(String::from(
                "planner_worker v1.4 integration inputs are missing",
            ))
        })?;
        let inputs: serde_json::Value = serde_json::from_str(inputs).map_err(|_| {
            ProviderExecutionDependencyError::InvalidRequest(String::from(
                "planner_worker v1.4 integration inputs are invalid",
            ))
        })?;
        let evidence = inputs
            .pointer("/joined/artifact_evidence")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                ProviderExecutionDependencyError::InvalidRequest(String::from(
                    "planner_worker v1.4 integration artifact evidence is missing",
                ))
            })?;
        if evidence.len() < 2
            || evidence.iter().any(|item| {
                item.get("member_id")
                    .and_then(serde_json::Value::as_str)
                    .is_none()
                    || item
                        .get("child_session_id")
                        .and_then(serde_json::Value::as_str)
                        .is_none()
                    || item.get("content").is_none()
            })
        {
            return Err(ProviderExecutionDependencyError::InvalidRequest(
                String::from("planner_worker v1.4 integration evidence is incomplete"),
            ));
        }
        let member_order = evidence
            .iter()
            .map(|item| {
                item.get("member_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        ProviderExecutionDependencyError::InvalidRequest(String::from(
                            "planner_worker v1.4 integration member identity is invalid",
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if member_order.windows(2).any(|pair| pair[0] > pair[1]) {
            return Err(ProviderExecutionDependencyError::InvalidRequest(
                String::from("planner_worker v1.4 integration evidence order is not canonical"),
            ));
        }
        integration_member_order = Some(member_order);
    }
    let text = match (phase, pending_task) {
        (Some("plan" | "plan_v1_4"), _) => String::from(
            r#"{"tasks":[{"task_id":"task-1","description":"inspect runtime child recovery"},{"task_id":"task-2","description":"inspect planner join evidence"}]}"#,
        ),
        (None, Some(task)) => format!(
            r#"{{"worker_result":{task},"status":"completed","evidence":["canonical child journal"]}}"#
        ),
        (Some("integrate"), _) => format!(
            r#"{{"integration":"combined runtime-owned child handoffs","iteration":{iteration},"tests":"deterministic fixture passed"}}"#
        ),
        (Some("integrate_v1_4"), _) => serde_json::json!({
            "integration": "combined runtime-owned child handoffs",
            "iteration": iteration,
            "tests": "deterministic fixture passed",
            "member_order": integration_member_order.ok_or_else(|| {
                ProviderExecutionDependencyError::InvalidRequest(String::from(
                    "planner_worker v1.4 integration order is missing",
                ))
            })?,
        })
        .to_string(),
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
                reasoning_tokens: 0,
                estimated: false,
            },
            None,
        ),
    ])
}

fn graph_b_review_sequence_events(
    entries: &[DependencyConversationEntry],
) -> Result<Vec<DependencyProviderEvent>, ProviderExecutionDependencyError> {
    let request = entries
        .iter()
        .find_map(|entry| match entry {
            DependencyConversationEntry::Metadata { key, value_json }
                if key == "agentmod.generic_review_request" =>
            {
                Some(value_json)
            }
            _ => None,
        })
        .ok_or_else(|| {
            ProviderExecutionDependencyError::InvalidRequest(String::from(
                "graph_b_review_sequence requires generic review metadata",
            ))
        })?;
    let request: serde_json::Value = serde_json::from_str(request).map_err(|_| {
        ProviderExecutionDependencyError::InvalidRequest(String::from(
            "graph_b_review_sequence metadata is invalid",
        ))
    })?;
    let revision = request
        .get("current_revision")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            ProviderExecutionDependencyError::InvalidRequest(String::from(
                "graph_b_review_sequence requires current_revision",
            ))
        })?;
    let known_task_ids = request
        .get("known_task_ids")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            ProviderExecutionDependencyError::InvalidRequest(String::from(
                "graph_b_review_sequence requires known_task_ids",
            ))
        })?;
    let mut known_task_ids = known_task_ids
        .iter()
        .map(|value| {
            value.as_str().ok_or_else(|| {
                ProviderExecutionDependencyError::InvalidRequest(String::from(
                    "graph_b_review_sequence known_task_ids must contain only strings",
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    known_task_ids.sort_unstable();
    known_task_ids.dedup();
    let rejected_task = known_task_ids.first().copied().ok_or_else(|| {
        ProviderExecutionDependencyError::InvalidRequest(String::from(
            "graph_b_review_sequence requires a known task",
        ))
    })?;
    let (approved, findings, rejected_task_ids) = if revision == 0 {
        (
            false,
            serde_json::json!([{
                "code": "graph_b.revision_required",
                "message": "deterministic reviewer requires one evidence-bound revision",
                "artifact_references": [],
            }]),
            serde_json::json!([rejected_task]),
        )
    } else {
        (true, serde_json::json!([]), serde_json::json!([]))
    };
    let text = serde_json::to_string(&BTreeMap::from([
        ("approved", serde_json::Value::Bool(approved)),
        ("findings", findings),
        ("rejected_task_ids", rejected_task_ids),
    ]))
    .map_err(|_| {
        ProviderExecutionDependencyError::InvalidRequest(String::from(
            "graph_b_review_sequence response serialization failed",
        ))
    })?;
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
                reasoning_tokens: 0,
                estimated: false,
            },
            None,
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
        reasoning_tokens: 0,
        estimated: false,
    };
    let events = match scenario {
        "text" => vec![
            DependencyProviderEvent::Started,
            DependencyProviderEvent::TextDelta(text),
            completed("stop", usage, None),
        ],
        "streaming_text" => vec![
            DependencyProviderEvent::Started,
            DependencyProviderEvent::TextDelta("alpha ".into()),
            DependencyProviderEvent::TextDelta("beta ".into()),
            DependencyProviderEvent::TextDelta(text),
            completed("stop", usage, None),
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
                    reasoning_tokens: 0,
                    estimated: false,
                },
                None,
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

fn completed(
    reason: &str,
    usage: DependencyUsage,
    cost: Option<DependencyCostMetadata>,
) -> DependencyProviderEvent {
    DependencyProviderEvent::Completed {
        finish_reason: reason.into(),
        usage,
        cost,
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
    fn deterministic_gate_rejects_path_like_identifiers_before_environment_access() {
        let options = BTreeMap::from([(String::from("mock_gate_id"), String::from("../escape"))]);
        assert!(matches!(
            wait_for_test_gate(&options, "request"),
            Err(ProviderExecutionDependencyError::InvalidRequest(message))
                if message == "mock_gate_id is invalid"
        ));
    }

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
    fn frozen_mock_provider_selector_uses_the_same_deterministic_fixture() {
        let dependency = StaticProviderCatalogDependency::built_in();
        let mut legacy = request("text");
        legacy.provider_key = String::from("mock");
        assert!(
            dependency.execute_provider(legacy).is_ok(),
            "frozen 1.1 built-in styles retain their exact mock provider selector"
        );

        let mut unsupported = request("text");
        unsupported.provider_key = String::from("unconfigured-provider");
        assert!(matches!(
            dependency.execute_provider(unsupported),
            Err(ProviderExecutionDependencyError::ProviderNotConfigured)
        ));
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
    #[allow(
        clippy::too_many_lines,
        reason = "the deterministic planner fixture test keeps all canonical phases visible together"
    )]
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

        let generic_review = |revision| {
            let mut request = request("planner_worker");
            request.entries = vec![DependencyConversationEntry::Metadata {
                key: String::from("agentmod.generic_review_request"),
                value_json: serde_json::json!({
                    "current_revision": revision,
                    "known_task_ids": ["planner-task-0", "evidence-task-0"],
                })
                .to_string(),
            }];
            dependency
                .execute_provider(request)
                .expect("generic planner review")
        };
        assert!(generic_review(0).events.iter().any(|event| matches!(
            event,
            DependencyProviderEvent::TextDelta(text)
                if text.contains("\"approved\":false")
                    && text.contains("\"evidence-task-0\"")
                    && text.contains("\"planner.evidence_revision\"")
        )));
        assert!(generic_review(1).events.iter().any(|event| matches!(
            event,
            DependencyProviderEvent::TextDelta(text)
                if text.contains("\"approved\":true")
        )));

        let mut evidence_bound = request("planner_worker");
        evidence_bound.entries = vec![DependencyConversationEntry::Metadata {
            key: String::from("agentmod.generic_review_request"),
            value_json: serde_json::json!({
                "schema_version": 2,
                "current_revision": 0,
                "known_task_ids": ["planner-task-0", "evidence-task-0"],
                "artifact_evidence": [{
                    "artifact_reference": "artifact:blake3:review-evidence",
                }],
            })
            .to_string(),
        }];
        let evidence_bound = dependency
            .execute_provider(evidence_bound)
            .expect("evidence-bound generic planner review");
        assert!(evidence_bound.events.iter().any(|event| matches!(
            event,
            DependencyProviderEvent::TextDelta(text)
                if text.contains("\"artifact:blake3:review-evidence\"")
                    && !text.contains("\"integration_artifact\"")
        )));
    }

    #[test]
    fn planner_worker_child_routes_real_tools_from_canonical_loop_iteration() {
        let events_for = |loop_iteration| {
            planner_worker_child_events(&[DependencyConversationEntry::Metadata {
                key: String::from("agentmod.canonical_node_work"),
                value_json: serde_json::json!({
                    "run_id": "run-1",
                    "node_id": "research",
                    "attempt": 1,
                    "loop_iteration": loop_iteration,
                    "step": loop_iteration + 1,
                })
                .to_string(),
            }])
            .expect("canonical worker request")
        };
        for (iteration, call_id, tool) in [
            (0, "planner-worker-edit", "filesystem.edit"),
            (1, "planner-worker-test", "process.run"),
            (2, "planner-worker-diff", "git.diff"),
        ] {
            assert!(events_for(iteration).iter().any(|event| matches!(
                event,
                DependencyProviderEvent::ToolCallProposed {
                    call_id: actual_call_id,
                    tool: actual_tool,
                    ..
                } if actual_call_id == call_id && actual_tool == tool
            )));
        }
        assert!(matches!(
            events_for(3).last(),
            Some(DependencyProviderEvent::Completed { .. })
        ));
    }

    #[test]
    fn planner_worker_v1_4_integration_requires_canonical_member_order() {
        let integration = |members: &[&str]| {
            let evidence = members
                .iter()
                .map(|member| {
                    serde_json::json!({
                        "member_id": member,
                        "child_session_id": format!("child-{member}"),
                        "content": {"receipt": member},
                    })
                })
                .collect::<Vec<_>>();
            let mut request = request("planner_worker");
            request.options.push(DependencyProviderOption {
                key: String::from("mock_planner_phase"),
                value: String::from("integrate_v1_4"),
            });
            request.entries = vec![DependencyConversationEntry::Metadata {
                key: String::from("agentmod.canonical_model_inputs"),
                value_json: serde_json::json!({
                    "joined": {"artifact_evidence": evidence},
                })
                .to_string(),
            }];
            StaticProviderCatalogDependency::built_in().execute_provider(request)
        };
        let accepted = integration(&["evidence", "planner"]).expect("canonical order");
        assert!(accepted.events.iter().any(|event| matches!(
            event,
            DependencyProviderEvent::TextDelta(text)
                if text.contains("\"member_order\":[\"evidence\",\"planner\"]")
        )));
        assert!(matches!(
            integration(&["planner", "evidence"]),
            Err(ProviderExecutionDependencyError::InvalidRequest(message))
                if message.contains("order is not canonical")
        ));
    }

    #[test]
    fn graph_b_review_fixture_routes_from_canonical_revision_metadata() {
        let dependency = StaticProviderCatalogDependency::built_in();
        let review_request = |revision, known_task_ids: &[&str]| {
            let mut request = request("graph_b_review_sequence");
            request.entries = vec![DependencyConversationEntry::Metadata {
                key: String::from("agentmod.generic_review_request"),
                value_json: serde_json::json!({
                    "current_revision": revision,
                    "known_task_ids": known_task_ids,
                })
                .to_string(),
            }];
            request
        };

        let rejected = dependency
            .execute_provider(review_request(0, &["worker-b-0", "worker-a-0"]))
            .expect("revision zero is rejected");
        assert!(rejected.events.iter().any(|event| matches!(
            event,
            DependencyProviderEvent::TextDelta(text)
                if text.contains("\"approved\":false")
                    && text.contains("\"worker-a-0\"")
                    && !text.contains("\"worker-b-0\"")
                    && text.contains("\"graph_b.revision_required\"")
        )));

        let approved = dependency
            .execute_provider(review_request(1, &["worker-b-1", "worker-a-1"]))
            .expect("later revision is approved");
        assert!(approved.events.iter().any(|event| matches!(
            event,
            DependencyProviderEvent::TextDelta(text)
                if text == "{\"approved\":true,\"findings\":[],\"rejected_task_ids\":[]}"
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
