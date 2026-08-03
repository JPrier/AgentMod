//! Deterministic provider dependency for the independent harness fixture.
//!
//! This crate deliberately does not import the native harness dependency; the
//! fixture process proves the harness protocol is modular by implementing its
//! own provider execution and cancellation from scratch.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/// Stable identity and version of the independent fixture harness.
pub const FIXTURE_HARNESS_ID: &str = "independent-fixture";
/// Fixture adapter version.
pub const FIXTURE_HARNESS_VERSION: &str = "2.0.0";
/// Fixture provider key.
pub const FIXTURE_PROVIDER: &str = "fixture-deterministic";
/// Fixture model key.
pub const FIXTURE_MODEL: &str = "fixture-model";

/// Dependency-owned conversation entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FixtureConversationEntry {
    /// System instruction.
    System(String),
    /// User text.
    User(String),
    /// Image input; the fixture does not support images.
    Image {
        /// Media type.
        media_type: String,
        /// Base64 data.
        data_base64: String,
    },
    /// Assistant text.
    Assistant(String),
    /// Tool call.
    ToolCall {
        /// Call ID.
        call_id: String,
        /// Tool name.
        tool: String,
        /// JSON arguments.
        arguments_json: String,
    },
    /// Tool result.
    ToolResult {
        /// Call ID.
        call_id: String,
        /// Content.
        content: String,
        /// Truncation marker.
        truncated: bool,
    },
    /// Context summary.
    ContextSummary {
        /// Text.
        text: String,
        /// Start sequence.
        source_start: u64,
        /// End sequence.
        source_end: u64,
    },
    /// Metadata.
    Metadata {
        /// Key.
        key: String,
        /// JSON value.
        value_json: String,
    },
}

/// Dependency-owned provider option.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureProviderOption {
    /// Option key.
    pub key: String,
    /// Textual value.
    pub value: String,
}

/// Dependency-owned execution request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureExecutionRequest {
    /// Provider selection.
    pub provider_key: String,
    /// Model selection.
    pub model_key: String,
    /// Projected conversation.
    pub entries: Vec<FixtureConversationEntry>,
    /// Approved options.
    pub options: Vec<FixtureProviderOption>,
    /// Runtime-issued authorization grant.
    pub authorization_grant: String,
    /// Cancellation reference.
    pub cancellation_reference: String,
    /// True for a fresh request issued after a runtime continuation.
    pub resumed_after_continuation: bool,
}

/// Dependency-owned usage.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FixtureUsage {
    /// Input tokens.
    pub input_tokens: u64,
    /// Output tokens.
    pub output_tokens: u64,
}

/// Dependency-owned provider event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FixtureProviderEvent {
    /// Request started.
    Started,
    /// Visible text fragment.
    TextDelta(String),
    /// Tool-call fragment.
    ToolCallDelta {
        /// Call ID.
        call_id: String,
        /// Name fragment.
        name_fragment: String,
        /// Arguments fragment.
        arguments_fragment: String,
    },
    /// Complete tool-call proposal.
    ToolCallProposed {
        /// Continuation reference.
        continuation_reference: String,
        /// Call ID.
        call_id: String,
        /// Tool name.
        tool: String,
        /// JSON arguments.
        arguments_json: String,
    },
    /// Normal completion.
    Completed {
        /// Finish reason.
        finish_reason: String,
        /// Usage.
        usage: FixtureUsage,
    },
    /// Cancelled request.
    Cancelled,
    /// Classified failure.
    Failed {
        /// Stable code.
        code: String,
        /// Redacted message.
        message: String,
        /// Retry flag.
        retryable: bool,
    },
}

/// Dependency-owned bounded response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureExecutionResponse {
    /// Events in order.
    pub events: Vec<FixtureProviderEvent>,
}

/// Dependency failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum FixtureExecutionError {
    /// Request fields or scenario are invalid.
    #[error("fixture request is invalid: {0}")]
    InvalidRequest(String),
    /// Provider is not part of the fixture.
    #[error("fixture provider is not configured")]
    ProviderNotConfigured,
}

/// Dependency-owned catalog record.
#[allow(
    clippy::struct_excessive_bools,
    reason = "capability flags are the catalog contract"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureCatalogRecord {
    /// Stable harness ID.
    pub id: String,
    /// Adapter version.
    pub version: String,
    /// Model IDs.
    pub models: Vec<String>,
    /// Capability names.
    pub capabilities: BTreeSet<String>,
    /// Tool-call support.
    pub tool_support: bool,
    /// Image support.
    pub image_support: bool,
    /// Structured-output support.
    pub structured_output_support: bool,
    /// Streaming support.
    pub streaming_support: bool,
    /// Pricing source.
    pub pricing_source: String,
    /// Whether the provider accepts work.
    pub available: bool,
}

/// External provider execution interface consumed by fixture data.
#[async_trait]
pub trait FixtureProviderExecution: Send + Sync {
    /// Executes one deterministic provider request.
    ///
    /// # Errors
    ///
    /// Returns a dependency error for invalid selection or bounds.
    async fn execute(
        &self,
        request: FixtureExecutionRequest,
    ) -> Result<FixtureExecutionResponse, FixtureExecutionError>;
}

/// External provider cancellation interface consumed by fixture data.
#[async_trait]
pub trait FixtureProviderCancellation: Send + Sync {
    /// Requests cancellation of an in-flight fixture exchange.
    ///
    /// # Errors
    ///
    /// Returns a dependency error for a malformed reference.
    async fn cancel(&self, reference: &str) -> Result<bool, FixtureExecutionError>;
}

/// Deterministic fixture provider catalog dependency.
#[derive(Clone, Debug)]
pub struct FixtureProviderCatalogDependency {
    grant_validation: GrantValidation,
    active: Arc<Mutex<BTreeMap<String, CancellationToken>>>,
}

#[derive(Clone, Debug)]
enum GrantValidation {
    Development,
    Secure {
        key: [u8; 32],
        uses: Arc<Mutex<BTreeMap<uuid::Uuid, u8>>>,
    },
}

impl Default for FixtureProviderCatalogDependency {
    fn default() -> Self {
        Self::development()
    }
}

impl FixtureProviderCatalogDependency {
    /// Creates the fixture dependency in development grant mode.
    #[must_use]
    pub fn development() -> Self {
        Self {
            grant_validation: GrantValidation::Development,
            active: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Creates the fixture dependency with keyed grant validation.
    #[must_use]
    pub fn secure(authorization_key: [u8; 32]) -> Self {
        Self {
            grant_validation: GrantValidation::Secure {
                key: authorization_key,
                uses: Arc::new(Mutex::new(BTreeMap::new())),
            },
            active: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn validate_grant(&self, grant: &str, resumed: bool) -> Result<(), FixtureExecutionError> {
        match &self.grant_validation {
            GrantValidation::Development => {
                if grant == "grant" {
                    Ok(())
                } else {
                    Err(FixtureExecutionError::InvalidRequest(
                        "development authorization grant is invalid".into(),
                    ))
                }
            }
            GrantValidation::Secure { key, uses } => {
                validate_runtime_grant(grant, key, uses, resumed)
            }
        }
    }

    /// Returns the fixture's bounded capability set.
    #[must_use]
    pub fn capabilities() -> BTreeSet<String> {
        BTreeSet::from([
            "cancellation".to_owned(),
            "streaming".to_owned(),
            "structured_context_replacement".to_owned(),
            "token_usage".to_owned(),
            "tool_calls".to_owned(),
        ])
    }

    /// Returns the fixture catalog record.
    #[must_use]
    pub fn catalog_record() -> FixtureCatalogRecord {
        let capabilities = Self::capabilities();
        FixtureCatalogRecord {
            id: FIXTURE_HARNESS_ID.to_owned(),
            version: FIXTURE_HARNESS_VERSION.to_owned(),
            models: vec![FIXTURE_MODEL.to_owned()],
            capabilities: capabilities.clone(),
            tool_support: capabilities.contains("tool_calls"),
            image_support: capabilities.contains("images"),
            structured_output_support: capabilities.contains("structured_output"),
            streaming_support: capabilities.contains("streaming"),
            pricing_source: "deterministic-fixture".to_owned(),
            available: true,
        }
    }
}

#[async_trait]
impl FixtureProviderExecution for FixtureProviderCatalogDependency {
    async fn execute(
        &self,
        request: FixtureExecutionRequest,
    ) -> Result<FixtureExecutionResponse, FixtureExecutionError> {
        self.validate_grant(
            &request.authorization_grant,
            request.resumed_after_continuation,
        )?;
        if request.provider_key != FIXTURE_PROVIDER
            || request.model_key.trim().is_empty()
            || request.cancellation_reference.trim().is_empty()
        {
            return Err(FixtureExecutionError::InvalidRequest(
                "fixture requires the deterministic provider, model, and cancellation reference"
                    .into(),
            ));
        }
        let options: BTreeMap<_, _> = request
            .options
            .iter()
            .map(|option| (option.key.clone(), option.value.clone()))
            .collect();
        let scenario = options
            .get("fixture_scenario")
            .map_or("text", String::as_str);
        let text = options
            .get("fixture_text")
            .cloned()
            .unwrap_or_else(|| "independent fixture response".to_owned());
        let cancellation = CancellationToken::new();
        {
            let mut active = self.active.lock().map_err(|_| {
                FixtureExecutionError::InvalidRequest(
                    "fixture cancellation state is unavailable".into(),
                )
            })?;
            active.insert(request.cancellation_reference.clone(), cancellation.clone());
        }
        let events = if request.resumed_after_continuation {
            vec![
                FixtureProviderEvent::Started,
                FixtureProviderEvent::TextDelta("continued after approved runtime decision".into()),
                FixtureProviderEvent::Completed {
                    finish_reason: "stop".into(),
                    usage: FixtureUsage {
                        input_tokens: 4,
                        output_tokens: 2,
                    },
                },
            ]
        } else if scenario == "slow_stream" {
            tokio::select! {
                () = cancellation.cancelled() => {
                    vec![
                        FixtureProviderEvent::Started,
                        FixtureProviderEvent::TextDelta("partial before cancellation".into()),
                        FixtureProviderEvent::Cancelled,
                    ]
                }
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    vec![
                        FixtureProviderEvent::Started,
                        FixtureProviderEvent::TextDelta(text),
                        FixtureProviderEvent::Completed {
                            finish_reason: "stop".into(),
                            usage: FixtureUsage { input_tokens: 6, output_tokens: 4 },
                        },
                    ]
                }
            }
        } else if scenario == "cancelled" {
            vec![
                FixtureProviderEvent::Started,
                FixtureProviderEvent::TextDelta("partial before cancellation".into()),
                FixtureProviderEvent::Cancelled,
            ]
        } else {
            scenario_events(scenario, &text, &request.entries, &options)?
        };
        self.active
            .lock()
            .map_err(|_| {
                FixtureExecutionError::InvalidRequest(
                    "fixture cancellation state is unavailable".into(),
                )
            })?
            .remove(&request.cancellation_reference);
        Ok(FixtureExecutionResponse { events })
    }
}

#[async_trait]
impl FixtureProviderCancellation for FixtureProviderCatalogDependency {
    async fn cancel(&self, reference: &str) -> Result<bool, FixtureExecutionError> {
        if reference.trim().is_empty() {
            return Err(FixtureExecutionError::InvalidRequest(
                "cancellation reference is required".into(),
            ));
        }
        let token = self
            .active
            .lock()
            .map_err(|_| {
                FixtureExecutionError::InvalidRequest(
                    "fixture cancellation state is unavailable".into(),
                )
            })?
            .get(reference)
            .cloned();
        if let Some(token) = token {
            token.cancel();
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "the scenario matrix is intentionally explicit for auditability"
)]
fn scenario_events(
    scenario: &str,
    text: &str,
    entries: &[FixtureConversationEntry],
    options: &BTreeMap<String, String>,
) -> Result<Vec<FixtureProviderEvent>, FixtureExecutionError> {
    let usage = FixtureUsage {
        input_tokens: 5,
        output_tokens: 3,
    };
    let events = match scenario {
        "text" | "non_streaming" => vec![
            FixtureProviderEvent::Started,
            FixtureProviderEvent::TextDelta(text.to_owned()),
            FixtureProviderEvent::Completed {
                finish_reason: "stop".into(),
                usage,
            },
        ],
        "streaming_text" => vec![
            FixtureProviderEvent::Started,
            FixtureProviderEvent::TextDelta("alpha ".into()),
            FixtureProviderEvent::TextDelta("beta ".into()),
            FixtureProviderEvent::TextDelta(text.to_owned()),
            FixtureProviderEvent::Completed {
                finish_reason: "stop".into(),
                usage,
            },
        ],
        "one_tool_call" => {
            let mut events = vec![FixtureProviderEvent::Started];
            events.push(FixtureProviderEvent::ToolCallDelta {
                call_id: "fixture-call-1".into(),
                name_fragment: "read_file".into(),
                arguments_fragment: r#"{"path":"src/lib.rs"}"#.into(),
            });
            events.push(FixtureProviderEvent::ToolCallProposed {
                continuation_reference: "018f6f83-7b80-7000-8000-000000000101".into(),
                call_id: "fixture-call-1".into(),
                tool: "read_file".into(),
                arguments_json: r#"{"path":"src/lib.rs"}"#.into(),
            });
            events
        }
        "rate_limited" => vec![
            FixtureProviderEvent::Started,
            FixtureProviderEvent::Failed {
                code: "rate_limited".into(),
                message: "fixture provider rate limited the request".into(),
                retryable: true,
            },
        ],
        "authentication_failed" => vec![FixtureProviderEvent::Failed {
            code: "authentication_failed".into(),
            message: "fixture provider rejected the supplied credentials".into(),
            retryable: false,
        }],
        "malformed_arguments" => vec![
            FixtureProviderEvent::Started,
            FixtureProviderEvent::ToolCallDelta {
                call_id: "fixture-call-bad".into(),
                name_fragment: "read_file".into(),
                arguments_fragment: "{not-json".into(),
            },
            FixtureProviderEvent::Failed {
                code: "malformed_tool_arguments".into(),
                message: "fixture provider returned malformed tool arguments".into(),
                retryable: false,
            },
        ],
        "unsupported_image" => vec![FixtureProviderEvent::Failed {
            code: "unsupported_capability".into(),
            message: "independent fixture harness does not support image inputs".into(),
            retryable: false,
        }],
        "unsupported_structured_output" => vec![FixtureProviderEvent::Failed {
            code: "unsupported_capability".into(),
            message: "independent fixture harness does not support structured output".into(),
            retryable: false,
        }],
        "unsupported_response_format" => vec![FixtureProviderEvent::Failed {
            code: "unsupported_capability".into(),
            message: "independent fixture harness does not support response_format".into(),
            retryable: false,
        }],
        other => {
            return Err(FixtureExecutionError::InvalidRequest(format!(
                "unsupported fixture scenario `{other}`"
            )));
        }
    };
    // Negative capability guards: images and structured output are rejected
    // regardless of the selected scenario.
    if entries
        .iter()
        .any(|entry| matches!(entry, FixtureConversationEntry::Image { .. }))
    {
        return Ok(vec![FixtureProviderEvent::Failed {
            code: "unsupported_capability".into(),
            message: "independent fixture harness does not support image inputs".into(),
            retryable: false,
        }]);
    }
    if options.contains_key("response_format")
        || options.contains_key("structured_output")
        || options.contains_key("responseSchema")
    {
        return Ok(vec![FixtureProviderEvent::Failed {
            code: "unsupported_capability".into(),
            message: "independent fixture harness does not support structured output".into(),
            retryable: false,
        }]);
    }
    Ok(events)
}

/// Validates a runtime-issued keyed grant.
pub(crate) fn validate_runtime_grant(
    grant: &str,
    key: &[u8; 32],
    uses: &Arc<Mutex<BTreeMap<uuid::Uuid, u8>>>,
    resumed: bool,
) -> Result<(), FixtureExecutionError> {
    let fields: Vec<_> = grant.split('.').collect();
    if fields.len() != 5
        || fields[0] != "v1"
        || fields[3].len() != 64
        || !fields[3].bytes().all(|byte| byte.is_ascii_hexdigit())
        || fields[4].len() != 64
        || !fields[4].bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(FixtureExecutionError::InvalidRequest(
            "runtime authorization grant is invalid".into(),
        ));
    }
    let expires = fields[1].parse::<u128>().map_err(|_| {
        FixtureExecutionError::InvalidRequest("runtime authorization grant is invalid".into())
    })?;
    let nonce = fields[2]
        .parse::<uuid::Uuid>()
        .map_err(|_| FixtureExecutionError::InvalidRequest("grant nonce is invalid".into()))?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| FixtureExecutionError::InvalidRequest("clock is unavailable".into()))?
        .as_millis();
    if expires < now || expires.saturating_sub(now) > 300_000 {
        return Err(FixtureExecutionError::InvalidRequest(
            "runtime authorization grant is expired".into(),
        ));
    }
    let payload = fields[..4].join(".");
    let expected = blake3::keyed_hash(key, payload.as_bytes())
        .to_hex()
        .to_string();
    if !constant_time_equal(expected.as_bytes(), fields[4].as_bytes()) {
        return Err(FixtureExecutionError::InvalidRequest(
            "runtime authorization grant signature is invalid".into(),
        ));
    }
    let mut uses = uses.lock().map_err(|_| {
        FixtureExecutionError::InvalidRequest("grant replay state is unavailable".into())
    })?;
    match (uses.get_mut(&nonce), resumed) {
        (None, false) => {
            uses.insert(nonce, 1);
        }
        (Some(count), true) if *count < 16 => {
            *count += 1;
        }
        _ => {
            return Err(FixtureExecutionError::InvalidRequest(
                "runtime authorization grant was replayed".into(),
            ));
        }
    }
    Ok(())
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

/// Parses an exact 32-byte hexadecimal bootstrap key.
///
/// # Errors
///
/// Returns [`FixtureExecutionError::InvalidRequest`] for malformed input.
pub fn parse_authorization_key(value: &str) -> Result<[u8; 32], FixtureExecutionError> {
    if value.len() != 64 {
        return Err(FixtureExecutionError::InvalidRequest(
            "fixture authorization key must be 64 hex characters".into(),
        ));
    }
    let mut key = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(chunk).map_err(|_| {
            FixtureExecutionError::InvalidRequest("fixture authorization key is invalid".into())
        })?;
        key[index] = u8::from_str_radix(text, 16).map_err(|_| {
            FixtureExecutionError::InvalidRequest("fixture authorization key is invalid".into())
        })?;
    }
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(scenario: &str) -> FixtureExecutionRequest {
        FixtureExecutionRequest {
            provider_key: FIXTURE_PROVIDER.into(),
            model_key: FIXTURE_MODEL.into(),
            entries: vec![FixtureConversationEntry::User("hello".into())],
            options: vec![FixtureProviderOption {
                key: "fixture_scenario".into(),
                value: scenario.into(),
            }],
            authorization_grant: "grant".into(),
            cancellation_reference: "fixture-cancel-1".into(),
            resumed_after_continuation: false,
        }
    }

    #[tokio::test]
    async fn deterministic_scenarios_cover_required_behaviors() {
        let dependency = FixtureProviderCatalogDependency::development();
        for scenario in [
            "text",
            "streaming_text",
            "non_streaming",
            "one_tool_call",
            "rate_limited",
            "authentication_failed",
            "malformed_arguments",
            "cancelled",
            "unsupported_image",
            "unsupported_structured_output",
        ] {
            let response = dependency
                .execute(request(scenario))
                .await
                .expect("scenario");
            assert!(matches!(
                response.events.first(),
                Some(FixtureProviderEvent::Started | FixtureProviderEvent::Failed { .. })
            ));
        }
    }

    #[tokio::test]
    async fn image_entries_are_rejected_with_unsupported_capability() {
        let dependency = FixtureProviderCatalogDependency::development();
        let mut request = request("text");
        request.entries = vec![FixtureConversationEntry::Image {
            media_type: "image/png".into(),
            data_base64: "aGVsbG8=".into(),
        }];
        let response = dependency.execute(request).await.expect("execution");
        assert!(matches!(
            response.events.last(),
            Some(FixtureProviderEvent::Failed {
                code,
                retryable: false,
                ..
            }) if code == "unsupported_capability"
        ));
    }

    #[tokio::test]
    async fn structured_output_options_are_rejected() {
        let dependency = FixtureProviderCatalogDependency::development();
        let mut request = request("text");
        request.options.push(FixtureProviderOption {
            key: "response_format".into(),
            value: r#"{"type":"json_object"}"#.into(),
        });
        let response = dependency.execute(request).await.expect("execution");
        assert!(matches!(
            response.events.last(),
            Some(FixtureProviderEvent::Failed {
                code,
                retryable: false,
                ..
            }) if code == "unsupported_capability"
        ));
    }

    #[tokio::test]
    async fn slow_stream_can_be_cancelled_in_flight() {
        let dependency = FixtureProviderCatalogDependency::development();
        let task = tokio::spawn({
            let dependency = dependency.clone();
            async move {
                dependency
                    .execute(request("slow_stream"))
                    .await
                    .expect("execution")
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(dependency.cancel("fixture-cancel-1").await.expect("cancel"));
        let response = task.await.expect("task");
        assert_eq!(
            response.events.last(),
            Some(&FixtureProviderEvent::Cancelled)
        );
    }

    #[tokio::test]
    async fn secure_catalog_rejects_replayed_grants() {
        let key = [3_u8; 32];
        let dependency = FixtureProviderCatalogDependency::secure(key);
        let mut signed = request("text");
        signed.authorization_grant = signed_grant(&key, uuid::Uuid::from_u128(301));
        dependency.execute(signed.clone()).await.expect("first use");
        assert!(dependency.execute(signed).await.is_err());
    }

    fn signed_grant(key: &[u8; 32], nonce: uuid::Uuid) -> String {
        let expires = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis()
            + 60_000;
        let binding = "cd".repeat(32);
        let payload = format!("v1.{expires}.{nonce}.{binding}");
        let signature = blake3::keyed_hash(key, payload.as_bytes());
        format!("{payload}.{}", signature.to_hex())
    }
}
