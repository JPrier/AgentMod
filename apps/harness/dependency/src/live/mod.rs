//! Live provider adapters and catalog for the native harness dependency.
//!
//! This is a synchronous port of the live-provider stack so the harness
//! dependency boundary retains its existing synchronous
//! [`ProviderExecutionDependency`] contract. HTTP exchanges use the blocking
//! reqwest client; cancellation is signalled through a shared atomic flag so
//! an in-flight exchange can be interrupted between stream chunks without
//! introducing a second asynchronous execution model.

pub mod pricing;
pub mod retry;
pub mod sse;
pub mod wire_anthropic;
pub mod wire_gemini;
pub mod wire_openai;

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use crate::execution::{
    DependencyProviderEvent, DependencyProviderExecutionRequest,
    DependencyProviderExecutionResponse, DependencyProviderFailureKind, DependencyProviderOption,
    DependencyRetryClassification, DependencyUsage, ProviderCancellationDependency,
    ProviderExecutionDependency, ProviderExecutionDependencyError, validate_runtime_grant,
};
use crate::{
    DependencyCatalogRecord, DependencyError, DependencyProviderRecord, ProviderCatalogDependency,
    ProviderCatalogDetailDependency, ProviderCatalogDetailResponse, ProviderCatalogProbeRequest,
    ProviderCatalogProbeResponse,
};

/// Stable live provider adapter IDs.
/// Generic OpenAI-compatible HTTP endpoint.
pub const PROVIDER_OPENAI_COMPATIBLE: &str = "openai-compatible";
/// `OpenRouter` endpoint.
pub const PROVIDER_OPENROUTER: &str = "openrouter";
/// `OpenAI` official endpoint.
pub const PROVIDER_OPENAI: &str = "openai";
/// Anthropic official endpoint.
pub const PROVIDER_ANTHROPIC: &str = "anthropic";
/// Google Gemini endpoint.
pub const PROVIDER_GEMINI: &str = "gemini";
/// Local OpenAI-compatible endpoint.
pub const PROVIDER_LOCAL: &str = "local";

/// All supported live provider IDs in stable order.
pub const LIVE_PROVIDER_IDS: [&str; 6] = [
    PROVIDER_OPENAI_COMPATIBLE,
    PROVIDER_OPENROUTER,
    PROVIDER_OPENAI,
    PROVIDER_ANTHROPIC,
    PROVIDER_GEMINI,
    PROVIDER_LOCAL,
];

const MAX_STREAM_EVENTS: usize = 65_536;
const MAX_EVENT_DELTAS_BYTES: usize = 16 * 1024 * 1024;
const MAX_NON_STREAM_BODY_BYTES: usize = 32 * 1024 * 1024;

/// Resolved endpoint configuration for one provider exchange.
#[derive(Clone, Debug)]
pub struct ProviderEndpointConfig {
    /// Stable provider key.
    pub provider_key: String,
    /// Explicit base URL.
    pub base_url: String,
    /// Resolved API key; never serialized or logged.
    pub api_key: Option<String>,
    /// Whether TLS peer verification is enabled.
    pub tls_verify: bool,
    /// Per-request deadline.
    pub timeout: Duration,
    /// Optional pricing record table keyed by model ID.
    pub pricing: pricing::PricingTable,
}

/// Live provider catalog dependency.
///
/// Reads provider configuration from the process environment and request
/// options. Secret values are always resolved from environment references and
/// never cross protocol frames, events, logs, or errors.
#[derive(Clone, Debug)]
pub struct LiveProviderCatalogDependency {
    grant_validation: GrantValidation,
    active: Arc<Mutex<BTreeMap<String, Arc<AtomicBool>>>>,
}

#[derive(Clone, Debug)]
enum GrantValidation {
    Development,
    Secure {
        key: [u8; 32],
        uses: Arc<Mutex<BTreeMap<uuid::Uuid, u8>>>,
    },
}

impl Default for LiveProviderCatalogDependency {
    fn default() -> Self {
        Self::development()
    }
}

impl LiveProviderCatalogDependency {
    /// Creates the live catalog in development grant mode.
    #[must_use]
    pub fn development() -> Self {
        Self {
            grant_validation: GrantValidation::Development,
            active: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Creates the live catalog with mandatory keyed grant validation.
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

    fn validate_grant(
        &self,
        grant: &str,
        resumed: bool,
    ) -> Result<(), ProviderExecutionDependencyError> {
        match &self.grant_validation {
            GrantValidation::Development => {
                if grant == "grant" {
                    Ok(())
                } else {
                    Err(ProviderExecutionDependencyError::InvalidRequest(
                        "development authorization grant is invalid".into(),
                    ))
                }
            }
            GrantValidation::Secure { key, uses } => {
                validate_runtime_grant(grant, key, uses, resumed)
            }
        }
    }
}

/// Resolves the endpoint configuration for one request from environment and
/// approved options. Never accepts secret values inside options.
///
/// # Errors
///
/// Returns a redacted configuration error.
pub fn resolve_endpoint_config(
    provider_key: &str,
    options: &[DependencyProviderOption],
) -> Result<ProviderEndpointConfig, ProviderExecutionDependencyError> {
    let options: BTreeMap<_, _> = options
        .iter()
        .map(|option| (option.key.as_str(), option.value.as_str()))
        .collect();
    let env_prefix = format!("AGENTMOD_PROVIDER_{}", provider_key.to_ascii_uppercase());
    let base_url = options
        .get("base_url")
        .map(|value| (*value).to_owned())
        .or_else(|| std::env::var(format!("{env_prefix}_BASE_URL")).ok())
        .or_else(|| default_base_url(provider_key).map(str::to_owned))
        .filter(|value| !value.trim().is_empty());
    let Some(base_url) = base_url else {
        return Err(ProviderExecutionDependencyError::InvalidRequest(format!(
            "provider `{provider_key}` has no configured base URL; set {env_prefix}_BASE_URL"
        )));
    };
    let api_key = resolve_api_key(provider_key, &env_prefix, &options)?;
    let tls_verify = options
        .get("tls_verify")
        .and_then(|value| match *value {
            "false" | "0" => Some(false),
            "true" | "1" => Some(true),
            _ => None,
        })
        .or_else(|| {
            std::env::var(format!("{env_prefix}_TLS_VERIFY"))
                .ok()
                .map(|value| !matches!(value.as_str(), "false" | "0"))
        })
        .unwrap_or(true);
    let timeout = options
        .get("timeout_ms")
        .and_then(|value| value.parse::<u64>().ok())
        .or_else(|| {
            std::env::var(format!("{env_prefix}_TIMEOUT_MS"))
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
        })
        .map_or_else(|| Duration::from_secs(120), Duration::from_millis);
    let pricing = if let Some(raw) = options.get("pricing_json") {
        pricing::PricingTable::parse(raw).unwrap_or_default()
    } else {
        pricing::PricingTable::from_env(&env_prefix)
    };
    Ok(ProviderEndpointConfig {
        provider_key: provider_key.to_owned(),
        base_url,
        api_key,
        tls_verify,
        timeout,
        pricing,
    })
}

fn resolve_api_key(
    provider_key: &str,
    env_prefix: &str,
    options: &BTreeMap<&str, &str>,
) -> Result<Option<String>, ProviderExecutionDependencyError> {
    if let Some(_value) = options.get("api_key") {
        return Err(ProviderExecutionDependencyError::InvalidRequest(format!(
            "provider `{provider_key}`: secret values must be provided through an environment reference, not an option"
        )));
    }
    let reference = options
        .get("api_key_ref")
        .or_else(|| options.get("api_key_env"))
        .map_or_else(
            || format!("{env_prefix}_API_KEY"),
            |value| (*value).to_owned(),
        );
    let value = if let Some(path) = reference.strip_prefix("file:") {
        read_secret_file(path)?
    } else {
        std::env::var(&reference).ok()
    };
    if value.is_none() && std::env::var(format!("{env_prefix}_REQUIRE_KEY")).is_ok() {
        return Err(ProviderExecutionDependencyError::InvalidRequest(format!(
            "provider `{provider_key}` requires API key reference {reference}"
        )));
    }
    Ok(value)
}

fn read_secret_file(path: &str) -> Result<Option<String>, ProviderExecutionDependencyError> {
    let mut file = std::fs::File::open(path).map_err(|_| {
        ProviderExecutionDependencyError::InvalidRequest(format!(
            "provider secret file `{path}` cannot be opened"
        ))
    })?;
    let mut bytes = Vec::new();
    if file.read_to_end(&mut bytes).is_err() || bytes.len() > 64 * 1024 {
        return Err(ProviderExecutionDependencyError::InvalidRequest(
            "provider secret file is unreadable or exceeds 64 KiB".into(),
        ));
    }
    let mut value = String::from_utf8(bytes).map_err(|_| {
        ProviderExecutionDependencyError::InvalidRequest(
            "provider secret file is not valid UTF-8".into(),
        )
    })?;
    while value.ends_with(['\r', '\n']) {
        value.pop();
    }
    Ok(Some(value))
}

fn default_base_url(provider_key: &str) -> Option<&'static str> {
    match provider_key {
        PROVIDER_OPENAI => Some("https://api.openai.com/v1"),
        PROVIDER_OPENROUTER => Some("https://openrouter.ai/api/v1"),
        PROVIDER_ANTHROPIC => Some("https://api.anthropic.com"),
        PROVIDER_GEMINI => Some("https://generativelanguage.googleapis.com"),
        _ => None,
    }
}

fn endpoint_path(
    provider_key: &str,
    model: &str,
) -> Result<String, ProviderExecutionDependencyError> {
    match provider_key {
        PROVIDER_OPENAI | PROVIDER_OPENROUTER | PROVIDER_OPENAI_COMPATIBLE | PROVIDER_LOCAL => {
            Ok("/chat/completions".to_owned())
        }
        PROVIDER_ANTHROPIC => Ok("/v1/messages".to_owned()),
        PROVIDER_GEMINI => Ok(format!(
            "/v1beta/models/{model}:streamGenerateContent?alt=sse"
        )),
        _other => Err(ProviderExecutionDependencyError::ProviderNotConfigured),
    }
}

/// Resolves the configured model list for a provider from environment.
fn configured_models(provider_key: &str) -> Vec<String> {
    let env_prefix = format!("AGENTMOD_PROVIDER_{}", provider_key.to_ascii_uppercase());
    std::env::var(format!("{env_prefix}_MODELS"))
        .ok()
        .map_or_else(
            || {
                default_models(provider_key)
                    .iter()
                    .map(|value| (*value).to_owned())
                    .collect()
            },
            |value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
                    .collect()
            },
        )
}

fn default_models(provider_key: &str) -> &'static [&'static str] {
    match provider_key {
        PROVIDER_OPENAI => &["gpt-4o-mini"],
        PROVIDER_ANTHROPIC => &["claude-3-5-haiku-latest"],
        PROVIDER_GEMINI => &["gemini-2.0-flash"],
        _ => &[],
    }
}

fn provider_ready(provider_key: &str) -> bool {
    match resolve_endpoint_config(provider_key, &[]) {
        Ok(config) => match provider_key {
            PROVIDER_OPENAI | PROVIDER_OPENROUTER | PROVIDER_ANTHROPIC | PROVIDER_GEMINI => {
                config.api_key.is_some()
            }
            _ => true,
        },
        Err(_) => false,
    }
}

fn capabilities_for(provider_key: &str) -> BTreeSet<String> {
    let mut capabilities = BTreeSet::from([
        "cancellation".to_owned(),
        "multiple_tool_calls".to_owned(),
        "streaming".to_owned(),
        "structured_context_replacement".to_owned(),
        "token_usage".to_owned(),
        "tool_calls".to_owned(),
    ]);
    match provider_key {
        PROVIDER_OPENAI | PROVIDER_OPENROUTER => {
            capabilities.insert("cost_metadata".to_owned());
            capabilities.insert("images".to_owned());
            capabilities.insert("provider_switching".to_owned());
            capabilities.insert("structured_output".to_owned());
        }
        PROVIDER_ANTHROPIC | PROVIDER_GEMINI => {
            capabilities.insert("images".to_owned());
            capabilities.insert("structured_output".to_owned());
        }
        _ => {}
    }
    capabilities
}

impl ProviderCatalogDependency for LiveProviderCatalogDependency {
    fn probe_catalog(
        &self,
        request: ProviderCatalogProbeRequest,
    ) -> Result<ProviderCatalogProbeResponse, DependencyError> {
        let mut providers = Vec::new();
        for provider_key in LIVE_PROVIDER_IDS {
            let ready = provider_ready(provider_key);
            if !request.include_unavailable && !ready {
                continue;
            }
            providers.push(DependencyProviderRecord {
                provider_key: provider_key.to_owned(),
                ready,
                capabilities: capabilities_for(provider_key),
            });
        }
        Ok(ProviderCatalogProbeResponse { providers })
    }
}

impl ProviderCatalogDetailDependency for LiveProviderCatalogDependency {
    fn probe_catalog_details(
        &self,
        request: ProviderCatalogProbeRequest,
    ) -> Result<ProviderCatalogDetailResponse, DependencyError> {
        let mut providers = Vec::new();
        for provider_key in LIVE_PROVIDER_IDS {
            let ready = provider_ready(provider_key);
            if !request.include_unavailable && !ready {
                continue;
            }
            let capabilities = capabilities_for(provider_key);
            let models = configured_models(provider_key);
            providers.push(DependencyCatalogRecord {
                provider_key: provider_key.to_owned(),
                version: env!("CARGO_PKG_VERSION").to_owned(),
                model_ids: models,
                capabilities: capabilities.clone(),
                context_limit: match provider_key {
                    PROVIDER_OPENAI => Some(128_000),
                    PROVIDER_ANTHROPIC => Some(200_000),
                    PROVIDER_GEMINI => Some(1_000_000),
                    _ => None,
                },
                tool_support: capabilities.contains("tool_calls"),
                image_support: capabilities.contains("images"),
                structured_output_support: capabilities.contains("structured_output"),
                streaming_support: capabilities.contains("streaming"),
                pricing_source: if provider_key == PROVIDER_OPENROUTER {
                    "openrouter-model-catalog".to_owned()
                } else {
                    "unknown".to_owned()
                },
                ready,
            });
        }
        Ok(ProviderCatalogDetailResponse { providers })
    }
}

impl ProviderCancellationDependency for LiveProviderCatalogDependency {
    fn cancel_provider(
        &self,
        cancellation_reference: &str,
    ) -> Result<bool, ProviderExecutionDependencyError> {
        if cancellation_reference.trim().is_empty() {
            return Err(ProviderExecutionDependencyError::InvalidRequest(
                "cancellation reference is required".into(),
            ));
        }
        let token = self
            .active
            .lock()
            .map_err(|_| {
                ProviderExecutionDependencyError::InvalidRequest(
                    "provider cancellation state is unavailable".into(),
                )
            })?
            .get(cancellation_reference)
            .cloned();
        if let Some(token) = token {
            token.store(true, Ordering::SeqCst);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl ProviderExecutionDependency for LiveProviderCatalogDependency {
    fn execute_provider(
        &self,
        request: DependencyProviderExecutionRequest,
    ) -> Result<DependencyProviderExecutionResponse, ProviderExecutionDependencyError> {
        self.validate_grant(
            &request.authorization_grant,
            request.resumed_after_continuation,
        )?;
        if !LIVE_PROVIDER_IDS.contains(&request.provider_key.as_str()) {
            return Err(ProviderExecutionDependencyError::ProviderNotConfigured);
        }
        let config = resolve_endpoint_config(&request.provider_key, &request.options)?;
        let cancelled = Arc::new(AtomicBool::new(false));
        {
            let mut active = self.active.lock().map_err(|_| {
                ProviderExecutionDependencyError::InvalidRequest(
                    "provider cancellation state is unavailable".into(),
                )
            })?;
            if active
                .insert(request.cancellation_reference.clone(), cancelled.clone())
                .is_some()
            {
                return Err(ProviderExecutionDependencyError::InvalidRequest(
                    "duplicate in-flight cancellation reference".into(),
                ));
            }
        }
        let result = execute_live(&config, &request, &cancelled);
        self.active
            .lock()
            .map_err(|_| {
                ProviderExecutionDependencyError::InvalidRequest(
                    "provider cancellation state is unavailable".into(),
                )
            })?
            .remove(&request.cancellation_reference);
        result
    }
}

/// Persistent provider-specific stream normalizer.
enum StreamNormalizer {
    OpenAi(wire_openai::OpenAiStreamNormalizer),
    Anthropic(wire_anthropic::AnthropicStreamNormalizer),
    Gemini(wire_gemini::GeminiStreamNormalizer),
}

impl StreamNormalizer {
    fn new(provider_key: &str) -> Result<Self, String> {
        match provider_key {
            PROVIDER_OPENAI | PROVIDER_OPENROUTER | PROVIDER_OPENAI_COMPATIBLE | PROVIDER_LOCAL => {
                Ok(Self::OpenAi(wire_openai::OpenAiStreamNormalizer::new()))
            }
            PROVIDER_ANTHROPIC => Ok(Self::Anthropic(
                wire_anthropic::AnthropicStreamNormalizer::new(),
            )),
            PROVIDER_GEMINI => Ok(Self::Gemini(wire_gemini::GeminiStreamNormalizer::new())),
            other => Err(format!("provider `{other}` is not configured")),
        }
    }

    fn started(&self) -> bool {
        match self {
            Self::OpenAi(normalizer) => normalizer.started,
            Self::Anthropic(normalizer) => normalizer.started,
            Self::Gemini(normalizer) => normalizer.started,
        }
    }

    fn usage(&self) -> Option<DependencyUsage> {
        match self {
            Self::OpenAi(normalizer) => normalizer.usage(),
            Self::Anthropic(normalizer) => normalizer.usage(),
            Self::Gemini(normalizer) => normalizer.usage(),
        }
    }

    fn finish_reason(&self) -> Option<&str> {
        match self {
            Self::OpenAi(normalizer) => normalizer.finish_reason(),
            Self::Anthropic(normalizer) => normalizer.finish_reason(),
            Self::Gemini(normalizer) => normalizer.finish_reason(),
        }
    }

    fn handle(
        &mut self,
        event_type: &str,
        value: &serde_json::Value,
    ) -> Result<Vec<DependencyProviderEvent>, String> {
        match self {
            Self::OpenAi(normalizer) => normalizer.handle(value),
            Self::Anthropic(normalizer) => normalizer.handle(event_type, value),
            Self::Gemini(normalizer) => normalizer.handle(value),
        }
    }

    fn finish_openai_tools(&mut self) -> Result<Vec<DependencyProviderEvent>, String> {
        let Self::OpenAi(normalizer) = self else {
            return Ok(Vec::new());
        };
        let calls = normalizer.finish()?;
        let mut events = Vec::new();
        for call in calls {
            let call_id = call.call_id.clone();
            events.push(DependencyProviderEvent::ToolCallProposed {
                continuation_reference: call_id.clone(),
                call_id,
                tool: call.name.clone(),
                arguments_json: call.arguments_json.clone(),
            });
        }
        Ok(events)
    }
}

fn failed_event(
    kind: DependencyProviderFailureKind,
    message: impl Into<String>,
    retry: DependencyRetryClassification,
) -> DependencyProviderEvent {
    DependencyProviderEvent::Failed {
        kind,
        message: message.into(),
        retry,
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "the live execution matrix keeps connect, stream, and normalize steps together for auditability"
)]
fn execute_live(
    config: &ProviderEndpointConfig,
    request: &DependencyProviderExecutionRequest,
    cancelled: &Arc<AtomicBool>,
) -> Result<DependencyProviderExecutionResponse, ProviderExecutionDependencyError> {
    let options: BTreeMap<_, _> = request
        .options
        .iter()
        .map(|option| (option.key.clone(), option.value.clone()))
        .collect();
    let streaming = options
        .get("streaming")
        .is_none_or(|value| value != "false" && value != "0");
    let body = match config.provider_key.as_str() {
        PROVIDER_OPENAI | PROVIDER_OPENROUTER | PROVIDER_OPENAI_COMPATIBLE | PROVIDER_LOCAL => {
            wire_openai::build_request_body(&request.model_key, &request.entries, &options)
        }
        PROVIDER_ANTHROPIC => {
            wire_anthropic::build_request_body(&request.model_key, &request.entries, &options)
        }
        PROVIDER_GEMINI => {
            wire_gemini::build_request_body(&request.model_key, &request.entries, &options)
        }
        _other => return Err(ProviderExecutionDependencyError::ProviderNotConfigured),
    }
    .map_err(ProviderExecutionDependencyError::InvalidRequest)?;

    let client = reqwest::blocking::Client::builder()
        .timeout(config.timeout)
        .danger_accept_invalid_certs(!config.tls_verify)
        .build()
        .map_err(|error| {
            ProviderExecutionDependencyError::InvalidRequest(format!(
                "provider HTTP client could not be built: {error}"
            ))
        })?;
    let path = endpoint_path(&config.provider_key, &request.model_key)?;
    let url = format!("{}{}", config.base_url.trim_end_matches('/'), path);
    let mut builder = client.post(&url).json(&body);
    match config.provider_key.as_str() {
        PROVIDER_GEMINI => {
            if let Some(key) = &config.api_key {
                builder = builder.header("x-goog-api-key", key);
            }
        }
        PROVIDER_ANTHROPIC => {
            builder = builder.header("anthropic-version", "2023-06-01");
            if let Some(key) = &config.api_key {
                builder = builder.header("x-api-key", key);
            }
        }
        _ => {
            if let Some(key) = &config.api_key {
                builder = builder.header("authorization", format!("Bearer {key}"));
            }
        }
    }
    if cancelled.load(Ordering::SeqCst) {
        return Ok(DependencyProviderExecutionResponse {
            events: vec![DependencyProviderEvent::Cancelled],
        });
    }
    let mut response = match builder.send() {
        Ok(response) => response,
        Err(error) => {
            let classified = retry::classify_pre_dispatch_transport(&error.to_string());
            return Ok(DependencyProviderExecutionResponse {
                events: vec![failed_event(
                    classified.kind,
                    classified.message,
                    classified.retry,
                )],
            });
        }
    };
    let status = response.status();
    if !status.is_success() {
        let headers = response.headers().clone();
        let body = response.text().unwrap_or_default();
        let classified = retry::classify_http_status(status, &headers, &body);
        return Ok(DependencyProviderExecutionResponse {
            events: vec![failed_event(
                classified.kind,
                classified.message,
                classified.retry,
            )],
        });
    }
    let mut events: Vec<DependencyProviderEvent> = vec![DependencyProviderEvent::Started];

    if !streaming {
        return execute_non_streaming(config, request, response, &options, events);
    }
    let mut normalizer = StreamNormalizer::new(&config.provider_key)
        .map_err(ProviderExecutionDependencyError::InvalidRequest)?;
    let mut event_bytes: usize = 0;
    let mut received_any = false;
    let mut stream_parser = sse::SseParser::new();
    let mut saw_stream_error = false;
    // The blocking response read runs on a background thread so a wire
    // cancellation can interrupt an in-flight slow exchange between chunks.
    // This mirrors the async cancellation semantics without introducing a
    // second asynchronous execution model into the dependency layer.
    let (chunk_sender, chunk_receiver) = std::sync::mpsc::channel::<Result<Vec<u8>, String>>();
    std::thread::spawn(move || {
        let mut buffer = [0_u8; 8192];
        loop {
            let read = match response.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => read,
                Err(error) => {
                    let _ = chunk_sender.send(Err(error.to_string()));
                    return;
                }
            };
            if chunk_sender.send(Ok(buffer[..read].to_vec())).is_err() {
                return;
            }
        }
        let _ = chunk_sender.send(Ok(Vec::new()));
    });
    loop {
        if cancelled.load(Ordering::SeqCst) {
            events.push(DependencyProviderEvent::Cancelled);
            return Ok(DependencyProviderExecutionResponse { events });
        }
        let chunk = match chunk_receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(Ok(chunk)) if chunk.is_empty() => break,
            Ok(Ok(chunk)) => chunk,
            Ok(Err(message)) => {
                let classified = if received_any {
                    retry::classify_ambiguous_disconnect(&message)
                } else {
                    retry::classify_pre_dispatch_transport(&message)
                };
                events.push(failed_event(
                    classified.kind,
                    classified.message,
                    classified.retry,
                ));
                return Ok(DependencyProviderExecutionResponse { events });
            }
            Err(_timeout) => continue,
        };
        let parsed = match stream_parser.push(&chunk) {
            Ok(parsed) => parsed,
            Err(sse::SseParseError::Oversized) => {
                events.push(failed_event(
                    DependencyProviderFailureKind::PartialOutputFailure,
                    "provider stream exceeded the bounded frame size",
                    DependencyRetryClassification::Never,
                ));
                return Ok(DependencyProviderExecutionResponse { events });
            }
        };
        for sse_event in parsed {
            if sse_event.data == "[DONE]" {
                // OpenAI-compatible terminal marker; the stream ended normally.
                return Ok(finish_live_stream(
                    config,
                    request,
                    normalizer,
                    events,
                    event_bytes,
                ));
            }
            if sse_event.data.trim().is_empty() {
                continue;
            }
            let value: serde_json::Value = if let Ok(value) = serde_json::from_str(&sse_event.data)
            {
                value
            } else {
                events.push(failed_event(
                    if received_any {
                        DependencyProviderFailureKind::PartialOutputFailure
                    } else {
                        DependencyProviderFailureKind::InvalidRequest
                    },
                    "provider emitted a malformed stream event",
                    DependencyRetryClassification::Never,
                ));
                return Ok(DependencyProviderExecutionResponse { events });
            };
            if let Some(error) = value.get("error") {
                let kind = error
                    .get("type")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let message = error
                    .get("message")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let classified = retry::classify_provider_error(kind, message);
                events.push(failed_event(
                    classified.kind,
                    classified.message,
                    classified.retry,
                ));
                saw_stream_error = true;
                break;
            }
            let emitted = match normalizer.handle(&sse_event.event, &value) {
                Ok(emitted) => emitted,
                Err(message) => {
                    events.push(failed_event(
                        DependencyProviderFailureKind::InvalidRequest,
                        message,
                        DependencyRetryClassification::Never,
                    ));
                    return Ok(DependencyProviderExecutionResponse { events });
                }
            };
            if normalizer.started() {
                received_any = true;
            }
            for event in emitted {
                if let DependencyProviderEvent::TextDelta(text) = &event {
                    event_bytes = event_bytes.saturating_add(text.len());
                }
                events.push(event);
            }
            if events.len() > MAX_STREAM_EVENTS || event_bytes > MAX_EVENT_DELTAS_BYTES {
                events.push(failed_event(
                    DependencyProviderFailureKind::PartialOutputFailure,
                    "provider stream exceeded the bounded event budget",
                    DependencyRetryClassification::Never,
                ));
                return Ok(DependencyProviderExecutionResponse { events });
            }
        }
        if saw_stream_error {
            return Ok(DependencyProviderExecutionResponse { events });
        }
    }
    Ok(finish_live_stream(
        config,
        request,
        normalizer,
        events,
        event_bytes,
    ))
}

#[allow(clippy::needless_pass_by_value)]
fn finish_live_stream(
    config: &ProviderEndpointConfig,
    request: &DependencyProviderExecutionRequest,
    mut normalizer: StreamNormalizer,
    mut events: Vec<DependencyProviderEvent>,
    event_bytes: usize,
) -> DependencyProviderExecutionResponse {
    if events.len() > MAX_STREAM_EVENTS || event_bytes > MAX_EVENT_DELTAS_BYTES {
        events.push(failed_event(
            DependencyProviderFailureKind::PartialOutputFailure,
            "provider stream exceeded the bounded output budget",
            DependencyRetryClassification::Never,
        ));
        return DependencyProviderExecutionResponse { events };
    }
    let finish_reason = normalizer.finish_reason().unwrap_or("stop").to_owned();
    let usage = normalizer.usage().unwrap_or_default();
    let tool_proposals = match normalizer.finish_openai_tools() {
        Ok(proposals) => proposals,
        Err(message) => {
            events.push(failed_event(
                DependencyProviderFailureKind::MalformedToolArguments,
                message,
                DependencyRetryClassification::Never,
            ));
            return DependencyProviderExecutionResponse { events };
        }
    };
    events.extend(tool_proposals);
    let cost = config.pricing.compute(&request.model_key, usage);
    events.push(DependencyProviderEvent::Completed {
        finish_reason,
        usage,
        cost,
    });
    DependencyProviderExecutionResponse { events }
}

#[allow(clippy::needless_pass_by_value)]
fn execute_non_streaming(
    config: &ProviderEndpointConfig,
    request: &DependencyProviderExecutionRequest,
    response: reqwest::blocking::Response,
    _options: &BTreeMap<String, String>,
    mut events: Vec<DependencyProviderEvent>,
) -> Result<DependencyProviderExecutionResponse, ProviderExecutionDependencyError> {
    let mut body = response.text().unwrap_or_default();
    if body.len() > MAX_NON_STREAM_BODY_BYTES {
        body.truncate(MAX_NON_STREAM_BODY_BYTES);
    }
    let value: serde_json::Value = match serde_json::from_str(&body) {
        Ok(value) => value,
        Err(_) => {
            return Ok(DependencyProviderExecutionResponse {
                events: vec![failed_event(
                    DependencyProviderFailureKind::InvalidRequest,
                    "provider returned a malformed non-streaming response",
                    DependencyRetryClassification::Never,
                )],
            });
        }
    };
    let mut normalizer = StreamNormalizer::new(&config.provider_key)
        .map_err(ProviderExecutionDependencyError::InvalidRequest)?;
    match config.provider_key.as_str() {
        PROVIDER_OPENAI | PROVIDER_OPENROUTER | PROVIDER_OPENAI_COMPATIBLE | PROVIDER_LOCAL => {
            // Non-stream chat completion: map choices[].message into chunks.
            let synthesized = synthesize_openai_non_stream(&value)
                .map_err(ProviderExecutionDependencyError::InvalidRequest)?;
            for chunk in synthesized {
                events.extend(
                    normalizer
                        .handle("message", &chunk)
                        .map_err(ProviderExecutionDependencyError::InvalidRequest)?,
                );
            }
        }
        PROVIDER_ANTHROPIC => {
            let synthesized = synthesize_anthropic_non_stream(&value)
                .map_err(ProviderExecutionDependencyError::InvalidRequest)?;
            for (event_type, chunk) in synthesized {
                events.extend(
                    normalizer
                        .handle(&event_type, &chunk)
                        .map_err(ProviderExecutionDependencyError::InvalidRequest)?,
                );
            }
        }
        PROVIDER_GEMINI => {
            events.extend(
                normalizer
                    .handle("message", &value)
                    .map_err(ProviderExecutionDependencyError::InvalidRequest)?,
            );
        }
        _ => {}
    }
    if let Ok(proposals) = normalizer.finish_openai_tools() {
        events.extend(proposals);
    }
    let usage = normalizer.usage().unwrap_or_default();
    let finish_reason = normalizer.finish_reason().unwrap_or("stop").to_owned();
    let cost = config.pricing.compute(&request.model_key, usage);
    events.push(DependencyProviderEvent::Completed {
        finish_reason,
        usage,
        cost,
    });
    Ok(DependencyProviderExecutionResponse { events })
}

fn synthesize_openai_non_stream(
    value: &serde_json::Value,
) -> Result<Vec<serde_json::Value>, String> {
    let mut chunks = Vec::new();
    if let Some(usage) = value.get("usage") {
        chunks.push(serde_json::json!({"usage": usage}));
    }
    let choices = value.get("choices").and_then(serde_json::Value::as_array);
    let Some(choices) = choices else {
        return Err("provider response has no choices".into());
    };
    for choice in choices {
        let mut chunk = serde_json::json!({
            "choices": [{
                "delta": {},
                "finish_reason": choice.get("finish_reason").cloned().unwrap_or(serde_json::json!(null)),
            }]
        });
        if let Some(message) = choice.get("message") {
            if let Some(content) = message.get("content").and_then(serde_json::Value::as_str) {
                chunk["choices"][0]["delta"]["content"] =
                    serde_json::Value::String(content.to_owned());
            }
            if let Some(tool_calls) = message
                .get("tool_calls")
                .and_then(serde_json::Value::as_array)
            {
                let mapped: Vec<serde_json::Value> = tool_calls
                    .iter()
                    .enumerate()
                    .map(|(index, call)| {
                        serde_json::json!({
                            "index": index,
                            "id": call.get("id").cloned().unwrap_or(serde_json::Value::Null),
                            "function": call.get("function").cloned().unwrap_or(serde_json::json!({})),
                        })
                    })
                    .collect();
                chunk["choices"][0]["delta"]["tool_calls"] = serde_json::Value::Array(mapped);
            }
        }
        chunks.push(chunk);
    }
    Ok(chunks)
}

fn synthesize_anthropic_non_stream(
    value: &serde_json::Value,
) -> Result<Vec<(String, serde_json::Value)>, String> {
    let mut events = Vec::new();
    if let Some(usage) = value.get("usage") {
        events.push((
            "message_start".into(),
            serde_json::json!({"message": {"usage": usage}}),
        ));
    }
    let content = value
        .get("content")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "provider response has no content".to_owned())?;
    for (index, block) in content.iter().enumerate() {
        let block_type = block
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if block_type == "text" {
            let text = block
                .get("text")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            events.push((
                "content_block_start".into(),
                serde_json::json!({"index": index, "content_block": {"type": "text", "text": ""}}),
            ));
            events.push((
                "content_block_delta".into(),
                serde_json::json!({"index": index, "delta": {"type": "text_delta", "text": text}}),
            ));
            events.push((
                "content_block_stop".into(),
                serde_json::json!({"index": index}),
            ));
        } else if block_type == "tool_use" {
            events.push((
                "content_block_start".into(),
                serde_json::json!({"index": index, "content_block": block}),
            ));
            if let Some(input) = block.get("input") {
                let fragment = input.to_string();
                events.push((
                    "content_block_delta".into(),
                    serde_json::json!({"index": index, "delta": {"type": "input_json_delta", "partial_json": fragment}}),
                ));
            }
            events.push((
                "content_block_stop".into(),
                serde_json::json!({"index": index}),
            ));
        }
    }
    if let Some(reason) = value.get("stop_reason") {
        events.push((
            "message_delta".into(),
            serde_json::json!({"delta": {"stop_reason": reason}, "usage": value.get("usage").cloned().unwrap_or(serde_json::json!({}))}),
        ));
    }
    events.push(("message_stop".into(), serde_json::json!({})));
    Ok(events)
}

#[cfg(test)]
mod tests {
    use crate::execution::DependencyConversationEntry;

    use super::*;

    fn option(key: &str, value: &str) -> DependencyProviderOption {
        DependencyProviderOption {
            key: key.into(),
            value: value.into(),
        }
    }

    #[test]
    fn development_grant_and_provider_validation() {
        let dependency = LiveProviderCatalogDependency::development();
        assert!(dependency.validate_grant("grant", false).is_ok());
        assert!(dependency.validate_grant("wrong", false).is_err());
    }

    #[test]
    fn local_provider_resolves_without_a_secret() {
        let config = resolve_endpoint_config(
            PROVIDER_LOCAL,
            &[option("base_url", "http://127.0.0.1:9000/v1")],
        )
        .expect("local endpoint");
        assert_eq!(config.base_url, "http://127.0.0.1:9000/v1");
        assert!(config.api_key.is_none());
    }

    #[test]
    fn literal_secret_options_are_rejected() {
        let result = resolve_endpoint_config(
            PROVIDER_OPENAI,
            &[
                option("base_url", "http://127.0.0.1:9000/v1"),
                option("api_key", "sk-literal"),
            ],
        );
        assert!(result.is_err());
    }

    #[test]
    fn transport_failure_is_classified_safe_before_dispatch() {
        let dependency = LiveProviderCatalogDependency::development();
        let response = dependency
            .execute_provider(DependencyProviderExecutionRequest {
                provider_key: PROVIDER_LOCAL.into(),
                model_key: "model".into(),
                entries: vec![DependencyConversationEntry::User("hi".into())],
                options: vec![option("base_url", "http://127.0.0.1:1/v1")],
                authorization_grant: "grant".into(),
                cancellation_reference: "cancel-1".into(),
                resumed_after_continuation: false,
            })
            .expect("execution");
        assert!(matches!(
            response.events.last(),
            Some(DependencyProviderEvent::Failed {
                kind: DependencyProviderFailureKind::TransportFailure,
                retry: DependencyRetryClassification::Immediate,
                ..
            })
        ));
    }

    #[test]
    fn catalog_reports_only_configured_providers() {
        let dependency = LiveProviderCatalogDependency::development();
        let response = dependency
            .probe_catalog(ProviderCatalogProbeRequest {
                include_unavailable: false,
            })
            .expect("catalog");
        let keys: Vec<_> = response
            .providers
            .iter()
            .map(|provider| provider.provider_key.as_str())
            .collect();
        assert!(keys.is_empty(), "no provider is configured without env");
        let detail = dependency
            .probe_catalog_details(ProviderCatalogProbeRequest {
                include_unavailable: true,
            })
            .expect("detail catalog");
        assert_eq!(detail.providers.len(), LIVE_PROVIDER_IDS.len());
    }
}
