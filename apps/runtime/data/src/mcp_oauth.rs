//! Dedicated MCP OAuth management datasets.

use std::{path::PathBuf, sync::Arc};

use agentmod_runtime_dependency::journal::JsonlJournalDependency;
use agentmod_runtime_dependency::tool::{
    DependencyToolCommand, DependencyToolEvent, ToolHostDependencyError, ToolHostDependencyPort,
};
use async_trait::async_trait;
use serde_json::{Value, json};
use thiserror::Error;

use crate::{
    RuntimeData,
    identity::{
        AllocateEventIdentityDataRequest, EventIdentityDataError, EventIdentityDataPort,
        EventIdentityDataRecord,
    },
    journal::{
        AppendEventDataRequest, AppendedEventDataRecord, JournalDataError, JournalEventDataPort,
        RecoverJournalDataRequest, RecoveredJournalDataRecord, ScanEventsDataRequest,
        ScannedEventsDataRecord,
    },
};

/// Data-owned OAuth management action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpOAuthDataAction {
    /// Begin discovery and PKCE authorization.
    Begin,
    /// Read redacted state.
    Status,
    /// Cancel an exact pending transaction.
    Cancel {
        /// Opaque pending transaction.
        transaction_id: String,
    },
}

/// Data-owned OAuth management request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpOAuthDataRequest {
    /// Canonical session owning the MCP host.
    pub session_id: String,
    /// Exact configured MCP server.
    pub server_id: String,
    /// Explicit user management action.
    pub action: McpOAuthDataAction,
    /// Stable cancellation lineage.
    pub cancellation_id: String,
}

/// Data-owned begin result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpOAuthBeginDataRecord {
    /// Exact server.
    pub server_id: String,
    /// Opaque transaction.
    pub transaction_id: String,
    /// Transient user authorization URL.
    pub authorization_url: String,
    /// Transaction expiry.
    pub expires_at_ms: i64,
    /// Stable hash of the exact configured OAuth server binding.
    pub configuration_hash: String,
}

/// Data-owned redacted status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpOAuthStatusDataRecord {
    /// Exact server.
    pub server_id: String,
    /// `unauthorized`, `pending`, `authorized`, or `failed`.
    pub status: String,
    /// Opaque pending transaction.
    pub transaction_id: Option<String>,
    /// Transaction or token expiry.
    pub expires_at_ms: Option<i64>,
    /// Non-secret granted scopes.
    pub scopes: Vec<String>,
    /// Stable hash of the exact configured OAuth server binding.
    pub configuration_hash: String,
}

/// Data-owned OAuth management result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpOAuthDataResult {
    /// Begin result with transient URL.
    Begin(McpOAuthBeginDataRecord),
    /// Redacted status/cancellation result.
    Status(McpOAuthStatusDataRecord),
}

/// Narrow management data port.
#[async_trait]
pub trait McpOAuthDataPort: Send + Sync {
    /// Resolves the canonical directory for one exact session.
    ///
    /// # Errors
    ///
    /// Returns a data-owned invalid-request error when the session identity is
    /// not canonical.
    fn session_directory(&self, session_id: &str) -> Result<PathBuf, McpOAuthDataError>;

    /// Executes one explicitly user-authorized management action.
    async fn manage_oauth(
        &self,
        request: McpOAuthDataRequest,
    ) -> Result<McpOAuthDataResult, McpOAuthDataError>;
}

/// Dedicated MCP management data adapter.
#[derive(Clone)]
pub struct RuntimeMcpOAuthData {
    dependency: Arc<dyn ToolHostDependencyPort>,
    session_root: PathBuf,
    journal: RuntimeData<JsonlJournalDependency>,
}

impl RuntimeMcpOAuthData {
    /// Injects the MCP host dependency and canonical session root.
    #[must_use]
    pub fn new(
        dependency: Arc<dyn ToolHostDependencyPort>,
        session_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            dependency,
            session_root: session_root.into(),
            journal: RuntimeData::new(JsonlJournalDependency),
        }
    }
}

impl JournalEventDataPort for RuntimeMcpOAuthData {
    fn append_event(
        &self,
        request: AppendEventDataRequest,
    ) -> Result<AppendedEventDataRecord, JournalDataError> {
        self.journal.append_event(request)
    }

    fn scan_events(
        &self,
        request: ScanEventsDataRequest,
    ) -> Result<ScannedEventsDataRecord, JournalDataError> {
        self.journal.scan_events(request)
    }

    fn recover_journal(
        &self,
        request: RecoverJournalDataRequest,
    ) -> Result<RecoveredJournalDataRecord, JournalDataError> {
        self.journal.recover_journal(request)
    }
}

impl EventIdentityDataPort for RuntimeMcpOAuthData {
    fn allocate_event_identity(
        &self,
        _request: AllocateEventIdentityDataRequest,
    ) -> Result<EventIdentityDataRecord, EventIdentityDataError> {
        agentmod_runtime_dependency::identity::allocate()
            .map(|value| EventIdentityDataRecord {
                event_id: value.event_id,
                correlation_id: value.correlation_id,
                causation_id: value.causation_id,
                timestamp: value.timestamp,
            })
            .map_err(|_| EventIdentityDataError::Unavailable)
    }
}

#[async_trait]
impl McpOAuthDataPort for RuntimeMcpOAuthData {
    fn session_directory(&self, session_id: &str) -> Result<PathBuf, McpOAuthDataError> {
        let parsed = uuid::Uuid::parse_str(session_id).map_err(|_| McpOAuthDataError::Invalid)?;
        if parsed.to_string() != session_id {
            return Err(McpOAuthDataError::Invalid);
        }
        Ok(self.session_root.join(session_id))
    }

    async fn manage_oauth(
        &self,
        request: McpOAuthDataRequest,
    ) -> Result<McpOAuthDataResult, McpOAuthDataError> {
        let (tool, arguments) = match &request.action {
            McpOAuthDataAction::Begin => {
                ("mcp.oauth.begin", json!({"server_id": request.server_id}))
            }
            McpOAuthDataAction::Status => {
                ("mcp.oauth.status", json!({"server_id": request.server_id}))
            }
            McpOAuthDataAction::Cancel { transaction_id } => (
                "mcp.oauth.cancel",
                json!({
                    "server_id": request.server_id,
                    "transaction_id": transaction_id,
                }),
            ),
        };
        let call_id = format!("mcp-oauth-{}", uuid::Uuid::now_v7());
        let events = self
            .dependency
            .execute(DependencyToolCommand {
                execution_id: call_id.clone(),
                receipt_only: false,
                session_id: request.session_id.clone(),
                workspace: self.session_root.join(&request.session_id),
                call_id: call_id.clone(),
                tool: tool.to_owned(),
                arguments,
                cancellation_id: request.cancellation_id,
                workspace_authorization: None,
            })
            .await
            .map_err(|error| map_dependency_error(&error))?;
        let result = exact_terminal_result(&events, &call_id)?;
        match request.action {
            McpOAuthDataAction::Begin => parse_begin(result),
            McpOAuthDataAction::Status | McpOAuthDataAction::Cancel { .. } => {
                parse_status(result).map(McpOAuthDataResult::Status)
            }
        }
    }
}

fn exact_terminal_result<'a>(
    events: &'a [DependencyToolEvent],
    call_id: &str,
) -> Result<&'a Value, McpOAuthDataError> {
    if events.len() != 2
        || !matches!(
            &events[0],
            DependencyToolEvent::Started { call_id: started } if started == call_id
        )
    {
        return Err(McpOAuthDataError::Protocol);
    }
    match &events[1] {
        DependencyToolEvent::Completed {
            call_id: completed,
            result,
            artifact: None,
            truncated: false,
        } if completed == call_id => Ok(result),
        DependencyToolEvent::Failed { .. } | DependencyToolEvent::Cancelled { .. } => {
            Err(McpOAuthDataError::Operation)
        }
        _ => Err(McpOAuthDataError::Protocol),
    }
}

fn parse_begin(value: &Value) -> Result<McpOAuthDataResult, McpOAuthDataError> {
    let object = exact_object(
        value,
        &[
            "server_id",
            "transaction_id",
            "authorization_url",
            "expires_at_ms",
            "configuration_hash",
        ],
    )?;
    let authorization_url = required_string(object, "authorization_url", 8_192)?;
    let parsed = url::Url::parse(authorization_url).map_err(|_| McpOAuthDataError::Protocol)?;
    if parsed.scheme() != "https"
        && !(parsed.scheme() == "http"
            && parsed
                .host_str()
                .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1")))
    {
        return Err(McpOAuthDataError::Protocol);
    }
    Ok(McpOAuthDataResult::Begin(McpOAuthBeginDataRecord {
        server_id: required_string(object, "server_id", 64)?.to_owned(),
        transaction_id: required_string(object, "transaction_id", 256)?.to_owned(),
        authorization_url: authorization_url.to_owned(),
        expires_at_ms: object
            .get("expires_at_ms")
            .and_then(Value::as_i64)
            .ok_or(McpOAuthDataError::Protocol)?,
        configuration_hash: required_hash(object, "configuration_hash")?,
    }))
}

fn parse_status(value: &Value) -> Result<McpOAuthStatusDataRecord, McpOAuthDataError> {
    let object = exact_object(
        value,
        &[
            "server_id",
            "status",
            "transaction_id",
            "expires_at_ms",
            "scopes",
            "configuration_hash",
        ],
    )?;
    let status = required_string(object, "status", 32)?;
    if !matches!(status, "unauthorized" | "pending" | "authorized" | "failed") {
        return Err(McpOAuthDataError::Protocol);
    }
    let transaction_id = object
        .get("transaction_id")
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.is_empty() && value.len() <= 256)
                .map(str::to_owned)
                .ok_or(McpOAuthDataError::Protocol)
        })
        .transpose()?;
    let scopes = object
        .get("scopes")
        .and_then(Value::as_array)
        .ok_or(McpOAuthDataError::Protocol)?
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.is_empty() && value.len() <= 256)
                .map(str::to_owned)
                .ok_or(McpOAuthDataError::Protocol)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(McpOAuthStatusDataRecord {
        server_id: required_string(object, "server_id", 64)?.to_owned(),
        status: status.to_owned(),
        transaction_id,
        expires_at_ms: match object.get("expires_at_ms") {
            Some(Value::Null) => None,
            Some(value) => Some(value.as_i64().ok_or(McpOAuthDataError::Protocol)?),
            None => return Err(McpOAuthDataError::Protocol),
        },
        scopes,
        configuration_hash: required_hash(object, "configuration_hash")?,
    })
}

fn required_hash(
    object: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<String, McpOAuthDataError> {
    let value = required_string(object, field, 64)?;
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(McpOAuthDataError::Protocol);
    }
    Ok(value.to_ascii_lowercase())
}

fn exact_object<'a>(
    value: &'a Value,
    fields: &[&str],
) -> Result<&'a serde_json::Map<String, Value>, McpOAuthDataError> {
    let object = value.as_object().ok_or(McpOAuthDataError::Protocol)?;
    if object.len() != fields.len() || object.keys().any(|key| !fields.contains(&key.as_str())) {
        return Err(McpOAuthDataError::Protocol);
    }
    Ok(object)
}

fn required_string<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &str,
    maximum: usize,
) -> Result<&'a str, McpOAuthDataError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= maximum)
        .ok_or(McpOAuthDataError::Protocol)
}

fn map_dependency_error(error: &ToolHostDependencyError) -> McpOAuthDataError {
    eprintln!(
        "{}",
        json!({
            "event": "runtime.mcp_oauth_host_failed",
            "category": match &error {
                ToolHostDependencyError::Timeout => "timeout",
                ToolHostDependencyError::InvalidConfiguration => "invalid_configuration",
                ToolHostDependencyError::InvalidRequest => "invalid_request",
                ToolHostDependencyError::UnsupportedTool => "unsupported_tool",
                ToolHostDependencyError::Unavailable => "unavailable",
                ToolHostDependencyError::Transport => "transport",
                ToolHostDependencyError::Protocol => "protocol",
                ToolHostDependencyError::FrameTooLarge => "frame_too_large",
                ToolHostDependencyError::Authorization => "authorization",
                ToolHostDependencyError::Clock => "clock",
                ToolHostDependencyError::ReceiptStorage => "receipt_storage",
                ToolHostDependencyError::ReceiptCorrupt => "receipt_corrupt",
                ToolHostDependencyError::ReceiptConflict => "receipt_conflict",
                ToolHostDependencyError::ReceiptMissing => "receipt_missing",
            }
        })
    );
    match error {
        ToolHostDependencyError::Timeout => McpOAuthDataError::Timeout,
        ToolHostDependencyError::InvalidRequest
        | ToolHostDependencyError::UnsupportedTool
        | ToolHostDependencyError::Authorization => McpOAuthDataError::Invalid,
        _ => McpOAuthDataError::Operation,
    }
}

/// Stable data-layer failures.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum McpOAuthDataError {
    /// Invalid request.
    #[error("MCP OAuth management request is invalid")]
    Invalid,
    /// Host timed out.
    #[error("MCP OAuth management timed out")]
    Timeout,
    /// Host response violated the redacted contract.
    #[error("MCP OAuth management protocol violation")]
    Protocol,
    /// Host operation failed.
    #[error("MCP OAuth management failed")]
    Operation,
}
