//! Explicit user-only MCP OAuth management orchestration.

use agentmod_event_model::{
    EventClassification, EventEnvelope, EventMetadata, EventOrigin, EventScope,
};
use agentmod_primitives::{CancellationId, CausationId, ContentHash, SessionId, Version};
use agentmod_runtime_data::mcp_oauth::{
    McpOAuthDataAction, McpOAuthDataError, McpOAuthDataPort, McpOAuthDataRequest,
    McpOAuthDataResult, McpOAuthStatusDataRecord,
};
use agentmod_runtime_data::{
    identity::{AllocateEventIdentityDataRequest, EventIdentityDataPort},
    journal::JournalEventDataPort,
};
use async_trait::async_trait;
use serde_json::json;
use thiserror::Error;

use crate::{
    persistence::{
        CommitDurability, CompareAppendSessionEventCommand, CompareAppendSessionEventResult,
        LoadSessionCommand, SessionPersistenceLogic, SessionPersistenceLogicPort,
    },
    session::{McpOAuthManagementAuditedEvent, RuntimeCommittedEvent},
};

/// Logic-owned management action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpOAuthAction {
    /// Begin discovery and PKCE authorization.
    Begin,
    /// Read redacted status.
    Status,
    /// Cancel the exact pending transaction.
    Cancel {
        /// Opaque transaction.
        transaction_id: String,
    },
}

/// Logic-owned management command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManageMcpOAuthCommand {
    /// Canonical session.
    pub session_id: SessionId,
    /// Exact configured MCP server.
    pub server_id: String,
    /// Explicit user action.
    pub action: McpOAuthAction,
    /// Stable cancellation lineage.
    pub cancellation_id: CancellationId,
}

/// Logic-owned begin result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpOAuthBeginResult {
    /// Exact server.
    pub server_id: String,
    /// Opaque transaction.
    pub transaction_id: String,
    /// Transient authorization URL.
    pub authorization_url: String,
    /// Hash retained for redacted audit correlation.
    pub authorization_url_hash: ContentHash,
    /// Transaction expiry.
    pub expires_at_ms: i64,
    /// Stable hash of the exact configured OAuth server binding.
    pub configuration_hash: ContentHash,
}

/// Logic-owned redacted status result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpOAuthStatusResult {
    /// Exact server.
    pub server_id: String,
    /// Stable redacted state.
    pub status: String,
    /// Opaque pending transaction.
    pub transaction_id: Option<String>,
    /// Transaction or token expiry.
    pub expires_at_ms: Option<i64>,
    /// Granted non-secret scopes.
    pub scopes: Vec<String>,
    /// Hash of the complete redacted result.
    pub status_hash: ContentHash,
    /// Stable hash of the exact configured OAuth server binding.
    pub configuration_hash: ContentHash,
}

/// Logic-owned management result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpOAuthResult {
    /// Authorization started.
    Begin(McpOAuthBeginResult),
    /// Redacted status/cancellation result.
    Status(McpOAuthStatusResult),
}

/// Dedicated management logic port. This is intentionally separate from tool execution.
#[async_trait]
pub trait McpOAuthLogicPort: Send + Sync {
    /// Executes one explicit user management action.
    async fn manage_mcp_oauth(
        &self,
        command: ManageMcpOAuthCommand,
    ) -> Result<McpOAuthResult, McpOAuthError>;
}

/// MCP OAuth management logic.
#[derive(Clone)]
pub struct McpOAuthLogic<D> {
    data: D,
}

impl<D> McpOAuthLogic<D> {
    /// Injects the dedicated data adapter.
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}

#[async_trait]
impl<D> McpOAuthLogicPort for McpOAuthLogic<D>
where
    D: Clone + McpOAuthDataPort + JournalEventDataPort + EventIdentityDataPort,
{
    #[allow(
        clippy::too_many_lines,
        reason = "the user-only route validates, redacts, hashes, and canonically audits both exact result variants"
    )]
    async fn manage_mcp_oauth(
        &self,
        command: ManageMcpOAuthCommand,
    ) -> Result<McpOAuthResult, McpOAuthError> {
        validate_component(&command.server_id)?;
        let session_directory = self
            .data
            .session_directory(&command.session_id.to_string())
            .map_err(|error| map_data_error(&error))?;
        let (action_name, requested_transaction, action) = match command.action {
            McpOAuthAction::Begin => ("begin", None, McpOAuthDataAction::Begin),
            McpOAuthAction::Status => ("status", None, McpOAuthDataAction::Status),
            McpOAuthAction::Cancel { transaction_id } => {
                validate_transaction(&transaction_id)?;
                (
                    "cancel",
                    Some(transaction_id.clone()),
                    McpOAuthDataAction::Cancel { transaction_id },
                )
            }
        };
        let request_hash = hash_json(&json!({
            "session_id": command.session_id,
            "server_id": &command.server_id,
            "action": action_name,
            "transaction_id": &requested_transaction,
            "cancellation_id": command.cancellation_id,
        }))?;
        let result = self
            .data
            .manage_oauth(McpOAuthDataRequest {
                session_id: command.session_id.to_string(),
                server_id: command.server_id.clone(),
                action,
                cancellation_id: command.cancellation_id.to_string(),
            })
            .await
            .map_err(|error| map_data_error(&error))?;
        let (mapped, audit) = match result {
            McpOAuthDataResult::Begin(value) => {
                validate_component(&value.server_id)?;
                validate_transaction(&value.transaction_id)?;
                if value.expires_at_ms <= 0 {
                    return Err(McpOAuthError::InvalidOutcome);
                }
                let configuration_hash = parse_hash(&value.configuration_hash)?;
                let authorization_url_hash =
                    ContentHash::digest(value.authorization_url.as_bytes());
                let result_hash = hash_json(&json!({
                    "server_id": &value.server_id,
                    "transaction_id": &value.transaction_id,
                    "authorization_url_hash": authorization_url_hash,
                    "expires_at_ms": value.expires_at_ms,
                    "configuration_hash": configuration_hash,
                }))?;
                let transaction_id = value.transaction_id.clone();
                (
                    McpOAuthResult::Begin(McpOAuthBeginResult {
                        server_id: value.server_id,
                        transaction_id: value.transaction_id,
                        authorization_url: value.authorization_url,
                        authorization_url_hash,
                        expires_at_ms: value.expires_at_ms,
                        configuration_hash,
                    }),
                    McpOAuthManagementAuditedEvent {
                        session_id: command.session_id,
                        server_id: command.server_id.clone(),
                        action: action_name.to_owned(),
                        transaction_id: Some(transaction_id),
                        status: "pending".to_owned(),
                        request_hash,
                        configuration_hash,
                        result_hash,
                    },
                )
            }
            McpOAuthDataResult::Status(value) => {
                validate_status(&value)?;
                let configuration_hash = parse_hash(&value.configuration_hash)?;
                let status_hash = serde_json::to_vec(&json!({
                    "server_id": &value.server_id,
                    "status": &value.status,
                    "transaction_id": &value.transaction_id,
                    "expires_at_ms": value.expires_at_ms,
                    "scopes": &value.scopes,
                    "configuration_hash": configuration_hash,
                }))
                .map(|bytes| ContentHash::digest(&bytes))
                .map_err(|_| McpOAuthError::InvalidOutcome)?;
                let audit_transaction =
                    requested_transaction.or_else(|| value.transaction_id.clone());
                let audit_status = value.status.clone();
                (
                    McpOAuthResult::Status(McpOAuthStatusResult {
                        server_id: value.server_id,
                        status: value.status,
                        transaction_id: value.transaction_id,
                        expires_at_ms: value.expires_at_ms,
                        scopes: value.scopes,
                        status_hash,
                        configuration_hash,
                    }),
                    McpOAuthManagementAuditedEvent {
                        session_id: command.session_id,
                        server_id: command.server_id,
                        action: action_name.to_owned(),
                        transaction_id: audit_transaction,
                        status: audit_status,
                        request_hash,
                        configuration_hash,
                        result_hash: status_hash,
                    },
                )
            }
        };
        append_audit(&self.data, &session_directory, command.session_id, &audit)?;
        Ok(mapped)
    }
}

fn parse_hash(value: &str) -> Result<ContentHash, McpOAuthError> {
    value.parse().map_err(|_| McpOAuthError::InvalidOutcome)
}

fn hash_json(value: &serde_json::Value) -> Result<ContentHash, McpOAuthError> {
    serde_json::to_vec(value)
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| McpOAuthError::InvalidOutcome)
}

fn append_audit<D>(
    data: &D,
    session_directory: &std::path::Path,
    session_id: SessionId,
    audit: &McpOAuthManagementAuditedEvent,
) -> Result<(), McpOAuthError>
where
    D: Clone + JournalEventDataPort + EventIdentityDataPort,
{
    let persistence = SessionPersistenceLogic::new(data.clone());
    for _ in 0..8 {
        let loaded = persistence
            .load_session(LoadSessionCommand {
                session_directory: session_directory.to_owned(),
                expected_session_id: session_id,
            })
            .map_err(|_| McpOAuthError::Operation)?;
        let sequence = loaded
            .state
            .last_sequence
            .checked_next()
            .map_err(|_| McpOAuthError::Operation)?;
        let identity = data
            .allocate_event_identity(AllocateEventIdentityDataRequest)
            .map_err(|_| McpOAuthError::Operation)?;
        let payload = RuntimeCommittedEvent::McpOAuthManagementAudited(audit.clone());
        let event = EventEnvelope::seal(
            EventMetadata {
                event_id: identity.event_id,
                scope: EventScope::Session(session_id),
                sequence,
                timestamp: identity.timestamp,
                event_type: payload.event_type().to_owned(),
                event_version: Version::new(1, 0),
                correlation_id: identity.correlation_id,
                causation_id: CausationId::from_uuid(loaded.last_event_id.into_uuid()),
                parent_graph_node_id: None,
                origin: EventOrigin {
                    subsystem: "runtime".to_owned(),
                    plugin: None,
                },
                schema_version: Version::new(1, 0),
                artifacts: Vec::new(),
                classification: EventClassification::Committed,
            },
            payload,
        )
        .map_err(|_| McpOAuthError::Operation)?;
        match persistence
            .compare_append_event(CompareAppendSessionEventCommand {
                session_directory: session_directory.to_owned(),
                expected_head_event_id: loaded.last_event_id,
                event,
                durability: CommitDurability::Data,
            })
            .map_err(|_| McpOAuthError::Operation)?
        {
            CompareAppendSessionEventResult::Appended(appended)
                if appended.event_id == identity.event_id && appended.sequence == sequence =>
            {
                return Ok(());
            }
            CompareAppendSessionEventResult::Conflict => {}
            CompareAppendSessionEventResult::Appended(_) => {
                return Err(McpOAuthError::Operation);
            }
        }
    }
    Err(McpOAuthError::Operation)
}

fn validate_component(value: &str) -> Result<(), McpOAuthError> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Err(McpOAuthError::InvalidCommand)
    } else {
        Ok(())
    }
}

fn validate_transaction(value: &str) -> Result<(), McpOAuthError> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Err(McpOAuthError::InvalidCommand)
    } else {
        Ok(())
    }
}

fn validate_status(value: &McpOAuthStatusDataRecord) -> Result<(), McpOAuthError> {
    validate_component(&value.server_id)?;
    if !matches!(
        value.status.as_str(),
        "unauthorized" | "pending" | "authorized" | "failed"
    ) || value
        .transaction_id
        .as_deref()
        .is_some_and(|transaction| validate_transaction(transaction).is_err())
        || value.scopes.len() > 64
        || value
            .scopes
            .iter()
            .any(|scope| scope.is_empty() || scope.len() > 256)
    {
        return Err(McpOAuthError::InvalidOutcome);
    }
    Ok(())
}

fn map_data_error(error: &McpOAuthDataError) -> McpOAuthError {
    match error {
        McpOAuthDataError::Invalid => McpOAuthError::InvalidCommand,
        McpOAuthDataError::Timeout => McpOAuthError::Timeout,
        McpOAuthDataError::Protocol => McpOAuthError::InvalidOutcome,
        McpOAuthDataError::Operation => McpOAuthError::Operation,
    }
}

/// Stable management failures.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum McpOAuthError {
    /// User command is invalid.
    #[error("MCP OAuth management command is invalid")]
    InvalidCommand,
    /// Host operation timed out.
    #[error("MCP OAuth management timed out")]
    Timeout,
    /// Host returned an unsafe or malformed projection.
    #[error("MCP OAuth management outcome is invalid")]
    InvalidOutcome,
    /// Host operation failed.
    #[error("MCP OAuth management failed")]
    Operation,
}
