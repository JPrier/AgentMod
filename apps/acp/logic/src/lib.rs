//! ACP session and prompt business logic.
#![allow(missing_docs, reason = "logic-local records are boundary-specific")]
#![allow(
    async_fn_in_trait,
    reason = "the first-party ACP logic boundary intentionally uses static async dispatch"
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the logic port exposes one closed error taxonomy"
)]

use std::{collections::HashMap, str::FromStr, sync::Mutex};

use agentmod_acp_data::{AcpDataError, AcpDataPort, TurnDataEvent};
use agentmod_primitives::{CancellationId, SessionId};
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateSessionCommand {
    pub workspace: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadSessionCommand {
    pub session_id: String,
    pub workspace: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PromptPart {
    Text(String),
    ResourceLink { name: String, uri: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromptCommand {
    pub session_id: String,
    pub parts: Vec<PromptPart>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PromptUpdate {
    Text(String),
    ToolCall {
        call_id: String,
        name: String,
        arguments: Value,
    },
    Failure {
        code: String,
        message: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct ApprovalRequired {
    pub continuation_id: String,
    pub call_id: String,
    pub tool: String,
    pub arguments: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PromptResult {
    pub session_id: String,
    pub updates: Vec<PromptUpdate>,
    pub approval: Option<ApprovalRequired>,
    pub cancelled: bool,
}

#[async_trait]
pub trait AcpLogicPort: Send + Sync {
    async fn create_session(&self, command: CreateSessionCommand) -> Result<String, AcpLogicError>;
    async fn load_session(&self, command: LoadSessionCommand) -> Result<(), AcpLogicError>;
    async fn prompt(&self, command: PromptCommand) -> Result<PromptResult, AcpLogicError>;
    async fn resolve_approval(
        &self,
        session_id: String,
        approval: ApprovalRequired,
        approved: bool,
    ) -> Result<Vec<PromptUpdate>, AcpLogicError>;
    async fn cancel_session(&self, session_id: String) -> Result<(), AcpLogicError>;
}

pub struct AcpLogic<D> {
    data: D,
    active: Mutex<HashMap<SessionId, CancellationId>>,
}

impl<D> AcpLogic<D> {
    #[must_use]
    pub fn new(data: D) -> Self {
        Self {
            data,
            active: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl<D: AcpDataPort> AcpLogicPort for AcpLogic<D> {
    async fn create_session(&self, command: CreateSessionCommand) -> Result<String, AcpLogicError> {
        if command.workspace.trim().is_empty() {
            return Err(AcpLogicError::InvalidWorkspace);
        }
        self.data
            .create_session(command.workspace, String::from("persistent-chat"))
            .await
            .map(|value| value.to_string())
            .map_err(map_error)
    }

    async fn load_session(&self, command: LoadSessionCommand) -> Result<(), AcpLogicError> {
        let session_id = parse_session(&command.session_id)?;
        let session = self
            .data
            .find_session(session_id)
            .await
            .map_err(map_error)?
            .ok_or(AcpLogicError::SessionNotFound)?;
        if session.workspace != command.workspace {
            return Err(AcpLogicError::WorkspaceMismatch);
        }
        Ok(())
    }

    async fn prompt(&self, command: PromptCommand) -> Result<PromptResult, AcpLogicError> {
        let session_id = parse_session(&command.session_id)?;
        if command.parts.is_empty() {
            return Err(AcpLogicError::EmptyPrompt);
        }
        let prompt = command
            .parts
            .into_iter()
            .map(|part| match part {
                PromptPart::Text(value) => value,
                PromptPart::ResourceLink { name, uri } => format!("[{name}]({uri})"),
            })
            .collect::<Vec<_>>()
            .join("\n");
        let cancellation_id = CancellationId::from_uuid(Uuid::now_v7());
        {
            let mut active = self
                .active
                .lock()
                .map_err(|_| AcpLogicError::StateUnavailable)?;
            if active.contains_key(&session_id) {
                return Err(AcpLogicError::SessionBusy);
            }
            active.insert(session_id, cancellation_id);
        }
        let turn = self
            .data
            .run_turn(session_id, prompt, cancellation_id)
            .await;
        self.active
            .lock()
            .map_err(|_| AcpLogicError::StateUnavailable)?
            .remove(&session_id);
        let turn = turn.map_err(map_error)?;
        let mut updates = Vec::new();
        let mut approval = None;
        let mut cancelled = false;
        for event in turn.events {
            match event {
                TurnDataEvent::Text(value) => updates.push(PromptUpdate::Text(value)),
                TurnDataEvent::ToolProposed {
                    continuation_id,
                    call_id,
                    tool,
                    arguments,
                } => {
                    updates.push(PromptUpdate::ToolCall {
                        call_id: call_id.clone(),
                        name: tool.clone(),
                        arguments: arguments.clone(),
                    });
                    approval = Some(ApprovalRequired {
                        continuation_id,
                        call_id,
                        tool,
                        arguments,
                    });
                }
                TurnDataEvent::Cancelled => cancelled = true,
                TurnDataEvent::Failed { code, message, .. } => {
                    updates.push(PromptUpdate::Failure { code, message });
                }
                TurnDataEvent::Started
                | TurnDataEvent::ToolDelta { .. }
                | TurnDataEvent::Completed { .. } => {}
            }
        }
        if approval.is_none() && turn.awaiting_continuation.is_some() {
            return Err(AcpLogicError::InvalidRuntimeResult);
        }
        Ok(PromptResult {
            session_id: session_id.to_string(),
            updates,
            approval,
            cancelled,
        })
    }

    async fn resolve_approval(
        &self,
        session_id: String,
        approval: ApprovalRequired,
        approved: bool,
    ) -> Result<Vec<PromptUpdate>, AcpLogicError> {
        let session_id = parse_session(&session_id)?;
        self.data
            .resolve_approval(session_id, approval.continuation_id, approved)
            .await
            .map(|events| {
                events
                    .into_iter()
                    .filter_map(|event| match event {
                        TurnDataEvent::Text(value) => Some(PromptUpdate::Text(value)),
                        TurnDataEvent::Failed { code, message, .. } => {
                            Some(PromptUpdate::Failure { code, message })
                        }
                        _ => None,
                    })
                    .collect()
            })
            .map_err(map_error)
    }

    async fn cancel_session(&self, session_id: String) -> Result<(), AcpLogicError> {
        let session_id = parse_session(&session_id)?;
        let cancellation_id = self
            .active
            .lock()
            .map_err(|_| AcpLogicError::StateUnavailable)?
            .get(&session_id)
            .copied()
            .ok_or(AcpLogicError::NoActiveTurn)?;
        self.data
            .cancel(cancellation_id, String::from("cancelled by ACP client"))
            .await
            .map_err(map_error)
    }
}

fn parse_session(value: &str) -> Result<SessionId, AcpLogicError> {
    SessionId::from_str(value).map_err(|_| AcpLogicError::InvalidSessionId)
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err consumes the lower-layer error at this explicit boundary"
)]
fn map_error(error: AcpDataError) -> AcpLogicError {
    AcpLogicError::Data(error.to_string())
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum AcpLogicError {
    #[error("ACP data operation failed: {0}")]
    Data(String),
    #[error("workspace is invalid")]
    InvalidWorkspace,
    #[error("ACP session identifier is invalid")]
    InvalidSessionId,
    #[error("ACP session was not found")]
    SessionNotFound,
    #[error("ACP session workspace does not match")]
    WorkspaceMismatch,
    #[error("ACP prompt is empty")]
    EmptyPrompt,
    #[error("ACP session already has an active prompt")]
    SessionBusy,
    #[error("ACP session has no active prompt")]
    NoActiveTurn,
    #[error("ACP runtime returned inconsistent continuation state")]
    InvalidRuntimeResult,
    #[error("ACP runtime state is unavailable")]
    StateUnavailable,
}
