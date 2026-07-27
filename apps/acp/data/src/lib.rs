//! ACP business datasets and explicit runtime-dependency normalization.
#![allow(missing_docs, reason = "data-local records are boundary-specific")]
#![allow(
    async_fn_in_trait,
    reason = "the first-party ACP data boundary intentionally uses static async dispatch"
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the data port exposes one closed error taxonomy"
)]

use agentmod_acp_dependency::{AcpDependencyError, AcpRuntimeDependencyPort, DependencyTurnEvent};
use agentmod_primitives::{CancellationId, SessionId};
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionDataRecord {
    pub id: SessionId,
    pub workspace: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TurnDataEvent {
    Started,
    Text(String),
    ToolDelta {
        call_id: String,
        name: String,
        arguments: String,
    },
    ToolProposed {
        continuation_id: String,
        call_id: String,
        tool: String,
        arguments: Value,
    },
    Completed {
        reason: String,
    },
    Cancelled,
    Failed {
        code: String,
        message: String,
        retryable: bool,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct TurnDataRecord {
    pub events: Vec<TurnDataEvent>,
    pub awaiting_continuation: Option<String>,
}

#[async_trait]
pub trait AcpDataPort: Send + Sync {
    async fn create_session(
        &self,
        workspace: String,
        style: String,
    ) -> Result<SessionId, AcpDataError>;
    async fn find_session(
        &self,
        session_id: SessionId,
    ) -> Result<Option<SessionDataRecord>, AcpDataError>;
    async fn run_turn(
        &self,
        session_id: SessionId,
        prompt: String,
        cancellation_id: CancellationId,
    ) -> Result<TurnDataRecord, AcpDataError>;
    async fn resolve_approval(
        &self,
        session_id: SessionId,
        continuation_id: String,
        approved: bool,
    ) -> Result<Vec<TurnDataEvent>, AcpDataError>;
    async fn cancel(
        &self,
        cancellation_id: CancellationId,
        reason: String,
    ) -> Result<(), AcpDataError>;
}

#[derive(Clone, Debug)]
pub struct AcpData<D> {
    dependency: D,
}

impl<D> AcpData<D> {
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

#[async_trait]
impl<D: AcpRuntimeDependencyPort> AcpDataPort for AcpData<D> {
    async fn create_session(
        &self,
        workspace: String,
        style: String,
    ) -> Result<SessionId, AcpDataError> {
        self.dependency
            .create_session(workspace, style)
            .await
            .map_err(map_error)
    }

    async fn find_session(
        &self,
        session_id: SessionId,
    ) -> Result<Option<SessionDataRecord>, AcpDataError> {
        self.dependency
            .find_session(session_id)
            .await
            .map(|value| {
                value.map(|session| SessionDataRecord {
                    id: session.id,
                    workspace: session.workspace,
                })
            })
            .map_err(map_error)
    }

    async fn run_turn(
        &self,
        session_id: SessionId,
        prompt: String,
        cancellation_id: CancellationId,
    ) -> Result<TurnDataRecord, AcpDataError> {
        self.dependency
            .run_turn(session_id, prompt, cancellation_id)
            .await
            .map(|turn| TurnDataRecord {
                events: turn.events.into_iter().map(map_event).collect(),
                awaiting_continuation: turn.awaiting_continuation,
            })
            .map_err(map_error)
    }

    async fn resolve_approval(
        &self,
        session_id: SessionId,
        continuation_id: String,
        approved: bool,
    ) -> Result<Vec<TurnDataEvent>, AcpDataError> {
        self.dependency
            .resolve_approval(session_id, continuation_id, approved)
            .await
            .map(|events| events.into_iter().map(map_event).collect())
            .map_err(map_error)
    }

    async fn cancel(
        &self,
        cancellation_id: CancellationId,
        reason: String,
    ) -> Result<(), AcpDataError> {
        self.dependency
            .cancel(cancellation_id, reason)
            .await
            .map_err(map_error)
    }
}

fn map_event(event: DependencyTurnEvent) -> TurnDataEvent {
    match event {
        DependencyTurnEvent::Started => TurnDataEvent::Started,
        DependencyTurnEvent::Text(value) => TurnDataEvent::Text(value),
        DependencyTurnEvent::ToolDelta {
            call_id,
            name,
            arguments,
        } => TurnDataEvent::ToolDelta {
            call_id,
            name,
            arguments,
        },
        DependencyTurnEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        } => TurnDataEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        },
        DependencyTurnEvent::Completed { reason } => TurnDataEvent::Completed { reason },
        DependencyTurnEvent::Cancelled => TurnDataEvent::Cancelled,
        DependencyTurnEvent::Failed {
            code,
            message,
            retryable,
        } => TurnDataEvent::Failed {
            code,
            message,
            retryable,
        },
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err consumes the lower-layer error at this explicit boundary"
)]
fn map_error(error: AcpDependencyError) -> AcpDataError {
    AcpDataError::Dependency(error.to_string())
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum AcpDataError {
    #[error("ACP runtime dependency failed: {0}")]
    Dependency(String),
}
