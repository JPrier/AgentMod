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

use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, Mutex},
};

use agentmod_acp_data::{
    AcpDataError, AcpDataPort, TurnDataEvent, TurnDataStream, TurnDataStreamItem,
};
use agentmod_primitives::{CancellationId, SessionId};
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;
use tokio::sync::mpsc;
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
pub enum PromptStreamItem {
    Update(PromptUpdate),
    Approval(ApprovalRequired),
    Complete,
    Cancelled,
}

pub struct PromptStream {
    pub session_id: String,
    receiver: mpsc::Receiver<Result<PromptStreamItem, AcpLogicError>>,
}

impl PromptStream {
    pub async fn recv(&mut self) -> Option<Result<PromptStreamItem, AcpLogicError>> {
        self.receiver.recv().await
    }
}

#[async_trait]
pub trait AcpLogicPort: Send + Sync {
    async fn create_session(&self, command: CreateSessionCommand) -> Result<String, AcpLogicError>;
    async fn load_session(&self, command: LoadSessionCommand) -> Result<(), AcpLogicError>;
    async fn prompt_stream(&self, command: PromptCommand) -> Result<PromptStream, AcpLogicError>;
    async fn resolve_approval(
        &self,
        session_id: String,
        approval: ApprovalRequired,
        approved: bool,
        resume_after_resolution: bool,
    ) -> Result<Vec<PromptUpdate>, AcpLogicError>;
    async fn cancel_session(&self, session_id: String) -> Result<(), AcpLogicError>;
}

pub struct AcpLogic<D> {
    data: D,
    active: Arc<Mutex<HashMap<SessionId, CancellationId>>>,
}

impl<D> AcpLogic<D> {
    #[must_use]
    pub fn new(data: D) -> Self {
        Self {
            data,
            active: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl<D: AcpDataPort + Clone + 'static> AcpLogicPort for AcpLogic<D> {
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

    async fn prompt_stream(&self, command: PromptCommand) -> Result<PromptStream, AcpLogicError> {
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
            .run_turn_stream(session_id, prompt, cancellation_id)
            .await
            .map_err(|error| {
                remove_active(&self.active, session_id, cancellation_id);
                map_error(error)
            })?;
        let receiver = spawn_prompt_forwarder(
            turn,
            self.data.clone(),
            Arc::clone(&self.active),
            session_id,
            cancellation_id,
        );
        Ok(PromptStream {
            session_id: session_id.to_string(),
            receiver,
        })
    }

    async fn resolve_approval(
        &self,
        session_id: String,
        approval: ApprovalRequired,
        approved: bool,
        resume_after_resolution: bool,
    ) -> Result<Vec<PromptUpdate>, AcpLogicError> {
        let session_id = parse_session(&session_id)?;
        self.data
            .resolve_approval(
                session_id,
                approval.continuation_id,
                approved,
                resume_after_resolution,
            )
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

fn spawn_prompt_forwarder<D: AcpDataPort + Clone + 'static>(
    mut turn: TurnDataStream,
    data: D,
    active: Arc<Mutex<HashMap<SessionId, CancellationId>>>,
    session_id: SessionId,
    cancellation_id: CancellationId,
) -> mpsc::Receiver<Result<PromptStreamItem, AcpLogicError>> {
    let (sender, receiver) = mpsc::channel(1);
    tokio::spawn(async move {
        forward_prompt_stream(
            &mut turn,
            &data,
            &active,
            session_id,
            cancellation_id,
            &sender,
        )
        .await;
    });
    receiver
}

async fn forward_prompt_stream<D: AcpDataPort>(
    turn: &mut TurnDataStream,
    data: &D,
    active: &Mutex<HashMap<SessionId, CancellationId>>,
    session_id: SessionId,
    cancellation_id: CancellationId,
    sender: &mpsc::Sender<Result<PromptStreamItem, AcpLogicError>>,
) {
    let mut pending_approval = None;
    while let Some(item) = turn.recv().await {
        let item = match item {
            Ok(item) => item,
            Err(error) => {
                let _ = sender.send(Err(map_error(error))).await;
                remove_active(active, session_id, cancellation_id);
                return;
            }
        };
        let outgoing = match item {
            TurnDataStreamItem::Event(TurnDataEvent::Text(value)) => {
                Some(PromptStreamItem::Update(PromptUpdate::Text(value)))
            }
            TurnDataStreamItem::Event(TurnDataEvent::ToolProposed {
                continuation_id,
                call_id,
                tool,
                arguments,
            }) => {
                pending_approval = Some(ApprovalRequired {
                    continuation_id,
                    call_id: call_id.clone(),
                    tool: tool.clone(),
                    arguments: arguments.clone(),
                });
                Some(PromptStreamItem::Update(PromptUpdate::ToolCall {
                    call_id,
                    name: tool,
                    arguments,
                }))
            }
            TurnDataStreamItem::Event(TurnDataEvent::Cancelled) => {
                remove_active(active, session_id, cancellation_id);
                if sender.send(Ok(PromptStreamItem::Cancelled)).await.is_err() {
                    cancel_disconnected(data, cancellation_id).await;
                }
                return;
            }
            TurnDataStreamItem::Event(TurnDataEvent::Failed { code, message, .. }) => {
                Some(PromptStreamItem::Update(PromptUpdate::Failure {
                    code,
                    message,
                }))
            }
            TurnDataStreamItem::Complete {
                awaiting_continuation,
            } => {
                let terminal = match awaiting_continuation {
                    Some(continuation_id) => match pending_approval {
                        Some(mut approval) => {
                            approval.continuation_id = continuation_id;
                            Ok(PromptStreamItem::Approval(approval))
                        }
                        _ => Err(AcpLogicError::InvalidRuntimeResult),
                    },
                    None => Ok(PromptStreamItem::Complete),
                };
                remove_active(active, session_id, cancellation_id);
                if sender.send(terminal).await.is_err() {
                    cancel_disconnected(data, cancellation_id).await;
                }
                return;
            }
            TurnDataStreamItem::Event(
                TurnDataEvent::Started
                | TurnDataEvent::ToolDelta { .. }
                | TurnDataEvent::Completed { .. },
            ) => None,
        };
        if let Some(outgoing) = outgoing
            && sender.send(Ok(outgoing)).await.is_err()
        {
            cancel_disconnected(data, cancellation_id).await;
            remove_active(active, session_id, cancellation_id);
            return;
        }
    }
    remove_active(active, session_id, cancellation_id);
    let _ = sender.send(Err(AcpLogicError::StreamClosed)).await;
}

fn remove_active(
    active: &Mutex<HashMap<SessionId, CancellationId>>,
    session_id: SessionId,
    cancellation_id: CancellationId,
) {
    if let Ok(mut active) = active.lock()
        && active.get(&session_id) == Some(&cancellation_id)
    {
        active.remove(&session_id);
    }
}

async fn cancel_disconnected<D: AcpDataPort>(data: &D, cancellation_id: CancellationId) {
    let _ = data
        .cancel(
            cancellation_id,
            String::from("ACP client disconnected during an active prompt"),
        )
        .await;
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
    #[error("ACP runtime stream closed without a terminal result")]
    StreamClosed,
    #[error("ACP runtime state is unavailable")]
    StateUnavailable,
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentmod_acp_data::{SessionDataRecord, TurnDataStreamSender};

    #[derive(Clone, Default)]
    struct MockData {
        sender: Arc<Mutex<Option<TurnDataStreamSender>>>,
        cancellations: Arc<Mutex<Vec<CancellationId>>>,
    }

    #[async_trait]
    impl AcpDataPort for MockData {
        async fn create_session(
            &self,
            _workspace: String,
            _style: String,
        ) -> Result<SessionId, AcpDataError> {
            Ok(SessionId::from_uuid(Uuid::now_v7()))
        }

        async fn find_session(
            &self,
            session_id: SessionId,
        ) -> Result<Option<SessionDataRecord>, AcpDataError> {
            Ok(Some(SessionDataRecord {
                id: session_id,
                workspace: String::from("workspace"),
            }))
        }

        async fn run_turn_stream(
            &self,
            _session_id: SessionId,
            _prompt: String,
            _cancellation_id: CancellationId,
        ) -> Result<TurnDataStream, AcpDataError> {
            let (sender, stream) = TurnDataStream::channel(1);
            *self.sender.lock().expect("sender lock") = Some(sender);
            Ok(stream)
        }

        async fn resolve_approval(
            &self,
            _session_id: SessionId,
            _continuation_id: String,
            _approved: bool,
            _resume_after_resolution: bool,
        ) -> Result<Vec<TurnDataEvent>, AcpDataError> {
            Ok(Vec::new())
        }

        async fn cancel(
            &self,
            cancellation_id: CancellationId,
            _reason: String,
        ) -> Result<(), AcpDataError> {
            self.cancellations
                .lock()
                .expect("cancellation lock")
                .push(cancellation_id);
            Ok(())
        }
    }

    #[tokio::test]
    async fn forwards_updates_before_terminal_and_registers_cancellation() {
        let data = MockData::default();
        let logic = AcpLogic::new(data.clone());
        let session_id = SessionId::from_uuid(Uuid::now_v7()).to_string();
        let mut stream = logic
            .prompt_stream(PromptCommand {
                session_id: session_id.clone(),
                parts: vec![PromptPart::Text(String::from("hello"))],
            })
            .await
            .expect("prompt stream");
        let sender = data
            .sender
            .lock()
            .expect("sender lock")
            .clone()
            .expect("stream sender");
        sender
            .send(Ok(TurnDataStreamItem::Event(TurnDataEvent::Text(
                String::from("first"),
            ))))
            .await
            .expect("send text");
        assert_eq!(
            stream.recv().await.expect("update").expect("valid update"),
            PromptStreamItem::Update(PromptUpdate::Text(String::from("first")))
        );
        logic
            .cancel_session(session_id)
            .await
            .expect("active cancellation");
        assert_eq!(
            data.cancellations.lock().expect("cancellation lock").len(),
            1
        );
        sender
            .send(Ok(TurnDataStreamItem::Complete {
                awaiting_continuation: None,
            }))
            .await
            .expect("send completion");
        assert_eq!(
            stream
                .recv()
                .await
                .expect("terminal")
                .expect("valid terminal"),
            PromptStreamItem::Complete
        );
    }

    #[tokio::test]
    async fn rejects_inconsistent_approval_continuation() {
        let data = MockData::default();
        let logic = AcpLogic::new(data.clone());
        let mut stream = logic
            .prompt_stream(PromptCommand {
                session_id: SessionId::from_uuid(Uuid::now_v7()).to_string(),
                parts: vec![PromptPart::Text(String::from("approval"))],
            })
            .await
            .expect("prompt stream");
        let sender = data
            .sender
            .lock()
            .expect("sender lock")
            .clone()
            .expect("stream sender");
        sender
            .send(Ok(TurnDataStreamItem::Complete {
                awaiting_continuation: Some(String::from("missing")),
            }))
            .await
            .expect("send completion");
        assert_eq!(
            stream.recv().await.expect("terminal"),
            Err(AcpLogicError::InvalidRuntimeResult)
        );
    }
}
