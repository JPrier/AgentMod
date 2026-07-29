//! TUI business datasets and explicit dependency normalization.
#![allow(
    missing_docs,
    reason = "data-local frontend records are boundary-specific"
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the data port exposes one documented closed error taxonomy"
)]

use agentmod_primitives::{CancellationId, Sequence, SessionId};
use agentmod_tui_dependency::{
    DependencyBranchSessionRequest, DependencyStyleAvailability, DependencyStyleSourceKind,
    DependencyTurnEvent, DependencyTurnStream, DependencyTurnStreamItem, TuiDependencyError,
    TuiRuntimeDependencyPort,
};
use serde_json::Value;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeHealthDataRecord {
    pub ready: bool,
    pub version: String,
}

/// Data-owned style provenance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StyleDataSourceKind {
    BuiltIn,
    User,
    Project,
    Plugin,
    Inline,
}

/// Data-owned style selection availability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StyleDataAvailability {
    Available,
    Disabled,
    Invalid,
    Incompatible,
    Conflict,
}

/// Data-owned bounded style catalog row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StyleDataRecord {
    pub id: String,
    pub version: String,
    pub source: StyleDataSourceKind,
    pub availability: StyleDataAvailability,
    pub style_content_hash: String,
    pub compiled_cache_key: String,
    pub required_capabilities: Vec<String>,
}

/// Data-owned harness descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarnessDataRecord {
    pub id: String,
    pub version: String,
    pub capabilities: Vec<String>,
    pub capability_set_hash: String,
    pub availability: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionDataRecord {
    pub id: SessionId,
    pub workspace: String,
    pub style: String,
    pub sequence: Sequence,
    pub state: String,
}

/// Data-owned atomic branch request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchSessionDataRequest {
    pub parent_session_id: SessionId,
    pub at: Sequence,
    pub style: Option<String>,
}

/// Data-owned atomic branch record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchSessionDataRecord {
    pub session_id: SessionId,
    pub parent_session_id: SessionId,
    pub fork_sequence: Sequence,
    pub child_head_sequence: Sequence,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionEventDataRecord {
    pub sequence: Sequence,
    pub event_type: String,
    pub payload: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionEventPageDataRecord {
    pub events: Vec<SessionEventDataRecord>,
    pub head_sequence: Sequence,
    pub cursor: Option<Sequence>,
    pub has_more: bool,
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
        input_tokens: u64,
        output_tokens: u64,
    },
    Cancelled,
    Failed {
        code: String,
        message: String,
        retryable: bool,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum TurnDataStreamItem {
    Event {
        event: TurnDataEvent,
        committed_sequence: Sequence,
    },
    Complete {
        first_sequence: Sequence,
        last_sequence: Sequence,
        awaiting_continuation: Option<String>,
    },
}

pub struct TurnDataStream {
    dependency: DependencyTurnStream,
}

impl TurnDataStream {
    #[must_use]
    pub fn try_next(&self) -> Option<Result<TurnDataStreamItem, TuiDataError>> {
        self.dependency.try_next().map(|value| {
            value
                .map(map_stream_item)
                .map_err(|error| TuiDataError::Dependency(error.to_string()))
        })
    }
}

pub trait TuiDataPort {
    fn runtime_health(&self) -> Result<RuntimeHealthDataRecord, TuiDataError>;
    fn list_styles(&self) -> Result<Vec<StyleDataRecord>, TuiDataError>;
    fn list_harnesses(&self) -> Result<Vec<HarnessDataRecord>, TuiDataError> {
        Ok(Vec::new())
    }
    fn list_sessions(&self, limit: u32) -> Result<Vec<SessionDataRecord>, TuiDataError>;
    fn inspect_session(&self, _session_id: SessionId) -> Result<Value, TuiDataError> {
        Ok(Value::Null)
    }
    fn create_session(&self, workspace: String, style: String) -> Result<SessionId, TuiDataError>;
    fn create_session_with_harness(
        &self,
        workspace: String,
        style: String,
        harness: Option<String>,
    ) -> Result<SessionId, TuiDataError> {
        let _ = harness;
        self.create_session(workspace, style)
    }
    fn branch_session(
        &self,
        request: BranchSessionDataRequest,
    ) -> Result<BranchSessionDataRecord, TuiDataError>;
    fn session_events(
        &self,
        session_id: SessionId,
        after: Option<Sequence>,
        limit: u32,
    ) -> Result<SessionEventPageDataRecord, TuiDataError>;
    fn start_turn(
        &self,
        session_id: SessionId,
        prompt: String,
        provider: String,
        model: String,
        options: Value,
        cancellation_id: CancellationId,
    ) -> Result<TurnDataStream, TuiDataError>;
    fn resolve_approval(
        &self,
        session_id: SessionId,
        continuation_id: String,
        approved: bool,
    ) -> Result<Vec<TurnDataEvent>, TuiDataError>;
    fn cancel(&self, cancellation_id: CancellationId, reason: String) -> Result<(), TuiDataError>;
}

#[derive(Clone, Debug)]
pub struct TuiData<D> {
    dependency: D,
}

impl<D> TuiData<D> {
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

impl<D: TuiRuntimeDependencyPort> TuiDataPort for TuiData<D> {
    fn runtime_health(&self) -> Result<RuntimeHealthDataRecord, TuiDataError> {
        self.dependency
            .health()
            .map(|value| RuntimeHealthDataRecord {
                ready: value.status == "ok",
                version: value.version,
            })
            .map_err(map_error)
    }

    fn list_styles(&self) -> Result<Vec<StyleDataRecord>, TuiDataError> {
        self.dependency
            .list_styles()
            .map(|styles| {
                styles
                    .into_iter()
                    .map(|style| StyleDataRecord {
                        id: style.id,
                        version: style.version,
                        source: match style.source {
                            DependencyStyleSourceKind::BuiltIn => StyleDataSourceKind::BuiltIn,
                            DependencyStyleSourceKind::User => StyleDataSourceKind::User,
                            DependencyStyleSourceKind::Project => StyleDataSourceKind::Project,
                            DependencyStyleSourceKind::Plugin => StyleDataSourceKind::Plugin,
                            DependencyStyleSourceKind::Inline => StyleDataSourceKind::Inline,
                        },
                        availability: match style.availability {
                            DependencyStyleAvailability::Available => {
                                StyleDataAvailability::Available
                            }
                            DependencyStyleAvailability::Disabled => {
                                StyleDataAvailability::Disabled
                            }
                            DependencyStyleAvailability::Invalid => StyleDataAvailability::Invalid,
                            DependencyStyleAvailability::Incompatible => {
                                StyleDataAvailability::Incompatible
                            }
                            DependencyStyleAvailability::Conflict => {
                                StyleDataAvailability::Conflict
                            }
                        },
                        style_content_hash: style.style_content_hash,
                        compiled_cache_key: style.compiled_cache_key,
                        required_capabilities: style.required_capabilities,
                    })
                    .collect()
            })
            .map_err(map_error)
    }

    fn list_harnesses(&self) -> Result<Vec<HarnessDataRecord>, TuiDataError> {
        self.dependency
            .list_harnesses()
            .map(|values| {
                values
                    .into_iter()
                    .map(|value| HarnessDataRecord {
                        id: value.id,
                        version: value.version,
                        capabilities: value.capabilities,
                        capability_set_hash: value.capability_set_hash,
                        availability: value.availability,
                    })
                    .collect()
            })
            .map_err(map_error)
    }

    fn list_sessions(&self, limit: u32) -> Result<Vec<SessionDataRecord>, TuiDataError> {
        self.dependency
            .list_sessions(limit)
            .map(|values| {
                values
                    .into_iter()
                    .map(|value| SessionDataRecord {
                        id: value.id,
                        workspace: value.workspace,
                        style: value.style,
                        sequence: value.sequence,
                        state: value.state,
                    })
                    .collect()
            })
            .map_err(map_error)
    }

    fn inspect_session(&self, session_id: SessionId) -> Result<Value, TuiDataError> {
        self.dependency
            .inspect_session(session_id)
            .map_err(map_error)
    }

    fn create_session(&self, workspace: String, style: String) -> Result<SessionId, TuiDataError> {
        self.dependency
            .create_session(workspace, style)
            .map_err(map_error)
    }

    fn create_session_with_harness(
        &self,
        workspace: String,
        style: String,
        harness: Option<String>,
    ) -> Result<SessionId, TuiDataError> {
        self.dependency
            .create_session_with_harness(workspace, style, harness)
            .map_err(map_error)
    }

    fn branch_session(
        &self,
        request: BranchSessionDataRequest,
    ) -> Result<BranchSessionDataRecord, TuiDataError> {
        self.dependency
            .branch_session(DependencyBranchSessionRequest {
                parent_session_id: request.parent_session_id,
                at: request.at,
                style: request.style,
            })
            .map(|response| BranchSessionDataRecord {
                session_id: response.session_id,
                parent_session_id: response.parent_session_id,
                fork_sequence: response.fork_sequence,
                child_head_sequence: response.child_head_sequence,
            })
            .map_err(map_error)
    }

    fn session_events(
        &self,
        session_id: SessionId,
        after: Option<Sequence>,
        limit: u32,
    ) -> Result<SessionEventPageDataRecord, TuiDataError> {
        self.dependency
            .session_events(session_id, after, limit)
            .map(|value| SessionEventPageDataRecord {
                events: value
                    .events
                    .into_iter()
                    .map(|event| SessionEventDataRecord {
                        sequence: event.sequence,
                        event_type: event.event_type,
                        payload: event.payload,
                    })
                    .collect(),
                head_sequence: value.head_sequence,
                cursor: value.last_delivered_sequence,
                has_more: value.has_more,
            })
            .map_err(map_error)
    }

    fn start_turn(
        &self,
        session_id: SessionId,
        prompt: String,
        provider: String,
        model: String,
        options: Value,
        cancellation_id: CancellationId,
    ) -> Result<TurnDataStream, TuiDataError> {
        self.dependency
            .start_turn(
                session_id,
                prompt,
                provider,
                model,
                options,
                cancellation_id,
            )
            .map(|dependency| TurnDataStream { dependency })
            .map_err(map_error)
    }

    fn resolve_approval(
        &self,
        session_id: SessionId,
        continuation_id: String,
        approved: bool,
    ) -> Result<Vec<TurnDataEvent>, TuiDataError> {
        self.dependency
            .resolve_approval(session_id, continuation_id, approved)
            .map(|values| values.into_iter().map(map_event).collect())
            .map_err(map_error)
    }

    fn cancel(&self, cancellation_id: CancellationId, reason: String) -> Result<(), TuiDataError> {
        self.dependency
            .cancel(cancellation_id, reason)
            .map_err(map_error)
    }
}

fn map_stream_item(value: DependencyTurnStreamItem) -> TurnDataStreamItem {
    match value {
        DependencyTurnStreamItem::Event {
            event,
            committed_sequence,
        } => TurnDataStreamItem::Event {
            event: map_event(event),
            committed_sequence,
        },
        DependencyTurnStreamItem::Complete {
            first_committed_sequence,
            last_committed_sequence,
            awaiting_continuation,
        } => TurnDataStreamItem::Complete {
            first_sequence: first_committed_sequence,
            last_sequence: last_committed_sequence,
            awaiting_continuation,
        },
    }
}

fn map_event(value: DependencyTurnEvent) -> TurnDataEvent {
    match value {
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
        DependencyTurnEvent::Completed {
            reason,
            input_tokens,
            output_tokens,
        } => TurnDataEvent::Completed {
            reason,
            input_tokens,
            output_tokens,
        },
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
fn map_error(error: TuiDependencyError) -> TuiDataError {
    TuiDataError::Dependency(error.to_string())
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TuiDataError {
    #[error("TUI runtime dependency failed: {0}")]
    Dependency(String),
}
