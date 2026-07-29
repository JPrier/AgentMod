//! TUI interaction state and runtime use cases.
#![allow(
    missing_docs,
    reason = "logic-local frontend records are boundary-specific"
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the logic port exposes one documented closed error taxonomy"
)]

use agentmod_primitives::{CancellationId, Sequence};
use agentmod_tui_data::{
    SessionDataRecord, SessionEventDataRecord, StyleDataAvailability, StyleDataRecord,
    StyleDataSourceKind, TuiDataError, TuiDataPort, TurnDataEvent, TurnDataStream,
    TurnDataStreamItem,
};
use serde_json::{Value, json};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum View {
    Chat,
    Events,
    Context,
    Styles,
    Help,
}

/// Logic-owned style provenance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StyleSourceKind {
    BuiltIn,
    User,
    Project,
    Plugin,
    Inline,
}

/// Logic-owned style selection availability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StyleAvailability {
    Available,
    Disabled,
    Invalid,
    Incompatible,
    Conflict,
}

/// Logic-owned bounded style catalog row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StyleSummary {
    pub id: String,
    pub version: String,
    pub source: StyleSourceKind,
    pub availability: StyleAvailability,
    pub style_content_hash: String,
    pub compiled_cache_key: String,
    pub required_capabilities: Vec<String>,
}

impl StyleSummary {
    #[must_use]
    pub fn selector(&self) -> String {
        format!("{}@{}", self.id, self.version)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TranscriptRole {
    System,
    User,
    Assistant,
    Tool,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TranscriptEntry {
    pub role: TranscriptRole,
    pub text: String,
    pub sequence: Option<Sequence>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EventTimelineEntry {
    pub sequence: Sequence,
    pub event_type: String,
    pub summary: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ApprovalPrompt {
    pub continuation_id: String,
    pub call_id: String,
    pub tool: String,
    pub arguments: Value,
}

pub struct TuiState {
    pub runtime_ready: bool,
    pub runtime_version: String,
    pub sessions: Vec<SessionDataRecord>,
    pub styles: Vec<StyleSummary>,
    pub selected_style: Option<String>,
    pub default_style: String,
    pub selected_session: Option<usize>,
    pub transcript: Vec<TranscriptEntry>,
    pub timeline: Vec<EventTimelineEntry>,
    pub editor: String,
    pub editor_cursor: usize,
    pub history: Vec<String>,
    pub history_cursor: Option<usize>,
    pub provider: String,
    pub model: String,
    pub view: View,
    pub status: String,
    pub approval: Option<ApprovalPrompt>,
    pub active_cancellation: Option<CancellationId>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub should_quit: bool,
    stream: Option<TurnDataStream>,
}

impl Default for TuiState {
    fn default() -> Self {
        Self {
            runtime_ready: false,
            runtime_version: String::new(),
            sessions: Vec::new(),
            styles: Vec::new(),
            selected_style: None,
            default_style: String::from("persistent-chat"),
            selected_session: None,
            transcript: Vec::new(),
            timeline: Vec::new(),
            editor: String::new(),
            editor_cursor: 0,
            history: Vec::new(),
            history_cursor: None,
            provider: String::from("deterministic-mock"),
            model: String::from("mock-model"),
            view: View::Chat,
            status: String::from("connecting"),
            approval: None,
            active_cancellation: None,
            input_tokens: 0,
            output_tokens: 0,
            should_quit: false,
            stream: None,
        }
    }
}

impl TuiState {
    #[must_use]
    pub fn selected(&self) -> Option<&SessionDataRecord> {
        self.selected_session
            .and_then(|index| self.sessions.get(index))
    }

    #[must_use]
    pub const fn is_streaming(&self) -> bool {
        self.stream.is_some()
    }

    #[must_use]
    pub fn active_style(&self) -> &str {
        self.selected_style
            .as_deref()
            .unwrap_or(self.default_style.as_str())
    }
}

pub trait TuiLogicPort {
    fn state(&self) -> &TuiState;
    fn bootstrap(&mut self) -> Result<(), TuiLogicError>;
    fn refresh_sessions(&mut self) -> Result<(), TuiLogicError>;
    fn refresh_styles(&mut self) -> Result<(), TuiLogicError>;
    fn select_relative(&mut self, delta: i32) -> Result<(), TuiLogicError>;
    fn submit_editor(&mut self) -> Result<(), TuiLogicError>;
    fn poll_runtime(&mut self) -> Result<(), TuiLogicError>;
    fn resolve_approval(&mut self, approved: bool) -> Result<(), TuiLogicError>;
    fn cancel_active(&mut self) -> Result<(), TuiLogicError>;
    fn insert_char(&mut self, value: char);
    fn insert_text(&mut self, value: &str);
    fn insert_newline(&mut self);
    fn backspace(&mut self);
    fn delete(&mut self);
    fn move_cursor(&mut self, delta: i32);
    fn history_relative(&mut self, delta: i32);
    fn set_view(&mut self, view: View);
    fn request_quit(&mut self);
}

pub struct TuiLogic<D> {
    data: D,
    state: TuiState,
}

impl<D> TuiLogic<D> {
    #[must_use]
    pub fn new(data: D) -> Self {
        Self {
            data,
            state: TuiState::default(),
        }
    }
}

impl<D: TuiDataPort> TuiLogicPort for TuiLogic<D> {
    fn state(&self) -> &TuiState {
        &self.state
    }

    fn bootstrap(&mut self) -> Result<(), TuiLogicError> {
        let health = self.data.runtime_health().map_err(map_error)?;
        self.state.runtime_ready = health.ready;
        self.state.runtime_version = health.version;
        self.state.status = if health.ready {
            String::from("runtime ready")
        } else {
            String::from("runtime degraded")
        };
        self.refresh_styles()?;
        self.refresh_sessions()
    }

    fn refresh_sessions(&mut self) -> Result<(), TuiLogicError> {
        let selected_id = self.state.selected().map(|value| value.id);
        self.state.sessions = self.data.list_sessions(500).map_err(map_error)?;
        self.state.selected_session = selected_id
            .and_then(|id| self.state.sessions.iter().position(|value| value.id == id))
            .or_else(|| (!self.state.sessions.is_empty()).then_some(0));
        self.reload_selected_history()
    }

    fn refresh_styles(&mut self) -> Result<(), TuiLogicError> {
        self.state.styles = self
            .data
            .list_styles()
            .map_err(map_error)?
            .into_iter()
            .map(map_style)
            .collect();
        if let Some(selected) = &self.state.selected_style
            && !self
                .state
                .styles
                .iter()
                .any(|style| style.selector() == *selected)
        {
            self.state.selected_style = None;
        }
        Ok(())
    }

    fn select_relative(&mut self, delta: i32) -> Result<(), TuiLogicError> {
        if self.state.sessions.is_empty() {
            return Ok(());
        }
        let current = self.state.selected_session.unwrap_or(0);
        let maximum = self.state.sessions.len().saturating_sub(1);
        let next = if delta.is_negative() {
            current.saturating_sub(delta.unsigned_abs() as usize)
        } else {
            current
                .saturating_add(usize::try_from(delta).unwrap_or(usize::MAX))
                .min(maximum)
        };
        if Some(next) != self.state.selected_session {
            self.state.selected_session = Some(next);
            self.reload_selected_history()?;
        }
        Ok(())
    }

    fn submit_editor(&mut self) -> Result<(), TuiLogicError> {
        let input = self.state.editor.trim().to_owned();
        if input.is_empty() {
            return Ok(());
        }
        self.state.editor.clear();
        self.state.editor_cursor = 0;
        self.state.history.push(input.clone());
        self.state.history_cursor = None;
        if input.starts_with('/') {
            return self.execute_command(&input);
        }
        if self.state.stream.is_some() {
            return Err(TuiLogicError::Busy);
        }
        let session_id = self
            .state
            .selected()
            .map(|value| value.id)
            .ok_or(TuiLogicError::NoSession)?;
        let cancellation_id = CancellationId::from_uuid(Uuid::now_v7());
        self.state.transcript.push(TranscriptEntry {
            role: TranscriptRole::User,
            text: input.clone(),
            sequence: None,
        });
        let stream = self
            .data
            .start_turn(
                session_id,
                input,
                self.state.provider.clone(),
                self.state.model.clone(),
                json!({}),
                cancellation_id,
            )
            .map_err(map_error)?;
        self.state.stream = Some(stream);
        self.state.active_cancellation = Some(cancellation_id);
        self.state.status = String::from("generating");
        Ok(())
    }

    fn poll_runtime(&mut self) -> Result<(), TuiLogicError> {
        loop {
            let next = self
                .state
                .stream
                .as_ref()
                .and_then(TurnDataStream::try_next);
            let Some(next) = next else {
                break;
            };
            match next.map_err(map_error)? {
                TurnDataStreamItem::Event {
                    event,
                    committed_sequence,
                } => self.apply_turn_event(event, committed_sequence),
                TurnDataStreamItem::Complete {
                    first_sequence,
                    last_sequence,
                    awaiting_continuation,
                } => {
                    self.state.stream = None;
                    self.state.active_cancellation = None;
                    self.state.status = awaiting_continuation.as_ref().map_or_else(
                        || {
                            format!(
                                "turn committed {}–{}",
                                first_sequence.get(),
                                last_sequence.get()
                            )
                        },
                        |_| String::from("approval required"),
                    );
                    break;
                }
            }
        }
        Ok(())
    }

    fn resolve_approval(&mut self, approved: bool) -> Result<(), TuiLogicError> {
        let approval = self
            .state
            .approval
            .take()
            .ok_or(TuiLogicError::NoApproval)?;
        let session_id = self
            .state
            .selected()
            .map(|value| value.id)
            .ok_or(TuiLogicError::NoSession)?;
        self.data
            .resolve_approval(session_id, approval.continuation_id, approved)
            .map_err(map_error)?;
        self.reload_selected_history()?;
        self.state.status = if approved {
            String::from("action approved")
        } else {
            String::from("action denied")
        };
        Ok(())
    }

    fn cancel_active(&mut self) -> Result<(), TuiLogicError> {
        let cancellation_id = self
            .state
            .active_cancellation
            .ok_or(TuiLogicError::NotBusy)?;
        self.data
            .cancel(cancellation_id, String::from("cancelled from TUI"))
            .map_err(map_error)?;
        self.state.status = String::from("cancellation requested");
        Ok(())
    }

    fn insert_char(&mut self, value: char) {
        self.state.editor.insert(self.state.editor_cursor, value);
        self.state.editor_cursor += value.len_utf8();
    }

    fn insert_text(&mut self, value: &str) {
        self.state
            .editor
            .insert_str(self.state.editor_cursor, value);
        self.state.editor_cursor += value.len();
    }

    fn insert_newline(&mut self) {
        self.insert_char('\n');
    }

    fn backspace(&mut self) {
        if self.state.editor_cursor == 0 {
            return;
        }
        let previous = self.state.editor[..self.state.editor_cursor]
            .char_indices()
            .next_back()
            .map_or(0, |(index, _)| index);
        self.state.editor.drain(previous..self.state.editor_cursor);
        self.state.editor_cursor = previous;
    }

    fn delete(&mut self) {
        if self.state.editor_cursor >= self.state.editor.len() {
            return;
        }
        let width = self.state.editor[self.state.editor_cursor..]
            .chars()
            .next()
            .map_or(0, char::len_utf8);
        self.state
            .editor
            .drain(self.state.editor_cursor..self.state.editor_cursor + width);
    }

    fn move_cursor(&mut self, delta: i32) {
        if delta.is_negative() {
            if self.state.editor_cursor > 0 {
                self.state.editor_cursor = self.state.editor[..self.state.editor_cursor]
                    .char_indices()
                    .next_back()
                    .map_or(0, |(index, _)| index);
            }
        } else if self.state.editor_cursor < self.state.editor.len() {
            self.state.editor_cursor += self.state.editor[self.state.editor_cursor..]
                .chars()
                .next()
                .map_or(0, char::len_utf8);
        }
    }

    fn history_relative(&mut self, delta: i32) {
        if self.state.history.is_empty() {
            return;
        }
        let current = self
            .state
            .history_cursor
            .unwrap_or(self.state.history.len());
        let maximum = self.state.history.len().saturating_sub(1);
        let next = if delta.is_negative() {
            current.saturating_sub(delta.unsigned_abs() as usize)
        } else {
            current
                .saturating_add(usize::try_from(delta).unwrap_or(usize::MAX))
                .min(maximum)
        };
        self.state.history_cursor = Some(next);
        self.state.editor.clone_from(&self.state.history[next]);
        self.state.editor_cursor = self.state.editor.len();
    }

    fn set_view(&mut self, view: View) {
        self.state.view = view;
    }

    fn request_quit(&mut self) {
        self.state.should_quit = true;
    }
}

impl<D: TuiDataPort> TuiLogic<D> {
    fn execute_command(&mut self, input: &str) -> Result<(), TuiLogicError> {
        let mut parts = input.split_whitespace();
        match parts.next().unwrap_or_default() {
            "/new" => {
                let workspace = parts.next().unwrap_or(".").to_owned();
                let style = parts
                    .next()
                    .map_or_else(|| self.state.active_style().to_owned(), ToOwned::to_owned);
                if parts.next().is_some() {
                    return Err(TuiLogicError::InvalidCommand(String::from(
                        "/new [workspace] [style]",
                    )));
                }
                let id = self
                    .data
                    .create_session(workspace, style.clone())
                    .map_err(map_error)?;
                self.refresh_sessions()?;
                self.state.selected_session =
                    self.state.sessions.iter().position(|value| value.id == id);
                self.reload_selected_history()?;
                self.state.status = format!("created session {id} with {style}");
            }
            "/sessions" => self.refresh_sessions()?,
            "/styles" => {
                self.refresh_styles()?;
                self.state.view = View::Styles;
                self.state.status = format!("{} styles available", self.state.styles.len());
            }
            "/style" => {
                let selector = required_argument(parts.next(), "/style <id[@version]>")?;
                if parts.next().is_some() {
                    return Err(TuiLogicError::InvalidCommand(String::from(
                        "/style <id[@version]>",
                    )));
                }
                let style = self
                    .state
                    .styles
                    .iter()
                    .find(|style| style.selector() == selector || style.id == selector)
                    .ok_or_else(|| TuiLogicError::UnknownStyle(selector.clone()))?;
                if style.availability != StyleAvailability::Available {
                    return Err(TuiLogicError::UnavailableStyle(style.selector()));
                }
                self.state.selected_style = Some(style.selector());
                self.state.status = format!("style: {}", self.state.active_style());
            }
            "/model" => {
                self.state.model = required_argument(parts.next(), "/model <id>")?;
                self.state.status = format!("model: {}", self.state.model);
            }
            "/provider" => {
                self.state.provider = required_argument(parts.next(), "/provider <id>")?;
                self.state.status = format!("provider: {}", self.state.provider);
            }
            "/events" => self.state.view = View::Events,
            "/context" => self.state.view = View::Context,
            "/help" => self.state.view = View::Help,
            "/chat" => self.state.view = View::Chat,
            "/cancel" => self.cancel_active()?,
            "/approve" => self.resolve_approval(true)?,
            "/deny" => self.resolve_approval(false)?,
            "/quit" | "/exit" => self.state.should_quit = true,
            command => return Err(TuiLogicError::UnknownCommand(command.to_owned())),
        }
        Ok(())
    }

    fn reload_selected_history(&mut self) -> Result<(), TuiLogicError> {
        self.state.transcript.clear();
        self.state.timeline.clear();
        let Some(session_id) = self.state.selected().map(|value| value.id) else {
            self.state.status = String::from("no sessions — use /new");
            return Ok(());
        };
        let mut cursor = None;
        loop {
            let page = self
                .data
                .session_events(session_id, cursor, 512)
                .map_err(map_error)?;
            for event in &page.events {
                self.apply_history_event(event);
            }
            cursor = page.cursor;
            if !page.has_more {
                break;
            }
        }
        self.state.status = format!("session {session_id}");
        Ok(())
    }

    fn apply_history_event(&mut self, event: &SessionEventDataRecord) {
        let summary = summarize_payload(&event.payload);
        self.state.timeline.push(EventTimelineEntry {
            sequence: event.sequence,
            event_type: event.event_type.clone(),
            summary,
        });
        if event.event_type == "conversation.entry_committed"
            && let Some(entry) = event
                .payload
                .get("payload")
                .and_then(|value| value.get("entry"))
        {
            let kind = entry
                .get("kind")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let text = entry
                .get("text")
                .and_then(Value::as_str)
                .or_else(|| {
                    entry
                        .get("content")
                        .and_then(|value| value.get("content"))
                        .and_then(Value::as_str)
                })
                .unwrap_or_default();
            let role = match kind {
                "user_message" => TranscriptRole::User,
                "assistant_message" => TranscriptRole::Assistant,
                "tool_result" | "tool_call" => TranscriptRole::Tool,
                _ => TranscriptRole::System,
            };
            if !text.is_empty() {
                self.state.transcript.push(TranscriptEntry {
                    role,
                    text: text.to_owned(),
                    sequence: Some(event.sequence),
                });
            }
        }
    }

    fn apply_turn_event(&mut self, event: TurnDataEvent, sequence: Sequence) {
        match event {
            TurnDataEvent::Started => self.state.status = String::from("model started"),
            TurnDataEvent::Text(text) => {
                if let Some(last) = self.state.transcript.last_mut()
                    && last.role == TranscriptRole::Assistant
                {
                    last.text.push_str(&text);
                    last.sequence = Some(sequence);
                } else {
                    self.state.transcript.push(TranscriptEntry {
                        role: TranscriptRole::Assistant,
                        text,
                        sequence: Some(sequence),
                    });
                }
            }
            TurnDataEvent::ToolDelta { name, .. } => {
                self.state.status = format!("tool call streaming: {name}");
            }
            TurnDataEvent::ToolProposed {
                continuation_id,
                call_id,
                tool,
                arguments,
            } => {
                self.state.approval = Some(ApprovalPrompt {
                    continuation_id,
                    call_id,
                    tool: tool.clone(),
                    arguments,
                });
                self.state.status = format!("approval required: {tool}");
            }
            TurnDataEvent::Completed {
                reason,
                input_tokens,
                output_tokens,
            } => {
                self.state.input_tokens = self.state.input_tokens.saturating_add(input_tokens);
                self.state.output_tokens = self.state.output_tokens.saturating_add(output_tokens);
                self.state.status = format!("completed: {reason}");
            }
            TurnDataEvent::Cancelled => {
                self.state.status = String::from("cancelled");
            }
            TurnDataEvent::Failed { code, message, .. } => {
                self.state.transcript.push(TranscriptEntry {
                    role: TranscriptRole::Error,
                    text: format!("{code}: {message}"),
                    sequence: Some(sequence),
                });
                self.state.status = String::from("turn failed");
            }
        }
    }
}

fn required_argument(value: Option<&str>, usage: &str) -> Result<String, TuiLogicError> {
    value
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| TuiLogicError::InvalidCommand(usage.to_owned()))
}

fn summarize_payload(value: &Value) -> String {
    let rendered = value.to_string();
    if rendered.chars().count() > 96 {
        format!("{}…", rendered.chars().take(95).collect::<String>())
    } else {
        rendered
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err consumes the lower-layer error at this explicit boundary"
)]
fn map_error(error: TuiDataError) -> TuiLogicError {
    TuiLogicError::Data(error.to_string())
}

fn map_style(style: StyleDataRecord) -> StyleSummary {
    StyleSummary {
        id: style.id,
        version: style.version,
        source: match style.source {
            StyleDataSourceKind::BuiltIn => StyleSourceKind::BuiltIn,
            StyleDataSourceKind::User => StyleSourceKind::User,
            StyleDataSourceKind::Project => StyleSourceKind::Project,
            StyleDataSourceKind::Plugin => StyleSourceKind::Plugin,
            StyleDataSourceKind::Inline => StyleSourceKind::Inline,
        },
        availability: match style.availability {
            StyleDataAvailability::Available => StyleAvailability::Available,
            StyleDataAvailability::Disabled => StyleAvailability::Disabled,
            StyleDataAvailability::Invalid => StyleAvailability::Invalid,
            StyleDataAvailability::Incompatible => StyleAvailability::Incompatible,
            StyleDataAvailability::Conflict => StyleAvailability::Conflict,
        },
        style_content_hash: style.style_content_hash,
        compiled_cache_key: style.compiled_cache_key,
        required_capabilities: style.required_capabilities,
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TuiLogicError {
    #[error("TUI runtime data failed: {0}")]
    Data(String),
    #[error("no session is selected")]
    NoSession,
    #[error("a turn is already active")]
    Busy,
    #[error("no turn is active")]
    NotBusy,
    #[error("no approval is pending")]
    NoApproval,
    #[error("unknown command `{0}`")]
    UnknownCommand(String),
    #[error("invalid command; usage: {0}")]
    InvalidCommand(String),
    #[error("unknown style `{0}`")]
    UnknownStyle(String),
    #[error("style `{0}` is not available")]
    UnavailableStyle(String),
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use agentmod_primitives::{CancellationId, SessionId};
    use agentmod_tui_data::{
        RuntimeHealthDataRecord, SessionDataRecord, SessionEventDataRecord,
        SessionEventPageDataRecord, TurnDataEvent, TurnDataStream,
    };
    use serde_json::json;
    use uuid::Uuid;

    use super::*;

    #[derive(Default)]
    struct FixtureData {
        created: RefCell<Vec<(String, String)>>,
    }

    impl TuiDataPort for FixtureData {
        fn runtime_health(&self) -> Result<RuntimeHealthDataRecord, TuiDataError> {
            Ok(RuntimeHealthDataRecord {
                ready: true,
                version: String::from("2.1"),
            })
        }

        fn list_styles(&self) -> Result<Vec<StyleDataRecord>, TuiDataError> {
            Ok(vec![StyleDataRecord {
                id: String::from("focused"),
                version: String::from("1.0.0"),
                source: StyleDataSourceKind::Project,
                availability: StyleDataAvailability::Available,
                style_content_hash: String::from("hash"),
                compiled_cache_key: String::from("cache"),
                required_capabilities: vec![],
            }])
        }

        fn list_sessions(&self, _limit: u32) -> Result<Vec<SessionDataRecord>, TuiDataError> {
            Ok(vec![SessionDataRecord {
                id: SessionId::from_uuid(Uuid::nil()),
                workspace: String::from("workspace"),
                style: String::from("persistent-chat"),
                sequence: Sequence::new(2).expect("valid sequence"),
                state: String::from("active"),
            }])
        }

        fn create_session(
            &self,
            workspace: String,
            style: String,
        ) -> Result<SessionId, TuiDataError> {
            self.created.borrow_mut().push((workspace, style));
            Ok(SessionId::from_uuid(Uuid::from_u128(2)))
        }

        fn session_events(
            &self,
            _session_id: SessionId,
            _after: Option<Sequence>,
            _limit: u32,
        ) -> Result<SessionEventPageDataRecord, TuiDataError> {
            Ok(SessionEventPageDataRecord {
                events: vec![SessionEventDataRecord {
                    sequence: Sequence::new(2).expect("valid sequence"),
                    event_type: String::from("conversation.entry_committed"),
                    payload: json!({
                        "payload": {
                            "entry": {
                                "kind": "assistant_message",
                                "text": "ready"
                            }
                        }
                    }),
                }],
                head_sequence: Sequence::new(2).expect("valid sequence"),
                cursor: Some(Sequence::new(2).expect("valid sequence")),
                has_more: false,
            })
        }

        fn start_turn(
            &self,
            _session_id: SessionId,
            _prompt: String,
            _provider: String,
            _model: String,
            _options: Value,
            _cancellation_id: CancellationId,
        ) -> Result<TurnDataStream, TuiDataError> {
            unreachable!("not used by this fixture")
        }

        fn resolve_approval(
            &self,
            _session_id: SessionId,
            _continuation_id: String,
            _approved: bool,
        ) -> Result<Vec<TurnDataEvent>, TuiDataError> {
            Ok(Vec::new())
        }

        fn cancel(
            &self,
            _cancellation_id: CancellationId,
            _reason: String,
        ) -> Result<(), TuiDataError> {
            Ok(())
        }
    }

    #[test]
    fn bootstrap_reconstructs_visible_state_from_canonical_events() {
        let mut logic = TuiLogic::new(FixtureData::default());

        logic.bootstrap().expect("bootstrap");

        assert!(logic.state().runtime_ready);
        assert_eq!(logic.state().sessions.len(), 1);
        assert_eq!(logic.state().timeline.len(), 1);
        assert_eq!(logic.state().transcript[0].text, "ready");
    }

    #[test]
    fn editor_operations_preserve_utf8_boundaries() {
        let mut logic = TuiLogic::new(FixtureData::default());

        logic.insert_text("a🦀b");
        logic.move_cursor(-1);
        logic.backspace();

        assert_eq!(logic.state().editor, "ab");
        assert_eq!(logic.state().editor_cursor, 1);
        logic.delete();
        assert_eq!(logic.state().editor, "a");
    }

    #[test]
    fn event_summary_truncates_unicode_without_panicking() {
        let summary = summarize_payload(&Value::String("🦀".repeat(120)));

        assert!(summary.ends_with('…'));
        assert!(summary.chars().count() <= 96);
    }

    #[test]
    fn selected_non_default_style_reaches_create_session() {
        let mut logic = TuiLogic::new(FixtureData::default());
        logic.bootstrap().expect("bootstrap");
        logic.insert_text("/style focused@1.0.0");
        logic.submit_editor().expect("select style");
        logic.insert_text("/new workspace");
        logic.submit_editor().expect("create session");

        assert_eq!(
            logic.state().selected_style.as_deref(),
            Some("focused@1.0.0")
        );
        assert_eq!(
            logic.data.created.into_inner(),
            vec![(String::from("workspace"), String::from("focused@1.0.0"))]
        );
    }
}
