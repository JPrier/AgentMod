//! TUI interaction state and runtime use cases.
#![allow(
    missing_docs,
    reason = "logic-local frontend records are boundary-specific"
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the logic port exposes one documented closed error taxonomy"
)]

use agentmod_primitives::{CancellationId, Sequence, SessionId};
use agentmod_tui_data::{
    ArtifactResourceDataRecord, AttachmentDataKind, AttachmentDataRecord, BranchSessionDataRecord,
    BranchSessionDataRequest, ChildResourceDataRecord, CreateSessionDataRequest, HarnessDataRecord,
    McpOAuthDataAction, McpOAuthDataRecord, McpOAuthDataRequest, PluginLifecycleDataAction,
    PluginLifecycleDataRecord, PluginLifecycleDataRequest, ProcessResourceDataRecord,
    ScheduleDataPayload, ScheduleDataRecord, ScheduleDataTrigger, SessionBudgetDataRequest,
    SessionDataRecord, SessionEventDataRecord, SessionEventDataStream, StyleDataAvailability,
    StyleDataRecord, StyleDataSourceKind, StyleInspectionDataRecord, TuiDataError, TuiDataPort,
    TurnDataEvent, TurnDataStream, TurnDataStreamItem,
};
use serde_json::{Value, json};
use thiserror::Error;
use uuid::Uuid;

const MAX_TIMELINE_ENTRIES: usize = 4_096;
const MAX_MCP_OAUTH_SERVERS: usize = 256;
const MAX_ATTACHMENTS: usize = 8;
const MAX_ATTACHMENT_BYTES: u64 = 512 * 1024;
const MAX_RICH_PROMPT_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum View {
    Chat,
    Events,
    Context,
    Graph,
    Styles,
    Harnesses,
    Schedules,
    Plugins,
    Mcp,
    RuntimeResources,
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

/// Logic-owned selected style details.
#[derive(Clone, Debug, PartialEq)]
pub struct StyleInspectionDetail {
    pub summary: StyleSummary,
    pub source_locator: String,
    pub manifest: Value,
    pub compiled: Option<Value>,
    pub diagnostics: Vec<StyleInspectionDiagnostic>,
}

/// Logic-owned style validation diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StyleInspectionDiagnostic {
    pub code: String,
    pub path: String,
    pub message: String,
    pub help: String,
}

impl StyleSummary {
    #[must_use]
    pub fn selector(&self) -> String {
        format!("{}@{}", self.id, self.version)
    }
}

/// Logic-owned harness registry row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarnessSummary {
    pub id: String,
    pub version: String,
    pub capabilities: Vec<String>,
    pub capability_set_hash: String,
    pub availability: String,
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

/// Logic-owned canonical plugin lifecycle result shown by the management view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginLifecycleSummary {
    pub plugin_id: String,
    pub plugin_version: String,
    pub state: String,
    pub committed_sequence: Sequence,
    pub replayed: bool,
}

/// Logic-owned durable schedule summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduleSummary {
    pub schedule_id: String,
    pub session_id: agentmod_primitives::SessionId,
    pub trigger: String,
    pub payload: String,
    pub active: bool,
}

/// Logic-owned bounded MCP OAuth state; authorization URLs remain transient.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpOAuthSummary {
    pub server_id: String,
    pub status: String,
    pub transaction_id: Option<String>,
    pub expires_at_ms: Option<i64>,
    pub scopes: Vec<String>,
    pub status_hash: Option<String>,
    pub authorization_url: Option<String>,
    pub authorization_url_hash: Option<String>,
}

/// Logic-owned replay-only artifact row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactResourceSummary {
    pub execution_id: String,
    pub node_id: String,
    pub state: String,
    pub mime_type: String,
    pub byte_size: u64,
    pub artifact_reference: Option<String>,
}

/// Logic-owned replay-only child row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildResourceSummary {
    pub execution_id: String,
    pub task_id: String,
    pub state: String,
    pub child_style: String,
    pub workspace_mode: String,
    pub child_session_id: Option<String>,
    pub summary: Option<String>,
}

/// Logic-owned replay-only process reconciliation row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessResourceSummary {
    pub call_id: String,
    pub process_id: String,
    pub status: Option<String>,
    pub started_at: u64,
    pub completed_at: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttachmentKind {
    Image,
    Audio,
    Blob,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachmentSummary {
    pub name: String,
    pub mime_type: String,
    pub kind: AttachmentKind,
    pub byte_size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingAttachment {
    identity: String,
    name: String,
    uri: String,
    mime_type: String,
    kind: AttachmentKind,
    data_base64: String,
    byte_size: u64,
}

/// Logic-owned deliberate branch request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchSessionCommand {
    pub at: Sequence,
    pub style: Option<String>,
}

/// Logic-owned deliberate branch result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchSessionResult {
    pub session_id: agentmod_primitives::SessionId,
    pub parent_session_id: agentmod_primitives::SessionId,
    pub fork_sequence: Sequence,
    pub child_head_sequence: Sequence,
}

/// Logic-owned optional hard execution-budget selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionBudgetSelection {
    pub max_iterations: u32,
    pub max_steps: u64,
    pub max_tokens: u64,
    pub max_cost_micros: u64,
    pub max_duration_ms: u64,
}

pub struct TuiState {
    pub runtime_ready: bool,
    pub runtime_version: String,
    pub sessions: Vec<SessionDataRecord>,
    pub styles: Vec<StyleSummary>,
    pub selected_style: Option<String>,
    pub selected_style_inspection: Option<StyleInspectionDetail>,
    pub harnesses: Vec<HarnessSummary>,
    pub selected_harness: String,
    pub memory_providers: Vec<String>,
    pub selected_memory: Option<String>,
    pub compaction_strategies: Vec<String>,
    pub selected_compaction: Option<String>,
    pub selected_budgets: Option<SessionBudgetSelection>,
    pub default_style: String,
    pub selected_session: Option<usize>,
    pub transcript: Vec<TranscriptEntry>,
    pub timeline: Vec<EventTimelineEntry>,
    pub style_introspection: Option<Value>,
    pub editor: String,
    pub editor_cursor: usize,
    pub history: Vec<String>,
    pub history_cursor: Option<usize>,
    pub provider: String,
    pub model: String,
    pub view: View,
    pub status: String,
    pub approval: Option<ApprovalPrompt>,
    pub schedules: Vec<ScheduleSummary>,
    pub plugin_lifecycle: Vec<PluginLifecycleSummary>,
    pub mcp_oauth: Vec<McpOAuthSummary>,
    pub artifact_resources: Vec<ArtifactResourceSummary>,
    pub child_resources: Vec<ChildResourceSummary>,
    pub process_resources: Vec<ProcessResourceSummary>,
    pub attachments: Vec<AttachmentSummary>,
    pub active_cancellation: Option<CancellationId>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub should_quit: bool,
    stream: Option<TurnDataStream>,
    subscription: Option<SessionEventDataStream>,
}

impl Default for TuiState {
    fn default() -> Self {
        Self {
            runtime_ready: false,
            runtime_version: String::new(),
            sessions: Vec::new(),
            styles: Vec::new(),
            selected_style: None,
            selected_style_inspection: None,
            harnesses: Vec::new(),
            selected_harness: String::from("native"),
            memory_providers: Vec::new(),
            selected_memory: None,
            compaction_strategies: Vec::new(),
            selected_compaction: None,
            selected_budgets: None,
            default_style: String::from("persistent-chat"),
            selected_session: None,
            transcript: Vec::new(),
            timeline: Vec::new(),
            style_introspection: None,
            editor: String::new(),
            editor_cursor: 0,
            history: Vec::new(),
            history_cursor: None,
            provider: String::from("deterministic-mock"),
            model: String::from("mock-model"),
            view: View::Chat,
            status: String::from("connecting"),
            approval: None,
            schedules: Vec::new(),
            plugin_lifecycle: Vec::new(),
            mcp_oauth: Vec::new(),
            artifact_resources: Vec::new(),
            child_resources: Vec::new(),
            process_resources: Vec::new(),
            attachments: Vec::new(),
            active_cancellation: None,
            input_tokens: 0,
            output_tokens: 0,
            should_quit: false,
            stream: None,
            subscription: None,
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
    fn refresh_harnesses(&mut self) -> Result<(), TuiLogicError>;
    fn refresh_session_components(&mut self) -> Result<(), TuiLogicError>;
    fn refresh_schedules(&mut self) -> Result<(), TuiLogicError>;
    fn refresh_runtime_resources(&mut self) -> Result<(), TuiLogicError>;
    fn select_session_exact(&mut self, session_id: &str) -> Result<(), TuiLogicError>;
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
    pending_attachments: Vec<PendingAttachment>,
}

impl<D> TuiLogic<D> {
    #[must_use]
    pub fn new(data: D) -> Self {
        Self {
            data,
            state: TuiState::default(),
            pending_attachments: Vec::new(),
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
        self.refresh_harnesses()?;
        self.refresh_session_components()?;
        self.refresh_sessions()
    }

    fn refresh_sessions(&mut self) -> Result<(), TuiLogicError> {
        let selected_id = self.state.selected().map(|value| value.id);
        self.state.sessions = self.data.list_sessions(500).map_err(map_error)?;
        let selected_session = selected_id
            .and_then(|id| self.state.sessions.iter().position(|value| value.id == id))
            .or_else(|| (!self.state.sessions.is_empty()).then_some(0));
        let refreshed_id = selected_session.map(|index| self.state.sessions[index].id);
        if selected_id.is_some() && refreshed_id != selected_id {
            self.clear_attachments();
        }
        self.state.selected_session = selected_session;
        self.reload_selected_history()
    }

    fn select_session_exact(&mut self, session_id: &str) -> Result<(), TuiLogicError> {
        let requested = session_id
            .parse::<SessionId>()
            .map_err(|_| TuiLogicError::InvalidSessionId)?;
        let selected = self
            .state
            .sessions
            .iter()
            .position(|session| session.id == requested)
            .ok_or(TuiLogicError::SessionNotFound(requested))?;
        if self.state.selected_session != Some(selected) {
            self.clear_attachments();
            self.state.selected_session = Some(selected);
            self.reload_selected_history()?;
        }
        if self.state.selected().map(|session| session.id) != Some(requested) {
            return Err(TuiLogicError::SessionSelectionMismatch);
        }
        Ok(())
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

    fn refresh_harnesses(&mut self) -> Result<(), TuiLogicError> {
        self.state.harnesses = self
            .data
            .list_harnesses()
            .map_err(map_error)?
            .into_iter()
            .map(map_harness)
            .collect();
        if !self.state.harnesses.is_empty()
            && !self
                .state
                .harnesses
                .iter()
                .any(|harness| harness.id == self.state.selected_harness)
        {
            self.state.selected_harness = self.state.harnesses[0].id.clone();
        }
        Ok(())
    }

    fn refresh_session_components(&mut self) -> Result<(), TuiLogicError> {
        let catalog = self.data.list_session_components().map_err(map_error)?;
        self.state.memory_providers = catalog.memory_providers;
        self.state.compaction_strategies = catalog.compaction_strategies;
        if self
            .state
            .selected_memory
            .as_ref()
            .is_some_and(|id| !self.state.memory_providers.contains(id))
        {
            self.state.selected_memory = None;
        }
        if self
            .state
            .selected_compaction
            .as_ref()
            .is_some_and(|id| !self.state.compaction_strategies.contains(id))
        {
            self.state.selected_compaction = None;
        }
        Ok(())
    }

    fn refresh_schedules(&mut self) -> Result<(), TuiLogicError> {
        self.state.schedules = self
            .data
            .list_schedules(500)
            .map_err(map_error)?
            .into_iter()
            .map(map_schedule)
            .collect();
        Ok(())
    }

    fn refresh_runtime_resources(&mut self) -> Result<(), TuiLogicError> {
        let Some(session_id) = self.state.selected().map(|session| session.id) else {
            self.state.artifact_resources.clear();
            self.state.child_resources.clear();
            self.state.process_resources.clear();
            return Ok(());
        };
        let resources = self
            .data
            .inspect_runtime_resources(session_id)
            .map_err(map_error)?;
        self.state.artifact_resources = resources
            .artifacts
            .into_iter()
            .map(map_artifact_resource)
            .collect();
        self.state.child_resources = resources
            .children
            .into_iter()
            .map(map_child_resource)
            .collect();
        self.state.process_resources = resources
            .processes
            .into_iter()
            .map(map_process_resource)
            .collect();
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
            self.clear_attachments();
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
        let attachment_count = self.pending_attachments.len();
        let prompt = render_submission_prompt(&input, &self.pending_attachments);
        self.clear_attachments();
        let prompt = prompt?;
        let cancellation_id = CancellationId::from_uuid(Uuid::now_v7());
        self.state.transcript.push(TranscriptEntry {
            role: TranscriptRole::User,
            text: if attachment_count == 0 {
                input.clone()
            } else {
                format!("{input}\n[{attachment_count} attachments]")
            },
            sequence: None,
        });
        let stream = self
            .data
            .start_turn(
                session_id,
                prompt,
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
                } => {
                    let refresh_introspection = matches!(
                        &event,
                        TurnDataEvent::Started
                            | TurnDataEvent::ToolProposed { .. }
                            | TurnDataEvent::Completed { .. }
                            | TurnDataEvent::Cancelled
                            | TurnDataEvent::Failed { .. }
                    );
                    self.apply_turn_event(event, committed_sequence);
                    if refresh_introspection {
                        self.refresh_selected_introspection()?;
                    }
                }
                TurnDataStreamItem::Complete {
                    first_sequence,
                    last_sequence,
                    awaiting_continuation,
                } => {
                    self.state.stream = None;
                    self.state.active_cancellation = None;
                    self.refresh_selected_introspection()?;
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
        let mut received_live_event = false;
        loop {
            let next = self
                .state
                .subscription
                .as_ref()
                .and_then(SessionEventDataStream::try_next);
            let Some(next) = next else {
                break;
            };
            match next {
                Ok(event) => {
                    self.apply_history_event(&event);
                    received_live_event = true;
                }
                Err(error) => {
                    self.state.subscription = None;
                    self.state.status = format!("live subscription stopped: {error}");
                    break;
                }
            }
        }
        if received_live_event {
            self.refresh_selected_introspection()?;
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
    fn clear_attachments(&mut self) {
        self.pending_attachments.clear();
        self.state.attachments.clear();
    }

    fn execute_attach_command(
        &mut self,
        parts: &mut std::str::SplitWhitespace<'_>,
    ) -> Result<(), TuiLogicError> {
        const USAGE: &str = "/attach <workspace-relative-path>";
        if self.state.stream.is_some() {
            return Err(TuiLogicError::Busy);
        }
        if self.pending_attachments.len() == MAX_ATTACHMENTS {
            return Err(TuiLogicError::AttachmentLimit);
        }
        let path = parts.collect::<Vec<_>>().join(" ");
        if path.is_empty() {
            return Err(TuiLogicError::InvalidCommand(String::from(USAGE)));
        }
        let workspace = self
            .state
            .selected()
            .map(|session| session.workspace.clone())
            .ok_or(TuiLogicError::NoSession)?;
        let loaded = self
            .data
            .load_attachment(workspace, path)
            .map_err(map_error)?;
        if self
            .pending_attachments
            .iter()
            .any(|attachment| attachment.identity == loaded.identity)
        {
            return Err(TuiLogicError::DuplicateAttachment);
        }
        let total = self
            .pending_attachments
            .iter()
            .map(|attachment| attachment.byte_size)
            .sum::<u64>()
            .saturating_add(loaded.byte_size);
        if total > MAX_ATTACHMENT_BYTES {
            return Err(TuiLogicError::AttachmentBytesLimit);
        }
        let pending = map_pending_attachment(loaded);
        self.state.attachments.push(AttachmentSummary {
            name: pending.name.clone(),
            mime_type: pending.mime_type.clone(),
            kind: pending.kind,
            byte_size: pending.byte_size,
        });
        self.state.status = format!(
            "attached {} ({}, {} bytes); {} pending",
            pending.name,
            pending.mime_type,
            pending.byte_size,
            self.state.attachments.len()
        );
        self.pending_attachments.push(pending);
        Ok(())
    }

    fn execute_attachments_command(
        &mut self,
        parts: &mut std::str::SplitWhitespace<'_>,
    ) -> Result<(), TuiLogicError> {
        if parts.next().is_some() {
            return Err(TuiLogicError::InvalidCommand(String::from("/attachments")));
        }
        self.state.status = if self.state.attachments.is_empty() {
            String::from("attachments: none")
        } else {
            format!(
                "attachments: {}",
                self.state
                    .attachments
                    .iter()
                    .enumerate()
                    .map(|(index, attachment)| format!(
                        "{}:{} ({}, {} bytes)",
                        index + 1,
                        attachment.name,
                        attachment.mime_type,
                        attachment.byte_size
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        Ok(())
    }

    fn execute_attachment_remove_command(
        &mut self,
        parts: &mut std::str::SplitWhitespace<'_>,
    ) -> Result<(), TuiLogicError> {
        const USAGE: &str = "/attachment-remove <one-based-index>";
        let index = required_argument(parts.next(), USAGE)?
            .parse::<usize>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| TuiLogicError::InvalidCommand(String::from(USAGE)))?;
        if parts.next().is_some() || index > self.pending_attachments.len() {
            return Err(TuiLogicError::AttachmentIndex);
        }
        let removed = self.pending_attachments.remove(index - 1);
        self.state.attachments.remove(index - 1);
        self.state.status = format!(
            "removed {}; {} attachments pending",
            removed.name,
            self.pending_attachments.len()
        );
        Ok(())
    }

    fn execute_attachments_clear_command(
        &mut self,
        parts: &mut std::str::SplitWhitespace<'_>,
    ) -> Result<(), TuiLogicError> {
        if parts.next().is_some() {
            return Err(TuiLogicError::InvalidCommand(String::from(
                "/attachments-clear",
            )));
        }
        let count = self.pending_attachments.len();
        self.clear_attachments();
        self.state.status = format!("cleared {count} attachments");
        Ok(())
    }

    fn execute_attachment_palette_command(
        &mut self,
        command: &str,
        parts: &mut std::str::SplitWhitespace<'_>,
    ) -> Result<(), TuiLogicError> {
        match command {
            "/attach" => self.execute_attach_command(parts),
            "/attachments" | "/attachment-list" => self.execute_attachments_command(parts),
            "/attachment-remove" => self.execute_attachment_remove_command(parts),
            "/attachments-clear" => self.execute_attachments_clear_command(parts),
            _ => unreachable!("validated attachment command"),
        }
    }

    fn resolve_style_selector(&self, selector: &str) -> Result<String, TuiLogicError> {
        let style = self
            .state
            .styles
            .iter()
            .find(|style| style.selector() == selector || style.id == selector)
            .ok_or_else(|| TuiLogicError::UnknownStyle(selector.to_owned()))?;
        if style.availability != StyleAvailability::Available {
            return Err(TuiLogicError::UnavailableStyle(style.selector()));
        }
        Ok(style.selector())
    }

    fn branch_selected(
        &mut self,
        command: BranchSessionCommand,
    ) -> Result<BranchSessionResult, TuiLogicError> {
        if self.state.stream.is_some() {
            return Err(TuiLogicError::Busy);
        }
        let parent_session_id = self
            .state
            .selected()
            .map(|session| session.id)
            .ok_or(TuiLogicError::NoSession)?;
        let result = self
            .data
            .branch_session(BranchSessionDataRequest {
                parent_session_id,
                at: command.at,
                style: command.style,
            })
            .map_err(map_error)?;
        self.select_branched_session(&result)?;
        Ok(map_branch_result(&result))
    }

    fn select_branched_session(
        &mut self,
        result: &BranchSessionDataRecord,
    ) -> Result<(), TuiLogicError> {
        self.refresh_sessions()?;
        let selected = self
            .state
            .sessions
            .iter()
            .position(|session| session.id == result.session_id)
            .ok_or(TuiLogicError::BranchedSessionMissing(result.session_id))?;
        self.clear_attachments();
        self.state.selected_session = Some(selected);
        self.reload_selected_history()
    }

    fn execute_branch_command(
        &mut self,
        parts: &mut std::str::SplitWhitespace<'_>,
    ) -> Result<(), TuiLogicError> {
        let at = required_sequence(parts.next(), "/branch <sequence> [style]")?;
        let style = parts.next().map(ToOwned::to_owned);
        if parts.next().is_some() {
            return Err(TuiLogicError::InvalidCommand(String::from(
                "/branch <sequence> [style]",
            )));
        }
        if let Some(selector) = style.as_deref() {
            let _ = self.resolve_style_selector(selector)?;
        }
        let result = self.branch_selected(BranchSessionCommand { at, style })?;
        self.state.status = format!(
            "branched {} from {} at {}",
            result.session_id,
            result.parent_session_id,
            result.fork_sequence.get()
        );
        Ok(())
    }

    fn execute_new_command(
        &mut self,
        parts: &mut std::str::SplitWhitespace<'_>,
    ) -> Result<(), TuiLogicError> {
        let workspace = parts.next().unwrap_or(".").to_owned();
        let style = parts
            .next()
            .map_or_else(|| self.state.active_style().to_owned(), ToOwned::to_owned);
        let harness = parts
            .next()
            .map_or_else(|| self.state.selected_harness.clone(), ToOwned::to_owned);
        let memory = parts
            .next()
            .map(ToOwned::to_owned)
            .or_else(|| self.state.selected_memory.clone());
        let compaction = parts
            .next()
            .map(ToOwned::to_owned)
            .or_else(|| self.state.selected_compaction.clone());
        let budgets = if let Some(max_iterations) = parts.next() {
            const USAGE: &str = "/new [workspace] [style] [harness] [memory] [compaction] [iterations steps tokens cost-micros duration-ms]";
            let max_iterations = max_iterations
                .parse::<u32>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or_else(|| TuiLogicError::InvalidCommand(String::from(USAGE)))?;
            let max_steps = required_positive_u64(parts.next(), USAGE)?;
            let max_tokens = required_positive_u64(parts.next(), USAGE)?;
            let max_cost_micros = required_positive_u64(parts.next(), USAGE)?;
            let max_duration_ms = required_positive_u64(parts.next(), USAGE)?;
            if parts.next().is_some() {
                return Err(TuiLogicError::InvalidCommand(String::from(USAGE)));
            }
            Some(SessionBudgetSelection {
                max_iterations,
                max_steps,
                max_tokens,
                max_cost_micros,
                max_duration_ms,
            })
        } else {
            self.state.selected_budgets
        };
        let id = self
            .data
            .create_session_with_configuration(CreateSessionDataRequest {
                workspace,
                style: style.clone(),
                harness: Some(harness.clone()),
                memory: memory.clone(),
                compaction: compaction.clone(),
                budgets: budgets.map(|budgets| SessionBudgetDataRequest {
                    max_iterations: Some(budgets.max_iterations),
                    max_steps: Some(budgets.max_steps),
                    max_tokens: Some(budgets.max_tokens),
                    max_cost_micros: Some(budgets.max_cost_micros),
                    max_duration_ms: Some(budgets.max_duration_ms),
                }),
            })
            .map_err(map_error)?;
        self.refresh_sessions()?;
        self.clear_attachments();
        self.state.selected_session = self.state.sessions.iter().position(|value| value.id == id);
        self.reload_selected_history()?;
        self.state.status = format!(
            "created session {id} with {style} on {harness}; memory={}; compaction={}; budgets={}",
            memory.as_deref().unwrap_or("style-default"),
            compaction.as_deref().unwrap_or("style-default"),
            budgets.map_or_else(
                || String::from("style-default"),
                |value| format!(
                    "{}/{}/{}/{}/{}",
                    value.max_iterations,
                    value.max_steps,
                    value.max_tokens,
                    value.max_cost_micros,
                    value.max_duration_ms
                )
            )
        );
        Ok(())
    }

    fn execute_budget_command(
        &mut self,
        parts: &mut std::str::SplitWhitespace<'_>,
    ) -> Result<(), TuiLogicError> {
        const USAGE: &str =
            "/budget <style-default|iterations steps tokens cost-micros duration-ms>";
        let first = required_argument(parts.next(), USAGE)?;
        if first == "style-default" {
            if parts.next().is_some() {
                return Err(TuiLogicError::InvalidCommand(String::from(USAGE)));
            }
            self.state.selected_budgets = None;
            self.state.status = String::from("budgets: style-default");
            return Ok(());
        }
        let max_iterations = first
            .parse::<u32>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| TuiLogicError::InvalidCommand(String::from(USAGE)))?;
        let max_steps = required_positive_u64(parts.next(), USAGE)?;
        let max_tokens = required_positive_u64(parts.next(), USAGE)?;
        let max_cost_micros = required_positive_u64(parts.next(), USAGE)?;
        let max_duration_ms = required_positive_u64(parts.next(), USAGE)?;
        if parts.next().is_some() {
            return Err(TuiLogicError::InvalidCommand(String::from(USAGE)));
        }
        self.state.selected_budgets = Some(SessionBudgetSelection {
            max_iterations,
            max_steps,
            max_tokens,
            max_cost_micros,
            max_duration_ms,
        });
        self.state.status = format!(
            "budgets: {max_iterations}/{max_steps}/{max_tokens}/{max_cost_micros}/{max_duration_ms}"
        );
        Ok(())
    }

    fn execute_memory_command(
        &mut self,
        parts: &mut std::str::SplitWhitespace<'_>,
    ) -> Result<(), TuiLogicError> {
        let id = required_argument(parts.next(), "/memory <id|style-default>")?;
        if parts.next().is_some() {
            return Err(TuiLogicError::InvalidCommand(String::from(
                "/memory <id|style-default>",
            )));
        }
        if id == "style-default" {
            self.state.selected_memory = None;
        } else if self.state.memory_providers.contains(&id) {
            self.state.selected_memory = Some(id);
        } else {
            return Err(TuiLogicError::InvalidCommand(format!(
                "unknown memory provider {id}"
            )));
        }
        self.state.status = format!(
            "memory: {}",
            self.state
                .selected_memory
                .as_deref()
                .unwrap_or("style-default")
        );
        Ok(())
    }

    fn execute_compaction_command(
        &mut self,
        parts: &mut std::str::SplitWhitespace<'_>,
    ) -> Result<(), TuiLogicError> {
        let id = required_argument(parts.next(), "/compaction <id|style-default>")?;
        if parts.next().is_some() {
            return Err(TuiLogicError::InvalidCommand(String::from(
                "/compaction <id|style-default>",
            )));
        }
        if id == "style-default" {
            self.state.selected_compaction = None;
        } else if self.state.compaction_strategies.contains(&id) {
            self.state.selected_compaction = Some(id);
        } else {
            return Err(TuiLogicError::InvalidCommand(format!(
                "unknown compaction strategy {id}"
            )));
        }
        self.state.status = format!(
            "compaction: {}",
            self.state
                .selected_compaction
                .as_deref()
                .unwrap_or("style-default")
        );
        Ok(())
    }

    fn execute_plugin_lifecycle_command(
        &mut self,
        action: PluginLifecycleDataAction,
        parts: &mut std::str::SplitWhitespace<'_>,
    ) -> Result<(), TuiLogicError> {
        let usage = match action {
            PluginLifecycleDataAction::Disable => "/plugin-disable <plugin-id>",
            PluginLifecycleDataAction::Enable => "/plugin-enable <plugin-id>",
            PluginLifecycleDataAction::Quarantine => "/plugin-quarantine <plugin-id> <reason-code>",
            PluginLifecycleDataAction::Unquarantine => "/plugin-unquarantine <plugin-id>",
        };
        let plugin_id = required_argument(parts.next(), usage)?;
        let reason_code = if action == PluginLifecycleDataAction::Quarantine {
            Some(required_argument(parts.next(), usage)?)
        } else {
            None
        };
        if parts.next().is_some() {
            return Err(TuiLogicError::InvalidCommand(String::from(usage)));
        }
        let session_id = self
            .state
            .selected()
            .map(|session| session.id)
            .ok_or(TuiLogicError::NoSession)?;
        let changed = self
            .data
            .change_plugin_lifecycle(PluginLifecycleDataRequest {
                session_id,
                plugin_id,
                action,
                reason_code,
                cancellation_id: CancellationId::from_uuid(Uuid::now_v7()),
            })
            .map_err(map_error)?;
        let summary = PluginLifecycleSummary {
            plugin_id: changed.plugin_id.clone(),
            plugin_version: changed.plugin_version.clone(),
            state: changed.state.clone(),
            committed_sequence: changed.committed_sequence,
            replayed: changed.replayed,
        };
        if let Some(existing) = self
            .state
            .plugin_lifecycle
            .iter_mut()
            .find(|existing| existing.plugin_id == summary.plugin_id)
        {
            *existing = summary;
        } else {
            self.state.plugin_lifecycle.push(summary);
            self.state
                .plugin_lifecycle
                .sort_by(|left, right| left.plugin_id.cmp(&right.plugin_id));
        }
        self.refresh_selected_introspection()?;
        self.state.view = View::Plugins;
        self.state.status = format_plugin_lifecycle_status(&changed);
        Ok(())
    }

    fn execute_schedule_command(
        &mut self,
        trigger_kind: &str,
        parts: &mut std::str::SplitWhitespace<'_>,
    ) -> Result<(), TuiLogicError> {
        let usage = match trigger_kind {
            "once" => "/schedule-once <id> <unix-ms> <prompt>",
            "interval" => "/schedule-interval <id> <starts-ms> <every-ms> <prompt>",
            "event" => "/schedule-event <id> <event-type> <prompt>",
            _ => return Err(TuiLogicError::InvalidCommand(String::from("schedule"))),
        };
        let schedule_id = required_argument(parts.next(), usage)?;
        let trigger = match trigger_kind {
            "once" => ScheduleDataTrigger::AtMillis(required_i64(parts.next(), usage)?),
            "interval" => ScheduleDataTrigger::Interval {
                starts_at_ms: required_i64(parts.next(), usage)?,
                every_ms: required_positive_u64(parts.next(), usage)?,
            },
            "event" => ScheduleDataTrigger::RuntimeEvent {
                event_type: required_argument(parts.next(), usage)?,
            },
            _ => unreachable!("validated trigger kind"),
        };
        let prompt = parts.collect::<Vec<_>>().join(" ");
        if prompt.is_empty() || prompt.len() > 64 * 1024 {
            return Err(TuiLogicError::InvalidCommand(String::from(usage)));
        }
        let session = self
            .state
            .selected()
            .cloned()
            .ok_or(TuiLogicError::NoSession)?;
        let stored = self
            .data
            .upsert_schedule(ScheduleDataRecord {
                schedule_id: schedule_id.clone(),
                session_id: session.id,
                idempotency_id: Uuid::now_v7().to_string(),
                style: session.style,
                workspace: session.workspace,
                permission_policy: String::from("interactive"),
                provider: self.state.provider.clone(),
                model: self.state.model.clone(),
                token_budget: 100_000,
                cost_budget_micros: 0,
                trigger,
                payload: ScheduleDataPayload::Prompt { prompt },
                active: true,
            })
            .map_err(map_error)?;
        if stored.schedule_id != schedule_id {
            return Err(TuiLogicError::ScheduleIdentityMismatch);
        }
        self.refresh_schedules()?;
        self.state.view = View::Schedules;
        self.state.status = format!(
            "schedule {} stored{}",
            stored.schedule_id,
            if stored.replayed { " (replayed)" } else { "" }
        );
        Ok(())
    }

    fn execute_schedule_remove_command(
        &mut self,
        parts: &mut std::str::SplitWhitespace<'_>,
    ) -> Result<(), TuiLogicError> {
        const USAGE: &str = "/schedule-remove <id>";
        let schedule_id = required_argument(parts.next(), USAGE)?;
        if parts.next().is_some() {
            return Err(TuiLogicError::InvalidCommand(String::from(USAGE)));
        }
        let existed = self.data.remove_schedule(&schedule_id).map_err(map_error)?;
        self.refresh_schedules()?;
        self.state.view = View::Schedules;
        self.state.status = if existed {
            format!("schedule {schedule_id} removed")
        } else {
            format!("schedule {schedule_id} was absent")
        };
        Ok(())
    }

    fn execute_schedules_command(
        &mut self,
        parts: &mut std::str::SplitWhitespace<'_>,
    ) -> Result<(), TuiLogicError> {
        if parts.next().is_some() {
            return Err(TuiLogicError::InvalidCommand(String::from("/schedules")));
        }
        self.refresh_schedules()?;
        self.state.view = View::Schedules;
        self.state.status = format!("{} schedules", self.state.schedules.len());
        Ok(())
    }

    fn execute_plugins_command(
        &mut self,
        parts: &mut std::str::SplitWhitespace<'_>,
    ) -> Result<(), TuiLogicError> {
        if parts.next().is_some() {
            return Err(TuiLogicError::InvalidCommand(String::from("/plugins")));
        }
        self.refresh_selected_introspection()?;
        self.state.view = View::Plugins;
        Ok(())
    }

    fn execute_mcp_oauth_command(
        &mut self,
        action: &str,
        parts: &mut std::str::SplitWhitespace<'_>,
    ) -> Result<(), TuiLogicError> {
        let usage = match action {
            "begin" => "/mcp-oauth-begin <server-id>",
            "status" => "/mcp-oauth-status <server-id>",
            "cancel" => "/mcp-oauth-cancel <server-id> <transaction-id>",
            _ => return Err(TuiLogicError::InvalidCommand(String::from("MCP OAuth"))),
        };
        let server_id = required_argument(parts.next(), usage)?;
        let requested_transaction = if action == "cancel" {
            Some(required_argument(parts.next(), usage)?)
        } else {
            None
        };
        if parts.next().is_some() {
            return Err(TuiLogicError::InvalidCommand(String::from(usage)));
        }
        let session_id = self
            .state
            .selected()
            .map(|session| session.id)
            .ok_or(TuiLogicError::NoSession)?;
        let result = self
            .data
            .manage_mcp_oauth(McpOAuthDataRequest {
                session_id,
                server_id: server_id.clone(),
                action: match action {
                    "begin" => McpOAuthDataAction::Begin,
                    "status" => McpOAuthDataAction::Status,
                    "cancel" => McpOAuthDataAction::Cancel {
                        transaction_id: requested_transaction
                            .clone()
                            .ok_or_else(|| TuiLogicError::InvalidCommand(String::from(usage)))?,
                    },
                    _ => unreachable!("validated MCP OAuth action"),
                },
                cancellation_id: CancellationId::from_uuid(Uuid::now_v7()),
            })
            .map_err(map_error)?;
        let summary = match result {
            McpOAuthDataRecord::Started {
                server_id: returned_server,
                transaction_id,
                authorization_url,
                authorization_url_hash,
                expires_at_ms,
            } if action == "begin" && returned_server == server_id => McpOAuthSummary {
                server_id: returned_server,
                status: String::from("pending"),
                transaction_id: Some(transaction_id),
                expires_at_ms: Some(expires_at_ms),
                scopes: Vec::new(),
                status_hash: None,
                authorization_url: Some(authorization_url),
                authorization_url_hash: Some(authorization_url_hash),
            },
            McpOAuthDataRecord::Status {
                server_id: returned_server,
                status,
                transaction_id,
                expires_at_ms,
                scopes,
                status_hash,
            } if action != "begin"
                && returned_server == server_id
                && requested_transaction.as_ref().is_none_or(|requested| {
                    transaction_id
                        .as_ref()
                        .is_none_or(|returned| returned == requested)
                }) =>
            {
                McpOAuthSummary {
                    server_id: returned_server,
                    status,
                    transaction_id,
                    expires_at_ms,
                    scopes,
                    status_hash: Some(status_hash),
                    authorization_url: None,
                    authorization_url_hash: None,
                }
            }
            _ => return Err(TuiLogicError::McpOAuthOutcomeMismatch),
        };
        let status = summary.status.clone();
        let transaction = summary.transaction_id.clone();
        self.upsert_mcp_oauth(summary)?;
        self.state.view = View::Mcp;
        self.state.status = format!(
            "MCP OAuth {server_id}: {status}{}",
            transaction.map_or_else(String::new, |value| format!(" ({value})"))
        );
        Ok(())
    }

    fn upsert_mcp_oauth(&mut self, summary: McpOAuthSummary) -> Result<(), TuiLogicError> {
        if let Some(existing) = self
            .state
            .mcp_oauth
            .iter_mut()
            .find(|existing| existing.server_id == summary.server_id)
        {
            *existing = summary;
        } else {
            if self.state.mcp_oauth.len() == MAX_MCP_OAUTH_SERVERS {
                return Err(TuiLogicError::McpOAuthStateLimit);
            }
            self.state.mcp_oauth.push(summary);
            self.state
                .mcp_oauth
                .sort_by(|left, right| left.server_id.cmp(&right.server_id));
        }
        Ok(())
    }

    fn execute_mcp_view_command(
        &mut self,
        parts: &mut std::str::SplitWhitespace<'_>,
    ) -> Result<(), TuiLogicError> {
        if parts.next().is_some() {
            return Err(TuiLogicError::InvalidCommand(String::from("/mcp")));
        }
        self.state.view = View::Mcp;
        Ok(())
    }

    fn execute_runtime_resources_command(
        &mut self,
        parts: &mut std::str::SplitWhitespace<'_>,
    ) -> Result<(), TuiLogicError> {
        if parts.next().is_some() {
            return Err(TuiLogicError::InvalidCommand(String::from("/runtime")));
        }
        self.refresh_runtime_resources()?;
        self.state.view = View::RuntimeResources;
        self.state.status = format!(
            "{} artifacts · {} children · {} process recoveries",
            self.state.artifact_resources.len(),
            self.state.child_resources.len(),
            self.state.process_resources.len()
        );
        Ok(())
    }

    fn execute_command(&mut self, input: &str) -> Result<(), TuiLogicError> {
        let mut parts = input.split_whitespace();
        match parts.next().unwrap_or_default() {
            "/new" => self.execute_new_command(&mut parts)?,
            "/sessions" => self.refresh_sessions()?,
            "/styles" => {
                self.refresh_styles()?;
                self.state.view = View::Styles;
                self.state.status = format!("{} styles available", self.state.styles.len());
            }
            "/harnesses" => {
                self.refresh_harnesses()?;
                self.state.view = View::Harnesses;
                self.state.status = format!("{} harnesses available", self.state.harnesses.len());
            }
            "/harness" => {
                let id = required_argument(parts.next(), "/harness <id>")?;
                if parts.next().is_some() {
                    return Err(TuiLogicError::InvalidCommand(String::from("/harness <id>")));
                }
                let harness = self
                    .state
                    .harnesses
                    .iter()
                    .find(|harness| harness.id == id)
                    .ok_or_else(|| {
                        TuiLogicError::InvalidCommand(format!("unknown harness {id}"))
                    })?;
                if harness.availability != "available" {
                    return Err(TuiLogicError::InvalidCommand(format!(
                        "harness {id} is {}",
                        harness.availability
                    )));
                }
                self.state.selected_harness.clone_from(&id);
                self.state.status = format!("harness: {id}");
            }
            "/memory" => self.execute_memory_command(&mut parts)?,
            "/compaction" => self.execute_compaction_command(&mut parts)?,
            "/budget" => self.execute_budget_command(&mut parts)?,
            "/style" => {
                let selector = required_argument(parts.next(), "/style <id[@version]>")?;
                if parts.next().is_some() {
                    return Err(TuiLogicError::InvalidCommand(String::from(
                        "/style <id[@version]>",
                    )));
                }
                let selector = self.resolve_style_selector(&selector)?;
                self.state.selected_style = Some(selector.clone());
                self.state.selected_style_inspection = Some(map_style_inspection(
                    self.data.inspect_style(selector).map_err(map_error)?,
                ));
                self.state.status = format!("style: {}", self.state.active_style());
                self.state.view = View::Styles;
            }
            "/branch" => self.execute_branch_command(&mut parts)?,
            "/model" => {
                self.state.model = required_argument(parts.next(), "/model <id>")?;
                self.state.status = format!("model: {}", self.state.model);
            }
            "/provider" => {
                self.state.provider = required_argument(parts.next(), "/provider <id>")?;
                self.state.status = format!("provider: {}", self.state.provider);
            }
            command @ ("/events" | "/context" | "/graph" | "/help" | "/chat") => {
                self.state.view = command_view(command);
            }
            command @ ("/attach" | "/attachments" | "/attachment-list" | "/attachment-remove"
            | "/attachments-clear") => {
                self.execute_attachment_palette_command(command, &mut parts)?;
            }
            "/schedules" => self.execute_schedules_command(&mut parts)?,
            "/schedule-once" => self.execute_schedule_command("once", &mut parts)?,
            "/schedule-interval" => self.execute_schedule_command("interval", &mut parts)?,
            "/schedule-event" => self.execute_schedule_command("event", &mut parts)?,
            "/schedule-remove" => self.execute_schedule_remove_command(&mut parts)?,
            "/plugins" => self.execute_plugins_command(&mut parts)?,
            "/plugin-disable" => self
                .execute_plugin_lifecycle_command(PluginLifecycleDataAction::Disable, &mut parts)?,
            "/plugin-enable" => self
                .execute_plugin_lifecycle_command(PluginLifecycleDataAction::Enable, &mut parts)?,
            "/plugin-quarantine" => self.execute_plugin_lifecycle_command(
                PluginLifecycleDataAction::Quarantine,
                &mut parts,
            )?,
            "/plugin-unquarantine" => self.execute_plugin_lifecycle_command(
                PluginLifecycleDataAction::Unquarantine,
                &mut parts,
            )?,
            "/mcp" => self.execute_mcp_view_command(&mut parts)?,
            "/mcp-oauth-begin" => self.execute_mcp_oauth_command("begin", &mut parts)?,
            "/mcp-oauth-status" => self.execute_mcp_oauth_command("status", &mut parts)?,
            "/mcp-oauth-cancel" => self.execute_mcp_oauth_command("cancel", &mut parts)?,
            "/runtime" | "/resources" => self.execute_runtime_resources_command(&mut parts)?,
            "/cancel" => self.cancel_active()?,
            "/approve" => self.resolve_approval(true)?,
            "/deny" => self.resolve_approval(false)?,
            "/quit" | "/exit" => self.state.should_quit = true,
            command => return Err(TuiLogicError::UnknownCommand(command.to_owned())),
        }
        Ok(())
    }

    fn reload_selected_history(&mut self) -> Result<(), TuiLogicError> {
        self.state.subscription = None;
        self.state.transcript.clear();
        self.state.timeline.clear();
        self.state.style_introspection = None;
        self.state.artifact_resources.clear();
        self.state.child_resources.clear();
        self.state.process_resources.clear();
        let Some(session_id) = self.state.selected().map(|value| value.id) else {
            self.state.status = String::from("no sessions — use /new");
            return Ok(());
        };
        self.refresh_selected_introspection()?;
        self.refresh_runtime_resources()?;
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
        self.state.subscription = self
            .data
            .start_session_subscription(session_id, cursor)
            .ok();
        self.state.status = format!("session {session_id}");
        Ok(())
    }

    fn refresh_selected_introspection(&mut self) -> Result<(), TuiLogicError> {
        let Some(session_id) = self.state.selected().map(|value| value.id) else {
            self.state.style_introspection = None;
            return Ok(());
        };
        let inspection = self.data.inspect_session(session_id).map_err(map_error)?;
        self.state.style_introspection = inspection.get("style_introspection").cloned();
        self.synchronize_plugin_lifecycle();
        Ok(())
    }

    fn synchronize_plugin_lifecycle(&mut self) {
        let Some(lifecycle) = self
            .state
            .style_introspection
            .as_ref()
            .and_then(|value| value.pointer("/pipeline/plugin_lifecycle"))
            .and_then(Value::as_object)
        else {
            return;
        };
        let mut summaries = lifecycle
            .iter()
            .filter_map(|(plugin_id, value)| {
                let plugin_version = value.get("plugin_version")?.as_str()?.to_owned();
                let state = value.get("state")?.as_str()?.to_owned();
                let committed_sequence = value
                    .get("changed_at")
                    .and_then(Value::as_u64)
                    .or_else(|| value.get("requested_at").and_then(Value::as_u64))
                    .and_then(|value| Sequence::new(value).ok())?;
                Some(PluginLifecycleSummary {
                    plugin_id: plugin_id.clone(),
                    plugin_version,
                    state,
                    committed_sequence,
                    replayed: true,
                })
            })
            .collect::<Vec<_>>();
        summaries.sort_by(|left, right| left.plugin_id.cmp(&right.plugin_id));
        self.state.plugin_lifecycle = summaries;
    }

    fn apply_history_event(&mut self, event: &SessionEventDataRecord) {
        if self
            .state
            .timeline
            .iter()
            .any(|existing| existing.sequence == event.sequence)
        {
            return;
        }
        let summary = summarize_payload(&event.payload);
        if self.state.timeline.len() == MAX_TIMELINE_ENTRIES {
            self.state.timeline.remove(0);
        }
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
                        .and_then(|value| value.get("text"))
                        .and_then(Value::as_str)
                })
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

fn format_plugin_lifecycle_status(value: &PluginLifecycleDataRecord) -> String {
    let replay = if value.replayed { " (replayed)" } else { "" };
    format!(
        "plugin {}@{} {} at {}{}",
        value.plugin_id,
        value.plugin_version,
        value.state,
        value.committed_sequence.get(),
        replay
    )
}

fn command_view(command: &str) -> View {
    match command {
        "/events" => View::Events,
        "/context" => View::Context,
        "/graph" => View::Graph,
        "/help" => View::Help,
        "/chat" => View::Chat,
        _ => unreachable!("validated view command"),
    }
}

fn map_artifact_resource(value: ArtifactResourceDataRecord) -> ArtifactResourceSummary {
    ArtifactResourceSummary {
        execution_id: value.execution_id,
        node_id: value.node_id,
        state: value.state,
        mime_type: value.mime_type,
        byte_size: value.byte_size,
        artifact_reference: value.artifact_reference,
    }
}

fn map_child_resource(value: ChildResourceDataRecord) -> ChildResourceSummary {
    ChildResourceSummary {
        execution_id: value.execution_id,
        task_id: value.task_id,
        state: value.state,
        child_style: value.child_style,
        workspace_mode: value.workspace_mode,
        child_session_id: value.child_session_id,
        summary: value.summary,
    }
}

fn map_process_resource(value: ProcessResourceDataRecord) -> ProcessResourceSummary {
    ProcessResourceSummary {
        call_id: value.call_id,
        process_id: value.process_id,
        status: value.status,
        started_at: value.started_at,
        completed_at: value.completed_at,
    }
}

fn map_pending_attachment(value: AttachmentDataRecord) -> PendingAttachment {
    PendingAttachment {
        identity: value.identity,
        name: value.name,
        uri: value.uri,
        mime_type: value.mime_type,
        kind: match value.kind {
            AttachmentDataKind::Image => AttachmentKind::Image,
            AttachmentDataKind::Audio => AttachmentKind::Audio,
            AttachmentDataKind::Blob => AttachmentKind::Blob,
        },
        data_base64: value.data_base64,
        byte_size: value.byte_size,
    }
}

fn render_submission_prompt(
    text: &str,
    attachments: &[PendingAttachment],
) -> Result<String, TuiLogicError> {
    if attachments.is_empty() {
        return Ok(text.to_owned());
    }
    let mut blocks = Vec::with_capacity(attachments.len() + 1);
    blocks.push(json!({"type": "text", "text": text}));
    blocks.extend(attachments.iter().map(|attachment| match attachment.kind {
        AttachmentKind::Image => json!({
            "type": "image",
            "data": attachment.data_base64,
            "mime_type": attachment.mime_type,
            "uri": attachment.uri,
        }),
        AttachmentKind::Audio => json!({
            "type": "audio",
            "data": attachment.data_base64,
            "mime_type": attachment.mime_type,
        }),
        AttachmentKind::Blob => json!({
            "type": "resource",
            "resource": {
                "kind": "blob",
                "data": attachment.data_base64,
                "uri": attachment.uri,
                "mime_type": attachment.mime_type,
            },
        }),
    }));
    let prompt = json!({
        "agentmod_acp_content_version": 1,
        "blocks": blocks,
    })
    .to_string();
    if prompt.len() > MAX_RICH_PROMPT_BYTES {
        return Err(TuiLogicError::RichPromptTooLarge);
    }
    Ok(prompt)
}

fn required_argument(value: Option<&str>, usage: &str) -> Result<String, TuiLogicError> {
    value
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| TuiLogicError::InvalidCommand(usage.to_owned()))
}

fn required_sequence(value: Option<&str>, usage: &str) -> Result<Sequence, TuiLogicError> {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .and_then(|value| Sequence::new(value).ok())
        .ok_or_else(|| TuiLogicError::InvalidCommand(usage.to_owned()))
}

fn required_positive_u64(value: Option<&str>, usage: &str) -> Result<u64, TuiLogicError> {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| TuiLogicError::InvalidCommand(usage.to_owned()))
}

fn required_i64(value: Option<&str>, usage: &str) -> Result<i64, TuiLogicError> {
    value
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value >= 0)
        .ok_or_else(|| TuiLogicError::InvalidCommand(usage.to_owned()))
}

fn map_branch_result(record: &BranchSessionDataRecord) -> BranchSessionResult {
    BranchSessionResult {
        session_id: record.session_id,
        parent_session_id: record.parent_session_id,
        fork_sequence: record.fork_sequence,
        child_head_sequence: record.child_head_sequence,
    }
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

fn map_style_inspection(value: StyleInspectionDataRecord) -> StyleInspectionDetail {
    StyleInspectionDetail {
        summary: map_style(value.summary),
        source_locator: value.source_locator,
        manifest: value.manifest,
        compiled: value.compiled,
        diagnostics: value
            .diagnostics
            .into_iter()
            .map(|diagnostic| StyleInspectionDiagnostic {
                code: diagnostic.code,
                path: diagnostic.path,
                message: diagnostic.message,
                help: diagnostic.help,
            })
            .collect(),
    }
}

fn map_harness(harness: HarnessDataRecord) -> HarnessSummary {
    HarnessSummary {
        id: harness.id,
        version: harness.version,
        capabilities: harness.capabilities,
        capability_set_hash: harness.capability_set_hash,
        availability: harness.availability,
    }
}

fn map_schedule(schedule: ScheduleDataRecord) -> ScheduleSummary {
    let trigger = match schedule.trigger {
        ScheduleDataTrigger::AtMillis(value) => format!("at {value}"),
        ScheduleDataTrigger::Interval {
            starts_at_ms,
            every_ms,
        } => format!("from {starts_at_ms} every {every_ms} ms"),
        ScheduleDataTrigger::RuntimeEvent { event_type } => format!("event {event_type}"),
        ScheduleDataTrigger::ProcessOutput {
            process_id,
            contains,
        } => format!("process {process_id} contains {contains}"),
    };
    let payload = match schedule.payload {
        ScheduleDataPayload::Prompt { prompt } => format!("prompt {prompt}"),
        ScheduleDataPayload::Continuation { continuation_id } => {
            format!("continuation {continuation_id}")
        }
        ScheduleDataPayload::GraphTrigger { run_id, node_id } => {
            format!("graph {run_id}/{node_id}")
        }
    };
    ScheduleSummary {
        schedule_id: schedule.schedule_id,
        session_id: schedule.session_id,
        trigger,
        payload,
        active: schedule.active,
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
    #[error("branched session `{0}` is absent from the runtime catalog")]
    BranchedSessionMissing(agentmod_primitives::SessionId),
    #[error("runtime returned a different schedule identity")]
    ScheduleIdentityMismatch,
    #[error("runtime returned a mismatched MCP OAuth action or identity")]
    McpOAuthOutcomeMismatch,
    #[error("the bounded MCP OAuth frontend state is full")]
    McpOAuthStateLimit,
    #[error("at most eight attachments may be pending")]
    AttachmentLimit,
    #[error("the attachment is already pending")]
    DuplicateAttachment,
    #[error("pending attachments exceed the 524288-byte aggregate limit")]
    AttachmentBytesLimit,
    #[error("attachment index is absent or invalid")]
    AttachmentIndex,
    #[error("the rich prompt exceeds the 1048576-byte envelope limit")]
    RichPromptTooLarge,
    #[error("invalid session identifier")]
    InvalidSessionId,
    #[error("session {0} is not present in the canonical session list")]
    SessionNotFound(SessionId),
    #[error("exact session selection did not retain the requested identity")]
    SessionSelectionMismatch,
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use agentmod_primitives::{CancellationId, SessionId};
    use agentmod_tui_data::{
        AttachmentDataKind, AttachmentDataRecord, BranchSessionDataRecord,
        BranchSessionDataRequest, HarnessDataRecord, PluginLifecycleDataRecord,
        PluginLifecycleDataRequest, RuntimeHealthDataRecord, RuntimeResourcesDataRecord,
        ScheduleDataRecord, ScheduleStoreDataRecord, SessionComponentDataRecord, SessionDataRecord,
        SessionEventDataRecord, SessionEventPageDataRecord, TurnDataEvent, TurnDataStream,
    };
    use serde_json::json;
    use uuid::Uuid;

    use super::*;

    #[derive(Debug, Eq, PartialEq)]
    struct CreatedSessionSelection {
        workspace: String,
        style: String,
        harness: Option<String>,
        memory: Option<String>,
        compaction: Option<String>,
        budgets: Option<SessionBudgetDataRequest>,
    }

    #[derive(Default)]
    struct FixtureData {
        created: RefCell<Vec<CreatedSessionSelection>>,
        branched: RefCell<Vec<BranchSessionDataRequest>>,
        plugin_changes: RefCell<Vec<PluginLifecycleDataRequest>>,
        mcp_oauth_requests: RefCell<Vec<McpOAuthDataRequest>>,
        schedules: RefCell<Vec<ScheduleDataRecord>>,
        sessions: RefCell<Option<Vec<SessionDataRecord>>>,
    }

    impl TuiDataPort for FixtureData {
        fn load_attachment(
            &self,
            _workspace: String,
            path: String,
        ) -> Result<AttachmentDataRecord, TuiDataError> {
            let extension = std::path::Path::new(&path)
                .extension()
                .and_then(|value| value.to_str())
                .unwrap_or_default();
            let (kind, mime_type, data_base64) = if extension.eq_ignore_ascii_case("png") {
                (AttachmentDataKind::Image, "image/png", "iVBORw==")
            } else if extension.eq_ignore_ascii_case("wav") {
                (AttachmentDataKind::Audio, "audio/wav", "UklGRg==")
            } else {
                (
                    AttachmentDataKind::Blob,
                    "application/octet-stream",
                    "YmxvYg==",
                )
            };
            Ok(AttachmentDataRecord {
                identity: format!("/workspace/{path}"),
                name: path,
                uri: String::from("file:///workspace/fixture"),
                mime_type: String::from(mime_type),
                kind,
                data_base64: String::from(data_base64),
                byte_size: 4,
            })
        }

        fn runtime_health(&self) -> Result<RuntimeHealthDataRecord, TuiDataError> {
            Ok(RuntimeHealthDataRecord {
                ready: true,
                version: String::from("2.1"),
            })
        }

        fn list_styles(&self) -> Result<Vec<StyleDataRecord>, TuiDataError> {
            Ok(["focused", "ephemeral-turn"]
                .into_iter()
                .map(|id| StyleDataRecord {
                    id: id.to_owned(),
                    version: String::from("1.0.0"),
                    source: StyleDataSourceKind::Project,
                    availability: StyleDataAvailability::Available,
                    style_content_hash: format!("{id}-hash"),
                    compiled_cache_key: format!("{id}-cache"),
                    required_capabilities: vec![],
                })
                .collect())
        }

        fn inspect_style(
            &self,
            selector: String,
        ) -> Result<StyleInspectionDataRecord, TuiDataError> {
            let summary = self
                .list_styles()?
                .into_iter()
                .find(|style| {
                    style.id == selector || format!("{}@{}", style.id, style.version) == selector
                })
                .expect("fixture style");
            Ok(StyleInspectionDataRecord {
                summary,
                source_locator: String::from("fixture/styles/focused.toml"),
                manifest: json!({"identity": {"id": "focused"}}),
                compiled: Some(json!({"graph": {"entry": "respond"}})),
                diagnostics: Vec::new(),
            })
        }

        fn list_harnesses(&self) -> Result<Vec<HarnessDataRecord>, TuiDataError> {
            Ok(vec![HarnessDataRecord {
                id: String::from("fixture"),
                version: String::from("1.0.0"),
                capabilities: vec![String::from("streaming")],
                capability_set_hash: String::from("harness-hash"),
                availability: String::from("available"),
            }])
        }

        fn list_session_components(&self) -> Result<SessionComponentDataRecord, TuiDataError> {
            Ok(SessionComponentDataRecord {
                memory_providers: vec![
                    String::from("none"),
                    String::from("file"),
                    String::from("sqlite-fts"),
                ],
                compaction_strategies: vec![String::from("none"), String::from("sliding_window")],
            })
        }

        fn list_sessions(&self, _limit: u32) -> Result<Vec<SessionDataRecord>, TuiDataError> {
            if let Some(sessions) = self.sessions.borrow().as_ref() {
                return Ok(sessions.clone());
            }
            let mut sessions = vec![SessionDataRecord {
                id: SessionId::from_uuid(Uuid::nil()),
                workspace: String::from("workspace"),
                style: String::from("persistent-chat"),
                sequence: Sequence::new(2).expect("valid sequence"),
                state: String::from("active"),
            }];
            if !self.branched.borrow().is_empty() {
                sessions.push(SessionDataRecord {
                    id: SessionId::from_uuid(Uuid::from_u128(3)),
                    workspace: String::from("workspace"),
                    style: String::from("ephemeral-turn"),
                    sequence: Sequence::new(2).expect("valid sequence"),
                    state: String::from("active"),
                });
            }
            Ok(sessions)
        }

        fn create_session(
            &self,
            workspace: String,
            style: String,
        ) -> Result<SessionId, TuiDataError> {
            self.created.borrow_mut().push(CreatedSessionSelection {
                workspace,
                style,
                harness: None,
                memory: None,
                compaction: None,
                budgets: None,
            });
            Ok(SessionId::from_uuid(Uuid::from_u128(2)))
        }

        fn create_session_with_harness(
            &self,
            workspace: String,
            style: String,
            harness: Option<String>,
        ) -> Result<SessionId, TuiDataError> {
            self.created.borrow_mut().push(CreatedSessionSelection {
                workspace,
                style,
                harness,
                memory: None,
                compaction: None,
                budgets: None,
            });
            Ok(SessionId::from_uuid(Uuid::from_u128(2)))
        }

        fn create_session_with_components(
            &self,
            workspace: String,
            style: String,
            harness: Option<String>,
            memory: Option<String>,
            compaction: Option<String>,
        ) -> Result<SessionId, TuiDataError> {
            self.created.borrow_mut().push(CreatedSessionSelection {
                workspace,
                style,
                harness,
                memory,
                compaction,
                budgets: None,
            });
            Ok(SessionId::from_uuid(Uuid::from_u128(2)))
        }

        fn create_session_with_configuration(
            &self,
            request: CreateSessionDataRequest,
        ) -> Result<SessionId, TuiDataError> {
            self.created.borrow_mut().push(CreatedSessionSelection {
                workspace: request.workspace,
                style: request.style,
                harness: request.harness,
                memory: request.memory,
                compaction: request.compaction,
                budgets: request.budgets,
            });
            Ok(SessionId::from_uuid(Uuid::from_u128(2)))
        }

        fn branch_session(
            &self,
            request: BranchSessionDataRequest,
        ) -> Result<BranchSessionDataRecord, TuiDataError> {
            let parent_session_id = request.parent_session_id;
            let fork_sequence = request.at;
            self.branched.borrow_mut().push(request);
            Ok(BranchSessionDataRecord {
                session_id: SessionId::from_uuid(Uuid::from_u128(3)),
                parent_session_id,
                fork_sequence,
                child_head_sequence: Sequence::new(2).expect("valid sequence"),
            })
        }

        fn change_plugin_lifecycle(
            &self,
            request: PluginLifecycleDataRequest,
        ) -> Result<PluginLifecycleDataRecord, TuiDataError> {
            let state = match request.action {
                PluginLifecycleDataAction::Disable => "disabled",
                PluginLifecycleDataAction::Enable | PluginLifecycleDataAction::Unquarantine => {
                    "active"
                }
                PluginLifecycleDataAction::Quarantine => "quarantined",
            };
            let response = PluginLifecycleDataRecord {
                session_id: request.session_id,
                plugin_id: request.plugin_id.clone(),
                plugin_version: String::from("1.0.0"),
                state: String::from(state),
                committed_sequence: Sequence::new(3).expect("valid sequence"),
                replayed: false,
            };
            self.plugin_changes.borrow_mut().push(request);
            Ok(response)
        }

        fn manage_mcp_oauth(
            &self,
            request: McpOAuthDataRequest,
        ) -> Result<McpOAuthDataRecord, TuiDataError> {
            let response = match &request.action {
                McpOAuthDataAction::Begin => McpOAuthDataRecord::Started {
                    server_id: request.server_id.clone(),
                    transaction_id: String::from("transaction-1"),
                    authorization_url: String::from(
                        "https://identity.example.test/authorize?fixture=1",
                    ),
                    authorization_url_hash: "a".repeat(64),
                    expires_at_ms: 10_000,
                },
                McpOAuthDataAction::Status => McpOAuthDataRecord::Status {
                    server_id: request.server_id.clone(),
                    status: String::from("authorized"),
                    transaction_id: Some(String::from("transaction-1")),
                    expires_at_ms: Some(20_000),
                    scopes: vec![String::from("tools.read")],
                    status_hash: "b".repeat(64),
                },
                McpOAuthDataAction::Cancel { transaction_id } => McpOAuthDataRecord::Status {
                    server_id: request.server_id.clone(),
                    status: String::from("unauthorized"),
                    transaction_id: Some(transaction_id.clone()),
                    expires_at_ms: None,
                    scopes: Vec::new(),
                    status_hash: "c".repeat(64),
                },
            };
            self.mcp_oauth_requests.borrow_mut().push(request);
            Ok(response)
        }

        fn inspect_runtime_resources(
            &self,
            _session_id: SessionId,
        ) -> Result<RuntimeResourcesDataRecord, TuiDataError> {
            Ok(RuntimeResourcesDataRecord {
                artifacts: vec![ArtifactResourceDataRecord {
                    execution_id: String::from("artifact-execution"),
                    node_id: String::from("persist"),
                    state: String::from("completed"),
                    mime_type: String::from("text/markdown"),
                    byte_size: 42,
                    artifact_reference: Some(String::from("artifact:blake3:fixture")),
                }],
                children: vec![ChildResourceDataRecord {
                    execution_id: String::from("child-execution"),
                    task_id: String::from("task-1"),
                    state: String::from("completed"),
                    child_style: String::from("ephemeral-turn@1.2.0"),
                    workspace_mode: String::from("shared_read_only"),
                    child_session_id: Some(String::from("00000000-0000-0000-0000-000000000001")),
                    summary: Some(String::from("done")),
                }],
                processes: vec![ProcessResourceDataRecord {
                    call_id: String::from("call-1"),
                    process_id: String::from("process-1"),
                    status: Some(String::from("live")),
                    started_at: 7,
                    completed_at: Some(8),
                }],
            })
        }

        fn upsert_schedule(
            &self,
            schedule: ScheduleDataRecord,
        ) -> Result<ScheduleStoreDataRecord, TuiDataError> {
            let schedule_id = schedule.schedule_id.clone();
            self.schedules
                .borrow_mut()
                .retain(|existing| existing.schedule_id != schedule_id);
            self.schedules.borrow_mut().push(schedule);
            Ok(ScheduleStoreDataRecord {
                schedule_id,
                replayed: false,
            })
        }

        fn list_schedules(&self, _limit: u32) -> Result<Vec<ScheduleDataRecord>, TuiDataError> {
            Ok(self.schedules.borrow().clone())
        }

        fn remove_schedule(&self, schedule_id: &str) -> Result<bool, TuiDataError> {
            let before = self.schedules.borrow().len();
            self.schedules
                .borrow_mut()
                .retain(|schedule| schedule.schedule_id != schedule_id);
            Ok(self.schedules.borrow().len() != before)
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
                                "content": {"text": "ready"}
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
    fn attachment_commands_list_remove_clear_and_reject_duplicates() {
        let mut logic = TuiLogic::new(FixtureData::default());
        logic.bootstrap().expect("bootstrap");

        logic.insert_text("/attach pixel.png");
        logic.submit_editor().expect("attach image");
        assert_eq!(logic.state().attachments.len(), 1);
        assert_eq!(logic.state().attachments[0].kind, AttachmentKind::Image);
        assert!(logic.state().status.contains("pixel.png"));

        logic.insert_text("/attachments");
        logic.submit_editor().expect("list attachments");
        assert!(logic.state().status.contains("1:pixel.png"));

        logic.insert_text("/attach pixel.png");
        assert_eq!(
            logic.submit_editor(),
            Err(TuiLogicError::DuplicateAttachment)
        );

        logic.insert_text("/attach sound.wav");
        logic.submit_editor().expect("attach audio");
        logic.insert_text("/attachment-remove 1");
        logic.submit_editor().expect("remove image");
        assert_eq!(logic.state().attachments.len(), 1);
        assert_eq!(logic.state().attachments[0].kind, AttachmentKind::Audio);

        logic.insert_text("/attachments-clear");
        logic.submit_editor().expect("clear attachments");
        assert!(logic.state().attachments.is_empty());
        assert_eq!(logic.state().status, "cleared 1 attachments");
    }

    #[test]
    fn refresh_clears_attachments_when_missing_selection_falls_back() {
        let mut logic = TuiLogic::new(FixtureData::default());
        logic.bootstrap().expect("bootstrap");
        logic.insert_text("/attach evidence.bin");
        logic.submit_editor().expect("attach fixture");
        assert_eq!(logic.state().attachments.len(), 1);

        let replacement = SessionDataRecord {
            id: SessionId::from_uuid(Uuid::from_u128(9)),
            workspace: String::from("replacement-workspace"),
            style: String::from("persistent-chat"),
            sequence: Sequence::new(1).expect("valid sequence"),
            state: String::from("active"),
        };
        *logic.data.sessions.borrow_mut() = Some(vec![replacement.clone()]);
        logic.insert_text("/sessions");
        logic.submit_editor().expect("refresh sessions");

        assert_eq!(
            logic.state().selected().map(|session| session.id),
            Some(replacement.id)
        );
        assert!(logic.state().attachments.is_empty());
        assert!(logic.pending_attachments.is_empty());
    }

    #[test]
    fn rich_prompt_matches_acp_envelope_and_text_only_is_byte_compatible() {
        assert_eq!(
            render_submission_prompt("  existing text semantics  ", &[]).expect("plain prompt"),
            "  existing text semantics  "
        );
        let attachments = vec![
            PendingAttachment {
                identity: String::from("image"),
                name: String::from("pixel.png"),
                uri: String::from("file:///workspace/pixel.png"),
                mime_type: String::from("image/png"),
                kind: AttachmentKind::Image,
                data_base64: String::from("iVBORw=="),
                byte_size: 4,
            },
            PendingAttachment {
                identity: String::from("audio"),
                name: String::from("sound.wav"),
                uri: String::from("file:///workspace/sound.wav"),
                mime_type: String::from("audio/wav"),
                kind: AttachmentKind::Audio,
                data_base64: String::from("UklGRg=="),
                byte_size: 4,
            },
            PendingAttachment {
                identity: String::from("blob"),
                name: String::from("evidence.bin"),
                uri: String::from("file:///workspace/evidence.bin"),
                mime_type: String::from("application/octet-stream"),
                kind: AttachmentKind::Blob,
                data_base64: String::from("YmxvYg=="),
                byte_size: 4,
            },
        ];
        let prompt = render_submission_prompt("inspect", &attachments).expect("rich prompt");
        assert_eq!(
            serde_json::from_str::<Value>(&prompt).expect("typed envelope"),
            json!({
                "agentmod_acp_content_version": 1,
                "blocks": [
                    {"type": "text", "text": "inspect"},
                    {"type": "image", "data": "iVBORw==", "mime_type": "image/png", "uri": "file:///workspace/pixel.png"},
                    {"type": "audio", "data": "UklGRg==", "mime_type": "audio/wav"},
                    {"type": "resource", "resource": {"kind": "blob", "data": "YmxvYg==", "uri": "file:///workspace/evidence.bin", "mime_type": "application/octet-stream"}},
                ],
            })
        );
    }

    #[test]
    fn attachment_count_is_bounded_before_an_excess_file_is_loaded() {
        let mut logic = TuiLogic::new(FixtureData::default());
        logic.bootstrap().expect("bootstrap");
        for index in 0..MAX_ATTACHMENTS {
            logic.insert_text(&format!("/attach evidence-{index}.bin"));
            logic.submit_editor().expect("bounded attachment");
        }
        logic.insert_text("/attach excess.bin");
        assert_eq!(logic.submit_editor(), Err(TuiLogicError::AttachmentLimit));
        assert_eq!(logic.state().attachments.len(), MAX_ATTACHMENTS);
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
    fn selected_style_and_harness_reach_create_session() {
        let mut logic = TuiLogic::new(FixtureData::default());
        logic.bootstrap().expect("bootstrap");
        logic.insert_text("/style focused@1.0.0");
        logic.submit_editor().expect("select style");
        logic.insert_text("/harness fixture");
        logic.submit_editor().expect("select harness");
        logic.insert_text("/memory sqlite-fts");
        logic.submit_editor().expect("select memory");
        logic.insert_text("/compaction sliding_window");
        logic.submit_editor().expect("select compaction");
        logic.insert_text("/budget 3 40 100000 1000000 60000");
        logic.submit_editor().expect("select budgets");
        logic.insert_text("/new workspace");
        logic.submit_editor().expect("create session");

        assert_eq!(
            logic.state().selected_style.as_deref(),
            Some("focused@1.0.0")
        );
        assert_eq!(
            logic.data.created.into_inner(),
            vec![CreatedSessionSelection {
                workspace: String::from("workspace"),
                style: String::from("focused@1.0.0"),
                harness: Some(String::from("fixture")),
                memory: Some(String::from("sqlite-fts")),
                compaction: Some(String::from("sliding_window")),
                budgets: Some(SessionBudgetDataRequest {
                    max_iterations: Some(3),
                    max_steps: Some(40),
                    max_tokens: Some(100_000),
                    max_cost_micros: Some(1_000_000),
                    max_duration_ms: Some(60_000),
                }),
            }]
        );
    }

    #[test]
    fn branch_command_selects_a_deliberately_restyled_child() {
        let mut logic = TuiLogic::new(FixtureData::default());
        logic.bootstrap().expect("bootstrap");
        logic.insert_text("/branch 1 ephemeral-turn");
        logic.submit_editor().expect("branch");

        assert_eq!(
            logic.state().selected().map(|session| session.id),
            Some(SessionId::from_uuid(Uuid::from_u128(3)))
        );
        assert_eq!(
            logic.data.branched.into_inner(),
            vec![BranchSessionDataRequest {
                parent_session_id: SessionId::from_uuid(Uuid::nil()),
                at: Sequence::FIRST,
                style: Some(String::from("ephemeral-turn")),
            }]
        );
    }

    #[test]
    fn exact_session_selection_is_identity_checked_and_reloads_canonical_state() {
        let mut logic = TuiLogic::new(FixtureData::default());
        logic.bootstrap().expect("bootstrap");
        logic.insert_text("/branch 1 ephemeral-turn");
        logic.submit_editor().expect("branch");

        let parent = SessionId::from_uuid(Uuid::nil());
        logic
            .select_session_exact(&parent.to_string())
            .expect("select exact parent");
        assert_eq!(
            logic.state().selected().map(|session| session.id),
            Some(parent)
        );
        assert_eq!(
            logic.select_session_exact("not-a-session"),
            Err(TuiLogicError::InvalidSessionId)
        );
        assert_eq!(
            logic.select_session_exact(&Uuid::from_u128(99).to_string()),
            Err(TuiLogicError::SessionNotFound(SessionId::from_uuid(
                Uuid::from_u128(99)
            )))
        );
    }

    #[test]
    fn plugin_management_uses_selected_session_and_exact_layer_owned_action() {
        let mut logic = TuiLogic::new(FixtureData::default());
        logic.bootstrap().expect("bootstrap");

        logic.insert_text("/plugin-quarantine fixture.node integrity_violation");
        logic.submit_editor().expect("quarantine plugin");

        let observed = logic.data.plugin_changes.borrow();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].session_id, SessionId::from_uuid(Uuid::nil()));
        assert_eq!(observed[0].plugin_id, "fixture.node");
        assert_eq!(observed[0].action, PluginLifecycleDataAction::Quarantine);
        assert_eq!(
            observed[0].reason_code.as_deref(),
            Some("integrity_violation")
        );
        assert_ne!(
            observed[0].cancellation_id,
            CancellationId::from_uuid(Uuid::nil())
        );
        assert_eq!(logic.state().view, View::Plugins);
        assert_eq!(logic.state().plugin_lifecycle[0].state, "quarantined");
    }

    #[test]
    fn schedule_management_uses_selected_session_and_refreshes_canonical_list() {
        let mut logic = TuiLogic::new(FixtureData::default());
        logic.bootstrap().expect("bootstrap");

        logic.insert_text("/schedule-interval nightly 1000 500 run checks");
        logic.submit_editor().expect("store schedule");

        let observed = logic.data.schedules.borrow();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].schedule_id, "nightly");
        assert_eq!(observed[0].session_id, SessionId::from_uuid(Uuid::nil()));
        assert_eq!(
            observed[0].trigger,
            ScheduleDataTrigger::Interval {
                starts_at_ms: 1000,
                every_ms: 500,
            }
        );
        assert_eq!(
            observed[0].payload,
            ScheduleDataPayload::Prompt {
                prompt: String::from("run checks")
            }
        );
        drop(observed);
        assert_eq!(logic.state().view, View::Schedules);
        assert_eq!(logic.state().schedules.len(), 1);

        logic.insert_text("/schedule-remove nightly");
        logic.submit_editor().expect("remove schedule");
        assert!(logic.state().schedules.is_empty());
    }

    #[test]
    fn mcp_oauth_commands_bind_selected_session_action_and_cancellation_identity() {
        let mut logic = TuiLogic::new(FixtureData::default());
        logic.bootstrap().expect("bootstrap");

        logic.insert_text("/mcp-oauth-begin fixture_mcp");
        logic.submit_editor().expect("begin OAuth");
        assert_eq!(logic.state().view, View::Mcp);
        assert_eq!(logic.state().mcp_oauth[0].status, "pending");
        assert!(
            logic.state().mcp_oauth[0]
                .authorization_url
                .as_deref()
                .is_some_and(|url| url.starts_with("https://"))
        );

        logic.insert_text("/mcp-oauth-status fixture_mcp");
        logic.submit_editor().expect("read OAuth status");
        assert_eq!(logic.state().mcp_oauth[0].status, "authorized");
        assert_eq!(logic.state().mcp_oauth[0].scopes, ["tools.read"]);
        assert!(logic.state().mcp_oauth[0].authorization_url.is_none());

        logic.insert_text("/mcp-oauth-cancel fixture_mcp transaction-1");
        logic.submit_editor().expect("cancel OAuth");
        assert_eq!(logic.state().mcp_oauth[0].status, "unauthorized");

        let observed = logic.data.mcp_oauth_requests.borrow();
        assert_eq!(observed.len(), 3);
        assert!(
            observed
                .iter()
                .all(|request| request.session_id == SessionId::from_uuid(Uuid::nil()))
        );
        assert_eq!(observed[0].action, McpOAuthDataAction::Begin);
        assert_eq!(observed[1].action, McpOAuthDataAction::Status);
        assert_eq!(
            observed[2].action,
            McpOAuthDataAction::Cancel {
                transaction_id: String::from("transaction-1")
            }
        );
        assert!(
            observed
                .windows(2)
                .all(|pair| pair[0].cancellation_id != pair[1].cancellation_id)
        );
    }

    #[test]
    fn runtime_resource_view_uses_only_bounded_canonical_inspection_rows() {
        let mut logic = TuiLogic::new(FixtureData::default());
        logic.bootstrap().expect("bootstrap");
        logic.insert_text("/runtime");
        logic.submit_editor().expect("runtime resources");

        assert_eq!(logic.state().view, View::RuntimeResources);
        assert_eq!(logic.state().artifact_resources[0].node_id, "persist");
        assert_eq!(
            logic.state().child_resources[0].workspace_mode,
            "shared_read_only"
        );
        assert_eq!(
            logic.state().process_resources[0].status.as_deref(),
            Some("live")
        );
        assert!(logic.state().status.contains("1 artifacts"));
    }
}
