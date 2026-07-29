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
    BranchSessionDataRecord, BranchSessionDataRequest, CreateSessionDataRequest, HarnessDataRecord,
    SessionBudgetDataRequest, SessionDataRecord, SessionEventDataRecord, StyleDataAvailability,
    StyleDataRecord, StyleDataSourceKind, TuiDataError, TuiDataPort, TurnDataEvent, TurnDataStream,
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
    Graph,
    Styles,
    Harnesses,
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
    fn refresh_harnesses(&mut self) -> Result<(), TuiLogicError>;
    fn refresh_session_components(&mut self) -> Result<(), TuiLogicError>;
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
        self.refresh_harnesses()?;
        self.refresh_session_components()?;
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
                self.state.selected_style = Some(self.resolve_style_selector(&selector)?);
                self.state.status = format!("style: {}", self.state.active_style());
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
            "/events" => self.state.view = View::Events,
            "/context" => self.state.view = View::Context,
            "/graph" => self.state.view = View::Graph,
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
        self.state.style_introspection = None;
        let Some(session_id) = self.state.selected().map(|value| value.id) else {
            self.state.status = String::from("no sessions — use /new");
            return Ok(());
        };
        self.refresh_selected_introspection()?;
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

    fn refresh_selected_introspection(&mut self) -> Result<(), TuiLogicError> {
        let Some(session_id) = self.state.selected().map(|value| value.id) else {
            self.state.style_introspection = None;
            return Ok(());
        };
        let inspection = self.data.inspect_session(session_id).map_err(map_error)?;
        self.state.style_introspection = inspection.get("style_introspection").cloned();
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

fn map_harness(harness: HarnessDataRecord) -> HarnessSummary {
    HarnessSummary {
        id: harness.id,
        version: harness.version,
        capabilities: harness.capabilities,
        capability_set_hash: harness.capability_set_hash,
        availability: harness.availability,
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
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use agentmod_primitives::{CancellationId, SessionId};
    use agentmod_tui_data::{
        BranchSessionDataRecord, BranchSessionDataRequest, HarnessDataRecord,
        RuntimeHealthDataRecord, SessionComponentDataRecord, SessionDataRecord,
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
    }

    impl TuiDataPort for FixtureData {
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
}
