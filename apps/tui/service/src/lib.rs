//! Ratatui rendering, terminal lifecycle, and endpoint input mapping.
#![allow(
    clippy::missing_errors_doc,
    reason = "the service exposes one documented closed error taxonomy"
)]

use std::time::{Duration, Instant};

use agentmod_tui_logic::{TranscriptRole, TuiLogicError, TuiLogicPort, TuiState, View};
use ratatui::{
    Frame,
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    layout::{Constraint, Direction, Layout, Margin, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Tabs, Wrap},
};
use thiserror::Error;

/// Terminal event-loop configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TuiServiceConfig {
    /// Maximum wait between runtime stream polls and redraws.
    pub tick_rate: Duration,
}

/// Endpoint-facing terminal service.
pub struct TuiService<L> {
    logic: L,
    config: TuiServiceConfig,
}

impl<L> TuiService<L> {
    /// Creates a terminal service around injected frontend logic.
    #[must_use]
    pub const fn new(logic: L, config: TuiServiceConfig) -> Self {
        Self { logic, config }
    }
}

impl<L: TuiLogicPort> TuiService<L> {
    /// Bootstraps the frontend without entering raw-terminal mode.
    ///
    /// This is intended for installation diagnostics and automated transport
    /// validation, not as an alternate frontend implementation.
    pub fn smoke(mut self) -> Result<String, TuiServiceError> {
        self.logic.bootstrap().map_err(map_logic)?;
        let state = self.logic.state();
        Ok(format!(
            "runtime={} ready={} sessions={}",
            state.runtime_version,
            state.runtime_ready,
            state.sessions.len()
        ))
    }

    /// Executes one normal runtime turn without entering raw-terminal mode.
    ///
    /// This diagnostic traverses the same logic, data, dependency, streaming,
    /// credit-window, and canonical-commit path used by the fullscreen TUI.
    pub fn smoke_turn(mut self, prompt: &str) -> Result<String, TuiServiceError> {
        self.logic.bootstrap().map_err(map_logic)?;
        self.logic.insert_text(prompt);
        self.logic.submit_editor().map_err(map_logic)?;
        let deadline = Instant::now() + Duration::from_secs(15);
        while self.logic.state().is_streaming() {
            self.logic.poll_runtime().map_err(map_logic)?;
            if Instant::now() >= deadline {
                return Err(TuiServiceError::TurnTimeout);
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let assistant = self
            .logic
            .state()
            .transcript
            .iter()
            .filter(|entry| entry.role == TranscriptRole::Assistant)
            .map(|entry| entry.text.as_str())
            .collect::<String>();
        Ok(format!(
            "status={} assistant={assistant}",
            self.logic.state().status
        ))
    }

    /// Runs the fullscreen terminal endpoint and restores terminal state.
    ///
    /// # Errors
    ///
    /// Returns [`TuiServiceError`] for runtime bootstrap, terminal, or input failures.
    pub fn run(mut self) -> Result<(), TuiServiceError> {
        self.logic.bootstrap().map_err(map_logic)?;
        ratatui::run(|terminal| {
            loop {
                self.logic.poll_runtime().map_err(to_io)?;
                terminal.draw(|frame| render(frame, self.logic.state()))?;
                if self.logic.state().should_quit {
                    break Ok(());
                }
                if event::poll(self.config.tick_rate)? {
                    match event::read()? {
                        Event::Key(key) if key.is_press() => {
                            self.handle_key(key).map_err(to_io)?;
                        }
                        Event::Paste(value) => self.logic.insert_text(&value),
                        _ => {}
                    }
                }
            }
        })
        .map_err(TuiServiceError::Terminal)
    }

    fn handle_key(&mut self, key: KeyEvent) -> Result<(), TuiLogicError> {
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            return match key.code {
                KeyCode::Char('q') => {
                    self.logic.request_quit();
                    Ok(())
                }
                KeyCode::Char('c') if self.logic.state().is_streaming() => {
                    self.logic.cancel_active()
                }
                KeyCode::Char('r') => self.logic.refresh_sessions(),
                KeyCode::Char('1') => {
                    self.logic.set_view(View::Chat);
                    Ok(())
                }
                KeyCode::Char('2') => {
                    self.logic.set_view(View::Events);
                    Ok(())
                }
                KeyCode::Char('3') => {
                    self.logic.set_view(View::Context);
                    Ok(())
                }
                KeyCode::Char('4') => {
                    self.logic.set_view(View::Help);
                    Ok(())
                }
                _ => Ok(()),
            };
        }
        if self.logic.state().approval.is_some() {
            return match key.code {
                KeyCode::Char('y') | KeyCode::Enter => self.logic.resolve_approval(true),
                KeyCode::Char('n') | KeyCode::Esc => self.logic.resolve_approval(false),
                _ => Ok(()),
            };
        }
        match key.code {
            KeyCode::Esc => {
                self.logic.request_quit();
                Ok(())
            }
            KeyCode::Tab => {
                self.logic.set_view(next_view(self.logic.state().view));
                Ok(())
            }
            KeyCode::BackTab => {
                self.logic.set_view(previous_view(self.logic.state().view));
                Ok(())
            }
            KeyCode::Up if key.modifiers.contains(KeyModifiers::ALT) => {
                self.logic.select_relative(-1)
            }
            KeyCode::Down if key.modifiers.contains(KeyModifiers::ALT) => {
                self.logic.select_relative(1)
            }
            KeyCode::Up => {
                self.logic.history_relative(-1);
                Ok(())
            }
            KeyCode::Down => {
                self.logic.history_relative(1);
                Ok(())
            }
            KeyCode::Left => {
                self.logic.move_cursor(-1);
                Ok(())
            }
            KeyCode::Right => {
                self.logic.move_cursor(1);
                Ok(())
            }
            KeyCode::Backspace => {
                self.logic.backspace();
                Ok(())
            }
            KeyCode::Delete => {
                self.logic.delete();
                Ok(())
            }
            KeyCode::Enter if key.modifiers.contains(KeyModifiers::SHIFT) => {
                self.logic.insert_newline();
                Ok(())
            }
            KeyCode::Enter => self.logic.submit_editor(),
            KeyCode::Char(value) => {
                self.logic.insert_char(value);
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

fn render(frame: &mut Frame<'_>, state: &TuiState) {
    let [header, body, editor, status] = Layout::vertical([
        Constraint::Length(2),
        Constraint::Min(8),
        Constraint::Length(5),
        Constraint::Length(1),
    ])
    .areas(frame.area());
    render_header(frame, state, header);
    let [sessions, content] =
        Layout::horizontal([Constraint::Length(28), Constraint::Min(20)]).areas(body);
    render_sessions(frame, state, sessions);
    match state.view {
        View::Chat => render_chat(frame, state, content),
        View::Events => render_events(frame, state, content),
        View::Context => render_context(frame, state, content),
        View::Help => render_help(frame, content),
    }
    render_editor(frame, state, editor);
    frame.render_widget(
        Paragraph::new(state.status.as_str()).style(Style::new().fg(if state.runtime_ready {
            Color::Green
        } else {
            Color::Yellow
        })),
        status,
    );
    if state.approval.is_some() {
        render_approval(frame, state);
    }
}

fn render_header(frame: &mut Frame<'_>, state: &TuiState, area: Rect) {
    let tabs = Tabs::new(["Chat", "Events", "Context", "Help"])
        .select(match state.view {
            View::Chat => 0,
            View::Events => 1,
            View::Context => 2,
            View::Help => 3,
        })
        .block(
            Block::new()
                .borders(Borders::BOTTOM)
                .title(format!(" AgentMod {} ", state.runtime_version)),
        )
        .highlight_style(Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD));
    frame.render_widget(tabs, area);
}

fn render_sessions(frame: &mut Frame<'_>, state: &TuiState, area: Rect) {
    let items = state
        .sessions
        .iter()
        .map(|session| {
            ListItem::new(vec![
                Line::from(Span::styled(
                    short_id(&session.id.to_string()),
                    Style::new().add_modifier(Modifier::BOLD),
                )),
                Line::from(format!("{} · {}", session.style, session.sequence.get())),
                Line::from(Span::styled(
                    session.workspace.clone(),
                    Style::new().fg(Color::DarkGray),
                )),
            ])
        })
        .collect::<Vec<_>>();
    let mut selection = ListState::default();
    selection.select(state.selected_session);
    frame.render_stateful_widget(
        List::new(items)
            .block(Block::bordered().title(" Sessions · Alt↑/↓ "))
            .highlight_symbol("▸ ")
            .highlight_style(Style::new().bg(Color::DarkGray).fg(Color::White)),
        area,
        &mut selection,
    );
}

fn render_chat(frame: &mut Frame<'_>, state: &TuiState, area: Rect) {
    let lines = state
        .transcript
        .iter()
        .flat_map(|entry| {
            let (label, color) = match entry.role {
                TranscriptRole::System => ("system", Color::DarkGray),
                TranscriptRole::User => ("you", Color::Cyan),
                TranscriptRole::Assistant => ("agent", Color::Green),
                TranscriptRole::Tool => ("tool", Color::Magenta),
                TranscriptRole::Error => ("error", Color::Red),
            };
            vec![
                Line::from(Span::styled(
                    format!("{label}  "),
                    Style::new().fg(color).add_modifier(Modifier::BOLD),
                )),
                Line::from(entry.text.clone()),
                Line::default(),
            ]
        })
        .collect::<Vec<_>>();
    let scroll =
        u16::try_from(lines.len().saturating_sub(area.height as usize - 2)).unwrap_or(u16::MAX);
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(Block::bordered().title(" Conversation "))
            .wrap(Wrap { trim: false })
            .scroll((scroll, 0)),
        area,
    );
}

fn render_events(frame: &mut Frame<'_>, state: &TuiState, area: Rect) {
    let lines = state
        .timeline
        .iter()
        .map(|event| {
            Line::from(vec![
                Span::styled(
                    format!("{:>5} ", event.sequence.get()),
                    Style::new().fg(Color::DarkGray),
                ),
                Span::styled(
                    format!("{:<34}", event.event_type),
                    Style::new().fg(Color::Yellow),
                ),
                Span::raw(event.summary.clone()),
            ])
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::bordered().title(" Canonical event timeline "))
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn render_context(frame: &mut Frame<'_>, state: &TuiState, area: Rect) {
    let session = state.selected();
    let lines = vec![
        Line::from(vec![
            Span::styled("Provider  ", Style::new().fg(Color::DarkGray)),
            Span::raw(state.provider.clone()),
        ]),
        Line::from(vec![
            Span::styled("Model     ", Style::new().fg(Color::DarkGray)),
            Span::raw(state.model.clone()),
        ]),
        Line::from(vec![
            Span::styled("Tokens    ", Style::new().fg(Color::DarkGray)),
            Span::raw(format!(
                "{} input · {} output",
                state.input_tokens, state.output_tokens
            )),
        ]),
        Line::from(vec![
            Span::styled("Session   ", Style::new().fg(Color::DarkGray)),
            Span::raw(session.map_or_else(|| String::from("none"), |value| value.id.to_string())),
        ]),
        Line::from(vec![
            Span::styled("Style     ", Style::new().fg(Color::DarkGray)),
            Span::raw(session.map(|value| value.style.clone()).unwrap_or_default()),
        ]),
        Line::from(vec![
            Span::styled("Events    ", Style::new().fg(Color::DarkGray)),
            Span::raw(state.timeline.len().to_string()),
        ]),
    ];
    frame.render_widget(
        Paragraph::new(lines).block(Block::bordered().title(" Context and usage ")),
        area,
    );
}

fn render_help(frame: &mut Frame<'_>, area: Rect) {
    frame.render_widget(
        Paragraph::new(vec![
            Line::from("Ctrl+1…4 views · Tab cycle · Alt+↑/↓ sessions · Ctrl+R refresh"),
            Line::from("Enter send · Shift+Enter newline · ↑/↓ prompt history"),
            Line::from("Ctrl+C cancel active generation · Ctrl+Q or Esc quit"),
            Line::default(),
            Line::from("/new [workspace]  /sessions  /model <id>  /provider <id>"),
            Line::from("/chat  /events  /context  /help  /cancel"),
            Line::from("/approve  /deny  /quit"),
            Line::default(),
            Line::from("Permission dialog: Y/Enter approve · N/Esc deny"),
        ])
        .block(Block::bordered().title(" Command palette and keys "))
        .wrap(Wrap { trim: false }),
        area,
    );
}

fn render_editor(frame: &mut Frame<'_>, state: &TuiState, area: Rect) {
    let title = if state.is_streaming() {
        " Prompt · generating "
    } else {
        " Prompt · Enter send · Shift+Enter newline "
    };
    frame.render_widget(
        Paragraph::new(state.editor.as_str())
            .block(Block::bordered().title(title))
            .wrap(Wrap { trim: false }),
        area,
    );
    if !state.is_streaming() && state.approval.is_none() {
        let before = &state.editor[..state.editor_cursor];
        let line = before.chars().filter(|value| *value == '\n').count();
        let column = before
            .rsplit('\n')
            .next()
            .map_or(0, |value| value.chars().count());
        let x = area
            .x
            .saturating_add(1)
            .saturating_add(u16::try_from(column).unwrap_or(u16::MAX));
        let y = area
            .y
            .saturating_add(1)
            .saturating_add(u16::try_from(line).unwrap_or(u16::MAX));
        frame.set_cursor_position((
            x.min(area.right().saturating_sub(1)),
            y.min(area.bottom().saturating_sub(1)),
        ));
    }
}

fn render_approval(frame: &mut Frame<'_>, state: &TuiState) {
    let Some(approval) = &state.approval else {
        return;
    };
    let area = centered_rect(frame.area(), 70, 45);
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                approval.tool.clone(),
                Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD),
            )),
            Line::from(format!("call {}", approval.call_id)),
            Line::default(),
            Line::from(approval.arguments.to_string()),
            Line::default(),
            Line::from(Span::styled(
                "Y / Enter approve       N / Esc deny",
                Style::new().fg(Color::Cyan),
            )),
        ])
        .block(
            Block::bordered()
                .title(" Permission required ")
                .border_style(Style::new().fg(Color::Yellow)),
        )
        .wrap(Wrap { trim: false }),
        area,
    );
}

fn centered_rect(area: Rect, percent_x: u16, percent_y: u16) -> Rect {
    let vertical = Layout::new(
        Direction::Vertical,
        [
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ],
    )
    .split(area);
    let horizontal = Layout::new(
        Direction::Horizontal,
        [
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ],
    )
    .split(vertical[1]);
    horizontal[1].inner(Margin {
        horizontal: 0,
        vertical: 0,
    })
}

const fn next_view(view: View) -> View {
    match view {
        View::Chat => View::Events,
        View::Events => View::Context,
        View::Context => View::Help,
        View::Help => View::Chat,
    }
}

const fn previous_view(view: View) -> View {
    match view {
        View::Chat => View::Help,
        View::Events => View::Chat,
        View::Context => View::Events,
        View::Help => View::Context,
    }
}

fn short_id(value: &str) -> String {
    value.chars().take(8).collect()
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err consumes the lower-layer error at this explicit boundary"
)]
fn map_logic(error: TuiLogicError) -> TuiServiceError {
    TuiServiceError::Logic(error.to_string())
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "terminal closure error conversion consumes the logic error"
)]
fn to_io(error: TuiLogicError) -> std::io::Error {
    std::io::Error::other(error.to_string())
}

/// Failures exposed by the terminal endpoint.
#[derive(Debug, Error)]
pub enum TuiServiceError {
    /// Frontend business logic rejected an operation.
    #[error("TUI logic failed: {0}")]
    Logic(String),
    /// Terminal setup, restoration, rendering, or input failed.
    #[error("terminal failed: {0}")]
    Terminal(#[source] std::io::Error),
    /// A noninteractive diagnostic turn exceeded its fixed safety bound.
    #[error("TUI diagnostic turn timed out")]
    TurnTimeout,
}

#[cfg(test)]
mod tests {
    use agentmod_tui_logic::{TranscriptEntry, TranscriptRole, TuiState, View};
    use ratatui::{Terminal, backend::TestBackend};

    use super::render;

    #[test]
    fn dashboard_renders_core_interactive_surfaces() {
        let backend = TestBackend::new(100, 30);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        let mut state = TuiState::default();
        state.runtime_ready = true;
        state.runtime_version = String::from("2.1");
        state.view = View::Chat;
        state.transcript.push(TranscriptEntry {
            role: TranscriptRole::Assistant,
            text: String::from("streamed response"),
            sequence: None,
        });

        terminal
            .draw(|frame| render(frame, &state))
            .expect("render");

        let screen = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(screen.contains("AgentMod 2.1"));
        assert!(screen.contains("Sessions"));
        assert!(screen.contains("Conversation"));
        assert!(screen.contains("streamed response"));
        assert!(screen.contains("Prompt"));
    }
}
