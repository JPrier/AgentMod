//! TUI composition root.

use std::{process::ExitCode, time::Duration};

use agentmod_tui_data::TuiData;
use agentmod_tui_dependency::LocalRuntimeDependency;
use agentmod_tui_logic::TuiLogic;
use agentmod_tui_service::{TuiService, TuiServiceConfig};

fn main() -> ExitCode {
    let endpoint =
        std::env::var("AGENTMOD_RUNTIME_ENDPOINT").unwrap_or_else(|_| default_endpoint());
    let Ok(authorization_token) = std::env::var("AGENTMOD_RUNTIME_AUTH_TOKEN") else {
        eprintln!("AGENTMOD_RUNTIME_AUTH_TOKEN is required");
        return ExitCode::FAILURE;
    };
    let dependency = match LocalRuntimeDependency::new(
        endpoint,
        authorization_token,
        agentmod_protocol_support::DEFAULT_MAX_FRAME_BYTES,
    ) {
        Ok(dependency) => dependency,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let service = TuiService::new(
        TuiLogic::new(TuiData::new(dependency)),
        TuiServiceConfig {
            tick_rate: Duration::from_millis(33),
        },
    );

    let mut arguments = std::env::args().skip(1);
    let result = match arguments.next().as_deref() {
        Some("--smoke") => service.smoke().map(|output| println!("{output}")),
        Some("--smoke-turn") => {
            let prompt = arguments.collect::<Vec<_>>().join(" ");
            if prompt.trim().is_empty() {
                eprintln!("--smoke-turn requires a prompt");
                return ExitCode::from(2);
            }
            service
                .smoke_turn(&prompt)
                .map(|output| println!("{output}"))
        }
        Some("--smoke-attachment-turn") => smoke_attachment_turn(service, arguments),
        Some("--smoke-command") => {
            let command = arguments.collect::<Vec<_>>().join(" ");
            if command.trim().is_empty() {
                eprintln!("--smoke-command requires a command");
                return ExitCode::from(2);
            }
            service
                .smoke_command(&command)
                .map(|output| println!("{output}"))
        }
        Some("--smoke-session-command") => {
            let Some(session_id) = arguments.next() else {
                eprintln!("--smoke-session-command requires a session ID and command");
                return ExitCode::from(2);
            };
            let command = arguments.collect::<Vec<_>>().join(" ");
            if command.trim().is_empty() {
                eprintln!("--smoke-session-command requires a session ID and command");
                return ExitCode::from(2);
            }
            service
                .smoke_session_command(&session_id, &command)
                .map(|output| println!("{output}"))
        }
        Some("--smoke-watch") => {
            let Some(milliseconds) = arguments.next() else {
                eprintln!("--smoke-watch requires a duration in milliseconds");
                return ExitCode::from(2);
            };
            if arguments.next().is_some() {
                eprintln!("--smoke-watch accepts exactly one duration");
                return ExitCode::from(2);
            }
            let Ok(milliseconds) = milliseconds.parse::<u64>() else {
                eprintln!("--smoke-watch duration must be an integer");
                return ExitCode::from(2);
            };
            service
                .smoke_watch(Duration::from_millis(milliseconds))
                .map(|output| println!("{output}"))
        }
        Some(argument) => {
            eprintln!("unknown TUI option `{argument}`");
            return ExitCode::from(2);
        }
        None => service.run(),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn smoke_attachment_turn(
    service: TuiService<TuiLogic<TuiData<LocalRuntimeDependency>>>,
    mut arguments: impl Iterator<Item = String>,
) -> Result<(), agentmod_tui_service::TuiServiceError> {
    let prompt = arguments.next().unwrap_or_default();
    let paths = arguments.collect::<Vec<_>>();
    service
        .smoke_attachment_turn(&prompt, &paths)
        .map(|output| println!("{output}"))
}

#[cfg(windows)]
fn default_endpoint() -> String {
    String::from(r"\\.\pipe\agentmod-runtime")
}

#[cfg(unix)]
fn default_endpoint() -> String {
    String::from("/tmp/agentmod-runtime.sock")
}
