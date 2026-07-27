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

#[cfg(windows)]
fn default_endpoint() -> String {
    String::from(r"\\.\pipe\agentmod-runtime")
}

#[cfg(unix)]
fn default_endpoint() -> String {
    String::from("/tmp/agentmod-runtime.sock")
}
