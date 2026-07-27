//! CLI composition root.

use std::{
    io::{self, Write},
    process::ExitCode,
};

use agentmod_cli_data::CliData;
use agentmod_cli_dependency::LocalRuntimeClient;
use agentmod_cli_logic::CliLogic;
use agentmod_cli_service::{CliService, CliServiceConfig, ServiceInvocation};

fn main() -> ExitCode {
    let endpoint =
        std::env::var("AGENTMOD_RUNTIME_ENDPOINT").unwrap_or_else(|_| default_endpoint());
    let Ok(authorization_token) = std::env::var("AGENTMOD_RUNTIME_AUTH_TOKEN") else {
        eprintln!("AGENTMOD_RUNTIME_AUTH_TOKEN is required");
        return ExitCode::FAILURE;
    };
    let dependency = match LocalRuntimeClient::new(
        endpoint.clone(),
        authorization_token,
        agentmod_protocol_support::DEFAULT_MAX_FRAME_BYTES,
    ) {
        Ok(client) => client,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let data = CliData::new(dependency);
    let logic = CliLogic::new(data);
    let service = CliService::new(
        logic,
        CliServiceConfig {
            runtime_endpoint_label: endpoint,
        },
    );

    match service.start_from(std::env::args_os()) {
        Ok(ServiceInvocation::Complete(response)) => {
            println!("{}", response.output);
            ExitCode::from(response.exit_code)
        }
        Ok(ServiceInvocation::Stream(stream)) => {
            let mut stdout = io::stdout().lock();
            while let Some(item) = stream.next() {
                match item {
                    Ok(line) => {
                        if writeln!(stdout, "{line}")
                            .and_then(|()| stdout.flush())
                            .is_err()
                        {
                            eprintln!("failed to write CLI stream output");
                            return ExitCode::FAILURE;
                        }
                    }
                    Err(error) => {
                        eprintln!("{error}");
                        return ExitCode::FAILURE;
                    }
                }
            }
            ExitCode::SUCCESS
        }
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
