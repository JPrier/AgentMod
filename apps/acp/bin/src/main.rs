//! ACP frontend composition root.

use std::process::ExitCode;

use agentmod_acp_data::AcpData;
use agentmod_acp_dependency::LocalRuntimeDependency;
use agentmod_acp_logic::AcpLogic;
use agentmod_acp_service::AcpService;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
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
    let service = AcpService::new(AcpLogic::new(AcpData::new(dependency)));
    match service.run().await {
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
