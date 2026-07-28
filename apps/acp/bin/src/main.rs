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
    let provider = std::env::var("AGENTMOD_ACP_PROVIDER")
        .unwrap_or_else(|_| String::from("deterministic-mock"));
    let model = std::env::var("AGENTMOD_ACP_MODEL").unwrap_or_else(|_| String::from("mock-model"));
    let options = match std::env::var("AGENTMOD_ACP_PROVIDER_OPTIONS") {
        Ok(value) => match serde_json::from_str(&value) {
            Ok(value) => value,
            Err(error) => {
                eprintln!("AGENTMOD_ACP_PROVIDER_OPTIONS is invalid JSON: {error}");
                return ExitCode::FAILURE;
            }
        },
        Err(_) => serde_json::Value::Object(serde_json::Map::default()),
    };
    let dependency = match LocalRuntimeDependency::new(
        endpoint,
        authorization_token,
        agentmod_protocol_support::DEFAULT_MAX_FRAME_BYTES,
    )
    .and_then(|dependency| dependency.with_provider_request(provider, model, options))
    {
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
