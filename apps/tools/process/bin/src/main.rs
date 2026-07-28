//! Secure process-host composition root with concurrent bounded JSONL handling.

use std::{collections::BTreeSet, error::Error, io, sync::Arc, time::Duration};

use agentmod_process_host_data::ProcessData;
use agentmod_process_host_dependency::{
    DependencyExecutablePolicy, ProcessDependencyConfig, TokioProcessDependency,
    cleanup_local_endpoint, prepare_local_endpoint,
};
use agentmod_process_host_logic::{ProcessLogic, ProcessLogicConfig};
use agentmod_process_host_service::{
    ProcessHostService, ProcessHostServiceConfig, ProcessServiceError,
    local_rpc::{ProcessLocalRpcConfig, run_local},
};
use agentmod_tool_protocol::{ToolHostCommand, ToolHostEvent};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    sync::{Semaphore, mpsc},
};

const MAX_FRAME_BYTES: usize = 1024 * 1024;
const RESPONSE_CHANNEL_CAPACITY: usize = 128;
const MAX_IN_FLIGHT_REQUESTS: usize = 64;

#[tokio::main]
#[allow(
    clippy::too_many_lines,
    reason = "the composition root keeps security limits and the bounded transport assembly visible"
)]
async fn main() -> Result<(), Box<dyn Error>> {
    let workspace_root = std::env::current_dir()?;
    let owner_id = std::env::var("AGENTMOD_PROCESS_OWNER")?;
    let session_id = std::env::var("AGENTMOD_PROCESS_SESSION")?;
    let authorization_key_hex = std::env::var("AGENTMOD_PROCESS_AUTH_KEY")?;
    let executable_policy = std::env::var("AGENTMOD_PROCESS_ALLOWED_EXECUTABLES")
        .unwrap_or_default()
        .split(';')
        .filter(|value| !value.trim().is_empty())
        .map(|value| (value.trim().to_owned(), DependencyExecutablePolicy::Allow))
        .collect();
    let storage_root = workspace_root.join(".agentmod");
    let dependency = TokioProcessDependency::new(ProcessDependencyConfig {
        storage_root: storage_root.clone(),
        log_root: storage_root.join("process-logs"),
        authorization_key_hex: authorization_key_hex.clone(),
        owner_id: owner_id.clone(),
        session_id: session_id.clone(),
        inherited_environment_allowlist: BTreeSet::from([
            "ALLUSERSPROFILE".to_owned(),
            "APPDATA".to_owned(),
            "CARGO_HOME".to_owned(),
            "CommonProgramFiles".to_owned(),
            "CommonProgramFiles(x86)".to_owned(),
            "CommonProgramW6432".to_owned(),
            "ComSpec".to_owned(),
            "HOME".to_owned(),
            "HOMEDRIVE".to_owned(),
            "HOMEPATH".to_owned(),
            "LOCALAPPDATA".to_owned(),
            "NUMBER_OF_PROCESSORS".to_owned(),
            "PATH".to_owned(),
            "PATHEXT".to_owned(),
            "PROCESSOR_ARCHITECTURE".to_owned(),
            "ProgramData".to_owned(),
            "ProgramFiles".to_owned(),
            "ProgramFiles(x86)".to_owned(),
            "ProgramW6432".to_owned(),
            "RUSTUP_HOME".to_owned(),
            "SystemDrive".to_owned(),
            "SYSTEMROOT".to_owned(),
            "TEMP".to_owned(),
            "TMP".to_owned(),
            "USERNAME".to_owned(),
            "USERPROFILE".to_owned(),
            "WINDIR".to_owned(),
        ]),
        max_input_bytes: 1024 * 1024,
        max_range_bytes: 1024 * 1024,
        max_arguments: 256,
        max_argument_bytes: 1024 * 1024,
        max_environment_entries: 128,
        max_environment_bytes: 256 * 1024,
        max_active_processes: 100,
        max_total_retained_bytes: 1024 * 1024 * 1024,
        drain_timeout: Duration::from_secs(10),
        input_write_timeout: Duration::from_secs(2),
        max_replay_entries: 100_000,
        max_completed_entries: 1_000,
        max_waiters_per_process: 64,
        executable_policy,
        default_executable_policy: DependencyExecutablePolicy::Ask,
    })?;
    let data = ProcessData::new(dependency);
    let logic = ProcessLogic::new(
        data,
        ProcessLogicConfig {
            workspace_root,
            environment_allowlist: BTreeSet::new(),
            environment_denylist: BTreeSet::from([
                "ANTHROPIC_API_KEY".to_owned(),
                "AWS_SECRET_ACCESS_KEY".to_owned(),
                "GOOGLE_API_KEY".to_owned(),
                "OPENAI_API_KEY".to_owned(),
                "PATH".to_owned(),
            ]),
            max_timeout: Duration::from_secs(24 * 60 * 60),
            max_output_bytes: 64 * 1024 * 1024,
            max_projection_bytes: 1024 * 1024,
        },
    );
    let service = ProcessHostService::new(
        logic,
        ProcessHostServiceConfig {
            owner_id,
            session_id,
        },
    )?;
    if let Ok(endpoint) = std::env::var("AGENTMOD_PROCESS_ENDPOINT") {
        prepare_local_endpoint(&endpoint)?;
        let result = run_local(
            service,
            ProcessLocalRpcConfig {
                endpoint: endpoint.clone(),
                authorization_token: authorization_key_hex.into(),
                maximum_frame_bytes: MAX_FRAME_BYTES,
                idle_check_interval: Duration::from_millis(
                    std::env::var("AGENTMOD_PROCESS_IDLE_TIMEOUT_MS")
                        .ok()
                        .and_then(|value| value.parse().ok())
                        .unwrap_or(30_000),
                ),
            },
        )
        .await;
        let cleanup = cleanup_local_endpoint(&endpoint);
        result?;
        cleanup?;
        return Ok(());
    }
    let service = Arc::new(service);
    let (sender, mut receiver) = mpsc::channel::<ToolHostEvent>(RESPONSE_CHANNEL_CAPACITY);
    let request_limit = Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUESTS));
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(event) = receiver.recv().await {
            let mut encoded =
                serde_json::to_vec(&event).map_err(|error| io::Error::other(error.to_string()))?;
            encoded.push(b'\n');
            stdout.write_all(&encoded).await?;
            stdout.flush().await?;
        }
        Ok::<(), io::Error>(())
    });

    let mut input = BufReader::new(tokio::io::stdin());
    loop {
        match read_bounded_frame(&mut input, MAX_FRAME_BYTES).await? {
            Frame::Eof => break,
            Frame::Oversized => {
                sender
                    .send(failed("invalid", "frame_too_large"))
                    .await
                    .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer stopped"))?;
            }
            Frame::Bytes(bytes) => {
                let Ok(permit) = Arc::clone(&request_limit).try_acquire_owned() else {
                    sender
                        .send(failed("invalid", "host_overloaded"))
                        .await
                        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer stopped"))?;
                    continue;
                };
                let service = Arc::clone(&service);
                let sender = sender.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let command = serde_json::from_slice::<ToolHostCommand>(&bytes);
                    let events = match command {
                        Ok(command) => match service.handle(command).await {
                            Ok(events) => events,
                            Err(error) => vec![failed("invalid", service_error_code(&error))],
                        },
                        Err(_) => vec![failed("invalid", "invalid_json")],
                    };
                    for event in events {
                        if sender.send(event).await.is_err() {
                            break;
                        }
                    }
                });
            }
        }
    }
    drop(sender);
    writer.await??;
    Ok(())
}

enum Frame {
    Eof,
    Oversized,
    Bytes(Vec<u8>),
}

async fn read_bounded_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    maximum: usize,
) -> io::Result<Frame> {
    let mut bytes = Vec::new();
    let mut oversized = false;
    loop {
        match reader.read_u8().await {
            Ok(b'\n') => {
                return Ok(if oversized {
                    Frame::Oversized
                } else {
                    Frame::Bytes(bytes)
                });
            }
            Ok(byte) => {
                if bytes.len() < maximum {
                    bytes.push(byte);
                } else {
                    oversized = true;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                if bytes.is_empty() && !oversized {
                    return Ok(Frame::Eof);
                }
                return Ok(if oversized {
                    Frame::Oversized
                } else {
                    Frame::Bytes(bytes)
                });
            }
            Err(error) => return Err(error),
        }
    }
}

fn failed(call_id: &str, code: &str) -> ToolHostEvent {
    ToolHostEvent::Failed {
        call_id: call_id.to_owned(),
        code: code.to_owned(),
        message: "process request was rejected".to_owned(),
        retryable: false,
    }
}

const fn service_error_code(error: &ProcessServiceError) -> &'static str {
    match error {
        ProcessServiceError::MissingConfiguration => "host_misconfigured",
        ProcessServiceError::InvalidAuthorizationEnvelope => "authorization_invalid",
        ProcessServiceError::UnknownTool => "unknown_tool",
        ProcessServiceError::InvalidArguments => "invalid_arguments",
        ProcessServiceError::Authorization => "authorization_denied",
        ProcessServiceError::Logic => "operation_rejected",
    }
}
