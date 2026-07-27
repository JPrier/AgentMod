//! Web host composition root with bounded concurrent JSONL transport.

use std::{collections::BTreeSet, error::Error, io, sync::Arc, time::Duration};

use agentmod_tool_protocol::{ToolHostCommand, ToolHostEvent};
use agentmod_web_host_data::WebData;
use agentmod_web_host_dependency::{
    EnvironmentSecretDependency, MockSearchDocument, NetworkPolicy, ReqwestWebDependency,
    SearchProvider, WebDependencyConfig,
};
use agentmod_web_host_logic::{WebLogic, WebLogicConfig};
use agentmod_web_host_service::{WebHostService, WebHostServiceConfig};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    sync::{Semaphore, mpsc},
};

const MAX_FRAME_BYTES: usize = 1024 * 1024;
const RESPONSE_CHANNEL_CAPACITY: usize = 128;
const MAX_IN_FLIGHT_REQUESTS: usize = 64;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let owner_id = std::env::var("AGENTMOD_WEB_OWNER")?;
    let session_id = std::env::var("AGENTMOD_WEB_SESSION")?;
    let authorization_key_hex = std::env::var("AGENTMOD_WEB_AUTH_KEY")?;
    let artifact_root = std::env::var_os("AGENTMOD_WEB_ARTIFACT_ROOT").map_or_else(
        || {
            std::env::temp_dir()
                .join("agentmod")
                .join("web-artifacts")
                .join(safe_scope(&session_id))
        },
        std::path::PathBuf::from,
    );
    let allowed_domains = environment_list("AGENTMOD_WEB_ALLOWED_DOMAINS");
    let denied_domains = environment_list("AGENTMOD_WEB_DENIED_DOMAINS");
    let search_provider = std::env::var("AGENTMOD_BRAVE_API_KEY_REF").map_or_else(
        |_| SearchProvider::Mock {
            documents: Vec::<MockSearchDocument>::new(),
        },
        |api_key_reference| SearchProvider::Brave { api_key_reference },
    );
    let dependency = ReqwestWebDependency::new(
        WebDependencyConfig {
            artifact_root,
            authorization_key_hex,
            owner_id: owner_id.clone(),
            session_id: session_id.clone(),
            maximum_replay_entries: 100_000,
            maximum_active_calls: 64,
            network_policy: NetworkPolicy {
                allowed_domains,
                denied_domains,
                allow_private_network: environment_bool("AGENTMOD_WEB_ALLOW_PRIVATE"),
                allow_plain_http: environment_bool("AGENTMOD_WEB_ALLOW_HTTP"),
                allowed_methods: BTreeSet::from([
                    "DELETE".to_owned(),
                    "GET".to_owned(),
                    "HEAD".to_owned(),
                    "OPTIONS".to_owned(),
                    "PATCH".to_owned(),
                    "POST".to_owned(),
                    "PUT".to_owned(),
                ]),
            },
            maximum_redirects: 10,
            maximum_timeout: Duration::from_secs(120),
            maximum_response_bytes: 32 * 1024 * 1024,
            maximum_inline_bytes: 512 * 1024,
            maximum_url_length: 16 * 1024,
            maximum_headers: 128,
            maximum_request_body_bytes: 16 * 1024 * 1024,
            proxy_url: std::env::var("AGENTMOD_WEB_PROXY").ok(),
            cache_entries: 128,
            search_provider,
        },
        EnvironmentSecretDependency,
    )?;
    let data = WebData::new(dependency);
    let logic = WebLogic::new(
        data,
        WebLogicConfig {
            maximum_url_length: 16 * 1024,
            maximum_query_length: 4096,
            maximum_search_results: 50,
            maximum_headers: 128,
            maximum_request_body_bytes: 16 * 1024 * 1024,
            maximum_timeout: Duration::from_secs(120),
            maximum_redirects: 10,
            maximum_response_bytes: 32 * 1024 * 1024,
            maximum_inline_bytes: 512 * 1024,
        },
    )?;
    let service = Arc::new(WebHostService::new(
        logic,
        WebHostServiceConfig {
            owner_id,
            session_id,
        },
    )?);
    serve(service, tokio::io::stdin(), tokio::io::stdout()).await
}

fn safe_scope(session_id: &str) -> String {
    let scope: String = session_id
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '-')
        .take(128)
        .collect();
    if scope.is_empty() {
        String::from("standalone")
    } else {
        scope
    }
}

async fn serve<L, R, W>(
    service: Arc<WebHostService<L>>,
    input: R,
    mut output: W,
) -> Result<(), Box<dyn Error>>
where
    L: agentmod_web_host_logic::WebLogicPort + 'static,
    R: AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (sender, mut receiver) = mpsc::channel::<ToolHostEvent>(RESPONSE_CHANNEL_CAPACITY);
    let request_limit = Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUESTS));
    let writer = tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            let mut encoded =
                serde_json::to_vec(&event).map_err(|error| io::Error::other(error.to_string()))?;
            encoded.push(b'\n');
            output.write_all(&encoded).await?;
            output.flush().await?;
        }
        Ok::<(), io::Error>(())
    });
    let mut input = BufReader::new(input);
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
                    let events = match serde_json::from_slice::<ToolHostCommand>(&bytes) {
                        Ok(command) => service
                            .handle(command)
                            .await
                            .unwrap_or_else(|_| vec![failed("invalid", "request_failed")]),
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

fn environment_list(name: &str) -> Vec<String> {
    std::env::var(name).map_or_else(
        |_| Vec::new(),
        |value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        },
    )
}

fn environment_bool(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .is_some_and(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
}

fn failed(call_id: &str, code: &str) -> ToolHostEvent {
    ToolHostEvent::Failed {
        call_id: call_id.to_owned(),
        code: code.to_owned(),
        message: "web request was rejected".to_owned(),
        retryable: false,
    }
}
