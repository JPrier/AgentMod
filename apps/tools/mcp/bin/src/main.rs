//! MCP host composition root with bounded concurrent tool-protocol transport.

use std::{collections::BTreeMap, error::Error, io, sync::Arc, time::Duration};

use agentmod_mcp_host_data::McpData;
use agentmod_mcp_host_dependency::{
    DependencyServerConfig, DependencyTransportConfig, McpDependency, McpDependencyConfig,
    McpDependencyPort,
};
use agentmod_mcp_host_logic::McpLogic;
use agentmod_mcp_host_service::McpHostService;
use agentmod_tool_protocol::{ToolHostCommand, ToolHostEvent};
use serde::Deserialize;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    sync::{Semaphore, mpsc},
};

const MAX_FRAME_BYTES: usize = 1024 * 1024;
const MAX_IN_FLIGHT: usize = 32;

#[derive(Deserialize)]
struct BootstrapServer {
    id: String,
    #[serde(default)]
    display_name: String,
    #[serde(default)]
    active: bool,
    #[serde(flatten)]
    transport: BootstrapTransport,
}

#[derive(Deserialize)]
#[serde(tag = "transport", rename_all = "snake_case")]
enum BootstrapTransport {
    Stdio {
        program: String,
        #[serde(default)]
        arguments: Vec<String>,
        #[serde(default)]
        environment: BTreeMap<String, String>,
    },
    StreamableHttp {
        url: String,
        bearer_token_environment: Option<String>,
        #[serde(default)]
        header_environments: BTreeMap<String, String>,
    },
    LegacySse {
        url: String,
        #[serde(default)]
        header_environments: BTreeMap<String, String>,
    },
    #[serde(rename = "streamable_http_oauth")]
    StreamableHttpOAuth {
        url: String,
        authorization_server: String,
        client_id: String,
        client_secret_environment: Option<String>,
        redirect_uri: String,
        #[serde(default)]
        scopes: Vec<String>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let servers = std::env::var("AGENTMOD_MCP_SERVERS_JSON")
        .ok()
        .map(|value| serde_json::from_str::<Vec<BootstrapServer>>(&value))
        .transpose()?
        .unwrap_or_default()
        .into_iter()
        .map(|server| DependencyServerConfig {
            display_name: if server.display_name.is_empty() {
                server.id.clone()
            } else {
                server.display_name
            },
            id: server.id,
            active: server.active,
            transport: match server.transport {
                BootstrapTransport::Stdio {
                    program,
                    arguments,
                    environment,
                } => DependencyTransportConfig::Stdio {
                    program,
                    arguments,
                    environment,
                },
                BootstrapTransport::StreamableHttp {
                    url,
                    bearer_token_environment,
                    header_environments,
                } => DependencyTransportConfig::StreamableHttp {
                    url,
                    bearer_token_environment,
                    header_environments,
                },
                BootstrapTransport::LegacySse {
                    url,
                    header_environments,
                } => DependencyTransportConfig::LegacySse {
                    url,
                    header_environments,
                },
                BootstrapTransport::StreamableHttpOAuth {
                    url,
                    authorization_server,
                    client_id,
                    client_secret_environment,
                    redirect_uri,
                    scopes,
                } => DependencyTransportConfig::StreamableHttpOAuth {
                    url,
                    authorization_server,
                    client_id,
                    client_secret_environment,
                    redirect_uri,
                    scopes,
                },
            },
        })
        .collect();
    let dependency = McpDependency::new(McpDependencyConfig {
        servers,
        client_name: "agentmod".to_owned(),
        client_version: env!("CARGO_PKG_VERSION").to_owned(),
        request_timeout: Duration::from_secs(60),
        maximum_message_bytes: MAX_FRAME_BYTES,
        maximum_servers: 64,
        authorization_owner: std::env::var("AGENTMOD_MCP_OWNER")?,
        authorization_session: std::env::var("AGENTMOD_MCP_SESSION")?,
        authorization_key_hex: std::env::var("AGENTMOD_MCP_AUTH_KEY")?,
        authorization_replay_root: std::env::var_os("AGENTMOD_MCP_REPLAY_ROOT")
            .map(std::path::PathBuf::from)
            .ok_or("AGENTMOD_MCP_REPLAY_ROOT is required")?,
        http_state_root: std::env::var_os("AGENTMOD_MCP_HTTP_STATE_ROOT")
            .map(std::path::PathBuf::from)
            .ok_or("AGENTMOD_MCP_HTTP_STATE_ROOT is required")?,
        oauth_state_root: std::env::var_os("AGENTMOD_MCP_OAUTH_STATE_ROOT")
            .map(std::path::PathBuf::from)
            .or_else(|| {
                std::env::var_os("AGENTMOD_MCP_HTTP_STATE_ROOT")
                    .map(std::path::PathBuf::from)
                    .map(|root| root.join("oauth"))
            })
            .ok_or("MCP OAuth state root is required")?,
        oauth_encryption_key_hex: std::env::var("AGENTMOD_MCP_OAUTH_KEY").ok(),
    })?;
    let shutdown = dependency.clone();
    let service = Arc::new(McpHostService::new(McpLogic::new(McpData::new(dependency))));
    serve(service, tokio::io::stdin(), tokio::io::stdout()).await?;
    shutdown.shutdown().await;
    Ok(())
}

async fn serve<L, R, W>(
    service: Arc<McpHostService<L>>,
    input: R,
    mut output: W,
) -> Result<(), Box<dyn Error>>
where
    L: agentmod_mcp_host_logic::McpLogicPort + 'static,
    R: AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (sender, mut receiver) = mpsc::channel::<ToolHostEvent>(128);
    let writer = tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            let mut bytes =
                serde_json::to_vec(&event).map_err(|error| io::Error::other(error.to_string()))?;
            bytes.push(b'\n');
            output.write_all(&bytes).await?;
            output.flush().await?;
        }
        Ok::<(), io::Error>(())
    });
    let limit = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
    let mut input = BufReader::new(input);
    loop {
        match read_frame(&mut input).await? {
            Frame::Eof => break,
            Frame::Oversized => sender
                .send(failed("frame_too_large"))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer stopped"))?,
            Frame::Bytes(bytes) => {
                let Ok(permit) = Arc::clone(&limit).try_acquire_owned() else {
                    sender
                        .send(failed("host_overloaded"))
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
                            .unwrap_or_else(|_| vec![failed("request_failed")]),
                        Err(_) => vec![failed("invalid_json")],
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

async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Frame> {
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
            Ok(byte) if bytes.len() < MAX_FRAME_BYTES => bytes.push(byte),
            Ok(_) => oversized = true,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(if bytes.is_empty() && !oversized {
                    Frame::Eof
                } else if oversized {
                    Frame::Oversized
                } else {
                    Frame::Bytes(bytes)
                });
            }
            Err(error) => return Err(error),
        }
    }
}

fn failed(code: &str) -> ToolHostEvent {
    ToolHostEvent::Failed {
        call_id: "invalid".to_owned(),
        code: code.to_owned(),
        message: "MCP host request was rejected".to_owned(),
        retryable: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oauth_transport_uses_public_bootstrap_spelling() {
        let parsed: Vec<BootstrapServer> = serde_json::from_str(
            r#"[{
                "id":"protected",
                "active":true,
                "transport":"streamable_http_oauth",
                "url":"https://mcp.example/resource",
                "authorization_server":"https://login.example",
                "client_id":"client",
                "client_secret_environment":null,
                "redirect_uri":"http://127.0.0.1:49152/callback",
                "scopes":["tools.read"]
            }]"#,
        )
        .expect("public OAuth spelling");
        assert!(matches!(
            parsed[0].transport,
            BootstrapTransport::StreamableHttpOAuth { .. }
        ));
    }
}
