//! Native harness bounded JSONL process endpoint.

use agentmod_harness::build_secure_service;
use agentmod_harness_dependency::parse_authorization_key;
use agentmod_harness_protocol::{HarnessCommand, HarnessReply};
use agentmod_harness_service::ServiceResponse;
use std::{error::Error, io};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_FRAME: usize = 16 * 1024 * 1024;
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let key = std::env::var("AGENTMOD_HARNESS_AUTH_KEY")
        .map_err(|_| "AGENTMOD_HARNESS_AUTH_KEY is required")?;
    let service = build_secure_service(parse_authorization_key(&key)?);
    let frame_pacing = std::env::var("AGENTMOD_HARNESS_FRAME_PACING_MS")
        .unwrap_or_else(|_| "0".to_owned())
        .parse::<u64>()
        .map_err(|_| "AGENTMOD_HARNESS_FRAME_PACING_MS must be an integer")?;
    if frame_pacing > 5_000 {
        return Err("AGENTMOD_HARNESS_FRAME_PACING_MS exceeds 5000".into());
    }
    let frame_pacing = std::time::Duration::from_millis(frame_pacing);
    let mut input = tokio::io::stdin();
    let mut output = tokio::io::stdout();
    loop {
        let replies = match read_frame(&mut input).await? {
            Frame::Eof => break,
            Frame::Oversized => vec![failed("frame_too_large")],
            Frame::Bytes(bytes) => match serde_json::from_slice::<HarnessCommand>(&bytes) {
                Ok(HarnessCommand::Health) => {
                    match service.handle_wire_command(&HarnessCommand::Health) {
                        Ok(ServiceResponse::Health(v)) => vec![HarnessReply::Health {
                            status: v.status.as_str().into(),
                            ready_provider_count: v.ready_provider_count,
                            capabilities: v.capabilities,
                        }],
                        Ok(ServiceResponse::Catalog(_)) => vec![failed("catalog_unexpected")],
                        Err(_) => vec![failed("health_failed")],
                    }
                }
                Ok(HarnessCommand::Catalog) => {
                    match service.handle_wire_command(&HarnessCommand::Catalog) {
                        Ok(ServiceResponse::Catalog(providers)) => {
                            vec![HarnessReply::Catalog { providers }]
                        }
                        Ok(ServiceResponse::Health(_)) => vec![failed("health_unexpected")],
                        Err(_) => vec![failed("catalog_failed")],
                    }
                }
                Ok(command @ HarnessCommand::Execute { .. }) => service
                    .execute_wire(&command)
                    .map_or_else(|_| vec![failed("execution_failed")], incremental_replies),
                Ok(command @ HarnessCommand::Continue { .. }) => service
                    .continue_wire(&command)
                    .map_or_else(|_| vec![failed("continuation_failed")], incremental_replies),
                Ok(HarnessCommand::Cancel { .. }) => {
                    vec![failed("cancellation_unsupported")]
                }
                Err(_) => vec![failed("invalid_json")],
            },
        };
        for reply in replies {
            write_reply(&mut output, &reply).await?;
            if !frame_pacing.is_zero()
                && matches!(
                    reply,
                    HarnessReply::Event {
                        terminal: false,
                        ..
                    }
                )
            {
                tokio::time::sleep(frame_pacing).await;
            }
        }
    }
    Ok(())
}

fn incremental_replies(events: Vec<agentmod_harness_protocol::HarnessEvent>) -> Vec<HarnessReply> {
    let length = events.len();
    if length == 0 {
        return vec![failed("empty_execution")];
    }
    events
        .into_iter()
        .enumerate()
        .map(|(index, event)| HarnessReply::Event {
            event,
            terminal: index + 1 == length,
        })
        .collect()
}

async fn write_reply<W: tokio::io::AsyncWrite + Unpin>(
    output: &mut W,
    reply: &HarnessReply,
) -> Result<(), Box<dyn Error>> {
    let mut encoded = serde_json::to_vec(reply)?;
    encoded.push(b'\n');
    output.write_all(&encoded).await?;
    output.flush().await?;
    Ok(())
}
enum Frame {
    Eof,
    Oversized,
    Bytes(Vec<u8>),
}
async fn read_frame<R: tokio::io::AsyncRead + Unpin>(reader: &mut R) -> io::Result<Frame> {
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
            Ok(byte) if bytes.len() < MAX_FRAME => bytes.push(byte),
            Ok(_) => oversized = true,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return if bytes.is_empty() && !oversized {
                    Ok(Frame::Eof)
                } else {
                    Ok(if oversized {
                        Frame::Oversized
                    } else {
                        Frame::Bytes(bytes)
                    })
                };
            }
            Err(e) => return Err(e),
        }
    }
}
fn failed(code: &str) -> HarnessReply {
    HarnessReply::Failed {
        code: code.into(),
        message: "harness request was rejected".into(),
        retryable: false,
    }
}
