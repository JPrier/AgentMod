//! Independent deterministic harness fixture bounded JSONL process endpoint.
//!
//! The stdin reader runs concurrently with command execution so a wire
//! `Cancel` can interrupt an in-flight slow provider exchange. Replies are
//! written under a shared output lock as individual bounded frames.

use agentmod_harness_fixture::build_secure_service;
use agentmod_harness_fixture_dependency::parse_authorization_key;
use agentmod_harness_fixture_service::FixtureServiceReply;
use agentmod_harness_protocol::{HarnessCommand, HarnessReply};
use std::{error::Error, io, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

const MAX_FRAME: usize = 16 * 1024 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let key = std::env::var("AGENTMOD_HARNESS_AUTH_KEY")
        .map_err(|_| "AGENTMOD_HARNESS_AUTH_KEY is required")?;
    let development = std::env::var_os("AGENTMOD_FIXTURE_DEV_MODE").is_some();
    let service = if development {
        // Development mode accepts the literal `grant` marker for conformance
        // scripts; production supervision always uses the signed grant path.
        agentmod_harness_fixture::build_service()
    } else {
        build_secure_service(parse_authorization_key(&key)?)
    };
    let output = Arc::new(Mutex::new(tokio::io::stdout()));
    let mut input = tokio::io::stdin();
    loop {
        let frame = match read_frame(&mut input).await? {
            Frame::Eof => break,
            Frame::Oversized => {
                write_replies(&output, vec![failed("frame_too_large")]).await?;
                continue;
            }
            Frame::Bytes(bytes) => bytes,
        };
        let command: HarnessCommand = if let Ok(command) = serde_json::from_slice(&frame) { command } else {
            write_replies(&output, vec![failed("invalid_json")]).await?;
            continue;
        };
        match command {
            HarnessCommand::Health => {
                let reply = match service.handle_wire_command(&HarnessCommand::Health).await {
                    Ok(FixtureServiceReply::Health(health)) => vec![HarnessReply::Health {
                        status: String::from("ok"),
                        ready_provider_count: health.ready_provider_count,
                        capabilities: health.capabilities,
                    }],
                    _ => vec![failed("health_failed")],
                };
                write_replies(&output, reply).await?;
            }
            HarnessCommand::Catalog => {
                let reply = match service.handle_wire_command(&HarnessCommand::Catalog).await {
                    Ok(FixtureServiceReply::Catalog(providers)) => {
                        vec![HarnessReply::Catalog { providers }]
                    }
                    _ => vec![failed("catalog_failed")],
                };
                write_replies(&output, reply).await?;
            }
            command @ HarnessCommand::Execute { .. } => {
                let service = service.clone();
                let output = output.clone();
                tokio::spawn(async move {
                    let replies = match service.execute_wire(&command).await {
                        Ok(events) => incremental_replies(events),
                        Err(error) => vec![failed_with("execution_failed", &error.to_string())],
                    };
                    if write_replies(&output, replies).await.is_err() {
                    }
                });
            }
            command @ HarnessCommand::Continue { .. } => {
                let service = service.clone();
                let output = output.clone();
                tokio::spawn(async move {
                    let replies = match service.continue_wire(&command).await {
                        Ok(events) => incremental_replies(events),
                        Err(_) => vec![failed("continuation_failed")],
                    };
                    if write_replies(&output, replies).await.is_err() {
                    }
                });
            }
            HarnessCommand::Cancel { cancellation_id } => {
                let Ok(cancelled) = service
                    .cancel_wire(&HarnessCommand::Cancel { cancellation_id })
                    .await
                else {
                    write_replies(&output, vec![failed("cancellation_failed")]).await?;
                    continue;
                };
                // An active exchange writes its own Cancelled event through the
                // spawned execution task; this handler only reports the case
                // where no exchange was active.
                if !cancelled {
                    write_replies(&output, vec![failed("no_active_cancellation")]).await?;
                }
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

async fn write_replies<W>(
    output: &Arc<Mutex<W>>,
    replies: Vec<HarnessReply>,
) -> Result<(), Box<dyn Error>>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut output = output.lock().await;
    for reply in replies {
        let mut encoded = serde_json::to_vec(&reply)?;
        encoded.push(b'\n');
        output.write_all(&encoded).await?;
        output.flush().await?;
    }
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
    let mut buffer = [0_u8; 8192];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            return Ok(if bytes.is_empty() && !oversized {
                Frame::Eof
            } else if oversized {
                Frame::Oversized
            } else {
                Frame::Bytes(bytes)
            });
        }
        for &byte in &buffer[..read] {
            if byte == b'\n' {
                return Ok(if oversized {
                    Frame::Oversized
                } else {
                    Frame::Bytes(bytes)
                });
            }
            if bytes.len() < MAX_FRAME {
                bytes.push(byte);
            } else {
                oversized = true;
            }
        }
    }
}

fn failed(code: &str) -> HarnessReply {
    failed_with(code, "fixture harness request was rejected")
}

fn failed_with(code: &str, message: &str) -> HarnessReply {
    HarnessReply::Failed {
        code: code.into(),
        message: message.to_owned(),
        retryable: false,
    }
}
