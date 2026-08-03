//! Independently compiled public-protocol harness fixture.
//!
//! This binary intentionally depends on no native harness service, logic, data,
//! or dependency crate. It exists only to prove runtime harness modularity.

mod dependency;

use std::{
    error::Error,
    io::{self, BufRead, BufReader, BufWriter, Write},
};

use agentmod_harness_protocol::{HarnessCommand, HarnessEvent, HarnessReply};
use dependency::{
    DependencyError, ExecuteRequest, IndependentProviderDependency, ProviderEvent, parse_key,
};

const MAX_FRAME_BYTES: usize = 1024 * 1024;

fn main() -> Result<(), Box<dyn Error>> {
    let key = std::env::var("AGENTMOD_HARNESS_AUTH_KEY")
        .map_err(|_| "AGENTMOD_HARNESS_AUTH_KEY is required")?;
    let mut application = IndependentHarness::new(parse_key(&key).map_err(redacted_error)?);
    let mut input = BufReader::new(io::stdin().lock());
    let mut output = BufWriter::new(io::stdout().lock());
    loop {
        let replies = match read_frame(&mut input)? {
            Frame::Eof => break,
            Frame::Oversized => vec![failed("frame_too_large")],
            Frame::Bytes(bytes) => serde_json::from_slice::<HarnessCommand>(&bytes).map_or_else(
                |_| vec![failed("invalid_json")],
                |command| application.handle(command),
            ),
        };
        for reply in replies {
            serde_json::to_writer(&mut output, &reply)?;
            output.write_all(b"\n")?;
            output.flush()?;
        }
    }
    Ok(())
}

fn redacted_error(_: DependencyError) -> &'static str {
    "independent harness configuration is invalid"
}

struct IndependentHarness {
    provider: IndependentProviderDependency,
}

impl IndependentHarness {
    const fn new(key: [u8; 32]) -> Self {
        Self {
            provider: IndependentProviderDependency::new(key),
        }
    }

    fn handle(&mut self, command: HarnessCommand) -> Vec<HarnessReply> {
        match command {
            HarnessCommand::Health => vec![HarnessReply::Health {
                status: String::from("ok"),
                ready_provider_count: 1,
                capabilities: vec![
                    String::from("cancellation"),
                    String::from("streaming"),
                    String::from("structured_context_replacement"),
                    String::from("structured_output"),
                    String::from("token_usage"),
                    String::from("tool_calls"),
                ],
            }],
            HarnessCommand::Execute {
                provider,
                model,
                entries,
                options,
                authorization_grant,
                ..
            } => self
                .provider
                .execute(&ExecuteRequest {
                    provider,
                    model,
                    entries,
                    options,
                    authorization_grant,
                })
                .map_or_else(map_dependency_error, incremental_replies),
            HarnessCommand::Cancel { .. } => vec![HarnessReply::Events {
                events: vec![HarnessEvent::Cancelled],
            }],
            HarnessCommand::Continue { .. } => vec![failed("continuation_unsupported")],
            HarnessCommand::Catalog => vec![failed("catalog_unsupported")],
        }
    }
}

fn incremental_replies(events: Vec<ProviderEvent>) -> Vec<HarnessReply> {
    let length = events.len();
    events
        .into_iter()
        .enumerate()
        .map(|(index, event)| HarnessReply::Event {
            event: match event {
                ProviderEvent::Started => HarnessEvent::Started,
                ProviderEvent::Text(text) => HarnessEvent::TextDelta { text },
                ProviderEvent::Completed {
                    finish_reason,
                    usage,
                } => HarnessEvent::Completed {
                    finish_reason,
                    usage,
                },
            },
            terminal: index + 1 == length,
        })
        .collect()
}

fn map_dependency_error(error: DependencyError) -> Vec<HarnessReply> {
    let code = match error {
        DependencyError::Authorization => "authorization_rejected",
        DependencyError::InvalidRequest => "invalid_request",
        DependencyError::Clock => "clock_unavailable",
    };
    vec![failed(code)]
}

fn failed(code: &str) -> HarnessReply {
    HarnessReply::Failed {
        code: code.to_owned(),
        message: String::from("independent harness request was rejected"),
        retryable: false,
    }
}

enum Frame {
    Eof,
    Oversized,
    Bytes(Vec<u8>),
}

fn read_frame<R: BufRead>(reader: &mut R) -> io::Result<Frame> {
    let mut bytes = Vec::new();
    let mut oversized = false;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return if bytes.is_empty() && !oversized {
                Ok(Frame::Eof)
            } else if oversized {
                Ok(Frame::Oversized)
            } else {
                Ok(Frame::Bytes(bytes))
            };
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |index| index + 1);
        let content_end = newline.unwrap_or(available.len());
        if !oversized {
            let remaining = MAX_FRAME_BYTES.saturating_sub(bytes.len());
            if content_end > remaining {
                oversized = true;
            } else {
                bytes.extend_from_slice(&available[..content_end]);
            }
        }
        reader.consume(consumed);
        if newline.is_some() {
            return if oversized {
                Ok(Frame::Oversized)
            } else {
                Ok(Frame::Bytes(bytes))
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framing_is_bounded_and_recovers_after_an_oversized_line() {
        let mut input = Vec::with_capacity(MAX_FRAME_BYTES + 16);
        input.extend(std::iter::repeat_n(b'x', MAX_FRAME_BYTES + 1));
        input.extend_from_slice(b"\n{}\n");
        let mut reader = BufReader::new(input.as_slice());
        assert!(matches!(
            read_frame(&mut reader).expect("oversized frame"),
            Frame::Oversized
        ));
        assert!(matches!(
            read_frame(&mut reader).expect("next frame"),
            Frame::Bytes(bytes) if bytes == b"{}"
        ));
    }

    #[test]
    fn health_contract_is_public_protocol_only() {
        let mut harness = IndependentHarness::new([7_u8; 32]);
        assert!(matches!(
            harness.handle(HarnessCommand::Health).as_slice(),
            [HarnessReply::Health {
                status,
                ready_provider_count: 1,
                capabilities,
            }] if status == "ok"
                && capabilities.iter().any(|capability| capability == "streaming")
        ));
    }
}
