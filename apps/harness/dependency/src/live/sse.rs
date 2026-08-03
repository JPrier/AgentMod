//! Bounded incremental SSE parsing with fragmented-UTF-8 tolerance.

/// One parsed SSE event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SseEvent {
    /// Event type; empty maps to the default `message` event.
    pub event: String,
    /// Data payload with data-lines joined by `\n`.
    pub data: String,
}

/// Bounded SSE parsing failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SseParseError {
    /// A line or accumulated event exceeded the configured byte bound.
    Oversized,
}

const DEFAULT_MAX_LINE_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_EVENT_BYTES: usize = 8 * 1024 * 1024;

/// Incremental SSE parser over arbitrary byte chunks.
///
/// Handles CRLF and LF framing, `:` keepalive comments, multi-line `data`
/// fields, `event`/`id`/`retry` fields, and fragmented UTF-8 by keeping raw
/// bytes until an event boundary. Malformed unknown fields are ignored; an
/// event whose accumulated bytes exceed the bound fails closed.
#[derive(Clone, Debug)]
pub struct SseParser {
    pending: Vec<u8>,
    event_name: Vec<u8>,
    data_lines: Vec<Vec<u8>>,
    max_line_bytes: usize,
    max_event_bytes: usize,
}

impl SseParser {
    /// Creates a parser with default byte bounds.
    #[must_use]
    pub fn new() -> Self {
        Self::with_bounds(DEFAULT_MAX_LINE_BYTES, DEFAULT_MAX_EVENT_BYTES)
    }

    /// Creates a parser with explicit byte bounds.
    #[must_use]
    pub const fn with_bounds(max_line_bytes: usize, max_event_bytes: usize) -> Self {
        Self {
            pending: Vec::new(),
            event_name: Vec::new(),
            data_lines: Vec::new(),
            max_line_bytes,
            max_event_bytes,
        }
    }

    /// Feeds a byte chunk and returns any complete events in order.
    ///
    /// # Errors
    ///
    /// Returns [`SseParseError::Oversized`] when a line or event exceeds the
    /// configured bounds. The parser then retains no partial state.
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<SseEvent>, SseParseError> {
        let mut events = Vec::new();
        for &byte in chunk {
            if byte == b'\n' {
                let line = std::mem::take(&mut self.pending);
                if line.len() > self.max_line_bytes {
                    self.reset();
                    return Err(SseParseError::Oversized);
                }
                self.dispatch_line(&line, &mut events)?;
            } else {
                self.pending.push(byte);
                if self.pending.len() > self.max_line_bytes {
                    self.reset();
                    return Err(SseParseError::Oversized);
                }
            }
        }
        Ok(events)
    }

    /// Finishes the stream and returns any trailing complete event.
    ///
    /// # Errors
    ///
    /// Returns [`SseParseError::Oversized`] when the final unterminated line
    /// exceeds the bound.
    pub fn finish(&mut self) -> Result<Vec<SseEvent>, SseParseError> {
        if self.pending.is_empty() && self.data_lines.is_empty() && self.event_name.is_empty() {
            return Ok(Vec::new());
        }
        let line = std::mem::take(&mut self.pending);
        if line.len() > self.max_line_bytes {
            self.reset();
            return Err(SseParseError::Oversized);
        }
        if !line.is_empty() {
            self.dispatch_line(&line, &mut Vec::new())?;
        }
        let event = self.take_event();
        Ok(event.into_iter().collect())
    }

    fn dispatch_line(
        &mut self,
        line: &[u8],
        events: &mut Vec<SseEvent>,
    ) -> Result<(), SseParseError> {
        let line = trim_cr(line);
        if line.is_empty() {
            if let Some(event) = self.take_event() {
                events.push(event);
            }
            return Ok(());
        }
        if line[0] == b':' {
            // Comment/keepalive.
            return Ok(());
        }
        let Some((field, value)) = line.iter().position(|&byte| byte == b':').map(|index| {
            (
                &line[..index],
                value_with_optional_space(&line[index + 1..]),
            )
        }) else {
            // Unknown field without a colon; tolerated as malformed.
            return Ok(());
        };
        match field {
            b"data" => {
                if self.data_lines.iter().map(Vec::len).sum::<usize>() + value.len()
                    > self.max_event_bytes
                {
                    self.reset();
                    return Err(SseParseError::Oversized);
                }
                self.data_lines.push(value.to_vec());
            }
            b"event" => self.event_name = value.to_vec(),
            _ => {}
        }
        Ok(())
    }

    fn take_event(&mut self) -> Option<SseEvent> {
        if self.data_lines.is_empty() {
            self.event_name.clear();
            return None;
        }
        let data = join_data(&mut self.data_lines);
        let event = if self.event_name.is_empty() {
            String::from("message")
        } else {
            String::from_utf8_lossy(&self.event_name).into_owned()
        };
        self.event_name.clear();
        Some(SseEvent { event, data })
    }

    fn reset(&mut self) {
        self.pending.clear();
        self.event_name.clear();
        self.data_lines.clear();
    }
}

impl Default for SseParser {
    fn default() -> Self {
        Self::new()
    }
}

fn trim_cr(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn value_with_optional_space(value: &[u8]) -> &[u8] {
    value.strip_prefix(b" ").unwrap_or(value)
}

fn join_data(lines: &mut Vec<Vec<u8>>) -> String {
    let mut joined = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if index > 0 {
            joined.push(b'\n');
        }
        joined.extend_from_slice(line);
    }
    lines.clear();
    String::from_utf8_lossy(&joined).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_multiline_data_and_events() {
        let mut parser = SseParser::new();
        let events = parser
            .push(b": keepalive\nid: 7\nevent: delta\ndata: {\"a\":\ndata: 1}\n\n")
            .expect("parse");
        assert_eq!(
            events,
            [SseEvent {
                event: "delta".into(),
                data: "{\"a\":\n1}".into(),
            }]
        );
        assert_eq!(parser.finish().expect("finish"), Vec::new());
    }

    #[test]
    fn handles_fragmented_utf8_across_chunks() {
        let mut parser = SseParser::new();
        // "héllo" split mid multi-byte character.
        let first = "data: h\u{e9}".as_bytes();
        let second = "llo\n\n".as_bytes();
        let mut events = parser.push(first).expect("first chunk");
        events.extend(parser.push(second).expect("second chunk"));
        assert_eq!(
            events,
            [SseEvent {
                event: "message".into(),
                data: "h\u{e9}llo".into(),
            }]
        );
    }

    #[test]
    fn handles_crlf_and_ignores_unknown_fields() {
        let mut parser = SseParser::new();
        let events = parser
            .push(b"random: junk\r\ndata: hello\r\n\r\n")
            .expect("parse");
        assert_eq!(
            events,
            [SseEvent {
                event: "message".into(),
                data: "hello".into(),
            }]
        );
    }

    #[test]
    fn event_without_data_is_ignored() {
        let mut parser = SseParser::new();
        assert_eq!(parser.push(b"event: ping\n\n").expect("parse"), Vec::new());
        assert_eq!(
            parser.push(b"data: real\n\n").expect("parse"),
            [SseEvent {
                event: "message".into(),
                data: "real".into(),
            }]
        );
    }

    #[test]
    fn oversized_line_fails_closed() {
        let mut parser = SseParser::with_bounds(8, 64);
        assert_eq!(
            parser
                .push(b"data: way-too-long-line")
                .expect_err("oversized"),
            SseParseError::Oversized
        );
    }
}
