use std::io::{self, BufRead, Write};

fn main() {
    let stdin = io::stdin();
    let mut reader = stdin.lock();
    let mut stdout = io::stdout().lock();
    while let Some(message) = read_message(&mut reader) {
        let Some(id) = extract_string(&message, "id") else {
            continue;
        };
        let method = extract_string(&message, "method").unwrap_or_default();
        let result = match method.as_str() {
            "initialize" => String::from(
                r#"{"protocolVersion":"2025-06-18","capabilities":{"tools":{}},"serverInfo":{"name":"agentmod-e2e","version":"1"}}"#,
            ),
            "tools/list" => String::from(
                r#"{"tools":[{"name":"echo","description":"Echo fixture","inputSchema":{"type":"object"}}]}"#,
            ),
            "resources/list" => String::from(r#"{"resources":[]}"#),
            "prompts/list" => String::from(r#"{"prompts":[]}"#),
            "tools/call" => {
                write_message(
                    &mut stdout,
                    r#"{"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":"runtime-e2e","progress":1,"total":1}}"#,
                );
                String::from(r#"{"content":[{"type":"text","text":"echoed-through-runtime"}]}"#)
            }
            _ => String::from("{}"),
        };
        write_message(
            &mut stdout,
            &format!(r#"{{"jsonrpc":"2.0","id":"{id}","result":{result}}}"#),
        );
    }
}

fn read_message(reader: &mut impl BufRead) -> Option<String> {
    let mut length = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).ok()? == 0 {
            return None;
        }
        if line == "\r\n" || line == "\n" {
            break;
        }
        if let Some(value) = line.trim().strip_prefix("Content-Length:") {
            length = value.trim().parse::<usize>().ok();
        }
    }
    let mut bytes = vec![0; length?];
    reader.read_exact(&mut bytes).ok()?;
    String::from_utf8(bytes).ok()
}

fn extract_string(value: &str, key: &str) -> Option<String> {
    let marker = format!("\"{key}\":\"");
    let tail = value.split_once(&marker)?.1;
    let end = tail.find('"')?;
    Some(tail[..end].to_owned())
}

fn write_message(writer: &mut impl Write, value: &str) {
    let bytes = value.as_bytes();
    write!(writer, "Content-Length: {}\r\n\r\n", bytes.len()).expect("header");
    writer.write_all(bytes).expect("body");
    writer.flush().expect("flush");
}
