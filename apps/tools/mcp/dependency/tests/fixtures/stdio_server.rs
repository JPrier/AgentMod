use std::io::{self, BufRead, Write};

use serde_json::{Value, json};

fn main() {
    let stdin = io::stdin();
    let mut reader = stdin.lock();
    let mut stdout = io::stdout().lock();
    loop {
        let Some(message) = read_message(&mut reader) else {
            break;
        };
        let method = message["method"].as_str().unwrap_or_default();
        let Some(id) = message.get("id").cloned() else {
            continue;
        };
        let result = match method {
            "initialize" => json!({
                "protocolVersion":"2025-06-18",
                "capabilities":{"tools":{}},
                "serverInfo":{"name":"fixture","version":"1"}
            }),
            "tools/list" => json!({"tools":[{
                "name":"echo",
                "description":"Echo",
                "inputSchema":{"type":"object"}
            }]}),
            "resources/list" => json!({"resources":[]}),
            "prompts/list" => json!({"prompts":[]}),
            "tools/call" => {
                write_message(
                    &mut stdout,
                    &json!({
                        "jsonrpc":"2.0",
                        "method":"notifications/progress",
                        "params":{"progressToken":"p","progress":1,"total":1}
                    }),
                );
                json!({"content":[{"type":"text","text":"echoed"}]})
            }
            _ => json!({}),
        };
        write_message(
            &mut stdout,
            &json!({"jsonrpc":"2.0","id":id,"result":result}),
        );
    }
}

fn read_message(reader: &mut impl BufRead) -> Option<Value> {
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
    serde_json::from_slice(&bytes).ok()
}

fn write_message(writer: &mut impl Write, value: &Value) {
    let bytes = serde_json::to_vec(value).expect("json");
    write!(writer, "Content-Length: {}\r\n\r\n", bytes.len()).expect("header");
    writer.write_all(&bytes).expect("body");
    writer.flush().expect("flush");
}
