//! Deterministic, credential-free LSP 3.17 fixture server used by adapter tests.

use std::collections::BTreeSet;
use std::io::{self, BufRead, BufReader, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde_json::{Value, json};

const MAX_FRAME: usize = 1024 * 1024;

#[allow(clippy::too_many_lines)]
fn main() {
    let output = Arc::new(Mutex::new(io::stdout()));
    let cancelled = Arc::new(Mutex::new(BTreeSet::<u64>::new()));
    let mut input = BufReader::new(io::stdin());
    let mut document_uri = String::new();

    while let Ok(Some(message)) = read_frame(&mut input) {
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            continue;
        };
        let id = message.get("id").and_then(Value::as_u64);
        match method {
            "initialize" => {
                if let Some(root) = message.pointer("/params/rootUri").and_then(Value::as_str) {
                    document_uri = format!("{root}/src/main.rs");
                }
                respond(
                    &output,
                    id,
                    json!({
                        "capabilities": {
                            "documentSymbolProvider": true,
                            "workspaceSymbolProvider": true,
                            "definitionProvider": true,
                            "referencesProvider": true,
                            "hoverProvider": true,
                            "signatureHelpProvider": true,
                            "renameProvider": true,
                            "documentFormattingProvider": true,
                            "codeActionProvider": true
                        }
                    }),
                );
            }
            "initialized" => {}
            "shutdown" => respond(&output, id, Value::Null),
            "exit" => break,
            "$/cancelRequest" => {
                if let Some(cancel_id) = message.pointer("/params/id").and_then(Value::as_u64) {
                    cancelled.lock().expect("cancel lock").insert(cancel_id);
                }
            }
            "textDocument/didOpen" => {
                if let Some(uri) = message
                    .pointer("/params/textDocument/uri")
                    .and_then(Value::as_str)
                {
                    uri.clone_into(&mut document_uri);
                }
                send(
                    &output,
                    &json!({
                        "jsonrpc":"2.0",
                        "method":"textDocument/publishDiagnostics",
                        "params":{
                            "uri":document_uri,
                            "diagnostics":[{
                                "range":range(),
                                "severity":2,
                                "code":"fixture",
                                "source":"agentmod-fixture",
                                "message":"deterministic diagnostic"
                            }]
                        }
                    }),
                );
            }
            "workspace/symbol" => {
                let query = message
                    .pointer("/params/query")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                if query == "crash-once"
                    && let Ok(marker) = std::env::var("AGENTMOD_LSP_FIXTURE_CRASH_MARKER")
                    && !std::path::Path::new(&marker).exists()
                {
                    let _ = std::fs::write(marker, b"crashed");
                    std::process::exit(17);
                }
                let output = Arc::clone(&output);
                let cancelled = Arc::clone(&cancelled);
                let uri = document_uri.clone();
                thread::spawn(move || {
                    if query == "slow" {
                        for _ in 0..100 {
                            if cancelled
                                .lock()
                                .expect("cancel lock")
                                .contains(&id.unwrap_or(0))
                            {
                                send(
                                    &output,
                                    &json!({
                                        "jsonrpc":"2.0",
                                        "id":id,
                                        "error":{"code":-32800,"message":"Request cancelled"}
                                    }),
                                );
                                return;
                            }
                            thread::sleep(Duration::from_millis(5));
                        }
                    }
                    respond(&output, id, json!([symbol(&uri)]));
                });
            }
            "textDocument/documentSymbol" => {
                respond(
                    &output,
                    id,
                    json!([{
                        "name":"main",
                        "detail":"fn main()",
                        "kind":12,
                        "range":range(),
                        "selectionRange":range()
                    }]),
                );
            }
            "textDocument/definition" | "textDocument/references" => {
                respond(&output, id, json!([location(&document_uri)]));
            }
            "textDocument/hover" => {
                respond(
                    &output,
                    id,
                    json!({"contents":{"kind":"markdown","value":"`fixture hover`"},"range":range()}),
                );
            }
            "textDocument/signatureHelp" => {
                respond(
                    &output,
                    id,
                    json!({"signatures":[{"label":"main()"}],"activeSignature":0,"activeParameter":0}),
                );
            }
            "textDocument/rename" => {
                respond(
                    &output,
                    id,
                    json!({"changes":{document_uri.clone():[{"range":range(),"newText":"renamed"}]}}),
                );
            }
            "textDocument/formatting" => {
                respond(
                    &output,
                    id,
                    json!([{"range":range(),"newText":"fn main() {}"}]),
                );
            }
            "textDocument/codeAction" => {
                respond(
                    &output,
                    id,
                    json!([{
                        "title":"Fixture fix",
                        "kind":"quickfix",
                        "edit":{"changes":{document_uri.clone():[{"range":range(),"newText":"fixed"}]}},
                        "command":{"title":"Describe command","command":"fixture.describe"}
                    }]),
                );
            }
            _ => {
                send(
                    &output,
                    &json!({
                        "jsonrpc":"2.0",
                        "id":id,
                        "error":{"code":-32601,"message":"Method not found"}
                    }),
                );
            }
        }
    }
}

fn symbol(uri: &str) -> Value {
    json!({"name":"main","kind":12,"location":location(uri)})
}

fn location(uri: &str) -> Value {
    json!({"uri":uri,"range":range()})
}

fn range() -> Value {
    json!({
        "start":{"line":0,"character":0},
        "end":{"line":0,"character":4}
    })
}

#[allow(clippy::needless_pass_by_value)]
fn respond(output: &Arc<Mutex<io::Stdout>>, id: Option<u64>, result: Value) {
    send(output, &json!({"jsonrpc":"2.0","id":id,"result":result}));
}

fn send(output: &Arc<Mutex<io::Stdout>>, value: &Value) {
    let body = serde_json::to_vec(value).expect("serialize fixture response");
    let mut output = output.lock().expect("stdout lock");
    write!(output, "Content-Length: {}\r\n\r\n", body.len()).expect("write fixture header");
    output.write_all(&body).expect("write fixture body");
    output.flush().expect("flush fixture response");
}

fn read_frame(reader: &mut impl BufRead) -> io::Result<Option<Value>> {
    let mut content_length = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            return Ok(None);
        }
        if line == "\r\n" {
            break;
        }
        if let Some(value) = line.strip_prefix("Content-Length:") {
            content_length = value.trim().parse::<usize>().ok();
        }
    }
    let length = content_length
        .filter(|length| *length <= MAX_FRAME)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid frame length"))?;
    let mut body = vec![0; length];
    reader.read_exact(&mut body)?;
    serde_json::from_slice(&body)
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}
