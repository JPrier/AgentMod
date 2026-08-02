//! Deterministic isolated worker used by correlated plugin-host process tests.

use std::{
    fs::OpenOptions,
    io::{Read, Write},
    sync::OnceLock,
    thread,
    time::Duration,
};

use serde_json::{Value, json};

static MARKER_PATH: OnceLock<String> = OnceLock::new();

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(path) = std::env::args().nth(1) {
        let _ = MARKER_PATH.set(path);
    }
    let mut input = Vec::new();
    std::io::stdin().read_to_end(&mut input)?;
    let request: Value = serde_json::from_slice(&input)?;
    match request.get("operation").and_then(Value::as_str) {
        Some("initialize") => write_response(&json!({"result":"ready"}))?,
        Some("memory_retrieve") => handle_retrieve(&request)?,
        Some("memory_write") => handle_write(&request)?,
        Some("intercept") => handle_intercept(&request)?,
        Some("node_executor") => handle_node(&request)?,
        Some("context_transform") => handle_context_transform(&request)?,
        _ => write_response(&json!({"result":"reject","reason":"unsupported"}))?,
    }
    Ok(())
}

fn handle_intercept(request: &Value) -> Result<(), Box<dyn std::error::Error>> {
    mark_dispatch(request)?;
    sleep_for_handler(request);
    write_response(&json!({
        "result":"continue",
        "proposal":request.get("proposal").cloned().unwrap_or(Value::Null)
    }))
}

fn handle_node(request: &Value) -> Result<(), Box<dyn std::error::Error>> {
    mark_dispatch(request)?;
    sleep_for_handler(request);
    write_response(&json!({
        "result":"node_outcome",
        "output":{"completed":true},
        "preserved_state":{"cursor":1},
        "proposed_actions":[]
    }))
}

fn handle_context_transform(request: &Value) -> Result<(), Box<dyn std::error::Error>> {
    mark_dispatch(request)?;
    sleep_for_handler(request);
    write_response(&json!({
        "result":"context_transform_proposal",
        "replacement":request.get("input").cloned().unwrap_or(Value::Null)
    }))
}

fn sleep_for_handler(request: &Value) {
    if request
        .get("handler")
        .and_then(Value::as_str)
        .is_some_and(|handler| handler.starts_with("slow_"))
    {
        thread::sleep(Duration::from_secs(30));
    }
}

fn handle_retrieve(request: &Value) -> Result<(), Box<dyn std::error::Error>> {
    mark_dispatch(request)?;
    let handler = request.get("handler").and_then(Value::as_str).unwrap_or("");
    if handler == "slow_retrieve" {
        thread::sleep(Duration::from_secs(30));
    } else if handler == "delayed_retrieve" {
        thread::sleep(Duration::from_millis(250));
    }
    write_response(&json!({
        "result":"memory_retrieved",
        "binding":request.get("binding").cloned().unwrap_or(Value::Null),
        "provider_id":request.get("provider_id").cloned().unwrap_or(Value::Null),
        "provider_version":request.get("provider_version").cloned().unwrap_or(Value::Null),
        "items":[]
    }))
}

fn handle_write(request: &Value) -> Result<(), Box<dyn std::error::Error>> {
    mark_dispatch(request)?;
    thread::sleep(Duration::from_secs(30));
    write_response(&json!({
        "result":"memory_written",
        "binding":request.get("binding").cloned().unwrap_or(Value::Null),
        "provider_id":request.get("provider_id").cloned().unwrap_or(Value::Null),
        "provider_version":request.get("provider_version").cloned().unwrap_or(Value::Null),
        "provider_record_id":"should-not-complete",
        "value_hash":request
            .get("request")
            .and_then(|value| value.get("value_hash"))
            .cloned()
            .unwrap_or(Value::Null),
        "receipt":{}
    }))
}

fn mark_dispatch(request: &Value) -> Result<(), Box<dyn std::error::Error>> {
    let path = request
        .get("configuration")
        .and_then(|configuration| configuration.get("marker_path"))
        .and_then(Value::as_str)
        .or_else(|| MARKER_PATH.get().map(String::as_str));
    let Some(path) = path else {
        return Ok(());
    };
    let invocation_id = request
        .get("binding")
        .and_then(|binding| binding.get("invocation_id"))
        .and_then(Value::as_str)
        .or_else(|| request.get("invocation_id").and_then(Value::as_str))
        .unwrap_or("unknown");
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{invocation_id}")?;
    file.flush()?;
    Ok(())
}

fn write_response(response: &Value) -> Result<(), Box<dyn std::error::Error>> {
    serde_json::to_writer(std::io::stdout(), response)?;
    Ok(())
}
