//! Deterministic one-request plugin worker used by process-level tests.
//!
//! Supported operations: initialize, migrate, intercept, observe, tool,
//! execute_node, memory_describe, memory_retrieve, memory_commit_write,
//! memory_health, compaction_propose, context_transform.
//!
//! A request may carry a `behavior` field selecting a deterministic failure
//! mode: `invalid`, `timeout`, `crash`, or `reject`.

use std::{
    fs::OpenOptions,
    io::{self, Read, Write},
    path::Path,
    thread,
    time::Duration,
};

use serde_json::{Value, json};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    let request: Value = serde_json::from_str(&input)?;
    let operation = request
        .get("operation")
        .and_then(Value::as_str)
        .ok_or("missing operation")?;
    let behavior = request.get("behavior").and_then(Value::as_str);
    match behavior {
        Some("invalid") => {
            println!("not-json{{");
            return Ok(());
        }
        Some("timeout") => {
            thread::sleep(Duration::from_secs(30));
            return Ok(());
        }
        Some("crash") => {
            std::process::exit(42);
        }
        _ => {}
    }
    let response = match operation {
        "initialize" => json!({"result":"ready"}),
        "migrate" => json!({
            "result":"state",
            "state":request.get("state").cloned().unwrap_or_else(|| json!({})),
        }),
        "intercept" => intercept(&request)?,
        "observe" => observe(&request)?,
        "tool" => json!({
            "result":"tool_result",
            "value":{"fixture":true},
        }),
        "execute_node" => json!({
            "result":"node_result",
            "value":{
                "node_id":request.get("node_id").cloned().unwrap_or_else(|| json!("node")),
                "executor_id":request.get("executor_id").cloned().unwrap_or_else(|| json!("fixture.node")),
                "ok":true,
            },
        }),
        "memory_describe" => json!({
            "result":"memory_describe",
            "scopes":["session","project"],
            "capabilities":["retrieve","write"],
            "bounded_bytes":1048576,
        }),
        "memory_retrieve" => json!({
            "result":"memory_retrieve",
            "items":[
                {
                    "reference":"fixture-item-1",
                    "content":"plugin-retrieved-memory",
                    "score":0.9,
                    "created_at_ms":1700000000000_i64,
                },
            ],
        }),
        "memory_commit_write" => json!({
            "result":"memory_commit_write",
            "retained":true,
            "references":["fixture-item-1"],
        }),
        "memory_health" => json!({
            "result":"memory_health",
            "healthy":true,
            "item_count":1,
            "retained_bytes":128,
        }),
        "compaction_propose" => json!({
            "result":"compaction_proposal_accepted",
            "replacement":{
                "entries":[
                    {"kind":"summary","text":"plugin-compacted"},
                ],
                "preserved":true,
            },
            "size_bytes":256,
        }),
        "context_transform" => json!({
            "result":"transform_result",
            "value":{
                "transform_id":request.get("transform_id").cloned().unwrap_or_else(|| json!("fixture.transform")),
                "applied":true,
            },
        }),
        _ => return Err("unsupported operation".into()),
    };
    println!("{}", serde_json::to_string(&response)?);
    Ok(())
}

fn observe(request: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let event_type = request
        .get("event_type")
        .and_then(Value::as_str)
        .ok_or("missing event type")?;
    let idempotency_key = request
        .get("idempotency_key")
        .and_then(Value::as_str)
        .unwrap_or(event_type);
    let marker_path = "fixture-observer-received.log";
    if idempotent_already_seen(marker_path, idempotency_key) {
        return Ok(json!({"result":"observed"}));
    }
    let mut marker = OpenOptions::new()
        .create(true)
        .append(true)
        .open(marker_path)?;
    writeln!(marker, "{event_type}:{idempotency_key}")?;
    Ok(json!({"result":"observed"}))
}

fn idempotent_already_seen(marker_path: &str, key: &str) -> bool {
    if !Path::new(marker_path).exists() {
        return false;
    }
    std::fs::read_to_string(marker_path)
        .map(|contents| {
            contents
                .lines()
                .any(|line| line.ends_with(&format!(":{key}")))
        })
        .unwrap_or(false)
}

fn intercept(request: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    if request.get("behavior").and_then(Value::as_str) == Some("reject") {
        return Ok(json!({"result":"reject","reason":"fixture rejection"}));
    }
    let mut proposal = request.get("proposal").cloned().ok_or("missing proposal")?;
    if request.get("handler").and_then(Value::as_str) == Some("rewrite-tool")
        && let Some(arguments) = proposal
            .pointer_mut("/action/details/arguments")
            .and_then(Value::as_object_mut)
    {
        arguments.insert(
            String::from("path"),
            Value::String(String::from("plugin-selected.txt")),
        );
    }
    Ok(json!({"result":"replace","proposal":proposal}))
}
