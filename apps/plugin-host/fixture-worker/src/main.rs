//! Deterministic one-request plugin worker used by process-level tests.

use std::{
    fs::OpenOptions,
    io::{self, Read, Write},
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
    let mut marker = OpenOptions::new()
        .create(true)
        .append(true)
        .open("fixture-observer-received.log")?;
    writeln!(marker, "{event_type}")?;
    Ok(json!({"result":"observed"}))
}

fn intercept(request: &Value) -> Result<Value, Box<dyn std::error::Error>> {
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
