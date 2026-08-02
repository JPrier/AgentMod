//! Deterministic one-request plugin worker used by process-level tests.

use std::{
    fs::OpenOptions,
    io::{self, Read, Write},
};

use agentmod_primitives::ContentHash;
use serde_json::{Value, json};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let marker_path = match (
        arguments.next().as_deref(),
        arguments.next(),
        arguments.next(),
    ) {
        (Some("--memory-marker"), Some(path), None) if !path.is_empty() => Some(path),
        (None, None, None) => None,
        _ => return Err("invalid fixture-worker arguments".into()),
    };
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    let request: Value = serde_json::from_str(&input)?;
    let operation = request
        .get("operation")
        .and_then(Value::as_str)
        .ok_or("missing operation")?;
    if let Some(path) = marker_path.as_deref() {
        if matches!(operation, "memory_retrieve" | "memory_write" | "compaction") {
            mark_memory_invocation_at(&request, path);
        } else {
            mark_worker_start(operation, path);
        }
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
        "node_executor" => node_executor(&request)?,
        "context_transform" => context_transform(&request)?,
        "memory_retrieve" => memory_retrieve(&request),
        "memory_write" => memory_write(&request),
        "compaction" => compaction(&request),
        _ => return Err("unsupported operation".into()),
    };
    println!("{}", serde_json::to_string(&response)?);
    Ok(())
}

fn memory_retrieve(request: &Value) -> Value {
    mark_memory_invocation(request);
    match request.get("handler").and_then(Value::as_str) {
        Some("timeout_memory_retrieve") => {
            std::thread::sleep(std::time::Duration::from_millis(250));
        }
        Some("crash_memory_retrieve") => std::process::exit(17),
        Some("invalid_memory_retrieve") => {
            return json!({
                "result":"memory_retrieved",
                "binding":request.get("binding"),
                "provider_id":request.get("provider_id"),
                "provider_version":request.get("provider_version"),
                "items":[],
                "undeclared":"must-fail-closed",
            });
        }
        _ => {}
    }
    let value = json!({"memory":"typed process fixture"});
    let value_hash = ContentHash::digest(
        &serde_json::to_vec(&value).expect("fixture memory value serialization"),
    );
    json!({
        "result":"memory_retrieved",
        "binding":request.get("binding"),
        "provider_id":request.get("provider_id"),
        "provider_version":request.get("provider_version"),
        "items":[{
            "item_id":"fixture-memory-1",
            "scope":"session",
            "value":value,
            "value_hash":value_hash,
            "artifacts":[],
            "references":[],
            "security_classification":"private",
            "metadata":{"source":"fixture-worker"}
        }],
    })
}

fn memory_write(request: &Value) -> Value {
    mark_memory_invocation(request);
    if matches!(
        request.get("handler").and_then(Value::as_str),
        Some("timeout_memory_write" | "ambiguous_memory_write")
    ) {
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    if request.get("handler").and_then(Value::as_str) == Some("invalid_memory_write") {
        return json!({"result":"tool_result","value":{"invalid":true}});
    }
    if request.get("handler").and_then(Value::as_str) == Some("invalid_record_memory_write") {
        return memory_write_response(
            request,
            "invalid record id",
            &json!({"accepted":true}),
            None,
        );
    }
    if request.get("handler").and_then(Value::as_str) == Some("invalid_receipt_memory_write") {
        return memory_write_response(request, "fixture-record", &json!("wrong-shape"), None);
    }
    if request.get("handler").and_then(Value::as_str) == Some("oversized_receipt_memory_write") {
        return memory_write_response(
            request,
            "fixture-record",
            &json!({"accepted":true,"padding":"x".repeat(512 * 1024)}),
            None,
        );
    }
    if request.get("handler").and_then(Value::as_str) == Some("wrong_hash_memory_write") {
        return memory_write_response(
            request,
            "fixture-record",
            &json!({"accepted":true}),
            Some(Value::String("11".repeat(32))),
        );
    }
    if request.get("handler").and_then(Value::as_str) == Some("wrong_identity_memory_write") {
        let mut response =
            memory_write_response(request, "fixture-record", &json!({"accepted":true}), None);
        response["provider_id"] = Value::String(String::from("substituted.provider"));
        return response;
    }
    memory_write_response(request, "fixture-record", &json!({"accepted":true}), None)
}

fn memory_write_response(
    request: &Value,
    provider_record_id: &str,
    receipt: &Value,
    value_hash: Option<Value>,
) -> Value {
    json!({
        "result":"memory_written",
        "binding":request.get("binding"),
        "provider_id":request.get("provider_id"),
        "provider_version":request.get("provider_version"),
        "provider_record_id":provider_record_id,
        "value_hash":value_hash.unwrap_or_else(|| {
            request.pointer("/request/value_hash").cloned().unwrap_or(Value::Null)
        }),
        "receipt":receipt,
    })
}

fn compaction(request: &Value) -> Value {
    mark_memory_invocation(request);
    if request.get("handler").and_then(Value::as_str) == Some("timeout_compaction") {
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    if request.get("handler").and_then(Value::as_str) == Some("invalid_compaction") {
        return json!({
            "result":"compaction_proposed",
            "binding":request.get("binding"),
            "compactor_id":request.get("compactor_id"),
            "compactor_version":request.get("compactor_version"),
            "replacement":{},
            "replacement_hash":"not-a-hash",
            "preserved_references":[],
            "preserved_artifacts":[],
        });
    }
    let replacement = request
        .pointer("/readable_state/recorded_runtime_values/canonical_projection")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let replacement_hash = ContentHash::digest(
        &serde_json::to_vec(&replacement).expect("fixture compaction serialization"),
    );
    json!({
        "result":"compaction_proposed",
        "binding":request.get("binding"),
        "compactor_id":request.get("compactor_id"),
        "compactor_version":request.get("compactor_version"),
        "replacement":replacement,
        "replacement_hash":replacement_hash,
        "preserved_references":request.pointer("/request/required_references"),
        "preserved_artifacts":request.pointer("/request/required_artifacts"),
    })
}

fn mark_memory_invocation(request: &Value) {
    let Some(path) = request
        .pointer("/configuration/marker_path")
        .and_then(Value::as_str)
    else {
        return;
    };
    if let Ok(mut marker) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(
            marker,
            "{}",
            request
                .get("handler")
                .and_then(Value::as_str)
                .unwrap_or("missing")
        );
    }
}

fn mark_memory_invocation_at(request: &Value, path: &str) {
    if let Ok(mut marker) = OpenOptions::new().create(true).append(true).open(path) {
        let invocation_id = request
            .pointer("/binding/invocation_id")
            .and_then(Value::as_str)
            .unwrap_or("missing");
        let handler = request
            .get("handler")
            .and_then(Value::as_str)
            .unwrap_or("missing");
        let _ = writeln!(marker, "{invocation_id}|{handler}");
    }
}

fn mark_worker_start(operation: &str, path: &str) {
    if let Ok(mut marker) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(marker, "worker-start|{operation}");
    }
}

fn mark_node_invocation(request: &Value) -> Result<(), Box<dyn std::error::Error>> {
    let mut marker = OpenOptions::new()
        .create(true)
        .append(true)
        .open("fixture-node-invocations.log")?;
    writeln!(
        marker,
        "{}",
        request
            .get("invocation_id")
            .and_then(Value::as_str)
            .unwrap_or("missing")
    )?;
    Ok(())
}

fn timeout_node_effect(request: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    std::thread::sleep(std::time::Duration::from_millis(3_000));
    let mut late_effect = OpenOptions::new()
        .create(true)
        .append(true)
        .open("fixture-node-late-effects.log")?;
    writeln!(
        late_effect,
        "{}",
        request
            .get("invocation_id")
            .and_then(Value::as_str)
            .unwrap_or("missing")
    )?;
    Ok(json!({
        "result":"node_outcome",
        "output":{"late":true},
        "preserved_state":{},
        "proposed_actions":[{"kind":"tool.call","payload":{"tool":"fixture.effect"}}],
    }))
}

fn node_executor(request: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    mark_node_invocation(request)?;
    Ok(match request.get("handler").and_then(Value::as_str) {
        Some("graph_success") => json!({
            "result":"node_outcome",
            "output":{
                "variables":{},
                "variable_versions":{},
                "transition":"renamed_done",
                "artifact_references":[],
                "budget_usage":{
                    "steps":1,
                    "tokens":0,
                    "cost_micros":0,
                    "duration_ms":0
                }
            },
            "preserved_state":{"cursor":1,"status":"ready"},
            "proposed_actions":[],
        }),
        Some("graph_action") => json!({
            "result":"node_outcome",
            "output":{
                "variables":{},
                "variable_versions":{},
                "transition":"renamed_done",
                "artifact_references":[],
                "budget_usage":{
                    "steps":1,
                    "tokens":0,
                    "cost_micros":0,
                    "duration_ms":0
                }
            },
            "preserved_state":{"cursor":1,"status":"action-ready"},
            "proposed_actions":[{
                "kind":"tool.call",
                "payload":{
                    "tool":"filesystem.read",
                    "arguments":{"path":"README.md"}
                }
            }],
        }),
        Some("graph_action_artifact") => {
            let artifact = request
                .pointer("/readable_state/seed_artifact")
                .cloned()
                .unwrap_or(Value::Null);
            json!({
                "result":"node_outcome",
                "output":{
                    "variables":{},
                    "variable_versions":{},
                    "transition":"renamed_done",
                    "artifact_references":[artifact],
                    "budget_usage":{
                        "steps":1,
                        "tokens":0,
                        "cost_micros":0,
                        "duration_ms":0
                    }
                },
                "preserved_state":{"cursor":1,"status":"action-artifact-ready"},
                "proposed_actions":[{
                    "kind":"tool.call",
                    "payload":{
                        "tool":"filesystem.read",
                        "arguments":{"path":"README.md"}
                    }
                }],
            })
        }
        Some("invalid_output") => json!({
            "result":"node_outcome",
            "output":"invalid",
            "preserved_state":{},
            "proposed_actions":[],
        }),
        Some("timeout_effect") => timeout_node_effect(request)?,
        _ => json!({
            "result":"node_outcome",
            "output":{
                "fixture":true,
                "executor_id":request.get("executor_id"),
                "node_kind":request.get("node_kind"),
                "input":request.get("input"),
            },
            "preserved_state":request.get("readable_state").cloned().unwrap_or_else(|| json!({})),
            "proposed_actions":[],
        }),
    })
}

fn context_transform(request: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    if request.get("handler").and_then(Value::as_str) == Some("invalid_transform_response") {
        return Ok(json!({"result":"tool_result","value":{"invalid":true}}));
    }
    if request.get("handler").and_then(Value::as_str) == Some("timeout_transform") {
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    let replacement = request
        .pointer("/input/projection")
        .cloned()
        .ok_or("missing context projection")?;
    Ok(json!({
        "result":"context_transform_proposal",
        "replacement":replacement,
    }))
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
