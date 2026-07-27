//! Small dependency-free benchmark runner that emits machine-readable JSON.

use std::{collections::BTreeSet, hint::black_box, time::Instant};

use agentmod_event_model::{
    EventClassification, EventEnvelope, EventMetadata, EventOrigin, EventScope,
};
use agentmod_expression_engine::{Expression, ExpressionLimits};
use agentmod_graph_engine::{CompilerLimits, GraphCacheInputs, compile};
use agentmod_primitives::{
    CancellationId, CausationId, CorrelationId, EventId, IdempotencyId, RequestId, Sequence,
    SessionId, TimestampMillis, Version,
};
use agentmod_protocol_support::{FrameHeader, FrameKind, WireFrame, decode_frame, encode_frame};
use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

const GRAPH: &str = r#"
format_version = 1
entry = "plan"

[budget]
max_steps = 100
max_tokens = 10000
max_cost_micros = 500000
max_duration_ms = 60000

[declarations]
capabilities = ["model", "tools"]
tools = ["filesystem.read"]
providers = ["mock"]

[[nodes]]
id = "plan"
kind = "model_call"
provider = "mock"
condition = "session.ready == true"
retry_limit = 2

[[nodes]]
id = "read"
kind = "tool_execution_gate"
tool = "filesystem.read"
read_scopes = ["workspace"]

[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "plan"
to = "read"
condition = "model.tool_requested"

[[edges]]
from = "read"
to = "done"
"#;

#[derive(Serialize)]
struct BenchmarkResult {
    name: &'static str,
    iterations: u32,
    elapsed_ns: u128,
    ns_per_operation: f64,
    operations_per_second: f64,
}

fn measure(name: &'static str, iterations: u32, mut operation: impl FnMut()) -> BenchmarkResult {
    for _ in 0..iterations.min(100) {
        operation();
    }
    let started = Instant::now();
    for _ in 0..iterations {
        operation();
    }
    let elapsed = started.elapsed();
    let elapsed_ns = elapsed.as_nanos();
    let ns_per_operation = elapsed.as_secs_f64() * 1_000_000_000.0 / f64::from(iterations);
    BenchmarkResult {
        name,
        iterations,
        elapsed_ns,
        ns_per_operation,
        operations_per_second: 1_000_000_000.0 / ns_per_operation,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let event_metadata = metadata();
    let sealed = EventEnvelope::seal(event_metadata.clone(), json!({"text": "benchmark"}))?;
    let frame = frame();
    let encoded = encode_frame(&frame, 1024 * 1024)?;
    let expression_source = "session.ready == true && retry.count < 3 && exists(tool.result)";
    let expression_environment = json!({
        "session": {"ready": true},
        "retry": {"count": 1},
        "tool": {"result": "ok"}
    });
    let parsed_expression = Expression::parse(expression_source, ExpressionLimits::default())?;
    let graph_inputs = GraphCacheInputs {
        plugin_set_hash: agentmod_primitives::ContentHash::digest(b"plugins"),
        runtime_api_version: "1.0".into(),
        capability_set: BTreeSet::from(["model".into(), "tools".into()]),
    };

    let results = vec![
        measure("event_seal", 100_000, || {
            black_box(
                EventEnvelope::seal(event_metadata.clone(), json!({"text": "benchmark"}))
                    .expect("seal"),
            );
        }),
        measure("event_verify", 250_000, || {
            black_box(&sealed).verify().expect("verify");
        }),
        measure("protocol_cbor_round_trip", 50_000, || {
            let encoded = encode_frame(&frame, 1024 * 1024).expect("encode");
            black_box(decode_frame::<serde_json::Value>(&encoded, 1024 * 1024).expect("decode"));
        }),
        measure("expression_parse", 100_000, || {
            black_box(
                Expression::parse(expression_source, ExpressionLimits::default()).expect("parse"),
            );
        }),
        measure("expression_evaluate", 500_000, || {
            black_box(
                parsed_expression
                    .evaluate(&expression_environment)
                    .expect("evaluate"),
            );
        }),
        measure("graph_compile", 10_000, || {
            black_box(compile(GRAPH, &graph_inputs, CompilerLimits::default()).expect("compile"));
        }),
        measure("protocol_decode_only", 100_000, || {
            black_box(decode_frame::<serde_json::Value>(&encoded, 1024 * 1024).expect("decode"));
        }),
    ];
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema_version": 1,
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "target_os": std::env::consts::OS,
            "target_arch": std::env::consts::ARCH,
            "results": results,
        }))?
    );
    Ok(())
}

fn metadata() -> EventMetadata {
    EventMetadata {
        event_id: EventId::from_uuid(Uuid::from_u128(1)),
        scope: EventScope::Session(SessionId::from_uuid(Uuid::from_u128(2))),
        sequence: Sequence::FIRST,
        timestamp: TimestampMillis::new(1_700_000_000_000),
        event_type: "benchmark.event".into(),
        event_version: Version::new(1, 0),
        correlation_id: CorrelationId::from_uuid(Uuid::from_u128(3)),
        causation_id: CausationId::from_uuid(Uuid::from_u128(4)),
        parent_graph_node_id: None,
        origin: EventOrigin {
            subsystem: "benchmarks".into(),
            plugin: None,
        },
        schema_version: Version::new(1, 0),
        artifacts: vec![],
        classification: EventClassification::Committed,
    }
}

fn frame() -> WireFrame<serde_json::Value> {
    WireFrame {
        header: FrameHeader {
            family: "benchmark".into(),
            version: Version::new(1, 0),
            kind: FrameKind::Request,
            request_id: RequestId::from_uuid(Uuid::from_u128(5)),
            stream_sequence: None,
            correlation_id: CorrelationId::from_uuid(Uuid::from_u128(6)),
            causation_id: CausationId::from_uuid(Uuid::from_u128(7)),
            idempotency_id: IdempotencyId::from_uuid(Uuid::from_u128(8)),
            cancellation_id: Some(CancellationId::from_uuid(Uuid::from_u128(9))),
        },
        payload: json!({
            "operation": "benchmark",
            "arguments": {"path": "src/lib.rs", "limit": 100}
        }),
    }
}
