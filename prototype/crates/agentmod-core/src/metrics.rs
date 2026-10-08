//! Metrics derived from a session log alone (no telemetry backend: the log is
//! the record). Works on any log, including ones written before live streams
//! and keyed dispatch existed, so before/after comparisons use one definition.
//!
//! Three groups:
//! * **control plane** — what the runtime did per unit of semantic work:
//!   records and bytes, events, pipelines, invocations (blocking / async),
//!   owner vs broadcast tool dispatch, no-op invocations, stream frames;
//! * **model efficiency** — requests, tokens (cached / uncached), cost,
//!   retries, recoveries, compaction, context per turn, time to first edit;
//! * **ratios** — amplification: pipelines per event, plugin invocations per
//!   tool call, live frames per provider event, events per model response,
//!   journal bytes per useful output byte.
//!
//! Session and stream ids never become metric labels; per-turn rows carry the
//! event id for tracing.

use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

use crate::projection::project;
use crate::record::{Body, Outcome, Record};
use crate::types::Mode;

fn num(v: Option<&Value>) -> f64 {
    v.and_then(Value::as_f64).unwrap_or(0.0)
}

fn bytes_of(v: &Value) -> usize {
    match v {
        Value::String(s) => s.len(),
        Value::Null => 0,
        other => other.to_string().len(),
    }
}

/// Per-turn row: one model request and everything until the next one.
#[derive(Default)]
struct Turn {
    event_id: String,
    sequence: u64,
    at: u64,
    records: u64,
    record_bytes: u64,
    events: u64,
    pipelines: u64,
    invocations: u64,
    tool_calls: u64,
    model: Value,
}

/// Compute a session's metrics (see the module docs). `record_bytes` uses
/// each record's serialized JSON size, as stored (before spill).
#[must_use]
#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
pub fn session_metrics(records: &[Record]) -> Value {
    let view = project(records);
    let mut m: BTreeMap<&'static str, f64> = BTreeMap::new();
    let add = |m: &mut BTreeMap<&'static str, f64>, k: &'static str, x: f64| {
        *m.entry(k).or_insert(0.0) += x;
    };

    // ---------------- control plane, from records ----------------
    let mut event_names: BTreeMap<String, String> = BTreeMap::new();
    let mut inv_event: BTreeMap<String, String> = BTreeMap::new();
    let mut inv_routed: BTreeMap<String, bool> = BTreeMap::new();
    let mut by_name: BTreeMap<String, u64> = BTreeMap::new();
    let mut turns: Vec<Turn> = Vec::new();
    for r in records {
        let size = serde_json::to_string(r).map_or(0, |s| s.len() + 1) as f64;
        add(&mut m, "records", 1.0);
        add(&mut m, "record_bytes", size);
        if let Some(t) = turns.last_mut() {
            t.records += 1;
            t.record_bytes += size as u64;
        }
        match &r.body {
            Body::EventAppended { event } => {
                event_names.insert(event.event_id.clone(), event.event_name.clone());
                *by_name.entry(event.event_name.clone()).or_insert(0) += 1;
                add(&mut m, "events", 1.0);
                if event.event_name == "stream-chunk" {
                    // Pre-hub logs: every provider delta was a canonical event.
                    add(&mut m, "stream_chunk_events", 1.0);
                } else {
                    add(&mut m, "semantic_events", 1.0);
                }
                if event.event_name == "model-request" {
                    turns.push(Turn {
                        event_id: event.event_id.clone(),
                        sequence: r.sequence,
                        at: r.at,
                        records: 1,
                        record_bytes: size as u64,
                        ..Default::default()
                    });
                }
                if let Some(t) = turns.last_mut() {
                    t.events += 1;
                    if event.event_name == "tool-call" && event.payload.get("approved").is_none() {
                        t.tool_calls += 1;
                    }
                }
            }
            Body::PipelineStarted { .. } => {
                add(&mut m, "pipeline_starts", 1.0);
                if let Some(t) = turns.last_mut() {
                    t.pipelines += 1;
                }
            }
            Body::PipelineSettled { .. } => add(&mut m, "pipeline_settled", 1.0),
            Body::InvocationStarted {
                invocation_id,
                event_id,
                mode,
                route,
                ..
            } => {
                add(&mut m, "plugin_invocations", 1.0);
                match mode {
                    Mode::Blocking => add(&mut m, "blocking_invocations", 1.0),
                    Mode::Async => add(&mut m, "async_invocations", 1.0),
                }
                let name = event_names.get(event_id).cloned().unwrap_or_default();
                if name == "tool-call" {
                    add(&mut m, "tool_call_invocations", 1.0);
                    if route.is_some() {
                        add(&mut m, "exact_owner_dispatches", 1.0);
                    } else if *mode == Mode::Async {
                        // Delivered to a plugin that was not selected as owner:
                        // a broadcast candidate (or a pure observer).
                        add(&mut m, "candidate_dispatches", 1.0);
                    }
                }
                inv_event.insert(invocation_id.clone(), name);
                inv_routed.insert(invocation_id.clone(), route.is_some());
                if let Some(t) = turns.last_mut() {
                    t.invocations += 1;
                }
            }
            Body::InvocationCompleted {
                invocation_id,
                outcome,
                transform,
                contributions,
                published,
            } => {
                let noop = *outcome == Outcome::Ok
                    && transform.is_none()
                    && contributions.is_empty()
                    && published.is_empty();
                if noop {
                    add(&mut m, "noop_invocations", 1.0);
                    if inv_event.get(invocation_id).map(String::as_str) == Some("tool-call") {
                        add(&mut m, "noop_tool_dispatches", 1.0);
                    }
                }
                if matches!(outcome, Outcome::Failed { .. }) {
                    add(&mut m, "failed_invocations", 1.0);
                }
            }
            Body::InvocationRetried { .. } => add(&mut m, "invocation_retries", 1.0),
            _ => {}
        }
    }

    // ---------------- semantic + model efficiency, from the view ----------------
    let first_at = records.first().map_or(0, |r| r.at);
    let mut first_edit: Option<u64> = None;
    let mut context_per_turn: Vec<f64> = Vec::new();
    let mut useful = 0usize;
    let mut turn_i = 0usize;
    for e in &view.events {
        let p = &e.payload;
        match e.event_name.as_str() {
            "model-response" => {
                let mt = p.get("metrics");
                let g = |k: &str| num(mt.and_then(|x| x.get(k)));
                add(&mut m, "model_requests", 1.0);
                add(&mut m, "input_tokens", g("input_tokens"));
                add(&mut m, "output_tokens", g("output_tokens"));
                add(&mut m, "cached_tokens", g("cached_tokens"));
                add(&mut m, "reasoning_tokens", g("reasoning_tokens"));
                add(&mut m, "cost", g("cost"));
                add(&mut m, "model_latency_ms", g("latency_ms"));
                add(&mut m, "provider_retries", g("retries"));
                add(&mut m, "elided_tool_outputs", g("elided_tool_outputs"));
                add(&mut m, "dropped_messages", g("dropped_messages"));
                if g("elided_tool_outputs") > 0.0 || g("dropped_messages") > 0.0 {
                    add(&mut m, "compactions", 1.0);
                }
                context_per_turn.push(g("context_tokens"));
                let st = mt.and_then(|x| x.get("stream"));
                let s = |k: &str| num(st.and_then(|x| x.get(k)));
                add(&mut m, "stream_provider_events", s("provider_events"));
                add(&mut m, "stream_frames", s("frames"));
                add(&mut m, "stream_live_frames", s("live_frames"));
                add(&mut m, "stream_attempts", s("attempts"));
                add(&mut m, "stream_late_frames", s("late_frames"));
                add(&mut m, "stream_duplicate_frames", s("duplicate_frames"));
                add(&mut m, "recovery_writes", s("recovery_writes"));
                add(&mut m, "recovery_bytes", s("recovery_bytes"));
                useful += bytes_of(p.get("text").unwrap_or(&Value::Null));
                for c in p
                    .get("tool_calls")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    useful += bytes_of(c.get("args").unwrap_or(&Value::Null));
                }
                // Attach the response to its turn (requests and responses pair in order).
                if let Some(t) = turns.get_mut(turn_i) {
                    t.model = json!({
                        "input_tokens": mt.and_then(|x| x.get("input_tokens")),
                        "cached_tokens": mt.and_then(|x| x.get("cached_tokens")),
                        "output_tokens": mt.and_then(|x| x.get("output_tokens")),
                        "context_tokens": mt.and_then(|x| x.get("context_tokens")),
                        "tool_calls": p.get("tool_calls").and_then(Value::as_array).map(Vec::len),
                        "latency_ms": mt.and_then(|x| x.get("latency_ms")),
                    });
                }
                turn_i += 1;
            }
            "tool-call" if p.get("approved").is_none() => {
                add(&mut m, "tool_calls", 1.0);
                if p.get("name").and_then(Value::as_str) == Some("delegate") {
                    add(&mut m, "child_agent_calls", 1.0);
                }
            }
            "tool-result" => {
                if p.get("error").and_then(Value::as_bool) == Some(true) {
                    add(&mut m, "tool_errors", 1.0);
                }
                add(&mut m, "tool_time_ms", num(p.get("duration_ms")));
                useful += bytes_of(p.get("output").unwrap_or(&Value::Null));
            }
            "dispatch-failed" => add(&mut m, "dispatch_failures", 1.0),
            "workspace-change" => {
                add(&mut m, "edits", 1.0);
                first_edit.get_or_insert(e.at);
            }
            "approval-requested" => add(&mut m, "permission_prompts", 1.0),
            "user-input-requested" => add(&mut m, "questions", 1.0),
            "subagent-started" => add(&mut m, "child_agents", 1.0),
            "checkpoint-created" => add(&mut m, "checkpoints", 1.0),
            "workspace-restored" => add(&mut m, "restores", 1.0),
            "process-started" => add(&mut m, "processes", 1.0),
            "budget-exhausted" => add(&mut m, "budget_exhausted", 1.0),
            "plan-updated" => add(&mut m, "plan_updates", 1.0),
            _ => {}
        }
        for i in &e.invocations {
            if i.attempts > 1 {
                add(&mut m, "recovered_invocations", 1.0);
            }
        }
    }
    // Pre-hub logs: each stream-chunk was one provider event, one frame, and
    // one live delivery (through the whole pipeline).
    let chunks = m.get("stream_chunk_events").copied().unwrap_or(0.0);
    if chunks > 0.0 {
        add(&mut m, "stream_provider_events", chunks);
        add(&mut m, "stream_frames", chunks);
        add(&mut m, "stream_live_frames", chunks);
    }
    let g = |k: &str| m.get(k).copied().unwrap_or(0.0);
    let uncached = (g("input_tokens") - g("cached_tokens")).max(0.0);
    let ratio = |a: f64, b: f64| {
        if b > 0.0 {
            json!((a / b * 1000.0).round() / 1000.0)
        } else {
            Value::Null
        }
    };
    let ratios = json!({
        "pipelines_per_semantic_event": ratio(g("pipeline_starts"), g("semantic_events")),
        "invocations_per_semantic_event": ratio(g("plugin_invocations"), g("semantic_events")),
        "plugin_invocations_per_tool_call": ratio(g("tool_call_invocations"), g("tool_calls")),
        "live_frames_per_provider_event": ratio(g("stream_live_frames"), g("stream_provider_events")),
        "canonical_events_per_model_response": ratio(g("events"), g("model_requests")),
        "records_per_model_response": ratio(g("records"), g("model_requests")),
        "journal_bytes_per_useful_output_byte": ratio(g("record_bytes"), useful as f64),
        "noop_share_of_invocations": ratio(g("noop_invocations"), g("plugin_invocations")),
    });
    let mut out = Map::new();
    for (k, v) in &m {
        out.insert((*k).to_owned(), json!(v));
    }
    out.insert("uncached_input_tokens".into(), json!(uncached));
    out.insert("useful_output_bytes".into(), json!(useful));
    out.insert(
        "max_context_tokens".into(),
        json!(context_per_turn.iter().copied().fold(0.0_f64, f64::max)),
    );
    out.insert("context_tokens_per_turn".into(), json!(context_per_turn));
    out.insert(
        "wall_ms".into(),
        json!(records.last().map_or(0, |r| r.at.saturating_sub(first_at))),
    );
    out.insert(
        "time_to_first_edit_ms".into(),
        first_edit.map_or(Value::Null, |t| json!(t.saturating_sub(first_at))),
    );
    out.insert("events_by_name".into(), json!(by_name));
    out.insert("ratios".into(), ratios);
    out.insert(
        "turns".into(),
        json!(turns
            .iter()
            .map(|t| json!({
                "event_id": t.event_id, "sequence": t.sequence, "at_ms": t.at.saturating_sub(first_at),
                "records": t.records, "record_bytes": t.record_bytes, "events": t.events,
                "pipelines": t.pipelines, "invocations": t.invocations, "tool_calls": t.tool_calls,
                "model": t.model,
            }))
            .collect::<Vec<_>>()),
    );
    // Compatibility with earlier readers of `metrics --json`.
    out.insert("tool_schema_tokens".into(), json!(max_schema(&view)));
    Value::Object(out)
}

fn max_schema(view: &crate::projection::SessionView) -> f64 {
    view.events
        .iter()
        .filter(|e| e.event_name == "model-response")
        .map(|e| num(e.payload.pointer("/metrics/tool_schema_tokens")))
        .fold(0.0, f64::max)
}
