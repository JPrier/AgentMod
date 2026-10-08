//! Replay-as-reading: inspect and verify logs without running any plugin.

use std::path::Path;

use agentmod_core::kernel::Kernel;
use agentmod_core::projection::project;
use agentmod_core::record::Outcome;
use serde_json::json;

use crate::store::Store;

/// Print the session table, or one session's event → invocation tree.
///
/// # Errors
/// Store failures or an unknown session.
pub fn inspect(data: &Path, session: Option<&str>, as_json: bool) -> Result<(), String> {
    let store = Store::open(data, usize::MAX)?;
    let Some(sid) = session else {
        if as_json {
            println!(
                "{}",
                serde_json::to_string_pretty(&store.index).map_err(|e| e.to_string())?
            );
            return Ok(());
        }
        println!(
            "{:<7} {:<12} {:>6} {:<8} title",
            "session", "definition", "seq", "active"
        );
        for e in store.index.sessions.values() {
            println!(
                "{:<7} {:<12} {:>6} {:<8} {}",
                e.session_id,
                e.definition,
                e.last_sequence,
                e.active,
                e.title.as_deref().unwrap_or("")
            );
        }
        return Ok(());
    };
    let records = store.read(sid)?;
    let view = project(&records);
    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&view).map_err(|e| e.to_string())?
        );
        return Ok(());
    }
    println!(
        "{} · definition `{}` · config {} · {} records · state {:?}",
        view.session_id, view.definition, view.config, view.records, view.state
    );
    for e in &view.events {
        println!(
            "#{:<4} {:<22} {:<9} {}",
            e.sequence,
            e.event_name,
            e.status,
            json!(e.payload)
                .to_string()
                .chars()
                .take(80)
                .collect::<String>()
        );
        for i in &e.invocations {
            let out = match &i.outcome {
                Some(Outcome::Ok) => "ok".to_owned(),
                Some(Outcome::Veto { reason }) => format!("veto: {reason}"),
                Some(Outcome::Failed { error }) => format!("failed: {error}"),
                Some(Outcome::Cancelled { reason }) => format!("cancelled: {reason}"),
                None => "open".to_owned(),
            };
            let publ = if i.published.is_empty() {
                String::new()
            } else {
                format!(" → {}", i.published.join(", "))
            };
            println!("        {:?} {:<18} {}{}", i.mode, i.plugin, out, publ);
        }
    }
    for b in &view.blocked {
        println!(
            "blocked #{} {} {}: {}",
            b.sequence, b.plugin, b.event_name, b.reason
        );
    }
    Ok(())
}

/// Rebuild every session through a fresh kernel and check log integrity.
///
/// # Errors
/// Any log that fails to replay.
pub fn verify(data: &Path) -> Result<(), String> {
    let store = Store::open(data, usize::MAX)?;
    let mut k = Kernel::new();
    let configs = store.compilations();
    for c in configs {
        let _ = k.install(c);
    }
    let mut n = 0;
    let mut records_total = 0;
    for sid in store.index.sessions.keys() {
        let records = store.read(sid)?;
        for (i, r) in records.iter().enumerate() {
            if r.sequence != i as u64 + 1 {
                return Err(format!(
                    "{sid}: sequence gap at record {} (found {})",
                    i + 1,
                    r.sequence
                ));
            }
        }
        k.load_session(&records)
            .map_err(|e| format!("{sid}: {e}"))?;
        let view = project(&records);
        let status = k.status(sid).ok_or(format!("{sid}: not loaded"))?;
        if status.last_sequence != view.last_sequence {
            return Err(format!(
                "{sid}: kernel and projection disagree on last sequence"
            ));
        }
        if k.context(sid).unwrap_or_default() != view.context {
            return Err(format!("{sid}: kernel and projection disagree on context"));
        }
        records_total += records.len();
        n += 1;
    }
    println!(
        "verified {n} session log(s), {records_total} record(s): dense sequences, replayable, kernel == projection"
    );
    Ok(())
}

/// Metrics derived from a session's log (no telemetry backend: the log is the record).
fn session_metrics(records: &[agentmod_core::record::Record]) -> serde_json::Value {
    use serde_json::Value;
    let view = project(records);
    let num = |v: Option<&Value>| v.and_then(Value::as_f64).unwrap_or(0.0);
    let mut m = serde_json::Map::new();
    let mut add = |k: &str, x: f64| {
        let cur = m.get(k).and_then(Value::as_f64).unwrap_or(0.0);
        m.insert(k.to_owned(), json!(cur + x));
    };
    let mut max_context = 0.0_f64;
    let mut schema_tokens = 0.0_f64;
    let mut first_edit: Option<u64> = None;
    let first_at = records.first().map_or(0, |r| r.at);
    for e in &view.events {
        let p = &e.payload;
        match e.event_name.as_str() {
            "model-response" => {
                let mt = p.get("metrics");
                add("model_requests", 1.0);
                add("input_tokens", num(mt.and_then(|x| x.get("input_tokens"))));
                add("output_tokens", num(mt.and_then(|x| x.get("output_tokens"))));
                add("cached_tokens", num(mt.and_then(|x| x.get("cached_tokens"))));
                add("cost", num(mt.and_then(|x| x.get("cost"))));
                add("model_latency_ms", num(mt.and_then(|x| x.get("latency_ms"))));
                add("provider_retries", num(mt.and_then(|x| x.get("retries"))));
                add("elided_tool_outputs", num(mt.and_then(|x| x.get("elided_tool_outputs"))));
                add("dropped_messages", num(mt.and_then(|x| x.get("dropped_messages"))));
                max_context = max_context.max(num(mt.and_then(|x| x.get("context_tokens"))));
                schema_tokens = schema_tokens.max(num(mt.and_then(|x| x.get("tool_schema_tokens"))));
            }
            "tool-call" if p.get("approved").is_none() => add("tool_calls", 1.0),
            "tool-result" => {
                if p.get("error").and_then(Value::as_bool) == Some(true) {
                    add("tool_errors", 1.0);
                }
                add("tool_time_ms", num(p.get("duration_ms")));
            }
            "workspace-change" => {
                add("edits", 1.0);
                first_edit.get_or_insert(e.at);
            }
            "approval-requested" => add("permission_prompts", 1.0),
            "user-input-requested" => add("questions", 1.0),
            "subagent-started" => add("child_agents", 1.0),
            "checkpoint-created" => add("checkpoints", 1.0),
            "workspace-restored" => add("restores", 1.0),
            "process-started" => add("processes", 1.0),
            "budget-exhausted" => add("budget_exhausted", 1.0),
            _ => {}
        }
        for i in &e.invocations {
            if i.attempts > 1 {
                add("recovered_invocations", 1.0);
            }
        }
    }
    m.insert("max_context_tokens".into(), json!(max_context));
    m.insert("tool_schema_tokens".into(), json!(schema_tokens));
    m.insert(
        "wall_ms".into(),
        json!(records.last().map_or(0, |r| r.at.saturating_sub(first_at))),
    );
    m.insert(
        "time_to_first_edit_ms".into(),
        first_edit.map_or(Value::Null, |t| json!(t.saturating_sub(first_at))),
    );
    m.insert("records".into(), json!(records.len()));
    Value::Object(m)
}

/// Print metrics for one session or every session (derived from logs only).
///
/// # Errors
/// Store failures or an unknown session.
pub fn metrics(data: &Path, session: Option<&str>, as_json: bool) -> Result<(), String> {
    let store = Store::open(data, usize::MAX)?;
    let ids: Vec<String> = match session {
        Some(s) => vec![s.to_owned()],
        None => store.index.sessions.keys().cloned().collect(),
    };
    let mut out = serde_json::Map::new();
    for sid in ids {
        let records = store.read(&sid)?;
        out.insert(sid, session_metrics(&records));
    }
    if as_json {
        println!("{}", serde_json::to_string_pretty(&out).map_err(|e| e.to_string())?);
        return Ok(());
    }
    for (sid, m) in &out {
        let g = |k: &str| m.get(k).and_then(serde_json::Value::as_f64).unwrap_or(0.0);
        println!(
            "{sid}: {} model requests, {} in / {} out tokens ({} cached), ${:.4}, {} tool calls ({} errors), {} edits, {} prompts, {} children, max context ≈{} tokens, schema ≈{} tokens, {:.1}s",
            g("model_requests"), g("input_tokens"), g("output_tokens"), g("cached_tokens"), g("cost"),
            g("tool_calls"), g("tool_errors"), g("edits"), g("permission_prompts"), g("child_agents"),
            g("max_context_tokens"), g("tool_schema_tokens"), g("wall_ms") / 1000.0
        );
    }
    Ok(())
}
