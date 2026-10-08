//! Replay-as-reading: inspect and verify logs without running any plugin.

use std::path::Path;

use agentmod_core::kernel::Kernel;
use agentmod_core::metrics::session_metrics;
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

/// Print metrics for one session or every session (derived from logs only).
///
/// # Errors
/// Store failures or an unknown session.
pub fn metrics(
    data: &Path,
    session: Option<&str>,
    as_json: bool,
    turns: bool,
) -> Result<(), String> {
    use serde_json::Value;
    let store = Store::open(data, usize::MAX)?;
    let ids: Vec<String> = match session {
        Some(s) => vec![s.to_owned()],
        None => store.index.sessions.keys().cloned().collect(),
    };
    let mut out = serde_json::Map::new();
    for sid in ids {
        let records = store.read(&sid)?;
        let mut m = session_metrics(&records);
        if !turns && let Some(o) = m.as_object_mut() {
            o.remove("turns");
        }
        out.insert(sid, m);
    }
    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&out).map_err(|e| e.to_string())?
        );
        return Ok(());
    }
    for (sid, m) in &out {
        let g = |k: &str| m.get(k).and_then(Value::as_f64).unwrap_or(0.0);
        let r = |k: &str| {
            m.pointer(&format!("/ratios/{k}"))
                .and_then(Value::as_f64)
                .map_or_else(|| "-".to_owned(), |x| format!("{x}"))
        };
        println!(
            "{sid}: {} model requests, {} in ({} cached, {} uncached) / {} out tokens, ${:.4}, {} tool calls ({} errors), {} edits, {} prompts, {} children, max context ~{} tokens, {:.1}s",
            g("model_requests"),
            g("input_tokens"),
            g("cached_tokens"),
            g("uncached_input_tokens"),
            g("output_tokens"),
            g("cost"),
            g("tool_calls"),
            g("tool_errors"),
            g("edits"),
            g("permission_prompts"),
            g("child_agents"),
            g("max_context_tokens"),
            g("wall_ms") / 1000.0
        );
        println!(
            "  control plane: {} records / {} bytes, {} events ({} semantic, {} stream-chunk), {} pipelines, {} invocations ({} blocking, {} async, {} no-op), tool dispatch: {} owner / {} broadcast",
            g("records"),
            g("record_bytes"),
            g("events"),
            g("semantic_events"),
            g("stream_chunk_events"),
            g("pipeline_starts"),
            g("plugin_invocations"),
            g("blocking_invocations"),
            g("async_invocations"),
            g("noop_invocations"),
            g("exact_owner_dispatches"),
            g("candidate_dispatches"),
        );
        println!(
            "  streams: {} provider events -> {} frames -> {} live frames; pipelines/event {}, invocations/tool call {}, live frames/provider event {}, events/model response {}, journal bytes/useful byte {}",
            g("stream_provider_events"),
            g("stream_frames"),
            g("stream_live_frames"),
            r("pipelines_per_semantic_event"),
            r("plugin_invocations_per_tool_call"),
            r("live_frames_per_provider_event"),
            r("canonical_events_per_model_response"),
            r("journal_bytes_per_useful_output_byte"),
        );
        if let Some(ts) = m.get("turns").and_then(Value::as_array) {
            println!(
                "  {:>4} {:>8} {:>7} {:>9} {:>6} {:>9} {:>11} {:>5} {:>8}",
                "turn",
                "at s",
                "records",
                "bytes",
                "events",
                "pipelines",
                "invocations",
                "tools",
                "context"
            );
            for (i, t) in ts.iter().enumerate() {
                let n = |k: &str| t.get(k).and_then(Value::as_f64).unwrap_or(0.0);
                println!(
                    "  {:>4} {:>8.1} {:>7} {:>9} {:>6} {:>9} {:>11} {:>5} {:>8}",
                    i + 1,
                    n("at_ms") / 1000.0,
                    n("records"),
                    n("record_bytes"),
                    n("events"),
                    n("pipelines"),
                    n("invocations"),
                    n("tool_calls"),
                    t.pointer("/model/context_tokens")
                        .and_then(Value::as_f64)
                        .unwrap_or(0.0),
                );
            }
        }
    }
    Ok(())
}
