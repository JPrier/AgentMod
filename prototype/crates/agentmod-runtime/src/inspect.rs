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
            println!("{}", serde_json::to_string_pretty(&store.index).map_err(|e| e.to_string())?);
            return Ok(());
        }
        println!("{:<7} {:<12} {:>6} {:<8} title", "session", "definition", "seq", "active");
        for e in store.index.sessions.values() {
            println!("{:<7} {:<12} {:>6} {:<8} {}", e.session_id, e.definition, e.last_sequence, e.active, e.title.as_deref().unwrap_or(""));
        }
        return Ok(());
    };
    let records = store.read(sid)?;
    let view = project(&records);
    if as_json {
        println!("{}", serde_json::to_string_pretty(&view).map_err(|e| e.to_string())?);
        return Ok(());
    }
    println!("{} · definition `{}` · config {} · {} records · state {:?}", view.session_id, view.definition, view.config, view.records, view.state);
    for e in &view.events {
        println!("#{:<4} {:<22} {:<9} {}", e.sequence, e.event_name, e.status, json!(e.payload).to_string().chars().take(80).collect::<String>());
        for i in &e.invocations {
            let out = match &i.outcome {
                Some(Outcome::Ok) => "ok".to_owned(),
                Some(Outcome::Veto { reason }) => format!("veto: {reason}"),
                Some(Outcome::Failed { error }) => format!("failed: {error}"),
                Some(Outcome::Cancelled { reason }) => format!("cancelled: {reason}"),
                None => "open".to_owned(),
            };
            let publ = if i.published.is_empty() { String::new() } else { format!(" → {}", i.published.join(", ")) };
            println!("        {:?} {:<18} {}{}", i.mode, i.plugin, out, publ);
        }
    }
    for b in &view.blocked {
        println!("blocked #{} {} {}: {}", b.sequence, b.plugin, b.event_name, b.reason);
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
                return Err(format!("{sid}: sequence gap at record {} (found {})", i + 1, r.sequence));
            }
        }
        k.load_session(&records).map_err(|e| format!("{sid}: {e}"))?;
        let view = project(&records);
        let status = k.status(sid).ok_or(format!("{sid}: not loaded"))?;
        if status.last_sequence != view.last_sequence {
            return Err(format!("{sid}: kernel and projection disagree on last sequence"));
        }
        if k.context(sid).unwrap_or_default() != view.context {
            return Err(format!("{sid}: kernel and projection disagree on context"));
        }
        records_total += records.len();
        n += 1;
    }
    println!("verified {n} session log(s), {records_total} record(s): dense sequences, replayable, kernel == projection");
    Ok(())
}
