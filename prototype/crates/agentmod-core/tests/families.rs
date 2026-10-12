//! Event families with exactly one handler: tool calls are `tool-call/<name>`
//! events, and each member reaches exactly the one plugin the compiled graph
//! names as its handler (compiler.md §3.3), plus the family's observers.

mod common;

use std::collections::BTreeMap;

use agentmod_core::compiler::{Compilation, compile};
use agentmod_core::core_manifest::DISPATCH_FAILED;
use agentmod_core::kernel::{
    Effect, InvocationResult, InvokeRequest, Kernel, PublishRequest, Scope,
};
use agentmod_core::manifest::{DeploymentConfig, Manifest};
use agentmod_core::record::{Body, Outcome};
use agentmod_core::types::ContextOp;
use common::{S, Sim, deployment, k, opt};
use serde_json::{Value, json};

/// ui (frontend), agent (emits the `tool-call/*` family, collects results),
/// gate (blocking observer of the family), audit (async observer), and three
/// handlers: files (two members), shell (one), mcp (a sub-family).
fn manifests() -> BTreeMap<String, Manifest> {
    let call = json!({ "call_id": k(S), "name": k(S), "args": opt("object") });
    let handler = |events: &[&str]| -> Value {
        json!({
            "subscribe": events.iter().map(|e| json!({ "event": e, "mode": "async", "receives": call })).collect::<Vec<_>>(),
            "emit": [{ "event": "tool-result", "keys": { "call_id": k(S), "by": k(S) } }]
        })
    };
    common::manifests(json!({
        "ui": {
            "subscribe": [{ "event": "*", "mode": "async" }],
            "emit": [{ "event": "user-message", "deferred": true, "keys": { "text": k(S) } }],
            "capabilities": ["start-session", "deferred-publish"],
            "sink": true
        },
        "agent": {
            "subscribe": [
                { "event": "user-message", "mode": "blocking", "receives": { "text": k(S) } },
                { "event": "tool-result", "mode": "blocking", "actions": ["contribute-context"], "receives": { "call_id": k(S) } },
                { "event": DISPATCH_FAILED, "mode": "blocking", "actions": ["contribute-context"], "receives": { "reason": k(S), "member": k(S) } }
            ],
            "emit": [{ "event": "tool-call/*", "exactly_one_handler": true, "keys": { "call_id": k(S), "name": k(S) } }]
        },
        "gate": { "subscribe": [{ "event": "tool-call/*", "mode": "blocking", "receives": call }] },
        "audit": { "subscribe": [{ "event": "tool-call/*", "mode": "async" }], "sink": true },
        "files": handler(&["tool-call/read_file", "tool-call/list_dir"]),
        "shell": handler(&["tool-call/shell"]),
        "mcp": handler(&["tool-call/mcp__*"])
    }))
}

fn config(extra: Value) -> DeploymentConfig {
    let mut def = json!({
        "runtime": "0.1",
        "phases": ["guard", "loop"],
        "plugins": { "ui": {}, "agent": { "phase": "loop" }, "gate": { "phase": "guard" }, "audit": {}, "files": {}, "shell": {}, "mcp": {} }
    });
    if let (Some(d), Some(e)) = (def.as_object_mut(), extra.as_object()) {
        d.extend(e.clone());
    }
    deployment(&manifests(), json!({ "coder": def }))
}

fn compiled(cfg: &DeploymentConfig) -> Compilation {
    let c = compile(cfg, &manifests());
    assert!(c.ok, "{:#?}", c.errors());
    c
}

/// Each user message makes the agent call the tools named in its text; a
/// handler named in `fail` reports an error instead of answering.
fn behaviour(
    fail: Option<&'static str>,
) -> impl FnMut(&mut Sim, &str, &InvokeRequest) -> InvocationResult {
    move |sim: &mut Sim, plugin: &str, req: &InvokeRequest| {
        let ev = &req.event;
        match (plugin, ev.event_name.as_str()) {
            ("agent", "user-message") => {
                for (i, name) in ev.payload["text"]
                    .as_str()
                    .unwrap_or("")
                    .split_whitespace()
                    .enumerate()
                {
                    sim.emit(
                        "agent",
                        &req.invocation_id,
                        &format!("tool-call/{name}"),
                        json!({ "call_id": format!("{}:{i}", ev.event_id), "name": name }),
                    )
                    .unwrap();
                }
                InvocationResult::default()
            }
            ("agent", DISPATCH_FAILED) => InvocationResult {
                contributions: vec![ContextOp::Add {
                    slot: "answers".into(),
                    value: ev.payload["member"].clone(),
                }],
                ..Default::default()
            },
            ("files" | "shell" | "mcp", name) if name.starts_with("tool-call/") => {
                if fail == Some(plugin) {
                    return InvocationResult {
                        error: Some("boom".into()),
                        ..Default::default()
                    };
                }
                sim.emit(
                    plugin,
                    &req.invocation_id,
                    "tool-result",
                    json!({ "call_id": ev.payload["call_id"], "by": plugin }),
                )
                .unwrap();
                InvocationResult::default()
            }
            _ => InvocationResult::default(),
        }
    }
}

fn user(sim: &Sim, sid: &str, text: &str) -> PublishRequest {
    PublishRequest {
        plugin: "ui".into(),
        invocation_id: None,
        cite: Some(sim.first_invocation(sid, "ui")),
        event_name: "user-message".into(),
        payload: json!({ "text": text }),
        ui: None,
        lane: None,
        target_session: None,
    }
}

/// (event name, plugin, route) of every invocation in a session.
fn started(sim: &Sim, sid: &str) -> Vec<(String, String, Option<String>)> {
    let names: BTreeMap<String, String> = sim
        .log(sid)
        .iter()
        .filter_map(|r| match &r.body {
            Body::EventAppended { event } => {
                Some((event.event_id.clone(), event.event_name.clone()))
            }
            _ => None,
        })
        .collect();
    sim.log(sid)
        .iter()
        .filter_map(|r| match &r.body {
            Body::InvocationStarted {
                event_id,
                plugin,
                route,
                ..
            } => Some((names[event_id].clone(), plugin.clone(), route.clone())),
            _ => None,
        })
        .collect()
}

fn go(sim: &mut Sim, text: &str, fail: Option<&'static str>) -> String {
    let sid = sim.start(
        "ui",
        "coder",
        Some(("user-message", json!({ "text": text }))),
    );
    sim.run(&mut behaviour(fail));
    sid
}

#[test]
fn each_tool_call_reaches_exactly_its_handler() {
    let mut sim = Sim::new(compiled(&config(json!({}))));
    let sid = go(&mut sim, "read_file shell mcp__gh__issues list_dir", None);
    let calls: Vec<(String, String, Option<String>)> = started(&sim, &sid)
        .into_iter()
        .filter(|(e, _, _)| e.starts_with("tool-call/"))
        .collect();
    let handlers: Vec<(String, String)> = calls
        .iter()
        .filter_map(|(_, p, r)| r.clone().map(|r| (r, p.clone())))
        .collect();
    assert_eq!(
        handlers,
        vec![
            ("read_file".into(), "files".into()),
            ("shell".into(), "shell".into()),
            ("mcp__gh__issues".into(), "mcp".into()),
            ("list_dir".into(), "files".into()),
        ]
    );
    // Per call: gate (blocking), audit and ui (observers), one handler. No other tool plugin.
    assert_eq!(calls.len(), 4 * 4, "{calls:?}");
    let results = sim.payloads(&sid, "tool-result");
    assert_eq!(results.len(), 4);
    assert_eq!(results[1]["by"], "shell");
}

#[test]
fn unhandled_members_and_failed_handlers_become_dispatch_failed() {
    let mut sim = Sim::new(compiled(&config(json!({}))));
    let sid = go(&mut sim, "write_file shell", Some("shell"));
    let failed = sim.payloads(&sid, DISPATCH_FAILED);
    assert_eq!(failed.len(), 2, "{failed:#?}");
    assert_eq!(failed[0]["reason"], "no-handler");
    assert_eq!(failed[0]["member"], "write_file");
    assert_eq!(failed[0]["family"], "tool-call/*");
    assert_eq!(
        failed[0]["handled"],
        json!(["list_dir", "read_file", "shell", "mcp__*"])
    );
    assert_eq!(failed[1]["reason"], "handler-failed");
    assert_eq!(failed[1]["plugin"], "shell");
    assert_eq!(failed[1]["error"], "boom");
    // The loop got told about both calls instead of waiting forever.
    assert_eq!(
        sim.k
            .context(&sid)
            .unwrap()
            .iter()
            .filter(|c| c.slot == "answers")
            .count(),
        2
    );
    // No handler saw the unhandled call; observers still did.
    let wf: Vec<String> = started(&sim, &sid)
        .into_iter()
        .filter(|(e, _, _)| e == "tool-call/write_file")
        .map(|(_, p, _)| p)
        .collect();
    assert_eq!(wf, vec!["gate", "audit", "ui"]);
}

#[test]
fn handlers_change_only_at_a_revision_boundary_and_retries_keep_their_executor() {
    let mut sim = Sim::new(compiled(&config(json!({}))));
    let sid = go(&mut sim, "", None);
    sim.publish(user(&sim, &sid, "shell")).unwrap();
    // Hold the handler's invocation open.
    let mut held = None;
    let mut b = behaviour(None);
    while let Some((plugin, stamp, req)) = sim.pending.pop_front() {
        if plugin == "shell" {
            held = Some((req.invocation_id.clone(), stamp));
            continue;
        }
        let r = b(&mut sim, &plugin, &req);
        sim.complete(&req.invocation_id, r);
    }
    let (held_inv, held_stamp) = held.expect("shell handler dispatched");
    let held_rev = sim.k.invocation_revision(&held_inv).unwrap();
    // New config: the same plugin with a new config stamp (a hot swap of the handler).
    let mut cfg2 = config(json!({}));
    cfg2.plugins.get_mut("shell").unwrap().config = json!({ "v": 2 });
    let h2 = sim.k.install(compiled(&cfg2)).unwrap();
    sim.now += 1;
    let out = sim.k.apply_config(&h2, &Scope::Global, sim.now);
    sim.take(out.effects);
    // The plugin crashed: its retry goes to the executor recorded at dispatch.
    sim.now += 1;
    let fx = sim.k.retry(&held_inv, "crash", 3, sim.now);
    let retried = fx.iter().find_map(|f| match f {
        Effect::Invoke { stamp, .. } => Some(stamp.clone()),
        _ => None,
    });
    assert_eq!(retried, Some(held_stamp.clone()));
    assert_eq!(sim.k.invocation_revision(&held_inv).unwrap(), held_rev);
    sim.take(fx);
    sim.pending.clear();
    sim.complete(&held_inv, InvocationResult::default());
    sim.run(&mut behaviour(None));
    // The next call dispatches under the new revision's stamp.
    sim.publish(user(&sim, &sid, "shell")).unwrap();
    let mut new_stamp = None;
    while let Some((plugin, stamp, req)) = sim.pending.pop_front() {
        if plugin == "shell" {
            new_stamp = Some(stamp);
        }
        let r = b(&mut sim, &plugin, &req);
        sim.complete(&req.invocation_id, r);
    }
    let new_stamp = new_stamp.expect("dispatched");
    assert_ne!(new_stamp, held_stamp);
    assert_eq!(new_stamp.config, cfg2.plugins["shell"].stamp().config);
}

#[test]
fn family_logs_replay_exactly_and_record_the_member() {
    let c = compiled(&config(json!({})));
    let mut sim = Sim::new(c.clone());
    let sid = go(&mut sim, "read_file nope shell", Some("files"));
    let mut fresh = Kernel::new();
    fresh.install(c).unwrap();
    fresh.load_session(&sim.logs[&sid]).unwrap();
    assert_eq!(sim.k.status(&sid), fresh.status(&sid));
    assert_eq!(sim.k.context(&sid), fresh.context(&sid));
    // Every handler dispatch is recorded with the member that selected it.
    let routed: Vec<Option<String>> = started(&sim, &sid)
        .into_iter()
        .filter(|(e, p, _)| e.starts_with("tool-call/") && ["files", "shell"].contains(&p.as_str()))
        .map(|(_, _, r)| r)
        .collect();
    assert_eq!(routed, vec![Some("read_file".into()), Some("shell".into())]);
    assert!(sim.logs[&sid].iter().any(|r| matches!(
        &r.body,
        Body::InvocationCompleted {
            outcome: Outcome::Failed { .. },
            ..
        }
    )));
    let fam = &fresh.families(&sid).unwrap()["tool-call/*"];
    assert_eq!(fam.handler("tool-call/shell"), Some("shell"));
    assert_eq!(fam.handler("tool-call/mcp__x__y"), Some("mcp"));
    assert_eq!(fam.handler("tool-call/nope"), None);
}

#[test]
fn metrics_count_handler_dispatch_and_amplification() {
    let mut sim = Sim::new(compiled(&config(json!({}))));
    let sid = go(&mut sim, "read_file shell list_dir", None);
    let m = agentmod_core::metrics::session_metrics(&sim.logs[&sid]);
    assert_eq!(m["tool_calls"], 3.0);
    assert_eq!(m["exact_handler_dispatches"], 3.0);
    // ui and audit observe each call (family observers, not handlers).
    assert_eq!(m["observer_dispatches"], 6.0);
    assert_eq!(m["tool_call_invocations"], 12.0);
    assert_eq!(m["ratios"]["plugin_invocations_per_tool_call"], 4.0);
    assert_eq!(
        m["records"].as_f64().unwrap() as usize,
        sim.logs[&sid].len()
    );
}
