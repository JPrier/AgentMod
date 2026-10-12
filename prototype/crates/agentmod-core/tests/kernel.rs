//! Scenario tests for the kernel: dispatch along the compiled graph,
//! publishing, control, recovery, revisions.

mod common;

use std::collections::BTreeMap;

use agentmod_core::compiler::{Compilation, compile};
use agentmod_core::kernel::{
    InvocationResult, InvokeRequest, Kernel, PublishRequest, Scope, StartRequest,
};
use agentmod_core::manifest::{DeploymentConfig, Manifest};
use agentmod_core::projection::{context_at, project};
use agentmod_core::record::{Body, Command, DispatchState, Outcome, Record, Settlement};
use agentmod_core::types::{Cause, ContextOp, Lane, LogRef, Mode, Origin};
use common::{S, Sim, deployment, k};
use serde_json::{Value, json};

// ----------------------------------------------------------------------------
// Fixtures
// ----------------------------------------------------------------------------

/// A small chat deployment: ui (frontend, wildcard observer), redact (returns
/// a rewritten `text`), gate (veto), chat (context + loop), model (async
/// provider), ping/pong (a cycle), reporter (cross-session), collector.
fn manifests() -> BTreeMap<String, Manifest> {
    let ctx = json!({ "context": k("array<object>") });
    common::manifests(json!({
        "ui": {
            "subscribe": [{ "event": "*", "mode": "async" }],
            "emit": [
                { "event": "user-message", "deferred": true, "lanes": ["normal", "interrupt"], "keys": { "text": k(S) } },
                { "event": "note", "deferred": true, "keys": { "text": k(S) } }
            ],
            "capabilities": ["start-session", "deferred-publish", "interrupt", "control", "observe"],
            "sink": true
        },
        "redact": {
            "subscribe": [{ "event": "user-message", "mode": "blocking", "receives": { "text": k(S) }, "returns": { "text": k(S), "redacted": k("bool") } }]
        },
        "gate": {
            "subscribe": [{ "event": "user-message", "mode": "blocking", "actions": ["veto"], "receives": { "text": k(S) } }]
        },
        "chat": {
            "subscribe": [
                { "event": "user-message", "mode": "blocking", "actions": ["contribute-context"], "receives": { "text": k(S) } },
                { "event": "model-response", "mode": "blocking", "actions": ["contribute-context"], "receives": { "text": k(S) } }
            ],
            "emit": [
                { "event": "model-request", "keys": { "turn": k(S) } },
                { "event": "assistant-message", "keys": { "text": k(S) } }
            ]
        },
        "model": {
            "subscribe": [{ "event": "model-request", "mode": "async", "receives": { "turn": k(S) }, "slots": ctx }],
            "emit": [{ "event": "model-response", "keys": { "text": k(S) } }]
        },
        "ping": { "subscribe": [{ "event": "pong", "mode": "blocking" }], "emit": [{ "event": "ping" }] },
        "pong": {
            "subscribe": [{ "event": "ping", "mode": "blocking" }, { "event": "session-started", "mode": "blocking" }],
            "emit": [{ "event": "pong" }]
        },
        "reporter": {
            "subscribe": [{ "event": "assistant-message", "mode": "async", "receives": { "text": k(S) } }],
            "outbound": [{ "event": "worker-result", "keys": { "text": k(S) } }],
            "capabilities": ["cross-session"]
        },
        "nocross": { "subscribe": [{ "event": "assistant-message", "mode": "async", "receives": { "text": k(S) } }] },
        "collector": { "subscribe": [{ "event": "worker-result", "mode": "async", "receives": { "text": k(S) } }], "sink": true }
    }))
}

fn phase_of(p: &str) -> Value {
    match p {
        "redact" => json!({ "phase": "intake" }),
        "gate" => json!({ "phase": "guard" }),
        "chat" => json!({ "phase": "loop" }),
        _ => json!({}),
    }
}

fn config(chat_plugins: &[&str]) -> DeploymentConfig {
    let plugins: serde_json::Map<String, Value> = chat_plugins
        .iter()
        .map(|p| ((*p).to_owned(), phase_of(p)))
        .collect();
    deployment(
        &manifests(),
        json!({
            "chat": { "description": "chat", "runtime": "0.1", "phases": ["intake", "guard", "loop"], "plugins": plugins, "inbound": { "worker-result": { "text": k(S) } } },
            "pingpong": { "runtime": "0.1", "phases": ["main"], "plugins": { "ui": {}, "ping": { "phase": "main" }, "pong": { "phase": "main" } } },
            "worker": { "runtime": "0.1", "phases": ["loop"], "plugins": { "ui": {}, "chat": { "phase": "loop" }, "model": {}, "reporter": {}, "nocross": {} } }
        }),
    )
}

const CHAT: &[&str] = &["ui", "redact", "gate", "chat", "model", "collector"];

fn compiled(cfg: &DeploymentConfig) -> Compilation {
    let c = compile(cfg, &manifests());
    assert!(c.ok, "{:#?}", c.errors());
    c
}

fn new_sim(cfg: &DeploymentConfig) -> Sim {
    Sim::new(compiled(cfg))
}

thread_local! {
    static PARENT: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

/// Default plugin behaviour.
fn behave(sim: &mut Sim, plugin: &str, req: &InvokeRequest) -> InvocationResult {
    let ev = &req.event;
    let inv = req.invocation_id.clone();
    let mut result = InvocationResult::default();
    match (plugin, ev.event_name.as_str()) {
        ("redact", "user-message") => {
            let text = ev.payload["text"]
                .as_str()
                .unwrap_or("")
                .replace("secret", "******");
            result.returns = Some(
                json!({ "text": text, "redacted": true })
                    .as_object()
                    .unwrap()
                    .clone(),
            );
        }
        ("gate", "user-message")
            if ev.payload["text"]
                .as_str()
                .unwrap_or("")
                .contains("forbidden") =>
        {
            result.veto = Some("policy".into());
        }
        ("chat", "user-message") => {
            result.contributions = vec![ContextOp::Add {
                section: "messages".into(),
                value: json!({ "role": "user", "content": ev.payload["text"] }),
            }];
            let _ = sim.emit(
                "chat",
                &inv,
                "model-request",
                json!({ "turn": ev.event_id }),
            );
        }
        ("chat", "model-response") => {
            result.contributions = vec![ContextOp::Add {
                section: "messages".into(),
                value: json!({ "role": "assistant", "content": ev.payload["text"] }),
            }];
            let _ = sim.emit(
                "chat",
                &inv,
                "assistant-message",
                json!({ "text": ev.payload["text"] }),
            );
        }
        ("model", "model-request") => {
            let n = ev
                .slots
                .as_ref()
                .and_then(|s| s.get("context"))
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            let _ = sim.emit(
                "model",
                &inv,
                "model-response",
                json!({ "text": format!("reply with {n} context items") }),
            );
        }
        ("ping", "pong") => {
            let _ = sim.emit("ping", &inv, "ping", json!({}));
        }
        ("pong", "session-started" | "ping") => {
            let _ = sim.emit("pong", &inv, "pong", json!({}));
        }
        ("reporter" | "nocross", "assistant-message") => {
            if let Some(parent) = PARENT.with(|p| p.borrow().clone()) {
                let _ = sim.publish(PublishRequest {
                    plugin: plugin.into(),
                    invocation_id: Some(inv.clone()),
                    cite: None,
                    event_name: "worker-result".into(),
                    payload: json!({ "text": ev.payload["text"] }),
                    ui: None,
                    lane: None,
                    target_session: Some(parent),
                });
            }
        }
        _ => {}
    }
    result
}

fn run(sim: &mut Sim) {
    sim.run(&mut behave);
}

fn step(sim: &mut Sim, plugin: &str, req: &InvokeRequest) {
    let r = behave(sim, plugin, req);
    sim.complete(&req.invocation_id, r);
}

/// Pop pending invocations, behaving normally, until `plugin`'s turn comes.
fn until(sim: &mut Sim, plugin: &str) -> InvokeRequest {
    loop {
        let (p, _, req) = sim
            .pending
            .pop_front()
            .unwrap_or_else(|| panic!("{plugin} reached"));
        if p == plugin {
            return req;
        }
        step(sim, &p, &req);
    }
}

/// Start a session (a `start-session` call); with `text`, the frontend then
/// sends it as the first user message, citing its standing invocation.
fn start(sim: &mut Sim, def: &str, text: Option<&str>) -> String {
    let sid = sim.start("ui", def, None);
    if let Some(t) = text {
        sim.publish(user(sim, &sid, t)).unwrap();
    }
    sid
}

fn ui_cite(sim: &Sim, sid: &str) -> String {
    sim.first_invocation(sid, "ui")
}

fn user(sim: &Sim, sid: &str, text: &str) -> PublishRequest {
    PublishRequest {
        plugin: "ui".into(),
        invocation_id: None,
        cite: Some(ui_cite(sim, sid)),
        event_name: "user-message".into(),
        payload: json!({ "text": text }),
        ui: None,
        // Queue: the normal lane.
        lane: Some(Lane::Normal),
        target_session: None,
    }
}

/// The same user message, steered: published on the interrupt lane.
fn steer(sim: &Sim, sid: &str, text: &str) -> PublishRequest {
    PublishRequest {
        lane: Some(Lane::Interrupt),
        ..user(sim, sid, text)
    }
}

// ----------------------------------------------------------------------------
// Scenarios
// ----------------------------------------------------------------------------

#[test]
fn chat_turn_runs_the_compiled_chain_in_phase_order_and_records_everything() {
    let mut sim = new_sim(&config(CHAT));
    let sid = start(&mut sim, "chat", Some("hello secret world"));
    run(&mut sim);
    assert_eq!(
        sim.events(&sid),
        vec![
            "session-started",
            "user-message",
            "model-request",
            "model-response",
            "assistant-message"
        ]
    );
    let view = project(sim.log(&sid));
    assert!(view.events.iter().all(|e| e.status == "delivered"));
    // The blocking chain ran in phase order; the return reached downstream. The
    // wildcard observer `ui` is not invoked for its own `user-message`.
    let um = &view.events[1];
    let order: Vec<_> = um
        .invocations
        .iter()
        .map(|i| (i.plugin.as_str(), i.mode, i.phase.as_deref()))
        .collect();
    assert_eq!(
        order,
        vec![
            ("redact", Mode::Blocking, Some("intake")),
            ("gate", Mode::Blocking, Some("guard")),
            ("chat", Mode::Blocking, Some("loop")),
        ]
    );
    assert_eq!(um.invocations[1].input["text"], "hello ****** world");
    assert_eq!(um.invocations[0].input["text"], "hello secret world");
    // Projection: each subscriber received exactly the keys it declared.
    assert_eq!(
        um.invocations[1].input,
        json!({ "text": "hello ****** world" }),
        "gate did not get `redacted`"
    );
    // Stamps and the revision are recorded on every invocation.
    let rev = sim.k.status(&sid).unwrap().revision;
    assert_eq!(um.invocations[0].stamp.binary, "bin-redact");
    assert!(
        view.events
            .iter()
            .flat_map(|e| &e.invocations)
            .all(|i| i.revision == rev)
    );
    // Context: user + assistant messages, attributed to chat.
    assert_eq!(view.context.len(), 2);
    assert!(view.context.iter().all(|c| c.plugin == "chat"));
    // The model asked for the context slot and saw the user message in it.
    assert_eq!(view.events[3].payload["text"], "reply with 1 context items");
    // Pipeline outputs have deterministic ids and causes.
    let mr = &view.events[2];
    assert!(mr.event_id.ends_with(".o0"));
    assert!(matches!(&mr.origin, Origin::Pipeline { plugin, .. } if plugin == "chat"));
    assert_eq!(sim.k.status(&sid).unwrap().activity, "idle");
    assert!(view.breaches.is_empty());
}

#[test]
fn context_is_delivered_only_to_subscriptions_that_request_it() {
    let mut sim = new_sim(&config(CHAT));
    let sid = start(&mut sim, "chat", Some("hi"));
    let mut seen: Vec<(String, bool)> = Vec::new();
    sim.run(&mut |s: &mut Sim, p: &str, req: &InvokeRequest| {
        seen.push((
            format!("{p}:{}", req.event.event_name),
            req.event.slots.is_some(),
        ));
        behave(s, p, req)
    });
    let with: Vec<&String> = seen.iter().filter(|(_, c)| *c).map(|(n, _)| n).collect();
    assert_eq!(with, vec!["model:model-request"], "{seen:?}");
    let _ = sid;
}

#[test]
fn replay_rebuilds_identical_state_and_reruns_identically() {
    let cfg = config(CHAT);
    let mut a = new_sim(&cfg);
    let sid = start(&mut a, "chat", Some("one"));
    run(&mut a);
    a.publish(user(&a, &sid, "two")).unwrap();
    run(&mut a);
    // Deterministic: an identical script yields byte-identical logs.
    let mut b = new_sim(&cfg);
    let sid_b = start(&mut b, "chat", Some("one"));
    run(&mut b);
    b.publish(user(&b, &sid_b, "two")).unwrap();
    run(&mut b);
    assert_eq!(
        serde_json::to_string(a.log(&sid)).unwrap(),
        serde_json::to_string(b.log(&sid_b)).unwrap()
    );
    // Replay-as-reading rebuilds the same status and context.
    let mut k2 = Kernel::new();
    k2.install(compiled(&cfg)).unwrap();
    k2.load_session(a.log(&sid)).unwrap();
    assert_eq!(k2.status(&sid), a.k.status(&sid));
    assert_eq!(k2.context(&sid), a.k.context(&sid));
    // Records round-trip through JSON.
    let json = serde_json::to_string(a.log(&sid)).unwrap();
    let back: Vec<Record> = serde_json::from_str(&json).unwrap();
    assert_eq!(back, a.log(&sid));
}

#[test]
fn veto_settles_the_event_and_skips_async_observers() {
    let mut sim = new_sim(&config(CHAT));
    let sid = start(&mut sim, "chat", Some("this is forbidden"));
    run(&mut sim);
    let view = project(sim.log(&sid));
    let um = &view.events[1];
    assert_eq!(um.status, "vetoed");
    assert!(matches!(&um.settlement, Some(Settlement::Vetoed { plugin, .. }) if plugin == "gate"));
    assert!(um.invocations.iter().all(|i| i.plugin != "chat"));
    assert_eq!(sim.events(&sid), vec!["session-started", "user-message"]);
}

#[test]
fn returns_contradicting_the_declaration_fail_the_pipeline_with_a_breach() {
    let mut sim = new_sim(&config(CHAT));
    let sid = start(&mut sim, "chat", Some("x"));
    sim.run(&mut |s: &mut Sim, p: &str, req: &InvokeRequest| {
        if p == "redact" {
            InvocationResult {
                returns: Some(
                    json!({ "text": 42, "redacted": true })
                        .as_object()
                        .unwrap()
                        .clone(),
                ),
                ..Default::default()
            }
        } else {
            behave(s, p, req)
        }
    });
    let view = project(sim.log(&sid));
    assert_eq!(view.events[1].status, "failed");
    let out = view.events[1].invocations[0].outcome.clone().unwrap();
    assert!(matches!(out, Outcome::Failed { error } if error.contains("returned `text`")));
    let b = &view.breaches[0];
    assert_eq!(b["plugin"], "redact");
    assert_eq!(b["declaration"]["kind"], "returns");
    assert_eq!(b["declaration"]["key"], "text");
    assert_eq!(b["declaration"]["shape"]["shape"], "string");
    assert_eq!(b["observed"], 42);
    assert_eq!(b["reaction"], "invocation-failed");
}

#[test]
fn returns_may_not_add_undeclared_keys_or_omit_required_ones() {
    let mut sim = new_sim(&config(CHAT));
    let sid = start(&mut sim, "chat", Some("x"));
    sim.run(&mut |s: &mut Sim, p: &str, req: &InvokeRequest| match p {
        "redact" => InvocationResult {
            returns: Some(
                json!({ "text": "t", "redacted": true, "extra": 1 })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
            ..Default::default()
        },
        _ => behave(s, p, req),
    });
    let view = project(sim.log(&sid));
    assert!(
        view.breaches[0]["reason"]
            .as_str()
            .unwrap()
            .contains("does not declare"),
        "{:?}",
        view.breaches
    );
    let mut sim2 = new_sim(&config(CHAT));
    let sid2 = start(&mut sim2, "chat", Some("x"));
    sim2.run(&mut |s: &mut Sim, p: &str, req: &InvokeRequest| match p {
        "redact" => InvocationResult {
            returns: Some(json!({ "text": "t" }).as_object().unwrap().clone()),
            ..Default::default()
        },
        _ => behave(s, p, req),
    });
    let view = project(sim2.log(&sid2));
    assert!(
        view.breaches[0]["reason"]
            .as_str()
            .unwrap()
            .contains("did not return required `redacted`")
    );
}

#[test]
fn async_subscribers_cannot_veto_and_undeclared_actions_are_breaches() {
    let mut sim = new_sim(&config(CHAT));
    let sid = start(&mut sim, "chat", None);
    let (_, _, req) = sim.pending.pop_front().unwrap();
    sim.complete(
        &req.invocation_id,
        InvocationResult {
            veto: Some("no".into()),
            ..Default::default()
        },
    );
    let view = project(sim.log(&sid));
    assert!(matches!(
        view.events[0].invocations[0].outcome,
        Some(Outcome::Failed { .. })
    ));
    assert_eq!(view.events[0].status, "delivered");
    assert_eq!(view.breaches[0]["declaration"]["kind"], "actions");
    // gate contributes context without declaring `contribute-context`.
    let mut sim = new_sim(&config(CHAT));
    let sid = start(&mut sim, "chat", Some("hi"));
    sim.run(&mut |s: &mut Sim, p: &str, req: &InvokeRequest| match p {
        "gate" => InvocationResult {
            contributions: vec![ContextOp::Add {
                section: "x".into(),
                value: json!(1),
            }],
            ..Default::default()
        },
        _ => behave(s, p, req),
    });
    let view = project(sim.log(&sid));
    assert!(
        view.breaches[0]["reason"]
            .as_str()
            .unwrap()
            .contains("contribute-context")
    );
    assert!(view.context.iter().all(|c| c.section != "x"));
}

#[test]
fn publish_classification_blocks_and_breaches_are_recorded() {
    let mut sim = new_sim(&config(CHAT));
    let sid = start(&mut sim, "chat", None);
    run(&mut sim);
    let base = PublishRequest {
        plugin: "ui".into(),
        invocation_id: None,
        cite: None,
        event_name: "user-message".into(),
        payload: json!({ "text": "hi" }),
        ui: None,
        lane: Some(Lane::Normal),
        target_session: None,
    };
    // No invocation, no citation.
    assert!(
        sim.publish(PublishRequest {
            target_session: Some(sid.clone()),
            ..base.clone()
        })
        .unwrap_err()
        .contains("must cite")
    );
    // Citing an invocation dispatched to another plugin.
    assert!(
        sim.publish(PublishRequest {
            cite: Some(format!("{sid}/i999")),
            ..base.clone()
        })
        .unwrap_err()
        .contains("uncitable")
    );
    // Undeclared emit: a breach.
    let cite = ui_cite(&sim, &sid);
    assert!(
        sim.publish(PublishRequest {
            cite: Some(cite.clone()),
            event_name: "model-request".into(),
            ..base.clone()
        })
        .unwrap_err()
        .contains("undeclared emit")
    );
    // Missing declared key: a breach.
    assert!(
        sim.publish(PublishRequest {
            cite: Some(cite.clone()),
            payload: json!({}),
            ..base.clone()
        })
        .unwrap_err()
        .contains("`text`: expected required string, observed absent")
    );
    // Wrong shape: a breach.
    assert!(
        sim.publish(PublishRequest {
            cite: Some(cite.clone()),
            payload: json!({ "text": 1 }),
            ..base.clone()
        })
        .unwrap_err()
        .contains("expected string")
    );
    // No lane where the emit declares two: a breach (the publisher chooses).
    assert!(
        sim.publish(PublishRequest {
            cite: Some(cite.clone()),
            lane: None,
            ..base.clone()
        })
        .unwrap_err()
        .contains("must choose its lane")
    );
    // Bad UI hint.
    assert!(
        sim.publish(PublishRequest {
            cite: Some(cite.clone()),
            ui: Some(json!({ "x": 1 })),
            ..base.clone()
        })
        .unwrap_err()
        .contains("ui hint")
    );
    // A valid deferred publish; an undeclared extra key passes untyped.
    let id = sim
        .publish(PublishRequest {
            cite: Some(cite.clone()),
            payload: json!({ "text": "hi", "extra": [1] }),
            ui: Some(json!({ "kind": "text", "text": "hi" })),
            ..base.clone()
        })
        .unwrap();
    run(&mut sim);
    let view = project(sim.log(&sid));
    let ev = view.events.iter().find(|e| e.event_id == id).unwrap();
    assert!(matches!(&ev.origin, Origin::Deferred { cites, .. } if cites == &cite));
    assert_eq!(ev.payload["extra"], json!([1]));
    assert_eq!(view.blocked.len(), 3, "{:#?}", view.blocked);
    assert_eq!(view.breaches.len(), 4, "{:#?}", view.breaches);
    assert!(
        view.breaches
            .iter()
            .all(|b| b["reaction"] == "publish-blocked" && b["revision"] == json!(view.revision))
    );
    // A non-deferred emit published without an open invocation is a breach.
    let chat_closed = sim.first_invocation(&sid, "chat");
    let late = PublishRequest {
        plugin: "chat".into(),
        invocation_id: Some(chat_closed),
        event_name: "model-request".into(),
        payload: json!({ "turn": "x" }),
        ..base
    };
    assert!(
        sim.publish(late)
            .unwrap_err()
            .contains("may not be published deferred")
    );
}

#[test]
fn interrupt_lane_wins_next_but_never_interrupts() {
    let mut sim = new_sim(&config(CHAT));
    let sid = start(&mut sim, "chat", None);
    let (_, _, ui0) = sim.pending.pop_front().unwrap();
    sim.complete(&ui0.invocation_id, InvocationResult::default());
    sim.publish(user(&sim, &sid, "first")).unwrap();
    // First pipeline is now running (redact open). Queue normal, then interrupt.
    sim.publish(user(&sim, &sid, "second")).unwrap();
    sim.publish(steer(&sim, &sid, "urgent")).unwrap();
    let status = sim.k.status(&sid).unwrap();
    assert_eq!((status.queued_normal, status.queued_interrupt), (1, 1));
    run(&mut sim);
    let view = project(sim.log(&sid));
    let text = |id: &str| {
        view.events
            .iter()
            .find(|e| e.event_id == id)
            .and_then(|e| e.payload["text"].as_str().map(str::to_owned))
    };
    let appended: Vec<String> = view
        .events
        .iter()
        .filter_map(|e| {
            e.payload["text"]
                .as_str()
                .filter(|_| e.event_name == "user-message")
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(appended, vec!["first", "second", "urgent"], "append order");
    let dispatched: Vec<String> = sim
        .log(&sid)
        .iter()
        .filter_map(|r| match &r.body {
            Body::PipelineStarted { event_id } => text(event_id),
            _ => None,
        })
        .filter(|t| ["first", "second", "urgent"].contains(&t.as_str()))
        .collect();
    assert_eq!(dispatched, vec!["first", "urgent", "second"]);
    // One event on two lanes: the envelope says which, and the same
    // subscribers handle both the same way.
    let lanes: Vec<(String, Lane)> = view
        .events
        .iter()
        .filter(|e| e.event_name == "user-message")
        .map(|e| (e.payload["text"].as_str().unwrap_or("").to_owned(), e.lane))
        .collect();
    assert_eq!(
        lanes,
        vec![
            ("first".into(), Lane::Normal),
            ("second".into(), Lane::Normal),
            ("urgent".into(), Lane::Interrupt)
        ]
    );
    let subscribers = |text: &str| -> Vec<String> {
        let ev = view
            .events
            .iter()
            .find(|e| e.payload["text"] == text)
            .unwrap();
        ev.invocations.iter().map(|i| i.plugin.clone()).collect()
    };
    assert_eq!(subscribers("urgent"), subscribers("second"));
}

#[test]
fn a_publish_chooses_its_lane_from_the_ones_its_emit_declares() {
    let mut sim = new_sim(&config(CHAT));
    let sid = start(&mut sim, "chat", None);
    run(&mut sim);
    let cite = ui_cite(&sim, &sid);
    let note = |lane| PublishRequest {
        plugin: "ui".into(),
        invocation_id: None,
        cite: Some(cite.clone()),
        event_name: "note".into(),
        payload: json!({ "text": "n" }),
        ui: None,
        lane,
        target_session: None,
    };
    // `note` declares only the normal lane: omitting the lane picks it.
    let id = sim.publish(note(None)).unwrap();
    // Any other lane is a hard failure, recorded against the declaration.
    let err = sim.publish(note(Some(Lane::Interrupt))).unwrap_err();
    assert!(err.contains("which its emit does not declare"), "{err}");
    let breach = sim.breaches(&sid).pop().unwrap();
    assert_eq!(breach["declaration"]["lanes"], json!(["normal"]));
    assert_eq!(breach["observed"]["lane"], "interrupt");
    // The envelope delivered to subscribers carries the lane used.
    sim.publish(steer(&sim, &sid, "now")).unwrap();
    let delivered = sim
        .pending
        .iter()
        .find(|(_, _, r)| r.event.payload.get("text") == Some(&json!("now")))
        .map(|(_, _, r)| r.event.lane);
    assert_eq!(delivered, Some(Lane::Interrupt));
    run(&mut sim);
    let view = project(sim.log(&sid));
    assert_eq!(
        view.events.iter().find(|e| e.event_id == id).unwrap().lane,
        Lane::Normal
    );
}

#[test]
fn soft_stop_drains_then_parks_and_resume_continues() {
    let mut sim = new_sim(&config(CHAT));
    let sid = start(&mut sim, "chat", Some("a"));
    let fx = sim.k.command(&sid, Command::SoftStop, "test", 1).unwrap();
    sim.take(fx);
    assert_eq!(sim.k.status(&sid).unwrap().state, DispatchState::Draining);
    run(&mut sim);
    let st = sim.k.status(&sid).unwrap();
    assert_eq!(st.activity, "parked");
    assert!(st.queued_normal >= 1);
    let fx = sim.k.command(&sid, Command::Resume, "test", 2).unwrap();
    sim.take(fx);
    run(&mut sim);
    assert_eq!(sim.k.status(&sid).unwrap().activity, "idle");
    assert!(sim.events(&sid).contains(&"assistant-message".to_owned()));
}

#[test]
fn hard_stop_cancels_in_flight_work_and_late_results_are_inert() {
    let mut sim = new_sim(&config(CHAT));
    let sid = start(&mut sim, "chat", Some("a"));
    let model_inv = until(&mut sim, "model");
    let fx = sim.k.command(&sid, Command::HardStop, "user", 99).unwrap();
    sim.take(fx);
    assert!(sim.cancelled.contains(&model_inv.invocation_id));
    let st = sim.k.status(&sid).unwrap();
    assert_eq!(st.state, DispatchState::Halted);
    assert!(st.open_invocations.is_empty());
    // A late completion is ignored; a late publish is classified deferred and refused.
    let before = sim.log(&sid).len();
    sim.complete(&model_inv.invocation_id, InvocationResult::default());
    assert_eq!(sim.log(&sid).len(), before);
    assert!(
        sim.emit(
            "model",
            &model_inv.invocation_id,
            "model-response",
            json!({ "text": "late" })
        )
        .is_err()
    );
    let view = project(sim.log(&sid));
    assert_eq!(view.state, DispatchState::Halted);
    assert!(
        view.events
            .iter()
            .flat_map(|e| &e.invocations)
            .any(|i| matches!(i.outcome, Some(Outcome::Cancelled { .. })))
    );
}

#[test]
fn hard_stop_aborts_a_running_blocking_pipeline() {
    let mut sim = new_sim(&config(CHAT));
    let sid = start(&mut sim, "chat", Some("a"));
    let redact = until(&mut sim, "redact");
    let fx = sim.k.command(&sid, Command::HardStop, "user", 5).unwrap();
    sim.take(fx);
    assert!(sim.cancelled.contains(&redact.invocation_id));
    let view = project(sim.log(&sid));
    assert_eq!(view.events[1].status, "aborted");
    let fx = sim.k.command(&sid, Command::Resume, "user", 6).unwrap();
    sim.take(fx);
    run(&mut sim);
    assert!(!sim.events(&sid).contains(&"model-request".to_owned()));
}

#[test]
fn crash_mid_pipeline_recovers_from_history_with_idempotent_outputs() {
    let cfg = config(CHAT);
    let mut sim = new_sim(&cfg);
    let sid = start(&mut sim, "chat", Some("hello"));
    let chat_req = until(&mut sim, "chat");
    sim.emit(
        "chat",
        &chat_req.invocation_id,
        "model-request",
        json!({ "turn": "t" }),
    )
    .unwrap();
    let crashed_log = sim.log(&sid).to_vec();
    // "Restart": fresh kernel, replay, recover.
    let mut fresh = Sim::new(compiled(&cfg));
    fresh.k.load_session(&crashed_log).unwrap();
    fresh.logs.insert(sid.clone(), crashed_log);
    let fx = fresh.k.recover(&sid, 3, 1000);
    fresh.take(fx);
    let retried: Vec<_> = fresh
        .pending
        .iter()
        .filter(|(_, _, r)| r.attempt == 2)
        .map(|(p, _, r)| (p.clone(), r.invocation_id.clone()))
        .collect();
    assert!(
        retried.contains(&("chat".to_owned(), chat_req.invocation_id.clone())),
        "{retried:?}"
    );
    // The orphan's re-publish (same deterministic output id) is deduplicated.
    run(&mut fresh);
    let view = project(fresh.log(&sid));
    assert!(view.blocked.is_empty() && view.breaches.is_empty());
    assert_eq!(
        fresh
            .events(&sid)
            .iter()
            .filter(|e| *e == "model-request")
            .count(),
        1
    );
    assert!(fresh.events(&sid).contains(&"assistant-message".to_owned()));
    assert!(fresh.kinds(&sid).contains(&"invocation-retried".to_owned()));
}

#[test]
fn orphans_give_up_after_max_attempts() {
    let mut sim = new_sim(&config(CHAT));
    let sid = start(&mut sim, "chat", None);
    let (_, _, req) = sim.pending.pop_front().unwrap();
    for _ in 0..3 {
        let fx = sim.k.retry(&req.invocation_id, "crash", 3, 7);
        sim.take(fx);
    }
    let view = project(sim.log(&sid));
    let inv = &view.events[0].invocations[0];
    assert_eq!(inv.attempts, 3);
    assert!(matches!(&inv.outcome, Some(Outcome::Failed { error }) if error.contains("gave up")));
}

#[test]
fn a_new_revision_takes_effect_at_the_event_boundary() {
    let cfg = config(CHAT);
    let mut sim = new_sim(&cfg);
    let sid = start(&mut sim, "chat", Some("first"));
    let redact_req = until(&mut sim, "redact");
    let old = sim.k.status(&sid).unwrap().revision;
    // New config: drop the gate. The chat definition compiles to a new revision.
    let new_cfg = config(&["ui", "redact", "chat", "model", "collector"]);
    let comp = compiled(&new_cfg);
    let new_rev = comp.definitions["chat"].revision.clone();
    assert_ne!(new_rev, old);
    let hash = sim.k.install(comp).unwrap();
    let out = sim.k.apply_config(&hash, &Scope::Global, 50);
    assert!(out.error.is_none());
    assert_eq!(out.revisions[&sid], new_rev);
    sim.take(out.effects);
    assert!(
        !sim.kinds(&sid).contains(&"config-applied".to_owned()),
        "not mid-pipeline"
    );
    step(&mut sim, "redact", &redact_req);
    run(&mut sim);
    let view = project(sim.log(&sid));
    assert_eq!(view.revision, new_rev);
    let applied_at = sim
        .log(&sid)
        .iter()
        .position(|r| matches!(r.body, Body::ConfigApplied { .. }))
        .unwrap();
    let um_settled = sim.log(&sid).iter().position(|r| matches!(&r.body, Body::PipelineSettled { event_id, .. } if event_id == &view.events[1].event_id)).unwrap();
    assert!(applied_at > um_settled);
    assert!(
        view.events[1]
            .invocations
            .iter()
            .any(|i| i.plugin == "gate" && i.revision == old),
        "old revision for the in-flight event"
    );
    // config-applied is a core event on the interrupt lane, with the revisions.
    let ca = view
        .events
        .iter()
        .find(|e| e.event_name == "config-applied")
        .unwrap();
    assert_eq!(ca.lane, Lane::Interrupt);
    assert_eq!(ca.payload, json!({ "revision": new_rev, "previous": old }));
    // The next turn runs under the new revision, without the gate.
    sim.publish(user(&sim, &sid, "second")).unwrap();
    run(&mut sim);
    let view = project(sim.log(&sid));
    let last = view
        .events
        .iter()
        .rev()
        .find(|e| e.event_name == "user-message")
        .unwrap();
    assert!(
        last.invocations
            .iter()
            .all(|i| i.plugin != "gate" && i.revision == new_rev)
    );
}

#[test]
fn apply_targets_sessions_and_is_rejected_whole_on_any_error() {
    let mut sim = new_sim(&config(CHAT));
    let a = start(&mut sim, "chat", None);
    let b = start(&mut sim, "chat", None);
    let w = start(&mut sim, "worker", None);
    run(&mut sim);
    let (ra, rb) = (
        sim.k.status(&a).unwrap().revision,
        sim.k.status(&b).unwrap().revision,
    );
    let comp = compiled(&config(&["ui", "chat", "model", "collector"]));
    let new_rev = comp.definitions["chat"].revision.clone();
    let hash = sim.k.install(comp).unwrap();
    let out = sim.k.apply_config(
        &hash,
        &Scope::Session {
            session_id: a.clone(),
        },
        5,
    );
    sim.take(out.effects);
    assert_eq!(sim.k.status(&a).unwrap().revision, new_rev);
    assert_eq!(sim.k.status(&b).unwrap().revision, rb);
    // A target whose definition the config lacks rejects the whole apply.
    let mut cfg = config(CHAT);
    cfg.definitions.remove("worker");
    let h2 = sim.k.install(compiled(&cfg)).unwrap();
    let out = sim.k.apply_config(
        &h2,
        &Scope::Sessions {
            session_ids: vec![b.clone(), w.clone()],
        },
        6,
    );
    assert!(
        out.error.as_deref().unwrap().contains("no session changed"),
        "{:?}",
        out.error
    );
    assert!(out.effects.is_empty() && out.revisions.is_empty());
    assert_eq!(sim.k.status(&b).unwrap().revision, rb);
    let _ = ra;
}

#[test]
fn rejected_configs_are_not_installed() {
    let mut k2 = Kernel::new();
    let mut cfg = config(CHAT);
    cfg.definitions
        .get_mut("chat")
        .unwrap()
        .plugins
        .insert("ghost".into(), Default::default());
    let err = k2.install(compile(&cfg, &manifests())).unwrap_err();
    assert!(err.contains("ghost"));
}

#[test]
fn cycles_run_without_a_core_depth_limit() {
    // The core has no recursion or depth limit: a ping-pong cycle runs until
    // something stops it (here, the operator).
    let mut sim = new_sim(&config(CHAT));
    let sid = sim.start("ui", "pingpong", None);
    sim.run_steps(600, &mut behave);
    let n = sim.events(&sid).len();
    assert!(n > 300, "{n} events");
    let view = project(sim.log(&sid));
    assert!(view.blocked.is_empty() && view.breaches.is_empty());
    let fx = sim
        .k
        .command(&sid, Command::HardStop, "operator", 1)
        .unwrap();
    sim.take(fx);
    assert_eq!(sim.k.status(&sid).unwrap().activity, "halted");
}

#[test]
fn cross_session_publishes_are_paired_with_the_targets_inbound_and_mirrored() {
    let mut sim = new_sim(&config(CHAT));
    let parent = start(&mut sim, "chat", None);
    run(&mut sim);
    PARENT.with(|p| *p.borrow_mut() = Some(parent.clone()));
    let worker = start(&mut sim, "worker", Some("do work"));
    run(&mut sim);
    PARENT.with(|p| *p.borrow_mut() = None);
    let pv = project(sim.log(&parent));
    let results: Vec<_> = pv
        .events
        .iter()
        .filter(|e| e.event_name == "worker-result")
        .collect();
    assert_eq!(results.len(), 1, "only the declared outbound got through");
    assert!(
        matches!(&results[0].origin, Origin::CrossSession { from_session, .. } if from_session == &worker)
    );
    assert_eq!(results[0].status, "delivered");
    // The boundary node emits it in the target, to the inbound's subscriber.
    assert!(
        results[0]
            .invocations
            .iter()
            .any(|i| i.plugin == "collector")
    );
    // Cause links cross sessions; ids are globally unique (`<session>/i<n>.o<k>`).
    assert!(results[0].event_id.starts_with(&format!("{worker}/")));
    let wv = project(sim.log(&worker));
    assert_eq!(wv.sent.len(), 1, "mirrored in the sender's log");
    assert_eq!(wv.sent[0]["event_id"], json!(results[0].event_id));
    assert!(wv.breaches.iter().any(|b| {
        b["plugin"] == "nocross"
            && b["reason"]
                .as_str()
                .unwrap()
                .contains("undeclared outbound")
    }));
}

#[test]
fn a_cross_session_pairing_mismatch_fails_back_to_the_sender_only() {
    let mut sim = new_sim(&config(CHAT));
    // The target is another worker: its definition accepts no `worker-result`.
    let other = start(&mut sim, "worker", None);
    run(&mut sim);
    PARENT.with(|p| *p.borrow_mut() = Some(other.clone()));
    let worker = start(&mut sim, "worker", Some("do work"));
    run(&mut sim);
    PARENT.with(|p| *p.borrow_mut() = None);
    let wv = project(sim.log(&worker));
    assert!(
        wv.blocked.iter().any(|b| b.plugin == "reporter"
            && b.reason.contains("pairing failed")
            && b.reason.contains("does not accept")),
        "{:#?}",
        wv.blocked
    );
    assert!(
        project(sim.log(&other))
            .events
            .iter()
            .all(|e| e.event_name != "worker-result")
    );
    // The sender's session carries on.
    assert_eq!(sim.k.status(&worker).unwrap().state, DispatchState::Running);
    assert!(
        sim.events(&worker)
            .contains(&"assistant-message".to_owned())
    );
}

#[test]
fn fork_seeds_context_and_restore_is_an_append() {
    let mut sim = new_sim(&config(CHAT));
    let sid = start(&mut sim, "chat", Some("one"));
    run(&mut sim);
    let mid = sim.log(&sid).last().unwrap().sequence;
    sim.publish(user(&sim, &sid, "two")).unwrap();
    run(&mut sim);
    assert_eq!(sim.k.context(&sid).unwrap().len(), 4);
    assert_eq!(context_at(sim.log(&sid), mid).len(), 2);
    let fork = sim
        .start_req(&StartRequest {
            plugin: "ui".into(),
            definition: "chat".into(),
            spec: None,
            invocation_id: None,
            input: None,
            ui: None,
            fork_from: Some(LogRef {
                session_id: sid.clone(),
                sequence: mid,
            }),
        })
        .unwrap();
    run(&mut sim);
    assert_eq!(sim.k.context(&fork).unwrap().len(), 2);
    assert!(sim.kinds(&fork).contains(&"context-seeded".to_owned()));
    // Restore in the original session via a contribution (on a steered user
    // message); history keeps both states.
    sim.publish(steer(&sim, &sid, "restore")).unwrap();
    while let Some((plugin, _, req)) = sim.pending.pop_front() {
        let mut r = InvocationResult::default();
        match plugin.as_str() {
            "chat" if req.event.event_name == "user-message" => {
                r.contributions = vec![ContextOp::Restore { to_sequence: mid }];
            }
            "redact" => {
                r.returns = Some(
                    json!({ "text": "restore", "redacted": false })
                        .as_object()
                        .cloned()
                        .unwrap(),
                );
            }
            _ => {}
        }
        sim.complete(&req.invocation_id, r);
    }
    assert_eq!(sim.k.context(&sid).unwrap().len(), 2);
    let restore_seq = sim
        .log(&sid)
        .iter()
        .find_map(|r| match &r.body {
            Body::InvocationCompleted { contributions, .. }
                if contributions
                    .iter()
                    .any(|c| matches!(c, ContextOp::Restore { .. })) =>
            {
                Some(r.sequence)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(
        context_at(sim.log(&sid), restore_seq - 1).len(),
        4,
        "pre-revert state still reconstructable"
    );
}

#[test]
fn idle_sessions_unload_and_reload_on_demand() {
    let mut sim = new_sim(&config(CHAT));
    let sid = start(&mut sim, "chat", Some("hi"));
    run(&mut sim);
    assert!(sim.k.unload_if_idle(&sid));
    assert!(!sim.k.is_loaded(&sid));
    let req = user(&sim, &sid, "again");
    assert!(
        sim.k
            .publish(&req, 1)
            .blocked
            .unwrap()
            .contains("not loaded")
    );
    let log = sim.log(&sid).to_vec();
    sim.k.load_session(&log).unwrap();
    sim.publish(req).unwrap();
    run(&mut sim);
    assert_eq!(
        sim.events(&sid)
            .iter()
            .filter(|e| *e == "assistant-message")
            .count(),
        2
    );
}

#[test]
fn start_session_validates_capability_definition_and_input() {
    let mut sim = new_sim(&config(CHAT));
    let req = |plugin: &str, def: &str, input: Value| StartRequest {
        plugin: plugin.into(),
        definition: def.into(),
        spec: None,
        invocation_id: None,
        input: Some(input),
        ui: None,
        fork_from: None,
    };
    assert!(
        sim.k
            .start_session(&req("chat", "chat", json!({ "text": "x" })), 1)
            .error
            .unwrap()
            .contains("start-session capability")
    );
    assert!(
        sim.k
            .start_session(&req("ui", "nope", json!({ "text": "x" })), 1)
            .error
            .unwrap()
            .contains("unknown session definition")
    );
    // The input has the core manifest's fixed shape.
    let bad = sim
        .k
        .start_session(&req("ui", "chat", json!({ "text": 5 })), 1);
    assert!(bad.session_id.is_none());
    assert!(bad.error.unwrap().contains("input.text"));
    assert!(
        sim.k
            .start_session(&req("ui", "chat", json!("hello")), 1)
            .error
            .unwrap()
            .contains("contradicts")
    );
    assert!(sim.logs.is_empty(), "no session was created");
    let ok = start(&mut sim, "chat", None);
    assert_eq!(ok, "s0001");
    assert_eq!(start(&mut sim, "chat", None), "s0002");
}

#[test]
fn session_started_is_emitted_by_the_core_and_carries_the_input() {
    let mut sim = new_sim(&config(CHAT));
    let now = sim.tick();
    let out = sim.k.start_session(
        &StartRequest {
            plugin: "ui".into(),
            definition: "chat".into(),
            spec: None,
            invocation_id: None,
            input: Some(json!({ "text": "hi", "data": { "from": "a test" } })),
            ui: Some(json!({ "v": 1, "kind": "text", "role": "user", "text": "hi" })),
            fork_from: None,
        },
        now,
    );
    let sid = out.session_id.clone().unwrap();
    sim.take(out.effects);
    let view = project(sim.log(&sid));
    // The session's first and only event so far: nothing else was published
    // into it by the starter.
    assert_eq!(sim.events(&sid), vec!["session-started"]);
    let started = &view.events[0];
    assert!(matches!(started.origin, Origin::Core));
    assert!(matches!(&started.cause, Cause::Root { plugin } if plugin == "ui"));
    assert_eq!(
        started.payload["input"],
        json!({ "text": "hi", "data": { "from": "a test" } })
    );
    assert_eq!(started.payload["definition"], "chat");
    assert_eq!(started.ui.as_ref().unwrap()["kind"], "text");
}

/// Randomized interleavings: after every step, replaying the log into a fresh
/// kernel reproduces the live kernel's status and context exactly.
#[test]
fn randomized_interleavings_replay_exactly() {
    let cfg = config(CHAT);
    let alt = config(&["ui", "chat", "model", "collector"]);
    let (comp_main, comp_alt) = (compiled(&cfg), compiled(&alt));
    for seed in 1..=40u64 {
        let mut rng = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let mut next = move |n: u64| {
            rng = rng
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (rng >> 33) % n
        };
        let mut sim = Sim::new(comp_main.clone());
        let h_alt = sim.k.install(comp_alt.clone()).unwrap();
        let h_main = sim.k.active_hash().to_owned();
        let sid = start(&mut sim, "chat", Some("start"));
        for step_n in 0..60 {
            match next(9) {
                0 | 1 => {
                    let text = if next(5) == 0 { "forbidden" } else { "msg" };
                    let req = if next(3) == 0 {
                        steer(&sim, &sid, text)
                    } else {
                        user(&sim, &sid, text)
                    };
                    let _ = sim.publish(req);
                }
                2..=4 => {
                    if !sim.pending.is_empty() {
                        let i = usize::try_from(next(sim.pending.len() as u64)).unwrap();
                        let (plugin, _, req) = sim.pending.remove(i).unwrap();
                        if !sim.cancelled.contains(&req.invocation_id) {
                            step(&mut sim, &plugin, &req);
                        }
                    }
                }
                5 => {
                    let cmd = [
                        Command::SoftStop,
                        Command::HardStop,
                        Command::Resume,
                        Command::Resume,
                    ][usize::try_from(next(4)).unwrap()];
                    let fx = sim.k.command(&sid, cmd, "rng", step_n).unwrap();
                    sim.take(fx);
                }
                6 => {
                    if let Some((_, _, req)) = sim.pending.front().cloned() {
                        let fx = sim.k.retry(&req.invocation_id, "chaos", 3, step_n);
                        sim.take(fx);
                    }
                }
                7 => {
                    let h = if next(2) == 0 { &h_alt } else { &h_main };
                    let out = sim.k.apply_config(h, &Scope::Global, step_n);
                    sim.take(out.effects);
                }
                _ => {
                    let fx = sim.k.command(&sid, Command::Resume, "rng", step_n).unwrap();
                    sim.take(fx);
                }
            }
            let mut k2 = Kernel::new();
            k2.install(comp_main.clone()).unwrap();
            k2.install(comp_alt.clone()).unwrap();
            k2.load_session(sim.log(&sid)).unwrap();
            let mut live = sim.k.status(&sid).unwrap();
            let mut replayed = k2.status(&sid).unwrap();
            // A pending revision is not in the log until it takes effect.
            live.pending_revision = None;
            replayed.pending_revision = None;
            assert_eq!(replayed, live, "seed {seed} step {step_n}");
            assert_eq!(
                k2.context(&sid),
                sim.k.context(&sid),
                "seed {seed} step {step_n}"
            );
            assert_eq!(
                project(sim.log(&sid)).last_sequence,
                sim.log(&sid).len() as u64
            );
        }
    }
}

#[test]
fn published_links_survive_replay_and_retry() {
    let cfg = config(CHAT);
    let mut sim = new_sim(&cfg);
    let sid = start(&mut sim, "chat", Some("hi"));
    let model = until(&mut sim, "model");
    sim.emit(
        "model",
        &model.invocation_id,
        "model-response",
        json!({ "text": "a" }),
    )
    .unwrap();
    // Crash + replay + retry: the earlier output stays linked to the invocation.
    let log = sim.log(&sid).to_vec();
    let mut fresh = Sim::new(compiled(&cfg));
    fresh.k.load_session(&log).unwrap();
    fresh.logs.insert(sid.clone(), log);
    let fx = fresh.k.recover(&sid, 3, 500);
    fresh.take(fx);
    let req = PublishRequest {
        plugin: "model".into(),
        invocation_id: Some(model.invocation_id.clone()),
        cite: None,
        event_name: "model-response".into(),
        payload: json!({ "text": "a" }),
        ui: None,
        lane: None,
        target_session: None,
    };
    let dup = fresh.k.publish(&req, 501);
    assert!(dup.duplicate);
    fresh
        .emit(
            "model",
            &model.invocation_id,
            "model-response",
            json!({ "text": "b" }),
        )
        .unwrap();
    fresh.complete(&model.invocation_id, InvocationResult::default());
    let view = project(fresh.log(&sid));
    let inv = view
        .events
        .iter()
        .flat_map(|e| &e.invocations)
        .find(|i| i.invocation_id == model.invocation_id)
        .unwrap();
    assert_eq!(inv.published.len(), 2, "{:?}", inv.published);
}
