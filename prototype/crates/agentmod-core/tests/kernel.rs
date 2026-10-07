//! Scenario tests for the kernel: dispatch, publishing, control, recovery.

use std::collections::{BTreeMap, VecDeque};

use agentmod_core::compiler::compile;
use agentmod_core::kernel::{
    Effect, InitialEvent, InvocationResult, InvokeRequest, Kernel, PublishRequest, Scope,
    StartRequest,
};
use agentmod_core::manifest::{
    Capability, Consume, Definition, DeploymentConfig, Emit, Manifest, PluginConfig, Subscriber,
    Transform,
};
use agentmod_core::projection::{context_at, project};
use agentmod_core::record::{Body, Command, DispatchState, Outcome, Record, Settlement};
use agentmod_core::types::{ContextOp, Lane, LogRef, Mode, Origin};
use serde_json::{Value, json};

// ----------------------------------------------------------------------------
// Fixtures
// ----------------------------------------------------------------------------

fn c(event: &str, demands: &[&str]) -> Consume {
    Consume {
        event: event.into(),
        demands: demands.iter().map(|s| (*s).into()).collect(),
        mode: None,
        context: true,
    }
}
fn ca(event: &str) -> Consume {
    Consume {
        event: event.into(),
        demands: vec![],
        mode: Some(Mode::Async),
        context: false,
    }
}
fn e(event: &str, supplies: &[&str]) -> Emit {
    Emit {
        event: event.into(),
        supplies: supplies.iter().map(|s| (*s).into()).collect(),
        deferred: false,
    }
}
fn ed(event: &str, supplies: &[&str]) -> Emit {
    Emit {
        deferred: true,
        ..e(event, supplies)
    }
}
fn m(name: &str, consumes: Vec<Consume>, emits: Vec<Emit>, caps: Vec<Capability>) -> Manifest {
    Manifest {
        name: name.into(),
        version: "1".into(),
        description: String::new(),
        consumes,
        emits,
        transforms: vec![],
        capabilities: caps,
        config_schema: Value::Null,
    }
}

/// A small chat deployment: ui (frontend), redact (transformer), gate (veto),
/// chat (context + loop), model (async provider), worker (cross-session).
fn manifests() -> BTreeMap<String, Manifest> {
    let ui = m(
        "ui",
        vec![ca("*")],
        vec![ed("user-message", &["text"]), ed("steer", &["text"])],
        vec![
            Capability::StartSession,
            Capability::DeferredPublish,
            Capability::Control,
            Capability::Observe,
        ],
    );
    let mut redact = m("redact", vec![c("user-message", &["text"])], vec![], vec![]);
    redact.transforms = vec![Transform {
        event: "user-message".into(),
        preserves: vec!["text".into()],
        adds: vec!["redacted".into()],
    }];
    let gate = m("gate", vec![c("user-message", &["text"])], vec![], vec![]);
    let chat = m(
        "chat",
        vec![
            c("user-message", &["text"]),
            c("model-response", &["text"]),
            c("steer", &["text"]),
        ],
        vec![
            e("model-request", &["turn"]),
            e("assistant-message", &["text"]),
        ],
        vec![],
    );
    let model = m(
        "model",
        vec![Consume {
            mode: Some(Mode::Async),
            ..c("model-request", &["turn"])
        }],
        vec![e("model-response", &["text"]), e("stream-chunk", &["text"])],
        vec![],
    );
    let ping = m("ping", vec![c("pong", &[])], vec![e("ping", &[])], vec![]);
    let pong = m(
        "pong",
        vec![c("ping", &[]), c("session-started", &[])],
        vec![e("pong", &[])],
        vec![],
    );
    let reporter = m(
        "reporter",
        vec![c("assistant-message", &["text"])],
        vec![e("worker-result", &["text"])],
        vec![Capability::CrossSession],
    );
    let nocross = m(
        "nocross",
        vec![c("assistant-message", &["text"])],
        vec![e("worker-result", &["text"])],
        vec![],
    );
    let collector = m(
        "collector",
        vec![c("worker-result", &["text"])],
        vec![],
        vec![],
    );
    [
        ui, redact, gate, chat, model, ping, pong, reporter, nocross, collector,
    ]
    .into_iter()
    .map(|m| (m.name.clone(), m))
    .collect()
}

fn sub(p: &str) -> Subscriber {
    Subscriber {
        plugin: p.into(),
        mode: None,
    }
}

fn config(chat_subs: &[&str]) -> DeploymentConfig {
    let mut cfg = DeploymentConfig::default();
    for name in manifests().keys() {
        cfg.plugins.insert(
            name.clone(),
            PluginConfig {
                command: vec![name.clone()],
                module: None,
                config: json!({}),
                timeout_ms: None,
                binary_hash: Some(format!("bin-{name}")),
                disabled: false,
            },
        );
    }
    cfg.runtime.max_causal_depth = 12;
    cfg.definitions.insert(
        "chat".into(),
        Definition {
            description: "chat".into(),
            subscribers: chat_subs.iter().map(|s| sub(s)).collect(),
        },
    );
    cfg.definitions.insert(
        "pingpong".into(),
        Definition {
            description: String::new(),
            subscribers: vec![sub("ui"), sub("ping"), sub("pong")],
        },
    );
    cfg.definitions.insert(
        "worker".into(),
        Definition {
            description: String::new(),
            subscribers: vec![
                sub("ui"),
                sub("chat"),
                sub("model"),
                sub("reporter"),
                sub("nocross"),
            ],
        },
    );
    cfg
}

const CHAT: &[&str] = &["ui", "redact", "gate", "chat", "model", "collector"];

// ----------------------------------------------------------------------------
// Simulator: plays plugins against the kernel deterministically.
// ----------------------------------------------------------------------------

#[derive(Default)]
struct Sim {
    k: Kernel,
    logs: BTreeMap<String, Vec<Record>>,
    pending: VecDeque<(String, InvokeRequest)>,
    cancelled: Vec<String>,
    now: u64,
    auto: bool,
}

impl Sim {
    fn new(cfg: &DeploymentConfig) -> Self {
        let mut k = Kernel::new();
        let comp = compile(cfg, &manifests());
        assert!(comp.ok, "{:#?}", comp.diagnostics);
        k.install(comp).unwrap();
        Sim {
            k,
            auto: true,
            ..Default::default()
        }
    }

    fn take(&mut self, fx: Vec<Effect>) {
        for f in fx {
            match f {
                Effect::Append { record } => {
                    let log = self.logs.entry(record.session_id.clone()).or_default();
                    assert_eq!(
                        record.sequence,
                        log.len() as u64 + 1,
                        "sequences are dense and monotonic"
                    );
                    log.push(record);
                }
                Effect::Invoke {
                    plugin, request, ..
                } => self.pending.push_back((plugin, request)),
                Effect::Cancel { invocation_id, .. } => self.cancelled.push(invocation_id),
            }
        }
    }

    fn tick(&mut self) -> u64 {
        self.now += 10;
        self.now
    }

    fn start(&mut self, def: &str, text: Option<&str>) -> String {
        let req = StartRequest {
            plugin: "ui".into(),
            definition: def.into(),
            invocation_id: None,
            initial: text.map(|t| InitialEvent {
                event_name: "user-message".into(),
                payload: json!({ "text": t }),
                ui: None,
            }),
            fork_from: None,
        };
        let now = self.tick();
        let out = self.k.start_session(&req, now);
        assert!(out.error.is_none(), "{:?}", out.error);
        self.take(out.effects);
        out.session_id.unwrap()
    }

    fn publish(&mut self, req: PublishRequest) -> Result<String, String> {
        let now = self.tick();
        let out = self.k.publish(&req, now);
        self.take(out.effects);
        match (out.event_id, out.blocked) {
            (Some(id), None) => Ok(id),
            (_, Some(b)) => Err(b),
            _ => unreachable!(),
        }
    }

    fn complete(&mut self, inv: &str, result: InvocationResult) {
        let now = self.tick();
        let fx = self.k.complete(inv, &result, now);
        self.take(fx);
    }

    /// Default plugin behaviour, used when `auto` is on.
    fn behave(&mut self, plugin: &str, req: &InvokeRequest) {
        let ev = &req.event;
        let inv = req.invocation_id.clone();
        let publish = |name: &str, payload: Value| PublishRequest {
            plugin: plugin.into(),
            invocation_id: Some(inv.clone()),
            cite: None,
            event_name: name.into(),
            payload,
            ui: None,
            lane: Lane::Normal,
            target_session: None,
        };
        let mut result = InvocationResult::default();
        match (plugin, ev.event_name.as_str()) {
            ("redact", "user-message") => {
                let text = ev.payload["text"]
                    .as_str()
                    .unwrap_or("")
                    .replace("secret", "******");
                result.transform = Some(json!({ "text": text, "redacted": true }));
            }
            ("gate", "user-message")
                if ev.payload["text"]
                    .as_str()
                    .unwrap_or("")
                    .contains("forbidden") =>
            {
                result.veto = Some("policy".into());
            }
            ("chat", "user-message" | "steer") => {
                result.contributions = vec![ContextOp::Add {
                    slot: "messages".into(),
                    value: json!({ "role": "user", "content": ev.payload["text"] }),
                }];
                let _ = self.publish(publish("model-request", json!({ "turn": ev.event_id })));
            }
            ("chat", "model-response") => {
                result.contributions = vec![ContextOp::Add {
                    slot: "messages".into(),
                    value: json!({ "role": "assistant", "content": ev.payload["text"] }),
                }];
                let _ = self.publish(publish(
                    "assistant-message",
                    json!({ "text": ev.payload["text"] }),
                ));
            }
            ("model", "model-request") => {
                let n = ev.context.as_ref().map_or(0, Vec::len);
                let _ = self.publish(publish("stream-chunk", json!({ "text": "he" })));
                let _ = self.publish(publish(
                    "model-response",
                    json!({ "text": format!("reply with {n} context items") }),
                ));
            }
            ("ping", "pong") => {
                let _ = self.publish(publish("ping", json!({})));
            }
            ("pong", "session-started") => {
                let _ = self.publish(publish("pong", json!({})));
            }
            ("pong", "ping") => {
                let _ = self.publish(publish("pong", json!({})));
            }
            ("reporter" | "nocross", "assistant-message") => {
                let parent = ev
                    .payload
                    .get("parent")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| PARENT.with(|p| p.borrow().clone()));
                if let Some(parent) = parent {
                    let mut p = publish("worker-result", json!({ "text": ev.payload["text"] }));
                    p.target_session = Some(parent);
                    let _ = self.publish(p);
                }
            }
            _ => {}
        }
        self.complete(&inv, result);
    }

    fn run(&mut self) {
        let mut guard = 0;
        while let Some((plugin, req)) = self.pending.pop_front() {
            guard += 1;
            assert!(guard < 10_000, "runaway");
            if self.cancelled.contains(&req.invocation_id) {
                continue;
            }
            self.behave(&plugin, &req);
        }
    }

    fn log(&self, sid: &str) -> &[Record] {
        &self.logs[sid]
    }

    fn kinds(&self, sid: &str) -> Vec<String> {
        self.log(sid)
            .iter()
            .map(|r| r.body.kind().to_owned())
            .collect()
    }

    fn events(&self, sid: &str) -> Vec<String> {
        self.log(sid)
            .iter()
            .filter_map(|r| match &r.body {
                Body::EventAppended { event } => Some(event.event_name.clone()),
                _ => None,
            })
            .collect()
    }
}

thread_local! {
    static PARENT: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

fn ui_cite(sim: &Sim, sid: &str) -> String {
    sim.log(sid)
        .iter()
        .rev()
        .find_map(|r| match &r.body {
            Body::InvocationStarted {
                invocation_id,
                plugin,
                ..
            } if plugin == "ui" => Some(invocation_id.clone()),
            _ => None,
        })
        .expect("ui has a standing invocation")
}

fn user(sim: &Sim, sid: &str, text: &str, lane: Lane) -> PublishRequest {
    PublishRequest {
        plugin: "ui".into(),
        invocation_id: None,
        cite: Some(ui_cite(sim, sid)),
        event_name: "user-message".into(),
        payload: json!({ "text": text }),
        ui: None,
        lane,
        target_session: None,
    }
}

// ----------------------------------------------------------------------------
// Scenarios
// ----------------------------------------------------------------------------

#[test]
fn chat_turn_runs_pipelines_in_order_and_records_everything() {
    let mut sim = Sim::new(&config(CHAT));
    let sid = sim.start("chat", Some("hello secret world"));
    sim.run();
    assert_eq!(
        sim.events(&sid),
        vec![
            "session-started",
            "user-message",
            "model-request",
            "stream-chunk",
            "model-response",
            "assistant-message"
        ]
    );
    let view = project(sim.log(&sid));
    assert!(
        view.events.iter().all(|e| e.status == "delivered"),
        "{:#?}",
        view.events
            .iter()
            .map(|e| (&e.event_name, &e.status))
            .collect::<Vec<_>>()
    );
    // The blocking chain ran in declared order; the transform reached downstream.
    let um = &view.events[1];
    let order: Vec<_> = um
        .invocations
        .iter()
        .map(|i| (i.plugin.as_str(), i.mode))
        .collect();
    assert_eq!(
        order,
        vec![
            ("redact", Mode::Blocking),
            ("gate", Mode::Blocking),
            ("chat", Mode::Blocking),
            ("ui", Mode::Async)
        ]
    );
    assert_eq!(um.invocations[1].input["text"], "hello ****** world");
    assert_eq!(um.invocations[0].input["text"], "hello secret world");
    // Stamps are recorded on every invocation.
    assert_eq!(um.invocations[0].stamp.binary, "bin-redact");
    // Context: user + assistant messages, attributed to chat.
    assert_eq!(view.context.len(), 2);
    assert!(view.context.iter().all(|c| c.plugin == "chat"));
    // The model saw the user message in its context.
    assert_eq!(view.events[4].payload["text"], "reply with 1 context items");
    // Pipeline outputs have deterministic ids and causes.
    let mr = &view.events[2];
    assert!(mr.event_id.ends_with(".o0"));
    assert!(matches!(&mr.origin, Origin::Pipeline { plugin, .. } if plugin == "chat"));
    assert_eq!(mr.depth, 1);
    assert_eq!(sim.k.status(&sid).unwrap().activity, "idle");
}

#[test]
fn replay_rebuilds_identical_state_and_reruns_identically() {
    let cfg = config(CHAT);
    let mut a = Sim::new(&cfg);
    let sid = a.start("chat", Some("one"));
    a.run();
    let p = a.publish(user(&a, &sid, "two", Lane::Normal));
    assert!(p.is_ok());
    a.run();
    // Deterministic: an identical script yields byte-identical logs.
    let mut b = Sim::new(&cfg);
    let sid_b = b.start("chat", Some("one"));
    b.run();
    b.publish(user(&b, &sid_b, "two", Lane::Normal)).unwrap();
    b.run();
    assert_eq!(
        serde_json::to_string(a.log(&sid)).unwrap(),
        serde_json::to_string(b.log(&sid_b)).unwrap()
    );
    // Replay-as-reading rebuilds the same status and context.
    let mut k = Kernel::new();
    k.install(compile(&cfg, &manifests())).unwrap();
    k.load_session(a.log(&sid)).unwrap();
    assert_eq!(k.status(&sid), a.k.status(&sid));
    assert_eq!(k.context(&sid), a.k.context(&sid));
    // Records round-trip through JSON.
    let json = serde_json::to_string(a.log(&sid)).unwrap();
    let back: Vec<Record> = serde_json::from_str(&json).unwrap();
    assert_eq!(back, a.log(&sid));
}

#[test]
fn veto_settles_the_event_and_skips_async_observers() {
    let mut sim = Sim::new(&config(CHAT));
    let sid = sim.start("chat", Some("this is forbidden"));
    sim.run();
    let view = project(sim.log(&sid));
    let um = &view.events[1];
    assert_eq!(um.status, "vetoed");
    assert!(matches!(&um.settlement, Some(Settlement::Vetoed { plugin, .. }) if plugin == "gate"));
    assert!(
        um.invocations
            .iter()
            .all(|i| i.plugin != "chat" && i.plugin != "ui")
    );
    assert_eq!(sim.events(&sid), vec!["session-started", "user-message"]);
}

#[test]
fn transform_violating_its_declaration_fails_the_pipeline() {
    let mut sim = Sim::new(&config(CHAT));
    sim.auto = false;
    let sid = sim.start("chat", Some("x"));
    // session-started → ui async only.
    while let Some((plugin, req)) = sim.pending.pop_front() {
        if plugin == "redact" {
            sim.complete(
                &req.invocation_id,
                InvocationResult {
                    transform: Some(json!({ "other": 1 })),
                    ..Default::default()
                },
            );
        } else {
            sim.complete(&req.invocation_id, InvocationResult::default());
        }
    }
    let view = project(sim.log(&sid));
    assert_eq!(view.events[1].status, "failed");
    let out = view.events[1].invocations[0].outcome.clone().unwrap();
    assert!(matches!(out, Outcome::Failed { error } if error.contains("preserved key `text`")));
}

#[test]
fn async_subscribers_cannot_veto() {
    let mut sim = Sim::new(&config(CHAT));
    sim.auto = false;
    let sid = sim.start("chat", None);
    let (_, req) = sim.pending.pop_front().unwrap();
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
}

#[test]
fn publish_classification_and_blocking_are_recorded() {
    let mut sim = Sim::new(&config(CHAT));
    let sid = sim.start("chat", None);
    sim.run();
    let base = PublishRequest {
        plugin: "ui".into(),
        invocation_id: None,
        cite: None,
        event_name: "user-message".into(),
        payload: json!({ "text": "hi" }),
        ui: None,
        lane: Lane::Normal,
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
    let chat_inv = format!("{sid}/i999");
    assert!(
        sim.publish(PublishRequest {
            cite: Some(chat_inv),
            ..base.clone()
        })
        .unwrap_err()
        .contains("uncitable")
    );
    // Undeclared emit.
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
    // Missing supplied key.
    assert!(
        sim.publish(PublishRequest {
            cite: Some(cite.clone()),
            payload: json!({}),
            ..base.clone()
        })
        .unwrap_err()
        .contains("missing supplied keys [text]")
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
    // A valid deferred publish.
    let id = sim
        .publish(PublishRequest {
            cite: Some(cite.clone()),
            ui: Some(json!({ "kind": "text", "text": "hi" })),
            ..base.clone()
        })
        .unwrap();
    sim.run();
    let view = project(sim.log(&sid));
    let ev = view.events.iter().find(|e| e.event_id == id).unwrap();
    assert!(matches!(&ev.origin, Origin::Deferred { cites, .. } if cites == &cite));
    assert_eq!(view.blocked.len(), 5, "{:#?}", view.blocked);
    // A non-deferred emit published without an open invocation is blocked.
    let chat_closed = sim.log(&sid).iter().find_map(|r| match &r.body {
        Body::InvocationStarted {
            invocation_id,
            plugin,
            ..
        } if plugin == "chat" => Some(invocation_id.clone()),
        _ => None,
    });
    let late = PublishRequest {
        plugin: "chat".into(),
        invocation_id: chat_closed,
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
fn priority_lane_wins_next_but_never_interrupts() {
    let mut sim = Sim::new(&config(CHAT));
    sim.auto = false;
    let sid = sim.start("chat", None);
    let (_, ui0) = sim.pending.pop_front().unwrap();
    sim.complete(&ui0.invocation_id, InvocationResult::default());
    let cite = ui0.invocation_id.clone();
    let mk = |text: &str, lane: Lane| PublishRequest {
        plugin: "ui".into(),
        invocation_id: None,
        cite: Some(cite.clone()),
        event_name: "user-message".into(),
        payload: json!({ "text": text }),
        ui: None,
        lane,
        target_session: None,
    };
    sim.publish(mk("first", Lane::Normal)).unwrap();
    // First pipeline is now running (redact open). Queue normal, then priority.
    sim.publish(mk("second", Lane::Normal)).unwrap();
    sim.publish(mk("urgent", Lane::Priority)).unwrap();
    let status = sim.k.status(&sid).unwrap();
    assert_eq!((status.queued_normal, status.queued_priority), (1, 1));
    sim.auto = true;
    sim.run();
    let order: Vec<String> = project(sim.log(&sid))
        .events
        .iter()
        .filter(|e| e.event_name == "user-message")
        .map(|e| e.payload["text"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(order, vec!["first", "second", "urgent"], "append order");
    let dispatched: Vec<String> = sim
        .log(&sid)
        .iter()
        .filter_map(|r| match &r.body {
            Body::PipelineStarted { event_id } => Some(event_id.clone()),
            _ => None,
        })
        .collect();
    let view = project(sim.log(&sid));
    let name = |id: &str| {
        view.events
            .iter()
            .find(|e| e.event_id == id)
            .map(|e| {
                e.payload["text"]
                    .as_str()
                    .unwrap_or(&e.event_name)
                    .to_owned()
            })
            .unwrap()
    };
    let users: Vec<String> = dispatched
        .iter()
        .map(|d| name(d))
        .filter(|n| ["first", "second", "urgent"].contains(&n.as_str()))
        .collect();
    assert_eq!(users, vec!["first", "urgent", "second"]);
}

#[test]
fn soft_stop_drains_then_parks_and_resume_continues() {
    let mut sim = Sim::new(&config(CHAT));
    sim.auto = false;
    let sid = sim.start("chat", Some("a"));
    // Pipeline for session-started is running (ui async is not blocking) → settle.
    let fx = sim.k.command(&sid, Command::SoftStop, "test", 1).unwrap();
    sim.take(fx);
    assert_eq!(sim.k.status(&sid).unwrap().state, DispatchState::Draining);
    sim.auto = true;
    sim.run();
    let st = sim.k.status(&sid).unwrap();
    assert_eq!(st.activity, "parked");
    // The in-flight pipeline (session-started) finished; user-message waits.
    assert!(st.queued_normal >= 1);
    let fx = sim.k.command(&sid, Command::Resume, "test", 2).unwrap();
    sim.take(fx);
    sim.run();
    assert_eq!(sim.k.status(&sid).unwrap().activity, "idle");
    assert!(sim.events(&sid).contains(&"assistant-message".to_owned()));
}

#[test]
fn hard_stop_cancels_in_flight_work_and_late_results_are_inert() {
    let mut sim = Sim::new(&config(CHAT));
    sim.auto = false;
    let sid = sim.start("chat", Some("a"));
    // Drive until the model invocation is open.
    let model_inv = loop {
        let (plugin, req) = sim.pending.pop_front().expect("model reached");
        if plugin == "model" {
            break req;
        }
        sim.behave(&plugin, &req);
    };
    let fx = sim.k.command(&sid, Command::HardStop, "user", 99).unwrap();
    sim.take(fx);
    assert!(sim.cancelled.contains(&model_inv.invocation_id));
    let st = sim.k.status(&sid).unwrap();
    assert_eq!(st.state, DispatchState::Halted);
    assert!(st.open_invocations.is_empty());
    // A late completion is ignored; a late publish is classified deferred and blocked.
    let before = sim.log(&sid).len();
    sim.complete(&model_inv.invocation_id, InvocationResult::default());
    assert_eq!(sim.log(&sid).len(), before);
    let late = PublishRequest {
        plugin: "model".into(),
        invocation_id: Some(model_inv.invocation_id.clone()),
        cite: None,
        event_name: "model-response".into(),
        payload: json!({ "text": "late" }),
        ui: None,
        lane: Lane::Normal,
        target_session: None,
    };
    assert!(sim.publish(late).is_err());
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
    let mut sim = Sim::new(&config(CHAT));
    sim.auto = false;
    let sid = sim.start("chat", Some("a"));
    loop {
        let (plugin, req) = sim.pending.pop_front().unwrap();
        if plugin == "redact" {
            let fx = sim.k.command(&sid, Command::HardStop, "user", 5).unwrap();
            sim.take(fx);
            assert!(sim.cancelled.contains(&req.invocation_id));
            break;
        }
        sim.behave(&plugin, &req);
    }
    let view = project(sim.log(&sid));
    assert_eq!(view.events[1].status, "aborted");
    // Resume continues with remaining queued work, not the aborted event.
    let fx = sim.k.command(&sid, Command::Resume, "user", 6).unwrap();
    sim.take(fx);
    sim.auto = true;
    sim.run();
    assert!(!sim.events(&sid).contains(&"model-request".to_owned()));
}

#[test]
fn crash_mid_pipeline_recovers_from_history_with_idempotent_outputs() {
    let cfg = config(CHAT);
    let mut sim = Sim::new(&cfg);
    sim.auto = false;
    let sid = sim.start("chat", Some("hello"));
    // Run until chat's invocation publishes model-request but crash before completion.
    let chat_req = loop {
        let (plugin, req) = sim.pending.pop_front().unwrap();
        if plugin == "chat" {
            break req;
        }
        sim.behave(&plugin, &req);
    };
    sim.publish(PublishRequest {
        plugin: "chat".into(),
        invocation_id: Some(chat_req.invocation_id.clone()),
        cite: None,
        event_name: "model-request".into(),
        payload: json!({ "turn": "t" }),
        ui: None,
        lane: Lane::Normal,
        target_session: None,
    })
    .unwrap();
    let crashed_log = sim.log(&sid).to_vec();
    // "Restart": fresh kernel, replay, recover.
    let mut fresh = Sim::new(&cfg);
    fresh.k.load_session(&crashed_log).unwrap();
    fresh.logs.insert(sid.clone(), crashed_log.clone());
    let fx = fresh.k.recover(&sid, 3, 1000);
    fresh.take(fx);
    let retried: Vec<_> = fresh
        .pending
        .iter()
        .filter(|(_, r)| r.attempt == 2)
        .map(|(p, r)| (p.clone(), r.invocation_id.clone()))
        .collect();
    assert!(
        retried.contains(&("chat".to_owned(), chat_req.invocation_id.clone())),
        "{retried:?}"
    );
    // The orphan's re-publish (same deterministic output id) is deduplicated.
    fresh.run();
    assert!(project(fresh.log(&sid)).blocked.is_empty());
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
    let mut sim = Sim::new(&config(CHAT));
    sim.auto = false;
    let sid = sim.start("chat", None);
    let (_, req) = sim.pending.pop_front().unwrap();
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
fn config_apply_waits_for_the_event_boundary() {
    let cfg = config(CHAT);
    let mut sim = Sim::new(&cfg);
    sim.auto = false;
    let sid = sim.start("chat", Some("first"));
    // Advance until redact (blocking) is open on user-message.
    let redact_req = loop {
        let (plugin, req) = sim.pending.pop_front().unwrap();
        if plugin == "redact" {
            break req;
        }
        sim.behave(&plugin, &req);
    };
    let old = sim.k.active_hash().to_owned();
    // New config: drop the gate.
    let new_cfg = config(&["ui", "redact", "chat", "model", "collector"]);
    let hash = sim.k.install(compile(&new_cfg, &manifests())).unwrap();
    assert_ne!(hash, old);
    let out = sim.k.apply_config(&hash, &Scope::Global, 50);
    sim.take(out.effects);
    assert!(
        !sim.kinds(&sid).contains(&"config-applied".to_owned()),
        "not mid-pipeline"
    );
    sim.behave("redact", &redact_req);
    // Remaining blocking subscribers of the in-flight event still follow the old graph.
    sim.auto = true;
    sim.run();
    let view = project(sim.log(&sid));
    assert_eq!(view.config, hash);
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
            .any(|i| i.plugin == "gate"),
        "old graph for in-flight event"
    );
    // config-applied lifecycle event went through the priority lane.
    let ca = view
        .events
        .iter()
        .find(|e| e.event_name == "config-applied")
        .unwrap();
    assert_eq!(ca.lane, Lane::Priority);
    // Next turn uses the new graph.
    sim.publish(user(&sim, &sid, "second", Lane::Normal))
        .unwrap();
    sim.run();
    let view = project(sim.log(&sid));
    let last = view
        .events
        .iter()
        .rev()
        .find(|e| e.event_name == "user-message")
        .unwrap();
    assert!(last.invocations.iter().all(|i| i.plugin != "gate"));
}

#[test]
fn session_scoped_apply_leaves_other_sessions_alone() {
    let mut sim = Sim::new(&config(CHAT));
    let a = sim.start("chat", None);
    let b = sim.start("chat", None);
    sim.run();
    let hash = sim
        .k
        .install(compile(
            &config(&["ui", "chat", "model", "collector"]),
            &manifests(),
        ))
        .unwrap();
    let out = sim.k.apply_config(
        &hash,
        &Scope::Session {
            session_id: a.clone(),
        },
        5,
    );
    sim.take(out.effects);
    assert_eq!(sim.k.status(&a).unwrap().config, hash);
    assert_ne!(sim.k.status(&b).unwrap().config, hash);
    // A missing definition is skipped, not silently applied.
    let mut cfg = config(CHAT);
    cfg.definitions.remove("chat");
    let h2 = sim.k.install(compile(&cfg, &manifests())).unwrap();
    let out = sim.k.apply_config(&h2, &Scope::Global, 6);
    assert_eq!(out.skipped, vec![b]);
}

#[test]
fn rejected_configs_are_not_installed() {
    let mut k = Kernel::new();
    let mut cfg = config(CHAT);
    cfg.definitions
        .get_mut("chat")
        .unwrap()
        .subscribers
        .push(sub("ghost"));
    let err = k.install(compile(&cfg, &manifests())).unwrap_err();
    assert!(err.contains("ghost"));
}

#[test]
fn causal_depth_bounds_cycles() {
    let mut sim = Sim::new(&config(CHAT));
    let sid = sim.start("pingpong", None);
    sim.run();
    let view = project(sim.log(&sid));
    assert!(!view.blocked.is_empty());
    assert!(view.blocked[0].reason.contains("max_causal_depth=12"));
    assert!(view.events.iter().all(|e| e.depth <= 12));
    assert_eq!(sim.k.status(&sid).unwrap().activity, "idle");
}

#[test]
fn cross_session_publish_requires_capability_and_is_mirrored() {
    let mut sim = Sim::new(&config(CHAT));
    let parent = sim.start("chat", None);
    sim.run();
    PARENT.with(|p| *p.borrow_mut() = Some(parent.clone()));
    let worker = sim.start("worker", Some("do work"));
    sim.run();
    PARENT.with(|p| *p.borrow_mut() = None);
    let pv = project(sim.log(&parent));
    let results: Vec<_> = pv
        .events
        .iter()
        .filter(|e| e.event_name == "worker-result")
        .collect();
    assert_eq!(results.len(), 1, "only the capable reporter got through");
    assert!(
        matches!(&results[0].origin, Origin::CrossSession { from_session, .. } if from_session == &worker)
    );
    assert_eq!(results[0].status, "delivered");
    let wv = project(sim.log(&worker));
    assert_eq!(wv.sent.len(), 1);
    assert!(
        wv.blocked
            .iter()
            .any(|b| b.plugin == "nocross" && b.reason.contains("cross-session"))
    );
}

#[test]
fn fork_seeds_context_and_restore_is_an_append() {
    let mut sim = Sim::new(&config(CHAT));
    let sid = sim.start("chat", Some("one"));
    sim.run();
    let mid = sim.log(&sid).last().unwrap().sequence;
    sim.publish(user(&sim, &sid, "two", Lane::Normal)).unwrap();
    sim.run();
    assert_eq!(sim.k.context(&sid).unwrap().len(), 4);
    assert_eq!(context_at(sim.log(&sid), mid).len(), 2);
    // Fork from the midpoint.
    let now = sim.tick();
    let out = sim.k.start_session(
        &StartRequest {
            plugin: "ui".into(),
            definition: "chat".into(),
            invocation_id: None,
            initial: None,
            fork_from: Some(LogRef {
                session_id: sid.clone(),
                sequence: mid,
            }),
        },
        now,
    );
    sim.take(out.effects);
    let fork = out.session_id.unwrap();
    sim.run();
    assert_eq!(sim.k.context(&fork).unwrap().len(), 2);
    assert!(sim.kinds(&fork).contains(&"context-seeded".to_owned()));
    // Restore in the original session via a contribution; history keeps both states.
    sim.auto = false;
    sim.publish(PublishRequest {
        plugin: "ui".into(),
        invocation_id: None,
        cite: Some(ui_cite(&sim, &sid)),
        event_name: "steer".into(),
        payload: json!({ "text": "restore" }),
        ui: None,
        lane: Lane::Priority,
        target_session: None,
    })
    .unwrap();
    while let Some((plugin, req)) = sim.pending.pop_front() {
        let mut r = InvocationResult::default();
        if plugin == "chat" {
            r.contributions = vec![ContextOp::Restore { to_sequence: mid }];
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
    let before_restore = restore_seq - 1;
    assert_eq!(
        context_at(sim.log(&sid), before_restore).len(),
        4,
        "pre-revert state still reconstructable"
    );
}

#[test]
fn idle_sessions_unload_and_reload_on_demand() {
    let cfg = config(CHAT);
    let mut sim = Sim::new(&cfg);
    let sid = sim.start("chat", Some("hi"));
    sim.run();
    assert!(sim.k.unload_if_idle(&sid));
    assert!(!sim.k.is_loaded(&sid));
    // A publish into an unloaded session asks the host to load it first.
    let req = user(&sim, &sid, "again", Lane::Normal);
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
    sim.run();
    assert_eq!(
        sim.events(&sid)
            .iter()
            .filter(|e| *e == "assistant-message")
            .count(),
        2
    );
}

#[test]
fn start_session_validates_capability_definition_and_supply() {
    let mut sim = Sim::new(&config(CHAT));
    let bad = |plugin: &str, def: &str, payload: Value| StartRequest {
        plugin: plugin.into(),
        definition: def.into(),
        invocation_id: None,
        initial: Some(InitialEvent {
            event_name: "user-message".into(),
            payload,
            ui: None,
        }),
        fork_from: None,
    };
    assert!(
        sim.k
            .start_session(&bad("chat", "chat", json!({ "text": "x" })), 1)
            .error
            .unwrap()
            .contains("start-session capability")
    );
    assert!(
        sim.k
            .start_session(&bad("ui", "nope", json!({ "text": "x" })), 1)
            .error
            .unwrap()
            .contains("unknown session definition")
    );
    assert!(
        sim.k
            .start_session(&bad("ui", "chat", json!({})), 1)
            .error
            .unwrap()
            .contains("missing supplied keys")
    );
    let ok = sim.start("chat", None);
    assert_eq!(ok, "s0001");
    assert_eq!(sim.start("chat", None), "s0002");
}

/// Randomized interleavings: after every step, replaying the log into a fresh
/// kernel reproduces the live kernel's status and context exactly.
#[test]
fn randomized_interleavings_replay_exactly() {
    let cfg = config(CHAT);
    let alt = config(&["ui", "chat", "model", "collector"]);
    for seed in 1..=40u64 {
        let mut rng = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let mut next = move |n: u64| {
            rng = rng
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (rng >> 33) % n
        };
        let mut sim = Sim::new(&cfg);
        let h_alt = sim.k.install(compile(&alt, &manifests())).unwrap();
        let h_main = sim.k.active_hash().to_owned();
        let sid = sim.start("chat", Some("start"));
        for step in 0..60 {
            match next(9) {
                0 | 1 => {
                    let lane = if next(3) == 0 {
                        Lane::Priority
                    } else {
                        Lane::Normal
                    };
                    let text = if next(5) == 0 { "forbidden" } else { "msg" };
                    let _ = sim.publish(user(&sim, &sid, text, lane));
                }
                2..=4 => {
                    if !sim.pending.is_empty() {
                        let i = usize::try_from(next(sim.pending.len() as u64)).unwrap();
                        let (plugin, req) = sim.pending.remove(i).unwrap();
                        if !sim.cancelled.contains(&req.invocation_id) {
                            sim.behave(&plugin, &req);
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
                    let fx = sim.k.command(&sid, cmd, "rng", step).unwrap();
                    sim.take(fx);
                }
                6 => {
                    if let Some((_, req)) = sim.pending.front().cloned() {
                        let fx = sim.k.retry(&req.invocation_id, "chaos", 3, step);
                        sim.take(fx);
                    }
                }
                7 => {
                    let h = if next(2) == 0 { &h_alt } else { &h_main };
                    let out = sim.k.apply_config(h, &Scope::Global, step);
                    sim.take(out.effects);
                }
                _ => {
                    let fx = sim.k.command(&sid, Command::Resume, "rng", step).unwrap();
                    sim.take(fx);
                }
            }
            let mut k = Kernel::new();
            k.install(compile(&cfg, &manifests())).unwrap();
            k.install(compile(&alt, &manifests())).unwrap();
            k.set_active(sim.k.active_hash()).unwrap();
            k.load_session(sim.log(&sid)).unwrap();
            let mut live = sim.k.status(&sid).unwrap();
            let mut replayed = k.status(&sid).unwrap();
            // A replayed session may pick up the active config lazily; compare the rest.
            live.pending_config = None;
            replayed.pending_config = None;
            assert_eq!(replayed, live, "seed {seed} step {step}");
            assert_eq!(
                k.context(&sid),
                sim.k.context(&sid),
                "seed {seed} step {step}"
            );
            let view = project(sim.log(&sid));
            assert_eq!(view.last_sequence, sim.log(&sid).len() as u64);
        }
    }
}

#[test]
fn published_links_survive_replay_and_retry() {
    let cfg = config(CHAT);
    let mut sim = Sim::new(&cfg);
    sim.auto = false;
    let sid = sim.start("chat", Some("hi"));
    let model = loop {
        let (plugin, req) = sim.pending.pop_front().unwrap();
        if plugin == "model" {
            break req;
        }
        sim.behave(&plugin, &req);
    };
    let mk = |t: &str| PublishRequest {
        plugin: "model".into(),
        invocation_id: Some(model.invocation_id.clone()),
        cite: None,
        event_name: "stream-chunk".into(),
        payload: json!({ "text": t }),
        ui: None,
        lane: Lane::Normal,
        target_session: None,
    };
    sim.publish(mk("a")).unwrap();
    // Crash + replay + retry: the earlier output stays linked to the invocation.
    let log = sim.log(&sid).to_vec();
    let mut fresh = Sim::new(&cfg);
    fresh.k.load_session(&log).unwrap();
    fresh.logs.insert(sid.clone(), log);
    let fx = fresh.k.recover(&sid, 3, 500);
    fresh.take(fx);
    let dup = fresh.k.publish(&mk("a"), 501);
    assert!(dup.duplicate);
    fresh.publish(mk("b")).unwrap();
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
