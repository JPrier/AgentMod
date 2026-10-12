//! Event families and tool calls (compiler.md §3.3, §3.4): the compiler
//! resolves a family emit into the concrete members the definition's
//! subscribers name, each plugin gets its resolved emit set, a member's
//! subscribers are ordinary subscribers, publishing outside the resolved set
//! is an undeclared emit, and any failed invocation becomes the core's
//! `invocation-failed`.

mod common;

use std::collections::BTreeMap;

use agentmod_core::compiler::{Compilation, compile};
use agentmod_core::core_manifest::INVOCATION_FAILED;
use agentmod_core::kernel::{
    Effect, InvocationResult, InvokeRequest, Kernel, PublishRequest, Scope,
};
use agentmod_core::manifest::{DeploymentConfig, Manifest};
use agentmod_core::record::{Body, Outcome};
use agentmod_core::types::ContextOp;
use common::{S, Sim, codes, deployment, k, opt};
use serde_json::{Value, json};

/// ui (frontend), agent (emits the `tool-call/*` family, collects results,
/// answers failures), gate (blocking subscriber of the family), audit (async
/// observer), gh-watch (observer of a sub-family: adds no members), and tools:
/// files (two members), shell (one), backup-shell (also `shell`), gh (an MCP
/// member). The tools declare their arguments in `receives.args`.
fn manifests() -> BTreeMap<String, Manifest> {
    let call = |args: Value| json!({ "call_id": k(S), "name": k(S), "args": args });
    let tool = |events: &[(&str, Value)]| -> Value {
        json!({
            "subscribe": events.iter().map(|(e, args)| json!({ "event": e, "mode": "async", "description": format!("the {e} tool"), "hints": { "tier": "core" }, "receives": call(args.clone()) })).collect::<Vec<_>>(),
            "emit": [{ "event": "tool-result", "keys": { "call_id": k(S), "by": k(S) } }]
        })
    };
    let path = json!({ "shape": "object", "keys": { "path": { "shape": "string", "description": "file path" } } });
    let command =
        json!({ "shape": "object", "keys": { "command": k(S), "timeout": opt("integer") } });
    common::manifests(json!({
        "ui": {
            "subscribe": [{ "event": "session-started", "mode": "async" }],
            "emit": [{ "event": "user-message", "deferred": true, "keys": { "text": k(S) } }],
            "capabilities": ["start-session", "deferred-publish", "observe"],
            "sink": true
        },
        "agent": {
            "subscribe": [
                { "event": "user-message", "mode": "blocking", "receives": { "text": k(S) } },
                { "event": "tool-result", "mode": "blocking", "actions": ["contribute-context"], "receives": { "call_id": k(S) } },
                { "event": INVOCATION_FAILED, "mode": "blocking", "actions": ["contribute-context"], "receives": { "event_name": k(S), "payload": k("object"), "plugin": k(S), "error": k(S) } }
            ],
            "emit": [
                { "event": "tool-call/*", "keys": { "call_id": k(S), "name": k(S), "args": k("object") } },
                { "event": "tool-call-rejected", "keys": { "call_id": k(S), "output": k(S) } }
            ]
        },
        "gate": { "subscribe": [{ "event": "tool-call/*", "mode": "blocking", "actions": ["veto"], "receives": { "call_id": k(S), "name": k(S) } }] },
        "audit": { "subscribe": [{ "event": "tool-call/*", "mode": "async" }], "sink": true },
        "gh-watch": { "subscribe": [{ "event": "tool-call/mcp__gh__*", "mode": "async" }], "sink": true },
        "files": tool(&[("tool-call/read_file", path.clone()), ("tool-call/list_dir", path)]),
        "shell": tool(&[("tool-call/shell", command.clone())]),
        "backup-shell": tool(&[("tool-call/shell", json!({ "shape": "object", "keys": { "command": k(S), "cwd": opt(S) } }))]),
        "gh": tool(&[("tool-call/mcp__gh__issues", k("object"))])
    }))
}

fn config(plugins: &[&str]) -> DeploymentConfig {
    let mut ps = serde_json::Map::new();
    for p in plugins {
        let entry = match *p {
            "agent" => json!({ "phase": "loop" }),
            "gate" => json!({ "phase": "guard" }),
            _ => json!({}),
        };
        ps.insert((*p).to_owned(), entry);
    }
    deployment(
        &manifests(),
        json!({ "coder": { "runtime": "0.1", "phases": ["guard", "loop"], "plugins": ps } }),
    )
}

const ALL: &[&str] = &[
    "ui", "agent", "gate", "audit", "gh-watch", "files", "shell", "gh",
];

fn compiled(cfg: &DeploymentConfig) -> Compilation {
    let c = compile(cfg, &manifests());
    assert!(c.ok, "{:#?}", c.errors());
    c
}

/// The agent behaves like chat-context: each word of a user message is a tool
/// call the model asked for. A name outside the agent's resolved emit set is
/// answered by the agent itself (never published) unless `publish_unknown`.
/// A tool named in `fail` reports an error instead of answering; failures of
/// tool calls reach the agent as `invocation-failed` and become answers.
struct Behaviour {
    fail: Option<&'static str>,
    publish_unknown: bool,
    blocked: Vec<String>,
}

impl Behaviour {
    fn new(fail: Option<&'static str>) -> Self {
        Self {
            fail,
            publish_unknown: false,
            blocked: Vec::new(),
        }
    }

    fn act(&mut self, sim: &mut Sim, plugin: &str, req: &InvokeRequest) -> InvocationResult {
        let ev = &req.event;
        match (plugin, ev.event_name.as_str()) {
            ("agent", "user-message") => {
                // The agent's resolved emit set under this invocation's revision.
                let set = sim.k.emit_set(&req.revision, "agent").unwrap();
                for (i, name) in ev.payload["text"]
                    .as_str()
                    .unwrap_or("")
                    .split_whitespace()
                    .enumerate()
                {
                    let call_id = format!("{}:{i}", ev.event_id);
                    let event = format!("tool-call/{name}");
                    if !self.publish_unknown && !set.iter().any(|e| e.event == event) {
                        sim.emit(
                            "agent",
                            &req.invocation_id,
                            "tool-call-rejected",
                            json!({ "call_id": call_id, "output": format!("no such tool: {name}") }),
                        )
                        .unwrap();
                        continue;
                    }
                    let payload = json!({ "call_id": call_id, "name": name, "args": { "path": "a", "command": "ls" } });
                    if let Err(e) = sim.emit("agent", &req.invocation_id, &event, payload) {
                        self.blocked.push(e);
                    }
                }
                InvocationResult::default()
            }
            ("agent", INVOCATION_FAILED) => InvocationResult {
                contributions: vec![ContextOp::Add {
                    slot: "answers".into(),
                    value: json!({ "call": ev.payload["payload"]["call_id"], "error": ev.payload["error"], "plugin": ev.payload["plugin"] }),
                }],
                ..Default::default()
            },
            ("files" | "shell" | "backup-shell" | "gh", name) if name.starts_with("tool-call/") => {
                if self.fail == Some(plugin) {
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

/// (event name, plugin) of every invocation in a session.
fn started(sim: &Sim, sid: &str) -> Vec<(String, String)> {
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
                event_id, plugin, ..
            } => Some((names[event_id].clone(), plugin.clone())),
            _ => None,
        })
        .collect()
}

fn go(sim: &mut Sim, text: &str, b: &mut Behaviour) -> String {
    let sid = sim.start("ui", "coder", None);
    sim.run(&mut |s, p, r| b.act(s, p, r));
    sim.publish(user(sim, &sid, text)).unwrap();
    sim.run(&mut |s, p, r| b.act(s, p, r));
    sid
}

fn answers(sim: &Sim, sid: &str) -> Vec<Value> {
    sim.k
        .context(sid)
        .unwrap()
        .iter()
        .filter(|c| c.slot == "answers")
        .map(|c| c.value.clone())
        .collect()
}

// ----------------------------------------------------------------------------
// Compile time: resolution and resolved emit sets.
// ----------------------------------------------------------------------------

#[test]
fn a_family_resolves_into_exactly_the_members_its_subscribers_name() {
    let c = compiled(&config(ALL));
    let r = &c.definitions["coder"];
    // Only concrete-member subscriptions define members; gate, audit (the
    // family) and gh-watch (a sub-family) observe without adding any.
    let members = vec![
        "tool-call/list_dir",
        "tool-call/mcp__gh__issues",
        "tool-call/read_file",
        "tool-call/shell",
    ];
    assert_eq!(r.report.families["tool-call/*"]["agent"], members);
    // The compiled graph holds only concrete events.
    assert!(r.graph.edges.iter().all(|e| !e.event.contains('*')));
    // The agent's resolved emit set: its single events plus the members.
    let set: Vec<&str> = r
        .emit_set("agent")
        .iter()
        .map(|e| e.event.as_str())
        .collect();
    assert_eq!(
        set,
        vec![
            "tool-call-rejected",
            "tool-call/list_dir",
            "tool-call/mcp__gh__issues",
            "tool-call/read_file",
            "tool-call/shell",
        ]
    );
    // Each member carries its subscribers' declarations: the tool's
    // description, hints, and argument schema.
    let read = r.resolved_emit("agent", "tool-call/read_file").unwrap();
    assert_eq!(read.family.as_deref(), Some("tool-call/*"));
    assert_eq!(read.subscribers.len(), 1);
    assert_eq!(read.subscribers[0].plugin, "files");
    assert_eq!(
        read.subscribers[0].description,
        "the tool-call/read_file tool"
    );
    assert_eq!(read.subscribers[0].hints["tier"], "core");
    // The family's open `args` is narrowed to what the member's subscribers declare.
    let args = serde_json::to_value(&read.keys["args"]).unwrap();
    assert_eq!(
        args,
        json!({ "shape": "object", "keys": { "path": { "shape": "string", "description": "file path" } } })
    );
    // An open `args` subscriber leaves the member's args open.
    let gh = r
        .resolved_emit("agent", "tool-call/mcp__gh__issues")
        .unwrap();
    assert_eq!(
        serde_json::to_value(&gh.keys["args"]).unwrap(),
        json!({ "shape": "object" })
    );
    // Reported as information, never an error.
    let info: Vec<_> = c
        .diagnostics
        .iter()
        .filter(|d| d.code == "resolved-family")
        .collect();
    assert_eq!(info.len(), 1);
    assert!(info[0].message.contains("resolved to 4 members"));
}

#[test]
fn a_definition_without_tools_resolves_the_family_to_nothing() {
    let c = compile(&config(&["ui", "agent", "audit"]), &manifests());
    let r = &c.definitions["coder"];
    assert!(r.report.families["tool-call/*"]["agent"].is_empty());
    assert!(
        !r.emit_set("agent")
            .iter()
            .any(|e| e.event.starts_with("tool-call/"))
    );
    assert!(
        c.diagnostics
            .iter()
            .any(|d| d.code == "resolved-family" && d.message.contains("resolves to nothing"))
    );
    // The observer, which only watches the family, is now fed by nothing.
    assert!(codes(&c.diagnostics).contains(&"never-fires"));
}

#[test]
fn several_subscribers_of_one_member_all_run_and_their_args_merge() {
    let c = compiled(&config(&[
        "ui",
        "agent",
        "gate",
        "audit",
        "files",
        "shell",
        "backup-shell",
    ]));
    let r = &c.definitions["coder"];
    let shell = r.resolved_emit("agent", "tool-call/shell").unwrap();
    let subs: Vec<&str> = shell
        .subscribers
        .iter()
        .map(|s| s.plugin.as_str())
        .collect();
    assert_eq!(subs, vec!["backup-shell", "shell"]);
    // The member's args are the union: `command` required (both require it),
    // `timeout` and `cwd` optional.
    let args = serde_json::to_value(&shell.keys["args"]).unwrap();
    assert_eq!(
        args["keys"],
        json!({ "command": { "shape": "string" }, "cwd": { "shape": "string", "optional": true }, "timeout": { "shape": "integer", "optional": true } })
    );
    // No error and no warning for two answering plugins: the author's choice.
    assert!(
        c.diagnostics
            .iter()
            .all(|d| d.severity == agentmod_core::compiler::Severity::Info),
        "{:#?}",
        c.diagnostics
    );
    let mut sim = Sim::new(c);
    let sid = go(&mut sim, "shell", &mut Behaviour::new(None));
    let to: Vec<String> = started(&sim, &sid)
        .into_iter()
        .filter(|(e, _)| e == "tool-call/shell")
        .map(|(_, p)| p)
        .collect();
    assert_eq!(to, vec!["gate", "audit", "backup-shell", "shell"]);
    assert_eq!(sim.payloads(&sid, "tool-result").len(), 2);
}

#[test]
fn member_subscribers_that_disagree_on_an_argument_shape_are_an_error() {
    let mut ms = serde_json::to_value(manifests()).unwrap();
    ms["backup-shell"]["subscribe"][0]["receives"]["args"]["keys"]["command"] =
        json!({ "shape": "array<string>" });
    let ms: BTreeMap<String, Manifest> = serde_json::from_value(ms).unwrap();
    let c = compile(&config(&["ui", "agent", "shell", "backup-shell"]), &ms);
    let d = c
        .diagnostics
        .iter()
        .find(|d| d.code == "conflicting-member-shape")
        .expect("conflict reported");
    assert_eq!(d.event.as_deref(), Some("tool-call/shell"));
    assert_eq!(d.key.as_deref(), Some("args.command"));
    assert!(!c.ok);
}

// ----------------------------------------------------------------------------
// Runtime: routing, the resolved set, and failures.
// ----------------------------------------------------------------------------

#[test]
fn each_call_reaches_the_subscribers_of_its_member_and_the_observers() {
    let mut sim = Sim::new(compiled(&config(ALL)));
    let sid = go(
        &mut sim,
        "read_file shell mcp__gh__issues list_dir",
        &mut Behaviour::new(None),
    );
    let calls: Vec<(String, String)> = started(&sim, &sid)
        .into_iter()
        .filter(|(e, _)| e.starts_with("tool-call/"))
        .collect();
    let tools: Vec<(String, String)> = calls
        .iter()
        .filter(|(_, p)| !["gate", "audit", "gh-watch"].contains(&p.as_str()))
        .cloned()
        .collect();
    assert_eq!(
        tools,
        vec![
            ("tool-call/read_file".into(), "files".into()),
            ("tool-call/shell".into(), "shell".into()),
            ("tool-call/mcp__gh__issues".into(), "gh".into()),
            ("tool-call/list_dir".into(), "files".into()),
        ]
    );
    // Per call: gate (blocking), audit (observer), the member's subscriber;
    // plus gh-watch for the gh member. No other tool plugin sees a call.
    assert_eq!(calls.len(), 4 * 3 + 1, "{calls:?}");
    assert_eq!(sim.payloads(&sid, "tool-result").len(), 4);
}

#[test]
fn every_plugin_invocation_carries_its_revision_for_the_emit_set() {
    let c = compiled(&config(ALL));
    let rev = c.definitions["coder"].revision.clone();
    let mut sim = Sim::new(c);
    let sid = sim.start("ui", "coder", None);
    let (_, _, req) = sim.pending.front().cloned().expect("ui is invoked");
    assert_eq!(req.revision, rev);
    assert_eq!(sim.k.session_revision(&sid).unwrap().revision, rev);
    let set = sim.k.emit_set(&rev, "agent").unwrap();
    assert!(set.iter().any(|e| e.event == "tool-call/read_file"));
    assert!(sim.k.emit_set("no-such-revision", "agent").is_none());
}

/// compiler.md §11: the model invents a tool.
#[test]
fn the_model_inventing_a_tool_is_answered_by_the_emitter_and_never_published() {
    let mut sim = Sim::new(compiled(&config(ALL)));
    let mut b = Behaviour::new(None);
    let sid = go(&mut sim, "delete_everything read_file", &mut b);
    // The agent answered it itself, from its resolved emit set.
    let rejected = sim.payloads(&sid, "tool-call-rejected");
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0]["output"], "no such tool: delete_everything");
    assert!(
        !sim.events(&sid)
            .iter()
            .any(|e| e == "tool-call/delete_everything")
    );
    assert!(sim.breaches(&sid).is_empty());
    // The real tool still ran.
    assert_eq!(sim.payloads(&sid, "tool-result").len(), 1);
}

#[test]
fn publishing_outside_the_resolved_set_is_an_undeclared_emit() {
    let mut sim = Sim::new(compiled(&config(ALL)));
    let mut b = Behaviour::new(None);
    b.publish_unknown = true;
    let sid = go(&mut sim, "delete_everything", &mut b);
    // A hard failure like any other undeclared emit: blocked and recorded
    // against the declaration it broke. No separate rejection path.
    assert_eq!(b.blocked.len(), 1, "{:?}", b.blocked);
    assert!(b.blocked[0].contains("undeclared emit `tool-call/delete_everything`"));
    let breaches = sim.breaches(&sid);
    assert_eq!(breaches.len(), 1);
    assert_eq!(breaches[0]["declaration"]["kind"], "emit");
    assert_eq!(breaches[0]["reaction"], "publish-blocked");
    assert!(!sim.events(&sid).iter().any(|e| e.starts_with("tool-call/")));
}

#[test]
fn a_call_whose_arguments_contradict_the_member_shape_is_a_hard_failure() {
    let mut sim = Sim::new(compiled(&config(ALL)));
    let sid = sim.start("ui", "coder", None);
    sim.run(&mut |s, p, r| Behaviour::new(None).act(s, p, r));
    sim.publish(user(&sim, &sid, "")).unwrap();
    let inv = sim
        .pending
        .iter()
        .find(|(p, _, r)| p == "agent" && r.event.event_name == "user-message")
        .map(|(_, _, r)| r.invocation_id.clone())
        .unwrap();
    let err = sim
        .emit(
            "agent",
            &inv,
            "tool-call/read_file",
            json!({ "call_id": "c", "name": "read_file", "args": { "path": 7 } }),
        )
        .unwrap_err();
    assert!(err.contains("args.path"), "{err}");
}

/// A crashed tool reaches the model through `invocation-failed`.
#[test]
fn a_crashed_tool_reaches_the_loop_through_invocation_failed() {
    let mut sim = Sim::new(compiled(&config(ALL)));
    let sid = go(
        &mut sim,
        "read_file shell",
        &mut Behaviour::new(Some("shell")),
    );
    let failed = sim.payloads(&sid, INVOCATION_FAILED);
    assert_eq!(failed.len(), 1, "{failed:#?}");
    assert_eq!(failed[0]["event_name"], "tool-call/shell");
    assert_eq!(failed[0]["plugin"], "shell");
    assert_eq!(failed[0]["mode"], "async");
    assert_eq!(failed[0]["error"], "boom");
    assert_eq!(
        failed[0]["payload"]["call_id"],
        sim.payloads(&sid, "tool-call/shell")[0]["call_id"]
    );
    // Caused by the failed invocation, emitted by the core.
    let rec = sim
        .log(&sid)
        .iter()
        .find_map(|r| match &r.body {
            Body::EventAppended { event } if event.event_name == INVOCATION_FAILED => {
                Some(event.clone())
            }
            _ => None,
        })
        .unwrap();
    assert!(matches!(rec.origin, agentmod_core::types::Origin::Core));
    assert!(matches!(
        rec.cause,
        agentmod_core::types::Cause::Invocation { ref invocation_id } if *invocation_id == failed[0]["invocation_id"]
    ));
    // The loop turned it into an answer, so the call does not leave it waiting.
    let a = answers(&sim, &sid);
    assert_eq!(a.len(), 1);
    assert_eq!(a[0]["plugin"], "shell");
    assert_eq!(a[0]["call"], failed[0]["payload"]["call_id"]);
    // The other call was answered normally.
    assert_eq!(sim.payloads(&sid, "tool-result").len(), 1);
}

#[test]
fn any_failed_invocation_is_reported_but_a_failure_handling_one_is_not_reported_again() {
    let mut sim = Sim::new(compiled(&config(ALL)));
    let sid = sim.start("ui", "coder", None);
    sim.run(&mut |s, p, r| Behaviour::new(None).act(s, p, r));
    sim.publish(user(&sim, &sid, "read_file")).unwrap();
    // The blocking gate fails: its pipeline fails and the tool never runs.
    let mut b = Behaviour::new(None);
    sim.run(&mut |s, p, r| {
        if p == "gate" {
            return InvocationResult {
                error: Some("gate crashed".into()),
                ..Default::default()
            };
        }
        if p == "agent" && r.event.event_name == INVOCATION_FAILED {
            // The loop itself fails while handling the report.
            return InvocationResult {
                error: Some("loop crashed too".into()),
                ..Default::default()
            };
        }
        b.act(s, p, r)
    });
    let failed = sim.payloads(&sid, INVOCATION_FAILED);
    assert_eq!(failed.len(), 1, "{failed:#?}");
    assert_eq!(failed[0]["plugin"], "gate");
    assert_eq!(failed[0]["mode"], "blocking");
    assert!(sim.payloads(&sid, "tool-result").is_empty());
    // The second failure is recorded on its invocation, not re-reported.
    assert_eq!(
        sim.log(&sid)
            .iter()
            .filter(|r| matches!(
                &r.body,
                Body::InvocationCompleted {
                    outcome: Outcome::Failed { .. },
                    ..
                }
            ))
            .count(),
        2
    );
}

#[test]
fn members_change_only_at_a_revision_boundary_and_retries_keep_their_executor() {
    let mut sim = Sim::new(compiled(&config(ALL)));
    let mut b = Behaviour::new(None);
    let sid = go(&mut sim, "", &mut b);
    sim.publish(user(&sim, &sid, "shell")).unwrap();
    // Hold the tool's invocation open.
    let mut held = None;
    while let Some((plugin, stamp, req)) = sim.pending.pop_front() {
        if plugin == "shell" {
            held = Some((req.invocation_id.clone(), stamp));
            continue;
        }
        let r = b.act(&mut sim, &plugin, &req);
        sim.complete(&req.invocation_id, r);
    }
    let (held_inv, held_stamp) = held.expect("shell dispatched");
    let held_rev = sim.k.invocation_revision(&held_inv).unwrap();
    // New config: the same plugin with a new config stamp (a hot swap of the tool).
    let mut cfg2 = config(ALL);
    cfg2.plugins.get_mut("shell").unwrap().config = json!({ "v": 2 });
    let h2 = sim.k.install(compiled(&cfg2)).unwrap();
    sim.now += 1;
    let out = sim.k.apply_config(&h2, &Scope::Global, sim.now);
    sim.take(out.effects);
    // The plugin crashed: its retry goes to the executor recorded at dispatch,
    // under the revision it started with.
    sim.now += 1;
    let fx = sim.k.retry(&held_inv, "crash", 3, sim.now);
    let retried = fx.iter().find_map(|f| match f {
        Effect::Invoke { stamp, request, .. } => Some((stamp.clone(), request.revision.clone())),
        _ => None,
    });
    assert_eq!(retried, Some((held_stamp.clone(), held_rev.clone())));
    sim.take(fx);
    sim.pending.clear();
    sim.complete(&held_inv, InvocationResult::default());
    sim.run(&mut |s, p, r| b.act(s, p, r));
    // The next call dispatches under the new revision's stamp.
    sim.publish(user(&sim, &sid, "shell")).unwrap();
    let mut new_stamp = None;
    while let Some((plugin, stamp, req)) = sim.pending.pop_front() {
        if plugin == "shell" {
            new_stamp = Some(stamp);
        }
        let r = b.act(&mut sim, &plugin, &req);
        sim.complete(&req.invocation_id, r);
    }
    let new_stamp = new_stamp.expect("dispatched");
    assert_ne!(new_stamp, held_stamp);
    assert_eq!(new_stamp.config, cfg2.plugins["shell"].stamp().config);
}

#[test]
fn a_new_revision_changes_the_resolved_set() {
    let c1 = compiled(&config(&["ui", "agent", "files"]));
    let rev1 = c1.definitions["coder"].revision.clone();
    let mut sim = Sim::new(c1);
    let mut b = Behaviour::new(None);
    let sid = go(&mut sim, "shell", &mut b);
    // shell is not a tool of this definition yet.
    assert_eq!(sim.payloads(&sid, "tool-call-rejected").len(), 1);
    let c2 = compiled(&config(&["ui", "agent", "files", "shell"]));
    let rev2 = c2.definitions["coder"].revision.clone();
    assert_ne!(rev1, rev2);
    let h2 = sim.k.install(c2).unwrap();
    sim.now += 1;
    let out = sim.k.apply_config(&h2, &Scope::Global, sim.now);
    sim.take(out.effects);
    sim.run(&mut |s, p, r| b.act(s, p, r));
    assert!(
        sim.k
            .emit_set(&rev2, "agent")
            .unwrap()
            .iter()
            .any(|e| e.event == "tool-call/shell")
    );
    sim.publish(user(&sim, &sid, "shell")).unwrap();
    sim.run(&mut |s, p, r| b.act(s, p, r));
    assert_eq!(sim.payloads(&sid, "tool-result").len(), 1);
}

#[test]
fn family_logs_replay_exactly() {
    let c = compiled(&config(ALL));
    let mut sim = Sim::new(c.clone());
    let sid = go(
        &mut sim,
        "read_file nope shell",
        &mut Behaviour::new(Some("files")),
    );
    let mut fresh = Kernel::new();
    fresh.install(c).unwrap();
    fresh.load_session(&sim.logs[&sid]).unwrap();
    assert_eq!(sim.k.status(&sid), fresh.status(&sid));
    assert_eq!(sim.k.context(&sid), fresh.context(&sid));
    assert!(sim.logs[&sid].iter().any(|r| matches!(
        &r.body,
        Body::InvocationCompleted {
            outcome: Outcome::Failed { .. },
            ..
        }
    )));
    assert_eq!(sim.payloads(&sid, INVOCATION_FAILED).len(), 1);
}

#[test]
fn metrics_count_member_and_observer_dispatches() {
    let mut sim = Sim::new(compiled(&config(ALL)));
    let sid = go(
        &mut sim,
        "read_file shell list_dir",
        &mut Behaviour::new(None),
    );
    let m = agentmod_core::metrics::session_metrics(&sim.logs[&sid]);
    assert_eq!(m["tool_calls"], 3.0);
    assert_eq!(m["member_dispatches"], 3.0);
    // audit observes each call (an async family subscriber; the gate is blocking).
    assert_eq!(m["observer_dispatches"], 3.0);
    assert_eq!(m["tool_call_invocations"], 9.0);
    assert_eq!(m["ratios"]["plugin_invocations_per_tool_call"], 3.0);
    assert_eq!(
        m["records"].as_f64().unwrap() as usize,
        sim.logs[&sid].len()
    );
}
