//! Compiled keyed dispatch: tool calls reach exactly one compiled owner.

use std::collections::BTreeMap;

use agentmod_core::compiler::{Compilation, DISPATCH_FAILED, compile};
use agentmod_core::kernel::{
    Effect, InitialEvent, InvocationResult, InvokeRequest, Kernel, PublishRequest, Scope,
    StartRequest,
};
use agentmod_core::manifest::{
    Capability, Consume, Definition, DeploymentConfig, Emit, Keyed, Manifest, PluginConfig,
    Subscriber, ToolDecl,
};
use agentmod_core::record::{Body, Outcome, Record};
use agentmod_core::types::{Lane, Mode, Stamp};
use serde_json::{Value, json};

fn consume(event: &str, mode: Mode) -> Consume {
    Consume {
        event: event.into(),
        demands: vec![],
        mode: Some(mode),
        context: false,
        keyed: None,
    }
}

fn owner(values: &[&str]) -> Consume {
    Consume {
        keyed: Some(Keyed {
            key: "name".into(),
            values: values.iter().map(|v| (*v).into()).collect(),
        }),
        demands: vec!["call_id".into(), "name".into()],
        ..consume("tool-call", Mode::Async)
    }
}

fn emit(event: &str, supplies: &[&str]) -> Emit {
    Emit {
        event: event.into(),
        supplies: supplies.iter().map(|s| (*s).into()).collect(),
        deferred: false,
    }
}

fn tool(name: &str) -> ToolDecl {
    ToolDecl {
        name: name.into(),
        parameters: json!({ "path": { "type": "string" } }),
        required: vec![],
        tier: None,
    }
}

fn manifest(name: &str, consumes: Vec<Consume>, emits: Vec<Emit>) -> Manifest {
    Manifest {
        name: name.into(),
        version: "1.2.0".into(),
        consumes,
        emits,
        ..Default::default()
    }
}

/// ui (frontend), agent (issues tool calls, collects results), gate (blocking
/// observer), audit (async observer), and three owners.
fn manifests() -> BTreeMap<String, Manifest> {
    let mut ui = manifest(
        "ui",
        vec![consume("*", Mode::Async)],
        vec![Emit {
            deferred: true,
            ..emit("user-message", &["text"])
        }],
    );
    ui.capabilities = vec![
        Capability::StartSession,
        Capability::DeferredPublish,
        Capability::Control,
    ];
    let agent = manifest(
        "agent",
        vec![
            consume("user-message", Mode::Blocking),
            consume("tool-result", Mode::Blocking),
            consume(DISPATCH_FAILED, Mode::Blocking),
        ],
        vec![
            emit("tool-call", &["call_id", "name"]),
            emit("tool-result", &["call_id"]),
        ],
    );
    let gate = manifest("gate", vec![consume("tool-call", Mode::Blocking)], vec![]);
    let audit = manifest("audit", vec![consume("tool-call", Mode::Async)], vec![]);
    let mut files = manifest(
        "files",
        vec![owner(&["read_file", "list_dir"])],
        vec![emit("tool-result", &["call_id"])],
    );
    files.tools = vec![tool("read_file"), tool("list_dir")];
    let mut shell = manifest(
        "shell",
        vec![owner(&["shell"])],
        vec![emit("tool-result", &["call_id"])],
    );
    shell.tools = vec![tool("shell")];
    let mut mcp = manifest(
        "mcp",
        vec![owner(&["mcp__*", "mcp__gh__special"])],
        vec![emit("tool-result", &["call_id"])],
    );
    mcp.tools = vec![tool("mcp__*")];
    [ui, agent, gate, audit, files, shell, mcp]
        .into_iter()
        .map(|m| (m.name.clone(), m))
        .collect()
}

fn config(members: &[&str]) -> DeploymentConfig {
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
                version: None,
            },
        );
    }
    cfg.definitions.insert(
        "coder".into(),
        Definition {
            subscribers: members
                .iter()
                .map(|p| Subscriber {
                    plugin: (*p).into(),
                    mode: None,
                })
                .collect(),
            ..Default::default()
        },
    );
    cfg
}

const ALL: &[&str] = &["ui", "gate", "agent", "audit", "files", "shell", "mcp"];

fn codes(c: &Compilation) -> Vec<&str> {
    c.diagnostics.iter().map(|d| d.code.as_str()).collect()
}

// ----------------------------------------------------------------------------
// Compile-time ownership.
// ----------------------------------------------------------------------------

#[test]
fn owners_compile_into_one_table_per_keyed_event() {
    let c = compile(&config(ALL), &manifests());
    assert!(c.ok, "{:#?}", c.diagnostics);
    let p = c.definitions["coder"].pipeline("tool-call");
    let r = p.routed.as_ref().expect("route table");
    assert_eq!(r.key, "name");
    assert_eq!(r.owner("read_file").unwrap().plugin, "files");
    assert_eq!(r.owner("shell").unwrap().plugin, "shell");
    // Longest claim wins: the exact `mcp__gh__special` and the `mcp__*` family.
    assert_eq!(r.owner("mcp__gh__special").unwrap().plugin, "mcp");
    assert_eq!(r.owner("mcp__gh__issues").unwrap().plugin, "mcp");
    assert!(r.owner("write_file").is_none());
    // Owners are not broadcast observers.
    let observers: Vec<&str> = p.asyncs.iter().map(|s| s.plugin.as_str()).collect();
    assert_eq!(observers, vec!["ui", "audit"]);
    assert_eq!(p.blocking[0].plugin, "gate");
    assert!(!c.definitions["coder"].routes_revision.is_empty());
    // The audit observer is reported (info), not rejected.
    assert!(codes(&c).contains(&"unkeyed-consumer"));
}

#[test]
fn duplicate_owner_is_rejected_unless_pinned() {
    let mut ms = manifests();
    let mut dup = ms["shell"].clone();
    dup.name = "shell2".into();
    ms.insert("shell2".into(), dup);
    let mut cfg = config(&["ui", "agent", "files", "shell", "shell2"]);
    cfg.plugins
        .insert("shell2".into(), cfg.plugins["shell"].clone());
    let out = compile(&cfg, &ms);
    assert!(!out.ok);
    assert!(
        codes(&out).contains(&"duplicate-route-owner"),
        "{:#?}",
        out.diagnostics
    );
    // An explicit pin resolves it deterministically.
    cfg.definitions.get_mut("coder").unwrap().route_owners = BTreeMap::from([(
        "tool-call".into(),
        BTreeMap::from([("shell".into(), "shell2".into())]),
    )]);
    let out = compile(&cfg, &ms);
    assert!(out.ok, "{:#?}", out.diagnostics);
    let r = out.definitions["coder"]
        .pipeline("tool-call")
        .routed
        .clone()
        .unwrap();
    assert_eq!(r.owner("shell").unwrap().plugin, "shell2");
    // A pin naming a plugin that does not own the value is stale.
    cfg.definitions.get_mut("coder").unwrap().route_owners = BTreeMap::from([(
        "tool-call".into(),
        BTreeMap::from([("shell".into(), "files".into())]),
    )]);
    assert!(codes(&compile(&cfg, &ms)).contains(&"stale-route-owner"));
}

#[test]
fn overlapping_prefix_claims_are_rejected() {
    let mut ms = manifests();
    let mut other = ms["mcp"].clone();
    other.name = "mcp-gh".into();
    other.consumes = vec![owner(&["mcp__gh__*"])];
    other.tools = vec![tool("mcp__gh__*")];
    ms.insert("mcp-gh".into(), other);
    let mut cfg = config(&["ui", "agent", "mcp", "mcp-gh"]);
    cfg.plugins
        .insert("mcp-gh".into(), cfg.plugins["mcp"].clone());
    let out = compile(&cfg, &ms);
    assert!(
        codes(&out).contains(&"duplicate-route-owner"),
        "{:#?}",
        out.diagnostics
    );
}

#[test]
fn missing_owner_bad_schema_and_unavailable_version_are_rejected() {
    // Tools declared without owning a keyed event: nothing would route to them.
    let mut ms = manifests();
    ms.get_mut("audit").unwrap().tools = vec![tool("audit_log")];
    let out = compile(&config(ALL), &ms);
    assert!(codes(&out).contains(&"missing-route-owner"));
    // A declared tool outside the plugin's keyed values.
    let mut ms = manifests();
    ms.get_mut("files")
        .unwrap()
        .tools
        .push(tool("delete_everything"));
    assert!(codes(&compile(&config(ALL), &ms)).contains(&"undeclared-tool-route"));
    // Required parameters the schema does not define.
    let mut ms = manifests();
    ms.get_mut("files").unwrap().tools[0].required = vec!["nope".into()];
    assert!(codes(&compile(&config(ALL), &ms)).contains(&"invalid-tool-schema"));
    // Keyed consumers must be async owners.
    let mut ms = manifests();
    ms.get_mut("shell").unwrap().consumes[0].mode = Some(Mode::Blocking);
    assert!(codes(&compile(&config(ALL), &ms)).contains(&"keyed-consumer"));
    // A pinned plugin version the running plugin does not report.
    let mut cfg = config(ALL);
    cfg.plugins.get_mut("shell").unwrap().version = Some("2".into());
    assert!(codes(&compile(&cfg, &manifests())).contains(&"plugin-version-unavailable"));
    cfg.plugins.get_mut("shell").unwrap().version = Some("1.2".into());
    assert!(compile(&cfg, &manifests()).ok);
}

// ----------------------------------------------------------------------------
// Runtime dispatch.
// ----------------------------------------------------------------------------

struct Sim {
    k: Kernel,
    logs: BTreeMap<String, Vec<Record>>,
    pending: Vec<(String, Stamp, InvokeRequest)>,
    now: u64,
}

impl Sim {
    fn new(c: Compilation) -> Self {
        let mut k = Kernel::new();
        k.install(c).unwrap();
        Sim {
            k,
            logs: BTreeMap::new(),
            pending: Vec::new(),
            now: 0,
        }
    }
    fn take(&mut self, fx: Vec<Effect>) {
        for f in fx {
            match f {
                Effect::Append { record } => {
                    self.logs
                        .entry(record.session_id.clone())
                        .or_default()
                        .push(record);
                }
                Effect::Invoke {
                    plugin,
                    stamp,
                    request,
                    ..
                } => self.pending.push((plugin, stamp, request)),
                Effect::Cancel { .. } => {}
            }
        }
    }
    fn start(&mut self, text: &str) -> String {
        self.now += 1;
        let out = self.k.start_session(
            &StartRequest {
                plugin: "ui".into(),
                definition: "coder".into(),
                invocation_id: None,
                initial: Some(InitialEvent {
                    event_name: "user-message".into(),
                    payload: json!({ "text": text }),
                    ui: None,
                }),
                fork_from: None,
            },
            self.now,
        );
        self.take(out.effects);
        out.session_id.unwrap()
    }
    fn publish(&mut self, plugin: &str, inv: &str, event: &str, payload: Value) {
        self.now += 1;
        let out = self.k.publish(
            &PublishRequest {
                plugin: plugin.into(),
                invocation_id: Some(inv.into()),
                cite: None,
                event_name: event.into(),
                payload,
                ui: None,
                lane: Lane::Normal,
                target_session: None,
            },
            self.now,
        );
        assert!(out.blocked.is_none(), "{:?}", out.blocked);
        self.take(out.effects);
    }
    fn complete(&mut self, inv: &str, r: InvocationResult) {
        self.now += 1;
        let fx = self.k.complete(inv, &r, self.now);
        self.take(fx);
    }
    /// Plugins: agent issues `calls` on the user message; owners answer, except
    /// `fail` names a tool whose owner fails without answering.
    fn run(&mut self, calls: &[&str], fail: Option<&str>) {
        let mut guard = 0;
        while !self.pending.is_empty() {
            guard += 1;
            assert!(guard < 1000);
            let (plugin, _, req) = self.pending.remove(0);
            let inv = req.invocation_id.clone();
            let ev = &req.event;
            let mut result = InvocationResult::default();
            match (plugin.as_str(), ev.event_name.as_str()) {
                ("agent", "user-message") => {
                    for (i, name) in calls.iter().enumerate() {
                        self.publish(
                            "agent",
                            &inv,
                            "tool-call",
                            json!({ "call_id": format!("c{i}"), "name": name }),
                        );
                    }
                }
                ("agent", DISPATCH_FAILED) => {
                    let call = &ev.payload["payload"];
                    self.publish(
                        "agent",
                        &inv,
                        "tool-result",
                        json!({ "call_id": call["call_id"], "error": ev.payload["reason"] }),
                    );
                }
                (p, "tool-call") if ["files", "shell", "mcp"].contains(&p) => {
                    if Some(ev.payload["name"].as_str().unwrap()) == fail {
                        result.error = Some("boom".into());
                    } else {
                        self.publish(
                            p,
                            &inv,
                            "tool-result",
                            json!({ "call_id": ev.payload["call_id"], "by": p }),
                        );
                    }
                }
                _ => {}
            }
            self.complete(&inv, result);
        }
    }
    fn started(&self, sid: &str) -> Vec<(String, String, Option<String>)> {
        let names: BTreeMap<String, String> = self.logs[sid]
            .iter()
            .filter_map(|r| match &r.body {
                Body::EventAppended { event } => {
                    Some((event.event_id.clone(), event.event_name.clone()))
                }
                _ => None,
            })
            .collect();
        self.logs[sid]
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
    fn events(&self, sid: &str, name: &str) -> Vec<Value> {
        self.logs[sid]
            .iter()
            .filter_map(|r| match &r.body {
                Body::EventAppended { event } if event.event_name == name => {
                    Some(event.payload.clone())
                }
                _ => None,
            })
            .collect()
    }
}

#[test]
fn each_tool_call_reaches_exactly_its_owner() {
    let mut sim = Sim::new(compile(&config(ALL), &manifests()));
    let sid = sim.start("go");
    sim.run(&["read_file", "shell", "mcp__gh__issues", "list_dir"], None);
    let tool_invs: Vec<(String, Option<String>)> = sim
        .started(&sid)
        .into_iter()
        .filter(|(e, _, _)| e == "tool-call")
        .map(|(_, p, r)| (p, r))
        .collect();
    // Per call: gate (blocking), ui + audit (observers), one owner. No other tool plugin.
    let owners: Vec<(String, String)> = tool_invs
        .iter()
        .filter_map(|(p, r)| r.clone().map(|r| (r, p.clone())))
        .collect();
    assert_eq!(
        owners,
        vec![
            ("read_file".into(), "files".into()),
            ("shell".into(), "shell".into()),
            ("mcp__gh__issues".into(), "mcp".into()),
            ("list_dir".into(), "files".into()),
        ]
    );
    assert_eq!(tool_invs.len(), 4 * 4, "{tool_invs:?}");
    let results = sim.events(&sid, "tool-result");
    assert_eq!(results.len(), 4);
    assert_eq!(results[1]["by"], "shell");
}

#[test]
fn unowned_and_failed_calls_are_answered_through_dispatch_failed() {
    let mut sim = Sim::new(compile(&config(ALL), &manifests()));
    let sid = sim.start("go");
    sim.run(&["write_file", "shell"], Some("shell"));
    let failed = sim.events(&sid, DISPATCH_FAILED);
    assert_eq!(failed.len(), 2, "{failed:#?}");
    assert_eq!(failed[0]["reason"], "no-owner");
    assert_eq!(failed[0]["value"], "write_file");
    assert_eq!(failed[1]["reason"], "owner-failed");
    assert_eq!(failed[1]["plugin"], "shell");
    assert_eq!(failed[1]["error"], "boom");
    // The loop got an answer for both calls instead of waiting forever.
    let results = sim.events(&sid, "tool-result");
    assert_eq!(results.len(), 2);
    // No owner plugin saw the unowned call.
    assert!(
        !sim.started(&sid)
            .iter()
            .any(|(e, p, _)| e == "tool-call" && ["files", "mcp"].contains(&p.as_str()))
    );
}

#[test]
fn ownership_changes_only_at_a_config_boundary_and_retries_keep_their_executor() {
    let c1 = compile(&config(ALL), &manifests());
    let mut sim = Sim::new(c1);
    let sid = sim.start("go");
    // Hold the owner's invocation open, then move `shell` to a new owner.
    let mut held = None;
    let mut guard = 0;
    sim.run(&[], None);
    sim.now += 1;
    let out = sim.k.publish(
        &PublishRequest {
            plugin: "ui".into(),
            invocation_id: None,
            cite: Some(format!("{sid}/i1")),
            event_name: "user-message".into(),
            payload: json!({ "text": "again" }),
            ui: None,
            lane: Lane::Normal,
            target_session: None,
        },
        sim.now,
    );
    assert!(out.blocked.is_none(), "{:?}", out.blocked);
    sim.take(out.effects);
    while !sim.pending.is_empty() {
        guard += 1;
        assert!(guard < 100);
        let (plugin, stamp, req) = sim.pending.remove(0);
        if req.event.event_name == "user-message" && plugin == "agent" {
            sim.publish(
                "agent",
                &req.invocation_id,
                "tool-call",
                json!({ "call_id": "x1", "name": "shell" }),
            );
        }
        if plugin == "shell" {
            held = Some((req.invocation_id.clone(), stamp));
            continue;
        }
        sim.complete(&req.invocation_id, InvocationResult::default());
    }
    let (held_inv, held_stamp) = held.expect("shell owner dispatched");
    // New config: same plugin, new config stamp (a hot swap of the owner).
    let mut cfg2 = config(ALL);
    cfg2.plugins.get_mut("shell").unwrap().config = json!({ "v": 2 });
    let c2 = compile(&cfg2, &manifests());
    let h2 = sim.k.install(c2).unwrap();
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
    sim.take(fx);
    sim.pending.clear();
    sim.complete(&held_inv, InvocationResult::default());
    sim.pending.clear();
    // The next call dispatches under the new revision's stamp.
    sim.now += 1;
    let out = sim.k.publish(
        &PublishRequest {
            plugin: "ui".into(),
            invocation_id: None,
            cite: Some(format!("{sid}/i1")),
            event_name: "user-message".into(),
            payload: json!({ "text": "third" }),
            ui: None,
            lane: Lane::Normal,
            target_session: None,
        },
        sim.now,
    );
    sim.take(out.effects);
    let mut new_stamp = None;
    while !sim.pending.is_empty() {
        let (plugin, stamp, req) = sim.pending.remove(0);
        if req.event.event_name == "user-message" && plugin == "agent" {
            sim.publish(
                "agent",
                &req.invocation_id,
                "tool-call",
                json!({ "call_id": "x2", "name": "shell" }),
            );
        }
        if plugin == "shell" {
            new_stamp = Some(stamp);
        }
        sim.complete(&req.invocation_id, InvocationResult::default());
    }
    let new_stamp = new_stamp.expect("dispatched");
    assert_ne!(new_stamp, held_stamp);
    assert_eq!(new_stamp.config, cfg2.plugins["shell"].stamp().config);
}

#[test]
fn routed_logs_replay_exactly() {
    let c = compile(&config(ALL), &manifests());
    let mut sim = Sim::new(c.clone());
    let sid = sim.start("go");
    sim.run(&["read_file", "nope", "shell"], Some("read_file"));
    let mut fresh = Kernel::new();
    fresh.install(c).unwrap();
    fresh.load_session(&sim.logs[&sid]).unwrap();
    let live = sim.k.status(&sid).unwrap();
    let replayed = fresh.status(&sid).unwrap();
    assert_eq!(live, replayed);
    assert_eq!(sim.k.context(&sid), fresh.context(&sid));
    // Every owner dispatch is recorded with the key value that selected it.
    let routed: Vec<Option<String>> = sim
        .started(&sid)
        .into_iter()
        .filter(|(e, p, _)| e == "tool-call" && ["files", "shell"].contains(&p.as_str()))
        .map(|(_, _, r)| r)
        .collect();
    assert_eq!(routed, vec![Some("read_file".into()), Some("shell".into())]);
    // The failure is canonical.
    assert!(sim.logs[&sid].iter().any(|r| matches!(
        &r.body,
        Body::InvocationCompleted {
            outcome: Outcome::Failed { .. },
            ..
        }
    )));
    assert_eq!(
        fresh.routes(&sid).unwrap()["tool-call"]
            .owner("shell")
            .unwrap()
            .plugin,
        "shell"
    );
}
