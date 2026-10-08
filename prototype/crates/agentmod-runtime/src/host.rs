//! The native host: one actor owns the kernel, the store, and the plugin
//! process table. Plugin I/O arrives as messages; the actor feeds the kernel
//! and executes its effects (append + fsync before any dispatch).

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use agentmod_core::compiler::{Compilation, compile};
use agentmod_core::kernel::{
    Effect, InvocationResult, Kernel, PublishRequest, Scope, StartRequest,
};
use agentmod_core::manifest::{Capability, DeploymentConfig, Manifest, PluginConfig};
use agentmod_core::projection::{context_at, project};
use agentmod_core::record::{Body, Command, Outcome, Record};
use agentmod_core::stream::{InFrame, StreamHub, StreamOutput, StreamStatus};
use agentmod_core::types::{Mode, Stamp};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::config::{disable_unrunnable, stamp_binaries};
use crate::proc::{Msg, ProcHandle, spawn};
use crate::store::Store;

const PROTOCOL: &str = "agentmod/0.1";
const CANCEL_GRACE: Duration = Duration::from_secs(3);
const IDLE_UNLOAD: Duration = Duration::from_secs(60);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);

type Key = (String, String, String);

fn key(name: &str, stamp: &Stamp) -> Key {
    (name.to_owned(), stamp.binary.clone(), stamp.config.clone())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

enum Pending {
    Initialize,
    Invoke(String),
    /// A read-only service call routed for another process (`from`, its request id).
    Service {
        from: u64,
        id: Value,
    },
    Shutdown,
}

struct Proc {
    name: String,
    stamp: Stamp,
    cfg: PluginConfig,
    handle: ProcHandle,
    ready: bool,
    failed: bool,
    manifest: Option<Manifest>,
    next_id: u64,
    pending: BTreeMap<u64, Pending>,
    queued: Vec<String>,
    /// Open invocations on this process with their deadlines.
    invocations: BTreeMap<String, Instant>,
    draining: bool,
}

impl Proc {
    fn send(&mut self, method: &str, params: &Value, pending: Option<Pending>) {
        let mut msg = json!({ "jsonrpc": "2.0", "method": method, "params": params });
        if let Some(p) = pending {
            let id = self.next_id;
            self.next_id += 1;
            msg["id"] = json!(id);
            self.pending.insert(id, p);
        }
        let line = msg.to_string();
        if self.ready || method == "initialize" {
            self.handle.send(line);
        } else {
            self.queued.push(line);
        }
    }

    fn reply(&self, id: &Value, result: Result<Value, (i64, String)>) {
        let msg = match result {
            Ok(r) => json!({ "jsonrpc": "2.0", "id": id, "result": r }),
            Err((code, message)) => {
                json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
            }
        };
        self.handle.send(msg.to_string());
    }

    fn has(&self, cap: Capability) -> bool {
        self.manifest.as_ref().is_some_and(|m| m.has(cap))
    }
}

/// Host-side hot-path counters (kinds only; per-session numbers come from the logs).
#[derive(Debug, Default, Clone, serde::Serialize)]
struct HostCounters {
    records: u64,
    record_bytes: u64,
    fsyncs: u64,
    invocations: u64,
    blocking_invocations: u64,
    async_invocations: u64,
    routed_invocations: u64,
    record_notifications: u64,
    stream_messages: u64,
    stream_frames_in: u64,
    live_messages_out: u64,
    resync_requests: u64,
}

/// Bytes of live frames sent to a watcher per drain, and the stdin backlog
/// past which delivery waits (the hub bounds what queues meanwhile).
const LIVE_DRAIN_BYTES: usize = 64 * 1024;
const LIVE_BACKLOG_LIMIT: usize = 1024 * 1024;

struct PendingApply {
    requester: Option<(u64, Value)>,
    config: DeploymentConfig,
    scope: Scope,
    waiting: BTreeSet<u64>,
    started: Instant,
}

/// Host options.
pub struct Options {
    pub config: DeploymentConfig,
    pub base_dir: PathBuf,
    pub data_dir: PathBuf,
    /// Compile only: handshake, print the compilation, exit.
    pub compile_only: bool,
}

/// The host actor.
pub struct Host {
    kernel: Kernel,
    store: Store,
    base_dir: PathBuf,
    data_dir: PathBuf,
    procs: BTreeMap<u64, Proc>,
    next_proc: u64,
    records: BTreeMap<String, Vec<Record>>,
    watchers: BTreeSet<u64>,
    tx: mpsc::UnboundedSender<Msg>,
    rx: mpsc::UnboundedReceiver<Msg>,
    applies: Vec<PendingApply>,
    cancel_deadlines: BTreeMap<String, (u64, Instant)>,
    crashes: BTreeMap<Key, Vec<Instant>>,
    last_touch: BTreeMap<String, Instant>,
    max_attempts: u32,
    ready: bool,
    buffered: Vec<Msg>,
    /// Live provider streams: outside the event pipeline and the log.
    hub: StreamHub,
    /// Hub client → watcher process.
    stream_clients: BTreeMap<u64, u64>,
    poll_at: Option<u64>,
    counters: HostCounters,
}

impl Host {
    /// Boot: spawn plugins, handshake, compile, recover. Returns the compilation.
    ///
    /// # Errors
    /// Store, spawn, or compilation failures.
    pub async fn boot(opts: Options) -> Result<(Self, Compilation), String> {
        let (tx, rx) = mpsc::unbounded_channel();
        let store = Store::open(&opts.data_dir, opts.config.runtime.spill_threshold_bytes)?;
        let mut host = Host {
            kernel: Kernel::new(),
            store,
            base_dir: opts.base_dir,
            data_dir: opts.data_dir,
            procs: BTreeMap::new(),
            next_proc: 1,
            records: BTreeMap::new(),
            watchers: BTreeSet::new(),
            tx,
            rx,
            applies: Vec::new(),
            cancel_deadlines: BTreeMap::new(),
            crashes: BTreeMap::new(),
            last_touch: BTreeMap::new(),
            max_attempts: opts.config.runtime.max_attempts,
            ready: false,
            buffered: Vec::new(),
            hub: StreamHub::new(opts.config.runtime.streaming.clone()),
            stream_clients: BTreeMap::new(),
            poll_at: None,
            counters: HostCounters::default(),
        };
        // Handshake every enabled plugin.
        let mut waiting = BTreeSet::new();
        for (name, cfg) in opts.config.plugins.iter().filter(|(_, c)| !c.disabled) {
            match host.spawn_proc(name, cfg) {
                Ok(id) => {
                    waiting.insert(id);
                }
                Err(e) => eprintln!("agentmod: {e}"),
            }
        }
        let deadline = tokio::time::Instant::now() + HANDSHAKE_TIMEOUT;
        while waiting
            .iter()
            .any(|id| host.procs.get(id).is_some_and(|p| !p.ready && !p.failed))
        {
            match tokio::time::timeout_at(deadline, host.rx.recv()).await {
                Ok(Some(msg)) => host.handle_boot(msg),
                Ok(None) | Err(_) => break,
            }
        }
        let manifests = host.manifests_for(&opts.config);
        let compilation = compile(&opts.config, &manifests);
        if opts.compile_only || !compilation.ok {
            host.kill_all();
            return Ok((host, compilation));
        }
        // Install stored configs (sessions reference them), then the new one.
        for c in host.store.compilations() {
            if c.ok {
                let _ = host.kernel.install(c);
            }
        }
        host.store.save_compilation(&compilation)?;
        let hash = host.kernel.install(compilation.clone())?;
        host.kernel.set_active(&hash)?;
        host.kernel
            .reserve_session_ids(host.store.index.next_session);
        host.store
            .journal(now_ms(), "config-loaded", &json!({ "hash": hash }));
        // Partial streams from before a crash: restored for reconnecting clients
        // (marked interrupted); the invocation's retry opens a new attempt.
        for ops in host.store.stream_recovery() {
            if let Some(state) = StreamHub::replay_recovery(&ops, &opts.config.runtime.streaming) {
                host.hub.restore(state);
            }
        }
        // Recovery: the orphan scan is bounded by the active-set index.
        let active: Vec<String> = host
            .store
            .index
            .sessions
            .values()
            .filter(|e| e.active)
            .map(|e| e.session_id.clone())
            .collect();
        for sid in active {
            if let Err(e) = host.ensure_loaded(&sid) {
                eprintln!("agentmod: recovering {sid}: {e}");
            }
        }
        // Streams whose invocation already completed (a crash during
        // finalization): the canonical response is logged; drop the partials.
        for st in host.hub.snapshots(None) {
            if !host
                .kernel
                .invocation(&st.stream_id)
                .is_some_and(|(_, open)| open)
            {
                let mut out = StreamOutput::default();
                host.hub
                    .finalize(&st.stream_id, StreamStatus::Interrupted, &mut out);
                host.stream_effects(out);
            }
        }
        host.ready = true;
        for msg in std::mem::take(&mut host.buffered) {
            host.handle(msg);
        }
        Ok((host, compilation))
    }

    /// Sender for external signals (ticks, shutdown).
    #[must_use]
    pub fn sender(&self) -> mpsc::UnboundedSender<Msg> {
        self.tx.clone()
    }

    /// Run the actor loop until shutdown.
    pub async fn run(mut self) {
        let tick = self.tx.clone();
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(Duration::from_millis(250));
            loop {
                iv.tick().await;
                if tick.send(Msg::Tick).is_err() {
                    break;
                }
            }
        });
        while let Some(msg) = self.rx.recv().await {
            if matches!(msg, Msg::Shutdown) {
                break;
            }
            self.handle(msg);
        }
        self.shutdown().await;
    }

    async fn shutdown(&mut self) {
        let ids: Vec<u64> = self.procs.keys().copied().collect();
        for id in ids {
            if let Some(p) = self.procs.get_mut(&id) {
                p.send("shutdown", &json!({}), Some(Pending::Shutdown));
            }
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !self.procs.is_empty() {
            match tokio::time::timeout_at(deadline, self.rx.recv()).await {
                Ok(Some(Msg::Exited { proc, .. })) => {
                    self.procs.remove(&proc);
                }
                Ok(Some(_)) => {}
                _ => break,
            }
        }
        self.kill_all();
        let _ = self.store.sync();
        let _ = self.store.flush_index();
    }

    fn kill_all(&mut self) {
        for p in self.procs.values_mut() {
            p.handle.kill();
        }
    }

    fn manifests_for(&self, config: &DeploymentConfig) -> BTreeMap<String, Manifest> {
        let mut out = BTreeMap::new();
        for (name, cfg) in &config.plugins {
            let k = key(name, &cfg.stamp());
            if let Some(m) = self
                .procs
                .values()
                .find(|p| key(&p.name, &p.stamp) == k && p.ready)
                .and_then(|p| p.manifest.clone())
            {
                out.insert(name.clone(), m);
            }
        }
        out
    }

    fn spawn_proc(&mut self, name: &str, cfg: &PluginConfig) -> Result<u64, String> {
        let id = self.next_proc;
        self.next_proc += 1;
        let handle = spawn(
            id,
            name,
            &cfg.command,
            &self.base_dir,
            &self.data_dir,
            self.tx.clone(),
        )?;
        let mut p = Proc {
            name: name.to_owned(),
            stamp: cfg.stamp(),
            cfg: cfg.clone(),
            handle,
            ready: false,
            failed: false,
            manifest: None,
            next_id: 1,
            pending: BTreeMap::new(),
            queued: Vec::new(),
            invocations: BTreeMap::new(),
            draining: false,
        };
        p.send(
            "initialize",
            &json!({ "protocol": PROTOCOL, "plugin": name, "config": cfg.config }),
            Some(Pending::Initialize),
        );
        eprintln!(
            "agentmod: started plugin `{name}` (pid {:?}, stamp {}/{})",
            p.handle.pid, p.stamp.binary, p.stamp.config
        );
        self.procs.insert(id, p);
        Ok(id)
    }

    fn handle_boot(&mut self, msg: Msg) {
        match msg {
            Msg::Line { proc, line } => {
                let Ok(v) = serde_json::from_str::<Value>(&line) else {
                    return;
                };
                if v.get("method").is_none() {
                    self.on_response(proc, &v);
                } else {
                    self.buffered.push(Msg::Line { proc, line });
                }
            }
            Msg::Exited { proc, status } => {
                eprintln!("agentmod: plugin process {proc} exited during handshake: {status}");
                if let Some(p) = self.procs.get_mut(&proc) {
                    p.failed = true;
                }
            }
            other => self.buffered.push(other),
        }
    }

    fn handle(&mut self, msg: Msg) {
        if !self.ready {
            self.handle_boot(msg);
            return;
        }
        match msg {
            Msg::Line { proc, line } => {
                let Ok(v) = serde_json::from_str::<Value>(&line) else {
                    eprintln!("agentmod: non-JSON line from process {proc}: {line}");
                    return;
                };
                match v.get("method").and_then(Value::as_str) {
                    None => self.on_response(proc, &v),
                    Some(method) => {
                        let method = method.to_owned();
                        let params = v.get("params").cloned().unwrap_or(Value::Null);
                        let result = self.on_request(proc, &method, params, v.get("id").cloned());
                        if let (Some(id), Some(result)) = (v.get("id"), result)
                            && let Some(p) = self.procs.get(&proc)
                        {
                            p.reply(id, result);
                        }
                    }
                }
            }
            Msg::Exited { proc, status } => self.on_exit(proc, &status),
            Msg::Tick => self.on_tick(),
            Msg::StreamPoll => {
                self.poll_at = None;
                let mut out = StreamOutput::default();
                self.hub.poll(now_ms(), &mut out);
                self.stream_effects(out);
            }
            Msg::Shutdown => {}
        }
    }

    fn on_response(&mut self, proc: u64, v: &Value) {
        let Some(id) = v.get("id").and_then(Value::as_u64) else {
            return;
        };
        let Some(p) = self.procs.get_mut(&proc) else {
            return;
        };
        let Some(pending) = p.pending.remove(&id) else {
            return;
        };
        match pending {
            Pending::Initialize => {
                let manifest = v
                    .pointer("/result/manifest")
                    .cloned()
                    .map(serde_json::from_value::<Manifest>);
                match manifest {
                    Some(Ok(m)) => {
                        p.manifest = Some(m);
                        p.ready = true;
                        for line in std::mem::take(&mut p.queued) {
                            p.handle.send(line);
                        }
                    }
                    other => {
                        match v.pointer("/error/message").and_then(Value::as_str) {
                            Some(msg) => {
                                eprintln!("agentmod: plugin `{}` refused to start: {msg}", p.name)
                            }
                            None => eprintln!(
                                "agentmod: plugin `{}` returned an invalid manifest: {other:?} {v}",
                                p.name
                            ),
                        }
                        p.failed = true;
                        p.handle.kill();
                    }
                }
                self.progress_applies(proc);
            }
            Pending::Invoke(inv) => {
                p.invocations.remove(&inv);
                self.cancel_deadlines.remove(&inv);
                let result = match (v.get("result"), v.get("error")) {
                    (_, Some(err)) => InvocationResult {
                        error: Some(
                            err.get("message")
                                .and_then(Value::as_str)
                                .unwrap_or("plugin error")
                                .to_owned(),
                        ),
                        ..Default::default()
                    },
                    (Some(r), None) => {
                        serde_json::from_value(r.clone()).unwrap_or_else(|e| InvocationResult {
                            error: Some(format!("malformed invocation result: {e}")),
                            ..Default::default()
                        })
                    }
                    (None, None) => InvocationResult {
                        error: Some("empty response".into()),
                        ..Default::default()
                    },
                };
                let fx = self.kernel.complete(&inv, &result, now_ms());
                self.execute(fx);
            }
            Pending::Service { from, id } => {
                let result = match (v.get("result"), v.get("error")) {
                    (_, Some(err)) => Err((
                        err.get("code").and_then(Value::as_i64).unwrap_or(-32000),
                        err.get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("service error")
                            .to_owned(),
                    )),
                    (Some(r), None) => Ok(r.clone()),
                    (None, None) => Ok(Value::Null),
                };
                if let Some(req) = self.procs.get(&from) {
                    req.reply(&id, result);
                }
            }
            Pending::Shutdown => {}
        }
    }

    /// Route a read-only service call to a plugin of the active config.
    fn req_service(
        &mut self,
        from: u64,
        params: &Value,
        id: Option<Value>,
    ) -> Result<(), (i64, String)> {
        let id = id.ok_or((-32600, "call_service needs a request id".to_owned()))?;
        let plugin = params
            .get("plugin")
            .and_then(Value::as_str)
            .ok_or((-32602, "plugin required".to_owned()))?
            .to_owned();
        let service = params
            .get("service")
            .and_then(Value::as_str)
            .ok_or((-32602, "service required".to_owned()))?
            .to_owned();
        let active = self
            .kernel
            .active()
            .ok_or((-32001, "no active config".to_owned()))?;
        let stamp = active
            .stamps
            .get(&plugin)
            .cloned()
            .ok_or((-32001, format!("`{plugin}` is not in the active config")))?;
        if !active
            .manifest(&plugin)
            .is_some_and(|m| m.has_service(&service))
        {
            return Err((
                -32601,
                format!("`{plugin}` declares no service `{service}`"),
            ));
        }
        let target = self.ensure_proc(&plugin, &stamp).map_err(|e| (-32001, e))?;
        let p = self
            .procs
            .get_mut(&target)
            .ok_or((-32001, "plugin process gone".to_owned()))?;
        p.send(
            "service",
            &json!({ "service": service, "args": params.get("args").cloned().unwrap_or(json!({})) }),
            Some(Pending::Service { from, id }),
        );
        Ok(())
    }

    fn on_request(
        &mut self,
        proc: u64,
        method: &str,
        params: Value,
        id: Option<Value>,
    ) -> Option<Result<Value, (i64, String)>> {
        let p = self.procs.get(&proc)?;
        let plugin = p.name.clone();
        let res = match method {
            "log" => {
                eprintln!(
                    "  [{plugin}] {}",
                    params.get("message").and_then(Value::as_str).unwrap_or("")
                );
                return None;
            }
            "publish" => self.req_publish(&plugin, params),
            "stream" => self.req_stream(&plugin, &params),
            "stream_resync" => {
                let client = self
                    .stream_clients
                    .iter()
                    .find(|(_, p)| **p == proc)
                    .map(|(c, _)| *c);
                self.counters.resync_requests += 1;
                Ok(json!({ "streams": client.map(|c| self.hub.resync(c)).unwrap_or_default() }))
            }
            "start_session" => self.req_start(&plugin, params),
            "query" => self.req_query(&params),
            "command" => {
                if !p.has(Capability::Control) {
                    Err((-32003, format!("`{plugin}` lacks the control capability")))
                } else {
                    self.req_command(&plugin, &params)
                }
            }
            "watch" => {
                if p.has(Capability::Observe) {
                    self.watchers.insert(proc);
                    // Live streams on request (frontends): attach returns the
                    // snapshots atomically with subscribing.
                    if params.get("streams").and_then(Value::as_bool) == Some(true) {
                        let (client, streams) = self.hub.attach(None);
                        self.stream_clients.insert(client, proc);
                        Ok(json!({ "watching": true, "streams": streams }))
                    } else {
                        Ok(json!({ "watching": true }))
                    }
                } else {
                    Err((-32003, format!("`{plugin}` lacks the observe capability")))
                }
            }
            "call_service" => {
                if !p.has(Capability::Control) {
                    Err((-32003, format!("`{plugin}` lacks the control capability")))
                } else {
                    match self.req_service(proc, &params, id.clone()) {
                        Ok(()) => return None, // answered when the service replies
                        Err(e) => Err(e),
                    }
                }
            }
            "apply_config" => {
                if !p.has(Capability::Control) {
                    Err((-32003, format!("`{plugin}` lacks the control capability")))
                } else {
                    match self.begin_apply(Some((proc, id.clone().unwrap_or(Value::Null))), &params)
                    {
                        Ok(()) => return None, // answered when the apply completes
                        Err(e) => Err((-32002, e)),
                    }
                }
            }
            other => Err((-32601, format!("unknown method `{other}`"))),
        };
        Some(res)
    }

    fn req_publish(&mut self, plugin: &str, params: Value) -> Result<Value, (i64, String)> {
        let mut req: PublishRequest =
            serde_json::from_value(params).map_err(|e| (-32602, e.to_string()))?;
        plugin.clone_into(&mut req.plugin);
        for sid in req.sessions() {
            self.ensure_loaded(&sid).map_err(|e| (-32001, e))?;
        }
        let out = self.kernel.publish(&req, now_ms());
        let blocked_unrecorded = out.blocked.is_some() && out.effects.is_empty();
        self.execute(out.effects);
        if let Some(reason) = out.blocked {
            if blocked_unrecorded {
                self.store.journal(
                    now_ms(),
                    "publish-blocked",
                    &json!({ "plugin": plugin, "event_name": req.event_name, "reason": reason }),
                );
            }
            return Err((-32001, format!("publish blocked: {reason}")));
        }
        Ok(json!({ "event_id": out.event_id, "duplicate": out.duplicate }))
    }

    /// Provider frames for an open invocation (a stream is its invocation).
    /// Notifications for the run; the closing batch is a request whose reply
    /// carries the stream's statistics for the canonical response.
    fn req_stream(&mut self, plugin: &str, params: &Value) -> Result<Value, (i64, String)> {
        let inv = params
            .get("invocation_id")
            .and_then(Value::as_str)
            .ok_or((-32602, "invocation_id required".to_owned()))?
            .to_owned();
        match self.kernel.invocation(&inv) {
            Some((p, true)) if p == plugin => {}
            Some((p, _)) if p != plugin => {
                return Err((-32001, format!("`{inv}` was not dispatched to `{plugin}`")));
            }
            _ => return Err((-32001, format!("`{inv}` is not an open invocation"))),
        }
        let frames: Vec<InFrame> =
            serde_json::from_value(params.get("frames").cloned().unwrap_or_else(|| json!([])))
                .map_err(|e| (-32602, format!("invalid frames: {e}")))?;
        self.counters.stream_messages += 1;
        self.counters.stream_frames_in += frames.len() as u64;
        let mut out = StreamOutput::default();
        let r = self.hub.ingest(&inv, frames, now_ms(), &mut out);
        self.stream_effects(out);
        Ok(json!(r))
    }

    /// Execute hub output: recovery writes, live delivery, the flush timer.
    fn stream_effects(&mut self, out: StreamOutput) {
        for op in &out.recovery {
            if let Err(e) = self.store.stream_op(op) {
                eprintln!("agentmod: stream recovery write failed: {e}");
            }
        }
        for client in out.ready {
            self.deliver_live(client);
        }
        if let Some(at) = self.hub.next_deadline()
            && self.poll_at.is_none_or(|p| at < p)
        {
            self.poll_at = Some(at);
            let tx = self.tx.clone();
            let wait = at.saturating_sub(now_ms());
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(wait)).await;
                let _ = tx.send(Msg::StreamPoll);
            });
        }
    }

    /// Send a watcher its queued live messages unless its stdin is backed up
    /// (then they wait in the hub's bounded queue; overflow means resync).
    fn deliver_live(&mut self, client: u64) {
        let Some(proc) = self.stream_clients.get(&client).copied() else {
            return;
        };
        let Some(p) = self.procs.get(&proc) else {
            self.hub.detach(client);
            self.stream_clients.remove(&client);
            return;
        };
        if p.handle.backlog() > LIVE_BACKLOG_LIMIT {
            return;
        }
        let msgs = self.hub.drain(client, LIVE_DRAIN_BYTES);
        if msgs.is_empty() {
            return;
        }
        self.counters.live_messages_out += msgs.len() as u64;
        let note = json!({ "jsonrpc": "2.0", "method": "stream", "params": { "messages": msgs } });
        p.handle.send(note.to_string());
    }

    fn req_start(&mut self, plugin: &str, params: Value) -> Result<Value, (i64, String)> {
        let mut req: StartRequest =
            serde_json::from_value(params).map_err(|e| (-32602, e.to_string()))?;
        plugin.clone_into(&mut req.plugin);
        let mut need: Vec<String> = req
            .invocation_id
            .iter()
            .filter_map(|i| i.split('/').next().map(str::to_owned))
            .collect();
        need.extend(req.fork_from.iter().map(|f| f.session_id.clone()));
        for sid in need {
            self.ensure_loaded(&sid).map_err(|e| (-32001, e))?;
        }
        let out = self.kernel.start_session(&req, now_ms());
        self.execute(out.effects);
        match (out.session_id, out.error) {
            (Some(sid), _) => Ok(json!({ "session_id": sid })),
            (None, e) => Err((-32001, e.unwrap_or_default())),
        }
    }

    fn req_command(&mut self, plugin: &str, params: &Value) -> Result<Value, (i64, String)> {
        let sid = params
            .get("session_id")
            .and_then(Value::as_str)
            .ok_or((-32602, "session_id required".to_owned()))?
            .to_owned();
        let cmd: Command =
            serde_json::from_value(params.get("command").cloned().unwrap_or(Value::Null))
                .map_err(|e| (-32602, e.to_string()))?;
        self.ensure_loaded(&sid).map_err(|e| (-32001, e))?;
        let fx = self
            .kernel
            .command(&sid, cmd, plugin, now_ms())
            .map_err(|e| (-32001, e))?;
        self.execute(fx);
        Ok(json!({ "status": self.kernel.status(&sid) }))
    }

    fn session_records(&self, sid: &str) -> Result<Vec<Record>, String> {
        match self.records.get(sid) {
            Some(r) => Ok(r.clone()),
            None => self.store.read(sid),
        }
    }

    fn req_query(&self, params: &Value) -> Result<Value, (i64, String)> {
        let what = params.get("what").and_then(Value::as_str).unwrap_or("");
        let sid = params
            .get("session_id")
            .and_then(Value::as_str)
            .unwrap_or("");
        let err = |e: String| (-32004, e);
        match what {
            "sessions" => {
                let list: Vec<Value> = self
                    .store
                    .index
                    .sessions
                    .values()
                    .map(|e| {
                        let status = self.kernel.status(&e.session_id);
                        json!({
                            "session_id": e.session_id, "definition": e.definition, "title": e.title,
                            "created_at": e.created_at, "updated_at": e.updated_at, "last_sequence": e.last_sequence,
                            "parent": e.parent, "fork_of": e.fork_of, "loaded": status.is_some(),
                            "activity": status.as_ref().map_or_else(|| if e.state.is_empty() { "idle".to_owned() } else { e.state.clone() }, |s| s.activity.clone()),
                        })
                    })
                    .collect();
                Ok(json!(list))
            }
            "session" => {
                let records = self.session_records(sid).map_err(err)?;
                let mut v =
                    serde_json::to_value(project(&records)).map_err(|e| err(e.to_string()))?;
                v["status"] = json!(self.kernel.status(sid));
                Ok(v)
            }
            "records" => {
                let from = params.get("from").and_then(Value::as_u64).unwrap_or(0);
                let records: Vec<Record> = self
                    .session_records(sid)
                    .map_err(err)?
                    .into_iter()
                    .filter(|r| r.sequence >= from)
                    .collect();
                Ok(json!(records))
            }
            "event" => {
                let eid = params.get("event_id").and_then(Value::as_str).unwrap_or("");
                let records = self.session_records(sid).map_err(err)?;
                Ok(project(&records)
                    .events
                    .into_iter()
                    .find(|e| e.event_id == eid)
                    .map_or(Value::Null, |e| json!(e)))
            }
            "context" => {
                let records = self.session_records(sid).map_err(err)?;
                let seq = params
                    .get("sequence")
                    .and_then(Value::as_u64)
                    .unwrap_or(u64::MAX);
                Ok(json!(context_at(&records, seq)))
            }
            "status" => Ok(json!(self.kernel.status(sid))),
            "session-metrics" => {
                let records = self.session_records(sid).map_err(err)?;
                Ok(agentmod_core::metrics::session_metrics(&records))
            }
            "streams" => Ok(json!(self.hub.snapshots(
                Some(sid).filter(|s| !s.is_empty())
            ))),
            "routes" => Ok(json!({
                "routes": self.kernel.routes(sid),
                "revision": self.kernel.status(sid).and_then(|st| self.kernel.config(&st.config).and_then(|c| c.definitions.get(&st.definition)).map(|d| d.routes_revision.clone())),
            })),
            "metrics" => Ok(json!({
                "host": self.counters,
                "streams": self.hub.counters,
                "open_streams": self.hub.snapshots(None).len(),
                "stream_clients": self.stream_clients.len(),
            })),
            "graph" => Ok(json!(self.kernel.active())),
            "services" => Ok(json!(self.kernel.active().map_or_else(Vec::new, |c| {
                c.manifests
                    .iter()
                    .filter(|(_, m)| !m.services.is_empty() || !m.settings.is_empty() || !m.provides.is_empty())
                    .map(|(name, m)| json!({ "plugin": name, "description": m.description, "services": m.services, "settings": m.settings, "provides": m.provides, "version": m.version }))
                    .collect::<Vec<_>>()
            }))),
            "config" => Ok(json!({
                "hash": self.kernel.active_hash(),
                "config": self.kernel.active().map(|c| &c.config),
                "installed": self.store.compilations().iter().map(|c| c.hash.clone()).collect::<Vec<_>>(),
                "host": "native",
            })),
            other => Err((-32602, format!("unknown query `{other}`"))),
        }
    }

    /// Load a session from its log into the kernel (if not already) and recover it.
    fn ensure_loaded(&mut self, sid: &str) -> Result<(), String> {
        if self.kernel.is_loaded(sid) {
            return Ok(());
        }
        let records = self.store.read(sid)?;
        self.kernel.load_session(&records)?;
        self.records.insert(sid.to_owned(), records);
        self.last_touch.insert(sid.to_owned(), Instant::now());
        let fx = self.kernel.recover(sid, self.max_attempts, now_ms());
        self.execute(fx);
        Ok(())
    }

    /// Execute kernel effects: append + fsync first, then dispatch.
    fn execute(&mut self, effects: Vec<Effect>) {
        if effects.is_empty() {
            return;
        }
        let mut touched = BTreeSet::new();
        let mut appended = Vec::new();
        let mut dispatch = Vec::new();
        for fx in effects {
            match fx {
                Effect::Append { record } => {
                    match self.store.append(&record) {
                        Ok(n) => {
                            self.counters.records += 1;
                            self.counters.record_bytes += n as u64;
                        }
                        Err(e) => {
                            eprintln!("agentmod: FATAL append failed: {e}");
                            std::process::exit(2);
                        }
                    }
                    touched.insert(record.session_id.clone());
                    self.records
                        .entry(record.session_id.clone())
                        .or_default()
                        .push(record.clone());
                    appended.push(record);
                }
                other => dispatch.push(other),
            }
        }
        if let Err(e) = self.store.sync() {
            eprintln!("agentmod: FATAL sync failed: {e}");
            std::process::exit(2);
        }
        self.counters.fsyncs += 1;
        // A stream lives exactly as long as its invocation.
        let mut sout = StreamOutput::default();
        for r in &appended {
            match &r.body {
                Body::InvocationStarted { mode, route, .. } => {
                    self.counters.invocations += 1;
                    match mode {
                        Mode::Blocking => self.counters.blocking_invocations += 1,
                        Mode::Async => self.counters.async_invocations += 1,
                    }
                    if route.is_some() {
                        self.counters.routed_invocations += 1;
                    }
                }
                Body::InvocationCompleted {
                    invocation_id,
                    outcome,
                    ..
                } if self.hub.is_open(invocation_id) => {
                    let status = match outcome {
                        Outcome::Ok => StreamStatus::Complete,
                        Outcome::Cancelled { .. } => StreamStatus::Cancelled,
                        Outcome::Failed { .. } | Outcome::Veto { .. } => StreamStatus::Failed,
                    };
                    self.hub.finalize(invocation_id, status, &mut sout);
                }
                Body::InvocationRetried { invocation_id, .. } => {
                    self.hub.interrupt(invocation_id, &mut sout);
                }
                _ => {}
            }
        }
        self.stream_effects(sout);
        let mut write_ahead = false;
        for sid in &touched {
            self.last_touch.insert(sid.clone(), Instant::now());
            let (active, state) = self
                .kernel
                .status(sid)
                .map_or((false, "idle".to_owned()), |s| {
                    (!self.kernel.is_idle(sid), s.activity)
                });
            write_ahead |= self.store.set_active(sid, active, &state);
        }
        if write_ahead {
            let _ = self.store.flush_index();
        }
        for fx in dispatch {
            match fx {
                Effect::Invoke {
                    plugin,
                    stamp,
                    request,
                    ..
                } => {
                    let inv = request.invocation_id.clone();
                    match self.ensure_proc(&plugin, &stamp) {
                        Ok(id) => {
                            let p = self.procs.get_mut(&id).expect("proc");
                            let timeout =
                                Duration::from_millis(p.cfg.timeout_ms.unwrap_or(120_000));
                            p.invocations.insert(inv.clone(), Instant::now() + timeout);
                            p.send("invoke", &json!(request), Some(Pending::Invoke(inv)));
                        }
                        Err(e) => {
                            let fx = self.kernel.complete(
                                &inv,
                                &InvocationResult {
                                    error: Some(e),
                                    ..Default::default()
                                },
                                now_ms(),
                            );
                            self.execute(fx);
                        }
                    }
                }
                Effect::Cancel {
                    plugin,
                    stamp,
                    invocation_id,
                } => {
                    let k = key(&plugin, &stamp);
                    if let Some((id, p)) = self.procs.iter_mut().find(|(_, p)| {
                        key(&p.name, &p.stamp) == k && p.invocations.contains_key(&invocation_id)
                    }) {
                        p.send("cancel", &json!({ "invocation_id": invocation_id }), None);
                        p.invocations.remove(&invocation_id);
                        self.cancel_deadlines
                            .insert(invocation_id, (*id, Instant::now() + CANCEL_GRACE));
                    }
                }
                Effect::Append { .. } => {}
            }
        }
        for r in appended {
            let note = json!({ "jsonrpc": "2.0", "method": "record", "params": { "record": r } })
                .to_string();
            self.counters.record_notifications += self.watchers.len() as u64;
            for w in &self.watchers {
                if let Some(p) = self.procs.get(w) {
                    p.handle.send(note.clone());
                }
            }
        }
    }

    fn ensure_proc(&mut self, plugin: &str, stamp: &Stamp) -> Result<u64, String> {
        let k = key(plugin, stamp);
        if let Some((id, _)) = self
            .procs
            .iter()
            .find(|(_, p)| key(&p.name, &p.stamp) == k && !p.failed && !p.draining)
        {
            return Ok(*id);
        }
        let recent = self.crashes.get(&k).map_or(0, |v| {
            v.iter()
                .filter(|t| t.elapsed() < Duration::from_secs(60))
                .count()
        });
        if recent >= 5 {
            return Err(format!(
                "plugin `{plugin}` is crash-looping ({recent} exits in the last minute)"
            ));
        }
        // Find the plugin's config in any installed compilation with this stamp.
        let hashes = self.kernel.configs_in_use();
        let cfg = hashes
            .iter()
            .filter_map(|h| self.kernel.config(h))
            .find(|c| c.stamps.get(plugin) == Some(stamp))
            .and_then(|c| c.config.plugins.get(plugin).cloned())
            .ok_or_else(|| {
                format!(
                    "no configuration for plugin `{plugin}` with stamp {}/{}",
                    stamp.binary, stamp.config
                )
            })?;
        self.spawn_proc(plugin, &cfg)
    }

    fn on_exit(&mut self, proc: u64, status: &str) {
        self.watchers.remove(&proc);
        let gone: Vec<u64> = self
            .stream_clients
            .iter()
            .filter(|(_, p)| **p == proc)
            .map(|(c, _)| *c)
            .collect();
        for c in gone {
            self.hub.detach(c);
            self.stream_clients.remove(&c);
        }
        let Some(p) = self.procs.remove(&proc) else {
            return;
        };
        if p.draining {
            eprintln!("agentmod: plugin `{}` drained and stopped", p.name);
            return;
        }
        for pending in p.pending.values() {
            if let Pending::Service { from, id } = pending
                && let Some(req) = self.procs.get(from)
            {
                req.reply(id, Err((-32001, format!("`{}` exited: {status}", p.name))));
            }
        }
        eprintln!("agentmod: plugin `{}` exited: {status}", p.name);
        self.crashes
            .entry(key(&p.name, &p.stamp))
            .or_default()
            .push(Instant::now());
        // Fault isolation: only this plugin's in-flight invocations are affected;
        // each is restarted from the record (bounded by max_attempts).
        for inv in p.invocations.keys() {
            let fx = self.kernel.retry(
                inv,
                &format!("plugin process exited: {status}"),
                self.max_attempts,
                now_ms(),
            );
            self.execute(fx);
        }
        self.progress_applies(proc);
    }

    fn on_tick(&mut self) {
        // Watchers whose stdin drained: deliver what waited in their queues.
        let waiting: Vec<u64> = self
            .stream_clients
            .keys()
            .copied()
            .filter(|c| self.hub.queued(*c) > 0)
            .collect();
        for c in waiting {
            self.deliver_live(c);
        }
        let now = Instant::now();
        // Invocation timeouts: cancel, record failure, escalate if ignored.
        let expired: Vec<(u64, String)> = self
            .procs
            .iter()
            .flat_map(|(id, p)| {
                p.invocations
                    .iter()
                    .filter(|(_, d)| **d <= now)
                    .map(|(inv, _)| (*id, inv.clone()))
            })
            .collect();
        for (id, inv) in expired {
            if let Some(p) = self.procs.get_mut(&id) {
                p.invocations.remove(&inv);
                p.send("cancel", &json!({ "invocation_id": inv }), None);
                self.cancel_deadlines
                    .insert(inv.clone(), (id, now + CANCEL_GRACE));
            }
            let fx = self.kernel.complete(
                &inv,
                &InvocationResult {
                    error: Some("invocation timed out".into()),
                    ..Default::default()
                },
                now_ms(),
            );
            self.execute(fx);
        }
        // Plugins that ignore cancellation are killed (others' work is retried).
        let overdue: Vec<(String, u64)> = self
            .cancel_deadlines
            .iter()
            .filter(|(_, (_, d))| *d <= now)
            .map(|(inv, (id, _))| (inv.clone(), *id))
            .collect();
        for (inv, id) in overdue {
            self.cancel_deadlines.remove(&inv);
            let still_running = self.procs.get(&id).is_some_and(|p| {
                p.pending
                    .values()
                    .any(|pe| matches!(pe, Pending::Invoke(i) if *i == inv))
            });
            if still_running && let Some(p) = self.procs.get_mut(&id) {
                eprintln!(
                    "agentmod: `{}` ignored cancellation of {inv}; killing the process",
                    p.name
                );
                p.handle.kill();
            }
        }
        // Pending applies that waited too long finish with what they have.
        let stale: Vec<usize> = self
            .applies
            .iter()
            .enumerate()
            .filter(|(_, a)| a.started.elapsed() > HANDSHAKE_TIMEOUT)
            .map(|(i, _)| i)
            .collect();
        for i in stale.into_iter().rev() {
            let a = self.applies.remove(i);
            self.finish_apply(a);
        }
        // Inactive sessions leave memory entirely.
        let idle: Vec<String> = self
            .last_touch
            .iter()
            .filter(|(_, t)| t.elapsed() > IDLE_UNLOAD)
            .map(|(s, _)| s.clone())
            .collect();
        for sid in idle {
            if self.kernel.unload_if_idle(&sid) {
                self.records.remove(&sid);
                self.last_touch.remove(&sid);
                self.store.close(&sid);
            }
        }
        // Drain processes no installed-and-used config references.
        if self.applies.is_empty() {
            let in_use: BTreeSet<Key> = self
                .kernel
                .configs_in_use()
                .iter()
                .filter_map(|h| self.kernel.config(h))
                .flat_map(|c| c.stamps.iter().map(|(n, s)| key(n, s)).collect::<Vec<_>>())
                .collect();
            for p in self.procs.values_mut() {
                if !p.draining
                    && p.ready
                    && p.invocations.is_empty()
                    && !in_use.contains(&key(&p.name, &p.stamp))
                {
                    p.draining = true;
                    p.send("shutdown", &json!({}), Some(Pending::Shutdown));
                }
            }
        }
        let _ = self.store.flush_index();
    }

    // ------------------------------------------------------------------
    // Live configuration apply (hot swap).
    // ------------------------------------------------------------------

    fn begin_apply(
        &mut self,
        requester: Option<(u64, Value)>,
        params: &Value,
    ) -> Result<(), String> {
        let mut config: DeploymentConfig =
            serde_json::from_value(params.get("config").cloned().ok_or("config required")?)
                .map_err(|e| format!("invalid config: {e}"))?;
        let scope: Scope = params
            .get("scope")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| format!("invalid scope: {e}"))?
            .unwrap_or(Scope::Global);
        if let Scope::Session { session_id } = &scope {
            self.ensure_loaded(session_id)?;
        }
        stamp_binaries(&mut config, &self.base_dir);
        disable_unrunnable(&mut config);
        let mut waiting = BTreeSet::new();
        for (name, cfg) in config.plugins.iter().filter(|(_, c)| !c.disabled) {
            let k = key(name, &cfg.stamp());
            if !self
                .procs
                .values()
                .any(|p| key(&p.name, &p.stamp) == k && !p.failed && !p.draining)
            {
                match self.spawn_proc(name, cfg) {
                    Ok(id) => {
                        waiting.insert(id);
                    }
                    Err(e) => eprintln!("agentmod: {e}"),
                }
            }
        }
        let apply = PendingApply {
            requester,
            config,
            scope,
            waiting,
            started: Instant::now(),
        };
        if apply.waiting.is_empty() {
            self.finish_apply(apply);
        } else {
            self.applies.push(apply);
        }
        Ok(())
    }

    fn progress_applies(&mut self, proc: u64) {
        let mut done = Vec::new();
        for (i, a) in self.applies.iter_mut().enumerate() {
            if a.waiting.remove(&proc) && a.waiting.is_empty() {
                done.push(i);
            }
        }
        for i in done.into_iter().rev() {
            let a = self.applies.remove(i);
            self.finish_apply(a);
        }
    }

    fn finish_apply(&mut self, a: PendingApply) {
        let manifests = self.manifests_for(&a.config);
        let compilation = compile(&a.config, &manifests);
        let summary = json!({ "hash": compilation.hash, "ok": compilation.ok, "diagnostics": compilation.diagnostics });
        let result = if compilation.ok {
            let _ = self.store.save_compilation(&compilation);
            match self.kernel.install(compilation) {
                Ok(hash) => {
                    let out = self.kernel.apply_config(&hash, &a.scope, now_ms());
                    self.store.journal(
                        now_ms(),
                        "config-applied",
                        &json!({ "hash": hash, "scope": a.scope, "skipped": out.skipped }),
                    );
                    let skipped = out.skipped.clone();
                    self.execute(out.effects);
                    match out.error {
                        Some(e) => Err((-32002, e)),
                        None => Ok(
                            json!({ "hash": hash, "ok": true, "skipped": skipped, "diagnostics": summary["diagnostics"] }),
                        ),
                    }
                }
                Err(e) => Err((-32002, e)),
            }
        } else {
            self.store.journal(now_ms(), "config-rejected", &summary);
            // Rejected whole: nothing changes; the diagnostics explain why.
            Ok(summary)
        };
        if let Some((proc, id)) = a.requester
            && let Some(p) = self.procs.get(&proc)
        {
            p.reply(&id, result);
        }
    }
}
