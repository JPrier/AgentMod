// In-browser runtime host.
//
// Runs the real AgentMod kernel (Rust compiled to WebAssembly) in the page and
// each plugin in its own module Web Worker, speaking the same JSON-RPC
// messages the native runtime speaks over stdio. The web frontend is the page
// itself, presented to the kernel as the `web-ui` plugin with the same
// manifest the native gateway uses.

import init, { WasmKernel, compile, project, context_at } from '../pkg/agentmod_wasm.js';
import { WEB_UI_MANIFEST } from '../plugins/web-ui/manifest.js';
import { Devices } from './devices.js';
import { openStore } from './persist.js';

const PROTOCOL = 'agentmod/0.1';
const CANCEL_GRACE_MS = 3000;
const PAGE = 'web-ui';

const J = (s) => {
  const v = JSON.parse(s);
  if (v && typeof v === 'object' && !Array.isArray(v) && typeof v.error === 'string' && Object.keys(v).length === 1) throw new Error(v.error);
  return v;
};

/** Relative imports of a module's source (static `from '…'` and `import('…')`). */
function relativeImports(src) {
  const out = [];
  for (const m of src.matchAll(/(?:from\s+|import\s*\(\s*|import\s+)['"](\.{1,2}\/[^'"]+)['"]/g)) out.push(m[1]);
  return out;
}

async function sha256(text) {
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, '0')).join('').slice(0, 16);
}

function stampOf(name, cfg) {
  return `${name}|${cfg.binary_hash}|${JSON.stringify(cfg.config ?? null)}`;
}

class WorkerProc {
  constructor(host, name, cfg) {
    this.host = host;
    this.name = name;
    this.cfg = cfg;
    this.key = stampOf(name, cfg);
    this.ready = false;
    this.failed = false;
    this.draining = false;
    this.manifest = null;
    this.nextId = 1;
    this.pending = new Map();
    this.invocations = new Map(); // invocation_id -> deadline
    this.queue = [];
    this.readyWaiters = [];
    const url = new URL(cfg.module, host.base);
    this.worker = new Worker(url, { type: 'module', name });
    this.worker.onmessage = (e) => host.onMessage(this, e.data);
    this.worker.onerror = (e) => {
      e.preventDefault?.();
      host.log(`plugin ${name} error: ${e.message || 'worker error'}`);
      host.onExit(this, e.message || 'worker error');
    };
    this.send('initialize', { protocol: PROTOCOL, plugin: name, config: cfg.config ?? {} }, { kind: 'initialize' });
  }

  send(method, params, pending) {
    const msg = { jsonrpc: '2.0', method, params };
    if (pending) {
      msg.id = this.nextId++;
      this.pending.set(msg.id, pending);
    }
    if (this.ready || method === 'initialize') this.worker.postMessage(msg);
    else this.queue.push(msg);
  }

  reply(id, result, error) {
    this.worker.postMessage(error ? { jsonrpc: '2.0', id, error } : { jsonrpc: '2.0', id, result });
  }

  whenReady() {
    if (this.ready || this.failed) return Promise.resolve(this);
    return new Promise((r) => this.readyWaiters.push(r));
  }

  markReady(manifest) {
    this.manifest = manifest;
    this.ready = true;
    for (const m of this.queue.splice(0)) this.worker.postMessage(m);
    for (const r of this.readyWaiters.splice(0)) r(this);
  }

  fail(refusal) {
    this.failed = true;
    if (refusal) this.refusal = refusal;
    for (const r of this.readyWaiters.splice(0)) r(this);
  }

  has(cap) {
    return !!this.manifest?.capabilities?.includes(cap);
  }

  kill() {
    this.worker.terminate();
  }
}

/** Remove secret values from a compilation (declared secret settings, api_key, secrets.*.value). */
export function redactConfig(c) {
  const secretKeys = new Set(['api_key']);
  for (const m of Object.values(c.manifests || {})) for (const s of m.settings || []) if (s.secret) secretKeys.add(s.key);
  for (const p of Object.values(c.config?.plugins || {})) {
    if (!p.config) continue;
    for (const k of Object.keys(p.config)) if (secretKeys.has(k)) p.config[k] = '(omitted)';
    for (const s of Object.values(p.config.secrets || {})) if (s && typeof s === 'object' && 'value' in s) s.value = '(omitted)';
  }
  return c;
}

export class BrowserRuntime {
  constructor({ base = new URL('./', location.href), log = console.log, persistence = true, store = null } = {}) {
    this.base = base;
    this.log = log;
    this.procs = new Set();
    this.records = new Map();
    this.listeners = new Set();
    this.cites = new Map();
    this.cancelDeadlines = new Map();
    this.crashes = new Map();
    this.maxAttempts = 3;
    this.journal = [];
    this.mode = 'browser';
    // Page-only resources lent to plugins that declare them (see devices.js).
    // They outlive plugin workers, so a restarted plugin finds its VM running.
    this.devices = new Devices();
    this.persistence = persistence;
    this.store = store;
  }

  // ------------------------------------------------------------------
  // Boot
  // ------------------------------------------------------------------

  async boot(config) {
    if (!this.wasmReady) {
      await init();
      this.wasmReady = true;
    }
    this.kernel = new WasmKernel();
    const cfg = await this.prepare(config ?? (await (await fetch(new URL('runtime/agentmod.config.json', this.base))).json()));
    this.maxAttempts = cfg.runtime?.max_attempts ?? 3;
    await Promise.all(Object.entries(cfg.plugins).filter(([, p]) => !p.disabled && p.module).map(([n, p]) => this.ensureProcByConfig(n, p).whenReady()));
    const comp = J(compile(JSON.stringify(cfg), JSON.stringify(this.manifestsFor(cfg))));
    if (!comp.ok) {
      const refusals = [...this.procs].filter((p) => p.failed && p.refusal).map((p) => p.refusal);
      throw Object.assign(new Error(`config rejected: ${comp.diagnostics.filter((d) => d.severity === 'error').map((d) => d.message).join('; ')}`), { refusals, diagnostics: comp.diagnostics });
    }
    // Durable history: install every stored compilation (sessions reference
    // them), then the new one, then replay every stored session.
    if (this.persistence !== false) this.store ??= await openStore();
    this.compilations = new Map();
    let stored = { compilations: [], sessions: new Map() };
    if (this.store) {
      stored = await this.store.loadAll();
      for (const c of stored.compilations) {
        if (!c.ok) continue;
        J(this.kernel.install(JSON.stringify(c)));
        this.compilations.set(c.hash, c);
      }
      await this.store.putCompilation(comp);
    }
    const { hash } = J(this.kernel.install(JSON.stringify(comp)));
    J(this.kernel.set_active(hash));
    this.compilations.set(hash, comp);
    this.journal.push({ at: Date.now(), kind: 'config-loaded', detail: { hash, durable: !!this.store, persisted: this.store?.persisted ?? null } });
    const recovered = [];
    for (const [sid, recs] of [...stored.sessions].sort(([a], [b]) => a.localeCompare(b))) {
      try {
        J(this.kernel.load_session(JSON.stringify(recs)));
        this.records.set(sid, recs);
      } catch (e) {
        this.journal.push({ at: Date.now(), kind: 'session-unrecoverable', detail: { session_id: sid, error: e.message } });
      }
    }
    this.tick = setInterval(() => this.onTick(), 250);
    for (const sid of this.records.keys()) {
      const orphans = J(this.kernel.status(sid))?.open_invocations?.length || 0;
      if (orphans) recovered.push({ session_id: sid, orphans });
      this.execute(J(this.kernel.recover(sid, this.maxAttempts, Date.now())).effects);
    }
    if (stored.sessions.size) this.journal.push({ at: Date.now(), kind: 'sessions-restored', detail: { sessions: stored.sessions.size, recovered } });
    return comp;
  }

  /** Is history durable in this browser? (false: memory only) */
  get durable() {
    return !!this.store;
  }

  /** Delete every stored session and compilation (this browser only). */
  async clearStored() {
    await this.store?.clear();
  }

  /** Stamp code hashes; disable plugins this host cannot run (no `module`). */
  async prepare(config) {
    const cfg = structuredClone(config);
    await Promise.all(
      Object.entries(cfg.plugins).map(async ([name, p]) => {
        if (name === PAGE) {
          p.binary_hash = await sha256(JSON.stringify(WEB_UI_MANIFEST));
          p.disabled = false;
          return;
        }
        if (!p.module) {
          p.disabled = true;
          return;
        }
        // The stamp covers the module and everything it imports (shared SDK
        // modules included), so any code change is a new plugin identity.
        const seen = new Map();
        const visit = async (url) => {
          if (seen.has(url.href)) return;
          seen.set(url.href, '');
          const src = await (await fetch(url)).text();
          seen.set(url.href, src);
          for (const spec of relativeImports(src)) await visit(new URL(spec, url));
        };
        await visit(new URL(p.module, this.base));
        p.binary_hash = await sha256([...seen.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([u, src]) => `${u.replace(this.base.href, '')}\n${src}`).join('\n'));
      }),
    );
    return cfg;
  }

  manifestsFor(cfg) {
    const out = {};
    for (const [name, p] of Object.entries(cfg.plugins)) {
      if (p.disabled) continue;
      if (name === PAGE) {
        out[name] = WEB_UI_MANIFEST;
        continue;
      }
      const proc = [...this.procs].find((x) => x.key === stampOf(name, p) && x.ready);
      if (proc) out[name] = proc.manifest;
    }
    return out;
  }

  ensureProcByConfig(name, cfg) {
    const key = stampOf(name, cfg);
    let proc = [...this.procs].find((p) => p.key === key && !p.failed && !p.draining);
    if (!proc) {
      proc = new WorkerProc(this, name, cfg);
      this.procs.add(proc);
      this.log(`started plugin ${name} (worker, stamp ${cfg.binary_hash})`);
    }
    return proc;
  }

  ensureProc(plugin, stamp) {
    for (const hash of J(this.kernel.configs_in_use())) {
      const comp = this.compilation(hash);
      if (comp?.stamps?.[plugin]?.binary === stamp.binary && comp.stamps[plugin].config === stamp.config) {
        const cfg = comp.config.plugins[plugin];
        const recent = (this.crashes.get(stampOf(plugin, cfg)) || []).filter((t) => Date.now() - t < 60000).length;
        if (recent >= 5) throw new Error(`plugin \`${plugin}\` is crash-looping`);
        return this.ensureProcByConfig(plugin, cfg);
      }
    }
    throw new Error(`no configuration for plugin \`${plugin}\``);
  }

  compilation(hash) {
    if (!this.compilations.has(hash)) {
      const c = J(this.kernel.config(hash));
      if (c) this.compilations.set(hash, c);
    }
    return this.compilations.get(hash);
  }

  // ------------------------------------------------------------------
  // Effects
  // ------------------------------------------------------------------

  execute(effects) {
    if (!effects?.length) return;
    const appended = [];
    for (const fx of effects) {
      if (fx.type !== 'append') continue;
      const r = fx.record;
      if (!this.records.has(r.session_id)) this.records.set(r.session_id, []);
      this.records.get(r.session_id).push(r);
      appended.push(r);
    }
    const act = () => {
      for (const fx of effects) {
        if (fx.type === 'invoke') this.dispatch(fx);
        else if (fx.type === 'cancel') this.cancel(fx);
      }
      for (const r of appended) for (const l of this.listeners) l(r);
    };
    if (!this.store) return act();
    // Write-ahead: records are durable before anything they describe happens.
    // Batches stay in order behind one promise chain.
    this.durableTail = (this.durableTail || Promise.resolve())
      .then(() => this.store.appendRecords(appended))
      .then(act, (e) => {
        this.persistError = e;
        this.log(`FATAL: could not persist records (${e?.message || e}); dispatch stopped`);
        this.journal.push({ at: Date.now(), kind: 'persist-failed', detail: { error: String(e?.message || e) } });
      });
  }

  /** Resolves once every record produced so far is durable and dispatched. */
  flushed() {
    return this.durableTail || Promise.resolve();
  }

  dispatch(fx) {
    const inv = fx.request.invocation_id;
    if (fx.plugin === PAGE) {
      // The page frontend answers at once; its first invocation in a session is
      // the citation for user actions, so each starts a fresh causal chain.
      if (!this.cites.has(fx.request.event.session_id)) this.cites.set(fx.request.event.session_id, inv);
      queueMicrotask(() => this.execute(J(this.kernel.complete(inv, '{}', Date.now()))?.effects));
      return;
    }
    let proc;
    try {
      proc = this.ensureProc(fx.plugin, fx.stamp);
    } catch (e) {
      this.execute(J(this.kernel.complete(inv, JSON.stringify({ error: e.message }), Date.now())).effects);
      return;
    }
    proc.invocations.set(inv, Date.now() + (proc.cfg.timeout_ms ?? 120000));
    proc.send('invoke', fx.request, { kind: 'invoke', inv });
  }

  cancel(fx) {
    for (const p of this.procs) {
      if (p.name === fx.plugin && p.invocations.has(fx.invocation_id)) {
        p.send('cancel', { invocation_id: fx.invocation_id });
        p.invocations.delete(fx.invocation_id);
        this.cancelDeadlines.set(fx.invocation_id, { proc: p, at: Date.now() + CANCEL_GRACE_MS });
      }
    }
  }

  onMessage(proc, msg) {
    if (msg.method === undefined) {
      const p = proc.pending.get(msg.id);
      if (!p) return;
      proc.pending.delete(msg.id);
      if (p.kind === 'initialize') {
        if (msg.result?.manifest) proc.markReady(msg.result.manifest);
        else {
          // A refusal (e.g. a missing credential) keeps the worker alive so its
          // read-only services (a model catalog) can still help the user fix it.
          this.log(`plugin ${proc.name} refused to start: ${msg.error?.message || 'invalid handshake'}`);
          proc.fail({ plugin: proc.name, message: msg.error?.message || 'invalid handshake', ...(msg.error?.data || {}) });
        }
      } else if (p.kind === 'service') {
        if (msg.error) p.reject(Object.assign(new Error(msg.error.message), { code: msg.error.code }));
        else p.resolve(msg.result);
      } else if (p.kind === 'invoke') {
        proc.invocations.delete(p.inv);
        this.cancelDeadlines.delete(p.inv);
        const result = msg.error ? { error: msg.error.message } : msg.result || {};
        this.execute(J(this.kernel.complete(p.inv, JSON.stringify(result), Date.now())).effects);
      }
      return;
    }
    const answer = (fn) => {
      Promise.resolve()
        .then(fn)
        .then(
          (r) => r !== undefined && msg.id !== undefined && proc.reply(msg.id, r),
          (e) => msg.id !== undefined && proc.reply(msg.id, null, { code: e.code ?? -32001, message: e.message }),
        );
    };
    const params = msg.params || {};
    switch (msg.method) {
      case 'publish':
        return answer(() => this.publishAs(proc.name, params));
      case 'start_session':
        return answer(() => this.startAs(proc.name, params));
      case 'query':
        return answer(() => this.query(params));
      case 'command':
        return answer(() => {
          if (!proc.has('control')) throw Object.assign(new Error(`\`${proc.name}\` lacks the control capability`), { code: -32003 });
          return this.command(params.session_id, params.command, proc.name);
        });
      case 'apply_config':
        return answer(() => {
          if (!proc.has('control')) throw Object.assign(new Error(`\`${proc.name}\` lacks the control capability`), { code: -32003 });
          return this.applyConfig(params.config, params.scope);
        });
      case 'watch':
        return answer(() => {
          if (!proc.has('observe')) throw new Error(`\`${proc.name}\` lacks the observe capability`);
          const l = (r) => proc.worker.postMessage({ jsonrpc: '2.0', method: 'record', params: { record: r } });
          this.listeners.add(l);
          return { watching: true };
        });
      case 'device':
        return answer(() => this.devices.call(proc.name, proc.manifest, params));
      case 'call_service':
        return answer(() => {
          if (!proc.has('control')) throw Object.assign(new Error(`\`${proc.name}\` lacks the control capability`), { code: -32003 });
          return this.callService(params.plugin, params.service, params.args);
        });
      case 'log':
        this.log(`[${proc.name}] ${params.message}`);
        return undefined;
      default:
        return answer(() => {
          throw Object.assign(new Error(`unknown method ${msg.method}`), { code: -32601 });
        });
    }
  }

  onExit(proc, reason) {
    if (!this.procs.has(proc)) return;
    this.procs.delete(proc);
    proc.kill();
    proc.fail();
    if (proc.draining) return;
    if (!this.crashes.has(proc.key)) this.crashes.set(proc.key, []);
    this.crashes.get(proc.key).push(Date.now());
    for (const inv of proc.invocations.keys()) {
      this.execute(J(this.kernel.retry(inv, `plugin worker exited: ${reason}`, this.maxAttempts, Date.now())).effects);
    }
  }

  onTick() {
    const now = Date.now();
    for (const p of this.procs) {
      for (const [inv, deadline] of p.invocations) {
        if (deadline > now) continue;
        p.invocations.delete(inv);
        p.send('cancel', { invocation_id: inv });
        this.cancelDeadlines.set(inv, { proc: p, at: now + CANCEL_GRACE_MS });
        this.execute(J(this.kernel.complete(inv, JSON.stringify({ error: 'invocation timed out' }), now)).effects);
      }
    }
    for (const [inv, { proc, at }] of this.cancelDeadlines) {
      if (at > now) continue;
      this.cancelDeadlines.delete(inv);
      const stillRunning = [...proc.pending.values()].some((p) => p.kind === 'invoke' && p.inv === inv);
      if (stillRunning) {
        this.log(`${proc.name} ignored cancellation of ${inv}; terminating its worker`);
        this.onExit(proc, 'terminated after ignoring cancellation');
      }
    }
    if (!this.applying) {
      const inUse = new Set();
      for (const h of J(this.kernel.configs_in_use())) {
        const c = this.compilation(h);
        for (const [n, p] of Object.entries(c?.config?.plugins || {})) if (!p.disabled) inUse.add(stampOf(n, p));
      }
      for (const p of this.procs) {
        if (!p.draining && p.ready && p.invocations.size === 0 && !inUse.has(p.key)) {
          p.draining = true;
          p.send('shutdown', {}, { kind: 'shutdown' });
          setTimeout(() => {
            p.kill();
            this.procs.delete(p);
            this.log(`plugin ${p.name} drained and stopped`);
          }, 300);
        }
      }
    }
  }

  // ------------------------------------------------------------------
  // Host API (also the frontend client API)
  // ------------------------------------------------------------------

  publishAs(plugin, params) {
    const req = { ...params, plugin };
    for (const k of ['cite', 'invocation_id', 'ui', 'target_session', 'lane']) if (req[k] == null) delete req[k];
    const out = J(this.kernel.publish(JSON.stringify(req), Date.now()));
    this.execute(out.effects);
    if (out.blocked) {
      if (!out.effects?.length) this.journal.push({ at: Date.now(), kind: 'publish-blocked', detail: { plugin, event_name: req.event_name, reason: out.blocked } });
      throw Object.assign(new Error(`publish blocked: ${out.blocked}`), { code: -32001 });
    }
    return { event_id: out.event_id, duplicate: !!out.duplicate };
  }

  startAs(plugin, params) {
    const req = { plugin, definition: params.definition };
    if (params.invocation_id) req.invocation_id = params.invocation_id;
    if (params.initial) req.initial = params.initial;
    if (params.fork_from) req.fork_from = params.fork_from;
    const out = J(this.kernel.start_session(JSON.stringify(req), Date.now()));
    this.execute(out.effects);
    if (!out.session_id) throw new Error(out.error);
    return { session_id: out.session_id };
  }

  command(sessionId, command, by = PAGE) {
    const out = J(this.kernel.command(sessionId, command, by, Date.now()));
    this.execute(out.effects);
    return { status: out.status };
  }

  query({ what, session_id, event_id, sequence, from }) {
    const recs = () => {
      const r = this.records.get(session_id);
      if (!r) throw Object.assign(new Error(`unknown session ${session_id}`), { code: -32004 });
      return r;
    };
    switch (what) {
      case 'sessions':
        return [...this.records.keys()].map((sid) => this.summary(sid));
      case 'session':
        return { ...J(project(JSON.stringify(recs()))), status: J(this.kernel.status(session_id)) };
      case 'records':
        return recs().filter((r) => r.sequence >= (from ?? 0));
      case 'event':
        return J(project(JSON.stringify(recs()))).events.find((e) => e.event_id === event_id) ?? null;
      case 'context':
        return J(context_at(JSON.stringify(recs()), sequence ?? Number.MAX_SAFE_INTEGER));
      case 'status':
        return J(this.kernel.status(session_id));
      case 'graph':
        return J(this.kernel.active());
      case 'config':
        return { hash: this.kernel.active_hash(), config: J(this.kernel.active())?.config, installed: [...this.compilations.keys()], host: 'browser', durable: this.durable };
      case 'services': {
        const comp = J(this.kernel.active());
        return Object.entries(comp?.manifests || {})
          .filter(([, m]) => m.services?.length || m.settings?.length || m.provides?.length)
          .map(([plugin, m]) => ({ plugin, description: m.description, services: m.services || [], settings: m.settings || [], provides: m.provides || [], version: m.version }));
      }
      default:
        throw new Error(`unknown query ${what}`);
    }
  }

  summary(sid) {
    const r = this.records.get(sid);
    const created = r[0];
    let title = null;
    for (const rec of r) {
      if (rec.type === 'invocation-completed') for (const c of rec.contributions || []) if (c.op === 'add' && c.slot === 'title') title = c.value;
    }
    const status = J(this.kernel.status(sid));
    const parent = created.cause?.kind === 'invocation' ? created.cause.invocation_id.split('/')[0] : null;
    return { session_id: sid, definition: created.definition, title, created_at: created.at, updated_at: r[r.length - 1].at, last_sequence: r.length, parent, loaded: !!status, activity: status?.activity ?? 'idle' };
  }

  /**
   * Call a plugin's read-only service (e.g. a provider's model catalog). Also
   * works on a plugin that refused its handshake, so a frontend can help the
   * user supply what it needs. Not recorded: services change no state.
   */
  callService(plugin, service, args = {}) {
    const procs = [...this.procs].filter((p) => p.name === plugin && !p.draining);
    const proc = procs.find((p) => p.ready) || procs.find((p) => p.refusal);
    if (!proc) return Promise.reject(new Error(`no running plugin \`${plugin}\``));
    const declared = (proc.manifest?.services || proc.refusal?.services || []).some((s) => s.name === service);
    if (!declared) return Promise.reject(new Error(`\`${plugin}\` declares no service \`${service}\``));
    return new Promise((resolve, reject) => {
      const msg = { jsonrpc: '2.0', id: proc.nextId++, method: 'service', params: { service, args } };
      proc.pending.set(msg.id, { kind: 'service', resolve, reject });
      proc.worker.postMessage(msg);
    });
  }

  async listServices() {
    return this.query({ what: 'services' });
  }

  // ------------------------------------------------------------------
  // Live configuration apply
  // ------------------------------------------------------------------

  async applyConfig(config, scope = { kind: 'global' }) {
    const cfg = await this.prepare(config);
    const waits = Object.entries(cfg.plugins)
      .filter(([n, p]) => !p.disabled && n !== PAGE)
      .map(([n, p]) => this.ensureProcByConfig(n, p));
    this.applying = (this.applying || 0) + 1;
    try {
      await Promise.race([Promise.all(waits.map((w) => w.whenReady())), new Promise((r) => setTimeout(r, 15000))]);
    } finally {
      this.applying -= 1;
    }
    const comp = J(compile(JSON.stringify(cfg), JSON.stringify(this.manifestsFor(cfg))));
    const summary = { hash: comp.hash, ok: comp.ok, diagnostics: comp.diagnostics };
    if (!comp.ok) {
      this.journal.push({ at: Date.now(), kind: 'config-rejected', detail: summary });
      return summary;
    }
    const { hash } = J(this.kernel.install(JSON.stringify(comp)));
    this.compilations.set(hash, comp);
    const out = J(this.kernel.apply_config(hash, JSON.stringify(scope), Date.now()));
    this.journal.push({ at: Date.now(), kind: 'config-applied', detail: { hash, scope, skipped: out.skipped || [] } });
    this.execute(out.effects);
    if (out.error) throw new Error(out.error);
    return { ...summary, skipped: out.skipped || [] };
  }

  // ------------------------------------------------------------------
  // Crash simulation: history is the truth.
  // ------------------------------------------------------------------

  /**
   * Kill every plugin worker and the kernel, then rebuild from the logs alone:
   * install the recorded compilations, replay each session, and restart orphans.
   */
  async crashAndRecover() {
    clearInterval(this.tick);
    for (const p of this.procs) p.kill();
    this.procs.clear();
    this.cancelDeadlines.clear();
    // With durable storage, recover from what is *stored*, not from memory.
    if (this.store) {
      await this.flushed();
      const stored = await this.store.loadAll();
      this.records = new Map(stored.sessions);
    }
    const active = this.kernel.active_hash();
    this.kernel.free?.();
    this.kernel = new WasmKernel();
    for (const c of this.compilations.values()) J(this.kernel.install(JSON.stringify(c)));
    J(this.kernel.set_active(active));
    const recovered = [];
    for (const [sid, recs] of this.records) {
      J(this.kernel.load_session(JSON.stringify(recs)));
      const orphans = J(this.kernel.status(sid))?.open_invocations?.length || 0;
      if (orphans) recovered.push({ session_id: sid, orphans });
    }
    this.tick = setInterval(() => this.onTick(), 250);
    for (const sid of this.records.keys()) this.execute(J(this.kernel.recover(sid, this.maxAttempts, Date.now())).effects);
    this.journal.push({ at: Date.now(), kind: 'crash-recovered', detail: { recovered } });
    return recovered;
  }

  // ------------------------------------------------------------------
  // Frontend client surface (shared with the live client)
  // ------------------------------------------------------------------

  citeFor(sid) {
    let c = this.cites.get(sid);
    if (!c) {
      // After a reload, find the page's first invocation in the stored log.
      c = (this.records.get(sid) || []).find((r) => r.type === 'invocation-started' && r.plugin === PAGE)?.invocation_id;
      if (c) this.cites.set(sid, c);
    }
    if (!c) throw new Error(`the web frontend has no standing invocation in ${sid} yet`);
    return c;
  }

  onRecord(cb) {
    this.listeners.add(cb);
    return () => this.listeners.delete(cb);
  }

  async info() {
    return { mode: 'browser', manifest: WEB_UI_MANIFEST };
  }

  async listSessions() {
    return this.query({ what: 'sessions' });
  }

  async getSession(id) {
    return this.query({ what: 'session', session_id: id });
  }

  async getRecords(id) {
    return this.query({ what: 'records', session_id: id });
  }

  async getContext(id, sequence) {
    return this.query({ what: 'context', session_id: id, sequence });
  }

  async getGraph() {
    return this.query({ what: 'graph' });
  }

  async getConfig() {
    return this.query({ what: 'config' });
  }

  async startSession({ definition, text, fork_from }) {
    const initial = text ? { event_name: 'user-message', payload: { text }, ui: { v: 1, kind: 'text', role: 'user', text } } : undefined;
    return this.startAs(PAGE, { definition, initial, fork_from });
  }

  async sendMessage(id, text, lane = 'normal') {
    return this.publishAs(PAGE, { event_name: 'user-message', payload: { text }, cite: this.citeFor(id), lane, ui: { v: 1, kind: 'text', role: 'user', text } });
  }

  async uiAction(id, reply_to, action, values = {}) {
    return this.publishAs(PAGE, { event_name: 'ui-action', payload: { reply_to, action, values }, cite: this.citeFor(id) });
  }

  async contextEdit(id, ops) {
    return this.publishAs(PAGE, { event_name: 'context-edit', payload: { ops }, cite: this.citeFor(id), lane: 'priority' });
  }

  async sendCommand(id, command) {
    return this.command(id, command, PAGE);
  }

  async exportLogs() {
    // Secrets entered in the browser (provider keys, any setting a plugin
    // declares `secret`, configured secret values) are not exported.
    const compilations = [...this.compilations.values()].map((c) => redactConfig(structuredClone(c)));
    return { sessions: Object.fromEntries(this.records), compilations, journal: this.journal };
  }

  async projectRecords(records) {
    return J(project(JSON.stringify(records)));
  }
}
