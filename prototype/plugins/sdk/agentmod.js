// AgentMod plugin SDK for JavaScript.
//
// One plugin file runs unchanged in two hosts:
//   * the native runtime spawns it with Node and speaks newline-delimited
//     JSON-RPC 2.0 over stdin/stdout;
//   * the browser runtime loads it as a module Web Worker and speaks the same
//     messages over postMessage.
//
// A plugin is uncategorized: event in -> contributed context + published events.

import { createStreamWriter } from './stream.js';

export const PROTOCOL = 'agentmod/0.1';

const isNode = typeof process !== 'undefined' && !!process.versions?.node;
const isWorker = !isNode && typeof self !== 'undefined' && typeof self.postMessage === 'function' && typeof window === 'undefined';

async function createTransport() {
  if (isNode) {
    const readline = await import('node:readline');
    const util = await import('node:util');
    // stdout carries the protocol; route console output to stderr.
    for (const k of ['log', 'info', 'debug']) {
      console[k] = (...a) => process.stderr.write(util.format(...a) + '\n');
    }
    let handler = () => {};
    const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
    rl.on('line', (line) => {
      if (!line.trim()) return;
      let msg;
      try { msg = JSON.parse(line); } catch (e) { console.error('agentmod-sdk: bad json', e.message); return; }
      handler(msg);
    });
    rl.on('close', () => process.exit(0));
    return {
      kind: 'node',
      onMessage: (h) => { handler = h; },
      send: (obj) => process.stdout.write(JSON.stringify(obj) + '\n'),
      exit: (code = 0) => setTimeout(() => process.exit(code), 10),
      env: (k) => process.env[k],
    };
  }
  if (isWorker) {
    let handler = () => {};
    self.onmessage = (e) => handler(e.data);
    return {
      kind: 'worker',
      onMessage: (h) => { handler = h; },
      send: (obj) => self.postMessage(obj),
      exit: () => self.close(),
      env: () => undefined,
    };
  }
  throw new Error('agentmod-sdk: unsupported environment (expected Node or a Web Worker)');
}

async function createStorage(name, env) {
  if (isNode) {
    const fs = await import('node:fs');
    const path = await import('node:path');
    const dir = env('AGENTMOD_PLUGIN_DATA') || '.';
    const file = path.join(dir, `${name}.json`);
    let data = {};
    try { data = JSON.parse(fs.readFileSync(file, 'utf8')); } catch { /* fresh */ }
    return {
      get: (k, d) => (k in data ? data[k] : d),
      set: (k, v) => {
        data[k] = v;
        fs.mkdirSync(dir, { recursive: true });
        const tmp = file + '.tmp';
        fs.writeFileSync(tmp, JSON.stringify(data, null, 2));
        fs.renameSync(tmp, file);
      },
      all: () => ({ ...data }),
    };
  }
  const data = {};
  return { get: (k, d) => (k in data ? data[k] : d), set: (k, v) => { data[k] = v; }, all: () => ({ ...data }) };
}

/** Sleep that rejects when the signal aborts. */
export function sleep(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(new Error('cancelled'));
    const t = setTimeout(resolve, ms);
    signal?.addEventListener('abort', () => { clearTimeout(t); reject(new Error('cancelled')); }, { once: true });
  });
}

/** Read context items of one slot (values only). */
export function slot(context, name) {
  return (context || []).filter((c) => c.slot === name).map((c) => c.value);
}

/**
 * Define and start a plugin.
 *
 * @param {object} spec
 * @param {object} spec.manifest  name, version, consumes, emits, transforms, capabilities
 * @param {Record<string, Function>} spec.handlers  event name (or '*') -> async (ctx) => result
 * @param {Function} [spec.validate] async (config, {env}) => void; throw to refuse the handshake
 * @param {Function} [spec.init]   async (plugin) => void, after the handshake
 * @param {Function} [spec.onRecord] (record) => void, for plugins that `watch`
 * @param {Function} [spec.onStream] (messages) => void, live stream messages for watchers
 */
export function definePlugin(spec) {
  const plugin = new Plugin(spec);
  plugin.start().catch((e) => console.error('agentmod-sdk: failed to start', e));
  return plugin;
}

class Plugin {
  constructor(spec) {
    this.spec = spec;
    this.nextId = 1;
    this.pending = new Map();
    this.aborts = new Map();
    this.config = {};
    this.manifest = typeof spec.manifest === 'function' ? null : spec.manifest;
    this.name = spec.name || this.manifest?.name || 'plugin';
  }

  /** The manifest for a config (a manifest may depend on it, e.g. declared tools). */
  manifestFor(config) {
    const m = typeof this.spec.manifest === 'function' ? this.spec.manifest(config || {}) : this.spec.manifest;
    return m;
  }

  async start() {
    this.transport = await createTransport();
    this.transport.onMessage((m) => this.handle(m));
  }

  send(obj) { this.transport.send({ jsonrpc: '2.0', ...obj }); }

  /** A JSON-RPC notification (no reply). */
  notify(method, params) { this.send({ method, params }); }

  request(method, params) {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.send({ id, method, params });
    });
  }

  log(...args) { console.error(`[${this.name}]`, ...args); }

  /** Host API available to every plugin (capabilities are enforced by the runtime). */
  get host() {
    return {
      query: (what, args = {}) => this.request('query', { what, ...args }),
      startSession: ({ definition, initial, invocationId, forkFrom }) =>
        this.request('start_session', { definition, initial, invocation_id: invocationId, fork_from: forkFrom }),
      publishDeferred: (event_name, payload = {}, opts = {}) =>
        this.request('publish', { event_name, payload, cite: opts.cite, ui: opts.ui, lane: opts.lane, target_session: opts.targetSession }),
      command: (session_id, command) => this.request('command', { session_id, command }),
      applyConfig: (config, scope) => this.request('apply_config', { config, scope }),
      /** Call a read-only service another plugin declares (control capability). */
      callService: (plugin, service, args = {}) => this.request('call_service', { plugin, service, args }),
      /** Tail records; `{ streams: true }` also delivers live stream messages (onStream). */
      watch: (opts = {}) => this.request('watch', opts),
      /** After `resync-required`: snapshots of open streams; delivery resumes. */
      streamResync: () => this.request('stream_resync', {}),
      /** Use a host device the manifest declares (browser runtime; see ui/runtime/devices.js). */
      device: (device, op, args = {}, config = {}) => this.request('device', { device, op, args, config }),
    };
  }

  async handle(msg) {
    if (msg.method === undefined) {
      const p = this.pending.get(msg.id);
      if (!p) return;
      this.pending.delete(msg.id);
      if (msg.error) p.reject(Object.assign(new Error(msg.error.message), { code: msg.error.code }));
      else p.resolve(msg.result);
      return;
    }
    const { id, method, params } = msg;
    try {
      switch (method) {
        case 'initialize': {
          this.config = params.config || {};
          this.instance = params.plugin;
          this.manifest = this.manifestFor(this.config);
          this.name = this.manifest?.name || this.name;
          if (this.spec.validate) {
            // A plugin may refuse to start (e.g. a required secret is missing);
            // the runtime then cannot compile a config that uses it.
            try {
              await this.spec.validate(this.config, { env: this.transport.env });
            } catch (e) {
              // The refusal carries what a frontend needs to help: the settings
              // and services the plugin declares (services stay callable).
              const m = this.manifest;
              this.send({ id, error: { code: -32010, message: e.message, data: { ...(e.data || {}), description: m.description, settings: m.settings || [], services: m.services || [] } } });
              return;
            }
          }
          this.storage = await createStorage(params.plugin || this.name, this.transport.env);
          this.send({ id, result: { protocol: PROTOCOL, manifest: this.manifest } });
          if (this.spec.init) {
            Promise.resolve()
              .then(() => this.spec.init(this))
              .catch((e) => this.log('init failed:', e.message));
          }
          return;
        }
        case 'invoke': {
          const result = await this.invoke(params);
          this.send({ id, result });
          return;
        }
        case 'service': {
          // Read-only services a manifest declares (e.g. a provider's model
          // catalog). Not invocations: they are not recorded and must not
          // change state.
          const fn = this.spec.services?.[params?.service];
          if (!fn) {
            this.send({ id, error: { code: -32601, message: `\`${this.name}\` has no service \`${params?.service}\`` } });
            return;
          }
          const result = await fn(params.args || {}, { config: this.config, env: this.transport.env, plugin: this });
          this.send({ id, result: result ?? null });
          return;
        }
        case 'cancel': {
          this.aborts.get(params.invocation_id)?.abort();
          return;
        }
        case 'record': {
          this.spec.onRecord?.(params.record, this);
          return;
        }
        case 'stream': {
          // Live stream messages for watchers (frames, finalization, resync).
          this.spec.onStream?.(params.messages || [], this);
          return;
        }
        case 'shutdown': {
          await this.spec.shutdown?.(this);
          this.send({ id, result: null });
          this.transport.exit(0);
          return;
        }
        default:
          if (id !== undefined) this.send({ id, error: { code: -32601, message: `unknown method ${method}` } });
      }
    } catch (e) {
      if (id !== undefined) this.send({ id, error: { code: -32000, message: e.message } });
    }
  }

  async invoke(req) {
    const ev = req.event;
    const handler = this.spec.handlers?.[ev.event_name] ?? this.spec.handlers?.['*'];
    const result = { contributions: [] };
    if (!handler) return result;
    const abort = new AbortController();
    this.aborts.set(req.invocation_id, abort);
    const plugin = this;
    const ctx = {
      plugin,
      event: ev,
      payload: ev.payload,
      context: ev.context || [],
      sessionId: ev.session_id,
      invocationId: req.invocation_id,
      attempt: req.attempt,
      mode: req.mode,
      signal: abort.signal,
      config: this.config,
      storage: this.storage,
      host: this.host,
      log: (...a) => this.log(...a),
      sleep: (ms) => sleep(ms, abort.signal),
      slot: (name) => slot(ev.context, name),
      /** Publish as output of this invocation. */
      publish: (event_name, payload = {}, opts = {}) =>
        this.request('publish', {
          invocation_id: req.invocation_id,
          event_name,
          payload,
          ui: opts.ui,
          lane: opts.lane,
          target_session: opts.targetSession,
        }),
      add: (slotName, value) => result.contributions.push({ op: 'add', slot: slotName, value }),
      replace: (id, value) => result.contributions.push({ op: 'replace', id, value }),
      remove: (id) => result.contributions.push({ op: 'remove', id }),
      clearSlot: (slotName) => result.contributions.push({ op: 'clear-slot', slot: slotName }),
      restore: (to_sequence) => result.contributions.push({ op: 'restore', to_sequence }),
      contribute: (op) => result.contributions.push(op),
      veto: (reason) => { result.veto = reason; },
      transform: (payload) => { result.transform = payload; },
      /** A provider stream for this invocation (see sdk/stream.js). */
      stream: (opts) => createStreamWriter(ctx, opts),
    };
    try {
      const ret = await handler(ctx);
      if (ret && typeof ret === 'object') {
        if (ret.contributions) result.contributions.push(...ret.contributions);
        if (ret.transform) result.transform = ret.transform;
        if (ret.veto) result.veto = ret.veto;
      }
      return result;
    } catch (e) {
      if (abort.signal.aborted) return { contributions: [], error: 'cancelled' };
      return { contributions: [], error: e?.message || String(e) };
    } finally {
      this.aborts.delete(req.invocation_id);
    }
  }
}

/**
 * Standard tool description contributed to the `tools` context slot.
 *
 * `opts.tier` is `core` (sent to the model every turn) or `deferred` (found
 * through tool_search; see sdk/projection.js). `group` names a capability
 * family for discovery and policy, `effects` is read | write | varies, and
 * `required` lists required parameters.
 */
export function toolSpec(name, description, parameters = {}, opts = {}) {
  const spec = { name, description, parameters };
  if (opts.required?.length) spec.required = opts.required;
  if (opts.tier) spec.tier = opts.tier;
  if (opts.group) spec.group = opts.group;
  if (opts.effects) spec.effects = opts.effects;
  if (opts.trust) spec.trust = opts.trust;
  // Phrases in a user message that make a deferred tool load by itself.
  if (opts.intents?.length) spec.intents = opts.intents;
  return spec;
}

/**
 * Contribute this plugin's tool descriptions unless already present
 * (used on session-started and config-applied). A tool the plugin's manifest
 * does not declare is not offered: every offered tool has a compiled owner.
 */
export function offerTools(ctx, tools) {
  const present = new Set(ctx.slot('tools').map((t) => t?.name));
  const declared = ctx.plugin?.manifest?.tools;
  const owns = (name) => !declared || declared.some((d) => d.name === name || (d.name.endsWith('*') && name.startsWith(d.name.slice(0, -1))));
  for (const t of tools) {
    if (present.has(t.name)) continue;
    if (!owns(t.name)) {
      ctx.log(`not offering \`${t.name}\`: it is not declared in this plugin's manifest tools`);
      continue;
    }
    ctx.add('tools', t);
  }
}

/** Manifest `tools` declarations from tool specs (ownership + schema, compile-time). */
export function declareTools(specs) {
  return specs.map((t) => ({
    name: t.name,
    ...(t.parameters && Object.keys(t.parameters).length ? { parameters: t.parameters } : {}),
    ...(t.required?.length ? { required: t.required } : {}),
    ...(t.tier ? { tier: t.tier } : {}),
  }));
}

/**
 * The consume entry that makes a plugin the compiled owner of these tools: the
 * kernel routes each `tool-call` to exactly the plugin whose keyed values
 * contain its name (a value ending in `*` owns a family, e.g. `mcp__github__*`).
 */
export function ownTools(names, extra = {}) {
  return { event: 'tool-call', demands: ['call_id', 'name', 'args'], mode: 'async', context: false, keyed: { key: 'name', values: [...names] }, ...extra };
}
