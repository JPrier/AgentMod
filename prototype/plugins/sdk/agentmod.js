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
import { argsKey } from './toolschema.js';

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

/** The input a session was started with (`session-started`'s `input`), from a session view. */
export function startInputOf(view) {
  return (view?.events || []).find((e) => e.event_name === 'session-started')?.payload?.input || null;
}

/** The `receives` key for a `session-started` subscriber that reads the start input. */
export const START_INPUT = { input: { shape: 'object', optional: true, keys: { text: { shape: 'string', optional: true }, attachments: { shape: 'array<object>', optional: true }, data: { shape: 'object', optional: true } } } };

/** Read the context items of one section (values only). */
export function section(context, name) {
  return (context || []).filter((c) => c.section === name).map((c) => c.value);
}

// ---------------------------------------------------------------------------
// Manifest helpers: shapes (compiler.md §3.1).
// ---------------------------------------------------------------------------

/** A required key of a shape (`string`, `array<object>`, `"ok" | "error"`, …). */
export const key = (shape, extra = {}) => ({ shape, ...extra });
/** An optional key. */
export const optional = (shape, extra = {}) => ({ shape, optional: true, ...extra });

/** The default `context` slot, requested like a key (`[subscribe.slots]`). */
export const CONTEXT = { context: key('array<object>') };

/**
 * A subscription that receives `keys` and reads the context slot: spread it
 * into the subscription (`{ event, mode, ...withContext({ text: key('string') }) }`).
 * Slots are a sibling of `receives` (compiler.md §3.2).
 */
export const withContext = (keys = {}) => ({ receives: keys, slots: CONTEXT });

/** Keys every tool call carries (`tool-call/<name>`, emitted by the loop plugin). */
export const TOOL_CALL_KEYS = {
  call_id: key('string'),
  name: key('string'),
  args: key('object'),
  approved: optional('object'),
};

/** Keys every tool result carries; tools may add more (they pass untyped). */
export const TOOL_RESULT_KEYS = {
  call_id: key('string'),
  name: key('string'),
  output: key('string'),
  error: optional('bool'),
};

/** Keys of a provider's canonical `model-response`. */
export const MODEL_RESPONSE_KEYS = {
  text: key('string'),
  stream_id: key('string'),
  tool_calls: optional('array<object>'),
  model: optional('string'),
  provider: optional('string'),
  usage: optional('object'),
  metrics: optional('object'),
};

/** The `tool-result` emit declaration (extra keys may be declared in `more`). */
export const toolResult = (more = {}) => ({ event: 'tool-result', keys: { ...TOOL_RESULT_KEYS, ...more } });

/** Hints a tool spec carries for the loop, discovery, and policy plugins. */
const HINTS = ['tier', 'group', 'effects', 'trust', 'intents'];

/**
 * The subscriptions that make these tools exist (compiler.md §3.4): one
 * concrete `tool-call/<name>` member per tool spec, async. The member's
 * `receives.args` is the tool's argument schema (as a shape, from the spec's
 * parameters), its `description` the tool's description, and its `hints` the
 * spec's tier, group, effects, trust, and intents. The compiler resolves the
 * loop plugin's `tool-call/*` family into exactly these members, and the model
 * is offered exactly them. Several plugins may subscribe to one member: all run.
 */
export function toolCalls(specs, extra = {}) {
  const { receives = {}, ...rest } = extra;
  return [...specs].map((spec) => {
    const hints = Object.fromEntries(HINTS.filter((k) => spec[k] !== undefined).map((k) => [k, spec[k]]));
    return {
      event: `tool-call/${spec.name}`,
      mode: 'async',
      ...(spec.description ? { description: spec.description } : {}),
      ...(Object.keys(hints).length ? { hints } : {}),
      receives: { ...TOOL_CALL_KEYS, ...receives, args: argsKey(spec) },
      ...rest,
    };
  });
}

/** Does a pattern (`tool-call/*`, `tool-call/mcp__x__*`, exact) match a name? */
export function patternMatches(pattern, name) {
  if (pattern === '*') return true;
  if (pattern.endsWith('*')) return name.startsWith(pattern.slice(0, -1)) && name.length > pattern.length - 1;
  return pattern === name;
}

/**
 * Define and start a plugin.
 *
 * @param {object} spec
 * @param {object|Function} spec.manifest  name, version, subscribe, emit, capabilities, outbound, sink;
 *   or (config) => manifest (may be async: an MCP bridge connects before it can declare its tools)
 * @param {Record<string, Function>} spec.handlers  event name, family pattern (`tool-call/*`), or '*' -> async (ctx) => result
 * @param {Function} [spec.validate] async (config, {env}) => void; throw to refuse the handshake
 * @param {Function} [spec.init]   async (plugin) => void, after the handshake
 * @param {Function} [spec.onRecord] (record) => void, for plugins that `watch`
 * @param {Function} [spec.onStream] (messages) => void, live stream messages for watchers
 * @param {Function} [spec.onEmitSet] (revision, emits) => void, when the runtime sends a resolved emit set
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
    // Revision → this plugin's resolved emit set under it (compiler.md §3.4):
    // the concrete events it may emit, sent by the runtime after the handshake
    // and before its first invocation under any new revision.
    this.emitSets = new Map();
    this.latestRevision = null;
  }

  /** The manifest for a config (a manifest may depend on it, e.g. declared tools). */
  async manifestFor(config) {
    return typeof this.spec.manifest === 'function' ? await this.spec.manifest(config || {}) : this.spec.manifest;
  }

  /** The resolved emit set under a revision (the latest one received if omitted). */
  emitsFor(revision) {
    return this.emitSets.get(revision ?? this.latestRevision) || [];
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
      /**
       * `start-session`, a runtime call (compiler.md §9): a definition (by
       * name, or a `spec` written now) and an initial `input` (`{ text,
       * attachments, data }`). The runtime compiles the definition, creates
       * the session, and emits `session-started` carrying the input; `ui` is
       * an optional hint for that event. A definition that fails to compile
       * creates no session and rejects with `error.data.diagnostics`.
       */
      startSession: ({ definition, spec, input, ui, invocationId, forkFrom }) =>
        this.request('start_session', { definition, spec, input, ui, invocation_id: invocationId, fork_from: forkFrom }),
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
      if (msg.error) p.reject(Object.assign(new Error(msg.error.message), { code: msg.error.code, data: msg.error.data }));
      else p.resolve(msg.result);
      return;
    }
    const { id, method, params } = msg;
    try {
      switch (method) {
        case 'initialize': {
          this.config = params.config || {};
          this.instance = params.plugin;
          this.manifest = await this.manifestFor(this.config);
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
        case 'emit_set': {
          this.emitSets.set(params.revision, params.emits || []);
          this.latestRevision = params.revision;
          this.spec.onEmitSet?.(params.revision, params.emits || [], this);
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

  /** The handler for an event: exact name, the subscription's pattern, a matching family, or '*'. */
  handlerFor(name, subscription) {
    const hs = this.spec.handlers || {};
    if (hs[name]) return hs[name];
    if (subscription && hs[subscription]) return hs[subscription];
    const fam = Object.keys(hs).filter((k) => k !== '*' && k.endsWith('*') && patternMatches(k, name)).sort((a, b) => b.length - a.length)[0];
    return fam ? hs[fam] : hs['*'];
  }

  async invoke(req) {
    const ev = req.event;
    const handler = this.handlerFor(ev.event_name, req.subscription);
    const result = { contributions: [] };
    if (!handler) {
      this.log(`no handler for \`${ev.event_name}\` (subscription \`${req.subscription}\`)`);
      return {};
    }
    const abort = new AbortController();
    this.aborts.set(req.invocation_id, abort);
    const plugin = this;
    const slots = ev.slots;
    const noContext = () => {
      throw new Error(`\`${this.name}\` read the context on \`${ev.event_name}\`, but its subscription (${req.subscription || ev.event_name}) does not request the \`context\` slot (slots.context)`);
    };
    const ctx = {
      plugin,
      event: ev,
      payload: ev.payload,
      /** The `context` slot (only when the subscription requested it). */
      get context() { return slots && 'context' in slots ? slots.context || [] : noContext(); },
      slots: slots || {},
      subscription: req.subscription,
      sessionId: ev.session_id,
      invocationId: req.invocation_id,
      /** The revision this invocation runs under. */
      revision: req.revision,
      /** This plugin's resolved emit set under that revision: the concrete events it may emit. */
      get emits() { return plugin.emitsFor(req.revision); },
      /** May this plugin emit `name` under this invocation's revision? */
      mayEmit: (name) => plugin.emitsFor(req.revision).some((e) => e.event === name),
      attempt: req.attempt,
      mode: req.mode,
      signal: abort.signal,
      config: this.config,
      storage: this.storage,
      host: this.host,
      log: (...a) => this.log(...a),
      sleep: (ms) => sleep(ms, abort.signal),
      /** The values of one context section (`messages`, `tools`, …). */
      section: (name) => section(ctx.context, name),
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
      add: (name, value) => result.contributions.push({ op: 'add', section: name, value }),
      replace: (id, value) => result.contributions.push({ op: 'replace', id, value }),
      remove: (id) => result.contributions.push({ op: 'remove', id }),
      clearSection: (name) => result.contributions.push({ op: 'clear-section', section: name }),
      restore: (to_sequence) => result.contributions.push({ op: 'restore', to_sequence }),
      contribute: (op) => result.contributions.push(op),
      veto: (reason) => { result.veto = reason; },
      /** Blocking subscribers: hand keys back to the chain (declared in `returns`). */
      returns: (keys) => { result.returns = { ...(result.returns || {}), ...keys }; },
      /** A provider stream for this invocation (see sdk/stream.js). */
      stream: (opts) => createStreamWriter(ctx, opts),
    };
    try {
      const ret = await handler(ctx);
      if (ret && typeof ret === 'object') {
        if (ret.contributions) result.contributions.push(...ret.contributions);
        if (ret.returns) result.returns = { ...(result.returns || {}), ...ret.returns };
        if (ret.veto) result.veto = ret.veto;
      }
      if (!result.contributions.length) delete result.contributions;
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
 * A tool, as its plugin writes it: name, description, JSON-Schema-style
 * parameters (`required` lists required ones). `toolCalls` turns specs into
 * the member subscriptions that make the tools exist; the loop plugin offers
 * the model the tools its `tool-call/*` family resolves to (sdk/toolschema.js).
 *
 * `opts.tier` is `core` (sent to the model every turn) or `deferred` (found
 * through tool_search; see sdk/projection.js). `group` names a capability
 * family for discovery and policy, and `effects` is read | write | varies.
 * These travel as the subscription's `hints`.
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
