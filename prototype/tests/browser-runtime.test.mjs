// The in-browser runtime's durable storage and write-ahead ordering, under Node:
// IndexedDB through fake-indexeddb, the host module with a stub kernel package.
// (The real WASM kernel in a real browser is covered by tests/browser.mjs.)
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { stageSite } from './stage.mjs';

let idb = null;
try {
  idb = (await import('fake-indexeddb')).indexedDB;
} catch { /* run `npm install` in tests/ */ }

/** Stage the site with a stub kernel package so browser-host.js can load under Node. */
async function hostModule() {
  const site = stageSite();
  const pkg = path.join(site, 'pkg');
  fs.mkdirSync(pkg, { recursive: true });
  fs.writeFileSync(path.join(pkg, 'agentmod_wasm.js'), 'export default async function init() {}\nexport class WasmKernel {}\nexport class WasmStreamHub {}\nexport const compile = () => "{}", project = () => "{}", context_at = () => "[]", replay_recovery = () => "null", session_metrics = () => "{}";\n');
  globalThis.location ??= { href: 'http://localhost/' };
  return import(pathToFileURL(path.join(site, 'runtime', 'browser-host.js')).href);
}

const rec = (sid, seq, extra = {}) => ({ session_id: sid, sequence: seq, at: seq, type: 'event-appended', event: { event_id: `${sid}/e${seq}` }, ...extra });

test('IndexedDB store: append, load in order, compilations, clear', { skip: !idb && 'fake-indexeddb not installed' }, async () => {
  const { openStore } = await import(pathToFileURL(path.join(stageSite(), 'runtime', 'persist.js')).href);
  const store = await openStore({ idb, name: `t-${Date.now()}` });
  assert.ok(store);
  await store.appendRecords([rec('s0002', 1), rec('s0001', 2), rec('s0001', 1)]);
  await store.appendRecords([rec('s0001', 3)]);
  await store.putCompilation({ hash: 'abc', ok: true });
  const { sessions, compilations } = await store.loadAll();
  assert.deepEqual([...sessions.keys()].sort(), ['s0001', 's0002']);
  assert.deepEqual(sessions.get('s0001').map((r) => r.sequence), [1, 2, 3]);
  assert.deepEqual(compilations.map((c) => c.hash), ['abc']);
  // A record is keyed by (session, sequence): rewriting the same key is idempotent.
  await store.appendRecords([rec('s0001', 3)]);
  assert.equal((await store.loadAll()).sessions.get('s0001').length, 3);
  await store.clear();
  assert.equal((await store.loadAll()).sessions.size, 0);
  assert.equal(await openStore({ idb: undefined }), null, 'no IndexedDB: memory only');
});

test('one writer per store: a second tab gets no store and a reason', { skip: !idb && 'fake-indexeddb not installed' }, async () => {
  const { openStore } = await import(pathToFileURL(path.join(stageSite(), 'runtime', 'persist.js')).href);
  // Minimal Web Locks: exclusive, ifAvailable, held while the callback's promise is pending.
  const held = new Set();
  const locks = {
    async request(name, opts, cb) {
      if (held.has(name)) return cb(null);
      held.add(name);
      return cb({ name });
    },
  };
  const name = `lock-${Date.now()}`;
  const first = await openStore({ idb, name, locks });
  assert.ok(first && !first.blocked && first.appendRecords, 'first tab writes');
  const second = await openStore({ idb, name, locks });
  assert.match(second.blocked, /another tab/);
});

test('write-ahead: dispatch and listeners only after the batch is durable, in order', { skip: !idb && 'fake-indexeddb not installed' }, async () => {
  const { BrowserRuntime } = await hostModule();
  const order = [];
  let release;
  const gate = new Promise((r) => { release = r; });
  const store = {
    appendRecords: async (records) => {
      order.push(`write ${records.map((r) => r.sequence).join(',')}`);
      if (records[0]?.sequence === 1) await gate;
      order.push(`durable ${records.map((r) => r.sequence).join(',')}`);
    },
  };
  const rt = new BrowserRuntime({ base: new URL('http://localhost/'), log: () => {}, store });
  rt.dispatch = (fx) => order.push(`dispatch ${fx.request.invocation_id}`);
  rt.onRecord((r) => order.push(`notify ${r.sequence}`));
  rt.execute([{ type: 'append', record: rec('s0001', 1) }, { type: 'invoke', request: { invocation_id: 's0001/i1' } }]);
  rt.execute([{ type: 'append', record: rec('s0001', 2) }, { type: 'invoke', request: { invocation_id: 's0001/i2' } }]);
  // In memory at once (queries see it), but nothing acted on yet.
  assert.equal(rt.records.get('s0001').length, 2);
  await new Promise((r) => setTimeout(r, 20));
  assert.deepEqual(order, ['write 1']);
  release();
  await rt.flushed();
  assert.deepEqual(order, ['write 1', 'durable 1', 'dispatch s0001/i1', 'notify 1', 'write 2', 'durable 2', 'dispatch s0001/i2', 'notify 2']);
  // A failed write stops dispatch and is recorded in the runtime journal.
  const bad = new BrowserRuntime({ base: new URL('http://localhost/'), log: () => {}, store: { appendRecords: async () => { throw new Error('quota'); } } });
  let dispatched = false;
  bad.dispatch = () => { dispatched = true; };
  bad.execute([{ type: 'append', record: rec('s0001', 1) }, { type: 'invoke', request: { invocation_id: 'x' } }]);
  await bad.flushed();
  assert.equal(dispatched, false);
  assert.equal(bad.journal.pop().kind, 'persist-failed');
});

test('exported logs omit every declared secret', async () => {
  const { redactConfig } = await hostModule();
  const c = redactConfig({
    manifests: { p: { settings: [{ key: 'token', secret: true }, { key: 'model' }] } },
    config: { plugins: { p: { config: { token: 't0k', model: 'm', api_key: 'k' } }, q: { config: { secrets: { GH: { value: 'ghp_x', env: 'GH' } } } } } },
  });
  assert.deepEqual(c.config.plugins.p.config, { token: '(omitted)', model: 'm', api_key: '(omitted)' });
  assert.equal(c.config.plugins.q.config.secrets.GH.value, '(omitted)');
});

test('the harness view summarizes a session from its log', async () => {
  const { harnessSummary } = await import(pathToFileURL(path.join(stageSite(), 'runtime', 'harness-view.js')).href);
  const view = { events: [
    { event_name: 'model-response', payload: { model: 'm1', provider: 'p', metrics: { input_tokens: 1000, output_tokens: 50, cached_tokens: 800, cost: 0.01, context_tokens: 3000, tool_schema_tokens: 900, tools_sent: 10, tools_deferred: 9 } } },
    { event_name: 'workspace-info', payload: { root: '/w', mode: 'primary' } },
    { event_name: 'plan-updated', payload: { items: [{ text: 'a', status: 'completed' }] } },
    { event_name: 'tool-call', payload: { call_id: 'c1', name: 'shell' } },
    { event_name: 'tool-result', payload: { call_id: 'c1', error: true } },
    { event_name: 'tool-call', payload: { call_id: 'c2', name: 'process' } },
    { event_id: 'e7', event_name: 'process-started', payload: { process_id: 'p1', name: 'srv' } },
    { event_id: 'e8', sequence: 8, event_name: 'checkpoint-created', payload: { checkpoint: 'abc', reason: 'before x' } },
    { event_name: 'subagent-started', payload: { child: 's0009', call_id: 'c3', workspace: 'isolated' } },
  ] };
  const x = harnessSummary(view, [{ session_id: 's0009', activity: 'running' }]);
  assert.equal(x.usage.cached_tokens, 800);
  assert.equal(x.context.tools_sent, 10);
  assert.equal(x.tool_calls, 2);
  assert.equal(x.tool_errors, 1);
  assert.deepEqual(x.pending_calls, ['process']);
  assert.deepEqual(x.processes.map((p) => p.state), ['running']);
  assert.equal(x.checkpoints[0].checkpoint, 'abc');
  assert.equal(x.children[0].activity, 'running');
});

test('stream recovery ops: segments append, snapshots compact, discard removes', { skip: !idb && 'fake-indexeddb not installed' }, async () => {
  const { openStore } = await import(pathToFileURL(path.join(stageSite(), 'runtime', 'persist.js')).href);
  globalThis.IDBKeyRange ??= (await import('fake-indexeddb')).IDBKeyRange;
  const store = await openStore({ idb, name: `s-${Date.now()}` });
  const seg = (id, n) => ({ op: 'append', stream_id: id, session_id: 's1', segment: { seq: n } });
  await store.streamOps([seg('s1/i2', 1), seg('s1/i2', 2), seg('s1/i5', 1)]);
  await store.streamOps([{ op: 'snapshot', state: { stream_id: 's1/i2', seq: 2 } }, seg('s1/i2', 3)]);
  let streams = await store.loadStreams();
  const a = streams.find((ops) => (ops[0].stream_id ?? ops[0].state?.stream_id) === 's1/i2');
  assert.deepEqual(a.map((o) => o.op), ['snapshot', 'append'], 'compaction replaced the earlier segments');
  await store.streamOps([{ op: 'discard', stream_id: 's1/i2' }]);
  streams = await store.loadStreams();
  assert.equal(streams.length, 1);
  assert.equal(streams[0][0].stream_id, 's1/i5');
});

test('browser host: provider frames go to the hub, never the log; streams end with their invocation', async () => {
  const { BrowserRuntime } = await hostModule();
  const rt = new BrowserRuntime({ base: new URL('http://localhost/'), log: () => {}, persistence: false });
  const calls = [];
  const ops = [];
  rt.store = { streamOps: async (o) => { ops.push(...o); } };
  rt.kernel = { invocation: (inv) => JSON.stringify(inv === 's1/i3' ? ['model', true] : ['model', false]) };
  let open = true;
  rt.hub = {
    ingest: (id, frames, now) => { calls.push(['ingest', id, JSON.parse(frames).length]); return JSON.stringify({ result: { seq: 2 }, output: { recovery: [{ op: 'append', stream_id: id }], ready: [7] } }); },
    drain: (c) => JSON.stringify(c === 7 ? [{ type: 'frame', stream_id: 's1/i3', seq: 2 }] : []),
    next_deadline: () => -1,
    is_open: () => open,
    finalize: (id, outcome) => { calls.push(['finalize', id, outcome]); open = false; return JSON.stringify({ recovery: [{ op: 'discard', stream_id: id }], ready: [] }); },
    attach: () => JSON.stringify({ client: 7, streams: [] }),
    detach: () => {},
  };
  const got = [];
  rt.onStream((m) => got.push(...m));
  const r = rt.streamAs('model', { invocation_id: 's1/i3', frames: [{ type: 'open', attempt: '1.1' }, { type: 'text-delta', attempt: '1.1', block: 0, text: 'hi' }] });
  assert.equal(r.seq, 2);
  assert.deepEqual(got, [{ type: 'frame', stream_id: 's1/i3', seq: 2 }]);
  assert.throws(() => rt.streamAs('model', { invocation_id: 's1/i9', frames: [] }), /not an open invocation/);
  assert.throws(() => rt.streamAs('other', { invocation_id: 's1/i3', frames: [] }), /not an open invocation/);
  // No record was appended for any of it.
  assert.equal(rt.records.size, 0);
  rt.streamRecords([{ type: 'invocation-completed', invocation_id: 's1/i3', outcome: { status: 'ok' } }]);
  assert.deepEqual(calls.at(-1), ['finalize', 's1/i3', 'complete']);
  await rt.streamTail;
  assert.deepEqual(ops.map((o) => o.op), ['append', 'discard']);
});
