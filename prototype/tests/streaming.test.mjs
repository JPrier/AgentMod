// The provider stream path, plugin and client side: SSE parsing under any byte
// split, normalized frames, attempts (retry / fallback never concatenate),
// usage accounting, cancellation, tool-argument streaming, and the client
// store's hydration barrier, resync, and render scheduling.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const mod = (p) => import(pathToFileURL(path.join(here, '..', p)).href);
const { SseParser } = await mod('plugins/sdk/sse.js');
const { UsageAccumulator, normalizeUsage, createStreamWriter } = await mod('plugins/sdk/stream.js');
const { complete } = await mod('plugins/sdk/openai-compat.js');
const { StreamStore, renderScheduler } = await mod('ui/runtime/stream-store.js');

const enc = new TextEncoder();

// ---------------------------------------------------------------------------
// SSE parsing
// ---------------------------------------------------------------------------

test('SSE: every byte split, UTF-8 across chunks, CRLF / LF / CR, comments, multi-line data', () => {
  const text = ': OPENROUTER PROCESSING\r\n\r\ndata: {"a":"héllo – 世界 🎉"}\r\n\r\ndata: line1\ndata: line2\n\nevent: x\rdata: cr-only\r\rdata: [DONE]\n\n';
  const bytes = enc.encode(text);
  const parse = (chunks) => {
    const p = new SseParser();
    const out = [];
    for (const c of chunks) out.push(...p.push(c));
    out.push(...p.end());
    return out.map((e) => `${e.event}|${e.data}`);
  };
  const want = parse([bytes]);
  assert.deepEqual(want, ['message|{"a":"héllo – 世界 🎉"}', 'message|line1\nline2', 'x|cr-only', 'message|[DONE]']);
  // Every two-way split, and byte-at-a-time.
  for (let i = 1; i < bytes.length; i++) assert.deepEqual(parse([bytes.slice(0, i), bytes.slice(i)]), want, `split at ${i}`);
  assert.deepEqual(parse([...bytes].map((b) => new Uint8Array([b]))), want);
});

test('SSE: a malformed final frame is reported as partial, and events are bounded', () => {
  const p = new SseParser();
  assert.deepEqual(p.push(enc.encode('data: {"ok":1}\n\ndata: {"trunc')), [{ event: 'message', data: '{"ok":1}', id: null }]);
  const tail = p.end();
  assert.equal(tail.length, 1);
  assert.equal(tail[0].partial, true);
  const small = new SseParser({ maxEventBytes: 100 });
  assert.throws(() => small.push(enc.encode(`data: ${'x'.repeat(200)}`)), /exceeds/);
});

// ---------------------------------------------------------------------------
// Usage
// ---------------------------------------------------------------------------

test('usage: cumulative reports are not summed, deltas are, attempts are billed', () => {
  const u = new UsageAccumulator();
  u.begin();
  u.report({ prompt_tokens: 100, completion_tokens: 5 }, 'cumulative');
  u.report({ prompt_tokens: 100, completion_tokens: 20, prompt_tokens_details: { cached_tokens: 80 }, cost: 0.01 }, 'cumulative');
  assert.deepEqual(u.last, { input_tokens: 100, output_tokens: 20, cached_tokens: 80, reasoning_tokens: null, cost: 0.01 });
  u.begin();
  u.report({ input_tokens: 100, output_tokens: 3 }, 'delta');
  u.report({ output_tokens: 4 }, 'delta');
  assert.equal(u.last.output_tokens, 7);
  assert.equal(u.total.input_tokens, 200);
  assert.equal(u.total.output_tokens, 27);
  assert.equal(normalizeUsage({ input_tokens: 1, cache_read_input_tokens: 2 }).cached_tokens, 2);
});

// ---------------------------------------------------------------------------
// Provider adapter against a scripted endpoint
// ---------------------------------------------------------------------------

/** A fake plugin link capturing what the writer sends to the host. */
function fakeCtx({ abort } = {}) {
  const sent = [];
  const plugin = {
    notify: (method, params) => sent.push({ method, params, kind: 'notify' }),
    request: async (method, params) => {
      sent.push({ method, params, kind: 'request' });
      return { seq: 1, stats: { live_frames: 3, late_frames: 0 } };
    },
  };
  const ctrl = new AbortController();
  if (abort) abort(ctrl);
  const ctx = { context: [{ slot: 'messages', value: { role: 'user', content: 'hi' } }], signal: ctrl.signal, invocationId: 's0001/i4', attempt: 1, plugin };
  ctx.stream = (o) => createStreamWriter(ctx, o);
  ctx.frames = () => sent.flatMap((s) => s.params.frames);
  ctx.sent = sent;
  return ctx;
}

/** Each scripted response: { status?, chunks: [string|Error] }. */
function fakeFetch(responses) {
  const calls = [];
  globalThis.fetch = async (url, init) => {
    const body = JSON.parse(init.body);
    calls.push(body.model);
    const r = responses.shift();
    if (!r) throw new Error('no scripted response');
    const chunks = [...r.chunks];
    const stream = new ReadableStream({
      async pull(c) {
        await new Promise((res) => setTimeout(res, 1));
        if (init.signal?.aborted) return c.error(new Error('aborted'));
        const next = chunks.shift();
        if (next === undefined) return c.close();
        if (next instanceof Error) return c.error(next);
        c.enqueue(enc.encode(next));
      },
    });
    return new Response(stream, { status: r.status || 200, headers: { 'content-type': 'text/event-stream' } });
  };
  return calls;
}
const sse = (o) => `data: ${JSON.stringify(o)}\n\n`;
const delta = (d, extra = {}) => sse({ model: 'm', choices: [{ delta: d, ...extra }] });
const opts = { baseUrl: 'http://x/api/v1', key: 'k', model: 'm', retries: 0 };

test('a completion streams normalized frames and yields one canonical response', async () => {
  fakeFetch([{ chunks: [delta({ content: 'Hel' }), delta({ content: 'lo' }), sse({ model: 'm', choices: [{ delta: {}, finish_reason: 'stop' }], usage: { prompt_tokens: 9, completion_tokens: 2, cost: 0.001 } }), 'data: [DONE]\n\n'] }]);
  const ctx = fakeCtx();
  const out = await complete(ctx, opts);
  assert.equal(out.text, 'Hello');
  assert.equal(out.stream_id, 's0001/i4');
  assert.equal(out.attempt_id, '1.1');
  assert.deepEqual(out.usage, { input_tokens: 9, output_tokens: 2, cached_tokens: null, reasoning_tokens: null, cost: 0.001 });
  assert.equal(out.metrics.stream.live_frames, 3, 'hub stats returned by the closing request');
  const types = ctx.frames().map((f) => f.type);
  assert.deepEqual(types, ['open', 'block-start', 'text-delta', 'text-delta', 'usage', 'block-end', 'complete']);
  assert.ok(ctx.frames().every((f) => f.attempt === '1.1'));
  // Batched onto the host link; only the close is a request.
  assert.equal(ctx.sent.filter((s) => s.kind === 'request').length, 1);
  assert.equal(out.metrics.stream.provider_events, 4);
});

test('tool arguments stream per block and parse only when complete', async () => {
  const tc = (index, extra) => delta({ tool_calls: [{ index, ...extra }] });
  fakeFetch([{ chunks: [
    tc(0, { id: 'c1', function: { name: 'read_file', arguments: '{"pa' } }),
    tc(1, { id: 'c2', function: { name: 'shell', arguments: '{"comm' } }),
    tc(0, { function: { arguments: 'th":"kilo.c"}' } }),
    tc(1, { function: { arguments: 'and":"make' } }), // never closed: invalid JSON
    sse({ model: 'm', choices: [{ delta: {}, finish_reason: 'tool_calls' }] }),
    'data: [DONE]\n\n',
  ] }]);
  const ctx = fakeCtx();
  const out = await complete(ctx, opts);
  assert.deepEqual(out.tool_calls[0], { call_id: 'c1', name: 'read_file', args: { path: 'kilo.c' } });
  assert.equal(out.tool_calls[1].name, 'shell');
  assert.match(out.tool_calls[1].parse_error, /not valid JSON/);
  assert.deepEqual(out.tool_calls[1].args, {}, 'partial arguments are never passed on');
  const starts = ctx.frames().filter((f) => f.type === 'block-start');
  assert.deepEqual(starts.map((s) => [s.kind, s.name]), [['tool-call', 'read_file'], ['tool-call', 'shell']]);
  assert.equal(ctx.frames().filter((f) => f.type === 'tool-args-delta').length, 4);
});

test('an error after HTTP 200 retries as a new attempt; output is never concatenated', async () => {
  const calls = fakeFetch([
    { chunks: [delta({ content: 'partial ' }), sse({ error: { message: 'upstream reset' } })] },
    { chunks: [delta({ content: 'complete answer' }), sse({ model: 'm', choices: [{ delta: {}, finish_reason: 'stop' }], usage: { prompt_tokens: 10, completion_tokens: 3 } }), 'data: [DONE]\n\n'] },
  ]);
  const ctx = fakeCtx();
  const out = await complete(ctx, { ...opts, streamRetries: 1 });
  assert.equal(out.text, 'complete answer');
  assert.equal(out.attempt_id, '1.2');
  assert.deepEqual(calls, ['m', 'm']);
  const opens = ctx.frames().filter((f) => f.type === 'open');
  assert.deepEqual(opens.map((o) => [o.attempt, o.reason]), [['1.1', undefined], ['1.2', 'retry']]);
  assert.ok(ctx.frames().some((f) => f.type === 'error' && f.retryable && f.attempt === '1.1'));
  assert.equal(out.metrics.retries, 1);
});

test('a truncated stream or a malformed final frame is retried; then fallback models', async () => {
  const calls = fakeFetch([
    { chunks: [delta({ content: 'cut off' })] }, // no finish, no [DONE]
    { chunks: [delta({ content: 'x' }), 'data: {"broken'] }, // malformed final frame
    { chunks: [delta({ content: 'from the fallback' }), sse({ model: 'fb-served', choices: [{ delta: {}, finish_reason: 'stop' }] }), 'data: [DONE]\n\n'] },
  ]);
  const ctx = fakeCtx();
  const out = await complete(ctx, { ...opts, streamRetries: 1, fallbackModels: ['fb'] });
  assert.deepEqual(calls, ['m', 'm', 'fb']);
  assert.equal(out.text, 'from the fallback');
  assert.equal(out.model, 'fb-served');
  assert.equal(out.requested_model, 'm');
  const opens = ctx.frames().filter((f) => f.type === 'open');
  assert.deepEqual(opens.map((o) => o.reason), [undefined, 'retry', 'fallback']);
  // The served model changed mid-attempt: a boundary frame.
  assert.ok(ctx.frames().some((f) => f.type === 'boundary' && f.model === 'fb-served'));
});

test('a failing provider closes the stream terminally and throws', async () => {
  fakeFetch([{ status: 400, chunks: ['{"error":"bad request"}'] }]);
  const ctx = fakeCtx();
  await assert.rejects(complete(ctx, opts), /HTTP 400/);
  const last = ctx.frames().at(-1);
  assert.equal(last.type, 'error');
  assert.equal(last.retryable, false);
  assert.equal(ctx.sent.at(-1).kind, 'request');
});

test('cancellation closes the stream as cancelled', async () => {
  let ctrl;
  fakeFetch([{ chunks: [delta({ content: 'a' }), delta({ content: 'b' }), delta({ content: 'c' }), delta({ content: 'd' })] }]);
  const ctx = fakeCtx({ abort: (c) => { ctrl = c; } });
  setTimeout(() => ctrl.abort(), 3);
  await assert.rejects(complete(ctx, opts));
  assert.equal(ctx.frames().at(-1).type, 'cancelled');
});

// ---------------------------------------------------------------------------
// Client store
// ---------------------------------------------------------------------------

const frame = (seq, f, extra = {}) => ({ type: 'frame', session_id: 's1', stream_id: 's1/i2', attempt: '1.1', seq, from_seq: seq, frame: f, ...extra });

test('client store: hydration barrier, duplicates, gaps, new attempts', () => {
  let resyncs = 0;
  const st = new StreamStore({ onResync: () => resyncs++ });
  st.beginHydrate();
  // Arrive while the snapshot request is in flight.
  st.apply(frame(3, { type: 'text-delta', block: 0, text: 'old' })); // already in the snapshot
  st.apply(frame(4, { type: 'text-delta', block: 0, text: ' new' }));
  st.hydrate([{ session_id: 's1', stream_id: 's1/i2', attempt: '1.1', seq: 3, status: 'open', blocks: [{ block: 0, kind: 'text', text: 'hi old', closed: false }] }]);
  const s = st.streams.get('s1/i2');
  assert.equal(s.blocks.get(0).text, 'hi old new');
  st.apply(frame(4, { type: 'text-delta', block: 0, text: ' dup' }));
  assert.equal(st.stats.duplicates, 2);
  st.apply(frame(9, { type: 'text-delta', block: 0, text: '?' }));
  assert.equal(resyncs, 1, 'a gap asks for a snapshot');
  // A retry: new attempt replaces the partial text.
  st.apply({ ...frame(10, { type: 'open', model: 'm' }), attempt: '1.2' });
  st.apply({ ...frame(11, { type: 'block-start', block: 0, kind: 'text' }), attempt: '1.2' });
  st.apply({ ...frame(12, { type: 'text-delta', block: 0, text: 'fresh' }), attempt: '1.2' });
  st.apply(frame(13, { type: 'text-delta', block: 0, text: 'LATE' })); // old attempt
  assert.equal(st.streams.get('s1/i2').blocks.get(0).text, 'fresh');
  assert.equal(st.stats.late, 1);
  st.apply({ type: 'finalized', session_id: 's1', stream_id: 's1/i2', outcome: 'complete' });
  assert.equal(st.live('s1').length, 1, 'shown until its canonical events arrive');
  assert.equal(st.live('s1', { canonical: new Set(['s1/i2']) }).length, 0);
  st.apply({ type: 'resync-required' });
  assert.equal(resyncs, 2);
});

test('rendering is decoupled from ingestion: hidden tabs ingest, then render the latest once', () => {
  const listeners = {};
  const doc = { visibilityState: 'hidden', addEventListener: (k, f) => { listeners[k] = f; } };
  const frames = [];
  const win = { document: doc, requestAnimationFrame: (cb) => frames.push(cb) };
  let renders = 0;
  const st = new StreamStore();
  const schedule = renderScheduler(() => renders++, { win });
  st.onChange = schedule;
  st.applyAll([frame(1, { type: 'open' }), frame(2, { type: 'block-start', block: 0, kind: 'text' })]);
  for (let i = 3; i < 500; i++) st.applyAll([frame(i, { type: 'text-delta', block: 0, text: 'x' })]);
  assert.equal(frames.length, 0, 'no animation frames requested while hidden');
  assert.equal(st.streams.get('s1/i2').blocks.get(0).text.length, 497, 'but every message was applied');
  doc.visibilityState = 'visible';
  listeners.visibilitychange();
  assert.equal(renders, 1, 'one render of the latest state on becoming visible');
  for (let i = 500; i < 520; i++) st.applyAll([frame(i, { type: 'text-delta', block: 0, text: 'y' })]);
  assert.equal(frames.length, 1, 'at most one render per animation frame');
  frames.shift()();
  assert.equal(renders, 2);
});
