// Shared provider logic for OpenAI-compatible /chat/completions endpoints
// (OpenAI, OpenRouter, Ollama, vLLM, LM Studio). The request is built from the
// assembled context by the shared projection (sdk/projection.js); this module
// owns only the wire format, streaming, retries, and usage accounting.

import { project, toOpenAI } from './projection.js';
import { sleep } from './agentmod.js';
import { SseParser } from './sse.js';

/** Resolve an API key from config: `api_key` (browser) or `api_key_env` (native). */
export function resolveKey(cfg, fallbackEnv) {
  if (cfg.api_key) return cfg.api_key;
  const env = cfg.api_key_env || fallbackEnv;
  if (env && typeof process !== 'undefined' && process.env) return process.env[env];
  return undefined;
}

const RETRYABLE = new Set([408, 409, 425, 429, 500, 502, 503, 504, 529]);

/** POST with bounded retries for rate limits and transient failures (before any streaming). */
async function post(url, init, { signal, retries = 3, onRetry }) {
  let attempt = 0;
  for (;;) {
    let res;
    try {
      res = await fetch(url, { ...init, signal });
    } catch (e) {
      if (signal?.aborted || attempt >= retries) throw e;
      attempt++;
      onRetry?.({ attempt, reason: `network: ${e.message}` });
      await sleep(Math.min(8000, 500 * 2 ** attempt), signal);
      continue;
    }
    if (res.ok || !RETRYABLE.has(res.status) || attempt >= retries) return { res, retries: attempt };
    attempt++;
    const after = Number(res.headers.get('retry-after'));
    const wait = Number.isFinite(after) && after > 0 ? Math.min(30_000, after * 1000) : Math.min(8000, 500 * 2 ** attempt);
    onRetry?.({ attempt, reason: `HTTP ${res.status}`, wait_ms: wait });
    try { await res.body?.cancel(); } catch { /* ignore */ }
    await sleep(wait, signal);
  }
}

class ProviderError extends Error {
  constructor(message, { retryable = false, midStream = false, status } = {}) {
    super(message);
    this.retryable = retryable;
    this.midStream = midStream;
    this.status = status;
  }
}

/**
 * Stream one completion. Token deltas go to the host's stream hub as
 * normalized frames (sdk/stream.js) — never as events. Returns the canonical
 * model-response payload: assembled text, tool calls, model identity,
 * normalized usage (last attempt; `usage_total` sums every billed attempt),
 * and metrics including the stream's own counts.
 *
 * Failures: HTTP errors before streaming are retried by `post`; a stream that
 * fails after HTTP 200 (an error event, a dropped connection, a truncated or
 * malformed final frame) is retried as a *new attempt* (`stream_retries`,
 * default 1), then `fallback_models` are tried in order, each a new attempt.
 * A new attempt never continues the previous attempt's partial output.
 */
export async function complete(ctx, { baseUrl, key, model, fallbackModels = [], headers = {}, temperature = 0.3, extra = {}, projection = {}, cacheSystem = false, retries = 3, streamRetries = 1, provider = 'openai-compatible' }) {
  const p = project(ctx.context, projection.minimal ? { preamble: 'You are a helpful assistant with tools.', maxContextTokens: Number.MAX_SAFE_INTEGER, defaultTier: 'core' } : projection);
  const { messages, tools } = toOpenAI(p, { cacheSystem });
  const w = ctx.stream();
  const t0 = Date.now();
  const retryLog = [];
  let httpRetries = 0;
  let firstToken = null;
  const url = `${baseUrl.replace(/\/$/, '')}/chat/completions`;

  async function attempt(m, reason) {
    w.open({ model: m, provider, reason });
    const body = { model: m, messages, stream: true, temperature, ...extra };
    if (tools.length) body.tools = tools;
    const { res, retries: retried } = await post(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json', ...(key ? { authorization: `Bearer ${key}` } : {}), ...headers },
      body: JSON.stringify(body),
    }, { signal: ctx.signal, retries, onRetry: (r) => retryLog.push(r) });
    httpRetries += retried;
    if (!res.ok) throw new ProviderError(`provider HTTP ${res.status}${retried ? ` after ${retried} retries` : ''}: ${(await res.text()).slice(0, 400)}`, { status: res.status });
    const parser = new SseParser();
    const reader = res.body.getReader();
    let text = '';
    let served = m;
    let finish = null;
    let done = false;
    let textBlock = null;
    let reasoningBlock = null;
    const calls = new Map(); // provider index -> { call_id, name, args, block }
    const onEvent = (ev) => {
      w.stats.provider_events++;
      if (ev.data === '[DONE]') { done = true; return; }
      let j;
      try { j = JSON.parse(ev.data); } catch (e) {
        if (ev.partial) throw new ProviderError(`malformed final stream frame: ${ev.data.slice(0, 120)}`, { retryable: true, midStream: true });
        return; // a non-JSON keep-alive payload
      }
      if (j.error) throw new ProviderError(`provider error: ${j.error.message || JSON.stringify(j.error)}`, { retryable: true, midStream: true });
      if (j.model && j.model !== served) {
        if (served !== m || text || calls.size) w.boundary({ model: j.model });
        served = j.model;
      }
      if (j.usage) w.reportUsage(j.usage, 'cumulative');
      const choice = j.choices?.[0] || {};
      if (choice.finish_reason) finish = choice.finish_reason;
      const delta = choice.delta || {};
      const reasoning = delta.reasoning ?? delta.reasoning_content;
      if (reasoning) {
        reasoningBlock ??= w.blockStart('reasoning');
        w.reasoning(reasoningBlock, reasoning);
      }
      if (delta.content) {
        firstToken ??= Date.now();
        if (reasoningBlock != null && textBlock == null) w.blockEnd(reasoningBlock);
        textBlock ??= w.blockStart('text');
        text += delta.content;
        w.text(textBlock, delta.content);
      }
      for (const tc of delta.tool_calls || []) {
        firstToken ??= Date.now();
        const idx = tc.index ?? 0;
        let c = calls.get(idx);
        if (!c) {
          c = { call_id: tc.id || '', name: tc.function?.name || '', args: '' };
          c.block = w.blockStart('tool-call', { name: c.name || undefined, call_id: c.call_id || undefined });
          calls.set(idx, c);
        } else {
          if (tc.id) c.call_id = tc.id;
          if (tc.function?.name) c.name += tc.function.name;
        }
        if (tc.function?.arguments) {
          c.args += tc.function.arguments;
          w.toolArgs(c.block, tc.function.arguments);
        }
      }
    };
    try {
      for (;;) {
        let chunk;
        try {
          chunk = await reader.read();
        } catch (e) {
          if (ctx.signal.aborted) throw e;
          throw new ProviderError(`stream interrupted: ${e.message}`, { retryable: true, midStream: true });
        }
        if (chunk.done) break;
        w.stats.transport_chunks++;
        for (const ev of parser.push(chunk.value)) onEvent(ev);
        if (done) break;
      }
      for (const ev of parser.end()) onEvent(ev);
    } finally {
      try { reader.releaseLock(); } catch { /* released */ }
    }
    if (!done && !finish) throw new ProviderError('the stream ended before the provider finished (truncated response)', { retryable: true, midStream: true });
    for (const b of [textBlock, reasoningBlock]) if (b != null) w.blockEnd(b);
    for (const c of calls.values()) w.blockEnd(c.block);
    // Tool calls run only from here: complete blocks, parsed whole. Partial or
    // invalid JSON is reported to the model, never executed (chat-context).
    const tool_calls = [...calls.values()].map((c, i) => {
      let args = {};
      let parse_error;
      try { args = c.args ? JSON.parse(c.args) : {}; } catch (e) { args = {}; parse_error = `the arguments were not valid JSON (${e.message}): ${c.args.slice(0, 200)}`; }
      if (!parse_error && (args === null || typeof args !== 'object' || Array.isArray(args))) { args = {}; parse_error = 'the arguments must be a JSON object'; }
      return { call_id: c.call_id || `${ctx.invocationId}:${i}`, name: c.name, args, ...(parse_error ? { parse_error } : {}) };
    });
    return { text, tool_calls, served, finish };
  }

  const models = [model, ...fallbackModels.filter((x) => x && x !== model)];
  let lastErr = null;
  let reason;
  for (const [mi, m] of models.entries()) {
    for (let n = 0; n <= streamRetries; n++) {
      if (mi > 0 && n === 0) reason = 'fallback';
      try {
        const r = await attempt(m, reason);
        const hub = await w.close({ type: 'complete', ...(r.finish ? { finish_reason: r.finish } : {}) });
        const last = w.usage.last;
        const total = w.usage.total;
        const metrics = {
          ...p.metrics,
          latency_ms: Date.now() - t0,
          ttft_ms: firstToken ? firstToken - t0 : null,
          input_tokens: total?.input_tokens ?? null,
          output_tokens: total?.output_tokens ?? null,
          cached_tokens: total?.cached_tokens ?? null,
          reasoning_tokens: total?.reasoning_tokens ?? null,
          cost: total?.cost ?? null,
          retries: httpRetries + (w.stats.attempts - 1),
          retry_log: retryLog.length ? retryLog : undefined,
          finish_reason: r.finish,
          stream: { ...w.stats, ...(hub?.stats ? { live_frames: hub.stats.live_frames, late_frames: hub.stats.late_frames, duplicate_frames: hub.stats.duplicate_frames, missing_frames: hub.stats.missing_frames, recovery_writes: hub.stats.recovery_writes, recovery_bytes: hub.stats.recovery_bytes } : {}) },
        };
        return { text: r.text, tool_calls: r.tool_calls, stream_id: ctx.invocationId, attempt_id: w.attempt, model: r.served, ...(models.length > 1 ? { requested_model: model } : {}), usage: last, ...(w.stats.attempts > 1 ? { usage_total: total } : {}), metrics };
      } catch (e) {
        if (ctx.signal?.aborted) {
          await w.close({ type: 'cancelled', reason: 'cancelled' });
          throw e;
        }
        lastErr = e;
        retryLog.push({ attempt: w.attempt, reason: e.message.slice(0, 200), model: m });
        w.error(e.message, true);
        reason = 'retry';
        // HTTP failures were already retried; only mid-stream ones retry the model.
        if (!(e instanceof ProviderError) || !e.midStream) break;
      }
    }
  }
  await w.close({ type: 'error', message: lastErr?.message || 'provider failed', retryable: false });
  throw lastErr;
}
