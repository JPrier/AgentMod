// Shared provider logic for OpenAI-compatible /chat/completions endpoints
// (OpenAI, OpenRouter, Ollama, vLLM, LM Studio). The request is built from the
// assembled context by the shared projection (sdk/projection.js); this module
// owns only the wire format, streaming, retries, and usage accounting.

import { project, toOpenAI } from './projection.js';
import { sleep } from './agentmod.js';

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

/**
 * Stream one completion; publishes stream-chunks and returns the
 * model-response payload (text, tool_calls, usage, metrics).
 */
export async function complete(ctx, { baseUrl, key, model, headers = {}, temperature = 0.3, extra = {}, projection = {}, cacheSystem = false, retries = 3 }) {
  const p = project(ctx.context, projection.minimal ? { preamble: 'You are a helpful assistant with tools.', maxContextTokens: Number.MAX_SAFE_INTEGER, defaultTier: 'core' } : projection);
  const { messages, tools } = toOpenAI(p, { cacheSystem });
  const body = { model, messages, stream: true, temperature, ...extra };
  if (tools.length) body.tools = tools;
  const t0 = Date.now();
  const retryLog = [];
  const { res, retries: retried } = await post(`${baseUrl.replace(/\/$/, '')}/chat/completions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...(key ? { authorization: `Bearer ${key}` } : {}), ...headers },
    body: JSON.stringify(body),
  }, { signal: ctx.signal, retries, onRetry: (r) => retryLog.push(r) });
  if (!res.ok) throw new Error(`provider HTTP ${res.status}${retried ? ` after ${retried} retries` : ''}: ${(await res.text()).slice(0, 400)}`);
  const streamId = ctx.invocationId;
  const reader = res.body.getReader();
  const dec = new TextDecoder();
  let buf = '';
  let text = '';
  let usage = null;
  let served = model;
  let firstToken = null;
  let finish = null;
  const calls = {};
  let index = 0;
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    let nl;
    while ((nl = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, nl).trim();
      buf = buf.slice(nl + 1);
      if (!line.startsWith('data:')) continue;
      const data = line.slice(5).trim();
      if (data === '[DONE]') continue;
      let j;
      try { j = JSON.parse(data); } catch { continue; }
      if (j.error) throw new Error(`provider error: ${j.error.message || JSON.stringify(j.error)}`);
      if (j.model) served = j.model;
      if (j.usage) usage = j.usage;
      const choice = j.choices?.[0] || {};
      if (choice.finish_reason) finish = choice.finish_reason;
      const delta = choice.delta || {};
      if (delta.content) {
        firstToken ??= Date.now();
        text += delta.content;
        await ctx.publish('stream-chunk', { stream_id: streamId, text: delta.content, index: index++ }, { ui: { v: 1, kind: 'stream-chunk', stream_id: streamId, text: delta.content } });
      }
      for (const tc of delta.tool_calls || []) {
        firstToken ??= Date.now();
        const c = (calls[tc.index ?? 0] ||= { call_id: '', name: '', args: '' });
        if (tc.id) c.call_id = tc.id;
        if (tc.function?.name) c.name += tc.function.name;
        if (tc.function?.arguments) c.args += tc.function.arguments;
      }
    }
  }
  const tool_calls = Object.values(calls).map((c, i) => {
    let args = {};
    let parse_error;
    try { args = c.args ? JSON.parse(c.args) : {}; } catch (e) { args = {}; parse_error = `the arguments were not valid JSON (${e.message}): ${c.args.slice(0, 200)}`; }
    return { call_id: c.call_id || `${ctx.invocationId}:${i}`, name: c.name, args, ...(parse_error ? { parse_error } : {}) };
  });
  const cached = usage?.prompt_tokens_details?.cached_tokens ?? usage?.cache_read_input_tokens ?? null;
  const metrics = {
    ...p.metrics,
    latency_ms: Date.now() - t0,
    ttft_ms: firstToken ? firstToken - t0 : null,
    input_tokens: usage?.prompt_tokens ?? null,
    output_tokens: usage?.completion_tokens ?? null,
    cached_tokens: cached,
    reasoning_tokens: usage?.completion_tokens_details?.reasoning_tokens ?? null,
    cost: typeof usage?.cost === 'number' ? usage.cost : null,
    retries: retried,
    retry_log: retryLog.length ? retryLog : undefined,
    finish_reason: finish,
  };
  return { text, tool_calls, stream_id: streamId, model: served, usage, metrics };
}
