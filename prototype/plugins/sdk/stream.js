// Provider streams, plugin side: normalized frames to the host's stream hub.
//
// Token deltas are not events. A provider adapter turns its wire format into
// typed frames (open, block start/end, text/reasoning/tool-argument deltas,
// usage, boundary, error, cancellation, completion) and hands them to a
// StreamWriter, which batches them onto the host's `stream` method. The host
// sequences, coalesces, fans them out to frontends, and keeps compact recovery
// state — all outside the event pipeline and the log. The adapter still
// publishes exactly one canonical `model-response` at the end.
//
// Identity: the stream is the invocation (`invocationId`); every attempt —
// first try, retry after a failure, provider/model fallback — gets a fresh
// attempt id `<invocation attempt>.<n>`, so a retry supersedes the partial
// output of the attempt before it instead of being concatenated to it.

/** Usage as providers report it, normalized. */
export function normalizeUsage(u) {
  if (!u || typeof u !== 'object') return null;
  const n = (v) => (typeof v === 'number' && Number.isFinite(v) ? v : null);
  return {
    input_tokens: n(u.prompt_tokens ?? u.input_tokens),
    output_tokens: n(u.completion_tokens ?? u.output_tokens),
    cached_tokens: n(u.prompt_tokens_details?.cached_tokens ?? u.cache_read_input_tokens ?? u.cached_tokens),
    reasoning_tokens: n(u.completion_tokens_details?.reasoning_tokens ?? u.reasoning_tokens),
    cost: n(u.cost),
  };
}

const KEYS = ['input_tokens', 'output_tokens', 'cached_tokens', 'reasoning_tokens', 'cost'];

/**
 * Per-attempt usage accounting. Providers report usage `final`-only,
 * `cumulative` (each report includes the earlier ones: take the latest, never
 * sum), or as `delta`s (sum them). Every attempt is billed, so the total sums
 * attempts; the response's `usage` is the last attempt's.
 */
export class UsageAccumulator {
  constructor() {
    this.attempts = [];
  }

  begin() {
    this.attempts.push(null);
  }

  report(raw, mode = 'cumulative') {
    const u = normalizeUsage(raw);
    if (!u) return;
    if (!this.attempts.length) this.begin();
    const i = this.attempts.length - 1;
    const cur = this.attempts[i];
    if (mode === 'delta' && cur) {
      this.attempts[i] = Object.fromEntries(KEYS.map((k) => [k, u[k] == null ? cur[k] : (cur[k] ?? 0) + u[k]]));
    } else {
      // cumulative and final: the latest report is authoritative for the attempt.
      this.attempts[i] = cur ? Object.fromEntries(KEYS.map((k) => [k, u[k] ?? cur[k]])) : u;
    }
  }

  /** The last attempt's usage. */
  get last() {
    return this.attempts.filter(Boolean).pop() || null;
  }

  /** Summed over every attempt (what was billed). */
  get total() {
    const done = this.attempts.filter(Boolean);
    if (!done.length) return null;
    return Object.fromEntries(KEYS.map((k) => {
      const xs = done.map((a) => a[k]).filter((v) => v != null);
      return [k, xs.length ? xs.reduce((a, b) => a + b, 0) : null];
    }));
  }
}

/**
 * Write normalized frames for one invocation.
 *
 * @param {object} ctx  the invocation context (needs plugin, invocationId, attempt)
 * @param {object} [o]
 * @param {number} [o.flushMs]       batch window for the host link (default 16)
 * @param {number} [o.maxBatchBytes] flush early past this (default 64 KiB)
 */
export function createStreamWriter(ctx, { flushMs = 16, maxBatchBytes = 64 * 1024 } = {}) {
  const plugin = ctx.plugin;
  const inv = ctx.invocationId;
  let n = 0;
  let attempt = null;
  let block = 0;
  let batch = [];
  let bytes = 0;
  let timer = null;
  let closed = false;
  const usage = new UsageAccumulator();
  const stats = { provider_events: 0, frames: 0, batches: 0, attempts: 0, transport_chunks: 0 };

  const send = (final) => {
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
    if (!batch.length && !final) return null;
    const frames = batch;
    batch = [];
    bytes = 0;
    stats.batches++;
    const params = { invocation_id: inv, frames };
    // Intermediate batches are notifications; the last is a request whose reply
    // carries the hub's statistics for the canonical response.
    if (final) return plugin.request('stream', params).catch(() => null);
    plugin.notify('stream', params);
    return null;
  };

  const push = (frame) => {
    if (closed) return;
    if (!attempt) throw new Error('stream: open() before sending frames');
    batch.push({ attempt, ...frame });
    stats.frames++;
    bytes += (frame.text?.length || 0) + 48;
    if (bytes >= maxBatchBytes) send(false);
    else if (!timer) timer = setTimeout(() => send(false), flushMs);
  };

  return {
    usage,
    stats,
    get attempt() {
      return attempt;
    },
    /** Start an attempt (the first, a retry, or a fallback). */
    open({ model, provider, reason } = {}) {
      n++;
      attempt = `${ctx.attempt ?? 1}.${n}`;
      block = 0;
      stats.attempts++;
      usage.begin();
      push({ type: 'open', ...(model ? { model } : {}), ...(provider ? { provider } : {}), ...(reason ? { reason } : {}) });
      return attempt;
    },
    blockStart(kind, { name, call_id } = {}) {
      const b = block++;
      push({ type: 'block-start', block: b, kind, ...(name ? { name } : {}), ...(call_id ? { call_id } : {}) });
      return b;
    },
    text: (b, text) => text && push({ type: 'text-delta', block: b, text }),
    reasoning: (b, text) => text && push({ type: 'reasoning-delta', block: b, text }),
    toolArgs: (b, text) => text && push({ type: 'tool-args-delta', block: b, text }),
    blockEnd: (b) => push({ type: 'block-end', block: b }),
    metadata: (data) => push({ type: 'metadata', data }),
    boundary: ({ model, provider }) => push({ type: 'boundary', ...(model ? { model } : {}), ...(provider ? { provider } : {}) }),
    /** Record provider usage (normalized here, per attempt). */
    reportUsage(raw, mode) {
      usage.report(raw, mode);
      const u = usage.attempts[usage.attempts.length - 1];
      if (u) push({ type: 'usage', usage: u });
    },
    error: (message, retryable) => push({ type: 'error', message: String(message).slice(0, 2000), retryable: !!retryable }),
    /** End the stream (completion, failure, or cancellation); resolves to the hub's stats. */
    async close(frame) {
      if (closed) return null;
      if (frame && attempt) push(frame);
      closed = true;
      return send(true);
    },
  };
}
