// Shared provider logic for OpenAI-compatible /chat/completions endpoints
// (OpenAI, OpenRouter, Ollama, vLLM, LM Studio). Builds the request entirely
// from the assembled context — `system`, `memory`, `messages`, `tools` slots
// contributed by other plugins — and streams the reply as `stream-chunk` events.

export function toMessages(context) {
  const out = [];
  const memory = context.filter((c) => c.slot === 'memory').map((c) => c.value);
  const system = context.filter((c) => c.slot === 'system').map((c) => c.value);
  const sys = [
    'You are an agent running on AgentMod, an event-bus runtime where every capability (including you) is a plugin. Use the provided tools when they help. Be concise.',
    ...system,
    ...(memory.length ? [`Remembered notes:\n- ${memory.join('\n- ')}`] : []),
  ];
  out.push({ role: 'system', content: sys.join('\n\n') });
  for (const { value: m } of context.filter((c) => c.slot === 'messages')) {
    if (m.role === 'assistant') {
      out.push({
        role: 'assistant',
        content: m.content || '',
        ...(m.tool_calls?.length ? { tool_calls: m.tool_calls.map((c) => ({ id: c.call_id, type: 'function', function: { name: c.name, arguments: JSON.stringify(c.args || {}) } })) } : {}),
      });
    } else if (m.role === 'tool') {
      out.push({ role: 'tool', tool_call_id: m.call_id, content: String(m.content) });
    } else {
      out.push({ role: 'user', content: String(m.content) });
    }
  }
  return out;
}

export function toTools(context) {
  return context
    .filter((c) => c.slot === 'tools')
    .map(({ value: t }) => ({
      type: 'function',
      function: { name: t.name, description: t.description, parameters: { type: 'object', properties: t.parameters || {} } },
    }));
}

/** Resolve an API key from config: `api_key` (browser) or `api_key_env` (native). */
export function resolveKey(cfg, fallbackEnv) {
  if (cfg.api_key) return cfg.api_key;
  const env = cfg.api_key_env || fallbackEnv;
  if (env && typeof process !== 'undefined' && process.env) return process.env[env];
  return undefined;
}

/** Stream one completion; publishes stream-chunks and returns the model-response payload. */
export async function complete(ctx, { baseUrl, key, model, headers = {}, temperature = 0.3, extra = {} }) {
  const tools = toTools(ctx.context);
  const body = { model, messages: toMessages(ctx.context), stream: true, temperature, ...extra };
  if (tools.length) body.tools = tools;
  const res = await fetch(`${baseUrl.replace(/\/$/, '')}/chat/completions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...(key ? { authorization: `Bearer ${key}` } : {}), ...headers },
    body: JSON.stringify(body),
    signal: ctx.signal,
  });
  if (!res.ok) throw new Error(`provider HTTP ${res.status}: ${(await res.text()).slice(0, 400)}`);
  const streamId = ctx.invocationId;
  const reader = res.body.getReader();
  const dec = new TextDecoder();
  let buf = '';
  let text = '';
  let usage = null;
  let served = model;
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
      const delta = j.choices?.[0]?.delta || {};
      if (delta.content) {
        text += delta.content;
        await ctx.publish('stream-chunk', { stream_id: streamId, text: delta.content, index: index++ }, { ui: { v: 1, kind: 'stream-chunk', stream_id: streamId, text: delta.content } });
      }
      for (const tc of delta.tool_calls || []) {
        const c = (calls[tc.index ?? 0] ||= { call_id: '', name: '', args: '' });
        if (tc.id) c.call_id = tc.id;
        if (tc.function?.name) c.name += tc.function.name;
        if (tc.function?.arguments) c.args += tc.function.arguments;
      }
    }
  }
  const tool_calls = Object.values(calls).map((c, i) => {
    let args = {};
    try { args = c.args ? JSON.parse(c.args) : {}; } catch { args = { raw: c.args }; }
    return { call_id: c.call_id || `${ctx.payload.turn}:${c.name}:${i}`, name: c.name, args };
  });
  return { text, tool_calls, stream_id: streamId, model: served, usage };
}
