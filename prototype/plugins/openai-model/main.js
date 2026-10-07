// openai-model: a live provider plugin for any OpenAI-compatible
// /chat/completions endpoint (OpenAI, OpenRouter, Ollama, vLLM, LM Studio...).
//
// Config: { base_url, model, api_key_env?, api_key?, temperature? }
// In the native runtime prefer api_key_env (the key never enters the log).
// The request is built entirely from the assembled context: `messages`,
// `tools`, and `memory` slots contributed by other plugins.
import { definePlugin } from '../sdk/agentmod.js';

function toOpenAI(context) {
  const out = [];
  const memory = context.filter((c) => c.slot === 'memory').map((c) => c.value);
  const system = context.filter((c) => c.slot === 'system').map((c) => c.value);
  if (system.length || memory.length) {
    out.push({ role: 'system', content: [...system, ...(memory.length ? [`Remembered notes:\n- ${memory.join('\n- ')}`] : [])].join('\n\n') });
  }
  for (const { value: m } of context.filter((c) => c.slot === 'messages')) {
    if (m.role === 'assistant') {
      out.push({
        role: 'assistant',
        content: m.content || null,
        tool_calls: m.tool_calls?.map((c) => ({ id: c.call_id, type: 'function', function: { name: c.name, arguments: JSON.stringify(c.args || {}) } })),
      });
    } else if (m.role === 'tool') {
      out.push({ role: 'tool', tool_call_id: m.call_id, content: String(m.content) });
    } else {
      out.push({ role: 'user', content: String(m.content) });
    }
  }
  return out;
}

function toolDefs(context) {
  return context
    .filter((c) => c.slot === 'tools')
    .map(({ value: t }) => ({ type: 'function', function: { name: t.name, description: t.description, parameters: { type: 'object', properties: t.parameters || {}, additionalProperties: true } } }));
}

definePlugin({
  manifest: {
    name: 'openai-model',
    version: '0.1.0',
    description: 'Live OpenAI-compatible chat-completions provider with streaming and tool calls.',
    consumes: [{ event: 'model-request', demands: ['turn'], mode: 'async' }],
    emits: [
      { event: 'stream-chunk', supplies: ['stream_id', 'text'] },
      { event: 'model-response', supplies: ['text', 'stream_id'] },
    ],
    config_schema: { base_url: 'https://api.openai.com/v1', model: 'gpt-4o-mini', api_key_env: 'OPENAI_API_KEY', api_key: 'browser only' },
  },
  handlers: {
    'model-request': async (ctx) => {
      const cfg = ctx.config;
      const base = (cfg.base_url || 'https://api.openai.com/v1').replace(/\/$/, '');
      const key = cfg.api_key || (cfg.api_key_env && typeof process !== 'undefined' ? process.env[cfg.api_key_env] : undefined);
      const tools = toolDefs(ctx.context);
      const body = { model: cfg.model || 'gpt-4o-mini', messages: toOpenAI(ctx.context), stream: true, temperature: cfg.temperature ?? 0.3 };
      if (tools.length) body.tools = tools;
      const res = await fetch(`${base}/chat/completions`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...(key ? { authorization: `Bearer ${key}` } : {}) },
        body: JSON.stringify(body),
        signal: ctx.signal,
      });
      if (!res.ok) throw new Error(`provider HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`);
      const streamId = ctx.invocationId;
      const reader = res.body.getReader();
      const dec = new TextDecoder();
      let buf = '';
      let text = '';
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
          const delta = j.choices?.[0]?.delta || {};
          if (delta.content) {
            text += delta.content;
            await ctx.publish('stream-chunk', { stream_id: streamId, text: delta.content, index: index++ }, { ui: { v: 1, kind: 'stream-chunk', stream_id: streamId, text: delta.content } });
          }
          for (const tc of delta.tool_calls || []) {
            const c = (calls[tc.index] ||= { call_id: '', name: '', args: '' });
            if (tc.id) c.call_id = tc.id;
            if (tc.function?.name) c.name += tc.function.name;
            if (tc.function?.arguments) c.args += tc.function.arguments;
          }
        }
      }
      const tool_calls = Object.values(calls).map((c) => {
        let args = {};
        try { args = c.args ? JSON.parse(c.args) : {}; } catch { args = { raw: c.args }; }
        return { call_id: c.call_id || `${ctx.payload.turn}:${c.name}`, name: c.name, args };
      });
      await ctx.publish('model-response', { text, tool_calls, stream_id: streamId, model: body.model });
    },
  },
});
