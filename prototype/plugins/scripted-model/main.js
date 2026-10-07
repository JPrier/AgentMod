// scripted-model: a deterministic stand-in for an LLM provider.
//
// The core contains no model client; providers are ordinary plugins. This one
// needs no network or API key: it reads the assembled context, picks tools by
// simple intent rules, and streams its reply as `stream-chunk` events so
// frontends render token-smooth output. Swap it for `openai-model` live.
import { definePlugin } from '../sdk/agentmod.js';

const words = (s) => s.match(/\s*\S+\s*/g) || [s];

function lastUser(messages) {
  return [...messages].reverse().find((m) => m.role === 'user');
}

function plan(messages, tools, memory, turn) {
  const has = (n) => tools.some((t) => t?.name === n);
  const last = messages[messages.length - 1];
  const call = (name, args) => ({ call_id: `${turn}:${name}`, name, args });

  if (last?.role === 'tool') {
    // Summarize the results of the latest tool round.
    const results = [];
    for (let i = messages.length - 1; i >= 0 && messages[i].role === 'tool'; i--) results.unshift(messages[i]);
    const clip = (t) => (t.length > 220 ? `${t.slice(0, 220)}…` : t);
    const parts = results.map((r) => (r.error ? `\`${r.name}\` failed: ${clip(r.content)}` : r.name === 'delegate' ? `The sub-agent reported back:\n\n> ${clip(r.content)}` : `\`${r.name}\` returned **${clip(r.content)}**`));
    return { text: `${parts.join('; ')}.` };
  }

  const text = (lastUser(messages)?.content || '').trim();
  const lower = text.toLowerCase();
  const expr = text.match(/(-?\d[\d\s.+\-*/^()%]*[\d)])/);
  if (/\b(time|clock|date|today)\b/.test(lower) && has('clock')) return { text: 'Let me check the clock.', tool_calls: [call('clock', {})] };
  if ((/\b(calc|calculate|compute|what is|what's)\b/.test(lower) || /^\s*[\d(][\d\s.+\-*/^()%]*$/.test(text)) && expr && /[+\-*/^%]/.test(expr[1]) && has('calc')) {
    return { text: 'Calculating.', tool_calls: [call('calc', { expression: expr[1].trim() })] };
  }
  const rem = text.match(/\bremember(?: that)?\s+(.+)/i);
  if (rem && has('remember')) return { text: 'Saving that to memory.', tool_calls: [call('remember', { note: rem[1] })] };
  if (/\bwhat do you (remember|know)\b|\brecall\b/.test(lower)) {
    if (memory.length) return { text: `From memory injected into this session: ${memory.map((m) => `“${m}”`).join(', ')}.` };
    if (has('recall')) return { text: 'Checking memory.', tool_calls: [call('recall', {})] };
  }
  const del = text.match(/\b(?:delegate|subagent|ask a worker to)\b[:\s]+(.+)/i);
  if (del && has('delegate')) return { text: 'Delegating to a sub-session.', tool_calls: [call('delegate', { task: del[1] })] };
  if (/\bcount (the )?words\b/.test(lower) && has('wordcount')) {
    const body = text.replace(/.*count (the )?words( in)?:?/i, '').trim() || text;
    return { text: 'Counting words.', tool_calls: [call('wordcount', { text: body })] };
  }
  const task = text.match(/^(summarize|research|draft|plan|list|outline|review)\b\s*(.*)/i);
  if (task) {
    const topic = task[2] || 'the request';
    return { text: `Worker report on “${topic}”: (1) gathered the context I was given, (2) identified ${3 + (topic.length % 3)} key points, (3) recommend confirming owners and dates.` };
  }
  const toolNames = tools.map((t) => `\`${t.name}\``).join(', ') || 'none';
  return {
    text:
      `You said: “${text}”. I'm the **scripted model**, a deterministic stand-in provider plugin. ` +
      `My context holds ${messages.length} message(s)` +
      (memory.length ? ` and ${memory.length} remembered note(s)` : '') +
      `; tools offered: ${toolNames}.\n\n` +
      'Try *what time is it*, *calculate (12+30)*7*, *remember that the deploy is Friday*, or *delegate: summarize the launch plan*.',
  };
}

definePlugin({
  manifest: {
    name: 'scripted-model',
    version: '0.1.0',
    description: 'Deterministic stand-in model provider (no network). Streams replies and requests tools.',
    consumes: [{ event: 'model-request', demands: ['turn'], mode: 'async' }],
    emits: [
      { event: 'stream-chunk', supplies: ['stream_id', 'text'] },
      { event: 'model-response', supplies: ['text', 'stream_id'] },
    ],
    config_schema: { delay_ms: 'per-token delay (default 25)' },
  },
  handlers: {
    'model-request': async (ctx) => {
      const messages = ctx.slot('messages');
      const tools = ctx.slot('tools');
      const memory = ctx.slot('memory');
      const out = plan(messages, tools, memory, ctx.payload.turn);
      const streamId = ctx.invocationId;
      const delay = Number(ctx.config.delay_ms ?? 25);
      let i = 0;
      for (const w of words(out.text)) {
        await ctx.sleep(delay);
        await ctx.publish('stream-chunk', { stream_id: streamId, text: w, index: i++ }, { ui: { v: 1, kind: 'stream-chunk', stream_id: streamId, text: w } });
      }
      await ctx.publish('model-response', { text: out.text, tool_calls: out.tool_calls || [], stream_id: streamId, model: 'scripted' });
    },
  },
});
