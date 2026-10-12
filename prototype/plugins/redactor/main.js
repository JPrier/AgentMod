// redactor: a blocking subscriber that returns a rewritten `text` before any
// later subscriber sees secrets. It declares what it returns, so the compiler
// walks supply through it (and checks `text` keeps its shape). A session's
// first message is the start-session input on `session-started`, so it masks
// that input's text the same way.
import { definePlugin, key, START_INPUT } from '../sdk/agentmod.js';

const PATTERNS = [
  [/\bsk-[A-Za-z0-9_-]{16,}\b/g, 'sk-…redacted'],
  [/\bAKIA[0-9A-Z]{16}\b/g, 'AKIA…redacted'],
  [/\bgh[pousr]_[A-Za-z0-9]{20,}\b/g, 'gh_…redacted'],
  [/((?:password|passwd|secret|token)\s*[:=]\s*)\S+/gi, '$1••••••'],
];

function mask(text) {
  let hits = 0;
  for (const [re, rep] of PATTERNS) text = text.replace(re, (...m) => { hits++; return rep.replace('$1', m[1] ?? ''); });
  return { text, hits };
}

definePlugin({
  manifest: {
    name: 'redactor',
    version: '0.1.0',
    description: 'Masks credentials in user messages before anything else sees them.',
    subscribe: [
      { event: 'user-message', mode: 'blocking', receives: { text: key('string') }, returns: { text: key('string'), redacted: key('integer') } },
      { event: 'session-started', mode: 'blocking', receives: { ...START_INPUT }, returns: { ...START_INPUT, redacted: key('integer') } },
    ],
  },
  handlers: {
    'user-message': (ctx) => {
      const { text, hits } = mask(String(ctx.payload.text));
      ctx.returns({ text, redacted: hits });
    },
    'session-started': (ctx) => {
      const input = ctx.payload.input;
      if (typeof input?.text !== 'string') {
        ctx.returns({ redacted: 0 });
        return;
      }
      const { text, hits } = mask(input.text);
      ctx.returns({ input: { ...input, text }, redacted: hits });
    },
  },
});
