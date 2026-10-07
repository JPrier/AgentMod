// redactor: a blocking transformer. It rewrites user messages in place before
// any later subscriber (or the log's downstream readers) sees secrets. It
// declares what it preserves and adds, so the compiler can walk supply.
import { definePlugin } from '../sdk/agentmod.js';

const PATTERNS = [
  [/\bsk-[A-Za-z0-9_-]{16,}\b/g, 'sk-…redacted'],
  [/\bAKIA[0-9A-Z]{16}\b/g, 'AKIA…redacted'],
  [/\bgh[pousr]_[A-Za-z0-9]{20,}\b/g, 'gh_…redacted'],
  [/((?:password|passwd|secret|token)\s*[:=]\s*)\S+/gi, '$1••••••'],
];

definePlugin({
  manifest: {
    name: 'redactor',
    version: '0.1.0',
    description: 'Transformer: masks credentials in user messages before anything else sees them.',
    consumes: [{ event: 'user-message', demands: ['text'], mode: 'blocking', context: false }],
    transforms: [{ event: 'user-message', preserves: ['*'], adds: ['redacted'] }],
  },
  handlers: {
    'user-message': (ctx) => {
      let text = String(ctx.payload.text);
      let hits = 0;
      for (const [re, rep] of PATTERNS) text = text.replace(re, (...m) => { hits++; return rep.replace('$1', m[1] ?? ''); });
      ctx.transform({ ...ctx.payload, text, redacted: hits });
    },
  },
});
