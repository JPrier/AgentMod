// titler: background session auto-titling. An async observer of the session's
// start input and user messages that contributes a `title` context item once
// per session.
import { definePlugin, key, withContext, START_INPUT } from '../sdk/agentmod.js';

async function title(ctx, text) {
  if (!text || ctx.slot('title').length) return;
  const words = String(text).replace(/\s+/g, ' ').trim().split(' ');
  const t = words.slice(0, 6).join(' ') + (words.length > 6 ? '…' : '');
  ctx.add('title', t);
  await ctx.publish('session-titled', { title: t });
}

definePlugin({
  manifest: {
    name: 'titler',
    version: '0.1.0',
    description: 'Auto-titles a session from its first user message.',
    subscribe: [
      { event: 'session-started', mode: 'async', actions: ['contribute-context'], receives: withContext({ ...START_INPUT }) },
      { event: 'user-message', mode: 'async', actions: ['contribute-context'], receives: withContext({ text: key('string') }) },
    ],
    emit: [{ event: 'session-titled', keys: { title: key('string') } }],
  },
  handlers: {
    'session-started': (ctx) => title(ctx, ctx.payload.input?.text),
    'user-message': (ctx) => title(ctx, ctx.payload.text),
  },
});
