// titler: background session auto-titling. An async observer of user messages
// that contributes a `title` context item once per session.
import { definePlugin, key, withContext } from '../sdk/agentmod.js';

definePlugin({
  manifest: {
    name: 'titler',
    version: '0.1.0',
    description: 'Auto-titles a session from its first user message.',
    subscribe: [{ event: 'user-message', mode: 'async', actions: ['contribute-context'], receives: withContext({ text: key('string') }) }],
    emit: [{ event: 'session-titled', keys: { title: key('string') } }],
  },
  handlers: {
    'user-message': async (ctx) => {
      if (ctx.slot('title').length) return;
      const words = String(ctx.payload.text).replace(/\s+/g, ' ').trim().split(' ');
      const title = words.slice(0, 6).join(' ') + (words.length > 6 ? '…' : '');
      ctx.add('title', title);
      await ctx.publish('session-titled', { title });
    },
  },
});
