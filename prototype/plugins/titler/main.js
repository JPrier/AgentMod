// titler: background session auto-titling. An async observer of user messages
// that contributes a `title` context item once per session.
import { definePlugin } from '../sdk/agentmod.js';

definePlugin({
  manifest: {
    name: 'titler',
    version: '0.1.0',
    description: 'Auto-titles a session from its first user message.',
    consumes: [{ event: 'user-message', demands: ['text'], mode: 'async' }],
    emits: [{ event: 'session-titled', supplies: ['title'] }],
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
