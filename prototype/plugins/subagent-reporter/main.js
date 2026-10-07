// subagent-reporter: runs inside worker sessions. When the worker produces an
// assistant message, it publishes the answer to the parent session (a recorded
// cross-session publish; the parent link comes from the worker's own log).
import { definePlugin } from '../sdk/agentmod.js';

definePlugin({
  manifest: {
    name: 'subagent-reporter',
    version: '0.1.0',
    description: 'Reports a worker session\'s answer back to its parent session.',
    consumes: [{ event: 'assistant-message', demands: ['text'], mode: 'async', context: false }],
    emits: [{ event: 'subagent-result', supplies: ['call_id', 'text'] }],
    capabilities: ['cross-session'],
  },
  handlers: {
    'assistant-message': async (ctx) => {
      const view = await ctx.host.query('session', { session_id: ctx.sessionId });
      const first = view.events.find((e) => e.event_name === 'user-message' && e.payload.parent);
      if (!first) return; // not a worker started by `delegate`
      const already = view.sent?.some((s) => s.event_name === 'subagent-result');
      if (already) return;
      const { session_id, call_id } = first.payload.parent;
      await ctx.publish('subagent-result', { call_id, text: ctx.payload.text, worker: ctx.sessionId }, { targetSession: session_id });
    },
  },
});
