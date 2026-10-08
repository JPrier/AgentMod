// subagent-reporter: runs inside child sessions. When the child produces its
// final answer (an assistant message), it reports to the parent once: the
// conclusion plus evidence derived from the child's own log (commands and exit
// codes, tests, files changed, workspace, usage). It is a recorded
// cross-session publish; the parent link comes from the child's first message.
import { definePlugin } from '../sdk/agentmod.js';
import { evidenceOf } from '../sdk/delegation.js';

definePlugin({
  manifest: {
    name: 'subagent-reporter',
    version: '0.2.0',
    description: 'Reports a child session\'s answer and evidence back to its parent session.',
    consumes: [{ event: 'assistant-message', demands: ['text'], mode: 'async', context: false }],
    emits: [{ event: 'subagent-result', supplies: ['call_id', 'text'] }],
    capabilities: ['cross-session'],
  },
  handlers: {
    'assistant-message': async (ctx) => {
      const view = await ctx.host.query('session', { session_id: ctx.sessionId });
      const first = view.events.find((e) => e.event_name === 'user-message' && e.payload.parent);
      if (!first) return; // not a child started by `delegate`
      const already = view.sent?.some((s) => s.event_name === 'subagent-result');
      if (already) return;
      const { session_id, call_id } = first.payload.parent;
      await ctx.publish('subagent-result', { call_id, text: ctx.payload.text, worker: ctx.sessionId, evidence: evidenceOf(view) }, { targetSession: session_id });
    },
  },
});
