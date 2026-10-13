// subagent-reporter: runs inside child sessions. When the child produces its
// final answer (an assistant message), it reports to the parent once: the
// conclusion plus evidence derived from the child's own log (commands and exit
// codes, tests, files changed, workspace, usage). It is a recorded
// cross-session publish; the parent link comes from the child's start input.
import { definePlugin, key, startInputOf } from '../sdk/agentmod.js';
import { evidenceOf } from '../sdk/delegation.js';

definePlugin({
  manifest: {
    name: 'subagent-reporter',
    version: '0.2.0',
    description: 'Reports a child session\'s answer and evidence back to its parent session.',
    subscribe: [{ event: 'assistant-message', mode: 'async', receives: { text: key('string') } }],
    // Sent to the parent session, which must declare `subagent-result` inbound.
    outbound: [{ event: 'subagent-result', keys: { call_id: key('string'), text: key('string'), worker: key('string'), evidence: key('object') } }],
    capabilities: ['cross-session'],
  },
  handlers: {
    'assistant-message': async (ctx) => {
      const view = await ctx.host.query('session', { session_id: ctx.sessionId });
      const parent = startInputOf(view)?.data?.parent;
      if (!parent) return; // not a child started by `delegate`
      const already = view.sent?.some((s) => s.event_name === 'subagent-result');
      if (already) return;
      const { session_id, call_id } = parent;
      await ctx.publish('subagent-result', { call_id, text: ctx.payload.text, worker: ctx.sessionId, evidence: evidenceOf(view) }, { targetSession: session_id });
    },
  },
});
