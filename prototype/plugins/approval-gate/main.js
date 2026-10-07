// approval-gate: a blocking hook on tool calls. Approval is a plugin, never a
// runtime default. Unapproved calls to guarded tools are vetoed and an
// `approval-requested` event carries a `choice` UI hint. The frontend's answer
// arrives as a `ui-action`; approving re-publishes the call with `approved`.
// Stateless: decisions are recovered from the session log via host queries.
import { definePlugin } from '../sdk/agentmod.js';

const guarded = (cfg, name) => {
  const req = cfg.require ?? ['delegate'];
  return req === '*' || (Array.isArray(req) && (req.includes('*') || req.includes(name)));
};

definePlugin({
  manifest: {
    name: 'approval-gate',
    version: '0.1.0',
    description: 'Veto guarded tool calls until a human approves them.',
    consumes: [
      { event: 'tool-call', demands: ['call_id', 'name', 'args'], mode: 'blocking', context: false },
      { event: 'ui-action', demands: ['reply_to', 'action'], mode: 'blocking', context: false },
    ],
    emits: [
      { event: 'approval-requested', supplies: ['call_id', 'name', 'args', 'tool_call'] },
      { event: 'tool-call', supplies: ['call_id', 'name', 'args', 'approved'] },
      { event: 'tool-result', supplies: ['call_id', 'name', 'output'] },
    ],
    config_schema: { require: 'list of tool names, or "*" (default ["delegate"])' },
  },
  handlers: {
    'tool-call': async (ctx) => {
      const call = ctx.payload;
      if (call.approved || !guarded(ctx.config, call.name)) return;
      ctx.veto(`tool \`${call.name}\` requires approval`);
      await ctx.publish('approval-requested', { call_id: call.call_id, name: call.name, args: call.args, tool_call: call }, {
        ui: {
          v: 1,
          kind: 'choice',
          prompt: `Allow the agent to run \`${call.name}\`?`,
          detail: JSON.stringify(call.args),
          options: [
            { id: 'approve', label: 'Approve', style: 'primary' },
            { id: 'deny', label: 'Deny', style: 'danger' },
          ],
        },
      });
    },
    'ui-action': async (ctx) => {
      const { reply_to, action } = ctx.payload;
      const req = await ctx.host.query('event', { session_id: ctx.sessionId, event_id: reply_to });
      if (!req || req.event_name !== 'approval-requested') return;
      // Idempotence: was this request already answered?
      const view = await ctx.host.query('session', { session_id: ctx.sessionId });
      const answered = view.events.some(
        (e) => (e.event_name === 'tool-call' && e.payload.approved?.ref === reply_to) || (e.event_name === 'tool-result' && e.payload.approval_ref === reply_to),
      );
      if (answered) return;
      const call = req.payload.tool_call;
      if (action === 'approve') {
        await ctx.publish('tool-call', { ...call, approved: { by: 'user', ref: reply_to } }, { ui: { v: 1, kind: 'tool', name: call.name, args: call.args, call_id: call.call_id, status: 'approved' } });
      } else {
        await ctx.publish('tool-result', { call_id: call.call_id, name: call.name, output: 'denied by the user', error: true, approval_ref: reply_to }, { ui: { v: 1, kind: 'tool', name: call.name, call_id: call.call_id, status: 'denied' } });
      }
    },
  },
});
