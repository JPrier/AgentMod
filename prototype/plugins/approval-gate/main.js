// approval-gate: a blocking hook on the `tool-call/*` family. Approval is a
// plugin, never a runtime default. Unapproved calls to guarded tools are
// vetoed and an `approval-requested` event carries a `choice` UI hint. The
// frontend's answer arrives as a `ui-action`; approving publishes
// `approval-granted`, the loop re-publishes the call citing the grant, and this
// gate lets it through only if the grant is its own, for this call.
// Stateless: decisions are recovered from the session log via host queries.
import { definePlugin, key, optional, TOOL_CALL_KEYS, toolResult } from '../sdk/agentmod.js';

const guarded = (cfg, name) => {
  const req = cfg.require ?? ['delegate'];
  return req === '*' || (Array.isArray(req) && (req.includes('*') || req.includes(name)));
};

definePlugin({
  manifest: {
    name: 'approval-gate',
    version: '0.1.0',
    description: 'Veto guarded tool calls until a human approves them.',
    subscribe: [
      { event: 'tool-call/*', mode: 'blocking', actions: ['veto'], receives: { ...TOOL_CALL_KEYS } },
      { event: 'ui-action', mode: 'async', receives: { reply_to: key('string'), action: key('string') } },
    ],
    emit: [
      { event: 'approval-requested', keys: { call_id: key('string'), name: key('string'), args: key('object'), tool_call: key('object') } },
      { event: 'approval-granted', keys: { call_id: key('string'), name: key('string'), args: key('object'), approved: key('object') } },
      toolResult({ approval_ref: optional('string') }),
    ],
    config_schema: { require: 'list of tool names, or "*" (default ["delegate"])' },
  },
  handlers: {
    'tool-call/*': async (ctx) => {
      const call = ctx.payload;
      if (!guarded(ctx.config, call.name)) return;
      if (call.approved) {
        // Only a grant this gate published, for this exact call, counts.
        const grant = await ctx.host.query('event', { session_id: ctx.sessionId, event_id: call.approved.grant });
        if (grant?.event_name === 'approval-granted' && grant.origin?.plugin === ctx.plugin.instance && grant.payload?.call_id === call.call_id) return;
      }
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
        (e) => (e.event_name === 'approval-granted' && e.payload.approved?.ref === reply_to) || (e.event_name === 'tool-result' && e.payload.approval_ref === reply_to),
      );
      if (answered) return;
      const call = req.payload.tool_call;
      if (action === 'approve') {
        await ctx.publish('approval-granted', { call_id: call.call_id, name: call.name, args: call.args || {}, approved: { by: 'user', ref: reply_to } });
      } else {
        await ctx.publish('tool-result', { call_id: call.call_id, name: call.name, output: 'denied by the user', error: true, approval_ref: reply_to }, { ui: { v: 1, kind: 'tool', name: call.name, call_id: call.call_id, status: 'denied' } });
      }
    },
  },
});
