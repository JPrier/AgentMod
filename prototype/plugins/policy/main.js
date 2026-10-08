// policy: layered tool permissions (see sdk/policy.js for the rules engine).
//
// A blocking subscriber on tool-call, placed before the plugins that execute
// tools. For each call it decides allow / ask / deny from the session's rule
// layers and permission mode, publishes the decision with its explanation
// (`policy-decision`), and:
//   allow  lets the call through;
//   deny   vetoes it and answers with a tool-result saying which rule denied it;
//   ask    vetoes it and publishes `approval-requested` (a `choice` UI hint)
//          bound to the call's action digest.
// The human's answer is a `ui-action` from an approver plugin (a frontend).
// Policy is evaluated *again* when the answer arrives — a deny added while the
// question was open wins — and the approved call is re-published with the
// digest, then checked once more on its way through this plugin, immediately
// before execution. Text in a tool result cannot approve anything: only events
// published by configured approver plugins count, and the kernel blocks
// undeclared publishes.
//
// Visibility: on every model-request it contributes `tool-policy` { hidden },
// the tools this session can never use (child allowlist, read-only mode,
// unconditional denies), so the projection does not show them to the model.
// Hiding is not the enforcement; the tool-call check above is.
//
// Config:
//   mode            auto | default | ask | read-only   (default "default")
//   rules           runtime-scope rules (appended to the built-in defaults)
//   user_rules      user-scope rules
//   session_mode, session_rules   for per-session config layers
//   approvers       plugins whose ui-actions count as human answers (default ["web-ui"])
//   defaults        false to drop the built-in rules (sdk/policy.js DEFAULT_RULES)
import { definePlugin } from '../sdk/agentmod.js';
import { DEFAULT_RULES, actionDigest, decide, explain, factsOf, grantFor, hiddenTools, stricterMode } from '../sdk/policy.js';

const delegations = new Map(); // session -> delegation (immutable once recorded)

async function delegationOf(ctx) {
  if (!delegations.has(ctx.sessionId)) {
    const view = await ctx.host.query('session', { session_id: ctx.sessionId });
    const first = view.events.find((e) => e.event_name === 'user-message');
    if (!first) return null; // not known yet; ask again later
    delegations.set(ctx.sessionId, first.payload?.delegation || null);
  }
  return delegations.get(ctx.sessionId);
}

const slotValues = (context, name) => (context || []).filter((c) => c.slot === name).map((c) => c.value);

/** Everything the engine needs for this session, from config + context + log. */
async function policyFor(ctx, context) {
  const cfg = ctx.config || {};
  const del = await delegationOf(ctx);
  const ws = slotValues(context, 'workspace-policy').pop();
  const layers = [
    { scope: 'runtime', rules: [...(cfg.defaults === false ? [] : DEFAULT_RULES), ...(cfg.rules || [])] },
    { scope: 'user', rules: cfg.user_rules || [] },
    { scope: 'workspace', rules: Array.isArray(ws?.rules) ? ws.rules : [] },
    { scope: 'session', rules: cfg.session_rules || [] },
    { scope: 'child', rules: [...(del?.rules || []), ...(del && !del.secrets ? [{ id: 'child-no-secrets', tool: '*', when: { secrets: true }, effect: 'deny', reason: 'child agents get no secrets unless the delegation names them' }] : [])] },
  ];
  let mode = stricterMode(cfg.mode || 'default', cfg.session_mode);
  if (del?.mode) mode = stricterMode(mode, del.mode);
  const allowlist = Array.isArray(del?.tools) ? [...del.tools, 'tool_search'] : null;
  const grants = slotValues(context, 'policy-grants');
  return { layers, mode, allowlist, grants };
}

const specOf = (context, name) => slotValues(context, 'tools').filter((t) => t?.name === name).pop() || {};
const brief = (args) => {
  const s = JSON.stringify(args ?? {});
  return s.length > 600 ? `${s.slice(0, 600)}…` : s;
};

definePlugin({
  manifest: {
    name: 'policy',
    version: '0.1.0',
    description: 'Layered tool policy: deny > ask > allow across scopes, explained decisions, digest-bound approvals, tool visibility.',
    consumes: [
      { event: 'tool-call', demands: ['call_id', 'name', 'args'], mode: 'blocking' },
      { event: 'ui-action', demands: ['reply_to', 'action'], mode: 'async', context: false },
      { event: 'model-request', demands: ['turn'], mode: 'blocking' },
    ],
    emits: [
      { event: 'policy-decision', supplies: ['call_id', 'effect'] },
      { event: 'approval-requested', supplies: ['call_id', 'name', 'args', 'tool_call'] },
      { event: 'tool-call', supplies: ['call_id', 'name', 'args', 'approved'] },
      { event: 'tool-result', supplies: ['call_id', 'name', 'output'] },
    ],
    config_schema: { mode: 'auto | default | ask | read-only', rules: [], user_rules: [], session_mode: null, session_rules: [], approvers: ['web-ui'], defaults: true },
  },
  handlers: {
    'model-request': async (ctx) => {
      const pol = await policyFor(ctx, ctx.context);
      const specs = slotValues(ctx.context, 'tools').filter((t) => t?.name);
      const hidden = hiddenTools(specs, pol);
      const current = slotValues(ctx.context, 'tool-policy').pop();
      const same = current && JSON.stringify(current.hidden) === JSON.stringify(hidden) && current.mode === pol.mode;
      if (!same) {
        ctx.clearSlot('tool-policy');
        ctx.add('tool-policy', { hidden, mode: pol.mode });
      }
    },
    'tool-call': async (ctx) => {
      const call = ctx.payload;
      const pol = await policyFor(ctx, ctx.context);
      const f = factsOf(call.name, call.args || {}, specOf(ctx.context, call.name));
      const digest = await actionDigest(call.name, call.args || {});
      // A re-published approved call counts only if *this plugin* published it
      // for this exact action.
      let approved = false;
      if (call.approved) {
        const me = await ctx.host.query('event', { session_id: ctx.sessionId, event_id: ctx.event.event_id });
        approved = me?.origin?.plugin === ctx.plugin.instance && call.approved.digest === digest;
      }
      const d = decide(f, { ...pol, approved });
      const quiet = d.effect === 'allow' && d.winner.scope === 'mode' && f.readOnly;
      if (!quiet) {
        await ctx.publish('policy-decision', { call_id: call.call_id, tool: call.name, effect: d.effect, rule: d.winner.id, scope: d.winner.scope, reason: d.winner.reason, explanation: explain(d), matched: d.matched, digest, mode: pol.mode });
      }
      if (d.effect === 'allow') return;
      ctx.veto(explain(d));
      if (d.effect === 'deny') {
        await ctx.publish('tool-result', { call_id: call.call_id, name: call.name, output: `Denied by policy — ${explain(d)}. Do not retry this action; choose another approach or ask the user.`, error: true, policy: { effect: 'deny', rule: d.winner.id, scope: d.winner.scope } }, {
          ui: { v: 1, kind: 'tool', name: call.name, call_id: call.call_id, status: 'denied', result: d.winner.reason },
        });
        return;
      }
      const options = [{ id: 'approve', label: 'Approve once', style: 'primary' }];
      if (d.grantable) options.push({ id: 'approve-session', label: 'Allow for this session' });
      options.push({ id: 'deny', label: 'Deny', style: 'danger' });
      await ctx.publish('approval-requested', { call_id: call.call_id, name: call.name, args: call.args, tool_call: { call_id: call.call_id, name: call.name, args: call.args }, digest, reason: d.winner.reason, rule: d.winner.id, scope: d.winner.scope }, {
        ui: { v: 1, kind: 'choice', prompt: `Allow the agent to run \`${call.name}\`?`, detail: `${f.command ? `$ ${f.command}` : brief(call.args)}\nWhy approval is needed: ${d.winner.reason} (rule ${d.winner.id}, ${d.winner.scope} scope)`, options, purpose: 'approval' },
      });
    },
    'ui-action': async (ctx) => {
      const { reply_to, action } = ctx.payload;
      if (!['approve', 'approve-session', 'deny'].includes(action)) return;
      const req = await ctx.host.query('event', { session_id: ctx.sessionId, event_id: reply_to });
      if (!req || req.event_name !== 'approval-requested' || req.origin?.plugin !== ctx.plugin.instance) return;
      // Only frontends configured as approvers can answer.
      const answer = await ctx.host.query('event', { session_id: ctx.sessionId, event_id: ctx.event.event_id });
      const approvers = ctx.config?.approvers || ['web-ui'];
      if (!approvers.includes(answer?.origin?.plugin)) return;
      // Idempotence: one answer per request.
      const view = await ctx.host.query('session', { session_id: ctx.sessionId });
      const done = view.events.some((e) => (e.event_name === 'tool-call' && e.payload.approved?.ref === reply_to) || (e.event_name === 'tool-result' && e.payload.approval_ref === reply_to));
      if (done) return;
      const call = req.payload.tool_call;
      if (action === 'deny') {
        await ctx.publish('tool-result', { call_id: call.call_id, name: call.name, output: 'The user denied this action. Do not retry it; continue another way or ask the user.', error: true, approval_ref: reply_to }, { ui: { v: 1, kind: 'tool', name: call.name, call_id: call.call_id, status: 'denied' } });
        return;
      }
      // Re-validate against the policy as it is *now* (config may have changed).
      const context = await ctx.host.query('context', { session_id: ctx.sessionId });
      const pol = await policyFor(ctx, context);
      const f = factsOf(call.name, call.args || {}, specOf(context, call.name));
      const now = decide(f, { ...pol, approved: true });
      const digest = await actionDigest(call.name, call.args || {});
      if (now.effect === 'deny' || digest !== req.payload.digest) {
        const why = digest !== req.payload.digest ? 'the action changed after the approval was requested' : `policy changed while the approval was pending — ${explain(now)}`;
        await ctx.publish('policy-decision', { call_id: call.call_id, tool: call.name, effect: 'deny', rule: now.winner.id, scope: now.winner.scope, reason: why, explanation: why, digest, revalidated: true });
        await ctx.publish('tool-result', { call_id: call.call_id, name: call.name, output: `Not run: ${why}.`, error: true, approval_ref: reply_to }, { ui: { v: 1, kind: 'tool', name: call.name, call_id: call.call_id, status: 'denied', result: 'revalidation failed' } });
        return;
      }
      if (action === 'approve-session' && now.grantable !== false) ctx.add('policy-grants', grantFor(f, reply_to));
      await ctx.publish('tool-call', { ...call, approved: { by: 'user', ref: reply_to, digest, session: action === 'approve-session' } }, { ui: { v: 1, kind: 'tool', name: call.name, args: call.args, call_id: call.call_id, status: 'approved' } });
    },
  },
});
