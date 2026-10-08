// plan: an intentionally tiny planning tool.
//
// `update_plan` replaces the whole plan (a short list of steps with a status
// each). The plan is session context (slot `plan`), so it is durable in the
// log and survives restarts, model switches, and projection-side compaction —
// the projection renders it at the end of every request so a resumed agent can
// reorient at once. Children keep their own plans; nothing is shared.
//
//   consumes  session-started, config-applied   offer the tool
//             tool-call (async)                  update_plan
//   emits     plan-updated (UI hint `plan`), tool-result
import { definePlugin, offerTools } from '../sdk/agentmod.js';

import { UPDATE_PLAN, validatePlan } from '../sdk/plan.js';

definePlugin({
  manifest: {
    name: 'plan',
    version: '0.1.0',
    description: 'update_plan: a tiny, durable task list kept in session context.',
    consumes: [
      { event: 'session-started' },
      { event: 'config-applied' },
      { event: 'tool-call', demands: ['call_id', 'name', 'args'], mode: 'async', context: false },
    ],
    emits: [
      { event: 'plan-updated', supplies: ['items'] },
      { event: 'tool-result', supplies: ['call_id', 'name', 'output'] },
    ],
  },
  handlers: {
    'session-started': (ctx) => offerTools(ctx, [UPDATE_PLAN]),
    'config-applied': (ctx) => offerTools(ctx, [UPDATE_PLAN]),
    'tool-call': async (ctx) => {
      const { call_id, name, args } = ctx.payload;
      if (name !== 'update_plan') return;
      let items;
      try {
        items = validatePlan(args?.items);
      } catch (e) {
        await ctx.publish('tool-result', { call_id, name, output: e.message, error: true });
        return;
      }
      const active = items.filter((i) => i.status === 'in_progress').length;
      ctx.clearSlot('plan');
      ctx.add('plan', { items, note: args?.note ? String(args.note).slice(0, 200) : undefined, call_id });
      await ctx.publish('plan-updated', { items, call_id }, { ui: { v: 1, kind: 'plan', items, note: args?.note } });
      const done = items.filter((i) => i.status === 'completed').length;
      await ctx.publish('tool-result', { call_id, name, output: `Plan updated: ${done}/${items.length} completed${active > 1 ? ' (note: more than one step is in_progress)' : ''}.` }, { ui: { v: 1, kind: 'tool', name, call_id, status: 'done', result: `${done}/${items.length}` } });
    },
  },
});
