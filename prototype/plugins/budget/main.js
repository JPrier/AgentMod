// budget: hard bounds on a session's work (used for delegated children; usable
// anywhere). A blocking subscriber on model-request: once a bound is reached it
// vetoes further model requests and publishes a final assistant message saying
// why, so a child reports back instead of running away.
//
// Bounds come from config ({ max_model_requests, max_tool_calls,
// max_output_tokens, max_wall_seconds }) and, tighter, from a delegation's
// `budget` in the session's first user message.
import { definePlugin } from '../sdk/agentmod.js';

import { effectiveBudget, usage, exceeded } from '../sdk/budget.js';

definePlugin({
  manifest: {
    name: 'budget',
    version: '0.1.0',
    description: 'Hard per-session bounds on model requests, tool calls, tokens, and wall time.',
    consumes: [{ event: 'model-request', demands: ['turn'], mode: 'blocking', context: false }],
    emits: [
      { event: 'budget-exhausted', supplies: ['reason'] },
      { event: 'assistant-message', supplies: ['text'] },
    ],
    config_schema: { max_model_requests: 0, max_tool_calls: 0, max_output_tokens: 0, max_wall_seconds: 0 },
  },
  handlers: {
    'model-request': async (ctx) => {
      const view = await ctx.host.query('session', { session_id: ctx.sessionId });
      const delegation = view.events.find((e) => e.event_name === 'user-message')?.payload?.delegation?.budget;
      const budget = effectiveBudget(ctx.config, delegation);
      if (!Object.keys(budget).length) return;
      const u = usage(view.events);
      const why = exceeded(budget, u);
      if (!why) return;
      if (view.events.some((e) => e.event_name === 'budget-exhausted')) {
        ctx.veto(`budget exhausted: ${why}`);
        return;
      }
      await ctx.publish('budget-exhausted', { reason: why, usage: u, budget }, { ui: { v: 1, kind: 'progress', label: `Budget exhausted: ${why}` } });
      await ctx.publish('assistant-message', { text: `Stopped: the session's budget is exhausted (${why}). Work so far is in the workspace and the session log.`, budget_exhausted: true }, { ui: { v: 1, kind: 'markdown', role: 'assistant', text: `Stopped: budget exhausted (${why}).` } });
      ctx.veto(`budget exhausted: ${why}`);
    },
  },
});
