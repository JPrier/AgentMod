// tool-clock: answers `clock` tool calls. Tools are not a core concept: a tool
// call is an event, and a plugin that answers it is a tool.
import { definePlugin, offerTools, toolSpec } from '../sdk/agentmod.js';

const TOOLS = [toolSpec('clock', 'Current date and time.', {})];

definePlugin({
  manifest: {
    name: 'tool-clock',
    version: '0.1.0',
    description: 'Tool: current date/time.',
    consumes: [
      { event: 'session-started' },
      { event: 'config-applied' },
      { event: 'tool-call', demands: ['call_id', 'name', 'args'], mode: 'async', context: false },
    ],
    emits: [{ event: 'tool-result', supplies: ['call_id', 'name', 'output'] }],
  },
  handlers: {
    'session-started': (ctx) => offerTools(ctx, TOOLS),
    'config-applied': (ctx) => offerTools(ctx, TOOLS),
    'tool-call': async (ctx) => {
      if (ctx.payload.name !== 'clock') return;
      const now = new Date();
      const tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
      const output = `${now.toISOString()} (${now.toLocaleString('en-US', { timeZone: tz, dateStyle: 'full', timeStyle: 'short' })}, ${tz})`;
      await ctx.publish('tool-result', { call_id: ctx.payload.call_id, name: 'clock', output }, { ui: { v: 1, kind: 'tool', name: 'clock', call_id: ctx.payload.call_id, status: 'done', result: output } });
    },
  },
});
