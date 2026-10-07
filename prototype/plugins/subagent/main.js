// subagent: the `delegate` tool. It starts a peer session from the `worker`
// definition (its cause is this invocation) and turns the worker's
// cross-session `subagent-result` back into a `tool-result`.
import { definePlugin, offerTools, toolSpec } from '../sdk/agentmod.js';

const TOOLS = [toolSpec('delegate', 'Hand a task to a sub-session agent and wait for its answer.', { task: { type: 'string' } })];

definePlugin({
  manifest: {
    name: 'subagent',
    version: '0.1.0',
    description: 'Tool: delegate work to a sub-session (agents starting sessions for other agents).',
    consumes: [
      { event: 'session-started' },
      { event: 'config-applied' },
      { event: 'tool-call', demands: ['call_id', 'name', 'args'], mode: 'async', context: false },
      { event: 'subagent-result', demands: ['call_id', 'text'], mode: 'async', context: false },
    ],
    emits: [
      { event: 'user-message', supplies: ['text', 'parent'] },
      { event: 'tool-result', supplies: ['call_id', 'name', 'output'] },
    ],
    capabilities: ['start-session'],
    config_schema: { definition: 'session definition for workers (default "worker")' },
  },
  handlers: {
    'session-started': (ctx) => offerTools(ctx, TOOLS),
    'config-applied': (ctx) => offerTools(ctx, TOOLS),
    'tool-call': async (ctx) => {
      if (ctx.payload.name !== 'delegate') return;
      const task = String(ctx.payload.args?.task ?? '');
      const { session_id } = await ctx.host.startSession({
        definition: ctx.config.definition || 'worker',
        invocationId: ctx.invocationId,
        initial: {
          event_name: 'user-message',
          payload: { text: task, parent: { session_id: ctx.sessionId, call_id: ctx.payload.call_id } },
          ui: { v: 1, kind: 'text', role: 'user', text: task },
        },
      });
      ctx.log(`delegated ${ctx.payload.call_id} to ${session_id}`);
    },
    'subagent-result': async (ctx) => {
      await ctx.publish('tool-result', { call_id: ctx.payload.call_id, name: 'delegate', output: ctx.payload.text, worker: ctx.payload.worker });
    },
  },
});
