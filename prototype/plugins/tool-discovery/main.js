// tool-discovery: lazy capability discovery.
//
// Tools declare a tier. `core` tools are sent to the model every turn;
// `deferred` tools (delegation, MCP servers, browser, web, repo map, …) are
// not, so their schemas cost nothing until needed. `tool_search` finds them by
// name, group, or description and *loads* the matches: it contributes their
// names to the `tools-loaded` slot, and the projection (sdk/projection.js)
// sends their full schemas from the next model request on. "select:a,b" loads
// exact names. Tools the session's policy hides are never found.
import { definePlugin, declareTools, offerTools, ownTools } from '../sdk/agentmod.js';

import { TOOL_SEARCH, searchTools, brief } from '../sdk/discovery.js';

definePlugin({
  manifest: {
    name: 'tool-discovery',
    version: '0.1.0',
    description: 'tool_search: find and load deferred tools on demand.',
    consumes: [
      { event: 'session-started' },
      { event: 'config-applied' },
      ownTools(['tool_search']),
    ],
    tools: declareTools([TOOL_SEARCH]),
    emits: [{ event: 'tool-result', supplies: ['call_id', 'name', 'output'] }],
  },
  handlers: {
    'session-started': (ctx) => offerTools(ctx, [TOOL_SEARCH]),
    'config-applied': (ctx) => offerTools(ctx, [TOOL_SEARCH]),
    'tool-call': async (ctx) => {
      const { call_id, name, args } = ctx.payload;
      if (name !== 'tool_search') return;
      const context = await ctx.host.query('context', { session_id: ctx.sessionId });
      const tools = context.filter((c) => c.slot === 'tools').map((c) => c.value);
      const policy = context.filter((c) => c.slot === 'tool-policy').pop()?.value;
      const hidden = new Set(policy?.hidden || []);
      const found = searchTools(tools, args?.query, { hidden, limit: Number(args?.limit) || 5 });
      const loaded = new Set(context.filter((c) => c.slot === 'tools-loaded').flatMap((c) => c.value?.names || []));
      const fresh = found.filter((t) => t.tier === 'deferred' && !loaded.has(t.name)).map((t) => t.name);
      if (fresh.length) ctx.add('tools-loaded', { names: fresh, call_id });
      const output = found.length
        ? `${fresh.length ? `Loaded ${fresh.join(', ')}; callable from your next step.` : 'Already available:'}\n${found.map(brief).join('\n')}`
        : `No tools match ${JSON.stringify(String(args?.query || ''))}.`;
      await ctx.publish('tool-result', { call_id, name, output, loaded: fresh }, { ui: { v: 1, kind: 'tool', name, call_id, status: 'done', result: fresh.length ? `loaded ${fresh.join(', ')}` : `${found.length} found` } });
    },
  },
});
