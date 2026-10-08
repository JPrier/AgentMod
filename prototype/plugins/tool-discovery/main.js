// tool-discovery: lazy capability discovery.
//
// Tools declare a tier. `core` tools are sent to the model every turn;
// `deferred` tools (delegation, MCP servers, browser, web, repo map, …) are
// not, so their schemas cost nothing until needed. `tool_search` finds them by
// name, group, or description and *loads* the matches: it contributes their
// names to the `tools-loaded` slot, and the projection (sdk/projection.js)
// sends their full schemas from the next model request on. "select:a,b" loads
// exact names. Tools the session's policy hides are never found.
//
// Loaded tools bind to their compiled owner: a tool is loaded only when the
// session's routing table (compiled from manifests) has an owner for it, so a
// call can never fall back to a broadcast scan. Loading is session-sticky
// (the tools-loaded slot is durable). A user message whose words match a
// deferred tool's declared `intents` (e.g. "import the GitHub repo …" →
// import_repo) loads it before the first model request, saving the model a
// discovery turn for an obvious capability.
import { definePlugin, declareTools, offerTools, ownTools } from '../sdk/agentmod.js';

import { TOOL_SEARCH, searchTools, brief } from '../sdk/discovery.js';

/** The session's compiled tool owners (null when the host cannot say). */
async function ownedBy(ctx) {
  try {
    const r = await ctx.host.query('routes', { session_id: ctx.sessionId });
    const table = r?.routes?.['tool-call'];
    if (!table) return null;
    const prefixes = (table.prefixes || []).map((p) => p.prefix);
    return (name) => Object.hasOwn(table.exact || {}, name) || prefixes.some((p) => name.startsWith(p));
  } catch {
    return null;
  }
}

definePlugin({
  manifest: {
    name: 'tool-discovery',
    version: '0.1.0',
    description: 'tool_search: find and load deferred tools on demand.',
    consumes: [
      { event: 'session-started' },
      { event: 'config-applied' },
      { event: 'user-message', demands: ['text'], mode: 'blocking' },
      ownTools(['tool_search']),
    ],
    tools: declareTools([TOOL_SEARCH]),
    emits: [{ event: 'tool-result', supplies: ['call_id', 'name', 'output'] }],
  },
  handlers: {
    'user-message': async (ctx) => {
      const text = String(ctx.payload.text || '').toLowerCase();
      if (!text) return;
      const tools = ctx.slot('tools');
      const loaded = new Set(ctx.slot('tools-loaded').flatMap((v) => v?.names || []));
      const hidden = new Set(ctx.slot('tool-policy').pop()?.hidden || []);
      const match = tools.filter((t) => t?.tier === 'deferred' && !loaded.has(t.name) && !hidden.has(t.name) && (t.intents || []).some((i) => text.includes(String(i).toLowerCase())));
      if (!match.length) return;
      const owned = await ownedBy(ctx);
      const names = match.map((t) => t.name).filter((n) => !owned || owned(n));
      if (names.length) ctx.add('tools-loaded', { names, reason: 'intent', event: ctx.event.event_id });
    },
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
      const owned = await ownedBy(ctx);
      const unowned = found.filter((t) => owned && !owned(t.name)).map((t) => t.name);
      const fresh = found.filter((t) => t.tier === 'deferred' && !loaded.has(t.name) && !unowned.includes(t.name)).map((t) => t.name);
      if (fresh.length) ctx.add('tools-loaded', { names: fresh, call_id });
      const output = found.length
        ? `${fresh.length ? `Loaded ${fresh.join(', ')}; callable from your next step.` : 'Already available:'}\n${found.map(brief).join('\n')}`
        : `No tools match ${JSON.stringify(String(args?.query || ''))}.`
      const note = unowned.length ? `\nNot loaded (no plugin in this session owns them): ${unowned.join(', ')}.` : '';
      await ctx.publish('tool-result', { call_id, name, output: output + note, loaded: fresh }, { ui: { v: 1, kind: 'tool', name, call_id, status: 'done', result: fresh.length ? `loaded ${fresh.join(', ')}` : `${found.length} found` } });
    },
  },
});
