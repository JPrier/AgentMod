// tool-discovery: lazy capability discovery.
//
// Tools declare a tier. `core` tools are sent to the model every turn;
// `deferred` tools (delegation, MCP servers, browser, web, repo map, …) are
// not, so their schemas cost nothing until needed. `tool_search` finds them by
// name, group, or description and *loads* the matches: it contributes their
// names to the `tools-loaded` context section, and the projection (sdk/projection.js)
// sends their full schemas from the next model request on. "select:a,b" loads
// exact names. Tools the session's policy hides are never found.
//
// It searches the session's `tools` section, which the loop plugin writes from
// its resolved `tool-call/*` members: exactly the tools the compiled graph
// provides, the same set the model is offered (compiler.md §3.4). Loading is
// session-sticky (the tools-loaded section is durable). A user message whose words match a
// deferred tool's declared `intents` (e.g. "import the GitHub repo …" →
// import_repo) loads it before the first model request, saving the model a
// discovery turn for an obvious capability.
import { definePlugin, key, optional, toolCalls, toolResult, withContext, START_INPUT } from '../sdk/agentmod.js';

import { TOOL_SEARCH, searchTools, brief } from '../sdk/discovery.js';

/** Load deferred tools whose declared intents the message mentions. */
function loadByIntent(ctx, message) {
  const text = String(message || '').toLowerCase();
  if (!text) return;
  const tools = ctx.section('tools');
  const loaded = new Set(ctx.section('tools-loaded').flatMap((v) => v?.names || []));
  const hidden = new Set(ctx.section('tool-policy').pop()?.hidden || []);
  const match = tools.filter((t) => t?.tier === 'deferred' && !loaded.has(t.name) && !hidden.has(t.name) && (t.intents || []).some((i) => text.includes(String(i).toLowerCase())));
  if (match.length) ctx.add('tools-loaded', { names: match.map((t) => t.name), reason: 'intent', event: ctx.event.event_id });
}

definePlugin({
  manifest: {
    name: 'tool-discovery',
    version: '0.1.0',
    description: 'tool_search: find and load deferred tools on demand.',
    subscribe: [
      { event: 'user-message', mode: 'blocking', actions: ['contribute-context'], ...withContext({ text: key('string') }) },
      // The first message is the start input.
      { event: 'session-started', mode: 'blocking', actions: ['contribute-context'], ...withContext({ ...START_INPUT }) },
      ...toolCalls([TOOL_SEARCH], { actions: ['contribute-context'] }),
    ],
    emit: [toolResult({ loaded: optional('array<string>') })],
  },
  handlers: {
    'session-started': (ctx) => loadByIntent(ctx, ctx.payload.input?.text),
    'user-message': (ctx) => loadByIntent(ctx, ctx.payload.text),
    'tool-call/*': async (ctx) => {
      const { call_id, name, args } = ctx.payload;
      if (name !== 'tool_search') return;
      const context = await ctx.host.query('context', { session_id: ctx.sessionId });
      const tools = context.filter((c) => c.section === 'tools').map((c) => c.value);
      const policy = context.filter((c) => c.section === 'tool-policy').pop()?.value;
      const hidden = new Set(policy?.hidden || []);
      const found = searchTools(tools, args?.query, { hidden, limit: Number(args?.limit) || 5 });
      const loaded = new Set(context.filter((c) => c.section === 'tools-loaded').flatMap((c) => c.value?.names || []));
      const fresh = found.filter((t) => t.tier === 'deferred' && !loaded.has(t.name)).map((t) => t.name);
      if (fresh.length) ctx.add('tools-loaded', { names: fresh, call_id });
      const output = found.length
        ? `${fresh.length ? `Loaded ${fresh.join(', ')}; callable from your next step.` : 'Already available:'}\n${found.map(brief).join('\n')}`
        : `No tools match ${JSON.stringify(String(args?.query || ''))}.`;
      await ctx.publish('tool-result', { call_id, name, output, loaded: fresh }, { ui: { v: 1, kind: 'tool', name, call_id, status: 'done', result: fresh.length ? `loaded ${fresh.join(', ')}` : `${found.length} found` } });
    },
  },
});
