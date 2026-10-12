// mcp-bridge: MCP servers as AgentMod tools (native runtime; stdio or HTTP).
//
// Each configured server is connected during the handshake, and the bridge
// then declares one concrete member per tool the server offers
// (`tool-call/mcp__<server>__<tool>`, compiler.md §3.4): the tools exist
// because this plugin subscribes to them, so the loop's `tool-call/*` family
// resolves to them like any other tool. They are deferred tools (found through
// tool_search), so their schemas cost nothing until used. A server that cannot
// be reached during the handshake contributes no tools; each session the
// bridge runs in records which servers connected (`mcp-status`), and the
// `servers` service reports why one did not. Applying the config again
// reconnects. MCP never bypasses the runtime:
// every call is a recorded tool-call that the policy plugin decides (MCP tools
// are `external` effects — they ask in default mode — unless the server marks
// them read-only), results are bounded, labelled untrusted, and recorded with
// the server and tool that produced them. "The MCP server said so" carries no
// authority: its text is data.
//
// Stdio servers run as local processes with the runtime user's permissions and
// an allowlisted environment (PATH, HOME, LANG, TMPDIR plus the server's own
// `env`); they are not sandboxed by AgentMod.
//
// Config: { servers: { <name>: { command: [...], env: {}, cwd } | { url, headers } , tier? },
//           timeout_seconds: 120, max_output_bytes: 20000 }
import { definePlugin, key, optional, toolCalls, toolResult } from '../sdk/agentmod.js';
import { connectStdio, connectHttp, toSpec, flattenResult } from '../sdk/mcp.js';

const servers = new Map(); // name -> { client, specs, error }
let ready = null;

async function connect(name, cfg) {
  try {
    const client = cfg.url ? await connectHttp({ url: cfg.url, headers: cfg.headers || {} }) : await connectStdio({ command: cfg.command, env: cfg.env || {}, cwd: cfg.cwd });
    const tools = await client.listTools();
    const specs = tools.map((t) => toSpec(name, t, { tier: cfg.tier || 'deferred' }));
    servers.set(name, { client, specs, cfg, error: null });
  } catch (e) {
    servers.set(name, { client: null, specs: [], cfg, error: e.message });
  }
}

function start(config) {
  ready ??= Promise.all(Object.entries(config.servers || {}).map(([n, c]) => connect(n, c)));
  return ready;
}

const allSpecs = () => [...servers.values()].flatMap((s) => s.specs);

// The handshake waits this long for servers before declaring their tools (the
// runtime's own handshake limit is longer).
const CONNECT_WAIT_MS = 10_000;

function manifestWith(specs) {
  return {
    name: 'mcp-bridge',
    version: '0.2.0',
    description: 'MCP servers (stdio / streamable HTTP) as deferred, policy-gated, untrusted tools.',
    subscribe: [
      // Record in each session which servers connected (and why one did not).
      { event: 'session-started', mode: 'async' },
      ...toolCalls(specs),
    ],
    emit: [
      toolResult({ trust: optional('string'), server: optional('string'), tool: optional('string') }),
      { event: 'mcp-status', keys: { servers: key('array<object>') } },
    ],
    services: [{ name: 'servers', description: 'Connected MCP servers, their tools, and connection errors' }],
    config_schema: { servers: {}, timeout_seconds: 120, max_output_bytes: 20000 },
  };
}

definePlugin({
  name: 'mcp-bridge',
  // Connect first, then declare one member per tool the servers offer.
  manifest: async (cfg) => {
    await Promise.race([start(cfg || {}), new Promise((r) => setTimeout(r, CONNECT_WAIT_MS))]);
    return manifestWith(allSpecs());
  },
  init: (p) => start(p.config || {}),
  shutdown: async () => {
    for (const s of servers.values()) await s.client?.close().catch(() => {});
  },
  services: {
    servers: async () => [...servers.entries()].map(([name, s]) => ({ name, connected: !!s.client, error: s.error, tools: s.specs.map((t) => t.name) })),
  },
  handlers: {
    'session-started': async (ctx) => {
      const list = [...servers.entries()].map(([name, s]) => ({ name, connected: !!s.client, tools: s.specs.length, ...(s.error ? { error: s.error } : {}) }));
      if (!list.length) return;
      const down = list.filter((s) => !s.connected);
      const label = `MCP: ${list.filter((s) => s.connected).map((s) => `${s.name} (${s.tools} tools)`).join(', ') || 'no servers connected'}${down.length ? `; unavailable: ${down.map((s) => s.name).join(', ')}` : ''}`;
      await ctx.publish('mcp-status', { servers: list }, { ui: { v: 1, kind: 'progress', label } });
    },
    'tool-call/*': async (ctx) => {
      const { call_id, name, args } = ctx.payload;
      if (!name.startsWith('mcp__')) return;
      await start(ctx.config || {});
      const spec = allSpecs().find((s) => s.name === name);
      if (!spec) return;
      const entry = servers.get(spec.mcp.server);
      const t0 = Date.now();
      try {
        if (!entry.client || !entry.client.alive()) await connect(spec.mcp.server, entry.cfg);
        const live = servers.get(spec.mcp.server);
        if (!live.client) throw new Error(`MCP server ${spec.mcp.server} is unavailable: ${live.error}`);
        const result = await live.client.callTool(spec.mcp.tool, args || {}, { signal: ctx.signal, timeoutMs: ((ctx.config || {}).timeout_seconds || 120) * 1000 });
        const flat = flattenResult(result, { maxBytes: (ctx.config || {}).max_output_bytes || 20_000 });
        await ctx.publish('tool-result', { call_id, name, output: flat.text, error: flat.error, trust: 'external', mcp: spec.mcp, truncated: flat.truncated, duration_ms: Date.now() - t0, ...(flat.attachments.length ? { attachments: flat.attachments } : {}) });
      } catch (e) {
        if (ctx.signal.aborted) throw e;
        await ctx.publish('tool-result', { call_id, name, output: `MCP call failed: ${e.message}`, error: true, trust: 'external', mcp: spec.mcp });
      }
    },
  },
});
