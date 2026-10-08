// mcp-bridge: MCP servers as AgentMod tools (native runtime; stdio or HTTP).
//
// Each configured server is connected at start; its tools are offered as
// deferred tools named mcp__<server>__<tool> (found through tool_search), so
// their schemas cost nothing until used. MCP never bypasses the runtime:
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
import { definePlugin, offerTools, ownTools } from '../sdk/agentmod.js';
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

async function offer(ctx) {
  await Promise.race([start(ctx.config || {}), new Promise((r) => setTimeout(r, 15_000))]);
  offerTools(ctx, allSpecs());
}

// Each configured server owns the family `mcp__<server>__*`: ownership is
// known from config at compile time even though the tool list arrives later.
const families = (cfg) => Object.keys(cfg.servers || {}).map((n) => `mcp__${n}__*`);

definePlugin({
  name: 'mcp-bridge',
  manifest: (cfg) => ({
    name: 'mcp-bridge',
    version: '0.1.0',
    description: 'MCP servers (stdio / streamable HTTP) as deferred, policy-gated, untrusted tools.',
    consumes: [
      { event: 'session-started' },
      { event: 'config-applied' },
      ownTools(families(cfg)),
    ],
    emits: [{ event: 'tool-result', supplies: ['call_id', 'name', 'output'] }],
    services: [{ name: 'servers', description: 'Connected MCP servers, their tools, and connection errors' }],
    tools: families(cfg).map((name) => ({ name, tier: 'deferred' })),
    config_schema: { servers: {}, timeout_seconds: 120, max_output_bytes: 20000 },
  }),
  init: (p) => start(p.config || {}),
  shutdown: async () => {
    for (const s of servers.values()) await s.client?.close().catch(() => {});
  },
  services: {
    servers: async () => [...servers.entries()].map(([name, s]) => ({ name, connected: !!s.client, error: s.error, tools: s.specs.map((t) => t.name) })),
  },
  handlers: {
    'session-started': offer,
    'config-applied': offer,
    'tool-call': async (ctx) => {
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
