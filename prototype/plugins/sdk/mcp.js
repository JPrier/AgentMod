// A small Model Context Protocol client (stdio and streamable HTTP) and the
// mapping from MCP tools to AgentMod tool specs. Used by the mcp-bridge plugin.
//
// MCP gets no special status: its tools are ordinary deferred tools whose calls
// pass through policy (effects `external` unless the server marks a tool
// read-only), whose results are bounded and labelled untrusted, and whose
// every call and result is a recorded event.

export const PROTOCOL_VERSION = '2025-06-18';

const sanitize = (s) => String(s).replace(/[^A-Za-z0-9_-]/g, '_');

/** AgentMod tool name for an MCP tool (provider limit: 64 chars). */
export function toolName(server, tool) {
  const n = `mcp__${sanitize(server)}__${sanitize(tool)}`;
  return n.length <= 64 ? n : `${n.slice(0, 55)}_${(Array.from(n).reduce((h, c) => (h * 31 + c.charCodeAt(0)) >>> 0, 7) % 1e8).toString(36)}`;
}

/** Map an MCP tool definition to an AgentMod tool spec. */
export function toSpec(server, tool, { tier = 'deferred' } = {}) {
  const schema = tool.inputSchema || {};
  const ro = tool.annotations?.readOnlyHint === true;
  return {
    name: toolName(server, tool.name),
    description: `[MCP ${server}] ${String(tool.description || tool.title || tool.name).slice(0, 1000)}`,
    parameters: schema.properties || {},
    ...(Array.isArray(schema.required) && schema.required.length ? { required: schema.required } : {}),
    tier,
    group: `mcp:${server}`,
    effects: ro ? 'read' : 'external',
    trust: 'external',
    mcp: { server, tool: tool.name },
  };
}

/** Flatten an MCP tools/call result into text + image attachments. */
export function flattenResult(result, { maxBytes = 20_000 } = {}) {
  const parts = [];
  const attachments = [];
  for (const c of result?.content || []) {
    if (c.type === 'text') parts.push(c.text);
    else if (c.type === 'image' && c.data) attachments.push({ type: 'image', media_type: c.mimeType || 'image/png', data: c.data });
    else if (c.type === 'resource') parts.push(c.resource?.text ? `[resource ${c.resource.uri}]\n${c.resource.text}` : `[resource ${c.resource?.uri}]`);
    else if (c.type === 'resource_link') parts.push(`[resource link ${c.uri}${c.name ? ` — ${c.name}` : ''}]`);
    else parts.push(`[${c.type} content]`);
  }
  if (result?.structuredContent && !parts.length) parts.push(JSON.stringify(result.structuredContent, null, 2));
  let text = parts.join('\n');
  let truncated = false;
  if (text.length > maxBytes) {
    text = `${text.slice(0, Math.floor(maxBytes * 0.7))}\n… [${text.length - maxBytes} characters omitted] …\n${text.slice(-Math.floor(maxBytes * 0.3))}`;
    truncated = true;
  }
  return { text: text || '(no content)', attachments, error: !!result?.isError, truncated };
}

class Rpc {
  constructor(send) {
    this.send = send;
    this.next = 1;
    this.pending = new Map();
  }
  request(method, params, { signal, timeoutMs = 60_000 } = {}) {
    const id = this.next++;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`MCP ${method} timed out`)); }, timeoutMs);
      const abort = () => { this.pending.delete(id); clearTimeout(timer); this.notify('notifications/cancelled', { requestId: id, reason: 'cancelled by AgentMod' }); reject(new Error('cancelled')); };
      signal?.addEventListener('abort', abort, { once: true });
      this.pending.set(id, { resolve: (v) => { clearTimeout(timer); signal?.removeEventListener('abort', abort); resolve(v); }, reject: (e) => { clearTimeout(timer); signal?.removeEventListener('abort', abort); reject(e); } });
      Promise.resolve(this.send({ jsonrpc: '2.0', id, method, params })).catch((e) => this.fail(id, e));
    });
  }
  notify(method, params) {
    Promise.resolve(this.send({ jsonrpc: '2.0', method, params })).catch(() => {});
  }
  fail(id, e) {
    const p = this.pending.get(id);
    if (p) { this.pending.delete(id); p.reject(e); }
  }
  receive(msg) {
    if (msg.id != null && (msg.result !== undefined || msg.error)) {
      const p = this.pending.get(msg.id);
      if (!p) return;
      this.pending.delete(msg.id);
      if (msg.error) p.reject(new Error(`MCP error ${msg.error.code}: ${msg.error.message}`));
      else p.resolve(msg.result);
    } else if (msg.id != null && msg.method) {
      // Server → client requests (sampling, elicitation, roots) are not offered.
      Promise.resolve(this.send({ jsonrpc: '2.0', id: msg.id, error: { code: -32601, message: `AgentMod's MCP bridge does not support ${msg.method}` } })).catch(() => {});
    }
  }
  closeAll(e) {
    for (const p of this.pending.values()) p.reject(e);
    this.pending.clear();
  }
}

async function handshake(rpc) {
  const init = await rpc.request('initialize', { protocolVersion: PROTOCOL_VERSION, capabilities: {}, clientInfo: { name: 'agentmod-mcp-bridge', version: '0.1.0' } }, { timeoutMs: 30_000 });
  rpc.notify('notifications/initialized', {});
  return init;
}

/** Connect to a stdio MCP server (Node only). */
export async function connectStdio({ command, env = {}, cwd, passEnv = ['PATH', 'HOME', 'LANG', 'TMPDIR'] }) {
  const { spawn } = await import('node:child_process');
  const readline = await import('node:readline');
  const base = Object.fromEntries(passEnv.filter((k) => process.env[k] !== undefined).map((k) => [k, process.env[k]]));
  const child = spawn(command[0], command.slice(1), { cwd, env: { ...base, ...env }, stdio: ['pipe', 'pipe', 'pipe'] });
  const rpc = new Rpc((msg) => child.stdin.write(`${JSON.stringify(msg)}\n`));
  readline.createInterface({ input: child.stdout }).on('line', (line) => {
    try { rpc.receive(JSON.parse(line)); } catch { /* not protocol */ }
  });
  let stderr = '';
  child.stderr.on('data', (d) => { stderr = (stderr + d).slice(-2000); });
  const exited = new Promise((resolve) => child.on('exit', (code, sig) => { rpc.closeAll(new Error(`MCP server exited (${code ?? sig}): ${stderr.slice(-300)}`)); resolve(); }));
  child.on('error', (e) => rpc.closeAll(e));
  const info = await handshake(rpc);
  return {
    info,
    listTools: async () => {
      const tools = [];
      let cursor;
      do {
        const r = await rpc.request('tools/list', cursor ? { cursor } : {});
        tools.push(...(r.tools || []));
        cursor = r.nextCursor;
      } while (cursor);
      return tools;
    },
    callTool: (name, args, { signal, timeoutMs } = {}) => rpc.request('tools/call', { name, arguments: args ?? {} }, { signal, timeoutMs }),
    close: async () => { child.kill(); await exited; },
    alive: () => child.exitCode === null && !child.killed,
  };
}

/** Connect to a streamable-HTTP MCP server. */
export async function connectHttp({ url, headers = {} }) {
  let session = null;
  let rpc;
  const send = async (msg) => {
    const res = await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json', accept: 'application/json, text/event-stream', 'mcp-protocol-version': PROTOCOL_VERSION, ...(session ? { 'mcp-session-id': session } : {}), ...headers }, body: JSON.stringify(msg) });
    if (res.headers.get('mcp-session-id')) session = res.headers.get('mcp-session-id');
    if (msg.id == null) return;
    if (!res.ok) throw new Error(`MCP HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`);
    const type = res.headers.get('content-type') || '';
    if (type.includes('text/event-stream')) {
      const text = await res.text();
      for (const block of text.split('\n\n')) {
        const data = block.split('\n').filter((l) => l.startsWith('data:')).map((l) => l.slice(5).trim()).join('\n');
        if (data) rpc.receive(JSON.parse(data));
      }
    } else {
      rpc.receive(await res.json());
    }
  };
  rpc = new Rpc(send);
  const info = await handshake(rpc);
  return {
    info,
    listTools: async () => (await rpc.request('tools/list', {})).tools || [],
    callTool: (name, args, { signal, timeoutMs } = {}) => rpc.request('tools/call', { name, arguments: args ?? {} }, { signal, timeoutMs }),
    close: async () => {},
    alive: () => true,
  };
}
