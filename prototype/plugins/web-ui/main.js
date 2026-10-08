// web-ui (native gateway): the web frontend as an ordinary plugin. It is an
// async subscriber of session-started (that invocation is its standing
// citation in the session), a watcher of records and live streams, a
// publisher of user actions (deferred publishes), and a sender of dispatcher
// commands. It serves the static UI and a small HTTP + Server-Sent-Events API.
//
// Config: { port: 7700, host: "127.0.0.1", static_dir: "ui" }
import { definePlugin } from '../sdk/agentmod.js';
import { WEB_UI_MANIFEST } from './manifest.js';

const http = await import('node:http');
const fs = await import('node:fs');
const path = await import('node:path');

// session_id -> the first invocation dispatched to us there. User actions cite
// it, so each one starts a fresh causal chain (depth 1) instead of extending
// the session's deepest chain.
const cites = new Map();
// SSE clients: { res, id, behind } — each with a bounded socket buffer. Records
// (canonical, low-volume) always go out; live stream frames are dropped for a
// client whose buffer is past the bound, which then gets `resync-required` and
// re-hydrates from /api/streams (snapshot + sequence).
const clients = new Set();
let nextClient = 1;
const LIVE_LIMIT = 512 * 1024;
const live = { frames_in: 0, sse_writes: 0, dropped: 0, resyncs: 0 };
let plugin;

const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.wasm': 'application/wasm', '.svg': 'image/svg+xml', '.toml': 'text/plain' };

function cors(res) {
  res.setHeader('access-control-allow-origin', '*');
  res.setHeader('access-control-allow-methods', 'GET, POST, OPTIONS');
  res.setHeader('access-control-allow-headers', 'content-type');
  // Lets the GitHub Pages build attach to this local runtime (Private Network Access).
  res.setHeader('access-control-allow-private-network', 'true');
}

function send(res, code, body) {
  cors(res);
  res.writeHead(code, { 'content-type': 'application/json' });
  res.end(JSON.stringify(body ?? null));
}

async function readBody(req) {
  let data = '';
  for await (const chunk of req) data += chunk;
  return data ? JSON.parse(data) : {};
}

async function citeFor(sessionId) {
  if (cites.has(sessionId)) return cites.get(sessionId);
  const view = await plugin.host.query('session', { session_id: sessionId });
  const inv = view.events.flatMap((e) => e.invocations).find((i) => i.plugin === plugin.instance);
  if (!inv) throw new Error(`web-ui has no standing invocation in ${sessionId}`);
  cites.set(sessionId, inv.invocation_id);
  return inv.invocation_id;
}

async function route(req, res) {
  const url = new URL(req.url, 'http://local');
  const p = url.pathname;
  const m = p.match(/^\/api\/sessions\/([^/]+)(?:\/([a-z-]+))?$/);
  const q = (what, args) => plugin.host.query(what, args);
  if (req.method === 'OPTIONS') { cors(res); res.writeHead(204); return res.end(); }
  if (p === '/api/info') return send(res, 200, { mode: 'live', plugin: plugin.instance, manifest: WEB_UI_MANIFEST });
  if (p === '/api/stream') {
    cors(res);
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive' });
    const c = { res, id: nextClient++, behind: false };
    res.write(`event: hello\ndata: ${JSON.stringify({ client: c.id })}\n\n`);
    clients.add(c);
    // A disconnect never cancels generation: execution owns the stream.
    req.on('close', () => clients.delete(c));
    return;
  }
  if (p === '/api/streams') {
    // Hydration snapshot (subscribe first, then fetch: frames with a greater
    // seq than the snapshot's apply on top). Clears a client's resync state.
    const id = Number(url.searchParams.get('client'));
    for (const c of clients) if (c.id === id) c.behind = false;
    return send(res, 200, await q('streams', { session_id: url.searchParams.get('session') || '' }));
  }
  if (p === '/api/metrics') return send(res, 200, { runtime: await q('metrics'), gateway: { ...live, clients: clients.size } });
  if (p === '/api/sessions' && req.method === 'GET') return send(res, 200, await q('sessions'));
  if (p === '/api/sessions' && req.method === 'POST') {
    const b = await readBody(req);
    const initial = b.text ? { event_name: 'user-message', payload: { text: b.text }, ui: { v: 1, kind: 'text', role: 'user', text: b.text } } : undefined;
    return send(res, 200, await plugin.host.startSession({ definition: b.definition || 'chat', initial, forkFrom: b.fork_from }));
  }
  if (p === '/api/graph') return send(res, 200, await q('graph'));
  if (p === '/api/services' && req.method === 'GET') return send(res, 200, await q('services'));
  const sm = p.match(/^\/api\/services\/([^/]+)\/([^/]+)$/);
  if (sm && req.method === 'POST') return send(res, 200, await plugin.host.callService(decodeURIComponent(sm[1]), decodeURIComponent(sm[2]), await readBody(req)));
  if (p === '/api/config' && req.method === 'GET') return send(res, 200, await q('config'));
  if (p === '/api/config/apply' && req.method === 'POST') {
    const b = await readBody(req);
    return send(res, 200, await plugin.host.applyConfig(b.config, b.scope || { kind: 'global' }));
  }
  if (m) {
    const [, sid, sub] = m;
    if (!sub && req.method === 'GET') return send(res, 200, await q('session', { session_id: sid }));
    if (sub === 'records') return send(res, 200, await q('records', { session_id: sid }));
    if (sub === 'context') return send(res, 200, await q('context', { session_id: sid, sequence: url.searchParams.has('sequence') ? Number(url.searchParams.get('sequence')) : undefined }));
    if (sub === 'status') return send(res, 200, await q('status', { session_id: sid }));
    if (req.method === 'POST') {
      const b = await readBody(req);
      if (sub === 'commands') return send(res, 200, await plugin.host.command(sid, b.command));
      const cite = await citeFor(sid);
      if (sub === 'messages') {
        return send(res, 200, await plugin.host.publishDeferred('user-message', { text: b.text }, { cite, lane: b.lane || 'normal', ui: { v: 1, kind: 'text', role: 'user', text: b.text } }));
      }
      if (sub === 'actions') return send(res, 200, await plugin.host.publishDeferred('ui-action', { reply_to: b.reply_to, action: b.action, values: b.values || {} }, { cite }));
      if (sub === 'context-edit') return send(res, 200, await plugin.host.publishDeferred('context-edit', { ops: b.ops || [] }, { cite, lane: 'priority' }));
    }
  }
  return null;
}

function serveStatic(req, res, root) {
  const url = new URL(req.url, 'http://local');
  let rel = decodeURIComponent(url.pathname);
  if (rel.endsWith('/')) rel += 'index.html';
  const file = path.resolve(root, '.' + rel);
  if (!file.startsWith(path.resolve(root))) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end('not found'); }
    cors(res);
    res.writeHead(200, { 'content-type': MIME[path.extname(file)] || 'application/octet-stream' });
    res.end(data);
  });
}

definePlugin({
  manifest: WEB_UI_MANIFEST,
  init: async (p) => {
    plugin = p;
    const port = Number(p.config.port || 7700);
    const host = p.config.host || '127.0.0.1';
    const root = path.resolve(p.config.static_dir || 'ui');
    const server = http.createServer(async (req, res) => {
      try {
        if (req.url.startsWith('/api/')) {
          const handled = await route(req, res);
          if (handled === null) send(res, 404, { error: 'no such endpoint' });
        } else if (req.url.startsWith('/plugins/')) {
          // Plugin sources, so the in-browser runtime can also be tried from here.
          serveStatic(req, res, path.resolve('.'));
        } else {
          serveStatic(req, res, root);
        }
      } catch (e) {
        send(res, 400, { error: e.message });
      }
    });
    server.on('error', (e) => p.log('http error:', e.message));
    server.listen(port, host, () => p.log(`web UI on http://${host}:${port}/`));
    await p.host.watch({ streams: true });
    setInterval(() => { for (const c of clients) c.res.write(': ping\n\n'); }, 15000);
  },
  onRecord: (record) => {
    const line = `event: record\ndata: ${JSON.stringify(record)}\n\n`;
    for (const c of clients) c.res.write(line);
  },
  onStream: (messages) => {
    live.frames_in += messages.length;
    // The runtime dropped frames for this gateway: every client re-hydrates.
    if (messages.some((m) => m.type === 'resync-required')) {
      plugin.host.streamResync().catch(() => {});
      for (const c of clients) c.behind = true;
    }
    const line = `event: stream\ndata: ${JSON.stringify(messages.filter((m) => m.type !== 'resync-required'))}\n\n`;
    for (const c of clients) {
      if (c.behind) {
        live.dropped += messages.length;
        if (!c.notified) {
          c.notified = true;
          c.res.write(`event: stream\ndata: ${JSON.stringify([{ type: 'resync-required' }])}\n\n`);
        }
        continue;
      }
      if (c.res.writableLength > LIVE_LIMIT) {
        c.behind = true;
        c.notified = true;
        live.resyncs++;
        c.res.write(`event: stream\ndata: ${JSON.stringify([{ type: 'resync-required' }])}\n\n`);
        continue;
      }
      c.notified = false;
      live.sse_writes++;
      c.res.write(line);
    }
  },
  handlers: {
    'session-started': (ctx) => { if (!cites.has(ctx.sessionId)) cites.set(ctx.sessionId, ctx.invocationId); },
  },
});
