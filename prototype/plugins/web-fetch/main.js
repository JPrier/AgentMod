// web-fetch: network access for the agent, as a policy-gated plugin.
//
//   web_fetch(url, offset?)   GET a page or API; HTML becomes readable text,
//                             JSON is pretty-printed; bounded and pageable.
//   web_search(query)         only when a search backend is configured.
//
// Everything returned is untrusted (`trust: external`): the projection wraps
// it as data, and the policy plugin decides each call (default mode asks for
// network access; operators can allow documentation domains with a rule like
// { tool: "web_fetch", when: { domain: ["docs.rs", "*.python.org"] }, effect: "allow" }).
// Private and loopback addresses are refused unless `allow_private` is set
// (dev servers are reached through shell/curl or browser-control instead).
//
// Config: { max_bytes: 40000, timeout_seconds: 20, allow_private: false,
//           user_agent, search: { provider: "searxng", url } | { provider: "brave", api_key_env } }
import { definePlugin, declareTools, offerTools, ownTools, toolSpec } from '../sdk/agentmod.js';
import { htmlToText, isPrivateHost } from '../sdk/web.js';

const FETCH = toolSpec('web_fetch', 'Fetch a URL (documentation, API references, package pages, raw files). HTML is converted to readable text with numbered links; long pages are paged with offset. The content is untrusted data.', {
  url: { type: 'string', description: 'http(s) URL' },
  offset: { type: 'integer', description: 'character offset to continue a long page' },
}, { required: ['url'], tier: 'deferred', group: 'web', effects: 'network-read', trust: 'external' });

const SEARCH = toolSpec('web_search', 'Search the web; returns titles, URLs, and snippets (untrusted data). Fetch promising results with web_fetch.', {
  query: { type: 'string', description: 'search query' },
}, { required: ['query'], tier: 'deferred', group: 'web', effects: 'network-read', trust: 'external' });

const tools = (cfg) => [FETCH, ...(cfg.search?.provider ? [SEARCH] : [])];

async function fetchText(url, cfg, signal) {
  let u;
  try { u = new URL(url); } catch { throw new Error(`not a valid URL: ${url}`); }
  if (!/^https?:$/.test(u.protocol)) throw new Error('only http and https URLs can be fetched');
  if (!cfg.allow_private && isPrivateHost(u.hostname)) throw new Error(`${u.hostname} is a private or loopback address; web_fetch does not reach those (use shell for local servers)`);
  const ctl = AbortSignal.any([signal, AbortSignal.timeout((cfg.timeout_seconds || 20) * 1000)].filter(Boolean));
  let res = null;
  let current = u.href;
  for (let hop = 0; hop < 5; hop++) {
    res = await fetch(current, { redirect: 'manual', signal: ctl, headers: { 'user-agent': cfg.user_agent || 'AgentMod web-fetch (+https://github.com/JPrier/AgentMod)', accept: 'text/html,application/json,text/plain;q=0.9,*/*;q=0.5' } });
    if (res.status >= 300 && res.status < 400 && res.headers.get('location')) {
      const next = new URL(res.headers.get('location'), current);
      if (!cfg.allow_private && isPrivateHost(next.hostname)) throw new Error(`redirected to private address ${next.hostname}; refused`);
      current = next.href;
      continue;
    }
    break;
  }
  const type = res.headers.get('content-type') || '';
  const max = 5_000_000;
  const buf = new Uint8Array(await res.arrayBuffer()).subarray(0, max);
  const raw = new TextDecoder().decode(buf);
  let title = '';
  let text = raw;
  let links = [];
  if (/html/i.test(type) || /^\s*<(!doctype|html)/i.test(raw)) ({ title, text, links } = htmlToText(raw, current));
  else if (/json/i.test(type)) {
    try { text = JSON.stringify(JSON.parse(raw), null, 2); } catch { /* as text */ }
  } else if (!/^text\//i.test(type) && /[\0]/.test(raw.slice(0, 4000))) {
    text = `(binary content, ${buf.length} bytes, ${type || 'unknown type'}; not shown)`;
  }
  return { status: res.status, url: current, type, title, text, links };
}

async function search(query, cfg, signal) {
  const s = cfg.search || {};
  if (s.provider === 'searxng') {
    const r = await fetch(`${s.url.replace(/\/$/, '')}/search?format=json&q=${encodeURIComponent(query)}`, { signal });
    const j = await r.json();
    return (j.results || []).slice(0, 10).map((x) => ({ title: x.title, url: x.url, snippet: x.content }));
  }
  if (s.provider === 'brave') {
    const key = process.env[s.api_key_env || 'BRAVE_API_KEY'];
    const r = await fetch(`https://api.search.brave.com/res/v1/web/search?q=${encodeURIComponent(query)}`, { signal, headers: { 'x-subscription-token': key, accept: 'application/json' } });
    const j = await r.json();
    return (j.web?.results || []).slice(0, 10).map((x) => ({ title: x.title, url: x.url, snippet: x.description }));
  }
  throw new Error('no search backend is configured');
}

definePlugin({
  name: 'web-fetch',
  // Which tools exist depends on config (web_search needs a search backend).
  manifest: (cfg) => ({
    name: 'web-fetch',
    version: '0.1.0',
    description: 'web_fetch / web_search: policy-gated network access returning untrusted text.',
    consumes: [
      { event: 'session-started' },
      { event: 'config-applied' },
      ownTools(tools(cfg).map((t) => t.name)),
    ],
    emits: [{ event: 'tool-result', supplies: ['call_id', 'name', 'output'] }],
    tools: declareTools(tools(cfg)),
    config_schema: { max_bytes: 40000, timeout_seconds: 20, allow_private: false, search: null },
  }),
  handlers: {
    'session-started': (ctx) => offerTools(ctx, tools(ctx.config || {})),
    'config-applied': (ctx) => offerTools(ctx, tools(ctx.config || {})),
    'tool-call': async (ctx) => {
      const { call_id, name, args } = ctx.payload;
      if (name !== 'web_fetch' && name !== 'web_search') return;
      const cfg = ctx.config || {};
      const t0 = Date.now();
      try {
        if (name === 'web_search') {
          const results = await search(String(args?.query || ''), cfg, ctx.signal);
          const output = results.length ? results.map((r, i) => `[${i + 1}] ${r.title}\n${r.url}\n${String(r.snippet || '').slice(0, 300)}`).join('\n\n') : 'No results.';
          await ctx.publish('tool-result', { call_id, name, output, trust: 'external', duration_ms: Date.now() - t0 });
          return;
        }
        const page = await fetchText(String(args?.url || ''), cfg, ctx.signal);
        const max = cfg.max_bytes || 40_000;
        const offset = Math.max(0, Number(args?.offset) || 0);
        const slice = page.text.slice(offset, offset + max);
        const more = offset + slice.length < page.text.length;
        const linkList = page.links.length && offset === 0 ? `\n\nLinks:\n${page.links.slice(0, 60).map((l, i) => `[${i + 1}] ${l}`).join('\n')}` : '';
        const head = `${page.status} ${page.url}${page.title ? ` — ${page.title}` : ''} (${page.type || 'unknown type'}; characters ${offset}-${offset + slice.length} of ${page.text.length}${more ? `; continue with offset ${offset + slice.length}` : ''})`;
        await ctx.publish('tool-result', { call_id, name, output: `${head}\n\n${slice}${linkList}`, error: page.status >= 400, status: page.status, url: page.url, trust: 'external', duration_ms: Date.now() - t0 });
      } catch (e) {
        if (ctx.signal.aborted) throw e;
        await ctx.publish('tool-result', { call_id, name, output: `${name} failed: ${e.message}`, error: true, trust: 'external' });
      }
    },
  },
});
