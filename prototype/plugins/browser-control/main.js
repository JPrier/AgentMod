// browser-control: drive a headless Chrome to validate web applications.
//
// Deferred tools (found through tool_search "browser"): navigate, snapshot
// (text + interactive elements with refs), click, type, screenshot (an image
// the model can see), console (page errors and logs), and eval (JavaScript in
// the page). One throwaway Chrome profile per plugin process — no user
// cookies or extensions — and one tab per session. Everything returned is
// untrusted page content. Calls go through policy like any tool: navigating to
// non-local sites is network access (asks in default mode); `browser_eval` is
// an external effect.
//
// Native runtime only. Config: { chrome: "/path/to/chrome" (default: found on
// PATH or CHROME_PATH), cdp_url: "ws://…" (attach to a running Chrome instead),
// args: [] }
import { definePlugin, declareTools, offerTools, ownTools, toolSpec } from '../sdk/agentmod.js';
import { Cdp, findChrome, launchChrome, SNAPSHOT_JS } from '../sdk/cdp.js';

const target = (desc) => ({ ref: { type: 'string', description: `element ref from browser_snapshot (e.g. "e3")${desc ? `; ${desc}` : ''}` }, selector: { type: 'string', description: 'CSS selector (alternative to ref)' } });
const TOOLS = [
  toolSpec('browser_navigate', 'Open a URL in the headless browser (e.g. your dev server at http://localhost:3000) and return the page snapshot.', { url: { type: 'string', description: 'URL' } }, { required: ['url'], tier: 'deferred', group: 'browser', effects: 'network-read', trust: 'external' }),
  toolSpec('browser_snapshot', 'The current page: title, URL, visible text, and interactive elements with refs for click/type.', {}, { tier: 'deferred', group: 'browser', effects: 'read', trust: 'external' }),
  toolSpec('browser_click', 'Click an element.', target(), { tier: 'deferred', group: 'browser', effects: 'varies', trust: 'external' }),
  toolSpec('browser_type', 'Type text into an input (optionally press Enter).', { ...target(), text: { type: 'string', description: 'text to type' }, submit: { type: 'boolean', description: 'press Enter afterwards' } }, { required: ['text'], tier: 'deferred', group: 'browser', effects: 'varies', trust: 'external' }),
  toolSpec('browser_screenshot', 'Screenshot the page; the image is attached for you to see.', { full_page: { type: 'boolean', description: 'capture the whole page, not just the viewport' } }, { tier: 'deferred', group: 'browser', effects: 'read', trust: 'external' }),
  toolSpec('browser_console', 'Console messages and uncaught errors from the page since the last call.', {}, { tier: 'deferred', group: 'browser', effects: 'read', trust: 'external' }),
  toolSpec('browser_eval', 'Evaluate a JavaScript expression in the page and return its JSON value.', { expression: { type: 'string', description: 'JavaScript expression' } }, { required: ['expression'], tier: 'deferred', group: 'browser', effects: 'external', trust: 'external' }),
];
const NAMES = new Set(TOOLS.map((t) => t.name));

let browser = null; // { cdp, close }
const tabs = new Map(); // session -> { sessionId, logs: [] }

async function connect(cfg) {
  if (browser) return browser;
  let wsUrl = cfg.cdp_url;
  let close = () => {};
  if (!wsUrl) {
    const exe = await findChrome(cfg.chrome);
    if (!exe) throw new Error('no Chrome or Chromium found; set `chrome` (or CHROME_PATH) or `cdp_url` in the browser-control config');
    ({ wsUrl, close } = await launchChrome(exe, { extraArgs: cfg.args || [] }));
  }
  const cdp = await Cdp.connect(wsUrl);
  browser = { cdp, close: () => { cdp.close(); close(); } };
  cdp.on((m) => {
    for (const t of tabs.values()) {
      if (m.sessionId !== t.sessionId) continue;
      if (m.method === 'Runtime.consoleAPICalled') t.logs.push(`${m.params.type}: ${(m.params.args || []).map((a) => a.value ?? a.description ?? '').join(' ')}`.slice(0, 500));
      if (m.method === 'Runtime.exceptionThrown') t.logs.push(`uncaught: ${m.params.exceptionDetails?.exception?.description || m.params.exceptionDetails?.text}`.slice(0, 800));
      if (t.logs.length > 200) t.logs.splice(0, t.logs.length - 200);
    }
  });
  return browser;
}

async function tab(cfg, session) {
  const { cdp } = await connect(cfg);
  if (!tabs.has(session)) {
    const { targetId } = await cdp.send('Target.createTarget', { url: 'about:blank' });
    const { sessionId } = await cdp.send('Target.attachToTarget', { targetId, flatten: true });
    await cdp.send('Page.enable', {}, sessionId);
    await cdp.send('Runtime.enable', {}, sessionId);
    tabs.set(session, { sessionId, targetId, logs: [] });
  }
  return { cdp, t: tabs.get(session) };
}

const evaluate = async (cdp, sessionId, expression) => {
  const r = await cdp.send('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true }, sessionId);
  if (r.exceptionDetails) throw new Error(r.exceptionDetails.exception?.description || r.exceptionDetails.text || 'evaluation failed');
  return r.result?.value;
};

const formatSnapshot = (s) => `${s.title || '(untitled)'} — ${s.url}\n\n${s.text}\n\nInteractive elements:\n${(s.elements || []).join('\n') || '(none)'}`;
const locate = (args) => (args.ref ? `document.querySelector('[data-agentmod-ref=${JSON.stringify(String(args.ref))}]')` : `document.querySelector(${JSON.stringify(String(args.selector || ''))})`);

async function run(name, args, cfg, session) {
  const { cdp, t } = await tab(cfg, session);
  const sid = t.sessionId;
  switch (name) {
    case 'browser_navigate': {
      const loaded = cdp.waitFor('Page.loadEventFired', sid, 15_000);
      const nav = await cdp.send('Page.navigate', { url: String(args.url) }, sid);
      if (nav.errorText) throw new Error(`navigation failed: ${nav.errorText}`);
      await loaded;
      return { output: formatSnapshot(await evaluate(cdp, sid, SNAPSHOT_JS)) };
    }
    case 'browser_snapshot':
      return { output: formatSnapshot(await evaluate(cdp, sid, SNAPSHOT_JS)) };
    case 'browser_click': {
      const ok = await evaluate(cdp, sid, `(() => { const el = ${locate(args)}; if (!el) return false; el.scrollIntoView({block:'center'}); el.click(); return true; })()`);
      if (!ok) throw new Error('no such element (take a fresh browser_snapshot)');
      await new Promise((r) => setTimeout(r, 300));
      return { output: formatSnapshot(await evaluate(cdp, sid, SNAPSHOT_JS)) };
    }
    case 'browser_type': {
      const ok = await evaluate(cdp, sid, `(() => { const el = ${locate(args)}; if (!el) return false; el.focus(); return true; })()`);
      if (!ok) throw new Error('no such element (take a fresh browser_snapshot)');
      await cdp.send('Input.insertText', { text: String(args.text ?? '') }, sid);
      if (args.submit) {
        for (const type of ['keyDown', 'keyUp']) await cdp.send('Input.dispatchKeyEvent', { type, key: 'Enter', code: 'Enter', windowsVirtualKeyCode: 13, text: type === 'keyDown' ? '\r' : undefined }, sid);
      }
      await new Promise((r) => setTimeout(r, 300));
      return { output: formatSnapshot(await evaluate(cdp, sid, SNAPSHOT_JS)) };
    }
    case 'browser_screenshot': {
      const shot = await cdp.send('Page.captureScreenshot', { format: 'png', captureBeyondViewport: !!args.full_page }, sid);
      const title = await evaluate(cdp, sid, 'document.title + " — " + location.href');
      return { output: `Screenshot of ${title} attached.`, attachments: [{ type: 'image', media_type: 'image/png', data: shot.data }] };
    }
    case 'browser_console': {
      const logs = t.logs.splice(0);
      return { output: logs.length ? logs.join('\n') : 'No console output since the last call.' };
    }
    case 'browser_eval': {
      const v = await evaluate(cdp, sid, String(args.expression));
      const text = JSON.stringify(v, null, 2) ?? 'undefined';
      return { output: text.length > 20_000 ? `${text.slice(0, 20_000)}… [truncated]` : text };
    }
    default:
      throw new Error(`unknown tool ${name}`);
  }
}

definePlugin({
  manifest: {
    name: 'browser-control',
    version: '0.1.0',
    description: 'Headless Chrome automation (CDP) to validate web apps: navigate, snapshot, click, type, screenshot, console, eval.',
    consumes: [
      { event: 'session-started' },
      { event: 'config-applied' },
      ownTools(NAMES),
    ],
    tools: declareTools(TOOLS),
    emits: [{ event: 'tool-result', supplies: ['call_id', 'name', 'output'] }],
    config_schema: { chrome: 'path (default: PATH / CHROME_PATH)', cdp_url: null, args: [] },
  },
  shutdown: () => browser?.close(),
  handlers: {
    'session-started': (ctx) => offerTools(ctx, TOOLS),
    'config-applied': (ctx) => offerTools(ctx, TOOLS),
    'tool-call': async (ctx) => {
      const { call_id, name, args } = ctx.payload;
      if (!NAMES.has(name)) return;
      const t0 = Date.now();
      try {
        const r = await run(name, args || {}, ctx.config || {}, ctx.sessionId);
        await ctx.publish('tool-result', { call_id, name, output: r.output, trust: 'external', duration_ms: Date.now() - t0, ...(r.attachments ? { attachments: r.attachments } : {}) });
      } catch (e) {
        if (ctx.signal.aborted) throw e;
        await ctx.publish('tool-result', { call_id, name, output: `${name} failed: ${e.message}`, error: true, trust: 'external' });
      }
    },
  },
});
