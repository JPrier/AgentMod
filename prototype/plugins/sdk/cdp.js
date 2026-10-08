// A minimal Chrome DevTools Protocol client (Node 22+: global WebSocket) and a
// headless-Chrome launcher. Used by the browser-control plugin.

export class Cdp {
  constructor(ws) {
    this.ws = ws;
    this.next = 1;
    this.pending = new Map();
    this.listeners = new Set();
    ws.addEventListener('message', (e) => {
      const msg = JSON.parse(typeof e.data === 'string' ? e.data : new TextDecoder().decode(e.data));
      if (msg.id != null) {
        const p = this.pending.get(msg.id);
        if (!p) return;
        this.pending.delete(msg.id);
        if (msg.error) p.reject(new Error(`CDP ${p.method}: ${msg.error.message}`));
        else p.resolve(msg.result);
      } else {
        for (const l of this.listeners) l(msg);
      }
    });
    ws.addEventListener('close', () => {
      for (const p of this.pending.values()) p.reject(new Error('the browser connection closed'));
      this.pending.clear();
    });
  }

  static async connect(url) {
    const ws = new WebSocket(url);
    await new Promise((resolve, reject) => {
      ws.addEventListener('open', resolve, { once: true });
      ws.addEventListener('error', () => reject(new Error(`cannot connect to the browser at ${url}`)), { once: true });
    });
    return new Cdp(ws);
  }

  send(method, params = {}, sessionId, timeoutMs = 30_000) {
    const id = this.next++;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`CDP ${method} timed out`)); }, timeoutMs);
      this.pending.set(id, { method, resolve: (v) => { clearTimeout(timer); resolve(v); }, reject: (e) => { clearTimeout(timer); reject(e); } });
      this.ws.send(JSON.stringify({ id, method, params, ...(sessionId ? { sessionId } : {}) }));
    });
  }

  on(fn) {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  /** Wait for an event (optionally for one session), or time out. */
  waitFor(method, sessionId, timeoutMs = 15_000) {
    return new Promise((resolve) => {
      const timer = setTimeout(() => { off(); resolve(null); }, timeoutMs);
      const off = this.on((m) => {
        if (m.method === method && (!sessionId || m.sessionId === sessionId)) { clearTimeout(timer); off(); resolve(m.params); }
      });
    });
  }

  close() {
    try { this.ws.close(); } catch { /* gone */ }
  }
}

const CANDIDATES = ['google-chrome', 'google-chrome-stable', 'chromium', 'chromium-browser', 'chrome', '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', '/Applications/Chromium.app/Contents/MacOS/Chromium'];

/** Find a Chrome/Chromium executable. */
export async function findChrome(preferred) {
  const { execFileSync } = await import('node:child_process');
  const fs = await import('node:fs');
  for (const c of [preferred, process.env.CHROME_PATH, ...CANDIDATES].filter(Boolean)) {
    if (c.includes('/') ? fs.existsSync(c) : (() => { try { execFileSync('sh', ['-c', `command -v ${JSON.stringify(c)}`], { stdio: 'ignore' }); return true; } catch { return false; } })()) return c;
  }
  return null;
}

/** Launch headless Chrome with a throwaway profile; returns { wsUrl, close }. */
export async function launchChrome(executable, { extraArgs = [] } = {}) {
  const { spawn } = await import('node:child_process');
  const fs = await import('node:fs');
  const os = await import('node:os');
  const path = await import('node:path');
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'agentmod-chrome-'));
  const child = spawn(executable, ['--headless=new', '--remote-debugging-port=0', `--user-data-dir=${profile}`, '--no-first-run', '--no-default-browser-check', '--disable-extensions', '--disable-sync', ...extraArgs, 'about:blank'], { stdio: 'ignore' });
  const portFile = path.join(profile, 'DevToolsActivePort');
  for (let i = 0; i < 100; i++) {
    if (fs.existsSync(portFile)) {
      const [port, wsPath] = fs.readFileSync(portFile, 'utf8').trim().split('\n');
      if (port && wsPath) return { wsUrl: `ws://127.0.0.1:${port}${wsPath}`, close: () => { child.kill(); fs.rmSync(profile, { recursive: true, force: true }); } };
    }
    if (child.exitCode !== null) break;
    await new Promise((r) => setTimeout(r, 100));
  }
  child.kill();
  throw new Error(`Chrome did not start (${executable})`);
}

/** In-page script: a compact, bounded snapshot with stable element refs. */
export const SNAPSHOT_JS = `(() => {
  const max = 12000;
  const vis = (el) => { const r = el.getBoundingClientRect(); const s = getComputedStyle(el); return r.width > 0 && r.height > 0 && s.visibility !== 'hidden' && s.display !== 'none'; };
  let n = 0;
  const items = [];
  for (const el of document.querySelectorAll('a[href],button,input,select,textarea,[role=button],[role=link],[contenteditable=true],[onclick]')) {
    if (!vis(el)) continue;
    if (!el.dataset.agentmodRef) el.dataset.agentmodRef = 'e' + (++window.__agentmodRefs || (window.__agentmodRefs = 1));
    const name = (el.getAttribute('aria-label') || el.innerText || el.value || el.placeholder || el.title || el.name || '').replace(/\\s+/g, ' ').trim().slice(0, 80);
    items.push(el.dataset.agentmodRef + ' ' + el.tagName.toLowerCase() + (el.type ? '[' + el.type + ']' : '') + (name ? ' "' + name + '"' : ''));
    if (++n >= 150) break;
  }
  const text = (document.body?.innerText || '').replace(/\\n{3,}/g, '\\n\\n').slice(0, max);
  return { title: document.title, url: location.href, text, elements: items };
})()`;
