// Browser end-to-end check of the Linux sandbox: the real CheerpX in headless
// Chrome, driven through the real UI and plugin path (coder session → mock
// model emits a tool call → linux-sandbox worker → CheerpX VM → tool-result).
//
// Needs network access to Leaning Technologies' CDN (cxrtnc.leaningtech.com)
// and the WebVM disk server (disks.webvm.io). It is the feasibility gate from
// the design: boot, command execution, exit codes, file round-trips, a native
// compile, and persistence across a reload. Results go to
// $SHOTS_DIR/sandbox-report.json (and the CI job summary).
//
// Usage: node tests/sandbox-browser.mjs [dist-dir]   (CHROME_PATH=/path/to/chrome)
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import puppeteer from 'puppeteer-core';

const here = path.dirname(fileURLToPath(import.meta.url));
const ROOT = process.argv[2] || path.join(here, '..', 'dist');
const SHOTS = process.env.SHOTS_DIR || path.join(here, '..', 'target', 'shots');
const BOOT_MS = Number(process.env.SANDBOX_BOOT_MS || 600000);
fs.mkdirSync(SHOTS, { recursive: true });

const report = { started: new Date().toISOString(), checks: {}, notes: [] };
const check = (name, ok, detail) => {
  report.checks[name] = { ok, ...(detail ? { detail } : {}) };
  console.log(`${ok ? 'ok  ' : 'FAIL'} ${name}${detail ? ` — ${detail}` : ''}`);
};

const mock = spawn(process.execPath, [path.join(here, 'mock-openrouter.mjs'), '8765'], { stdio: 'ignore' });
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.wasm': 'application/wasm' };
// Serve with the isolation headers directly (as a header-capable host would).
const server = http.createServer((req, res) => {
  let p = decodeURIComponent(new URL(req.url, 'http://x').pathname);
  if (p.endsWith('/')) p += 'index.html';
  const f = path.join(ROOT, p);
  fs.readFile(f, (e, d) => {
    if (e) { res.writeHead(404); return res.end(); }
    res.writeHead(200, {
      'content-type': MIME[path.extname(f)] || 'application/octet-stream',
      'cross-origin-opener-policy': 'same-origin',
      'cross-origin-embedder-policy': 'require-corp',
      'cross-origin-resource-policy': 'same-origin',
    });
    res.end(d);
  });
});
await new Promise((r) => server.listen(8098, r));

async function chromePath() {
  if (process.env.CHROME_PATH) return process.env.CHROME_PATH;
  for (const p of ['/usr/bin/google-chrome', '/usr/bin/chromium', '/usr/bin/chromium-browser']) if (fs.existsSync(p)) return p;
  const { default: chromium } = await import('@sparticuz/chromium');
  return chromium.executablePath();
}

const browser = await puppeteer.launch({ executablePath: await chromePath(), args: ['--no-sandbox', '--disable-gpu'], headless: true, defaultViewport: { width: 1440, height: 900 } });
const page = await browser.newPage();
const logs = [];
page.on('console', (m) => logs.push(`${m.type()}: ${m.text()}`));
page.on('pageerror', (e) => logs.push('pageerror: ' + e.message));
const shot = (n) => page.screenshot({ path: path.join(SHOTS, `sandbox-${n}.png`) });
const bodyHas = (t, ms) => page.waitForFunction((t) => document.body.innerText.includes(t), { timeout: ms }, t).then(() => true, () => false);

async function newCoderSession() {
  await page.waitForSelector('.new-session select', { timeout: 60000 });
  await page.select('.new-session select', 'coder');
  await page.evaluate(() => document.querySelector('.new-session .btn.primary').click());
  await page.waitForFunction(() => document.querySelector('.chat-head .sub')?.textContent.includes('coder'), { timeout: 20000 });
}

async function say(text) {
  // Set and submit in one step: the thread can re-render while typing.
  await page.waitForSelector('#composer');
  await page.evaluate((t) => { const ta = document.querySelector('#composer'); ta.value = t; ta.form.requestSubmit(); }, text);
}

/** Send a direct tool call (the mock model echoes "tool <name> <json>") and wait for its result row. */
async function tool(name, args, expect, ms = 180000) {
  const before = await page.$$eval('.tool', (t) => t.length);
  await say(`tool ${name} ${JSON.stringify(args)}`);
  try {
    await page.waitForFunction((n) => {
      const rows = [...document.querySelectorAll('.tool')].slice(n);
      return rows.some((r) => /done|failed/.test(r.querySelector('.pill')?.textContent || ''));
    }, { timeout: ms }, before);
  } catch {
    return { ok: false, out: 'no tool result before timeout' };
  }
  // The row carries the tool's full output (tool-result payload) in its title.
  const out = await page.$$eval('.tool', (rows, n) => rows.slice(n).map((r) => `${r.querySelector('.pill')?.textContent} ${r.querySelector('.res')?.title || ''}`).join('\n'), before);
  return { ok: expect.test(out), out: out.slice(0, 400) };
}

let failed = false;
try {
  await page.goto('http://localhost:8098/?host=browser&openrouter_base=http://127.0.0.1:8765/api/v1');
  await page.waitForSelector('.gate-card input[type=password]', { timeout: 30000 });
  await page.waitForFunction(() => document.querySelectorAll('.gate-card datalist option').length >= 2, { timeout: 15000 });
  const modelInput = '.gate-card input[aria-label="OpenRouter model"]';
  await page.$eval(modelInput, (el) => { el.value = ''; });
  await page.type(modelInput, 'mock/tool-model');
  await page.type('.gate-card input[type=password]', 'test-key');
  await page.click('.gate-card button.btn.primary');
  await page.waitForSelector('.chat-head', { timeout: 60000 });
  check('page is cross-origin isolated', await page.evaluate(() => crossOriginIsolated));
  await newCoderSession();
  check('coder session shows the sandbox notice', await bodyHas('Linux sandbox:', 10000));
  await shot('01-coder');

  const t0 = Date.now();
  let r = await tool('run', { command: 'uname -m; echo hello-from-cheerpx' }, /hello-from-cheerpx/, BOOT_MS);
  report.boot_ms = Date.now() - t0;
  check('boot + first command (stdout, exit code)', r.ok, `${Math.round(report.boot_ms / 1000)}s; ${r.out}`);
  await shot('02-first-command');
  if (!r.ok) throw new Error('sandbox did not run a command; see logs');

  r = await tool('run', { command: 'echo to-stderr >&2; exit 7' }, /exit code 7[\s\S]*to-stderr/);
  check('non-zero exit and stderr are separate', r.ok, r.out);
  r = await tool('write_file', { path: 'hello.c', content: '#include <stdio.h>\nint main(void){puts("hi from c");return 0;}\n' }, /Created \/workspace\/hello\.c/);
  check('write_file into /workspace', r.ok, r.out);
  r = await tool('edit_file', { path: 'hello.c', old_text: 'hi from c', new_text: 'hi from gcc in the browser' }, /Edited \/workspace\/hello\.c/);
  check('edit_file (read back, write)', r.ok, r.out);
  r = await tool('run', { command: 'gcc hello.c -o hello && ./hello' }, /hi from gcc in the browser/, 300000);
  check('native compile and run (gcc)', r.ok, r.out);
  r = await tool('run', { command: 'python3 -c "print(6*7)"' }, /\b42\b/);
  check('python3', r.ok, r.out);
  r = await tool('run', { command: 'git --version && cd /workspace && git init -q demo && echo ok-git' }, /ok-git/);
  check('git', r.ok, r.out);
  // Diagnostics: how signals behave in the guest (each line: what, exit code, seconds waited).
  r = await tool('run', { command: [
    "w() { s=$(date +%s); wait $1; echo \"$2 rc=$? waited=$(( $(date +%s) - s ))s\"; }",
    'sleep 6 & p=$!; kill -TERM $p; w $p term-sleep',
    'sleep 6 & p=$!; kill -KILL $p; w $p kill-sleep',
    'yes >/dev/null & p=$!; sleep 1; kill -TERM $p; w $p term-busy-syscalls',
    'python3 -c "while True: pass" & p=$!; sleep 1; kill -TERM $p; w $p term-busy-cpu',
    "bash -c 'trap \"exit 3\" TERM; sleep 6 & wait' & p=$!; sleep 1; kill -TERM $p; w $p trap-term",
    's=$(date +%s); timeout 2 sleep 6; echo "gnu-timeout rc=$? waited=$(( $(date +%s) - s ))s"',
  ].join('\n'), timeout_seconds: 120 }, /term-sleep rc=143 waited=[01]s/, 180000);
  report.signals = r.out;
  check('signals end a sleeping process promptly', r.ok, r.out);
  const t1 = Date.now();
  r = await tool('run', { command: 'sleep 30', timeout_seconds: 3 }, /timed out after 3s/, 120000);
  check('timeout kills a command', r.ok && Date.now() - t1 < 25000, `${Math.round((Date.now() - t1) / 1000)}s; ${r.out}`);
  r = await tool('run', { command: 'node -e "console.log(6*7)" && node --version' }, /\b42\b/);
  check('node', r.ok, r.out);
  r = await tool('list_files', {}, /hello\.c/);
  check('list_files', r.ok, r.out);
  await shot('03-work');

  // Persistence: reload the page; the workspace lives in IndexedDB.
  await page.reload();
  await page.waitForSelector('.chat-head', { timeout: 60000 });
  await newCoderSession();
  r = await tool('read_file', { path: 'hello.c' }, /hi from gcc in the browser/, BOOT_MS);
  check('workspace persists across reload', r.ok, r.out);
  await shot('04-after-reload');
} catch (e) {
  failed = true;
  report.error = e.message;
  console.log('TEST FAILURE:', e.message);
  await shot('99-failure').catch(() => {});
} finally {
  report.finished = new Date().toISOString();
  report.console = logs.slice(-60);
  report.thread = await page.evaluate(() => document.querySelector('.thread')?.innerText.slice(-1500)).catch(() => null);
  failed ||= Object.values(report.checks).some((c) => !c.ok);
  report.ok = !failed;
  fs.writeFileSync(path.join(SHOTS, 'sandbox-report.json'), JSON.stringify(report, null, 2));
  if (process.env.GITHUB_ACTIONS) {
    // Annotations: readable from the checks API without downloading logs.
    const esc = (t) => String(t).replace(/%/g, '%25').replace(/\r/g, '').replace(/\n/g, '%0A').slice(0, 4000);
    const lines = Object.entries(report.checks).map(([k, v]) => `${v.ok ? 'ok' : 'FAIL'} ${k}${v.detail ? ` — ${v.detail.slice(0, k.startsWith("signals") ? 900 : 200)}` : ''}`);
    console.log(`::${failed ? 'error' : 'notice'} title=Linux sandbox (CheerpX)::${esc([report.error ? `error: ${report.error}` : null, ...lines].filter(Boolean).join('\n'))}`);
    if (failed) {
      console.log(`::warning title=Linux sandbox console::${esc(report.console.join('\n').slice(-2500) || '(no console output)')}`);
      console.log(`::warning title=Linux sandbox thread::${esc(report.thread || '(no thread)')}`);
    }
  }
  if (process.env.GITHUB_STEP_SUMMARY) {
    const rows = Object.entries(report.checks).map(([k, v]) => `| ${v.ok ? '✅' : '❌'} | ${k} | ${(v.detail || '').replace(/\|/g, '\\|').replace(/\n/g, ' ').slice(0, 160)} |`);
    fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, `### Linux sandbox (CheerpX) feasibility\n\n| | check | detail |\n|---|---|---|\n${rows.join('\n')}\n\n${report.error ? `Error: ${report.error}\n` : ''}`);
  }
  await browser.close();
  server.close();
  mock.kill();
  console.log(failed ? 'sandbox e2e: FAILED' : 'sandbox e2e: all checks passed');
  process.exit(failed ? 1 : 0);
}
