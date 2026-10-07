import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
// Browser end-to-end test of the static site (in-browser runtime).
// Usage: node tests/browser.mjs [dist-dir]   (CHROME_PATH=/path/to/chrome)
// Serves dist/, starts the mock OpenRouter, and drives the UI headlessly:
// key gate, tool loop, approval + sub-agent, inspector tabs, hard stop, crash & recover.
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import puppeteer from 'puppeteer-core';

const here = path.dirname(fileURLToPath(import.meta.url));
const ROOT = process.argv[2] || path.join(here, '..', 'dist');
const URLARG = process.argv[3];
const SHOTS = process.env.SHOTS_DIR || path.join(here, '..', 'target', 'shots');
fs.mkdirSync(SHOTS, { recursive: true });
const mock = spawn(process.execPath, [path.join(here, 'mock-openrouter.mjs'), '8765'], { stdio: 'ignore' });
let failed = false;
async function chromePath() {
  if (process.env.CHROME_PATH) return process.env.CHROME_PATH;
  for (const p of ['/usr/bin/google-chrome', '/usr/bin/chromium', '/usr/bin/chromium-browser']) if (fs.existsSync(p)) return p;
  const { default: chromium } = await import('@sparticuz/chromium');
  return chromium.executablePath();
}
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.wasm': 'application/wasm' };
const server = http.createServer((req, res) => {
  let p = decodeURIComponent(new URL(req.url, 'http://x').pathname);
  if (p.endsWith('/')) p += 'index.html';
  const f = path.join(ROOT, p);
  fs.readFile(f, (e, d) => { if (e) { res.writeHead(404); return res.end(); } res.writeHead(200, { 'content-type': MIME[path.extname(f)] || 'application/octet-stream' }); res.end(d); });
});
await new Promise((r) => server.listen(8099, r));
await new Promise((r) => setTimeout(r, 500));
const browser = await puppeteer.launch({ executablePath: await chromePath(), args: ['--no-sandbox', '--disable-gpu'], headless: true, defaultViewport: { width: 1440, height: 900 } });
const page = await browser.newPage();
const errors = [];
page.on('console', (m) => { if (m.type() === 'error' || m.type() === 'warning') errors.push(`${m.type()}: ${m.text()}`); });
page.on('pageerror', (e) => errors.push('pageerror: ' + e.message));
const shot = (n) => page.screenshot({ path: path.join(SHOTS, `${n}.png`) });
const waitText = async (t, ms = 20000) => page.waitForFunction((t) => document.body.innerText.includes(t), { timeout: ms }, t);
try {
  await page.goto(URLARG || 'http://localhost:8099/?host=browser&openrouter_base=http://127.0.0.1:8765/api/v1');
  if (!URLARG) {
    await page.waitForSelector('.gate-card input[type=password]', { timeout: 15000 });
    // The model list comes from the provider's /models endpoint, not the page.
    await page.waitForFunction(() => document.querySelectorAll('.gate-card datalist option').length === 2, { timeout: 10000 });
    const opts = await page.$$eval('.gate-card datalist option', (o) => o.map((x) => x.value));
    if (opts.join() !== 'mock/tool-model,openai/gpt-4o-mini') throw new Error(`unexpected model options ${opts}`);
    await page.click('.gate-card .inline-check input');
    await page.waitForFunction(() => document.querySelectorAll('.gate-card datalist option').length === 3);
    await shot('00-gate');
    const modelInput = '.gate-card input[aria-label="OpenRouter model"]';
    await page.$eval(modelInput, (el) => { el.value = ''; });
    await page.type(modelInput, 'nobody/not-a-model');
    await page.type('.gate-card input[type=password]', 'test-key');
    await page.click('.gate-card button.btn.primary');
    await waitText('not in OpenRouter');
    await page.waitForFunction(() => document.querySelectorAll('.gate-card datalist option').length >= 2);
    await page.$eval(modelInput, (el) => { el.value = ''; });
    await page.type(modelInput, 'mock/tool-model');
    await page.type('.gate-card input[type=password]', 'wrong-key');
    await page.click('.gate-card button.btn.primary');
    await waitText('rejected that key');
    await page.$eval(modelInput, (el) => { el.value = 'mock/tool-model'; });
    await page.type('.gate-card input[type=password]', 'test-key');
    await page.click('.gate-card button.btn.primary');
  }
  await page.waitForSelector('.chat-head', { timeout: 30000 });
  if (!URLARG) {
    const label = await page.$eval('.provider .btn', (b) => b.textContent);
    if (!label.includes('mock/tool-model')) throw new Error(`chosen model not applied: ${label}`);
  }
  await shot('01-boot');
  await page.click('.suggest button:nth-child(2)');
  await waitText('The tool returned: 294');
  await new Promise((r) => setTimeout(r, 800));
  await shot('02-calc');
  // Delegate with approval.
  await page.type('#composer', 'Delegate: summarize the launch plan');
  await page.keyboard.press('Enter');
  await page.waitForSelector('.choice .btn.primary', { timeout: 20000 });
  await shot('03-approval');
  await page.click('.choice .btn.primary');
  await waitText('reported back', 20000);
  await new Promise((r) => setTimeout(r, 1500));
  await shot('04-delegate');
  // Redaction + titles.
  await page.type('#composer', 'remember that the deploy is Friday');
  await page.keyboard.press('Enter');
  await waitText('The tool returned: Saved');
  await new Promise((r) => setTimeout(r, 1500));
  // Inspector tabs.
  for (const [i, tab] of ['Context', 'Graph', 'Config', 'Log', 'Pipeline'].entries()) {
    const btns = await page.$$('.tabs button');
    for (const b of btns) if ((await b.evaluate((x) => x.textContent)) === tab) await b.click();
    await new Promise((r) => setTimeout(r, 400));
    await shot(`05-tab-${i}-${tab}`);
  }
  // Expand an event row.
  await (await page.$$('.ev'))[2].click();
  await new Promise((r) => setTimeout(r, 300));
  await shot('06-expanded');
  // Hard stop mid-stream, then resume.
  await page.type('#composer', 'tell me about yourself in detail');
  await page.keyboard.press('Enter');
  await new Promise((r) => setTimeout(r, 400));
  await page.evaluate(() => [...document.querySelectorAll('.controls button')].find((b) => b.textContent === 'Hard stop').click());
  await waitText('Hard stop by');
  await shot('07-hardstop');
  await page.evaluate(() => [...document.querySelectorAll('.controls button')].find((b) => b.textContent === 'Resume').click());
  // Crash & recover mid-stream.
  await page.type('#composer', 'hello once more, tell me everything you can');
  await page.keyboard.press('Enter');
  await new Promise((r) => setTimeout(r, 500));
  await page.evaluate(() => [...document.querySelectorAll('.topbar button')].find((b) => b.textContent.startsWith('Crash')).click());
  await waitText('replayed the logs', 10000);
  await new Promise((r) => setTimeout(r, 4000));
  await shot('08-recovered');
  const lastMsg = await page.evaluate(() => [...document.querySelectorAll('.msg.assistant')].pop()?.innerText);
  console.log('last assistant message after recovery:', lastMsg?.slice(0, 160));
  // Mobile.
  await page.setViewport({ width: 390, height: 844 });
  await new Promise((r) => setTimeout(r, 500));
  await shot('09-mobile');
} catch (e) {
  failed = true;
  console.log('TEST FAILURE:', e.message);
  await shot('99-failure');
} finally {
  console.log('console errors:', errors.length ? errors.join('\n') : 'none');
  await browser.close();
  server.close();
  mock.kill();
  const real = errors.filter((e) => !/ERR_TUNNEL|ERR_NAME|fonts\.g|status of 401|\[subagent\]/.test(e));
  if (real.length) failed = true;
  console.log(failed ? 'browser e2e: FAILED' : 'browser e2e: all checks passed');
  process.exit(failed ? 1 : 0);
}
