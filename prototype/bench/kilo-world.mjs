// The Kilo benchmark's world, served by the test-only mock provider:
//
// * a fake GitHub (API, raw files, tarball) backed by the vendored snapshot in
//   bench/tasks/*/github/<owner>/<repo>/, so `import_repo` and the minimal
//   loop's download behave the same offline, before and after a change;
// * a scripted model for the task "Import the GitHub repo antirez/kilo, build
//   it with make, and explain how it draws the screen." It is NOT a real
//   model. It reproduces habits that cost turns in real runs — a plan update
//   as its own turn, probing the environment, a discovery turn for an obvious
//   tool, one read per turn, a search with a file as its path, re-reading —
//   and, in `adaptive` mode, takes the shortcut the harness offers when it is
//   visible in the request (environment facts, deferred-tool signatures,
//   batching and plan guidance, an actionable error). `fixed` mode never
//   adapts, so before/after runs follow an identical trajectory and differ
//   only in what the harness itself costs.
//
// Token usage is simulated: prompt tokens ≈ request characters / 4, cached
// tokens = the prefix shared with the previous request of the conversation
// (how provider prompt caches behave), and a gpt-4o-mini-like price.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const TASK_RE = /antirez\/kilo/;

// ---------------------------------------------------------------------------
// Fake GitHub
// ---------------------------------------------------------------------------

function repos() {
  const out = new Map();
  const tasks = path.join(here, 'tasks');
  for (const t of fs.existsSync(tasks) ? fs.readdirSync(tasks) : []) {
    const gh = path.join(tasks, t, 'github');
    if (!fs.existsSync(gh)) continue;
    for (const owner of fs.readdirSync(gh)) for (const repo of fs.readdirSync(path.join(gh, owner))) out.set(`${owner}/${repo}`, path.join(gh, owner, repo));
  }
  return out;
}

function files(dir) {
  const out = [];
  const walk = (d, rel) => {
    for (const n of fs.readdirSync(d).sort()) {
      const p = path.join(d, n);
      const r = rel ? `${rel}/${n}` : n;
      if (fs.statSync(p).isDirectory()) walk(p, r);
      else out.push({ path: r, abs: p, size: fs.statSync(p).size });
    }
  };
  walk(dir, '');
  return out;
}

/** Handle /gh-api, /gh-raw, /gh-tar; returns true when it answered. */
export function serveGithub(req, res) {
  const url = new URL(req.url, 'http://x');
  const m = url.pathname.match(/^\/(gh-api|gh-raw|gh-tar)\/(.*)$/);
  if (!m) return false;
  const [, kind, rest] = m;
  const all = repos();
  const json = (code, o) => { res.writeHead(code, { 'content-type': 'application/json' }); res.end(JSON.stringify(o)); };
  if (kind === 'gh-api') {
    const r = rest.match(/^repos\/([^/]+)\/([^/]+)(?:\/(commits|git\/trees)\/([^?]+))?/);
    const dir = r && all.get(`${r[1]}/${r[2]}`);
    if (!dir) return json(404, { message: 'Not Found' }), true;
    const fl = files(dir);
    const sha = crypto.createHash('sha1').update(fl.map((f) => f.path + f.size).join()).digest('hex');
    if (!r[3]) return json(200, { default_branch: 'master', full_name: `${r[1]}/${r[2]}` }), true;
    if (r[3] === 'commits') return json(200, { sha, commit: { tree: { sha: `t${sha}` } } }), true;
    return json(200, { truncated: false, tree: fl.map((f) => ({ path: f.path, type: 'blob', mode: '100644', size: f.size })) }), true;
  }
  if (kind === 'gh-raw') {
    const [owner, repo, , ...p] = rest.split('/');
    const dir = all.get(`${owner}/${repo}`);
    const f = dir && path.join(dir, ...p.map(decodeURIComponent));
    if (!f || !f.startsWith(dir) || !fs.existsSync(f)) { res.writeHead(404); res.end(); return true; }
    res.writeHead(200);
    res.end(fs.readFileSync(f));
    return true;
  }
  const [owner, repo] = rest.split('/');
  const dir = all.get(`${owner}/${repo}`);
  if (!dir) { res.writeHead(404); res.end(); return true; }
  const tar = spawnSync('tar', ['czf', '-', '-C', path.dirname(dir), '--transform', `s,^${repo},${repo}-master,`, repo]);
  res.writeHead(200, { 'content-type': 'application/gzip' });
  res.end(tar.stdout);
  return true;
}

// ---------------------------------------------------------------------------
// The scripted model
// ---------------------------------------------------------------------------

export const isKiloTask = (messages) => messages.some((m) => m.role === 'user' && TASK_RE.test(String(m.content || '')));

const sys = (j) => j.messages.filter((m) => m.role === 'system').map((m) => (typeof m.content === 'string' ? m.content : (m.content || []).map((c) => c.text).join(''))).join('\n');

/** What the harness makes visible in this request (all off in `fixed` mode). */
function features(j, mode) {
  if (mode === 'fixed') return {};
  const s = sys(j);
  const tools = (j.tools || []).map((t) => t.function);
  const search = tools.find((t) => t.name === 'tool_search')?.description || '';
  return {
    env: /Environment \(probed once/.test(s),
    sig: tools.some((t) => t.name === 'import_repo') || /import_repo\(repo/.test(search),
    batch: /Put independent tool calls in one response/.test(s),
    plan: /never as a turn of its own/.test(s),
  };
}

const PLAN = (done) => ['import the repository', 'build it with make', 'find the screen-drawing code', 'explain it'].map((text, i) => ({ text, status: i < done ? 'completed' : i === done ? 'in_progress' : 'pending' }));

/**
 * The next move given the conversation so far. Returns { text, calls }.
 * `outs` are the tool outputs since the task message, in order.
 */
export function kiloStep(j, { mode = 'adaptive', minimal = false, base = '' } = {}) {
  const msgs = j.messages;
  const start = msgs.findIndex((m) => m.role === 'user' && TASK_RE.test(String(m.content || '')));
  const after = msgs.slice(start + 1);
  const done = after.filter((m) => m.role === 'assistant').flatMap((m) => (m.tool_calls || []).map((c) => ({ name: c.function.name, args: JSON.parse(c.function.arguments || '{}') })));
  const outs = after.filter((m) => m.role === 'tool').map((m) => String(m.content));
  const last = outs[outs.length - 1] || '';
  const f = features(j, mode);
  const did = (pred) => done.some(pred);
  const count = (pred) => done.filter(pred).length;
  if (minimal) return minimalStep({ done, outs, did, count, base });

  const plans = count((c) => c.name === 'update_plan');
  const withPlan = (n, calls) => (f.plan ? [['update_plan', { items: PLAN(n) }], ...calls] : calls);
  const planTurn = (n) => ({ text: 'Updating the plan.', calls: [['update_plan', { items: PLAN(n) }]] });
  // 1. A plan first (its own turn unless the harness says otherwise).
  if (!plans && !f.plan) return planTurn(0);
  // 2. Find the import tool, unless its signature is already visible.
  const imported = did((c) => c.name === 'import_repo');
  if (!imported && !f.sig && !did((c) => c.name === 'tool_search')) return { text: 'I need a way to bring in a GitHub repository; let me look for a tool.', calls: [['tool_search', { query: 'import github repository' }]] };
  if (!imported) return { text: "I'll import antirez/kilo into the workspace.", calls: withPlan(0, [['import_repo', { repo: 'antirez/kilo' }]]) };
  // 3. Probe the environment, unless its facts are already in context.
  const probes = count((c) => c.name === 'shell' && /^(uname|which)/.test(c.args.command));
  if (!f.env && probes === 0) return { text: 'Let me check the system first.', calls: [['shell', { command: 'uname -a && cat /etc/os-release' }]] };
  if (!f.env && probes === 1) return { text: 'And whether a compiler and make are installed.', calls: [['shell', { command: 'which gcc cc make' }]] };
  // 4. Look around: one read per turn, or all at once.
  const look = [['list_dir', { path: 'kilo' }], ['read_file', { path: 'kilo/Makefile' }], ['read_file', { path: 'kilo/README.md' }]];
  const pending = look.filter(([n, a]) => !did((c) => c.name === n && c.args.path === a.path));
  if (pending.length) return { text: 'Looking at the repository layout and build files.', calls: f.batch ? pending : [pending[0]] };
  // 5. Build, then verify.
  if (!did((c) => c.name === 'shell' && /(^|&& )make$/.test(c.args.command))) return { text: 'Building with make.', calls: [['shell', { command: 'cd kilo && make' }]] };
  if (!did((c) => c.name === 'shell' && /ls -la/.test(c.args.command))) return { text: 'The build finished; checking the binary.', calls: [['shell', { command: 'ls -la kilo/kilo && file kilo/kilo || true' }]] };
  // 6. Plan progress, then look for the drawing code (with the classic mistake:
  //    a file passed as the search path).
  if (plans < 2 && !f.plan) return planTurn(2);
  const searches = done.filter((c) => c.name === 'search_text' || c.name === 'search_files');
  if (!searches.length) return { text: 'Searching for the screen refresh code.', calls: withPlan(2, [['search_text', { query: 'editorRefreshScreen', path: 'kilo/kilo.c' }]]) };
  const found = outs.some((o) => /kilo\.c[\s\S]*\d+:\d+: .*editorRefreshScreen/.test(o));
  if (!found) {
    if (mode !== 'fixed' && /include=\[/.test(last)) {
      const m = last.match(/path="([^"]+)", include=\[([^\]]+)\]/);
      return { text: 'Following the suggestion.', calls: [['search_text', { query: 'editorRefreshScreen', path: m[1], include: JSON.parse(`[${m[2]}]`) }]] };
    }
    if (!did((c) => c.name === 'search_files')) return { text: 'That path did not work; let me find the file.', calls: [['search_files', { pattern: '**/kilo.c' }]] };
    return { text: 'Searching the directory instead.', calls: [['search_text', { query: 'editorRefreshScreen', path: 'kilo' }]] };
  }
  // 7. Read the relevant parts: serially, or together.
  const ranges = [[860, 880], [882, 990], [540, 620], [1, 120]];
  const unread = ranges.filter(([a]) => !did((c) => c.name === 'read_file' && c.args.start_line === a));
  if (unread.length) {
    const calls = unread.map(([a, b]) => ['read_file', { path: 'kilo/kilo.c', start_line: a, end_line: b }]);
    return { text: 'Reading the rendering code.', calls: f.batch ? calls : [calls[0]] };
  }
  const extra = [['search_text', { query: 'abAppend\\(', path: 'kilo' }], ['search_text', { query: 'write\\(STDOUT_FILENO', path: 'kilo' }]];
  const unsearched = extra.filter(([, a]) => !did((c) => c.name === 'search_text' && c.args.query === a.query));
  if (unsearched.length) return { text: 'Checking where output is written.', calls: f.batch ? unsearched : [unsearched[0]] };
  // 8. Re-read the main function once more (a real habit), then finish.
  const rereads = count((c) => c.name === 'read_file' && c.args.start_line === 882);
  if (rereads < 2) return { text: 'Re-checking editorRefreshScreen before writing it up.', calls: withPlan(4, [['read_file', { path: 'kilo/kilo.c', start_line: 882, end_line: 990 }]]) };
  if (plans < 3 && !f.plan) return planTurn(4);
  return { text: answer(outs) };
}

function minimalStep({ done, outs, did, base }) {
  const sh = (command) => ['shell', { command }];
  const seq = [
    ['Downloading the repository.', sh(`curl -sL ${base}/gh-tar/antirez/kilo | tar xz && mv kilo-master kilo && ls kilo`)],
    ['Checking the system.', sh('uname -a && which gcc cc make')],
    ['Reading the Makefile.', sh('cat kilo/Makefile')],
    ['Building.', sh('cd kilo && make')],
    ['Checking the binary.', sh('ls -la kilo/kilo')],
    ['Finding the refresh code.', sh('grep -n editorRefreshScreen kilo/kilo.c')],
    ['Reading the append buffer.', sh("sed -n '860,880p' kilo/kilo.c")],
    ['Reading editorRefreshScreen.', sh("sed -n '882,990p' kilo/kilo.c")],
    ['Reading row rendering.', sh("sed -n '540,620p' kilo/kilo.c")],
    ['Reading the headers.', sh("sed -n '1,120p' kilo/kilo.c")],
    ['Where output is written.', sh("grep -n 'write(STDOUT_FILENO' kilo/kilo.c")],
    ['Re-checking the refresh function.', sh("sed -n '882,990p' kilo/kilo.c")],
  ];
  const n = done.length;
  if (n < seq.length) return { text: seq[n][0], calls: [seq[n][1]] };
  return { text: answer(outs) };
}

function answer(outs) {
  const all = outs.join('\n');
  const line = (all.match(/kilo\.c[^\n]*\n\s*(\d+):\d+: void editorRefreshScreen/) || all.match(/^(\d+):void editorRefreshScreen/m) || [])[1] || '?';
  const built = /kilo\/kilo|\bkilo\b.*\d{4,} /.test(all) && !/No such file/.test(all.split('ls -la').pop() || '');
  return [
    `Imported antirez/kilo and built it with \`make\`${built ? ' (it produced the `kilo` binary)' : ''}.`,
    '',
    `**How kilo draws the screen** — everything happens in \`editorRefreshScreen()\` (kilo.c:${line}), called once per keypress from the main loop:`,
    '',
    '1. It builds the whole frame in memory first, in an *append buffer* (`struct abuf` with `abAppend()`), so the terminal receives one `write()` per refresh and does not flicker.',
    '2. It hides the cursor (`ESC[?25l`) and moves it home (`ESC[H`) — plain VT100 escape sequences, no curses.',
    '3. For each screen row it appends the visible slice of the file row (`E.rowoff`/`E.coloff` scrolling), coloring characters by their syntax-highlight class with `ESC[...m`, or `~` past the end of the file (plus a centered welcome message on an empty buffer), and clears the rest of the line with `ESC[0K`.',
    '4. It draws the inverted status bar (`ESC[7m`: file name, line count, modified flag) and the message bar.',
    '5. It positions the cursor (`ESC[row;colH`, accounting for tabs via the rendered row), shows it again (`ESC[?25h`), and writes the buffer to `STDOUT_FILENO` in one call.',
    '',
    '`editorUpdateRow()` precomputes each row\'s render form (tabs expanded) and highlight, so drawing is just copying bytes.',
  ].join('\n');
}

// ---------------------------------------------------------------------------
// Simulated usage
// ---------------------------------------------------------------------------

const last = new Map(); // conversation -> serialized previous request

export function simulatedUsage(j, completionText) {
  const conv = String(j.messages.find((m) => m.role === 'user')?.content || '').slice(0, 200);
  const body = JSON.stringify({ tools: j.tools, messages: j.messages });
  const prev = last.get(conv) || '';
  let k = 0;
  const n = Math.min(prev.length, body.length);
  while (k < n && prev.charCodeAt(k) === body.charCodeAt(k)) k++;
  last.set(conv, body);
  const prompt = Math.ceil(body.length / 4);
  const cached = Math.floor((k >= 4096 ? k : 0) / 4); // caches need a minimum prefix
  const completion = Math.ceil(completionText.length / 4);
  const cost = ((prompt - cached) * 0.15 + cached * 0.075 + completion * 0.6) / 1e6;
  return { prompt_tokens: prompt, completion_tokens: completion, prompt_tokens_details: { cached_tokens: cached }, cost: Number(cost.toFixed(8)) };
}
