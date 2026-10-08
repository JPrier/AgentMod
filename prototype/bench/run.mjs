#!/usr/bin/env node
// AgentMod coding benchmark: does the harness earn its complexity?
//
// Runs each task under each session definition with the same model — by
// default the full `coder` harness and the deliberately `minimal` loop
// (provider + chat loop + one shell tool) — in fresh workspaces, then grades
// with hidden checks and regression checks. Every number except success and
// regressions is derived from the session logs (`agentmod metrics`), so there
// is no telemetry backend to trust.
//
//   node bench/run.mjs [--tasks a,b] [--definitions coder,minimal] [--model openai/gpt-4o-mini]
//                      [--runs 1] [--mode auto|default] [--out bench/results] [--external claude-code,…]
//   node bench/run.mjs --selftest      plumbing check with the test-only mock provider
//
// Needs OPENROUTER_API_KEY (except --selftest) and a built runtime
// (cargo build -p agentmod-runtime; AGENTMOD_BIN overrides the path).
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import net from 'node:net';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const proto = path.join(here, '..');
const argv = process.argv.slice(2);
const opt = (k, d) => {
  const i = argv.indexOf(`--${k}`);
  return i >= 0 ? argv[i + 1] : d;
};
const flag = (k) => argv.includes(`--${k}`);
const SELFTEST = flag('selftest');
const BIN = process.env.AGENTMOD_BIN || path.join(proto, 'target', 'debug', 'agentmod');
const MODEL = opt('model', SELFTEST ? 'mock/tool-model' : 'openai/gpt-4o-mini');
const DEFS = opt('definitions', 'coder,minimal').split(',');
const RUNS = Number(opt('runs', '1'));
const MODE = opt('mode', 'auto');
const OUT = path.resolve(opt('out', path.join(here, 'results')));
const allTasks = fs.readdirSync(path.join(here, 'tasks')).filter((t) => fs.existsSync(path.join(here, 'tasks', t, 'task.json'))).sort();
const TASKS = SELFTEST ? ['fix-bug-js'] : (opt('tasks', 'all') === 'all' ? allTasks : opt('tasks').split(','));
const EXTERNAL = opt('external', '').split(',').filter(Boolean);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const freePort = () => new Promise((resolve) => { const s = net.createServer(); s.listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => resolve(p)); }); });
const sh = (cmd, cwd, timeout = 300_000) => spawnSync('bash', ['-c', cmd], { cwd, timeout, encoding: 'utf8', env: { PATH: process.env.PATH, HOME: process.env.HOME, LANG: 'C.UTF-8' } });

/** A fresh git workspace from the task's repo; records whether the regression suite passed before. */
function prepareWorkspace(task, spec) {
  const ws = fs.mkdtempSync(path.join(os.tmpdir(), `bench-${task}-`));
  fs.cpSync(path.join(here, 'tasks', task, 'repo'), ws, { recursive: true });
  sh('git init -q && git add -A && git -c user.name=bench -c user.email=bench@localhost commit -qm initial', ws);
  const before = spec.regressions ? sh(spec.regressions, ws).status === 0 : null;
  return { ws, before };
}

function writeConfig({ ws, port, mockBase }) {
  let t = fs.readFileSync(path.join(proto, 'agentmod.toml'), 'utf8');
  t = t.replace(/port = 7700/, `port = ${port}`)
    .replaceAll('root = ".agentmod/workspace"', `root = ${JSON.stringify(ws)}`)
    .replaceAll('model = "openai/gpt-4o-mini"', `model = ${JSON.stringify(MODEL)}`)
    .replace('mode = "default"', `mode = ${JSON.stringify(MODE)}`);
  if (mockBase) t = t.replaceAll('max_tokens = 4096', `max_tokens = 4096, base_url = ${JSON.stringify(mockBase)}`);
  const file = path.join(proto, `.bench-${process.pid}-${port}.toml`);
  fs.writeFileSync(file, t);
  return file;
}

async function api(base, p, body) {
  const res = await fetch(base + p, body === undefined ? {} : { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
  return res.json();
}

/** Run one task under one definition through the real runtime. */
async function runAgentMod(task, definition, spec, mockBase) {
  const { ws, before } = prepareWorkspace(task, spec);
  const data = fs.mkdtempSync(path.join(os.tmpdir(), 'bench-data-'));
  const port = await freePort();
  const cfg = writeConfig({ ws, port, mockBase });
  const log = fs.openSync(path.join(data, 'serve.log'), 'w');
  const rt = spawn(BIN, ['serve', '--config', cfg, '--data', data], { cwd: proto, stdio: ['ignore', log, log], env: process.env });
  const base = `http://127.0.0.1:${port}/api`;
  const t0 = Date.now();
  let prompts = 0;
  let questions = 0;
  let timedOut = false;
  let sid;
  try {
    for (let i = 0; i < 100; i++) {
      try { await api(base, '/info'); break; } catch { await sleep(200); }
    }
    const prompt = SELFTEST ? `script:fix ${spec.prompt}` : spec.prompt;
    ({ session_id: sid } = await api(base, '/sessions', { definition, text: prompt }));
    if (!sid) throw new Error('could not start a session (see serve.log)');
    const deadline = t0 + (spec.timeout_seconds || 600) * 1000;
    const answered = new Set();
    let quietSince = null;
    for (;;) {
      if (Date.now() > deadline) { timedOut = true; break; }
      await sleep(1500);
      const sessions = await api(base, '/sessions');
      const mine = sessions.filter((s) => s.session_id === sid || s.parent === sid);
      for (const s of mine) {
        const v = await api(base, `/sessions/${s.session_id}`);
        for (const e of v.events) {
          if (answered.has(e.event_id)) continue;
          // Unattended runs: approve what policy asks, answer questions neutrally (both counted).
          if (e.event_name === 'approval-requested' && !v.events.some((x) => x.event_name === 'ui-action' && x.payload.reply_to === e.event_id)) {
            answered.add(e.event_id);
            prompts++;
            await api(base, `/sessions/${s.session_id}/actions`, { reply_to: e.event_id, action: 'approve' });
          }
          if (e.event_name === 'user-input-requested' && !v.events.some((x) => x.event_name === 'tool-result' && x.payload.call_id === e.payload.call_id)) {
            answered.add(e.event_id);
            questions++;
            await api(base, `/sessions/${s.session_id}/messages`, { text: 'Use your best judgment; I am not available.' });
          }
        }
      }
      const idle = mine.every((s) => s.activity === 'idle' || s.activity === 'parked' || s.activity === 'halted');
      const v = await api(base, `/sessions/${sid}`);
      const lastAssistant = [...v.events].reverse().find((e) => e.event_name === 'assistant-message');
      const lastRequest = [...v.events].reverse().find((e) => e.event_name === 'model-request');
      const finished = idle && lastAssistant && (!lastRequest || lastAssistant.sequence > lastRequest.sequence);
      if (finished) {
        quietSince ??= Date.now();
        if (Date.now() - quietSince > 2000) break;
      } else quietSince = null;
    }
  } finally {
    rt.kill('SIGINT');
    await new Promise((r) => rt.once('exit', r));
    fs.rmSync(cfg, { force: true });
    sh(`pkill -f ${JSON.stringify(ws)} || true`, os.tmpdir());
  }
  const grade = gradeWorkspace(ws, spec, before);
  const m = JSON.parse(spawnSync(BIN, ['metrics', '--data', data, '--json'], { encoding: 'utf8' }).stdout || '{}');
  const total = {};
  for (const sm of Object.values(m)) for (const [k, v] of Object.entries(sm)) if (typeof v === 'number') total[k] = (k.startsWith('max_') || k === 'tool_schema_tokens' ? Math.max(total[k] || 0, v) : (total[k] || 0) + v);
  const main = m[sid] || {};
  return {
    ...grade,
    harness: `agentmod:${definition}`,
    timed_out: timedOut,
    wall_ms: Date.now() - t0,
    permission_prompts: prompts,
    questions,
    sessions: Object.keys(m).length,
    child_agents: Object.keys(m).length - 1,
    metrics: { ...total, time_to_first_edit_ms: main.time_to_first_edit_ms ?? null },
    data,
    workspace: ws,
  };
}

function gradeWorkspace(ws, spec, before) {
  for (const [f, body] of Object.entries(spec.hidden || {})) fs.writeFileSync(path.join(ws, f), body);
  const check = sh(spec.check, ws);
  const after = spec.regressions ? sh(spec.regressions, ws).status === 0 : null;
  return { success: check.status === 0, check_output: `${check.stdout}${check.stderr}`.slice(-600), regression: before === true && after === false };
}

async function runExternal(name, task, spec) {
  const mod = await import(pathToFileURL(path.join(here, 'adapters', `${name}.mjs`)).href);
  if (!(await mod.available())) return { harness: name, skipped: 'not installed' };
  const { ws, before } = prepareWorkspace(task, spec);
  const t0 = Date.now();
  const r = await mod.run({ workspace: ws, prompt: spec.prompt, model: MODEL, timeoutMs: (spec.timeout_seconds || 600) * 1000 });
  return { ...gradeWorkspace(ws, spec, before), harness: name, wall_ms: Date.now() - t0, metrics: r.usage || {}, workspace: ws };
}

function summarize(results) {
  const by = new Map();
  for (const r of results.filter((x) => !x.skipped)) {
    if (!by.has(r.harness)) by.set(r.harness, []);
    by.get(r.harness).push(r);
  }
  const mean = (xs) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : 0);
  const rows = [['harness', 'runs', 'success', 'regressions', 'model calls', 'tool calls', 'input tok', 'cached %', 'output tok', 'cost $', 'max context', 'schema tok', 'first edit s', 'wall s', 'prompts', 'children', 'recovered']];
  for (const [h, rs] of by) {
    const g = (k) => mean(rs.map((r) => r.metrics?.[k] || 0));
    const inTok = g('input_tokens');
    rows.push([h, rs.length, `${Math.round((100 * rs.filter((r) => r.success).length) / rs.length)}%`, rs.filter((r) => r.regression).length, g('model_requests').toFixed(1), g('tool_calls').toFixed(1), Math.round(inTok), inTok ? `${Math.round((100 * g('cached_tokens')) / inTok)}%` : '-', Math.round(g('output_tokens')), g('cost').toFixed(4), Math.round(g('max_context_tokens')), Math.round(g('tool_schema_tokens')), (() => { const xs = rs.filter((r) => r.metrics?.time_to_first_edit_ms != null).map((r) => r.metrics.time_to_first_edit_ms); return xs.length ? (mean(xs) / 1000).toFixed(1) : '-'; })(), (mean(rs.map((r) => r.wall_ms)) / 1000).toFixed(1), mean(rs.map((r) => r.permission_prompts || 0)).toFixed(1), mean(rs.map((r) => r.child_agents || 0)).toFixed(1), g('recovered_invocations').toFixed(1)]);
  }
  const md = rows.map((r, i) => `| ${r.join(' | ')} |${i === 0 ? `\n|${r.map(() => '---').join('|')}|` : ''}`).join('\n');
  const perTask = results.map((r) => `- ${r.task} · ${r.harness}${r.run != null ? ` #${r.run}` : ''}: ${r.skipped ? `skipped (${r.skipped})` : `${r.success ? 'pass' : 'FAIL'}${r.regression ? ', regression' : ''}${r.timed_out ? ', timed out' : ''}`}`).join('\n');
  return `${md}\n\n${perTask}\n`;
}

async function main() {
  if (!fs.existsSync(BIN)) throw new Error(`runtime binary not found at ${BIN}; run cargo build -p agentmod-runtime`);
  let mock = null;
  let mockBase = null;
  if (SELFTEST) {
    const port = await freePort();
    mock = spawn(process.execPath, [path.join(proto, 'tests', 'mock-openrouter.mjs'), String(port)], { stdio: 'ignore', env: { ...process.env, MOCK_DELAY_MS: '5' } });
    mockBase = `http://127.0.0.1:${port}/api/v1`;
    process.env.OPENROUTER_API_KEY = 'test-key';
    await sleep(500);
  } else if (!process.env.OPENROUTER_API_KEY) {
    throw new Error('OPENROUTER_API_KEY is required (the benchmark runs a real model); use --selftest to check the plumbing');
  }
  const results = [];
  try {
    for (const task of TASKS) {
      const spec = JSON.parse(fs.readFileSync(path.join(here, 'tasks', task, 'task.json'), 'utf8'));
      for (let run = 0; run < RUNS; run++) {
        for (const def of DEFS) {
          process.stderr.write(`· ${task} · ${def} #${run} … `);
          const r = { task, run, ...(await runAgentMod(task, def, spec, mockBase)) };
          process.stderr.write(`${r.success ? 'pass' : 'fail'} (${(r.wall_ms / 1000).toFixed(1)}s)\n`);
          results.push(r);
        }
        for (const ext of EXTERNAL) results.push({ task, run, ...(await runExternal(ext, task, spec)) });
      }
    }
  } finally {
    mock?.kill();
  }
  fs.mkdirSync(OUT, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const file = path.join(OUT, `${SELFTEST ? 'selftest' : 'run'}-${stamp}.json`);
  fs.writeFileSync(file, JSON.stringify({ model: MODEL, mode: MODE, definitions: DEFS, results }, null, 2));
  const md = `# AgentMod benchmark — ${MODEL} (${MODE} mode)\n\n${summarize(results)}`;
  fs.writeFileSync(file.replace(/\.json$/, '.md'), md);
  console.log(md);
  console.log(`results: ${file}`);
  if (SELFTEST) {
    const coder = results.find((r) => r.harness === 'agentmod:coder');
    const minimal = results.find((r) => r.harness === 'agentmod:minimal');
    const ok = coder?.success && coder.metrics.model_requests >= 5 && coder.metrics.cached_tokens > 0 && minimal && !minimal.success && minimal.metrics.model_requests >= 1;
    console.log(ok ? 'selftest: ok' : 'selftest: FAILED');
    process.exit(ok ? 0 : 1);
  }
}

main().catch((e) => {
  console.error(e.message);
  process.exit(2);
});
