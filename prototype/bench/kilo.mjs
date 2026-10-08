#!/usr/bin/env node
// The Kilo regression benchmark: "Import the GitHub repo antirez/kilo, build
// it with make, and explain how it draws the screen."
//
//   node bench/kilo.mjs --before-ref <git ref> [--out bench/results]     offline, CI-compatible
//   node bench/kilo.mjs --real --model <id> [--definitions coder,minimal]  a real model (needs OPENROUTER_API_KEY and GitHub access)
//
// Offline runs use the mock provider's scripted model and fake GitHub
// (bench/kilo-world.mjs) with real tools: files are written, `make` runs.
// Each harness under test is a built tree: the current one ("after") and,
// with --before-ref, a git worktree of that ref built on the side ("before").
// Every number comes from the session logs, computed by the *current*
// `agentmod metrics` (one definition for both builds) and bench/turns.mjs.
//
// Runs:
//   coder · fixed      the same scripted trajectory on both builds: the
//                      difference is only what the harness costs
//   coder · adaptive   the script takes shortcuts the harness offers when it
//                      can see them (a simulated model, not a measurement)
//   minimal            provider + loop + one shell tool, same task
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import net from 'node:net';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { classifyTurns, summarizeTurns } from './turns.mjs';
import { SseParser } from '../plugins/sdk/sse.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const proto = path.join(here, '..');
const argv = process.argv.slice(2);
const opt = (k, d) => (argv.includes(`--${k}`) ? argv[argv.indexOf(`--${k}`) + 1] : d);
const REAL = argv.includes('--real');
const TASK = 'Import the GitHub repo antirez/kilo, build it with make, and explain how it draws the screen.';
const OUT = path.resolve(opt('out', path.join(here, 'results')));
const AFTER_BIN = process.env.AGENTMOD_BIN || path.join(proto, 'target', 'debug', 'agentmod');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const freePort = () => new Promise((resolve) => { const s = net.createServer(); s.listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => resolve(p)); }); });
const api = async (base, p, body) => (await fetch(base + p, body === undefined ? {} : { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) })).json();

/** Build a ref's prototype in a worktree (cached by commit). */
function buildRef(ref) {
  const sha = spawnSync('git', ['rev-parse', ref], { cwd: proto, encoding: 'utf8' }).stdout.trim();
  if (!sha) throw new Error(`unknown ref ${ref} (fetch it first)`);
  const dir = path.join(os.tmpdir(), `agentmod-bench-${sha.slice(0, 12)}`);
  if (!fs.existsSync(dir)) {
    const r = spawnSync('git', ['worktree', 'add', '--detach', dir, sha], { cwd: proto, encoding: 'utf8' });
    if (r.status !== 0) throw new Error(r.stderr);
  }
  const p = path.join(dir, 'prototype');
  process.stderr.write(`building ${ref} (${sha.slice(0, 10)}) in ${p} …\n`);
  const b = spawnSync('cargo', ['build', '-q', '-p', 'agentmod-runtime'], { cwd: p, stdio: 'inherit' });
  if (b.status !== 0) throw new Error('build failed');
  return { label: `before (${ref} ${sha.slice(0, 7)})`, proto: p, bin: path.join(p, 'target', 'debug', 'agentmod'), sha };
}

function config(tree, { port, ws, mockBase, model }) {
  let t = fs.readFileSync(path.join(tree.proto, 'agentmod.toml'), 'utf8');
  t = t.replace(/port = 7700/, `port = ${port}`)
    .replaceAll('root = ".agentmod/workspace"', `root = ${JSON.stringify(ws)}`)
    .replaceAll('model = "openai/gpt-4o-mini"', `model = ${JSON.stringify(model)}`)
    .replace('mode = "default"', 'mode = "auto"');
  if (mockBase) t = t.replaceAll('max_tokens = 4096', `max_tokens = 4096, base_url = ${JSON.stringify(mockBase)}`);
  const file = path.join(tree.proto, `.bench-kilo-${process.pid}-${port}.toml`);
  fs.writeFileSync(file, t);
  return file;
}

function grade(ws, answer) {
  const bin = path.join(ws, 'kilo', 'kilo');
  const built = fs.existsSync(bin) && (fs.statSync(bin).mode & 0o111) !== 0;
  const imported = fs.existsSync(path.join(ws, 'kilo', 'kilo.c'));
  const rubric = { refresh: /editorRefreshScreen/, buffer: /abuf|append buffer/i, escapes: /escape|ESC\[|VT100/i, single_write: /one `?write|single write|STDOUT_FILENO/i };
  const quality = Object.fromEntries(Object.entries(rubric).map(([k, re]) => [k, re.test(answer || '')]));
  const score = Object.values(quality).filter(Boolean).length;
  return { success: imported && built && score === 4, imported, build_success: built, answer_quality: `${score}/4`, quality };
}

/**
 * What a frontend sees: subscribe to the gateway's SSE stream like the UI does
 * and measure deliveries, bytes, and the delay from each model request to the
 * first visible text of its reply (a stream-chunk record before; a live text
 * frame after).
 */
function watchUi(base) {
  const ui = { messages: 0, bytes: 0, record_messages: 0, stream_messages: 0, first_text_ms: [] };
  const ctrl = new AbortController();
  let pendingSince = null;
  (async () => {
    try {
      const res = await fetch(`${base}/stream`, { signal: ctrl.signal });
      const reader = res.body.getReader();
      const parser = new SseParser({ maxEventBytes: 64 << 20 });
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        ui.bytes += value.length;
        for (const ev of parser.push(value)) {
          ui.messages++;
          const now = Date.now();
          if (ev.event === 'record') {
            ui.record_messages++;
            const r = JSON.parse(ev.data);
            if (r.type === 'event-appended' && r.event.event_name === 'model-request') pendingSince = now;
            if (pendingSince && r.type === 'event-appended' && r.event.event_name === 'stream-chunk') { ui.first_text_ms.push(now - pendingSince); pendingSince = null; }
          } else if (ev.event === 'stream') {
            ui.stream_messages++;
            const msgs = JSON.parse(ev.data);
            if (pendingSince && msgs.some((m) => m.type === 'frame' && m.frame.type === 'text-delta')) { ui.first_text_ms.push(now - pendingSince); pendingSince = null; }
          }
        }
      }
    } catch { /* closed */ }
  })();
  return { ui, stop: () => ctrl.abort() };
}

async function startMock(mode) {
  const port = await freePort();
  const p = spawn(process.execPath, [path.join(proto, 'tests', 'mock-openrouter.mjs'), String(port)], { stdio: ['ignore', 'ignore', 'ignore'], env: { ...process.env, MOCK_KILO: mode, MOCK_DELAY_MS: process.env.MOCK_DELAY_MS || '2' } });
  for (let i = 0; i < 50; i++) {
    try { await fetch(`http://127.0.0.1:${port}/api/v1/models`); break; } catch { await sleep(100); }
  }
  return { proc: p, port, base: `http://127.0.0.1:${port}/api/v1` };
}

async function run(tree, { definition, mode }) {
  // A fresh mock per run: the scripted model's mode, and no shared cache state.
  const mock = REAL ? null : await startMock(mode);
  const mockBase = mock?.base;
  const mockPort = mock?.port;
  const ws = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'kilo-ws-')));
  const data = fs.mkdtempSync(path.join(os.tmpdir(), 'kilo-data-'));
  const port = await freePort();
  const cfg = config(tree, { port, ws, mockBase, model: REAL ? opt('model', 'openai/gpt-4o-mini') : 'mock/tool-model' });
  const log = fs.openSync(path.join(data, 'serve.log'), 'w');
  const env = { ...process.env, MOCK_KILO: mode };
  if (!REAL) Object.assign(env, { OPENROUTER_API_KEY: 'test-key', NODE_OPTIONS: `--import=${path.join(here, 'github-redirect.mjs')}`, AGENTMOD_GITHUB_MOCK: `http://127.0.0.1:${mockPort}` });
  const rt = spawn(tree.bin, ['serve', '--config', cfg, '--data', data], { cwd: tree.proto, stdio: ['ignore', log, log], env });
  const base = `http://127.0.0.1:${port}/api`;
  const t0 = Date.now();
  let sid;
  let prompts = 0;
  let runtime = null;
  let watcher = null;
  try {
    for (let i = 0; i < 150; i++) { try { await api(base, '/info'); break; } catch { await sleep(200); } }
    watcher = watchUi(base);
    await sleep(200);
    ({ session_id: sid } = await api(base, '/sessions', { definition, text: TASK }));
    if (!sid) throw new Error(`no session (see ${data}/serve.log)`);
    let quiet = null;
    const answered = new Set();
    for (;;) {
      if (Date.now() - t0 > 600_000) break;
      await sleep(300);
      const v = await api(base, `/sessions/${sid}`);
      for (const e of v.events) {
        if (e.event_name === 'approval-requested' && !answered.has(e.event_id)) {
          answered.add(e.event_id);
          prompts++;
          await api(base, `/sessions/${sid}/actions`, { reply_to: e.event_id, action: 'approve' });
        }
      }
      const lastA = [...v.events].reverse().find((e) => e.event_name === 'assistant-message');
      const lastR = [...v.events].reverse().find((e) => e.event_name === 'model-request');
      const done = v.status?.activity === 'idle' && lastA && lastA.sequence > lastR.sequence;
      if (done) { quiet ??= Date.now(); if (Date.now() - quiet > 600) break; } else quiet = null;
    }
    runtime = await api(base, '/metrics').catch(() => null);
  } finally {
    watcher?.stop();
    rt.kill('SIGINT');
    await new Promise((r) => rt.once('exit', r));
    fs.rmSync(cfg, { force: true });
    mock?.proc.kill();
  }
  const wall = Date.now() - t0;
  const metrics = JSON.parse(spawnSync(AFTER_BIN, ['metrics', '--data', data, sid, '--json', '--turns'], { encoding: 'utf8', maxBuffer: 1 << 28 }).stdout)[sid];
  const view = JSON.parse(spawnSync(AFTER_BIN, ['inspect', '--data', data, sid, '--json'], { encoding: 'utf8', maxBuffer: 1 << 28 }).stdout);
  const answer = [...view.events].reverse().find((e) => e.event_name === 'assistant-message')?.payload?.text || '';
  const turns = classifyTurns(view);
  const ft = [...(watcher?.ui.first_text_ms || [])].sort((a, b) => a - b);
  const ui = watcher ? { ...watcher.ui, first_text_ms: undefined, first_text_median_ms: ft.length ? ft[Math.floor(ft.length / 2)] : null, first_text_max_ms: ft.length ? ft[ft.length - 1] : null, first_text_samples: ft.length } : null;
  return { harness: tree.label, definition, mode, session: sid, data, workspace: ws, wall_ms: wall, permission_prompts: prompts, ...grade(ws, answer), metrics, runtime, ui, turns: { ...summarizeTurns(turns), rows: turns }, answer };
}

const fmt = (x) => (x == null ? '—' : typeof x === 'number' ? (Number.isInteger(x) ? x.toLocaleString('en-US') : x.toFixed(x < 1 ? 4 : 1)) : String(x));
const change = (a, b) => (typeof a === 'number' && typeof b === 'number' && a ? `${b - a >= 0 ? '+' : ''}${(((b - a) / a) * 100).toFixed(0)}%` : '—');

function table(before, after, rows) {
  const w = [34, 16, 16, 9];
  const line = (c) => `| ${c.map((x, i) => String(x).padEnd(w[i])).join(' | ')} |`;
  const out = [line(['Metric', 'Before', 'After', 'Change']), `|${w.map((n) => '-'.repeat(n + 2)).join('|')}|`];
  for (const [label, get] of rows) {
    const a = get(before);
    const b = get(after);
    out.push(line([label, fmt(a), fmt(b), change(a, b)]));
  }
  return out.join('\n');
}

const M = (k) => (r) => r.metrics?.[k] ?? null;
const R = (k) => (r) => r.metrics?.ratios?.[k] ?? null;
const ROWS = [
  ['Task success', (r) => (r.success ? 'yes' : 'no')],
  ['Build success', (r) => (r.build_success ? 'yes' : 'no')],
  ['Answer quality (rubric)', (r) => r.answer_quality],
  ['Model calls', M('model_requests')],
  ['Tool calls', M('tool_calls')],
  ['Failed tools', M('tool_errors')],
  ['Runtime recoveries', M('recovered_invocations')],
  ['Wall time (s)', (r) => r.wall_ms / 1000],
  ['Prompt tokens', M('input_tokens')],
  ['Cached prompt tokens', M('cached_tokens')],
  ['Uncached prompt tokens', M('uncached_input_tokens')],
  ['Completion tokens', M('output_tokens')],
  ['Cost ($, simulated pricing)', M('cost')],
  ['Canonical events', M('events')],
  ['  of which stream-chunk events', M('stream_chunk_events')],
  ['Pipeline executions', M('pipeline_starts')],
  ['Plugin invocations', M('plugin_invocations')],
  ['  no-op invocations', M('noop_invocations')],
  ['  tool-call invocations', M('tool_call_invocations')],
  ['  exact-owner dispatches', M('exact_owner_dispatches')],
  ['  broadcast dispatches', M('candidate_dispatches')],
  ['Stream provider events', M('stream_provider_events')],
  ['Live frames', M('stream_live_frames')],
  ['UI deliveries (SSE messages)', (r) => r.ui?.messages ?? null],
  ['UI bytes received', (r) => r.ui?.bytes ?? null],
  ['First visible text, median (ms)', (r) => r.ui?.first_text_median_ms ?? null],
  ['First visible text, max (ms)', (r) => r.ui?.first_text_max_ms ?? null],
  ['Journal records', M('records')],
  ['Journal bytes', M('record_bytes')],
  ['Pipelines / semantic event', R('pipelines_per_semantic_event')],
  ['Plugin invocations / tool call', R('plugin_invocations_per_tool_call')],
  ['Live frames / provider event', R('live_frames_per_provider_event')],
  ['Canonical events / model response', R('canonical_events_per_model_response')],
  ['Journal bytes / useful output byte', R('journal_bytes_per_useful_output_byte')],
];

function turnTable(runs) {
  const cats = [...new Set(runs.flatMap((r) => Object.keys(r.turns.by_category)))].sort();
  const head = `| category | ${runs.map((r) => `${r.harness.split(' ')[0]} · ${r.definition} · ${r.mode}`).join(' | ')} |`;
  const sep = `|---|${runs.map(() => '---').join('|')}|`;
  const rows = cats.map((c) => `| ${c} | ${runs.map((r) => r.turns.by_category[c] || 0).join(' | ')} |`);
  rows.push(`| **total model requests** | ${runs.map((r) => r.turns.total).join(' | ')} |`);
  rows.push(`| **avoidable (classifier)** | ${runs.map((r) => r.turns.avoidable).join(' | ')} |`);
  return [head, sep, ...rows].join('\n');
}

async function main() {
  if (!fs.existsSync(AFTER_BIN)) throw new Error(`build the runtime first: ${AFTER_BIN}`);
  const after = { label: 'after (working tree)', proto, bin: AFTER_BIN };
  const before = opt('before-ref') ? buildRef(opt('before-ref')) : null;
  if (REAL && !process.env.OPENROUTER_API_KEY) throw new Error('--real needs OPENROUTER_API_KEY');
  const plan = REAL
    ? opt('definitions', 'coder,minimal').split(',').map((d) => [after, d, 'real'])
    : [
      ...(before ? [[before, 'coder', 'fixed'], [before, 'coder', 'adaptive'], [before, 'minimal', 'adaptive']] : []),
      [after, 'coder', 'fixed'], [after, 'coder', 'adaptive'], [after, 'minimal', 'adaptive'],
    ];
  const results = [];
  for (const [tree, definition, mode] of plan) {
    process.stderr.write(`· ${tree.label} · ${definition} · ${mode} … `);
    const r = await run(tree, { definition, mode });
    process.stderr.write(`${r.success ? 'pass' : 'FAIL'} · ${r.metrics.model_requests} model calls · ${r.metrics.records} records (${(r.wall_ms / 1000).toFixed(1)}s)\n`);
    results.push(r);
  }
  const pick = (h, d, m) => results.find((r) => r.harness.startsWith(h) && r.definition === d && r.mode === m);
  const parts = [`# Kilo benchmark${REAL ? ` — ${opt('model', 'openai/gpt-4o-mini')}` : ' — offline (scripted model, fake GitHub, real tools)'}`, '', `Task: _${TASK}_`, ''];
  const bf = pick('before', 'coder', 'fixed');
  const af = pick('after', 'coder', 'fixed');
  if (bf && af) parts.push('## Control plane: same trajectory, before vs after', '', table(bf, af, ROWS), '');
  const aa = pick('after', 'coder', 'adaptive');
  const ba = pick('before', 'coder', 'adaptive');
  if (ba && aa) parts.push('## Turns: the adaptive script on each harness (simulated model)', '', table(ba, aa, ROWS.filter(([l]) => /success|quality|Model calls|Tool calls|Failed|Wall|tokens|Cost|Journal records|Journal bytes/.test(l))), '');
  parts.push('## Why each model request happened (bench/turns.mjs)', '', turnTable(results), '');
  const mins = results.filter((r) => r.definition === 'minimal');
  if (mins.length && aa) parts.push('## Minimal loop vs the coder harness (after)', '', table(pick('after', 'minimal', 'adaptive') || mins[0], aa, ROWS).replace('| Before', '| Minimal').replace('| After ', '| Coder  '), '');
  for (const r of results) {
    parts.push(`<details><summary>${r.harness} · ${r.definition} · ${r.mode}: per-turn classification</summary>`, '', ...r.turns.rows.map((t) => `${t.turn}. ${t.avoidable ? '**avoidable** ' : ''}${t.category} — ${t.calls.join(', ') || 'text'}${t.evidence ? ` (${t.evidence})` : ''}`), '', '</details>', '');
  }
  const md = parts.join('\n');
  fs.mkdirSync(OUT, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const file = path.join(OUT, `kilo-${REAL ? 'real' : 'offline'}-${stamp}`);
  fs.writeFileSync(`${file}.json`, JSON.stringify({ task: TASK, real: REAL, before: before?.sha ?? null, results }, null, 2));
  fs.writeFileSync(`${file}.md`, md);
  console.log(md);
  console.log(`results: ${file}.md`);
  if (results.some((r) => !r.success)) process.exitCode = 1;
  if (argv.includes('--assert')) {
    // Regression guard (CI): the hot path must stay specialized.
    const f = pick('after', 'coder', 'fixed');
    const ad = pick('after', 'coder', 'adaptive');
    const m = f.metrics;
    const checks = [
      ['all runs succeed', results.every((r) => r.success)],
      ['no token fragments in the log', !m.stream_chunk_events],
      ['every tool call dispatched to exactly one owner', m.exact_owner_dispatches === m.tool_calls && !m.candidate_dispatches],
      ['≤ 2 plugin invocations per tool call', m.ratios.plugin_invocations_per_tool_call <= 2],
      ['≤ 30 journal records per model response', m.ratios.records_per_model_response <= 30],
      ['≤ 6 canonical events per model response', m.ratios.canonical_events_per_model_response <= 6],
      ['live frames coalesce (< 1 per provider event)', m.ratios.live_frames_per_provider_event < 1],
      ['the adaptive script needs ≤ 10 model requests', ad.metrics.model_requests <= 10],
      ['the adaptive script has no avoidable turns', ad.turns.avoidable === 0],
    ];
    for (const [name, ok] of checks) console.log(`${ok ? 'ok  ' : 'FAIL'} ${name}`);
    if (checks.some(([, ok]) => !ok)) process.exitCode = 1;
  }
}

main().catch((e) => {
  console.error(e.stack || e.message);
  process.exit(2);
});
