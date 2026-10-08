#!/usr/bin/env node
// Why did each model request happen? An empirical turn breakdown of a session.
//
//   node bench/turns.mjs --data .agentmod [SESSION]         a native data directory
//   node bench/turns.mjs export.json [SESSION]              a browser "Export logs" file
//   node bench/turns.mjs ... --json                         machine-readable
//
// Every model request after the first is caused by the results of the turn
// before it, so a request is classified by what that previous turn did and
// what this one does with it. Categories (first matching rule wins):
//
//   initial               the first request after a user message
//   final answer          ends the loop with text (the report)
//   bad arguments         recovering from a tool error caused by the call itself
//                         (wrong path kind, missing/invalid args, unknown tool)
//   sandbox recovery      after sandbox_* tools or a crashed/unresponsive sandbox
//   tool discovery        the previous turn only ran tool_search
//   plan only             the previous turn only updated the plan
//   environment probing   the previous turn only asked what is installed / where
//   redundant reinspection  the previous turn re-read or re-listed something it had already seen unchanged
//   build/test iteration  after a build/test command (make, gcc, npm test, …)
//   error recovery        after another tool failure
//   avoidable serial step one read-only call whose arguments did not depend on
//                         the previous result, after a turn of read-only calls (heuristic)
//   repository navigation after reads, listings, and searches the next step depended on
//   tool result follow-up after other work (imports, edits, processes)
//
// `avoidable` marks categories a better harness or prompt could remove
// without losing information: tool discovery, plan only, environment probing,
// redundant reinspection, avoidable serial step, and the second and later
// attempts in a bad-arguments chain.
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const READ_ONLY = new Set(['read_file', 'list_dir', 'search_files', 'search_text', 'repo_map', 'view_image', 'tool_search', 'recall', 'web_fetch']);
const PROBE = /^\s*(which|command -v|type|uname|whoami|id|hostname|lsb_release|env|printenv|echo \$\w+|cat \/etc\/(os-release|issue|debian_version)|ls (-\w+ )*\/(usr\/)?(bin|local)?\s*$|\w+ (--version|-v|-V)\b|dpkg -l|apt list|nproc|free|df)/;
const BUILD = /\b(make|cmake|gcc|cc|clang|g\+\+|cargo (build|test|run)|npm (test|run|install)|pnpm|yarn|pytest|python3? -m (pytest|unittest)|go (build|test)|mvn|gradle|tsc|node \S+test)/;
// Read-only inspection through the shell (the minimal loop has nothing else).
const SHELL_READ = /^\s*(cat|sed -n|grep|rg|ls|head|tail|find|wc|file|stat|less|nl)\b/;
const readOnly = (c) => READ_ONLY.has(c.name) || (c.name === 'shell' && SHELL_READ.test(String(c.args?.command || '')) && !BUILD.test(String(c.args?.command || '')));
const BAD_ARGS = /(Invalid call|must be a directory|is not a directory|is a directory|does not exist|There is no tool named|not valid JSON|is required|must be one of|missing required)/;
const SANDBOX = /(sandbox stopped responding|sandbox (crashed|is not responding)|CheerpX|VM (crashed|stopped))/i;

/** Model turns of a projected session view (`agentmod inspect --json`). */
export function turnsOf(view) {
  const events = view.events || [];
  const results = new Map();
  for (const e of events) if (e.event_name === 'tool-result') results.set(e.payload.call_id, e.payload);
  const turns = [];
  let trigger = null;
  for (const e of events) {
    if (e.event_name === 'user-message') trigger = 'user';
    if (e.event_name === 'model-request') turns.push({ event_id: e.event_id, sequence: e.sequence, at: e.at, trigger: trigger || 'tools', calls: [], text: '' });
    if (e.event_name === 'model-request') trigger = null;
    if (e.event_name === 'model-response' && turns.length) {
      const t = turns[turns.length - 1];
      t.text = e.payload.text || '';
      t.usage = e.payload.metrics || {};
      t.calls = (e.payload.tool_calls || []).map((c) => ({ name: c.name, args: c.args || {}, result: results.get(c.call_id) || null }));
    }
  }
  return turns;
}

const argText = (c) => JSON.stringify(c.args || {});
const key = (c) => (c.name === 'shell' ? `shell:${c.args?.command}` : `${c.name}:${c.args?.path ?? ''}:${c.args?.start_line ?? ''}:${c.args?.end_line ?? ''}:${c.args?.depth ?? ''}:${c.args?.query ?? ''}`);

function dependsOn(call, prev) {
  // Do this call's arguments use something only the previous results revealed?
  const out = prev.calls.map((c) => String(c.result?.output || '')).join('\n');
  const tokens = argText(call).match(/[A-Za-z0-9_./-]{4,}/g) || [];
  return tokens.some((t) => out.includes(t) && !prev.calls.some((c) => argText(c).includes(t)));
}

/** Classify every model request of a session. */
export function classifyTurns(view) {
  const turns = turnsOf(view);
  const seen = new Map(); // read key -> sha
  let badChain = 0;
  return turns.map((t, i) => {
    const prev = turns[i - 1];
    let category = 'other';
    let evidence = '';
    const names = prev?.calls.map((c) => c.name) || [];
    const errs = prev?.calls.filter((c) => c.result?.error) || [];
    if (t.trigger === 'user' || !prev) category = 'initial';
    else if (errs.some((c) => BAD_ARGS.test(String(c.result?.output || '')))) {
      category = 'bad arguments';
      evidence = errs.map((c) => `${c.name}: ${String(c.result.output).split('\n')[0].slice(0, 90)}`).join(' | ');
    } else if (names.some((n) => n.startsWith('sandbox_')) || errs.some((c) => SANDBOX.test(String(c.result?.output || '')))) category = 'sandbox recovery';
    else if (names.length && names.every((n) => n === 'tool_search')) category = 'tool discovery';
    else if (names.length && names.every((n) => n === 'update_plan')) category = 'plan only';
    else if (names.length && prev.calls.every((c) => c.name === 'shell' && PROBE.test(String(c.args?.command || '')))) {
      category = 'environment probing';
      evidence = prev.calls.map((c) => c.args.command).join(' ; ');
    } else if (prev.calls.some((c) => READ_ONLY.has(c.name) && seen.has(key(c)) && (!c.result?.sha256 || seen.get(key(c)) === c.result.sha256))) {
      category = 'redundant reinspection';
      evidence = prev.calls.filter((c) => seen.has(key(c))).map(key).join(' ');
    } else if (prev.calls.some((c) => c.name === 'shell' && BUILD.test(String(c.args?.command || '')))) category = 'build/test iteration';
    else if (errs.length) category = 'error recovery';
    else if (names.length && prev.calls.every(readOnly)) {
      const pp = turns[i - 2];
      const single = prev.calls.length === 1 && t.calls.length >= 1 && t.calls.every(readOnly);
      category = single && pp && pp.calls.every(readOnly) && !t.calls.some((c) => dependsOn(c, prev)) ? 'avoidable serial step' : 'repository navigation';
    } else if (names.length) category = 'tool result follow-up';
    if (!t.calls.length && t.text && i === turns.length - 1 && category !== 'initial') {
      // The last request writes the answer; its cause is still recorded.
      evidence = evidence || `after ${category}`;
      category = 'final answer';
    }
    if (category === 'bad arguments') badChain++;
    else badChain = 0;
    for (const c of prev?.calls || []) if (readOnly(c) && !c.result?.error) seen.set(key(c), c.result?.sha256 ?? null);
    const avoidable = ['tool discovery', 'plan only', 'environment probing', 'redundant reinspection', 'avoidable serial step'].includes(category) || (category === 'bad arguments' && badChain > 1);
    return { turn: i + 1, event_id: t.event_id, category, avoidable, calls: t.calls.map((c) => c.name), evidence };
  });
}

export function summarizeTurns(rows) {
  const by = {};
  for (const r of rows) by[r.category] = (by[r.category] || 0) + 1;
  return { total: rows.length, avoidable: rows.filter((r) => r.avoidable).length, by_category: by };
}

function loadViews(argv) {
  const here = path.dirname(fileURLToPath(import.meta.url));
  const bin = process.env.AGENTMOD_BIN || path.join(here, '..', 'target', 'debug', 'agentmod');
  const di = argv.indexOf('--data');
  const pos = argv.filter((a, i) => !a.startsWith('--') && argv[i - 1] !== '--data');
  if (di >= 0) {
    const data = argv[di + 1];
    const index = JSON.parse(spawnSync(bin, ['inspect', '--data', data, '--json'], { encoding: 'utf8' }).stdout);
    const ids = pos.length ? pos : Object.keys(index.sessions);
    return ids.map((sid) => JSON.parse(spawnSync(bin, ['inspect', '--data', data, sid, '--json'], { encoding: 'utf8', maxBuffer: 1 << 28 }).stdout));
  }
  // A browser export: { sessions: { sid: records[] } } → a minimal projection.
  const exp = JSON.parse(fs.readFileSync(pos[0], 'utf8'));
  const ids = pos.length > 1 ? pos.slice(1) : Object.keys(exp.sessions);
  return ids.map((sid) => {
    const events = [];
    for (const r of exp.sessions[sid]) {
      if (r.type === 'event-appended') events.push({ ...r.event, sequence: r.sequence, at: r.at });
    }
    return { session_id: sid, events };
  });
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const argv = process.argv.slice(2);
  const out = loadViews(argv).map((v) => {
    const rows = classifyTurns(v);
    return { session: v.session_id, ...summarizeTurns(rows), turns: rows };
  });
  if (argv.includes('--json')) console.log(JSON.stringify(out, null, 2));
  else {
    for (const s of out) {
      console.log(`${s.session}: ${s.total} model requests, ${s.avoidable} avoidable`);
      for (const [k, n] of Object.entries(s.by_category).sort((a, b) => b[1] - a[1])) console.log(`  ${String(n).padStart(3)}  ${k}`);
      for (const r of s.turns) console.log(`    ${String(r.turn).padStart(3)} ${r.avoidable ? '*' : ' '} ${r.category.padEnd(24)} ${r.calls.join(', ')}${r.evidence ? `  — ${r.evidence}` : ''}`);
    }
  }
}
