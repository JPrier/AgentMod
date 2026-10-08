// Persistent processes (dev servers, watchers, REPLs, long test runs).
//
// A process is started through the target's shell, detached, with its command,
// working directory, pid, output, and exit status kept in files under
// `<state>/procs/<id>/`. Nothing about a process lives only in plugin memory:
//
//   * its id is derived from the starting tool call, so a retried invocation
//     finds the process it already started instead of starting another;
//   * after a plugin or runtime restart the files are read back and the
//     process is *reconciled* (still running / exited with code / lost) —
//     the toolkit never claims to have replayed it;
//   * if the execution environment itself went away (a VM restart, a reboot)
//     and the pid is no longer this wrapper, the process is reported `lost`.
//
// Output is append-only files; each read returns what is new since the
// process's cursor (also a file), preferring the most recent bytes when there
// is more than the bound, with the full log path for anything skipped.

import { shq } from './paths.js';
import { sha256 } from './text.js';

const KILLTREE = 'killtree() { local c; for c in $(pgrep -P "$1" 2>/dev/null); do killtree "$c" "$2"; done; kill -"$2" "$1" 2>/dev/null; }';

// The wrapper runs the command as a child, records the child's pid, and waits:
// terminating a process signals that child (leaf first), never the waiting
// wrapper, which then records the exit status itself. This matters under
// CheerpX, where `pgrep -P` sees no children and SIGKILLing a process that
// still has a running child can crash the VM.
const WRAPPER = [
  '#!/bin/bash',
  '# agentmod process wrapper: $1 = process directory',
  'D="$1"',
  '[ -f "$D/env" ] && { . "$D/env"; rm -f "$D/env"; }',
  'cd "$(cat "$D/cwd")" 2>>"$D/stderr" || { echo 126 > "$D/exit"; exit 0; }',
  'if [ -f "$D/stdin" ]; then',
  '  exec 3< <(exec tail -c +1 -f "$D/stdin" 2>/dev/null)',
  '  echo $! > "$D/tailpid"',
  '  /bin/bash -c "$(cat "$D/cmd")" <&3 >>"$D/stdout" 2>>"$D/stderr" &',
  'else',
  '  /bin/bash -c "$(cat "$D/cmd")" </dev/null >>"$D/stdout" 2>>"$D/stderr" &',
  'fi',
  'echo $! > "$D/cmdpid"',
  'wait $!; rc=$?',
  '[ -f "$D/tailpid" ] && kill "$(cat "$D/tailpid")" 2>/dev/null',
  'echo "$rc" > "$D/exit.tmp"; mv "$D/exit.tmp" "$D/exit"',
].join('\n');

/** Shell snippet: is the wrapper for $D still running as pid $pid? Sets alive=0/1. */
const ALIVE = [
  'alive=0',
  'if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then',
  '  if [ -r "/proc/$pid/cmdline" ]; then tr "\\0" " " < "/proc/$pid/cmdline" | grep -q "procwrap" && alive=1; else alive=1; fi',
  'fi',
].join('\n');

/**
 * @param {object} o
 * @param {ReturnType<import('./runner.js').makeRunner>} o.runner
 * @param {object} o.target
 * @param {string} o.stateDir
 */
export function makeProcesses({ runner, target, stateDir }) {
  const PROCS = `${stateDir}/procs`;
  const WRAP = `${stateDir}/procwrap.sh`;
  let wrapperWritten = false;

  async function ensureWrapper() {
    if (wrapperWritten) return;
    await target.writeFile(WRAP, new TextEncoder().encode(WRAPPER + '\n'));
    wrapperWritten = true;
  }

  /** Stable process id for (session, call). */
  async function idFor(session, callId) {
    return `p${(await sha256(`${session}\u0000${callId}`)).slice(0, 10)}`;
  }

  function parseStatus(line) {
    const [id, pid, alive, exit, out, err, started, killed, ...rest] = line.split('|');
    const meta = (() => { try { return JSON.parse(rest.join('|') || '{}'); } catch { return {}; } })();
    const exitCode = exit === '' ? null : Number(exit);
    let state;
    if (exitCode != null && killed) state = 'killed';
    else if (exitCode != null) state = 'exited';
    else if (alive === '1') state = 'running';
    else if (killed) state = 'killed';
    else state = 'lost';
    return {
      id,
      pid: pid ? Number(pid) : null,
      state,
      exit_code: exitCode,
      killed: killed || null,
      stdout_bytes: Number(out || 0),
      stderr_bytes: Number(err || 0),
      started_at: started ? Number(started) * 1000 : null,
      session: meta.session,
      call_id: meta.call_id,
      name: meta.name,
      command: meta.command,
      cwd: meta.cwd,
    };
  }

  const STATUS_ONE = [
    'D="$1"; id=$(basename "$D")',
    'pid=$(cat "$D/pid" 2>/dev/null)',
    ALIVE,
    'exit=$(cat "$D/exit" 2>/dev/null)',
    'out=$(wc -c < "$D/stdout" 2>/dev/null | tr -d " "); err=$(wc -c < "$D/stderr" 2>/dev/null | tr -d " ")',
    'printf "%s|%s|%s|%s|%s|%s|%s|%s|%s\\n" "$id" "$pid" "$alive" "$exit" "${out:-0}" "${err:-0}" "$(cat "$D/started" 2>/dev/null)" "$(cat "$D/killed" 2>/dev/null)" "$(cat "$D/meta.json" 2>/dev/null)"',
  ].join('\n');

  async function status(id) {
    const D = `${PROCS}/${id}`;
    const r = await runner.run(`[ -d ${shq(D)} ] || exit 3\nbash -c ${shq(STATUS_ONE)} _ ${shq(D)}`, { cwd: '/', timeoutMs: 30_000 });
    if (r.exitCode === 3) return null;
    if (r.exitCode !== 0) throw new Error(`process status failed: ${r.stderr.slice(0, 300)}`);
    return parseStatus(r.stdout.trim());
  }

  async function list() {
    const r = await runner.run(`[ -d ${shq(PROCS)} ] || exit 0\nfor D in ${shq(PROCS)}/*/; do [ -d "$D" ] && bash -c ${shq(STATUS_ONE)} _ "\${D%/}"; done`, { cwd: '/', timeoutMs: 60_000 });
    return r.stdout.split('\n').filter(Boolean).map(parseStatus).sort((a, b) => (a.started_at ?? 0) - (b.started_at ?? 0) || a.id.localeCompare(b.id));
  }

  /**
   * Start a process (idempotent per session + call id).
   * @returns {Promise<{ process: object, existing: boolean }>}
   */
  async function start({ session, callId, command, cwd, name, stdin = false, env }) {
    await ensureWrapper();
    const id = await idFor(session, callId);
    const existing = await status(id);
    if (existing) return { process: existing, existing: true };
    const D = `${PROCS}/${id}`;
    const meta = JSON.stringify({ session, call_id: callId, name: name || null, command, cwd });
    const files = [
      { path: `${D}/cmd`, bytes: new TextEncoder().encode(command) },
      { path: `${D}/cwd`, bytes: new TextEncoder().encode(cwd) },
      { path: `${D}/meta.json`, bytes: new TextEncoder().encode(meta) },
    ];
    if (env && Object.keys(env).length) {
      files.push({ path: `${D}/env`, bytes: new TextEncoder().encode(Object.entries(env).map(([k, v]) => `export ${k}=${shq(v)}`).join('\n') + '\n') });
    }
    if (target.writeFiles) await target.writeFiles(files);
    else for (const f of files) await target.writeFile(f.path, f.bytes);
    const script = [
      `D=${shq(D)}`,
      ': > "$D/stdout"; : > "$D/stderr"; echo "0 0" > "$D/cursor"',
      stdin ? ': > "$D/stdin"' : 'rm -f "$D/stdin"',
      'date +%s > "$D/started"',
      `nohup /bin/bash ${shq(WRAP)} "$D" procwrap >/dev/null 2>&1 &`,
      'echo $! > "$D/pid"',
    ].join('\n');
    await runner.must(script, { cwd: '/', timeoutMs: 30_000, what: 'process start' });
    return { process: await status(id), existing: false };
  }

  /**
   * Read output since the process's cursor (or since explicit offsets).
   * Prefers the newest bytes when more than `max` is available.
   */
  async function read(id, { max = 16_000, advance = true } = {}) {
    const D = `${PROCS}/${id}`;
    const script = [
      `D=${shq(D)}; MAX=${Math.max(256, Math.floor(max))}`,
      '[ -d "$D" ] || exit 3',
      'set -- $(cat "$D/cursor" 2>/dev/null); co=${1:-0}; ce=${2:-0}',
      'so=$(wc -c < "$D/stdout" | tr -d " "); se=$(wc -c < "$D/stderr" | tr -d " ")',
      'no=$((so-co)); ne=$((se-ce)); [ $no -lt 0 ] && { co=0; no=$so; }; [ $ne -lt 0 ] && { ce=0; ne=$se; }',
      // Split the budget between the streams in proportion to what is new.
      'tot=$((no+ne)); if [ $tot -gt $MAX ]; then bo=$(( MAX * no / tot )); be=$(( MAX - bo )); else bo=$no; be=$ne; fi',
      'echo "$co $so $ce $se $bo $be"',
      '[ $bo -gt 0 ] && tail -c "$bo" "$D/stdout" | head -c "$((so-co))"',
      '[ $be -gt 0 ] && tail -c "$be" "$D/stderr" | head -c "$((se-ce))" >&2',
      advance ? 'echo "$so $se" > "$D/cursor"' : ':',
    ].join('\n');
    const r = await runner.run(script, { cwd: '/', timeoutMs: 30_000 });
    if (r.exitCode === 3) return null;
    // The header is the first line of stdout; the rest is the output slice.
    const nl = r.stdout.indexOf('\n');
    const [co, so, ce, se, bo, be] = r.stdout.slice(0, nl).trim().split(' ').map(Number);
    const stdout = r.stdout.slice(nl + 1);
    const skippedOut = so - co - bo;
    const skippedErr = se - ce - be;
    return { stdout, stderr: r.stderr, skipped_stdout: Math.max(0, skippedOut), skipped_stderr: Math.max(0, skippedErr), stdout_path: `${D}/stdout`, stderr_path: `${D}/stderr`, stdout_bytes: so, stderr_bytes: se };
  }

  /** Append to the process's stdin (only for processes started with stdin). */
  async function write(id, data, { close = false } = {}) {
    const D = `${PROCS}/${id}`;
    const script = [
      `D=${shq(D)}`,
      '[ -d "$D" ] || exit 3',
      '[ -f "$D/stdin" ] || exit 4',
      data ? `printf %s ${shq(data)} >> "$D/stdin"` : ':',
      close ? 'sleep 0.3; kill "$(cat "$D/tailpid" 2>/dev/null)" 2>/dev/null; :' : ':',
    ].join('\n');
    const r = await runner.run(script, { cwd: '/', timeoutMs: 30_000 });
    if (r.exitCode === 3) throw new Error(`no process ${id}`);
    if (r.exitCode === 4) throw new Error(`process ${id} was not started with stdin: true`);
  }

  /**
   * Terminate (SIGTERM) or force-kill (SIGKILL) a process: its command and the
   * command's children (where the OS lets us see them), leaf first. The
   * wrapper is left to record the exit status; it is signalled only when the
   * command's pid is unknown (a process started before this wrapper format).
   */
  async function kill(id, { force = false } = {}) {
    const D = `${PROCS}/${id}`;
    const sig = force ? 'KILL' : 'TERM';
    const script = [
      KILLTREE,
      `D=${shq(D)}`,
      '[ -d "$D" ] || exit 3',
      'pid=$(cat "$D/pid" 2>/dev/null)',
      ALIVE,
      '[ "$alive" = 1 ] || exit 0',
      `echo "SIG${sig} $(date +%s)" > "$D/killed"`,
      'cpid=$(cat "$D/cmdpid" 2>/dev/null)',
      `if [ -n "$cpid" ] && kill -0 "$cpid" 2>/dev/null; then killtree "$cpid" ${sig}; else killtree "$pid" ${sig}; fi`,
      ':',
    ].join('\n');
    const r = await runner.run(script, { cwd: '/', timeoutMs: 30_000 });
    if (r.exitCode === 3) throw new Error(`no process ${id}`);
    // Give the tree a moment to go, then report what is actually true.
    for (let i = 0; i < 10; i++) {
      const st = await status(id);
      if (!st || st.state !== 'running') return st;
      await new Promise((res) => setTimeout(res, 200));
    }
    return status(id);
  }

  return { idFor, start, status, list, read, write, kill, dir: (id) => `${PROCS}/${id}` };
}
