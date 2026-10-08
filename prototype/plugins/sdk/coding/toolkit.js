// The coding toolkit: every model-facing coding tool, implemented once against
// an execution target. Plugins choose the target (the in-browser Linux VM, a
// local directory); nothing here knows which one runs.
//
// Execution target interface (absolute paths in the target's namespace):
//
//   exec({ command, cwd, timeoutMs, signal, env? }) -> { exitCode, stdout, stderr, timedOut }
//       stdout/stderr are Uint8Array; `command` is a bash command line.
//       `env` is honoured only when the target sets `supportsEnv`.
//   readFile(path) -> Uint8Array | null
//   writeFile(path, bytes) / writeFiles([{ path, bytes, executable }])
//   identity() -> { kind, id }           optional: environment identity for provenance
//   lifecycle { status, logs, restart, stop }   optional (machines: the VM)
//
// Tool tiers: `core` tools are sent to the model every turn; `deferred` tools are
// found through tool_search (see sdk/projection.js). The core set is small on
// purpose: one shell, one process tool, structured reads/search, one patch tool.

import { toolSpec } from '../agentmod.js';
import { ToolError, normalizePath, resolveIn, relativeTo, shq, dirname } from './paths.js';
import { decode, decodeChecked, encode, isBinary, sha256, truncate, numberLines, splitLines, unifiedDiff, diffStat } from './text.js';
import { normalizeChanges, applyEdits, applyHunks } from './patch.js';
import { classifyCommand } from './shellclass.js';
import { parseDiagnostics, summarizeDiagnostics } from './diagnostics.js';
import { makeRunner, makeRedactor } from './runner.js';
import { makeCheckpoints } from './checkpoints.js';
import { makeProcesses } from './processes.js';
import { makeSearch } from './search.js';
import { makeRepoMap } from './repomap.js';
import { toB64 } from '../b64.js';

export const DEFAULT_LIMITS = Object.freeze({
  max_output_bytes: 20_000,
  default_timeout_seconds: 120,
  max_timeout_seconds: 1800,
  max_read_lines: 400,
  max_read_bytes: 60_000,
  max_list_entries: 300,
  max_search_results: 200,
  default_search_results: 50,
  max_process_read_bytes: 16_000,
  max_image_bytes: 3_000_000,
  import_max_files: 2000,
  import_max_file_bytes: 1_000_000,
  import_max_total_bytes: 30_000_000,
});

const INSTRUCTION_FILES = ['AGENTS.md', 'CLAUDE.md', '.agentmod/instructions.md', '.github/copilot-instructions.md'];
const POLICY_FILE = '.agentmod/policy.json';

/** Serialize work per key (one workspace's mutations never interleave). */
function keyedMutex() {
  const tails = new Map();
  return (key, fn) => {
    const prev = tails.get(key) || Promise.resolve();
    const run = prev.then(fn, fn);
    const tail = run.catch(() => {});
    tails.set(key, tail);
    tail.then(() => { if (tails.get(key) === tail) tails.delete(key); });
    return run;
  };
}
const mutex = keyedMutex();

const clampInt = (v, d, lo, hi) => {
  const n = Number.isFinite(Number(v)) && v !== null && v !== '' ? Math.trunc(Number(v)) : d;
  return Math.min(hi, Math.max(lo, n));
};
const kb = (n) => (n == null ? '' : n < 1024 ? `${n} B` : n < 1024 * 1024 ? `${(n / 1024).toFixed(1)} KB` : `${(n / 1048576).toFixed(1)} MB`);
const secs = (ms) => `${(ms / 1000).toFixed(ms < 10_000 ? 1 : 0)}s`;

/** Tool specifications (independent of any target), for schema checks and docs. */
export function toolSpecs({ L = DEFAULT_LIMITS, root = '/workspace', lifecycle = false, importRepos = true } = {}) {
  const P = (description, type = 'string', extra = {}) => ({ type, description, ...extra });
  const specs = [
    toolSpec('shell', `Run a bash command in the workspace (${root}); returns exit code, stdout, stderr, duration. Non-interactive: stdin is empty. Use it for builds, tests, git, package managers and any CLI. For servers, watchers, REPLs or anything long-running use \`process\`. Timeout default ${L.default_timeout_seconds}s, max ${L.max_timeout_seconds}s. Long output is truncated (head+tail) and saved to a file you can read_file.`, {
      command: P('bash command line'),
      cwd: P(`working directory (absolute, or relative to ${root})`),
      timeout_seconds: P('kill the command after this many seconds', 'integer'),
      secrets: P('names of configured secrets this command needs in its environment (policy may ask the user)', 'array', { items: { type: 'string' } }),
    }, { required: ['command'], tier: 'core', group: 'shell', effects: 'varies' }),
    toolSpec('process', 'Manage long-running processes (dev servers, watch mode, REPLs, long test runs). start returns an id; read returns new output since the last read; wait blocks until `until` (a regex) appears in new output, the process exits, or the timeout; write sends stdin (start with stdin: true); kill terminates (force: SIGKILL); status/list report state. Processes survive this tool call and are reconciled after restarts.', {
      action: P('start | read | wait | write | kill | status | list', 'string', { enum: ['start', 'read', 'wait', 'write', 'kill', 'status', 'list'] }),
      id: P('process id (from start)'),
      command: P('start: bash command line'),
      cwd: P('start: working directory'),
      name: P('start: short label'),
      stdin: P('start: keep stdin open for write', 'boolean'),
      input: P('write: text to send (include \\n for Enter)'),
      close_stdin: P('write: close stdin after sending', 'boolean'),
      until: P('wait: regex to wait for in new output'),
      timeout_seconds: P('wait: maximum seconds (default 30, max 600)', 'integer'),
      force: P('kill: SIGKILL instead of SIGTERM', 'boolean'),
    }, { required: ['action'], tier: 'core', group: 'shell', effects: 'varies' }),
    toolSpec('read_file', `Read a text file with line numbers. Returns at most ${L.max_read_lines} lines; pass start_line/end_line for other ranges. The header gives the file's sha256 (use it as expected_sha256 in apply_patch to guard against stale edits).`, {
      path: P(`file path (absolute, or relative to ${root})`),
      start_line: P('first line (1-based, default 1)', 'integer'),
      end_line: P('last line (inclusive)', 'integer'),
    }, { required: ['path'], tier: 'core', group: 'files', effects: 'read' }),
    toolSpec('list_dir', 'List a directory: names, types, sizes. depth 1 (default) to 5. Heavy directories (.git, node_modules, target, …) are listed but not descended.', {
      path: P(`directory (default ${root})`),
      depth: P('levels to descend (1-5)', 'integer'),
    }, { tier: 'core', group: 'files', effects: 'read' }),
    toolSpec('search_files', 'Find files by glob, e.g. "**/*.rs", "src/**", "**/Cargo.toml", "*.test.ts". Respects .gitignore. Returns paths and sizes.', {
      pattern: P('glob pattern'),
      path: P('directory to search (default: workspace root)'),
    }, { required: ['pattern'], tier: 'core', group: 'files', effects: 'read' }),
    toolSpec('search_text', 'Search file contents (ripgrep-style regex; smart case unless case_sensitive is set). Returns path:line:column with a short excerpt, grouped by file, sorted. Respects .gitignore.', {
      query: P('regex (or literal text with regex: false)'),
      path: P('directory to search (default: workspace root)'),
      regex: P('treat query as a regex (default true)', 'boolean'),
      case_sensitive: P('force case sensitivity', 'boolean'),
      include: P('only files matching these globs', 'array', { items: { type: 'string' } }),
      exclude: P('skip files matching these globs', 'array', { items: { type: 'string' } }),
      max_results: P(`default ${L.default_search_results}, max ${L.max_search_results}`, 'integer'),
    }, { required: ['query'], tier: 'core', group: 'files', effects: 'read' }),
    toolSpec('apply_patch', 'Create, update, delete, or move files. Prefer edits: each old_text must match the current file exactly once (or set replace_all); edits that no longer match are rejected, so read files before editing. All changes apply together or not at all, after an automatic workspace checkpoint. Alternatively pass `patch` in the "*** Begin Patch" format.', {
      changes: P('list of { action: create|update|delete|move, path, content? (create, or full replacement), edits?: [{ old_text, new_text, replace_all? }], to? (move), expected_sha256? }', 'array', {
        items: {
          type: 'object',
          properties: {
            action: { type: 'string', enum: ['create', 'update', 'delete', 'move'] },
            path: { type: 'string' },
            content: { type: 'string' },
            edits: { type: 'array', items: { type: 'object', properties: { old_text: { type: 'string' }, new_text: { type: 'string' }, replace_all: { type: 'boolean' } }, required: ['old_text', 'new_text'] } },
            to: { type: 'string' },
            expected_sha256: { type: 'string' },
          },
          required: ['action', 'path'],
        },
      }),
      patch: P('alternative to changes: a "*** Begin Patch" … "*** End Patch" envelope'),
    }, { tier: 'core', group: 'files', effects: 'write' }),
    toolSpec('view_image', 'Look at an image file in the workspace (PNG, JPEG, GIF, WebP), e.g. a screenshot or generated chart. The image is attached for you to see.', {
      path: P('image path'),
    }, { required: ['path'], tier: 'deferred', group: 'files', effects: 'read' }),
    toolSpec('repo_map', 'A compact map of the repository: file counts per directory and top-level symbol signatures per source file (approximate, regex-based). Use it to orient in an unfamiliar codebase.', {
      path: P('directory (default: workspace root)'),
      max_files: P('source files to include (default 200)', 'integer'),
    }, { tier: 'deferred', group: 'files', effects: 'read' }),
    toolSpec('checkpoints', 'Workspace checkpoints taken automatically before edits and mutating commands. list them, diff one against now (or two against each other), or restore the workspace to one (the current state is checkpointed first, so a restore can be undone).', {
      action: P('list | diff | restore', 'string', { enum: ['list', 'diff', 'restore'] }),
      checkpoint: P('checkpoint id'),
      to: P('diff: second checkpoint (default: the current workspace)'),
      paths: P('diff: limit to these paths', 'array', { items: { type: 'string' } }),
    }, { required: ['action'], tier: 'deferred', group: 'workspace', effects: 'varies' }),
  ];
  if (importRepos) {
    specs.push(toolSpec('import_repo', 'Copy a public GitHub repository into the workspace as a fresh git repository (one commit). Downloads happen outside the workspace, so it works without network access inside it.', {
      repo: P('"owner/name" or https://github.com/owner/name'),
      ref: P('branch, tag, or commit (default: the default branch)'),
      dest: P(`destination directory (default ${root}/<name>)`),
    }, { required: ['repo'], tier: 'deferred', group: 'workspace', effects: 'write' }));
  }
  if (lifecycle) {
    specs.push(
      toolSpec('sandbox_status', 'The sandbox VM\'s state (running, busy, stopped, crashed), current operation, uptime, counts, and browser.', {}, { tier: 'deferred', group: 'sandbox', effects: 'read' }),
      toolSpec('sandbox_logs', 'The sandbox VM\'s own journal, kept outside the VM so it survives a crash: boots, every command with exit code and duration, timeouts, stalls, page errors, console. Use it when commands fail strangely or the sandbox stops responding.', {
        limit: P('most recent entries (default 60, max 500)', 'integer'),
        kinds: P('only these kinds: boot-start, boot-ok, boot-failed, op, op-error, stalled, page-error, interrupt, restart, stopped, note', 'array', { items: { type: 'string' } }),
      }, { tier: 'deferred', group: 'sandbox', effects: 'read' }),
      toolSpec('sandbox_restart', 'Throw away the current VM, crashed or not, and boot a fresh one. Workspace files are kept; running processes are lost.', {}, { tier: 'deferred', group: 'sandbox', effects: 'write' }),
      toolSpec('sandbox_stop', 'Stop the sandbox VM to free the browser\'s CPU and memory. Workspace files are kept; the next coding tool starts it again.', {}, { tier: 'deferred', group: 'sandbox', effects: 'write' }),
    );
  }
  return specs;
}

/**
 * Build the toolkit for one execution target and workspace root.
 *
 * @param {object} o
 * @param {object} o.target
 * @param {string} o.root                 absolute workspace root in the target
 * @param {string} [o.stateDir]           default `<root>/.agentmod/state` (repository config may live in .agentmod/)
 * @param {object} [o.limits]
 * @param {boolean} [o.importRepos]
 * @param {Function} [o.fetch]
 * @param {object} [o.env]                extra environment for commands
 * @param {Record<string, {value: string, commands?: string[]}>} [o.secrets]
 * @param {object} [o.diagnostics]        { patterns: [{ regex, source }] }
 * @param {boolean} [o.checkpoints]       default true
 * @param {string} [o.shadowGitDir]       shared checkpoint object store (isolated worktrees)
 */
export function codingToolkit({ target, root, stateDir, limits = {}, importRepos = true, fetch: fetchImpl = globalThis.fetch, env = {}, secrets = {}, diagnostics = {}, checkpoints: useCheckpoints = true, shadowGitDir }) {
  const L = { ...DEFAULT_LIMITS, ...limits };
  const ROOT = normalizePath(root);
  const STATE = normalizePath(stateDir || `${ROOT}/.agentmod/state`);
  const runner = makeRunner({ target, stateDir: STATE });
  const cps = makeCheckpoints({ runner, root: ROOT, stateDir: STATE, gitDir: shadowGitDir });
  const procs = makeProcesses({ runner, target, stateDir: STATE });
  let probed = null;
  const probe = () => (probed ??= runner.run('for t in rg git; do command -v $t >/dev/null 2>&1 && printf "%s " $t; done; find --version >/dev/null 2>&1 && printf gnufind; true', { cwd: '/', timeoutMs: 30_000 }).then((r) => {
    const w = r.stdout.split(/\s+/);
    return { rg: w.includes('rg'), git: w.includes('git'), gnuFind: w.includes('gnufind') };
  }).catch((e) => { probed = null; throw e; }));
  const search = makeSearch({ runner, probe });
  const repoMap = makeRepoMap({ runner, search, probe });
  const redactSecrets = makeRedactor(Object.fromEntries(Object.entries(secrets).map(([k, s]) => [k, s?.value])));
  const redact = (t) => redactSecrets(t);
  const specs = toolSpecs({ L, root: ROOT, lifecycle: !!target.lifecycle, importRepos });
  const names = new Set(specs.map((s) => s.name));
  const lifecycleNames = new Set(specs.filter((s) => s.group === 'sandbox').map((s) => s.name));
  const rel = (abs) => relativeTo(ROOT, abs) || '.';

  // ------------------------------------------------------------------
  // Checkpoints
  // ------------------------------------------------------------------

  const running = new Set(); // in-flight marker names of commands running now

  /** Snapshot before a mutation; emits checkpoint-created for new trees. */
  async function checkpoint(reason, { session, call_id, emit, signal } = {}) {
    if (!useCheckpoints) return null;
    try {
      const cp = await cps.snapshot({ reason, meta: { session, call_id }, signal });
      // Markers of commands that are not running now: they died mid-command
      // (a crash, a cancel, a VM restart) and may have changed files.
      cp.interrupted = cp.inflight.filter((n) => !running.has(n));
      if (cp.interrupted.length) await cps.clearInflight(cp.interrupted);
      if (cp.created) await emit?.('checkpoint-created', { checkpoint: cp.checkpoint, tree: cp.tree, reason, call_id: call_id ?? null, root: ROOT }, { v: 1, kind: 'checkpoint', checkpoint: cp.checkpoint, reason });
      return cp;
    } catch (e) {
      if (signal?.aborted) throw e;
      // A workspace without git cannot be checkpointed; the edit still proceeds.
      return { error: String(e.message || e).slice(0, 300) };
    }
  }

  // ------------------------------------------------------------------
  // shell
  // ------------------------------------------------------------------

  function grantedSecrets(names = [], command) {
    const out = {};
    const used = [];
    for (const name of Array.isArray(names) ? names : []) {
      const s = secrets[name];
      if (!s || typeof s.value !== 'string') throw new ToolError(`unknown secret \`${name}\` (configured: ${Object.keys(secrets).join(', ') || 'none'})`);
      const allowed = Array.isArray(s.commands) && s.commands.length ? s.commands.some((p) => new RegExp(p).test(command.trim())) : true;
      if (!allowed) throw new ToolError(`secret \`${name}\` is not allowed for this command (allowed patterns: ${s.commands.join(', ')})`);
      out[s.env || name] = s.value;
      used.push(name);
    }
    return { env: out, used };
  }

  async function shell(args, { signal, session, call_id, emit }) {
    const command = String(args.command ?? '').trim();
    if (!command) throw new ToolError('`command` is required');
    const cwd = args.cwd ? resolveIn(ROOT, String(args.cwd)) : ROOT;
    const timeoutSeconds = clampInt(args.timeout_seconds, L.default_timeout_seconds, 1, L.max_timeout_seconds);
    const cls = classifyCommand(command);
    const granted = grantedSecrets(args.secrets, command);
    let cp = null;
    let line = command;
    let cleanup = () => {};
    if (!cls.readOnly) {
      cp = await mutex(ROOT, () => checkpoint(`before shell: ${command.slice(0, 80)}`, { session, call_id, emit, signal }));
      // An in-flight marker survives a crash or cancel, so the next call can say
      // that this command may have changed files without finishing.
      const markerName = String(call_id || `call-${Date.now()}`).replace(/[^\w.-]/g, '_');
      const marker = `${STATE}/inflight/${markerName}`;
      running.add(markerName);
      cleanup = () => running.delete(markerName);
      line = `mkdir -p ${shq(`${STATE}/inflight`)} && printf '%s' ${shq(command.slice(0, 200))} > ${shq(marker)}\n(\n${command}\n)\nrc=$?; rm -f ${shq(marker)}; exit $rc`;
    }
    let r;
    try {
      r = await runner.run(line, { cwd, timeoutMs: timeoutSeconds * 1000, signal, env: { ...env, ...granted.env } });
    } finally {
      cleanup();
    }
    const stdout = redact(r.stdout);
    const stderr = redact(r.stderr);
    const diags = parseDiagnostics(`${stdout}\n${stderr}`, { patterns: diagnostics.patterns || [] });
    const budget = L.max_output_bytes;
    const total = Math.max(1, stdout.length + stderr.length);
    const outShown = truncate(stdout, Math.max(512, Math.floor((budget * stdout.length) / total)));
    const errShown = truncate(stderr, Math.max(512, Math.floor((budget * stderr.length) / total)));
    const truncated = outShown !== stdout || errShown !== stderr;
    let fullPath = null;
    if (truncated) {
      fullPath = `${STATE}/outputs/${String(call_id || Date.now()).replace(/[^\w.-]/g, '_')}.log`;
      try {
        await target.writeFile(fullPath, encode(`$ ${command}\n--- stdout ---\n${stdout}\n--- stderr ---\n${stderr}\n--- exit ${r.exitCode} ---\n`));
      } catch {
        fullPath = null;
      }
    }
    const head = [`exit ${r.exitCode}${r.timedOut ? ` (timed out after ${timeoutSeconds}s)` : ''} · ${secs(r.ms)}${truncated ? ` · output truncated${fullPath ? `; full output: ${rel(fullPath)}` : ''}` : ''}`];
    if (cp?.interrupted?.length) head.push(`note: earlier command(s) were interrupted before finishing: ${cp.interrupted.join(', ')}`);
    const summary = summarizeDiagnostics(diags);
    const parts = [head.join('\n')];
    if (summary) parts.push(summary);
    if (outShown) parts.push(`--- stdout ---\n${outShown}`);
    if (errShown) parts.push(`--- stderr ---\n${errShown}`);
    if (!outShown && !errShown) parts.push('(no output)');
    if (diags.length) await emit?.('diagnostics', { call_id: call_id ?? null, diagnostics: diags.slice(0, 50), command: command.slice(0, 200) }, { v: 1, kind: 'diagnostics', items: diags.slice(0, 50) });
    return {
      output: parts.join('\n'),
      error: r.exitCode !== 0 || r.timedOut,
      summary: r.timedOut ? 'timed out' : `exit ${r.exitCode}`,
      data: {
        exit_code: r.exitCode,
        timed_out: r.timedOut,
        duration_ms: r.ms,
        truncated,
        full_output: fullPath ? rel(fullPath) : null,
        read_only: cls.readOnly,
        checkpoint: cp?.checkpoint ?? null,
        diagnostics: diags.length,
        secrets_used: granted.used,
        environment: target.identity?.() ?? null,
      },
    };
  }

  // ------------------------------------------------------------------
  // process
  // ------------------------------------------------------------------

  function describeProc(p) {
    if (!p) return '(unknown process)';
    const st = p.state === 'exited' ? `exited with code ${p.exit_code}` : p.state === 'killed' ? `killed (${p.killed?.split(' ')[0] || 'signal'})` : p.state === 'lost' ? 'lost (not running and no exit status: the environment restarted or it was killed externally)' : `running (pid ${p.pid})`;
    return `${p.id}${p.name ? ` [${p.name}]` : ''}: ${st} · ${kb(p.stdout_bytes)} stdout, ${kb(p.stderr_bytes)} stderr · ${p.command ? `$ ${String(p.command).slice(0, 120)}` : ''}`;
  }

  function formatRead(rd) {
    const parts = [];
    if (rd.skipped_stdout) parts.push(`(${kb(rd.skipped_stdout)} of earlier stdout skipped; full log: ${rel(rd.stdout_path)})`);
    if (rd.stdout) parts.push(`--- stdout ---\n${redact(rd.stdout)}`);
    if (rd.skipped_stderr) parts.push(`(${kb(rd.skipped_stderr)} of earlier stderr skipped; full log: ${rel(rd.stderr_path)})`);
    if (rd.stderr) parts.push(`--- stderr ---\n${redact(rd.stderr)}`);
    if (!rd.stdout && !rd.stderr) parts.push('(no new output)');
    return parts.join('\n');
  }

  async function processTool(args, { signal, session, call_id, emit }) {
    const action = String(args.action || '');
    const needId = () => {
      const id = String(args.id || '').trim();
      if (!/^p[0-9a-f]{10}$/.test(id)) throw new ToolError('`id` is required (as returned by start or list)');
      return id;
    };
    if (action === 'start') {
      const command = String(args.command ?? '').trim();
      if (!command) throw new ToolError('start needs `command`');
      const cwd = args.cwd ? resolveIn(ROOT, String(args.cwd)) : ROOT;
      const cp = await mutex(ROOT, () => checkpoint(`before process: ${command.slice(0, 80)}`, { session, call_id, emit, signal }));
      const { process: p, existing } = await procs.start({ session: session || '-', callId: call_id || String(Date.now()), command, cwd, name: args.name, stdin: !!args.stdin, env });
      if (!existing) await emit?.('process-started', { process_id: p.id, command: command.slice(0, 500), cwd: rel(cwd), name: args.name || null, call_id: call_id ?? null, pid: p.pid }, { v: 1, kind: 'process', id: p.id, name: args.name || null, command: command.slice(0, 200), state: 'running' });
      // Give it a moment, then show the first output.
      await new Promise((res) => setTimeout(res, 800));
      const rd = await procs.read(p.id, { max: L.max_process_read_bytes });
      const now = await procs.status(p.id);
      return { output: `${existing ? 'Already started by this call: ' : 'Started '}${describeProc(now)}\n${formatRead(rd)}`, summary: now.state, data: { process_id: p.id, state: now.state, checkpoint: cp?.checkpoint ?? null } };
    }
    if (action === 'list') {
      const all = await procs.list();
      const mine = all.filter((p) => !session || p.session === session);
      return { output: mine.length ? mine.map(describeProc).join('\n') : 'No processes in this session.', summary: `${mine.length} processes`, data: { processes: mine.map((p) => ({ id: p.id, state: p.state, exit_code: p.exit_code })) } };
    }
    const id = needId();
    if (action === 'status') {
      const p = await procs.status(id);
      if (!p) throw new ToolError(`no process ${id}`);
      return { output: describeProc(p), summary: p.state, data: { process_id: id, state: p.state, exit_code: p.exit_code } };
    }
    if (action === 'read') {
      const rd = await procs.read(id, { max: L.max_process_read_bytes });
      if (!rd) throw new ToolError(`no process ${id}`);
      const p = await procs.status(id);
      await observeExit(p, emit, call_id);
      return { output: `${describeProc(p)}\n${formatRead(rd)}`, summary: p.state, data: { process_id: id, state: p.state, exit_code: p.exit_code } };
    }
    if (action === 'wait') {
      const timeout = clampInt(args.timeout_seconds, 30, 1, 600) * 1000;
      let re = null;
      if (args.until) {
        try { re = new RegExp(String(args.until), 'm'); } catch (e) { throw new ToolError(`invalid \`until\` regex: ${e.message}`); }
      }
      const t0 = Date.now();
      let seen = '';
      let p;
      let matched = false;
      for (;;) {
        if (signal?.aborted) throw new Error('cancelled');
        const peek = await procs.read(id, { max: L.max_process_read_bytes, advance: false });
        if (!peek) throw new ToolError(`no process ${id}`);
        seen = `${peek.stdout}\n${peek.stderr}`;
        p = await procs.status(id);
        if (re && re.test(seen)) { matched = true; break; }
        if (p.state !== 'running') break;
        if (Date.now() - t0 >= timeout) break;
        await new Promise((res) => setTimeout(res, 500));
      }
      const rd = await procs.read(id, { max: L.max_process_read_bytes });
      await observeExit(p, emit, call_id);
      const why = matched ? `matched /${args.until}/` : p.state !== 'running' ? `process ${p.state}` : `timed out after ${secs(Date.now() - t0)}`;
      return { output: `${why} · ${describeProc(p)}\n${formatRead(rd)}`, summary: why, error: !matched && !!re && p.state === 'running', data: { process_id: id, state: p.state, matched } };
    }
    if (action === 'write') {
      const input = args.input == null ? '' : String(args.input);
      if (input.length > 65536) throw new ToolError('input is limited to 64 KB per write');
      await procs.write(id, input, { close: !!args.close_stdin });
      await new Promise((res) => setTimeout(res, 500));
      const rd = await procs.read(id, { max: L.max_process_read_bytes });
      const p = await procs.status(id);
      return { output: `sent ${input.length} chars${args.close_stdin ? ' and closed stdin' : ''} · ${describeProc(p)}\n${formatRead(rd)}`, summary: 'sent', data: { process_id: id, state: p.state } };
    }
    if (action === 'kill') {
      const p = await procs.kill(id, { force: !!args.force });
      await observeExit(p, emit, call_id);
      const still = p?.state === 'running';
      return { output: still ? `${id} is still running after SIG${args.force ? 'KILL' : 'TERM'}; try force: true` : `Stopped. ${describeProc(p)}`, error: still, summary: p?.state, data: { process_id: id, state: p?.state } };
    }
    throw new ToolError('`action` must be one of start, read, wait, write, kill, status, list');
  }

  const exitsSeen = new Set();
  async function observeExit(p, emit, call_id) {
    if (!p || p.state === 'running' || exitsSeen.has(p.id)) return;
    exitsSeen.add(p.id);
    await emit?.('process-exited', { process_id: p.id, state: p.state, exit_code: p.exit_code, observed_by: call_id ?? null }, { v: 1, kind: 'process', id: p.id, state: p.state, exit_code: p.exit_code });
  }

  // ------------------------------------------------------------------
  // Files
  // ------------------------------------------------------------------

  const READ_SCRIPT = [
    'f="$1"; a="$2"; b="$3"; maxb="$4"',
    '[ -e "$f" ] || exit 3',
    '[ -d "$f" ] && exit 4',
    '[ -r "$f" ] || exit 5',
    'size=$(wc -c < "$f" | tr -d " ")',
    'sum=$( (sha256sum "$f" 2>/dev/null || shasum -a 256 "$f") | cut -c1-64)',
    'lines=$(wc -l < "$f" | tr -d " ")',
    '[ "$size" -gt 0 ] && [ "$(tail -c 1 "$f" | od -An -tx1 | tr -d " \\n")" != "0a" ] && lines=$((lines+1))',
    'probe=$(head -c 8000 "$f" | wc -c | tr -d " "); text=$(head -c 8000 "$f" | tr -d "\\000" | wc -c | tr -d " ")',
    'echo "$size $sum $lines $probe $text"',
    '[ "$probe" = "$text" ] && sed -n "${a},${b}p" "$f" | head -c "$maxb"',
    'true',
  ].join('\n');

  async function readFile(args) {
    const abs = resolveIn(ROOT, args.path);
    const start = clampInt(args.start_line, 1, 1, Number.MAX_SAFE_INTEGER);
    const wantEnd = args.end_line != null ? clampInt(args.end_line, start, start, Number.MAX_SAFE_INTEGER) : start + L.max_read_lines - 1;
    const end = Math.min(wantEnd, start + L.max_read_lines - 1);
    const r = await runner.run(`bash -c ${shq(READ_SCRIPT)} _ ${shq(abs)} ${start} ${end} ${L.max_read_bytes}`, { cwd: '/', timeoutMs: 60_000 });
    if (r.exitCode === 3) throw new ToolError(`${rel(abs)} does not exist`);
    if (r.exitCode === 4) throw new ToolError(`${rel(abs)} is a directory; use list_dir`);
    if (r.exitCode === 5) throw new ToolError(`${rel(abs)} is not readable`);
    if (r.exitCode !== 0) throw new ToolError(`reading ${rel(abs)} failed: ${r.stderr.slice(0, 300)}`);
    const nl = r.stdoutBytes.indexOf(10);
    const header = decode(r.stdoutBytes.subarray(0, nl)).trim().split(' ');
    const [size, sum, total, probeN, textN] = [Number(header[0]), header[1], Number(header[2]), Number(header[3]), Number(header[4])];
    if (probeN !== textN) {
      const img = /\.(png|jpe?g|gif|webp)$/i.test(abs) ? ' Use view_image to look at it.' : ' Inspect it with shell (e.g. `xxd file | head`).';
      return { output: `${rel(abs)} is a binary file (${kb(size)}, sha256 ${sum}).${img}`, summary: 'binary', data: { sha256: sum, size, binary: true } };
    }
    const { text, valid } = decodeChecked(r.stdoutBytes.subarray(nl + 1));
    let lines = splitLines(text);
    const clipped = lines.length < end - start + 1 && start + lines.length - 1 < total;
    if (start > total && total > 0) throw new ToolError(`${rel(abs)} has ${total} lines; start_line ${start} is past the end`);
    lines = lines.slice(0, end - start + 1);
    const shownEnd = start + lines.length - 1;
    const more = shownEnd < total;
    const head = total === 0
      ? `${rel(abs)} is empty · sha256 ${sum}`
      : `${rel(abs)} · lines ${start}-${shownEnd} of ${total} · ${kb(size)} · sha256 ${sum}${more ? ` · continue with start_line ${shownEnd + 1}` : ''}${clipped ? ' (byte limit reached)' : ''}${valid ? '' : ' · not valid UTF-8 (shown with replacement characters)'}`;
    return { output: `${head}\n${numberLines(lines.map(redact), start)}`, summary: `${lines.length} lines`, data: { sha256: sum, size, lines: total, start, end: shownEnd } };
  }

  async function listDir(args, { signal }) {
    const abs = args.path ? resolveIn(ROOT, String(args.path)) : ROOT;
    const depth = clampInt(args.depth, 1, 1, 5);
    const res = await search.listDir(abs, { root: ROOT, depth, maxEntries: L.max_list_entries, signal });
    if (!res) throw new ToolError(`${rel(abs)} is not a directory`);
    const body = res.entries.map((e) => `${e.path}${e.type === 'dir' ? '/' : ''}${e.skipped ? '  (not descended)' : e.type === 'file' ? `  ${kb(e.size)}` : ''}`).join('\n');
    return {
      output: `${rel(abs)}/ (depth ${depth}): ${res.total} entr${res.total === 1 ? 'y' : 'ies'}${res.truncated ? `, first ${res.entries.length} shown (narrow the path or depth)` : ''}\n${body || '(empty)'}`,
      summary: `${res.total} entries`,
      data: { total: res.total, truncated: res.truncated },
    };
  }

  async function searchFiles(args, { signal }) {
    const pattern = String(args.pattern ?? '').trim();
    if (!pattern) throw new ToolError('`pattern` is required');
    const abs = args.path ? resolveIn(ROOT, String(args.path)) : ROOT;
    const res = await search.searchFiles(abs, { root: ROOT, pattern, maxResults: L.max_search_results / 2, signal });
    if (!res) throw new ToolError(`${rel(abs)} is not a directory`);
    const body = res.files.map((f) => `${f.path}  ${kb(f.size)}`).join('\n');
    return { output: `${res.total} file${res.total === 1 ? '' : 's'} match ${JSON.stringify(pattern)}${res.truncated ? ` (first ${res.files.length})` : ''}\n${body}`.trim(), summary: `${res.total} files`, data: { total: res.total, truncated: res.truncated } };
  }

  async function searchText(args, { signal }) {
    const query = String(args.query ?? '');
    if (!query) throw new ToolError('`query` is required');
    const regex = args.regex !== false;
    if (regex) {
      try { new RegExp(query); } catch (e) { throw new ToolError(`invalid regex: ${e.message} (pass regex: false for literal text)`); }
    }
    const abs = args.path ? resolveIn(ROOT, String(args.path)) : ROOT;
    const list = (v) => (Array.isArray(v) ? v.map(String) : v ? [String(v)] : []);
    const res = await search.searchText(abs, {
      root: ROOT,
      query,
      regex,
      caseSensitive: args.case_sensitive == null ? undefined : !!args.case_sensitive,
      include: list(args.include),
      exclude: list(args.exclude),
      maxResults: clampInt(args.max_results, L.default_search_results, 1, L.max_search_results),
      signal,
    });
    if (!res) throw new ToolError(`${rel(abs)} is not a directory`);
    const groups = [];
    let last = null;
    for (const m of res.matches) {
      if (m.path !== last) { groups.push(m.path); last = m.path; }
      groups.push(`  ${m.line}:${m.col}: ${redact(m.text)}`);
    }
    const head = res.total ? `${res.total} match${res.total === 1 ? '' : 'es'} in ${res.files} file${res.files === 1 ? '' : 's'}${res.truncated ? ` (first ${res.matches.length} shown; narrow with path/include)` : ''} · ${res.engine}` : `no matches for ${JSON.stringify(query)} · ${res.engine}`;
    return { output: `${head}\n${groups.join('\n')}`.trim(), summary: `${res.total} matches`, data: { total: res.total, files: res.files, truncated: res.truncated } };
  }

  async function readCurrent(abs) {
    const bytes = await target.readFile(abs);
    if (bytes == null) return null;
    return { bytes, binary: isBinary(bytes), text: decode(bytes), sha: await sha256(bytes) };
  }

  async function applyPatch(args, { signal, session, call_id, emit }) {
    const changes = normalizeChanges(args).map((c) => ({ ...c, abs: resolveIn(ROOT, c.path), toAbs: c.to ? resolveIn(ROOT, c.to) : null }));
    for (const c of changes) {
      if (c.abs.startsWith(`${STATE}/`) || c.abs === STATE || c.toAbs?.startsWith(`${STATE}/`)) throw new ToolError(`${rel(c.abs)} is in the toolkit's state directory`);
      if (c.abs === ROOT) throw new ToolError('cannot patch the workspace root itself');
    }
    return mutex(ROOT, async () => {
      // 1. Validate everything against the files as they are now.
      const files = new Map(); // abs -> { orig, now: text|null, exists, sha }
      const load = async (abs) => {
        if (!files.has(abs)) {
          const cur = await readCurrent(abs);
          files.set(abs, { orig: cur, now: cur ? cur.text : null, binary: cur?.binary ?? false });
        }
        return files.get(abs);
      };
      const touched = [];
      for (const [i, c] of changes.entries()) {
        const where = `change ${i + 1} (${c.action} ${rel(c.abs)})`;
        const f = await load(c.abs);
        if (c.expected_sha256) {
          const want = c.expected_sha256;
          if (want.length < 8) throw new ToolError(`${where}: expected_sha256 needs at least 8 hex characters`);
          if (!f.orig) throw new ToolError(`${where}: expected_sha256 given but the file does not exist`);
          if (!f.orig.sha.startsWith(want)) throw new ToolError(`${where}: the file changed since it was read (sha256 is ${f.orig.sha.slice(0, 16)}…, expected ${want.slice(0, 16)}…); re-read it`);
        }
        if (c.action === 'create') {
          if (f.now != null && !c.overwrite) throw new ToolError(`${where}: the file already exists; use update (or overwrite: true)`);
          f.now = c.content;
        } else if (c.action === 'delete') {
          if (f.now == null) throw new ToolError(`${where}: the file does not exist`);
          f.now = null;
        } else {
          if (f.now == null) throw new ToolError(`${where}: the file does not exist (use create)`);
          if (f.binary && (c.edits || c.hunks)) throw new ToolError(`${where}: the file is binary; edits apply to text files only`);
          let text = f.now;
          if (c.content !== undefined) text = c.content;
          if (c.edits) text = applyEdits(text, c.edits, rel(c.abs));
          if (c.hunks) text = applyHunks(text, c.hunks, rel(c.abs));
          if (c.action === 'move') {
            const dst = await load(c.toAbs);
            if (dst.now != null) throw new ToolError(`${where}: destination ${rel(c.toAbs)} already exists`);
            dst.now = text;
            dst.movedFrom = c.abs;
            f.now = null;
            f.movedTo = c.toAbs;
            touched.push(c.toAbs);
          } else {
            f.now = text;
          }
        }
        touched.push(c.abs);
      }
      const effective = [...files.entries()].filter(([, f]) => (f.orig?.text ?? null) !== f.now || (f.orig && f.now != null && f.orig.binary));
      if (!effective.length) return { output: 'No changes: the patch leaves every file as it is.', summary: 'no-op', data: { files: [] } };
      // 2. Checkpoint, then write; on failure, restore the checkpoint.
      const cp = await checkpoint(`before apply_patch: ${effective.map(([abs]) => rel(abs)).slice(0, 4).join(', ')}`, { session, call_id, emit, signal });
      const writes = effective.filter(([, f]) => f.now != null).map(([abs, f]) => ({ path: abs, bytes: encode(f.now) }));
      const removes = effective.filter(([, f]) => f.now == null).map(([abs]) => abs);
      try {
        // Moves keep the source's mode: mv first, then write new content.
        const moves = effective.filter(([, f]) => f.movedTo).map(([abs, f]) => [abs, f.movedTo]);
        if (moves.length) await runner.must(moves.map(([a, b]) => `mkdir -p -- ${shq(dirname(b))} && mv -- ${shq(a)} ${shq(b)}`).join(' && '), { cwd: '/', timeoutMs: 60_000, what: 'move' });
        if (writes.length) {
          if (target.writeFiles) await target.writeFiles(writes);
          else for (const w of writes) await target.writeFile(w.path, w.bytes);
        }
        const stillThere = removes.filter((a) => !moves.some(([src]) => src === a));
        if (stillThere.length) await runner.must(`rm -f -- ${stillThere.map(shq).join(' ')}`, { cwd: '/', timeoutMs: 60_000, what: 'delete' });
      } catch (e) {
        let rolled = 'not attempted (no checkpoint)';
        if (cp?.tree) {
          try {
            const now = await cps.snapshot({ reason: 'failed apply_patch (rolled back)', meta: { session, call_id } });
            await cps.restore(cp.tree, now.tree);
            rolled = `rolled back to checkpoint ${cp.checkpoint.slice(0, 12)}`;
          } catch (re) {
            rolled = `rollback failed: ${re.message}`;
          }
        }
        throw new ToolError(`writing the patch failed: ${e.message}; ${rolled}`);
      }
      // 3. Report diffs and new identities.
      const results = [];
      const diffs = [];
      for (const [abs, f] of effective) {
        const before = f.orig?.text ?? '';
        const after = f.now ?? '';
        const action = f.movedTo ? 'moved' : f.movedFrom ? (f.orig ? 'updated' : 'created') : !f.orig ? 'created' : f.now == null ? 'deleted' : 'updated';
        const unified = f.movedTo ? '' : unifiedDiff(`/${rel(abs)}`, before, after);
        const st = diffStat(unified);
        const sha = f.now != null ? await sha256(encode(f.now)) : null;
        results.push({ path: rel(abs), action, added: st.added, removed: st.removed, sha256: sha, ...(f.movedTo ? { to: rel(f.movedTo) } : {}), ...(f.movedFrom ? { from: rel(f.movedFrom) } : {}) });
        if (unified) diffs.push(unified);
      }
      results.sort((a, b) => a.path.localeCompare(b.path));
      const unified = diffs.join('\n');
      if (unified) await emit?.('workspace-change', { call_id: call_id ?? null, path: results.map((r) => r.path).join(', '), unified, files: results, checkpoint: cp?.checkpoint ?? null }, { v: 1, kind: 'diff', path: results.map((r) => r.path).join(', '), unified });
      const lines = results.map((r) => r.action === 'moved' ? `moved ${r.path} → ${r.to}` : `${r.action} ${r.path}${r.action === 'deleted' ? '' : ` (+${r.added} -${r.removed}) sha256 ${r.sha256.slice(0, 16)}`}`);
      const cpNote = cp?.checkpoint ? `checkpoint before: ${cp.checkpoint.slice(0, 12)}` : cp?.error ? `no checkpoint (${cp.error.slice(0, 80)})` : '';
      return { output: `Applied ${results.length} file change${results.length === 1 ? '' : 's'}${cpNote ? ` · ${cpNote}` : ''}\n${lines.join('\n')}`, summary: `${results.length} files`, data: { files: results, checkpoint: cp?.checkpoint ?? null }, diff: unified ? { path: results.map((r) => r.path).join(', '), unified } : null };
    });
  }

  async function viewImage(args) {
    const abs = resolveIn(ROOT, args.path);
    const bytes = await target.readFile(abs);
    if (bytes == null) throw new ToolError(`${rel(abs)} does not exist`);
    if (bytes.length > L.max_image_bytes) throw new ToolError(`${rel(abs)} is ${kb(bytes.length)}; images are limited to ${kb(L.max_image_bytes)} (scale it down with shell first)`);
    const b = bytes;
    let media = null;
    let dims = '';
    if (b[0] === 0x89 && b[1] === 0x50 && b[2] === 0x4e && b[3] === 0x47) {
      media = 'image/png';
      const v = new DataView(b.buffer, b.byteOffset);
      dims = `${v.getUint32(16)}×${v.getUint32(20)}`;
    } else if (b[0] === 0xff && b[1] === 0xd8) media = 'image/jpeg';
    else if (b[0] === 0x47 && b[1] === 0x49 && b[2] === 0x46) {
      media = 'image/gif';
      dims = `${b[6] | (b[7] << 8)}×${b[8] | (b[9] << 8)}`;
    } else if (b[0] === 0x52 && b[1] === 0x49 && b[2] === 0x46 && b[3] === 0x46 && b[8] === 0x57 && b[9] === 0x45) media = 'image/webp';
    if (!media) throw new ToolError(`${rel(abs)} is not a PNG, JPEG, GIF, or WebP image`);
    const sum = await sha256(bytes);
    return {
      output: `Image ${rel(abs)} (${media}${dims ? `, ${dims}` : ''}, ${kb(bytes.length)}, sha256 ${sum.slice(0, 16)}) is attached.`,
      summary: media,
      data: { sha256: sum, size: bytes.length, media_type: media },
      attachments: [{ type: 'image', media_type: media, data: toB64(bytes), path: rel(abs), sha256: sum, bytes: bytes.length }],
    };
  }

  async function repoMapTool(args, { signal }) {
    const abs = args.path ? resolveIn(ROOT, String(args.path)) : ROOT;
    const res = await repoMap(abs, { root: ROOT, maxFiles: clampInt(args.max_files, 200, 10, 1000), signal });
    if (!res) throw new ToolError(`${rel(abs)} is not a directory`);
    return { output: res.text, summary: `${res.sources} sources`, data: { files: res.files, sources: res.sources, shown: res.shown } };
  }

  async function checkpointsTool(args, { signal, session, call_id, emit }) {
    const action = String(args.action || '');
    if (action === 'list') {
      const list = await cps.list(30);
      return { output: list.length ? list.map((c) => `${c.checkpoint.slice(0, 12)}  ${new Date(c.at).toISOString().replace('T', ' ').slice(0, 19)}  ${c.reason}`).join('\n') : 'No checkpoints yet.', summary: `${list.length} checkpoints`, data: { checkpoints: list.map((c) => c.checkpoint) } };
    }
    if (!args.checkpoint) throw new ToolError('`checkpoint` is required');
    const tree = await cps.treeOf(args.checkpoint);
    if (action === 'diff') {
      const to = args.to ? await cps.treeOf(args.to) : (await mutex(ROOT, () => cps.snapshot({ reason: 'diff: current state', meta: { session, call_id }, signal }))).tree;
      const paths = Array.isArray(args.paths) ? args.paths.map((p) => relativeTo(ROOT, resolveIn(ROOT, String(p)))) : [];
      const stat = await cps.diff(tree, to, { paths, stat: true });
      const full = await cps.diff(tree, to, { paths });
      const text = full ? `${stat}\n${truncate(full, L.max_output_bytes)}` : 'No differences.';
      return { output: text, summary: 'diff', data: { truncated: full.length > L.max_output_bytes } };
    }
    if (action === 'restore') {
      return mutex(ROOT, async () => {
        const before = await cps.snapshot({ reason: `before restoring ${String(args.checkpoint).slice(0, 12)}`, meta: { session, call_id }, signal });
        if (before.created) await emit?.('checkpoint-created', { checkpoint: before.checkpoint, tree: before.tree, reason: 'before restore', call_id: call_id ?? null, root: ROOT });
        const res = await cps.restore(tree, before.tree, { signal });
        await emit?.('workspace-restored', { checkpoint: String(args.checkpoint), tree, previous: before.checkpoint, written: res.written, deleted: res.deleted, root: ROOT, call_id: call_id ?? null }, { v: 1, kind: 'checkpoint', checkpoint: String(args.checkpoint), reason: 'restored' });
        return { output: `Restored the workspace to checkpoint ${String(args.checkpoint).slice(0, 12)}: ${res.written} files written, ${res.deleted} deleted. The state before the restore is checkpoint ${before.checkpoint.slice(0, 12)}.`, summary: 'restored', data: { previous: before.checkpoint, ...res } };
      });
    }
    throw new ToolError('`action` must be list, diff, or restore');
  }

  // ------------------------------------------------------------------
  // Repository import (outside the target: the target needs no network)
  // ------------------------------------------------------------------

  async function importRepo(args, signal, progress, ctx) {
    if (typeof fetchImpl !== 'function') throw new ToolError('no fetch implementation available to import repositories');
    const s = String(args.repo || '').trim().replace(/\.git$/, '').replace(/\/$/, '');
    const m = s.match(/^(?:https?:\/\/github\.com\/)?([A-Za-z0-9_.-]+)\/([A-Za-z0-9_.-]+)$/);
    if (!m) throw new ToolError('`repo` must be "owner/name" or a https://github.com/owner/name URL');
    const [, owner, name] = m;
    const dest = args.dest ? resolveIn(ROOT, String(args.dest)) : resolveIn(ROOT, name);
    if (dest === ROOT) throw new ToolError('import into a subdirectory of the workspace, not its root');
    const exists = await runner.run(`[ -e ${shq(dest)} ] && [ -n "$(ls -A ${shq(dest)} 2>/dev/null)" ] && exit 9; exit 0`, { cwd: '/', timeoutMs: 30_000, signal });
    if (exists.exitCode === 9) throw new ToolError(`${rel(dest)} already exists and is not empty; choose another dest`);
    const getJson = async (url) => {
      const res = await fetchImpl(url, { headers: { accept: 'application/vnd.github+json' }, signal });
      if (!res.ok) {
        const body = (await res.text()).slice(0, 200);
        if (res.status === 403 || res.status === 429) throw new ToolError(`GitHub API rate limit or permission error (${res.status}): ${body}`);
        if (res.status === 404) throw new ToolError('repository or ref not found (only public GitHub repositories can be imported)');
        throw new ToolError(`GitHub API HTTP ${res.status}: ${body}`);
      }
      return res.json();
    };
    const api = `https://api.github.com/repos/${owner}/${name}`;
    const ref = args.ref ? String(args.ref) : (await getJson(api)).default_branch;
    const commit = await getJson(`${api}/commits/${encodeURIComponent(ref)}`);
    const tree = await getJson(`${api}/git/trees/${commit.commit.tree.sha}?recursive=1`);
    const skipped = [];
    let total = 0;
    const blobs = [];
    for (const e of tree.tree || []) {
      if (e.type === 'commit') { skipped.push(`${e.path} (submodule)`); continue; }
      if (e.type !== 'blob') continue;
      if (e.mode === '120000') { skipped.push(`${e.path} (symlink)`); continue; }
      if (e.size > L.import_max_file_bytes) { skipped.push(`${e.path} (${e.size} bytes)`); continue; }
      if (blobs.length >= L.import_max_files || total + e.size > L.import_max_total_bytes) { skipped.push(`${e.path} (import limit reached)`); continue; }
      total += e.size;
      blobs.push(e);
    }
    progress?.(`Downloading ${blobs.length} files from ${owner}/${name}@${commit.sha.slice(0, 7)}…`);
    const raw = `https://raw.githubusercontent.com/${owner}/${name}/${commit.sha}/`;
    const out = new Array(blobs.length);
    let next = 0;
    await Promise.all(Array.from({ length: Math.min(8, blobs.length) }, async () => {
      while (next < blobs.length) {
        const k = next++;
        const e = blobs[k];
        const res = await fetchImpl(raw + e.path.split('/').map(encodeURIComponent).join('/'), { signal });
        if (!res.ok) throw new ToolError(`downloading ${e.path} failed: HTTP ${res.status}`);
        out[k] = { path: `${dest}/${e.path}`, bytes: new Uint8Array(await res.arrayBuffer()), executable: e.mode === '100755' };
      }
    }));
    await mutex(ROOT, () => checkpoint(`before import_repo ${owner}/${name}`, { ...ctx, signal }));
    progress?.(`Writing ${out.length} files into ${rel(dest)}…`);
    if (target.writeFiles) await target.writeFiles(out);
    else for (const f of out) await target.writeFile(f.path, f.bytes);
    const git = await runner.run(`cd -- ${shq(dest)} && git init -q && git add -A && git -c user.name=AgentMod -c user.email=agentmod@localhost commit -qm ${shq(`Import ${owner}/${name}@${commit.sha}`)} && git log --oneline -1`, { cwd: '/', timeoutMs: 300_000, signal });
    const gitNote = git.exitCode === 0 ? `git: ${git.stdout.trim()}` : `git init failed (exit ${git.exitCode}); files are in place without history`;
    const lines = [
      `Imported ${owner}/${name} at ${commit.sha.slice(0, 12)} (${ref}) into ${rel(dest)}: ${out.length} files, ${total} bytes.`,
      gitNote,
      tree.truncated ? 'Warning: GitHub truncated the file tree; some files are missing.' : null,
      skipped.length ? `Skipped ${skipped.length}: ${skipped.slice(0, 20).join(', ')}${skipped.length > 20 ? ', …' : ''}` : null,
    ].filter(Boolean);
    return { output: lines.join('\n'), summary: `${out.length} files` };
  }

  // ------------------------------------------------------------------
  // Machine lifecycle (the VM)
  // ------------------------------------------------------------------

  const fmtTime = (t) => new Date(t).toISOString().slice(11, 23);
  async function sandbox(name, args, progress) {
    const lifecycle = target.lifecycle;
    if (name === 'sandbox_status') {
      const st = await lifecycle.status();
      const lines = [
        `state: ${st.state}${st.broken ? ` — ${st.broken}` : ''}`,
        st.info ? `system: ${st.info.os} (${st.info.kernel}); tools: ${(st.info.tools || []).join(' ')}` : null,
        st.uptime_s != null ? `uptime: ${st.uptime_s}s` : null,
        st.running ? `running now: operation #${st.running.op} for ${st.running.for_s}s: ${st.running.command}` : null,
        `operations: ${st.stats.ops} (${st.stats.failed} non-zero exits, ${st.stats.timedOut} timeouts); boots: ${st.stats.boots}; processes created: ${st.stats.processes_created}`,
        `image: ${st.config.image} (${st.config.image_type}); CheerpX ${st.config.cheerpx}; workspace "${st.config.workspace}" at ${st.config.workspace_path}`,
        `browser: ${st.page.user_agent || 'unknown'}; cores ${st.page.cores ?? '?'}; memory ${st.page.memory_gb ?? '?'} GB${st.page.js_heap_mb != null ? `; JS heap ${st.page.js_heap_mb} MB` : ''}; cross-origin isolated: ${st.page.cross_origin_isolated}`,
        st.unavailable && !st.broken ? `unavailable: ${st.unavailable}` : null,
      ].filter(Boolean);
      return { output: lines.join('\n'), summary: st.state };
    }
    if (name === 'sandbox_logs') {
      const limit = clampInt(args.limit, 60, 1, 500);
      const kinds = Array.isArray(args.kinds) ? args.kinds.map(String) : undefined;
      const { entries, total, console_tail: tail } = await lifecycle.logs({ limit, kinds });
      const fmt = (e) => {
        const { at, kind, ...rest } = e;
        const parts = Object.entries(rest).map(([k, v]) => `${k}=${JSON.stringify(v)}`);
        return `${fmtTime(at)} ${kind}${parts.length ? ' ' + parts.join(' ') : ''}`;
      };
      const head = `VM journal: ${entries.length} of ${total} entr${total === 1 ? 'y' : 'ies'}${kinds ? ` (kinds: ${kinds.join(', ')})` : ''}, oldest first`;
      const body = entries.map(fmt).join('\n') || '(empty: the VM has not been started in this page)';
      const con = tail ? `\n--- VM console (last ${tail.length} chars) ---\n${tail}` : '';
      return { output: truncate(redact(`${head}\n${body}${con}`), L.max_output_bytes), summary: `${entries.length} entries` };
    }
    if (name === 'sandbox_restart') {
      const t0 = Date.now();
      const info = await lifecycle.restart({ status: progress ? (_s, m) => progress(m) : undefined });
      return { output: `Restarted the Linux sandbox in ${((Date.now() - t0) / 1000).toFixed(1)}s: ${info.os} (${info.kernel}). The workspace is intact; processes from before are gone (they will report as lost).`, summary: 'restarted' };
    }
    if (name === 'sandbox_stop') {
      await lifecycle.stop();
      return { output: 'Stopped the Linux sandbox. The workspace is kept; the next coding tool starts it again.', summary: 'stopped' };
    }
    throw new ToolError(`unknown tool ${name}`);
  }

  // ------------------------------------------------------------------
  // Workspace facts (provenance, instructions)
  // ------------------------------------------------------------------

  /** Workspace identity and Git awareness. */
  async function info() {
    const r = await runner.run([
      `cd -- ${shq(ROOT)} 2>/dev/null || exit 3`,
      'if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then',
      '  printf "git|%s|%s|%s|%s\\n" "$(git rev-parse --show-toplevel)" "$(git rev-parse HEAD 2>/dev/null)" "$(git rev-parse --abbrev-ref HEAD 2>/dev/null)" "$(git status --porcelain 2>/dev/null | wc -l | tr -d " ")"',
      'fi',
      `for d in $(find . -mindepth 2 -maxdepth 3 -name .git -not -path './.agentmod/*' 2>/dev/null | sort | head -n 20); do r=\${d%/.git}; printf "repo|%s|%s|%s|%s\\n" "\${r#./}" "$(git -C "$r" rev-parse HEAD 2>/dev/null)" "$(git -C "$r" rev-parse --abbrev-ref HEAD 2>/dev/null)" "$(git -C "$r" status --porcelain 2>/dev/null | wc -l | tr -d " ")"; done`,
      'true',
    ].join('\n'), { cwd: '/', timeoutMs: 60_000 });
    const repos = [];
    let git = null;
    for (const l of r.stdout.split('\n')) {
      const [kind, path, head, branch, dirty] = l.split('|');
      if (kind === 'git') git = { toplevel: path, head: head || null, branch: branch || null, dirty: Number(dirty || 0) };
      if (kind === 'repo') repos.push({ path, head: head || null, branch: branch || null, dirty: Number(dirty || 0) });
    }
    return { root: ROOT, state_dir: STATE, environment: target.identity?.() ?? null, git, repos };
  }

  /** Project instruction files (AGENTS.md, CLAUDE.md, …), bounded. */
  async function instructions({ maxBytes = 16_000 } = {}) {
    const script = INSTRUCTION_FILES.map((f) => `[ -f ${shq(`${ROOT}/${f}`)} ] && { printf '\\n\\0FILE %s\\n' ${shq(f)}; head -c ${maxBytes} ${shq(`${ROOT}/${f}`)}; }`).join('\n') + '\ntrue';
    const r = await runner.run(script, { cwd: '/', timeoutMs: 30_000 });
    const out = [];
    for (const chunk of r.stdout.split('\n\0FILE ').slice(1)) {
      const nl = chunk.indexOf('\n');
      const path = chunk.slice(0, nl);
      const text = chunk.slice(nl + 1);
      if (text.trim()) out.push({ path, text: redact(text), sha256: await sha256(text) });
    }
    return out;
  }

  /**
   * Skills: compact procedures in SKILL.md files (front matter `name`,
   * `description`). Repository skills (.agentmod/skills/*) carry workspace
   * authority; configured directories carry the authority they were given.
   */
  async function skills(extraDirs = []) {
    const dirs = [{ dir: `${ROOT}/.agentmod/skills`, authority: 'workspace' }, ...extraDirs];
    const script = dirs.map((d, i) => `[ -d ${shq(d.dir)} ] && for f in ${shq(d.dir)}/*/SKILL.md; do [ -f "$f" ] && { printf '\\n\\0SKILL ${i} %s\\n' "$f"; head -n 12 "$f"; }; done`).join('\n') + '\ntrue';
    const r = await runner.run(script, { cwd: '/', timeoutMs: 30_000 });
    const out = [];
    for (const chunk of r.stdout.split('\n\0SKILL ').slice(1)) {
      const nl = chunk.indexOf('\n');
      const [i, ...p] = chunk.slice(0, nl).split(' ');
      const head = chunk.slice(nl + 1);
      const fm = head.match(/^---\n([\s\S]*?)\n---/);
      const field = (k) => fm?.[1].match(new RegExp(`^${k}:\\s*(.+)$`, 'm'))?.[1].trim().replace(/^["']|["']$/g, '');
      const path = p.join(' ');
      const name = field('name') || path.split('/').slice(-2, -1)[0];
      out.push({ name, description: (field('description') || '').slice(0, 300), path, authority: dirs[Number(i)].authority });
    }
    return out.slice(0, 50);
  }

  async function readSkill(path) {
    const r = await runner.run(`head -c 20000 ${shq(path)}`, { cwd: '/', timeoutMs: 30_000 });
    if (r.exitCode !== 0) throw new ToolError(`cannot read ${path}`);
    return { text: r.stdout.replace(/^---\n[\s\S]*?\n---\n?/, ''), sha256: await sha256(r.stdout) };
  }

  /** The repository's own policy file (workspace scope: it may only restrict). */
  async function workspacePolicy() {
    const r = await runner.run(`[ -f ${shq(`${ROOT}/${POLICY_FILE}`)} ] && head -c 65536 ${shq(`${ROOT}/${POLICY_FILE}`)}`, { cwd: '/', timeoutMs: 30_000 });
    if (r.exitCode !== 0 || !r.stdout.trim()) return null;
    try {
      const json = JSON.parse(r.stdout);
      return { path: POLICY_FILE, sha256: await sha256(r.stdout), rules: Array.isArray(json.rules) ? json.rules : [] };
    } catch (e) {
      return { path: POLICY_FILE, error: `invalid JSON: ${e.message}`, rules: [] };
    }
  }

  /**
   * Adopt changes made in another workspace (an isolated child or branch
   * worktree sharing this workspace's checkpoint store). A file is applied only
   * if this workspace still has the content the other one started from; any
   * other file is reported as a conflict and left alone.
   */
  async function adopt({ baseTree, otherTree, paths = [], session, call_id, emit, signal }) {
    const all = await cps.changes(baseTree, otherTree);
    const want = paths.length ? all.filter((c) => paths.some((p) => c.path === p || c.path.startsWith(`${p.replace(/\/$/, '')}/`))) : all;
    return mutex(ROOT, async () => {
      const apply = [];
      const conflicts = [];
      for (const c of want) {
        const abs = `${ROOT}/${c.path}`;
        const [base, mine] = await Promise.all([cps.fileAt(baseTree, c.path), target.readFile(abs)]);
        const same = (base == null && mine == null) || (base != null && mine != null && base.length === mine.length && base.every((b, i) => b === mine[i]));
        if (!same) conflicts.push(c.path);
        else apply.push(c);
      }
      if (!apply.length) return { adopted: [], conflicts, checkpoint: null };
      const cp = await checkpoint(`before adopting ${apply.length} file(s)`, { session, call_id, emit, signal });
      const writes = [];
      const removes = [];
      const diffs = [];
      for (const c of apply) {
        const abs = `${ROOT}/${c.path}`;
        const before = c.status === 'A' ? null : await cps.fileAt(baseTree, c.path);
        if (c.status === 'D') {
          removes.push(abs);
          diffs.push(unifiedDiff(`/${c.path}`, decode(before), ''));
        } else {
          const bytes = await cps.fileAt(otherTree, c.path);
          writes.push({ path: abs, bytes });
          diffs.push(unifiedDiff(`/${c.path}`, before ? decode(before) : '', decode(bytes)));
        }
      }
      if (writes.length) {
        if (target.writeFiles) await target.writeFiles(writes);
        else for (const w of writes) await target.writeFile(w.path, w.bytes);
      }
      if (removes.length) await runner.must(`rm -f -- ${removes.map(shq).join(' ')}`, { cwd: '/', timeoutMs: 60_000, what: 'delete' });
      const unified = diffs.filter(Boolean).join('\n');
      if (unified) await emit?.('workspace-change', { call_id: call_id ?? null, path: apply.map((c) => c.path).join(', '), unified, checkpoint: cp?.checkpoint ?? null, adopted: true }, { v: 1, kind: 'diff', path: apply.map((c) => c.path).join(', '), unified });
      return { adopted: apply.map((c) => ({ path: c.path, status: c.status })), conflicts, checkpoint: cp?.checkpoint ?? null, unified };
    });
  }

  /**
   * Run one tool. Returns { output, error?, summary, data?, diff?, attachments? }
   * — never throws for tool-level failures; those come back as `error: true`.
   */
  async function call(name, args = {}, { signal, progress, session, call_id, emit } = {}) {
    const ctx = { signal, session, call_id, emit };
    try {
      switch (name) {
        case 'shell': return await shell(args, ctx);
        case 'process': return await processTool(args, ctx);
        case 'read_file': return await readFile(args, ctx);
        case 'list_dir': return await listDir(args, ctx);
        case 'search_files': return await searchFiles(args, ctx);
        case 'search_text': return await searchText(args, ctx);
        case 'apply_patch': return await applyPatch(args, ctx);
        case 'view_image': return await viewImage(args, ctx);
        case 'repo_map': return await repoMapTool(args, ctx);
        case 'checkpoints': return await checkpointsTool(args, ctx);
        case 'import_repo': return await importRepo(args, signal, progress, ctx);
        default:
          if (lifecycleNames.has(name)) return await sandbox(name, args, progress);
          throw new ToolError(`unknown tool ${name}`);
      }
    } catch (e) {
      if (signal?.aborted) throw e;
      return { output: redact(e instanceof ToolError ? e.message : `${name} failed: ${e?.message || e}`), error: true, summary: 'error' };
    }
  }

  return { specs, names, lifecycleNames, root: ROOT, stateDir: STATE, shadowGitDir: shadowGitDir || `${STATE}/shadow.git`, call, info, instructions, workspacePolicy, skills, readSkill, checkpoints: cps, processes: procs, checkpoint, adopt, redact, runner, mutex: (fn) => mutex(ROOT, fn) };
}
