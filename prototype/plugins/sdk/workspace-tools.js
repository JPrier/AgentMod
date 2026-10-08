// Coding tools over an execution target.
//
// The tools an agent needs to work on code — run a command, read, write and
// edit files, list a directory, import a repository — are implemented here
// once, against a small *execution target* interface. Plugins pick the target:
//
//   * `linux-sandbox`   CheerpX: an x86 Linux VM inside the user's browser
//   * `local-workspace` a directory on the machine running the native runtime
//
// A session definition chooses which of those plugins answers the tool calls,
// the same way it chooses a model provider. Nothing here knows which one runs.
//
// Execution target interface (all paths are absolute, in the target's namespace):
//
//   exec({ command, cwd, timeoutMs, signal }) -> { exitCode, stdout, stderr, timedOut }
//       stdout/stderr are Uint8Array. `command` is a shell (bash) command line.
//   readFile(path) -> Uint8Array | null          null when the path does not exist
//   writeFile(path, bytes) -> void               creates parent directories
//   writeFiles([{ path, bytes, executable }])    optional batch form (repo import)
//   lifecycle { status, logs, restart, stop }    optional: a target that runs a
//       machine (a VM) can let the agent inspect, restart and stop it; the
//       `sandbox_*` tools appear only for such targets
//
// Everything else — path confinement, output limits, edit semantics, diffs,
// repository import — is shared logic, so every target behaves the same.

import { toolSpec } from './agentmod.js';

const enc = new TextEncoder();
const dec = new TextDecoder('utf-8', { fatal: false });

/** An expected, user-facing failure (reported to the model as a tool error). */
export class ToolError extends Error {}

export const DEFAULT_LIMITS = Object.freeze({
  max_output_bytes: 24000, // per run result, stdout + stderr after truncation
  default_timeout_seconds: 120,
  max_timeout_seconds: 900,
  max_read_lines: 400,
  max_list_entries: 600,
  import_max_files: 2000,
  import_max_file_bytes: 1_000_000,
  import_max_total_bytes: 30_000_000,
});

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

/** Normalize an absolute POSIX path (resolves `.` and `..`, collapses slashes). */
export function normalizePath(p) {
  const out = [];
  for (const seg of String(p).split('/')) {
    if (!seg || seg === '.') continue;
    if (seg === '..') out.pop();
    else out.push(seg);
  }
  return '/' + out.join('/');
}

/** Resolve `p` (absolute, or relative to `cwd`) and require it to stay inside `root`. */
export function resolveIn(root, p, cwd = root) {
  if (typeof p !== 'string' || !p.trim()) throw new ToolError('a path is required');
  if (p.includes('\0')) throw new ToolError('paths may not contain NUL bytes');
  const r = normalizePath(root);
  const abs = normalizePath(p.startsWith('/') ? p : `${cwd}/${p}`);
  if (abs !== r && !abs.startsWith(r === '/' ? '/' : `${r}/`)) {
    throw new ToolError(`\`${p}\` is outside the workspace (${r}); use paths under it`);
  }
  return abs;
}

/** Quote a string for a POSIX shell. */
export function shq(s) {
  return `'${String(s).replace(/'/g, `'\\''`)}'`;
}

// ---------------------------------------------------------------------------
// Text helpers
// ---------------------------------------------------------------------------

export function isBinary(bytes) {
  const n = Math.min(bytes.length, 8000);
  for (let i = 0; i < n; i++) if (bytes[i] === 0) return true;
  return false;
}

/** Keep the head and tail of long output, marking what was dropped. */
export function truncate(text, maxBytes) {
  const bytes = enc.encode(text);
  if (bytes.length <= maxBytes) return text;
  const head = Math.floor(maxBytes * 0.6);
  const tail = maxBytes - head;
  const a = dec.decode(bytes.subarray(0, head));
  const b = dec.decode(bytes.subarray(bytes.length - tail));
  return `${a}\n… [${bytes.length - head - tail} bytes omitted] …\n${b}`;
}

export function formatRun({ exitCode, stdout, stderr, timedOut }, { timeoutSeconds, maxBytes }) {
  const out = dec.decode(stdout || new Uint8Array());
  const err = dec.decode(stderr || new Uint8Array());
  const total = Math.max(1, out.length + err.length);
  const parts = [timedOut ? `timed out after ${timeoutSeconds}s (exit code ${exitCode})` : `exit code ${exitCode}`];
  if (out) parts.push(`--- stdout ---\n${truncate(out, Math.max(512, Math.floor((maxBytes * out.length) / total)))}`);
  if (err) parts.push(`--- stderr ---\n${truncate(err, Math.max(512, Math.floor((maxBytes * err.length) / total)))}`);
  if (!out && !err) parts.push('(no output)');
  return parts.join('\n');
}

/**
 * Unified diff of two texts (3 lines of context). Common prefix and suffix are
 * stripped first, so a small edit to a large file stays cheap; a very large
 * changed region falls back to one replace hunk.
 */
export function unifiedDiff(path, before, after, context = 3) {
  // A final line without "\n" is a different line from the same text with one;
  // mark it so it compares unequal and prints with diff(1)'s note.
  const NOEOL = '\u0000noeol';
  const toLines = (t) => {
    if (t === '') return [];
    const l = t.split('\n');
    if (t.endsWith('\n')) l.pop();
    else l[l.length - 1] += NOEOL;
    return l;
  };
  const a = toLines(before);
  const b = toLines(after);
  let pre = 0;
  while (pre < a.length && pre < b.length && a[pre] === b[pre]) pre++;
  let suf = 0;
  while (suf < a.length - pre && suf < b.length - pre && a[a.length - 1 - suf] === b[b.length - 1 - suf]) suf++;
  const am = a.slice(pre, a.length - suf);
  const bm = b.slice(pre, b.length - suf);
  if (!am.length && !bm.length) return '';
  // Edit script for the middle: LCS table when small, else replace-all.
  const ops = [];
  if (am.length * bm.length <= 4_000_000) {
    const n = am.length;
    const m = bm.length;
    const t = new Uint32Array((n + 1) * (m + 1));
    for (let i = n - 1; i >= 0; i--) {
      for (let j = m - 1; j >= 0; j--) {
        t[i * (m + 1) + j] = am[i] === bm[j] ? t[(i + 1) * (m + 1) + j + 1] + 1 : Math.max(t[(i + 1) * (m + 1) + j], t[i * (m + 1) + j + 1]);
      }
    }
    let i = 0;
    let j = 0;
    while (i < n && j < m) {
      if (am[i] === bm[j]) { ops.push([' ', am[i]]); i++; j++; }
      else if (t[(i + 1) * (m + 1) + j] >= t[i * (m + 1) + j + 1]) ops.push(['-', am[i++]]);
      else ops.push(['+', bm[j++]]);
    }
    while (i < n) ops.push(['-', am[i++]]);
    while (j < m) ops.push(['+', bm[j++]]);
  } else {
    for (const l of am) ops.push(['-', l]);
    for (const l of bm) ops.push(['+', l]);
  }
  // Full op list with the shared prefix/suffix, then cut into hunks.
  const all = [...a.slice(0, pre).map((l) => [' ', l]), ...ops, ...a.slice(a.length - suf).map((l) => [' ', l])];
  // Line numbers (1-based) each op starts at, in the old and new text.
  const aAt = new Array(all.length);
  const bAt = new Array(all.length);
  for (let k = 0, ai = 1, bi = 1; k < all.length; k++) {
    aAt[k] = ai;
    bAt[k] = bi;
    if (all[k][0] !== '+') ai++;
    if (all[k][0] !== '-') bi++;
  }
  // Group changes whose separating context is short enough to share a hunk.
  const changed = [];
  for (let k = 0; k < all.length; k++) if (all[k][0] !== ' ') changed.push(k);
  const groups = [];
  for (const k of changed) {
    const g = groups[groups.length - 1];
    if (g && k - g[1] - 1 <= 2 * context) g[1] = k;
    else groups.push([k, k]);
  }
  const lines = [`--- a${path}`, `+++ b${path}`];
  for (const [first, last] of groups) {
    const start = Math.max(0, first - context);
    const end = Math.min(all.length, last + context + 1);
    const hunk = all.slice(start, end);
    const aLen = hunk.filter((o) => o[0] !== '+').length;
    const bLen = hunk.filter((o) => o[0] !== '-').length;
    // An empty side is addressed by the line *before* it (diff(1) convention).
    lines.push(`@@ -${aLen ? aAt[start] : aAt[start] - 1},${aLen} +${bLen ? bAt[start] : bAt[start] - 1},${bLen} @@`);
    for (const [op, l] of hunk) {
      if (l.endsWith(NOEOL)) lines.push(op + l.slice(0, -NOEOL.length), '\\ No newline at end of file');
      else lines.push(op + l);
    }
  }
  return lines.join('\n');
}

// ---------------------------------------------------------------------------
// Repository import (outside the target: the target needs no network)
// ---------------------------------------------------------------------------

export function parseRepo(spec) {
  const s = String(spec || '').trim().replace(/\.git$/, '').replace(/\/$/, '');
  const m = s.match(/^(?:https?:\/\/github\.com\/)?([A-Za-z0-9_.-]+)\/([A-Za-z0-9_.-]+)$/);
  if (!m) throw new ToolError('`repo` must be "owner/name" or a https://github.com/owner/name URL');
  return { owner: m[1], name: m[2] };
}

async function mapLimit(items, limit, fn) {
  const out = new Array(items.length);
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const i = next++;
      out[i] = await fn(items[i], i);
    }
  });
  await Promise.all(workers);
  return out;
}

async function getJson(fetchImpl, url, signal) {
  const res = await fetchImpl(url, { headers: { accept: 'application/vnd.github+json' }, signal });
  if (!res.ok) {
    const body = (await res.text()).slice(0, 200);
    if (res.status === 403 || res.status === 429) throw new ToolError(`GitHub API rate limit or permission error (${res.status}): ${body}`);
    if (res.status === 404) throw new ToolError('repository or ref not found (only public GitHub repositories can be imported)');
    throw new ToolError(`GitHub API HTTP ${res.status}: ${body}`);
  }
  return res.json();
}

// ---------------------------------------------------------------------------
// The tool set
// ---------------------------------------------------------------------------

/**
 * Build the coding tools for one execution target.
 *
 * @param {object} o
 * @param {object} o.target       the execution target (see the header)
 * @param {string} o.root         absolute workspace root in the target's namespace
 * @param {object} [o.limits]     overrides for DEFAULT_LIMITS
 * @param {boolean} [o.importRepos] offer `import_repo`
 * @param {Function} [o.fetch]    fetch implementation for `import_repo`
 */
export function workspaceTools({ target, root, limits = {}, importRepos = true, fetch: fetchImpl = globalThis.fetch }) {
  const L = { ...DEFAULT_LIMITS, ...limits };
  const ROOT = normalizePath(root);

  const specs = [
    toolSpec('run', `Run a bash command in the workspace and return its exit code, stdout and stderr. Non-interactive (stdin is empty). The working directory defaults to ${ROOT}. Default timeout ${L.default_timeout_seconds}s, maximum ${L.max_timeout_seconds}s.`, {
      command: { type: 'string', description: 'bash command line, e.g. "gcc main.c -o main && ./main"' },
      cwd: { type: 'string', description: `directory to run in (absolute, or relative to ${ROOT})` },
      timeout_seconds: { type: 'integer', description: 'kill the command after this many seconds' },
    }),
    toolSpec('read_file', `Read a text file in the workspace. Returns up to ${L.max_read_lines} lines starting at start_line.`, {
      path: { type: 'string', description: `file path (absolute, or relative to ${ROOT})` },
      start_line: { type: 'integer', description: '1-based first line (default 1)' },
      max_lines: { type: 'integer', description: `number of lines (default and maximum ${L.max_read_lines})` },
    }),
    toolSpec('write_file', 'Create or overwrite a file in the workspace with the given content. Parent directories are created.', {
      path: { type: 'string', description: `file path (absolute, or relative to ${ROOT})` },
      content: { type: 'string', description: 'the complete new file content' },
    }),
    toolSpec('edit_file', 'Replace exact text in a file. old_text must match exactly once unless replace_all is true. Read the file first.', {
      path: { type: 'string', description: `file path (absolute, or relative to ${ROOT})` },
      old_text: { type: 'string', description: 'exact text to replace, including whitespace' },
      new_text: { type: 'string', description: 'replacement text' },
      replace_all: { type: 'boolean', description: 'replace every occurrence' },
    }),
    toolSpec('list_files', 'List files and directories (directories end with /). Skips .git and node_modules.', {
      path: { type: 'string', description: `directory (default ${ROOT})` },
      max_depth: { type: 'integer', description: 'how deep to recurse (default 3)' },
    }),
  ];
  if (importRepos) {
    specs.push(toolSpec('import_repo', `Copy a public GitHub repository into the workspace as a fresh git repository (one commit). The import is done outside the workspace, so it works without network access inside it.`, {
      repo: { type: 'string', description: '"owner/name" or https://github.com/owner/name' },
      ref: { type: 'string', description: 'branch, tag, or commit (default: the default branch)' },
      dest: { type: 'string', description: `destination directory (default ${ROOT}/<name>)` },
    }));
  }
  // Machine lifecycle tools, for targets that run a machine (see the header).
  const lifecycle = target.lifecycle || null;
  const lifecycleNames = new Set();
  if (lifecycle) {
    const add = (spec) => { specs.push(spec); lifecycleNames.add(spec.name); };
    add(toolSpec('sandbox_status', 'Show the sandbox VM\'s state (running, busy, stopped, crashed), what it is running now, uptime, operation counts, and the browser it runs in.', {}));
    add(toolSpec('sandbox_logs', 'Read the sandbox VM\'s own journal, kept outside the VM so it survives a crash: boots, every command with exit code and duration, timeouts, stalls, page errors (e.g. a CheerpX crash), and the VM console. Use it when commands fail strangely or the sandbox stops responding.', {
      limit: { type: 'integer', description: 'most recent entries to show (default 60, max 500)' },
      kinds: { type: 'array', items: { type: 'string' }, description: 'only these kinds: boot-start, boot-ok, boot-failed, op, op-error, stalled, page-error, interrupt, restart, stopped, note' },
    }));
    add(toolSpec('sandbox_restart', 'Throw away the current VM, crashed or not, and boot a fresh one. Files in the workspace are kept; running processes and anything outside the workspace that was not saved are lost.', {}));
    add(toolSpec('sandbox_stop', 'Stop the sandbox VM to free the browser\'s CPU and memory. Files in the workspace are kept; the next coding tool starts the VM again.', {}));
  }
  const names = new Set(specs.map((s) => s.name));

  const text = (bytes) => dec.decode(bytes);
  const readText = async (abs) => {
    const bytes = await target.readFile(abs);
    if (bytes == null) return null;
    if (isBinary(bytes)) throw new ToolError(`${abs} is a binary file (${bytes.length} bytes); inspect it with \`run\` (e.g. xxd | head)`);
    return text(bytes);
  };
  const clampInt = (v, d, lo, hi) => {
    const n = Number.isFinite(Number(v)) && v !== null && v !== '' ? Math.trunc(Number(v)) : d;
    return Math.min(hi, Math.max(lo, n));
  };

  async function run(args, signal) {
    const command = String(args.command ?? '').trim();
    if (!command) throw new ToolError('`command` is required');
    const cwd = args.cwd ? resolveIn(ROOT, String(args.cwd)) : ROOT;
    const timeoutSeconds = clampInt(args.timeout_seconds, L.default_timeout_seconds, 1, L.max_timeout_seconds);
    const r = await target.exec({ command, cwd, timeoutMs: timeoutSeconds * 1000, signal });
    const output = formatRun(r, { timeoutSeconds, maxBytes: L.max_output_bytes });
    return { output, error: r.exitCode !== 0 || r.timedOut, summary: r.timedOut ? 'timed out' : `exit ${r.exitCode}` };
  }

  async function readFile(args) {
    const abs = resolveIn(ROOT, args.path);
    const content = await readText(abs);
    if (content == null) throw new ToolError(`${abs} does not exist (or is a directory; use list_files)`);
    const all = content.split('\n');
    if (all.length && all[all.length - 1] === '') all.pop();
    const start = clampInt(args.start_line, 1, 1, Math.max(1, all.length));
    const count = clampInt(args.max_lines, L.max_read_lines, 1, L.max_read_lines);
    const slice = all.slice(start - 1, start - 1 + count);
    const end = start - 1 + slice.length;
    const head = all.length === 0 ? `${abs} is empty` : `${abs} — lines ${start}-${end} of ${all.length}${end < all.length ? ` (continue with start_line ${end + 1})` : ''}`;
    return { output: `${head}\n${truncate(slice.join('\n'), L.max_output_bytes)}`, summary: `${slice.length} lines` };
  }

  async function writeFile(args) {
    const abs = resolveIn(ROOT, args.path);
    if (typeof args.content !== 'string') throw new ToolError('`content` must be a string');
    let before = null;
    try { before = await readText(abs); } catch { before = null; }
    await target.writeFile(abs, enc.encode(args.content));
    const lines = args.content.split('\n').length;
    return {
      output: `${before == null ? 'Created' : 'Wrote'} ${abs} (${enc.encode(args.content).length} bytes, ${lines} lines).`,
      summary: before == null ? 'created' : 'written',
      diff: { path: abs, unified: unifiedDiff(abs, before ?? '', args.content) },
    };
  }

  async function editFile(args) {
    const abs = resolveIn(ROOT, args.path);
    const oldText = args.old_text;
    const newText = args.new_text ?? '';
    if (typeof oldText !== 'string' || !oldText) throw new ToolError('`old_text` must be a non-empty string');
    if (typeof newText !== 'string') throw new ToolError('`new_text` must be a string');
    const before = await readText(abs);
    if (before == null) throw new ToolError(`${abs} does not exist`);
    const count = before.split(oldText).length - 1;
    if (count === 0) throw new ToolError(`old_text was not found in ${abs}; read the file and copy the exact text`);
    if (count > 1 && !args.replace_all) throw new ToolError(`old_text matches ${count} places in ${abs}; include more surrounding text, or set replace_all`);
    const after = args.replace_all ? before.split(oldText).join(newText) : before.replace(oldText, () => newText);
    await target.writeFile(abs, enc.encode(after));
    return {
      output: `Edited ${abs} (${count} replacement${count === 1 ? '' : 's'}).`,
      summary: `${count} replacement${count === 1 ? '' : 's'}`,
      diff: { path: abs, unified: unifiedDiff(abs, before, after) },
    };
  }

  async function listFiles(args, signal) {
    const abs = args.path ? resolveIn(ROOT, String(args.path)) : ROOT;
    const depth = clampInt(args.max_depth, 3, 1, 12);
    const prune = `\\( -name .git -o -name node_modules \\) -prune -o`;
    const command = `cd -- ${shq(abs)} || exit 3; find . -mindepth 1 -maxdepth ${depth} ${prune} -type d -print; echo ::files::; find . -mindepth 1 -maxdepth ${depth} ${prune} ! -type d -print`;
    const r = await target.exec({ command, cwd: ROOT, timeoutMs: 60_000, signal });
    if (r.exitCode === 3) throw new ToolError(`${abs} is not a directory`);
    if (r.exitCode !== 0) throw new ToolError(`listing failed: ${text(r.stderr).slice(0, 400)}`);
    const [dirs, files] = text(r.stdout).split('::files::\n');
    const entries = [
      ...(dirs || '').split('\n').filter(Boolean).map((d) => `${d.replace(/^\.\//, '')}/`),
      ...(files || '').split('\n').filter(Boolean).map((f) => f.replace(/^\.\//, '')),
    ].sort();
    const shown = entries.slice(0, L.max_list_entries);
    const more = entries.length - shown.length;
    return {
      output: `${abs} (depth ${depth}): ${entries.length} entr${entries.length === 1 ? 'y' : 'ies'}\n${shown.join('\n') || '(empty)'}${more > 0 ? `\n… ${more} more (narrow the path or depth)` : ''}`,
      summary: `${entries.length} entries`,
    };
  }

  async function importRepo(args, signal, progress) {
    if (typeof fetchImpl !== 'function') throw new ToolError('no fetch implementation available to import repositories');
    const { owner, name } = parseRepo(args.repo);
    const dest = args.dest ? resolveIn(ROOT, String(args.dest)) : resolveIn(ROOT, name);
    if (dest === ROOT) throw new ToolError('import into a subdirectory of the workspace, not its root');
    const exists = await target.exec({ command: `[ -e ${shq(dest)} ] && [ -n "$(ls -A ${shq(dest)} 2>/dev/null)" ] && exit 9; exit 0`, cwd: ROOT, timeoutMs: 30_000, signal });
    if (exists.exitCode === 9) throw new ToolError(`${dest} already exists and is not empty; choose another dest`);
    const api = `https://api.github.com/repos/${owner}/${name}`;
    const ref = args.ref ? String(args.ref) : (await getJson(fetchImpl, api, signal)).default_branch;
    const commit = await getJson(fetchImpl, `${api}/commits/${encodeURIComponent(ref)}`, signal);
    const tree = await getJson(fetchImpl, `${api}/git/trees/${commit.commit.tree.sha}?recursive=1`, signal);
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
    const files = await mapLimit(blobs, 8, async (e) => {
      const res = await fetchImpl(raw + e.path.split('/').map(encodeURIComponent).join('/'), { signal });
      if (!res.ok) throw new ToolError(`downloading ${e.path} failed: HTTP ${res.status}`);
      return { path: `${dest}/${e.path}`, bytes: new Uint8Array(await res.arrayBuffer()), executable: e.mode === '100755' };
    });
    progress?.(`Writing ${files.length} files into ${dest}…`);
    if (target.writeFiles) await target.writeFiles(files);
    else for (const f of files) await target.writeFile(f.path, f.bytes);
    const git = await target.exec({
      command: `cd -- ${shq(dest)} && git init -q && git add -A && git -c user.name=AgentMod -c user.email=agentmod@localhost commit -qm ${shq(`Import ${owner}/${name}@${commit.sha}`)} && git log --oneline -1`,
      cwd: ROOT,
      timeoutMs: 300_000,
      signal,
    });
    const gitNote = git.exitCode === 0 ? `git: ${text(git.stdout).trim()}` : `git init failed (exit ${git.exitCode}); files are in place without history`;
    const lines = [
      `Imported ${owner}/${name} at ${commit.sha.slice(0, 12)} (${ref}) into ${dest}: ${files.length} files, ${total} bytes.`,
      gitNote,
      tree.truncated ? 'Warning: GitHub truncated the file tree; some files are missing.' : null,
      skipped.length ? `Skipped ${skipped.length}: ${skipped.slice(0, 20).join(', ')}${skipped.length > 20 ? ', …' : ''}` : null,
    ].filter(Boolean);
    return { output: lines.join('\n'), summary: `${files.length} files` };
  }

  const fmtTime = (t) => new Date(t).toISOString().slice(11, 23);
  function formatEntry(e) {
    const { at, kind, ...rest } = e;
    const parts = Object.entries(rest).map(([k, v]) => `${k}=${typeof v === 'string' ? JSON.stringify(v) : JSON.stringify(v)}`);
    return `${fmtTime(at)} ${kind}${parts.length ? ' ' + parts.join(' ') : ''}`;
  }

  async function sandbox(name, args, progress) {
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
      const head = `VM journal: ${entries.length} of ${total} entr${total === 1 ? 'y' : 'ies'}${kinds ? ` (kinds: ${kinds.join(', ')})` : ''}, oldest first`;
      const body = entries.map(formatEntry).join('\n') || '(empty: the VM has not been started in this page)';
      const con = tail ? `\n--- VM console (last ${tail.length} chars) ---\n${tail}` : '';
      return { output: truncate(`${head}\n${body}${con}`, L.max_output_bytes), summary: `${entries.length} entries` };
    }
    if (name === 'sandbox_restart') {
      const t0 = Date.now();
      const info = await lifecycle.restart({ status: progress ? (_s, m) => progress(m) : undefined });
      return { output: `Restarted the Linux sandbox in ${((Date.now() - t0) / 1000).toFixed(1)}s: ${info.os} (${info.kernel}). The workspace is intact; re-run anything that was in progress.`, summary: 'restarted' };
    }
    if (name === 'sandbox_stop') {
      await lifecycle.stop();
      return { output: 'Stopped the Linux sandbox. The workspace is kept; the next coding tool starts it again.', summary: 'stopped' };
    }
    throw new ToolError(`unknown tool ${name}`);
  }

  /**
   * Run one tool. Returns { output, error, summary, diff? } — never throws for
   * tool-level failures; those come back as `error: true` with a message.
   */
  async function call(name, args = {}, { signal, progress } = {}) {
    try {
      switch (name) {
        case 'run': return await run(args, signal);
        case 'read_file': return await readFile(args);
        case 'write_file': return await writeFile(args);
        case 'edit_file': return await editFile(args);
        case 'list_files': return await listFiles(args, signal);
        case 'import_repo': return await importRepo(args, signal, progress);
        default:
          if (lifecycleNames.has(name)) return await sandbox(name, args, progress);
          throw new ToolError(`unknown tool ${name}`);
      }
    } catch (e) {
      if (signal?.aborted) throw e;
      return { output: e instanceof ToolError ? e.message : `${name} failed: ${e?.message || e}`, error: true, summary: 'error' };
    }
  }

  return { specs, names, lifecycleNames, root: ROOT, call };
}
