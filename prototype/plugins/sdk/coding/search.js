// Directory listing, file discovery, and text search over an execution target.
//
// Everything runs as one shell command in the target (one round trip, which
// matters for the browser VM) and is bounded and sorted, so results are
// deterministic and cheap enough to use heavily. ripgrep is used when the
// target has it; otherwise git ls-files / find and grep give the same shapes.

import { shq, relativeTo } from './paths.js';
import { globToRegExp, matchesAny } from './glob.js';

/** Directories never descended into (still listed, marked as skipped). */
export const SKIP_DIRS = ['.git', 'node_modules', '.agentmod', 'target', '__pycache__', '.venv', 'venv', '.next', '.cache', 'dist', 'build', '.gradle', '.idea', '.tox'];

/**
 * @param {object} o
 * @param {ReturnType<import('./runner.js').makeRunner>} o.runner
 * @param {() => Promise<{rg:boolean, git:boolean, gnuFind:boolean}>} o.probe
 */
export function makeSearch({ runner, probe }) {
  const prune = SKIP_DIRS.map((d) => `-name ${shq(d)}`).join(' -o ');

  /** List a directory to `depth` levels; entries sorted, directories first per level. */
  async function listDir(abs, { root, depth = 1, maxEntries = 200, signal } = {}) {
    const tools = await probe();
    const skipped = tools.gnuFind ? `-printf 'S 0 %P\\n'` : `-exec sh -c 'for f; do echo "S 0 \${f#./}"; done' _ {} +`;
    const shown = tools.gnuFind ? `-printf '%y %s %P\\n'` : `-exec sh -c 'for f; do if [ -d "$f" ]; then echo "d 0 \${f#./}"; else echo "f $(wc -c < "$f" | tr -d " ") \${f#./}"; fi; done' _ {} +`;
    const cmd = [
      `cd -- ${shq(abs)} 2>/dev/null || exit 3`,
      `find . -mindepth 1 -maxdepth ${depth} \\( -type d \\( ${prune} \\) ${skipped} -prune \\) -o ${shown} 2>/dev/null | head -n 20000`,
    ].join('\n');
    const r = await runner.run(cmd, { cwd: '/', timeoutMs: 60_000, signal });
    if (r.exitCode === 3) return null;
    const entries = [];
    for (const line of r.stdout.split('\n')) {
      const m = line.match(/^(\w) (\d+) (.+)$/);
      if (!m) continue;
      const path = m[3].replace(/^\.\//, '');
      if (m[1] === 'S') entries.push({ path, type: 'dir', skipped: true });
      else entries.push({ path, type: m[1] === 'd' ? 'dir' : m[1] === 'l' ? 'link' : 'file', size: Number(m[2]) });
    }
    entries.sort((a, b) => a.path.localeCompare(b.path));
    const total = entries.length;
    return { entries: entries.slice(0, maxEntries), total, truncated: total > maxEntries, root: relativeTo(root, abs) };
  }

  /** All non-ignored files under `abs` (relative paths), deterministic. */
  async function allFiles(abs, { hidden = false, signal } = {}) {
    const tools = await probe();
    let cmd;
    if (tools.rg) {
      cmd = `cd -- ${shq(abs)} || exit 3\nrg --files --sort path ${hidden ? '--hidden ' : ''}${SKIP_DIRS.map((d) => `-g ${shq(`!${d}/`)}`).join(' ')} 2>/dev/null; true`;
    } else {
      cmd = `cd -- ${shq(abs)} || exit 3\nif ${tools.git ? 'git rev-parse --is-inside-work-tree >/dev/null 2>&1 && [ "$(git rev-parse --show-toplevel)" = "$(pwd -P)" ]' : 'false'}; then git ls-files -co --exclude-standard | grep -v -E ${shq(`(^|/)(${SKIP_DIRS.map((d) => d.replace(/\./g, '\\.')).join('|')})/`)} | sort; else find . \\( -type d \\( ${prune} \\) -prune \\) -o -type f -print | sed 's|^\\./||' | sort; fi`;
    }
    const r = await runner.run(cmd, { cwd: '/', timeoutMs: 120_000, signal });
    if (r.exitCode === 3) return null;
    return r.stdout.split('\n').filter(Boolean).filter((p) => hidden || !p.split('/').some((s) => s.startsWith('.') && s !== '.github'));
  }

  /** Files matching a glob, with sizes. */
  async function searchFiles(abs, { root, pattern, maxResults = 100, signal } = {}) {
    const re = globToRegExp(pattern);
    const hidden = /(^|\/)\./.test(pattern);
    const files = await allFiles(abs, { hidden, signal });
    if (files == null) return null;
    const hits = files.filter((f) => re.test(f));
    const shown = hits.slice(0, maxResults);
    let sizes = {};
    if (shown.length) {
      const r = await runner.run(`cd -- ${shq(abs)} && for f in ${shown.map(shq).join(' ')}; do printf '%s\\t%s\\n' "$(wc -c < "$f" 2>/dev/null | tr -d ' ')" "$f"; done`, { cwd: '/', timeoutMs: 60_000, signal });
      sizes = Object.fromEntries(r.stdout.split('\n').filter(Boolean).map((l) => { const i = l.indexOf('\t'); return [l.slice(i + 1), Number(l.slice(0, i))]; }));
    }
    const base = relativeTo(root, abs);
    return { files: shown.map((f) => ({ path: base ? `${base}/${f}` : f, size: sizes[f] ?? null })), total: hits.length, truncated: hits.length > shown.length };
  }

  /**
   * Text search. Returns matches sorted by path then line.
   * @returns {Promise<{matches: {path,line,col,text}[], total:number, files:number, truncated:boolean, engine:string} | null>}
   */
  async function searchText(abs, { root, query, regex = true, caseSensitive, include = [], exclude = [], maxResults = 50, perFile = 20, contextChars = 160, signal } = {}) {
    const tools = await probe();
    const smart = caseSensitive == null ? (/[A-Z]/.test(query) ? 'sensitive' : 'insensitive') : caseSensitive ? 'sensitive' : 'insensitive';
    const limit = Math.max(1, Math.min(500, maxResults));
    let cmd;
    let engine;
    if (tools.rg) {
      engine = 'ripgrep';
      const flags = ['-n', '--column', '--no-heading', '--color', 'never', '--sort', 'path', '-m', String(perFile), '--max-columns', '400', '--max-columns-preview'];
      if (!regex) flags.push('-F');
      flags.push(smart === 'insensitive' ? '-i' : '-s');
      for (const g of include) flags.push('-g', g);
      for (const g of exclude) flags.push('-g', `!${g}`);
      for (const d of SKIP_DIRS) flags.push('-g', `!${d}/`);
      cmd = `cd -- ${shq(abs)} || exit 3\nrg ${flags.map(shq).join(' ')} -e ${shq(query)} . 2>/dev/null | head -n ${limit * 4 + 200}; true`;
    } else {
      engine = 'grep';
      const flags = ['-rnI', regex ? '-E' : '-F', '-m', String(perFile)];
      if (smart === 'insensitive') flags.push('-i');
      for (const d of SKIP_DIRS) flags.push(`--exclude-dir=${d}`);
      for (const g of include) if (!g.includes('/')) flags.push(`--include=${g}`);
      cmd = `cd -- ${shq(abs)} || exit 3\ngrep ${flags.map(shq).join(' ')} -e ${shq(query)} . 2>/dev/null | head -n ${limit * 4 + 200} | sort -t: -k1,1 -k2,2n; true`;
    }
    const r = await runner.run(cmd, { cwd: '/', timeoutMs: 120_000, signal });
    if (r.exitCode === 3) return null;
    const base = relativeTo(root, abs);
    let matcher;
    try {
      matcher = regex ? new RegExp(query, smart === 'insensitive' ? 'i' : '') : null;
    } catch {
      matcher = null;
    }
    const all = [];
    for (const line of r.stdout.split('\n')) {
      if (!line) continue;
      const m = engine === 'ripgrep' ? line.match(/^(.*?):(\d+):(\d+):(.*)$/) : line.match(/^(.*?):(\d+):(.*)$/);
      if (!m) continue;
      const file = m[1].replace(/^\.\//, '');
      if (include.some((g) => g.includes('/')) && !matchesAny(file, include)) continue;
      if (exclude.length && matchesAny(file, exclude)) continue;
      const text = engine === 'ripgrep' ? m[4] : m[3];
      let col = engine === 'ripgrep' ? Number(m[3]) : null;
      if (col == null) {
        const at = matcher ? text.search(matcher) : smart === 'insensitive' ? text.toLowerCase().indexOf(query.toLowerCase()) : text.indexOf(query);
        col = at >= 0 ? at + 1 : 1;
      }
      // Excerpt centered on the match.
      const start = Math.max(0, col - 1 - Math.floor(contextChars / 3));
      let excerpt = text.slice(start, start + contextChars).replace(/\s+$/, '');
      if (start > 0) excerpt = `…${excerpt}`;
      if (start + contextChars < text.length) excerpt += '…';
      all.push({ path: base ? `${base}/${file}` : file, line: Number(m[2]), col, text: excerpt });
    }
    all.sort((a, b) => a.path.localeCompare(b.path) || a.line - b.line);
    const files = new Set(all.map((x) => x.path)).size;
    return { matches: all.slice(0, limit), total: all.length, files, truncated: all.length > limit, engine };
  }

  return { listDir, allFiles, searchFiles, searchText };
}
