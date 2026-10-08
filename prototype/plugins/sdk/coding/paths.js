// Paths in an execution target's namespace (absolute POSIX paths).

/** An expected, user-facing failure (reported to the model as a tool error). */
export class ToolError extends Error {}

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
  if (!within(r, abs)) throw new ToolError(`\`${p}\` is outside the workspace (${r}); use paths under it`);
  return abs;
}

/** Is `abs` equal to or inside `root`? (both normalized) */
export function within(root, abs) {
  return abs === root || abs.startsWith(root === '/' ? '/' : `${root}/`);
}

/** Path of `abs` relative to `root` ('' for the root itself). */
export function relativeTo(root, abs) {
  if (abs === root) return '';
  return abs.slice(root === '/' ? 1 : root.length + 1);
}

/** Quote a string for a POSIX shell. */
export function shq(s) {
  return `'${String(s).replace(/'/g, `'\\''`)}'`;
}

/** Directory part of an absolute path. */
export function dirname(p) {
  const i = p.lastIndexOf('/');
  return i <= 0 ? '/' : p.slice(0, i);
}
