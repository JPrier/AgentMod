// Typed diagnostics from compiler, linter, and test-runner output.
//
// A generic parser, not a per-language subsystem: a small table of line
// patterns covers the common shapes (GCC/Clang/Go/ESLint-unix/"file:line:col:
// severity: message", rustc/cargo "--> file:line:col", TypeScript
// "file(line,col): error TS…", Python tracebacks, pytest/TAP failures). Plugin
// config can add patterns. Raw output always stays available; diagnostics are
// a structured, deduplicated digest of it.

const SEV = (s) => {
  const v = String(s || '').toLowerCase();
  if (v.startsWith('err') || v === 'fatal' || v === 'fail' || v === 'failed' || v === 'e') return 'error';
  if (v.startsWith('warn') || v === 'w') return 'warning';
  if (v === 'note' || v === 'info' || v === 'help' || v === 'hint') return 'note';
  return 'error';
};

/** Built-in single-line patterns. Each yields { file, line, col?, severity, message, source }. */
const LINE_PATTERNS = [
  // TypeScript: src/a.ts(3,5): error TS2304: Cannot find name 'x'.
  { source: 'tsc', re: /^(?<file>[^\s(][^(]*?)\((?<line>\d+),(?<col>\d+)\): (?<sev>error|warning) (?<code>TS\d+): (?<msg>.+)$/ },
  // GCC/Clang/Go/ESLint (unix)/mypy/flake8/shellcheck(gcc): file:line:col: severity: message
  { source: 'compiler', re: /^(?<file>[^\s:][^:]*?):(?<line>\d+):(?<col>\d+): (?:(?<sev>fatal error|error|warning|note|info)(?:\[[^\]]*\])?: )?(?<msg>.+)$/ },
  // file:line: severity: message (no column), e.g. mypy, some linters
  { source: 'compiler', re: /^(?<file>[^\s:][^:]*?\.[A-Za-z0-9]+):(?<line>\d+): (?<sev>error|warning|note): (?<msg>.+)$/ },
  // pytest short summary: FAILED tests/test_x.py::test_y - AssertionError: …
  { source: 'pytest', re: /^(?<sev>FAILED|ERROR) (?<file>[^\s:]+\.py)::(?<test>\S+)(?: - (?<msg>.+))?$/ },
  // pytest/unittest location line: tests/test_x.py:12: AssertionError
  { source: 'pytest', re: /^(?<file>[^\s:]+\.py):(?<line>\d+): (?<msg>\w*(?:Error|Exception|Failure)\b.*)$/ },
  // ESLint stylish entries are handled statefully below.
];

/**
 * Parse diagnostics from command output.
 * @param {string} text  combined stdout + stderr
 * @param {object} [opts]
 * @param {string} [opts.cwd]   directory relative paths are relative to
 * @param {Array<{regex: string, source?: string}>} [opts.patterns]  extra named-group patterns
 * @param {number} [opts.max]   result bound (default 50)
 */
export function parseDiagnostics(text, { patterns = [], max = 50 } = {}) {
  const out = [];
  const seen = new Set();
  const push = (d) => {
    if (!d.file || !d.message) return;
    const key = `${d.file}:${d.line ?? ''}:${d.col ?? ''}:${d.severity}:${d.message}`;
    if (seen.has(key)) return;
    seen.add(key);
    out.push(d);
  };
  const extra = patterns
    .map((p) => {
      try { return { source: p.source || 'custom', re: new RegExp(p.regex) }; } catch { return null; }
    })
    .filter(Boolean);
  const lines = String(text || '').replace(/\x1b\[[0-9;]*m/g, '').split('\n');
  let rust = null; // pending rustc headline: { severity, message, code }
  let stylish = null; // current ESLint stylish file
  let pyFile = null; // last python traceback frame
  for (let i = 0; i < lines.length && out.length < max; i++) {
    const l = lines[i].replace(/\r$/, '');
    // rustc/cargo: "error[E0425]: msg" then " --> src/main.rs:3:5"
    let m = l.match(/^(error|warning)(?:\[(\w+)\])?: (.+)$/);
    if (m) { rust = { severity: SEV(m[1]), code: m[2], message: m[3] }; continue; }
    m = l.match(/^\s*--> ([^:]+):(\d+):(\d+)$/);
    if (m && rust) {
      push({ file: m[1], line: +m[2], col: +m[3], severity: rust.severity, message: rust.code ? `${rust.code}: ${rust.message}` : rust.message, source: 'rustc' });
      rust = null;
      continue;
    }
    // Python traceback frames; the exception line closes the traceback.
    m = l.match(/^\s*File "([^"]+)", line (\d+)/);
    if (m) { pyFile = { file: m[1], line: +m[2] }; continue; }
    m = l.match(/^(\w+(?:\.\w+)*(?:Error|Exception|Exit|Interrupt|Warning)): ?(.*)$/);
    if (m && pyFile) {
      push({ ...pyFile, severity: /Warning$/.test(m[1]) ? 'warning' : 'error', message: `${m[1]}: ${m[2]}`.trim(), source: 'python' });
      pyFile = null;
      continue;
    }
    // ESLint stylish: a path line, then "  3:5  error  message  rule".
    if (/^(\/|\.{0,2}\/|[A-Za-z]:\\|[\w.-]+\/)[^\s:]+\.\w+$/.test(l)) { stylish = l.trim(); continue; }
    m = l.match(/^\s+(\d+):(\d+)\s+(error|warning)\s+(.+?)(?:\s{2,}(\S+))?$/);
    if (m && stylish) {
      push({ file: stylish, line: +m[1], col: +m[2], severity: m[3], message: m[5] ? `${m[4]} (${m[5]})` : m[4], source: 'eslint' });
      continue;
    }
    if (!l.trim()) stylish = null;
    // TAP: "not ok 3 - name"
    m = l.match(/^\s*not ok \d+ - (.+)$/);
    if (m) { push({ file: '(test)', severity: 'error', message: `failed: ${m[1]}`, source: 'tap' }); continue; }
    for (const p of [...extra, ...LINE_PATTERNS]) {
      const g = l.match(p.re)?.groups;
      if (!g) continue;
      if (/^(https?|file)$/.test(g.file || '') || !/[./\\]/.test(g.file || '') || /\s{2,}/.test(g.file || '')) continue;
      const message = g.test ? `${g.test}${g.msg ? `: ${g.msg}` : ''}` : g.code && !String(g.msg).startsWith(g.code) ? `${g.code}: ${g.msg}` : g.msg;
      push({ file: g.file?.trim(), line: g.line ? +g.line : undefined, col: g.col ? +g.col : undefined, severity: SEV(g.sev), message: String(message || '').trim(), source: p.source });
      break;
    }
  }
  return out;
}

/** One-line summary plus the first few, for the model-facing text. */
export function summarizeDiagnostics(diags, show = 10) {
  if (!diags.length) return '';
  const n = (s) => diags.filter((d) => d.severity === s).length;
  const counts = [['error', n('error')], ['warning', n('warning')], ['note', n('note')]].filter(([, c]) => c).map(([s, c]) => `${c} ${s}${c === 1 ? '' : 's'}`);
  const lines = diags.slice(0, show).map((d) => `  ${d.file}${d.line ? `:${d.line}` : ''}${d.col ? `:${d.col}` : ''} ${d.severity}: ${d.message.slice(0, 200)}`);
  return `Diagnostics (parsed): ${counts.join(', ')}\n${lines.join('\n')}${diags.length > show ? `\n  … ${diags.length - show} more` : ''}`;
}
