// A compact repository map: the file tree plus top-level symbol signatures.
//
// Language-neutral and approximate by design (one regex pass over source
// files; no parser, no LSP). It orients an agent in an unfamiliar codebase for
// a few thousand tokens and complements search_text — it does not replace it.

import { shq } from './paths.js';

const SOURCE_EXT = /\.(rs|go|py|pyi|js|jsx|mjs|cjs|ts|tsx|java|kt|kts|scala|swift|c|h|cc|cpp|cxx|hpp|hh|cs|rb|php|ex|exs|erl|hs|ml|mli|clj|lua|dart|zig|nim|sh|bash|sql|proto|graphql|vue|svelte)$/;

// One ERE (grep -E / rg) matching common top-level definitions.
const DEFS = [
  '^\\s*(pub(\\([^)]*\\))?\\s+)?(async\\s+)?(unsafe\\s+)?(fn|struct|enum|trait|impl|mod|type|union|macro_rules!)\\s+[A-Za-z_]',
  '^\\s*(export\\s+)?(default\\s+)?(declare\\s+)?(abstract\\s+)?(async\\s+)?(function\\*?|class|interface|type|enum)\\s+[A-Za-z_$]',
  '^\\s*export\\s+(const|let)\\s+[A-Za-z_$]',
  '^\\s*(async\\s+)?def\\s+[A-Za-z_]',
  '^\\s*class\\s+[A-Za-z_]',
  '^func\\s+(\\([^)]*\\)\\s*)?[A-Za-z_]',
  '^type\\s+[A-Za-z_]\\w*\\s+(struct|interface)',
  '^\\s*((public|private|protected|internal|static|final|abstract|sealed|data|open)\\s+)*(class|interface|enum|record|object)\\s+[A-Za-z_]',
  '^\\s*(def|defp|defmodule)\\s+[A-Za-z_]',
  '^(CREATE|create)\\s+(TABLE|table|VIEW|view|FUNCTION|function)\\s',
  '^(message|service|enum)\\s+[A-Za-z_]',
].join('|');

function signature(line) {
  let s = line.trim();
  const brace = s.indexOf('{');
  if (brace > 0) s = s.slice(0, brace).trim();
  if (s.endsWith(':') && /^(async\s+)?def |^class /.test(s)) s = s.slice(0, -1);
  return s.length > 140 ? `${s.slice(0, 140)}…` : s;
}

/**
 * @param {object} o
 * @param {ReturnType<import('./runner.js').makeRunner>} o.runner
 * @param {ReturnType<import('./search.js').makeSearch>} o.search
 * @param {() => Promise<{rg:boolean}>} o.probe
 */
export function makeRepoMap({ runner, search, probe }) {
  return async function repoMap(abs, { root, maxFiles = 200, perFile = 12, maxBytes = 14_000, signal } = {}) {
    const files = await search.allFiles(abs, { signal });
    if (files == null) return null;
    const sources = files.filter((f) => SOURCE_EXT.test(f));
    const tools = await probe();
    const symbols = new Map();
    if (sources.length) {
      const cmd = tools.rg
        ? `cd -- ${shq(abs)} && rg -n --no-heading --color never --sort path -m ${perFile * 3} -e ${shq(DEFS)} -g ${shq(`*.{${'rs,go,py,pyi,js,jsx,mjs,cjs,ts,tsx,java,kt,kts,scala,swift,c,h,cc,cpp,cxx,hpp,hh,cs,rb,php,ex,exs,erl,hs,ml,mli,clj,lua,dart,zig,nim,sh,bash,sql,proto,graphql,vue,svelte'}}`)} . 2>/dev/null | head -n 20000; true`
        : `cd -- ${shq(abs)} && printf '%s\\n' ${sources.slice(0, 3000).map(shq).join(' ')} | xargs grep -nHE -m ${perFile * 3} -e ${shq(DEFS)} 2>/dev/null | head -n 20000; true`;
      const r = await runner.run(cmd, { cwd: '/', timeoutMs: 120_000, signal });
      for (const line of r.stdout.split('\n')) {
        const m = line.match(/^(?:\.\/)?(.*?):(\d+):(.*)$/);
        if (!m) continue;
        if (!symbols.has(m[1])) symbols.set(m[1], []);
        const list = symbols.get(m[1]);
        if (list.length < perFile) list.push(`${m[2]}: ${signature(m[3])}`);
      }
    }
    // Directory summary for everything, symbols for the first maxFiles sources.
    const dirs = new Map();
    for (const f of files) {
      const d = f.includes('/') ? f.slice(0, f.lastIndexOf('/')) : '.';
      dirs.set(d, (dirs.get(d) || 0) + 1);
    }
    const lines = [`${files.length} files (${sources.length} source) in ${dirs.size} directories`];
    let bytes = lines[0].length;
    let shown = 0;
    let omitted = 0;
    for (const f of sources) {
      const syms = symbols.get(f) || [];
      const block = [`${f}`, ...syms.map((s) => `  ${s}`)].join('\n');
      if (shown >= maxFiles || bytes + block.length > maxBytes) {
        omitted++;
        continue;
      }
      lines.push(block);
      bytes += block.length + 1;
      shown++;
    }
    if (omitted) lines.push(`… ${omitted} more source files (narrow with \`path\`, or use search_files / search_text)`);
    const other = [...dirs.entries()].sort((a, b) => b[1] - a[1]).slice(0, 15).map(([d, n]) => `${d}/ (${n})`);
    lines.push(`largest directories: ${other.join(', ')}`);
    return { text: lines.join('\n'), files: files.length, sources: sources.length, shown, omitted, root: abs === root ? '' : abs };
  };
}
