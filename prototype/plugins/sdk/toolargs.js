// Argument checks and tool-name suggestions, written so a model can correct
// itself in one step: say what was wrong, what the tool takes, and what to send.
// Deliberately lenient: tools coerce reasonable inputs (an integer as a string,
// one glob instead of a list), so only clear mistakes are rejected here.

const typeOf = (v) => (Array.isArray(v) ? 'array' : v === null ? 'null' : typeof v);

function describeParams(spec) {
  const props = spec?.parameters && typeof spec.parameters === 'object' ? spec.parameters : {};
  const req = new Set(spec?.required || []);
  return Object.entries(props)
    .map(([k, v]) => `${k}${req.has(k) ? '' : '?'}: ${v?.type || 'any'}${v?.description ? ` (${String(v.description).slice(0, 80)})` : ''}`)
    .join('; ');
}

/** null when the arguments are acceptable, otherwise an actionable message. */
export function checkArgs(spec, args) {
  if (!spec) return null;
  const props = spec.parameters && typeof spec.parameters === 'object' ? spec.parameters : {};
  const missing = (spec.required || []).filter((k) => args[k] === undefined || args[k] === null || args[k] === '');
  const wrong = [];
  for (const [k, v] of Object.entries(args)) {
    const want = props[k]?.type;
    if (!want || v === undefined || v === null) continue;
    const got = typeOf(v);
    // Clear mismatches only: structured values where text or a flag is expected,
    // or text where a structure is expected.
    const bad = (want === 'string' && (got === 'object' || got === 'array'))
      || (want === 'boolean' && (got === 'object' || got === 'array'))
      || ((want === 'object') && got !== 'object')
      || (want === 'array' && (got === 'object' || got === 'boolean' || got === 'number'));
    if (bad) wrong.push(`\`${k}\` must be ${want === 'array' ? 'an array' : `a ${want}`} (got ${got})`);
  }
  if (!missing.length && !wrong.length) return null;
  const parts = [];
  if (missing.length) parts.push(`missing required ${missing.map((k) => `\`${k}\``).join(', ')}`);
  parts.push(...wrong);
  const example = Object.fromEntries((spec.required || []).map((k) => [k, props[k]?.type === 'array' ? [] : props[k]?.type === 'integer' || props[k]?.type === 'number' ? 0 : props[k]?.type === 'boolean' ? false : `<${k}>`]));
  return `Invalid call to \`${spec.name}\`: ${parts.join('; ')}. Its parameters: ${describeParams(spec) || 'none'}. Call it again like ${JSON.stringify(example)}.`;
}

function distance(a, b) {
  const d = Array.from({ length: a.length + 1 }, (_, i) => [i, ...Array(b.length).fill(0)]);
  for (let j = 1; j <= b.length; j++) d[0][j] = j;
  for (let i = 1; i <= a.length; i++) {
    for (let j = 1; j <= b.length; j++) {
      d[i][j] = Math.min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1));
    }
  }
  return d[a.length][b.length];
}

/** Message for a call to a tool that is not available, with the closest names. */
export function suggestTools(name, offered, { routed } = {}) {
  const n = String(name || '');
  const close = offered
    .map((t) => ({ t, d: distance(n.toLowerCase(), t.toLowerCase()) - (t.includes(n) || n.includes(t) ? 2 : 0) }))
    .filter((x) => x.d <= Math.max(2, Math.floor(n.length / 3)))
    .sort((a, b) => a.d - b.d || a.t.localeCompare(b.t))
    .slice(0, 3)
    .map((x) => x.t);
  const search = offered.includes('tool_search') ? ' If you need another capability, call tool_search.' : '';
  const owned = routed && !offered.includes(n) && routed.some((v) => v === n) ? ' (it exists but is not loaded: call tool_search with "select:' + n + '")' : '';
  return `There is no tool named \`${n}\`${owned}.${close.length ? ` Did you mean ${close.map((c) => `\`${c}\``).join(' or ')}?` : ''} Available tools: ${[...offered].sort().join(', ')}.${search}`;
}
