// Messages a model can correct itself from in one step: what a tool takes,
// and the closest names when it calls a tool that does not exist. (Arguments
// are fitted to a tool's shape in sdk/toolschema.js.)

export function describeParams(spec) {
  const props = spec?.parameters && typeof spec.parameters === 'object' ? spec.parameters : {};
  const req = new Set(spec?.required || []);
  return Object.entries(props)
    .map(([k, v]) => `${k}${req.has(k) ? '' : '?'}: ${v?.type || 'any'}${v?.description ? ` (${String(v.description).slice(0, 80)})` : ''}`)
    .join('; ');
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

/**
 * The answer to a call of a tool outside the session's resolved tool set
 * ("no such tool"), with the closest names so the model can correct itself.
 */
export function suggestTools(name, offered) {
  const n = String(name || '');
  const close = offered
    .map((t) => ({ t, d: distance(n.toLowerCase(), t.toLowerCase()) - (t.includes(n) || n.includes(t) ? 2 : 0) }))
    .filter((x) => x.d <= Math.max(2, Math.floor(n.length / 3)))
    .sort((a, b) => a.d - b.d || a.t.localeCompare(b.t))
    .slice(0, 3)
    .map((x) => x.t);
  const search = offered.includes('tool_search') ? ' If you need another capability, call tool_search.' : '';
  return `no such tool: ${n}.${close.length ? ` Did you mean ${close.map((c) => `\`${c}\``).join(' or ')}?` : ''} Available tools: ${[...offered].sort().join(', ') || 'none'}.${search}`;
}
