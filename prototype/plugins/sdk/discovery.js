// Lazy tool discovery shared by the `tool-discovery` plugin (pure; importable by tests).
import { toolSpec } from './agentmod.js';

export const TOOL_SEARCH = toolSpec('tool_search', 'Find and load additional tools by keyword (e.g. "browser", "github", "delegate", "image", "mcp") or exact names with "select:name1,name2". Loaded tools become callable from your next step.', {
  query: { type: 'string', description: 'keywords, or select:name1,name2' },
  limit: { type: 'integer', description: 'maximum tools to load (default 5)' },
}, { required: ['query'], tier: 'core', group: 'discovery', effects: 'read' });

export function searchTools(tools, query, { hidden = new Set(), limit = 5 } = {}) {
  const q = String(query || '').trim();
  const pool = tools.filter((t) => t && t.name && !hidden.has(t.name) && t.name !== 'tool_search');
  if (q.startsWith('select:')) {
    const want = q.slice(7).split(',').map((s) => s.trim()).filter(Boolean);
    return pool.filter((t) => want.includes(t.name));
  }
  const words = q.toLowerCase().split(/[^a-z0-9_]+/).filter((w) => w.length > 1);
  if (!words.length) return [];
  const scored = pool.map((t) => {
    const name = t.name.toLowerCase();
    const group = String(t.group || '').toLowerCase();
    const desc = String(t.description || '').toLowerCase();
    let score = 0;
    for (const w of words) {
      if (name === w) score += 10;
      else if (name.includes(w)) score += 6;
      if (group === w || group.includes(w)) score += 4;
      if (desc.includes(w)) score += 2;
    }
    if (t.tier !== 'deferred') score -= 1; // already sent; still listed if it matches
    return { t, score };
  }).filter((x) => x.score > 1);
  scored.sort((a, b) => b.score - a.score || a.t.name.localeCompare(b.t.name));
  return scored.slice(0, Math.max(1, Math.min(20, limit))).map((x) => x.t);
}

export const brief = (t) => {
  const params = Object.entries(t.parameters || {}).map(([k, v]) => `${k}${(t.required || []).includes(k) ? '' : '?'}: ${v?.type || 'any'}`).join(', ');
  return `${t.name}(${params}) — ${String(t.description || '').slice(0, 300)}`;
};

