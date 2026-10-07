// The OpenRouter model catalog, fetched live from the provider's `/models`
// endpoint. Nothing here is a hardcoded model list. The key is sent when one
// is available (OpenRouter documents bearer auth for this endpoint).

const DEFAULT_BASE = 'https://openrouter.ai/api/v1';
const CACHE_KEY = 'agentmod.openrouter.models';
const CACHE_TTL_MS = 60 * 60 * 1000;

const inflight = new Map();

/**
 * @typedef {{ id: string, name: string, context: number|null, tools: boolean,
 *   promptPrice: number|null, completionPrice: number|null }} ModelInfo
 */

function normalize(m) {
  const price = (v) => (v == null || v === '' || Number.isNaN(Number(v)) ? null : Number(v));
  const params = Array.isArray(m.supported_parameters) ? m.supported_parameters : [];
  return {
    id: String(m.id),
    name: String(m.name || m.id),
    context: m.context_length ?? m.top_provider?.context_length ?? null,
    tools: params.includes('tools'),
    promptPrice: price(m.pricing?.prompt),
    completionPrice: price(m.pricing?.completion),
  };
}

function readCache(base) {
  try {
    const c = JSON.parse(sessionStorage.getItem(CACHE_KEY) || 'null');
    if (c && c.base === base && Date.now() - c.at < CACHE_TTL_MS && Array.isArray(c.models)) return c.models;
  } catch { /* ignore */ }
  return null;
}

/**
 * List models available on an OpenRouter-compatible endpoint.
 * @param {string} [base]
 * @param {string} [key] API key, sent as a bearer token when present
 * @returns {Promise<ModelInfo[]>}
 */
export function listModels(base = DEFAULT_BASE, key = '') {
  base = base.replace(/\/$/, '');
  const cached = readCache(base);
  if (cached) return Promise.resolve(cached);
  if (!inflight.has(base)) {
    inflight.set(
      base,
      (async () => {
        const headers = key ? { authorization: `Bearer ${key}` } : {};
        const res = await fetch(`${base}/models`, { headers, signal: AbortSignal.timeout(10000) });
        if (!res.ok) throw new Error(`model list unavailable (HTTP ${res.status})`);
        const body = await res.json();
        const models = (Array.isArray(body?.data) ? body.data : []).filter((m) => m && m.id).map(normalize);
        models.sort((a, b) => a.id.localeCompare(b.id));
        try {
          sessionStorage.setItem(CACHE_KEY, JSON.stringify({ base, at: Date.now(), models }));
        } catch { /* storage full or blocked */ }
        return models;
      })().finally(() => inflight.delete(base)),
    );
  }
  return inflight.get(base);
}

/** Per-million-token price text, or '' when unknown. */
export function priceLabel(m) {
  if (m.promptPrice == null || m.completionPrice == null) return '';
  if (m.promptPrice === 0 && m.completionPrice === 0) return 'free';
  const per = (v) => `$${(v * 1e6).toFixed(v * 1e6 < 1 ? 2 : 1)}`;
  return `${per(m.promptPrice)} in / ${per(m.completionPrice)} out per M`;
}

/** One-line description used in the picker. */
export function describe(m) {
  const parts = [m.name !== m.id ? m.name : null, m.context ? `${Math.round(m.context / 1000)}k ctx` : null, priceLabel(m) || null, m.tools ? null : 'no tool calling'];
  return parts.filter(Boolean).join(' · ');
}
