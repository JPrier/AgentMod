// Model catalogs for OpenAI-compatible providers, fetched live from the
// provider's `/models` endpoint and normalized to one provider-neutral shape.
// Provider plugins expose this as their `model-catalog` service; frontends
// never call providers directly.

/**
 * @typedef {{ id: string, name: string, context: number|null, tools: boolean|null,
 *   vision: boolean|null, prompt_price: number|null, completion_price: number|null }} ModelInfo
 */

const price = (v) => (v == null || v === '' || Number.isNaN(Number(v)) ? null : Number(v));

/** Normalize one `/models` entry (OpenRouter's rich shape, or OpenAI's bare one). */
export function normalizeModel(m) {
  const params = Array.isArray(m.supported_parameters) ? m.supported_parameters : null;
  const modalities = m.architecture?.input_modalities;
  return {
    id: String(m.id),
    name: String(m.name || m.id),
    context: m.context_length ?? m.top_provider?.context_length ?? null,
    tools: params ? params.includes('tools') : null,
    vision: Array.isArray(modalities) ? modalities.includes('image') : null,
    prompt_price: price(m.pricing?.prompt),
    completion_price: price(m.pricing?.completion),
  };
}

const caches = new Map(); // base -> { at, models }
const TTL = 60 * 60 * 1000;

/** Fetch (and cache for an hour) a provider's model list. */
export async function fetchCatalog(base, key, { signal, fetchImpl = globalThis.fetch } = {}) {
  const b = base.replace(/\/$/, '');
  const hit = caches.get(b);
  if (hit && Date.now() - hit.at < TTL) return hit.models;
  const res = await fetchImpl(`${b}/models`, { headers: key ? { authorization: `Bearer ${key}` } : {}, signal: signal ?? AbortSignal.timeout(15_000) });
  if (!res.ok) throw new Error(`model list unavailable (HTTP ${res.status})`);
  const body = await res.json();
  const models = (Array.isArray(body?.data) ? body.data : []).filter((m) => m && m.id).map(normalizeModel);
  models.sort((a, b2) => a.id.localeCompare(b2.id));
  caches.set(b, { at: Date.now(), models });
  return models;
}

/** Context budget (tokens) for a model, from the catalog when known. */
export async function contextBudget(base, key, model, fallback = 96_000) {
  try {
    const m = (await fetchCatalog(base, key)).find((x) => x.id === model);
    if (m?.context) return Math.max(8_000, Math.floor(m.context * 0.75));
  } catch { /* the catalog is optional for requests */ }
  return fallback;
}
