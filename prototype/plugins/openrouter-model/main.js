// openrouter-model: the OpenRouter provider plugin (https://openrouter.ai).
// One key reaches hundreds of models; tool calls and streaming use the
// OpenAI-compatible API. The core has no model client — this is just a plugin.
//
// Everything provider-specific lives here: the model catalog (exposed as the
// `model-catalog` service for frontends), model metadata (context length →
// the projection's context budget), request shaping (prompt-cache breakpoints
// for Anthropic models, usage accounting), credentials, and retries.
//
// Config:
//   model        OpenRouter model slug (default "openai/gpt-4o-mini")
//   api_key_env  env var holding the key (native runtime; default OPENROUTER_API_KEY)
//   api_key      key value (browser runtime only; never set this in agentmod.toml)
//   temperature, max_tokens, provider (OpenRouter provider-routing preferences)
//   max_context_tokens   override the context budget (default: 75% of the model's window)
//   fallback_models      models tried in order when the model fails (each a new stream attempt)
//   stream_retries       retries of a stream that fails after HTTP 200 (default 1)
import { definePlugin } from '../sdk/agentmod.js';
import { complete, resolveKey } from '../sdk/openai-compat.js';
import { fetchCatalog, contextBudget } from '../sdk/catalog.js';

const BASE = 'https://openrouter.ai/api/v1';
const base = (cfg) => cfg.base_url || BASE;

definePlugin({
  manifest: {
    name: 'openrouter-model',
    version: '0.2.0',
    description: 'OpenRouter provider: any OpenRouter model, streamed, with tool calls.',
    consumes: [{ event: 'model-request', demands: ['turn'], mode: 'async' }],
    // One canonical event per model turn. Token deltas are not events: they
    // stream to the host's stream hub (sdk/stream.js) for live delivery.
    emits: [{ event: 'model-response', supplies: ['text', 'stream_id'] }],
    // Read-only services frontends can call through the host (not recorded:
    // they change no state; choosing a model is a recorded config apply).
    services: [
      { name: 'model-catalog', description: 'List models: id, name, context window, tool and vision support, prices' },
      { name: 'check-credentials', description: 'Verify the configured key' },
    ],
    // What a frontend needs to configure this provider, provider-neutrally.
    settings: [
      { key: 'api_key', label: 'OpenRouter API key', secret: true, required: true, help: 'Kept by your runtime; sent only to openrouter.ai.', link: 'https://openrouter.ai/keys', browser_only: true },
      { key: 'model', label: 'Model', type: 'model', required: true },
    ],
    provides: ['model'],
    config_schema: { model: 'openai/gpt-4o-mini', api_key_env: 'OPENROUTER_API_KEY', api_key: 'browser only', temperature: 0.3, max_tokens: 1024 },
  },
  // A key is mandatory: without one the plugin refuses its handshake, so no
  // config that uses it can compile and the runtime will not start.
  validate: (cfg, { env }) => {
    const name = cfg.api_key_env || 'OPENROUTER_API_KEY';
    if (!cfg.api_key && !env(name)) {
      throw Object.assign(new Error(`OpenRouter API key required: export ${name} before \`agentmod serve\` (it is read from the environment and never written to the log)`), { data: { needs: ['api_key'] } });
    }
  },
  services: {
    'model-catalog': async (_args, { config }) => ({ models: await fetchCatalog(base(config), resolveKey(config, 'OPENROUTER_API_KEY')), selected: config.model || 'openai/gpt-4o-mini' }),
    'check-credentials': async (args, { config }) => {
      const key = args?.api_key || resolveKey(config, 'OPENROUTER_API_KEY');
      const res = await fetch(`${base(config).replace(/\/$/, '')}/key`, { headers: { authorization: `Bearer ${key}` }, signal: AbortSignal.timeout(10_000) });
      return { ok: res.ok, status: res.status };
    },
  },
  handlers: {
    'model-request': async (ctx) => {
      const cfg = ctx.config;
      const key = resolveKey(cfg, 'OPENROUTER_API_KEY');
      const model = cfg.model || 'openai/gpt-4o-mini';
      const extra = { usage: { include: true } };
      if (cfg.max_tokens) extra.max_tokens = cfg.max_tokens;
      if (cfg.provider) extra.provider = cfg.provider;
      const out = await complete(ctx, {
        baseUrl: base(cfg),
        key,
        model,
        temperature: cfg.temperature ?? 0.3,
        fallbackModels: cfg.fallback_models || [],
        streamRetries: cfg.stream_retries ?? 1,
        provider: 'openrouter',
        headers: { 'HTTP-Referer': cfg.referer || 'https://github.com/JPrier/AgentMod', 'X-Title': 'AgentMod prototype' },
        extra,
        // Anthropic models cache only at explicit breakpoints; the system prompt
        // (with tool-stable content) is the stable prefix.
        cacheSystem: cfg.prompt_cache !== false && /^anthropic\//.test(model),
        projection: cfg.minimal ? { minimal: true } : { maxContextTokens: cfg.max_context_tokens || (await contextBudget(base(cfg), key, model)) },
      });
      await ctx.publish('model-response', { ...out, provider: 'openrouter' });
    },
  },
});
