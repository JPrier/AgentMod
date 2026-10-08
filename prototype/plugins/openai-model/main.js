// openai-model: live provider for any OpenAI-compatible /chat/completions
// endpoint (OpenAI, Ollama, vLLM, LM Studio...).
// Config: { base_url, model, api_key_env? (native), api_key? (browser), temperature?, max_context_tokens?,
//           fallback_models?, stream_retries? }
import { definePlugin } from '../sdk/agentmod.js';
import { complete, resolveKey } from '../sdk/openai-compat.js';
import { fetchCatalog } from '../sdk/catalog.js';

const base = (cfg) => cfg.base_url || 'https://api.openai.com/v1';

definePlugin({
  manifest: {
    name: 'openai-model',
    version: '0.2.0',
    description: 'Live OpenAI-compatible chat-completions provider with streaming and tool calls.',
    consumes: [{ event: 'model-request', demands: ['turn'], mode: 'async' }],
    // One canonical event per model turn. Token deltas are not events: they
    // stream to the host's stream hub (sdk/stream.js) for live delivery.
    emits: [{ event: 'model-response', supplies: ['text', 'stream_id'] }],
    services: [{ name: 'model-catalog', description: 'List models served by the endpoint' }],
    settings: [
      { key: 'base_url', label: 'Endpoint (OpenAI-compatible)', required: true },
      { key: 'api_key', label: 'API key', secret: true, browser_only: true },
      { key: 'model', label: 'Model', type: 'model', required: true },
    ],
    provides: ['model'],
    config_schema: { base_url: 'https://api.openai.com/v1', model: 'gpt-4o-mini', api_key_env: 'OPENAI_API_KEY', api_key: 'browser only', max_context_tokens: 96000 },
  },
  services: {
    'model-catalog': async (_args, { config }) => ({ models: await fetchCatalog(base(config), resolveKey(config, 'OPENAI_API_KEY')), selected: config.model || 'gpt-4o-mini' }),
  },
  handlers: {
    'model-request': async (ctx) => {
      const cfg = ctx.config;
      const out = await complete(ctx, {
        baseUrl: base(cfg),
        key: resolveKey(cfg, 'OPENAI_API_KEY'),
        model: cfg.model || 'gpt-4o-mini',
        temperature: cfg.temperature ?? 0.3,
        fallbackModels: cfg.fallback_models || [],
        streamRetries: cfg.stream_retries ?? 1,
        provider: 'openai-compatible',
        projection: cfg.minimal ? { minimal: true } : { maxContextTokens: cfg.max_context_tokens || 96_000 },
      });
      await ctx.publish('model-response', { ...out, provider: 'openai-compatible' });
    },
  },
});
