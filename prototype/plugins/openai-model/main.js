// openai-model: live provider for any OpenAI-compatible /chat/completions
// endpoint (OpenAI, Ollama, vLLM, LM Studio...).
// Config: { base_url, model, api_key_env? (native), api_key? (browser), temperature? }
import { definePlugin } from '../sdk/agentmod.js';
import { complete, resolveKey } from '../sdk/openai-compat.js';

definePlugin({
  manifest: {
    name: 'openai-model',
    version: '0.1.0',
    description: 'Live OpenAI-compatible chat-completions provider with streaming and tool calls.',
    consumes: [{ event: 'model-request', demands: ['turn'], mode: 'async' }],
    emits: [
      { event: 'stream-chunk', supplies: ['stream_id', 'text'] },
      { event: 'model-response', supplies: ['text', 'stream_id'] },
    ],
    config_schema: { base_url: 'https://api.openai.com/v1', model: 'gpt-4o-mini', api_key_env: 'OPENAI_API_KEY', api_key: 'browser only' },
  },
  handlers: {
    'model-request': async (ctx) => {
      const cfg = ctx.config;
      const out = await complete(ctx, { baseUrl: cfg.base_url || 'https://api.openai.com/v1', key: resolveKey(cfg, 'OPENAI_API_KEY'), model: cfg.model || 'gpt-4o-mini', temperature: cfg.temperature ?? 0.3 });
      await ctx.publish('model-response', out);
    },
  },
});
