// openrouter-model: the OpenRouter provider plugin (https://openrouter.ai).
// One key reaches hundreds of models; tool calls and streaming use the
// OpenAI-compatible API. The core has no model client — this is just a plugin.
//
// Config:
//   model        OpenRouter model slug (default "openai/gpt-4o-mini")
//   api_key_env  env var holding the key (native runtime; default OPENROUTER_API_KEY)
//   api_key      key value (browser runtime only; never set this in agentmod.toml)
//   temperature, max_tokens, provider (OpenRouter provider-routing preferences)
import { definePlugin } from '../sdk/agentmod.js';
import { complete, resolveKey } from '../sdk/openai-compat.js';

const BASE = 'https://openrouter.ai/api/v1';

definePlugin({
  manifest: {
    name: 'openrouter-model',
    version: '0.1.0',
    description: 'OpenRouter provider: any OpenRouter model, streamed, with tool calls.',
    consumes: [{ event: 'model-request', demands: ['turn'], mode: 'async' }],
    emits: [
      { event: 'stream-chunk', supplies: ['stream_id', 'text'] },
      { event: 'model-response', supplies: ['text', 'stream_id'] },
    ],
    config_schema: { model: 'openai/gpt-4o-mini', api_key_env: 'OPENROUTER_API_KEY', api_key: 'browser only', temperature: 0.3, max_tokens: 1024 },
  },
  // A key is mandatory: without one the plugin refuses its handshake, so no
  // config that uses it can compile and the runtime will not start.
  validate: (cfg, { env }) => {
    const name = cfg.api_key_env || 'OPENROUTER_API_KEY';
    if (!cfg.api_key && !env(name)) {
      throw new Error(`OpenRouter API key required: export ${name} before \`agentmod serve\` (it is read from the environment and never written to the log)`);
    }
  },
  handlers: {
    'model-request': async (ctx) => {
      const cfg = ctx.config;
      const key = resolveKey(cfg, 'OPENROUTER_API_KEY');
      const extra = { usage: { include: true } };
      if (cfg.max_tokens) extra.max_tokens = cfg.max_tokens;
      if (cfg.provider) extra.provider = cfg.provider;
      const out = await complete(ctx, {
        baseUrl: cfg.base_url || BASE,
        key,
        model: cfg.model || 'openai/gpt-4o-mini',
        temperature: cfg.temperature ?? 0.3,
        headers: { 'HTTP-Referer': cfg.referer || 'https://github.com/JPrier/AgentMod', 'X-Title': 'AgentMod prototype' },
        extra,
      });
      await ctx.publish('model-response', out);
    },
  },
});
