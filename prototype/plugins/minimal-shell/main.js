// minimal-shell: the deliberately minimal baseline for benchmarking.
//
// One tool, `shell`, run in a local directory with bounded output — no
// checkpoints, no structured file tools, no policy, no discovery. With
// chat-context and a provider configured with `minimal: true`, it is the
// "model → tool → result → model" loop the full harness is measured against
// (see bench/). Native runtime only.
import { definePlugin, declareTools, offerTools, ownTools, toolSpec } from '../sdk/agentmod.js';
import { localTarget } from '../local-workspace/target.js';
import { truncate } from '../sdk/coding/text.js';

const SHELL = toolSpec('shell', 'Run a bash command in the working directory and return exit code, stdout and stderr.', {
  command: { type: 'string', description: 'bash command line' },
  timeout_seconds: { type: 'integer', description: 'default 120' },
}, { required: ['command'] });

let target = null;
const root = (cfg) => localTarget.resolveRoot(cfg.root || '.agentmod/workspace');

definePlugin({
  manifest: {
    name: 'minimal-shell',
    version: '0.1.0',
    description: 'Baseline: a single shell tool, nothing else.',
    consumes: [
      { event: 'session-started' },
      ownTools(['shell']),
    ],
    tools: declareTools([SHELL]),
    emits: [{ event: 'tool-result', supplies: ['call_id', 'name', 'output'] }],
    config_schema: { root: '.agentmod/workspace' },
  },
  handlers: {
    'session-started': (ctx) => {
      offerTools(ctx, [SHELL]);
      ctx.add('system', `You work in ${root(ctx.config)} with a single shell tool.`);
    },
    'tool-call': async (ctx) => {
      const { call_id, name, args } = ctx.payload;
      if (name !== 'shell') return;
      target ??= localTarget({ root: root(ctx.config) });
      await target.ensureReady();
      const t0 = Date.now();
      const timeout = Math.min(1800, Math.max(1, Number(args?.timeout_seconds) || 120));
      const r = await target.exec({ command: String(args?.command || 'true'), cwd: root(ctx.config), timeoutMs: timeout * 1000, signal: ctx.signal });
      const out = new TextDecoder().decode(r.stdout);
      const err = new TextDecoder().decode(r.stderr);
      const output = `exit ${r.exitCode}${r.timedOut ? ' (timed out)' : ''}\n${truncate(out, 12000)}${err ? `\n--- stderr ---\n${truncate(err, 8000)}` : ''}`;
      await ctx.publish('tool-result', { call_id, name, output, error: r.exitCode !== 0, exit_code: r.exitCode, duration_ms: Date.now() - t0 });
    },
  },
});
