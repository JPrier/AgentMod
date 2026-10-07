// A plugin that answers the coding tools (run, read_file, write_file,
// edit_file, list_files, import_repo) against one execution target.
//
// `linux-sandbox` and `local-workspace` are both this plugin with a different
// target, like `openrouter-model` and `openai-model` share openai-compat.js.
// Which one a session uses is decided by its definition — config, not code.
//
// Events:
//   consumes  session-started, config-applied   offer tools + describe the target
//             tool-call (async)                  answer calls for our tool names
//   emits     tool-result                        what the model sees
//             workspace-change                   a file edit, with a `diff` UI hint
//             workspace-status                   target lifecycle (booting, ready, failed)
import { definePlugin, offerTools } from './agentmod.js';
import { workspaceTools } from './workspace-tools.js';

/**
 * @param {object} spec
 * @param {object} spec.manifest     name, version, description, config_schema
 * @param {(config) => object} spec.createTarget   build the execution target (may boot lazily)
 * @param {(config) => string} spec.root           workspace root for this config
 * @param {(config) => string} spec.describe       system-prompt text describing the target
 * @param {Function} [spec.validate]  handshake validation (throw to refuse)
 */
export function defineWorkspacePlugin(spec) {
  let state = null; // { key, target, tools } for the current plugin config

  function current(config) {
    const key = JSON.stringify(config ?? {});
    if (!state || state.key !== key) {
      const target = spec.createTarget(config ?? {});
      const tools = workspaceTools({
        target,
        root: spec.root(config ?? {}),
        limits: config?.limits || {},
        importRepos: config?.import_repos !== false,
      });
      state = { key, target, tools };
    }
    return state;
  }

  function offer(ctx) {
    const { tools } = current(ctx.config);
    offerTools(ctx, tools.specs);
    const note = spec.describe(ctx.config ?? {});
    if (note && !ctx.slot('system').includes(note)) ctx.add('system', note);
  }

  return definePlugin({
    manifest: {
      ...spec.manifest,
      consumes: [
        { event: 'session-started' },
        { event: 'config-applied' },
        { event: 'tool-call', demands: ['call_id', 'name', 'args'], mode: 'async', context: false },
      ],
      emits: [
        { event: 'tool-result', supplies: ['call_id', 'name', 'output'] },
        { event: 'workspace-change', supplies: ['call_id', 'path', 'unified'] },
        { event: 'workspace-status', supplies: ['state', 'message'] },
      ],
    },
    validate: spec.validate,
    shutdown: async () => {
      await state?.target?.dispose?.();
    },
    handlers: {
      'session-started': offer,
      'config-applied': offer,
      'tool-call': async (ctx) => {
        const { call_id, name, args } = ctx.payload;
        const { target, tools } = current(ctx.config);
        if (!tools.names.has(name)) return;
        const status = (st, message) =>
          ctx.publish('workspace-status', { state: st, message, call_id }, { ui: { v: 1, kind: 'progress', label: message } });
        let result;
        try {
          if (target.ensureReady) await target.ensureReady({ status, signal: ctx.signal });
          result = await tools.call(name, args || {}, { signal: ctx.signal, progress: (m) => status('working', m) });
        } catch (e) {
          if (ctx.signal.aborted) throw e;
          result = { output: e?.message || String(e), error: true, summary: 'unavailable' };
        }
        if (result.diff?.unified) {
          await ctx.publish('workspace-change', { call_id, path: result.diff.path, unified: result.diff.unified }, {
            ui: { v: 1, kind: 'diff', path: result.diff.path, unified: result.diff.unified },
          });
        }
        await ctx.publish('tool-result', { call_id, name, output: result.output, error: !!result.error }, {
          ui: { v: 1, kind: 'tool', name, call_id, args, status: result.error ? 'error' : 'done', result: result.summary },
        });
      },
    },
  });
}
