// A plugin that answers the coding tools against one execution target.
//
// `linux-sandbox` and `local-workspace` are both this plugin with a different
// target, like `openrouter-model` and `openai-model` share openai-compat.js.
// Which one a session uses is decided by its definition — config, not code.
//
// Workspaces are per session. A session works in the configured root unless:
//   * it is a delegated child asking for an `isolated` workspace, or
//   * it is a branch (`fork_of`) and `fork_workspace` is "isolated" (default);
// then it gets its own worktree under the parent root's state directory,
// materialized from the parent's checkpoint at the delegation/fork point.
// The choice is recorded in the session's `workspace` context slot (so it is
// replayable and visible) and announced with a `workspace-info` event.
//
// Events:
//   consumes  session-started, config-applied   offer tools
//             tool-call (async)                  answer calls for our tool names
//             ui-action (async)                  "restore" on a checkpoint event
//   emits     tool-result                        what the model sees
//             workspace-info                     root, environment, git state
//             workspace-change                   a file edit, with a `diff` hint
//             checkpoint-created / workspace-restored
//             process-started / process-exited
//             diagnostics                        typed compiler/test findings
//             workspace-status                   target lifecycle (booting, ready, failed)
import { declareTools, definePlugin, offerTools, ownTools } from './agentmod.js';
import { codingToolkit, toolSpecs } from './coding/toolkit.js';
import { normalizePath, shq } from './coding/paths.js';

const WORKSPACE_EVENTS = ['workspace-info', 'workspace-change', 'checkpoint-created', 'workspace-restored', 'process-started', 'process-exited', 'diagnostics', 'workspace-status'];

/**
 * @param {object} spec
 * @param {object} spec.manifest     name, version, description, config_schema
 * @param {(config, { host }) => object} spec.createTarget   build the execution target (may boot lazily)
 * @param {(config) => string} spec.root           default workspace root for this config
 * @param {(config) => string} spec.describe       system-prompt text describing the target
 * @param {(config) => object} [spec.secrets]      resolve configured secrets to { NAME: { value, env, commands } }
 * @param {Function} [spec.validate]  handshake validation (throw to refuse)
 * @param {boolean} [spec.lifecycle]   the target is a machine with sandbox_* lifecycle tools
 * @param {(config) => string} [spec.network]  how commands reach the network (for the model)
 */
export function defineWorkspacePlugin(spec) {
  let state = null; // { key, target } for the current plugin config
  const kits = new Map(); // root -> toolkit
  const sessions = new Map(); // session -> Promise<{ root, mode, base, gitDir }>
  const announced = new Set(); // sessions whose workspace-info / instructions were published

  function current(ctx) {
    const config = ctx.config ?? {};
    const key = JSON.stringify(config);
    if (!state || state.key !== key) {
      state = { key, config, target: spec.createTarget(config, { host: ctx.host }) };
      kits.clear();
    }
    return state;
  }

  function kit(ctx, root, gitDir) {
    const { target, config } = current(ctx);
    const k = `${root}|${gitDir || ''}`;
    if (!kits.has(k)) {
      kits.set(k, codingToolkit({
        target,
        root,
        limits: config.limits || {},
        importRepos: config.import_repos !== false,
        env: config.env || {},
        secrets: spec.secrets?.(config) || {},
        diagnostics: config.diagnostics || {},
        checkpoints: config.checkpoints !== false,
        shadowGitDir: gitDir,
      }));
    }
    return kits.get(k);
  }

  const defaultRoot = (ctx) => normalizePath(spec.root(ctx.config ?? {}));

  /** Where checkpoint state lives for a root (worktrees share their parent's store). */
  const storeOf = (root) => `${root}/.agentmod/state/shadow.git`;

  /** The tree a session's workspace was in at log position `sequence`. */
  async function treeAt(ctx, sessionId, sequence, root) {
    const view = await ctx.host.query('session', { session_id: sessionId });
    // World state at N = the "before" checkpoint of the first mutation after N.
    for (const e of view.events) {
      if (e.sequence <= sequence) continue;
      const cp = e.event_name === 'checkpoint-created' ? e.payload.checkpoint : e.event_name === 'tool-result' ? e.payload.checkpoint : null;
      if (cp) return kit(ctx, root).checkpoints.treeOf(cp);
    }
    return (await kit(ctx, root).checkpoint('branch point (current state)', { session: sessionId })).tree;
  }

  async function parentRootOf(ctx, sessionId) {
    if (!sessionId) return defaultRoot(ctx);
    const ws = (await ctx.host.query('context', { session_id: sessionId })).filter((c) => c.slot === 'workspace').pop()?.value;
    return ws?.root ? normalizePath(ws.root) : defaultRoot(ctx);
  }

  async function isolate(ctx, sessionId, parentRoot, baseTree, from) {
    const dir = `${parentRoot}/.agentmod/state/worktrees/${sessionId}`;
    const parentKit = kit(ctx, parentRoot);
    const exists = await parentKit.runner.run(`[ -f ${shq(`${dir}/.agentmod/state/base`)} ] && cat ${shq(`${dir}/.agentmod/state/base`)}`, { cwd: '/', timeoutMs: 30_000 });
    if (exists.exitCode === 0 && exists.stdout.trim()) {
      return { root: dir, mode: 'isolated', base: { ...from, tree: exists.stdout.trim() }, gitDir: storeOf(parentRoot) };
    }
    await parentKit.checkpoints.materialize(baseTree, dir);
    await parentKit.runner.must(`mkdir -p ${shq(`${dir}/.agentmod/state`)} && printf '*\\n' > ${shq(`${dir}/.agentmod/state/.gitignore`)} && printf '%s\\n' ${shq(baseTree)} > ${shq(`${dir}/.agentmod/state/base`)}`, { cwd: '/', timeoutMs: 30_000, what: 'worktree' });
    return { root: dir, mode: 'isolated', base: { ...from, tree: baseTree }, gitDir: storeOf(parentRoot) };
  }

  /** Resolve (once per session) which workspace a session works in. */
  function workspaceOf(ctx) {
    const sid = ctx.sessionId;
    if (!sessions.has(sid)) {
      const p = (async () => {
        const recorded = (await ctx.host.query('context', { session_id: sid })).filter((c) => c.slot === 'workspace').pop()?.value;
        if (recorded?.root && recorded.session === sid) {
          return { root: normalizePath(recorded.root), mode: recorded.mode, base: recorded.base, gitDir: recorded.git_dir || undefined, recorded: true };
        }
        const view = await ctx.host.query('session', { session_id: sid });
        const first = view.events.find((e) => e.event_name === 'user-message');
        const delegation = first?.payload?.delegation;
        const started = view.events.find((e) => e.event_name === 'session-started')?.payload || {};
        const cfg = ctx.config ?? {};
        if (delegation && delegation.workspace && delegation.workspace !== 'none') {
          const parent = delegation.parent_session || started.parent_session;
          const parentRoot = await parentRootOf(ctx, parent);
          if (delegation.workspace === 'shared') return { root: parentRoot, mode: 'shared', base: { session: parent } };
          const snap = await kit(ctx, parentRoot).checkpoint('delegation point', { session: parent, call_id: delegation.call_id });
          if (!snap?.tree) throw new Error(`cannot isolate the child workspace: ${snap?.error || 'no checkpoint'}`);
          return isolate(ctx, sid, parentRoot, snap.tree, { session: parent, checkpoint: snap.checkpoint });
        }
        if (started.fork_of && (cfg.fork_workspace ?? 'isolated') === 'isolated') {
          const src = started.fork_of.session_id;
          const parentRoot = await parentRootOf(ctx, src);
          const tree = await treeAt(ctx, src, started.fork_of.sequence, parentRoot);
          return isolate(ctx, sid, parentRoot, tree, { session: src, sequence: started.fork_of.sequence });
        }
        if (started.fork_of) return { root: await parentRootOf(ctx, started.fork_of.session_id), mode: 'shared', base: { session: started.fork_of.session_id } };
        return { root: defaultRoot(ctx), mode: 'primary', base: null };
      })();
      sessions.set(sid, p);
      p.catch(() => sessions.delete(sid));
    }
    return sessions.get(sid);
  }

  function offer(ctx) {
    const tk = kit(ctx, defaultRoot(ctx));
    offerTools(ctx, tk.specs);
    offerTools(ctx, [ADOPT_SPEC, LOAD_SKILL_SPEC]);
    const note = spec.describe(ctx.config ?? {});
    if (note && !ctx.slot('system').includes(note)) ctx.add('system', note);
  }

  /** First tool call in a session: record the workspace, announce it, read project instructions. */
  async function announce(ctx, ws, tk) {
    if (announced.has(ctx.sessionId)) return;
    announced.add(ctx.sessionId);
    if (!ws.recorded) {
      ctx.add('workspace', { session: ctx.sessionId, root: ws.root, mode: ws.mode, base: ws.base, git_dir: ws.gitDir || storeOf(ws.root) });
    }
    try {
      // Environment facts once per session (OS, architecture, installed
      // tools): the model should not spend turns probing for them.
      if (!ctx.slot('environment').some((e) => e?.root === ws.root)) {
        try {
          const env = await tk.environment();
          ctx.add('environment', { root: ws.root, ...env, network: spec.network?.(ctx.config ?? {}) ?? null });
        } catch (e) {
          ctx.log('environment probe failed:', e.message);
        }
      }
      const info = await tk.info();
      await ctx.publish('workspace-info', { ...info, mode: ws.mode, base: ws.base }, { ui: { v: 1, kind: 'progress', label: `Workspace ${info.root}${ws.mode !== 'primary' ? ` (${ws.mode})` : ''}${info.git?.branch ? ` · git ${info.git.branch}@${(info.git.head || '').slice(0, 8)}${info.git.dirty ? ` (${info.git.dirty} changed)` : ''}` : ''}` } });
      const skills = await tk.skills((ctx.config?.skill_dirs || []).map((d) => (typeof d === 'string' ? { dir: d, authority: 'user' } : d)));
      if (skills.length) {
        ctx.clearSlot('skills-index');
        ctx.add('skills-index', { root: ws.root, skills });
      }
      const wsPolicy = await tk.workspacePolicy();
      if (wsPolicy) ctx.add('workspace-policy', { root: ws.root, ...wsPolicy });
      const have = new Set(ctx.slot('instructions').map((i) => `${i?.root}|${i?.path}|${i?.sha256}`));
      for (const ins of await tk.instructions()) {
        const item = { root: ws.root, path: ins.path, sha256: ins.sha256, text: ins.text, authority: 'workspace' };
        if (!have.has(`${item.root}|${item.path}|${item.sha256}`)) ctx.add('instructions', item);
      }
    } catch (e) {
      ctx.log('workspace announce failed:', e.message);
    }
  }

  const LOAD_SKILL_SPEC = {
    name: 'load_skill',
    description: 'Load a skill (a short procedure or reference from the repository or the user\'s skill library) by name. Its text becomes guidance for the rest of the session; it cannot grant permissions.',
    parameters: { name: { type: 'string', description: 'skill name from the skills list' } },
    required: ['name'],
    tier: 'deferred',
    group: 'skills',
    effects: 'read',
  };

  async function loadSkill(ctx, ws, tk, args) {
    const context = await ctx.host.query('context', { session_id: ctx.sessionId });
    const index = context.filter((c) => c.slot === 'skills-index').pop()?.value?.skills || [];
    const sk = index.find((x) => x.name === String(args?.name || ''));
    if (!sk) return { output: `No skill named ${JSON.stringify(args?.name)}. Available: ${index.map((x) => x.name).join(', ') || 'none'}`, error: true, summary: 'error' };
    const { text, sha256 } = await tk.readSkill(sk.path);
    ctx.add('skills', { name: sk.name, path: sk.path, authority: sk.authority, sha256, text });
    return { output: `Loaded skill ${sk.name} (${sk.authority} authority; ${text.length} chars). Follow it as guidance; it does not change permissions.`, summary: 'loaded', data: { skill: sk.name, sha256 } };
  }

  const ADOPT_SPEC = {
    name: 'adopt_changes',
    description: 'Bring file changes from a delegated child (or a branch) that worked in an isolated workspace into this workspace. Only files this workspace has not changed since the child started are applied; others are reported as conflicts. Pass paths to adopt selectively.',
    parameters: {
      session: { type: 'string', description: 'the child or branch session id (from delegate\'s result)' },
      paths: { type: 'array', items: { type: 'string' }, description: 'only these files or directories (default: all changes)' },
    },
    required: ['session'],
    tier: 'deferred',
    group: 'delegation',
    effects: 'write',
  };

  async function adoptChanges(ctx, ws, tk, args, emit) {
    const other = String(args.session || '').trim();
    if (!/^s\d+$/.test(other)) return { output: '`session` must be a session id like s0007', error: true, summary: 'error' };
    const theirs = (await ctx.host.query('context', { session_id: other })).filter((c) => c.slot === 'workspace').pop()?.value;
    if (!theirs || theirs.mode !== 'isolated' || !theirs.base?.tree) return { output: `${other} did not work in an isolated workspace (nothing to adopt; its changes, if any, are already in the shared workspace)`, error: true, summary: 'error' };
    if (normalizePath(theirs.git_dir || '') !== normalizePath(tk.shadowGitDir)) {
      return { output: `${other}'s workspace was not branched from this workspace (${theirs.git_dir} vs ${tk.shadowGitDir}); adopt from its parent instead`, error: true, summary: 'error' };
    }
    const otherKit = kit(ctx, normalizePath(theirs.root), theirs.git_dir);
    const snap = await otherKit.checkpoints.snapshot({ reason: `adopted into ${ctx.sessionId}`, meta: { session: other } });
    const paths = Array.isArray(args.paths) ? args.paths.map((p) => String(p).replace(/^\/+/, '').replace(/^\.\//, '')) : [];
    const res = await tk.adopt({ baseTree: theirs.base.tree, otherTree: snap.tree, paths, session: ctx.sessionId, call_id: ctx.payload.call_id, emit, signal: ctx.signal });
    const lines = [`Adopted ${res.adopted.length} file change${res.adopted.length === 1 ? '' : 's'} from ${other}${res.checkpoint ? ` (checkpoint before: ${res.checkpoint.slice(0, 12)})` : ''}.`];
    for (const a of res.adopted) lines.push(`  ${a.status === 'D' ? 'deleted' : a.status === 'A' ? 'added' : 'updated'} ${a.path}`);
    if (res.conflicts.length) lines.push(`Conflicts (changed here since ${other} started; not applied): ${res.conflicts.join(', ')}`);
    return { output: lines.join('\n'), error: res.adopted.length === 0 && res.conflicts.length > 0, summary: `${res.adopted.length} adopted`, data: { adopted: res.adopted, conflicts: res.conflicts, checkpoint: res.checkpoint, from: other, from_tree: snap.tree } };
  }

  // The tools this plugin owns, from config alone (compile-time ownership).
  const declared = (cfg) => [
    ...toolSpecs({ root: normalizePath(spec.root(cfg || {})), lifecycle: !!spec.lifecycle, importRepos: (cfg || {}).import_repos !== false }),
    ADOPT_SPEC,
    LOAD_SKILL_SPEC,
  ];

  return definePlugin({
    name: spec.manifest.name,
    manifest: (cfg) => ({
      ...spec.manifest,
      consumes: [
        { event: 'session-started' },
        { event: 'config-applied' },
        ownTools(declared(cfg).map((t) => t.name)),
        { event: 'ui-action', demands: ['reply_to', 'action'], mode: 'async', context: false },
      ],
      tools: declareTools(declared(cfg)),
      emits: [
        { event: 'tool-result', supplies: ['call_id', 'name', 'output'] },
        ...WORKSPACE_EVENTS.map((event) => ({ event, supplies: [] })),
      ],
    }),
    validate: spec.validate,
    shutdown: async () => {
      await state?.target?.dispose?.();
    },
    handlers: {
      'session-started': offer,
      'config-applied': offer,
      'tool-call': async (ctx) => {
        const { call_id, name, args } = ctx.payload;
        const { target } = current(ctx);
        const base = kit(ctx, defaultRoot(ctx));
        if (!base.names.has(name) && name !== 'adopt_changes' && name !== 'load_skill') return;
        const emit = (event, payload, ui) => ctx.publish(event, payload, ui ? { ui } : {});
        const status = (st, message) => ctx.publish('workspace-status', { state: st, message, call_id }, { ui: { v: 1, kind: 'progress', label: message } });
        let result;
        const t0 = Date.now();
        try {
          // Lifecycle tools manage the machine themselves; everything else needs it up.
          if (target.ensureReady && !base.lifecycleNames.has(name)) await target.ensureReady({ status, signal: ctx.signal });
          const ws = await workspaceOf(ctx);
          const tk = kit(ctx, ws.root, ws.gitDir);
          await announce(ctx, ws, tk);
          if (name === 'adopt_changes') result = await adoptChanges(ctx, ws, tk, args || {}, emit);
          else if (name === 'load_skill') result = await loadSkill(ctx, ws, tk, args || {});
          else result = await tk.call(name, args || {}, { signal: ctx.signal, progress: (m) => status('working', m), session: ctx.sessionId, call_id, emit });
        } catch (e) {
          if (ctx.signal.aborted) throw e;
          result = { output: e?.message || String(e), error: true, summary: 'unavailable' };
        }
        const data = result.data || {};
        await ctx.publish('tool-result', {
          call_id,
          name,
          output: result.output,
          error: !!result.error,
          ...data,
          duration_ms: data.duration_ms ?? Date.now() - t0,
          ...(result.attachments ? { attachments: result.attachments } : {}),
        }, {
          ui: { v: 1, kind: 'tool', name, call_id, args, status: result.error ? 'error' : 'done', result: result.summary },
        });
      },
      'ui-action': async (ctx) => {
        const { reply_to, action } = ctx.payload;
        if (action !== 'restore' && action !== 'kill') return;
        const ev = await ctx.host.query('event', { session_id: ctx.sessionId, event_id: reply_to });
        if (action === 'kill') {
          // "stop" on a process from the UI: the user's own action, recorded as the ui-action.
          if (ev?.event_name !== 'process-started') return;
          const ws = await workspaceOf(ctx);
          const tk = kit(ctx, ws.root, ws.gitDir);
          const emit = (event, payload, ui) => ctx.publish(event, { ...payload, ...(event === 'process-exited' ? { by: 'user', request: reply_to } : {}) }, ui ? { ui } : {});
          await tk.call('process', { action: 'kill', id: ev.payload.process_id }, { signal: ctx.signal, session: ctx.sessionId, call_id: `ui:${reply_to}`, emit });
          return;
        }
        const cp = ev?.event_name === 'checkpoint-created' ? ev.payload.checkpoint : ev?.payload?.checkpoint;
        if (!cp) return;
        const ws = await workspaceOf(ctx);
        const tk = kit(ctx, ws.root, ws.gitDir);
        // Idempotence: one restore per request event.
        const view = await ctx.host.query('session', { session_id: ctx.sessionId });
        if (view.events.some((e) => e.event_name === 'workspace-restored' && e.payload.request === reply_to)) return;
        const emit = (event, payload, ui) => ctx.publish(event, { ...payload, ...(event === 'workspace-restored' ? { request: reply_to, by: 'user' } : {}) }, ui ? { ui } : {});
        const r = await tk.call('checkpoints', { action: 'restore', checkpoint: cp }, { signal: ctx.signal, session: ctx.sessionId, call_id: `ui:${reply_to}`, emit });
        if (r.error) await ctx.publish('workspace-status', { state: 'failed', message: r.output }, { ui: { v: 1, kind: 'progress', label: r.output } });
      },
    },
  });
}
