// subagent: delegation to isolated child agents.
//
// `delegate` starts a peer session (cause: this invocation) from a configured
// definition — the model, tools, and plugins of the child are configuration,
// never decided here. The child receives only the task, the context the parent
// chose to pass, and a `delegation` record (tool allowlist, workspace choice,
// mode, budget) that the child's own plugins enforce: policy (allowlist, mode,
// no secrets unless named), budget (bounds), the workspace plugin (isolated
// worktree), subagent-reporter (evidence back). Nothing is inherited by
// default: no memory, no conversation, no secrets.
//
// The child's full transcript stays in its own log; the parent gets a concise
// conclusion plus evidence. `subagents` lists children and cancels one; a hard
// stop of the parent cascades to its running children.
import { definePlugin, declareTools, offerTools, ownTools } from '../sdk/agentmod.js';
import { DELEGATE, SUBAGENTS, CHILD_DEFAULT_TOOLS, READ_ONLY_TOOLS, childReport } from '../sdk/delegation.js';

const TOOLS = [DELEGATE, SUBAGENTS];
let plugin;

async function childrenOf(host, sessionId) {
  const all = await host.query('sessions');
  return all.filter((s) => s.parent === sessionId);
}

definePlugin({
  manifest: {
    name: 'subagent',
    version: '0.2.0',
    description: 'delegate / subagents: isolated child agents with scoped tools, workspaces, and budgets.',
    consumes: [
      { event: 'session-started' },
      { event: 'config-applied' },
      ownTools(TOOLS.map((t) => t.name)),
      { event: 'subagent-result', demands: ['call_id', 'text'], mode: 'async', context: false },
    ],
    tools: declareTools(TOOLS),
    emits: [
      { event: 'user-message', supplies: ['text', 'parent'] },
      { event: 'subagent-started', supplies: ['call_id', 'child'] },
      { event: 'tool-result', supplies: ['call_id', 'name', 'output'] },
    ],
    capabilities: ['start-session', 'control', 'observe'],
    config_schema: { definition: 'session definition for children (default "worker")', default_tools: CHILD_DEFAULT_TOOLS, max_steps: 30, max_children: 8 },
  },
  init: async (p) => {
    plugin = p;
    // Cascade hard stops from parents to their running children.
    try { await p.host.watch(); } catch (e) { p.log('watch unavailable; no hard-stop cascade:', e.message); }
  },
  onRecord: async (record) => {
    if (record.type !== 'dispatcher-command' || record.command !== 'hard-stop' || !plugin) return;
    try {
      for (const c of await childrenOf(plugin.host, record.session_id)) {
        if (c.activity !== 'halted') await plugin.host.command(c.session_id, 'hard-stop');
      }
    } catch (e) {
      plugin.log('cascade failed:', e.message);
    }
  },
  handlers: {
    'session-started': (ctx) => offerTools(ctx, TOOLS),
    'config-applied': (ctx) => offerTools(ctx, TOOLS),
    'tool-call': async (ctx) => {
      const { call_id, name, args } = ctx.payload;
      if (name === 'subagents') {
        const kids = await childrenOf(ctx.host, ctx.sessionId);
        if (args?.action === 'cancel') {
          const kid = kids.find((k) => k.session_id === args.session);
          if (!kid) {
            await ctx.publish('tool-result', { call_id, name, output: `${args.session} is not a child of this session`, error: true });
            return;
          }
          await ctx.host.command(kid.session_id, 'hard-stop');
          // Answer the delegate call that is waiting on it, if any.
          const view = await ctx.host.query('session', { session_id: ctx.sessionId });
          const started = view.events.find((e) => e.event_name === 'subagent-started' && e.payload.child === kid.session_id);
          const answered = started && view.events.some((e) => e.event_name === 'tool-result' && e.payload.call_id === started.payload.call_id);
          if (started && !answered) await ctx.publish('tool-result', { call_id: started.payload.call_id, name: 'delegate', output: `Child ${kid.session_id} was cancelled before it finished. Its work so far is in its session log${started.payload.workspace === 'isolated' ? ' and isolated workspace' : ''}.`, error: true, child: kid.session_id, cancelled: true });
          await ctx.publish('tool-result', { call_id, name, output: `Cancelled ${kid.session_id} (hard stop: in-flight work was terminated; effects that already happened are not undone).` });
          return;
        }
        const out = kids.length ? kids.map((k) => `${k.session_id} [${k.definition}] ${k.activity}${k.title ? ` — ${k.title}` : ''}`).join('\n') : 'No child agents.';
        await ctx.publish('tool-result', { call_id, name, output: out });
        return;
      }
      if (name !== 'delegate') return;
      const task = String(args?.task ?? '').trim();
      if (!task) {
        await ctx.publish('tool-result', { call_id, name, output: '`task` is required', error: true });
        return;
      }
      const cfg = ctx.config || {};
      const kids = await childrenOf(ctx.host, ctx.sessionId);
      const running = kids.filter((k) => k.activity === 'running' || k.activity === 'idle').length;
      if (running >= (cfg.max_children || 8)) {
        await ctx.publish('tool-result', { call_id, name, output: `Too many child agents (${running} active, limit ${cfg.max_children || 8}); wait for some to finish.`, error: true });
        return;
      }
      const readOnly = !!args?.read_only;
      const base = Array.isArray(args?.tools) && args.tools.length ? args.tools.map(String) : cfg.default_tools || CHILD_DEFAULT_TOOLS;
      const workspace = ['isolated', 'shared', 'none'].includes(args?.workspace) ? args.workspace : 'isolated';
      const WORKSPACE_TOOLS = ['shell', 'process', 'read_file', 'list_dir', 'search_files', 'search_text', 'apply_patch', 'view_image', 'repo_map', 'checkpoints'];
      let tools = readOnly ? base.filter((t) => READ_ONLY_TOOLS.includes(t)) : base;
      if (workspace === 'none') tools = tools.filter((t) => !WORKSPACE_TOOLS.includes(t));
      const delegation = {
        parent_session: ctx.sessionId,
        call_id,
        tools,
        workspace,
        mode: readOnly ? 'read-only' : undefined,
        budget: { max_model_requests: Math.min(Number(args?.max_steps) || cfg.max_steps || 30, cfg.max_steps || 30) },
      };
      const text = args?.context ? `${task}\n\nContext from the delegating agent:\n${String(args.context)}` : task;
      const { session_id } = await ctx.host.startSession({
        definition: cfg.definition || 'worker',
        invocationId: ctx.invocationId,
        initial: {
          event_name: 'user-message',
          payload: { text, parent: { session_id: ctx.sessionId, call_id }, delegation },
          ui: { v: 1, kind: 'text', role: 'user', text },
        },
      });
      await ctx.publish('subagent-started', { call_id, child: session_id, definition: cfg.definition || 'worker', workspace, tools }, { ui: { v: 1, kind: 'progress', label: `Delegated to ${session_id}: ${task.slice(0, 80)}`, session: session_id } });
    },
    'subagent-result': async (ctx) => {
      const p = ctx.payload;
      await ctx.publish('tool-result', { call_id: p.call_id, name: 'delegate', output: childReport(p.worker, p.text, p.evidence), child: p.worker, evidence: p.evidence, trust: 'external' }, {
        ui: { v: 1, kind: 'tool', name: 'delegate', call_id: p.call_id, status: 'done', result: `${p.worker} finished` },
      });
    },
  },
});
