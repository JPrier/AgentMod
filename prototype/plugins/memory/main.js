// memory: remember/recall tools plus memory *injection* into fresh sessions.
// Notes live in a store the plugin owns (a JSON file natively, in-memory in the
// browser) — the core has no memory feature. Several memory plugins can run at once.
//
// Injection is observable, never hidden prompt mutation: each injected note is
// a `memory` context item carrying its provenance (source plugin, when it was
// saved, by which session, why it was injected), and a `memory-injected` event
// records what entered the session. Children started by `delegate` are not
// given memory unless their definition includes this plugin.
import { definePlugin, offerTools, toolSpec } from '../sdk/agentmod.js';

const TOOLS = [
  toolSpec('remember', 'Save a short note that future sessions will see (preferences, conventions, facts about this user or project).', { note: { type: 'string' } }, { required: ['note'], tier: 'deferred', group: 'memory', effects: 'write' }),
  toolSpec('recall', 'List remembered notes.', {}, { tier: 'deferred', group: 'memory', effects: 'read' }),
];

const normalize = (n) => (typeof n === 'string' ? { text: n } : n);

definePlugin({
  manifest: {
    name: 'memory',
    version: '0.2.0',
    description: 'Persistent notes injected into every new session, with provenance; remember/recall tools.',
    consumes: [
      { event: 'session-started' },
      { event: 'config-applied' },
      { event: 'tool-call', demands: ['call_id', 'name', 'args'], mode: 'async', context: false },
    ],
    emits: [
      { event: 'tool-result', supplies: ['call_id', 'name', 'output'] },
      { event: 'memory-injected', supplies: ['count'] },
    ],
  },
  handlers: {
    'session-started': async (ctx) => {
      offerTools(ctx, TOOLS);
      const notes = ctx.storage.get('notes', []).map(normalize);
      for (const n of notes) ctx.add('memory', { text: n.text, source: 'memory', saved_at: n.saved_at ?? null, saved_by: n.session ?? null, reason: 'injected at session start' });
      if (notes.length) await ctx.publish('memory-injected', { count: notes.length, notes: notes.map((n) => n.text.slice(0, 200)) }, { ui: { v: 1, kind: 'progress', label: `${notes.length} remembered note(s) injected` } });
    },
    'config-applied': (ctx) => offerTools(ctx, TOOLS),
    'tool-call': async (ctx) => {
      const { name, call_id, args } = ctx.payload;
      if (name === 'remember') {
        const text = String(args?.note ?? '').trim();
        const notes = ctx.storage.get('notes', []).map(normalize);
        if (text && !notes.some((n) => n.text === text)) ctx.storage.set('notes', [...notes, { text, saved_at: ctx.event.arrived_at, session: ctx.sessionId }]);
        ctx.add('memory', { text, source: 'memory', saved_at: ctx.event.arrived_at, saved_by: ctx.sessionId, reason: 'saved in this session' });
        await ctx.publish('tool-result', { call_id, name, output: `Saved: “${text}”` }, { ui: { v: 1, kind: 'tool', name, call_id, status: 'done', result: 'saved' } });
      } else if (name === 'recall') {
        const notes = ctx.storage.get('notes', []).map(normalize);
        await ctx.publish('tool-result', { call_id, name, output: notes.length ? notes.map((n) => n.text).join(' | ') : '(no notes yet)' });
      }
    },
  },
});
