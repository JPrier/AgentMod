// memory: remember/recall tools plus memory *injection* into fresh sessions.
// Notes live in a store the plugin owns (a JSON file natively, in-memory in the
// browser) — the core has no memory feature. Several memory plugins can run at once.
import { definePlugin, offerTools, toolSpec } from '../sdk/agentmod.js';

const TOOLS = [
  toolSpec('remember', 'Save a note that future sessions will see.', { note: { type: 'string' } }),
  toolSpec('recall', 'List remembered notes.', {}),
];

definePlugin({
  manifest: {
    name: 'memory',
    version: '0.1.0',
    description: 'Persistent notes injected into every new session; remember/recall tools.',
    consumes: [
      { event: 'session-started' },
      { event: 'config-applied' },
      { event: 'tool-call', demands: ['call_id', 'name', 'args'], mode: 'async', context: false },
    ],
    emits: [{ event: 'tool-result', supplies: ['call_id', 'name', 'output'] }],
  },
  handlers: {
    'session-started': (ctx) => {
      offerTools(ctx, TOOLS);
      for (const note of ctx.storage.get('notes', [])) ctx.add('memory', note);
    },
    'config-applied': (ctx) => offerTools(ctx, TOOLS),
    'tool-call': async (ctx) => {
      const { name, call_id, args } = ctx.payload;
      if (name === 'remember') {
        const note = String(args?.note ?? '').trim();
        const notes = ctx.storage.get('notes', []);
        if (note && !notes.includes(note)) ctx.storage.set('notes', [...notes, note]);
        ctx.add('memory', note);
        await ctx.publish('tool-result', { call_id, name, output: `Saved: “${note}”` }, { ui: { v: 1, kind: 'tool', name, call_id, status: 'done', result: 'saved' } });
      } else if (name === 'recall') {
        const notes = ctx.storage.get('notes', []);
        await ctx.publish('tool-result', { call_id, name, output: notes.length ? notes.join(' | ') : '(no notes yet)' });
      }
    },
  },
});
