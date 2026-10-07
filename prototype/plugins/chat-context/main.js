// chat-context: the bundled chat convention. Folds user, assistant, and tool
// messages into the `messages` context slot and drives the agent loop:
//   user-message -> model-request -> model-response -> (tool-call* -> tool-result*)* -> assistant-message
// It keeps no in-memory state: everything it needs is in the delivered context.
import { definePlugin } from '../sdk/agentmod.js';

const msgs = (ctx) => ctx.context.filter((c) => c.slot === 'messages');

definePlugin({
  manifest: {
    name: 'chat-context',
    version: '0.1.0',
    description: 'Chat context assembly and the model/tool loop (a convention, not a core concept).',
    consumes: [
      { event: 'user-message', demands: ['text'] },
      { event: 'model-response', demands: ['text', 'tool_calls?'] },
      { event: 'tool-result', demands: ['call_id', 'output'] },
      { event: 'context-edit', demands: ['ops'] },
    ],
    emits: [
      { event: 'model-request', supplies: ['turn'] },
      { event: 'tool-call', supplies: ['call_id', 'name', 'args'] },
      { event: 'assistant-message', supplies: ['text'] },
    ],
  },
  handlers: {
    'user-message': async (ctx) => {
      ctx.add('messages', { role: 'user', content: ctx.payload.text });
      await ctx.publish('model-request', { turn: ctx.event.event_id });
    },
    'model-response': async (ctx) => {
      const calls = Array.isArray(ctx.payload.tool_calls) ? ctx.payload.tool_calls : [];
      ctx.add('messages', { role: 'assistant', content: ctx.payload.text, tool_calls: calls.length ? calls : undefined });
      if (calls.length) {
        for (const call of calls) {
          await ctx.publish('tool-call', { call_id: call.call_id, name: call.name, args: call.args || {} }, {
            ui: { v: 1, kind: 'tool', name: call.name, args: call.args || {}, call_id: call.call_id, status: 'requested' },
          });
        }
      } else {
        await ctx.publish('assistant-message', { text: ctx.payload.text, stream_id: ctx.payload.stream_id }, {
          ui: { v: 1, kind: 'markdown', role: 'assistant', text: ctx.payload.text, replaces_stream: ctx.payload.stream_id },
        });
      }
    },
    'tool-result': async (ctx) => {
      const { call_id, name, output, error } = ctx.payload;
      ctx.add('messages', { role: 'tool', call_id, name, content: typeof output === 'string' ? output : JSON.stringify(output), error: !!error });
      // Continue the loop once every call of the latest assistant turn is answered.
      const all = msgs(ctx);
      const lastAssistant = [...all].reverse().find((m) => m.value.role === 'assistant');
      const wanted = lastAssistant?.value.tool_calls?.map((c) => c.call_id) ?? [call_id];
      const answered = new Set(all.filter((m) => m.value.role === 'tool').map((m) => m.value.call_id));
      answered.add(call_id);
      if (wanted.every((id) => answered.has(id))) {
        await ctx.publish('model-request', { turn: ctx.event.event_id });
      }
    },
    'context-edit': async (ctx) => {
      // Operator-driven context manipulation (add/replace/remove/clear/restore).
      for (const op of ctx.payload.ops || []) ctx.contribute(op);
    },
  },
});
