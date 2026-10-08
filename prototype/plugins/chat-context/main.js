// chat-context: the bundled chat convention. Folds user, assistant, and tool
// messages into the `messages` context slot and drives the agent loop:
//   user-message -> model-request -> model-response -> (tool-call* -> tool-result*)* -> assistant-message
// It keeps no in-memory state: everything it needs is in the delivered context.
//
// Messages carry their authority (sdk/projection.js): a user message from a
// frontend is `user`; a task handed down by a delegating agent is `agent`.
// Tool results keep compact metadata (exit code, checkpoint, trust) and any
// attachments (images), so the projection can label and bound them.
//
// A tool call that cannot be dispatched — malformed JSON arguments, arguments
// that do not match the tool's schema, or a tool no plugin offers — is
// answered here with an actionable error result, so the loop never waits for
// an answer that will not come. The same holds after dispatch: the kernel
// routes each call to its one compiled owner, and reports `dispatch-failed`
// when no owner exists or the owner's invocation failed without answering.
//
// Batch settlement: every tool call of one assistant turn is answered before
// the next model request, which is published exactly once — however many calls
// there were and in whatever order their results arrive.
import { definePlugin } from '../sdk/agentmod.js';
import { checkArgs, suggestTools } from '../sdk/toolargs.js';

const msgs = (ctx) => ctx.context.filter((c) => c.slot === 'messages');
const META_KEYS = ['exit_code', 'timed_out', 'duration_ms', 'truncated', 'checkpoint', 'process_id', 'state', 'sha256', 'diagnostics', 'read_only', 'secrets_used'];

definePlugin({
  manifest: {
    name: 'chat-context',
    version: '0.2.0',
    description: 'Chat context assembly and the model/tool loop (a convention, not a core concept).',
    consumes: [
      { event: 'user-message', demands: ['text'] },
      { event: 'model-response', demands: ['text', 'tool_calls?'] },
      { event: 'tool-result', demands: ['call_id', 'output'] },
      { event: 'context-edit', demands: ['ops'] },
      { event: 'dispatch-failed', demands: ['reason', 'event_name', 'payload'] },
    ],
    emits: [
      { event: 'model-request', supplies: ['turn'] },
      { event: 'tool-call', supplies: ['call_id', 'name', 'args'] },
      { event: 'tool-result', supplies: ['call_id', 'name', 'output'] },
      { event: 'assistant-message', supplies: ['text'] },
    ],
  },
  handlers: {
    'user-message': async (ctx) => {
      const p = ctx.payload;
      const authority = p.delegation || p.parent ? 'agent' : 'user';
      ctx.add('messages', { role: 'user', content: p.text, authority, ...(p.attachments?.length ? { attachments: p.attachments } : {}) });
      await ctx.publish('model-request', { turn: ctx.event.event_id });
    },
    'model-response': async (ctx) => {
      const calls = Array.isArray(ctx.payload.tool_calls) ? ctx.payload.tool_calls : [];
      ctx.add('messages', { role: 'assistant', content: ctx.payload.text, tool_calls: calls.length ? calls.map(({ call_id, name, args }) => ({ call_id, name, args })) : undefined });
      if (calls.length) {
        const specs = new Map(ctx.slot('tools').filter((t) => t?.name).map((t) => [t.name, t]));
        const offered = new Set(specs.keys());
        for (const call of calls) {
          const reject = call.parse_error ? `Invalid call to \`${call.name}\`: ${call.parse_error}. Call it again with a JSON object of arguments.`
            : !offered.has(call.name) ? suggestTools(call.name, [...offered])
            : checkArgs(specs.get(call.name), call.args || {});
          if (reject) {
            await ctx.publish('tool-result', { call_id: call.call_id, name: call.name, output: reject, error: true }, { ui: { v: 1, kind: 'tool', name: call.name, call_id: call.call_id, status: 'error', result: 'rejected' } });
            continue;
          }
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
      const p = ctx.payload;
      const meta = Object.fromEntries(META_KEYS.filter((k) => p[k] !== undefined && p[k] !== null).map((k) => [k, p[k]]));
      ctx.add('messages', {
        role: 'tool',
        call_id: p.call_id,
        name: p.name,
        content: typeof p.output === 'string' ? p.output : JSON.stringify(p.output),
        error: !!p.error,
        ...(p.trust ? { trust: p.trust } : {}),
        ...(Object.keys(meta).length ? { meta } : {}),
        ...(Array.isArray(p.attachments) && p.attachments.length ? { attachments: p.attachments } : {}),
      });
      // Continue the loop once every call of the latest assistant turn is answered.
      const all = msgs(ctx);
      const lastAssistant = [...all].reverse().find((m) => m.value.role === 'assistant');
      const wanted = lastAssistant?.value.tool_calls?.map((c) => c.call_id) ?? [p.call_id];
      if (!wanted.includes(p.call_id)) return; // a late result for an earlier turn: recorded, no new request
      const answered = new Set(all.filter((m) => m.value.role === 'tool').map((m) => m.value.call_id));
      if (answered.has(p.call_id)) return; // a duplicate answer: recorded, no second request
      answered.add(p.call_id);
      if (wanted.every((id) => answered.has(id))) {
        await ctx.publish('model-request', { turn: ctx.event.event_id });
      }
    },
    'dispatch-failed': async (ctx) => {
      const p = ctx.payload;
      if (p.event_name !== 'tool-call') return;
      const call = p.payload || {};
      if (!call.call_id) return;
      const answered = msgs(ctx).some((m) => m.value.role === 'tool' && m.value.call_id === call.call_id);
      if (answered) return; // the owner answered before failing
      const offered = ctx.slot('tools').map((t) => t?.name).filter(Boolean);
      const output = p.reason === 'owner-failed'
        ? `The \`${call.name}\` tool failed before returning a result (${p.plugin}: ${String(p.error || 'error').slice(0, 300)}). Retry once if it looks transient; otherwise take another approach.`
        : suggestTools(call.name, offered, { routed: p.owners });
      await ctx.publish('tool-result', { call_id: call.call_id, name: call.name, output, error: true, dispatch: p.reason }, { ui: { v: 1, kind: 'tool', name: call.name, call_id: call.call_id, status: 'error', result: p.reason } });
    },
    'context-edit': async (ctx) => {
      // Operator-driven context manipulation (add/replace/remove/clear/restore).
      for (const op of ctx.payload.ops || []) ctx.contribute(op);
    },
  },
});
