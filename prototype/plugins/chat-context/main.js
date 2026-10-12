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
// Tool calls are the `tool-call/<name>` event family, emitted here with
// exactly one handler per member: the compiler routes each member to the one
// plugin that subscribes to it (compiler.md §3.3).
//
// A tool call that cannot be dispatched — malformed JSON arguments, arguments
// that do not match the tool's schema, or a tool no plugin offers — is
// answered here, inline: the tool message goes straight into context and a
// `tool-call-rejected` event records it, so the loop never waits for an answer
// that will not come. (This plugin consumes `tool-result`, so it never emits
// one: a plugin may not consume its own emits.) The same holds after
// dispatch: `dispatch-failed` (a member with no handler, or a handler that
// failed without answering) is answered inline the same way.
//
// Approvals: a gate (policy, approval-gate) vetoes a call and later publishes
// `approval-granted`; this plugin re-publishes the call with `approved`, and the
// gate checks the grant on its way through.
//
// Batch settlement: every tool call of one assistant turn is answered before
// the next model request, which is published exactly once — however many calls
// there were and in whatever order their results arrive.
import { definePlugin, key, optional, withContext, TOOL_CALL_KEYS } from '../sdk/agentmod.js';
import { checkArgs, suggestTools } from '../sdk/toolargs.js';

const msgs = (ctx) => ctx.context.filter((c) => c.slot === 'messages');
const META_KEYS = ['exit_code', 'timed_out', 'duration_ms', 'truncated', 'checkpoint', 'process_id', 'state', 'sha256', 'diagnostics', 'read_only', 'secrets_used'];
const CONTRIBUTES = ['contribute-context'];

/** Answer a call inline (rejected before dispatch, or dispatch failed). */
async function answerInline(ctx, call, output, extra = {}) {
  ctx.add('messages', { role: 'tool', call_id: call.call_id, name: call.name, content: output, error: true });
  await ctx.publish('tool-call-rejected', { call_id: call.call_id, name: call.name, output, error: true, ...extra }, { ui: { v: 1, kind: 'tool', name: call.name, call_id: call.call_id, status: 'error', result: extra.dispatch || 'rejected' } });
}

/** Continue the loop if every call of the latest assistant turn is answered. */
async function settle(ctx, answeredNow, latestCalls) {
  const all = msgs(ctx);
  const lastAssistant = [...all].reverse().find((m) => m.value.role === 'assistant');
  const wanted = latestCalls ?? lastAssistant?.value.tool_calls?.map((c) => c.call_id) ?? [];
  const answered = new Set(all.filter((m) => m.value.role === 'tool').map((m) => m.value.call_id));
  for (const id of answeredNow) answered.add(id);
  if (wanted.length && wanted.every((id) => answered.has(id))) await ctx.publish('model-request', { turn: ctx.event.event_id });
}


definePlugin({
  manifest: {
    name: 'chat-context',
    version: '0.3.0',
    description: 'Chat context assembly and the model/tool loop (a convention, not a core concept).',
    subscribe: [
      { event: 'user-message', mode: 'blocking', actions: CONTRIBUTES, receives: { text: key('string'), delegation: optional('object'), parent: optional('object'), attachments: optional('array<object>') } },
      { event: 'model-response', mode: 'blocking', actions: CONTRIBUTES, receives: withContext({ text: key('string'), tool_calls: optional('array<object>'), stream_id: optional('string') }) },
      // Tools attach metadata of their own (exit codes, checkpoints, …): kept
      // in the tool message untyped, so this subscription takes unlisted keys.
      { event: 'tool-result', mode: 'blocking', actions: CONTRIBUTES, unlisted_keys: true, receives: withContext({ call_id: key('string'), name: optional('string'), output: key('string | object'), error: optional('bool') }) },
      { event: 'context-edit', mode: 'blocking', actions: CONTRIBUTES, receives: { ops: key('array<object>') } },
      { event: 'dispatch-failed', mode: 'blocking', actions: CONTRIBUTES, receives: withContext({ reason: key('string'), event_name: key('string'), member: key('string'), payload: key('object'), plugin: optional('string'), error: optional('string'), handled: optional('array<string>') }) },
      { event: 'approval-granted', mode: 'async', receives: { call_id: key('string'), name: key('string'), args: key('object'), approved: key('object') } },
    ],
    emit: [
      { event: 'model-request', keys: { turn: key('string') } },
      { event: 'tool-call/*', exactly_one_handler: true, keys: { ...TOOL_CALL_KEYS } },
      { event: 'tool-call-rejected', keys: { call_id: key('string'), name: key('string'), output: key('string'), error: key('bool'), dispatch: optional('string') } },
      { event: 'assistant-message', keys: { text: key('string'), stream_id: optional('string') } },
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
        const rejected = [];
        for (const call of calls) {
          const reject = call.parse_error ? `Invalid call to \`${call.name}\`: ${call.parse_error}. Call it again with a JSON object of arguments.`
            : !offered.has(call.name) ? suggestTools(call.name, [...offered])
            : checkArgs(specs.get(call.name), call.args || {});
          if (reject) {
            await answerInline(ctx, call, reject);
            rejected.push(call.call_id);
            continue;
          }
          await ctx.publish(`tool-call/${call.name}`, { call_id: call.call_id, name: call.name, args: call.args || {} }, {
            ui: { v: 1, kind: 'tool', name: call.name, args: call.args || {}, call_id: call.call_id, status: 'requested' },
          });
        }
        // Every call rejected: nothing will answer, so ask the model again now.
        if (rejected.length === calls.length) await ctx.publish('model-request', { turn: ctx.event.event_id });
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
      await settle(ctx, [p.call_id], wanted);
    },
    'dispatch-failed': async (ctx) => {
      const p = ctx.payload;
      if (!p.event_name.startsWith('tool-call/')) return;
      const call = p.payload || {};
      if (!call.call_id) return;
      const answered = msgs(ctx).some((m) => m.value.role === 'tool' && m.value.call_id === call.call_id);
      if (answered) return; // the handler answered before failing
      const offered = ctx.slot('tools').map((t) => t?.name).filter(Boolean);
      const output = p.reason === 'handler-failed'
        ? `The \`${call.name}\` tool failed before returning a result (${p.plugin}: ${String(p.error || 'error').slice(0, 300)}). Retry once if it looks transient; otherwise take another approach.`
        : suggestTools(call.name, offered, { routed: p.handled });
      await answerInline(ctx, call, output, { dispatch: p.reason });
      await settle(ctx, [call.call_id]);
    },
    'approval-granted': async (ctx) => {
      // A gate approved a call it vetoed: re-publish it, citing the grant.
      const p = ctx.payload;
      await ctx.publish(`tool-call/${p.name}`, { call_id: p.call_id, name: p.name, args: p.args || {}, approved: { ...p.approved, grant: ctx.event.event_id } }, {
        ui: { v: 1, kind: 'tool', name: p.name, args: p.args, call_id: p.call_id, status: 'approved' },
      });
    },
    'context-edit': async (ctx) => {
      // Operator-driven context manipulation (add/replace/remove/clear/restore).
      for (const op of ctx.payload.ops || []) ctx.contribute(op);
    },
  },
});
