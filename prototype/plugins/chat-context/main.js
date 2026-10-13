// chat-context: the bundled chat convention. Folds user, assistant, and tool
// messages into the `messages` context section and drives the agent loop:
//   user-message -> model-request -> model-response -> (tool-call* -> tool-result*)* -> assistant-message
// It keeps no in-memory state: everything it needs is in the delivered context.
//
// A session starts with the core's `session-started`, which carries the
// start-session input (compiler.md §9): its text is the first user message
// (or a delegated task), so this plugin turns it into the first model request.
//
// Messages carry their authority (sdk/projection.js): a user message from a
// frontend is `user`; a task handed down by a delegating agent is `agent`.
// Tool results keep compact metadata (exit code, checkpoint, trust) and any
// attachments (images), so the projection can label and bound them.
//
// Tool calls are the `tool-call/*` event family (compiler.md §3.4). This
// plugin is written before any tool is known, so it declares the family; the
// compiler resolves it into exactly the members the definition's plugins
// subscribe to, and the runtime sends this plugin that resolved emit set. The
// set is the session's tools: at session start and after a config apply this
// plugin writes the `tools` context section from it (name = member name,
// argument schema = the member subscriber's `receives`), which is what the
// model is offered and what tool_search searches.
//
// A tool call that cannot be published — a name outside the resolved set ("no
// such tool"), malformed JSON arguments, or arguments that do not fit the
// member's shape — is answered here, inline: the tool message goes straight
// into context and a `tool-call-rejected` event records it, so the loop never
// waits for an answer that will not come. Unknown names are never published:
// publishing outside the resolved set would be an undeclared emit. (This
// plugin consumes `tool-result`, so it never emits one: a plugin may not
// consume its own emits.) After dispatch, a failed invocation reaches this
// plugin as the core's `invocation-failed` and is answered the same way.
//
// Approvals: a gate (policy, approval-gate) vetoes a call and later publishes
// `approval-granted`; this plugin re-publishes the call with `approved`, and the
// gate checks the grant on its way through.
//
// Batch settlement: every tool call of one assistant turn is answered before
// the next model request, which is published exactly once — however many calls
// there were and in whatever order their results arrive.
import { definePlugin, key, optional, withContext, START_INPUT, TOOL_CALL_KEYS } from '../sdk/agentmod.js';
import { describeParams, suggestTools } from '../sdk/toolargs.js';
import { conformArgs, toolOfMember } from '../sdk/toolschema.js';

const msgs = (ctx) => ctx.context.filter((c) => c.section === 'messages');
const META_KEYS = ['exit_code', 'timed_out', 'duration_ms', 'truncated', 'checkpoint', 'process_id', 'state', 'sha256', 'diagnostics', 'read_only', 'secrets_used'];
const CONTRIBUTES = ['contribute-context'];

/** Answer a call inline (rejected before dispatch, or dispatch failed). */
async function answerInline(ctx, call, output, extra = {}) {
  ctx.add('messages', { role: 'tool', call_id: call.call_id, name: call.name, content: output, error: true });
  await ctx.publish('tool-call-rejected', { call_id: call.call_id, name: call.name, output, error: true, ...extra }, { ui: { v: 1, kind: 'tool', name: call.name, call_id: call.call_id, status: 'error', result: extra.dispatch || 'rejected' } });
}

const TOOL_PREFIX = 'tool-call/';

/** The resolved tool members of this invocation's revision, by tool name. */
const toolMembers = (ctx) => new Map(ctx.emits.filter((e) => e.event.startsWith(TOOL_PREFIX)).map((e) => [e.event.slice(TOOL_PREFIX.length), e]));

/** Write the `tools` section from the resolved emit set (replacing any earlier one). */
function offerTools(ctx) {
  ctx.clearSection('tools');
  for (const member of toolMembers(ctx).values()) ctx.add('tools', toolOfMember(member));
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
      // The session's tools are its resolved `tool-call/*` members: written
      // into context at start and whenever a config apply changes them.
      // The start input's text is the first message: the first model request.
      { event: 'session-started', mode: 'blocking', actions: CONTRIBUTES, receives: { ...START_INPUT } },
      { event: 'config-applied', mode: 'blocking', actions: CONTRIBUTES },
      { event: 'user-message', mode: 'blocking', actions: CONTRIBUTES, receives: { text: key('string'), attachments: optional('array<object>') } },
      { event: 'model-response', mode: 'blocking', actions: CONTRIBUTES, ...withContext({ text: key('string'), tool_calls: optional('array<object>'), stream_id: optional('string') }) },
      // Tools attach metadata of their own (exit codes, checkpoints, …): kept
      // in the tool message untyped, so this subscription takes unlisted keys.
      { event: 'tool-result', mode: 'blocking', actions: CONTRIBUTES, unlisted_keys: true, ...withContext({ call_id: key('string'), name: optional('string'), output: key('string | object'), error: optional('bool') }) },
      { event: 'context-edit', mode: 'blocking', actions: CONTRIBUTES, receives: { ops: key('array<object>') } },
      // Any failed invocation, reported by the core: a crashed tool (or a gate
      // that failed) is answered for the model, so a call never leaves it waiting.
      { event: 'invocation-failed', mode: 'blocking', actions: CONTRIBUTES, ...withContext({ event_name: key('string'), payload: key('object'), plugin: key('string'), mode: key('"blocking" | "async"'), error: key('string') }) },
      { event: 'approval-granted', mode: 'async', receives: { call_id: key('string'), name: key('string'), args: key('object'), approved: key('object') } },
    ],
    emit: [
      { event: 'model-request', keys: { turn: key('string') } },
      // A family: the compiler resolves it into the tools the definition provides.
      { event: 'tool-call/*', keys: { ...TOOL_CALL_KEYS } },
      { event: 'tool-call-rejected', keys: { call_id: key('string'), name: key('string'), output: key('string'), error: key('bool'), dispatch: optional('string') } },
      { event: 'assistant-message', keys: { text: key('string'), stream_id: optional('string') } },
    ],
  },
  handlers: {
    'session-started': async (ctx) => {
      offerTools(ctx);
      const input = ctx.payload.input || {};
      if (typeof input.text !== 'string' || !input.text) return;
      // A task handed down by a delegating agent carries its parent link.
      const authority = input.data?.delegation || input.data?.parent ? 'agent' : 'user';
      ctx.add('messages', { role: 'user', content: input.text, authority, ...(input.attachments?.length ? { attachments: input.attachments } : {}) });
      await ctx.publish('model-request', { turn: ctx.event.event_id });
    },
    'config-applied': (ctx) => offerTools(ctx),
    'user-message': async (ctx) => {
      // The same on either lane (steer or queue).
      const p = ctx.payload;
      ctx.add('messages', { role: 'user', content: p.text, authority: 'user', ...(p.attachments?.length ? { attachments: p.attachments } : {}) });
      await ctx.publish('model-request', { turn: ctx.event.event_id });
    },
    'model-response': async (ctx) => {
      const calls = Array.isArray(ctx.payload.tool_calls) ? ctx.payload.tool_calls : [];
      ctx.add('messages', { role: 'assistant', content: ctx.payload.text, tool_calls: calls.length ? calls.map(({ call_id, name, args }) => ({ call_id, name, args })) : undefined });
      if (calls.length) {
        // Every name the model writes is checked against the resolved emit set.
        const members = toolMembers(ctx);
        const rejected = [];
        for (const call of calls) {
          const member = members.get(call.name);
          let reject = null;
          let args = call.args || {};
          if (call.parse_error) reject = `Invalid call to \`${call.name}\`: ${call.parse_error}. Call it again with a JSON object of arguments.`;
          else if (!member) reject = suggestTools(call.name, [...members.keys()]);
          else {
            const fit = conformArgs(member.keys?.args?.keys, args);
            if (fit.errors.length) {
              const spec = toolOfMember(member);
              reject = `Invalid call to \`${call.name}\`: ${fit.errors.join('; ')}. Its parameters: ${describeParams(spec) || 'none'}.`;
            } else args = fit.args;
          }
          if (reject) {
            await answerInline(ctx, call, reject, member ? {} : { dispatch: 'no-such-tool' });
            rejected.push(call.call_id);
            continue;
          }
          await ctx.publish(`${TOOL_PREFIX}${call.name}`, { call_id: call.call_id, name: call.name, args }, {
            ui: { v: 1, kind: 'tool', name: call.name, args, call_id: call.call_id, status: 'requested' },
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
    'invocation-failed': async (ctx) => {
      const p = ctx.payload;
      if (!p.event_name.startsWith(TOOL_PREFIX)) return;
      const call = p.payload || {};
      if (!call.call_id) return;
      // Only a failure that leaves the call unanswered: the tool itself (a
      // subscriber of the member), or a blocking gate (the pipeline failed, so
      // the tool never ran). An async observer's failure does not stop the call.
      const member = toolMembers(ctx).get(call.name);
      const isTool = (member?.subscribers || []).some((s) => s.plugin === p.plugin);
      if (p.mode !== 'blocking' && !isTool) return;
      const answered = msgs(ctx).some((m) => m.value.role === 'tool' && m.value.call_id === call.call_id);
      if (answered) return; // the tool answered before failing
      const output = `The \`${call.name}\` tool failed before returning a result (${p.plugin}: ${String(p.error || 'error').slice(0, 300)}). Retry once if it looks transient; otherwise take another approach.`;
      await answerInline(ctx, call, output, { dispatch: 'invocation-failed' });
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
