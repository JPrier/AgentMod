// ask-user: lets the model ask the human a question, as an event continuation.
//
// `ask_user` is not a blocking call. The tool call publishes
// `user-input-requested` (a `choice` UI hint) and returns without a result, so
// the loop simply has an unanswered call and waits; nothing in the runtime
// blocks. The answer arrives later as an ordinary recorded event:
//   * a `ui-action` replying to the request (an option, or free text in
//     values.text), or
//   * the user's next `user-message` while a question is open — that message is
//     taken as the answer (the event is vetoed with that reason, so it is not
//     also a new turn).
// Either way the plugin publishes the `tool-result`, and the loop continues.
// Policy approvals are a different thing (policy plugin): the model never
// decides whether an approval is needed.
//
// Place it before chat-context in a definition (it is a blocking subscriber on
// user-message).
import { definePlugin, key, offerSubscriptions, offerTools, optional, toolCalls, toolResult } from '../sdk/agentmod.js';

import { ASK_USER } from '../sdk/ask.js';

async function openRequests(ctx) {
  const view = await ctx.host.query('session', { session_id: ctx.sessionId });
  const answered = new Set(view.events.filter((e) => e.event_name === 'tool-result').map((e) => e.payload.call_id));
  return view.events.filter((e) => e.event_name === 'user-input-requested' && !answered.has(e.payload.call_id));
}

definePlugin({
  manifest: {
    name: 'ask-user',
    version: '0.1.0',
    description: 'ask_user: questions to the human as recorded continuations.',
    subscribe: [
      ...offerSubscriptions(),
      ...toolCalls(['ask_user']),
      { event: 'ui-action', mode: 'async', receives: { reply_to: key('string'), action: key('string'), values: optional('object') } },
      { event: 'user-message', mode: 'blocking', actions: ['veto'], receives: { text: key('string') } },
    ],
    emit: [
      { event: 'user-input-requested', keys: { call_id: key('string'), question: key('string'), options: optional('array<string>') } },
      toolResult({ authority: optional('string'), request: optional('string') }),
    ],
  },
  handlers: {
    'session-started': (ctx) => offerTools(ctx, [ASK_USER]),
    'config-applied': (ctx) => offerTools(ctx, [ASK_USER]),
    'tool-call/*': async (ctx) => {
      const { call_id, name, args } = ctx.payload;
      if (name !== 'ask_user') return;
      const question = String(args?.question ?? '').trim();
      if (!question) {
        await ctx.publish('tool-result', { call_id, name, output: '`question` is required', error: true });
        return;
      }
      const options = Array.isArray(args?.options) ? args.options.map(String).filter(Boolean).slice(0, 8) : [];
      await ctx.publish('user-input-requested', { call_id, question, options }, {
        ui: { v: 1, kind: 'choice', prompt: question, options: options.map((o, i) => ({ id: `o${i}`, label: o })), free_text: true, purpose: 'question' },
      });
    },
    'ui-action': async (ctx) => {
      const { reply_to, action, values } = ctx.payload;
      const open = await openRequests(ctx);
      const req = open.find((e) => e.event_id === reply_to);
      if (!req) return; // not ours, or already answered (idempotent)
      const opt = /^o(\d+)$/.test(action) ? req.payload.options[Number(action.slice(1))] : null;
      const answer = String(values?.text ?? opt ?? action);
      await ctx.publish('tool-result', { call_id: req.payload.call_id, name: 'ask_user', output: `The user answered: ${answer}`, authority: 'user', request: reply_to }, {
        ui: { v: 1, kind: 'tool', name: 'ask_user', call_id: req.payload.call_id, status: 'done', result: answer.slice(0, 80) },
      });
    },
    'user-message': async (ctx) => {
      const open = await openRequests(ctx);
      if (!open.length) return;
      const req = open[open.length - 1];
      await ctx.publish('tool-result', { call_id: req.payload.call_id, name: 'ask_user', output: `The user answered: ${ctx.payload.text}`, authority: 'user', request: req.event_id }, {
        ui: { v: 1, kind: 'tool', name: 'ask_user', call_id: req.payload.call_id, status: 'done', result: String(ctx.payload.text).slice(0, 80) },
      });
      ctx.veto(`answered the open question (${req.payload.call_id})`);
    },
  },
});
