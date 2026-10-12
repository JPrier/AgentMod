// Shared manifest of the web frontend plugin. The native gateway (main.js) and
// the in-browser runtime's page-hosted frontend both present exactly this.
export const WEB_UI_MANIFEST = {
  name: 'web-ui',
  version: '0.1.0',
  description: 'Web frontend: renders UI hints, publishes user actions, sends dispatcher commands.',
  // One standing invocation per session (at its start) is all a frontend
  // needs: user actions cite it. It reads everything else as a watcher
  // (records and live streams), so it is not invoked for every event — not
  // even the `tool-call/*` family, which it renders from the records.
  subscribe: [{ event: 'session-started', mode: 'async' }],
  emit: [
    // One event, two lanes: the user chooses per message whether it steers
    // (interrupt lane) or queues (normal lane).
    { event: 'user-message', deferred: true, lanes: ['normal', 'interrupt'], keys: { text: { shape: 'string' }, attachments: { shape: 'array<object>', optional: true } } },
    { event: 'ui-action', deferred: true, keys: { reply_to: { shape: 'string' }, action: { shape: 'string' }, values: { shape: 'object', optional: true } } },
    // Operator edits of the context jump the queue: the interrupt lane.
    { event: 'context-edit', deferred: true, lanes: ['interrupt'], keys: { ops: { shape: 'array<object>' } } },
  ],
  capabilities: ['deferred-publish', 'start-session', 'interrupt', 'control', 'observe'],
  // Its effect is outside the bus: rendering for a human.
  sink: true,
};
