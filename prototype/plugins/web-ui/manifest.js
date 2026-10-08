// Shared manifest of the web frontend plugin. The native gateway (main.js) and
// the in-browser runtime's page-hosted frontend both present exactly this.
export const WEB_UI_MANIFEST = {
  name: 'web-ui',
  version: '0.1.0',
  description: 'Web frontend: renders UI hints, publishes user actions, sends dispatcher commands.',
  // One standing invocation per session (at its start) is all a frontend
  // needs: user actions cite it. It reads everything else as a watcher
  // (records and live streams), so it is not invoked for every event.
  consumes: [{ event: 'session-started', mode: 'async', context: false }],
  emits: [
    { event: 'user-message', supplies: ['text'], deferred: true },
    { event: 'ui-action', supplies: ['reply_to', 'action'], deferred: true },
    { event: 'context-edit', supplies: ['ops'], deferred: true },
  ],
  capabilities: ['deferred-publish', 'start-session', 'control', 'observe'],
};
