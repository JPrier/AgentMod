// heartbeat: a trigger plugin. The core has no scheduler and no clock; this
// plugin holds `deferred-publish` + `start-session` and keeps a *journal
// session* whose log is its durable memory (one record per fire). On restart it
// finds its journal and keeps citing its standing invocation there.
//
// Config: { every_seconds: 0 (off), agent_every: 0, agent_definition: "chat",
//           agent_prompt: "Heartbeat check-in: what time is it?" }
import { definePlugin } from '../sdk/agentmod.js';

let timer = null;
let beats = 0;

async function journal(plugin) {
  const sessions = await plugin.host.query('sessions');
  let j = sessions.find((s) => s.definition === 'heartbeat');
  if (!j) {
    const { session_id } = await plugin.host.startSession({ definition: 'heartbeat' });
    return session_id;
  }
  return j.session_id;
}

async function standingCite(plugin, sessionId) {
  for (let i = 0; i < 50; i++) {
    const view = await plugin.host.query('session', { session_id: sessionId });
    const inv = view.events.flatMap((e) => e.invocations).filter((v) => v.plugin === plugin.instance || v.plugin === 'heartbeat').pop();
    if (inv) return { cite: inv.invocation_id, beats: view.events.filter((e) => e.event_name === 'heartbeat').length };
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error('no standing invocation in journal');
}

definePlugin({
  manifest: {
    name: 'heartbeat',
    version: '0.1.0',
    description: 'Interval trigger with a journal session; can spawn background agent sessions.',
    consumes: [{ event: 'session-started', mode: 'async', context: false }],
    emits: [
      { event: 'heartbeat', supplies: ['beat'], deferred: true },
      { event: 'user-message', supplies: ['text'] },
    ],
    capabilities: ['deferred-publish', 'start-session'],
    config_schema: { every_seconds: 0, agent_every: 0, agent_definition: 'chat', agent_prompt: 'Heartbeat check-in: what time is it?' },
  },
  init: async (plugin) => {
    const every = Number(plugin.config.every_seconds || 0);
    if (!every) return;
    const sid = await journal(plugin);
    const st = await standingCite(plugin, sid);
    beats = st.beats; // catch-up policy: continue counting, do not replay missed fires
    timer = setInterval(async () => {
      beats += 1;
      try {
        await plugin.host.publishDeferred('heartbeat', { beat: beats }, { cite: st.cite, ui: { v: 1, kind: 'progress', label: `heartbeat #${beats}`, value: null } });
        const k = Number(plugin.config.agent_every || 0);
        if (k && beats % k === 0) {
          await plugin.host.startSession({
            definition: plugin.config.agent_definition || 'chat',
            invocationId: st.cite,
            initial: { event_name: 'user-message', payload: { text: plugin.config.agent_prompt || 'Heartbeat check-in: what time is it?' } },
          });
        }
      } catch (e) {
        plugin.log('beat failed:', e.message);
      }
    }, every * 1000);
  },
  shutdown: () => clearInterval(timer),
  handlers: {
    'session-started': () => {},
  },
});
