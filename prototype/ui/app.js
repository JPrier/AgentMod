// AgentMod prototype UI: a frontend over one schema. It renders UI hints from
// the log, publishes user actions, sends dispatcher commands, and inspects the
// session → events → invocations tree. It talks to either host through the
// same client surface: the in-browser runtime (WASM kernel + worker plugins)
// or a native runtime's web-ui gateway.

import { markdown } from './runtime/markdown.js';
import { LiveClient } from './runtime/live-client.js';
import { harnessSummary } from './runtime/harness-view.js';
import { isolated, isolationSupported, ensureIsolation, enableIsolation, disableIsolation, takeAfterReload } from './runtime/isolation.js';

const $ = (sel, root = document) => root.querySelector(sel);

/** Tiny element builder: h('div.cls', {attrs}, ...children) */
function h(tag, attrs, ...kids) {
  const [head, ...classes] = tag.split('.');
  const [name, id] = head.split('#');
  const el = document.createElement(name || 'div');
  if (id) el.id = id;
  if (classes.length) el.className = classes.join(' ');
  if (attrs && (typeof attrs !== 'object' || attrs instanceof Node || Array.isArray(attrs))) {
    kids.unshift(attrs);
    attrs = null;
  }
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v == null || v === false) continue;
    if (k.startsWith('on')) el.addEventListener(k.slice(2).toLowerCase(), v);
    else if (k === 'html') el.innerHTML = v;
    else if (k === 'class') el.className += ' ' + v;
    else el.setAttribute(k, v === true ? '' : v);
  }
  for (const kid of kids.flat(Infinity)) {
    if (kid == null || kid === false) continue;
    el.append(kid instanceof Node ? kid : document.createTextNode(String(kid)));
  }
  return el;
}

const state = {
  client: null,
  host: 'browser',
  liveUrl: localStorage.getItem('agentmod.liveUrl') || 'http://127.0.0.1:7700',
  sessions: [],
  selected: null,
  view: null,
  graph: null,
  config: null,
  definitions: [],
  tab: 'pipeline',
  mobileView: 'chat',
  hideChunks: true,
  hideFrontend: true,
  expanded: new Set(),
  flash: null,
  draftConfig: null,
  applyResult: null,
  graphDef: null,
};

// ---------------------------------------------------------------------------
// Plugins that need settings, and model providers, through generic interfaces.
//
// Nothing here knows a provider. Plugins declare `settings` (with `secret`
// fields) and read-only `services` (e.g. `model-catalog`) in their manifests;
// a plugin that refuses to start reports them, and this page renders a form
// from that declaration. Model catalogs come from the provider plugin's
// service; choosing a model is a recorded config apply.
// ---------------------------------------------------------------------------

const SETTINGS = 'agentmod.settings';
const loadSettings = () => { try { return JSON.parse(localStorage.getItem(SETTINGS) || '{}'); } catch { return {}; } };
const saveSettings = (all) => localStorage.setItem(SETTINGS, JSON.stringify(all));
// Test/dev overrides for this tab only: ?set=<plugin>.<key>=<value> (repeatable).
const overrides = () => { try { return JSON.parse(sessionStorage.getItem(`${SETTINGS}.override`) || '{}'); } catch { return {}; } };
(function migrateStorage() {
  // Earlier builds stored one provider's key under its own names.
  const k = localStorage.getItem('agentmod.openrouter.key');
  if (!k) return;
  const all = loadSettings();
  all['openrouter-model'] = { ...(all['openrouter-model'] || {}), api_key: k, ...(localStorage.getItem('agentmod.openrouter.model') ? { model: localStorage.getItem('agentmod.openrouter.model') } : {}) };
  saveSettings(all);
  localStorage.removeItem('agentmod.openrouter.key');
  localStorage.removeItem('agentmod.openrouter.model');
})();

/** The deployment config with this browser's saved plugin settings merged in. */
function withSettings(config) {
  const cfg = structuredClone(config);
  for (const src of [loadSettings(), overrides()]) {
    for (const [plugin, values] of Object.entries(src)) {
      if (cfg.plugins?.[plugin]) cfg.plugins[plugin].config = { ...(cfg.plugins[plugin].config || {}), ...values };
    }
  }
  return cfg;
}

const modelProviders = () => (state.services || []).filter((s) => s.provides?.includes('model'));

/** The provider a definition uses (the first subscriber that provides a model). */
function providerOf(cfg, definition) {
  const names = new Set(modelProviders().map((s) => s.plugin));
  const def = cfg?.definitions?.[definition] || Object.values(cfg?.definitions || {}).find((d) => d.subscribers.some((x) => names.has(x.plugin)));
  return def?.subscribers.find((x) => names.has(x.plugin))?.plugin;
}

const describeModel = (m) => {
  const price = (v) => `$${(v * 1e6).toFixed(v * 1e6 < 1 ? 2 : 1)}`;
  const cost = m.prompt_price == null || m.completion_price == null ? null : m.prompt_price === 0 && m.completion_price === 0 ? 'free' : `${price(m.prompt_price)} in / ${price(m.completion_price)} out per M`;
  return [m.name !== m.id ? m.name : null, m.context ? `${Math.round(m.context / 1000)}k ctx` : null, cost, m.tools === false ? 'no tool calling' : null, m.vision ? 'images' : null].filter(Boolean).join(' · ');
};

const catalogs = new Map(); // plugin -> models[] | Error
let showAllModels = false;
let pickerSeq = 0;

/** A model field whose options come from the plugin's `model-catalog` service. */
function modelPicker({ plugin, value, call }) {
  const listId = `models-${++pickerSeq}`;
  const list = h(`datalist#${listId}`);
  const input = h('input', { value: value || '', list: listId, 'aria-label': 'Model', autocomplete: 'off', spellcheck: 'false', placeholder: 'model id' });
  const status = h('span.help.model-status');
  const toggle = h('input', { type: 'checkbox', checked: showAllModels });
  const refresh = () => {
    const got = catalogs.get(plugin);
    status.replaceChildren();
    if (Array.isArray(got)) {
      list.replaceChildren(...got.filter((m) => showAllModels || m.tools !== false).map((m) => h('option', { value: m.id }, describeModel(m))));
      const shown = got.filter((m) => showAllModels || m.tools !== false).length;
      const match = got.find((m) => m.id === input.value.trim());
      status.append(`${shown} of ${got.length} models${showAllModels ? '' : ' (tool-capable only)'}. `, match ? describeModel(match) || match.name : input.value.trim() ? `Not in ${plugin}'s catalog.` : '');
    } else if (got instanceof Error) {
      status.append(`Couldn't load the model list (${got.message}). Enter any model id the provider accepts.`);
    } else status.append(`Loading models from ${plugin}…`);
  };
  toggle.addEventListener('change', () => { showAllModels = toggle.checked; refresh(); });
  input.addEventListener('input', refresh);
  const load = () => call(plugin, 'model-catalog', {}).then((r) => catalogs.set(plugin, r?.models || []), (e) => catalogs.set(plugin, e instanceof Error ? e : new Error(String(e)))).then(refresh);
  refresh();
  if (!Array.isArray(catalogs.get(plugin))) load();
  const field = h('div.model-field', h('label', 'Model', input), list, status, h('label.inline-check', toggle, 'Include models without tool calling'));
  const validate = () => {
    const id = input.value.trim();
    if (!id) return 'Choose a model.';
    const got = catalogs.get(plugin);
    if (Array.isArray(got) && !got.some((m) => m.id === id)) return `“${id}” is not in ${plugin}'s model catalog.`;
    return null;
  };
  return { field, input, validate, reload: () => { catalogs.delete(plugin); refresh(); load(); } };
}

/** Form fields for one plugin's declared settings. */
function settingsFields({ plugin, settings, current, call, browser }) {
  const fields = [];
  const inputs = {};
  let picker = null;
  for (const s of settings || []) {
    if (s.browser_only && !browser) continue;
    if (s.type === 'model') {
      picker = modelPicker({ plugin, value: current[s.key], call });
      inputs[s.key] = picker.input;
      fields.push(picker.field);
      continue;
    }
    const input = h('input', { type: s.secret ? 'password' : 'text', value: current[s.key] ?? '', autocomplete: 'off', 'aria-label': s.label || s.key, required: s.required ? true : null });
    inputs[s.key] = input;
    fields.push(h('label', s.label || s.key, input), s.help && h('span.help', s.help, s.link ? [' ', h('a', { href: s.link, target: '_blank', rel: 'noopener' }, s.link.replace(/^https?:\/\//, ''))] : null));
  }
  const values = () => Object.fromEntries(Object.entries(inputs).map(([k, el]) => [k, el.value.trim()]).filter(([, v]) => v !== ''));
  const validate = () => {
    for (const s of settings || []) if (s.required && !(s.browser_only && !browser) && !inputs[s.key]?.value.trim()) return `${s.label || s.key} is required.`;
    return picker?.validate() || null;
  };
  // A secret entered after the catalog failed may unlock it.
  for (const s of settings || []) if (s.secret && inputs[s.key] && picker) inputs[s.key].addEventListener('change', () => { if (!Array.isArray(catalogs.get(plugin))) picker.reload(); });
  return { fields, values, validate };
}

/** Plugins refused to start (e.g. a missing credential): ask for their settings. */
function settingsGate(runtime, refusals, error) {
  const app = $('#app');
  app.innerHTML = '';
  const saved = loadSettings();
  const forms = refusals.map((r) => ({ r, f: settingsFields({ plugin: r.plugin, settings: r.settings, current: saved[r.plugin] || {}, call: (p, svc, a) => runtime.callService(p, svc, a), browser: true }) }));
  app.append(
    h('div.boot.gate',
      h('div.gate-card',
        h('div.brand', h('span.brand-mark', { 'aria-hidden': 'true' }), 'AgentMod'),
        h('p', 'This prototype runs the AgentMod kernel in your browser. Some plugins need settings before the runtime can start:'),
        error && h('p.gate-err', error),
        h('form', { onsubmit: async (e) => {
          e.preventDefault();
          for (const { r, f } of forms) {
            const bad = f.validate();
            if (bad) return settingsGate(runtime, refusals, `${r.plugin}: ${bad}`);
          }
          const btn = e.target.querySelector('button[type=submit]');
          btn.disabled = true;
          btn.textContent = 'Checking…';
          const all = loadSettings();
          for (const { r, f } of forms) {
            const values = f.values();
            if ((r.services || []).some((x) => x.name === 'check-credentials')) {
              try {
                const ok = await runtime.callService(r.plugin, 'check-credentials', values);
                if (ok && ok.ok === false && (ok.status === 401 || ok.status === 403)) return settingsGate(runtime, refusals, `${r.plugin} rejected these credentials. Check them and try again.`);
              } catch { /* offline: let the plugin report errors on first use */ }
            }
            all[r.plugin] = { ...(all[r.plugin] || {}), ...values };
          }
          saveSettings(all);
          connect('browser');
        } },
          ...forms.map(({ r, f }) => h('fieldset', h('legend', r.plugin), h('p.help', r.message), ...f.fields)),
          h('button.btn.primary', { type: 'submit' }, 'Start the runtime')),
        h('p.help', 'Settings are kept in this browser (localStorage) and handed only to the plugin that declared them. Secret settings are omitted from exported logs.'),
        h('p.help', 'Or ', h('button.linkish', { type: 'button', onclick: () => promptLive() }, 'attach to a local runtime'), ' started with ', h('code', 'agentmod serve'), '.'))),
  );
  app.querySelector('input')?.focus();
}

/** Apply a provider choice and its settings as a live config change. */
async function applyProvider({ provider, values }) {
  const active = state.config?.config;
  if (!active) return;
  const cfg = structuredClone(active);
  const providers = new Set(modelProviders().map((s) => s.plugin));
  const current = providerOf(cfg, state.view?.definition);
  if (current && provider !== current) {
    for (const def of Object.values(cfg.definitions || {})) def.subscribers = def.subscribers.map((x) => (x.plugin === current && providers.has(provider) ? { ...x, plugin: provider } : x));
  }
  cfg.plugins[provider].config = { ...(cfg.plugins[provider].config || {}), ...values };
  if (state.host === 'browser') {
    const all = loadSettings();
    all[provider] = { ...(all[provider] || {}), ...values };
    saveSettings(all);
  }
  const r = await act(() => state.client.applyConfig(cfg, { kind: 'global' }));
  if (!r) return;
  if (!r.ok) return toast(`Rejected: ${(r.diagnostics || []).filter((d) => d.severity === 'error').map((d) => d.message).join('; ')}`, true);
  state.showProvider = false;
  toast(`Sessions now use ${provider}${values.model ? ` · ${values.model}` : ''} from their next event. Nothing restarted.`);
  await refreshAll();
  render();
}

// ---------------------------------------------------------------------------
// Boot and host selection
// ---------------------------------------------------------------------------

async function connect(host) {
  state.client?.close?.();
  stopSub?.();
  state.host = host;
  state.selected = null;
  state.view = null;
  state.draftConfig = null;
  state.applyResult = null;
  bootMessage(host === 'live' ? `Connecting to ${state.liveUrl}…` : 'Loading the kernel (WebAssembly) and starting plugin workers…');
  try {
    if (host === 'live') {
      const c = new LiveClient(state.liveUrl);
      await c.boot();
      state.client = c;
    } else {
      const { BrowserRuntime } = await import('./runtime/browser-host.js');
      const c = new BrowserRuntime({ log: (m) => console.info('[agentmod]', m) });
      const base = await (await fetch('runtime/agentmod.config.json')).json();
      try {
        await c.boot(withSettings(base));
      } catch (e) {
        if (e.refusals?.length) {
          settingsGate(c, e.refusals);
          return;
        }
        throw e;
      }
      state.client = c;
    }
  } catch (e) {
    bootError(host, e);
    return;
  }
  stopSub = state.client.onRecord(onRecord);
  await refreshAll();
  const after = takeAfterReload();
  if (after && state.definitions.includes(after)) {
    await newSession(after);
  } else if (!state.sessions.length) {
    await newSession('chat');
  } else if (state.sessions.length) {
    await select(state.sessions[state.sessions.length - 1].session_id);
  }
  render();
}

let stopSub = null;

function bootMessage(msg) {
  const app = $('#app');
  app.innerHTML = '';
  app.append(h('div.boot', h('div.boot-mark', { 'aria-hidden': 'true' }), h('p', msg)));
}

function bootError(host, e) {
  const app = $('#app');
  app.innerHTML = '';
  const msg =
    host === 'live'
      ? [h('p', `Couldn't reach a runtime at ${state.liveUrl}: ${e.message}.`), h('p', 'Start one with ', h('code', 'cargo run -p agentmod-runtime -- serve'), ' in prototype/, or use the in-browser runtime.')]
      : [h('p', `The in-browser runtime failed to start: ${e.message}`), h('p', 'It needs a browser with WebAssembly and module workers (current Chrome, Edge, Firefox, or Safari).')];
  app.append(
    h('div.boot.err', h('div.boot-mark', { 'aria-hidden': 'true' }), ...msg,
      h('div.row-actions', h('button.btn', { onclick: () => connect('browser') }, 'Use the in-browser runtime'), h('button.btn', { onclick: () => promptLive() }, 'Connect to a local runtime'))),
  );
}

function promptLive() {
  const url = prompt('Runtime URL (the web-ui plugin of `agentmod serve`)', state.liveUrl);
  if (!url) return;
  state.liveUrl = url;
  localStorage.setItem('agentmod.liveUrl', url);
  connect('live');
}

async function refreshAll() {
  const [sessions, graph, config, services] = await Promise.all([state.client.listSessions(), state.client.getGraph(), state.client.getConfig(), state.client.listServices().catch(() => [])]);
  state.services = services;
  state.sessions = sessions.sort((a, b) => a.session_id.localeCompare(b.session_id));
  state.graph = graph;
  state.config = config;
  state.definitions = Object.keys(graph?.definitions || {});
}

async function select(id) {
  state.selected = id;
  state.expanded.clear();
  state.view = id ? await state.client.getSession(id) : null;
  state.graphDef = state.view?.definition || state.graphDef;
  state.mobileView = 'chat';
  render(true);
}

// Live updates: records stream in; refresh the affected views (coalesced).
let pending = new Set();
let timer = null;
function onRecord(r) {
  pending.add(r.session_id);
  if (r.type === 'config-applied') pending.add('*graph');
  if (!timer) timer = setTimeout(flush, 60);
}

async function flush() {
  timer = null;
  const touched = pending;
  pending = new Set();
  try {
    const known = new Set(state.sessions.map((s) => s.session_id));
    if ([...touched].some((s) => !known.has(s) && !s.startsWith('*')) || touched.size) {
      state.sessions = (await state.client.listSessions()).sort((a, b) => a.session_id.localeCompare(b.session_id));
    }
    if (touched.has('*graph')) {
      state.graph = await state.client.getGraph();
      state.config = await state.client.getConfig();
    }
    if (state.selected && touched.has(state.selected)) state.view = await state.client.getSession(state.selected);
  } catch (e) {
    console.warn(e);
  }
  if (editingInspector()) {
    deferred = true;
    return;
  }
  render();
}

let deferred = false;
function editingInspector() {
  const a = document.activeElement;
  return !!a && !!a.closest?.('.inspector, .provider-panel') && ['INPUT', 'TEXTAREA', 'SELECT'].includes(a.tagName);
}
document.addEventListener('focusout', () => {
  setTimeout(() => {
    if (deferred && !editingInspector()) {
      deferred = false;
      render();
    }
  }, 0);
});

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------

async function act(fn, ok) {
  try {
    const r = await fn();
    if (ok) toast(typeof ok === 'function' ? ok(r) : ok);
    return r;
  } catch (e) {
    toast(e.message, true);
    return null;
  }
}

async function newSession(definition, text, fork_from) {
  const r = await act(() => state.client.startSession({ definition, text, fork_from }));
  if (r?.session_id) {
    await refreshAll();
    await select(r.session_id);
  }
}

async function send(text, steer) {
  if (!text.trim() || !state.selected) return;
  await act(() => state.client.sendMessage(state.selected, text, steer ? 'priority' : 'normal'));
}

async function command(cmd) {
  const labels = { 'soft-stop': 'Soft stop: the running pipeline finishes, then the session parks.', 'hard-stop': 'Hard stop: in-flight invocations were cancelled.', resume: 'Resumed.' };
  await act(() => state.client.sendCommand(state.selected, cmd), labels[cmd]);
}

let toastTimer;
function toast(msg, bad = false) {
  document.querySelector('.toast')?.remove();
  const t = h('div.toast' + (bad ? '.bad' : ''), { role: 'status' }, msg);
  document.body.append(t);
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.remove(), bad ? 6000 : 3200);
}

async function crash() {
  const r = await act(() => state.client.crashAndRecover());
  if (!r) return;
  const n = r.reduce((a, s) => a + s.orphans, 0);
  toast(n ? `Killed every worker and the kernel, replayed the logs, and restarted ${n} orphaned invocation(s).` : 'Killed every worker and the kernel and rebuilt it from the logs. Nothing was in flight.');
  await refreshAll();
  if (state.selected) state.view = await state.client.getSession(state.selected);
  render();
}

async function exportLogs() {
  const data = await act(() => state.client.exportLogs());
  if (!data) return;
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
  const a = h('a', { href: URL.createObjectURL(blob), download: `agentmod-logs-${new Date().toISOString().slice(0, 19)}.json` });
  document.body.append(a);
  a.click();
  a.remove();
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

function render(scrollBottom = false) {
  if (!state.client) return;
  const app = $('#app');
  const thread = $('.thread', app);
  const atBottom = !thread || thread.scrollHeight - thread.scrollTop - thread.clientHeight < 80;
  const paneScroll = $('.pane', app)?.scrollTop ?? 0;
  const paneEl = $('.pane', app);
  const paneAtBottom = paneEl ? paneEl.scrollHeight - paneEl.scrollTop - paneEl.clientHeight < 60 : true;
  const focused = document.activeElement;
  const keepComposer = focused?.id === 'composer' ? { value: focused.value, start: focused.selectionStart } : null;
  if (keepComposer === null && $('#composer')) composerDraft = $('#composer').value;

  const nodes = [topbar(), mobileTabs(), sidebar(), chat(), inspector()];
  app.innerHTML = '';
  app.dataset.view = state.mobileView;
  app.append(...nodes);

  const t = $('.thread', app);
  if (t && (atBottom || scrollBottom)) t.scrollTop = t.scrollHeight;
  const p = $('.pane', app);
  if (p && state.resetPane) {
    p.scrollTop = state.tab === 'pipeline' ? p.scrollHeight : 0;
    state.resetPane = false;
  } else if (p) p.scrollTop = state.tab === 'pipeline' && paneAtBottom && !state.expanded.size ? p.scrollHeight : paneScroll;
  const c = $('#composer');
  if (c) {
    c.value = keepComposer ? keepComposer.value : composerDraft;
    if (keepComposer) {
      c.focus();
      c.setSelectionRange(keepComposer.start, keepComposer.start);
    }
    autosize(c);
  }
  if (state.flash) {
    const el = document.getElementById(`ev-${state.flash}`);
    el?.scrollIntoView({ block: 'center' });
    el?.classList.add('flash');
    state.flash = null;
  }
}
let composerDraft = '';

function topbar() {
  const live = state.host === 'live';
  return h('header.topbar',
    h('div.brand', h('span.brand-mark', { 'aria-hidden': 'true' }), 'AgentMod', h('small', 'event-bus prototype')),
    h('div.host-switch', { role: 'group', 'aria-label': 'Runtime host' },
      h('button', { 'aria-pressed': String(!live), onclick: () => !live || connect('browser'), title: 'Rust kernel compiled to WebAssembly; plugins run in Web Workers' }, 'In-browser'),
      h('button', { 'aria-pressed': String(live), onclick: () => (live ? promptLive() : promptLive()), title: 'Attach to `agentmod serve` on your machine' }, 'Local runtime')),
    h('span.host-note', live ? `Attached to ${state.liveUrl}` : 'The Rust kernel runs here as WebAssembly; each plugin is a Web Worker.'),
    h('div.spacer'),
    providerButton(),
    !live && h('button.btn', { onclick: crash, title: 'Terminate every plugin worker and the kernel, then recover all sessions from their logs' }, 'Crash & recover'),
    h('button.btn.ghost', { onclick: exportLogs }, 'Export logs'),
  );
}

function providerButton() {
  const cfg = state.config?.config;
  const p = providerOf(cfg, state.view?.definition);
  const model = cfg?.plugins?.[p]?.config?.model;
  const label = p ? `${p}${model ? ` · ${model}` : ''}` : 'No model';
  return h('div.provider',
    h('button.btn', { onclick: () => { state.showProvider = !state.showProvider; render(); }, 'aria-expanded': String(!!state.showProvider), title: 'The model is a plugin; changing it is a live config apply' }, label),
    state.showProvider && providerPanel());
}

function providerPanel() {
  const browser = state.host === 'browser';
  const cfg = state.config?.config;
  const providers = modelProviders().filter((x) => cfg?.plugins?.[x.plugin] && !cfg.plugins[x.plugin].disabled);
  const chosen = state.panelProvider && providers.some((x) => x.plugin === state.panelProvider) ? state.panelProvider : providerOf(cfg, state.view?.definition) || providers[0]?.plugin;
  const decl = providers.find((x) => x.plugin === chosen);
  const current = { ...(cfg?.plugins?.[chosen]?.config || {}), ...(browser ? loadSettings()[chosen] || {} : {}) };
  const form = decl ? settingsFields({ plugin: chosen, settings: decl.settings, current, call: (p, svc, a) => state.client.callService(p, svc, a), browser }) : null;
  return h('div.provider-panel', { role: 'dialog', 'aria-label': 'Model provider' },
    h('h3', 'Model provider'),
    providers.length > 1 && h('label', 'Provider plugin', h('select', { onchange: (e) => { state.panelProvider = e.target.value; render(); } }, providers.map((x) => h('option', { value: x.plugin, selected: x.plugin === chosen }, `${x.plugin} — ${x.description || ''}`)))),
    decl ? h('p.help', decl.description) : h('p.help', 'No plugin in this deployment declares that it provides a model.'),
    !browser && h('p.help', 'Secret settings (keys) are read by the plugin on the machine running agentmod serve.'),
    form?.fields,
    form && h('div.row-actions',
      h('button.btn.primary', { onclick: () => {
        const bad = form.validate();
        if (bad) return toast(bad, true);
        applyProvider({ provider: chosen, values: form.values() });
      } }, 'Apply'),
      browser && h('button.btn.ghost', { onclick: () => { const all = loadSettings(); delete all[chosen]; saveSettings(all); location.reload(); } }, 'Forget settings')),
    h('p.help', 'Applying is a live config apply: sessions pick up the change at their next event boundary.'));
}

function mobileTabs() {
  const b = (id, label) => h('button', { role: 'tab', 'aria-selected': String(state.mobileView === id), onclick: () => { state.mobileView = id; render(); } }, label);
  return h('nav.mobile-tabs', { role: 'tablist' }, b('sessions', 'Sessions'), b('chat', 'Chat'), b('inspect', 'Inspect'));
}

function sidebar() {
  const defs = state.definitions;
  const sel = h('select', { 'aria-label': 'Session definition' }, defs.map((d) => h('option', { value: d, selected: d === 'chat' }, d)));
  const byParent = new Map();
  for (const s of state.sessions) {
    const k = s.parent && state.sessions.some((x) => x.session_id === s.parent) ? s.parent : null;
    if (!byParent.has(k)) byParent.set(k, []);
    byParent.get(k).push(s);
  }
  const items = [];
  const walk = (parent, depth) => {
    for (const s of byParent.get(parent) || []) {
      items.push(
        h('button.session-item' + (depth ? '.child' : ''), { 'aria-current': String(s.session_id === state.selected), onclick: () => select(s.session_id), style: depth > 1 ? `padding-left:${10 + depth * 14}px` : null },
          h('span.dot.' + (s.activity || 'idle'), { title: s.activity }),
          h('span.t', s.title || (s.definition === 'chat' ? 'New chat' : s.definition)),
          h('span.m', `${s.session_id} · ${s.definition} · ${s.last_sequence} records`)),
      );
      walk(s.session_id, depth + 1);
    }
  };
  walk(null, 0);
  return h('aside.sidebar',
    h('div.new-session', sel, h('button.btn.primary', { onclick: () => newSession(sel.value) }, 'New session')),
    h('nav.sessions', { 'aria-label': 'Sessions' }, items.length ? items : h('p.help', { style: 'padding:10px' }, 'No sessions yet.')),
    h('div.side-foot', 'Design: ', h('a', { href: 'https://github.com/JPrier/AgentMod/blob/main/docs/design/high-level-design.md', target: '_blank', rel: 'noopener' }, 'High-Level Design'), ' · ', h('a', { href: 'https://github.com/JPrier/AgentMod/tree/prototype/event-bus/prototype', target: '_blank', rel: 'noopener' }, 'source')),
  );
}

// ---- chat thread: UI hints rendered from the log --------------------------------

function chat() {
  const v = state.view;
  if (!v) {
    return h('main.chat', h('div.empty', h('h2', 'Start a session'), h('p', 'Pick a session definition and create a session. Each one is its own append-only log.')));
  }
  const st = v.status?.activity || (v.state === 'halted' ? 'halted' : 'idle');
  const head = h('div.chat-head',
    h('h1', v.title || (v.definition === 'chat' ? 'New chat' : v.definition)),
    h('span.sub', `${v.session_id} · ${v.definition} · config ${v.config.slice(0, 8)}`),
    h('span.state.' + st, st),
    v.status && (v.status.queued_priority + v.status.queued_normal) > 0 && h('span.sub', `${v.status.queued_normal + v.status.queued_priority} queued`),
    modeControl(v),
    h('div.controls',
      h('button.btn.small', { onclick: () => command('soft-stop'), disabled: !['running', 'idle'].includes(st), title: 'Finish the running pipeline, then park' }, 'Soft stop'),
      h('button.btn.small.danger', { onclick: () => command('hard-stop'), disabled: st === 'halted', title: 'Cancel in-flight invocations now' }, 'Hard stop'),
      h('button.btn.small', { onclick: () => command('resume'), disabled: !['halted', 'parked', 'draining'].includes(st) }, 'Resume')),
  );
  const items = threadItems(v);
  const empty = !v.events.some((e) => e.event_name === 'user-message');
  const thread = h('div.thread', { 'aria-live': 'polite' },
    h('div.thread-inner', sandboxNotice(v), items, empty && v.definition !== 'heartbeat' && suggestions(v)));
  return h('main.chat', head, thread, composer(v));
}

// ---- permission mode (policy plugin), per session ---------------------------------

const MODES = [['auto', 'Auto: allow unless a rule denies/asks'], ['default', 'Default: edits allowed; network, destructive, publish ask'], ['ask', 'Ask: approve every change'], ['read-only', 'Read-only: no changes (plan mode)']];

function modeControl(v) {
  const cfg = state.config?.config;
  const subs = cfg?.definitions?.[v.definition]?.subscribers || [];
  if (!subs.some((s) => s.plugin === 'policy') || !cfg?.plugins?.policy) return null;
  const pc = cfg.plugins.policy.config || {};
  const current = pc.session_mode || pc.mode || 'default';
  return h('label.mode', { title: 'Permission mode for this session: a session-scoped config apply, recorded in its log' }, 'Mode ',
    h('select', { onchange: (e) => setMode(v.session_id, e.target.value) }, MODES.map(([m, label]) => h('option', { value: m, selected: m === current, title: label }, m))));
}

async function setMode(sid, mode) {
  const cfg = structuredClone(state.config.config);
  cfg.plugins.policy.config = { ...(cfg.plugins.policy.config || {}), session_mode: mode };
  const r = await act(() => state.client.applyConfig(cfg, { kind: 'session', session_id: sid }));
  if (r?.ok) toast(`${sid} now runs in ${mode} mode from its next event (this session only). The policy's runtime rules still apply; a mode can never loosen a deny.`);
  else if (r) toast(`Rejected: ${(r.diagnostics || []).filter((d) => d.severity === 'error').map((d) => d.message).join('; ')}`, true);
}

// ---- the Linux sandbox (linux-sandbox plugin) -----------------------------------

const SANDBOX = 'linux-sandbox';

/** Status line for sessions whose definition includes the in-browser Linux sandbox. */
function sandboxNotice(v) {
  const configured = state.config?.config?.definitions?.[v.definition]?.subscribers?.some((s) => s.plugin === SANDBOX);
  if (!configured) return null;
  const active = state.graph?.definitions?.[v.definition]?.plugins?.includes(SANDBOX);
  if (!active) {
    return h('div.notice', h('b', 'Linux sandbox unavailable on this host. '),
      'It runs only in the in-browser runtime (CheerpX needs a browser); this runtime disabled it, so the coding tools are missing here.');
  }
  if (isolated()) {
    return h('div.notice.ok', h('b', 'Linux sandbox: '),
      'an x86 Debian VM running in this tab (CheerpX), on your CPU and RAM. It starts on the first tool call; /workspace persists in this browser. ',
      h('button.linkish', { onclick: () => disableIsolation() }, 'Turn off isolation'));
  }
  if (!isolationSupported()) {
    return h('div.notice', h('b', 'Linux sandbox unavailable: '), 'it needs a secure (https or localhost) page with service workers to enable cross-origin isolation.');
  }
  return h('div.notice',
    h('p', h('b', 'Enable the Linux sandbox to let the agent code here. '),
      'It runs an x86 Linux VM in this tab with CheerpX, which needs a cross-origin-isolated page. Enabling installs a small service worker that adds the isolation headers, then reloads the page. ',
      state.config?.durable ? 'Sessions are stored in this browser (IndexedDB), so this one resumes after the reload.' : state.config?.storage_blocked ? `Sessions in this tab are not stored (${state.config.storage_blocked}), so this one ends on reload (export logs first to keep it).` : 'This browser gives the runtime no durable storage, so this session ends on reload (export logs first to keep it).'),
    h('div.row-actions', h('button.btn.primary', { onclick: () => act(() => enableIsolation(v.definition)) }, 'Enable the Linux sandbox')),
    h('p.help', 'Works in current Chrome, Edge, and Firefox. CheerpX is by Leaning Technologies and free for personal and open-source use.'));
}

function suggestions(v) {
  if (state.graph?.definitions?.[v?.definition]?.plugins?.includes(SANDBOX)) {
    const s = [
      'What languages and build tools are installed in the sandbox?',
      'Write a C program that prints the first 20 primes, compile it with gcc, and run it',
      'Create a Python module with a slugify() function plus unit tests, and run the tests',
      'Import the GitHub repo antirez/kilo, build it with make, and explain how it draws the screen',
    ];
    return h('div.empty', h('h2', 'Code in your browser'), h('p', 'The agent edits files and runs commands in a Linux VM on your machine. Every tool call, file diff, and result is an event in this session\u2019s log.'),
      h('div.suggest', s.map((t) => h('button', { onclick: () => send(t) }, t))));
  }
  const s = ['What time is it?', 'Calculate (12+30)*7', 'Remember that the deploy is on Friday', 'Delegate: summarize the launch plan', 'my token: sk-test1234567890abcdef please keep it safe'];
  return h('div.empty', h('h2', 'Talk to the agent'), h('p', 'Every reply is produced by plugins: a model provider, tools, memory, an approval gate, and sub-agents. Watch the pipeline on the right as it runs.'),
    h('div.suggest', s.map((t) => h('button', { onclick: () => send(t) }, t))));
}

function composer(v) {
  const ta = h('textarea#composer', { rows: 1, placeholder: 'Message the agent', 'aria-label': 'Message', oninput: (e) => autosize(e.target),
    onkeydown: (e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); e.target.form.requestSubmit(); } } });
  const steer = h('input', { type: 'checkbox', id: 'steer' });
  const form = h('form', { onsubmit: (e) => { e.preventDefault(); const t = ta.value; ta.value = ''; composerDraft = ''; send(t, steer.checked); } },
    ta, h('button.btn.primary', { type: 'submit' }, 'Send'),
    h('div.opts', h('label', steer, 'Steer: send on the priority lane'), h('span', v.definition === 'chat' ? 'Shift+Enter for a new line' : '')));
  return h('div.composer', form);
}

function autosize(t) {
  t.style.height = 'auto';
  t.style.height = Math.min(t.scrollHeight, 180) + 'px';
}

function settledPayload(e) {
  return e.settlement?.status === 'delivered' ? e.settlement.payload : e.payload;
}

function threadItems(v) {
  const out = [];
  const streams = new Map();
  const tools = new Map();
  const answered = new Map();
  answeredText.clear();
  for (const e of v.events) {
    if (e.event_name !== 'ui-action') continue;
    answered.set(e.payload.reply_to, e.payload.action);
    if (e.payload.values?.text) answeredText.set(e.payload.reply_to, e.payload.values.text);
  }
  const finalized = new Set(v.events.filter((e) => e.event_name === 'model-response').map((e) => e.payload.stream_id));
  // A stream also ends when its invocation closes (completed, failed, or cancelled by a hard stop).
  for (const e of v.events) for (const i of e.invocations) if (i.outcome) finalized.add(i.invocation_id);
  const chunked = new Set(v.events.filter((e) => e.event_name === 'stream-chunk').map((e) => e.payload.stream_id));
  const controls = (v.control || []).filter((c) => c.kind === 'hard-stop' || c.kind === 'soft-stop' || c.kind === 'config-applied');
  let ci = 0;
  const controlNote = (c) =>
    c.kind === 'hard-stop' ? h('div.sys.bad', `Hard stop by ${c.detail.by} at record #${c.sequence}.`)
      : c.kind === 'soft-stop' ? h('div.sys', `Soft stop by ${c.detail.by} at record #${c.sequence}.`)
        : h('div.sys', `Config ${c.detail.config.slice(0, 8)} applied at record #${c.sequence}${c.detail.scoped ? ' (this session only)' : ''}.`);

  for (const e of v.events) {
    while (ci < controls.length && controls[ci].sequence < e.sequence) out.push(controlNote(controls[ci++]));
    const ui = e.ui;
    const name = e.event_name;
    if (name === 'session-started' && e.payload.parent_session) {
      out.push(h('div.sys', 'Sub-session started by ', h('button.link', { onclick: () => select(e.payload.parent_session) }, e.payload.parent_session), ' through the delegate tool.'));
      continue;
    }
    if (name === 'session-started' && e.payload.fork_of) {
      out.push(h('div.sys', `Branched from ${e.payload.fork_of.session_id} at record #${e.payload.fork_of.sequence}; context was copied into this log.`));
      continue;
    }
    if (name === 'user-message') {
      const p = settledPayload(e);
      const changed = p.text !== e.payload.text;
      out.push(h('div.msg.user', { title: `${e.event_id} · ${e.status}` }, p.text,
        h('button.linkish.branch', { title: 'Start a new session from the context just before this message (its own log; with an isolated workspace for coding sessions)', onclick: () => branchFrom(e.sequence - 1) }, 'branch here'),
        changed && h('span.note', 'Redacted by a transformer before any later plugin saw it'),
        e.status === 'vetoed' && h('span.note', `Vetoed: ${e.settlement.reason}`),
        e.lane === 'priority' && h('span.note', 'Sent on the priority lane')));
      continue;
    }
    if (name === 'stream-chunk' && ui?.kind === 'stream-chunk') {
      let s = streams.get(ui.stream_id);
      if (!s) {
        s = { text: '', el: h('div.msg.assistant') };
        streams.set(ui.stream_id, s);
        out.push(s.el);
      }
      s.text += ui.text;
      s.el.innerHTML = markdown(s.text);
      s.el.classList.toggle('streaming', !finalized.has(ui.stream_id));
      continue;
    }
    if (name === 'model-request') {
      for (const i of e.invocations) {
        if (i.outcome?.status === 'failed') out.push(h('div.sys.bad', `${i.plugin} failed: ${i.outcome.error}`));
        if (i.outcome?.status === 'cancelled' && !chunked.has(i.invocation_id)) out.push(h('div.sys', `${i.plugin} was cancelled before replying.`));
      }
      continue;
    }
    if (name === 'model-response') {
      const s = streams.get(e.payload.stream_id);
      if (s) s.el.classList.remove('streaming');
      else if (e.payload.text) out.push(h('div.msg.assistant', { html: markdown(e.payload.text) }));
      continue;
    }
    if (name === 'assistant-message') {
      const s = ui?.replaces_stream && streams.get(ui.replaces_stream);
      if (s) {
        s.el.innerHTML = markdown(e.payload.text);
        s.el.classList.remove('streaming');
      } else out.push(h('div.msg.assistant', { html: markdown(e.payload.text) }));
      continue;
    }
    if (name === 'tool-call') {
      const id = e.payload.call_id;
      let t = tools.get(id);
      if (!t) {
        t = { status: h('span.pill', 'requested'), res: h('span.res') };
        tools.set(id, t);
        out.push(h('div.tool', h('span.name', e.payload.name), h('span.args', JSON.stringify(e.payload.args)), t.status, t.res));
      }
      if (e.status === 'vetoed') setPill(t.status, 'awaiting approval', 'wait');
      else if (e.payload.approved) setPill(t.status, 'approved, running', 'blk');
      else if (e.status === 'delivered') setPill(t.status, 'running', 'blk');
      continue;
    }
    if (name === 'tool-result') {
      const t = tools.get(e.payload.call_id);
      if (t) {
        setPill(t.status, e.payload.policy?.effect === 'deny' ? 'denied by policy' : e.payload.error ? 'failed' : 'done', e.payload.error ? 'bad' : 'ok');
        t.res.textContent = String(e.payload.output);
        t.res.title = String(e.payload.output);
      }
      continue;
    }
    if (name === 'subagent-result') {
      out.push(h('div.sys', 'Sub-session ', h('button.link', { onclick: () => select(e.payload.worker) }, e.payload.worker || 'worker'), ' reported back.'));
      continue;
    }
    if (ui) {
      const el = renderHint(ui, e, answered);
      if (el) out.push(el);
      continue;
    }
    if (['failed', 'aborted'].includes(e.status)) {
      const s = e.settlement;
      out.push(h('div.sys.bad', `${name} ${e.status}${s?.plugin ? ` in ${s.plugin}` : ''}: ${s?.error || s?.reason || ''}`));
    }
  }
  while (ci < controls.length) out.push(controlNote(controls[ci++]));
  for (const b of (v.blocked || []).slice(-3)) out.push(h('div.sys.bad', `Blocked publish from ${b.plugin} (${b.event_name}): ${b.reason}`));
  return out;
}

const answeredText = new Map();

/** New session from the context as of `sequence` (an auditable branch; the parent is untouched). */
async function branchFrom(sequence) {
  const v = state.view;
  if (!v) return;
  await newSession(v.definition, undefined, { session_id: v.session_id, sequence: Math.max(0, sequence) });
  toast(`Branched from ${v.session_id} at record #${sequence}. The original session and its history are unchanged.`);
}

/** Restore files to a checkpoint and rewind the conversation to just before it. */
async function rewindBoth(e) {
  const sid = state.selected;
  await act(() => state.client.uiAction(sid, e.event_id, 'restore'));
  await act(() => state.client.contextEdit(sid, [{ op: 'restore', to_sequence: Math.max(0, e.sequence - 1) }]), 'Rewound the files and the conversation. Both are new records; nothing was deleted.');
}

function setPill(el, text, kind) {
  el.textContent = text;
  el.className = 'pill ' + (kind || '');
}

/** UI-hint vocabulary v1: text, markdown, stream-chunk, tool, choice, progress, diff, form. */
function renderHint(ui, e, answered) {
  const sid = state.selected;
  switch (ui.kind) {
    case 'text':
      return ui.role === 'user' ? h('div.msg.user', ui.text) : h('div.sys', ui.text);
    case 'markdown':
      return h('div.msg.assistant', { html: markdown(ui.text) });
    case 'progress':
      return h('div.progress-line', ui.label, ui.value != null && h('span.bar', h('i', { style: `width:${Math.round(ui.value * 100)}%` })));
    case 'choice': {
      const done = answered.get(e.event_id);
      const label = (a) => (a === 'approve' ? 'Approved' : a === 'approve-session' ? 'Allowed for this session' : a === 'deny' ? 'Denied' : `Answered: ${a}`);
      const free = ui.free_text && !done && h('form.free', { onsubmit: (ev) => { ev.preventDefault(); const t = ev.target.elements.answer.value.trim(); if (t) act(() => state.client.uiAction(sid, e.event_id, 'answer', { text: t })); } },
        h('input', { name: 'answer', placeholder: 'Type an answer', 'aria-label': 'Answer' }), h('button.btn', { type: 'submit' }, 'Answer'));
      return h('div.choice' + (done ? '.answered' : ''),
        h('div.q', ui.prompt), ui.detail && h('div.d', ui.detail),
        done ? h('span.pill.' + (done === 'deny' ? 'bad' : 'ok'), label(answeredText.get(e.event_id) || done))
          : [h('div.opts', (ui.options || []).map((o) => h('button.btn' + (o.style === 'primary' ? '.primary' : o.style === 'danger' ? '.danger' : ''), { onclick: () => act(() => state.client.uiAction(sid, e.event_id, o.id)) }, o.label))), free]);
    }
    case 'plan': {
      const mark = { completed: '✓', in_progress: '▸', blocked: '!', pending: '○' };
      return h('div.plan', h('b', 'Plan'), ui.note && h('span.help', ` — ${ui.note}`), h('ul', (ui.items || []).map((i) => h('li.' + i.status, h('span.mark', mark[i.status] || '○'), i.text))));
    }
    case 'checkpoint': {
      if (ui.reason === 'restored') return h('div.sys', `Workspace restored to checkpoint ${String(ui.checkpoint).slice(0, 12)}.`);
      return h('div.checkpoint', h('span.cp', `checkpoint ${String(ui.checkpoint).slice(0, 10)}`), h('span.why', ui.reason),
        h('button.linkish', { title: 'Restore the files to this checkpoint (the current state is checkpointed first, so this can be undone)', onclick: () => act(() => state.client.uiAction(sid, e.event_id, 'restore'), 'Restoring files…') }, 'restore files'),
        h('button.linkish', { title: 'Restore the files and rewind the conversation to this point (both recorded as new history)', onclick: () => rewindBoth(e) }, 'rewind both'),
        h('button.linkish', { title: 'Start a branch session from here with its own copy of the workspace at this point', onclick: () => branchFrom(e.sequence - 1) }, 'branch'));
    }
    case 'diagnostics':
      return h('div.diagnostics', h('b', `${(ui.items || []).length} diagnostic(s)`), h('ul', (ui.items || []).slice(0, 8).map((d) => h('li.' + d.severity, `${d.file}${d.line ? `:${d.line}` : ''}${d.col ? `:${d.col}` : ''} ${d.severity}: ${d.message}`))));
    case 'process':
      return h('div.sys', `Process ${ui.id}${ui.name ? ` [${ui.name}]` : ''}: ${ui.state}${ui.exit_code != null ? ` (exit ${ui.exit_code})` : ''}${ui.command ? ` — ${ui.command}` : ''}`,
        ui.state === 'running' && h('button.linkish', { onclick: () => act(() => state.client.uiAction(sid, e.event_id, 'kill'), 'Stopping the process…') }, 'stop'));
    case 'diff': {
      const lines = ui.unified
        ? ui.unified.split('\n')
        : [...String(ui.before ?? '').split('\n').map((l) => '-' + l), ...String(ui.after ?? '').split('\n').map((l) => '+' + l)];
      return h('div.diff', h('div.p', ui.path || 'diff'), h('pre', lines.map((l) => h('div.' + (l[0] === '+' ? 'add' : l[0] === '-' ? 'del' : 'ctx'), l))));
    }
    case 'form': {
      const fields = (ui.fields || []).map((f) => {
        const input = f.options ? h('select', { name: f.name }, f.options.map((o) => h('option', { value: o }, o))) : h('input', { name: f.name, type: f.type || 'text' });
        return h('label', f.label || f.name, input);
      });
      const done = answered.get(e.event_id);
      return h('form.form-hint', { onsubmit: (ev) => { ev.preventDefault(); const values = Object.fromEntries(new FormData(ev.target)); act(() => state.client.uiAction(sid, e.event_id, 'submit', values), 'Submitted'); } },
        h('b', ui.title || 'Form'), fields, done ? h('span.pill.ok', 'Submitted') : h('button.btn.primary', { type: 'submit' }, ui.submit_label || 'Submit'));
    }
    case 'tool':
    case 'stream-chunk':
      return null;
    default:
      // Fallback: unknown hint kinds show the raw payload.
      return h('div.sys', `${e.event_name}: `, h('code', JSON.stringify(e.payload).slice(0, 160)));
  }
}

// ---- inspector ---------------------------------------------------------------------

function inspector() {
  const tabs = [
    ['harness', 'Harness'],
    ['pipeline', 'Pipeline'],
    ['context', 'Context'],
    ['graph', 'Graph'],
    ['config', 'Config'],
    ['log', 'Log'],
  ];
  let pane;
  try {
    pane = { harness: harnessPane, pipeline: pipelinePane, context: contextPane, graph: graphPane, config: configPane, log: logPane }[state.tab]();
  } catch (e) {
    console.error(e);
    pane = h('p.help', `This view failed to render: ${e.message}`);
  }
  return h('section.inspector', { 'aria-label': 'Inspector' },
    h('div.tabs', { role: 'tablist' }, tabs.map(([id, label]) => h('button', { role: 'tab', 'aria-selected': String(state.tab === id), onclick: () => { state.tab = id; state.resetPane = true; render(); } }, label))),
    h('div.pane', { role: 'tabpanel' }, pane));
}

function harnessPane() {
  const v = state.view;
  if (!v) return h('p.help', 'Select a session.');
  const x = harnessSummary(v, state.sessions);
  const sid = state.selected;
  const row = (k, val) => val == null || val === '' ? null : h('div.kv', h('span.k', k), h('span.v', val));
  const n = (v2) => (v2 || 0).toLocaleString();
  return h('div.harness',
    h('section', h('h4', 'Model'),
      row('provider · model', x.model ? `${x.model.provider || '?'} · ${x.model.model}` : 'no model call yet'),
      row('model requests', n(x.usage.model_requests)),
      row('tokens in / out', `${n(x.usage.input_tokens)} / ${n(x.usage.output_tokens)}`),
      row('cached input', x.usage.input_tokens ? `${n(x.usage.cached_tokens)} (${Math.round((100 * x.usage.cached_tokens) / x.usage.input_tokens)}%)` : n(x.usage.cached_tokens)),
      row('cost', x.usage.cost ? `$${x.usage.cost.toFixed(4)}` : null),
      x.context && row('last context', `≈${n(x.context.tokens)} tokens; ${x.context.tools_sent} tools sent (${n(x.context.tool_schema_tokens)} schema tokens), ${x.context.tools_deferred} on request`),
      (x.usage.elided || x.usage.dropped) ? row('compaction', `${x.usage.elided} old outputs elided, ${x.usage.dropped} messages summarized (projection only; the log is complete)`) : null,
      x.usage.retries ? row('provider retries', x.usage.retries) : null,
      x.recovery ? row('recovered invocations', x.recovery) : null),
    x.workspace && h('section', h('h4', 'Workspace'),
      row('root', x.workspace.root), row('mode', x.workspace.mode), row('environment', x.workspace.environment ? `${x.workspace.environment.kind} (${x.workspace.environment.id})` : null),
      row('git', x.workspace.git ? `${x.workspace.git.branch}@${(x.workspace.git.head || '').slice(0, 10)}${x.workspace.git.dirty ? `, ${x.workspace.git.dirty} changed` : ''}` : null),
      x.workspace.repos?.length ? row('repositories', x.workspace.repos.map((r) => `${r.path} (${r.branch || '?'})`).join(', ')) : null,
      x.files_changed.length ? row('files changed', x.files_changed.join(', ')) : null),
    x.plan && h('section', h('h4', 'Plan'), h('ul.plan-list', x.plan.map((i) => h('li.' + i.status, `${i.status === 'completed' ? '✓' : i.status === 'in_progress' ? '▸' : i.status === 'blocked' ? '!' : '○'} ${i.text}`)))),
    h('section', h('h4', 'Activity'), row('tool calls', `${x.tool_calls} (${x.tool_errors} errors)`), x.pending_calls.length ? row('waiting on', x.pending_calls.join(', ')) : null,
      x.approvals.filter((a) => !a.answered).map((a) => row('approval pending', `${a.tool}: ${a.reason}`)), x.budget_exhausted && row('budget', 'exhausted')),
    x.processes.length > 0 && h('section', h('h4', 'Processes'), x.processes.map((p) => h('div.kv', h('span.k', `${p.id}${p.name ? ` [${p.name}]` : ''}`), h('span.v', `${p.state}${p.exit_code != null ? ` (exit ${p.exit_code})` : ''} `, p.state === 'running' && h('button.linkish', { onclick: () => act(() => state.client.uiAction(sid, p.event_id, 'kill'), 'Stopping…') }, 'stop'))))),
    x.children.length > 0 && h('section', h('h4', 'Child agents'), x.children.map((c) => h('div.kv', h('button.linkish', { onclick: () => select(c.session_id) }, c.session_id), h('span.v', `${c.finished ? (c.error ? 'failed/cancelled' : 'finished') : c.activity} · ${c.workspace} workspace${c.title ? ` · ${c.title}` : ''}`)))),
    x.checkpoints.length > 0 && h('section', h('h4', 'Checkpoints (newest first)'), [...x.checkpoints].reverse().slice(0, 15).map((c) => h('div.kv', h('span.k', String(c.checkpoint).slice(0, 10)), h('span.v', `${c.reason} `,
      h('button.linkish', { onclick: () => act(() => state.client.uiAction(sid, c.event_id, 'restore'), 'Restoring files…') }, 'restore'), ' ',
      h('button.linkish', { onclick: () => branchFrom(c.sequence - 1) }, 'branch'))))),
    x.diagnostics.length > 0 && h('section', h('h4', 'Latest diagnostics'), h('ul', x.diagnostics.slice(0, 10).map((d) => h('li', `${d.file}${d.line ? `:${d.line}` : ''} ${d.severity}: ${d.message}`)))),
    x.decisions.length > 0 && h('section', h('h4', 'Recent policy decisions'), x.decisions.map((d) => h('div.kv', h('span.k', `${d.effect} ${d.tool}`), h('span.v', d.explanation || d.reason)))),
  );
}

function outcomeClass(inv) {
  const s = inv.outcome?.status;
  if (!s) return 'open';
  return s === 'ok' ? '' : s;
}

function pipelinePane() {
  const v = state.view;
  if (!v) return h('p.help', 'Select a session to inspect its log.');
  const tools = h('div.pane-tools',
    h('label', h('input', { type: 'checkbox', checked: state.hideChunks, onchange: (e) => { state.hideChunks = e.target.checked; render(); } }), 'Hide stream chunks'),
    h('label', h('input', { type: 'checkbox', checked: state.hideFrontend, onchange: (e) => { state.hideFrontend = e.target.checked; render(); } }), 'Hide web-ui'),
    h('span.legend', h('span', h('i', { style: 'background:var(--block)' }), 'blocking'), h('span', h('i', { style: 'background:var(--async)' }), 'async')),
  );
  const evs = v.events.filter((e) => !(state.hideChunks && e.event_name === 'stream-chunk'));
  const rows = evs.map((e) => eventRow(v, e));
  const hidden = v.events.length - evs.length;
  return [
    h('p.help', `${v.records} records in ${v.session_id}. Each row is an event; the chips are its pipeline: blocking subscribers in declared order, then async observers. Click a row for its full record.`),
    tools,
    hidden ? h('p.help', `${hidden} stream chunks hidden.`) : null,
    rows,
    (v.blocked || []).map((b) => h('div.blocked', h('b', `#${b.sequence} blocked `), `${b.plugin} → ${b.event_name}: ${b.reason}`)),
  ];
}

function eventRow(v, e) {
  const open = state.expanded.has(e.event_id);
  const invs = e.invocations.filter((i) => !(state.hideFrontend && i.plugin === 'web-ui'));
  const blocking = invs.filter((i) => i.mode === 'blocking');
  const asyncs = invs.filter((i) => i.mode === 'async');
  const chip = (i) => h('span.chip.' + (i.mode === 'blocking' ? 'b' : 'a') + (outcomeClass(i) ? '.' + outcomeClass(i) : ''), { title: `${i.invocation_id} · ${i.outcome?.status || 'running'}${i.attempts > 1 ? ` · attempt ${i.attempts}` : ''}` }, i.plugin + (i.attempts > 1 ? ` ×${i.attempts}` : ''));
  const chain = [];
  blocking.forEach((i, n) => { if (n) chain.push(h('span.arrow', '→')); chain.push(chip(i)); });
  if (asyncs.length) { if (blocking.length) chain.push(h('span.fork', '⋮')); asyncs.forEach((i) => chain.push(chip(i))); }
  const statusKind = { delivered: 'ok', vetoed: 'bad', failed: 'bad', aborted: 'bad', running: 'blk', queued: 'wait' }[e.status];
  return h('div.ev' + (open ? '.open' : ''), { id: `ev-${e.event_id}`, onclick: (ev) => { if (ev.target.closest('button, a, pre')) return; toggle(e.event_id); } },
    h('div.ev-head', h('span.ev-seq', `#${e.sequence}`), h('span.ev-name', e.event_name), e.lane === 'priority' && h('span.ev-lane', 'priority'), h('span.ev-status', h('span.pill.' + (statusKind || ''), e.status))),
    chain.length ? h('div.chain', chain) : null,
    open && eventDetail(v, e));
}

function toggle(id) {
  if (state.expanded.has(id)) state.expanded.delete(id);
  else state.expanded.add(id);
  render();
}

function jump(eventId) {
  if (!eventId) return;
  const sid = eventId.split('/')[0];
  const go = async () => {
    if (sid !== state.selected) await select(sid);
    const v = state.view;
    const ev = v.events.find((e) => e.event_id === eventId || e.invocations.some((i) => i.invocation_id === eventId));
    if (ev) {
      state.expanded.add(ev.event_id);
      state.flash = ev.event_id;
      if (ev.event_name === 'stream-chunk') state.hideChunks = false;
      state.tab = 'pipeline';
      render();
    }
  };
  go();
}

function causeText(c) {
  if (c.kind === 'invocation') return h('button.linkish', { onclick: () => jump(c.invocation_id) }, c.invocation_id);
  if (c.kind === 'root') return `root (start-session by ${c.plugin})`;
  return `core (${c.reason})`;
}

function originText(o) {
  switch (o.kind) {
    case 'pipeline': return `pipeline output of ${o.plugin}`;
    case 'deferred': return h('span', `deferred publish by ${o.plugin}, citing `, h('button.linkish', { onclick: () => jump(o.cites) }, o.cites));
    case 'start': return `initial event of start-session by ${o.plugin}`;
    case 'cross-session': return h('span', `cross-session publish from `, h('button.linkish', { onclick: () => select(o.from_session) }, o.from_session), ` by ${o.plugin}`);
    default: return 'core lifecycle event';
  }
}

function json(v) {
  return h('pre.json', JSON.stringify(v, null, 2));
}

function eventDetail(v, e) {
  const sid = v.session_id;
  return h('div.ev-detail',
    h('dl.kv',
      h('dt', 'event'), h('dd.mono', e.event_id),
      h('dt', 'cause'), h('dd', causeText(e.cause)),
      h('dt', 'origin'), h('dd', originText(e.origin)),
      h('dt', 'depth'), h('dd', String(e.depth)),
      e.settlement && [h('dt', 'settled'), h('dd', e.settlement.status + (e.settlement.reason ? `: ${e.settlement.reason}` : '') + (e.settlement.error ? `: ${e.settlement.error}` : ''))]),
    h('div', h('b', 'Payload as published'), json(e.payload)),
    e.ui && h('div', h('b', 'UI hint'), json(e.ui)),
    e.invocations.map((i) => h('div.inv' + (i.mode === 'async' ? '.async' : ''),
      h('h4', i.plugin, h('span.pill.' + (i.outcome?.status === 'ok' ? 'ok' : i.outcome ? 'bad' : 'blk'), i.outcome?.status || 'running'),
        h('small', `${i.invocation_id} · ${i.mode} · pos ${i.position}${i.attempts > 1 ? ` · ${i.attempts} attempts` : ''}${i.completed_at ? ` · ${i.completed_at - i.started_at} ms` : ''}`)),
      h('dl.kv',
        h('dt', 'stamp'), h('dd.mono', `${i.stamp.binary} / ${i.stamp.config}`),
        i.outcome?.reason && [h('dt', 'reason'), h('dd', i.outcome.reason)],
        i.outcome?.error && [h('dt', 'error'), h('dd', i.outcome.error)],
        i.published.length > 0 && [h('dt', 'published'), h('dd', i.published.map((p) => [h('button.linkish', { onclick: () => jump(p) }, p), ' ']))],
        i.contributions.length > 0 && [h('dt', 'context'), h('dd', json(i.contributions))],
        i.transform && [h('dt', 'transform'), h('dd', json(i.transform))],
        i.mode === 'blocking' && JSON.stringify(i.input) !== JSON.stringify(e.payload) && [h('dt', 'input'), h('dd', json(i.input))]))),
    h('div.row-actions',
      h('button.btn.small', { onclick: () => newSession(v.definition, undefined, { session_id: sid, sequence: e.sequence }), title: 'Start a new session whose context is this session\'s context as of this record' }, 'Branch from here'),
      h('button.btn.small', { onclick: () => act(() => state.client.contextEdit(sid, [{ op: 'restore', to_sequence: e.sequence }]), `Context restored to record #${e.sequence}. The restore is itself a new record.`) }, 'Restore context to here')));
}

function contextPane() {
  const v = state.view;
  if (!v) return h('p.help', 'Select a session.');
  const sid = v.session_id;
  const slot = h('select', ['system', 'memory', 'messages', 'note'].map((s) => h('option', s)));
  const text = h('input', { placeholder: 'Text to add', 'aria-label': 'Context text' });
  const seq = h('input', { type: 'number', min: 0, placeholder: 'Record #', 'aria-label': 'Record number', style: 'max-width:110px' });
  return [
    h('p.help', 'The context is an attributed fold over contributions in the log. Edits are published as a context-edit event, so every change is a new record and every earlier state stays reconstructable.'),
    v.context.length ? v.context.map((c) => h('div.ctx-item',
      h('span.ctx-slot', c.slot),
      h('span.ctx-val', typeof c.value === 'string' ? c.value : c.value?.content ? `${c.value.role}: ${c.value.content}` : c.value?.name ? `${c.value.name} — ${c.value.description || ''}` : JSON.stringify(c.value), h('span.by', `${c.plugin} · ${c.id}`)),
      h('button.btn.small.ghost', { title: 'Remove this item', onclick: () => act(() => state.client.contextEdit(sid, [{ op: 'remove', id: c.id }]), 'Removed. The model sees the change on its next request.') }, 'Remove'))) : h('p.help', 'Empty.'),
    h('form.inline-form', { onsubmit: (e) => { e.preventDefault(); if (!text.value.trim()) return; const val = slot.value === 'messages' ? { role: 'user', content: text.value } : text.value; act(() => state.client.contextEdit(sid, [{ op: 'add', slot: slot.value, value: val }]), 'Added to context.'); text.value = ''; } },
      h('h3', 'Add an item'), h('div.r', slot, text, h('button.btn', { type: 'submit' }, 'Add'))),
    h('form.inline-form', { onsubmit: (e) => { e.preventDefault(); const n = Number(seq.value); if (!Number.isFinite(n)) return; act(() => state.client.contextEdit(sid, [{ op: 'restore', to_sequence: n }]), `Context restored to record #${n}.`); } },
      h('h3', 'Restore'), h('p.help', 'Set the context to its state as of an earlier record. Restoring again to a later record undoes it.'), h('div.r', seq, h('button.btn', { type: 'submit' }, 'Restore'))),
  ];
}

function graphPane() {
  const g = state.graph;
  if (!g) return h('p.help', 'No compiled config.');
  const defs = Object.keys(g.definitions);
  const name = state.graphDef && g.definitions[state.graphDef] ? state.graphDef : defs[0];
  const d = g.definitions[name];
  const plugins = [...new Set([...d.plugins, ...d.edges.filter((x) => x.kind === 'external').map((x) => x.from), 'core'])];
  const events = [...new Set(d.edges.map((x) => (x.kind === 'consumes' ? x.from : x.to)).filter((x) => x !== '*'))].sort();
  const cell = (ev, p) => {
    const em = d.edges.find((x) => (x.kind === 'emits' || x.kind === 'external') && x.from === p && x.to === ev);
    const pipe = d.pipelines[ev] || d.fallback;
    const bi = pipe.blocking.findIndex((s) => s.plugin === p);
    const as = pipe.async.some((s) => s.plugin === p);
    const parts = [];
    if (em) parts.push(h('span.cell-e', { title: `emits ${em.keys.join(', ')}${em.deferred ? ' (deferred allowed)' : ''}` }, em.deferred ? 'E*' : 'E'));
    if (bi >= 0) parts.push(h('span.cell-b', { title: `blocking, position ${bi + 1}` }, `B${bi + 1}`));
    if (as) parts.push(h('span.cell-a', { title: 'async observer' }, 'A'));
    return h('td', parts);
  };
  return [
    h('div.pane-tools', h('label', 'Definition ', h('select', { onchange: (e) => { state.graphDef = e.target.value; render(); } }, defs.map((x) => h('option', { selected: x === name }, x))))),
    h('p.help', `Compiled config ${g.hash}. The graph is a compile-time artifact: every consumer is matched to an emitter and every demanded key to a supplied one. E = emits (E* may publish deferred), B1… = blocking position, A = async.`),
    h('div.matrix-wrap', h('table.matrix', h('thead', h('tr', h('th', ''), plugins.map((p) => h('th', p)))), h('tbody', events.map((ev) => h('tr', h('th', ev), plugins.map((p) => cell(ev, p))))))),
    d.cycles.length > 0 && h('p.help', { style: 'margin-top:10px' }, `Cycles (bounded at runtime by max causal depth ${g.config.runtime.max_causal_depth}): ${d.cycles.map((c) => c.join(' → ')).join('; ')}`),
    h('h3', 'Diagnostics'),
    g.diagnostics.length ? g.diagnostics.map((x) => h('div.diag.' + x.severity, h('b', x.code), ' ', x.message)) : h('p.help', 'None.'),
    h('h3', 'External surface'),
    h('p.help', 'Every way into the deployment: plugins holding deferred-publish, start-session, cross-session, control, or observe.'),
    g.surface.map((s) => h('div.diag', h('b', s.plugin), ` ${s.capability}${s.deferred_events?.length ? ` (${s.deferred_events.join(', ')})` : ''}`)),
  ];
}

// ---- config: live, layered, stamped -----------------------------------------------

function draft() {
  if (!state.draftConfig) state.draftConfig = structuredClone(state.config?.config || {});
  return state.draftConfig;
}

function presets(cfg) {
  const has = (n) => !!cfg.plugins?.[n];
  const chatSubs = () => cfg.definitions.chat.subscribers;
  return [
    has('approval-gate') && ['Approve every tool', () => { cfg.plugins['approval-gate'].config = { require: '*' }; return 'approval-gate guards every tool'; }],
    has('heartbeat') && ['Heartbeat every 5 s', () => { cfg.plugins.heartbeat.config = { ...(cfg.plugins.heartbeat.config || {}), every_seconds: 5 }; return 'heartbeat fires every 5 s into its journal session'; }],
    has('redactor') && ['Remove redactor', () => { cfg.definitions.chat.subscribers = chatSubs().filter((s) => s.plugin !== 'redactor'); return 'redactor removed from chat'; }],
    ['Break it (unknown plugin)', () => { chatSubs().push({ plugin: 'does-not-exist' }); return 'added a subscriber that does not exist; apply to see the whole config rejected'; }],
  ].filter(Boolean);
}

const openPlugins = new Set();

function configPane() {
  if (!state.config?.config) return h('p.help', 'No config.');
  const cfg = draft();
  const sid = state.selected;
  const defName = state.view?.definition && cfg.definitions[state.view.definition] ? state.view.definition : Object.keys(cfg.definitions)[0];
  const def = cfg.definitions[defName];
  const manifests = state.graph?.manifests || {};
  const members = def.subscribers;
  const move = (i, d) => { const j = i + d; if (j < 0 || j >= members.length) return; [members[i], members[j]] = [members[j], members[i]]; render(); };
  const notMembers = Object.keys(cfg.plugins).filter((p) => !members.some((m) => m.plugin === p));
  const addSel = h('select', { 'aria-label': 'Plugin to add' }, notMembers.map((p) => h('option', p)));

  const apply = async (scope) => {
    const r = await act(() => state.client.applyConfig(cfg, scope));
    if (!r) return;
    state.applyResult = r;
    if (r.ok) {
      state.draftConfig = null;
      toast(scope.kind === 'session' ? `Applied to ${scope.session_id} at its next event boundary.` : 'Applied. Sessions switch at their next event boundary; nothing restarts.');
    } else toast('Rejected whole: nothing changed. See the diagnostics.', true);
    await refreshAll();
    render();
  };

  return [
    h('p.help', `Active config ${state.config.hash}. Changes compile first; a config that fails is rejected whole. Applied configs take effect at each session's next event boundary while sessions keep running. Every invocation records the code and config hashes it ran with.`),
    h('div.presets', presets(cfg).map(([label, fn]) => h('button.btn.small', { onclick: () => { toast(fn()); render(); } }, label))),
    h('h3', `Definition: ${defName}`),
    h('p.help', 'Blocking subscribers run in this order. Mode overrides each plugin\'s default.'),
    members.map((m, i) => h('div.member',
      h('span.mono', String(i + 1)),
      h('span.mono', m.plugin),
      h('select', { 'aria-label': `Mode for ${m.plugin}`, onchange: (e) => { if (e.target.value) m.mode = e.target.value; else delete m.mode; } },
        h('option', { value: '' }, 'default'), h('option', { value: 'blocking', selected: m.mode === 'blocking' }, 'blocking'), h('option', { value: 'async', selected: m.mode === 'async' }, 'async')),
      h('span.mv', h('button.btn.small.ghost', { onclick: () => move(i, -1), 'aria-label': 'Move up' }, '↑'), h('button.btn.small.ghost', { onclick: () => move(i, 1), 'aria-label': 'Move down' }, '↓'), h('button.btn.small.ghost', { onclick: () => { members.splice(i, 1); render(); }, 'aria-label': `Remove ${m.plugin}` }, '✕')))),
    notMembers.length > 0 && h('div.inline-form', h('div.r', addSel, h('button.btn', { onclick: () => { members.push({ plugin: addSel.value }); render(); } }, 'Add to definition'))),
    h('h3', 'Plugins'),
    Object.entries(cfg.plugins).map(([name, p]) => {
      const ta = h('textarea', { 'aria-label': `${name} config`, spellcheck: 'false', onchange: (e) => { try { p.config = JSON.parse(e.target.value || 'null'); e.target.style.borderColor = ''; } catch { e.target.style.borderColor = 'var(--bad)'; } } }, JSON.stringify(p.config ?? {}, null, 2));
      return h('details.plug', { open: openPlugins.has(name), ontoggle: (e) => (e.target.open ? openPlugins.add(name) : openPlugins.delete(name)) }, h('summary', h('span.pn', name), p.disabled ? h('span.pill', 'native only, disabled here') : null, h('span.pd', manifests[name]?.description || '')),
        h('div.body', ta, h('span.help', `${(manifests[name]?.capabilities || []).join(', ') || 'no capabilities'} · ${p.module || p.command?.join(' ')}`)));
    }),
    state.applyResult && h('div', h('h3', state.applyResult.ok ? `Applied ${state.applyResult.hash}` : 'Rejected'),
      (state.applyResult.diagnostics || []).filter((x) => x.severity !== 'info').map((x) => h('div.diag.' + x.severity, h('b', x.code), ' ', x.message)),
      state.applyResult.skipped?.length > 0 && h('p.help', `Skipped (definition missing): ${state.applyResult.skipped.join(', ')}`)),
    h('div.apply-bar',
      h('button.btn.primary', { onclick: () => apply({ kind: 'global' }) }, 'Apply to all sessions'),
      sid && h('button.btn', { onclick: () => apply({ kind: 'session', session_id: sid }) }, `Apply to ${sid} only`),
      h('button.btn.ghost', { onclick: () => { state.draftConfig = null; state.applyResult = null; render(); } }, 'Discard changes')),
  ];
}

function logPane() {
  const v = state.view;
  if (!v) return h('p.help', 'Select a session.');
  const area = h('div', h('p.help', 'Loading records…'));
  state.client.getRecords(v.session_id).then((records) => {
    area.innerHTML = '';
    area.append(
      h('p.help', `The raw append-only log of ${v.session_id}: ${records.length} typed records. This is the source of truth; everything else on this page is derived from it.`),
      ...records.slice(-400).map((r) => {
        const { session_id, sequence, at, type, ...rest } = r;
        return h('div.rec', h('b', `#${sequence} ${type}`), ' ', JSON.stringify(rest));
      }),
    );
  });
  return area;
}

// ---------------------------------------------------------------------------

async function start() {
  // The Linux sandbox's opt-in cross-origin isolation may need one reload.
  if (await ensureIsolation()) return;
  const params = new URLSearchParams(location.search);
  const runtime = params.get('runtime');
  // Dev/test hook: per-tab plugin settings, ?set=<plugin>.<key>=<value> (repeatable).
  const sets = params.getAll('set');
  if (sets.length) {
    const o = {};
    for (const kv of sets) {
      const m = kv.match(/^([\w-]+)\.([\w-]+)=(.*)$/);
      if (m) (o[m[1]] ||= {})[m[2]] = m[3];
    }
    sessionStorage.setItem(`${SETTINGS}.override`, JSON.stringify(o));
  }
  if (params.get('host') === 'browser') return connect('browser');
  if (runtime) {
    state.liveUrl = runtime;
    return connect('live');
  }
  // Served by a native runtime's gateway? Attach to it.
  try {
    const r = await fetch('api/info', { signal: AbortSignal.timeout(800) });
    if (r.ok && (await r.json()).mode === 'live') {
      state.liveUrl = location.origin;
      return connect('live');
    }
  } catch { /* static hosting */ }
  return connect('browser');
}

start();
