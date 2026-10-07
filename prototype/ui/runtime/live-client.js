// Client for a native runtime's `web-ui` gateway plugin (HTTP + Server-Sent Events).
// Exposes the same surface as BrowserRuntime so the UI is host-agnostic.

export class LiveClient {
  constructor(baseUrl) {
    this.base = baseUrl.replace(/\/$/, '');
    this.mode = 'live';
    this.listeners = new Set();
  }

  async req(path, body) {
    const res = await fetch(this.base + path, body === undefined ? {} : { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
    const data = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    if (!res.ok || (data && data.error && Object.keys(data).length === 1)) throw new Error(data?.error || `HTTP ${res.status}`);
    return data;
  }

  async boot() {
    const info = await this.req('/api/info');
    this.es = new EventSource(this.base + '/api/stream');
    this.es.addEventListener('record', (e) => {
      const r = JSON.parse(e.data);
      for (const l of this.listeners) l(r);
    });
    return info;
  }

  close() {
    this.es?.close();
  }

  onRecord(cb) {
    this.listeners.add(cb);
    return () => this.listeners.delete(cb);
  }

  info() { return this.req('/api/info'); }
  listSessions() { return this.req('/api/sessions'); }
  getSession(id) { return this.req(`/api/sessions/${id}`); }
  getRecords(id) { return this.req(`/api/sessions/${id}/records`); }
  getContext(id, sequence) { return this.req(`/api/sessions/${id}/context${sequence != null ? `?sequence=${sequence}` : ''}`); }
  getGraph() { return this.req('/api/graph'); }
  getConfig() { return this.req('/api/config'); }
  startSession(body) { return this.req('/api/sessions', body); }
  sendMessage(id, text, lane = 'normal') { return this.req(`/api/sessions/${id}/messages`, { text, lane }); }
  uiAction(id, reply_to, action, values = {}) { return this.req(`/api/sessions/${id}/actions`, { reply_to, action, values }); }
  contextEdit(id, ops) { return this.req(`/api/sessions/${id}/context-edit`, { ops }); }
  sendCommand(id, command) { return this.req(`/api/sessions/${id}/commands`, { command }); }
  applyConfig(config, scope) { return this.req('/api/config/apply', { config, scope }); }

  async exportLogs() {
    const sessions = {};
    for (const s of await this.listSessions()) sessions[s.session_id] = await this.getRecords(s.session_id);
    return { sessions };
  }
}
