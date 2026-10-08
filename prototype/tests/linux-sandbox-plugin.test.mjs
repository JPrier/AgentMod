// The linux-sandbox plugin file the browser loads, run as a plugin process over
// the AgentMod wire protocol (JSON-RPC lines). The test plays the browser host:
// it answers `publish` and routes `device` requests to the real page-side
// `linux-vm` device (with the fake CheerpX module). Covers the handshake, the
// tool offer, tool calls answered with publishes, and other tools ignored.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { fakeVmConfig, hostDevices } from './stage.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));

function startPlugin(devices) {
  const child = spawn(process.execPath, [path.join(here, '..', 'plugins', 'linux-sandbox', 'main.js')], { stdio: ['pipe', 'pipe', 'inherit'] });
  let manifest = null;
  const pending = new Map();
  const publishes = [];
  let id = 1;
  readline.createInterface({ input: child.stdout }).on('line', (line) => {
    const msg = JSON.parse(line);
    const reply = (result, error) => child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: msg.id, ...(error ? { error } : { result }) }) + '\n');
    if (msg.method === 'publish') {
      publishes.push(msg.params);
      reply({ event_id: `e${publishes.length}` });
    } else if (msg.method === 'query') {
      const q = msg.params;
      if (q.what === 'context') reply([]);
      else if (q.what === 'session') reply({ events: [{ event_id: 's1/e1', sequence: 1, event_name: 'session-started', payload: {} }] });
      else reply(null);
    } else if (msg.method === 'device') {
      devices.call('linux-sandbox', manifest, msg.params).then((r) => reply(r), (e) => reply(null, { code: e.code ?? -32001, message: e.message }));
    } else if (msg.method === undefined) {
      if (msg.result?.manifest) manifest = msg.result.manifest;
      pending.get(msg.id)?.(msg);
      pending.delete(msg.id);
    }
  });
  const call = (method, params) => new Promise((resolve) => {
    const n = id++;
    pending.set(n, resolve);
    child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: n, method, params }) + '\n');
  });
  return { child, call, publishes };
}

const event = (event_name, payload, n) => ({ event_id: `s1.e${n}`, session_id: 's1', event_name, sequence: n, payload, context: [] });

test('linux-sandbox speaks the plugin protocol and answers its tools', async () => {
  // The page the device runs on is cross-origin isolated.
  globalThis.crossOriginIsolated = true;
  globalThis.indexedDB ??= {};
  const config = fakeVmConfig();
  const ws = config.workspace_path;
  const devices = await hostDevices();
  const p = startPlugin(devices);
  try {
    const init = await p.call('initialize', { protocol: 'agentmod/0.1', plugin: 'linux-sandbox', config });
    const m = init.result.manifest;
    assert.equal(m.name, 'linux-sandbox');
    assert.deepEqual(m.devices, ['linux-vm']);
    assert.deepEqual(m.consumes.map((c) => c.event), ['session-started', 'config-applied', 'tool-call', 'ui-action']);
    assert.equal(m.consumes[2].mode, 'async');
    assert.deepEqual(m.emits.map((e) => e.event), ['tool-result', 'workspace-info', 'workspace-change', 'checkpoint-created', 'workspace-restored', 'process-started', 'process-exited', 'diagnostics', 'workspace-status']);

    const started = await p.call('invoke', { invocation_id: 'i1', attempt: 1, mode: 'blocking', event: event('session-started', {}, 1) });
    const adds = started.result.contributions;
    const offered = adds.filter((c) => c.slot === 'tools').map((c) => c.value);
    assert.deepEqual(offered.filter((t) => t.tier === 'core').map((t) => t.name), ['shell', 'process', 'read_file', 'list_dir', 'search_files', 'search_text', 'apply_patch']);
    assert.deepEqual(offered.filter((t) => t.tier === 'deferred').map((t) => t.name), ['view_image', 'repo_map', 'checkpoints', 'import_repo', 'sandbox_status', 'sandbox_logs', 'sandbox_restart', 'sandbox_stop', 'adopt_changes']);
    assert.match(adds.find((c) => c.slot === 'system').value, new RegExp(`Work in ${ws}`));

    const run = await p.call('invoke', { invocation_id: 'i2', attempt: 1, mode: 'async', event: event('tool-call', { call_id: 'c1', name: 'shell', args: { command: 'echo hi from the vm' } }, 2) });
    assert.equal(run.result.error, undefined);
    const names = p.publishes.map((x) => `${x.event_name}:${x.payload.state ?? x.payload.name ?? ''}`);
    assert.deepEqual(names, ['workspace-status:booting', 'workspace-status:ready', 'workspace-info:', 'tool-result:shell']);
    assert.ok(p.publishes.every((x) => x.invocation_id === 'i2'), 'all publishes are pipeline outputs of the invocation');
    assert.equal(p.publishes[2].payload.root, ws);
    assert.equal(p.publishes[2].payload.environment.kind, 'linux-vm');
    const result = p.publishes[3].payload;
    assert.equal(result.call_id, 'c1');
    assert.equal(result.error, false);
    assert.equal(result.exit_code, 0);
    assert.match(result.output, /hi from the vm/);
    assert.equal(p.publishes[3].ui.kind, 'tool');
    // The first tool call records the session's workspace in context.
    assert.equal(run.result.contributions.find((c) => c.slot === 'workspace').value.root, ws);

    p.publishes.length = 0;
    await p.call('invoke', { invocation_id: 'i3', attempt: 1, mode: 'async', event: event('tool-call', { call_id: 'c2', name: 'apply_patch', args: { changes: [{ action: 'create', path: 'a.txt', content: 'one\n' }] } }, 3) });
    assert.deepEqual(p.publishes.map((x) => x.event_name), ['checkpoint-created', 'workspace-change', 'tool-result']);
    assert.equal(p.publishes[1].ui.kind, 'diff');
    assert.match(p.publishes[1].payload.unified, /\+one/);
    assert.equal(fs.readFileSync(path.join(ws, 'a.txt'), 'utf8'), 'one\n');
    assert.match(p.publishes[2].payload.checkpoint, /^[0-9a-f]{40}$/);

    p.publishes.length = 0;
    const other = await p.call('invoke', { invocation_id: 'i4', attempt: 1, mode: 'async', event: event('tool-call', { call_id: 'c3', name: 'calc', args: {} }, 4) });
    assert.deepEqual(other.result.contributions, []);
    assert.equal(p.publishes.length, 0, 'other plugins’ tools are not answered');
  } finally {
    p.child.kill();
    await devices.dispose();
  }
});
