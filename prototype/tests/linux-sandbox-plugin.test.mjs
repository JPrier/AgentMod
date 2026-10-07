// The linux-sandbox plugin file the browser loads, run as a plugin process over
// the AgentMod wire protocol (JSON-RPC lines) with the fake CheerpX module:
// handshake, tool offer, a tool call answered with publishes, and calls for
// other plugins' tools ignored.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import readline from 'node:readline';
import { spawn } from 'node:child_process';
import { pathToFileURL, fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const FAKE = pathToFileURL(path.join(here, 'fake-cheerpx.mjs')).href;
// What a cross-origin-isolated page gives a worker, as far as the plugin checks.
const GLOBALS = `data:text/javascript,${encodeURIComponent('globalThis.crossOriginIsolated = true; globalThis.indexedDB ??= {};')}`;

function startPlugin() {
  const child = spawn(process.execPath, ['--import', GLOBALS, path.join(here, '..', 'plugins', 'linux-sandbox', 'main.js')], { stdio: ['pipe', 'pipe', 'inherit'] });
  const pending = new Map();
  const publishes = [];
  let id = 1;
  readline.createInterface({ input: child.stdout }).on('line', (line) => {
    const msg = JSON.parse(line);
    if (msg.method === 'publish') {
      publishes.push(msg.params);
      child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { event_id: `e${publishes.length}` } }) + '\n');
    } else if (msg.method === undefined) {
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
  const base = fs.mkdtempSync(path.join(os.tmpdir(), 'sbx-plugin-'));
  const ws = path.join(base, 'workspace');
  const config = { cheerpx_url: FAKE, in_path: path.join(base, 'in'), out_path: path.join(base, 'out'), workspace_path: ws };
  const p = startPlugin();
  try {
    const init = await p.call('initialize', { protocol: 'agentmod/0.1', plugin: 'linux-sandbox', config });
    const m = init.result.manifest;
    assert.equal(m.name, 'linux-sandbox');
    assert.deepEqual(m.consumes.map((c) => c.event), ['session-started', 'config-applied', 'tool-call']);
    assert.equal(m.consumes[2].mode, 'async');
    assert.deepEqual(m.emits.map((e) => e.event), ['tool-result', 'workspace-change', 'workspace-status']);

    const started = await p.call('invoke', { invocation_id: 'i1', attempt: 1, mode: 'blocking', event: event('session-started', {}, 1) });
    const adds = started.result.contributions;
    assert.deepEqual(adds.filter((c) => c.slot === 'tools').map((c) => c.value.name), ['run', 'read_file', 'write_file', 'edit_file', 'list_files', 'import_repo']);
    assert.match(adds.find((c) => c.slot === 'system').value, new RegExp(`Work in ${ws}`));

    const run = await p.call('invoke', { invocation_id: 'i2', attempt: 1, mode: 'async', event: event('tool-call', { call_id: 'c1', name: 'run', args: { command: 'echo hi from the vm' } }, 2) });
    assert.equal(run.result.error, undefined);
    const names = p.publishes.map((x) => `${x.event_name}:${x.payload.state ?? x.payload.name}`);
    assert.deepEqual(names, ['workspace-status:booting', 'workspace-status:ready', 'tool-result:run']);
    assert.ok(p.publishes.every((x) => x.invocation_id === 'i2'), 'all publishes are pipeline outputs of the invocation');
    const result = p.publishes[2].payload;
    assert.equal(result.call_id, 'c1');
    assert.equal(result.error, false);
    assert.match(result.output, /hi from the vm/);
    assert.equal(p.publishes[2].ui.kind, 'tool');

    p.publishes.length = 0;
    await p.call('invoke', { invocation_id: 'i3', attempt: 1, mode: 'async', event: event('tool-call', { call_id: 'c2', name: 'write_file', args: { path: 'a.txt', content: 'one\n' } }, 3) });
    assert.deepEqual(p.publishes.map((x) => x.event_name), ['workspace-change', 'tool-result']);
    assert.equal(p.publishes[0].ui.kind, 'diff');
    assert.match(p.publishes[0].payload.unified, /\+one/);
    assert.equal(fs.readFileSync(path.join(ws, 'a.txt'), 'utf8'), 'one\n');

    p.publishes.length = 0;
    const other = await p.call('invoke', { invocation_id: 'i4', attempt: 1, mode: 'async', event: event('tool-call', { call_id: 'c3', name: 'calc', args: {} }, 4) });
    assert.deepEqual(other.result.contributions, []);
    assert.equal(p.publishes.length, 0, 'other plugins’ tools are not answered');
  } finally {
    p.child.kill();
  }
});
