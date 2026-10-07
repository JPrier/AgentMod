// The browser sandbox below the plugin: the host's `linux-vm` device (the
// CheerpX driver, against a fake CheerpX module) and the plugin's target that
// drives it across a JSON boundary. Verifies device access control, the boot
// sequence, the command wrapper, byte channels both ways, batched (tar)
// writes, and serialization.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { linuxVmTarget } from '../plugins/linux-sandbox/target.js';
import { workspaceTools } from '../plugins/sdk/workspace-tools.js';
import { FAKE_CHEERPX, fakeVmConfig, hostDevices, jsonHost, stageSite } from './stage.mjs';

test('devices are only lent to plugins that declare them', async () => {
  const devices = await hostDevices();
  await assert.rejects(devices.call('tool-calc', { devices: [] }, { device: 'linux-vm', op: 'state' }), /does not declare the `linux-vm` device/);
  await assert.rejects(devices.call('x', { devices: ['linux-vm'] }, { device: 'linux-vm', op: 'dispose' }), /no operation `dispose`/);
  await assert.rejects(devices.call('x', { devices: ['gpu'] }, { device: 'gpu', op: 'state' }), /unknown host device/);
});

test('the VM refuses to boot without cross-origin isolation', async () => {
  globalThis.crossOriginIsolated = false;
  const { createCheerpxVm, unavailableReason } = await import(pathToFileURL(path.join(stageSite(), 'runtime', 'devices', 'cheerpx-vm.js')).href);
  assert.match(unavailableReason(), /cross-origin isolated/);
  await assert.rejects(createCheerpxVm({ cheerpx_url: FAKE_CHEERPX }).boot(), /Linux sandbox unavailable: the page is not cross-origin isolated/);
  const target = linuxVmTarget({ host: jsonHost(await hostDevices()), config: fakeVmConfig() });
  await assert.rejects(target.ensureReady({ status: () => {} }), /Linux sandbox unavailable/);
});

test('boots once, runs commands, and moves files both ways', async () => {
  globalThis.crossOriginIsolated = true;
  globalThis.indexedDB ??= {};
  const fake = await import(FAKE_CHEERPX);
  const config = { ...fakeVmConfig(), image: 'wss://example.test/fail-wss.ext2' };
  const ws = config.workspace_path;
  const devices = await hostDevices();
  const target = linuxVmTarget({ host: jsonHost(devices), config });
  const tools = workspaceTools({ target, root: ws, importRepos: false });
  const statuses = [];
  const status = (s, m) => statuses.push([s, m]);
  await Promise.all([target.ensureReady({ status }), target.ensureReady({ status })]);
  await target.ensureReady({ status });
  assert.deepEqual(statuses.map((s) => s[0]), ['booting', 'ready'], 'boots exactly once');
  assert.match(statuses[1][1], /Linux sandbox ready/);
  // A second plugin worker (e.g. after a restart) finds the VM already running.
  const again = [];
  await linuxVmTarget({ host: jsonHost(devices), config }).ensureReady({ status: (s) => again.push(s) });
  assert.deepEqual(again, [], 'no second boot');
  // The wss: image fell back to https: (as WebVM does).
  assert.ok(fake.created.some((d) => d.url === 'https://example.test/fail-wss.ext2'));
  const mounts = fake.Linux.last.mounts.map((m) => `${m.type}:${m.path}`);
  assert.deepEqual(mounts.slice(0, 1), ['ext2:/']);
  assert.ok(mounts.includes(`dir:${ws}`));
  // Separate stores for the OS overlay, the workspace, and scratch I/O.
  const idbs = fake.created.filter((d) => d.kind === 'idb').map((d) => d.name);
  assert.ok(idbs.some((n) => n.startsWith('agentmod-root-')));
  assert.ok(idbs.includes(`agentmod-workspace-${config.workspace}`));

  let r = await tools.call('write_file', { path: 'hello.c', content: '#include <stdio.h>\nint main(void){puts("hi");return 0;}\n' });
  assert.equal(r.error, undefined, r.output);
  assert.ok(fs.readFileSync(path.join(ws, 'hello.c'), 'utf8').includes('puts("hi")'));
  r = await tools.call('edit_file', { path: 'hello.c', old_text: '"hi"', new_text: '"it\'s $HOME ✓"' });
  assert.equal(r.error, undefined, r.output);
  r = await tools.call('read_file', { path: 'hello.c' });
  assert.match(r.output, /it's \$HOME ✓/);
  r = await tools.call('run', { command: 'printf out; printf err >&2; exit 4' });
  assert.match(r.output, /^exit code 4\n--- stdout ---\nout\n--- stderr ---\nerr$/);
  // Output from an earlier command never leaks into a later one.
  r = await tools.call('run', { command: 'true' });
  assert.equal(r.output, 'exit code 0\n(no output)');
  r = await tools.call('run', { command: 'pwd', cwd: path.join(ws, 'nope') });
  assert.equal(r.error, true);
  assert.match(r.output, /exit code 126/);
  r = await tools.call('run', { command: 'sleep 4', timeout_seconds: 1 });
  assert.match(r.output, /timed out after 1s/);
  r = await tools.call('list_files', {});
  assert.match(r.output, /hello\.c/);
  r = await tools.call('read_file', { path: 'missing' });
  assert.match(r.output, /does not exist/);
  // Binary bytes survive the base64 round-trip.
  const bin = Uint8Array.from({ length: 256 }, (_, i) => i);
  await target.writeFile(`${ws}/bin.dat`, bin);
  assert.deepEqual(await target.readFile(`${ws}/bin.dat`), bin);

  await target.writeFiles([
    { path: `${ws}/proj/a.txt`, bytes: new TextEncoder().encode('A\n') },
    { path: `${ws}/proj/bin/run.sh`, bytes: new TextEncoder().encode('#!/bin/sh\necho ran\n'), executable: true },
  ]);
  r = await tools.call('run', { command: './bin/run.sh && cat a.txt', cwd: 'proj' });
  assert.equal(r.output, 'exit code 0\n--- stdout ---\nran\nA\n');
  // Concurrent calls are serialized, not interleaved.
  const outs = await Promise.all([1, 2, 3, 4].map((i) => tools.call('run', { command: `echo start ${i}; sleep 0.2; echo end ${i}` })));
  outs.forEach((o, i) => assert.equal(o.output, `exit code 0\n--- stdout ---\nstart ${i + 1}\nend ${i + 1}\n`));
  // A cancelled call returns at once.
  const ac = new AbortController();
  setTimeout(() => ac.abort(), 200);
  const t0 = Date.now();
  await assert.rejects(tools.call('run', { command: 'sleep 3' }, { signal: ac.signal }), /cancelled/);
  assert.ok(Date.now() - t0 < 1500);
  await devices.dispose();
  assert.equal(fake.Linux.last.deleted, true);
});
