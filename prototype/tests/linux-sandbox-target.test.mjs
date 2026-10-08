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

  let r = await tools.call('apply_patch', { changes: [{ action: 'create', path: 'hello.c', content: '#include <stdio.h>\nint main(void){puts("hi");return 0;}\n' }] });
  assert.equal(r.error, undefined, r.output);
  assert.ok(fs.readFileSync(path.join(ws, 'hello.c'), 'utf8').includes('puts("hi")'));
  r = await tools.call('apply_patch', { changes: [{ action: 'update', path: 'hello.c', edits: [{ old_text: '"hi"', new_text: '"it\'s $HOME ✓"' }] }] });
  assert.equal(r.error, undefined, r.output);
  r = await tools.call('read_file', { path: 'hello.c' });
  assert.match(r.output, /it's \$HOME ✓/);
  r = await tools.call('shell', { command: 'printf out; printf err >&2; exit 4' });
  assert.match(r.output, /^exit 4 · [\d.]+s\n--- stdout ---\nout\n--- stderr ---\nerr$/);
  // Output from an earlier command never leaks into a later one.
  r = await tools.call('shell', { command: 'true' });
  assert.match(r.output, /^exit 0 · [\d.]+s\n\(no output\)$/);
  r = await tools.call('shell', { command: 'pwd', cwd: path.join(ws, 'nope') });
  assert.equal(r.error, true);
  assert.match(r.output, /exit 126/);
  r = await tools.call('shell', { command: 'sleep 4', timeout_seconds: 1 });
  assert.match(r.output, /timed out after 1s/);
  r = await tools.call('list_dir', {});
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
  r = await tools.call('shell', { command: './bin/run.sh && cat a.txt', cwd: 'proj' });
  assert.match(r.output, /^exit 0 · [\d.]+s\n--- stdout ---\nran\nA\n$/);
  // Search works inside the VM too (grep/find fallbacks when ripgrep is absent).
  r = await tools.call('search_text', { query: 'HOME' });
  assert.match(r.output, /hello\.c\n {2}2:/);
  // Concurrent calls are serialized, not interleaved.
  const outs = await Promise.all([1, 2, 3, 4].map((i) => tools.call('shell', { command: `echo start ${i}; sleep 0.2; echo end ${i}` })));
  outs.forEach((o, i) => assert.match(o.output, new RegExp(`--- stdout ---\\nstart ${i + 1}\\nend ${i + 1}\\n$`)));
  // A cancelled call returns at once.
  const ac = new AbortController();
  setTimeout(() => ac.abort(), 200);
  const t0 = Date.now();
  await assert.rejects(tools.call('shell', { command: 'sleep 3' }, { signal: ac.signal }), /cancelled/);
  assert.ok(Date.now() - t0 < 1500);
  await devices.dispose();
  assert.equal(fake.Linux.last.deleted, true);
});

test('the agent can inspect, restart, and stop a crashed VM', async () => {
  globalThis.crossOriginIsolated = true;
  globalThis.indexedDB ??= {};
  const config = { ...fakeVmConfig(), stall_grace_seconds: 1 };
  const ws = config.workspace_path;
  const devices = await hostDevices();
  const target = linuxVmTarget({ host: jsonHost(devices), config });
  const tools = workspaceTools({ target, root: ws, importRepos: false });
  assert.deepEqual([...tools.lifecycleNames], ['sandbox_status', 'sandbox_logs', 'sandbox_restart', 'sandbox_stop']);
  const ready = () => target.ensureReady({ status: () => {} });

  // Before anything runs, status says stopped and the journal is empty.
  let r = await tools.call('sandbox_status', {});
  assert.match(r.output, /^state: stopped/);
  r = await tools.call('sandbox_logs', {});
  assert.match(r.output, /0 of 0 entries/);

  await ready();
  r = await tools.call('apply_patch', { changes: [{ action: 'create', path: 'keep.txt', content: 'survives\n' }] });
  assert.equal(r.error, undefined, r.output);
  r = await tools.call('sandbox_status', {});
  assert.match(r.output, /^state: running/);
  assert.match(r.output, /operations: \d+/);

  // The VM dies mid-command: the call fails after timeout + grace, and so do later ones.
  r = await tools.call('shell', { command: 'echo FAKE_CRASH', timeout_seconds: 1 });
  assert.equal(r.error, true);
  assert.match(r.output, /stopped responding during operation #\d+/);
  assert.match(r.output, /sandbox_restart/);
  await assert.rejects(ready(), /stopped responding/);
  r = await tools.call('sandbox_status', {});
  assert.match(r.output, /^state: crashed/);
  r = await tools.call('sandbox_logs', { kinds: ['stalled'] });
  assert.match(r.output, /stalled op=\d+ command="echo FAKE_CRASH"/);
  r = await tools.call('sandbox_logs', {});
  assert.match(r.output, /boot-ok/);
  assert.match(r.output, / op op=\d+ command=/);

  // Restart: a fresh VM, the workspace intact, commands work again.
  const fake = await import(FAKE_CHEERPX);
  const before = fake.Linux.last;
  r = await tools.call('sandbox_restart', {});
  assert.equal(r.error, undefined, r.output);
  assert.match(r.output, /Restarted the Linux sandbox/);
  assert.notEqual(fake.Linux.last, before, 'a new VM');
  assert.equal(before.deleted, true, 'the crashed VM was deleted');
  await ready();
  r = await tools.call('read_file', { path: 'keep.txt' });
  assert.match(r.output, /survives/);
  r = await tools.call('sandbox_logs', { kinds: ['restart', 'stopped', 'boot-ok'] });
  assert.match(r.output, /stopped reason="restart"[\s\S]*restart[\s\S]*boot-ok/);

  // Stop frees the VM; the next coding tool boots it again on its own.
  r = await tools.call('sandbox_stop', {});
  assert.match(r.output, /Stopped/);
  r = await tools.call('sandbox_status', {});
  assert.match(r.output, /^state: stopped/);
  const statuses = [];
  await target.ensureReady({ status: (s) => statuses.push(s) });
  assert.deepEqual(statuses, ['booting', 'ready']);
  r = await tools.call('shell', { command: 'cat keep.txt' });
  assert.match(r.output, /survives/);
  await devices.dispose();
});

test('persistent processes and checkpoints work through the VM device', async () => {
  globalThis.crossOriginIsolated = true;
  globalThis.indexedDB ??= {};
  const config = fakeVmConfig();
  const ws = config.workspace_path;
  const devices = await hostDevices();
  const target = linuxVmTarget({ host: jsonHost(devices), config });
  const tools = workspaceTools({ target, root: ws, importRepos: false });
  await target.ensureReady({ status: () => {} });
  const ctx = (call_id) => ({ session: 's1', call_id });
  let r = await tools.call('process', { action: 'start', command: 'echo first; sleep 1; echo ready-now; sleep 30' }, ctx('p1'));
  assert.ok(!r.error, r.output);
  const id = r.data.process_id;
  // The VM's operation finished (start returned) while the process keeps running.
  r = await tools.call('process', { action: 'wait', id, until: 'ready-now', timeout_seconds: 10 }, ctx('p2'));
  assert.ok(r.data.matched, r.output);
  r = await tools.call('shell', { command: 'echo concurrent works' }, ctx('p3'));
  assert.match(r.output, /concurrent works/);
  r = await tools.call('process', { action: 'kill', id }, ctx('p4'));
  assert.match(r.output, /Stopped/);
  // Checkpoint + restore inside the VM.
  r = await tools.call('apply_patch', { changes: [{ action: 'create', path: 'v.txt', content: 'one\n' }] }, ctx('p5'));
  r = await tools.call('apply_patch', { changes: [{ action: 'update', path: 'v.txt', content: 'two\n' }] }, ctx('p6'));
  const cp = r.data.checkpoint;
  r = await tools.call('checkpoints', { action: 'restore', checkpoint: cp }, ctx('p7'));
  assert.ok(!r.error, r.output);
  assert.equal(fs.readFileSync(path.join(ws, 'v.txt'), 'utf8'), 'one\n');
  await devices.dispose();
});
