// The linux-sandbox execution target, driven through a fake CheerpX module
// (tests/fake-cheerpx.mjs). Verifies the boot sequence, the command wrapper,
// the byte channels in and out of the guest, and batched (tar) writes.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { cheerpxTarget, unavailableReason } from '../plugins/linux-sandbox/target.js';
import { workspaceTools } from '../plugins/sdk/workspace-tools.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const FAKE = pathToFileURL(path.join(here, 'fake-cheerpx.mjs')).href;

function sandbox(extra = {}) {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), 'sbx-'));
  const ws = path.join(base, 'workspace');
  const target = cheerpxTarget({
    cheerpx_url: FAKE,
    in_path: path.join(base, 'in'),
    out_path: path.join(base, 'out'),
    workspace_path: ws,
    ...extra,
  });
  return { target, ws, tools: workspaceTools({ target, root: ws, importRepos: false }) };
}

test('refuses to boot without cross-origin isolation', async () => {
  globalThis.crossOriginIsolated = false;
  assert.match(unavailableReason(), /cross-origin isolated/);
  const { target } = sandbox();
  await assert.rejects(target.ensureReady({ status: () => {} }), /Linux sandbox unavailable: the page is not cross-origin isolated/);
});

test('boots once, runs commands, and moves files both ways', async () => {
  globalThis.crossOriginIsolated = true;
  globalThis.indexedDB ??= {};
  const fake = await import(FAKE);
  const { target, ws, tools } = sandbox({ image: 'wss://example.test/fail-wss.ext2' });
  const statuses = [];
  const status = (s, m) => statuses.push([s, m]);
  await Promise.all([target.ensureReady({ status }), target.ensureReady({ status })]);
  await target.ensureReady({ status });
  assert.deepEqual(statuses.map((s) => s[0]), ['booting', 'ready'], 'boots exactly once');
  assert.match(statuses[1][1], /Linux sandbox ready/);
  // The wss: image fell back to https: (as WebVM does).
  assert.ok(fake.created.some((d) => d.url === 'https://example.test/fail-wss.ext2'));
  const mounts = fake.Linux.last.mounts.map((m) => `${m.type}:${m.path}`);
  assert.deepEqual(mounts.slice(0, 1), ['ext2:/']);
  assert.ok(mounts.includes(`dir:${ws}`));
  // Separate stores for the OS overlay, the workspace, and scratch I/O.
  const idbs = fake.created.filter((d) => d.kind === 'idb').map((d) => d.name);
  assert.ok(idbs.some((n) => n.startsWith('agentmod-root-')));
  assert.ok(idbs.includes('agentmod-workspace-default'));

  let r = await tools.call('write_file', { path: 'hello.c', content: '#include <stdio.h>\nint main(void){puts("hi");return 0;}\n' });
  assert.equal(r.error, undefined, r.output);
  assert.equal(fs.readFileSync(path.join(ws, 'hello.c'), 'utf8').includes('puts("hi")'), true);
  r = await tools.call('edit_file', { path: 'hello.c', old_text: '"hi"', new_text: '"it\'s $HOME"' });
  assert.equal(r.error, undefined, r.output);
  r = await tools.call('read_file', { path: 'hello.c' });
  assert.match(r.output, /it's \$HOME/);
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

  await target.writeFiles([
    { path: `${ws}/proj/a.txt`, bytes: new TextEncoder().encode('A\n') },
    { path: `${ws}/proj/bin/run.sh`, bytes: new TextEncoder().encode('#!/bin/sh\necho ran\n'), executable: true },
  ]);
  r = await tools.call('run', { command: './bin/run.sh && cat a.txt', cwd: 'proj' });
  assert.equal(r.output, 'exit code 0\n--- stdout ---\nran\nA\n');
  // Concurrent calls are serialized, not interleaved.
  const outs = await Promise.all([1, 2, 3, 4].map((i) => tools.call('run', { command: `echo start ${i}; sleep 0.2; echo end ${i}` })));
  outs.forEach((o, i) => assert.equal(o.output, `exit code 0\n--- stdout ---\nstart ${i + 1}\nend ${i + 1}\n`));
  await target.dispose();
  assert.equal(fake.Linux.last.deleted, true);
});
