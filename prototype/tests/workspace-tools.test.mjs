// Unit tests for shared text/path helpers, repository import, and the tar writer the browser sandbox's VM device uses. Run: node --test tests/*.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { normalizePath, resolveIn, unifiedDiff, truncate, workspaceTools } from '../plugins/sdk/workspace-tools.js';
import { localTarget } from '../plugins/local-workspace/target.js';
import { makeTar } from '../ui/runtime/devices/tar.js';

const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'agentmod-ws-'));

test('paths are confined to the workspace', () => {
  assert.equal(normalizePath('/a//b/./c/../d'), '/a/b/d');
  assert.equal(resolveIn('/workspace', 'src/main.c'), '/workspace/src/main.c');
  assert.equal(resolveIn('/workspace', '/workspace'), '/workspace');
  assert.equal(resolveIn('/workspace', 'a/../b', '/workspace/x'), '/workspace/x/b');
  assert.throws(() => resolveIn('/workspace', '../etc/passwd'), /outside the workspace/);
  assert.throws(() => resolveIn('/workspace', '/workspace-other/x'), /outside the workspace/);
  assert.throws(() => resolveIn('/workspace', '/etc/passwd'), /outside the workspace/);
  assert.throws(() => resolveIn('/workspace', ''), /path is required/);
});

test('unified diffs apply cleanly with patch(1)', () => {
  const dir = tmp();
  let seed = 7;
  const rnd = (n) => { seed = (seed * 1103515245 + 12345) % 2 ** 31; return seed % n; };
  for (let round = 0; round < 200; round++) {
    const before = Array.from({ length: 5 + rnd(40) }, (_, i) => `line ${i} ${rnd(3)}`);
    const after = [...before];
    for (let k = 0; k < 1 + rnd(6); k++) {
      const at = rnd(after.length + 1);
      const op = rnd(3);
      if (op === 0) after.splice(at, 0, `inserted ${round}.${k}`);
      else if (op === 1 && after.length) after.splice(Math.min(at, after.length - 1), 1);
      else if (after.length) after[Math.min(at, after.length - 1)] = `changed ${round}.${k}`;
    }
    // Mostly newline-terminated; sometimes not, on either side.
    const a = before.join('\n') + (rnd(5) ? '\n' : '');
    const b = after.join('\n') + (rnd(5) ? '\n' : '');
    fs.writeFileSync(path.join(dir, 'f.txt'), a);
    const d = unifiedDiff('/f.txt', a, b);
    if (a === b) { assert.equal(d, ''); continue; }
    fs.writeFileSync(path.join(dir, 'f.patch'), d + '\n');
    execFileSync('patch', ['-s', '-p1', '-i', 'f.patch'], { cwd: dir });
    assert.equal(fs.readFileSync(path.join(dir, 'f.txt'), 'utf8'), b, `round ${round}\n${d}`);
  }
});

test('long output keeps head and tail', () => {
  const t = truncate('a'.repeat(5000) + 'END', 1000);
  assert.ok(t.length < 1200);
  assert.ok(t.startsWith('aaa'));
  assert.ok(t.endsWith('END'));
  assert.match(t, /bytes omitted/);
});

test('import_repo copies a GitHub tree and commits it', async () => {
  const files = { 'README.md': '# demo\n', 'src/main.c': 'int main(){return 0;}\n', 'run.sh': '#!/bin/sh\necho ok\n' };
  const calls = [];
  const fakeFetch = async (url) => {
    calls.push(url);
    const json = (o) => ({ ok: true, status: 200, json: async () => o, text: async () => JSON.stringify(o) });
    if (url === 'https://api.github.com/repos/octo/demo') return json({ default_branch: 'main' });
    if (url === 'https://api.github.com/repos/octo/demo/commits/main') return json({ sha: 'c0ffee1234567890', commit: { tree: { sha: 'tree1' } } });
    if (url === 'https://api.github.com/repos/octo/demo/git/trees/tree1?recursive=1') {
      return json({ truncated: false, tree: [
        { path: 'src', type: 'tree', mode: '040000' },
        ...Object.entries(files).map(([p, c]) => ({ path: p, type: 'blob', mode: p.endsWith('.sh') ? '100755' : '100644', size: c.length })),
        { path: 'vendor/lib', type: 'commit', mode: '160000' },
      ] });
    }
    const raw = 'https://raw.githubusercontent.com/octo/demo/c0ffee1234567890/';
    if (url.startsWith(raw)) {
      const body = files[decodeURIComponent(url.slice(raw.length))];
      return { ok: true, status: 200, arrayBuffer: async () => new TextEncoder().encode(body).buffer };
    }
    return { ok: false, status: 404, text: async () => 'nope' };
  };
  const root = tmp();
  const tools = workspaceTools({ target: localTarget({ root }), root, fetch: fakeFetch });
  const r = await tools.call('import_repo', { repo: 'https://github.com/octo/demo.git' });
  assert.equal(r.error, undefined, r.output);
  assert.match(r.output, /Imported octo\/demo at c0ffee123456 \(main\) into demo: 3 files/);
  assert.match(r.output, /Skipped 1: vendor\/lib \(submodule\)/);
  assert.equal(fs.readFileSync(path.join(root, 'demo/src/main.c'), 'utf8'), files['src/main.c']);
  assert.ok(fs.statSync(path.join(root, 'demo/run.sh')).mode & 0o100);
  assert.match(execFileSync('git', ['log', '--oneline'], { cwd: path.join(root, 'demo') }).toString(), /Import octo\/demo@c0ffee/);
  const again = await tools.call('import_repo', { repo: 'octo/demo' });
  assert.match(again.output, /already exists/);
  assert.match((await tools.call('import_repo', { repo: 'not a repo' })).output, /owner\/name/);
});

test('tar archives extract with tar(1), including long paths and modes', () => {
  const dir = tmp();
  const long = `${dir}/${'deep/'.repeat(30)}file-with-a-long-name.txt`;
  const tarball = makeTar([
    { path: `${dir}/a.txt`, bytes: new TextEncoder().encode('alpha\n') },
    { path: `${dir}/bin/run`, bytes: new TextEncoder().encode('#!/bin/sh\n'), executable: true },
    { path: long, bytes: new Uint8Array(1500).fill(65) },
    { path: `${dir}/empty`, bytes: new Uint8Array() },
  ]);
  assert.equal(tarball.length % 512, 0);
  const file = path.join(tmp(), 'x.tar');
  fs.writeFileSync(file, tarball);
  execFileSync('tar', ['-xf', file, '-C', '/']);
  assert.equal(fs.readFileSync(`${dir}/a.txt`, 'utf8'), 'alpha\n');
  assert.ok(fs.statSync(`${dir}/bin/run`).mode & 0o100);
  assert.equal(fs.readFileSync(long).length, 1500);
  assert.equal(fs.readFileSync(`${dir}/empty`).length, 0);
  assert.throws(() => makeTar([{ path: '/a/../b', bytes: new Uint8Array() }]), /refusing/);
});
