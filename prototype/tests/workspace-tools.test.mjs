// Unit tests for the shared coding-tool layer, the local execution target,
// and the tar writer the browser sandbox uses. Run: node --test tests/
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { normalizePath, resolveIn, unifiedDiff, truncate, workspaceTools, parseRepo } from '../plugins/sdk/workspace-tools.js';
import { localTarget } from '../plugins/local-workspace/target.js';
import { makeTar } from '../plugins/linux-sandbox/tar.js';

const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'agentmod-ws-'));

function setup(extra = {}) {
  const root = tmp();
  const target = localTarget({ root });
  return { root, target, tools: workspaceTools({ target, root, ...extra }) };
}

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

test('write, read, edit, list', async () => {
  const { root, tools } = setup({ importRepos: false });
  assert.deepEqual(tools.specs.map((s) => s.name), ['run', 'read_file', 'write_file', 'edit_file', 'list_files']);
  let r = await tools.call('write_file', { path: 'src/hello.c', content: '#include <stdio.h>\nint main(void){puts("hi");return 0;}\n' });
  assert.equal(r.error, undefined, r.output);
  assert.match(r.output, /^Created /);
  assert.match(r.diff.unified, /\+int main/);
  r = await tools.call('read_file', { path: `${root}/src/hello.c` });
  assert.match(r.output, /lines 1-2 of 2/);
  assert.match(r.output, /puts\("hi"\)/);
  r = await tools.call('edit_file', { path: 'src/hello.c', old_text: 'puts("hi")', new_text: 'puts("hello")' });
  assert.equal(r.error, undefined, r.output);
  assert.match(r.diff.unified, /-int main\(void\)\{puts\("hi"\)/);
  assert.match(fs.readFileSync(path.join(root, 'src/hello.c'), 'utf8'), /hello/);
  r = await tools.call('edit_file', { path: 'src/hello.c', old_text: 'nope', new_text: 'x' });
  assert.equal(r.error, true);
  assert.match(r.output, /not found/);
  await tools.call('write_file', { path: 'dup.txt', content: 'a\na\n' });
  r = await tools.call('edit_file', { path: 'dup.txt', old_text: 'a', new_text: 'b' });
  assert.match(r.output, /matches 2 places/);
  r = await tools.call('edit_file', { path: 'dup.txt', old_text: 'a', new_text: 'b$&', replace_all: true });
  assert.equal(fs.readFileSync(path.join(root, 'dup.txt'), 'utf8'), 'b$&\nb$&\n');
  fs.mkdirSync(path.join(root, '.git'));
  fs.writeFileSync(path.join(root, '.git', 'HEAD'), 'x');
  r = await tools.call('list_files', {});
  assert.equal(r.error, undefined, r.output);
  assert.match(r.output, /^src\/$/m);
  assert.match(r.output, /^src\/hello\.c$/m);
  assert.doesNotMatch(r.output, /\.git/);
  r = await tools.call('read_file', { path: '../outside' });
  assert.equal(r.error, true);
  r = await tools.call('read_file', { path: 'missing.txt' });
  assert.match(r.output, /does not exist/);
});

test('run: exit codes, stderr, cwd, timeout', async () => {
  const { tools } = setup();
  let r = await tools.call('run', { command: 'echo hello; echo oops >&2; exit 3' });
  assert.equal(r.error, true);
  assert.match(r.output, /^exit code 3/);
  assert.match(r.output, /--- stdout ---\nhello/);
  assert.match(r.output, /--- stderr ---\noops/);
  await tools.call('write_file', { path: 'sub/x.txt', content: 'x' });
  r = await tools.call('run', { command: 'ls', cwd: 'sub' });
  assert.equal(r.error, false);
  assert.match(r.output, /x\.txt/);
  r = await tools.call('run', { command: 'ls', cwd: '/' });
  assert.equal(r.error, true);
  assert.match(r.output, /outside the workspace/);
  r = await tools.call('run', { command: 'sleep 5', timeout_seconds: 1 });
  assert.equal(r.error, true);
  assert.match(r.output, /timed out after 1s/);
});

test('run is cancellable', async () => {
  const { tools } = setup();
  const ac = new AbortController();
  setTimeout(() => ac.abort(), 200);
  const t0 = Date.now();
  await assert.rejects(tools.call('run', { command: 'sleep 10' }, { signal: ac.signal }));
  assert.ok(Date.now() - t0 < 3000);
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
  assert.match(r.output, /Imported octo\/demo at c0ffee123456 \(main\) into .*\/demo: 3 files/);
  assert.match(r.output, /Skipped 1: vendor\/lib \(submodule\)/);
  assert.equal(fs.readFileSync(path.join(root, 'demo/src/main.c'), 'utf8'), files['src/main.c']);
  assert.ok(fs.statSync(path.join(root, 'demo/run.sh')).mode & 0o100);
  assert.match(execFileSync('git', ['log', '--oneline'], { cwd: path.join(root, 'demo') }).toString(), /Import octo\/demo@c0ffee/);
  const again = await tools.call('import_repo', { repo: 'octo/demo' });
  assert.match(again.output, /already exists/);
  assert.throws(() => parseRepo('not a repo'), /owner\/name/);
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
