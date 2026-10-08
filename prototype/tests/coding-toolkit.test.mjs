// The coding toolkit against a real local target: bash, git, ripgrep/grep.
// Run: node --test tests/*.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { codingToolkit, toolSpecs } from '../plugins/sdk/coding/toolkit.js';
import { localTarget } from '../plugins/local-workspace/target.js';
import { classifyCommand } from '../plugins/sdk/coding/shellclass.js';
import { parseDiagnostics } from '../plugins/sdk/coding/diagnostics.js';
import { globToRegExp } from '../plugins/sdk/coding/glob.js';
import { parseEnvelope, applyEdits, applyHunks } from '../plugins/sdk/coding/patch.js';

const tmp = () => fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'agentmod-tk-')));
let callSeq = 0;

function setup(opts = {}) {
  const root = tmp();
  const target = opts.target?.(root) ?? localTarget({ root });
  const tk = codingToolkit({ target, root, importRepos: false, ...opts.toolkit });
  const events = [];
  const emit = async (name, payload, ui) => { events.push({ name, payload, ui }); };
  const call = (name, args, extra = {}) => tk.call(name, args, { session: 's0001', call_id: `c${++callSeq}`, emit, ...extra });
  const write = (rel, text) => { fs.mkdirSync(path.dirname(path.join(root, rel)), { recursive: true }); fs.writeFileSync(path.join(root, rel), text); };
  const read = (rel) => fs.readFileSync(path.join(root, rel), 'utf8');
  return { root, target, tk, events, call, write, read };
}

test('tool specs: compact core, deferred extras, valid schemas, deterministic', () => {
  const specs = toolSpecs({ lifecycle: true });
  const core = specs.filter((s) => s.tier === 'core').map((s) => s.name);
  assert.deepEqual(core, ['shell', 'process', 'read_file', 'list_dir', 'search_files', 'search_text', 'apply_patch']);
  for (const s of specs) {
    assert.ok(s.name && s.description && typeof s.parameters === 'object', s.name);
    for (const r of s.required || []) assert.ok(r in s.parameters, `${s.name} requires undeclared ${r}`);
    for (const [k, p] of Object.entries(s.parameters)) assert.ok(p.type && p.description, `${s.name}.${k}`);
    assert.ok(['core', 'deferred'].includes(s.tier));
  }
  assert.equal(JSON.stringify(toolSpecs({ lifecycle: true })), JSON.stringify(specs), 'byte-stable');
});

test('read_file: numbered lines, ranges, hashes, binary, encoding, errors', async () => {
  const { call, write, root } = setup();
  write('a.txt', Array.from({ length: 1000 }, (_, i) => `line ${i + 1}`).join('\n') + '\n');
  let r = await call('read_file', { path: 'a.txt' });
  assert.ok(!r.error);
  assert.match(r.output, /^a\.txt · lines 1-400 of 1000 · .* · sha256 [0-9a-f]{64} · continue with start_line 401/);
  assert.match(r.output, /\n  1\tline 1\n/);
  assert.equal(r.data.lines, 1000);
  const sha = execFileSync('sha256sum', [path.join(root, 'a.txt')]).toString().slice(0, 64);
  assert.equal(r.data.sha256, sha);
  r = await call('read_file', { path: 'a.txt', start_line: 998, end_line: 2000 });
  assert.match(r.output, /lines 998-1000 of 1000/);
  assert.doesNotMatch(r.output, /continue with/);
  write('noeol.txt', 'one\ntwo');
  r = await call('read_file', { path: 'noeol.txt' });
  assert.match(r.output, /lines 1-2 of 2/);
  fs.writeFileSync(path.join(root, 'bin.dat'), Buffer.from([1, 2, 0, 3]));
  r = await call('read_file', { path: 'bin.dat' });
  assert.match(r.output, /binary file/);
  fs.writeFileSync(path.join(root, 'latin1.txt'), Buffer.from([0x63, 0x61, 0x66, 0xe9, 0x0a]));
  r = await call('read_file', { path: 'latin1.txt' });
  assert.match(r.output, /not valid UTF-8/);
  write('empty.txt', '');
  r = await call('read_file', { path: 'empty.txt' });
  assert.match(r.output, /is empty/);
  assert.match((await call('read_file', { path: 'missing.txt' })).output, /does not exist/);
  fs.mkdirSync(path.join(root, 'd'));
  assert.match((await call('read_file', { path: 'd' })).output, /is a directory/);
  assert.match((await call('read_file', { path: '../../etc/passwd' })).output, /outside the workspace/);
});

test('list_dir and search_files are bounded and skip heavy directories', async () => {
  const { call, write } = setup();
  write('src/main.rs', 'fn main() {}\n');
  write('src/lib/util.rs', 'pub fn util() {}\n');
  write('Cargo.toml', '[package]\n');
  write('node_modules/x/index.js', '');
  write('target/debug/out', '');
  let r = await call('list_dir', {});
  assert.match(r.output, /Cargo\.toml {2}\d+ B/);
  assert.match(r.output, /node_modules\/ {2}\(not descended\)/);
  assert.doesNotMatch(r.output, /index\.js/);
  r = await call('list_dir', { path: 'src', depth: 2 });
  assert.match(r.output, /lib\/util\.rs/);
  r = await call('search_files', { pattern: '**/*.rs' });
  assert.match(r.output, /^2 files match/);
  assert.match(r.output, /src\/lib\/util\.rs/);
  r = await call('search_files', { pattern: 'Cargo.toml' });
  assert.match(r.output, /^1 file match/);
  r = await call('search_files', { pattern: '**/*.js' });
  assert.match(r.output, /^0 files/, 'node_modules is not searched');
});

test('globs', () => {
  const m = (g, p) => globToRegExp(g).test(p);
  assert.ok(m('**/*.rs', 'a/b/c.rs') && m('**/*.rs', 'c.rs') && m('*.rs', 'deep/x.rs'));
  assert.ok(m('src/**', 'src/a/b.js') && !m('src/**', 'lib/a.js'));
  assert.ok(m('**/Cargo.toml', 'crates/x/Cargo.toml'));
  assert.ok(m('*.{ts,tsx}', 'a/b.tsx') && !m('*.{ts,tsx}', 'a.js'));
  assert.ok(m('test_?.py', 'test_a.py') && !m('test_?.py', 'test_ab.py'));
});

for (const engine of ['ripgrep', 'grep']) {
  test(`search_text (${engine}): structured, sorted, bounded, filters`, async () => {
    const noRg = (root) => {
      const t = localTarget({ root });
      const exec = t.exec.bind(t);
      t.exec = (o) => exec({ ...o, command: o.command.replace('for t in rg git;', 'for t in git;') });
      return t;
    };
    const { call, write } = setup(engine === 'grep' ? { target: noRg } : {});
    write('b.py', 'def Foo():\n    return foo_bar\n');
    write('a.js', 'const foo = 1;\n// FOO again\nfoo(foo);\n');
    write('sub/c.txt', 'nothing here\n');
    let r = await call('search_text', { query: 'foo' });
    assert.ok(!r.error, r.output);
    assert.match(r.output, new RegExp(`· ${engine}`));
    // smart case: lowercase query is case-insensitive
    assert.match(r.output, /^5 matches in 2 files/);
    const lines = r.output.split('\n');
    assert.equal(lines[1], 'a.js');
    assert.match(lines[2], /^ {2}1:7: const foo = 1;/);
    r = await call('search_text', { query: 'Foo' });
    assert.match(r.output, /^1 match in 1 file/);
    r = await call('search_text', { query: 'foo', include: ['*.py'] });
    assert.match(r.output, /^2 matches in 1 file/);
    r = await call('search_text', { query: 'foo(', regex: false });
    assert.match(r.output, /a\.js\n {2}3:1: foo\(foo\);/);
    r = await call('search_text', { query: 'foo', max_results: 2 });
    assert.match(r.output, /first 2 shown/);
    r = await call('search_text', { query: 'zzz' });
    assert.match(r.output, /^no matches/);
    assert.match((await call('search_text', { query: '(' })).output, /invalid regex/);
  });
}

test('apply_patch: create, update, delete, move; diffs and hashes; checkpoint first', async () => {
  const { call, write, read, events, root } = setup();
  write('keep.txt', 'a\nb\nc\n');
  write('old.txt', 'x\n');
  write('gone.txt', 'bye\n');
  fs.chmodSync(path.join(root, 'old.txt'), 0o755);
  const r = await call('apply_patch', {
    changes: [
      { action: 'create', path: 'new/hello.txt', content: 'hi\n' },
      { action: 'update', path: 'keep.txt', edits: [{ old_text: 'b\n', new_text: 'B\nB2\n' }] },
      { action: 'delete', path: 'gone.txt' },
      { action: 'move', path: 'old.txt', to: 'moved/old.txt', edits: [{ old_text: 'x', new_text: 'y' }] },
    ],
  });
  assert.ok(!r.error, r.output);
  assert.equal(read('new/hello.txt'), 'hi\n');
  assert.equal(read('keep.txt'), 'a\nB\nB2\nc\n');
  assert.ok(!fs.existsSync(path.join(root, 'gone.txt')));
  assert.equal(read('moved/old.txt'), 'y\n');
  assert.equal(fs.statSync(path.join(root, 'moved/old.txt')).mode & 0o777, 0o755, 'move keeps the mode');
  assert.match(r.output, /Applied 5 file changes · checkpoint before: [0-9a-f]{12}/);
  assert.match(r.output, /updated keep\.txt \(\+2 -1\) sha256 [0-9a-f]{16}/);
  const change = events.find((e) => e.name === 'workspace-change');
  assert.match(change.payload.unified, /^--- a\/keep\.txt\n\+\+\+ b\/keep\.txt\n@@ -1,3 \+1,4 @@/m);
  assert.ok(events.some((e) => e.name === 'checkpoint-created'));
  const keep = r.data.files.find((f) => f.path === 'keep.txt');
  assert.equal(keep.sha256, execFileSync('sha256sum', [path.join(root, 'keep.txt')]).toString().slice(0, 64));
});

test('apply_patch rejects stale edits and is all-or-nothing', async () => {
  const { call, write, read, root } = setup();
  write('f.txt', 'one\ntwo\nthree\n');
  const sha = (await call('read_file', { path: 'f.txt' })).data.sha256;
  write('f.txt', 'one\nTWO\nthree\n'); // changed behind the agent's back
  let r = await call('apply_patch', { changes: [{ action: 'update', path: 'f.txt', expected_sha256: sha.slice(0, 12), edits: [{ old_text: 'one', new_text: '1' }] }] });
  assert.ok(r.error);
  assert.match(r.output, /changed since it was read/);
  r = await call('apply_patch', { changes: [{ action: 'update', path: 'f.txt', edits: [{ old_text: 'two\n', new_text: '2\n' }] }] });
  assert.ok(r.error);
  assert.match(r.output, /not found in the file as it is now.*closest line is 2/s);
  r = await call('apply_patch', { changes: [
    { action: 'create', path: 'side.txt', content: 'side\n' },
    { action: 'update', path: 'f.txt', edits: [{ old_text: 'missing', new_text: 'x' }] },
  ] });
  assert.ok(r.error);
  assert.ok(!fs.existsSync(path.join(root, 'side.txt')), 'nothing written when any change fails');
  assert.equal(read('f.txt'), 'one\nTWO\nthree\n');
  r = await call('apply_patch', { changes: [{ action: 'update', path: 'f.txt', edits: [{ old_text: 'e', new_text: '3' }] }] });
  assert.match(r.output, /matches 3 places/);
  r = await call('apply_patch', { changes: [{ action: 'create', path: 'f.txt', content: 'x' }] });
  assert.match(r.output, /already exists/);
});

test('apply_patch rolls back when a write fails midway', async () => {
  const flaky = (root) => {
    const t = localTarget({ root });
    t.writeFiles = async (files) => {
      await t.writeFile(files[0].path, files[0].bytes);
      throw new Error('disk full');
    };
    return t;
  };
  const { call, write, read, root } = setup({ target: flaky });
  write('a.txt', 'A\n');
  write('b.txt', 'B\n');
  const r = await call('apply_patch', { changes: [
    { action: 'update', path: 'a.txt', content: 'A2\n' },
    { action: 'update', path: 'b.txt', content: 'B2\n' },
  ] });
  assert.ok(r.error);
  assert.match(r.output, /disk full; rolled back to checkpoint/);
  assert.equal(read('a.txt'), 'A\n');
  assert.equal(read('b.txt'), 'B\n');
  assert.ok(fs.existsSync(root));
});

test('apply_patch accepts the "*** Begin Patch" envelope', async () => {
  const { call, write, read } = setup();
  write('src/app.py', 'import os\n\ndef main():\n    print("hi")\n\nif __name__ == "__main__":\n    main()\n');
  write('obsolete.txt', 'x\n');
  const patch = [
    '*** Begin Patch',
    '*** Add File: docs/README.md',
    '+# Title',
    '+text',
    '*** Update File: src/app.py',
    '@@ def main():',
    '-    print("hi")',
    '+    print("hello")',
    '+    return 0',
    '*** Delete File: obsolete.txt',
    '*** End Patch',
  ].join('\n');
  const r = await call('apply_patch', { patch });
  assert.ok(!r.error, r.output);
  assert.equal(read('docs/README.md'), '# Title\ntext\n');
  assert.match(read('src/app.py'), /print\("hello"\)\n {4}return 0\n/);
  assert.throws(() => parseEnvelope('*** Update File: x\n'), /must start/);
  assert.throws(() => parseEnvelope('*** Begin Patch\n*** Update File: x\n'), /no hunks/);
  assert.equal(applyHunks('a\nb\nc\n', [{ anchor: '', lines: [[' ', 'a'], ['-', 'b'], ['+', 'B']] }], 'f'), 'a\nB\nc\n');
  // trailing whitespace differences still match, CRLF files keep CRLF
  assert.equal(applyEdits('x  \ny\n', [{ old_text: 'x\ny\n', new_text: 'z\n' }], 'f'), 'z\n');
  assert.equal(applyEdits('a\r\nb\r\n', [{ old_text: 'a\nb', new_text: 'c\nd' }], 'f'), 'c\r\nd\r\n');
});

test('checkpoints: dedupe, restore (reversible), nested repositories', async () => {
  const { call, write, read, root, tk } = setup();
  write('a.txt', 'v1\n');
  // An imported project with its own .git (a nested repository).
  write('proj/src.c', 'int x;\n');
  execFileSync('git', ['init', '-q'], { cwd: path.join(root, 'proj') });
  execFileSync('git', ['-c', 'user.name=t', '-c', 'user.email=t@t', 'commit', '-qm', 'init', '--allow-empty'], { cwd: path.join(root, 'proj') });
  const c1 = await tk.checkpoint('test 1', { session: 's', call_id: 'x' });
  assert.ok(c1.created, JSON.stringify(c1));
  const again = await tk.checkpoint('test 2', { session: 's', call_id: 'y' });
  assert.equal(again.created, false);
  assert.equal(again.checkpoint, c1.checkpoint, 'identical tree is deduplicated');
  const files = await tk.checkpoints.changes('4b825dc642cb6eb9a060e54bf8d69288fbee4904', c1.tree);
  assert.deepEqual(files.map((f) => f.path).sort(), ['a.txt', 'proj/src.c'], 'nested repository files are captured, not a gitlink');
  write('a.txt', 'v2\n');
  write('proj/src.c', 'int y;\n');
  write('added.txt', 'new\n');
  fs.rmSync(path.join(root, 'proj', '.git'), { recursive: false, force: false, maxRetries: 0, recursive: true });
  fs.mkdirSync(path.join(root, 'proj', '.git'));
  let r = await call('checkpoints', { action: 'diff', checkpoint: c1.checkpoint });
  assert.match(r.output, /a\.txt/);
  assert.match(r.output, /-v1\n\+v2/);
  r = await call('checkpoints', { action: 'restore', checkpoint: c1.checkpoint });
  assert.ok(!r.error, r.output);
  assert.equal(read('a.txt'), 'v1\n');
  assert.equal(read('proj/src.c'), 'int x;\n');
  assert.ok(!fs.existsSync(path.join(root, 'added.txt')));
  const previous = r.data.previous;
  r = await call('checkpoints', { action: 'restore', checkpoint: previous });
  assert.equal(read('a.txt'), 'v2\n', 'the restore itself can be undone');
  assert.ok(fs.existsSync(path.join(root, 'added.txt')));
  r = await call('checkpoints', { action: 'list' });
  assert.match(r.output, /before restoring/);
  // Checkpoints never touch the project's own git state.
  assert.ok(!fs.existsSync(path.join(root, '.git')));
  // The state dir hides itself from git.
  assert.equal(fs.readFileSync(path.join(root, '.agentmod', 'state', '.gitignore'), 'utf8'), '*\n');
});

test('shell: exit codes, cwd, timeout, diagnostics, truncation, checkpoints', async () => {
  const { call, write, events, root } = setup({ toolkit: { limits: { max_output_bytes: 2000 } } });
  write('sub/x.txt', 'hi\n');
  let r = await call('shell', { command: 'cat x.txt; echo err >&2; exit 3', cwd: 'sub' });
  assert.ok(r.error);
  assert.equal(r.data.exit_code, 3);
  assert.match(r.output, /^exit 3 · /);
  assert.match(r.output, /--- stdout ---\nhi/);
  assert.match(r.output, /--- stderr ---\nerr/);
  assert.equal(r.data.read_only, false, 'unknown/exit → mutating');
  r = await call('shell', { command: 'ls' });
  assert.equal(r.data.read_only, true);
  assert.equal(r.data.checkpoint, null, 'read-only commands take no checkpoint');
  r = await call('shell', { command: 'sleep 5', timeout_seconds: 1 });
  assert.ok(r.data.timed_out);
  assert.match(r.output, /timed out after 1s/);
  r = await call('shell', { command: 'printf "src/main.c:3:5: error: unknown type name \'foo\'\\nsrc/main.c:9:1: warning: unused\\n" >&2; exit 1' });
  assert.match(r.output, /Diagnostics \(parsed\): 1 error, 1 warning\n {2}src\/main\.c:3:5 error: unknown type name/);
  assert.ok(events.some((e) => e.name === 'diagnostics' && e.payload.diagnostics.length === 2));
  r = await call('shell', { command: 'seq 1 20000' });
  assert.ok(r.data.truncated);
  assert.match(r.output, /bytes omitted/);
  const full = path.join(root, r.data.full_output);
  assert.match(fs.readFileSync(full, 'utf8'), /\n19999\n20000\n/);
  r = await call('shell', { command: 'touch made.txt' });
  assert.match(r.data.checkpoint, /^[0-9a-f]{40}$/);
});

test('shell environment is an allowlist; secrets are scoped and redacted', async () => {
  process.env.AGENTMOD_TEST_LEAK = 'sk-or-should-never-leak-123';
  const { call } = setup({ toolkit: { secrets: { DEPLOY_TOKEN: { value: 'tok-secret-9876', env: 'DEPLOY_TOKEN', commands: ['^echo '] } } } });
  let r = await call('shell', { command: 'env' });
  assert.doesNotMatch(r.output, /should-never-leak/);
  assert.match(r.output, /PATH=/);
  r = await call('shell', { command: 'echo "token=$DEPLOY_TOKEN"', secrets: ['DEPLOY_TOKEN'] });
  assert.ok(!r.error, r.output);
  assert.match(r.output, /token=«secret:DEPLOY_TOKEN»/);
  assert.doesNotMatch(r.output, /tok-secret-9876/);
  assert.deepEqual(r.data.secrets_used, ['DEPLOY_TOKEN']);
  r = await call('shell', { command: 'echo "x=$DEPLOY_TOKEN"' });
  assert.match(r.output, /x=\n/, 'not injected without a grant');
  r = await call('shell', { command: 'printenv DEPLOY_TOKEN', secrets: ['DEPLOY_TOKEN'] });
  assert.match(r.output, /not allowed for this command/);
  r = await call('shell', { command: 'echo hi', secrets: ['NOPE'] });
  assert.match(r.output, /unknown secret/);
  delete process.env.AGENTMOD_TEST_LEAK;
});

test('shell is cancellable and an interrupted mutation is reported next time', async () => {
  const { call } = setup();
  const ac = new AbortController();
  const p = call('shell', { command: 'touch started; sleep 30' }, { call_id: 'interrupted-1', signal: ac.signal });
  await new Promise((r) => setTimeout(r, 1500));
  const t0 = Date.now();
  ac.abort();
  await assert.rejects(p, /cancelled/);
  assert.ok(Date.now() - t0 < 2000);
  const r = await call('shell', { command: 'touch next' });
  assert.match(r.output, /interrupted before finishing: interrupted-1/);
});

test('processes: start, read, wait, stdin, kill, reconcile across restarts', async () => {
  const { call, events, root } = setup();
  let r = await call('process', { action: 'start', name: 'srv', command: 'echo booting; sleep 1; echo "listening on 8080"; while true; do sleep 1; done' }, { call_id: 'start-1' });
  assert.ok(!r.error, r.output);
  const id = r.data.process_id;
  assert.match(id, /^p[0-9a-f]{10}$/);
  assert.ok(events.some((e) => e.name === 'process-started' && e.payload.process_id === id));
  r = await call('process', { action: 'wait', id, until: 'listening on \\d+', timeout_seconds: 10 });
  assert.ok(r.data.matched, r.output);
  assert.match(r.output, /listening on 8080/);
  // The same call id is idempotent: no second process.
  r = await call('process', { action: 'start', command: 'echo other' }, { call_id: 'start-1' });
  assert.match(r.output, /Already started by this call/);
  r = await call('process', { action: 'read', id });
  assert.match(r.output, /no new output/);
  // A fresh toolkit (as after a plugin or runtime restart) reconciles from files.
  const tk2 = codingToolkit({ target: localTarget({ root }), root, importRepos: false });
  r = await tk2.call('process', { action: 'list' }, { session: 's0001', call_id: 'l1' });
  assert.match(r.output, new RegExp(`${id} \\[srv\\]: running`));
  r = await call('process', { action: 'kill', id });
  assert.match(r.output, /Stopped\. .*killed \(SIGTERM\)/);
  assert.ok(events.some((e) => e.name === 'process-exited' && e.payload.process_id === id));
  // stdin
  r = await call('process', { action: 'start', command: 'while read l; do echo "got:$l"; done; echo done', stdin: true }, { call_id: 'start-2' });
  const id2 = r.data.process_id;
  r = await call('process', { action: 'write', id: id2, input: 'hello\n' });
  if (!/got:hello/.test(r.output)) {
    r = await call('process', { action: 'wait', id: id2, until: 'got:hello', timeout_seconds: 10 });
    assert.ok(r.data.matched, r.output);
  }
  r = await call('process', { action: 'write', id: id2, close_stdin: true });
  r = await call('process', { action: 'wait', id: id2, timeout_seconds: 10 });
  assert.match(r.output, /exited with code 0/);
  assert.match(r.output, /done/);
  // exit codes, and a process killed outside the toolkit is reported lost
  r = await call('process', { action: 'start', command: 'exit 7' }, { call_id: 'start-3' });
  r = await call('process', { action: 'wait', id: r.data.process_id, timeout_seconds: 5 });
  assert.match(r.output, /exited with code 7/);
  r = await call('process', { action: 'start', command: 'sleep 60' }, { call_id: 'start-4' });
  const id4 = r.data.process_id;
  const pid = Number(fs.readFileSync(path.join(root, '.agentmod', 'state', 'procs', id4, 'pid'), 'utf8'));
  for (const c of execFileSync('pgrep', ['-P', String(pid)]).toString().trim().split('\n')) process.kill(Number(c), 'SIGKILL');
  process.kill(pid, 'SIGKILL');
  await new Promise((res) => setTimeout(res, 300));
  r = await call('process', { action: 'status', id: id4 });
  assert.match(r.output, /lost/);
  r = await call('process', { action: 'read', id: 'nope' });
  assert.match(r.output, /id. is required/);
});

test('large output from a process prefers the newest bytes', async () => {
  const { call } = setup({ toolkit: { limits: { max_process_read_bytes: 1000 } } });
  const r = await call('process', { action: 'start', command: 'seq 1 50000; echo END' }, { call_id: 'big' });
  assert.match(r.output, /END/);
  assert.match(r.output, /of earlier stdout skipped; full log: \.agentmod\/state\/procs\/p[0-9a-f]+\/stdout/);
});

test('command classification', () => {
  const ro = (c) => classifyCommand(c).readOnly;
  for (const c of ['ls -la', 'git status && git diff HEAD~1', 'rg foo | head', 'cat a 2>&1 | grep b', 'find . -name "*.rs"', 'echo hi > /dev/null']) assert.ok(ro(c), c);
  for (const c of ['npm test', 'echo hi > out.txt', 'sed -i s/a/b/ f', 'find . -delete', 'git commit -m x', 'cargo build', 'rm x', 'tee f', 'python3 - <<EOF\nEOF']) assert.ok(!ro(c), c);
  assert.ok(classifyCommand('curl https://x').network);
  assert.ok(classifyCommand('npm install left-pad').network);
  assert.ok(classifyCommand('git push --force origin main').destructive);
  assert.ok(classifyCommand('git push origin main').publish);
  assert.ok(classifyCommand('cd /tmp && rm -rf build').destructive);
  assert.ok(!classifyCommand('rm -r build').destructive);
});

test('diagnostics from common toolchains', () => {
  const d = parseDiagnostics([
    'error[E0425]: cannot find value `x` in this scope',
    ' --> src/main.rs:3:5',
    'src/app.ts(10,3): error TS2304: Cannot find name \'y\'.',
    '/w/a.c:1:2: warning: implicit declaration',
    'Traceback (most recent call last):',
    '  File "/w/t.py", line 4, in <module>',
    'ValueError: bad value',
    'FAILED tests/test_x.py::test_add - assert 1 == 2',
    '/w/src/x.js',
    '  12:8  error  \'z\' is not defined  no-undef',
    '',
    'not ok 2 - adds numbers',
    'Started 2026-10-07 at 12:00:01: nothing',
  ].join('\n'));
  const brief = d.map((x) => `${x.source} ${x.file}:${x.line ?? ''}:${x.col ?? ''} ${x.severity} ${x.message}`);
  assert.deepEqual(brief, [
    'rustc src/main.rs:3:5 error E0425: cannot find value `x` in this scope',
    'tsc src/app.ts:10:3 error TS2304: Cannot find name \'y\'.',
    'compiler /w/a.c:1:2 warning implicit declaration',
    'python /w/t.py:4: error ValueError: bad value',
    'pytest tests/test_x.py:: error test_add: assert 1 == 2',
    'eslint /w/src/x.js:12:8 error \'z\' is not defined (no-undef)',
    'tap (test):: error failed: adds numbers',
  ]);
});

test('workspace info and project instructions', async () => {
  const { tk, write, root } = setup();
  write('AGENTS.md', '# Rules\nRun `make test` before finishing.\n');
  write('svc/README', 'x');
  execFileSync('git', ['init', '-q'], { cwd: path.join(root, 'svc') });
  const info = await tk.info();
  assert.equal(info.root, root);
  assert.equal(info.environment.kind, 'local');
  assert.deepEqual(info.repos.map((r) => r.path), ['svc']);
  const ins = await tk.instructions();
  assert.equal(ins.length, 1);
  assert.equal(ins[0].path, 'AGENTS.md');
  assert.match(ins[0].text, /make test/);
});

test('repo_map lists files and symbols', async () => {
  const { call, write } = setup();
  write('src/lib.rs', 'pub struct Kernel {}\nimpl Kernel {\n    pub fn new() -> Self { Kernel {} }\n}\nfn helper() {}\n');
  write('web/app.ts', 'export class App {}\nexport function start(port: number) {\n}\nconst x = 1;\n');
  write('tool.py', 'class Tool:\n    def run(self):\n        pass\n');
  const r = await call('repo_map', {});
  assert.ok(!r.error, r.output);
  assert.match(r.output, /src\/lib\.rs\n {2}1: pub struct Kernel/);
  assert.match(r.output, /impl Kernel/);
  assert.match(r.output, /web\/app\.ts\n {2}1: export class App/);
  assert.match(r.output, /export function start\(port: number\)/);
  assert.match(r.output, /tool\.py\n {2}1: class Tool/);
});

test('view_image attaches images and refuses non-images', async () => {
  const { call, root } = setup();
  // 1x1 PNG
  const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==', 'base64');
  fs.writeFileSync(path.join(root, 'shot.png'), png);
  let r = await call('view_image', { path: 'shot.png' });
  assert.ok(!r.error, r.output);
  assert.match(r.output, /image\/png, 1×1/);
  assert.equal(r.attachments[0].media_type, 'image/png');
  assert.equal(Buffer.from(r.attachments[0].data, 'base64').length, png.length);
  fs.writeFileSync(path.join(root, 'x.txt'), 'nope');
  r = await call('view_image', { path: 'x.txt' });
  assert.match(r.output, /not a PNG/);
});
