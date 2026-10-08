// Failure ergonomics and turn savers: errors that say what to send instead,
// argument checks before dispatch, close-name suggestions, environment facts
// probed once, and deferred tools callable from their listed signatures.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { codingToolkit, toolSpecs } from '../plugins/sdk/coding/toolkit.js';
import { localTarget } from '../plugins/local-workspace/target.js';
import { checkArgs, suggestTools } from '../plugins/sdk/toolargs.js';
import { project } from '../plugins/sdk/projection.js';

function setup() {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'agentmod-erg-')));
  fs.mkdirSync(path.join(root, 'kilo'));
  fs.writeFileSync(path.join(root, 'kilo', 'kilo.c'), 'void editorRefreshScreen(void) {}\n');
  const tk = codingToolkit({ target: localTarget({ root }), root, importRepos: false });
  return { root, tk, call: (n, a) => tk.call(n, a, { session: 's1', call_id: 'c' }) };
}

test('a file passed where a directory belongs: the error names the exact fix', async () => {
  const { call } = setup();
  const r = await call('search_text', { query: 'editorRefreshScreen', path: 'kilo/kilo.c' });
  assert.equal(r.error, true);
  assert.match(r.output, /search_text `path` must be a directory; kilo\/kilo\.c is a file/);
  assert.match(r.output, /path="kilo", include=\["kilo\.c"\]/);
  // Following the suggestion works in one step.
  const fixed = await call('search_text', { query: 'editorRefreshScreen', path: 'kilo', include: ['kilo.c'] });
  assert.equal(fixed.error, undefined, fixed.output);
  assert.match(fixed.output, /kilo\/kilo\.c/);
  assert.match((await call('list_dir', { path: 'kilo/kilo.c' })).output, /read_file path="kilo\/kilo\.c"/);
  assert.match((await call('read_file', { path: 'kilo' })).output, /is a directory, not a file: list it with list_dir path="kilo"/);
});

test('a missing path: nearest directory, what is there, and how to find it', async () => {
  const { call } = setup();
  const r = await call('read_file', { path: 'kilo/kilo.h' });
  assert.match(r.output, /kilo\/kilo\.h does not exist/);
  assert.match(r.output, /nearest existing directory is kilo\//);
  assert.match(r.output, /kilo\.c/);
  assert.match(r.output, /search_files pattern="\*\*\/kilo\.h"/);
});

test('environment facts are probed once, with versions', async () => {
  const { tk } = setup();
  const env = await tk.environment();
  assert.ok(env.os && env.arch);
  assert.ok('bash' in env.tools);
  const p = project([{ slot: 'environment', value: { ...env, network: 'none' } }]);
  assert.match(p.system, /Environment \(probed once; no need to check again\)/);
  assert.match(p.system, /Network: none\./);
});

test('argument checks reject clear mistakes with a corrected example, and nothing else', () => {
  const spec = toolSpecs().find((t) => t.name === 'read_file');
  assert.equal(checkArgs(spec, { path: 'a.c', start_line: '5' }), null, 'tools coerce reasonable input');
  const m = checkArgs(spec, { file: 'a.c' });
  assert.match(m, /missing required `path`/);
  assert.match(m, /Call it again like \{"path":"<path>"\}/);
  const st = toolSpecs().find((t) => t.name === 'search_text');
  assert.equal(checkArgs(st, { query: 'x', include: 'kilo.c' }), null, 'one glob instead of a list is fine');
  assert.match(checkArgs(st, { query: { regex: 'x' } }), /`query` must be a string/);
});

test('unknown tools get the closest names, and deferred ones say how to load', () => {
  const offered = ['read_file', 'search_text', 'shell', 'tool_search'];
  assert.match(suggestTools('readfile', offered), /Did you mean `read_file`\?/);
  assert.match(suggestTools('grep', offered), /call tool_search/);
});

test('deferred tools are listed with their arguments, so they can be called without a discovery turn', () => {
  const tools = [...toolSpecs({ importRepos: true }), { name: 'tool_search', description: 'Find tools.', parameters: { query: { type: 'string' } }, tier: 'core' }];
  const ctx = tools.map((t) => ({ slot: 'tools', value: t }));
  const p = project(ctx);
  const ts = p.tools.find((t) => t.name === 'tool_search');
  assert.match(ts.description, /import_repo\(repo, ref\?, dest\?\)/);
  assert.ok(!p.tools.some((t) => t.name === 'import_repo'), 'its full schema is still not sent');
  const spec = tools.find((t) => t.name === 'import_repo');
  assert.ok(spec.intents.includes('github.com/'));
});
