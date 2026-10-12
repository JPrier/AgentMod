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
import { suggestTools } from '../plugins/sdk/toolargs.js';
import { argsKey, conformArgs, schemaOfKeys, toolOfMember } from '../plugins/sdk/toolschema.js';
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
  const p = project([{ section: 'environment', value: { ...env, network: 'none' } }]);
  assert.match(p.system, /Environment \(probed once; no need to check again\)/);
  assert.match(p.system, /Network: none\./);
});

test('arguments are fitted to the member shape: unambiguous coercions pass, clear mistakes are named', () => {
  // A tool's argument schema is its member subscription's `receives.args` (compiler.md §3.4).
  const keys = argsKey(toolSpecs().find((t) => t.name === 'read_file')).keys;
  const ok = conformArgs(keys, { path: 'a.c', start_line: '5' });
  assert.deepEqual(ok.errors, []);
  assert.equal(ok.args.start_line, 5, 'a number given as text becomes a number');
  assert.deepEqual(conformArgs(keys, { file: 'a.c' }).errors, ['missing required `path`']);
  const st = argsKey(toolSpecs().find((t) => t.name === 'search_text')).keys;
  const glob = conformArgs(st, { query: 'x', include: 'kilo.c' });
  assert.deepEqual(glob.errors, [], 'one glob instead of a list is fine');
  assert.deepEqual(glob.args.include, ['kilo.c']);
  assert.match(conformArgs(st, { query: { regex: 'x' } }).errors[0], /`query` must be string/);
});

test('the model-facing tool comes from the resolved member: its name, its schema, its hints', () => {
  const spec = toolSpecs().find((t) => t.name === 'read_file');
  const args = argsKey(spec);
  const tool = toolOfMember({ event: 'tool-call/read_file', keys: { args }, subscribers: [{ plugin: 'local-workspace', description: spec.description, hints: { tier: spec.tier, group: spec.group, effects: spec.effects } }] });
  assert.equal(tool.name, 'read_file');
  assert.equal(tool.description, spec.description);
  assert.equal(tool.tier, spec.tier);
  assert.deepEqual(tool.required, spec.required);
  assert.deepEqual(Object.keys(tool.parameters), Object.keys(spec.parameters));
  assert.equal(schemaOfKeys(args.keys).properties.path.type, 'string');
});

test('unknown tools: "no such tool", with the closest names', () => {
  const offered = ['read_file', 'search_text', 'shell', 'tool_search'];
  assert.match(suggestTools('readfile', offered), /^no such tool: readfile\./);
  assert.match(suggestTools('readfile', offered), /Did you mean `read_file`\?/);
  assert.match(suggestTools('grep', offered), /call tool_search/);
});

test('deferred tools are listed with their arguments, so they can be called without a discovery turn', () => {
  const tools = [...toolSpecs({ importRepos: true }), { name: 'tool_search', description: 'Find tools.', parameters: { query: { type: 'string' } }, tier: 'core' }];
  const ctx = tools.map((t) => ({ section: 'tools', value: t }));
  const p = project(ctx);
  const ts = p.tools.find((t) => t.name === 'tool_search');
  assert.match(ts.description, /import_repo\(repo, ref\?, dest\?\)/);
  assert.ok(!p.tools.some((t) => t.name === 'import_repo'), 'its full schema is still not sent');
  const spec = tools.find((t) => t.name === 'import_repo');
  assert.ok(spec.intents.includes('github.com/'));
});
