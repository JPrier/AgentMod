// Workspace checkpoints in a shadow Git repository.
//
// The log reconstructs logical state exactly; it cannot reconstruct files.
// Checkpoints record world state so that it can be restored: a snapshot is a
// Git tree of the workspace written into a *shadow* repository under the
// toolkit's state directory (never the project's own .git), with
// GIT_WORK_TREE pointing at the workspace. Identical trees are deduplicated,
// so read-only work costs one `git add -A` scan and no new objects.
//
// Nested repositories (e.g. imported projects under the workspace root) would
// otherwise be recorded as gitlinks; each is snapshotted with its own index and
// grafted into the tree under its path, so their files are captured too. A
// repository nested inside a nested repository stays a gitlink (documented
// limit). Ignored files (the projects' .gitignore, plus node_modules, target,
// .venv, __pycache__, and the state directory) are not captured, so a rewind
// does not undo dependency installs or build outputs.

import { shq } from './paths.js';

const EXCLUDES = ['/.agentmod/', 'node_modules/', 'target/', '.venv/', 'venv/', '__pycache__/', '*.pyc', '.DS_Store'];

function gitEnv(stateDir, root, gitDir = `${stateDir}/shadow.git`) {
  return [
    `export GIT_DIR=${shq(gitDir)}`,
    `GIT_WORK_TREE=${shq(root)}`,
    `GIT_INDEX_FILE=${shq(`${stateDir}/shadow.index`)}`,
    'GIT_CONFIG_NOSYSTEM=1 GIT_TERMINAL_PROMPT=0 GIT_OPTIONAL_LOCKS=0',
    'GIT_AUTHOR_NAME=agentmod GIT_AUTHOR_EMAIL=agentmod@localhost GIT_COMMITTER_NAME=agentmod GIT_COMMITTER_EMAIL=agentmod@localhost',
  ].join(' ');
}

function initScript(stateDir) {
  const ex = EXCLUDES.map(shq).join(' ');
  return [
    `mkdir -p ${shq(stateDir)}`,
    `if [ ! -d "$GIT_DIR" ]; then`,
    `  (unset GIT_WORK_TREE GIT_INDEX_FILE; mkdir -p "$GIT_DIR" && git init -q --bare "$GIT_DIR") || exit 90`,
    '  git config core.bare false',
    `  printf '%s\\n' ${ex} > "$GIT_DIR/info/exclude"`,
    `  git config core.autocrlf false; git config gc.auto 0; git config core.bigFileThreshold 2m; git config core.quotePath false`,
    'fi',
    `[ -f ${shq(`${stateDir}/.gitignore`)} ] || printf '*\\n' > ${shq(`${stateDir}/.gitignore`)}`,
  ].join('\n');
}

/** Snapshot script: prints "new|same <commit> <tree>". $1 reason, $2 metadata JSON. */
function snapshotScript(stateDir, root, gitDir) {
  return [
    'set -e',
    gitEnv(stateDir, root, gitDir),
    initScript(stateDir),
    `cd ${shq(root)}`,
    'git add -A . >/dev/null 2>&1 || git add -A . >/dev/null',
    // Replace gitlinks (nested repositories) by their working trees.
    `git ls-files -s | grep '^160000' | cut -f2- > ${shq(`${stateDir}/nested.list`)} || true`,
    'while IFS= read -r d; do',
    '  [ -n "$d" ] || continue',
    `  ix=${shq(`${stateDir}/nested-`)}$(printf '%s' "$d" | cksum | cut -d' ' -f1).index`,
    '  git update-index --force-remove -- "$d"',
    '  t=$(cd "$d" && GIT_INDEX_FILE="$ix" GIT_WORK_TREE="$PWD" git add -A . >/dev/null 2>&1 && GIT_INDEX_FILE="$ix" GIT_WORK_TREE="$PWD" git write-tree)',
    '  git read-tree --prefix="$d/" "$t"',
    `done < ${shq(`${stateDir}/nested.list`)}`,
    'tree=$(git write-tree)',
    `for m in ${shq(`${stateDir}/inflight`)}/*; do [ -f "$m" ] && echo "inflight \${m##*/}"; done; true`,
    `last=$(cat ${shq(`${stateDir}/last`)} 2>/dev/null || true)`,
    'if [ -n "$last" ] && [ "${last#* }" = "$tree" ]; then echo "same ${last%% *} $tree"; exit 0; fi',
    'parent=${last%% *}',
    'c=$(printf \'%s\\n\\n%s\\n\' "$1" "$2" | git commit-tree "$tree" ${parent:+-p "$parent"})',
    'git update-ref "refs/agentmod/checkpoints/$c" "$c"',
    `printf '%s %s\\n' "$c" "$tree" > ${shq(`${stateDir}/last`)}`,
    'echo "new $c $tree"',
  ].join('\n');
}

/**
 * @param {object} o
 * @param {ReturnType<import('./runner.js').makeRunner>} o.runner
 * @param {string} o.root
 * @param {string} o.stateDir
 * @param {string} [o.gitDir]  shadow object store (default `<stateDir>/shadow.git`);
 *        isolated worktrees share their parent's so trees can be compared
 */
export function makeCheckpoints({ runner, root, stateDir, gitDir }) {
  const env = gitEnv(stateDir, root, gitDir);
  const git = (script, what, opts = {}) => runner.must(`${env}\n${initScript(stateDir)}\ncd ${shq(root)}\n${script}`, { cwd: '/', timeoutMs: 300_000, what, ...opts });

  /** Snapshot the workspace. Returns { checkpoint, tree, created }. */
  async function snapshot({ reason = 'checkpoint', meta = {}, signal } = {}) {
    const script = snapshotScript(stateDir, root, gitDir);
    const r = await runner.must(`bash -c ${shq(script)} agentmod-checkpoint ${shq(reason)} ${shq(JSON.stringify(meta))}`, { cwd: '/', timeoutMs: 300_000, signal, what: 'checkpoint' });
    const lines = r.stdout.trim().split('\n');
    const [kind, checkpoint, tree] = lines.pop().split(' ');
    const inflight = lines.filter((l) => l.startsWith('inflight ')).map((l) => l.slice(9));
    return { checkpoint, tree, created: kind === 'new', inflight, ms: r.ms };
  }

  /** Forget in-flight markers (after they have been reported). */
  async function clearInflight(names) {
    if (!names.length) return;
    await runner.run(`rm -f -- ${names.map((n) => shq(`${stateDir}/inflight/${n}`)).join(' ')}`, { cwd: '/', timeoutMs: 30_000 });
  }

  /** Resolve a checkpoint id (commit, unique prefix) to its tree. */
  async function treeOf(ref) {
    const id = String(ref || '').trim();
    if (!/^[0-9a-f]{4,64}$/i.test(id)) throw new Error(`\`${ref}\` is not a checkpoint id`);
    const r = await runner.run(`${env}\ngit rev-parse --verify --quiet ${shq(`${id}^{tree}`)}`, { cwd: '/', timeoutMs: 60_000 });
    const tree = r.stdout.trim();
    if (r.exitCode !== 0 || !tree) throw new Error(`unknown checkpoint \`${ref}\``);
    return tree;
  }

  /** Paths that differ between two trees: [{ status: 'A'|'M'|'D'|'T', path }]. */
  async function changes(fromTree, toTree) {
    const r = await git(`git diff --no-renames --name-status -z ${shq(fromTree)} ${shq(toTree)}`, 'diff');
    const parts = r.stdout.split('\0').filter((x) => x !== '');
    const out = [];
    for (let i = 0; i + 1 < parts.length; i += 2) out.push({ status: parts[i][0], path: parts[i + 1] });
    return out;
  }

  /** Unified diff (and stat) between two trees, optionally limited to paths. */
  async function diff(fromTree, toTree, { paths = [], stat = false } = {}) {
    const p = paths.length ? ` -- ${paths.map(shq).join(' ')}` : '';
    const r = await git(`git -c color.ui=never diff --no-renames ${stat ? '--stat=120 ' : ''}${shq(fromTree)} ${shq(toTree)}${p}`, 'diff');
    return r.stdout;
  }

  /** Bytes of a file in a tree, or null when absent. */
  async function fileAt(tree, path) {
    const r = await runner.run(`${env}\ngit cat-file blob ${shq(`${tree}:${path}`)}`, { cwd: '/', timeoutMs: 60_000 });
    return r.exitCode === 0 ? r.stdoutBytes : null;
  }

  /**
   * Restore the workspace to `tree`. The caller snapshots first and passes the
   * current tree, so the restore is itself reversible.
   */
  async function restore(tree, currentTree, { signal } = {}) {
    const script = [
      `git read-tree ${shq(tree)}`,
      `git diff --name-only -z --no-renames --diff-filter=AMT ${shq(currentTree)} ${shq(tree)} > ${shq(`${stateDir}/restore.write`)}`,
      `git diff --name-only -z --no-renames --diff-filter=D ${shq(currentTree)} ${shq(tree)} > ${shq(`${stateDir}/restore.delete`)}`,
      `git checkout-index -f -z --stdin < ${shq(`${stateDir}/restore.write`)}`,
      `xargs -0 rm -f -- < ${shq(`${stateDir}/restore.delete`)} 2>/dev/null || true`,
      `tr '\\0' '\\n' < ${shq(`${stateDir}/restore.delete`)} | while IFS= read -r f; do d=$(dirname -- "$f"); [ "$d" = . ] || rmdir -p -- "$d" 2>/dev/null || true; done`,
      `printf '%s %s\\n' "$(tr -cd '\\0' < ${shq(`${stateDir}/restore.write`)} | wc -c)" "$(tr -cd '\\0' < ${shq(`${stateDir}/restore.delete`)} | wc -c)"`,
    ].join('\n');
    const r = await git(script, 'restore', { signal });
    const [written, deleted] = r.stdout.trim().split(/\s+/).map(Number);
    return { written, deleted };
  }

  /** Recent checkpoints, newest first. */
  async function list(limit = 20) {
    const r = await git(`git for-each-ref --sort=-creatordate --count=${Math.max(1, Math.min(200, limit))} --format='%(objectname) %(tree) %(creatordate:unix) %(subject)' refs/agentmod/checkpoints`, 'list');
    return r.stdout
      .split('\n')
      .filter(Boolean)
      .map((l) => {
        const [checkpoint, tree, at, ...subject] = l.split(' ');
        return { checkpoint, tree, at: Number(at) * 1000, reason: subject.join(' ') };
      });
  }

  /** Materialize a tree into a fresh directory (isolated worktrees). */
  async function materialize(tree, dir, { signal } = {}) {
    const ix = `${stateDir}/materialize.index`;
    const script = [
      `mkdir -p ${shq(dir)} && cd ${shq(dir)}`,
      `rm -f ${shq(ix)}`,
      `GIT_INDEX_FILE=${shq(ix)} git read-tree ${shq(tree)}`,
      `GIT_INDEX_FILE=${shq(ix)} GIT_WORK_TREE=${shq(dir)} git checkout-index -a -f`,
      `rm -f ${shq(ix)}`,
    ].join('\n');
    await git(script, 'materialize', { signal });
  }

  return { snapshot, clearInflight, treeOf, changes, diff, fileAt, restore, list, materialize };
}
