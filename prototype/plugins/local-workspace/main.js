// local-workspace: the coding tools against a directory on the machine running
// the native runtime. It is the native counterpart of `linux-sandbox` — same
// tools, same results — so the same definition shape works on either host.
//
// This is NOT a sandbox: commands run as you, with your network and files.
// The tools confine paths to `root`, but a shell command can reach anything.
// It is not in any default definition; opt in deliberately, and consider
// guarding `run` with approval-gate (`require = ["run", ...]`).
//
// Config:
//   root     workspace directory (default ".agentmod/workspace", relative to the runtime's cwd)
//   shell    shell binary (default "/bin/bash")
//   limits   overrides for the shared tool limits (see sdk/workspace-tools.js)
import { defineWorkspacePlugin } from '../sdk/workspace-plugin.js';
import { localTarget } from './target.js';

const rootOf = (cfg) => localTarget.resolveRoot(cfg.root || '.agentmod/workspace');

defineWorkspacePlugin({
  manifest: {
    name: 'local-workspace',
    version: '0.1.0',
    description: 'Coding tools (run, files, repo import) in a local directory. Native runtime only; not a sandbox.',
    config_schema: { root: '.agentmod/workspace', shell: '/bin/bash', import_repos: true, limits: {} },
  },
  validate: async (cfg) => {
    if (process.platform === 'win32') throw new Error('local-workspace needs a POSIX shell (bash); it does not run on Windows');
    await localTarget.check(cfg.shell || '/bin/bash');
  },
  root: rootOf,
  createTarget: (cfg) => localTarget({ root: rootOf(cfg), shell: cfg.shell || '/bin/bash' }),
  describe: (cfg) =>
    `You can work on code with the run, read_file, write_file, edit_file, list_files and import_repo tools. ` +
    `They act on a local directory, ${rootOf(cfg)}, on the machine running AgentMod (not a sandbox). ` +
    `Keep all work inside that directory.`,
});
