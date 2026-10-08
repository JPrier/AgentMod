// local-workspace: the coding tools against a directory on the machine running
// the native runtime. It is the native counterpart of `linux-sandbox` — same
// tools, same results — so the same definition shape works on either host.
//
// This is NOT a sandbox: commands run as you, on your files and network. The
// tools confine paths to `root` and commands get an allowlisted environment
// (no runtime secrets), but a shell command can still reach anything you can.
// Pair it with the `policy` plugin (see agentmod.toml) when that matters.
//
// Config:
//   root        workspace directory (default ".agentmod/workspace", relative to the runtime's cwd)
//   shell       shell binary (default "/bin/bash")
//   pass_env    extra variable names passed through from the runtime's environment
//   env         fixed extra variables for commands
//   secrets     { NAME: { env: "SOURCE_VAR", commands: ["^gh ", …] } } — resolved from the
//               runtime's environment, injected only into commands that request them
//   fork_workspace  "isolated" (default) | "shared": the workspace of branched sessions
//   checkpoints     false to disable automatic checkpoints
//   limits, diagnostics   overrides for the shared toolkit (sdk/coding/toolkit.js)
import { defineWorkspacePlugin } from '../sdk/workspace-plugin.js';
import { localTarget } from './target.js';

const rootOf = (cfg) => localTarget.resolveRoot(cfg.root || '.agentmod/workspace');

defineWorkspacePlugin({
  manifest: {
    name: 'local-workspace',
    version: '0.2.0',
    description: 'Coding tools (shell, processes, files, search, patches, checkpoints) in a local directory. Native runtime only; not a sandbox.',
    config_schema: { root: '.agentmod/workspace', shell: '/bin/bash', pass_env: [], env: {}, secrets: {}, fork_workspace: 'isolated', import_repos: true, limits: {} },
  },
  validate: async (cfg) => {
    if (process.platform === 'win32') throw new Error('local-workspace needs a POSIX shell (bash); it does not run on Windows');
    await localTarget.check(cfg.shell || '/bin/bash');
  },
  root: rootOf,
  network: () => "the host machine's network (policy may ask before network commands)",
  createTarget: (cfg) => localTarget({ root: rootOf(cfg), shell: cfg.shell || '/bin/bash', passEnv: cfg.pass_env || [], env: cfg.env || {} }),
  secrets: (cfg) => Object.fromEntries(Object.entries(cfg.secrets || {}).map(([name, s]) => [name, { value: process.env[s.env || name], env: s.as || name, commands: s.commands || [] }]).filter(([, s]) => typeof s.value === 'string')),
  describe: (cfg) =>
    `Your workspace is a local directory, ${rootOf(cfg)}, on the machine running AgentMod (not a sandbox: commands run with the user's permissions). ` +
    'Keep work inside it. Commands get a minimal environment; ask for configured secrets by name with the shell tool\'s `secrets` argument.',
});
