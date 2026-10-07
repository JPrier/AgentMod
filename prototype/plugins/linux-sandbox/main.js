// linux-sandbox: lets the agent code in the browser. The coding tools (run,
// read_file, write_file, edit_file, list_files, import_repo) execute inside an
// x86 Linux VM that CheerpX runs in this browser tab — on the user's CPU and
// RAM, with nothing executed on a server. Browser runtime only (it has a
// `module` and no `command`, so the native runtime disables it).
//
// CheerpX needs the page itself, so the VM is a host device (`linux-vm`, see
// ui/runtime/devices.js) that this plugin declares and drives; every decision
// about what a tool does is made here, in the plugin.
//
// It is an ordinary tool plugin: a tool call is an event, and this plugin
// answers the ones it owns. Policy (approval-gate), the loop (chat-context),
// the model, and rendering (web-ui) are other plugins; see README.md.
//
// Config (all optional):
//   image            disk image URL (default: WebVM's public Debian image)
//   image_type       cloud | bytes | github
//   cheerpx_version  CheerpX runtime version loaded from Leaning Technologies' CDN
//   workspace        name of the persistent /workspace store (one per name)
//   workspace_path   where the workspace is mounted in the guest (default /workspace)
//   uid, gid         guest identity for commands (default root inside the VM)
//   import_repos     offer `import_repo` (default true)
//   limits           overrides for the shared tool limits
//
// CheerpX is proprietary software by Leaning Technologies, free for personal
// and open-source use; other uses need their commercial licence.
import { defineWorkspacePlugin } from '../sdk/workspace-plugin.js';
import { linuxVmTarget, DEVICE } from './target.js';

defineWorkspacePlugin({
  manifest: {
    name: 'linux-sandbox',
    version: '0.1.0',
    description: 'Coding tools in an x86 Linux VM running in the browser (CheerpX). Browser runtime only.',
    // Host devices this plugin uses (enforced by the browser host).
    devices: [DEVICE],
    config_schema: {
      image: 'wss://disks.webvm.io/… (WebVM\'s public Debian image)',
      image_type: 'cloud | bytes | github',
      cheerpx_version: '1.4.0',
      workspace: 'default',
      workspace_path: '/workspace',
      uid: 0,
      gid: 0,
      import_repos: true,
      limits: {},
    },
  },
  root: (cfg) => cfg.workspace_path || '/workspace',
  createTarget: (cfg, { host }) => linuxVmTarget({ host, config: cfg }),
  describe: (cfg) =>
    'You can write and run code with the run, read_file, write_file, edit_file, list_files and import_repo tools. ' +
    'They act on a Linux sandbox: a 32-bit x86 Debian VM (CheerpX) running inside the user\'s browser, on their own machine. ' +
    `Work in ${cfg.workspace_path || '/workspace'}, which persists across page reloads. ` +
    'The VM has no network access, so apt, pip, npm and cargo cannot download anything; ' +
    'use what is installed (check with `run`) and use import_repo to bring in public GitHub repositories. ' +
    'Emulation is slower than native hardware: prefer small, incremental builds and tests. ' +
    'Do not claim something works until you have run it.',
});
