// linux-sandbox: lets the agent code in the browser. The coding tools (shell,
// process, read_file, list_dir, search_files, search_text, apply_patch, and the
// deferred extras; see ../sdk/coding/toolkit.js) execute inside an
// x86 Linux VM that CheerpX runs in this browser tab — on the user's CPU and
// RAM, with nothing executed on a server. Browser runtime only (it has a
// `module` and no `command`, so the native runtime disables it).
//
// CheerpX needs the page itself, so the VM is a host device (`linux-vm`, see
// ui/runtime/devices.js) that this plugin declares and drives; every decision
// about what a tool does is made here, in the plugin.
//
// It is an ordinary tool plugin: a tool call is an event, and this plugin
// answers the ones it owns. Policy (policy / approval-gate), the loop (chat-context),
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
//   stall_grace_seconds  how long past a command's timeout before the VM is
//                    declared crashed (default 60)
//
// CheerpX is proprietary software by Leaning Technologies, free for personal
// and open-source use; other uses need their commercial licence.
import { defineWorkspacePlugin } from '../sdk/workspace-plugin.js';
import { linuxVmTarget, DEVICE } from './target.js';

defineWorkspacePlugin({
  manifest: {
    name: 'linux-sandbox',
    version: '0.2.0',
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
      fork_workspace: 'isolated',
      limits: {},
    },
  },
  root: (cfg) => cfg.workspace_path || '/workspace',
  lifecycle: true,
  network: () => 'none inside the VM (apt, pip, npm, git clone cannot download); use import_repo for GitHub repositories',
  createTarget: (cfg, { host }) => linuxVmTarget({ host, config: cfg }),
  describe: (cfg) =>
    'Your workspace is a Linux sandbox: a 32-bit x86 Debian VM (CheerpX) running inside the user\'s browser, on their own machine. ' +
    `Work in ${cfg.workspace_path || '/workspace'}, which persists across page reloads (processes do not). ` +
    'The VM has no network access, so apt, pip, npm and cargo cannot download anything; ' +
    'use what is installed (check with shell) and bring in public GitHub repositories with import_repo (find it with tool_search). ' +
    'Emulation is slower than native hardware: prefer small, incremental builds and tests. ' +
    'If tools report that the sandbox stopped responding, use the sandbox tools (tool_search "sandbox") to read its logs and restart it.',
});
