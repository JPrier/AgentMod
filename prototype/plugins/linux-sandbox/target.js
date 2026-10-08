// Execution target: the browser host's `linux-vm` device (an x86 Linux VM the
// page runs with CheerpX; see ui/runtime/devices/cheerpx-vm.js). Implements the
// interface in ../sdk/workspace-tools.js by calling the device over the
// plugin's own connection. CheerpX needs the page (`window`, `document`), so
// the VM cannot live in this worker; the plugin keeps all the logic.
import { toB64, fromB64 } from '../sdk/b64.js';

export const DEVICE = 'linux-vm';

// Plugin config keys that describe the VM (the rest configure the tools).
const VM_KEYS = ['cheerpx_version', 'cheerpx_url', 'image', 'image_type', 'workspace', 'workspace_path', 'uid', 'gid', 'in_path', 'out_path', 'stall_grace_seconds'];

export function linuxVmTarget({ host, config = {} }) {
  const vm = Object.fromEntries(VM_KEYS.filter((k) => config[k] !== undefined).map((k) => [k, config[k]]));
  const call = (op, args = {}) => host.device(DEVICE, op, args, vm);
  let starting = null;
  const bootMessage = 'Starting the Linux sandbox in your browser (CheerpX). The first start streams the disk image; later starts reuse the local cache…';
  const readyMessage = (info) => `Linux sandbox ready — ${info.os} (${info.kernel}); tools: ${info.tools.join(' ') || 'none found'}. Work persists in this browser.`;

  return {
    identity: () => ({ kind: 'linux-vm', id: `cheerpx:${config.workspace || 'default'}`, root: config.workspace_path || '/workspace' }),

    /** Is the VM up right now (without booting it)? */
    async isReady() {
      try { return !!(await call('state')).booted; } catch { return false; }
    },

    /**
     * Before each coding tool: make sure a VM is running. A stopped VM (never
     * started, or stopped by the agent) boots; a crashed one is reported, and
     * the agent decides whether to restart it.
     */
    async ensureReady({ status }) {
      const st = await call('state');
      if (st.booted) return;
      if (st.broken) throw new Error(st.broken);
      if (st.unavailable) throw new Error(`Linux sandbox unavailable: ${st.unavailable}`);
      starting ??= (async () => {
        await status('booting', bootMessage);
        const info = await call('boot');
        await status('ready', readyMessage(info));
      })().finally(() => { starting = null; });
      await starting;
    },

    /** VM lifecycle and diagnostics (the `sandbox_*` tools). */
    lifecycle: {
      status: () => call('status'),
      logs: (opts) => call('logs', opts),
      restart: async ({ status } = {}) => {
        await status?.('booting', 'Restarting the Linux sandbox with a fresh VM (/workspace is kept)…');
        const info = await call('restart');
        await status?.('ready', readyMessage(info));
        return info;
      },
      stop: () => call('stop'),
    },

    exec({ command, cwd, timeoutMs, signal }) {
      if (signal?.aborted) return Promise.reject(new Error('cancelled'));
      const run = call('exec', { command, cwd, timeoutSeconds: Math.ceil(timeoutMs / 1000) }).then((r) => ({
        exitCode: r.exitCode,
        timedOut: r.timedOut,
        stdout: fromB64(r.stdout),
        stderr: fromB64(r.stderr),
      }));
      if (!signal) return run;
      // The VM cannot be stopped mid-call; return at once and interrupt the command.
      return new Promise((resolve, reject) => {
        const onAbort = () => {
          call('interrupt').catch(() => {});
          reject(new Error('cancelled'));
        };
        signal.addEventListener('abort', onAbort, { once: true });
        run.then(
          (v) => { signal.removeEventListener('abort', onAbort); resolve(v); },
          (e) => { signal.removeEventListener('abort', onAbort); reject(e); },
        );
      });
    },

    async readFile(path) {
      const data = await call('readFile', { path });
      return data == null ? null : fromB64(data);
    },

    writeFile(path, bytes) {
      return this.writeFiles([{ path, bytes }]);
    },

    async writeFiles(files) {
      await call('writeFiles', { files: files.map((f) => ({ path: f.path, data: toB64(f.bytes), executable: !!f.executable })) });
    },
  };
}
