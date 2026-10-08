// Host devices: resources only the page can hold, lent to plugin workers.
//
// A plugin in the native runtime is a process and can use the operating system
// directly. A plugin in the browser runtime is a Web Worker, and some browser
// facilities exist only on the page — CheerpX, for one, needs `window` and
// `document`. A host device is the browser runtime's equivalent of that OS
// access: a narrow, page-side resource a plugin reaches through one JSON-RPC
// method, `device`, on the connection it already has.
//
//   plugin → host   { method: "device", params: { device, op, config, args } }
//
// Access is declared: a plugin lists the devices it uses in its manifest
// (`devices: ["linux-vm"]`) and the host refuses everyone else. Devices hold
// no agent logic; they are the hardware, the plugin is the program. Device
// effects, like a native plugin's syscalls, are not log records — the plugin's
// published events are.

import { toB64, fromB64 } from '../plugins/sdk/b64.js';

/**
 * The JSON face of the linux-vm device: bytes cross the wire as base64; the
 * VM driver itself works in Uint8Arrays.
 */
export function linuxVmOps(vm) {
  return {
    state: () => vm.state(),
    boot: () => vm.boot(),
    exec: async (a) => {
      const r = await vm.exec(a);
      return { exitCode: r.exitCode, timedOut: r.timedOut, stdout: toB64(r.stdout), stderr: toB64(r.stderr) };
    },
    readFile: async (a) => {
      const bytes = await vm.readFile(a);
      return bytes == null ? null : toB64(bytes);
    },
    writeFiles: (a) => vm.writeFiles({ files: (a.files || []).map((f) => ({ path: f.path, bytes: fromB64(f.data), executable: !!f.executable })) }),
    interrupt: () => vm.interrupt(),
    status: () => vm.status(),
    logs: (a) => vm.logs(a),
    restart: () => vm.restart(),
    stop: () => vm.stop(),
    dispose: () => vm.dispose(),
  };
}

const KINDS = {
  'linux-vm': {
    create: async (config) => linuxVmOps((await import('./devices/cheerpx-vm.js')).createCheerpxVm(config)),
    ops: new Set(['state', 'boot', 'exec', 'readFile', 'writeFiles', 'interrupt', 'status', 'logs', 'restart', 'stop']),
  },
};

const fail = (message, code) => Object.assign(new Error(message), { code });

export class Devices {
  constructor(kinds = KINDS) {
    this.kinds = kinds;
    this.instances = new Map(); // `${device}|${config}` -> Promise<device>
  }

  /** Handle one `device` request from a plugin. */
  async call(plugin, manifest, { device, op, config = {}, args = {} } = {}) {
    const kind = this.kinds[device];
    if (!kind) throw fail(`unknown host device \`${device}\``, -32601);
    if (!manifest?.devices?.includes(device)) throw fail(`\`${plugin}\` does not declare the \`${device}\` device in its manifest`, -32003);
    if (!kind.ops.has(op)) throw fail(`\`${device}\` has no operation \`${op}\``, -32601);
    const key = `${device}|${JSON.stringify(config)}`;
    if (!this.instances.has(key)) {
      this.instances.set(key, Promise.resolve(kind.create(config)).catch((e) => {
        this.instances.delete(key);
        throw e;
      }));
    }
    const dev = await this.instances.get(key);
    const out = await dev[op](args);
    return out === undefined ? null : out;
  }

  async dispose() {
    for (const p of this.instances.values()) (await p.catch(() => null))?.dispose?.();
    this.instances.clear();
  }
}
