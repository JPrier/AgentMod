// Host device "linux-vm": an x86 Linux VM in this page through CheerpX
// (https://cheerpx.io). CheerpX needs the page itself (it uses `window` and
// `document`), so it cannot live in a plugin's Web Worker; the browser host
// owns the VM and lends it to plugins that declare `devices: ["linux-vm"]`.
//
// This is "hardware", deliberately dumb: boot, run a command, move bytes in
// and out. What a tool means — paths, edits, diffs, imports, limits — lives in
// the plugin (plugins/linux-sandbox, via plugins/sdk/workspace-tools.js).
//
// Filesystems (all persisted in this browser's IndexedDB):
//
//   /               read-only Debian image streamed on demand  +  writable overlay
//                   (IndexedDB "agentmod-root-<image>"; disposable)
//   /workspace      the project files (IndexedDB "agentmod-workspace-<name>"),
//                   separate so the OS can be reset without touching the work
//   /agentmod-in    DataDevice: bytes the page writes for the guest to read
//   /agentmod-out   IndexedDB scratch: bytes the guest writes for the page to read
//
// Commands run through `cx.run('/bin/bash', …)` with output redirected into
// /agentmod-out, so stdout, stderr and the exit code come back separately; a
// watchdog in the wrapper enforces the timeout.
// Operations are serialized: one VM, one operation at a time.
import { makeTar } from './tar.js';

export const VM_DEFAULTS = Object.freeze({
  cheerpx_version: '1.4.0',
  // The public Debian image WebVM uses (32-bit x86; gcc, python3, git, …).
  image: 'wss://disks.webvm.io/debian_buster_large_permis_fixed_01-06-2026.ext2',
  image_type: 'cloud', // cloud (wss/https chunked) | bytes (HTTP range requests) | github
  workspace: 'default',
  workspace_path: '/workspace',
  uid: 0,
  gid: 0,
});

const ENV = [
  'HOME=/root',
  'USER=root',
  'SHELL=/bin/bash',
  'TERM=dumb',
  'PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
  'LANG=en_US.UTF-8',
  'LC_ALL=C',
  'PYTHONIOENCODING=utf-8',
  'CARGO_NET_OFFLINE=true',
];

async function hash(text) {
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
  return [...new Uint8Array(buf)].slice(0, 6).map((b) => b.toString(16).padStart(2, '0')).join('');
}

const blobBytes = async (blob) => (blob ? new Uint8Array(await blob.arrayBuffer()) : new Uint8Array());
const quote = (s) => `'${String(s).replace(/'/g, `'\\''`)}'`;

/** Why the VM cannot start in this page, or null. */
export function unavailableReason() {
  if (typeof WebAssembly === 'undefined') return 'this browser has no WebAssembly';
  if (typeof SharedArrayBuffer === 'undefined' || !globalThis.crossOriginIsolated) {
    return 'the page is not cross-origin isolated, which CheerpX needs (SharedArrayBuffer). Use "Enable the Linux sandbox" in the coder session, which reloads the page with isolation on (current Chrome, Edge, or Firefox).';
  }
  if (typeof indexedDB === 'undefined') return 'IndexedDB is unavailable (private browsing?)';
  return null;
}

export function createCheerpxVm(options = {}) {
  const cfg = { ...VM_DEFAULTS, ...options };
  // Guest mount points of the byte channels (overridable for tests).
  const IN = cfg.in_path || '/agentmod-in';
  const OUT = cfg.out_path || '/agentmod-out';
  let cx = null;
  let io = null; // IDBDevice behind OUT
  let data = null; // DataDevice behind IN
  let booting = null;
  let info = null;
  let queue = Promise.resolve();
  let seq = 0;
  let consoleTail = '';
  let releaseLock = null;

  /** Serialize VM operations. */
  function serial(fn) {
    const run = queue.then(fn, fn);
    queue = run.catch(() => {});
    return run;
  }

  async function lock() {
    if (!globalThis.navigator?.locks) return;
    // One live VM per workspace across tabs: the IndexedDB disks are not shared-safe.
    await new Promise((resolve, reject) => {
      navigator.locks.request(`agentmod-linux-vm:${cfg.workspace}`, { ifAvailable: true }, (l) => {
        if (!l) {
          reject(new Error(`the Linux sandbox for workspace "${cfg.workspace}" is already running in another tab; close it there first`));
          return undefined;
        }
        resolve();
        return new Promise((r) => { releaseLock = r; });
      });
    });
  }

  async function boot() {
    const why = unavailableReason();
    if (why) throw new Error(`Linux sandbox unavailable: ${why}`);
    await lock();
    const CheerpX = await import(/* @vite-ignore */ cfg.cheerpx_url || `https://cxrtnc.leaningtech.com/${cfg.cheerpx_version}/cx.esm.js`);
    let block;
    if (cfg.image_type === 'cloud') {
      try {
        block = await CheerpX.CloudDevice.create(cfg.image);
      } catch (e) {
        if (!cfg.image.startsWith('wss:')) throw e;
        block = await CheerpX.CloudDevice.create(cfg.image.replace(/^wss:/, 'https:'));
      }
    } else if (cfg.image_type === 'bytes') {
      block = await CheerpX.HttpBytesDevice.create(new URL(cfg.image, globalThis.location?.href).href);
    } else if (cfg.image_type === 'github') {
      block = await CheerpX.GitHubDevice.create(cfg.image);
    } else {
      throw new Error(`unknown image_type ${cfg.image_type}`);
    }
    const cache = await CheerpX.IDBDevice.create(`agentmod-root-${await hash(cfg.image)}`);
    const root = await CheerpX.OverlayDevice.create(block, cache);
    const workspace = await CheerpX.IDBDevice.create(`agentmod-workspace-${cfg.workspace}`);
    io = await CheerpX.IDBDevice.create(`agentmod-io-${cfg.workspace}`);
    data = await CheerpX.DataDevice.create();
    cx = await CheerpX.Linux.create({
      mounts: [
        { type: 'ext2', path: '/', dev: root },
        { type: 'dir', path: cfg.workspace_path, dev: workspace },
        { type: 'dir', path: IN, dev: data },
        { type: 'dir', path: OUT, dev: io },
        { type: 'devs', path: '/dev' },
        { type: 'devpts', path: '/dev/pts' },
        { type: 'proc', path: '/proc' },
        { type: 'sys', path: '/sys' },
      ],
    });
    // Nothing is meant to reach the console (all output is redirected), but keep
    // a tail of whatever does for error messages.
    cx.setCustomConsole((buf) => {
      consoleTail = (consoleTail + new TextDecoder().decode(buf)).slice(-2000);
    }, 120, 40);
    const probe = await rawExec('uname -srm; . /etc/os-release 2>/dev/null && echo "$PRETTY_NAME"; for t in gcc g++ make python3 node git; do command -v $t >/dev/null && printf "%s " $t; done; echo', '/', 60);
    const [kernel, os, tools] = new TextDecoder().decode(probe.stdout).trim().split('\n');
    info = { kernel: kernel || 'x86', os: os || 'Linux', tools: (tools || '').trim().split(/\s+/).filter(Boolean) };
    return info;
  }

  /** One command, outside the queue (callers serialize). */
  async function rawExec(command, cwd, timeoutSeconds) {
    const n = ++seq;
    // $0 marks the operation; $1 cwd, $2 timeout, $3 command. Output is
    // redirected into the scratch filesystem so it can be read back exactly.
    // GNU `timeout` does not fire under CheerpX (its timer never expires), so a
    // watchdog subshell sleeps and then signals the command's process group.
    // `set -m` gives the command its own group, so its children go with it.
    // The watchdog's marker and its kill are tied to this operation (n), so a
    // watchdog that outlives its command can never affect a later one.
    const wrapper = [
      `: >${OUT}/stdout; : >${OUT}/stderr`,
      `cd -- "$1" 2>${OUT}/stderr || exit 126`,
      'set -m',
      `/bin/bash -c "$3" </dev/null >${OUT}/stdout 2>>${OUT}/stderr &`,
      'pid=$!',
      `echo "${n} $pid" >${OUT}/pid`,
      `( sleep "$2"; [ "$(cat ${OUT}/pid)" = "${n} $pid" ] || exit 0; : >${OUT}/timedout-${n}; kill -TERM -- -$pid; sleep 5; [ "$(cat ${OUT}/pid)" = "${n} $pid" ] && kill -KILL -- -$pid ) </dev/null >/dev/null 2>&1 &`,
      'wd=$!',
      'wait $pid; rc=$?',
      `echo "${n} done" >${OUT}/pid`,
      'kill -KILL -- -$wd 2>/dev/null',
      `[ -e ${OUT}/timedout-${n} ] && { rm -f ${OUT}/timedout-${n}; exit 124; }`,
      'exit $rc',
    ].join('\n');
    const started = Date.now();
    const { status } = await cx.run('/bin/bash', ['-c', wrapper, `agentmod-op-${n}`, cwd, String(timeoutSeconds), command], { env: ENV, cwd: '/', uid: cfg.uid, gid: cfg.gid });
    const [stdout, stderr] = await Promise.all([io.readFileAsBlob('/stdout').then(blobBytes), io.readFileAsBlob('/stderr').then(blobBytes)]);
    const timedOut = status === 124 && Date.now() - started >= timeoutSeconds * 1000;
    return { exitCode: status, stdout, stderr, timedOut };
  }

  const ready = () => {
    if (!cx) throw new Error('the Linux sandbox is not booted');
  };

  return {
    state() {
      return { booted: !!cx, booting: !!booting && !cx, info, unavailable: unavailableReason() };
    },

    boot() {
      if (!booting) {
        booting = serial(boot).catch((e) => {
          booting = null;
          releaseLock?.();
          releaseLock = null;
          throw new Error(`${e.message || e}${consoleTail ? `\n(console: ${consoleTail.slice(-400)})` : ''}`);
        });
      }
      return booting;
    },

    exec({ command, cwd, timeoutSeconds }) {
      ready();
      return serial(() => rawExec(command, cwd, Math.max(1, Math.ceil(timeoutSeconds))));
    },

    readFile({ path }) {
      ready();
      return serial(async () => {
        const q = quote(path);
        const r = await rawExec(`[ -f ${q} ] || exit 3; cp -- ${q} ${OUT}/file`, '/', 120);
        if (r.exitCode === 3) return null;
        if (r.exitCode !== 0) throw new Error(`reading ${path} failed: ${new TextDecoder().decode(r.stderr).slice(0, 300)}`);
        return blobBytes(await io.readFileAsBlob('/file'));
      });
    },

    writeFiles({ files }) {
      ready();
      return serial(async () => {
        if (!files.length) return;
        const n = ++seq;
        let r;
        if (files.length === 1 && !files[0].executable) {
          await data.writeFile(`/blob-${n}`, files[0].bytes);
          const dst = quote(files[0].path);
          r = await rawExec(`mkdir -p -- "$(dirname -- ${dst})" && cp -- ${IN}/blob-${n} ${dst}`, '/', 120);
        } else {
          // Many files: one tar archive, one process.
          await data.writeFile(`/batch-${n}.tar`, makeTar(files));
          r = await rawExec(`tar -xf ${IN}/batch-${n}.tar -C / --no-same-owner`, '/', 600);
        }
        if (r.exitCode !== 0) throw new Error(`writing files failed (exit ${r.exitCode}): ${new TextDecoder().decode(r.stderr).slice(0, 400)}`);
      });
    },

    /** Best-effort interruption of the running command (outside the queue). */
    async interrupt() {
      try {
        await cx?.run('/bin/bash', ['-c', `set -- $(cat ${OUT}/pid 2>/dev/null); [ "$2" ] && [ "$2" != done ] && kill -TERM -- -$2`], { env: ENV, cwd: '/', uid: cfg.uid, gid: cfg.gid });
      } catch { /* nothing to interrupt */ }
      return true;
    },

    dispose() {
      try { cx?.delete(); } catch { /* already gone */ }
      cx = null;
      booting = null;
      info = null;
      releaseLock?.();
      releaseLock = null;
    },
  };
}
