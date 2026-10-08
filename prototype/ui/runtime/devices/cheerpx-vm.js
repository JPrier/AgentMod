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

// Kill a process and its descendants, children first (no process groups needed).
const KILLTREE = 'killtree() { local c; for c in $(pgrep -P "$1"); do killtree "$c"; done; kill -KILL "$1" 2>/dev/null; }';

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
  // How long past a command's own timeout to wait before declaring the VM dead.
  const STALL_GRACE = Number(cfg.stall_grace_seconds ?? 60);
  let cx = null;
  let io = null; // IDBDevice behind OUT
  let data = null; // DataDevice behind IN
  let booting = null;
  let info = null;
  let bootedAt = null;
  let queue = Promise.resolve();
  let gen = 0; // bumped by stop/restart: work from an older VM is discarded
  let seq = 0;
  let consoleTail = '';
  let broken = null; // set when the VM stops responding
  let releaseLock = null;
  let running = null; // the operation in flight: { n, command, started }
  let processes = 0; // processes CheerpX reports created since boot
  const stats = { ops: 0, failed: 0, timedOut: 0, boots: 0 };

  // The VM journal: what happened to the VM itself, kept on the page so it
  // survives a crash of the guest. The agent reads it with `sandbox_logs`.
  const journal = [];
  const log = (kind, detail = {}) => {
    journal.push({ at: Date.now(), kind, ...detail });
    if (journal.length > 500) journal.splice(0, journal.length - 500);
  };
  const clip = (t, n = 160) => (String(t).length > n ? `${String(t).slice(0, n)}…` : String(t));

  // Page-level errors while the VM is up: a CheerpX crash surfaces here (for
  // example a WebAssembly "memory access out of bounds"), not inside the guest.
  const onError = (e) => log('page-error', { message: clip(e?.message || e?.error?.message || String(e), 400) });
  const onRejection = (e) => log('page-error', { message: clip(e?.reason?.message || String(e?.reason), 400), unhandled: true });
  const watchPage = (on) => {
    if (typeof globalThis.addEventListener !== 'function') return;
    globalThis[on ? 'addEventListener' : 'removeEventListener']('error', onError);
    globalThis[on ? 'addEventListener' : 'removeEventListener']('unhandledrejection', onRejection);
  };

  /** Serialize VM operations (per VM generation). */
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
    const t0 = Date.now();
    log('boot-start', { image: cfg.image, cheerpx: cfg.cheerpx_version, workspace: cfg.workspace });
    await lock();
    const CheerpX = await import(/* @vite-ignore */ cfg.cheerpx_url || `https://cxrtnc.leaningtech.com/${cfg.cheerpx_version}/cx.esm.js`);
    let block;
    if (cfg.image_type === 'cloud') {
      try {
        block = await CheerpX.CloudDevice.create(cfg.image);
      } catch (e) {
        if (!cfg.image.startsWith('wss:')) throw e;
        log('note', { message: `wss: disk connection failed (${clip(e.message || e)}); retrying over https:` });
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
    watchPage(true);
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
    // a tail of whatever does: it is often the VM's last words.
    cx.setCustomConsole((buf) => {
      consoleTail = (consoleTail + new TextDecoder().decode(buf)).slice(-4000);
    }, 120, 40);
    try {
      cx.registerCallback?.('processCreated', () => { processes += 1; });
    } catch { /* optional instrumentation */ }
    const probe = await rawExec('uname -srm; . /etc/os-release 2>/dev/null && echo "$PRETTY_NAME"; for t in gcc g++ make python3 node git; do command -v $t >/dev/null && printf "%s " $t; done; echo', '/', 60);
    const [kernel, os, tools] = new TextDecoder().decode(probe.stdout).trim().split('\n');
    info = { kernel: kernel || 'x86', os: os || 'Linux', tools: (tools || '').trim().split(/\s+/).filter(Boolean) };
    bootedAt = Date.now();
    stats.boots += 1;
    log('boot-ok', { ms: bootedAt - t0, ...info });
    return info;
  }

  /** One command, outside the queue (callers serialize). */
  async function rawExec(command, cwd, timeoutSeconds) {
    const n = ++seq;
    const myGen = gen;
    // $0 marks the operation; $1 cwd, $2 timeout, $3 command. Output is
    // redirected into the scratch filesystem so it can be read back exactly.
    // GNU `timeout` does not fire under CheerpX (its timer never expires), so a
    // watchdog subshell counts seconds and kills the command when time is up.
    // Two CheerpX findings shape it: signals to a pid work but process-group
    // kills do not, and SIGKILLing a process that still has a running child
    // can crash the VM. So the watchdog is never killed — it polls once a
    // second and exits by itself once the command is done — and on timeout it
    // kills the command's process tree children-first (`pgrep -P`). Its marker
    // and its kill are tied to this operation (n), so it can never affect a
    // later one.
    const wrapper = [
      `: >${OUT}/stdout; : >${OUT}/stderr`,
      `cd -- "$1" 2>${OUT}/stderr || exit 126`,
      KILLTREE,
      'set -m',
      `/bin/bash -c "$3" </dev/null >${OUT}/stdout 2>>${OUT}/stderr &`,
      'pid=$!',
      `echo "${n} $pid" >${OUT}/pid`,
      `( i=0; while [ $i -lt "$2" ]; do sleep 1; i=$((i+1)); [ "$(cat ${OUT}/pid)" = "${n} $pid" ] || exit 0; done; : >${OUT}/timedout-${n}; killtree $pid ) </dev/null >/dev/null 2>&1 &`,
      'wait $pid; rc=$?',
      `echo "${n} done" >${OUT}/pid`,
      `[ -e ${OUT}/timedout-${n} ] && { rm -f ${OUT}/timedout-${n}; exit 124; }`,
      'exit $rc',
    ].join('\n');
    const started = Date.now();
    running = { n, command: clip(command), started };
    // If CheerpX itself dies (a WebAssembly trap), cx.run never settles; stop
    // waiting well after the command's own timeout and retire the VM.
    let guard;
    const stalled = new Promise((_, reject) => {
      guard = setTimeout(() => {
        if (myGen !== gen) return;
        broken = `the Linux sandbox stopped responding during operation #${n} (\`${clip(command, 80)}\`); the VM has probably crashed. Call sandbox_logs to see why and sandbox_restart to start a fresh VM (/workspace is kept).`;
        log('stalled', { op: n, command: clip(command), waited_ms: Date.now() - started, console_tail: clip(consoleTail.slice(-600), 600) });
        reject(new Error(broken));
      }, (timeoutSeconds + STALL_GRACE) * 1000);
    });
    let status;
    try {
      ({ status } = await Promise.race([cx.run('/bin/bash', ['-c', wrapper, `agentmod-op-${n}`, cwd, String(timeoutSeconds), command], { env: ENV, cwd: '/', uid: cfg.uid, gid: cfg.gid }), stalled]));
    } catch (e) {
      if (myGen === gen && !broken) log('op-error', { op: n, command: clip(command), error: clip(e?.message || e, 400) });
      throw e;
    } finally {
      clearTimeout(guard);
      if (running?.n === n) running = null;
    }
    if (myGen !== gen) throw new Error('the Linux sandbox was restarted while this command ran');
    const [stdout, stderr] = await Promise.all([io.readFileAsBlob('/stdout').then(blobBytes), io.readFileAsBlob('/stderr').then(blobBytes)]);
    const timedOut = status === 124 && Date.now() - started >= timeoutSeconds * 1000;
    stats.ops += 1;
    if (status !== 0) stats.failed += 1;
    if (timedOut) stats.timedOut += 1;
    log('op', { op: n, command: clip(command), cwd, exit: status, ms: Date.now() - started, ...(timedOut ? { timed_out: true } : {}), stdout_bytes: stdout.length, stderr_bytes: stderr.length });
    return { exitCode: status, stdout, stderr, timedOut };
  }

  const ready = () => {
    if (broken) throw new Error(broken);
    if (!cx) throw new Error('the Linux sandbox is not running');
  };

  function stop(reason) {
    gen += 1;
    queue = Promise.resolve(); // a stalled operation no longer blocks anything
    try { cx?.delete(); } catch (e) { log('note', { message: `deleting the old VM failed: ${clip(e?.message || e)}` }); }
    watchPage(false);
    const wasUp = !!cx || !!booting;
    cx = null;
    io = null;
    data = null;
    booting = null;
    info = null;
    bootedAt = null;
    broken = null;
    running = null;
    releaseLock?.();
    releaseLock = null;
    if (wasUp) log('stopped', { reason });
  }

  function start() {
    if (broken) return Promise.reject(new Error(broken));
    if (!booting) {
      booting = serial(boot).catch((e) => {
        log('boot-failed', { error: clip(e?.message || e, 400), console_tail: clip(consoleTail.slice(-600), 600) });
        booting = null;
        watchPage(false);
        releaseLock?.();
        releaseLock = null;
        throw new Error(`${e.message || e}${consoleTail ? `\n(console: ${consoleTail.slice(-400)})` : ''}`);
      });
    }
    return booting;
  }

  return {
    state() {
      return { booted: !!cx && !broken, booting: !!booting && !cx, broken, info, unavailable: broken || unavailableReason() };
    },

    /** Everything known about the VM, for `sandbox_status`. */
    status() {
      const env = globalThis.navigator || {};
      return {
        state: broken ? 'crashed' : cx ? (running ? 'busy' : 'running') : booting ? 'booting' : 'stopped',
        broken,
        info,
        uptime_s: bootedAt ? Math.round((Date.now() - bootedAt) / 1000) : null,
        running: running ? { op: running.n, command: running.command, for_s: Math.round((Date.now() - running.started) / 1000) } : null,
        stats: { ...stats, processes_created: processes },
        config: { image: cfg.image, image_type: cfg.image_type, cheerpx: cfg.cheerpx_version, workspace: cfg.workspace, workspace_path: cfg.workspace_path },
        page: {
          cross_origin_isolated: !!globalThis.crossOriginIsolated,
          user_agent: env.userAgent || null,
          cores: env.hardwareConcurrency || null,
          memory_gb: env.deviceMemory || null,
          js_heap_mb: globalThis.performance?.memory ? Math.round(globalThis.performance.memory.usedJSHeapSize / 1048576) : null,
        },
        unavailable: unavailableReason(),
      };
    },

    /** The VM journal (newest last), plus the console tail. */
    logs({ limit = 100, kinds } = {}) {
      const want = Array.isArray(kinds) && kinds.length ? new Set(kinds) : null;
      const entries = journal.filter((e) => !want || want.has(e.kind));
      return { entries: entries.slice(-Math.max(1, Math.min(500, limit))), total: entries.length, console_tail: consoleTail.slice(-2000) };
    },

    boot: start,

    /** Throw away the current VM (crashed or not) and boot a fresh one. */
    async restart() {
      stop('restart');
      log('restart', {});
      return start();
    },

    stop() {
      stop('stop');
      return true;
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
      if (!cx || broken) return false;
      log('interrupt', { op: running?.n ?? null });
      try {
        await cx.run('/bin/bash', ['-c', `${KILLTREE}\nset -- $(cat ${OUT}/pid 2>/dev/null); [ "$2" ] && [ "$2" != done ] && killtree $2`], { env: ENV, cwd: '/', uid: cfg.uid, gid: cfg.gid });
      } catch { /* nothing to interrupt */ }
      return true;
    },

    dispose() {
      stop('dispose');
    },
  };
}
