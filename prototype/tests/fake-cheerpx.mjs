// A stand-in for the CheerpX module (https://cxrtnc.leaningtech.com/<v>/cx.esm.js)
// so the browser host's `linux-vm` device can be tested under Node: devices are temp
// directories, `dir` mounts move them to their mount paths, and
// `Linux.run` runs the program on the host. It exercises everything the device
// does around CheerpX (wrapper script, byte channels, tar batches) — not
// CheerpX itself; tests/sandbox-browser.mjs covers the real thing.
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tmp = (p) => fs.mkdtempSync(path.join(os.tmpdir(), `fake-cx-${p}-`));
export const created = [];

class DirDevice {
  constructor(kind, name) {
    this.kind = kind;
    this.name = name;
    this.dir = tmp(kind);
    created.push(this);
  }
}

export class IDBDevice extends DirDevice {
  static async create(name) { return new IDBDevice('idb', name); }
  async readFileAsBlob(p) {
    try { return new Blob([fs.readFileSync(path.join(this.dir, p))]); } catch { return null; }
  }
  async reset() { fs.rmSync(this.dir, { recursive: true, force: true }); fs.mkdirSync(this.dir); }
}

export class DataDevice extends DirDevice {
  static async create() { return new DataDevice('data'); }
  async writeFile(p, data) { fs.writeFileSync(path.join(this.dir, p), data); }
}

class Block { constructor(url) { this.url = url; created.push(this); } }
export class CloudDevice extends Block { static async create(url) { if (url.includes('fail-wss') && url.startsWith('wss:')) throw new Error('wss refused'); return new CloudDevice(url); } }
export class HttpBytesDevice extends Block { static async create(url) { return new HttpBytesDevice(url); } }
export class GitHubDevice extends Block { static async create(url) { return new GitHubDevice(url); } }
export class OverlayDevice { static async create(src, idb) { return Object.assign(new OverlayDevice(), { src, idb }); } }

export class Linux {
  static async create({ mounts }) {
    const l = new Linux();
    l.mounts = mounts;
    for (const m of mounts) {
      if (m.type !== 'dir') continue;
      // The device's directory moves to the mount path (a real directory, not a
      // symlink, as a mount looks to the guest).
      fs.mkdirSync(path.dirname(m.path), { recursive: true });
      fs.rmSync(m.path, { recursive: true, force: true });
      fs.renameSync(m.dev.dir, m.path);
      m.dev.dir = m.path;
    }
    Linux.last = l;
    return l;
  }

  run(file, args, opts = {}) {
    Linux.runs = (Linux.runs || 0) + 1;
    this.running ??= new Set();
    // The device interrupts by signalling the running command's process group
    // (whose pid it recorded); stop what this fake VM started instead, so a
    // stale pid file can never signal an unrelated host process.
    if (/^p=\$\(cat .*\/pid 2>\/dev\/null\) && kill -TERM -- -\$p$/.test(args[1] || '')) {
      for (const pid of this.running) { try { process.kill(-pid, 'SIGTERM'); } catch { /* gone */ } }
      return Promise.resolve({ status: 0 });
    }
    const env = Object.fromEntries((opts.env || []).map((kv) => [kv.slice(0, kv.indexOf('=')), kv.slice(kv.indexOf('=') + 1)]));
    return new Promise((resolve, reject) => {
      const c = spawn(file, args, { cwd: opts.cwd || '/', env: { ...env, PATH: `${env.PATH}:${process.env.PATH}` }, stdio: 'ignore', detached: true });
      this.running.add(c.pid);
      c.on('error', reject);
      c.on('close', (code, sig) => {
        this.running.delete(c.pid);
        resolve({ status: code ?? (sig ? 128 + 9 : 1) });
      });
    });
  }

  setCustomConsole() { return () => {}; }
  delete() { this.deleted = true; }
}
