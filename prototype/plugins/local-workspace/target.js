// Execution target: a local directory, commands run with bash (Node only).
// Implements the interface documented in ../sdk/workspace-tools.js.
import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import path from 'node:path';

const MAX_CAPTURE = 8 * 1024 * 1024; // per stream; the tool layer truncates further

function collect(stream) {
  const chunks = [];
  let size = 0;
  stream.on('data', (c) => {
    if (size >= MAX_CAPTURE) return;
    chunks.push(c);
    size += c.length;
  });
  return () => new Uint8Array(Buffer.concat(chunks).subarray(0, MAX_CAPTURE));
}

export function localTarget({ root, shell = '/bin/bash' }) {
  let ready = null;
  return {
    async ensureReady() {
      ready ??= fs.mkdir(root, { recursive: true });
      await ready;
    },

    exec({ command, cwd, timeoutMs, signal }) {
      return new Promise((resolve, reject) => {
        // Own process group, so a timeout or cancel kills the whole command tree.
        const child = spawn(shell, ['-c', command], { cwd, detached: true, stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, WORKSPACE: root } });
        const out = collect(child.stdout);
        const err = collect(child.stderr);
        let timedOut = false;
        const killTree = (sig) => {
          try { process.kill(-child.pid, sig); } catch { /* already gone */ }
        };
        const timer = setTimeout(() => {
          timedOut = true;
          killTree('SIGTERM');
          setTimeout(() => killTree('SIGKILL'), 3000).unref();
        }, timeoutMs);
        const onAbort = () => killTree('SIGKILL');
        signal?.addEventListener('abort', onAbort, { once: true });
        child.on('error', (e) => {
          clearTimeout(timer);
          signal?.removeEventListener('abort', onAbort);
          reject(e);
        });
        child.on('close', (code, sig) => {
          clearTimeout(timer);
          signal?.removeEventListener('abort', onAbort);
          if (signal?.aborted) return reject(new Error('cancelled'));
          resolve({ exitCode: code ?? (timedOut ? 124 : 128 + (sig ? 9 : 0)), stdout: out(), stderr: err(), timedOut });
        });
      });
    },

    async readFile(p) {
      try {
        const st = await fs.stat(p);
        if (!st.isFile()) return null;
        return new Uint8Array(await fs.readFile(p));
      } catch (e) {
        if (e.code === 'ENOENT' || e.code === 'ENOTDIR') return null;
        throw e;
      }
    },

    async writeFile(p, bytes) {
      await fs.mkdir(path.dirname(p), { recursive: true });
      await fs.writeFile(p, bytes);
    },

    async writeFiles(files) {
      for (const f of files) {
        await this.writeFile(f.path, f.bytes);
        if (f.executable) await fs.chmod(f.path, 0o755);
      }
    },
  };
}

localTarget.resolveRoot = (root) => path.resolve(root);

localTarget.check = async (shell) => {
  try {
    await fs.access(shell, fs.constants.X_OK);
  } catch {
    throw new Error(`local-workspace: shell ${shell} is not executable`);
  }
};
