// Execution target: a local directory, commands run with bash (Node only).
// Implements the interface documented in ../sdk/coding/toolkit.js.
//
// Commands do NOT inherit the runtime's environment. They get an allowlisted
// base (PATH, HOME, locale, …), the plugin's configured `env`, and — per
// command — only the secrets that command was granted. The runtime's own
// variables (provider keys and anything else exported to `agentmod serve`) are
// never visible to the agent's shell.
import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

const MAX_CAPTURE = 8 * 1024 * 1024; // per stream; the tool layer truncates further

/** Variables passed through from the runtime's environment by default. */
export const BASE_ENV = ['PATH', 'HOME', 'USER', 'LOGNAME', 'LANG', 'LC_ALL', 'LC_CTYPE', 'SHELL', 'TMPDIR', 'TZ'];

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

/**
 * @param {object} o
 * @param {string} o.root
 * @param {string} [o.shell]
 * @param {string[]} [o.passEnv]   extra variable names to pass through
 * @param {object} [o.env]         fixed extra variables
 */
export function localTarget({ root, shell = '/bin/bash', passEnv = [], env = {} }) {
  let ready = null;
  const base = {};
  for (const k of [...BASE_ENV, ...passEnv]) if (process.env[k] !== undefined) base[k] = process.env[k];
  Object.assign(base, { TERM: 'dumb', PAGER: 'cat', GIT_PAGER: 'cat', GIT_TERMINAL_PROMPT: '0', WORKSPACE: root }, env);
  const host = os.hostname();
  return {
    supportsEnv: true,
    identity: () => ({ kind: 'local', id: host, root }),

    async ensureReady() {
      ready ??= fs.mkdir(root, { recursive: true });
      await ready;
    },

    exec({ command, cwd, timeoutMs, signal, env: extra }) {
      return new Promise((resolve, reject) => {
        // Own process group, so a timeout or cancel kills the whole command tree.
        const child = spawn(shell, ['-c', command], { cwd, detached: true, stdio: ['ignore', 'pipe', 'pipe'], env: { ...base, ...(extra || {}) } });
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
