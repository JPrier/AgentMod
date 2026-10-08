// A thin layer over an execution target's `exec`: text decoding, environment
// injection, and secret redaction.
//
// Targets differ in how a per-command environment reaches the shell. A target
// that sets `supportsEnv` takes `env` directly (the local target spawns bash
// with exactly that environment). Otherwise (the browser VM, whose device runs
// commands with a fixed environment) the variables are written to a file the
// command sources and deletes first, so values never appear in a command line
// — and so never in the VM's own operation journal.

import { decode, encode } from './text.js';
import { shq } from './paths.js';

let envSeq = 0;

/**
 * @param {object} o
 * @param {object} o.target
 * @param {string} o.stateDir   absolute state directory in the target (for env files)
 */
export function makeRunner({ target, stateDir }) {
  /**
   * Run a shell command line.
   * @returns {Promise<{exitCode:number, stdout:string, stderr:string, stdoutBytes:Uint8Array, stderrBytes:Uint8Array, timedOut:boolean, ms:number}>}
   */
  async function run(command, { cwd = '/', timeoutMs = 60_000, signal, env } = {}) {
    let line = command;
    const extra = env && Object.keys(env).length ? env : null;
    if (extra && !target.supportsEnv) {
      const file = `${stateDir}/env/e${Date.now().toString(36)}${(envSeq++).toString(36)}.sh`;
      const body = Object.entries(extra).map(([k, v]) => `export ${k}=${shq(v)}`).join('\n') + '\n';
      await target.writeFile(file, encode(body));
      line = `. ${shq(file)}; rm -f ${shq(file)}; ${command}`;
    }
    const t0 = Date.now();
    const r = await target.exec({ command: line, cwd, timeoutMs, signal, ...(extra && target.supportsEnv ? { env: extra } : {}) });
    return {
      exitCode: r.exitCode,
      timedOut: !!r.timedOut,
      stdoutBytes: r.stdout || new Uint8Array(),
      stderrBytes: r.stderr || new Uint8Array(),
      stdout: decode(r.stdout),
      stderr: decode(r.stderr),
      ms: Date.now() - t0,
    };
  }

  /** Run and require exit 0 (internal plumbing). */
  async function must(command, opts = {}) {
    const r = await run(command, opts);
    if (r.exitCode !== 0) {
      const msg = (r.stderr || r.stdout).trim().split('\n').slice(-6).join('\n');
      throw new Error(`${opts.what || 'command'} failed (exit ${r.exitCode}${r.timedOut ? ', timed out' : ''}): ${msg.slice(0, 600)}`);
    }
    return r;
  }

  return { run, must };
}

/** Build a redactor for secret values: every occurrence becomes «secret:NAME». */
export function makeRedactor(values = {}) {
  const pairs = Object.entries(values)
    .filter(([, v]) => typeof v === 'string' && v.length >= 4)
    .sort((a, b) => b[1].length - a[1].length);
  return (text) => {
    if (!pairs.length || typeof text !== 'string') return text;
    let out = text;
    for (const [name, v] of pairs) out = out.split(v).join(`«secret:${name}»`);
    return out;
  };
}
