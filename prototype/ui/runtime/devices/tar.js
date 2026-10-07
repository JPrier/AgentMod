// Minimal POSIX ustar writer (with PAX headers for long paths), used to move
// many files into the VM with a single `tar -x` instead of one process per file.

const enc = new TextEncoder();
const BLOCK = 512;

function octal(n, width) {
  return n.toString(8).padStart(width - 1, '0') + '\0';
}

function header({ name, size, mode, type = '0', mtime }) {
  const h = new Uint8Array(BLOCK);
  const put = (off, str, len) => h.set(enc.encode(str).subarray(0, len), off);
  put(0, name, 100);
  put(100, octal(mode, 8), 8);
  put(108, octal(0, 8), 8);
  put(116, octal(0, 8), 8);
  put(124, octal(size, 12), 12);
  put(136, octal(mtime, 12), 12);
  put(148, '        ', 8);
  put(156, type, 1);
  put(257, 'ustar\0', 6);
  put(263, '00', 2);
  put(265, 'root', 32);
  put(297, 'root', 32);
  let sum = 0;
  for (const b of h) sum += b;
  put(148, sum.toString(8).padStart(6, '0') + '\0 ', 8);
  return h;
}

function paxRecord(key, value) {
  // "<len> key=value\n" where <len> counts the whole record, itself included.
  const body = ` ${key}=${value}\n`;
  let len = enc.encode(body).length + 1;
  while (String(len).length + enc.encode(body).length !== len) len = String(len).length + enc.encode(body).length;
  return `${len}${body}`;
}

const pad = (n) => (BLOCK - (n % BLOCK)) % BLOCK;

/**
 * @param {{ path: string, bytes: Uint8Array, executable?: boolean }[]} files
 *   absolute paths; stored relative to `/` (extract with `tar -xf - -C /`)
 * @returns {Uint8Array}
 */
export function makeTar(files) {
  const mtime = Math.floor(Date.now() / 1000);
  const parts = [];
  let total = 0;
  const push = (u8) => { parts.push(u8); total += u8.length; };
  for (const f of files) {
    const name = f.path.replace(/^\/+/, '');
    if (!name || name.split('/').includes('..')) throw new Error(`refusing to archive path ${f.path}`);
    const mode = f.executable ? 0o755 : 0o644;
    if (enc.encode(name).length > 100) {
      const pax = enc.encode(paxRecord('path', name));
      push(header({ name: `PaxHeader/${name.slice(-80)}`, size: pax.length, mode: 0o644, type: 'x', mtime }));
      push(pax);
      push(new Uint8Array(pad(pax.length)));
    }
    push(header({ name, size: f.bytes.length, mode, mtime }));
    push(f.bytes);
    push(new Uint8Array(pad(f.bytes.length)));
  }
  push(new Uint8Array(BLOCK * 2));
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) { out.set(p, off); off += p.length; }
  return out;
}
