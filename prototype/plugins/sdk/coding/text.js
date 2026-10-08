// Text helpers shared by the coding tools: decoding, bounds, hashes, diffs.

const enc = new TextEncoder();
const lossy = new TextDecoder('utf-8', { fatal: false });
const strict = new TextDecoder('utf-8', { fatal: true });

export const encode = (s) => enc.encode(s);
export const decode = (bytes) => lossy.decode(bytes || new Uint8Array());

/** Decode UTF-8; `{ text, valid }` where invalid sequences became U+FFFD. */
export function decodeChecked(bytes) {
  try {
    return { text: strict.decode(bytes), valid: true };
  } catch {
    return { text: lossy.decode(bytes), valid: false };
  }
}

export function isBinary(bytes) {
  const n = Math.min(bytes.length, 8000);
  for (let i = 0; i < n; i++) if (bytes[i] === 0) return true;
  return false;
}

/** Lowercase hex SHA-256 of bytes or a string (WebCrypto: Node and workers). */
export async function sha256(data) {
  const bytes = typeof data === 'string' ? enc.encode(data) : data;
  const buf = await globalThis.crypto.subtle.digest('SHA-256', bytes);
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

/** Keep the head and tail of long output, marking what was dropped. */
export function truncate(text, maxBytes) {
  const bytes = enc.encode(text);
  if (bytes.length <= maxBytes) return text;
  const head = Math.floor(maxBytes * 0.6);
  const tail = maxBytes - head;
  const a = lossy.decode(bytes.subarray(0, head));
  const b = lossy.decode(bytes.subarray(bytes.length - tail));
  return `${a}\n… [${bytes.length - head - tail} bytes omitted] …\n${b}`;
}

/** Byte length of a string as UTF-8. */
export const byteLength = (s) => enc.encode(s).length;

/** Split text into lines (a trailing newline does not make an extra empty line). */
export function splitLines(text) {
  if (text === '') return [];
  const lines = text.split('\n');
  if (text.endsWith('\n')) lines.pop();
  return lines;
}

/** `cat -n`-style numbered lines, long lines clipped. */
export function numberLines(lines, start, maxLineChars = 2000) {
  const width = String(start + lines.length - 1).length;
  return lines
    .map((l, i) => {
      const clipped = l.length > maxLineChars ? `${l.slice(0, maxLineChars)}… [line clipped, ${l.length} chars]` : l;
      return `${String(start + i).padStart(width, ' ')}\t${clipped}`;
    })
    .join('\n');
}

/** Rough token estimate (≈4 bytes per token) for budgeting and metrics. */
export const estimateTokens = (s) => Math.ceil(byteLength(typeof s === 'string' ? s : JSON.stringify(s ?? '')) / 4);

/**
 * Unified diff of two texts (3 lines of context). Common prefix and suffix are
 * stripped first, so a small edit to a large file stays cheap; a very large
 * changed region falls back to one replace hunk.
 */
export function unifiedDiff(path, before, after, context = 3) {
  // A final line without "\n" is a different line from the same text with one;
  // mark it so it compares unequal and prints with diff(1)'s note.
  const NOEOL = '\u0000noeol';
  const toLines = (t) => {
    if (t === '') return [];
    const l = t.split('\n');
    if (t.endsWith('\n')) l.pop();
    else l[l.length - 1] += NOEOL;
    return l;
  };
  const a = toLines(before);
  const b = toLines(after);
  let pre = 0;
  while (pre < a.length && pre < b.length && a[pre] === b[pre]) pre++;
  let suf = 0;
  while (suf < a.length - pre && suf < b.length - pre && a[a.length - 1 - suf] === b[b.length - 1 - suf]) suf++;
  const am = a.slice(pre, a.length - suf);
  const bm = b.slice(pre, b.length - suf);
  if (!am.length && !bm.length) return '';
  const ops = [];
  if (am.length * bm.length <= 4_000_000) {
    const n = am.length;
    const m = bm.length;
    const t = new Uint32Array((n + 1) * (m + 1));
    for (let i = n - 1; i >= 0; i--) {
      for (let j = m - 1; j >= 0; j--) {
        t[i * (m + 1) + j] = am[i] === bm[j] ? t[(i + 1) * (m + 1) + j + 1] + 1 : Math.max(t[(i + 1) * (m + 1) + j], t[i * (m + 1) + j + 1]);
      }
    }
    let i = 0;
    let j = 0;
    while (i < n && j < m) {
      if (am[i] === bm[j]) { ops.push([' ', am[i]]); i++; j++; }
      else if (t[(i + 1) * (m + 1) + j] >= t[i * (m + 1) + j + 1]) ops.push(['-', am[i++]]);
      else ops.push(['+', bm[j++]]);
    }
    while (i < n) ops.push(['-', am[i++]]);
    while (j < m) ops.push(['+', bm[j++]]);
  } else {
    for (const l of am) ops.push(['-', l]);
    for (const l of bm) ops.push(['+', l]);
  }
  const all = [...a.slice(0, pre).map((l) => [' ', l]), ...ops, ...a.slice(a.length - suf).map((l) => [' ', l])];
  const aAt = new Array(all.length);
  const bAt = new Array(all.length);
  for (let k = 0, ai = 1, bi = 1; k < all.length; k++) {
    aAt[k] = ai;
    bAt[k] = bi;
    if (all[k][0] !== '+') ai++;
    if (all[k][0] !== '-') bi++;
  }
  const changed = [];
  for (let k = 0; k < all.length; k++) if (all[k][0] !== ' ') changed.push(k);
  const groups = [];
  for (const k of changed) {
    const g = groups[groups.length - 1];
    if (g && k - g[1] - 1 <= 2 * context) g[1] = k;
    else groups.push([k, k]);
  }
  const lines = [`--- a${path}`, `+++ b${path}`];
  for (const [first, last] of groups) {
    const start = Math.max(0, first - context);
    const end = Math.min(all.length, last + context + 1);
    const hunk = all.slice(start, end);
    const aLen = hunk.filter((o) => o[0] !== '+').length;
    const bLen = hunk.filter((o) => o[0] !== '-').length;
    lines.push(`@@ -${aLen ? aAt[start] : aAt[start] - 1},${aLen} +${bLen ? bAt[start] : bAt[start] - 1},${bLen} @@`);
    for (const [op, l] of hunk) {
      if (l.endsWith(NOEOL)) lines.push(op + l.slice(0, -NOEOL.length), '\\ No newline at end of file');
      else lines.push(op + l);
    }
  }
  return lines.join('\n');
}

/** +added/-removed line counts of a unified diff. */
export function diffStat(unified) {
  let added = 0;
  let removed = 0;
  for (const l of unified.split('\n')) {
    if (l.startsWith('+++') || l.startsWith('---')) continue;
    if (l.startsWith('+')) added++;
    else if (l.startsWith('-')) removed++;
  }
  return { added, removed };
}
