// Structured file mutation for `apply_patch`.
//
// Two input forms, one normalized change list:
//
//   changes: [{ action: "create" | "update" | "delete" | "move", path, to?,
//               content?, edits?: [{ old_text, new_text, replace_all? }],
//               expected_sha256? }]
//
//   patch: the "*** Begin Patch" envelope many models are trained on
//          (*** Add File / *** Update File [+ *** Move to] / *** Delete File,
//          hunks of " ", "-", "+" lines, optional "@@ anchor" lines).
//
// Edits are context-checked: every old_text (or hunk's old side) must be found
// in the file as it is *now*, so an edit planned against a stale read fails
// instead of landing somewhere else. Nothing is written until every change in
// the patch has been validated.

import { ToolError } from './paths.js';

const ACTIONS = new Set(['create', 'update', 'delete', 'move']);

/** Normalize the tool's arguments into a change list. */
export function normalizeChanges(args) {
  if (typeof args?.patch === 'string' && args.patch.trim()) return parseEnvelope(args.patch);
  const changes = args?.changes;
  if (!Array.isArray(changes) || !changes.length) throw new ToolError('apply_patch needs `changes` (a non-empty list) or a `patch` string');
  return changes.map((c, i) => {
    const where = `changes[${i}]`;
    if (!c || typeof c !== 'object') throw new ToolError(`${where} must be an object`);
    const action = String(c.action || (c.edits ? 'update' : c.content !== undefined ? 'create' : ''));
    if (!ACTIONS.has(action)) throw new ToolError(`${where}.action must be create, update, delete, or move`);
    if (typeof c.path !== 'string' || !c.path.trim()) throw new ToolError(`${where}.path is required`);
    const out = { action, path: c.path };
    if (c.expected_sha256 != null) out.expected_sha256 = String(c.expected_sha256).toLowerCase();
    if (action === 'create') {
      if (typeof c.content !== 'string') throw new ToolError(`${where}: create needs \`content\` (a string)`);
      out.content = c.content;
      out.overwrite = !!c.overwrite;
    }
    if (action === 'update' || action === 'move') {
      if (c.content !== undefined && typeof c.content !== 'string') throw new ToolError(`${where}.content must be a string`);
      if (c.content !== undefined) out.content = c.content;
      if (c.edits !== undefined) out.edits = checkEdits(c.edits, where);
      if (action === 'update' && out.content === undefined && !out.edits) throw new ToolError(`${where}: update needs \`edits\` or \`content\``);
    }
    if (action === 'move') {
      if (typeof c.to !== 'string' || !c.to.trim()) throw new ToolError(`${where}: move needs \`to\``);
      out.to = c.to;
    }
    return out;
  });
}

function checkEdits(edits, where) {
  if (!Array.isArray(edits) || !edits.length) throw new ToolError(`${where}.edits must be a non-empty list`);
  return edits.map((e, j) => {
    if (typeof e?.old_text !== 'string' || !e.old_text) throw new ToolError(`${where}.edits[${j}].old_text must be a non-empty string (to insert, include a neighboring line in old_text and new_text)`);
    if (typeof (e.new_text ?? '') !== 'string') throw new ToolError(`${where}.edits[${j}].new_text must be a string`);
    return { old_text: e.old_text, new_text: e.new_text ?? '', replace_all: !!e.replace_all };
  });
}

/** Parse a "*** Begin Patch" envelope. */
export function parseEnvelope(text) {
  const lines = String(text).replace(/\r\n/g, '\n').split('\n');
  let i = 0;
  while (i < lines.length && !lines[i].trim()) i++;
  if (lines[i]?.trim() !== '*** Begin Patch') throw new ToolError('a patch must start with "*** Begin Patch"');
  i++;
  const changes = [];
  const header = (l) => /^\*\*\* (Add File|Update File|Delete File|End Patch|Move to):?/.test(l);
  while (i < lines.length) {
    const l = lines[i];
    if (l.trim() === '*** End Patch') return finish(changes);
    let m;
    if ((m = l.match(/^\*\*\* Add File: (.+)$/))) {
      i++;
      const body = [];
      while (i < lines.length && !header(lines[i])) {
        if (!lines[i].startsWith('+') && lines[i] !== '') throw new ToolError(`Add File ${m[1]}: every content line must start with "+" (line ${i + 1})`);
        if (lines[i] !== '') body.push(lines[i].slice(1));
        i++;
      }
      changes.push({ action: 'create', path: m[1].trim(), content: body.length ? `${body.join('\n')}\n` : '' });
    } else if ((m = l.match(/^\*\*\* Delete File: (.+)$/))) {
      changes.push({ action: 'delete', path: m[1].trim() });
      i++;
    } else if ((m = l.match(/^\*\*\* Update File: (.+)$/))) {
      i++;
      const change = { action: 'update', path: m[1].trim(), hunks: [] };
      const mv = lines[i]?.match(/^\*\*\* Move to: (.+)$/);
      if (mv) {
        change.action = 'move';
        change.to = mv[1].trim();
        i++;
      }
      let hunk = null;
      while (i < lines.length && !header(lines[i])) {
        const h = lines[i];
        if (h.startsWith('@@')) {
          hunk = { anchor: h.replace(/^@@\s?/, '').trim(), lines: [] };
          change.hunks.push(hunk);
        } else if (h === '*** End of File') {
          if (hunk) hunk.eof = true;
        } else {
          const op = h[0];
          if (op !== ' ' && op !== '-' && op !== '+' && h !== '') throw new ToolError(`Update File ${change.path}: hunk lines must start with " ", "-", or "+" (line ${i + 1}: ${JSON.stringify(h.slice(0, 60))})`);
          if (!hunk) {
            hunk = { anchor: '', lines: [] };
            change.hunks.push(hunk);
          }
          hunk.lines.push(h === '' ? [' ', ''] : [op, h.slice(1)]);
        }
        i++;
      }
      // Blank lines after the last hunk line are separators, not context.
      for (const hk of change.hunks) while (hk.lines.length && hk.lines[hk.lines.length - 1][1] === '' && hk.lines[hk.lines.length - 1][0] === ' ') hk.lines.pop();
      if (change.action === 'update' && !change.hunks.some((x) => x.lines.length)) throw new ToolError(`Update File ${change.path} has no hunks`);
      changes.push(change);
    } else if (!l.trim()) {
      i++;
    } else {
      throw new ToolError(`unexpected patch line ${i + 1}: ${JSON.stringify(l.slice(0, 80))}`);
    }
  }
  throw new ToolError('a patch must end with "*** End Patch"');
}

function finish(changes) {
  if (!changes.length) throw new ToolError('the patch contains no file changes');
  return changes;
}

// ---------------------------------------------------------------------------
// Applying edits to text
// ---------------------------------------------------------------------------

function indexesOf(hay, needle) {
  const out = [];
  let at = hay.indexOf(needle);
  while (at >= 0) {
    out.push(at);
    at = hay.indexOf(needle, at + needle.length);
  }
  return out;
}

/** Offsets of `old` in `text` comparing lines with trailing whitespace ignored. */
function looseMatches(text, old) {
  const tl = text.split('\n');
  const ol = old.replace(/\n$/, '').split('\n');
  const norm = (s) => s.replace(/\s+$/, '');
  const starts = [];
  for (let i = 0; i + ol.length <= tl.length; i++) {
    let ok = true;
    for (let k = 0; k < ol.length && ok; k++) ok = norm(tl[i + k]) === norm(ol[k]);
    if (ok) starts.push(i);
  }
  // Convert line starts to [offset, length] spans in the original text.
  const lineOffset = [];
  for (let i = 0, off = 0; i <= tl.length; i++) {
    lineOffset.push(off);
    off += (tl[i]?.length ?? 0) + 1;
  }
  const endsWithNl = old.endsWith('\n');
  return starts.map((s) => {
    const from = lineOffset[s];
    const lastLine = s + ol.length - 1;
    const to = lineOffset[lastLine] + tl[lastLine].length + (endsWithNl ? 1 : 0);
    return [from, to - from];
  });
}

function nearest(text, oldText) {
  const first = oldText.split('\n').find((l) => l.trim()) || '';
  const want = first.trim().toLowerCase();
  if (!want) return '';
  const lines = text.split('\n');
  let best = -1;
  let bestScore = 0;
  for (let i = 0; i < lines.length; i++) {
    const l = lines[i].trim().toLowerCase();
    if (!l) continue;
    let score = 0;
    const n = Math.min(l.length, want.length);
    while (score < n && l[score] === want[score]) score++;
    if (l.includes(want) || want.includes(l)) score += 20;
    if (score > bestScore) { bestScore = score; best = i; }
  }
  return best >= 0 && bestScore >= 4 ? ` The closest line is ${best + 1}: ${JSON.stringify(lines[best].slice(0, 120))}.` : '';
}

/** Apply old_text/new_text edits in order; returns the new text. */
export function applyEdits(text, edits, path) {
  const crlf = text.includes('\r\n');
  let out = text;
  edits.forEach((e, j) => {
    const fix = (s) => (crlf ? s.replace(/\r?\n/g, '\r\n') : s);
    const oldText = fix(e.old_text);
    const newText = fix(e.new_text);
    let spans = indexesOf(out, oldText).map((at) => [at, oldText.length]);
    if (!spans.length) spans = looseMatches(out, oldText);
    if (!spans.length) {
      throw new ToolError(`edit ${j + 1} for ${path}: old_text was not found in the file as it is now (it may have changed since you read it; re-read it).${nearest(out, oldText)}`);
    }
    if (spans.length > 1 && !e.replace_all) {
      throw new ToolError(`edit ${j + 1} for ${path}: old_text matches ${spans.length} places; add surrounding lines to make it unique, or set replace_all`);
    }
    // Apply right-to-left so earlier offsets stay valid.
    for (const [at, len] of [...spans].reverse()) out = out.slice(0, at) + newText + out.slice(at + len);
  });
  return out;
}

/** Apply "*** Update File" hunks; returns the new text. */
export function applyHunks(text, hunks, path) {
  const crlf = text.includes('\r\n');
  const nl = crlf ? '\r\n' : '\n';
  let lines = text.split(nl);
  const trailing = lines.length && lines[lines.length - 1] === '';
  if (trailing) lines.pop();
  let cursor = 0;
  hunks.forEach((h, j) => {
    if (h.anchor) {
      const at = lines.findIndex((l, i) => i >= cursor && l.includes(h.anchor));
      const loose = at < 0 ? lines.findIndex((l, i) => i >= cursor && l.trim().includes(h.anchor.trim())) : at;
      if (loose < 0) throw new ToolError(`hunk ${j + 1} for ${path}: anchor ${JSON.stringify(h.anchor.slice(0, 80))} was not found after line ${cursor}`);
      cursor = loose + 1;
    }
    const oldSide = h.lines.filter(([op]) => op !== '+').map(([, l]) => l);
    const newSide = h.lines.filter(([op]) => op !== '-').map(([, l]) => l);
    if (!oldSide.length) {
      // Pure insertion: at the anchor, or at the end of file.
      const at = h.anchor ? cursor : h.eof ? lines.length : cursor;
      lines.splice(at, 0, ...newSide);
      cursor = at + newSide.length;
      return;
    }
    const find = (eq) => {
      const hits = [];
      for (let i = cursor; i + oldSide.length <= lines.length; i++) {
        let ok = true;
        for (let k = 0; k < oldSide.length && ok; k++) ok = eq(lines[i + k], oldSide[k]);
        if (ok) hits.push(i);
      }
      return hits;
    };
    let hits = find((a, b) => a === b);
    if (!hits.length) hits = find((a, b) => a.replace(/\s+$/, '') === b.replace(/\s+$/, ''));
    if (!hits.length) hits = find((a, b) => a.trim() === b.trim());
    if (h.eof && hits.length) hits = [hits[hits.length - 1]];
    if (!hits.length) {
      throw new ToolError(`hunk ${j + 1} for ${path}: its context/removed lines were not found after line ${cursor} (the file may have changed; re-read it).${nearest(lines.join('\n'), oldSide.join('\n'))}`);
    }
    const at = hits[0];
    lines.splice(at, oldSide.length, ...newSide);
    cursor = at + newSide.length;
  });
  return lines.join(nl) + (trailing || !text ? nl : '');
}
