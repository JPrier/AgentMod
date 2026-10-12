// Tool argument schemas as shapes, and back (compiler.md §3.4).
//
// A tool exists because a plugin subscribes to its member (`tool-call/<name>`),
// and the tool's argument schema *is* that subscription's `receives.args`
// shape. Tool plugins still write their parameters the way models read them
// (JSON-Schema-style property maps, see `toolSpec`); `argsKey` turns them into
// the shape the subscription declares. The loop plugin, which offers the tools
// to the model, turns the resolved member's shape back into a JSON schema with
// `schemaOfKeys`. Descriptions travel with the shapes (documentation only).
//
// Shapes are a small type language, so the mapping is deliberately lossy where
// JSON Schema is richer: defaults and bounds move into the description, and
// constructs a shape cannot express (`oneOf`, a missing `type`) become the most
// permissive shape, so a valid call is never rejected for them.

const ANY = 'string | number | bool | null | object | array<string | number | bool | null | object>';
const LITERAL_OK = (v) => typeof v === 'string' && !(v.includes('"') && v.includes("'"));
const literal = (v) => (v.includes('"') ? `'${v}'` : `"${v}"`);

function describe(schema) {
  const notes = [];
  if (schema.default !== undefined) notes.push(`default: ${JSON.stringify(schema.default)}`);
  if (schema.minimum !== undefined) notes.push(`minimum: ${schema.minimum}`);
  if (schema.maximum !== undefined) notes.push(`maximum: ${schema.maximum}`);
  const text = [schema.description, notes.length ? `(${notes.join(', ')})` : ''].filter(Boolean).join(' ');
  return text || undefined;
}

/** A JSON-schema type (or schema) as a shape string plus nested keys. */
function shapeOf(schema) {
  if (!schema || typeof schema !== 'object') return { shape: ANY };
  if (Array.isArray(schema.enum) && schema.enum.length && schema.enum.every(LITERAL_OK)) {
    return { shape: schema.enum.map(literal).join(' | ') };
  }
  const types = Array.isArray(schema.type) ? schema.type : schema.type ? [schema.type] : [];
  if (!types.length) return { shape: ANY };
  let keys;
  const terms = types.map((t) => {
    switch (t) {
      case 'string': return 'string';
      case 'integer': return 'integer';
      case 'number': return 'number';
      case 'boolean': return 'bool';
      case 'null': return 'null';
      case 'object':
        if (schema.properties && Object.keys(schema.properties).length) keys = keysOf(schema.properties, schema.required);
        return 'object';
      case 'array': {
        const inner = shapeOf(schema.items || {});
        if (inner.keys) keys = inner.keys;
        return `array<${inner.shape}>`;
      }
      default: return ANY;
    }
  });
  return keys ? { shape: terms.join(' | '), keys } : { shape: terms.join(' | ') };
}

/** JSON-schema properties as declared keys (required unless listed otherwise). */
export function keysOf(properties = {}, required = []) {
  const req = new Set(Array.isArray(required) ? required : []);
  const out = {};
  for (const [name, schema] of Object.entries(properties || {})) {
    const { shape, keys } = shapeOf(schema);
    const decl = { shape };
    if (keys) decl.keys = keys;
    if (!req.has(name)) decl.optional = true;
    const d = describe(schema || {});
    if (d) decl.description = d;
    out[name] = decl;
  }
  return out;
}

/** The `args` key a tool's member subscription declares, from its spec. */
export function argsKey(spec) {
  const keys = keysOf(spec.parameters || {}, spec.required || []);
  return Object.keys(keys).length ? { shape: 'object', keys } : { shape: 'object' };
}

/** Split a shape string at top-level `|` (not inside `array<…>` or literals). */
function terms(shape) {
  const out = [];
  let depth = 0;
  let quote = null;
  let cur = '';
  for (const c of shape) {
    if (quote) {
      cur += c;
      if (c === quote) quote = null;
      continue;
    }
    if (c === '"' || c === "'") quote = c;
    else if (c === '<') depth++;
    else if (c === '>') depth--;
    else if (c === '|' && depth === 0) {
      out.push(cur.trim());
      cur = '';
      continue;
    }
    cur += c;
  }
  if (cur.trim()) out.push(cur.trim());
  return out;
}

function termSchema(term, keys) {
  if (term.startsWith('"') || term.startsWith("'")) return { type: 'string', enum: [term.slice(1, -1)] };
  if (term.startsWith('array<') && term.endsWith('>')) return { type: 'array', items: shapeSchema(term.slice(6, -1), keys) };
  switch (term) {
    case 'string': return { type: 'string' };
    case 'integer': return { type: 'integer' };
    case 'number': return { type: 'number' };
    case 'bool': return { type: 'boolean' };
    case 'null': return { type: 'null' };
    case 'object': return keys ? schemaOfKeys(keys) : { type: 'object' };
    default: return {};
  }
}

function shapeSchema(shape, keys) {
  const ts = terms(String(shape));
  if (ts.length > 1 && ts.every((t) => t.startsWith('"') || t.startsWith("'"))) return { type: 'string', enum: ts.map((t) => t.slice(1, -1)) };
  const schemas = ts.map((t) => termSchema(t, keys));
  if (schemas.length === 1) return schemas[0];
  // A union of plain types is a JSON-schema type list; anything richer is anyOf.
  if (schemas.every((s) => Object.keys(s).length === 1 && typeof s.type === 'string')) return { type: schemas.map((s) => s.type) };
  return { anyOf: schemas };
}

/** Declared keys as a JSON schema `{ type: 'object', properties, required }`. */
export function schemaOfKeys(keys = {}) {
  const properties = {};
  const required = [];
  for (const [name, decl] of Object.entries(keys || {})) {
    const s = shapeSchema(decl.shape, decl.keys);
    if (decl.description) s.description = decl.description;
    properties[name] = s;
    if (!decl.optional) required.push(name);
  }
  return required.length ? { type: 'object', properties, required } : { type: 'object', properties };
}

/**
 * The tool a resolved `tool-call/<name>` member offers the model: its name is
 * the member name, its argument schema the member's (narrowed) `args` keys,
 * its description and hints come from its subscribers' declarations.
 */
export function toolOfMember(emit) {
  const name = emit.event.slice('tool-call/'.length);
  const subs = emit.subscribers || [];
  const description = subs.map((s) => s.description).find(Boolean) || '';
  const hints = {};
  for (const s of subs) for (const [k, v] of Object.entries(s.hints || {})) if (!(k in hints)) hints[k] = v;
  const args = emit.keys?.args;
  const schema = args?.keys ? schemaOfKeys(args.keys) : { type: 'object', properties: {} };
  const spec = { name, description, parameters: schema.properties };
  if (schema.required?.length) spec.required = schema.required;
  for (const k of ['tier', 'group', 'effects', 'trust', 'intents']) if (hints[k] !== undefined) spec[k] = hints[k];
  return spec;
}

// ---------------------------------------------------------------------------
// Conforming a model's arguments to the member's shape.
// ---------------------------------------------------------------------------

const kind = (v) => (Array.isArray(v) ? 'array' : v === null ? 'null' : typeof v);

/** Does a value fit one shape term (coercing where unambiguous)? → { ok, value }. */
function fitTerm(term, v, keys, path, errors) {
  if (term.startsWith('"') || term.startsWith("'")) return { ok: v === term.slice(1, -1), value: v };
  if (term.startsWith('array<') && term.endsWith('>')) {
    const inner = term.slice(6, -1);
    // One item where a list is expected (a single glob, a single path).
    const items = Array.isArray(v) ? v : typeof v === 'string' ? [v] : null;
    if (!items) return { ok: false, value: v };
    const out = [];
    for (let i = 0; i < items.length; i++) {
      const r = fitShape(inner, items[i], keys, `${path}[${i}]`, errors);
      if (!r.ok) return { ok: false, value: v };
      out.push(r.value);
    }
    return { ok: true, value: out };
  }
  switch (term) {
    case 'string':
      if (typeof v === 'string') return { ok: true, value: v };
      if (typeof v === 'number' || typeof v === 'boolean') return { ok: true, value: String(v) };
      return { ok: false, value: v };
    case 'integer':
    case 'number': {
      const n = typeof v === 'string' && v.trim() !== '' ? Number(v) : v;
      if (typeof n !== 'number' || !Number.isFinite(n)) return { ok: false, value: v };
      if (term === 'integer' && !Number.isInteger(n)) return { ok: false, value: v };
      return { ok: true, value: n };
    }
    case 'bool':
      if (typeof v === 'boolean') return { ok: true, value: v };
      if (v === 'true' || v === 'false') return { ok: true, value: v === 'true' };
      return { ok: false, value: v };
    case 'null':
      return { ok: v === null, value: v };
    case 'object': {
      if (kind(v) !== 'object') return { ok: false, value: v };
      if (!keys) return { ok: true, value: v };
      const sub = [];
      const value = conform(keys, v, path, sub);
      if (sub.length) {
        errors.push(...sub);
        return { ok: false, value: v, reported: true };
      }
      return { ok: true, value };
    }
    default:
      return { ok: false, value: v };
  }
}

function fitShape(shape, v, keys, path, errors) {
  let reported = false;
  for (const t of terms(String(shape))) {
    const local = [];
    const r = fitTerm(t, v, keys, path, local);
    if (r.ok) return r;
    if (r.reported && !reported) {
      errors.push(...local);
      reported = true;
    }
  }
  return { ok: false, value: v, reported };
}

function conform(keys, obj, path, errors) {
  const out = { ...obj };
  for (const [k, decl] of Object.entries(keys || {})) {
    const p = path ? `${path}.${k}` : k;
    const v = obj[k];
    if (v === undefined || (v === null && !String(decl.shape).split('|').some((t) => t.trim() === 'null'))) {
      delete out[k];
      if (!decl.optional) errors.push(`missing required \`${p}\``);
      continue;
    }
    const r = fitShape(decl.shape, v, decl.keys, p, errors);
    if (r.ok) out[k] = r.value;
    else if (!r.reported) errors.push(`\`${p}\` must be ${decl.shape} (got ${kind(v)})`);
  }
  return out;
}

/**
 * Fit a model's arguments to a member's declared `args` keys: unambiguous
 * coercions (a number given as text, one string where a list is expected) are
 * applied, keys the shape does not mention pass through, and anything else is
 * reported. → { args, errors }.
 */
export function conformArgs(keys, args) {
  const errors = [];
  const out = conform(keys || {}, args && typeof args === 'object' && !Array.isArray(args) ? args : {}, '', errors);
  return { args: out, errors };
}
