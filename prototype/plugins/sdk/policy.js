// Layered tool policy: a pure engine (no I/O), used by the `policy` plugin.
//
// A decision is made from *facts about one call* (tool, its declared group and
// effects, the shell command's classification, paths, requested secrets) and
// *rules from scopes*, highest authority first:
//
//   runtime     the deployment's plugin config (`rules`, `mode`)
//   user        the operator's user policy (`user_rules`)
//   workspace   the repository's .agentmod/policy.json — may only restrict
//               (its `allow` rules are ignored: repository content is untrusted)
//   session     per-session config layer (`session_rules`, `session_mode`)
//   child       a delegation's tool allowlist and mode (only ever narrower)
//   grant       "allow for this session" approvals recorded in the log
//   invocation  a one-time approval of this exact action (by digest)
//
// Precedence is deny > ask > allow:
//   * any matching deny, from any scope, denies — nothing lower can undo it;
//     `read-only` mode denies every change the same way;
//   * otherwise any matching ask rule asks, unless a human grant or a one-time
//     approval satisfies it (rules marked `always_ask` cannot be granted);
//   * otherwise a matching allow rule allows;
//   * otherwise the permission mode's default decides (its asks can be
//     satisfied by grants and approvals like any other).
// An `allow` rule never silences an `ask` rule. Every decision carries the
// rule that won, its scope, and the rules it overrode.

import { classifyCommand } from './coding/shellclass.js';
import { globToRegExp } from './coding/glob.js';
import { sha256 } from './coding/text.js';

export const SCOPES = ['runtime', 'user', 'workspace', 'session', 'child', 'grant', 'invocation', 'mode'];
const RANK = Object.fromEntries(SCOPES.map((s, i) => [s, i]));
export const MODES = ['auto', 'default', 'ask', 'read-only'];

/** Canonical JSON (sorted keys) for digests. */
export function canonical(v) {
  if (Array.isArray(v)) return `[${v.map(canonical).join(',')}]`;
  if (v && typeof v === 'object') return `{${Object.keys(v).sort().map((k) => `${JSON.stringify(k)}:${canonical(v[k])}`).join(',')}}`;
  return JSON.stringify(v ?? null);
}

/** Digest an approval binds to: the exact tool and arguments. */
export async function actionDigest(name, args) {
  return (await sha256(canonical({ name, args: args ?? {} }))).slice(0, 32);
}

/** Facts about a call, from its arguments and the tool's declared spec. */
export function factsOf(name, args = {}, spec = {}) {
  const f = { tool: name, group: spec.group || null, effects: spec.effects || 'varies', readOnly: spec.effects === 'read' || spec.effects === 'network-read', network: !!spec.network || spec.effects === 'network-read', destructive: false, publish: false, external: spec.effects === 'external', secrets: [], paths: [], command: null, reasons: [] };
  if (name === 'shell' || (name === 'process' && args.action === 'start')) {
    const c = classifyCommand(String(args.command || ''));
    Object.assign(f, { command: String(args.command || ''), readOnly: c.readOnly, network: c.network, destructive: c.destructive, publish: c.publish, reasons: c.reasons, effects: c.readOnly ? 'read' : 'write' });
  } else if (name === 'process') {
    f.readOnly = args.action !== 'write' && args.action !== 'kill';
    f.effects = f.readOnly ? 'read' : 'write';
  }
  if (Array.isArray(args.secrets) && args.secrets.length) f.secrets = args.secrets.map(String);
  if (typeof args.path === 'string') f.paths.push(args.path);
  if (typeof args.url === 'string') {
    f.url = args.url;
    try { f.domain = new URL(args.url).hostname.toLowerCase(); } catch { f.domain = null; }
  }
  if (Array.isArray(args.changes)) for (const c of args.changes) { if (c?.path) f.paths.push(String(c.path)); if (c?.to) f.paths.push(String(c.to)); }
  if (typeof args.patch === 'string') for (const m of args.patch.matchAll(/^\*\*\* (?:Add|Update|Delete) File: (.+)$|^\*\*\* Move to: (.+)$/gm)) f.paths.push((m[1] || m[2]).trim());
  if (name === 'checkpoints' && args.action === 'restore') { f.effects = 'write'; f.readOnly = false; }
  if (name === 'checkpoints' && args.action !== 'restore') { f.effects = 'read'; f.readOnly = true; }
  f.paths = f.paths.map((p) => p.replace(/^\.\//, '').replace(/^\/+/, ''));
  return f;
}

const toolMatches = (pattern, name) => {
  if (pattern == null || pattern === '*') return true;
  const list = Array.isArray(pattern) ? pattern : [pattern];
  return list.some((p) => (p.includes('*') ? new RegExp(`^${p.replace(/[.+^$()|\\]/g, '\\$&').replace(/\*/g, '.*')}$`).test(name) : p === name));
};

/** Does a rule's `when` hold for these facts? */
export function whenMatches(when, f) {
  if (!when) return true;
  for (const [k, v] of Object.entries(when)) {
    if (k === 'command') { if (!f.command || !new RegExp(v).test(f.command)) return false; continue; }
    if (k === 'path') { const globs = Array.isArray(v) ? v : [v]; if (!f.paths.some((p) => globs.some((g) => globToRegExp(g).test(p)))) return false; continue; }
    if (k === 'secrets') { if (Boolean(f.secrets.length) !== Boolean(v)) return false; continue; }
    if (k === 'domain') {
      const doms = [].concat(v).map((d) => String(d).toLowerCase());
      if (!f.domain || !doms.some((d) => (d.startsWith('*.') ? f.domain === d.slice(2) || f.domain.endsWith(d.slice(1)) : f.domain === d))) return false;
      continue;
    }
    if (k === 'group') { if (![].concat(v).includes(f.group)) return false; continue; }
    if (k === 'effects') { if (![].concat(v).includes(f.effects)) return false; continue; }
    if (k in f) { if (Boolean(f[k]) !== Boolean(v)) return false; continue; }
    return false; // unknown condition: never matches (fail closed for allow; deny rules should not rely on it)
  }
  return true;
}

/** Built-in rules every deployment starts from (runtime scope; overridable by config). */
export const DEFAULT_RULES = [
  { id: 'protect-vcs-internals', tool: ['apply_patch'], when: { path: ['.git/**', '**/.git/**', '.agentmod/**'] }, effect: 'deny', reason: 'version-control internals and the toolkit state directory are not edited directly' },
  { id: 'secret-files-read', tool: ['read_file'], when: { path: ['**/.env', '**/.env.*', '**/*.pem', '**/*.key', '**/id_rsa*', '**/id_ed25519*', '**/.npmrc', '**/.pypirc', '**/.netrc'] }, effect: 'ask', reason: 'the file may contain credentials' },
  { id: 'secrets-in-env', tool: '*', when: { secrets: true }, effect: 'ask', reason: 'the command asks for a secret' },
  { id: 'destructive-shell', tool: ['shell', 'process'], when: { destructive: true }, effect: 'ask', reason: 'destructive command' },
  { id: 'publish', tool: ['shell', 'process'], when: { publish: true }, effect: 'ask', reason: 'publishes outside this machine (push, release, publish)' },
  { id: 'local-browser', tool: ['browser_navigate'], when: { domain: ['localhost', '127.0.0.1', '0.0.0.0', '[::1]'] }, effect: 'allow', reason: 'validating a local dev server' },
];

/** Default effect of a permission mode for calls no rule decides. */
export function modeDefault(mode, f) {
  switch (mode) {
    case 'auto': return { effect: 'allow', reason: 'auto mode allows everything not denied or asked by a rule' };
    case 'read-only': return f.readOnly ? { effect: 'allow', reason: 'read-only mode: reading is allowed' } : { effect: 'deny', reason: 'read-only mode: no changes' };
    case 'ask': return f.readOnly ? { effect: 'allow', reason: 'ask mode: reading is allowed' } : { effect: 'ask', reason: 'ask mode: every change needs approval' };
    default:
      if (f.network) return { effect: 'ask', reason: 'default mode: network access needs approval' };
      if (f.external) return { effect: 'ask', reason: 'default mode: actions outside the workspace need approval' };
      return { effect: 'allow', reason: 'default mode: workspace reads and edits are allowed' };
  }
}

/** The stricter of two modes. */
export function stricterMode(a, b) {
  const order = { auto: 0, default: 1, ask: 2, 'read-only': 3 };
  const x = MODES.includes(a) ? a : 'default';
  const y = MODES.includes(b) ? b : x;
  return order[y] > order[x] ? y : x;
}

/**
 * Decide one call.
 *
 * @param {object} f   facts (factsOf)
 * @param {object} p
 * @param {Array<{scope: string, rules: object[]}>} p.layers   rule layers in any order
 * @param {string} p.mode                    effective permission mode
 * @param {string[]|null} [p.allowlist]      child allowlist (null: no restriction)
 * @param {object[]} [p.grants]              session grants [{ tool, command_prefix?, granted_by }]
 * @param {boolean} [p.approved]             a valid one-time approval of this exact action exists
 * @returns {{ effect: 'allow'|'ask'|'deny', winner: object, matched: object[], overridden: object[], grantable: boolean }}
 */
export function decide(f, { layers = [], mode = 'default', allowlist = null, grants = [], approved = false }) {
  const matched = [];
  for (const { scope, rules } of layers) {
    for (const r of rules || []) {
      if (!r || !['allow', 'ask', 'deny'].includes(r.effect)) continue;
      if (scope === 'workspace' && r.effect === 'allow') continue; // repositories cannot grant
      if (!toolMatches(r.tool, f.tool) || !whenMatches(r.when, f)) continue;
      matched.push({ id: r.id || null, scope, effect: r.effect, reason: r.reason || null, always_ask: !!r.always_ask });
    }
  }
  if (allowlist && !allowlist.includes(f.tool)) matched.push({ id: 'child-allowlist', scope: 'child', effect: 'deny', reason: 'not in the delegation\'s tool allowlist' });
  const def = modeDefault(mode, f);
  matched.sort((a, b) => RANK[a.scope] - RANK[b.scope]);
  const denies = matched.filter((m) => m.effect === 'deny');
  if (denies.length) return { effect: 'deny', winner: denies[0], matched, overridden: matched.filter((m) => m !== denies[0]), grantable: false };
  if (mode === 'read-only' && def.effect === 'deny') return { effect: 'deny', winner: { id: `mode:${mode}`, scope: 'mode', effect: 'deny', reason: def.reason }, matched, overridden: matched, grantable: false };
  const allowRule = matched.find((m) => m.effect === 'allow');
  let asks = matched.filter((m) => m.effect === 'ask');
  if (!asks.length && !allowRule) {
    if (def.effect === 'deny') return { effect: 'deny', winner: { id: `mode:${mode}`, scope: 'mode', effect: 'deny', reason: def.reason }, matched, overridden: matched, grantable: false };
    if (def.effect === 'ask') asks = [{ id: `mode:${mode}`, scope: 'mode', effect: 'ask', reason: def.reason }];
  }
  if (asks.length) {
    const hard = asks.find((a) => a.always_ask);
    const grant = grants.find((g) => grantCovers(g, f));
    if (approved) return { effect: 'allow', winner: { id: 'approval', scope: 'invocation', effect: 'allow', reason: 'approved by the user for this exact action' }, matched, overridden: asks, grantable: !hard };
    if (grant && !hard) return { effect: 'allow', winner: { id: 'grant', scope: 'grant', effect: 'allow', reason: `allowed for this session by ${grant.granted_by || 'the user'}` }, matched, overridden: asks, grantable: true };
    return { effect: 'ask', winner: asks[0], matched, overridden: matched.filter((m) => m !== asks[0]), grantable: !hard };
  }
  if (allowRule) return { effect: 'allow', winner: allowRule, matched, overridden: matched.filter((m) => m !== allowRule), grantable: true };
  return { effect: def.effect, winner: { id: `mode:${mode}`, scope: 'mode', effect: def.effect, reason: def.reason }, matched, overridden: matched, grantable: true };
}

/** A session grant derived from an approval ("allow this kind of call for the session"). */
export function grantFor(f, by) {
  const g = { tool: f.tool, granted_by: by };
  if (f.command) g.command_prefix = f.command.trim().split(/\s+/).slice(0, 2).join(' ');
  if (f.secrets.length) g.secrets = [...f.secrets].sort();
  return g;
}

export function grantCovers(g, f) {
  if (g.tool !== f.tool) return false;
  if (g.command_prefix && !(f.command || '').trim().startsWith(g.command_prefix)) return false;
  if (f.secrets.length && !(g.secrets || []).length) return false;
  if (f.secrets.length && f.secrets.some((s) => !g.secrets.includes(s))) return false;
  return true;
}

/** Tools that are denied whatever their arguments: hidden from the model. */
export function hiddenTools(specs, policy) {
  const out = [];
  for (const spec of specs) {
    // Probe with argument-free facts; only unconditional denials hide a tool.
    const f = factsOf(spec.name, {}, spec);
    if (spec.name === 'shell' || spec.name === 'process') f.readOnly = true; // a read-only shell is still usable
    const d = decide(f, { ...policy, approved: false });
    const unconditional = d.effect === 'deny' && (d.winner.scope === 'child' || d.winner.scope === 'mode' || !(policy.layers || []).some((l) => (l.rules || []).some((r) => r.id === d.winner.id && r.when)));
    if (unconditional) out.push(spec.name);
  }
  return out.sort();
}

/** One-line explanation for logs and model-facing denials. */
export function explain(d) {
  const w = d.winner;
  const others = d.overridden.filter((o) => o.effect !== w.effect).map((o) => `${o.effect}${o.id ? ` ${o.id}` : ''} (${o.scope})`);
  return `${d.effect}: ${w.reason || w.id} [rule ${w.id || '—'}, scope ${w.scope}]${others.length ? `; overrode ${others.join(', ')}` : ''}`;
}
