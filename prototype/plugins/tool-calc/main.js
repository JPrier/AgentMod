// tool-calc: a safe arithmetic evaluator (no eval). Supports + - * / % ^ and parentheses.
import { definePlugin, declareTools, offerTools, ownTools, toolSpec } from '../sdk/agentmod.js';

const TOOLS = [toolSpec('calc', 'Evaluate an arithmetic expression.', { expression: { type: 'string' } })];

export function evaluate(src) {
  const toks = src.match(/\d+(?:\.\d+)?|[-+*/%^()]/g) || [];
  if (toks.join('') !== src.replace(/\s+/g, '')) throw new Error('unsupported characters in expression');
  let i = 0;
  const peek = () => toks[i];
  const next = () => toks[i++];
  function primary() {
    const t = next();
    if (t === '(') { const v = expr(); if (next() !== ')') throw new Error('missing )'); return v; }
    if (t === '-') return -primary();
    if (t === '+') return primary();
    if (t !== undefined && /^\d/.test(t)) return Number(t);
    throw new Error(`unexpected ${t ?? 'end of input'}`);
  }
  function power() { const b = primary(); if (peek() === '^') { next(); return b ** power(); } return b; }
  function term() {
    let v = power();
    while (['*', '/', '%'].includes(peek())) {
      const op = next(); const r = power();
      v = op === '*' ? v * r : op === '/' ? v / r : v % r;
    }
    return v;
  }
  function expr() {
    let v = term();
    while (['+', '-'].includes(peek())) { const op = next(); const r = term(); v = op === '+' ? v + r : v - r; }
    return v;
  }
  const v = expr();
  if (i !== toks.length) throw new Error(`unexpected ${toks[i]}`);
  if (!Number.isFinite(v)) throw new Error('result is not finite');
  return Math.round(v * 1e12) / 1e12;
}

definePlugin({
  manifest: {
    name: 'tool-calc',
    version: '0.1.0',
    description: 'Tool: arithmetic.',
    consumes: [
      { event: 'session-started' },
      { event: 'config-applied' },
      ownTools(['calc']),
    ],
    tools: declareTools(TOOLS),
    emits: [{ event: 'tool-result', supplies: ['call_id', 'name', 'output'] }],
  },
  handlers: {
    'session-started': (ctx) => offerTools(ctx, TOOLS),
    'config-applied': (ctx) => offerTools(ctx, TOOLS),
    'tool-call': async (ctx) => {
      if (ctx.payload.name !== 'calc') return;
      const { call_id } = ctx.payload;
      let output; let error = false;
      try { output = String(evaluate(String(ctx.payload.args?.expression ?? ''))); } catch (e) { output = e.message; error = true; }
      await ctx.publish('tool-result', { call_id, name: 'calc', output, error }, { ui: { v: 1, kind: 'tool', name: 'calc', call_id, status: error ? 'error' : 'done', args: ctx.payload.args, result: output } });
    },
  },
});
