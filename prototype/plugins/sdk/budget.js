// Budget arithmetic shared by the `budget` plugin (pure; importable by tests).

const KEYS = ['max_model_requests', 'max_tool_calls', 'max_output_tokens', 'max_wall_seconds'];

export function effectiveBudget(config = {}, delegation = {}) {
  const out = {};
  for (const k of KEYS) {
    const vals = [config[k], delegation?.[k]].map(Number).filter((n) => Number.isFinite(n) && n > 0);
    if (vals.length) out[k] = Math.min(...vals);
  }
  return out;
}

export function usage(events) {
  const first = events[0];
  const last = events[events.length - 1];
  return {
    model_requests: events.filter((e) => e.event_name === 'model-response').length,
    tool_calls: events.filter((e) => e.event_name === 'tool-call' && !e.payload.approved).length,
    output_tokens: events.filter((e) => e.event_name === 'model-response').reduce((n, e) => n + (e.payload.metrics?.output_tokens || e.payload.usage?.completion_tokens || 0), 0),
    wall_seconds: first && last ? Math.round(((last.at ?? 0) - (first.at ?? 0)) / 1000) : 0,
  };
}

export function exceeded(budget, u) {
  if (budget.max_model_requests && u.model_requests >= budget.max_model_requests) return `reached max_model_requests=${budget.max_model_requests}`;
  if (budget.max_tool_calls && u.tool_calls >= budget.max_tool_calls) return `reached max_tool_calls=${budget.max_tool_calls}`;
  if (budget.max_output_tokens && u.output_tokens >= budget.max_output_tokens) return `reached max_output_tokens=${budget.max_output_tokens}`;
  if (budget.max_wall_seconds && u.wall_seconds >= budget.max_wall_seconds) return `reached max_wall_seconds=${budget.max_wall_seconds}`;
  return null;
}

