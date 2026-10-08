// A high-level view of a coding session, derived from its log alone (the
// session → events projection). Frontends render it; nothing here talks to a
// plugin, and the raw events stay available underneath.

const last = (list) => list[list.length - 1];

/**
 * @param {object} view   the `session` query result (events with payloads)
 * @param {object[]} [sessions]  the sessions list (for children's state)
 */
export function harnessSummary(view, sessions = []) {
  const ev = view?.events || [];
  const of = (name) => ev.filter((e) => e.event_name === name);
  const responses = of('model-response').map((e) => e.payload);
  const usage = responses.reduce((u, r) => {
    const m = r.metrics || {};
    u.model_requests += 1;
    u.input_tokens += m.input_tokens || 0;
    u.output_tokens += m.output_tokens || 0;
    u.cached_tokens += m.cached_tokens || 0;
    u.cost += m.cost || 0;
    u.retries += m.retries || 0;
    u.latency_ms += m.latency_ms || 0;
    u.elided += m.elided_tool_outputs || 0;
    u.dropped += m.dropped_messages || 0;
    return u;
  }, { model_requests: 0, input_tokens: 0, output_tokens: 0, cached_tokens: 0, cost: 0, retries: 0, latency_ms: 0, elided: 0, dropped: 0 });
  const lastMetrics = last(responses)?.metrics || null;
  const calls = of('tool-call').filter((e) => !e.payload.approved);
  const results = new Map(of('tool-result').map((e) => [e.payload.call_id, e.payload]));
  const processes = new Map();
  for (const e of of('process-started')) processes.set(e.payload.process_id, { id: e.payload.process_id, name: e.payload.name, command: e.payload.command, state: 'running', event_id: e.event_id });
  for (const e of of('process-exited')) {
    const p = processes.get(e.payload.process_id);
    if (p) Object.assign(p, { state: e.payload.state, exit_code: e.payload.exit_code });
  }
  const children = of('subagent-started').map((e) => {
    const s = sessions.find((x) => x.session_id === e.payload.child);
    const r = results.get(e.payload.call_id);
    return { session_id: e.payload.child, workspace: e.payload.workspace, activity: s?.activity || 'unknown', finished: !!r, error: !!r?.error, title: s?.title };
  });
  return {
    model: last(responses) ? { model: last(responses).model, provider: last(responses).provider } : null,
    usage,
    context: lastMetrics ? { tokens: lastMetrics.context_tokens, tool_schema_tokens: lastMetrics.tool_schema_tokens, tools_sent: lastMetrics.tools_sent, tools_deferred: lastMetrics.tools_deferred } : null,
    workspace: last(of('workspace-info'))?.payload || null,
    plan: last(of('plan-updated'))?.payload.items || null,
    tool_calls: calls.length,
    tool_errors: calls.filter((c) => results.get(c.payload.call_id)?.error).length,
    pending_calls: calls.filter((c) => !results.has(c.payload.call_id)).map((c) => c.payload.name),
    approvals: of('approval-requested').map((e) => ({ event_id: e.event_id, tool: e.payload.name, reason: e.payload.reason, answered: ev.some((x) => x.event_name === 'ui-action' && x.payload.reply_to === e.event_id) })),
    decisions: of('policy-decision').slice(-10).map((e) => e.payload),
    checkpoints: of('checkpoint-created').map((e) => ({ event_id: e.event_id, sequence: e.sequence, checkpoint: e.payload.checkpoint, reason: e.payload.reason })),
    restores: of('workspace-restored').map((e) => e.payload),
    processes: [...processes.values()],
    children,
    diagnostics: last(of('diagnostics'))?.payload.diagnostics || [],
    files_changed: [...new Set(of('workspace-change').flatMap((e) => (e.payload.files || []).map((f) => f.path)))].sort(),
    recovery: (view?.events || []).flatMap((e) => e.invocations || []).filter((i) => i.attempts > 1).length,
    budget_exhausted: of('budget-exhausted').length > 0,
  };
}
