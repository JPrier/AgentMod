// Delegation helpers shared by `subagent` and `subagent-reporter` (pure).
import { toolSpec } from './agentmod.js';

export const CHILD_DEFAULT_TOOLS = ['shell', 'process', 'read_file', 'list_dir', 'search_files', 'search_text', 'apply_patch', 'update_plan', 'view_image', 'repo_map'];
export const READ_ONLY_TOOLS = ['shell', 'read_file', 'list_dir', 'search_files', 'search_text', 'update_plan', 'view_image', 'repo_map'];

export const DELEGATE = toolSpec('delegate', 'Hand a bounded task to a child agent that runs in its own session with a fresh context, and wait for its result. Use it for self-contained work (investigate X, implement and test Y in isolation, review Z) to keep your own context small; several delegate calls in one step run in parallel. The child sees only what you put in task and context. By default it works in an isolated copy of your workspace; adopt its changes afterwards with adopt_changes.', {
  task: { type: 'string', description: 'what the child must do and what to report back; self-contained' },
  context: { type: 'string', description: 'facts, paths, constraints the child needs (it cannot see your conversation)' },
  workspace: { type: 'string', enum: ['isolated', 'shared', 'none'], description: 'isolated (default): its own copy of your workspace; shared: your workspace directly; none: no files' },
  tools: { type: 'array', items: { type: 'string' }, description: `tools the child may use (default: ${CHILD_DEFAULT_TOOLS.join(', ')})` },
  read_only: { type: 'boolean', description: 'forbid the child any changes' },
  max_steps: { type: 'integer', description: 'maximum model requests for the child (default 30)' },
}, { required: ['task'], tier: 'deferred', group: 'delegation', effects: 'varies' });

export const SUBAGENTS = toolSpec('subagents', 'List your child agents and their state, or cancel one.', {
  action: { type: 'string', enum: ['list', 'cancel'], description: 'list | cancel' },
  session: { type: 'string', description: 'cancel: the child session id' },
}, { required: ['action'], tier: 'deferred', group: 'delegation', effects: 'varies' });

/** Evidence a finished child reports, derived from its own session log. */
export function evidenceOf(view) {
  const ev = view.events || [];
  const results = new Map(ev.filter((e) => e.event_name === 'tool-result').map((e) => [e.payload.call_id, e.payload]));
  const commands = ev.filter((e) => e.event_name === 'tool-call' && e.payload.name === 'shell').map((e) => {
    const r = results.get(e.payload.call_id);
    return { command: String(e.payload.args?.command || '').slice(0, 200), exit_code: r?.exit_code ?? null, error: !!r?.error };
  });
  const files = new Set();
  for (const e of ev) {
    if (e.event_name === 'workspace-change') for (const f of e.payload.files || []) files.add(f.path);
  }
  const testLike = /\b(test|pytest|jest|vitest|mocha|cargo test|go test|npm (run )?test|make check|ctest|tox)\b/;
  const tests = commands.filter((c) => testLike.test(c.command));
  const diagnostics = ev.filter((e) => e.event_name === 'diagnostics').reduce((n, e) => n + (e.payload.diagnostics?.length || 0), 0);
  const workspace = ev.find((e) => e.event_name === 'workspace-info')?.payload;
  const checkpoints = ev.filter((e) => e.event_name === 'checkpoint-created').map((e) => e.payload.checkpoint);
  const usage = ev.filter((e) => e.event_name === 'model-response').reduce((u, e) => ({
    model_requests: u.model_requests + 1,
    input_tokens: u.input_tokens + (e.payload.metrics?.input_tokens || 0),
    output_tokens: u.output_tokens + (e.payload.metrics?.output_tokens || 0),
    cost: u.cost + (e.payload.metrics?.cost || 0),
  }), { model_requests: 0, input_tokens: 0, output_tokens: 0, cost: 0 });
  return {
    commands: commands.slice(-20),
    commands_total: commands.length,
    commands_failed: commands.filter((c) => c.error).length,
    tests: tests.slice(-5),
    files_changed: [...files].sort(),
    diagnostics,
    workspace: workspace ? { root: workspace.root, mode: workspace.mode } : null,
    last_checkpoint: checkpoints.pop() || null,
    budget_exhausted: ev.some((e) => e.event_name === 'budget-exhausted'),
    usage,
  };
}

/** The concise, bounded text the parent model sees. */
export function childReport(child, text, ev) {
  const lines = [`Child ${child} finished.`, '--- its conclusion ---', String(text || '(no text)').slice(0, 4000)];
  const facts = [];
  if (ev) {
    facts.push(`commands run: ${ev.commands_total} (${ev.commands_failed} failed)`);
    for (const t of ev.tests) facts.push(`test: \`${t.command}\` → exit ${t.exit_code}`);
    if (ev.files_changed.length) facts.push(`files changed: ${ev.files_changed.slice(0, 30).join(', ')}${ev.files_changed.length > 30 ? ', …' : ''}`);
    if (ev.diagnostics) facts.push(`diagnostics reported: ${ev.diagnostics}`);
    if (ev.workspace?.mode === 'isolated' && ev.files_changed.length) facts.push(`its changes are in an isolated workspace; bring them in with adopt_changes(session: "${child}") after reviewing`);
    if (ev.budget_exhausted) facts.push('it stopped because its budget ran out');
  }
  if (facts.length) lines.push('--- evidence (from its session log) ---', ...facts.map((f) => `- ${f}`));
  return lines.join('\n');
}
