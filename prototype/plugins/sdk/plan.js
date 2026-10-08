// Plan validation shared by the `plan` plugin (pure; importable by tests).
import { toolSpec } from './agentmod.js';

export const STATUSES = ['pending', 'in_progress', 'completed', 'blocked'];

export const UPDATE_PLAN = toolSpec('update_plan', 'Record or revise your plan for multi-step work: the full list of steps, each pending, in_progress, completed, or blocked. Keep one step in_progress at a time; send the whole list each time.', {
  items: { type: 'array', description: 'the plan: [{ text, status }] in order', items: { type: 'object', properties: { text: { type: 'string' }, status: { type: 'string', enum: STATUSES } }, required: ['text', 'status'] } },
  note: { type: 'string', description: 'optional one-line note on what changed' },
}, { required: ['items'], tier: 'core', group: 'planning', effects: 'read' });

export function validatePlan(items) {
  if (!Array.isArray(items) || !items.length) throw new Error('`items` must be a non-empty list of { text, status }');
  if (items.length > 50) throw new Error('keep the plan to at most 50 steps');
  return items.map((it, i) => {
    const text = String(it?.text ?? it?.step ?? '').trim();
    const status = String(it?.status ?? 'pending');
    if (!text) throw new Error(`step ${i + 1} needs text`);
    if (!STATUSES.includes(status)) throw new Error(`step ${i + 1}: status must be one of ${STATUSES.join(', ')}`);
    return { text: text.slice(0, 300), status };
  });
}

