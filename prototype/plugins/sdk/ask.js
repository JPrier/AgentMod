// The ask_user tool spec (shared; importable by tests).
import { toolSpec } from './agentmod.js';

export const ASK_USER = toolSpec('ask_user', 'Ask the user a question and wait for the answer. Use only when you need a decision or information only the user has (not for permission to run tools — that is handled separately). Offer options when there are a few clear choices.', {
  question: { type: 'string', description: 'the question, self-contained' },
  options: { type: 'array', items: { type: 'string' }, description: 'suggested answers (the user may still answer freely)' },
}, { required: ['question'], tier: 'core', group: 'interaction', effects: 'read' });

