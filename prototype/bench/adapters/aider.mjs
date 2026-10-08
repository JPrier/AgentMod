// Aider: one message, auto-confirm, through OpenRouter when the model is an OpenRouter slug.
import { has, runCli } from './_cli.mjs';

export const available = async () => has('aider');

export async function run({ workspace, prompt, model, timeoutMs }) {
  await runCli('aider', ['--yes-always', '--no-auto-commits', '--message', prompt, ...(model ? ['--model', `openrouter/${model}`] : [])], { cwd: workspace, timeoutMs });
  return { usage: {} };
}
