// OpenAI Codex CLI: non-interactive exec mode.
import { has, runCli } from './_cli.mjs';

export const available = async () => has('codex');

export async function run({ workspace, prompt, model, timeoutMs }) {
  await runCli('codex', ['exec', '--full-auto', ...(model ? ['--model', model.replace(/^openai\//, '')] : []), prompt], { cwd: workspace, timeoutMs });
  return { usage: {} };
}
