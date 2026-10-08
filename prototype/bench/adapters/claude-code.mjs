// Claude Code (https://docs.claude.com/en/docs/claude-code): non-interactive print mode.
import { has, runCli } from './_cli.mjs';

export const available = async () => has('claude');

export async function run({ workspace, prompt, timeoutMs }) {
  const r = await runCli('claude', ['-p', prompt, '--output-format', 'json', '--permission-mode', 'acceptEdits', '--allowedTools', 'Bash Edit Write Read Glob Grep'], { cwd: workspace, timeoutMs });
  try {
    const j = JSON.parse(r.out);
    return { usage: { input_tokens: j.usage?.input_tokens, output_tokens: j.usage?.output_tokens, cached_tokens: j.usage?.cache_read_input_tokens, cost: j.total_cost_usd, model_requests: j.num_turns } };
  } catch {
    return { usage: {} };
  }
}
