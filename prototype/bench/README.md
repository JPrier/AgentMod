# AgentMod coding benchmark

Does AgentMod's orchestration earn its complexity? This harness runs the same
coding tasks, with the same model, under:

* **`coder`** — the full harness: structured file/search/patch tools, process
  management, checkpoints, plan, policy, lazy tool discovery, delegation,
  projection-side context management;
* **`minimal`** — a deliberately bare loop: the provider (configured with
  `minimal: true`: no harness preamble, every tool every turn, no compaction),
  the chat loop, and one `shell` tool (`plugins/minimal-shell`);
* optionally, **external harnesses** through `adapters/` (Claude Code, Codex,
  Aider…), none of which is a test dependency.

```shell
cargo build -p agentmod-runtime
export OPENROUTER_API_KEY=sk-or-...
node bench/run.mjs --model openai/gpt-4o-mini --runs 3              # coder vs minimal, all tasks
node bench/run.mjs --tasks dark-mode-web --definitions coder --mode default
node bench/run.mjs --external claude-code,aider                     # add external harnesses (if installed)
node bench/run.mjs --selftest                                       # plumbing check with the test-only mock
```

## What is measured

| Metric | Source |
|---|---|
| task success | hidden check run after the agent finishes (the agent never sees it) |
| regressions | the repository's pre-existing suite passed before and fails after |
| model calls, tool calls, edits, checkpoints | the session logs (`agentmod metrics`) |
| input / output / cached tokens, cost | provider usage recorded in `model-response` events |
| context size per turn (max), static tool-schema tokens | the projection's metrics on each request |
| time to first useful edit, wall time | log timestamps |
| permission prompts, questions | approvals / `ask_user` requests (auto-answered and counted in unattended runs) |
| child-agent usage | sessions whose parent is the task's session |
| recovery events | invocations with more than one attempt |

Children's sessions are included in token, call, and cost totals. Results go
to `bench/results/` as JSON plus a Markdown table.

## Tasks

| Task | What it exercises |
|---|---|
| `fix-bug-js` | find and fix a bug from a failing test |
| `add-feature-py` | implement a function and add tests |
| `dark-mode-web` | the specification's own example: a feature across HTML/CSS/JS, keep tests passing |
| `rename-refactor-js` | a cross-file rename without behavior change |
| `debug-parser-py` | diagnose a failing test and fix the code, not the test |

Each task is `tasks/<name>/{task.json, repo/}`: the prompt, the hidden
`check`, the `regressions` suite, hidden files written only at grading time,
and a timeout. Add tasks the same way.

## Reading the results honestly

The interesting comparisons are *paired* (same task, same model, same run
index): success and regressions first, then cost and time for the runs that
succeeded. A single run per cell is noise; use `--runs 3` or more. The full
harness pays a fixed static cost (its core tool schemas, measured in the
`schema tok` column) on every request; it should buy that back in fewer
calls, fewer wasted tokens, and more successes. If it does not for a model or
task class, that is a finding, not a failure of the benchmark.

No real-model results are recorded in the repository yet (this change was
built without provider credentials); record runs under `docs/benchmarks/`.
The selftest only proves the plumbing: its "model" is a script.
