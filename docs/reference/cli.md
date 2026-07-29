# CLI Reference

## Implemented commands

```text
agentmod doctor [--json] [--strict]
agentmod run "<prompt>" --session <id> [--provider <id>] [--model <id>]
             [--option <key=value>]... [--cancellation-id <uuid>]
             [--json | --stream-json]
agentmod cancel <cancellation-id> [--reason <text>] [--json]
agentmod harness list [--json]
agentmod harness inspect <id> [--json]
agentmod style list [--json]
agentmod style inspect <id-or-id@version> [--json]
agentmod style validate <file> [--json]
agentmod style compile <file> [--json]
agentmod session create [--workspace <path>] [--style <id>] [--harness <id>]
                        [--memory <id>] [--compaction <id>]
                        [--max-iterations <count>] [--max-steps <count>]
                        [--max-tokens <count>] [--max-cost-micros <count>]
                        [--max-duration-ms <count>] [--json]
agentmod session list [--limit <count>] [--json]
agentmod session inspect <id> [--at <sequence>] [--json]
agentmod session replay <id> [--at <sequence>] [--json]
agentmod session branch <id> --at <sequence> [--style <id>] [--json]
agentmod session events <id> [--after <sequence>] [--limit <count>] [--json]
agentmod approval resolve <session-id> <continuation-id> <approve|deny> [--json]
agentmod schedule add <schedule-id> --session <id> --prompt <text>
             (--at-ms <unix-ms> [--every-ms <ms>] |
              --on-event <event-type> |
              --process-id <id> --contains <literal>)
             [--idempotency-id <id>]
             [--style <id>] [--workspace <path>]
             [--permission-policy <id>] [--provider <id>] [--model <id>]
             [--token-budget <count>] [--cost-budget-micros <count>] [--json]
agentmod schedule list [--limit <count>] [--json]
agentmod schedule remove <schedule-id> [--json]
agentmod schedule claim [--limit <count>] [--json]
agentmod schedule complete <execution-id> [--failed] [--json]
agentmod schedule run [--limit <count>] [--json]
agentmod-tui [--smoke | --smoke-turn <prompt> | --smoke-command "<slash-command>"]
agentmod-acp
```

`--json` emits a stable JSON object. `--strict` asks logic to treat degraded
runtime status as unsuccessful. Session creation defaults to the current
workspace, `persistent-chat`, and the style-selected `native` harness. Use
`harness list` and `harness inspect` to view adapter versions, availability,
capabilities, and the exact capability-set hash. An explicit `--harness`
override is accepted only when it satisfies the selected style.
`--memory` and `--compaction` apply SDK-owned component transforms and compile a
new immutable per-session binding. Omitting them retains the style defaults;
invalid or unavailable selections fail with style diagnostics.
The five optional budget flags also compile a new binding. The SDK narrows
subordinate inline-graph, compaction, retry, and child-agent bounds to the
selected hard ceilings before ordinary validation. Zero, over-policy, or
otherwise incompatible limits fail before session creation.

The TUI Styles view lists runtime-advertised memory and compaction components.
Use `/memory <id|style-default>` and
`/compaction <id|style-default>` before `/new`, or pass both after the harness
in `/new [workspace] [style] [harness] [memory] [compaction]`. Use
`/budget <style-default|iterations steps tokens cost-micros duration-ms>` before
`/new`, or append those five values to `/new`.

The TUI command palette supports `/branch <sequence> [style]`. Omitting the
style preserves the parent binding; providing one resolves and validates that
style through the runtime registry, atomically creates the child, and selects
it without modifying the parent.

`run` executes one durable turn in an existing session. Provider options are
repeatable and accept JSON scalars/objects or plain strings. The bundled offline
provider can be exercised with:

```sh
agentmod run "hello" --session <id> \
  --provider deterministic-mock --model mock-model \
  --option 'mock_scenario="streaming_text"' \
  --cancellation-id <uuid> \
  --option 'mock_text="done"' --json
```

The batch response contains ordered provider lifecycle events plus the first
and last canonical sequence committed by the turn. `--stream-json` instead
flushes newline-delimited `run_event` objects as soon as each event is
canonically committed, followed by exactly one `run_complete` object:

```sh
agentmod run "hello" --session <id> \
  --option 'mock_scenario="streaming_text"' --stream-json
```

Every event includes its canonical `committed_sequence`. The terminal object
includes the complete committed sequence range and any durable continuation.
`--json` and `--stream-json` are mutually exclusive.

An active request with a caller-selected cancellation ID can be stopped from a
second process:

```sh
agentmod cancel <uuid> --reason "cancelled by user" --json
```

Cancellation preserves visible deltas already received by runtime, commits a
typed cancellation event, and does not commit a model completion.

Sensitive tools return `awaiting_continuation` under the default interactive
policy. Resolve that durable request explicitly:

```sh
agentmod approval resolve <session-id> <continuation-id> approve --json
```

The winning request resumes the canonical turn. Repeating the same decision
returns `transitioned: false` and does not execute the tool again. `deny`
commits a structured permission-denied tool result and lets the model continue.

`schedule add` stores exactly one trigger: a one-time occurrence, a fixed
interval when `--every-ms` accompanies `--at-ms`, a canonical runtime event,
or a literal match in one exact process's durable output. Every schedule carries explicit style, workspace,
permission policy, provider, model, token budget, and cost budget. `schedule
run` is the normal headless-worker cycle: it atomically claims due occurrences,
commits `scheduler.fired`, executes prompts through the ordinary intercepted
turn path, and writes idempotent terminal markers. `schedule claim` and
`schedule complete` expose the lower-level durable worker boundary for
supervisors. The daemon also polls automatically with bounded skipped-tick
behavior; `AGENTMOD_SCHEDULER_POLL_MS=0` disables that loop.

`agentmod-tui` opens the fullscreen terminal frontend. It reads the same
`AGENTMOD_RUNTIME_ENDPOINT` and `AGENTMOD_RUNTIME_AUTH_TOKEN` settings as the
headless CLI. `--smoke` validates authenticated health and session-list access
without changing terminal mode. `--smoke-turn` additionally executes a normal
provider turn and consumes its committed incremental stream.

`agentmod-acp` is the editor-facing Agent Client Protocol v1 stdio process. An
ACP client launches it with the runtime endpoint and bootstrap token in the
environment, sends `initialize`, then uses `session/new`, `session/load`,
`session/prompt`, and `session/cancel`. Agent output is emitted as
`session/update`; sensitive runtime continuations use
`session/request_permission`.

`session inspect` and `session replay` reconstruct verified state at the journal
head or an inclusive `--at` sequence. Replay is reducer-only and does not repeat
provider, tool, process, or network effects. `session branch` atomically creates
a fresh child journal at the selected point, records parent/fork ancestry, and
optionally replaces the explicit top-level style. Continuing the child does not
append to the parent.

For style-bound sessions, inspect/replay state includes
`style_introspection`. This stable projection contains style/source/cache and
harness identity, compiled graph nodes and edges, active/control/previous node
state, known next transitions and conditional candidates, loop/retry counts,
remaining step/token/iteration budgets, tool/permission/retry/termination
configuration, pipeline activity, memory provenance, compaction history,
child/join/reviewer state, and termination. Remaining cost and duration are
`null` until those accounting dimensions are canonically retained.

`session events` is the durable reconnect surface. It returns verified canonical
events strictly after `--after` (or from sequence 1), bounded to `--limit`
(default 256, maximum 1024). JSON output includes `head_sequence`,
`last_delivered_sequence`, and `has_more`; use the last delivered sequence as
the next cursor until `has_more` is false. A caught-up request returns an empty,
stable page without executing provider or tool side effects.

The CLI connects to the runtime over authenticated local RPC. Both processes
must receive the same token:

From a source checkout:

```sh
export AGENTMOD_RUNTIME_AUTH_TOKEN=<private-secret-at-least-32-bytes>
cargo run -p agentmod-runtime -- serve

# in a second terminal with the same variable
cargo run -p agentmod-cli -- doctor
cargo run -p agentmod-cli -- doctor --json --strict
cargo run -p agentmod-cli -- harness list --json
cargo run -p agentmod-cli -- session create --workspace . --style persistent-chat --harness native --json
cargo run -p agentmod-cli -- session list --json
```

`AGENTMOD_RUNTIME_ENDPOINT` overrides the platform endpoint. The binary name is
`agentmod`. Interactive chat, resume/rewind, style/plugin/MCP/tool management,
attachments, and slash commands remain incomplete.

The Windows and Unix runtime/CLI smoke scenarios are automated in
`tests/e2e/runtime_cli.ps1` and `tests/e2e/runtime_cli.sh`; these include a real
runtime→harness provider turn and exact journal-order assertions.
Replay and branch isolation are covered by `runtime_replay_branch.ps1` and
`runtime_replay_branch.sh`. Live, pre-completion CLI delivery is covered by
`runtime_cli_stream.ps1` and `runtime_cli_stream.sh`. Credit-window paging and
gap-free reconnect cursors are covered by `runtime_session_reconnect.ps1` and
`runtime_session_reconnect.sh`. Durable runtime scheduling is covered by
`runtime_scheduler.ps1` and `runtime_scheduler.sh`.

`schedule add` accepts time, runtime-event, and exact process-output triggers.
Pass `--deferred` to bind the prompt to a durable resume-once continuation;
`--expires-at-ms` adds an absolute expiry. Deferred schedules reject
`--every-ms`.
