# Creating a Session Style

Runtime-loadable session styles use schema-version-1 TOML or JSON manifests.
The runtime discovers built-in, user, project, and plugin style sources, then
validates and compiles them with `agentmod-session-style-sdk`.

1. Start from the version 1 TOML example in the
   [format reference](../reference/session-style-format.md).
2. Give the style a stable ID, semantic version, and compatible runtime API
   requirement.
3. Declare hard step, iteration, token, cost, and duration budgets.
4. Declare every provider, tool group, plugin, and capability used by the graph
   or interceptor pipeline.
5. Select memory retrieval/write policy, compaction, approvals, child-agent
   limits, retry, and termination behavior explicitly.
6. Give every loop `max_iterations`; ensure every path reaches a terminal node.
7. Avoid overlapping write scopes on parallel branches.
8. Validate and inspect the deterministic compiled descriptor and cache key:

```shell
agentmod style validate path/to/style.toml
agentmod style compile path/to/style.toml
agentmod style list
agentmod style inspect your-style-id
```

Create a session with a registered style using
`agentmod session create --style your-style-id`. The selection is compiled and
persisted as an immutable binding; restart never silently substitutes another
style.

Compilation and runtime executability inspection resolve one exact registered
implementation for every node and persist that immutable execution plan with
the session. Arbitrary admitted graphs execute through those persisted executor
identities rather than a style ID, node label, bundled fixture, or complete
topology classifier. The admitted native set includes conditional and bounded
loop control flow, parallel branches and joins, model/tool/approval/artifact
effects, delay and scheduling, user-space event emission, child
spawn/message/wait/review orchestration, plugin-backed nodes, and terminal or
structured-failure routes. Cross-platform process matrices cover arbitrary
control-flow and child-orchestration graphs plus current built-in styles; check
`STATUS.md` for the exact platform evidence of an individual effect class.

The dedicated `arbitrary-graph-schedule.toml` fixture demonstrates a
user-supplied one-time `schedule` node that waits for its trigger. Its Windows
and Linux process scripts verify that `runtime.schedule@1.0.0` and the compiled
configuration reference are persisted in the immutable plan, schedule creation
passes through consequential-action policy, the scheduler request and
continuation survive daemon replacement, the wake resumes exactly once, and
pure replay performs no live query or duplicate effect. Recurring,
runtime-event, and process-output graph schedules are supported by the same
executor but currently have unit/integration rather than arbitrary-graph
process coverage.

Validation is still fail-closed: every edge, variable dependency, executor,
capability, permission, budget, parallel merge, and recovery semantic must be
supported by the live registry. Unsupported nested parallel executor classes or
malformed regions are rejected before session persistence. Inspect the compiled
descriptor and its executability diagnostics rather than assuming that an
unknown future node kind is available.
