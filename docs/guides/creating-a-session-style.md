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

Compilation validates every declared graph node kind, but runtime execution
support is currently narrower. Persistent-chat compatible
`model_call -> tool_execution_gate -> complete_turn` graphs execute through the
generic executor. Other graph shapes fail before journal mutation until their
runtime node adapters and recovery paths are implemented. Check `STATUS.md`
before treating validation success as an execution-availability guarantee.
