# Creating a Session Style

Runtime-loadable session styles are not implemented. You can currently author
and test the generic graph portion in Rust tests using
`agentmod_graph_engine::compile`.

1. Start from the version 1 TOML example in the
   [format reference](../reference/session-style-format.md).
2. Declare hard step, token, cost, and duration budgets.
3. Declare every provider, tool, and capability referenced by nodes.
4. Give every loop `max_iterations`; ensure every path reaches a terminal node.
5. Avoid overlapping write scopes on parallel branches.
6. Compile with explicit `GraphCacheInputs` and `CompilerLimits`.
7. Inspect the resulting deterministic JSON and cache key in a test.

There is no `agent style validate` command, registry, built-in style selection,
or runtime node executor yet. A graph that compiles cannot currently drive a
session.
