# Session Styles

The generic graph compiler and constrained expression engine are implemented.
The graph format is versioned TOML with bounded nodes, edges, identifiers,
conditions, retries, loops, capabilities, tools, providers, parallel writes, and
execution budgets. Compilation produces deterministic inspectable data and a
cache key bound to graph, plugins, runtime API, and capabilities.

No runtime session-style registry or executor exists yet. Persistent chat,
ephemeral turn, research loop, planner-worker, and declarative graph are target
styles, not currently selectable product modes. Runtime-specific behavior for
compiled node kinds remains planned.

See [Session-style format](../reference/session-style-format.md).
