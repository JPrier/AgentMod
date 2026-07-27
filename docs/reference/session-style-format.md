# Session-Style and Graph Format

There is no complete session-style manifest or runtime style loader yet. The
implemented reusable graph source is version 1 TOML and can be parsed and
compiled through `agentmod-graph-engine`.

```toml
format_version = 1
entry = "model"

[budget]
max_steps = 4
max_tokens = 4096
max_cost_micros = 1000000
max_duration_ms = 60000

[declarations]
capabilities = ["model"]
providers = ["deterministic-mock"]
tools = []

[[nodes]]
id = "model"
kind = "model_call"
provider = "deterministic-mock"
required_capabilities = ["model"]
read_scopes = ["session"]
write_scopes = []
retry_limit = 1

[[nodes]]
id = "done"
kind = "complete_turn"
required_capabilities = []
read_scopes = []
write_scopes = []
retry_limit = 0

[[edges]]
from = "model"
to = "done"
```

Implemented node kinds include context transformation, model/tool gates,
approval, agent coordination, review, bounded loop/branch/parallel control,
delay/schedule, event/artifact operations, and terminal outcomes. Compilation
validates structure, reachability, termination, bounded cycles, declarations,
parallel writes, retries, budgets, and constrained expressions.

Compilation does not execute nodes. Runtime bindings and the built-in style
definitions remain planned.
