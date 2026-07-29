# Session-Style and Graph Format

Session-style schema version 1 is implemented by
`agentmod-session-style-sdk`. The runtime registry discovers, validates,
compiles, caches, and binds these manifests to sessions. The embedded graph is
graph-engine format version 1.

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

The surrounding session-style manifest also declares identity, required
capabilities, allowed tool groups/providers/plugins, ordered interceptors,
memory, compaction, approval defaults, budgets, child-agent policy, retry,
termination, and explicit-selection policy. For example:

```toml
schema_version = 1
kind = "custom"
required_capabilities = ["model"]

[identity]
id = "example"
version = "1.0.0"
runtime_api = "^1.0"

[graph]
kind = "inline"
source = '''...graph TOML...'''

[memory]
provider = "file"
scopes = ["session"]
retrieval_timing = "before_model_request"
max_items = 8
max_injected_bytes = 32768
write_policy = "explicit_only"
injection_location = "before_current_input"

[memory.query]
source = "current_input"
include_active_artifacts = false
include_style_context = true
max_query_bytes = 16384

[compaction]
strategy = "sliding_window"
reserved_context_tokens = 4096
max_provider_projection_tokens = 32768
preserve_unresolved_tasks = true
preserve_active_processes = true
preservation_requirements = [
  "system_instructions",
  "current_input",
  "pending_control_state",
  "artifact_references",
  "memory_provenance",
  "active_graph_state",
  "tool_call_correlation",
]
```

Memory retrieval timing is one of `never`, `turn_start`,
`iteration_start`, `context_node`, or `before_model_request`. Query source is
`current_input`, `session_goal`, `current_input_and_goal`, or `explicit`.
Injection location is `none`, `before_conversation`, `after_conversation`,
`before_current_input`, or `context_artifact`. Write policy is `never`,
`explicit_only`, `turn_completion`, `iteration_completion`, or
`session_completion`.

Built-in compaction strategies are `none`, `sliding_window`, `summary`,
`artifact_handoff`, and `tool_output_eviction`. The runtime
currently executes no compaction, sliding window, and tool-output eviction.
Typed summary and artifact handoff are accepted by the compiler but fail
clearly at runtime unless approved summary or handoff material exists.

Declared graph node kinds include context transformation, model/tool gates,
approval, agent coordination, review, bounded loop/branch/parallel control,
delay/schedule, event/artifact operations, and terminal outcomes. Compilation
validates structure, reachability, termination, bounded cycles, declarations,
parallel writes, retries, budgets, and constrained expressions.

Compilation itself does not execute nodes. The runtime generic executor
currently supports persistent-chat compatible graph shapes. The remaining
declared node kinds are not all live runtime operations yet; validation success
therefore does not imply that every graph is currently executable.
