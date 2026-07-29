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

[harness]
id = "native"
required_capabilities = [
  "cancellation",
  "streaming",
  "structured_context_replacement",
  "token_usage",
]

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

[child_agents]
max_children = 4
max_concurrent = 2
max_depth = 1
per_child_token_budget = 16000
child_style = "ephemeral-turn@1.1.0"
workspace_mode = "shared_read_only"
inherit_provider = true
inherit_model = true
context_budget_tokens = 12000
per_child_cost_budget_micros = 250000
tool_groups = []
memory_access = "none"
join_behavior = "all"
cancellation_behavior = "cascade"
reviewer_max_attempts = 2
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

Calling clients may request a memory-provider or compaction-strategy override
when creating a session. This produces a newly compiled immutable binding; it
does not edit the source manifest or mutate an existing session. The SDK
normalizes `none` and transitions from disabled profiles, then applies the same
availability, bounds, preservation, and lifecycle validation used for ordinary
manifests. The selected configuration and its new content/cache hashes are
retained in session metadata and canonical `session.created` state.

Declared graph node kinds include context transformation, model/tool gates,
approval, agent coordination, review, bounded loop/branch/parallel control,
delay/schedule, event/artifact operations, and terminal outcomes. Compilation
validates structure, reachability, termination, bounded cycles, declarations,
parallel writes, retries, budgets, and constrained expressions.

Compilation itself does not execute nodes. The runtime generic executor
currently supports persistent-chat, ephemeral-turn, research-loop, and the
bounded built-in declarative fixture. The remaining declared node kinds are not
all live runtime operations yet; validation success therefore does not imply
that every graph is currently executable.

Set every child-agent field to zero and omit the extended fields to disable
children. An enabled policy must declare every extended field shown above.
`child_style` is an exact `style-id@semver` selector. `workspace_mode` is
`shared_read_only`, `shared_serialized_writes`, `independent_git_worktree`,
`temporary_copy`, or `explicit_custom_workspace`; the last also requires
`custom_workspace`. Memory access is `none`, `read_only`, or `read_write`.
Join behavior is `all`, `first_success`, or `any_terminal`; cancellation
behavior is `cascade`, `detach`, or `wait`. Child budgets must fit within the
parent style budgets, and child tool groups must be a subset of the style's
allowed tool groups.

The complete child policy was added in `persistent-chat@1.1.0` and
`planner-worker@1.1.0`. Style bindings are exact and immutable, so the runtime
does not substitute `1.1.0` for a persisted `1.0.0` binding. An unavailable old
version produces a compatibility error until the caller performs an explicit
migration or branches with a selected replacement style.

`harness.id` is a stable runtime harness-registry ID.
`required_capabilities` uses lower-case identifiers such as `streaming`,
`tool_calls`, `multiple_tool_calls`, `cancellation`, `images`,
`structured_output`, `structured_context_replacement`, `provider_switching`,
`token_usage`, `cost_metadata`, `external_tool_ownership`, and
`fine_grained_proposal_boundaries`. The SDK validates identifier syntax; the
runtime validates availability and the actual descriptor set during selection.
The exact harness ID, adapter version, capability-set hash, and required set
are retained in the immutable session binding and revalidated on resume.
Schema-v1 manifests that omit this table select `native` with no additional
requirements.
