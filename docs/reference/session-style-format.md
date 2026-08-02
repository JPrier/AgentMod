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
allowed_plugins = ["fixture.context"]

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

[[context_transforms]]
plugin_id = "fixture.context"
transform_id = "fixture.redact"
version = "1.0.0"
declaration_hash = "1111111111111111111111111111111111111111111111111111111111111111"
lifecycle = "before_model_request"
configuration_reference = "2222222222222222222222222222222222222222222222222222222222222222"

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
inherit_mcp = false
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
`session_completion`. `turn_completion` is live for approved first-party file
and SQLite providers with canonical proposal, approval, dispatch, and completion
events plus exact-idempotency recovery. `session_completion` binds the exact
successful lifecycle event and can retain bounded canonical terminal node and
artifact evidence even when no assistant message exists; failed sessions do not
write. `iteration_completion` binds every exact successful loop transition,
including its node IDs, counters, canonical sequence, and event checksum, to a
distinct versioned write identity. Its bounded typed content contains only that
iteration's conversation delta and canonical node/artifact references. An
An automatic-memory `ask` decision creates a durable, exact approval
continuation. Native and plugin payloads use separate identity domains; approval
resumes only the bound action digest, denial is terminal without dispatch, and
duplicate resolution is effect-free. Both paths are restart-tested on Windows
and Ubuntu/WSL2. Every retained entry carries a bounded runtime-owned
information-flow class, and common credential/path/URL/handle detection rejects
the whole projection before proposal. Broader semantic DLP remains a production
limitation.

`context_transforms` is an ordered immutable selection, not a discovery query.
Each entry must name a plugin in `allowed_plugins` and match exactly one
runtime-advertised transform declaration by plugin ID, transform ID, semantic
version, declaration hash, and lifecycle. The current live lifecycle is
`before_model_request`; selected transforms run in vector order between memory
retrieval and compaction. `configuration_reference` is the content hash of the
exact style-owned adapter configuration. Restart never selects a newer
compatible version or another declaration. Missing or changed declarations
fail live validation, while a retained terminal receipt can finish an already
dispatched invocation without querying the plugin again. Current transforms
must be idempotent and declare no external effects. Runtime logic validates the
bounded typed replacement, preservation requirements, and mandatory policy
before committing a canonical provider-projection replacement.

Built-in compaction strategies are `none`, `sliding_window`, `summary`,
`artifact_handoff`, and `tool_output_eviction`; all five execute in the live
runtime. Typed summary is runtime-derived deterministically from canonical
typed projection entries and remains bounded by the compiled projection
budget. Artifact handoff stores a deterministic, content-addressed
`agentmod.context-artifact.v1` document with secret/session handling, commits a
durable artifact outbox plus exact replacement approval, and projects only the
typed logical ID, portable artifact reference, hash, MIME type, and label to the
provider. Replay is storage-free, while a later live provider effect verifies
the exact retained object before dispatch.

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

Compilation itself does not execute nodes. Runtime admission resolves every
compiled node exactly once through the immutable node-executor registry,
persists the selected executor identity and registry/plan hashes, and validates
graph, capability, parallel/recovery, permission, and budget semantics. Generic
execution dispatches arbitrary admitted graphs from those exact persisted
resolutions; it does not require a built-in style or complete-topology adapter
classification. Unsupported node semantics still fail admission with stable
diagnostics, and historical built-in plan generations retain explicit
versioned compatibility adapters.

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

`inherit_mcp` is one explicit, style-wide child policy switch; omission and
`false` both produce an empty child MCP binding. `true` copies only the exact
sanitized MCP binding of the immediate parent, and is valid only when both the
parent style and the child tool grant contain the `mcp` group and the parent
binding has a nonempty authenticated configuration reference. It does not
implicitly authorize grandchildren: a child that may create another child must
make its own explicit policy selection. Child creation and exact recovery fail
closed if the retained binding, tool gate, origin, or MCP binding differs.

The complete child policy was added in `persistent-chat@1.1.0` and
`planner-worker@1.1.0`. Style bindings are exact and immutable, so the runtime
does not substitute `1.1.0` for a persisted `1.0.0` binding. An unavailable old
version produces a compatibility error until the caller performs an explicit
migration or branches with a selected replacement style.
`planner-worker@1.4.0` selects runtime-owned `branch_workspace` leases with
manual review, exact child filesystem/process/Git tool groups, evidence-aware
integration/reviewer executors, and the exact Research v1.3 child style. Earlier
planner-worker versions remain available and are never silently upgraded.

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
