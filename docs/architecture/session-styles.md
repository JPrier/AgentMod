# Session Styles

A session style is a selected, versioned execution contract. It is resolved and
compiled before the session is created, and its immutable binding is persisted
with the session. The binding includes the style identity and hashes, source,
runtime API and plugin/capability identities, graph cache key, harness
requirement, memory and compaction configuration, tool groups, approvals,
budgets, child-agent policy, retry policy, and termination policy. Restart and
branch operations revalidate that exact binding; the runtime does not silently
substitute another style.

The runtime style registry follows the runtime service -> logic -> data ->
dependency boundary. It discovers built-in, user, project, and plugin-provided
records; supports list, inspect, validate, compile, disablement, and availability
diagnostics; and stores compiled styles in memory and in a persistent cache.
Validation and graph compilation remain owned by `agentmod-session-style-sdk`
and `agentmod-graph-engine`.

The generic style executor consumes the compiled SDK graph. It records
initialization, node entry, node completion or failure, and selected transitions
as canonical events and reconstructs its active node from replay.
Persistent-chat, ephemeral-turn, research-loop, planner-worker-reviewer, and
the bounded declarative fixture execute through this path while provider calls, tool proposals,
permission checks, receipts, continuations, artifacts, and recovery remain in
their existing runtime components.

Style-selected context composition is live for persistent-chat and
ephemeral-turn compatible graphs. Memory retrieval, context replacement,
compaction, and ephemeral projection discard use the existing blocking proposal
pipeline and retain canonical provenance. Ephemeral turns build a fresh
provider projection from the current typed input plus only selected context,
then empty that projection before terminal node completion while preserving
canonical history. Exact context and graph events make restart recovery
fail-closed around both replacement boundaries. Unsupported graph shapes fail
before a turn mutates the journal.

The planner-worker-reviewer adapter executes its compiled plan, spawn, wait,
integrate, review, revision, and terminal nodes. Plans become bounded
runtime-owned task records. Each worker is an atomically created child session
with a typed parent/task link and a restricted immutable style binding. Exact
joins and structured reviewer decisions are canonical replay state; a rejection
selects only the rejected tasks for the next bounded loop iteration. Current
workers execute sequentially, workspace mode is selected but not yet enforced
by a dedicated isolation dependency, and child results are typed handoff entries
rather than immutable result/diff/test artifacts. Those limits keep the
deterministic adapter short of the complete product scenario. Arbitrary
declarative graphs are limited to graph shapes whose runtime node adapters are
implemented; compilation success alone does not make an unsupported node
combination executable. Plugin-selected pipelines remain planned integration
work.

Enabled child-agent policies are complete execution contracts rather than only
numeric limits. They select an exact `style-id@semver`, workspace mode,
provider/model inheritance, context/token/cost budgets, tool groups, memory
access, join semantics, cancellation propagation, and reviewer-attempt bound.
The built-in `persistent-chat@1.1.0` and `planner-worker@1.1.0` versions add
those fields. A persisted `1.0.0` binding is not silently upgraded: if that
exact style version is absent, restart or selection reports explicit
unavailability and requires a deliberate migration or branch-with-style.

See [Session-style format](../reference/session-style-format.md).
