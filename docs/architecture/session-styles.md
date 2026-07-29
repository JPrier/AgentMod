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
Persistent-chat and ephemeral-turn compatible graphs execute through this path
while provider calls, tool proposals, permission checks, receipts,
continuations, and recovery remain in their existing runtime components.

Style-selected context composition is live for persistent-chat and
ephemeral-turn compatible graphs. Memory retrieval, context replacement,
compaction, and ephemeral projection discard use the existing blocking proposal
pipeline and retain canonical provenance. Ephemeral turns build a fresh
provider projection from the current typed input plus only selected context,
then empty that projection before terminal node completion while preserving
canonical history. Exact context and graph events make restart recovery
fail-closed around both replacement boundaries. Unsupported graph shapes fail
before a turn mutates the journal.

The remaining built-in manifests are discoverable and inspectable, but their
runtime node implementations are still partial: research loop,
planner-worker-reviewer, and arbitrary declarative graphs must not be described
as executable until their required node kinds and recovery paths are complete.
Plugin-selected pipelines and child sessions are likewise planned integration
work.

See [Session-style format](../reference/session-style-format.md).
