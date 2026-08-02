# Context Model

Runtime logic implements `ConversationState` with separate immutable canonical
history and replaceable provider projection. Typed entries include instructions,
user/assistant text, tool calls/results, artifacts/images, summaries, retrieved
memory, metadata, tasks, active processes, and child-agent handoffs.

`replace_projection` preserves history and records `ProjectionProvenance`,
including source range, method, commit sequence, and optional artifact. Tests
prove that replacement does not fabricate a user message.

Runtime compaction logic implements no-op, sliding-window, typed summary,
artifact-handoff, and tool-output-eviction strategies while preserving canonical
history, unresolved tasks, active processes, and artifact references.

Branch materialization preserves exact structured history inline while it is at
most 32 entries and 64 KiB. Above either bound, logic serializes complete parent
history, provider projection, projection provenance, ancestry, and fork sequence
into a maximum-16-MiB JSON artifact. Data maps the artifact into a dependency
request; the session catalog verifies its UUID, media type, and BLAKE3 hash,
writes private/session-retained metadata, and atomically renames it with the
child journal. The child carries an explicit artifact-reference entry plus at
most the newest 16 projection entries and 64 KiB, with
`branch_artifact_handoff` provenance. No fabricated user message is used.

Replaceable memory boundaries now include no-memory, checksum-protected
append-only file memory, and `SQLite` FTS5 memory. Logic rejects unapproved
writes before data access and retrieved items carry provider, query, scope,
source, score, creation time, injection event, reference, and byte contribution.
The `SQLite` adapter follows FTS5 `MATCH` with deterministic `rank` ordering.

Memory and compaction are also explicit per-session creation selections. A
client override is not a mutable post-creation toggle: the session-style SDK
normalizes the requested profile, the runtime recompiles the complete style,
and the resulting configurations and hashes become part of the immutable
session binding. Selecting `none` installs disabled controls; enabling a
component from a disabled style installs bounded SDK defaults before normal
validation. The style's own selection remains the default when no override is
provided.

`agentmod-harness-protocol` defines a smaller provider-visible `ProjectedEntry`
wire representation. Before a provider request, runtime logic composes context
from the session's compiled style. It routes no-memory, file, and SQLite FTS
retrieval through dependency -> data -> logic; enforces configured timing,
query, scopes, item and byte limits, and injection location; and records
provider, query, source, score, reference, byte contribution, injection event,
and sequence as canonical provenance.

Context construction, context replacement, and compaction are authorized
through the existing blocking proposal pipeline. Approved projection
replacement is recorded canonically and never changes immutable conversation
history. `context.boundary_started`, `context.phase_started`,
`context.phase_completed`, and `context.boundary_completed` records make the
memory-before-compaction lifecycle replayable. Boundary identity binds graph
node, lifecycle point, turn/continuation origin, run ID, canonical request hash,
and source head. The reducer rejects overlaps, reversed phases, incomplete
phases, and supplied projection measurements that do not match the replayed
provider projection. A retry may reuse a completed phase only when provider,
model, canonical options, current input, and run identity still match; a crash
after an interceptor starts but before its completion fails closed.

Ephemeral-turn graphs use the same lifecycle rather than a parallel context
path. Their `turn_start` memory phase replaces the provider projection with the
current typed user input plus only style-selected records and records
`ephemeral_fresh_context` provenance. After the visible assistant entry is
committed, a `before_turn_completion` boundary runs one authorized `discard`
phase and records an empty projection with `ephemeral_discard` provenance.
Canonical history is never removed or converted into a fabricated user
handoff. Replay permits only the exact compiled context-transform-to-model edge
with matching run and request identity, and can finish already committed
discard evidence without redispatching the model.

Projection pressure uses a deterministic approximate-token estimator over the
complete provider wire representation and a separate exact 16-MiB serialized
safety cap. Provider token counters and compaction checkpoints are
replay-derived. No compaction, sliding-window, tool-output-eviction, typed
summary, and artifact-handoff strategies all execute live.

Typed-summary compaction runs only under the selected style's pressure rules.
The runtime builds a bounded typed request material from protected state plus
a bounded recent window (never a fabricated user message), prepares it as a
normal model-request proposal through the session's style, plugin, user, and
mandatory policies, and dispatches it through the selected harness with an
explicitly configured provider/model or the session's deterministic fixture.
Canonical `context.summary_proposed/approved/started/completed` events bind the
provider/model/harness identity, the exact request hash, the bounded summary
text, and provider-reported usage; summary usage counts toward the style token
budget. Terminal evidence is reused on restart and a started-but-unfinished
summary fails closed, so recovery can never duplicate the provider call. The
approved summary becomes a typed `ContextSummary` projection entry with the
exact source range, method, and optional artifact.

Artifact-handoff compaction serializes the complete selected context into an
immutable content-addressed payload that binds the source range, exact hash,
security classification, and media type
(`application/vnd.agentmod.context+json`). The write follows the canonical
artifact outbox (`context.artifact_proposed/approved/dispatched/completed`)
with reconcile-first recovery; the store's content addressing makes a
re-dispatched write idempotent. The replacement projection carries a bounded
typed `ArtifactReference` entry while protected runtime state and the current
input are restored, and canonical history is never mutated. Branch and
restart recovery reuse committed receipts without rewriting artifacts.

A style may select automatic memory writes with a trigger boundary, eligible
content categories, scope, provider, record/byte bounds, a cross-restart
deduplication policy, retention, approval mode, and failure behavior. Writes
are proposed at turn completion, research-finding persistence, child
completion, reviewer approval, and explicit memory-extraction nodes. Every
write follows proposal -> interceptors -> user policy -> mandatory policy ->
dispatch evidence -> memory provider -> canonical reference, recorded by the
`memory.write_proposed/approved/dispatched/completed` outbox. `RequireUserApproval`
routes through a durable approval continuation resolved by the standard
approval endpoint. Exact duplicate prevention uses a canonical write identity
plus a provider deduplication key, so an identical turn after restart never
duplicates a provider write.

Stable plugin-facing ports for plugin memory, plugin compaction, and context
transform lifecycle boundaries are defined in `context_ports`; runtime
validation rejects fabricated roles, undeclared secrets, required-state
removal, duplicate entries, and context-limit violations. Plugin-host
transport adapts these ports in a later workstream.
