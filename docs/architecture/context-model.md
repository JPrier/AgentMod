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
history. Provider token counters and compaction checkpoints are replay-derived.
No compaction, sliding-window compaction, and tool-output eviction execute live.
Typed-summary, artifact-handoff, and context-artifact modes fail closed when no
approved material is available.

Automatic style-selected memory writes, plugin-provided memory or compaction
implementations, plugin-composed context transforms, and all cancellation/rebuild
cases remain integration work.
