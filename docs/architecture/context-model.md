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
wire representation. Live runtime retrieval/compaction event coordination,
memory interceptor dispatch, token accounting, provider-specific serialization,
and cancellation/rebuild flows remain incomplete.
