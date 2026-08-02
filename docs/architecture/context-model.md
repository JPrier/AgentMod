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

The compiled session style may select an ordered vector of plugin context
transforms for `before_model_request`. Each immutable selection binds the exact
plugin ID, transform ID, semantic version, declaration hash, lifecycle, and
configuration-reference hash. Vector position is authoritative: runtime replay
names the phases `plugin_context_transform:0`,
`plugin_context_transform:1`, and so on, between `memory` and `compaction`.
Restart does not reorder the vector, substitute another compatible transform,
or accept a declaration with the same nominal version but a different hash.
The reducer rejects a missing, repeated, reversed, or substituted phase and
requires every selected transform phase to complete before compaction or
boundary completion.

Plugin transform invocation preserves the process boundaries:

```text
runtime logic
    ↓
runtime data
    ↓
runtime dependency
    ↓
plugin-host service
    ↓
plugin-host logic
    ↓
plugin-host data
    ↓
plugin-host dependency
    ↓
isolated plugin process
```

Runtime logic constructs a bounded typed input from canonical replay state.
Data rechecks the active exact declaration, and dependency owns protocol and
process types, keyed authorization, the exact action digest and nonce, bounded
frames, timeout, cancellation, and plugin-host response/audit validation. The
plugin returns only a proposed provider-projection replacement; it cannot
commit a runtime event or mutate canonical conversation state.

One transform has the canonical lifecycle
`context.phase_started` →
`plugin.context_transform_proposed` →
`plugin.context_transform_dispatched` → exactly one of
`plugin.context_transform_completed`, `plugin.context_transform_failed`, or
`plugin.context_transform_ambiguous`. A successful terminal proposal is still
non-authoritative. Runtime logic validates the bounded output schema, typed
conversation entries, projection size, and mandatory preservation requirements,
then runs the ordinary replacement proposal and policy pipeline.
`plugin.context_transform_replacement_approved` retains the exact replacement
hash and action digest before `context.projection_replaced` atomically applies
the replacement and completes that plugin phase. Runtime logic constructs the
canonical envelope and provenance.

Terminal plugin results are sealed into durable generic invocation receipts
before their terminal journal event is committed. Recovery dispatches a
proposal once, recovers a dispatched invocation only from its exact retained
receipt, resumes authorization from a committed terminal proposal, and applies
an already approved replacement without invoking the plugin again. A missing
receipt after dispatch, an explicit ambiguous result, declaration drift, or a
receipt/identity mismatch fails closed. Pure reducer replay reads only canonical
events and never calls the receipt store or plugin host; the live recovery
coordinator consults the retained receipt before resuming. Before live
invocation, logic revalidates the exact declaration and currently requires the
transform to be idempotent and declared free of external effects. Before
applying an already-approved replacement after restart, mandatory policy is
revalidated.

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
replay-derived. No-op, sliding-window, typed-summary, and tool-output-eviction
compaction execute live. Typed summary deterministically constructs a bounded
JSON summary from complete typed projection entries, retains the newest whole
replaceable records plus omission counts, restores declared live records, and
commits one typed summary with exact source provenance.

Artifact-handoff compaction also executes live. Runtime logic canonically
encodes the complete pre-compaction typed provider projection, prior projection
provenance, source projection hash/count, and exact context-boundary identity as
`agentmod.context-artifact.v1`. The document passes the normal artifact
proposal, style/plugin/user/mandatory policy, durable dispatch, and
content-addressed persistence path under the session's dedicated
`artifacts/context` root with secret handling and session retention. A logical
UUID is used by the typed projection while the portable
`artifact:blake3:<hash>` reference and event-envelope `blake3:<hash>` identity
bind the exact stored object.

Artifact persistence events advance the owning compaction phase. A separate
hash-only `context.projection_replacement_approved` event retains the exact
replacement/action approval before the replacement commit. Restart classifies
phase-only and proposal-only cuts as ambiguous and fails closed; approved
artifact writes dispatch once, dispatched writes reconcile an exact
content-addressed object, and approved replacements complete without rerunning
policy. Pure replay never queries artifact storage. Before a live provider
effect resumes, dependency inspection re-hashes the retained content and logic
verifies its hash, byte size, MIME type, producer, security, retention, stored
identity, portable reference, projection entry, and replacement envelope.
Artifact action digests bind exact security and complete retention, including
an exact expiry. Reducer replay derives proposal/execution IDs, reconstructs the
source-context document hash, authenticates the replacement-action digest, and
validates the logical entry, portable reference, physical object, label, source
sequence, and projection provenance. Restart revalidates current mandatory
policy before dispatching a previously approved artifact write.
Dedicated Windows and Ubuntu/WSL2 process tests also kill after dependency
finalization has made the content object and metadata durable but before the
completion result returns. Restart persists the same exact bytes through the
content-addressed deduplication path, commits one canonical artifact lifecycle
and provider request, and leaves journal and artifact hashes unchanged on pure
replay.

`turn_completion` automatic memory writes also execute live for approved
first-party file and SQLite providers. Runtime logic derives bounded
`agentmod.context-summary.v1` content from eligible canonical user/assistant
history as of successful turn completion; it is not limited to the latest pair.
Privileged instructions, tool payloads/results, artifact metadata, runtime
metadata, and child handoffs are excluded. A common credential marker skips the
automatic write before proposal commitment. Runtime logic commits an exact
`memory.write_proposed` →
`approved` → `dispatched` → `completed` outbox per configured scope. Its
immutable identity binds the session, provider, scope, policy, source, run,
exact provider request, content hash, byte size, and recorded timestamp.
Approved and dispatched recovery revalidates mandatory policy and reconciles
the same dependency idempotency request; it never chooses another provider or
automatically repeats an ambiguous proposal. Replay reduces the retained outbox
without consulting the memory provider.

When native automatic-memory policy returns `ask`, runtime logic persists a
native-only continuation payload and an approval subject bound to the exact
write ID and recomputed action digest. Approval resumes that same provider,
scope, request, style, and cancellation identity once; denial becomes a
terminal write failure without dispatch, and duplicate resolution is a no-op.
The existing plugin continuation kind and ID domain are unchanged. Windows and
Ubuntu/WSL2 `runtime_automatic_memory` process tests prove restart while pending,
approve once, duplicate suppression, and denial without a memory effect.

`iteration_completion` uses the same policy/outbox/provider boundary but owns a
separate, versioned identity domain. Every successfully completed loop node
records the exact selected transition: loop and destination node IDs, attempt,
loop iteration, step, canonical event sequence, and event checksum. One
automatic write binds that immutable boundary and a boundary-hashed source.
Content is a bounded `agentmod.iteration-completion-memory.v1` projection of
only the conversation delta, completed node-output references, and retained
artifact references belonging to that iteration. Large values remain artifact
references. Replay requires the exact retained boundary and rejects substituted
fields, duplicate writes for the same provider/scope/boundary, and legacy
iteration records without the boundary. Turn- and session-completion identities
retain their original v1 digest contract.

An immutable style may also select an automatic plugin memory writer. Runtime
logic binds the exact plugin, executor declaration, implementation version,
handler, immutable configuration reference, scope, typed value, and semantic
request hash before proposing the write. An `ask` decision is a durable
continuation; approval resumes the same bound operation. Dispatch is one-shot
and terminal plugin-host receipts are durable. A missing, corrupt, invalid, or
timed-out post-dispatch result becomes an explicit ambiguous failure and is
never automatically relaunched. Restart reconstructs the same exact provider
selection and rejects declaration or configuration drift before proposing or
dispatching work.

Windows and Ubuntu/WSL2 process tests kill the runtime after the file provider
has fsynced the record but before it returns, restart with both harness paths
unavailable, reconcile one retained write, and prove pure replay leaves the
journal and memory file unchanged. Focused three-iteration tests also prove
exact boundary checksums, distinct write identities, idempotent finalization,
and dispatched-write recovery without provider redispatch.

The `SQLite` dependency exposes the same deterministic validation seam at a
different durability boundary: it commits the exact FTS transaction before
delaying the terminal dependency response. Dedicated Windows and Ubuntu/WSL2
process tests observe one integrity-checked semantic row while
`memory.write_dispatched` is canonical and `memory.write_completed` is absent,
then kill the runtime. Restart sets the delay to zero and makes both harness
paths unavailable. Recovery issues only the same deterministic idempotency
request, retains one logical row, commits one canonical completion, and never
redispatches the model provider. Pure replay leaves the journal digest and the
ordered semantic row projection unchanged. The assertion deliberately avoids
`SQLite` database/WAL byte identity because valid checkpoint behavior may
change those storage bytes without changing canonical memory state.

Dedicated Windows and Ubuntu/WSL2 process tests kill the runtime after the
first iteration record
is durable but before its terminal event, restart with both harness paths
unavailable, recover all three writes without model redispatch, and prove pure
replay leaves the journal and memory file byte-identical. Dedicated Windows and
Ubuntu/WSL2 process tests also cover plugin automatic-memory approval restart,
a post-persist runtime crash, receipt-only recovery, invalid results, timeout
after isolated-worker entry, missing/corrupt receipts, no ambiguous redispatch,
and plugin unavailability after session creation. A separate cross-platform
session-completion test kills after the exact terminal memory record is durable,
restarts with both harness paths unavailable, completes from the retained
receipt without provider execution, and proves byte-identical pure replay. A
bounded runtime-owned information-flow classifier attaches an explicit class
to every retained automatic-memory entry. High-confidence credential, private
key, signed-token, path, URL, process/pipe handle, control-character, and
inspection-bound findings reject the whole projection before proposal. Node
and artifact references use an exact portable-domain allowlist. Windows and
Ubuntu/WSL2 session-completion process tests prove both retained classification
and raw sensitive/external-handle rejection. Broader semantic DLP remains
incomplete. Plugin-provided memory
retrieval and compaction now traverse runtime logic → data → dependency →
plugin-host service → logic → data → dependency → isolated worker. Their exact
immutable selections, request/readable-state hashes, dispatch intent, typed
proposal, sealed receipt, application approval, and canonical replacement are
replay-owned. A restart reduces an exact receipt without worker access or
redispatch, then revalidates live composition before any later effect. Missing,
corrupt, substituted, invalid, timed-out, and unavailable results fail closed.
Cross-platform process tests exercise retrieval at turn-start, context-node,
before-model, and repeated iteration-start boundaries. Every iteration-start
operation has a distinct canonical identity and terminal receipt; sealing a new
receipt binds only the entries introduced by that invocation, so earlier memory
keeps its exact original receipt provenance. Plugin context transforms are
currently limited to ordered `before_model_request`,
idempotent, external-effect-free projection replacements; other lifecycle
boundaries and effectful transform declarations are not enabled. The file
provider now quarantines and truncates only an invalid unterminated final
record at the last verified checksum boundary, completes a valid record missing
only its terminator, and never repairs complete or interior corruption.
