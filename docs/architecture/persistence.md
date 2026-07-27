# Persistence

## Implemented

`JsonlJournalDependency` provides locked append, monotonic sequence and duplicate
ID checks, frame and chain checksums, buffered/data/full durability choices,
scan validation, partial-tail detection, tail quarantine/truncation, and refusal
to repair interior corruption. Runtime data independently verifies canonical
event envelopes and frame identity.

`LocalArtifactDependency` provides bounded transactional writes, content-addressed
immutable objects, metadata, deduplication, bounded range reads, abort, and
incomplete-transaction cleanup.

`LocalSnapshotDependency` and `SnapshotDataPort` persist normalized JSON snapshots,
bind them to reducer/schema versions and terminal journal checksums, validate
content hashes, skip/report corrupt candidates, and select the latest compatible
anchor. Unit and local dependency round-trip tests cover these adapter boundaries.

`FileSessionCatalogDependency` atomically constructs a new session under a hidden
temporary directory, synchronizes the initial metadata and hash-chained creation
event, then renames it to `sessions/<id>`. It creates `metadata.json`,
`events.jsonl`, `style.json`, `style.lock`, `workspace.json`, `continuations/`,
`snapshots/`, `artifacts/`, `process-logs/`, and `branches/`. Bounded catalog
listing reads metadata only, so dormant conversations are not loaded and no task
is allocated per dormant session.

Point-in-time inspection scans the verified journal and invokes only pure
reducers through an inclusive sequence. It never enters provider or tool
dispatch paths. Branch creation reconstructs the selected structured
conversation, seals fresh child-session events, records immutable parent/fork
ancestry, and writes the complete child journal beneath a hidden temporary
directory before one final rename. Parent journal bytes are never modified.
The child starts active and may select a different explicit style.

Durable tool approvals are stored as schema-versioned, checksum-protected records
under `sessions/<session-id>/continuations/`. Each record contains the final
intercepted action and the provider/style data needed to reconstruct canonical
context after a daemon restart. Approval uses a locked pending-to-terminal
compare-and-set; duplicate equal decisions are idempotent. Unsupported record
schemas fail explicitly. The canonical journal acts as a dispatch outbox:
`tool.execution_dispatched` is committed before crossing the tool-host boundary,
and replay projects dispatched, started, and terminal states. A claim with no
dispatch can resume and a terminal dispatch is idempotent.

The supervised dependency layer writes a durable terminal receipt after the
isolated host returns its terminal stream and before that stream is returned to
runtime logic. Each receipt is atomically created beneath
`artifacts/tool-receipts/`, checksum protected, and bound to the exact execution,
session, call, tool, workspace, arguments, and cancellation identity. When
approval recovery finds a dispatched or started action, it issues a
`receipt_only` request. The dependency may return only a matching verified
receipt; it must not call a host when the receipt is absent. Runtime skips the
already committed event prefix using reducer-owned host-event counts and commits
the remaining terminal stream. Missing, corrupt, or conflicting receipts fail
closed as ambiguous.

`runtime_tool_receipt_recovery.ps1` kills the daemon in a bounded injected window
after a real filesystem write and durable receipt but before
`tool.execution_completed`. It restarts with a nonexistent filesystem-host
program and proves the original dispatch, start, completion, and side effect
each occur exactly once. Unix automation is also present. Startup-wide scanning
now enumerates checksum-verified receipts through dependency → data → logic
before the RPC listener opens. Logic reduces each selected journal, validates
the receipt's execution identity and reconstructed action digest against the
canonical outbox, and commits the missing host-event suffix plus the structured
tool projection. Already-terminal and orphaned receipts are classified.
Approved-continuation receipts remain with the resume-once continuation path,
which owns both receipt reconciliation and provider continuation.
`runtime_startup_tool_recovery.ps1` proves a non-approval dispatch recovers with
the host unavailable and can continue afterward. Transactional coupling of
arbitrary provider and process effects to receipts remains future work.

## Not implemented

Metadata sequence updates after later commits, integration of snapshot restore
into runtime startup replay,
runtime-wide SQLite
derived indexes and rebuild, orphaned `.creating-*` recovery, and provider/process
startup recovery are planned. Existing adapter tests do not establish full crash
recovery across every provider/tool/process effect.
