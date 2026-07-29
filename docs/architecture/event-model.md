# Event Model

`agentmod-event-model` implements generic `EventEnvelope<T>`, metadata,
classification, origin, artifact references, sealing, and checksum verification.
Checksums bind canonical JSON for metadata and payload.

Implemented metadata includes event ID, scope, sequence, timestamp, semantic and
schema versions, correlation and causation IDs, optional graph node, origin,
artifact references, and proposal/decision/committed/observation classification.

Runtime data accepts `EventEnvelope<serde_json::Value>`, verifies it before
append, serializes at the data boundary, and verifies it again after journal
scan. It also checks dependency-frame IDs, sequences, checksum chains, and
offset ordering. Runtime session logic owns typed committed payloads for
session and branch lifecycle, structured conversation and context replacement,
provider requests and streams, tool proposals/dispatch/output/terminal state,
durable approvals, style graph execution, context boundary/phase execution,
scheduler claims, and process reconciliation. The pure reducer covers complete
replay and replay to an inclusive prefix.

Provider lifecycle frames, tool-host lifecycle frames, runtime scheduler
claims, frontend turn streams, and process reconciliation all enter canonical
history through runtime logic. In particular, process reattachment records one
`process.reconciliation_started`/`process.reconciliation_completed` pair; the
completion classification is committed before terminal tool state so receipt
recovery cannot leave a terminal action without its reconciliation provenance.
Terminal tool state retains a bounded success/failure outcome and exact action
digest. Approval recovery compares the reconstructed full proposal digest,
repairs an absent or call-only typed provider conversation pair without
redispatch, and rejects conflicting, reversed, or digest-mismatched history.

The specification's complete taxonomy is still broader than the implemented
typed set: detailed plugin lifecycle, frontend connection lifecycle,
child-agent coordination, and some schedule delivery categories remain
incomplete. Hidden provider reasoning is never claimed or modeled.
