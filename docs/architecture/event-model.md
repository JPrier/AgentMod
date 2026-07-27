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
offset ordering. Runtime session logic contains a small typed committed-event
set and pure reducer with replay-to-prefix tests.

The complete product event taxonomy in the specification is not implemented.
There is no runtime event bus wiring provider, tools, plugins, frontends, or
scheduler events yet. Hidden provider reasoning is never claimed or modeled.
