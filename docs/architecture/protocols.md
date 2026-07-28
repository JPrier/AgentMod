# Protocols

Current wire crates:

- `agentmod-runtime-protocol`: health, session create/list/inspect/replay/branch,
  turn streaming, subscription, approval, and cancellation DTOs;
- `agentmod-harness-protocol`: provider execution lifecycle DTOs, including
  individually framed events with an explicit terminal marker;
- `agentmod-tool-protocol`: capability-host DTOs;
- `agentmod-plugin-protocol`: isolated plugin DTOs;
- `agentmod-frontend-protocol`: frontend capabilities and lifecycle DTOs.

`agentmod-protocol-support` implements version/capability negotiation, request,
correlation, causation, idempotency and cancellation metadata, bounded CBOR
framing, stream frame kinds, heartbeats, window updates, and error envelopes.
It also owns the common keyed authorization primitive used by capability hosts:
claims bind owner, session, call, action, exact operation digest, expiry, and
nonce.

Tool protocol 1.0 is used by the reconnectable process host over Unix sockets
and Windows named pipes. A token-authenticated handshake negotiates bounded
request/response, streaming, cancellation, idempotency, and backpressure
capabilities before any `ToolHostCommand` is decoded. Every response repeats
the exact request, correlation, causation, idempotency, and cancellation
identity with a monotonic stream sequence. Service failures become terminal
`ToolHostEvent::Failed` frames instead of tearing down the listener.

The runtime service uses this framing over Unix sockets and Windows named pipes,
with mandatory token-authenticated negotiation before endpoint dispatch.
Runtime wire version 2.1 includes session-scoped approval resolution and resumed
provider events. Provider lifecycle items are emitted as ordered `StreamItem`
frames only after their canonical event commit; `StreamEnd` carries the turn
sequence range and continuation state. When `credit_windows` is negotiated, one
initial nonterminal item is permitted; every later item requires a
request/correlation/idempotency-bound `WindowUpdate` acknowledging the exact
last contiguous stream sequence. Zero, excessive, stale, or cross-request
credits are rejected. Bounded channels propagate slow-client backpressure
through service, logic, data, and harness-process reads. The CLI
dependency validates request identity and monotonic stream sequence, exposes a
bounded stream through all four CLI layers, and either flushes live NDJSON or
aggregates a backwards-compatible batch result. Version 1.x peers fail major-version
negotiation instead of guessing the changed request shape.

`Subscribe` provides bounded durable catch-up strictly after a canonical
sequence. Each page reports the verified head, last delivered cursor, and
whether another immediate page exists. This gives reconnecting frontends
gap-free replay without repeating effects. Continuous live subscription after
catch-up, OS-owner validation, and cross-process golden interoperability remain
planned. Protocol DTOs are permitted only at service and dependency boundaries,
never in logic or data.
