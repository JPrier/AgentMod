# ADR 0003: Local IPC

Status: Accepted

AgentMod uses bounded, length-delimited CBOR frames over Unix-domain sockets and
current-user-restricted Windows named pipes. Frames carry versions, capabilities,
request/stream IDs, correlation/causation, idempotency, cancellation, heartbeat, and
flow-control information.

OS peer ownership is combined with a user-only runtime token. Consequential requests
carry a short-lived authorization grant bound to the approved proposal digest.
Reconnection resumes event subscriptions from committed sequences.
