# ADR 0017: Reconnectable process-host transport

- Status: accepted
- Date: 2026-07-28

## Context

Runtime-owned stdio pipes made a process host a child of one daemon lifetime. Dropping the daemon killed the host or made its inherited stdin, stdout, process-group, and PTY handles unreachable. Durable PID reconciliation prevents duplicate dispatch, but a replacement process cannot recreate those handles.

A permanently resident host per historical session would violate the dormant-session resource requirement. Retrying a failed transport request automatically would also be unsafe because the host may have executed the action before the connection failed.

## Decision

The process host supports two service transports:

- bounded JSONL stdio for direct development and compatibility tests;
- a reconnectable local endpoint using tool protocol 1.0 and bounded CBOR frames.

The reconnectable endpoint uses:

- absolute Unix sockets under a private runtime endpoint root;
- Windows named pipes with remote clients rejected;
- token authentication before command decoding;
- version and capability negotiation;
- exact request, correlation, causation, idempotency, cancellation, and stream-sequence validation;
- normal per-action owner/session/digest/expiry/nonce grants after transport authentication.

The runtime derives a restart-stable host key from its protected runtime bootstrap token using a domain-separated BLAKE3 digest. Endpoint names are deterministic hashes of owner, session, and workspace. The key is passed to a newly launched host through its bootstrap environment and is not written to canonical events or ordinary logs.

Hosts are launched without `kill_on_drop` in an independent process group. A replacement runtime possessing the same bootstrap authority reconnects to the existing host. The runtime closes local streams during graceful shutdown but does not terminate hosts that may own live children.

The service asks logic for a live-child count; logic maps through data; data maps through dependency. The endpoint exits after an idle interval only when no request is in flight and no locally supervised child remains. Recovered-unattached records do not keep a replacement host resident because that host owns no useful inherited handles.

A transport failure removes the client connection but is not retried automatically. Runtime outbox and terminal-receipt reconciliation decide whether a higher-level action can continue without repeating a side effect.

## Consequences

Runtime replacement can preserve interactive PTY and process control without weakening grant checks. Dormant sessions do not retain a helper process after their final child and request. A capability-host crash still loses inherited handles and uses the fail-closed durable recovery classification from ADR 0016.

The endpoint protocol adds one authenticated negotiation per new connection. Existing connections are reused until the host idles or transport fails.

The daemon-replacement acceptance test uses a distinct provider tool-call ID
for every control action. This is required because terminal receipts bind an
execution ID to one exact request; reusing an ID for `start_pty` and
`reattach` is rejected before transport, as intended.
