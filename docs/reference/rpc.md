# Local RPC Reference

The runtime service has an authenticated local listener: Unix domain sockets on
Unix-like systems and Tokio named pipes on Windows. Start it with:

```sh
# use a randomly generated 32-byte-or-longer secret in real use
AGENTMOD_RUNTIME_AUTH_TOKEN=<bootstrap-secret> \
cargo run -p agentmod-runtime -- serve
```

`AGENTMOD_RUNTIME_ENDPOINT` overrides `/tmp/agentmod-runtime.sock` or
`\\.\pipe\agentmod-runtime`. The listener refuses missing/short secrets and
oversized frames. The first frame must be a runtime `Handshake`; no business
request is decoded or dispatched before constant-time token authentication and
compatible-major-version negotiation.

On Unix, bootstrap refuses symlinks, regular files, and live sockets at the
configured endpoint; it removes only a verified stale socket and removes the
socket again after graceful `Ctrl-C` shutdown. Windows named pipes leave no
persistent endpoint entry.

The protocol foundation defines bounded CBOR `WireFrame<T>` values with a common
header containing family, version, frame kind, request ID, stream sequence,
correlation, causation, idempotency, and optional cancellation IDs. Handshakes
negotiate a compatible major version and capability intersection. Frame kinds
cover request/response, streams, cancellation, window updates, heartbeat, and
errors.

Served endpoints include health, style/harness/component discovery, durable
session create/list/inspect/replay/branch, provider turns, cancellation,
approval resolution, and bounded session event catch-up. Runtime protocol 2.3
adds optional memory and compaction selections to session creation plus the
component catalog used by frontends. Receiving service types translate runtime
wire contracts before calling logic; logic reaches external state only through
data and dependency.

Runtime protocol 2.1 negotiates `credit_windows`. One nonterminal stream item is
initially allowed; later items require a `WindowUpdate` bound to the original
request metadata and exact last contiguous stream sequence. Session
subscriptions page verified canonical events strictly after a supplied sequence
and return head/cursor/`has_more` metadata. Windows named-pipe flow and reconnect
are process-tested in `runtime_cli_stream.ps1` and
`runtime_session_reconnect.ps1`; equivalent Unix automation is present.
Continuous live subscription after catch-up, transport-level idempotency
persistence, OS-owner ACL validation, client endpoint discovery, and graceful
shutdown remain incomplete. The bootstrap token is mandatory but does not yet
replace platform ownership/ACL verification.
