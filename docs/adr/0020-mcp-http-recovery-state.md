# ADR 0020: MCP Streamable HTTP recovery state

## Status

Accepted.

## Context

Streamable HTTP can return progress without the selected JSON-RPC result and
requires a later GET carrying `MCP-Session-Id` and `Last-Event-ID`. Keeping
those values only in memory loses the stream when an isolated MCP host stops.
Reusing an unbound cursor for another server, runtime session, or operation can
cross security boundaries or deliver the wrong terminal result.

## Decision

The MCP dependency persists one state record per configured HTTP server. The
checksum-protected, atomically replaced record binds:

- server ID and a hash of its transport configuration;
- runtime owner and session;
- MCP session and last event IDs;
- pending JSON-RPC request ID; and
- normalized operation digest.

On reconstruction, the dependency validates every binding and resumes a
pending stream with GET only when the caller presents the same operation.
Changed server identity, owner/session, corruption, or a different operation
fails closed. HTTP calls for one server are serialized around its cursor.
Repeated delivery of the persisted cursor is suppressed. A terminal result
advances the cursor and clears the pending request durably.

## Consequences

An isolated host can continue an incomplete server stream without issuing a
second POST. Recovery remains entirely in the MCP dependency layer; protocol
and SDK state do not escape upward. The state contains no bearer token, and the
runtime supplies a private per-session directory. Resumption remains bounded
to three GET attempts per invocation.
