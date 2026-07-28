# MCP capability host

The MCP host is a distinct tool-protocol process with enforced
`service → logic → data → dependency` boundaries. It normalizes MCP tools,
resources, prompts, progress, cancellation, health, and transport errors into
AgentMod-owned records; MCP JSON-RPC and transport details remain in dependency.

| Layer | Responsibility |
|---|---|
| service | Lazy tool-group discovery, management/invocation endpoints, namespaced tool parsing, bounded projections |
| logic | Server/name validation, invocation-kind policy, cancellation semantics |
| data | Capability dataset assembly and `mcp__server__tool` namespacing |
| dependency | MCP initialization, version negotiation, stdio framing, Streamable HTTP, session IDs, bearer secret references, progress, reconnection boundary, shutdown |
| bin | Environment bootstrap, bounded JSONL transport, composition and graceful child shutdown |

Stdio uses exact executable/argument vectors without a shell, clears the inherited
environment, rejects secret-like literal environment entries, bounds all inbound
messages, and kills children on drop/shutdown. Streamable HTTP requires HTTPS except
for an explicit loopback endpoint, disables redirects, bounds responses, supports MCP
session IDs, and accepts only an environment variable name as the bearer-token
reference. Multi-event SSE is parsed deterministically. A progress-only response with
an event ID triggers at most three GET resumptions carrying the exact
`MCP-Session-Id` and `Last-Event-ID`; progress is retained and only the selected
JSON-RPC request's terminal result is accepted.

Streamable HTTP recovery state is stored as an atomically replaced,
checksum-protected dependency record. It binds the configured server identity,
runtime owner/session, MCP session ID, last event ID, pending JSON-RPC request
ID, and normalized operation digest. A new host instance resumes only an exact
pending operation with GET; a different operation or changed server identity
fails closed. The previous cursor is suppressed if a server replays it, and a
terminal result clears the pending request before the next operation.

`apps/tools/mcp/catalog/catalog.json` is an inert curated catalog. Every entry is
disabled by default and requires explicit installation and activation; the host never
downloads or executes catalog entries automatically.

Tests include deterministic mock discovery/calls, a real compiled stdio MCP fixture
covering initialization, protocol negotiation, tool discovery, progress, invocation,
and shutdown, and a real loopback HTTP fixture which verifies session/cursor-bound
SSE resumption, including destruction and reconstruction of the dependency
between progress and terminal delivery. `tests/e2e/runtime_mcp_invoke.ps1`
compiles a standalone external
stdio server and proves a configured call through the CLI, runtime policy, host,
canonical progress/result events, provider continuation, and durable authorization
replay state; the matching Unix script exercises the same topology.

Current limitations:

- OAuth authorization-code flows are pending. Bearer credentials are supported only
  through environment-backed secret references.
- Resource templates and prompt argument schemas are preserved only in raw provider
  results, not yet exposed as dedicated normalized descriptors.
