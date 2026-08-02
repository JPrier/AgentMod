# MCP capability host

The MCP host is a distinct tool-protocol process with enforced
`service → logic → data → dependency` boundaries. It normalizes MCP tools,
resources, prompts, progress, cancellation, health, and transport errors into
AgentMod-owned records; MCP JSON-RPC and transport details remain in dependency.

| Layer | Responsibility |
|---|---|
| service | Lazy tool-group discovery, hidden authenticated OAuth management, invocation endpoints, namespaced tool parsing, bounded projections |
| logic | Server/name validation, invocation-kind policy, OAuth transaction validation, cancellation semantics |
| data | Capability dataset assembly, OAuth result normalization, and `mcp__server__tool` namespacing |
| dependency | MCP initialization, version negotiation, stdio framing, Streamable HTTP, legacy SSE, OAuth discovery/PKCE/token lifecycle, session IDs, secret references, progress, reconnection boundary, cancellation, shutdown |
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
ID, negotiated protocol version, and normalized operation digest. Every
post-negotiation POST and resumed GET sends `MCP-Protocol-Version`. A new host
instance resumes only an exact
pending operation with GET; a different operation or changed server identity
fails closed. The previous cursor is suppressed if a server replays it, and a
terminal result clears the pending request before the next operation.

Legacy SSE is a distinct transport rather than a Streamable HTTP alias. The
dependency opens the declared SSE GET, accepts only one same-origin advertised
POST endpoint, and sends JSON-RPC commands there while consuming bounded
progress and terminal frames from the original stream. Session, negotiated
protocol, and reconnect cursor headers are preserved. Dropping a cancelled
request closes the in-flight response, and the runtime commits one cancelled
tool terminal instead of accepting a late server result.

OAuth Streamable HTTP follows the MCP 2025-11-25 authorization profile. The
dependency discovers RFC 9728 protected-resource metadata and RFC 8414/OIDC
authorization-server metadata, requires authorization code plus PKCE S256, and
sends the exact protected-resource URI during authorization, code exchange, and
refresh. Redirects stay disabled for metadata, token, and MCP requests. Only an
explicit loopback HTTP redirect URI is accepted; the dependency owns that
listener, binds it before returning the authorization URL, and reconstructs it
after a host restart.

OAuth management is absent from ordinary tool discovery and model tool
dispatch. The authenticated CLI/runtime management route exposes begin,
redacted status, and exact-transaction cancellation only; there is no protocol
operation carrying an authorization code. Runtime history receives a canonical
audit containing request, configuration, and redacted-result hashes, never the
authorization URL, state, code verifier, code, access token, or refresh token.
PKCE and token material is stored as separately encrypted secret references
using an operator-supplied stable `AGENTMOD_MCP_OAUTH_KEY`. An exchange or
refresh that was dispatched without a terminal receipt becomes failed and is
not retried automatically.

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
`tests/e2e/runtime_acp_mcp_http_sse.ps1` and `.sh` use real ACP, runtime,
harness, and MCP-host processes with deterministic loopback servers. They prove
Streamable HTTP resumption, legacy SSE endpoint/POST messaging, immutable
per-session header binding, secret non-persistence, and in-flight cancellation.
The Windows and WSL/Linux commands both passed on 2026-07-31.
`tests/e2e/runtime_acp_mcp_branch.ps1` and `.sh` additionally branch an
ACP-created stdio-bound session through the real CLI/runtime path. They verify
the exact immutable binding, fresh session-authenticated bootstrap encryption,
real server invocation before and after restart, fail-closed missing/tampered
source bootstrap handling, plaintext-secret non-persistence, and pure replay
with no additional MCP call. Both platform commands passed on 2026-07-31.
`tests/e2e/runtime_child_mcp_inheritance.ps1` and `.sh` cover runtime-managed
immediate children. Explicit style-wide `inherit_mcp = true` plus the `mcp` tool
gate copies the exact sanitized parent binding; default/false produces no child
MCP binding. The dependency authenticates the parent envelope, rewrites the
session ID, and seals fresh child nonce/AAD/ciphertext. The temporary-copy child
executes the canonical inherited-MCP task once; exact recovery, daemon restart,
and replay do not duplicate the effect. Declaration substitution and parent or
child envelope tamper fail closed as `InvalidConfiguration` before invocation.
Both Windows and Ubuntu/WSL2 commands passed on 2026-07-31.

Current limitations:

- Resource templates and prompt argument schemas are preserved only in raw provider
  results, not yet exposed as dedicated normalized descriptors.
- Transitive/grandchild MCP inheritance and macOS execution of the ACP MCP
  process matrix do not yet have equivalent evidence; the passing child matrix
  proves immediate-parent inheritance only.
