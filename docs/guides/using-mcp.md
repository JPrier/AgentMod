# Using MCP

MCP ships as a separate capability host. Configure only explicitly approved
servers through `AGENTMOD_MCP_SERVERS_JSON`; catalog entries are inert and
disabled by default. A stdio server uses an exact executable and argument vector:

```json
[{
  "id": "docs",
  "display_name": "Documentation",
  "active": true,
  "transport": "stdio",
  "program": "/approved/path/docs-mcp",
  "arguments": ["serve"],
  "environment": {}
}]
```

An ACP client may instead supply `mcpServers` during `session/new`. The runtime
hashes that exact declaration set into the immutable session binding and writes
only sanitized server metadata, secret-value hashes, and an encrypted bootstrap
reference to the style lock. The bounded activation payload is authenticated to
the session and binding hash. `session/load` must repeat the exact declaration;
changing a command, argument, environment/header value, or transport is rejected
instead of selecting a replacement. The runtime decrypts activation data only
when it lazily starts the per-session MCP host. Inline environment/header values
are not written to canonical events or ordinary logs.

A remote server uses Streamable HTTP:

```json
[{
  "id": "issues",
  "display_name": "Issue tracker",
  "active": true,
  "transport": "streamable_http",
  "url": "https://mcp.example.com/v1",
  "bearer_token_environment": "AGENTMOD_ISSUES_MCP_TOKEN"
}]
```

Only the environment-variable name is configured; the secret value is not
written to canonical events or ordinary logs. HTTPS is mandatory except for
loopback development endpoints. Redirects are disabled. JSON and bounded
multi-event SSE responses are supported, including progress and up to three
session/cursor-bound resumptions using `MCP-Session-Id` and `Last-Event-ID`.
After initialization, POST and resumed GET requests also carry the negotiated
`MCP-Protocol-Version`.
The runtime assigns each session a private MCP HTTP state directory. Pending
streams survive host reconstruction and resume only when server identity,
runtime owner/session, and normalized operation all match. A different
operation cannot consume the persisted cursor.

Legacy SSE uses a persistent event stream plus a server-advertised POST
endpoint:

```json
[{
  "id": "legacy-issues",
  "display_name": "Legacy issue tracker",
  "active": true,
  "transport": "legacy_sse",
  "url": "https://mcp.example.com/events",
  "header_environments": {
    "Authorization": "AGENTMOD_ISSUES_MCP_AUTHORIZATION"
  }
}]
```

The advertised endpoint must resolve to the same scheme, host, and port as the
configured event URL. Session and protocol headers are sent on later requests,
and a durable cursor is sent as `Last-Event-ID` when reconnecting. ACP clients
declare this transport with an official `mcpServers` entry whose `type` is
`sse`; inline ACP header values remain encrypted in the session bootstrap and
are injected into the isolated host as environment references.

For an OAuth-protected Streamable HTTP server, configure the exact protected
resource, authorization-server issuer, registered client ID, and a fixed
loopback redirect URI:

```json
[{
  "id": "issues",
  "display_name": "Issue tracker",
  "active": true,
  "transport": "streamable_http_oauth",
  "url": "https://mcp.example.com/v1",
  "authorization_server": "https://login.example.com",
  "client_id": "registered-agentmod-client",
  "client_secret_environment": null,
  "redirect_uri": "http://127.0.0.1:49152/callback",
  "scopes": ["issues.read"]
}]
```

Set `AGENTMOD_MCP_OAUTH_KEY` to a stable 32-byte key rendered as 64 lowercase
hex characters. It encrypts durable PKCE/token secret references and must
remain stable across runtime restarts. Begin and inspect authorization with:

```sh
agentmod mcp oauth begin issues --session <session-id> --json
agentmod mcp oauth status issues --session <session-id> --json
agentmod mcp oauth cancel issues <transaction-id> --session <session-id> --json
```

Open the transient `authorization_url` returned by `begin`. The MCP dependency
owns the registered loopback callback and performs code exchange; neither the
CLI nor runtime protocol accepts an authorization code. These management
operations are hidden from model tool discovery. Canonical history stores only
request, configuration, URL, and redacted-result hashes. Discovery, exchange,
refresh, encrypted secret references, and callback reconstruction fail closed
when issuer/resource/configuration identity changes or an external exchange has
an ambiguous outcome.

Runtime calls still pass through the normal proposal, mandatory permission,
event, artifact, cancellation, and output-limit path. Dependency-side keyed
grants bind the exact server, operation, arguments, cancellation ID, expiry,
and single-use nonce.

The repository's `runtime_mcp_invoke.ps1` and `.sh` acceptance tests compile a
deterministic external stdio server and exercise this complete runtime path,
including progress and result projection into the next provider request.
The `runtime_acp_mcp_http_sse.ps1` and `.sh` process tests exercise ACP-declared
Streamable HTTP and legacy SSE, exact immutable reload, cursor/session/protocol
headers, cancellation, canonical journal outcomes, and recursive secret
non-persistence. Both the Windows and WSL/Linux commands passed on 2026-07-31.
The `runtime_acp_mcp_branch.ps1` and `.sh` process tests branch an ACP-created
MCP-bound parent through the real CLI/runtime path. They prove exact immutable
declaration inheritance with fresh child-session encryption, invocation of the
same external server before and after a runtime restart, rejection of
substituted declarations and missing or unauthenticated source bootstrap data,
plaintext-secret non-persistence, and pure replay without a new server effect.
Both platform commands passed on 2026-07-31.

Runtime-managed child sessions use a separate, immediate-parent contract. The
parent style must set `child_agents.inherit_mcp = true`; omission or `false`
produces an empty child MCP binding. The runtime copies the parent's exact
sanitized binding only when both the parent binding and child grant allow the
`mcp` tool group. It authenticates the parent bootstrap, rewrites its embedded
session ID to the child ID, and emits fresh nonce, AAD, and ciphertext bound to
the child and exact declaration/binding hashes. Exact recovery reuses the one
already-created child only when its origin, style, workspace lease, tool grant,
and MCP binding still match.

Missing or substituted declarations and tampered parent or child bootstrap
envelopes fail closed as `InvalidConfiguration` before an MCP effect. Workspace
authorization hashes recursively canonicalize nested JSON arguments, so
equivalent `mcp.invoke` objects have one digest regardless of object insertion
order. The Windows and Ubuntu/WSL2 `runtime_child_mcp_inheritance.ps1`/`.sh`
process tests use the child state's exact immutable `temporary_copy` workspace
and the canonical task `invoke the inherited MCP fixture`. They prove the true,
omitted/default, and false paths, one successful MCP effect, no duplicate on
restart or replay, declaration/bootstrap tamper rejection with zero effects,
and exact child recovery. Both commands passed on 2026-07-31.

The remaining MCP descriptor limitation is normalized resource-template
metadata. ACP-created stdio, Streamable HTTP, and legacy SSE activation are
process-tested on Windows and WSL/Linux, including exact branch-bootstrap
inheritance and restart. Immediate-parent child-session inheritance is also
process-tested on Windows and Ubuntu/WSL2; transitive/grandchild inheritance and
macOS process execution are not established.
