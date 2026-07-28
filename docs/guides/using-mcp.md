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

Runtime calls still pass through the normal proposal, mandatory permission,
event, artifact, cancellation, and output-limit path. Dependency-side keyed
grants bind the exact server, operation, arguments, cancellation ID, expiry,
and single-use nonce.

The repository's `runtime_mcp_invoke.ps1` and `.sh` acceptance tests compile a
deterministic external stdio server and exercise this complete runtime path,
including progress and result projection into the next provider request.

Current limitations are OAuth authorization-code flow and normalized
resource-template descriptors.
