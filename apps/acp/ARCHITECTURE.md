# Agent Client Protocol frontend

The ACP adapter is a separate stdio JSON-RPC process using the official
`agent-client-protocol` Rust SDK and stable ACP wire version 1. It imports no
runtime internals and follows `service → logic → data → dependency`.

| Layer | Responsibility and owned types |
|---|---|
| service | ACP initialization/capabilities, JSON-RPC handlers, rich-content and MCP-declaration mapping, update notifications, permission requests, and stdio lifecycle |
| logic | Session identity/workspace invariants, bounded typed prompt construction, MCP-declaration validation, bounded prompt streams, active-turn cancellation state, disconnect cancellation, and approval coordination |
| data | Session/turn datasets, bounded layer-owned streams, and explicit runtime-dependency normalization |
| dependency | Authenticated runtime socket/named-pipe negotiation, bounded layer-owned streams, exact credit windows, cancellation, session creation/loading, turns, and approvals |
| bin | Environment bootstrap and concrete assembly |

ACP sessions are runtime sessions. Prompt and cancellation requests therefore
enter the same runtime interception, event, permission, provider, and tool paths
as CLI and TUI requests. ACP SDK types remain confined to the service crate.
Runtime protocol types remain confined to the dependency crate.

Permission resolution uses the continuation identifier committed by the
runtime stream, rather than trusting a provider-supplied tool identifier.
Approval resumes the action once. Denial and client cancellation durably
resolve the continuation without starting a replacement provider request.
Active cancellation is registered before endpoint response work is spawned, so
an immediate `session/cancel` is latched before provider dispatch. A confirmed
process-host cancellation is normalized to one `Started → Cancelled` tool
terminal sequence and never to successful completion.

Text, resource links, images, audio, and embedded text/blob resources are
accepted through layer-owned records. Rich prompts are projected into a stable
versioned JSON envelope inside the canonical runtime user message so block type,
order, MIME type, URI, and base64 payload survive the provider boundary. The
logic layer rejects malformed base64, mismatched image/audio MIME families,
invalid URIs, more than 64 blocks, a block over its type-specific bound, or a
rendered prompt over 1 MiB before runtime dispatch. ACP metadata and annotations
are intentionally not treated as provider instructions.

Per-session MCP declarations are mapped into ACP-owned stdio/HTTP/SSE records
and validated for count, duplicate identity, absolute stdio executables, field
size, environment/header shape, and secure HTTP endpoints. Session creation
binds the exact declaration hash into the immutable style lock and stores only
redacted transport metadata, secret-value hashes, and an encrypted bootstrap
reference there. The dependency layer encrypts the bounded activation payload
with a runtime-owned key and binds its authenticated data to the session,
declaration hash, and sanitized binding hash. Session load must supply the exact
same declaration set; secret or transport substitution fails closed instead of
silently rebinding. Runtime MCP activation decrypts the payload only at the host
boundary, and HTTP header values cross into the host as environment references.

Current limitations:

- The real Windows and WSL/Linux ACP process matrix proves exact stdio,
  Streamable HTTP, and legacy SSE binding; encrypted-at-rest secret handling;
  exact-load rejection; lazy host activation; actual tool invocation; HTTP
  cursor resumption; and in-flight transport cancellation. The HTTP/SSE proof
  is `tests/e2e/runtime_acp_mcp_http_sse.ps1` on Windows and the matching `.sh`
  script on Linux. Both passed on 2026-07-31.
- `tests/e2e/runtime_acp_mcp_branch.ps1` and the matching `.sh` script prove
  exact branch copying through the real ACP/runtime/CLI/MCP-host process path:
  the child keeps the immutable declaration binding, receives fresh
  session-authenticated encryption, invokes the same external server before
  and after runtime restart, rejects declaration substitution and missing or
  unauthenticated source bootstrap data, persists no plaintext inline secret,
  and performs no MCP effect during pure replay. Both the Windows and WSL/Linux
  commands passed on 2026-07-31.
- `tests/e2e/runtime_child_mcp_inheritance.ps1` and the matching `.sh` script
  prove immediate-parent child-session MCP inheritance through real
  ACP/runtime/harness/MCP-host processes. The style-wide policy must explicitly
  enable `inherit_mcp`; omission or `false` yields an empty child binding, and
  `true` requires the `mcp` tool gate. Creation binds the exact sanitized parent
  declaration, authenticates its bootstrap, rewrites the child session ID, and
  seals fresh nonce, AAD, and ciphertext. The fixture loads the returned child's
  exact immutable `temporary_copy` workspace and prompts with the canonical
  `invoke the inherited MCP fixture` task. Exact recovery, restart, and replay
  retain one MCP effect with no duplicate. Declaration substitution and parent
  or child bootstrap tamper fail closed as `InvalidConfiguration`; ACP closes
  the failed prompt path and the fixture records zero MCP effects. Both Windows
  and Ubuntu/WSL2 commands passed on 2026-07-31. This does not establish
  transitive/grandchild inheritance, and macOS was not run.
- Additional workspace directories are rejected until their runtime activation
  contract is available.
- Stable session listing, deletion, resume/close, and terminal callbacks are not
  advertised.

Provider events cross every ACP layer through capacity-one, layer-owned streams.
The dependency grants the runtime one additional credit only after the current
item enters the bounded dependency stream; the service emits each ordered ACP
notification immediately. Dropped frontend streams cancel the matching runtime
turn. The Windows process E2E uses paced harness frames and proves the first ACP
update is observable before provider completion; the Unix equivalent is
automated for CI. Additional real-process tests cover approval, denial,
approval-wait cancellation, pre-start and mid-stream provider cancellation, and
cancellation during a foreground process tool. `runtime_acp_rich_content` runs
on Windows and WSL/Linux and proves advertised rich capabilities, pre-dispatch
rejection of malformed binary content, canonical preservation of every rich
block type, one provider dispatch, exact per-session stdio MCP invocation,
non-disclosure of the inline secret in the style lock/journal/encrypted payload,
declaration-substitution rejection, and terminal completion.
`runtime_acp_mcp_http_sse` additionally proves distinct ACP HTTP/SSE transport
mapping, exact `MCP-Session-Id`/`MCP-Protocol-Version` propagation,
`Last-Event-ID` resumption, same-origin legacy endpoint advertisement, and one
canonical cancelled terminal without accepting the fixture's late result.
