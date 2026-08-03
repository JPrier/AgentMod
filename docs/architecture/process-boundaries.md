# Process Boundaries

## Implemented executable boundaries

The runtime, native harness, headless CLI, terminal frontend, ACP adapter,
plugin host, and filesystem, process,
Web, Git, LSP, and MCP capability hosts are separate binaries with independently
enforced N-tier crates. Executables share protocol crates and core primitives,
not one another's internal layers.

The production CLI path uses an authenticated Unix-domain socket or Windows
named pipe. The runtime lazily starts the per-session selected harness adapter
over bounded JSONL stdio, keeps its child alive across requests, kills it on
desynchronization, and never automatically retries an ambiguous provider
exchange. Runtime and harness negotiate provider behavior through
`agentmod-harness-protocol`. The composition root registers the native adapter,
an independent deterministic fixture adapter, and the independent
`agentmod-harness-fixture` adapter (registry ID `independent`); none of these
adapters is imported as a runtime internal.

Harness process launch uses a runtime-generated 256-bit key passed only through
the child environment. Each approved model action receives a short-lived,
nonce-bearing keyed grant. The harness dependency layer validates its signature,
expiry, and replay state before provider execution. The runtime commits the
original proposal and final approved action before dispatch.

The real process path is automated by:

- `tests/e2e/runtime_harness.ps1` and `.sh`;
- `tests/e2e/runtime_harness_selection.ps1` and `.sh`;
- `tests/e2e/runtime_cli.ps1` and `.sh`.

The latter creates a durable session through the frontend protocol, executes a
turn through the separate harness, verifies visible output, and checks the exact
canonical event order in the journal.

## Remaining boundary work

The runtime supervises selected harness adapters and first-party capability hosts.
Harness events traverse independent bounded frames with explicit credit
windows. Active cancellation covers provider startup, provider streaming,
approval waits, and foreground process-tool execution; exact process
cancellation is routed through an independent authenticated host connection.
Crash-injection coverage must still expand across every host category.

The ACP adapter is an independent five-crate process using the official stable
v1 Rust SDK over stdio; it maps session create/load, prompts, updates,
permission requests, and cancellation into normal runtime RPC. The TUI is an independent five-crate process that
uses only the runtime protocol, pages canonical history, consumes committed
incremental streams with credit-window acknowledgements, and sends approval or
cancellation through normal runtime endpoints. Browser isolation is implemented as a
managed WebDriver capability host. A durable scheduler worker owns schedule
storage and occurrence claims; the runtime supervises it, polls with a bounded
missed-tick policy, and executes claimed prompt work through the canonical
intercepted turn path. Runtime-event and process-output delivery are live: the
daemon observes newly committed canonical event ranges and supervised
process/log-stream byte ranges, carries the exact source session and observation
ID through the runtime/scheduler N-tier boundary, filters schedule ownership
before claim, and commits canonical delivery provenance before terminal worker
acknowledgement. Restart reuses that identity and deduplicates an existing
receipt rather than redispatching the effect. Windows and Ubuntu/WSL2 scheduler
process matrices cover time, runtime-event, process-output, deferred-turn, and
restart-reconciliation paths. Broader crash-injection coverage across every
host category and macOS process evidence remain open.
ACP rich-content projection is live and process-tested on Windows and WSL/Linux:
the official SDK types stop at the service boundary, bounded layer-owned blocks
are validated in logic, and a versioned typed projection enters the normal
canonical runtime turn. Per-session ACP MCP activation crosses the same explicit
service → logic → data → dependency boundaries. The immutable session binding
retains the exact declaration hash and sanitized server identities while an
authenticated encrypted bootstrap, bound to that session and binding hash,
holds the activation-only environment/header values. Exact declarations are
required again on load; substitution fails closed. Windows and WSL/Linux process
tests prove lazy stdio-host activation and a real tool invocation without the
inline secret appearing in the style lock, journal, or encrypted payload.
ACP-declared Streamable HTTP/legacy SSE and branch encrypted-bootstrap copying
also have real Windows and WSL/Linux process evidence. The branch path retains
the exact declaration under fresh child-session encryption, invokes before and
after runtime restart, rejects missing/substituted/unauthenticated source data,
and performs no replay effect.

Runtime-managed child sessions cross the same boundary only when their parent
style explicitly enables `child_agents.inherit_mcp` and both sides retain the
`mcp` tool gate. The dependency authenticates the immediate parent's exact
sanitized binding and bootstrap, rewrites the payload to the child session ID,
and seals fresh nonce, AAD, and ciphertext for the child. Omitted/false policy
binds an empty MCP configuration. Creation and recovery reject declaration,
origin, workspace, or binding substitution; parent- and child-envelope tamper
reaches `InvalidConfiguration` and closes the ACP path before host invocation.
Runtime and dependency workspace-authorization digests recursively canonicalize
nested JSON so the MCP call is bound identically at both layers.

The Windows and Ubuntu/WSL2 child process matrix uses the exact immutable
`temporary_copy` child workspace and canonical task `invoke the inherited MCP
fixture`. It proves one MCP effect across execution, daemon restart, exact
recovery, and replay, with no duplicate effect. This evidence covers only
immediate children; transitive/grandchild inheritance and macOS execution remain
open.

TUI rich attachments traverse only the frontend dependency → data → logic →
service boundary and the ordinary runtime turn stream. Windows and WSL/Linux
process tests prove confined image/blob loading, the versioned rich envelope,
transient-state clearing, restart, and pure replay. TUI LSP management remains
unsupported until the runtime exposes a stable canonical management contract.

Tool hosts return structured results only. The runtime alone decides which
canonical events are committed. The harness owns provider execution but not
runtime policy or history. Frontends submit runtime requests and never execute
actions directly.
