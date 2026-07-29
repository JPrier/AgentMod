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
`agentmod-harness-protocol`. The composition root registers the native adapter
and an independent deterministic fixture adapter; neither adapter is imported
as a runtime internal.

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
intercepted turn path. Event/output trigger delivery remains open.
ACP rich-content projection and per-session MCP activation remain open.

Tool hosts return structured results only. The runtime alone decides which
canonical events are committed. The harness owns provider execution but not
runtime policy or history. Frontends submit runtime requests and never execute
actions directly.
