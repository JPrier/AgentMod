# Process Boundaries

## Implemented executable boundaries

The runtime, native harness, headless CLI, terminal frontend, ACP adapter,
plugin host, and filesystem, process,
Web, Git, LSP, and MCP capability hosts are separate binaries with independently
enforced N-tier crates. Executables share protocol crates and core primitives,
not one another's internal layers.

The production CLI path uses an authenticated Unix-domain socket or Windows
named pipe. The runtime lazily starts the native harness over bounded JSONL
stdio, keeps the child alive across requests, kills it on desynchronization, and
never automatically retries an ambiguous provider exchange. Runtime and harness
negotiate provider behavior through `agentmod-harness-protocol`.

Harness process launch uses a runtime-generated 256-bit key passed only through
the child environment. Each approved model action receives a short-lived,
nonce-bearing keyed grant. The harness dependency layer validates its signature,
expiry, and replay state before provider execution. The runtime commits the
original proposal and final approved action before dispatch.

The real process path is automated by:

- `tests/e2e/runtime_harness.ps1` and `.sh`;
- `tests/e2e/runtime_cli.ps1` and `.sh`.

The latter creates a durable session through the frontend protocol, executes a
turn through the separate harness, verifies visible output, and checks the exact
canonical event order in the journal.

## Remaining boundary work

The runtime does not yet supervise every native tool host, plugin host, MCP
server, or LSP server from the canonical agent loop. Harness output is returned
as a bounded command reply rather than independent streaming frames, so active
mid-stream cancellation and transport backpressure remain incomplete.

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
Crash-injection coverage must expand
to harness/tool/plugin/frontend termination and recovery.

Tool hosts return structured results only. The runtime alone decides which
canonical events are committed. The harness owns provider execution but not
runtime policy or history. Frontends submit runtime requests and never execute
actions directly.
