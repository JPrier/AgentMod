# Tool Hosts

`agentmod-tool-protocol` is the versioned provider-independent wire boundary. It
defines lazy discovery, execute, cancel and health commands plus bounded
progress, output, completion, failure and artifact-bearing events. Runtime
logic never imports a host's internal crates.

The filesystem, process, Web, browser, Git, LSP and MCP capability domains are separate
binaries with Cargo-enforced `service → logic → data → dependency` layers.
The browser host uses an explicitly configured WebDriver endpoint and keeps rendered
session state, screenshots, downloads, cancellation, and driver protocol types behind
its dependency layer.

## Runtime execution path

The filesystem, process, Git, Web, browser, LSP, and MCP binaries are persistent bounded
JSONL endpoints.
For every call:

1. Harness output becomes a typed runtime tool proposal.
2. Runtime commits the immutable proposal.
3. Session-style and plugin interceptors run in order.
4. User policy runs, followed by mandatory runtime policy.
5. Runtime commits the final approved action.
6. Runtime dependency routes by capability to a lazy per-session/workspace
   capability host,
   normalizes the protocol operation, and signs a short-lived
   owner/session/call/action/digest/expiry/nonce grant.
7. The selected service maps wire arguments through its four layers.
8. Filesystem, process, Git, Web, LSP, and MCP dependencies reconstruct the
   canonical operation digest, verify and consume the grant, revalidate their
   security domain, then execute. MCP grants bind expanded operation arguments
   and cancellation identifiers, and their nonces are consumed in durable
   session-scoped replay state.
9. Structured events return to runtime. Runtime commits tool lifecycle events,
   a typed tool-call entry, and a bounded tool-result entry.
10. Runtime sends the replacement structured conversation to the waiting
    harness continuation.

An ambiguous transport failure is not retried. The desynchronized host is
terminated so business logic can make an idempotency-aware retry decision.
Dormant sessions own no host process; the first approved call in each
capability domain starts its host.

## Implemented filesystem operations

- bounded text/line/byte read with hashes and binary/encoding detection;
- stable list, glob and grep with ignore and result bounds;
- atomic create/replace with expected hashes;
- exact multi-replacement edit;
- prevalidated multi-file unified patch with rollback where possible.

Path dependencies enforce canonical workspace roots, symlink containment,
sensitive-file policy and special-device rejection. The real process test
`apps/tools/filesystem/bin/tests/process_protocol.rs` proves keyed authorization
and started/completed framing. `tests/e2e/runtime_tool_loop.ps1` and `.sh` prove
CLI → runtime → harness + filesystem-host continuation.

## Process execution

The runtime routes `process.*` tools through a separately supervised process
host using the same proposal, interception, policy, event, and continuation
path. Dependency grants are bound to the final executable and argument digest.
Executables are denied unless explicitly allowed. The host clears the ambient
environment and selectively inherits a documented set of non-secret
platform/toolchain discovery variables; secret-shaped names remain blocked.

The process host supports foreground, background, and native PTY execution,
bounded durable stdout/stderr or combined terminal logs, interactive input,
PTY resize, historical reads, wait, detach/reattach, interrupt, kill, listing,
cancellation, timeouts, and cleanup policies. PTY SDK types remain in
dependency; every upper layer owns and explicitly maps terminal dimensions.
`tests/e2e/runtime_process_loop.ps1` and `.sh` prove the live runtime route.
`tests/e2e/coding_task.ps1` and `.sh` prove a model-driven
read/edit/failing-test/fix/passing-test loop through both isolated hosts.

## Extended host routing

Git status, offline Web search, LSP project-root detection, MCP server listing,
and configured external MCP stdio calls use the same proposal, policy, event,
artifact, and continuation path.
The Windows process E2Es are `runtime_git_loop.ps1`,
`runtime_web_loop.ps1`, `runtime_lsp_loop.ps1`, and
`runtime_mcp_loop.ps1` plus `runtime_mcp_invoke.ps1`; matching Unix scripts are
present. The invocation fixture emits a real MCP progress notification and
terminal tool result, both of which are committed before provider continuation.

Interactive browser authentication handoff, runtime-wide tool discovery, active
cross-host cancellation, MCP OAuth, and broader reconnect/recovery acceptance
tests remain open. MCP HTTP recovery is covered by a real loopback server test
which reconstructs the dependency and verifies exact session, operation, and
event-cursor binding.
