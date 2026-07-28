# Process Host Architecture

The process host is a separate foreground, background, and PTY capability process. Its crates enforce:

```text
bin → service → logic → data → dependency
```

Each layer owns and explicitly maps identity, authorization, process, terminal-size, control, output, and error types. Only the dependency layer owns `tokio::process::Child` or `portable_pty` handles, invokes operating-system termination commands, resolves executables, and accesses durable logs.

Every execute request is bound to the configured local owner and session. The service constructs deterministic canonical operation bytes and carries the call ID, tool, digest, grant, cancellation ID, owner, and session through every layer. The dependency recomputes the BLAKE3 content digest and verifies the shared short-lived keyed authorization grant. Nonces are consumed atomically and cannot be replayed. The host refuses startup without an authorization key, owner, or session.

Process records and controls retain owner/session scope. Cross-owner reads, input, waits, interrupts, kills, detach/reattach operations, and listings are denied. Cancellation uses the configured identity and opaque cancellation ID because the current cancel wire message has no grant field.

Children start with `env_clear`. Only a small configured inherited allowlist is considered, secret-like names are excluded, and `PATH` overrides are denied. Executables are resolved before spawning. Working directories and log roots are canonicalized beneath configured roots. Input, frames, arguments, environment entries, active processes, retained output, projections, channels, and drain time are bounded.

Foreground completion captures stdout/stderr before cleanup. Cleanup failure is returned as an explicit completed-state flag so callers do not ambiguously retry a successful process. Capture or drain failures are surfaced.

`process.start_pty` and `process.run_pty` allocate a native PTY with explicit rows, columns, and optional cell dimensions. `process.input` writes interactive bytes, `process.resize` updates the kernel or ConPTY terminal size, `process.read` with the `terminal` stream reads the durable combined terminal projection, and normal detach/reattach controls preserve the live host handle. On Windows, the capture path answers the ConPTY cursor-position query before recording the control stream so console programs cannot stall during startup.

Each process log directory contains a generation-framed durable recovery record. The dependency commits `dispatching` before spawn, then records the OS PID, OS start time, resolved executable, and `running` state before the supervisor begins. Exit, detach, terminal size, truncation, and cleanup state are recorded in subsequent immutable generations. Startup scans those generations, quarantines malformed JSON, and classifies each record as recovered-exited, dispatch-uncertain, or recovered-running-unattached. A live identity requires PID, start time, and executable to match, so PID reuse cannot authorize control. Dispatch-uncertain actions are never repeated automatically.

The binary processes bounded JSONL requests concurrently, uses a bounded response channel, routes cancellation IDs, and converts malformed requests and service failures into protocol `Failed` events without exiting.

## Residual limitations

- On Windows, interruption uses documented exact-argument `taskkill /PID <pid> /T`, followed by bounded forced `/T /F` termination when needed. CTRL_BREAK is not used because safe console/process-group APIs are not available without an additional platform abstraction.
- On Unix, each child is placed in a dedicated process group. Interrupt and kill address the group through the platform `kill` utility and fall back to the direct child if group signaling is unavailable.
- PTY `close: true` input is rejected explicitly because the portable PTY API cannot close only the input half without also dropping the controlling terminal.
- A process-host crash cannot recreate inherited stdin, stdout, or PTY handles. An exact surviving OS child is therefore exposed as `recovered_running_unattached`; output recorded before the crash remains readable, but input, wait, resize, interrupt, kill, and reattach fail closed. Keeping the capability host alive across runtime-daemon replacement is the remaining route to true live-handle reattachment.
