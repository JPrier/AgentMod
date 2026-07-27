# Process Host Architecture

The process host is a separate non-PTY capability process. Its crates enforce:

```text
bin → service → logic → data → dependency
```

Each layer owns and explicitly maps identity, authorization, process, control, output, and error types. Only the dependency layer owns `tokio::process::Child`, invokes operating-system termination commands, resolves executables, and accesses durable logs.

Every execute request is bound to the configured local owner and session. The service constructs deterministic canonical operation bytes and carries the call ID, tool, digest, grant, cancellation ID, owner, and session through every layer. The dependency recomputes the BLAKE3 content digest and verifies the shared short-lived keyed authorization grant. Nonces are consumed atomically and cannot be replayed. The host refuses startup without an authorization key, owner, or session.

Process records and controls retain owner/session scope. Cross-owner reads, input, waits, interrupts, kills, detach/reattach operations, and listings are denied. Cancellation uses the configured identity and opaque cancellation ID because the current cancel wire message has no grant field.

Children start with `env_clear`. Only a small configured inherited allowlist is considered, secret-like names are excluded, and `PATH` overrides are denied. Executables are resolved before spawning. Working directories and log roots are canonicalized beneath configured roots. Input, frames, arguments, environment entries, active processes, retained output, projections, channels, and drain time are bounded.

Foreground completion captures stdout/stderr before cleanup. Cleanup failure is returned as an explicit completed-state flag so callers do not ambiguously retry a successful process. Capture or drain failures are surfaced.

The binary processes bounded JSONL requests concurrently, uses a bounded response channel, routes cancellation IDs, and converts malformed requests and service failures into protocol `Failed` events without exiting.

## Residual limitations

- PTY execution is not implemented or advertised.
- On Windows, interruption uses documented exact-argument `taskkill /PID <pid> /T`, followed by bounded forced `/T /F` termination when needed. CTRL_BREAK is not used because safe console/process-group APIs are not available without an additional platform abstraction.
- On Unix, each child is placed in a dedicated process group. Interrupt and kill address the group through the platform `kill` utility and fall back to the direct child if group signaling is unavailable.
- Durable restart reattachment is unsupported. `kill_on_drop` covers orderly host shutdown, but an abrupt host crash can leave an operating-system child, particularly on Unix. PID-only recovery was intentionally avoided because PID reuse makes it unsafe.
