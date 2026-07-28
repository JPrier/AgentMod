# ADR 0015: Native PTY execution through the process dependency layer

- Status: accepted
- Date: 2026-07-28

## Context

AgentMod already supervised non-PTY children in a dedicated process host. Daily-driver terminal workflows also require an interactive terminal, resize notifications, terminal control sequences, durable output, detach/reattach, and equivalent Windows and Unix behavior. Implementing platform FFI in AgentMod would conflict with the workspace-wide prohibition on unsafe application code and would duplicate mature ConPTY and Unix PTY handling.

## Decision

The process dependency crate uses `portable-pty` 0.9 for native ConPTY and Unix PTY handles. No portable-PTY type crosses the dependency boundary. Each upper layer owns and explicitly maps its terminal-size, start, resize, stream, result, and error types.

AgentMod exposes:

- `process.start_pty` for a long-running terminal child;
- `process.run_pty` for a foreground terminal child;
- `process.input` for interactive bytes;
- `process.resize` for rows, columns, and optional cell dimensions;
- `process.read` with the `terminal` stream for bounded historical output;
- the existing wait, detach, reattach, interrupt, kill, list, and cancellation controls.

The same exact-action grant, owner/session binding, nonce consumption, executable policy, workspace containment, environment filtering, resource limits, and canonical-operation digest checks apply to PTY and pipe execution.

PTY stdout and stderr are necessarily one terminal stream. It is stored in the existing stdout log and projected as `terminal`; the stderr log remains empty. Terminal input half-close is rejected explicitly because the portable abstraction cannot close only input without dropping the controlling terminal.

On Windows, ConPTY emits a device-status cursor-position query during initialization. The dependency capture path replies with a bounded cursor-position report before continuing capture. Without this terminal-host responsibility, console applications can stall before user code starts.

## Consequences

Interactive programs now run through the same isolated process host and permission pipeline as other tools. The runtime and frontends can render raw terminal control sequences and resize a live PTY without importing platform SDKs.

Restart reconciliation remains a separate concern. A live in-memory PTY can detach and reattach while its capability host remains active; safe post-crash classification requires durable process identity stronger than a PID and is addressed independently.
