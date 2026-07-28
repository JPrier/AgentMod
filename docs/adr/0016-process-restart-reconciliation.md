# ADR 0016: Durable process dispatch and restart reconciliation

- Status: accepted
- Date: 2026-07-28

## Context

A process action can execute successfully even if its capability host or runtime crashes before the terminal event is committed. Repeating that action during recovery can start a duplicate server, test run, migration, or destructive command. Conversely, trusting only a persisted PID is unsafe because operating systems reuse PIDs.

Pipe and PTY handles are inherited kernel resources. A replacement process host cannot recreate arbitrary stdin, stdout, process-group, or PTY handles after the original host has died.

## Decision

The process dependency writes immutable generation-framed recovery records inside each process log directory.

The lifecycle is:

1. allocate an AgentMod process ID and durable log directory;
2. persist a `dispatching` record before calling the OS;
3. spawn once;
4. inspect and persist the OS PID, OS start time, resolved executable, and `running` state before starting the supervisor;
5. persist exit, detach, terminal-size, truncation, and cleanup transitions;
6. on restart, load the latest valid generation and reconcile it without dispatching.

Recovered identity is exact only when all three values match:

- PID;
- OS-reported start time;
- resolved executable path.

Windows verbatim path prefixes are normalized solely for this comparison. A mismatched start time is treated as PID reuse and the record becomes recovered-exited.

Recovery states are:

- `live`: this host owns the inherited handles;
- `recovered_running_unattached`: the exact OS identity still exists but handles cannot be reconstructed;
- `recovered_exited`: the prior child no longer matches a live identity;
- `dispatch_uncertain`: the host died after durable intent and before confirmed dispatch state.

`dispatch_uncertain` is never executed again automatically. `recovered_running_unattached` is visible through list and inspection, but inherited-handle operations fail closed. Completed durable output ranges remain readable.

Malformed generation files are renamed with a corruption suffix and omitted. Owner, session, schema, and process-directory mismatches fail closed.

## Consequences

Recovery no longer relies on PID alone and cannot duplicate an ambiguous dispatch. The runtime can display exact recovery classification and causally decide whether a continuation is safe.

True interactive reattachment across a process-host crash remains impossible and is not simulated by reopening `/proc`, console devices, or guessed PTY paths. ADR 0017 adds a reconnectable authenticated transport so runtime-daemon replacement retains the original capability host and its handles.
