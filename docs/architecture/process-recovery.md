# Process recovery

The process host keeps canonical runtime state out of the capability process, but it owns the external-dispatch receipt needed to reconcile OS children safely.

## Durable record

Each retained process directory contains immutable `process-<generation>.json` files next to `stdout.log` and `stderr.log`. A record contains:

- AgentMod process ID;
- owner and session binding;
- redacted requested executable and resolved executable;
- working directory;
- lifecycle state;
- OS PID and OS start time;
- PTY marker and dimensions;
- output bounds and truncation state;
- detach, exit, and cleanup state.

Arguments, environment values, secret values, and full output are not copied into the recovery record.

The dependency writes and synchronizes a new file, renames it into the generation name, synchronizes the directory on Unix, and only then removes older generations. A crash therefore leaves either the prior complete generation or the new complete generation. Malformed JSON is quarantined.

## Reconciliation

At composition, the dependency scans records for the configured owner and session. It never invokes the start operation during this scan.

```text
dispatching  -> dispatch_uncertain
exited       -> recovered_exited
running      -> compare PID + start time + executable
                 | exact match    -> recovered_running_unattached
                 | any mismatch   -> recovered_exited
```

Recovered-running records are refreshed when listed or controlled. If the exact identity disappears, the durable state transitions to recovered-exited.

## Control policy

Live records retain their capability-host handles and support input, resize, wait, interrupt, kill, detach, and reattach.

Recovered-exited and dispatch-uncertain records remain inspectable and are never redispatched.

Recovered-running-unattached records prove that an OS child with the original identity remains, but they do not prove access to its inherited streams or PTY. Handle-dependent control is denied. This fail-closed result is preferable to sending input or signals to a reused or unrelated process.

## Runtime reconnection

The runtime connects to a deterministic endpoint bound to owner, session, and workspace. The process host runs in an independent process group and accepts the versioned tool protocol over an authenticated Unix socket or Windows named pipe. The authentication key is derived from the runtime's protected bootstrap authority, so a legitimate replacement runtime can negotiate with the existing host without persisting a plaintext key in canonical state.

A runtime connection disappearing does not terminate the host. The host retains the inherited process and PTY handles, and a replacement connection can list, attach, read, input, resize, interrupt, or kill through the normal exact-action grants. The host exits only when an idle check observes zero in-flight requests and zero live child handles.

If the process host itself crashes, live handle recreation remains impossible. The durable identity model then reports `recovered_running_unattached` and continues to fail closed.

`tests/e2e/runtime_process_restart.ps1` performs this path through the real
Windows named-pipe daemon boundary. It starts one interactive PTY, forcibly
terminates the runtime, starts a replacement runtime with the same protected
bootstrap authority, reattaches by AgentMod process ID, exchanges input and
terminal output, waits for exit, and proves the process directory was created
only once. `runtime_process_restart.sh` is the equivalent Unix-socket test.
