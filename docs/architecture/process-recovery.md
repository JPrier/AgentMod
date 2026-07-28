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

Cross-runtime live reattachment therefore depends on a surviving, reconnectable process-host transport. The durable identity model is designed to support that next step without changing logic or service semantics.
