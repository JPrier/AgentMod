# Processes and PTYs

AgentMod runs operating-system commands in the isolated process host. All starts, input, resize, interrupt, and kill operations pass through the runtime proposal and permission pipeline before the host verifies an exact short-lived action grant.

Use `process.run` for a foreground command with separate stdout and stderr. Use `process.start` for a long-running non-terminal process.

Use `process.start_pty` for an interactive terminal:

```json
{
  "executable": "cargo",
  "arguments": ["test"],
  "working_directory": null,
  "environment": {},
  "timeout_ms": 600000,
  "output_limit_bytes": 8388608,
  "cleanup": "retain",
  "terminal": {
    "columns": 120,
    "rows": 40,
    "pixel_width": 0,
    "pixel_height": 0
  }
}
```

The result contains the AgentMod process ID, the OS process ID when available, the current terminal dimensions, state, detach marker, truncation flags, and exit information.

Send text with `process.input`, resize with `process.resize`, and read historical terminal bytes with `process.read`:

```json
{"process_id":"<id>","content":"y\r\n","close":false}
```

```json
{"process_id":"<id>","columns":160,"rows":50,"pixel_width":0,"pixel_height":0}
```

```json
{"process_id":"<id>","stream":"terminal","offset":0,"length":65536}
```

PTY output is a combined terminal stream and can contain ANSI or other terminal control sequences. The full retained stream remains in the process log; model-visible projections remain bounded.

`process.detach` releases the frontend attachment state without terminating the child. `process.reattach` restores attachment while the same process host remains active. The process host uses an authenticated local socket or named pipe and survives a runtime-client disconnect while it owns a live child, so a replacement runtime can reconnect and reattach. The host exits after a bounded idle check when it has neither an in-flight request nor a live child. `process.interrupt` requests graceful termination and `process.kill` forces tree termination where supported.

Closing only the input half of a PTY is not portable. A PTY input request with `close: true` fails explicitly; use an application-specific EOF control byte or terminate the process.

After a host restart, completed records and retained output are recovered. AgentMod compares PID, OS start time, and resolved executable before reporting that a prior child still exists; it never repeats an uncertain dispatch. Because inherited streams and PTY handles cannot be recreated by a replacement host, such a child is reported as `recovered_running_unattached` and handle-dependent controls fail closed. Runtime restarts do not require host replacement: the restarted runtime reconnects to the surviving authenticated endpoint and preserves live handles.
