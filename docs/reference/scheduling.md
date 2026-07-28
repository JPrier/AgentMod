# Scheduling reference

Schedules are durable worker-owned trigger records whose payloads execute only
through the runtime's ordinary interception and mandatory-security pipeline.
Every schedule binds a session, workspace, style, permission policy, provider,
model, token budget, and cost budget.

Supported triggers are:

- `at_millis`: one occurrence at a Unix timestamp in milliseconds.
- `interval`: bounded recurring occurrences from a start timestamp.
- `runtime_event`: an exact canonical event type.
- `process_output`: a literal in one exact process's durable output.

The CLI exposes these as `--at-ms`, `--at-ms` plus `--every-ms`,
`--on-event`, and `--process-id` plus `--contains`. Exactly one trigger form is
required.

The scheduler creates a deterministic execution ID from the schedule and source
occurrence. Claims and terminal markers are checksum protected. Canonical
runtime events use their event IDs as source identities. Process reads use
process ID, source stream, and byte-range bounds, so rereading the same bytes
does not execute a matching schedule again.

The daemon automatically polls time schedules. Event and output triggers are
submitted only after their source events commit. A matched prompt commits
`scheduler.fired` and then runs as a normal agent turn. Failed observer delivery
does not roll back an already committed source turn.

Continuation payloads are persisted but currently fail closed at runtime.
Automatically resolving a manual tool-approval continuation would bypass the
user decision; scheduled wakeups require a separately typed deferred-action
payload and mandatory-policy revalidation.
