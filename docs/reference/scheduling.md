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

Add `--deferred` to persist a resume-once turn continuation rather than a plain
prompt payload. `--expires-at-ms` is optional and is valid only with
`--deferred`. Deferred schedules cannot be recurring because their continuation
may transition only once.

```sh
agentmod schedule add wait-for-build \
  --session <session-id> \
  --prompt "inspect the completed build" \
  --process-id <process-id> \
  --contains "BUILD READY" \
  --deferred \
  --expires-at-ms 1893456000000
```

The scheduler creates a deterministic execution ID from the schedule and source
occurrence. Claims and terminal markers are checksum protected. Canonical
runtime events use their event IDs as source identities. Process reads use
process ID, source stream, and byte-range bounds, so rereading the same bytes
does not execute a matching schedule again.

The daemon automatically polls time schedules. Event and output triggers are
submitted only after their source events commit. A matched prompt commits
`scheduler.fired` and then runs as a normal agent turn. Failed observer delivery
does not roll back an already committed source turn.

Deferred continuation payloads retain the exact session, schedule, prompt,
workspace, provider, model, style, options, and cancellation identity. The
runtime checks exact trigger proof and the scheduler's durable claim timestamp
against expiration, transitions pending state once, then re-enters normal
mandatory-policy evaluation. Manual tool approvals and scheduler continuations
are distinct types and cannot resolve through each other's endpoints.

At daemon startup, pending worker claims are reconciled against canonical
history. Work with no committed `scheduler.fired` provenance resumes through
the normal intercepted path. Canonically completed work is not redispatched;
the runtime commits `scheduler.delivery_reconciled` and repairs the worker's
terminal marker. Ambiguous work that may already have crossed an external
side-effect boundary fails closed. Operators can inspect the reconciliation
event by execution and schedule ID in the session timeline.
