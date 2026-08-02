# Scheduler worker

The scheduler is a separate process with enforced
`service → logic → data → dependency` calls and a dedicated versioned wire
protocol. It stores durable execution intent; it does not execute model, tool,
process, or continuation side effects itself.

| Layer | Responsibility and owned types |
|---|---|
| service | Protocol negotiation, local bootstrap authentication, wire validation, and service/logic mapping |
| logic | Schedule, trigger, budget, identifier, bounded-output, and recurrence policy |
| data | Business schedule datasets, dependency selection, and normalized error records |
| dependency | Checksum-protected files, exclusive locking, atomic schedule writes, system clock, occurrence claims, and terminal markers |
| bin | Configuration bootstrap, concrete assembly, bounded JSONL framing, and flushing |

The protocol includes complete workspace, style, permission-policy, provider,
model, token-budget, and cost-budget fields. Triggers cover one-time timestamps,
bounded recurring intervals, canonical runtime events, and process-output
literals. Payloads cover background prompts and deferred continuations.

Every occurrence receives a deterministic execution ID. Runtime-event and
process-output commands carry the committing runtime session through every
layer, and owner filtering occurs before a durable claim. Claims retain the
exact event or output observation ID, are written with `create_new`, and are
synced before return, so a restart or repeated observation cannot return the
same execution again. Completion is a separate immutable
success/failure marker and is idempotent for the same outcome. Opposite terminal
outcomes conflict. Schedule records have checksums; corrupt records fail closed.

`tests/e2e/scheduler_worker.ps1` proves authenticated negotiation, idempotent
upsert, listing, one-time and runtime-event claims, restart deduplication, and
completion exactly once against the real worker binary. A matching Unix script
is present.

Current limitations:

- Runtime RPC and CLI schedule management plus automatic prompt execution are
  wired; the TUI does not yet expose a schedule management panel.
- Recurrence is fixed interval rather than cron/calendar syntax.
- Committed runtime-event and process-output delivery, and deferred
  continuation wakeups, must still be connected by the runtime.
