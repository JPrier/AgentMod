# Scheduling

AgentMod's scheduler worker persists intent and occurrence claims independently
from runtime session state. The versioned `scheduler-protocol` is the only
cross-process contract; the scheduler does not import runtime internals.

A schedule cannot omit execution policy. It carries session, workspace, style,
permission policy, provider, model, token budget, cost budget, trigger, payload,
and an idempotency key. The worker authenticates its local runtime peer during
mandatory protocol negotiation.

Time polling, runtime-event delivery, and process-output delivery all converge
on the same durable claim mechanism. The runtime supervises one authenticated
scheduler worker, maps runtime protocol requests through its own four layers,
and exposes schedule create/update, remove, list, claim, completion, and bounded
execution operations.

After a user or frontend turn commits, the service reads the verified canonical
sequence range and submits each event's canonical event ID to the scheduler.
Process output additionally carries the exact process ID, durable source stream,
and byte-range bounds. The resulting range identity suppresses a repeated read
of the same durable bytes, including after restart. Observer failure cannot
roll back or retry the already committed initiating turn.

`agentmod schedule run` claims due prompt work, commits `scheduler.fired` with
the deterministic execution and schedule IDs, and then enters the same
runtime-owned model/tool proposal and mandatory-security pipeline as an
interactive turn. A successful turn is marked terminal only after its canonical
events commit. A turn awaiting durable approval remains nonterminal. Failed
turns receive an idempotent failure marker. Restarting the daemon cannot claim
the same occurrence again.

Before accepting RPC connections, the runtime asks the worker for bounded
nonterminal execution records and compares each record with verified canonical
session history. A claim with no exact `scheduler.fired` event is safe to resume
because that event is committed before provider or tool dispatch. A claim with
a canonical model terminal is reconciled without executing it again. An
approval boundary remains pending. Any fired claim without a recognizable
terminal state fails closed.

Recovery commits `scheduler.delivery_reconciled`, bound to the exact execution
and schedule, before writing the worker terminal marker. Approval recovery also
binds the continuation ID. If the daemon stops between those two writes, the
next startup consumes the canonical reconciliation outcome and only repairs the
worker marker. Conflicting success and failure markers are corruption.

Deferred turns use a separate `DeferredTurn` continuation payload. Creation is
an idempotent two-step logic use case: persist the exact schedule-bound
continuation, then store the schedule. A worker claim carries both the intended
occurrence and its durable claim timestamp. Runtime logic verifies the session,
schedule, trigger proof, and expiration before a pending-to-resumed
compare-and-set. Only the transition winner commits `scheduler.fired` and enters
the normal provider/tool policy path. Manual approval endpoints reject these
continuations, and repeated claims become successful no-ops rather than
re-executing the turn.

The daemon polls automatically with a bounded missed-tick policy; polling can
be tuned with `AGENTMOD_SCHEDULER_POLL_MS` and
`AGENTMOD_SCHEDULER_POLL_LIMIT`. The worker never executes payloads itself.
Triggered schedule executions are deliberately not fed recursively back into
the same observer pass, preventing an event schedule from immediately
self-triggering without an explicit later committed turn. TUI management
remains open.
