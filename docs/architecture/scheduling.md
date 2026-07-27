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

`agentmod schedule run` claims due prompt work, commits `scheduler.fired` with
the deterministic execution and schedule IDs, and then enters the same
runtime-owned model/tool proposal and mandatory-security pipeline as an
interactive turn. A successful turn is marked terminal only after its canonical
events commit. A turn awaiting durable approval remains nonterminal. Failed
turns receive an idempotent failure marker. Restarting the daemon cannot claim
the same occurrence again.

The daemon polls automatically with a bounded missed-tick policy; polling can
be tuned with `AGENTMOD_SCHEDULER_POLL_MS` and
`AGENTMOD_SCHEDULER_POLL_LIMIT`. The worker never executes payloads itself.
Runtime-event/process-output delivery from committed runtime streams,
continuation-wakeup execution, and TUI management remain open.
