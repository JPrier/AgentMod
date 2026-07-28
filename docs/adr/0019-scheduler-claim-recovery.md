# ADR 0019: Scheduler claim recovery and terminal reconciliation

## Status

Accepted.

## Context

The scheduler worker durably claims an occurrence before the runtime executes
it. A daemon can stop after the claim, after canonical dispatch provenance, or
after canonical completion but before the worker receives its terminal marker.
Blindly replaying every pending claim can duplicate provider, tool, or process
effects. Blindly failing every claim loses work that provably never started.

## Decision

The worker exposes a bounded list of checksum-verified, nonterminal execution
records. Runtime startup compares each exact execution and schedule identity
with verified canonical session history before accepting RPC clients.

- No exact `scheduler.fired` event means dispatch never began and the claim may
  enter the ordinary intercepted execution path.
- A canonical provider or session terminal is finalized without redispatch.
- A durable approval boundary remains nonterminal.
- A fired execution with no recognized terminal outcome fails closed.

Before updating the worker terminal marker, runtime logic commits
`scheduler.delivery_reconciled`. The event binds execution ID, schedule ID,
stable outcome, and the continuation ID for an approval boundary. A later
startup treats this event as authoritative, making a crash between canonical
reconciliation and worker completion idempotent.

The worker rejects an execution directory containing both success and failure
markers as corrupt.

## Consequences

Safe pre-dispatch work is not lost, canonical completion is never executed
twice, and ambiguous side effects are never guessed. Recovery remains a
runtime-owned business decision while record enumeration and marker writes stay
inside the scheduler's N-tier process boundary. Operators gain a canonical
audit record for every repaired or fail-closed delivery.
