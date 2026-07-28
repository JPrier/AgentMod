# ADR 0018: Schedule-bound deferred continuations

## Status

Accepted.

## Context

A scheduler must be able to wake deferred work after runtime restart without
turning a time or event trigger into an implicit user approval. Reusing the
manual tool-approval continuation would let a scheduler bypass the user's
decision and would make retries ambiguous.

## Decision

The runtime persists deferred turns as a distinct continuation payload. The
payload binds the target session, exact schedule ID, prompt, workspace,
provider, model, options, style, and cancellation ID. Its wake condition is one
of an absolute time, an exact canonical event type, or an exact process/output
pattern.

Scheduler executions carry both the intended occurrence timestamp and the
timestamp at which the worker durably claimed it. The runtime service maps the
authenticated worker result into a service-owned wake proof. Runtime logic
checks:

1. the continuation is a deferred turn rather than a tool approval;
2. its session and schedule IDs match;
3. the proof matches the stored trigger;
4. the durable claim timestamp does not exceed expiration; and
5. the continuation is still pending.

The continuation store performs an atomic pending-to-resumed transition.
Exact creation retries and repeated resumed wakeups are idempotent. Conflicting
creation content, mismatched proofs, expired claims, and manual approval
requests fail closed. Only the transition winner executes the existing
scheduled-turn use case, which commits scheduler provenance and re-enters normal
provider and tool interception.

## Consequences

Daemon restart while a continuation is waiting is safe and does not require a
resident task. The CLI can create deferred schedules using the same four-layer
path as prompt schedules. Recurring deferred schedules are rejected because a
continuation is resume-once.

The scheduler still needs a durable redelivery lease/outbox for a process crash
after a claim is created but before runtime records terminal completion. This
ADR does not treat a claimed execution as safe to redispatch without that
additional reconciliation state.
