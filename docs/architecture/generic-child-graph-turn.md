# Generic child-graph Turn coordination

`apps/runtime/logic/src/child_graph_turn.rs` is the style-independent durable
coordination seam for `spawn_child_agent`, `wait_for_agents`, and `review`
executors. It dispatches from the exact executor resolution retained in the
immutable execution contract. It does not inspect a style ID, node label,
fixture identity, or inferred topology.

The dependency direction is:

```text
future runtime Turn adapter
    -> ChildGraphTurnCoordinator
        -> child_graph_application (pure canonical event planning)
        -> ChildGraphTurnJournal (logic-owned replay/identity/CAS port)
        -> ChildGraphEffectPort (logic-owned typed effect port)
```

Both ports use runtime-logic types only.
`SessionChildGraphTurnJournal` maps replay, runtime identity allocation, and
atomic append through runtime data and dependency layers.
`RuntimeChildGraphChildSessions` maps creation through the existing
`RuntimeChildSessionLogic` use case and performs restart reconciliation by
reading the authoritative session catalog and replaying the exact child
journal. It never creates from the reconciliation path.

## Recoverable spawn protocol

Each task advances through the existing canonical child projection:

1. `GenericChildCreationProposed`
2. normal policy authorization
3. `GenericChildCreationApproved`
4. `GenericChildCreationDispatched`
5. create once, or reconcile after restart
6. `GenericChildCreated`
7. verified `GenericChildTerminal`

The coordinator reloads replay and calls
`plan_child_graph_application` before canonical phases. Every proposed event
is sealed with runtime identity, accepted by the shared session reducer, and
then appended with an exact-head compare-and-swap. A CAS conflict causes
bounded replay and replanning.

Only a dispatch intent appended by the current invocation may call
`create_after_dispatch`. Any dispatch found in replay calls
`reconcile_creation`; it is never blindly redispatched. A missing or
uncertain receipt returns `Waiting` or `Ambiguous`. Stable task ordering,
`maximum_in_flight`, and `maximum_queued` bound authorization, dispatch, and
creation.

`ContinuationChildGraphAncillaryEffects` persists creation approval through
the existing continuation use case. Its payload binds the exact session,
operation, run/work identity, execution-plan hash, compiled configuration
hash, complete request hash, and action digest. The continuation ID is
deterministically derived from that contract. Pending recovery does not
re-enter policy. An approved continuation is revalidated through the normal
policy/application port with the exact request hash as its idempotency key
before the coordinator can append `GenericChildCreationApproved`. Denied,
expired, and substituted payloads fail closed and cannot dispatch a child.

## Wait, cancellation, and review

Wait and review projections are committed only through the existing pure
application planner, retaining its idempotency and substitution checks. A
failed wait first becomes canonical. Only then may the effect port receive a
cancellation proposal bound to the committed projection hash, immutable
plan/configuration, and exact child set. The continuation-backed ancillary
adapter routes Ask through a deterministic child-graph approval continuation.
Before the continuation CAS can change `Pending` to `Resumed`, runtime logic
revalidates the exact session, run, branch path, node work, persisted executor,
compiled node-configuration hash, adapter-configuration reference, execution
plan, waiting effect receipt, and canonical cancellation record. Duplicate,
reordered, cross-branch, or substituted identities fail closed without
consuming the continuation.

Accepted cancellation then follows the canonical outbox:

```text
requested -> authorized -> dispatched -> completed|ambiguous
```

Only the production child-session boundary performs the exact child lifecycle
transition. Completion retains each child ID and terminal journal head.
Recovery completes from an exact receipt, never fabricates a user message, and
never redispatches a dispatched cancellation whose result is ambiguous.

Review evidence is verified by the effect port before canonical routing. The
returned evidence hash must equal the pure review proposal hash. Waiting,
rejected, substituted, and ambiguous evidence cannot commit a route. A
reviewer wait uses the same exact continuation contract, and approval returns
only the already-bound evidence hash.

## Production adapters and Turn wiring

This seam is executable with injected ports and is covered by real reducer,
CAS, crash-cut, reconciliation, ordering, concurrency, denial, ambiguity,
terminal, substitution, filesystem restart, and real child-catalog tests.
The concrete child adapter currently accepts shared-read-only workspaces and
inline typed task JSON. It fails closed for workspace modes whose enforcement
is not represented by the existing child-session boundary and for non-empty
artifact handoffs. Token, context, and cost ceilings are lowered into the
immutable child binding.

`ProductionChildGraphEffectPort` composes this receipt-authoritative child
adapter with `ContinuationChildGraphAncillaryEffects`. The latter composes
the existing runtime continuation logic with the narrow
`ChildGraphAncillaryApplicationPort`. `TurnLogic` routes policy, creation,
cancellation proposals, reviewer validation, continuation resolution, and
canonical failure mapping through these ports with the supplied idempotency
identity. Neither adapter can fabricate child creation or terminal receipts.

Graph B process tests exercise this composition through the Windows named-pipe
runtime and Ubuntu/WSL2 Unix runtime. The accepted-cancellation variant proves
Ask persistence, daemon replacement, two exact child cancellations, terminal
child lifecycle replay, and no duplicate dispatch or completion after restart.
