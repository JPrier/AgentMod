# Generic Node Dispatch

A compiled session-style graph executes because every node has an available
resolved executor identity, not because the complete topology matches one of a
small set of built-in adapter profiles. This document describes the generic
node-dispatch engine owned by runtime logic.

## Dispatch model

```text
current node + persisted resolved executor identity
        -> generic executor dispatch
        -> typed node outcome
        -> runtime validates outcome
        -> canonical node/variable/transition events
```

The runtime registry resolves every compiled node to an exact executor at
session-creation time (`apps/runtime/logic/src/node_executor.rs`). The generic
engine (`apps/runtime/logic/src/node_execution/`) consumes those identities and
never consults the style ID, adapter kind, fixture name, node/edge counts, node
sequence, or a hard-coded tool name.

## Contracts

- `ExecuteNodeCommand` — compiled node cursor, exact `NodeExecutorIdentity`,
  bounded `NodeExecutionInput`, attempt/loop/step counters and the effective
  step bound.
- `NodeExecutionInput` — canonical graph variables (Task 4 seam), result and
  artifact references, and durable continuation/child/parallel/scheduler
  evidence slots.
- `NodeExecutionOutcome` — the eight typed outcome classes: completed with
  bounded output, waiting on a durable continuation, waiting on child
  sessions, waiting on parallel branches, retry with a structured reason,
  failed with a structured failure, terminal turn completion, terminal
  session completion.
- `NodeExecutorPort` — the runtime-logic seam node executors implement. It
  never mutates canonical state and never fabricates external-effect
  completion; waiting outcomes must reference durable evidence.
- `dispatch_node` — validates the command, requires the exact identity, calls
  the port, and validates the outcome against the compiled node kind and
  bounds.

## Outcome validation

`node_execution::outcome` maps outcome classes to legal compiled node kinds:

| Outcome class | Legal node kinds |
|---|---|
| `Completed` | every non-terminal kind |
| `WaitingOnContinuation` | tool gate, user approval, schedule |
| `WaitingOnChildren` | spawn, send-message, wait, join |
| `WaitingOnParallelBranches` | parallel branch |
| `Retry` | model, tool gate, approval, review, spawn, persist, context |
| `Failed` | every kind except turn/session terminals |
| `CompleteTurn` / `CompleteSession` | the matching terminal kind only |

## Transition behavior

`node_execution::transition::select_transition` is deterministic and rejects:

- zero eligible outgoing edges from a nonterminal node;
- multiple eligible edges without explicit parallel semantics (`ParallelBranch`
  fans out into `ParallelSelection`);
- an outcome inconsistent with the node kind;
- a transition to a node absent from the persisted execution plan (`NodePlan`);
- variable writes not declared by the graph (declared writes must be covered by
  the compiled node's `write_scopes`);
- executor output above the declared engine bound;
- a repeat transition beyond the compiled loop bound.

Loop advancement (`advance_loop`) is derived from the compiled loop node kind
and the destination's terminality; the caller increments the canonical loop
iteration counter accordingly.

## Recovery

`node_execution::recovery::classify_node` consumes replay-derived node state and
durable effect evidence and classifies the current node as one of: not started,
entered but not dispatched, waiting on continuation, waiting on children,
waiting on parallel branches, completed, failed, ambiguous external effect, or
terminal. External completion is never inferred from graph control events
alone: dispatch started on an effectful node without terminal or waiting
evidence classifies as ambiguous and fails closed without redispatch.

## Event lifecycle

The reducer (`node_execution::reducer`) validates the outcome and produces a
typed decision plus lifecycle evidence: dispatch proposed, dispatch started,
node completed/failed, transition selected, execution waiting/resumed, and
terminal turn/session. The runtime commits the existing canonical events
(`style.execution_initialized`, `style.node_entered`, `style.node_completed`,
`style.node_failed`, `style.transition_selected`, terminal lifecycle) whose
ordering and recovery semantics are preserved. Dispatch-proposed/started
evidence is an in-memory trace on this branch; wire event variants are an
explicit later-task seam.

## Relationship to the turn runner

`apps/runtime/logic/src/turn.rs` still owns the effect-producing node adapters
(model call, tool gate, context transform, approval, child spawn/wait/review,
loop, branch, artifact persist, turn/session terminal). Entry, resume, and
recovery gates now key on the compiled node kind's dispatch capability instead
of the topology profile, and transition selection flows through the generic
engine. Task 3 moves the remaining node behaviors behind `NodeExecutorPort`;
Task 7 moves plugin-host execution behind the same port.

## Executability

Topology classification is no longer a condition of runtime executability.
`node_executor::inspect_runtime_executability` reports a graph executable when
every node resolves; the legacy adapter profile is retained only as the
non-blocking `NODEX007` advisory. `node_executor::dispatch_plan` exposes the
exact per-node identities that Task 1 persists as the immutable execution
plan.
