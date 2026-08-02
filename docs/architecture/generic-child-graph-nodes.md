# Generic Child-Orchestration Node Contracts

AgentMod compiles child spawning, child waiting, and structured review as
graph-owned node contracts. These contracts do not depend on the
`planner-worker` style ID, node labels, or one bundled topology.

The graph engine owns the serialized configuration schema and compile-time
bounds. Runtime logic owns a pure coordinator that accepts the exact compiled
node and its persisted executor resolution. The coordinator returns proposals
and projections only; it does not create sessions, invoke a model, commit
events, or mutate graph variables.

## Compiled configurations

`spawn_child_agent` declares:

- Static or canonical-variable task input.
- A stable task-ID prefix.
- Exact child style selector and allowed tool groups.
- Maximum children and recursive depth.
- Per-child token, context, and cost budgets.
- Workspace isolation.
- Declared immutable artifact references.
- Task security classification.
- Mandatory approval.

Static tasks are a bounded string, list of strings, or map from task ID to
string. Variable tasks must use a compatible declared canonical variable.
Inline secret-shaped fields are rejected; secret inputs must be external
secret references.

`wait_for_agents` declares:

- An exact child-ID set or a declared `list<child_id>` variable.
- Maximum children.
- Minimum successes.
- Durable timeout.
- Cascade, detach, or wait cancellation behavior.

`review` declares:

- Static or canonical-variable integration input.
- Declared artifact evidence.
- Finding, finding-size, and rejection bounds.
- Whether findings require artifact evidence.
- Exact approved, revision, and structured-failure destinations.
- Maximum revisions.

The compiler requires three distinct review routes and requires the failure
route to target a `fail` node.

## Immutable runtime identity

The pure coordinator verifies all of the following before returning a
proposal:

1. Session, run, node-work, and parent authorization identity.
2. Complete immutable execution-plan hash.
3. The exact authorized persisted executor resolution.
4. `runtime.child-spawn@1.0.0`, `runtime.child-wait@1.0.0`, or
   `runtime.review@1.0.0`, including the runtime-logic boundary.
5. The hash of the complete compiled node against the persisted adapter
   configuration reference.
6. Canonical variable reads and all runtime bounds.

Changing a configuration field, node identity, executor version, plan hash,
or parent authorization fails closed.

## Pure outcomes and recovery

Child spawn returns stable task-ordered creation proposals. Each proposal binds
the exact work identity, task and task hash, style, tools, depth, budgets,
workspace policy, artifacts, classification, mandatory approval, and proposal
hash. Normal runtime proposal, interceptor, user-policy, and mandatory-policy
handling remains required before any child-session effect.

Child wait is reconstructed only from canonical child state. It returns:

- `Waiting`, with completed receipts, missing children, remaining canonical
  timeout, and cancellation state.
- `Completed`, with stable child-ID-ordered result references, artifacts,
  task IDs, and completion sequences.
- `Failed`, with a structured timeout, cancellation, or impossible-threshold
  code and the exact children eligible for cancellation.

Input order does not affect the projection or its hash. No live process lookup
is used for recovery.

Review validates the provider or plugin terminal-result hash, known rejected
task identities, bounded structured findings, and artifact authorization. It
then proposes exactly one configured destination:

- Approved.
- Revision, with the next bounded revision number.
- Structured failure when the revision limit is exhausted.

The routing evidence hash binds the work, plan, input, candidate result,
disposition, destination, and revision.

## Canonical application and event versioning

Runtime logic applies the pure outcomes through a separate effect-free
next-event planner. The planner consumes only replayed `SessionState`, one
exact persisted node resolution, and receipts already obtained through normal
runtime use cases. It returns the next legal canonical event and never writes
the journal, calls a provider, or creates a child itself.

Child creation is recoverable at every boundary:

```text
proposed -> approved -> dispatched -> created -> completed|failed|cancelled
```

The proposal retains the complete zero-hash serialized `ChildSpawnProposal`,
typed task and task hash, plan/configuration hashes, budgets, workspace,
artifacts, classification, and exact executor-owned work identity. Approval,
dispatch, parent/child linking, and terminal receipts each have independent
domain-separated hashes. A committed dispatch without a matching creation
receipt remains an outbox fact; replay does not fabricate a child or redispatch
the effect.

Wait projections retain stable successful, unsuccessful, pending, and
cancellation child sets. Their pure result hash is independently recomputed
before the application-envelope hash is accepted. Completed, timeout,
parent-cancelled, and impossible-threshold states therefore reconstruct without
live process inspection. Review application similarly retains the exact
configured destination, revision counters, structured findings, artifact
evidence, and an application hash.

These are versioned additions to the existing child-agent and planner-worker
event family, not a second child authority. They reduce into the existing
`SessionState.child_agents` and `SessionState.planner_worker` projections.
The legacy child-created event cannot encode a dispatch outbox, typed task,
workspace/artifact/security contract, failed or cancelled terminal receipt, or
the immutable execution-plan identity. The legacy join and reviewer events
likewise cannot represent waiting/timeout/cancellation sets or exact configured
review routes. Reinterpreting those wire payloads would make old journal
entries ambiguous, so the generic events use distinct versioned wire names
while sharing the same authoritative replay state.

## Legacy migration boundary

Existing built-in planner manifests without these typed configurations remain
compilable for replay compatibility. The pure generic coordinator rejects such
planless nodes with an explicit versioned-migration diagnostic. A session must
branch with a recompiled style to adopt the generic contract; it is never
silently rebound.

These contracts are live in normal generic Turn execution. The production
composition root supplies the existing journal, child-session, continuation,
interception, permission, provider-review, and ancillary-application boundaries;
the pure next-event planner receives only their exact receipts. The registered
`runtime.child-spawn@1.0.0`, `runtime.child-wait@1.0.0`, and
`runtime.review@1.0.0` implementations are therefore selected from the persisted
execution plan without a planner-worker style or topology gate.

Graph B process runs on Windows and Ubuntu/WSL2 cover child creation, parent
restart, completion ordering, canonical wait reconstruction, reviewer rejection,
revision children, accepted cancellation with durable approval, terminal replay,
and duplicate-dispatch rejection. Planner-worker v1.4 uses the same generic
spawn/wait/review path and adds evidence-aware executors, concurrent child turns
in runtime-owned branch workspaces, child-owned edit/test/diff artifacts, and
artifact-bound integration/review. Windows and Ubuntu/WSL2 process tests prove
that path across daemon replacement; the prior v1.3 matrix retains pure-replay
evidence. Exact legacy planner
histories remain on their frozen versioned adapter; adopting the generic
contract still requires an explicit recompiled branch rather than silent
rebinding.
