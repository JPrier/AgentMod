# TASK-02 — Generic Node Dispatch

Task branch: `feature/generic-node-dispatch`
Task ID: `TASK-02-generic-dispatch`

## Exact base SHA

`abbf97b82687a4d1e7463aab33382258d6d38fd9`

Verified as the `origin/main` HEAD at task start; no advancement was recorded.
No merge, rebase, or pull from other workstreams was performed while
implementing.

## Mission outcome

The runtime no longer requires a recognized topology-adapter profile for a
compiled graph to be runtime-executable. Execution is driven by the exact
resolved executor identity of every node through a generic dispatch engine.
A graph executes because every node has an available resolved executor, not
because its node count and edge sequence match a built-in shape.

## Files changed

New modules (runtime logic):

- `apps/runtime/logic/src/node_execution/mod.rs` — generic dispatch engine:
  `ExecuteNodeCommand`, `NodeExecutionInput`, `NodeExecutionOutcome`,
  `NodeExecutorIdentity`, `NodeExecutorPort`, `dispatch_node`, `DispatchError`.
- `apps/runtime/logic/src/node_execution/outcome.rs` — outcome-class ×
  node-kind compatibility matrix and `requires_effect_evidence`.
- `apps/runtime/logic/src/node_execution/transition.rs` — deterministic
  generic transition selection with the full rejection contract and
  property-tested selection.
- `apps/runtime/logic/src/node_execution/recovery.rs` — replay-derived
  recovery classification (all waiting classes, fail-closed ambiguity).
- `apps/runtime/logic/src/node_execution/reducer.rs` — pure dispatch reducer
  producing canonical lifecycle evidence and dispatch decisions.
- `apps/runtime/logic/src/node_execution/dispatch_tests.rs` — focused
  regression/property tests for built-ins, style-ID independence, and
  structurally different compatible graphs.

Modified:

- `apps/runtime/logic/src/style_executor.rs` — transition selection now
  delegates to the generic engine; added `dispatch_cursor`,
  `validate_outcome`, `dispatch_command`, `dispatch` seams and
  `supported_entry_kinds`/`supports_node_dispatch`; `StyleAdapterKind` and
  `adapter_kind` retained only as temporary compatibility diagnostics.
- `apps/runtime/logic/src/node_executor.rs` — removed the topology-profile
  executability gate (NODEX006); NODEX007 is now a non-blocking advisory;
  added `dispatch_plan` (Task 1 seam) mapping exact resolutions to engine
  identities.
- `apps/runtime/logic/src/turn.rs` — entry/resume/recovery gates are now
  node-kind dispatch-capability based; context composition dispatches on the
  compiled node kind; recovery extends to unknown topologies through the
  engine's control-only classification.
- `apps/runtime/logic/src/lib.rs` — registered `node_execution` module.
- `apps/runtime/logic/Cargo.toml` — added `proptest.workspace = true`
  (dev-dependency) for the transition property tests.

## Public types and traits added

All under `agentmod_runtime_logic::node_execution`:

- `ExecuteNodeCommand`
- `NodeExecutionInput`
- `NodeExecutionOutcome` (eight outcome classes)
- `BoundedNodeOutput`, `ContinuationEvidence`, `ChildSessionEvidence`,
  `ParallelBranchEvidence`, `SchedulerEvidence`, `RetryReason`, `NodeFailure`
- `NodeExecutorIdentity`, `ExecutorBoundary`
- `NodeExecutorPort` (trait: `can_execute`, `execute`)
- `NodeCursor`, `NodePlan`
- `dispatch_node`, `validate_command`, `validate_outcome_for_kind`,
  `serialized_kind`, `OutcomeCompatibility`
- `select_transition`, `TransitionSelectionOutcome`, `TransitionSelection`,
  `ParallelSelection`, `BranchTarget`, `LoopState`, `TransitionError`
- `classify_node`, `NodeRecoveryClass`, `NodeStateEvidence`, `EffectEvidence`
- `NodeDispatchReducer`, `NodeDispatchDecision`, `NodeDispatchEvent`,
  `NodeDispatchEventKind`
- `DispatchError`

Internal seams (crate-private, exercised by tests):

- `CompiledStyleExecutor::dispatch_cursor / validate_outcome /
  dispatch_command / dispatch / dispatch_plan / entry_kind /
  supported_entry_kinds / supports_node_dispatch`
- `node_executor::dispatch_plan`

## Required composition-root wiring

None on this branch. The engine is pure logic; the runtime's existing node
adapters (model call, tool gate, context transform, approval, child spawn,
wait, review, loop, branch, artifact persist, turn/session terminal) continue
to implement effects through the existing proposal/policy/dispatch/receipt
paths. Task 3 node executors and Task 7 plugin-host transports plug into
`NodeExecutorPort` and `CompiledStyleExecutor::dispatch`; no new composition
root is required before then.

## Required protocol or manifest changes

None. No canonical event format changes were introduced: dispatch proposed /
started lifecycle evidence is produced by the reducer as a typed in-memory
trace (`NodeDispatchEvent`), and the canonical events already committed
(`style.execution_initialized`, `style.node_entered`, `style.node_completed`,
`style.node_failed`, `style.transition_selected`, terminal lifecycle) remain
unchanged. If a later task needs dispatch-proposed/started events on the wire,
they are additive serde-defaulted variants.

## Migration concerns

- **NODEX006 removed, NODEX007 advisory**: session creation no longer fails
  for topologies without a legacy adapter profile when every node resolves.
  The report's `advisory_diagnostics` field is new; consumers reading only
  `diagnostics` see unchanged behavior for blocked creations.
- **`RuntimeExecutabilityReport` gained `advisory_diagnostics`** — additive
  struct field; no serialized contract affected (report is logic-internal).
- **Entry gate relaxed**: `begin_style_turn` accepts any graph whose entry
  node kind is `ContextTransform`, `ModelCall`, or `ConditionalBranch`.
  Built-in styles are unaffected. Unknown topologies whose downstream nodes
  lack runtime behavior fail with `UnexpectedStyleNode`/recovery errors
  instead of being silently claimed executable at turn time.
- **Recovery gate relaxed**: `recover_style_control_gaps` now repairs
  control-only destination entries for unknown topologies; effectful
  destinations without durable evidence still fail closed
  (`StyleControlRecoveryRequired`), preserving the no-redispatch contract.
- **Loop-budget rejection is new** in the generic transition selector: a
  repeat transition beyond the compiled loop bound is rejected. Built-in
  loops always select the terminal edge at the bound, so process behavior is
  unchanged; property tests cover the rejection.
- **Variable-write validation is new**: outcomes declaring variable writes
  must be covered by the compiled node's `write_scopes`. No current adapter
  declares writes; the rule is fail-closed (undeclared writes rejected).

## Integration seams for parallel tasks

- **Task 1 (immutable execution plan)**: `node_executor::dispatch_plan` maps
  exact per-node resolutions to `NodeExecutorIdentity`s; `NodePlan` is the
  engine's destination-membership contract. Persist the plan into the session
  binding, then feed `NodePlan::from_resolutions` (or the persisted set) to
  `select_transition`/`NodeDispatchReducer` instead of `NodePlan::from_graph`.
- **Task 3 (native control-node executors)**: implement `NodeExecutorPort`
  for the missing node kinds (`SendChildAgentMessage`, `JoinResults`,
  `ParallelBranch`, `Delay`, `Schedule`, `EmitEvent`, `Fail`) and call
  `CompiledStyleExecutor::dispatch`; the engine already validates their
  outcome classes via `outcome.rs`.
- **Task 4 (graph variables)**: `NodeExecutionInput::variables` carries
  canonical variable input; `BoundedNodeOutput::variable_writes` is the
  declared-write contract validated against compiled `write_scopes`. The
  engine's read side is the Task 4 interface for condition evaluation.
- **Task 7 (plugin-host transport)**: plugin executors resolve to
  `ExecutorBoundary::PluginHost` identities; the dispatch path is identical,
  and `DispatchError::NoResolvedExecutor` fails closed when the transport is
  unavailable.

## Remaining integration steps

1. Task 1 persists the dispatch plan; swap `NodePlan::from_graph` for the
   persisted plan and bind it into recovery validation.
2. Task 3 implements the missing node behaviors behind `NodeExecutorPort`;
   turn.rs's per-directive drivers can then be replaced by port dispatch.
3. Task 4 replaces condition-evaluation variables with the canonical variable
   interface (the `style_transition_variables` conventions in turn.rs are the
   temporary port).
4. Task 7 adds plugin-host execution behind the same port; plugin-node
   dispatch currently resolves to `NODEX005` diagnostics when disabled.
5. Legacy `StyleAdapterKind` classifiers can be deleted once all node kinds
   dispatch through the port (they are advisory today).

## Commands actually run (all on Windows, branch `feature/generic-node-dispatch`)

```text
cargo check -p agentmod-runtime-logic
cargo test  -p agentmod-runtime-logic --lib node_execution   (28 passed)
cargo test  -p agentmod-runtime-logic --lib                  (158 passed)
cargo test  --workspace --all-features                       (no failures)
cargo test  --workspace --doc --all-features                 (no failures)
cargo clippy --workspace --all-targets --all-features        (no warnings)
cargo fmt --all -- --check                                   (clean)
cargo run -p xtask -- architecture --manifest-path Cargo.toml
    -> checked 89 packages; no violations
cargo check --workspace --all-targets                        (clean)
```

Process-level E2Es (Windows named pipe, real daemon + harness + CLI):

```text
powershell -ExecutionPolicy Bypass -File tests/e2e/runtime_style_registry.ps1
    -> runtime session-style registry/restart/branch E2E passed
powershell -ExecutionPolicy Bypass -File tests/e2e/runtime_ephemeral_turn.ps1
    -> runtime ephemeral-turn fresh-context/restart E2E passed
powershell -ExecutionPolicy Bypass -File tests/e2e/runtime_declarative_graph.ps1
    -> runtime declarative branch/loop/tool/approval/restart/replay E2E passed
powershell -ExecutionPolicy Bypass -File tests/e2e/runtime_research_loop.ps1
    -> runtime research-loop iteration/artifact/introspection/restart/replay E2E passed
powershell -ExecutionPolicy Bypass -File tests/e2e/runtime_planner_worker.ps1
    -> runtime planner-worker child/join/reject-once/restart E2E passed
```

Environment note: the daemon startup fails with `Error: Transport` until the
host binaries are built (`agentmod-scheduler` is spawned eagerly at serve
startup). The failure reproduces identically at the base commit and is
unrelated to this task; building the host packages resolves it.

## Verification of the definition of done

| Requirement | Evidence |
|---|---|
| Generic dispatch API exists | `node_execution` module; `dispatch_node`, `NodeExecutorPort`, `NodeDispatchReducer` unit-tested |
| Dispatch selected by exact executor identity | `dispatch_node` fails `NoResolvedExecutor` when the port cannot execute the exact identity; walk tests use identities from `dispatch_plan` |
| Topology adapter recognition not required for correctness | `node_executor::inspect_runtime_executability` reports executable for all-resolved unknown topologies (NODEX007 advisory); regression test updated to assert it |
| Built-ins execute through generic path | `every_built_in_style_executes_through_the_generic_dispatch_path` walks all five built-ins via `dispatch_node` + reducer with real compiled graphs and resolved identities; all five built-in style process E2Es pass on Windows through the live runtime |
| Generic waiting/retry/failure/terminal outcomes | outcome unit tests per class; reducer decisions per class; recovery classes per waiting class |
| Deterministic validated transitions | `select_transition` rejections unit-tested + proptest order/repeat independence |
| No external-effect bypass | engine consumes typed outcomes + durable evidence only; recovery fail-closed on ambiguous evidence; no new dispatch paths touch hosts |
| Integration seams documented | this document, plus `docs/architecture/node-dispatch.md` |

## Process-level validation note

All five built-in style process E2Es were executed on Windows and pass against
this branch's daemon: persistent chat (registry/restart/branch), ephemeral
turn (fresh-context/restart), declarative graph (branch/loop/tool/approval/
restart/replay), research loop (iteration/artifact/introspection/restart/
replay), and planner-worker (child/join/reject-once/restart). They exercise
the identity-driven executability gate at session creation, the generic
node-kind dispatch in the turn runner, and the recovery paths across daemon
restarts. The generic engine's outcome/transition/recovery contracts are
additionally proven by focused unit, integration, and property tests with
mock node-executor ports. Missing native node behaviors remain the property
of Task 3; plugin-host dispatch remains the property of Task 7.
