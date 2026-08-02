# TASK-03 — Native Control-Flow Node Executors

## Base SHA

`abbf97b82687a4d1e7463aab33382258d6d38fd9` (2026-07-29 20:34:20 -0400, "Add runtime node executor validation") — the agreed common base for the parallel campaign. `main` did not advance before this branch was created; no shared-base re-record was required.

## Task summary

Implemented the six native graph-node behaviors that Task 2's registry listed as
`available: false` (or that existed only inside the planner-worker special-purpose
adapter):

| Node kind | Implementation ID | Executor |
|---|---|---|
| `send_child_agent_message` | `runtime.child-message` | child-agent message |
| `join_results` | `runtime.join` | generic join |
| `parallel_branch` | `runtime.parallel` | bounded parallel branch |
| `delay` | `runtime.delay` | durable delay |
| `schedule` | `runtime.schedule` | graph-owned schedule |
| `emit_event` | `runtime.event-emission` | constrained event emission |

No second dispatcher was built. Each executor plugs into Task 2's generic
dispatcher contract: it declares the same kind/implementation/version/boundary/
capability record shape as the Task 2 capability registry, and the generic
dispatcher (`NativeNodeDispatcher` + `GenericNodeDispatcher`) resolves exactly one
executor per node and drives it through `prepare -> effect -> finalize`, with
recovery cuts classified explicitly.

## Files changed

### `apps/runtime/data/`
- `node_executors.rs` (new) — layer-owned record shapes for the six domains
  (`ChildMessageDataRecord`, `JoinDataRecord`, `ParallelBranchDataRecord`,
  `DelayDataRecord`, `GraphScheduleDataRecord`, `EmittedEventDataRecord`), the
  bounded `NodeExecutorStateDataSnapshot`, the `NodeExecutorStateDataPort` state
  seam, the in-memory `InMemoryNodeExecutorStateData` store, shared hard bounds,
  and snapshot validation. Unit tests cover round-trip and bounds.
- `lib.rs` — registered `pub mod node_executors;`.
- `node_executor.rs` — documentation only: explains why the six first-party
  implementations remain `available = false` until dispatcher wiring (fail-closed).

### `apps/runtime/logic/`
- `node_executors/mod.rs` (new) — the dispatcher contract: `NodeExecutorKind`,
  typed `NodeExecutorConfig` per node kind, `NodeExecutorInput` (exact
  session/run/node/attempt/iteration/step identity + caller-supplied clock +
  participant outcomes + wake claims + cancellation/removal requests),
  `NodeExecutorEffect` / `NodeExecutorEffectReceipt`, `NodeExecutorStep`,
  `NodeExecutorOutcome`, `ExecutorPhaseResult`, `NativeNodeExecutor` trait
  (`prepare`/`finalize`), `NodeExecutorError` with `recovery_classification()`,
  `NativeNodeDispatcher` resolver, hard bounds, and runtime-owned event prefixes.
- `node_executors/events.rs` (new) — 23 canonical committed event payloads plus
  the typed `NodeExecutorEventPayload` enum and its stable `event_type()` map.
- `node_executors/state.rs` (new) — replay-owned state records, the pure reducer
  (`NodeExecutorState::apply`/`reduce`), deterministic identity strings, and
  `ReplayClassification` (Consistent / SafeToProceed / ExternallyUncertain /
  InvalidTransition). Focused reducer and recovery tests for every domain.
- `node_executors/ports.rs` (new) — narrow logic-owned ports:
  `ChildSessionMessagePort`, `GraphSchedulePort`, `DurableDelayPort`, and the
  `NodeExecutorPorts` facade, with closed error taxonomies.
- `node_executors/child_message.rs` (new) — child-agent message executor.
- `node_executors/join.rs` (new) — generic join executor.
- `node_executors/parallel.rs` (new) — parallel branch executor.
- `node_executors/delay.rs` (new) — durable delay executor.
- `node_executors/schedule.rs` (new) — graph-owned schedule executor.
- `node_executors/event_emission.rs` (new) — constrained event-emission executor.
- `node_executors/dispatcher.rs` (new) — the generic dispatcher used for mock
  integration: `NodeExecutorCommitter` (in-memory reducer store),
  `NodeExecutorPolicy` (approve-all / deny-schedule), `GenericNodeDispatcher`
  driving prepare/effect/finalize with the replay gate, and recovery-cut tests
  for all six executors plus the Task 2 registry consistency test.
- `session.rs` — added 23 `RuntimeCommittedEvent` variants (boxed payloads),
  their `event_type()` arms, `apply_payload` arms delegating to the
  `node_executors::state` reducer, the `SessionState.node_executor` field
  (`#[serde(default)]`), initialization in `initialize()`, the
  `SessionReducerError::NodeExecutor` variant, and a session-level reducer test
  covering every new event category.
- `lib.rs` — registered `pub mod node_executors;`.

## Public types and traits added

Data layer (`agentmod-runtime-data`):
- `node_executors::{ChildMessageDataRecord, ChildMessageStateData,
  ChildMessageClassificationData, JoinDataRecord, JoinOrderingData,
  JoinProjectionData, JoinArtifactCollectionData, JoinStateData,
  ParallelBranchDataRecord, ParallelBranchMemberStateData,
  ParallelBranchStateData, DelayDataRecord, DelayStateData,
  GraphScheduleDataRecord, GraphScheduleTriggerData, GraphScheduleStateData,
  EmittedEventDataRecord, NodeExecutorStateDataSnapshot, NodeExecutorStateDataPort,
  InMemoryNodeExecutorStateData, NodeExecutorStateDataError}` and bound constants.

Logic layer (`agentmod-runtime-logic`):
- `node_executors::{NodeExecutorKind, NodeExecutorConfig, ChildMessageConfig,
  JoinConfig, ParallelBranchConfig, DelayConfig, ScheduleConfig, EmitEventConfig,
  NodeExecutorClock, ParticipantOutcome, NodeExecutorInput, NodeExecutorEffect,
  NodeExecutorEffectReceipt, NodeExecutorStep, NodeExecutorOutcome,
  NodeExecutorFailureClassification, ExecutorPhaseResult, NativeNodeExecutor,
  NodeExecutorError, NativeNodeDispatcher}`.
- `node_executors::events::{NodeExecutorEventPayload, ...23 payload structs...}`.
- `node_executors::state::{NodeExecutorState, NodeExecutorReducerError,
  ReplayClassification, ExecutorIdentity, ...records...}`.
- `node_executors::ports::{NodeExecutorPorts, ChildSessionMessagePort,
  GraphSchedulePort, DurableDelayPort, ...commands/receipts/errors...}`.
- `node_executors::dispatcher::{NodeExecutorCommitter,
  InMemoryNodeExecutorCommitter, NodeExecutorPolicy, NodeExecutorPolicyDecision,
  ApproveAllPolicy, DenySchedulePolicy, GenericNodeDispatcher, DispatchOutcome}`.
- `session::RuntimeCommittedEvent` gained 23 boxed variants;
  `SessionState.node_executor`; `SessionReducerError::NodeExecutor`.

## Required composition-root wiring (not performed — mock ports provided)

The generic dispatcher is intentionally wired with mocks in
`node_executors::dispatcher::tests`. The runtime composition root must bind:

1. **Ports** — a real `NodeExecutorPorts` implementation:
   - `child_messages()` → a `ChildSessionMessagePort` over the existing child
     session catalog (`ChildSessionLogicPort`/`SessionRegistryDataPort`). The
     delivery boundary MUST be create-once idempotent by `message_id` (the same
     duplicate-resolution contract used by durable approvals); the child style
     decides whether/how the message enters its provider projection. The parent
     session never receives a fabricated user message.
   - `schedules()` → a `GraphSchedulePort` over the existing
     `RuntimeScheduleLogicPort` (`upsert_schedule` create-once by deterministic
     schedule identity; `remove_schedule`).
   - `delays()` → a `DurableDelayPort` over the existing continuation logic
     (`ContinuationLogicPort`) plus the scheduler worker: `continuation_id` is
     deterministic (`delay-cont:{session}:{run}:{node}:{step}`), creation is
     create-once, wake claims are resume-once, and the scheduler fires the wake
     at the exact canonical `wake_time_ms`.
2. **Committer** — a journal-backed `NodeExecutorCommitter` that seals each
   `NodeExecutorEventPayload` as the matching `RuntimeCommittedEvent` (all
   variants already exist), commits through `SessionPersistenceLogic`, and
   reduces through the session reducer (already delegates to
   `node_executors::state`).
3. **Policy** — route consequential proposals (`GraphScheduleProposed`) through
   the existing style/plugin/user/mandatory interception pipeline before the
   idempotent effect; denial commits `GraphScheduleRejected`.
4. **Turn-adapter dispatch hook** — add a minimal, additive match arm in the
   runtime turn adapter (`turn.rs` dispatch — not touched by this task) for the
   six `StyleNodeDirective`s: build `NodeExecutorInput` from the compiled node
   and exact run identity, call `GenericNodeDispatcher::dispatch`, commit the
   returned events, and select the next transition from
   `transition_variables` via `CompiledStyleExecutor::transition` (the existing
   generic dispatcher). Participant outcomes for join/parallel are folded from
   verified canonical child completions; delay wake claims arrive from the
   scheduler claim path.
5. **Registry availability flip** — only after step 4 lands, flip the six
   `available: false` records in `apps/runtime/data/src/node_executor.rs`
   `native()` to `true`. Until then, graphs using these nodes fail closed at
   `validate_runtime_executability`, which is the intended behavior.

## Required protocol or manifest changes

None. No wire protocol, manifest schema, or SDK change was required: the six node
kinds were already part of the compiled graph model and the Task 2 registry. The
new events use the existing canonical journal framing (`EventEnvelope` +
`RuntimeCommittedEvent`), so no protocol version bump is needed.

## Migration concerns

- The 23 new `RuntimeCommittedEvent` variants are additive; existing journals
  replay unchanged because new variants cannot appear in old journals and
  `SessionState.node_executor` defaults to empty.
- Old session snapshots without `node_executor` deserialize with the serde
  default (empty state).
- `EventEnvelope` checksums bind each new payload; the typed-to-layer JSON
  mapping in `persistence.rs` is generic and needs no change.
- Do NOT flip registry availability before the turn-adapter dispatch hook exists:
  that would let structurally valid graphs pass executability validation and then
  fail at dispatch.
- Parallel recovery fails closed by design: members dispatched without terminal
  evidence are never redispatched (`Ambiguous` / `ExternallyUncertain`).
- Delay expiry is evaluated with the caller-supplied clock only; executors never
  read a clock inside logic.

## Commands actually run

```text
cargo check -p agentmod-runtime-data
cargo check -p agentmod-runtime-logic
cargo check --workspace --all-targets
cargo test -p agentmod-runtime-data -p agentmod-runtime-logic        # 34 + 178 tests pass
cargo test -p agentmod-runtime-logic node_executors                 # 47 executor tests pass
cargo test -p agentmod-runtime-logic session::tests::native_control_flow
cargo clippy -p agentmod-runtime-data -p agentmod-runtime-logic --all-targets   # clean
```

Full workspace tests, formatting, workspace Clippy, and the architecture check
still run as the final validation gate below.

## Remaining integration steps

1. Turn-adapter dispatch hook for the six `StyleNodeDirective`s (additive match
   arms in `turn.rs`; the generic dispatcher contract above is the adapter).
2. Real port implementations over `ChildSessionLogicPort`,
   `RuntimeScheduleLogicPort`, `ContinuationLogicPort`, and the scheduler worker
   claims (create-once / resume-once contracts documented above).
3. Registry availability flip (after 1 and 2).
4. Process-level E2E: a declarative graph using the six nodes through the real
   daemon, including a daemon restart at a delay wake and a child-message
   delivery cut, proving exactly-once behavior.
5. `STATUS.md` update by the integration owner after merge.
