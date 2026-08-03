# TASK-01-execution-plan — Immutable Node Execution Plan

## Base SHA

- Expected common base commit (campaign): `abbf97b82687a4d1e7463aab33382258d6d38fd9`
- Verified latest `main` before changing code: `5274b830d0622ac8985766a0da0da75a2d26128a`
  (`feat(runtime): implement generic node execution`)
- **New shared base recorded for this workstream: `5274b830d0622ac8985766a0da0da75a2d26128a`**
  (main advanced past the expected base; recorded per isolation rule before any
  code was changed. The generic-node-execution work in that commit already
  retained the resolution inside the style binding; this workstream adds the
  dedicated n-tier plan modules, durable checksummed plan-file persistence,
  exact restart validation, and inspection/migration typing.)
- Task branch: `feature/execution-plan` (created from the new shared base)

## Files changed

New modules:
- `apps/runtime/dependency/src/execution_plan.rs` — checksummed, atomic,
  immutable `execution-plan.json` envelope persistence (schema, BLAKE3 payload
  checksum, size bounds, dedupe, missing/corrupt classification).
- `apps/runtime/data/src/execution_plan.rs` — normalized plan identity records,
  canonical payload building/parsing, `ExecutionPlanDataPort`.
- `apps/runtime/logic/src/execution_plan.rs` — plan identity derivation,
  restart validation (strict + resume-tolerant), migration typing, pure
  inspection projection, availability projection.
- `docs/architecture/execution-plan.md` — architecture documentation.

Modified:
- `apps/runtime/dependency/src/lib.rs` — `pub mod execution_plan`; port impl
  for `LocalRuntimeDependencies`.
- `apps/runtime/dependency/src/registry.rs` — plan file staged atomically in
  session/branch/child creation; new `SessionCatalogDependencyError::ExecutionPlan`.
- `apps/runtime/dependency/src/supervised.rs` — `ExecutionPlanDependencyPort`
  for `SupervisedRuntimeDependencies`.
- `apps/runtime/data/src/lib.rs`, `apps/runtime/data/src/local.rs` — module
  registration and `ExecutionPlanDataPort` on `LocalRuntimeDataPort`.
- `apps/runtime/data/src/registry.rs` — plan file carried through
  `CreateSessionDataRequest` / `CreateBranchDataRequest` /
  `CreateChildSessionDataRequest`; mapping into dependency envelopes.
- `apps/runtime/logic/src/lib.rs` — `pub mod execution_plan`.
- `apps/runtime/logic/src/node_executor.rs` — `pub(crate) registry_hash_for`
  (narrow type exposure for the availability projection).
- `apps/runtime/logic/src/registry.rs` — session creation attaches the plan
  file; `SessionRegistryLogicError::ExecutionPlan`.
- `apps/runtime/logic/src/history.rs` — branch creation attaches the plan
  file; `SessionHistoryLogicError::ExecutionPlan`.
- `apps/runtime/logic/src/child_session.rs` — child creation attaches the plan
  file; `ChildSessionLogicError::ExecutionPlan`.
- `apps/runtime/logic/src/turn.rs` — resume/cancel/deferred/scheduled restart
  points validate the persisted plan file; `ExecutionPlanDataPort` added to
  `TurnLogic` impl bounds; mock data port; `execution_plan_resume_error`
  mapping; pre-existing dead-code/`too_many_lines` allow on
  `commit_generic_complete_turn_assistant` so `clippy -D warnings` passes.
- `tests/integration/src/lib.rs` — `execution-plan.json` added to required
  session files; three new integration tests.
- `tests/integration/tests/plugin_node_process.rs` — `ExecutionPlanDependencyPort`
  for the process test dependency bundle.

## Public types and traits added

Dependency:
- `ExecutionPlanDependencyPort` (store/load), `LocalExecutionPlanDependency`,
  `DependencyExecutionPlanFile`, `DependencyStoreExecutionPlanRequest/Response`,
  `DependencyLoadExecutionPlanRequest/Result`,
  `DependencyExecutionPlanRecord`, `ExecutionPlanDependencyError`.

Data:
- `ExecutionPlanDataPort`, `ExecutionPlanIdentityData`, `ExecutionPlanFileData`,
  `StoreExecutionPlanDataRequest/Record`, `LoadExecutionPlanDataRequest/Result`,
  `LoadedExecutionPlanDataRecord`, `ExecutionPlanDataError`,
  `EXECUTION_PLAN_RECORD_SCHEMA_VERSION`, `to_dependency_file`,
  `validate_file_identity`.

Logic:
- `ExecutionPlanIdentity` (+ `from_binding`, `to_data`, `first_mismatch`),
  `ExecutionPlanMigrationDiagnostic`, `ExecutionPlanRestartOutcome`,
  `ExecutionPlanNodeProjection`, `ExecutionPlanInspectionProjection`,
  `ExecutionPlanAvailability`, `ExecutionPlanNodeAvailability`,
  `PersistExecutionPlanFileCommand`, `to_plan_file_data`,
  `persist_execution_plan_file`, `validate_persisted_execution_plan`,
  `validate_session_resume_plan`, `inspect_execution_plan_file`,
  `availability_projection`, `ExecutionPlanLogicError`.

## Required composition-root wiring

No new binary composition-root edits were required; the existing
`LocalRuntimeDependencies`, `SupervisedRuntimeDependencies`, and the
integration `ProcessRuntimeDependencies` bundles implement
`ExecutionPlanDependencyPort` by delegation, which makes
`RuntimeData<D>::ExecutionPlanDataPort` and the `TurnLogic` bounds resolvable.
If a future composition root introduces another dependency bundle, it must
implement `ExecutionPlanDependencyPort` (delegate to
`LocalExecutionPlanDependency`) for session creation and restart validation to
work.

## Required protocol or manifest changes

None. The plan file is a runtime-owned storage artifact, not a wire DTO; the
runtime protocol and session-style manifests are unchanged. The canonical
style binding already carries `execution_plan` / `execution_plan_hash`
(unchanged JSON), so `session.created` evidence still binds plan hash and
registry hash without protocol versioning.

## Migration concerns

- Sessions created before this feature have a binding plan but no plan file.
  The strict subsystem API (`validate_persisted_execution_plan`) returns the
  typed `MigrationRequired` (`EPLAN-201`) outcome; the live resume path
  (`validate_session_resume_plan`) falls back to the existing exact
  binding-based revalidation so pre-file sessions keep their fail-closed
  restart guarantee. Branch-with-recompiled-style is the supported migration
  path and creates a fresh plan file.
- Sessions whose binding has no plan at all keep the existing
  `StyleMigrationRequired` behavior; no unsafe in-place plan mutation exists.
- The plan schema is frozen (compiler V2/V3). New identity fields live in the
  plan-file record, not the plan struct, so existing persisted plan hashes
  remain valid.
- Corrupt or drifted plan files fail closed with stable `EPLAN-1xx`
  (identity), `EPLAN-201`/`EPLAN-301` (migration/corruption), and `EPLAN-4xx`
  (turn resume) diagnostics.

## Commands actually run

All on the `feature/execution-plan` branch (Windows host):

```text
cargo fmt --all -- --check                                  -> clean
cargo clippy --workspace --all-targets --all-features -- -D warnings -> clean
cargo test --workspace --all-targets --all-features --locked
  -> all pass except the pre-existing base failure
     session::tests::ephemeral_context_completion_rejects_stale_user_and_memory_provenance
     (verified failing on the clean base commit 5274b83; unrelated to this task)
cargo test --workspace --doc --all-features --locked        -> pass
cargo run --locked -p xtask -- architecture --manifest-path Cargo.toml
  -> "checked 90 packages; no violations"
cargo test --locked -p xtask --test architecture            -> pass
```

Focused suites:
- `cargo test -p agentmod-runtime-dependency --lib execution_plan` — 8 pass
  (atomic write, dedupe, missing/corrupt/checksum/schema/truncation, bounds).
- `cargo test -p agentmod-runtime-data --lib execution_plan` — 6 pass
  (identity mapping, node-count/plan-hash/schema validation, round trip,
  corruption translation).
- `cargo test -p agentmod-runtime-logic --lib execution_plan` — 10 pass
  (identity derivation, plan-file mapping, missing/corrupt/drift fail-closed,
  restart valid, availability with an extra registration).
- `cargo test -p agentmod-integration-tests --lib` — 10 pass (3 new:
  create + restart validation + corruption fail-closed; branch-with-style
  distinct child plan with byte-identical parent; pure replay inspection with
  no live registry).

## Remaining integration steps

- The integration owner merges `feature/execution-plan` (and peer workstreams)
  and updates `STATUS.md`.
- A process-level E2E script exercising daemon restart with a corrupted plan
  file (kill mid-write / truncate `execution-plan.json`, restart, expect
  fail-closed diagnostics) should be added under `tests/e2e/` when the daemon
  environment is available; the in-process integration tests already prove the
  file is written atomically with session creation and validated on load.
- Optional: expose the logic-level plan inspection projection as a
  service/CLI endpoint (`RuntimeRequest`/CLI `session plan inspect`) — the
  service mapping is intentionally deferred ("only if endpoints are added").
