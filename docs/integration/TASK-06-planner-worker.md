# TASK-06-planner-worker — Productionize Planner-Worker-Reviewer

Task ID: `TASK-06-planner-worker`
Branch: `feature/planner-worker-production`

## Exact base SHA

`abbf97b82687a4d1e7463aab33382258d6d38fd9` (main upstream base for the
parallel campaign). The worktree was created from that commit; no merge,
rebase, or pull from other workstreams occurred.

## Scope

This workstream owns planner-worker business behavior and the workspace/result
packages. It does not modify the generic dispatcher core, graph-variable core,
context subsystem, plugin host, frontend rendering, or execution-plan
persistence. `STATUS.md` is not edited; the integration owner updates global
status after merging.

## Files changed

Modified:

- `sdk/session-style-sdk/src/builtins.rs` — planner-worker graph v1.2.0 adds a
  bounded `waves` loop node and conditional wait→waves/waves→spawn edges;
  every cycle now traverses a bounded loop node (STYLE025).
- `sdk/session-style-sdk/tests/style.rs` — version assertions for 1.2.0.
- `apps/runtime/logic/src/session.rs` — extended `PlannedTask` schema,
  `ChildSessionLinkedEvent`/`ChildSessionOrigin` workspace fields, new
  canonical events, reducer transitions, crash-cut matrix tests.
- `apps/runtime/logic/src/turn.rs` — planner adapter: validated plan, wave
  dispatch, concurrent child wait, result packages, integration, evidence
  review, durable child approval.
- `apps/runtime/logic/src/style_executor.rs` — 8-node/10-edge planner graph
  recognition.
- `apps/runtime/logic/src/child_session.rs` — workspace-mode enforcement in
  child binding restriction; workspace/task fields in the typed child link.
- `apps/runtime/logic/src/continuation.rs` — `ChildApprovalContinuation`
  payload.
- `apps/runtime/logic/src/conversation.rs` — `ChildHandoffEntry.artifact_id`
  changed from `Option<ArtifactId>` (UUID) to `Option<String>` so the
  content-addressed result-package reference is retained in the handoff.
- `apps/runtime/logic/src/lib.rs` — module registration.
- `apps/runtime/logic/Cargo.toml` — `futures` workspace dependency.
- `apps/runtime/data/src/artifact.rs` — `read_artifact` data-port method.
- `apps/runtime/data/src/continuation.rs` — `ChildApprovalPayloadRecord`.
- `apps/runtime/data/src/lib.rs` — `read_artifact` routing.
- `apps/harness/dependency/src/execution.rs` — deterministic fixture emits the
  validated task schema, evidence-bearing worker output, structured findings.
- `tests/e2e/runtime_planner_worker.ps1`, `runtime_planner_worker.sh` —
  planner-worker@1.2.0, result-package/integration/artifact assertions.
- `Cargo.toml` — `futures` pinned to `=0.3.33` (the previous `=0.3.31` pin was
  unused and conflicted with the lockfile version required by
  `agent-client-protocol`).

New:

- `apps/runtime/logic/src/planner.rs` — task-schema parsing and validation.
- `apps/runtime/logic/src/workspace.rs` — workspace-mode enforcement, write
  policy, canonical lease state machine.
- `apps/runtime/logic/src/result_package.rs` — immutable worker result
  packages.
- `apps/runtime/logic/src/integration.rs` — deterministic integration over
  applied packages with overlap/conflict detection.
- `docs/architecture/planner-worker-production.md` — architecture document.
- `docs/integration/TASK-06-planner-worker.md` — this document.

## Public types and traits added

`agentmod-runtime-logic` (new modules):

- `planner::PlannerValidationContext`, `planner::PlannerValidationError`,
  `planner::parse_and_validate_plan`, `planner::workspace_mode_string`.
- `workspace::modes` (five canonical mode constants), `WorkspaceEnforcement`,
  `LeaseResolution`, `WorkspacePolicyError`, `enforce_tool_group`,
  `enforce_process_command`, `write_phase_grant`, `dead_lease_owners`,
  `classify_lease`, `restrict_tool_groups`, `task_workspace_mode`.
- `result_package::WorkerResultPackage`, `PackageTaskIdentity`,
  `PackageChildIdentity`, `PackageProviderIdentity`, `PackageLspDiagnostic`,
  `PackageUsage`, `PackageEventRange`, `ResultPackageError`,
  `build_result_package`, `usage_from_state`, `handoff_line`,
  `RESULT_PACKAGE_SCHEMA_VERSION`.
- `integration::IntegrationResultArtifact`, `IntegrationDecision`,
  `IntegrationError`, `decide_integration`, `build_integration_artifact`,
  `INTEGRATION_RESULT_SCHEMA_VERSION`.

`agentmod-runtime-logic` (session/continuation):

- Session: `PlannedTask` extended with `goal`, `scope`, `dependencies`,
  `expected_artifacts`, `workspace_mode`, `tool_groups`,
  `validation_commands`, `completion_criteria`, `review_criteria`,
  `token_budget`, `cost_budget_micros`, `max_steps`, `retry_policy`, `risk`;
  new `TaskRisk`, `TaskRetryPolicy`.
- New canonical events: `WorkspaceLeaseAcquiredEvent`,
  `WorkspaceLeaseReleasedEvent`, `WorkspaceLeaseReconciledEvent`,
  `WorkerResultPackageCommittedEvent`, `IntegrationResultCommittedEvent`,
  `ChildCreationApprovalRequestedEvent`, `ChildCreationApprovalResolvedEvent`.
- New replay state: `WorkspaceLeaseRecord`, `WorkerResultPackageRecord`,
  `IntegrationResultRecord`, `ChildApprovalRecord`;
  `ChildAgentRecord` gains result-package fields;
  `PlannerWorkerState` gains leases/packages/integrations/approvals;
  `ChildSessionOrigin` and `ChildSessionLinkedEvent` gain
  `workspace_mode`/`expected_artifacts`/`validation_commands`.
- Continuation: `ContinuationPayload::ChildApproval` with
  `ChildApprovalContinuation` (logic) and `ChildApprovalPayloadRecord`
  (data).

`agentmod-runtime-data`:

- `artifact::ArtifactDataPort::read_artifact` with `ReadArtifactDataRequest`
  / `ReadArtifactDataRecord`.

## Required composition-root wiring

None. The runtime daemon composition root is unchanged: `TurnLogic::new` gains
no new dependencies, and the planner adapter is selected by the compiled graph
exactly as before. `futures` was added to the logic crate's dependency list
(workspace-pinned).

## Required protocol or manifest changes

- `planner-worker` built-in style is now `1.2.0` (graph changed). Sessions
  created with `planner-worker@1.1.0` remain bound to the old compiled graph
  (exact-match `built_in_manifest_for_version`); they are not silently
  upgraded. New sessions must select `planner-worker@1.2.0`.
- No wire-protocol version change. New canonical event types serialize into
  existing journals; older journals replay because every added field carries a
  serde default.
- `ChildHandoffEntry.artifact_id` type widened from `Option<ArtifactId>` to
  `Option<String>`; UUID string values already stored remain decodable.

## Migration concerns

- Old planner journals decode without changes (serde defaults on all new
  fields). Replayed old sessions continue with the old planner graph.
- A session that crashed mid-spawn before this change recovers through the
  existing proposal/approval/created reconciliation; the new workspace-mode
  fields are absent (empty) and `task_workspace_mode` falls back to the
  policy default (`shared_read_only`).
- `futures` pin moved from `=0.3.31` to `=0.3.33` in the workspace manifest;
  the old pin was unused and conflicted with `agent-client-protocol`.

## Commands actually run

- `cargo build --workspace --all-features` — passes.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` —
  passes (0 errors).
- `cargo test --workspace --all-features` — passes except the pre-existing
  `golden_toml_and_json_are_equivalent_and_round_trip` failure caused by CRLF
  line endings in the checked-in golden TOML on this Windows checkout
  (verified failing at the base commit).
- `cargo test -p agentmod-runtime-logic --all-features` — 171 passed.
- `powershell -ExecutionPolicy Bypass -File tests/e2e/runtime_planner_worker.ps1`
  — passes: planner-worker@1.2.0, two validated structured tasks, two initial
  workers dispatched concurrently, one reviewer rejection, one revision
  worker, two exact joins, three immutable result packages, two integration
  results, artifact persistence under `sessions/<id>/artifacts/workers|integration`,
  daemon restart, and replayed inspection.

## Remaining integration steps

- The generic dispatcher core (Task 2) can reuse the wave/readiness
  primitives in `planner.rs` and `workspace.rs`; the planner adapter's
  `tasks.ready_remaining` variable contract is the narrow port for parallel
  join behavior.
- Unix process execution of `runtime_planner_worker.sh` (syntax-checked only;
  no Unix runner was available).
- Real write-tool enforcement in the child tool path (the enforcement
  decision functions are unit-tested; the deterministic fixture children carry
  no write tools, so process-level write denial is covered only at the binding
  restriction and policy layers).
- Composition-root wiring of a dependency-side workspace host for
  `independent_git_worktree`/`temporary_copy` isolation (the SDK graph
  recognizes all five modes; mode policy is canonical and enforced, but
  filesystem-level worktree/copy creation remains delegated to the Git and
  filesystem hosts).
