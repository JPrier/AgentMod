# Planner-Worker-Reviewer Productionization

## Overview

The planner-worker-reviewer style (`planner-worker@1.2.0`) executes a
deterministic, validated, production-oriented developer workflow through
runtime-managed child sessions:

```text
plan -> spawn-workers -> wait-workers -> [waves] -> integrate -> review -> revision
                                                                           \-> done
```

The runtime owns planner business behavior and workspace/result packages. The
generic dispatcher core, graph-variable core, context subsystem, plugin host,
and frontend rendering are owned by other workstreams; the planner adapter is
a narrow port over them.

## Structured planner output

A planner model response must be a bounded JSON object with at least two task
records. Every task carries:

- `task_id`, `description`, `goal`, `scope`
- `dependencies` (referenced task IDs)
- `expected_artifacts`
- `workspace_mode` (one of the five canonical modes)
- `tool_groups` (validated against the parent style's allowlist)
- `validation_commands`, `completion_criteria`, `review_criteria`
- `token_budget`, `cost_budget_micros`, `max_steps` (hard, non-zero)
- `retry_policy` (max attempts, retryable failure classes)
- `risk` (low/medium/high)

`planner::parse_and_validate_plan` rejects duplicate IDs, cyclic or missing
dependencies, unavailable tools/styles/harnesses, invalid workspace policies,
unbounded tasks, and task totals that exceed the parent session limits. The
approved plan is committed canonically (`style.task_plan_committed`) before
any child is created.

## Concurrent workers

- **Explicit concurrency**: the child policy's `max_concurrent` bounds the
  number of simultaneously Active children. Capacity is
  `max_concurrent − active` at dispatch time and never exceeds the bound.
- **Stable readiness**: a task is ready when every dependency has a
  `Completed` child record for the current iteration (or an earlier iteration
  for revision waves).
- **Deterministic ordering**: equally ready tasks dispatch in task-ID order.
- **Bounded queues and waves**: remaining dependent tasks are dispatched in
  waves. The `waves` bounded loop node (max 32) routes
  `wait-workers → waves → spawn-workers` while
  `tasks.ready_remaining == true`, keeping every cycle inside a bounded loop
  node as the SDK compiler requires (STYLE025).
- **Restart recovery**: child creation is exact-identity (execution ID, task
  ID, revision, depth, binding); recovery reconciles existing children and
  never creates a duplicate. Crash-cut tests cover plan commitment, child
  preparation, catalog write, terminal events, join readiness, and more.

## Workspace isolation

All five modes are validated and enforced:

| Mode | Enforcement |
|---|---|
| `shared_read_only` | Write-capable tool groups are removed from the child binding (`restrict_tool_groups`); write-intent process commands are denied by policy; everything else fails closed. |
| `shared_serialized_writes` | Reads run concurrently; write phases require a canonical runtime-owned lease (`workspace.lease_acquired`/`released`). At most one write-capable child runs per batch; dead owners are reconciled after restart (`workspace.lease_reconciled`). |
| `independent_git_worktree` | Requires a Git-host-backed worktree; path containment and diff/patch production are delegated to the Git host (per Task-3 generic dispatch). |
| `temporary_copy` | Requires a bounded isolated copy with ignore rules; writes are denied by the same read-only tool policy until copy plumbing is wired. |
| `explicit_custom_workspace` | Requires an approved explicit `custom_workspace` configuration block in the task. |

The decision functions are pure and unit-tested; the child binding restriction
applies the mode at creation time.

## Worker result packages

Every completed child produces one immutable, content-addressed
`WorkerResultPackage` (`worker.result_package_committed`) containing task and
child identity, style/harness/provider/model identity, summary, changed-file
list, diff/patch reference, validation commands, stdout/stderr references,
exit status, LSP diagnostics, generated artifacts, unresolved issues,
completion reason, usage, and the canonical event range. The parent commits a
bounded typed handoff (`conversation.entry_committed` with
`ChildHandoffEntry.artifact_id` = package reference) instead of the full
transcript.

## Integration

`integration::decide_integration` verifies every expected result package,
orders applied children deterministically, detects overlapping changes, and
fails closed on conflicting changes (same path, different change identity).
The immutable `IntegrationResultArtifact` records the exact applied set,
overlaps, conflicts, and validation status, and is committed as
`integration.result_committed`.

## Evidence-based review

The reviewer receives bounded real evidence: worker result package references,
integration references, completion/review criteria, and risk. Findings are
structured (`finding_id`, `severity`, `affected_tasks`, `evidence`, and
`required_correction`). A rejection targets exact tasks and creates bounded
revision tasks for those task IDs only.

## Durable child approval

When policy requires child creation approval, the runtime creates a durable
`ChildApprovalContinuation` binding the exact parent/task/style/workspace/
tools/budgets, commits `child_creation.approval_requested`, and waits. On
resolution (`child_creation.approval_resolved` via the existing
resolve-approval RPC), policy is revalidated, identity/expiry are checked, and
the child is created exactly once; duplicate decisions are idempotent.

## Layer boundaries

- **Logic** (`planner.rs`, `workspace.rs`, `result_package.rs`,
  `integration.rs`, planner adapter in `turn.rs`) owns decisions and canonical
  events.
- **Data** owns the artifact store (`read_artifact`) and continuation
  payloads.
- **Dependency** owns immutable artifact storage and durable continuation
  state.
- The composition root is unchanged; no business logic was added to it.

## Definition of done

- Plans are structured and validated; independent workers execute
  concurrently; workspace modes are enforced; child results are immutable
  artifact-backed packages; integration operates on real artifacts; reviewers
  inspect real diffs/tests; revision is targeted; child approvals are durable;
  crash tests prove no duplicate work/effects; behavior is not hard-coded to a
  fixed number of tasks or one fixture outcome; this document set is complete.
