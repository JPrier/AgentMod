# Integration record — TASK-04-graph-state

## Exact base SHA

`abbf97b82687a4d1e7463aab33382258d6d38fd9` (verified equal to `origin/main`
at task start; no shared base advance was observed).

## Branch

`feature/graph-state` (isolated worktree `../AgentMod-TASK04`).

## Files changed

New:

- `core/graph-state/Cargo.toml`
- `core/graph-state/src/lib.rs`
- `core/graph-state/src/value.rs`
- `core/graph-state/src/declare.rs`
- `core/graph-state/src/event.rs`
- `core/graph-state/src/state.rs`
- `core/graph-state/src/reduce.rs`
- `core/graph-state/src/budget.rs`
- `core/graph-state/src/expression.rs`
- `core/graph-state/src/parallel.rs`
- `core/graph-state/src/port.rs`
- `core/graph-state/tests/determinism.rs` (property/replay/golden tests)
- `core/graph-state/tests/golden/graph-state-events-v1.json`
- `apps/runtime/logic/src/graph_state.rs`
- `docs/architecture/graph-state.md`
- `docs/integration/TASK-04-graph-state.md`

Modified:

- `apps/runtime/logic/Cargo.toml` (added `agentmod-graph-state`,
  `agentmod-expression-engine` deps)
- `apps/runtime/logic/src/lib.rs` (registered `pub mod graph_state`)
- `core/graph-state` (new) reflected in `Cargo.lock`

No files owned by other workstreams were edited (verified `git status` shows
only the files above).

## Public types and traits added

Core crate `agentmod-graph-state`:

- `value::GraphValue`, `value::Decimal`, `value::ApprovalDecision`,
  `value::SecretReference`, `value::canonical_value_bytes`,
  `value::MAX_DECIMAL_SCALE`
- `declare::VariableDeclaration`, `declare::VariableType`,
  `declare::VariableScope`, `declare::MutabilityPolicy`,
  `declare::SecurityClassification`, `declare::MergePolicy`,
  `declare::LastWriterOrdering`, `declare::BranchScopePolicy`,
  `declare::DeclarationSet`, `declare::DeclareError`
- `state::GraphState`, `state::ReadOutcome`, `state::AssignmentSource`,
  `state::MergeContribution`, `state::RejectionReason`,
  `state::validate_value_for`, `state::GraphStateError`
- `event::GraphStateEvent`, `event::BudgetEvent`
- `reduce::GraphStateReducer`, `reduce::ReducerError`
- `budget::BudgetLedger`, `budget::BudgetLimits`, `budget::BudgetDimension`,
  `budget::UsageKind`, `budget::UsageEvidence`, `budget::PricingBinding`,
  `budget::ChildBudgetReport`, `budget::RollupPolicy`, `budget::BudgetCell`,
  `budget::ConcurrentGauge`, `budget::BudgetDecision`, `budget::BudgetError`
- `expression::ConditionVerdict`, `expression::evaluate_condition`,
  `expression::COUNTERS_ROOT`
- `parallel::validate_parallel_write_safety`, `parallel::ParallelBranchPlan`,
  `parallel::ParallelSafetyReport`, `parallel::ParallelWriteVerdict`
- `port::GraphStateReadPort`, `port::BudgetReadPort`,
  `port::ExecutionGraphState`

Runtime logic `agentmod-runtime-logic`:

- `graph_state::SessionGraphState`,
  `graph_state::SessionGraphInitializationEvents`,
  `graph_state::RuntimeGraphStatePort`, `graph_state::GraphStateLogicError`

## Required composition-root wiring

None in this task. The session projection (`SessionGraphState`) is a pure
reducer; generic dispatch consumes it through `RuntimeGraphStatePort`
(`GraphStateReadPort` + `BudgetReadPort`). Wiring into the turn executor —
constructing `SessionGraphState` at session start, `check` before dispatch,
`commit` after completion — is the generic dispatch workstream's integration
step (see Remaining integration steps).

## Required protocol or manifest changes

None. No protocol DTO, style manifest, plugin manifest, or frontend contract
changed. The core crate carries `[package.metadata.agentmod] kind = "core"`,
`domain = "graph-state"`; the runtime logic crate already carries its layer
metadata, so the architecture check stays clean (90 packages, no violations).

## Migration concerns

- `SessionStyleBudgets.max_steps` maps to ledger `max_style_steps`,
  `max_tokens` to `max_total_tokens`, `max_cost_micros` to
  `max_provider_cost_micros`, and `max_duration_ms` to the explicitly
  selected wall-clock ceiling. Sessions created before budget wiring still
  replay; only new executions consume the ledger.
- `serde_json` in this workspace is built with `preserve_order`, so canonical
  bytes are produced only from `BTreeMap`-ordered sources; values must never
  be projected from `HashMap` iteration order.
- Decimals project into condition environments as `{unscaled, scale}`; the
  expression language has no float constants, so decimal ordering comparisons
  in conditions are not expressible (evaluation returns a stable
  invalid-expression outcome).
- `GraphState::apply_event` is the raw replay surface; stream-level callers
  should prefer `GraphStateReducer` which enforces initialization ordering.

## Commands actually run

On Windows (Rust 1.91.1 MSVC), in the task worktree:

```shell
cargo check -p agentmod-graph-state
cargo test  -p agentmod-graph-state            # 33 unit + 9 determinism + 9 state-contract, all pass
cargo clippy -p agentmod-graph-state --all-targets -- -D warnings   # clean
cargo test  -p agentmod-runtime-logic --lib graph_state              # 4 tests pass
cargo test  -p agentmod-runtime-logic                                # 134 pass
cargo clippy -p agentmod-runtime-logic --all-targets -- -D warnings  # clean
cargo check --workspace                                              # builds
cargo test --workspace --all-features                                # 205 pass; 1 pre-existing failure (see below)
cargo clippy --workspace --all-targets --all-features -- -D warnings # clean
cargo run -p xtask -- architecture --manifest-path Cargo.toml        # 90 packages, no violations
cargo fmt --all -- --check                                           # clean
```

## Pre-existing environmental failure (not caused by this workstream)

`agentmod-session-style-sdk::golden_toml_and_json_are_equivalent_and_round_trip`
fails identically at the untouched base commit `abbf97b` on this Windows
machine: `core.autocrlf=true` checks `custom-style.toml` out with CRLF endings
(the committed blob has LF), so the manifest parsed from TOML differs from the
LF golden JSON in the inline graph source. Verified in a scratch clone of the
base commit; no file owned by this task is involved.

## Definition-of-done evidence

| Criterion | Evidence |
|---|---|
| Graph variables typed and canonical | `GraphValue` + `VariableType` with bounds; 32 unit tests (value/declare/state) |
| Conditions deterministic after restart | `evaluate_condition` from canonical variables + counters; proptest `assignment_order_does_not_change_environment`, `counters_are_canonical_inputs` |
| Parallel write safety machine-validated | `validate_parallel_write_safety`; tests for reject/last-writer/union/undeclared/immutable |
| Consumed by generic dispatch through narrow port | `GraphStateReadPort`/`BudgetReadPort`/`RuntimeGraphStatePort`; port tests in core + logic |
| All budget dimensions with known/estimated/unknown | `BudgetLedger` cells; `final_action_consumes_budget`, `unknown_cost_remains_unknown_never_zero`, `estimated_usage_counts_conservatively` |
| Replay reconstructs exact remaining budget | `restart_reconstruction_reproduces_exact_remaining`, `budget_reconstruction_is_exact_under_random_commits`, proptest `replay_reconstructs_identical_live_state` |
| No external SDK/frontend type leaks into core | core deps are only primitives/expression-engine/event-model/serde; architecture check clean |
| `docs/integration/TASK-04-graph-state.md` complete | this file |

## Remaining integration steps (owned by other workstreams)

1. Generic dispatch: construct `SessionGraphState` at style-run start from
   compiled declarations and `SessionStyleBudgets`; journal
   `SessionGraphInitializationEvents`.
2. Generic dispatch: `check` each budget dimension before dispatching the next
   consequential action and `commit` exact evidence after completion; journal
   `BudgetEvent`s; record blocked checks.
3. Generic dispatch: map compiled graph node read/write scopes and merge
   policies into `DeclarationSet` and call `validate_parallel_write_safety`
   during runtime-executability validation before session mutation.
4. Context/memory workstreams may read the deterministic `environment`/
   `budget_environment` projections; they must not mutate canonical state.
5. A process-level E2E (style run exercising variables, merges, and budget
   exhaustion across a daemon restart) belongs to the integration owner after
   dispatch wiring lands.
