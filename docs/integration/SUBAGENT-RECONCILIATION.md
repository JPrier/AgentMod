# Subagent Reconciliation Inventory

This document records the full reconciliation analysis of the eight parallel
audit branches against the converged runtime base, the selection decisions for
every branch, and the traceability of what was integrated, ported, or
explicitly discarded. It is the authoritative record for the final mainline
reconciliation commit.

## 1. Reference SHAs

| Role | SHA |
|------|-----|
| Stale public base (used by Tasks 2–8) | `abbf97b82687a4d1e7463aab33382258d6d38fd9` |
| **Converged authoritative base** | `5274b830d0622ac8985766a0da0da75a2d26128a` (`feat(runtime): implement generic node execution`) |
| Protected recovery branch | `recovery/converged-5274b83` → `5274b830d0622ac8985766a0da0da75a2d26128a` |
| Integration branch | `integration/subagent-reconciliation` (from `5274b83…`) |

## 2. Branch heads, merge bases, and unique commits

Merge base is computed against the converged base `5274b83…`; all branches
other than Task 1 were created from the stale base, so their merge base with
the converged tree is `abbf97b…`.

| Branch | Head SHA | Merge base (vs converged) | Unique commits |
|--------|----------|---------------------------|----------------|
| `audit/task-01-execution-plan` | `df024900155fc7b454432dad219bccd31fabdc6e` | `5274b83…` (descendant) | `9a64404`, `04133c2`, `219f3b4`, `df02490` |
| `audit/task-02` | `101e656cc4222f83083a23c11c92fa668ebfb2db` | `abbf97b…` | `226df3c`, `101e656` |
| `audit/task-03-native-control-nodes` | `aeb3f7bfe41920171c586daba364e0ee5ad62cad` | `abbf97b…` | `229dd19`, `aeb3f7b` |
| `audit/task-04-graph-state` | `d1abd04c6be4de91e4280e6c48fb3a9fececfc67` | `abbf97b…` | `4437ba6`, `f846fda`, `c31a58a`, `84d62c9`, `d1abd04` |
| `audit/task-05-context-completion` | `0fe260bed7f4ff492b625b6ccb59ed69332324db` | `abbf97b…` | `8038bee`, `9a847d5`, `a26daa3`, `9faaf3b`, `3c3f270`, `d98054e`, `545bd7b`, `bf55053`, `0fe260b` |
| `audit/task-06-planner-worker` | `04fe7b8d54ea60078d9d4c8008a1972968cf39be` | `abbf97b…` | `2fa9bf5`, `d2f2e75`, `f29bdc5`, `049cf32`, `04fe7b8` |
| `audit/task-7` | `98a110994dd203000d6ae5f0fd93ccd004e813cf` | `abbf97b…` | `5cb53f6`, `7cb0ddd`, `32df27d`, `649d0dd`, `98a1109` |
| `audit/task-08` | `b8c2e206183cc65d2027d72a94d3054eb0ddcc91` | `abbf97b…` | `dfa8919`, `d084a47`, `9358c21`, `0016107`, `25e28cf`, `b8c2e20` |

## 3. Classification summary

| Branch | Classification | Rationale |
|--------|----------------|-----------|
| Task 1 (`audit/task-01-execution-plan`) | **Integrate substantially** | Immutable execution-plan mirror persistence, corruption classification, restart mismatch diagnostics, migration typing, inspection projections. Already validated on the previous local main (`df02490` = branch head). |
| Task 2 (`audit/task-02`) | **Mine tests only** | Duplicate generic dispatch engine; converged base has production generic dispatch. Property and transition-order tests are additive. |
| Task 3 (`audit/task-03-native-control-nodes`) | **Mine tests only** | Duplicate native control-node state machine; converged base has production control nodes. Select validation cases ported into the converged executor. |
| Task 4 (`audit/task-04-graph-state`) | **Mine tests only** | Duplicate `core/graph-state` crate; converged base has live canonical variables and budgets. Property/golden determinism concepts ported as tests where semantically applicable. |
| Task 5 (`audit/task-05-context-completion`) | **Integrate selectively** | Most features already present. Model-generated summary strategy is genuinely additive and integrated as a separate explicit strategy. |
| Task 6 (`audit/task-06-planner-worker`) | **Mine tests only** | Planner-worker v1.2 duplicates converged v1.4. Task-schema validation cases ported where the v1.4 graph does not cover them. |
| Task 7 (`audit/task-7`) | **Reference only** | Plugin protocol v2 downgrade of converged protocol v10. No production code merged; narrow validation cases reviewed against the converged suite. |
| Task 8 (`audit/task-08`) | **Integrate substantially** | Live provider adapters, SSE parser, retry classification, pricing/cost metadata, provider catalog, and an independent second harness binary. |

## 4. Task-by-task analysis

### Task 1 — `audit/task-01-execution-plan`

**Feature claims**

- Dedicated execution-plan dependency, data, and logic modules (n-tier).
- Atomic checksummed `execution-plan.json` persistence staged at session,
  branch, and child-session creation.
- Corruption classification (missing/corrupt/truncated/checksum).
- Restart mismatch diagnostics and executor/registry drift detection.
- Migration typing and inspection/availability projections.
- Tests for corrupt or missing plan records.

**Equivalent features already in the converged base**

- Exact per-node executor resolutions, registry hashes, plan hashes, and
  immutable execution-plan identity retained in the style binding and
  canonical execution state (`node_executor.rs`, `registry.rs`, `turn.rs`).
- Canonical `session.created` / execution-initialization evidence.
- `UnsupportedGenericExecutionPlan` fail-closed preflight.

**Genuinely additive behavior**

- A verified immutable **mirror** of the canonical plan on disk
  (`execution-plan.json`), with checksum and size bounds.
- Corruption classification and restart diagnostics that do not exist in the
  base.
- Inspection projection exposing node availability and migration typing.
- Atomic staging on session/branch/child creation.

**Alternative implementation approaches**

- (a) Mirror file as a separate non-authoritative artifact (selected).
- (b) No separate file, relying solely on in-memory canonical binding
  (rejected: loses corruption/restart diagnostics).

**Tests unique to Task 1**

- Corrupt mirror file, truncated mirror file, binding/mirror mismatch,
  executor version drift, registry hash drift, legacy session without mirror,
  branch with new execution plan (in `tests/integration/src/lib.rs` and
  `apps/runtime/logic/src/execution_plan.rs`).

**Migration risks**

- A second persisted representation could become a competing authority.
- Mitigated by the rule: the mirror is never authoritative; any
  binding/mirror mismatch fails closed; missing legacy mirrors produce an
  explicit migration diagnostic.

**Protocol conflicts**

- None. `execution-plan.json` is a new local artifact, not a protocol DTO.

**Selected integration action**

- **Integrate substantially** via the four validated commits already on the
  previous local main. Single-source-of-truth rule documented in
  `docs/architecture/execution-plan.md`.

### Task 2 — `audit/task-02`

**Feature claims**

- Generic node dispatch engine (`apps/runtime/logic/src/node_execution/`):
  outcome, recovery, reducer, transition modules, `dispatch_tests.rs`.

**Equivalent features already in the converged base**

- Persisted exact executor resolution, exact-ID routing, generic node
  execution, arbitrary graph execution, generic recovery, versioned
  compatibility adapters, Windows/Linux process E2Es.

**Genuinely additive behavior**

- Property tests: transition-order independence
  (`single_eligible_selection_is_order_and_repeat_independent`), repeated
  selection determinism on built-in graphs, structurally-different
  compatible-graph equivalence.

**Alternative implementation approaches**

- (a) Second generic dispatch engine (rejected — duplicate authority).
- (b) Port property tests into the existing converged dispatcher (selected).

**Tests unique to Task 2**

- `every_built_in_style_executes_through_the_generic_dispatch_path`,
  `style_id_does_not_affect_identical_compiled_semantics`,
  `single_eligible_selection_is_order_and_repeat_independent`,
  `repeated_selection_never_diverges_on_built_in_graphs`.

**Selected integration action**

- **Mine tests only.** Production `node_execution/` subsystem rejected and
  documented as discarded. Property tests ported against the converged
  `node_execution.rs` executor.

### Task 3 — `audit/task-03-native-control-nodes`

**Feature claims**

- Child messages, joins, parallel branches, delays, schedules, event emission
  as separate executors with a 23-event alternative state machine
  (`apps/runtime/logic/src/node_executors/`).

**Equivalent features already in the converged base**

- Production runtime paths for child messages, joins, parallel branches,
  delays, schedules, event emission via canonical state, scheduler
  continuations, recovery, and cross-platform tests.

**Genuinely additive behavior**

- Edge-case tests: delay wake-time recorded once/resumes exactly once, delay
  expiry without wake dispatch, delay cancellation, schedule removal,
  event-namespace restrictions.

**Alternative implementation approaches**

- (a) Merge the parallel event/state model (rejected — duplicate state
  machine).
- (b) Port selected validation cases into the converged native executor
  (selected).

**Tests unique to Task 3**

- `delay_records_wake_time_once_then_resumes_exactly_once`,
  `pending_delay_expiry_resolves_without_wake_dispatch`,
  `pending_delay_cancellation_cancels_the_continuation`; join
  optional/required members; parallel merge conflicts; schedule
  cancellation; event namespace restrictions.

**Selected integration action**

- **Mine tests only.** The alternative state machine is discarded; the
  converged native executor semantics already cover the behavior. Where a
  Task 3 test asserts a case the converged suite does not already assert
  (delay expiry, schedule cancellation), the case is ported.

### Task 4 — `audit/task-04-graph-state`

**Feature claims**

- `core/graph-state` crate: canonical typed values, declarations, merge
  policies, expression projections, parallel safety checks, budget ledger,
  property tests, golden serialization tests.

**Equivalent features already in the converged base**

- Live canonical-variable and budget system integrated into graph
  compilation, runtime canonical events, replay, generic node execution,
  parallel branches, conditions, and process E2Es.

**Genuinely additive behavior**

- Property-based deterministic replay tests (proptest) for random assignment
  order, prefix equivalence, and budget reconstruction.
- Golden serialization test vectors.

**Alternative implementation approaches**

- (a) Replace the production system with the isolated crate (rejected —
  would change canonical event schemas, recovery semantics, and risk
  behavior drift).
- (b) Port determinism/replay property tests into the existing
  implementation (selected).

**Tests unique to Task 4**

- `replay_reconstructs_identical_live_state`, `replay_is_prefix_equivalent`,
  `assignment_order_does_not_change_environment`,
  `merge_result_is_independent_of_contributor_order`,
  `budget_reconstruction_is_exact_under_random_commits`,
  golden `graph-state-events-v1.json`.

**Selected integration action**

- **Mine tests only.** `core/graph-state` crate is discarded; the production
  canonical-variable system is retained unchanged. Property concepts are
  ported as tests against the live `canonical_variables`/coordinator where
  they exercise the same invariants without changing behavior.

### Task 5 — `audit/task-05-context-completion`

**Feature claims**

- Live typed-summary compaction, artifact-handoff compaction, automatic
  memory writes, plugin-facing memory/compaction/context-transform ports,
  canonical outbox events, deduplication keys surviving restart.

**Equivalent features already in the converged base**

- Deterministic typed-summary compaction, artifact-handoff compaction,
  automatic first-party and plugin memory writes, plugin memory retrieval,
  plugin compaction, plugin context transforms, durable receipts, crash
  recovery, Windows/Linux process tests, DLP-style filtering.

**Genuinely additive behavior**

- **Model-generated summary strategy**: an explicit provider/model-selected
  summary request executed through the normal proposal → interceptors →
  policy → dispatch → terminal-evidence path, with:
  - selected summary provider/model (from `SummaryCompactionSelection`),
  - canonical `context.summary_*` outbox events,
  - exact source range and request hash,
  - durable provider terminal receipt (no duplicate summary call after
    restart),
  - fail-closed ambiguous completion (`AmbiguousSummaryProvider`),
  - explicit configuration and inspection,
  - offline deterministic test provider path.

**Alternative implementation approaches**

- (a) Replace the deterministic strategy with the model-generated one
  (rejected — deterministic strategy is production-tested).
- (b) Add model-generated summaries as an explicit separate strategy
  (selected).

**Tests unique to Task 5**

- `summary material and context artifact payload invariants`
  (`3c3f270`), automatic-write edge cases, dedup-survives-restart tests,
  `context_fixtures.rs` SDK fixture compiles.

**Migration risks**

- Two summary strategies must not both become authoritative. The strategy is
  selected by the style binding; the deterministic strategy remains the
  default when no model-generated selection exists.

**Protocol conflicts**

- None at the wire level; new canonical event types are additive.

**Selected integration action**

- **Integrate selectively.** Model-generated summary strategy added as a
  separate strategy behind explicit configuration. Deterministic strategy
  retained as the default. Context/memory/compaction subsystems already
  present in the base are not merged.

### Task 6 — `audit/task-06-planner-worker`

**Feature claims**

- Planner-worker v1.2: structured planner tasks, concurrent workers,
  workspace isolation, result packages, crash-cut matrix.

**Equivalent features already in the converged base**

- Planner-worker v1.4 with genuinely overlapping child turns, runtime-owned
  branch workspaces, child-owned edit/test/diff artifacts, deterministic
  integration, structured review rejection, revision, parent-workspace
  integrity, Windows/Linux validation.

**Genuinely additive behavior**

- Task-schema validation (`planner.rs`): dependency-cycle detection, missing
  dependencies, total budget overflow, retry policy parsing, workspace-mode
  validation.

**Alternative implementation approaches**

- (a) Downgrade to the v1.2 planner model (rejected).
- (b) Port validation rules and tests into the v1.4 path (selected).

**Tests unique to Task 6**

- `valid_plan_with_dependencies_parses`, `invalid_workspace_policy_is_rejected`,
  dependency-cycle tests, lease-recovery edge cases.

**Selected integration action**

- **Mine tests only.** v1.2 production code is discarded; v1.4 remains
  authoritative. Validation cases that the v1.4 graph path does not already
  cover are ported.

### Task 7 — `audit/task-7`

**Feature claims**

- Plugin protocol v2: plugin nodes, plugin memory, plugin compaction, context
  transforms, lifecycle, observer delivery, recovery, idle teardown.

**Equivalent features already in the converged base**

- Plugin protocol **v10** (`CURRENT_PROTOCOL_VERSION = 10`) with exact node
  execution, plugin state CAS, context transforms, memory/compaction,
  automatic plugin memory, lifecycle recovery, durable observer delivery,
  guarded idle teardown, Windows/Linux process tests.

**Genuinely additive behavior**

- Narrow manifest-validation cases (executor declaration required, memory
  category requirements, compaction category requirements, transform
  declaration requirements, at-least-once bounded retry policy).

**Alternative implementation approaches**

- (a) Merge/downgrade the plugin protocol to v2 (rejected).
- (b) Review only (selected): the converged suite already asserts the same
  validation invariants against protocol v10.

**Tests unique to Task 7**

- `graph_node_executor_declaration_is_accepted_and_round_trips`,
  `graph_node_plugin_without_executor_is_rejected`,
  `memory_declaration_requires_memory_category`,
  `compaction_declaration_requires_compaction_category`,
  `at_least_once_observer_delivery_requires_bounded_retry_policy`.

**Selected integration action**

- **Reference only.** No protocol or runtime files merged. Validation cases
  reviewed; where the converged v10 suite lacks an equivalent assertion, the
  invariant is confirmed covered by `plugin_schema.rs` tests or left as
  reference for the audit trail.

### Task 8 — `audit/task-08`

**Feature claims**

- OpenAI-compatible provider, OpenRouter, OpenAI, Anthropic, Gemini, local
  OpenAI-compatible HTTP, SSE parsing, provider-specific request/response
  mapping, retry classification, usage metadata, cost metadata, pricing
  support, provider catalog protocol, image provider entries, independent
  second harness (`apps/harness-fixture`), deterministic HTTP fixtures,
  provider documentation, optional real-provider smoke tests.

**Equivalent features already in the converged base**

- Provider-neutral dependency contract (`DependencyProviderExecutionRequest`,
  deterministic mock provider), static provider catalog
  (`StaticProviderCatalogDependency`), sync `ProviderExecutionDependency`
  trait, runtime harness registry with `native` and `fixture` adapters,
  supervised process harness, exact session-harness binding, canonical usage
  and budget state.

**Genuinely additive behavior**

- Live HTTP provider adapters (wire mapping for OpenAI/OpenRouter/Anthropic/
  Gemini), bounded incremental SSE parser, provider-neutral retry
  classification, pricing tables and computed cost metadata, provider catalog
  protocol messages (`HarnessCommand::Catalog`,
  `HarnessReply::Catalog`, `CatalogProvider`, `CostMetadata`), image input
  support, an independent second harness binary with its own n-tier
  implementation, deterministic HTTP fixture tests, provider documentation,
  opt-in live smoke scripts.

**Alternative implementation approaches**

- (a) Async conversion of the entire harness stack (Task 8's approach —
  rejected: would rewrite production harness logic, cancellation, and
  supervision).
- (b) Port the live provider stack synchronously on top of the existing sync
  trait, preserving the converged harness protocol and supervision (selected).

**Migration risks**

- Provider secrets must remain environment references; plaintext `api_key`
  options must fail closed.
- TLS verification defaults to enabled; custom endpoints require explicit
  configuration.
- The independent harness must be registered in the runtime registry without
  disturbing the exact session-harness binding.
- Provider completion receipts must remain durable; ambiguous disconnect must
  fail closed and never redispatch.

**Protocol conflicts**

- `HarnessCommand::Catalog` / `HarnessReply::Catalog` are additive wire
  variants; `Usage` gains `#[serde(default)]` fields (reasoning_tokens,
  estimated, cost), preserving backward compatibility with older harnesses.

**Selected integration action**

- **Integrate substantially.** Live provider adapters, SSE parser, retry
  classification, pricing/cost metadata, and provider catalog ported onto the
  converged sync trait. Independent harness (`apps/harness-fixture`)
  registered in the runtime harness registry. Deterministic HTTP fixture
  tests and runtime-supervised fixture E2Es added. Real credentialed smoke
  tests remain opt-in.

## 5. Canonical-authority matrix (post-integration)

| Concern | Authoritative owner | Non-authoritative mirrors/indexes |
|---------|--------------------|-----------------------------------|
| Execution plan | Canonical style binding + `session.created` evidence | `execution-plan.json` mirror (verified, never overrides) |
| Graph variables | `canonical_variables`/coordinator in runtime logic | — |
| Budget state | Canonical session budget events | — |
| Native control-node state | Canonical session events (delay/schedule/join/parallel) | — |
| Plugin lifecycle | Canonical plugin lifecycle events (protocol v10) | — |
| Observer delivery | Canonical observer events | — |
| Memory-write state | Canonical `memory.write_*` outbox events | — |
| Context compaction state | Canonical `context.summary_*` / `context.artifact_*` events | — |
| Planner-worker state | Canonical child-session events (v1.4) | — |
| Provider usage/cost | Canonical provider completion events | `CostMetadata` projection |

## 6. Security review notes

- Provider API keys are resolved exclusively from environment references or
  `file:` references; passing a plaintext `api_key` option is rejected.
- Secrets never enter normal events, logs, or request options.
- TLS peer verification defaults to enabled; `tls_verify=false` requires
  explicit configuration.
- Live provider endpoints follow the network policy; custom base URLs require
  explicit configuration.
- Provider response bodies and SSE streams are bounded (line/event byte
  bounds, event counts).
- Image and tool-call inputs are bounded and security-classified.
- Harness and plugin capabilities remain explicit in the registry/manifest.
- Ambiguous provider disconnect and summary completion fail closed and are
  never automatically redispatched.

## 7. Discarded implementations (documented)

| Discarded subsystem | Branch | Reason |
|---------------------|--------|--------|
| `apps/runtime/logic/src/node_execution/` generic dispatch engine | Task 2 | Duplicate of the converged generic dispatcher. |
| `apps/runtime/logic/src/node_executors/` 23-event control-node state machine | Task 3 | Duplicate of converged native control-node execution. |
| `core/graph-state` crate | Task 4 | Duplicate of the live canonical-variable/budget system; replacing would risk canonical schema and recovery drift. |
| Task 5's duplicate artifact/memory/plugin context subsystems | Task 5 | Already present and production-tested in the converged base. |
| Planner-worker v1.2 production code | Task 6 | v1.4 is strictly more advanced and validated. |
| Plugin protocol v2 (runtime, host, SDK) | Task 7 | Downgrade of protocol v10. |
| Async conversion of the harness stack | Task 8 | Would rewrite production harness logic; sync port preserves the converged protocol and supervision. |

## 8. Ported tests / integrated hardening

- Task 1: execution-plan mirror corruption, truncation, mismatch, drift,
  legacy-migration, branch-plan tests (integrated with the four Task 1
  commits).
- Task 2: generic-dispatch property tests (order independence, repeat
  determinism, style-id independence) ported onto the converged dispatcher.
- Task 3: delay expiry/cancellation, schedule cancellation, event-namespace
  validation cases ported onto the converged native executor.
- Task 4: deterministic replay property tests and golden-vector concepts
  ported against the live canonical-variable implementation.
- Task 5: model-generated summary strategy with canonical outbox events,
  receipts, fail-closed ambiguity, and offline deterministic test path.
- Task 6: task-schema validation rules (cycles, missing dependencies, budget
  overflow) ported into the v1.4 path where not already covered.
- Task 8: provider adapters, SSE parser, retry classification, pricing/cost,
  catalog protocol, independent harness, fixture tests, runtime-supervised
  provider fixture E2Es.

## 9. Integration order and commits

1. `tmp: integrate execution-plan mirror hardening` (Task 1)
2. `tmp: add live provider adapters` (Task 8 dependency layer)
3. `tmp: add provider catalog protocol` (Task 8 protocol)
4. `tmp: register independent harness` (Task 8 runtime registry)
5. `tmp: add model-generated summary strategy` (Task 5)
6. `tmp: port graph-state property tests` (Task 4)
7. `tmp: port dispatcher and control-node edge tests` (Task 2/3)
8. `tmp: port planner validation cases` (Task 6)
9. `tmp: reconcile documentation`
10. Final squashed commit after full validation.

All temporary commits are squashed into a single final reconciliation commit
before updating `main`.

## 10. Validation status

Linux (Ubuntu/WSL2) process E2Es executed against the reconciled tree:

- Arbitrary graph A, B, B-cancellation, C (plugin node), schedule: **passed**
- Planner-worker v1.4: **passed**
- Typed summary, artifact handoff, artifact-handoff finalize: **passed**
- Automatic memory, session-completion memory: **passed**
- Plugin context, plugin automatic memory, plugin node executor, MCP OAuth,
  ACP/MCP branch, TUI rich attachments, scheduler, scheduler recovery: **passed**
- Harness selection (independent harness), runtime-supervised live-provider
  fixture: **passed**
- `runtime_plugin_lifecycle`: **pre-existing converged-base failure** in the
  startup lifecycle-recovery phase (plugin post-receipt cut marker is never
  reached after daemon restart). Reproduces identically on the unmodified
  `5274b83` tree and was recorded as failing in the July 31 validation log
  (`all_scripts_passed: False`). Not caused by this reconciliation.

Two stale E2E scripts were corrected to match the converged generic contract:
- `runtime_typed_summary.sh`: the converged generic executor legitimately
  retains the user input plus the canonical tool-call request/result pair as
  conversation entries (count 2 -> 3); the summary-specific assertions
  (schema, projection method, restart, replay) were already correct.
- `runtime_automatic_memory.sh`/`.ps1`: replaced the sed-rewritten legacy
  fixture with a dedicated `automatic-memory-file.toml` that uses the generic
  `model_request`/`complete_turn` configuration contract and configures the
  filesystem host for the fixed `filesystem.read` gate.

Static validation gates against the reconciled tree:

- `cargo fmt --all -- --check`: **passed**
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`:
  **passed**
- `cargo test --workspace --all-targets --all-features --locked`: **passed**
  (124 test binaries, 0 failures; includes the two ported property tests and
  the summary outbox/material tests)
- `cargo test --workspace --doc --all-features --locked`: **passed**
- `cargo run --locked -p xtask -- architecture`: **passed** (95 packages)
- `cargo test --locked -p xtask --test architecture`: **passed**
- `cargo deny check`: **passed** (advisories, bans, licenses, sources)
- `cargo audit`: **passed** (1 pre-existing allowed warning: `fxhash`
  unmaintained)

Windows: the named-pipe daemon startup ("local runtime endpoint is invalid")
reproduces identically on the unmodified base in this environment and is
pre-existing; the Windows E2E variants are provided and the Linux/WSL2 matrix
above is the executed cross-platform evidence for this reconciliation.
