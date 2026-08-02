# TASK-07-plugin-expansion — Integration Notes

## Exact base SHA

`abbf97b82687a4d1e7463aab33382258d6d38fd9` (shared parallel-campaign base;
branch `feature/plugin-expansion`).

## What this workstream delivers

The plugin system now spans graph nodes, memory, compaction, and context
transforms through the full `service -> logic -> data -> dependency` path,
with lifecycle management below the frontend, canonical audit, explicit
observer delivery semantics, restart recovery, and idle host teardown with
lazy restart.

Definition-of-done status (evidence in `tests/` and `apps/`):

- Plugin graph-node transport and validation: protocol `ExecuteNode`, host
  worker execution, runtime adapters validating input/output schemas, run/node
  identity, undeclared effects, and canonical audit; registry resolution no
  longer rejects plugin executors (NODEX005 removed).
- Plugin memory, compaction, context transforms: protocol commands, host
  worker operations, and runtime adapters (memory writes require
  proposal/policy approval; compaction proposals are bounded; transforms run
  in compiled order and cannot touch protected keys).
- Lifecycle operations below the frontend: `PluginManagementLogicPort`
  (list/inspect/disable/quarantine/unquarantine/reload/health/audits) gated by
  a mandatory `LifecyclePolicyGate`, exposed on the runtime protocol and the
  daemon endpoint.
- Failures and deliveries canonically auditable: `plugin.audit_recorded`
  reducer events distinguish proposed/started/completed/rejected_by_plugin/
  rejected_by_runtime/timed_out/cancelled/crashed/invalid_response/
  quarantined and observer delivery outcomes; the host audit ring keeps bounded
  per-delivery terminal outcomes.
- Ambiguous invocations fail closed: no automatic redispatch of ambiguous
  host exchanges; durable deliveries past their retry budget are marked failed
  without redelivery; non-idempotent operations reject retry signals.
- Hosts shut down when idle and restart lazily: host self-teardown plus a
  runtime idle reaper; fresh hosts restore the durable loaded catalog and
  revalidate compatibility before serving.
- Dormant sessions need no retained process: process E2E proves the supervised
  host exits while a session is dormant.
- `docs/integration/TASK-07-plugin-expansion.md` (this file) is complete.

## Files changed

Protocols and SDK:

- `protocols/plugin-protocol/src/lib.rs` — protocol v2: node executor, memory,
  compaction, context transform, observer-delivery declarations; `ExecuteNode`,
  `MemoryDescribe/Retrieve/CommitWrite/Health`, `CompactionPropose`,
  `ContextTransform`, `Reload`, `Unquarantine`, `AuditList` commands and
  responses; `Observe` now carries the canonical event range; canonical audit
  outcome vocabulary (`audit_outcome`).
- `sdk/plugin-sdk/src/{lib,model,validation}.rs` — manifest sections for node
  executors, memory, compaction, context transforms, observer delivery;
  PLUG025–PLUG030 validation; golden manifest updated; `tests/expansion.rs`.
- `protocols/runtime-protocol/src/lib.rs` — plugin lifecycle wire requests
  (`PluginList/Inspect/Disable/Quarantine/Unquarantine/Reload/Health/Audits`)
  and responses (`RuntimePluginProjection`, `RuntimePluginAudit`).

Plugin host (`apps/plugin-host/`):

- `dependency/src/lib.rs` — worker operations for execute_node, memory
  describe/retrieve/commit/health, compaction proposals, context transforms;
  at-least-once durable delivery journal (event range, observer identity,
  attempts, idempotency key, retry policy, next retry); at-most-once
  deduplication; distinct audit outcomes; durable loaded-catalog persistence
  and restore; disable/quarantine/unquarantine/reload lifecycle; wrong-class
  rejection.
- `logic/src/lib.rs`, `data/src/lib.rs`, `service/src/lib.rs` — new
  operations, declarations, and lifecycle mappings through the four layers.
- `bin/src/main.rs` — startup catalog restore + delivery recovery, idle
  teardown (no active invocation, no pending delivery, durable flush), lazy
  restart revalidation via negotiate.
- `fixture-worker/src/main.rs` — deterministic fixture for all operations plus
  invalid/timeout/crash/reject behaviors and idempotency-key dedup.
- `dependency/tests/expansion.rs`, `dependency/tests/validation.rs` —
  process-level tests: node/memory/compaction/transform worker path, durable
  delivery + restart recovery, fail-closed exhausted deliveries, at-most-once
  dedup, lifecycle transitions, wrong-class rejection, catalog restore.

Runtime (`apps/runtime/`):

- `dependency/src/plugin.rs` — full transport for the new operations; lazy
  restart of the supervised host with compatibility revalidation; per-session
  idle reaper; local boundary types for memory items and transform boundaries.
- `data/src/plugin.rs` — catalog normalization of the new manifest sections;
  routing for all new operations; `node_executor_registrations()` for the
  registry; `plugin_ids()`/`manifest()` accessors.
- `data/src/node_executor.rs` — `native_with(additional)` merging plugin
  executor registrations.
- `data/src/lib.rs` — new `PluginDataPort` methods on `RuntimeData`.
- `logic/src/plugin.rs` — observer delivery audit records; non-deliverable
  `plugin.audit_recorded` events; observer enqueue with event ranges.
- `logic/src/plugin_management.rs` (new) — `PluginManagementLogicPort`,
  `LifecyclePolicyGate`, node execution with schema/identity/effect
  validation, compiled context transform pipelines, memory/compaction
  adapters, lifecycle operations; unit tests.
- `logic/src/node_executor.rs` — plugin executor resolution enabled.
- `logic/src/session.rs` — `PluginAuditRecorded` canonical event, replay state,
  known-outcome validation; reducer tests.
- `logic/src/turn.rs` — canonical commit of observer-delivery audit events.
- `service/src/turn.rs` — plugin management endpoints on the daemon;
  observer-delivery audit wiring.
- `bin/src/main.rs` — plugin host configuration (runtime API version,
  capabilities, idle shutdown), plugin node-executor registrations, plugin
  management adapter wiring.

Tests and fixtures:

- `tests/fixtures/plugins/plugin-expanded-style.toml` — style allowing the
  rewriter, durable observer, graph node, memory, compaction, and context
  transform plugins.
- `tests/e2e/runtime_plugin_expansion.ps1` — process E2E: catalog load,
  interceptor + durable at-least-once observer, canonical `plugin.audit_recorded`,
  durable delivery journal, idle host teardown, lazy restart, daemon-restart
  recovery without redelivery.

## Public types and traits added

Protocol (`agentmod-plugin-protocol`):

- `PluginNodeExecutorDeclaration`, `PluginMemoryDeclaration`,
  `PluginCompactionDeclaration`, `PluginContextTransformDeclaration`,
  `PluginContextTransformBoundary`, `PluginObserverDelivery`, `PluginMemoryItem`,
  `PluginClass::{GraphNode, Memory, Compaction, ContextTransform}`,
  `audit_outcome` constants, `CURRENT_PROTOCOL_VERSION = 2`.

Runtime protocol (`agentmod-runtime-protocol`):

- `RuntimePluginProjection`, `RuntimePluginAudit`, and the
  `Plugin*` request/response variants.

Plugin host (`agentmod-plugin-host-*`):

- `DependencyNodeExecutor`, `DependencyMemoryDeclaration`,
  `DependencyCompactionDeclaration`, `DependencyContextTransform`,
  `DependencyContextTransformBoundary`, `DependencyObserverDelivery`,
  `DependencyMemoryItem`, `DependencyMemoryResult`,
  `DurableDeliveryRecord`, `DependencyPluginStatus` (serde),
  `IsolatedPluginDependency::restore_loaded_plugins`,
  `PluginDependencyPort::{execute_node, memory, compaction_propose,
  context_transform, reload, unquarantine, deliveries, active_invocations,
  pending_deliveries, flush}`.
- Logic/data/service mirrors: `NodeExecutionCommand`, `MemoryOperationCommand`,
  `CompactionCommand`, `ContextTransformCommand`, `DeliveryRecord`, and the new
  trait methods on `PluginLogicPort`/`PluginDataPort`/`PluginHostService`.

Runtime:

- `RuntimePluginDependencyPort::{execute_node, memory, compaction_propose,
  context_transform, cancel, state_change, status, health, audits,
  deliveries}` and request/result records; `ProcessPluginDependencyConfig`
  gains `runtime_api_version`, `available_capabilities`, `idle_shutdown`.
- `PluginDataPort::{execute_plugin_node, plugin_memory,
  plugin_compaction_propose, plugin_context_transform, plugin_state_change,
  plugin_audits, plugin_health, plugin_ids, manifest}`.
- `PluginManagementLogicPort`, `LifecyclePolicyGate`,
  `AllowAllLifecyclePolicyGate`, `PluginManagementLogic<D, G>`,
  `PluginAuditRecord`, `PluginLifecycleProjection`, `PluginHostHealthProjection`,
  `ExecutePluginNodeCommand`, `PluginNodeResult`,
  `RunContextTransformsCommand`, `ContextTransformResult`, `PluginMemoryCommand`,
  `PluginCompactionCommand`, `compile_context_transform_pipeline`,
  `PluginManifestView`, `ContextTransformDeclarationView`,
  `PluginManagementError`.
- `RuntimeDaemonService::with_plugin_management`.
- Reducer: `PluginAuditRecordedEvent`, `PluginAuditRecord` replay state,
  `SessionReducerError::InvalidPluginAudit`.

## Required composition-root wiring

`apps/runtime/bin/src/main.rs` (already wired):

1. `ProcessPluginDependencyConfig` must carry `runtime_api_version` ("0.1.0"),
   `available_capabilities` (events/plugin_state/tools/memory/compaction/
   context/graph_nodes), and optionally `idle_shutdown` from
   `AGENTMOD_PLUGIN_IDLE_TIMEOUT_MS`.
2. `RuntimeNodeExecutorData::native_with(plugin_data.node_executor_registrations())`
   merges plugin graph-node executors into the runtime registry when a plugin
   catalog is configured.
3. `RuntimeDaemonService::with_plugin_management(Arc<PluginManagementLogic<
   RuntimeData, LifecyclePolicyGate>>)` exposes lifecycle endpoints; the gate
   is currently `AllowAllLifecyclePolicyGate` — wire the runtime permission
   chain here before release (see "Remaining integration steps").

## Required protocol/manifest changes

- Plugin manifests may now declare `[[node_executors]]`, `[memory]`,
  `[compaction]`, `[[context_transforms]]`, and `[observer_delivery]` (mode
  `best_effort` | `at_most_once` | `at_least_once` with `max_attempts` and
  `retry_backoff_ms`). See `tests/fixtures/plugins/plugin-expanded-style.toml`
  and the expansion E2E manifest writers.
- `Observe` now carries `event_range_start`/`event_range_end`; the runtime
  binds the authorization digest to the exact range, so the host verifies the
  same tuple.
- Plugin wire protocol version advanced to 2; host negotiation rejects older
  versions.

## Migration concerns

- `CURRENT_PROTOCOL_VERSION` is 2; the plugin host and runtime dependency must
  be deployed together (negotiation is fail-closed).
- The `PluginManifestDataRecord` gained a `category` field and new sections;
  the canonical manifest JSON changed (additive with serde defaults), so
  existing plugin catalogs still compile.
- `PluginObservationSummary` gained `audits`; serialized summaries now include
  delivery audit records (callers that construct it need the new field).
- Observer at-least-once deliveries now persist a durable journal under
  `.agentmod/plugin-state/deliveries.json` (generation files) in each session
  directory; pre-existing deployments have no journal and start clean.
- The loaded-plugin catalog is persisted at `.agentmod/plugin-state/loaded.json`
  so a fresh host restores plugins after idle teardown; restoring a plugin
  whose executable is no longer approved quarantines it (fail closed).

## Commands actually run (Windows MSVC, Rust 1.91.1)

- `cargo check --workspace` — clean.
- `cargo check --workspace --all-targets` — clean (no warnings).
- `cargo test --workspace --all-targets` — all suites pass except the
  pre-existing `agentmod-session-style-sdk` golden CRLF test, which also fails
  on the untouched base worktree under this machine's `core.autocrlf=true`
  (environmental line-ending issue, not introduced here).
- `cargo test -p agentmod-plugin-sdk`, `cargo test -p agentmod-plugin-host-dependency`
  — pass (including the new process-level expansion suite).
- `cargo test -p agentmod-runtime-logic` — 137 pass.
- `cargo fmt --all -- --check` — clean after formatting.
- `tests/e2e/runtime_plugin_expansion.ps1` — passes end to end (catalog,
  durable observer, canonical audit, idle teardown, lazy restart,
  daemon-restart recovery).
- `tests/e2e/runtime_plugin_composition.ps1` — still passes (no regression).

## Remaining integration steps

1. **Lifecycle policy gate**: replace `AllowAllLifecyclePolicyGate` in the
   composition root with a gate backed by the runtime proposal/permission
   chain so consequential lifecycle actions traverse normal proposals and
   mandatory policy.
2. **Frontend surfaces**: CLI/TUI plugin management commands and panels
   (owned by the frontend workstreams; the runtime protocol endpoints are
   ready).
3. **First-party context integration**: route style-selected memory,
   compaction, and provider-projection boundaries through
   `PluginManagementLogicPort` (`run_context_transforms` at
   Before/AfterMemoryRetrieval, Before/AfterCompaction,
   BeforeProviderProjection, BeforeTurnCompletion) once Task 5's component
   selection lands; the adapter and fixtures are in place.
4. **Generic node dispatch**: Task 2's dispatcher can now resolve plugin
   executors (registry + validation + transport are ready); the turn-loop
   dispatch for plugin-owned node kinds should consume
   `PluginManagementLogicPort::execute_node`.
5. **Unix E2E**: add `runtime_plugin_expansion.sh` (syntax parity with the
   PowerShell script).
6. **Dependency policy**: `cargo deny check` reconciliation remains a
   pre-existing repository-level item (unversioned path deps, xtask license,
   webpki-roots CDLA allowance).
