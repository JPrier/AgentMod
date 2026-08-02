# TASK-05 — Context, Memory, and Compaction Completion

## Base commit

`abbf97b82687a4d1e7463aab33382258d6d38fd9` (verified as `main` HEAD when this
workstream began; no advance was recorded on this branch).

## Branch

`feature/context-completion` (isolated worktree `AgentMod-TASK05`).

## Mission summary

Completed AgentMod's live style-selected context system:

1. **Typed-summary compaction** executes live under pressure rules, as a
   normal model-request proposal through style/plugin/user/mandatory policy,
   with an explicitly configured provider/model or the session's deterministic
   fixture, canonical budget consumption, a bounded schema, and terminal
   provider evidence that prevents duplicate summary calls on recovery.
2. **Artifact-handoff compaction** executes live: the complete selected
   context is serialized into an immutable content-addressed artifact binding
   source ranges, hashes, security classification, and media type; a bounded
   typed artifact-reference entry replaces the projection; recovery reconciles
   receipts before any re-dispatch and branch/restart work.
3. **Automatic memory writes** execute with a full policy surface (trigger
   boundary, categories, scope, provider, bounds, dedup, retention, approval
   mode, failure behavior), following
   `proposal -> interceptors -> user policy -> mandatory policy -> dispatch
   evidence -> memory provider -> canonical reference`, with canonical
   identity deduplication that survives restart.
4. **Plugin-facing ports** for plugin memory, plugin compaction, and context
   transforms are stable, bounded, and documented in
   `docs/reference/context-plugins.md`. Transport adaptation is Task 7.

## Files changed

SDK:

- `sdk/session-style-sdk/src/model.rs` — `MemoryAutoWriteSelection` +
  trigger/category/dedup/retention/approval/failure enums;
  `SummaryCompactionSelection`; new `CompactionSelection` summary fields.
- `sdk/session-style-sdk/src/validation.rs` — STYLE032–STYLE034 validation for
  automatic writes and summary selection; availability check for explicit
  summary providers.
- `sdk/session-style-sdk/src/selection.rs` — resets new controls when
  memory/compaction are disabled.
- `sdk/session-style-sdk/src/builtins.rs` — new-field defaults.
- `sdk/session-style-sdk/src/lib.rs` — re-exports.
- `sdk/session-style-sdk/tests/style.rs` — refreshed exact cache-key fixtures
  for the new canonical manifest fields.
- `sdk/session-style-sdk/tests/context_fixtures.rs` — compiles the TASK-05
  fixture styles through the SDK.

Runtime dependency:

- `apps/runtime/dependency/src/memory.rs` — canonical `deduplication_key` on
  writes; file-memory schema v2 (v1 still verifies) and SQLite `memory_dedup`
  table; idempotent duplicate-key writes return the existing reference.

Runtime data:

- `apps/runtime/data/src/memory.rs` — dedup key and `deduplicated` flag
  plumbing.
- `apps/runtime/data/src/continuation.rs` — `MemoryWritePayloadRecord`.

Runtime logic:

- `apps/runtime/logic/src/session.rs` — canonical outbox events and records
  for typed summaries (`context.summary_*`), automatic memory writes
  (`memory.write_*`), and artifact-handoff writes (`context.artifact_*`);
  reducer transition validation, content-hash consistency, and boundary-head
  advancement for boundary-scoped outbox events.
- `apps/runtime/logic/src/compaction.rs` — bounded summary request material
  builder, context-artifact payload serialization, and their tests.
- `apps/runtime/logic/src/continuation.rs` — `MemoryWriteApprovalContinuation`
  payload.
- `apps/runtime/logic/src/turn.rs` — live summary execution, live artifact
  handoff, automatic memory-write outbox at five triggers, durable
  user-approval resolution, required-state restoration for
  summary/artifact plans, UUID-bound projection artifact references, and
  recovery ordering.
- `apps/runtime/logic/src/context_ports.rs` — stable plugin-facing ports and
  runtime validation with tests.
- `apps/runtime/logic/src/lib.rs` — module registration.

Fixtures and tests:

- `tests/fixtures/styles/persistent-file-summary.toml`,
  `persistent-file-artifact.toml`, `persistent-file-auto-write.toml`.
- `tests/e2e/runtime_context_completion.ps1` (executed) and
  `runtime_context_completion.sh` (syntax-checked).

Docs:

- `docs/architecture/context-model.md` — live summary/artifact/write behavior.
- `docs/reference/context-plugins.md` — plugin port contracts and validation.

## Public types and traits added

SDK:

- `MemoryAutoWriteSelection`, `MemoryAutoWriteTrigger`, `MemoryContentCategory`,
  `MemoryDedupPolicy`, `MemoryRetentionPolicy`, `MemoryWriteApprovalMode`,
  `MemoryWriteFailureBehavior`, `SummaryCompactionSelection`.

Runtime logic (`agentmod_runtime_logic`):

- `context_ports`: `PluginMemoryPort`, `PluginCompactionPort`,
  `ContextTransformBoundary`, `ContextTransformResult`,
  `PluginMemoryRetrieveRequest/Item/WriteProposal/WriteProposalReceipt/
  WriteCommit/WriteReceipt/Health/Bounds`, `PluginCompactionPlan/Proposal`,
  `PluginArtifactReference`, `PluginPreservedState`, `PluginContextError`,
  `validate_plugin_context_effect`, `validate_plugin_memory_write`.
- `session`: `ContextSummaryIdentity/ProposedEvent/ApprovedEvent/StartedEvent/
  CompletedEvent/FailedEvent`, `ContextSummaryRecord/State`,
  `MemoryWriteIdentity/ProposedEvent/ApprovedEvent/DispatchedEvent/
  CompletedEvent/FailedEvent`, `MemoryWriteRecord/State`,
  `ContextArtifactIdentity/ProposedEvent/ApprovedEvent/DispatchedEvent/
  CompletedEvent/FailedEvent`, `ContextArtifactRecord/State`.
- `continuation`: `MemoryWriteApprovalContinuation`.
- `compaction`: `SummaryRequestMaterial`, `ContextArtifactPayload`,
  `build_summary_request_material`, `serialize_context_artifact`.
- `turn`: `RunTurnError` additions (`AmbiguousSummaryProvider`,
  `SummaryProviderFailed`, `AutomaticMemoryWriteFailed`,
  `MemoryWriteReceiptMissing`, `ContextArtifact`, `InvalidContextArtifactReceipt`).

## Required composition-root wiring

No composition-root changes were required: `TurnLogic` already carries
`MemoryDataPort`, `ArtifactDataPort`, and the policy chain, and the daemon
already wires `RuntimeMemoryData::first_party` and
`RuntimeArtifactData::first_party`. The scheduler worker binary
(`agentmod-scheduler`) must be built for the daemon to start; the new E2E
builds it explicitly.

## Required protocol or manifest changes

- Session-style manifest schema gains optional `memory.auto_write` and
  `compaction.summary`/`summary_max_bytes`/`summary_schema_version` (all
  serde-defaulted; old manifests remain valid, new fields are canonical once
  compiled).
- New committed event types (`context.summary_*`, `memory.write_*`,
  `context.artifact_*`) flow through the existing versioned journal/replay
  envelope; they are additive.
- Continuation payload records gain a `MemoryWrite` variant (additive tagged
  JSON).

## Migration concerns

- Exact compiled-style cache keys changed because the canonical manifest now
  includes the new fields; old bindings are unavailable rather than silently
  upgraded (consistent with existing SDK policy).
- File memory records gained a schema-v2 shape with an optional
  `deduplication_key`; schema-v1 records still verify, so existing memory
  files remain readable.
- Styles with a legacy `write_policy` (e.g. `turn_completion`) now activate
  bounded default automatic writes; styles that must not write should select
  `write_policy = "never"` or `auto_write.trigger = "never"`.
- Summary compaction with no explicit `[compaction.summary]` falls back to the
  session's provider/model (the deterministic fixture in tests); an explicit
  summary provider must be available in the runtime's advertised provider set.

## Commands actually run

- `cargo test --workspace --all-features --locked` (one pre-existing CRLF
  golden-file failure in `agentmod-plugin-sdk` on this Windows checkout;
  verified pre-existing on the pristine tree).
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — clean.
- `cargo fmt --all -- --check` — clean.
- `cargo run -p xtask -- architecture --manifest-path Cargo.toml` —
  89 packages, no violations.
- `cargo test -p agentmod-runtime-logic --lib`, `-p agentmod-session-style-sdk`,
  `-p agentmod-runtime-dependency --lib`, `-p agentmod-runtime-data --lib` — pass.
- `powershell -File tests/e2e/runtime_context_completion.ps1` — **passed**:
  typed-summary compaction live, artifact-handoff compaction live with a real
  persisted artifact, automatic memory writes with provider receipts and
  canonical dedup, restart survival of all selections, and branch behavior.
- `tests/e2e/runtime_context_completion.sh` — syntax-checked
  (`bash -n`), not process-executed on this Windows host.
- `cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli -p agentmod-scheduler`.

## Remaining integration steps

- Task 7 adapts `context_ports` to plugin-host transport (no transport exists
  yet; the validation guarantees are already runtime-side).
- Optional: expose automatic-memory-write and summary/artifact evidence in the
  TUI management panels and CLI inspection surfaces.
- The Unix E2E mirror should be process-executed on a Unix host.
- `STATUS.md` update is owned by the integration owner after merge.
