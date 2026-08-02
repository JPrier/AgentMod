# TASK-08-live-providers — Integration Record

Task ID: `TASK-08-live-providers`
Branch: `feature/live-providers-harness`

## Exact base SHA

`abbf97b82687a4d1e7463aab33382258d6d38fd9` (`main` at branch time; shared with
the parallel campaign; no independent re-base was chosen).

## Scope owned

Harness/provider execution: live provider adapters, streaming normalization,
retry classification, cost/usage metadata, provider/model discovery, and a
genuinely independent second harness binary. Runtime orchestration, node
dispatch, context selection, planner-worker, plugins, and frontends were not
modified except for the additive protocol reply arm in the runtime harness
dependency.

## Files changed (40)

### Protocol

- `protocols/harness-protocol/src/lib.rs` — `Usage` gains `reasoning_tokens`,
  `estimated`, and optional `cost: CostMetadata`; new `CostMetadata` type; new
  `ProjectedEntry::Image`; new `HarnessCommand::Catalog` and
  `HarnessReply::Catalog` with `CatalogProvider`.

### Native harness N-tier

- `apps/harness/dependency/Cargo.toml` — adds async-trait, reqwest, serde,
  tokio, tokio-util, url; tempfile for tests.
- `apps/harness/dependency/src/lib.rs` — detail catalog trait
  (`ProviderCatalogDetailDependency`, `DependencyCatalogRecord`),
  `CompositeProviderCatalogDependency`, live module registration.
- `apps/harness/dependency/src/execution.rs` — async
  `ProviderExecutionDependency`, new `ProviderCancellationDependency`,
  richer failure kinds (auth, overload, invalid request, unsupported
  capability, transport, ambiguous disconnect, user cancellation),
  `Image` conversation entry, usage/cost fields, `Started`-less execution
  contract preserved for the deterministic mock.
- `apps/harness/dependency/src/live/*` — `mod.rs` (config resolution, secret
  references, execution dispatch, cancellation registry, stream budget),
  `sse.rs`, `retry.rs`, `pricing.rs`, `wire_openai.rs`, `wire_anthropic.rs`,
  `wire_gemini.rs`.
- `apps/harness/dependency/tests/live_fixtures.rs` — deterministic local HTTP
  fixtures for every live adapter wire format (13 tests).
- `apps/harness/data/src/lib.rs` — `HarnessCatalogRecord`, `HarnessCatalogData`.
- `apps/harness/data/src/execution.rs` — async execution/cancellation data,
  image/cost/usage mapping.
- `apps/harness/logic/src/lib.rs` — `LogicCatalogRecord`, `HarnessCatalogLogic`.
- `apps/harness/logic/src/execution.rs` — async execution/continuation/
  cancellation logic.
- `apps/harness/service/src/lib.rs` — catalog endpoint mapping.
- `apps/harness/service/src/execution.rs` — async wire endpoints, image/cost
  mapping, cancellation endpoint.
- `apps/harness/bin/src/lib.rs` — composite composition root.
- `apps/harness/bin/src/main.rs` — async loop, `Cancel` and `Catalog` wire
  commands, buffered frame reader, documented dev-mode switch for direct
  binary smoke tests.

### Runtime (minimal, additive)

- `apps/runtime/dependency/src/harness.rs` — matches the new
  `HarnessReply::Catalog` variant as a protocol error (the runtime never sends
  `Catalog`).

### Independent second harness

- `apps/harness-fixture/{bin,dependency,data,logic,service}/` — five crates
  with their own `service → logic → data → dependency` layers, no import of
  native harness internals.
- `apps/harness-fixture/bin/tests/process.rs` — 7 process conformance tests.
- `Cargo.toml` — workspace member `apps/harness-fixture/*`.

### Tests and docs

- `tests/e2e/independent_harness.ps1` / `.sh` — independent harness process E2E.
- `tests/e2e/live_provider_smoke.ps1` / `.sh` — opt-in live smoke, gated by
  `AGENTMOD_LIVE_SMOKE=1` and credentials.
- `docs/guides/providers.md` — provider setup guide.
- `docs/integration/TASK-08-live-providers.md` — this record.

## Public types and traits added

Protocol (`agentmod-harness-protocol`):

- `Usage { reasoning_tokens, estimated, cost }`
- `CostMetadata`
- `CatalogProvider`
- `ProjectedEntry::Image`
- `HarnessCommand::Catalog`, `HarnessReply::Catalog`

Harness dependency (`agentmod-harness-dependency`):

- `ProviderExecutionDependency` (now async), `ProviderCancellationDependency`
- `ProviderCatalogDetailDependency`, `DependencyCatalogRecord`
- `CompositeProviderCatalogDependency`
- `live::LiveProviderCatalogDependency` and `live::resolve_endpoint_config`
- `live::sse::SseParser`, `live::retry::ClassifiedFailure`,
  `live::pricing::PricingTable`
- `DependencyProviderFailureKind` extended; `DependencyCostMetadata`

Harness data/logic/service: `HarnessCatalogData` / `HarnessCatalogLogic`,
async execution/cancellation traits, `DataCostRecord` / `LogicCostRecord`.

Independent fixture: `agentmod-harness-fixture-dependency` re-implements its
own provider/catalog/cancellation types; `agentmod-harness-fixture-{data,
logic,service}` own their layer types. Identity `independent-fixture` v2.0.0.

## Required composition-root wiring

The native harness composition root now uses
`CompositeProviderCatalogDependency` (deterministic mock + live adapters).
Live providers resolve configuration from the harness process environment.

The runtime supervises the harness with a cleared child environment, so the
following wiring is required before a runtime-daemon session can reach a live
provider (documented only; orchestration logic was not modified):

1. The runtime composition root that builds `HarnessDependencyConfig` must
   forward the needed provider environment variables to the harness child
   (for example by extending the `connect()` environment passthrough with a
   configured allowlist such as `AGENTMOD_PROVIDER_*`), or
2. pass non-secret configuration through request options (`base_url`,
   `tls_verify`, `timeout_ms`) and forward secret references
   (`api_key_ref`) through the runtime's option mapping so the harness can
   resolve the key from its own environment.

Until that wiring lands, direct-binary smoke tests
(`tests/e2e/live_provider_smoke.*`) drive the harness without the daemon.

## Required protocol or manifest changes

- Protocol: additive variants/fields only; the runtime still parses every
  reply it can receive. `HarnessReply::Catalog` is treated as a protocol
  error by the runtime's `from_wire`.
- Workspace: `apps/harness-fixture/*` added to workspace members.
- Architecture metadata: new fixture crates declare
  `kind = "process-layer"` / `process = "harness-fixture"`; the binary is a
  composition root. `cargo architecture` reports 94 packages, no violations.

## Migration concerns

- The harness execution traits are now async; any external crate calling
  `HarnessExecutionLogic` / `HarnessContinuationLogic` /
  `HarnessExecutionData` synchronously must add `.await`. Only in-workspace
  harness crates consume these traits.
- `Usage` gained fields with serde defaults; older frames still deserialize.
- `DependencyProviderEvent::Completed` gained a `cost` field; all constructors
  updated.
- Live providers require an explicit `base_url` (or documented default) and,
  for paid providers, a secret reference; the deterministic mock is unchanged.

## Commands actually run (this worktree)

- `cargo check` / `cargo test` for all harness and fixture crates:
  - `agentmod-harness-dependency`: 35 unit + 13 fixture tests pass.
  - `agentmod-harness` (bin/lib), data, logic, service: unit tests pass.
  - `agentmod-harness-fixture*`: unit + 7 process conformance tests pass.
  - Combined harness+fixture run: 84 tests passed, 0 failed.
- `cargo clippy --all-targets` on all harness/fixture crates: clean
  (`-D warnings` policy).
- `cargo run -p xtask -- architecture --manifest-path Cargo.toml`:
  94 packages, no violations.
- `tests/e2e/independent_harness.ps1` executed on Windows: passed.
- `tests/e2e/live_provider_smoke.ps1` skipped by default (no credentials);
  the script exits 0 with the documented skip message.

## Verified status per provider

| Adapter | Implementation | Fixture coverage | Live claim |
|---|---|---|---|
| `openai-compatible` | production adapter | full wire fixture | not claimed without smoke |
| `openrouter` | production adapter | full wire fixture + pricing | not claimed without smoke |
| `openai` | production adapter (Chat Completions) | non-stream + stream fixtures | not claimed without smoke |
| `anthropic` | production adapter | Messages SSE fixture incl. tool use | not claimed without smoke |
| `gemini` | production adapter | streamGenerateContent fixture | not claimed without smoke |
| `local` | production adapter | stream + image + cancel fixtures | not claimed without smoke |

A live provider is claimed only after the opt-in smoke script passes against
the real endpoint with real credentials.

## Remaining integration steps

1. Runtime composition-root env/secret forwarding (see above) so the daemon
   can supervise live-provider sessions.
2. Optional: expose the `Catalog` command through the runtime RPC surface so
   `harness list` can show live provider/models.
3. Optional: cache live model discovery with refresh bounds.
4. Run opt-in live smoke tests against real OpenRouter/OpenAI/Anthropic/Gemini
   endpoints and record results separately (never required for default CI).
5. Independent-harness registration in the runtime registry
   (a `DependencyHarnessDescriptor` + `ProcessHarnessDependency` entry) once
   the integration owner approves adding a third harness entry.
