# Session-Style Refocus Implementation Map

Status: Phases 1-6 executable; Phase 7 introspection vertical slice live

Branch: `feature/session-style-registry`

Baseline commit: `e99e9e1bf02f10475ada0a48fcb746f9fa1ead6b`

Verified: 2026-07-28 on Windows

## Verified current state

AgentMod already has the low-level kernels and process boundaries this phase must
preserve. The session-style SDK owns five built-in manifests, strict TOML/JSON
parsing, compatibility validation, graph and interceptor compilation, and
compatibility-bound cache keys. The graph engine, event pipeline, conversation
projection, compaction strategies, memory providers, plugin SDK/host, runtime
proposal and permission path, harness supervision, canonical journal, artifacts,
continuations, receipts, replay, branching, scheduling, and first-party tool
hosts are live and tested.

The production runtime now consumes `agentmod-session-style-sdk` through an
N-tier registry. Session creation and deliberate restyling resolve and compile
the selected manifest, persist the full immutable binding, and validate that
binding before execution resumes. The CLI and TUI expose style discovery and
selection through the runtime protocol.

The generic style executor consumes the retained compiled graph.
Persistent-chat, ephemeral-turn, research-loop, the deterministic declarative
graph, and planner-worker-reviewer execute through runtime-owned node adapters.
Style-selected memory retrieval, context composition, projection replacement,
and live compaction run before provider requests with canonical provenance.
Plugin-sourced styles can activate process blocking interceptors and
committed-event observers; activation and blocking invocation state is
canonical and replay-inspectable.
Native and deterministic fixture harnesses are registered through an injected
N-tier adapter catalog. Style requirements and explicit client overrides are
capability-checked before creation, and the exact adapter identity is retained
and revalidated across restart.

The following documents contradict the implementation and require reconciliation:

- Remaining reconciliation work is tracked in `STATUS.md`; architecture and
  plugin references now distinguish the live slices from planned extensions.

There is no `core/conversation-projection` crate. The live equivalent is
`apps/runtime/logic/src/conversation.rs`.

## Baseline evidence

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | passed |
| `cargo test --workspace --all-targets --all-features --locked` | passed; 351 tests enumerated |
| `cargo test --workspace --doc --all-features --locked` | passed; no doctests |
| `cargo run --locked -p xtask -- architecture --manifest-path Cargo.toml` | passed; 89 packages, no violations |
| `cargo test --locked -p xtask --test architecture` | passed; 2 tests |
| `cargo deny check` | fails on existing workspace dependency/license policy configuration; see `STATUS.md` |
| `cargo audit` | passed; zero known vulnerabilities and one allowed unmaintained `fxhash 0.2.1` warning |

This is Windows evidence only. Existing Unix scripts are not execution evidence.

## N-tier implementation map

### Runtime dependency

- Add bounded style-manifest discovery for configured user, project, and plugin
  roots.
- Read TOML/JSON bytes and disable markers without interpreting business rules.
- Read and atomically write compiled-cache bytes.
- Persist complete immutable session style descriptors and locks.
- Preserve atomic session/branch directory creation and journal ordering.

### Runtime data

- Own built-in, configured, and plugin-provided catalog records.
- Parse and compile manifests through `agentmod-session-style-sdk`.
- Build the runtime compile context from explicit capabilities, providers, tool
  groups, plugins, memory providers, and compaction strategies.
- Normalize sources, diagnostics, availability, and cache records into
  data-owned types.
- Maintain an injected, bounded compiled-style cache; do not use a global
  mutable locator.

### Runtime logic

- Own selector parsing, ID/version resolution, precedence, compatibility,
  activation, immutable binding identity, restart validation, and branch style
  semantics.
- Reject unavailable, disabled, ambiguous, missing, or incompatible styles.
- Map data records into logic-owned descriptors and session bindings.
- Keep dynamic session and capability decisions here while reusing SDK
  diagnostics instead of duplicating compiler rules.

### Runtime service

- Add list, inspect, validate, and compile style endpoints.
- Map runtime wire DTOs only at the service boundary.
- Resolve and bind a selected style during session creation.
- Surface style compatibility and complete binding details during session
  inspection.

### Frontends

- CLI: add `style list`, `style inspect`, `style validate`, and `style compile`;
  keep `session create --style`, add style-file/selection overrides as the
  runtime contracts become available, and render full session binding details.
- TUI: replace hard-coded `/new` behavior with explicit style selection and
  expose catalog/details/compatibility in the initial style-focused flow.
- ACP remains protocol-driven and must not import runtime internals.

## Durable binding

Every newly created or deliberately restyled branch will persist:

- style ID and semantic version;
- canonical manifest content hash;
- compiled-style cache key;
- source and relevant plugin-set hash;
- capability-set hash and runtime API version;
- style-specific configuration;
- memory and compaction selections;
- selected tool groups and harness requirement;
- execution budgets and permission defaults.

The binding is committed in canonical creation history and written atomically to
session metadata/style descriptors. Existing ID-only sessions remain readable
for replay, but continuation of a legacy or incompatible binding must fail with
an explicit migration/replacement diagnostic rather than silently substituting
another style.

## Delivery sequence

1. Registry and immutable session binding, including protocol, CLI, TUI,
   restart validation, branch semantics, and focused process E2E.
2. Generic graph executor state and transitions; route persistent chat through
   it before adding more styles.
3. Style-selected context, memory, compaction, provenance, and context
   pipelines.
4. Ephemeral turn, research loop, declarative graph, then planner-worker-reviewer
   and runtime-managed child sessions.
5. Live plugin composition for interceptors, observers, styles, and context
   components.
6. Harness registry, capability negotiation, native descriptor, and
   deterministic secondary fixture.
7. Complete frontend and introspection surfaces.
8. Recovery matrix, benchmarks, traceability, documentation, and Windows/Unix
   process-level acceptance evidence.

## Phase 6 verified result

Completed 2026-07-28 on Windows. The runtime dependency registry owns exact
adapter descriptors and routes approved model work by retained harness ID; data
and logic expose a normalized catalog, deterministic capability hashes,
availability, and compatibility decisions. Session selection persists adapter
ID/version/capability hash and binds model proposals, grants, canonical request
events, and restart validation to that identity. CLI and command-driven TUI
selection operate only through the runtime protocol.

`tests/e2e/runtime_harness_selection.ps1` passed with native and independently
supervised fixture sessions, a negative image-capability style, canonical
identity assertions, daemon restart, and successful post-restart fixture
execution. The Unix script passes syntax validation but has not been
process-executed. The fixture defaults to the same credential-free deterministic
harness executable through a separate adapter/process configuration; complete
third-party harness implementations are not claimed.

## Phase 7 introspection vertical-slice result

Completed 2026-07-28 on Windows. Runtime logic now derives a stable bounded
`style_introspection` projection solely from immutable session binding and
canonical replay state. CLI inspect/replay receive the projection through the
existing runtime protocol, and the TUI retrieves it through its own
dependency -> data -> logic -> service chain for a live Graph view. The
projection covers compiled graph/control/progress, conservative next-transition
visibility, remaining canonical budgets, pipelines, memory provenance,
compaction, children/joins/reviews, and termination without dispatching an
effect.

The extended `runtime_research_loop.ps1` passed with three iterations, restart,
and pure replay assertions, and `runtime_tui_smoke.ps1` remained green. Unix
automation is syntax-checked only. Cost/duration accounting, observer-order
receipts, and a canonical conditional-variable environment remain incomplete
and are reported as unknown rather than inferred.

## Phase 1 proof obligations

- Five built-ins list and inspect through the live daemon.
- Invalid and incompatible manifests return stable SDK-derived diagnostics.
- User, project, and plugin manifest fixtures are discovered with explicit
  source and availability.
- Two style selections produce distinct immutable bindings.
- Restart retains and validates each binding.
- Branch inheritance preserves the exact parent binding; an explicit branch
  style resolves and binds the requested replacement.
- CLI and TUI select styles without bypassing the runtime.
- Existing replay, branch, turn, tool, approval, receipt, scheduler, and
  architecture tests remain green.

## Phase 1 verified result

Completed 2026-07-28 on Windows. The runtime registry now owns five built-ins
plus bounded user, project, and plugin sources, explicit disablement, SDK-derived
diagnostics, content-addressed compiled caching, exact semantic-version
selection, and immutable activation bindings. New sessions and deliberately
restyled branches persist schema-v2 metadata plus full style and compiled locks.
Canonical `session.created` history contains the binding, so replay and restart
reconstruct it without opening a separate mutable catalog record.

Inspection reports `compatible`, `incompatible`, or `migration_required`.
Execution validates the exact persisted binding lazily before a turn or
continuation resumes; unavailable or changed styles fail closed and are never
replaced. CLI style commands and the TUI Styles view operate through protocol
boundaries. `tests/e2e/runtime_style_registry.ps1` passed with two styles,
restart continuation, branch restyling, disablement, and durable-file checks.
The matching Unix script is implemented but is not execution evidence.

Full workspace tests, strict Clippy, formatting, and the 88-package architecture
check pass. One process-host cancellation test timed out during the first
workspace run, passed immediately in isolation, and the complete workspace run
then passed.

## Phase 2 verified result

Completed 2026-07-28 on Windows. `CompiledStyleExecutor` loads the exact retained
SDK descriptor, verifies all binding hashes and identities, selects graph nodes
and transitions, and emits canonical initialization, node, and transition
events. Persistent-chat compatible graphs use this executor while provider,
tool, permission, receipt, continuation, and recovery behavior remains on the
existing runtime paths. Unsupported compiled graphs are rejected before turn
history changes.

## Phase 3 vertical-slice evidence

The initial slice passed its Windows checks on 2026-07-28. Session bindings now retain retrieval timing,
query construction, memory write policy and injection location, compaction
budgets, and preservation requirements. The runtime routes no-memory, file, and
SQLite FTS retrieval through dependency -> data -> logic, applies item/byte and
scope limits, authorizes context and compaction proposals, and records complete
memory and projection provenance. No compaction, sliding-window, and
tool-output-eviction strategies execute live; summary/artifact modes fail
closed without approved material.

`tests/e2e/runtime_style_context.ps1` passed with file-versus-none context and
provenance assertions plus 18-turn no-compaction-versus-sliding-window
comparison. Provider projections differed as configured while canonical
conversation history remained identical. The matching Unix script was syntax
checked but has not been executed.

Independent review then identified remaining release-blocking hardening:
branch-to-no-memory projection cleanup, enforcement of projection and
preservation limits, exact retrieval lifecycle timing on resumed model calls,
projection-pressure-based compaction triggering, and stronger storage-isolation,
SQLite, restart, and boundary E2Es. Phase 3 remains active until those findings
are resolved and reverified.

## Later acceptance accounting

The twelve orchestration scenarios in the phase request are release-blocking for
this refocus. Manifests, mocks, Unix scripts, or a parallel hard-coded chat loop
do not satisfy them. Each scenario requires canonical event/artifact assertions,
process-level restart evidence where specified, and verified Windows and Unix
execution before this phase may be reported complete.
