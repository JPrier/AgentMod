# AgentMod Status

Last updated: 2026-07-28

AgentMod begins from an empty upstream repository. This file reports only verified
implementation state; planned interfaces and scaffolds do not count as implemented.

## Current phase

Session-style refocus, Phase 4 — additional built-in execution modes. The
registry, immutable session binding, generic persistent/ephemeral/research/declarative executor,
and style-selected context composition are live. No/file/SQLite memory and
none/sliding-window/tool-output-eviction compaction run through recoverable
canonical lifecycle boundaries before provider requests. Research loops run
bounded fresh-context iterations with immutable findings, and the compiled
declarative fixture executes branch, approval, native-tool, loop, and terminal
nodes. Planner-worker-reviewer now has a typed, atomically created child-session
substrate but its plan/spawn/join/review graph adapter is not live.
Summary/artifact compaction, automatic memory
writes, plugin composition, and harness selection remain incomplete.

## Completed capabilities

| Capability | Status | Evidence |
|---|---|---|
| Repository orientation | Integration tested | Empty upstream clone confirmed; Rust 1.91.1 Windows MSVC toolchain recorded |
| Product/acceptance plan | Implemented and unit tested | `.omx/plans/prd-agentmod.md` |
| Verification contract | Implemented and unit tested | `.omx/plans/test-spec-agentmod.md` |
| Initial architecture/process/dependency maps | Implemented and unit tested | `docs/architecture/initial-maps.md` |
| Cargo architecture enforcement | Integration tested | `cargo run -p xtask -- architecture --manifest-path Cargo.toml`: 88 packages, no violations; intentional negative fixture tests pass |
| Versioned protocol framing/negotiation | Implemented and unit tested | Bounded CBOR frame and capability/version tests in `agentmod-protocol-support` |
| Runtime N-tier health slice | Integration tested | Real dependency → data → logic → service → composition-root path; layer tests and runnable binary |
| Harness N-tier health/capability slice | Integration tested | Separate harness binary and four layer test suites |
| Headless CLI daemon/session slice | End-to-end validated | Four-layer CLI uses authenticated local RPC for human/batch JSON plus live NDJSON turns, durable session create/list, point-in-time inspect/replay, and atomic branch; Windows named-pipe process E2Es pass and Unix automation is present |
| Terminal frontend vertical slice | End-to-end validated | Independent Ratatui `service → logic → data → dependency` crates use only the runtime protocol; authenticated bootstrap, lazy session listing, canonical-history paging, multiline UTF-8 editor/history, session switching, Chat/Events/Context/Help views, committed live streams with credit acknowledgements, cancellation, permission modal, token accounting, and terminal restoration are implemented. Rendering and logic tests pass; `runtime_tui_smoke.ps1` proves health/session mapping and one canonically committed provider stream through the real Windows named pipe, with a Unix script present. Management panels and rich content remain pending |
| ACP frontend vertical slice | End-to-end validated | Independent `service → logic → data → dependency` crates use the official `agent-client-protocol` 2.0 SDK and stable ACP wire version 1. Capacity-one layer-owned streams preserve runtime credit-window backpressure and emit each `session/update` without turn buffering; dropped client streams issue runtime cancellation, and active cancellation is registered before the endpoint spawns response work. The stdio adapter negotiates capabilities, creates and loads runtime sessions, accepts text/resource-link prompts, and maps durable approval, denial, and cancellation outcomes without duplicate execution. Real Windows process E2Es prove incremental delivery, immediate and mid-stream provider cancellation, approval/denial, and cancellation of a running process tool with exact canonical terminal events; Unix equivalents are present. Rich content and per-session MCP declarations remain pending |
| Event envelope and integrity | Implemented and unit tested | Typed envelopes, classifications, BLAKE3 integrity and tamper tests |
| Blocking event pipeline | Implemented and unit tested | Deterministic compile/order, decisions, timeouts, failure policies and bounded observers |
| Continuation resume-once core | Implemented and unit tested | Concurrent resume, cancel and expiry tests |
| JSONL journal dependency | Integration tested | Real filesystem append/scan, checksum chain, sequence/duplicate rejection, concurrent append, trailing recovery and interior quarantine tests |
| Canonical conversation projection | Implemented and unit tested | Structured entries and replacement provenance preserve canonical history without fake user messages |
| Pure session replay reducer | Implemented and unit tested | Integrity, sequence, approval-once, lifecycle, replay-to-sequence and context-replacement tests |
| Durable replay and branching | End-to-end validated | Runtime inspect/replay endpoints reduce verified journal prefixes without dispatching effects. Small branches remap exact structured history; histories over 32 entries or 64 KiB atomically store a hash-bound private/session-retained context artifact and a maximum 16-entry/64-KiB live projection with explicit ancestry/provenance. `runtime_replay_branch.ps1` and `runtime_large_branch_artifact.ps1` prove exact small branches, bounded large branches, independent continuation, and unchanged parents; Unix automation is present |
| Permission precedence kernel | Implemented and unit tested | Deterministic rules and mandatory-policy-last deny precedence |
| Content-addressed artifact storage | Integration tested | Transactional chunked writes, BLAKE3 verification, deduplication, bounded ranges and cleanup tests |
| Validated immutable snapshots | Integration tested | Atomic writes, journal anchors, reducer/schema compatibility, corruption isolation and latest-valid selection tests |
| Expression and graph kernels | Implemented and unit tested | Constrained expression evaluation plus bounded graph parsing, validation and compilation tests |
| Durable tool approvals | End-to-end validated | Session-scoped checksum-protected pending-action records, four-layer CLI resolution, continuation-resume interception, mandatory-policy revalidation, daemon restart before approval, no pre-approval execution, approved dispatch, structured denial, canonical model continuation, and duplicate-resolution idempotency pass `tests/e2e/durable_tool_approval.ps1` on Windows. Replay retains bounded terminal outcomes and exact action digests, repairs an absent or call-only provider conversation pair without redispatch, and rejects mismatched digests, reversed pairs, and conflicting history; canonical dispatch outbox plus request-bound terminal receipts reconcile post-dispatch crashes without re-execution in `runtime_tool_receipt_recovery.ps1` |
| Deterministic harness mock provider | Integration tested | Full harness N-tier mappings; text, streaming, tool-call, malformed, timeout, rate-limit, partial-failure, cancellation, usage and disconnect scenarios |
| Runtime action interception | Implemented and unit tested | Style-before-plugin ordering, exact replacement audit, final-proposal permission evaluation and mandatory-deny enforcement |
| Deterministic compaction strategies | Implemented and unit tested | No-op, sliding window, typed summary, artifact handoff and artifact-safe tool-output eviction with source provenance |
| Native process host | End-to-end validated | Authenticated foreground/background execution plus native PTY start/run, interactive input, resize, detach/reattach, merged durable terminal output, restart-persistent replay denial, strict owner/session scope, secret references, executable policy, and bounded concurrency/waiters pass. Recovery records commit before dispatch, bind PID + OS start time + resolved executable, reject PID reuse, preserve completed output, classify exact surviving children without redispatch, and quarantine malformed records. The host exposes authenticated versioned Unix-socket/Windows-pipe transport, survives runtime-client replacement in a separate process group, retains live PTY handles across reconnect, and exits after its last live child and request. A real Windows daemon E2E starts one PTY, kills and replaces the runtime, reattaches through the surviving host, exchanges canonical input/output, commits exit, and proves no redispatch; equivalent Unix automation is present. Typed `process.reconciliation_started/completed` events form one reducer-enforced pair around reattachment and precede terminal tool state. A separate forced host-crash test proves no redispatch and fail-closed inherited-handle recovery |
| Native filesystem host | Integration tested | Separate N-tier host with bounded read/list/glob/grep, atomic write/edit, prevalidated multi-file patch, encoding/binary handling, lazy schemas and path/symlink/device/sensitive-file controls; 13 tests |
| Plugin manifest SDK | Implemented and unit tested | Strict TOML/JSON model, PLUG001–PLUG024 validation, authority/trust/capability/version checks and cross-plugin ordering diagnostics; 12 tests |
| Harness continuation gate | Integration tested | Tool-call generation stops at a proposal; explicit runtime continuation issues a fresh provider request exactly once, with replacement structured context |
| Native Git host | End-to-end validated | Discovery/status/diff, detached worktrees, commit-free integrity-checked checkpoints and guarded restore have 9 host tests; runtime routing uses keyed grants and `tests/e2e/runtime_git_loop.ps1` validates a real repository status round trip |
| Native LSP host | End-to-end validated | Separate five-crate host implements LSP 3.17 lifecycle, all required query/edit-proposal operations, cancellation, timeout, restart, workspace containment, keyed authorization and deterministic fixture coverage; runtime project-root routing passes a process E2E |
| Session-style SDK | Implemented and unit tested | Five built-ins, strict owned TOML/JSON manifests, STYLE001–STYLE029 validation, graph/pipeline compilation, availability/budget checks, inspectable descriptors and compatibility-bound cache keys; 22 tests. Enabled child policies now select an exact child style, workspace mode, inheritance, context/token/cost budgets, tools, memory access, join/cancellation semantics, and reviewer bound. `persistent-chat@1.1.0` and `planner-worker@1.1.0` carry that complete policy; exact old bindings remain unavailable rather than being silently upgraded. `research-loop@1.1.0` declares the live bounded fresh-context/model/tool/artifact/loop graph, and `declarative-graph@1.1.0` supplies the live branch/approval/tool/bounded-loop fixture |
| Runtime session-style registry and binding | End-to-end validated on Windows | Runtime dependency discovers bounded user/project/plugin TOML/JSON sources and disable markers and persists compiled cache records; data compiles through the SDK and owns catalog/cache records; logic owns exact ID/version selection, compatibility, immutable binding, and fail-closed restart validation; service exposes list/inspect/validate/compile and binds creation/branch operations. Complete identity, manifest, compiled descriptor, memory/compaction/tool/harness/budget/permission selections are canonical and atomic in schema-v2 session metadata, `style.json`, and `style.lock`. CLI commands and the TUI Styles view select live styles. `runtime_style_registry.ps1` proves two distinct durable bindings, persistent and ephemeral restart continuation, branch restyling with continued parent and branch execution, and no fallback after disablement; the Unix equivalent has been syntax-checked but not process-executed |
| Generic runtime session-style executor | End-to-end validated for persistent chat on Windows | Runtime logic consumes the exact SDK-compiled graph retained by the immutable session binding, verifies all cache identity hashes, maps every compiled node kind to a runtime-owned directive, and rejects missing or ambiguous transitions. Canonical `style.execution_initialized`, `style.node_entered`, `style.node_completed`, `style.node_failed`, and `style.transition_selected` events reconstruct active/completed/failed nodes and transitions during replay without dispatching effects. Persistent chat follows `respond → tool → done` through this executor while its node adapters reuse the existing provider authorization, harness, tool proposal, permission, receipt, continuation, and assistant-commit paths. Turn-scoped provider failures clear the active node canonically; retained graph and style step limits fail closed. Windows durable-turn, streaming, reconnect, tool, process, approval, cancellation, registry/restart, and workspace tests pass; Unix equivalents are updated but not executed |
| Ephemeral-turn style execution | End-to-end validated on Windows | The SDK-compiled `fresh-context → respond → tool → done` graph executes through the generic style executor. Each turn authorizes and canonically records one current-turn-only provider projection, retains typed canonical user/assistant history without fabricated handoff messages, and authorizes a phase-bound empty projection before completing the turn. Exact graph-edge, run, request, provider, model, options, and input identities govern recovery; journal-cut tests cover fresh replacement, context-to-model transition, assistant commit, discard phase, and discard boundary without duplicate user commits or provider redispatch. `runtime_ephemeral_turn.ps1` proves two isolated turns across restart, empty dormant projection, complete canonical history, and no turn-one input/output in turn two's fresh provider projection; the Unix equivalent has been syntax-checked but not process-executed |
| Research-loop style execution | End-to-end validated on Windows | The SDK-compiled `fresh-context → research → tool → persist → repeat` graph executes through the generic style executor with a deterministic, style-bounded completion criterion. Every iteration receives a fresh provider projection, can execute native tools and resume approvals, commits visible output, persists a policy-approved immutable JSON finding through a canonical proposal/approval/dispatch/completion outbox, and records loop transitions and terminal lifecycle state. Replay retains structured provider tool proposals so restart cannot change finding bytes; exact request hashes reject changed provider/model/options/criteria. Unit crash matrices cover assistant, policy, dispatch, receipt, node, loop, transition, terminal lifecycle, tool, and approval cuts without ambiguous redispatch. `runtime_research_loop.ps1` proves three findings, three inspectable iterations, daemon restart, and pure replay; the Unix equivalent is syntax-checked but has not been process-executed |
| Declarative-graph style execution | End-to-end validated for the built-in fixture on Windows | The generic executor recognizes the compiled five-node graph semantically rather than by style ID, binds caller-controlled inputs canonically before entry, selects both branch outcomes from compiled expressions, creates cursor/cache/request-bound style approval continuations, executes the declared `filesystem.read` through the normal proposal/policy/grant/host/receipt path, enforces the compiled loop bound, and terminalizes the session. Reducer evidence binds approval completion to its exact continuation and tool completion to a terminal call receipt. Runtime rejects secondary tool-policy approval and interceptor replacement for this minimal adapter until their exact style-owned resume data can be retained, rather than inventing a harness continuation. `runtime_declarative_graph.ps1` proves three loop iterations, native tool calls, daemon restart at approval, resume-once approval, duplicate-resolution idempotency, inspection, and pure replay; its Unix equivalent is syntax-checked but not process-executed |
| Runtime-managed child-session substrate | Integration tested | Runtime logic owns exact parent proposal, graph node, task, revision, depth, style, and token-budget identity. Runtime data/dependency atomically create a fresh worker journal containing `session.created` plus `child_session.linked`; worker metadata is catalogued under distinct child-parent fields rather than branch ancestry. Recovery scans the parent proposal key and then replays the candidate journal to verify every typed field before accepting it. Child execution projects a canonical `PendingTask` into an ephemeral fresh context and does not commit a fabricated user message. Parent-side child creation events enforce Proposed → Approved → Created ordering. Focused reducer, dependency atomic-tree, typed-projection, and existing ephemeral recovery tests pass. Planner task planning, policy dispatch, joins, results, review artifacts, and revision execution remain incomplete. |
| Style-selected context, memory, and compaction | End-to-end validated for no/file/SQLite memory and none/sliding compaction on Windows | The SDK compiles retrieval timing, query construction, write policy, injection location, reserved context tokens, projection limits, and typed preservation requirements with fail-safe schema-v1 defaults. Runtime data routes no-memory, checksum-protected file memory, and SQLite FTS through distinct selected dependencies with session isolation plus item, query-byte, contribution-byte, projection-token, and hard serialized-byte bounds. Context construction, replacement, and compaction proposals traverse the existing style/plugin/user/mandatory pipeline; canonical boundary/phase events enforce exact memory-before-compaction ordering, bind retries to provider/model/options/current-input identity, recompute projection measurements during replay, recover completed phases exactly once, and fail closed after an ambiguous interceptor start. Canonical replacements preserve full conversation history and complete provenance. `runtime_style_context.ps1` proves no/file/SQLite selection, isolation, restart retrieval, branch-to-no-memory cleanup, limits, first-turn pressure, reserved budgets, and none-vs-sliding projection differences while preserving canonical history. Summary/artifact handoff and automatic writes remain incomplete; the Unix process script has only been syntax-checked |
| Runtime local RPC transport | Integration tested | Bounded framed negotiation, mandatory bootstrap-token authentication, concurrent local socket/named-pipe connections, request dispatch, ordered `StreamItem`/`StreamEnd` frames, committed-sequence binding, and bounded channel backpressure tests. A terminal-only style turn is explicitly framed as `StreamEnd` even when it emits zero provider events |
| Runtime↔harness durable turn | End-to-end validated | CLI create/run traverses authenticated named pipe, runtime replay/commit, ordered interception and policy, short-lived keyed grant, supervised harness, deterministic provider, canonical proposal/approval/started/delta/completion events, and assistant commit; Windows E2E passes and Unix automation is present |
| Runtime provider stream cancellation | End-to-end validated | Harness lifecycle events cross the process boundary as individual bounded frames, are committed one at a time, and cross runtime RPC as ordered bounded stream frames; caller-selected cancellation IDs travel through four-layer CLI/runtime mappings, active cancellation interrupts and drops the harness child, partial visible output and cancellation are committed without completion, and a fresh request reconnects in `runtime_stream_cancel.ps1`; Unix automation is present |
| Live headless CLI turn stream | End-to-end validated | `agentmod run --stream-json` maps bounded stream types through dependency → data → logic → service, validates RPC identity and monotonic frame order, flushes each NDJSON event only after its canonical commit, and ends with one sequence-bound terminal record; `runtime_cli_stream.ps1` proves the first frame is observable before provider completion and Unix automation is present |
| Credit-window flow control and reconnect | End-to-end validated | Runtime protocol 2.1 negotiates request-bound credit windows; the server emits one initial nonterminal frame and blocks until an exact sequence acknowledgement grants more capacity. `agentmod session events <id> --after <sequence> --limit <n>` scans verified canonical history into bounded pages, reports stable cursors/head/`has_more`, and `runtime_session_reconnect.ps1` proves pages 1–19 contain no gaps or duplicates, including the style graph events; Unix automation is present |
| Cross-host terminal receipts | End-to-end validated | The supervised runtime dependency atomically persists checksum- and exact-request-bound terminal event streams before returning them to logic; receipt-only recovery skips an already committed host-event prefix and refuses missing/corrupt/conflicting receipts. A forced daemon kill after a real filesystem write and receipt but before terminal journal commit recovers with the filesystem host deliberately unavailable, proving no redispatch; Unix automation is present |
| Startup-wide tool reconciliation | End-to-end validated | Before opening RPC, the runtime service scans every verified receipt, reduces the corresponding canonical journal, reconciles ordinary nonterminal dispatches from the receipt without spawning the disabled host, and classifies already-terminal, orphaned, and approval-owned receipts. `runtime_startup_tool_recovery.ps1` proves the recovered session can continue; approval-owned receipts remain with the resume-once continuation path, and Unix automation is present |
| Runtime filesystem tool loop | End-to-end validated | `tests/e2e/runtime_tool_loop.ps1` passes CLI → named-pipe runtime → keyed harness proposal → ordered runtime authorization → keyed per-session filesystem process → structured tool result → explicit harness continuation; Unix automation is present |
| Runtime process tool loop | End-to-end validated | `tests/e2e/runtime_process_loop.ps1` passes CLI → runtime → harness → ordered authorization → keyed per-session process host → captured output/result → explicit continuation; executable policy is deny-by-default and Unix automation is present |
| Multi-tool batch join | End-to-end validated | One provider response can propose multiple safe tool calls; runtime commits and executes each result before one resumed provider request, while the harness resolves all sibling continuation aliases as one batch. Windows `runtime_multi_tool_loop.ps1` passes and a Unix script is present |
| Extended runtime host routing | End-to-end validated | Git status, offline Web search, LSP project-root detection, MCP server listing, and a configured external MCP stdio invocation traverse CLI → runtime → harness proposal → policy → isolated host → canonical progress/result → provider continuation in dedicated Windows E2Es; Unix scripts are present |
| Autonomous coding loop | End-to-end validated | `tests/e2e/coding_task.ps1` reads a real Rust project, performs an intentionally incomplete edit, records a failing `cargo test`, fixes the source, records a passing test, and independently re-runs the final test; Unix automation is present |
| Durable session catalog | Integration tested | Runtime endpoint creates the complete required directory atomically with initial canonical event and lists bounded dormant metadata without loading conversation state |
| Replaceable memory providers | Integration tested | No-memory, checksum-protected file memory, and bundled `SQLite` FTS5 ranked retrieval pass dependency/data/logic tests; unapproved writes are rejected before data access and injection provenance is complete |
| Native Web host | End-to-end validated | Separate N-tier host implements authenticated HTTP, HTML fetch, deterministic/Brave search, dependency-reconstructed action grants, restart-persistent replay denial, per-hop DNS/domain/private-IP/redirect policy, secret references, cancellation, bounded projections/concurrency and atomic artifact overflow; offline runtime search routing passes a process E2E |
| Native browser host | End-to-end validated | Separate five-crate WebDriver host implements lifecycle, rendered navigation/inspection, final-URL policy, screenshot/download artifacts, CSS click/type/form submission, health, cancellation, keyed grants, durable replay denial and shutdown. A real compiled WebDriver fixture drives nine operations through runtime and provider continuation in `runtime_browser_loop.ps1`; Unix automation is present |
| Durable scheduling | End-to-end validated | A separate five-crate worker and dedicated protocol implement authenticated negotiation, checksum-protected one-time/recurring/runtime-event/process-output schedules, portable atomic replacement recovery, complete execution policy, deterministic create-once claims with claim timestamps, idempotent terminal markers, corruption rejection and restart deduplication. Runtime and CLI have layer-local management/claim/execute mappings; the daemon automatically polls time triggers and observes newly committed canonical ranges for exact event IDs and process/log-stream byte ranges. Typed deferred turns are bound to an exact session, schedule and trigger proof, reject manual approval, enforce expiry against the durable claim time, transition resume-once, and then re-enter the normal intercepted provider path. Startup enumerates durable nonterminal claims: claims without canonical dispatch provenance are safely resumed, canonical terminal outcomes are reconciled without redispatch, and ambiguous in-flight work fails closed. A canonical `scheduler.delivery_reconciled` event precedes the worker terminal marker so recovery itself is repeatable. `runtime_scheduler.ps1` proves deferred wakeup; `runtime_scheduler_recovery.ps1` kills the daemon before dispatch and again after canonical model completion, then proves exactly one provider execution, reconciliation event, and terminal marker. TUI management remains pending |
| Native MCP host | Partially implemented | Five-crate process implements initialization/version negotiation, lazy tools/resources/prompts discovery, namespacing, stdio and Streamable HTTP, session IDs, progress, cancellation, shutdown, inert managed catalog, deterministic mock and real compiled stdio fixture. Negotiated HTTP calls and resumptions send the required `MCP-Protocol-Version` header. Multi-event SSE and bounded resumption carry exact session/event cursors. Checksum-protected HTTP recovery state binds server configuration, runtime owner/session, negotiated version, MCP session, cursor, pending JSON-RPC request and normalized operation digest; a reconstructed dependency resumes the exact GET stream without a duplicate POST, rejects cross-operation/cross-server reuse, suppresses the prior cursor, and atomically clears pending state on terminal result. Dependency-owned request reconstruction, keyed grant verification, restart-persistent nonce consumption, runtime server-list routing, and configured external stdio progress/invocation/result projection pass focused and process E2E tests; OAuth remains pending |
| Isolated plugin host | Integration tested | Five-crate process maps the versioned plugin protocol through all layers; SDK validation, keyed action grants, durable replay/state generations, approved executable roots, per-invocation crash isolation, timeout/cancellation/retry/rate limits, bounded observers and state changes are implemented; authority/cycle rejection has a real dependency test |

## In progress

- Typed summary/artifact compaction and approved automatic memory-write flows.
- Planner-worker-reviewer plan/spawn/join/review/revision execution on the
  runtime-managed child-session substrate, plus general graph-node adapters
  beyond the four live built-in semantics.
- TUI management panels for schedules, plugins, MCP, processes, artifacts,
  child agents, and LSP; core interactive streaming is implemented.
- MCP OAuth authorization-code flow.
- Schedule TUI management.

## Failing tests

None. `cargo test --workspace --all-targets --all-features --locked`,
`cargo test --workspace --doc --all-features --locked`, strict workspace
Clippy, formatting, and the 88-package architecture command and fixture tests
pass locally on Windows for the current tree. The style-registry, ephemeral,
research-loop, and declarative-graph process E2Es pass on Windows; their Unix
scripts have been syntax-checked but not process-executed.

## Blockers

None.

## Next tasks

1. Implement planner-worker-reviewer as runtime-managed child sessions.
2. Finish summary/artifact/write context paths, then connect plugin composition
   and the harness capability registry.

## Performance results

Release-mode deterministic kernel measurements are recorded in
`docs/benchmarks/2026-07-27-windows-kernel.md`. They cover event integrity,
protocol serialization, expression parse/evaluation, and graph compilation only.
Journal, replay, snapshot, artifact, dormant-session, concurrency, frontend, and
cross-process benchmarks remain pending.

## Security review

Browser, filesystem, Git, LSP, MCP, process, Web, and the native harness fail closed with keyed
owner/session/call/action/digest/expiry/nonce grants and dependency-side verification.
Process and Web now reconstruct canonical action bytes from dependency-owned request
fields, bind bootstrap identity, bound concurrency, and persist replay protection across
restart. Runtime filesystem and process calls now traverse the complete proposal and
mandatory policy chain before a per-session host receives a single-use grant.
Runtime browser, Git, Web, LSP, and MCP calls traverse the same proposal and event path;
their dependencies verify exact host-specific grants. MCP additionally binds
expanded operation arguments and cancellation identifiers and consumes nonces in
durable per-session replay state before server discovery or invocation.
Pending tool actions are session-scoped and checksum protected. Continuation
resumption runs the blocking pipeline before the compare-and-set claim, and
mandatory tool policy is reapplied immediately before host dispatch. A canonical
`tool.execution_dispatched` outbox event is committed before the host boundary.
Replay resumes claims that never reached dispatch and treats terminal dispatches
as idempotent. Nonterminal approval dispatches use a receipt-only request:
checksum-protected terminal streams are bound to execution/session/call/tool,
workspace, arguments, and cancellation identity; already committed host-event
prefixes are skipped, and missing, corrupt, or conflicting receipts remain
ambiguous without redispatch. The receipt is durably written in the supervised
runtime dependency immediately after the host terminal frame and before returning
to logic. Before the RPC listener starts, the runtime scans every verified
receipt and reconciles ordinary nonterminal dispatches from canonical state.
Receipts owned by an already-approved durable continuation are classified and
left to the continuation's resume-once path so startup cannot consume the
action without also resuming its provider turn.
Scheduler startup recovery similarly separates durable occurrence claims from
canonical dispatch provenance. An unstarted claim may enter the ordinary
intercepted path; a canonically terminal claim receives a reconciliation event
before its worker marker; an ambiguous dispatched claim is terminally failed
without redispatch. Exact execution, schedule, and optional continuation
identity are retained in the canonical recovery event.
Process executable policy is explicit and deny-by-default. Its sanitized child
environment admits only non-secret platform/toolchain discovery variables plus
configured overrides; the generic secret-name filter remains mandatory. Harness grants are
ephemeral, nonce-bearing, action-digest bound, expiry
checked, and replay limited in the harness dependency layer; ambiguous failed exchanges
are never retried automatically. Process children use Windows tree termination or Unix process groups.
Runtime replacement now reattaches to the surviving authenticated process host
without redispatch; abrupt process-host crash reattachment remains intentionally
unavailable because inherited handles cannot be recreated. Web proxy upstream DNS is
necessarily delegated to an explicitly trusted proxy. Journal corruption/recovery,
mandatory permission precedence, and plugin authority validation have tests.

## Acceptance scenario status

All acceptance scenarios remain short of cross-platform full-suite completion.
Style registry selection, restart validation, and Scenario 12 branch restyling
pass their Windows process paths. Scenario 6 now covers the built-in compiled
branch/approval/tool/loop fixture, including restart at approval and resume-once
continuation, but does not yet prove every declared node kind or a user-supplied
graph through process-level execution.
Scenario 7 and the deterministic none/file portion of Scenario 8 now pass on
Windows through `runtime_style_context.ps1`; Unix scripts have not been executed.
Scenario
1 has a real model-driven read/edit/failing-test/fix/passing-test process E2E,
but still lacks symbol search and multi-file edits. Scenarios 3 and 4 now pass
their functional paths on Windows, including denial projection and daemon
restart before approval. A second Windows kill-injection E2E terminates the
daemon after a real write and durable terminal receipt but before canonical
terminal commit, then completes from the receipt with the host unavailable;
another Windows E2E proves startup-wide reconciliation of an interrupted
non-approval dispatch before RPC readiness with the host unavailable. Unix
scripts are present but not executed in this report. The PTY process scenario
now passes a Windows daemon-replacement path: one live PTY survives, reattaches,
accepts input, exposes durable output, exits normally, and is not redispatched.
Scenario 12 passes its complete functional path on
Windows with a Unix script present: replay to an earlier sequence, atomic branch
creation, explicit style replacement, independent continuation, and
unchanged-parent comparison. A second Windows/Unix pair proves large parent
history is retained in a hash-bound artifact while the child journal and live
provider projection remain bounded. Machine-readable evidence is recorded in
`docs/requirements/traceability.toml`. Scenarios 9–11 also have live runtime
host-boundary smoke paths, though their full required multi-step scenarios
remain partial.
Scheduler crash recovery has separate Windows kill-injection evidence: one
claimed occurrence survives a pre-dispatch daemon stop, then a second stop
after canonical provider completion but before the worker marker; startup
reconciliation commits one exact recovery event and never repeats provider
execution. The equivalent Unix script is present but was not run here.

## N-tier compliance status

Locally integration tested across 88 packages, including runtime, harness, CLI,
TUI, ACP, scheduler, filesystem, process, Git, and LSP processes. The metadata/source validator reports no
violations and its intentional violation fixtures emit stable diagnostics. Other
required deployable systems remain incomplete.

## Process-boundary status

Runtime, harness, CLI, TUI, ACP, scheduler, browser, filesystem, process, Git, LSP, Web, MCP, and plugin host are
distinct binaries. Runtime, CLI, harness, filesystem, process, Git, Web, LSP,
and MCP hosts now
complete real authenticated named-pipe turn E2Es on Windows, including durable
provider/tool lifecycle events, structured continuation, and an autonomous
test/fix loop. The runtime also survives a forced daemon restart while a
filesystem write awaits approval and resumes it through newly supervised
host paths, and it reconciles verified receipts for unrelated nonterminal
dispatches before accepting new RPC connections. A separate forced restart
preserves one live PTY in its surviving process host and reattaches through the
replacement runtime without duplicate dispatch. Equivalent Unix scripts exist for these tool loops,
but the approval scenario still needs a Unix port and execution.
Incremental harness frames, per-event canonical commits, ordered runtime RPC
stream frames, bounded channel backpressure, explicit one-item credit windows,
active cancellation, flushed headless NDJSON rendering, and bounded
reconnect-from-sequence pages and interactive TUI rendering are live. Continuous
live subscription after catch-up, complete management panels, and broader process-isolation acceptance tests
remain incomplete.
