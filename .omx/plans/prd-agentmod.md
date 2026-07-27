# AgentMod Product Requirements and Execution Plan

Status: approved planning baseline  
Owner: lead agent  
Repository: `JPrier/AgentMod`  
Target: a cross-platform, event-driven Rust developer-agent product and embeddable runtime

## Mission and product boundaries

AgentMod is a local-first developer-agent platform with a durable runtime daemon, a
provider-facing harness, capability-isolated tool hosts, replaceable plugins and
frontends, and immutable replayable history. It must be usable as both:

1. a daily-driver terminal development agent; and
2. an embeddable execution runtime for supervisors and experimental agent styles.

The runtime is authoritative for session state, proposals, policy, continuations,
events, artifacts, schedules, and child sessions. The harness is authoritative only
for provider execution. Tool hosts perform capability-scoped external operations and
never mutate runtime state. Frontends communicate solely through the runtime protocol.

The project will not claim completeness from scaffolds, interfaces, deterministic
mocks, or partial frontends. A capability advances from planned to complete only when
the implementation and the required unit, integration, end-to-end, and relevant
benchmark/security evidence exist.

## Non-negotiable architecture

Each deployable subsystem has four compile-time layers:

```text
service -> logic -> data -> dependency
```

Each boundary owns distinct request, response, identifier wrapper, record, and error
types. The caller maps down and maps returned values up. Transport DTOs stop at
service; SDK and operating-system types stop at dependency. Executable composition
roots may assemble concrete implementations but contain no business behavior.

Pure deterministic kernels remain narrow crates rather than artificial four-layer
systems. Cross-process protocol crates contain versioned wire types only.

## Process topology

```text
TUI / CLI / ACP
        |
        | runtime-protocol (local authenticated IPC)
        v
runtime daemon <----> scheduler worker
   |       |  \
   |       |   \---- plugin-protocol ---- plugin host / WASI components
   |       |
   |       \-------- harness-protocol --- native harness --- provider APIs
   |
   \---------------- tool-protocol ------ filesystem/process/web/browser/
                                          git/lsp/mcp hosts
```

Dormant sessions are metadata entries and consume no process, thread, Tokio task,
loaded transcript, or provider connection.

## Workspace and crate strategy

The workspace uses real architectural crate boundaries while grouping process-local
types cohesively:

- `core/`: primitives, event model, event pipeline, graph engine, continuation,
  expression engine, protocol support, canonical conversation projection.
- `protocols/`: runtime, harness, tool, plugin, and frontend wire contracts.
- `apps/runtime/`: four layers plus binary composition root.
- `apps/harness/`: four layers plus binary composition root.
- `apps/{cli,tui,acp,plugin-host,scheduler}/`: four layers plus binaries.
- `apps/tools/{filesystem,process,web,browser,git,lsp,mcp}/`: four layers and binaries.
- `sdk/`: plugin, session-style, and tool authoring SDKs based only on protocols and
  stable primitives.
- `tests/`: fixtures, integration, E2E, architecture, stress, and crash injection.

During early vertical slices, related layer modules may share a process-layer crate
only when a custom architecture check enforces module import direction equally
strongly. Before a process is considered release-ready its crate graph must provide
compile-time enforcement for its public layer boundaries.

## Major use cases

### Session and history

- Create, list, load, suspend, resume, archive, rewind, branch, inspect, replay, and
  recover sessions.
- Persist append-only checksummed JSONL events and immutable content-addressed
  artifacts under per-session directories.
- Rebuild derived SQLite indexes solely from canonical files.
- Restore from validated snapshots and pure reducers without repeating side effects.

### Execution and interception

- Turn every consequential action into a typed proposal.
- Execute deterministic session-style interceptors, plugin interceptors, user policy,
  and mandatory security policy before effects.
- Support `Continue`, `Replace`, `Reject`, `RequireApproval`, `Defer`, `Cancel`, and
  capability-valid `Fork`.
- Persist original proposal, each decision/modification, approved action, result, and
  causation chain.
- Deliver committed events to bounded asynchronous observers that cannot write
  canonical state.

### Model execution

- Send approved structured conversation projections to a separate harness.
- Support OpenAI-compatible, OpenRouter, OpenAI, Anthropic, Gemini, and local
  OpenAI-compatible providers through official APIs.
- Stream text, tool calls, usage, cost, cache metadata, cancellation, and classified
  retry state.
- Provide a deterministic offline mock provider for all required failure modes.

### Native tools and integrations

- Provide capability-isolated filesystem, process, web, browser, Git, LSP, and MCP
  hosts with bounded projections and artifact overflow.
- Provide runtime interaction tools for approval, tasks, artifacts, session
  inspection, and child agents.
- Discover tool schemas lazily by group, capability, style, and project.

### Styles, memory, plugins, and agents

- Ship persistent-chat, ephemeral-turn, research-loop, planner-worker-reviewer, and
  declarative-graph styles.
- Ship no-memory, file memory, SQLite FTS memory, and embedding abstraction.
- Ship sliding-window, summary, artifact-handoff, tool-output-eviction, and no-op
  compaction with complete provenance.
- Run untrusted plugins out of process or in an approved sandbox and validate scope,
  capability, authority, ordering, timeouts, and version compatibility.
- Run child agents as budgeted child sessions with controlled workspace modes and
  reviewer loops that inspect real diffs and test results.

### Product surfaces

- Ship a polished Ratatui-based TUI with session, provider, style, tool, plugin, MCP,
  permission, process, child-agent, context, token, event, artifact, memory, task, and
  diagnostics surfaces.
- Ship a headless CLI with human, JSON, and streaming JSON modes.
- Ship an ACP adapter mapping external operations into ordinary runtime requests.
- Ship durable scheduling and wakeup operations available through all frontends.

## Security invariants

- Mandatory runtime security policy is last and cannot be bypassed by styles/plugins.
- Filesystem operations enforce approved roots, canonical-path checks, symlink escape
  prevention, device-file rejection, sensitive-file policy, and atomic writes.
- Process operations enforce working directory, command/environment/secret/resource,
  process-group, timeout, cleanup, and optional sandbox rules.
- Network operations revalidate redirects and enforce domain, IP/private-network,
  method, proxy, header-redaction, TLS, timeout, and response-size policy.
- Secrets remain references outside the dependency boundary; values do not enter
  events, ordinary logs, provider context, or configuration.
- Third-party plugin trust and requested authority are explicit and auditable.

## Vertical delivery plan

### M0 — Orientation and planning

- Record architecture, process, and crate dependency maps.
- Establish this PRD, the test specification, and `STATUS.md`.
- Record initial ADR decisions and the release evidence model.

Exit: both planning artifacts exist and cover all acceptance scenarios.

### M1 — Enforced architecture and protocol foundation

- Create workspace, policy metadata, CI skeleton, and toolchain configuration.
- Implement foundational primitives and versioned protocol envelopes.
- Scaffold runtime, harness, CLI, and first capability hosts through all four layers.
- Implement cargo-metadata/source architecture validation plus intentionally failing
  fixtures.

Exit: workspace compiles; negative fixtures prove each prohibited dependency class is
detected; no process imports another process's internals.

### M2 — Event kernel and durable runtime

- Implement event envelopes, classifications, checksums, journal framing/recovery,
  artifacts, snapshots, reducers, replay, branch, rewind, and continuations.
- Implement pipeline compiler/executor, observer dispatcher, graph parser/compiler,
  constrained expressions, and deterministic clocks/IDs for tests.
- Implement minimal runtime/harness protocol path and mock provider.

Exit: proposal modification/denial, durable approval, context replacement, streaming
cancellation, replay, branching, and crash-journal tests pass offline.

### M3 — Complete coding loop

- Implement filesystem read/list/glob/grep/write/edit/patch.
- Implement foreground process execution and artifact-backed output.
- Implement permission engine, persistent-chat style, provider/tool lifecycle, runtime
  interaction tools, and headless CLI.
- Exercise an actual repository task using the mock provider.

Exit: coding-task E2E passes with events, artifacts, diffs, failure/fix/test evidence.

### M4 — Daemon, IPC, TUI, supervision, and background processes

- Implement secure local IPC, streaming/backpressure/cancellation/reconnection,
  session registry, lazy loading, process supervision, PTY, detach/reattach, and
  durable logs.
- Implement harness/tool-host restart recovery and the TUI's core daily-driver flow.

Exit: background process, process isolation, restart/resume, and TUI/CLI parity
evidence passes on Windows and Unix CI.

### M5 — Providers and web

- Add first-party provider adapters, discovery, switching, usage/cost accounting.
- Add HTTP, fetch/extraction, search-provider abstraction and one usable adapter.
- Add browser supervision and bounded rich-content artifacts.

Exit: provider contract suite and Web E2E pass without credentials via local fixtures;
credentialed smoke jobs remain optional.

### M6 — MCP, LSP, Git, and discovery

- Implement MCP transports/capabilities/catalog, LSP supervision/operations, Git and
  worktree/checkpoint operations, and schema discovery/accounting.

Exit: MCP, LSP, and Git fixture suites and reconnect/restart cases pass offline.

### M7 — Styles, memory, compaction, scheduling, and child agents

- Implement all built-in styles, memories, compaction strategies, schedules/triggers,
  child-session workspace modes, budget controls, and reviewer loops.

Exit: ephemeral-turn and planner-worker-reviewer scenarios pass; graph inspection and
  memory provenance are visible through CLI/TUI.

### M8 — Plugin platform and ACP

- Implement plugin SDK/host/protocol, trusted and out-of-process examples, optional
  WASI boundary, migrations, backpressure, rate limits, quarantine/disable, and ACP.

Exit: plugin-authority and full frontend-parity scenarios pass.

### M9 — hardening and release

- Complete secret/keychain adapters, network/filesystem/process controls, sandbox
  adapters, corruption recovery, fuzz targets, stress profiles, benchmarks, packaging,
  installation/upgrade, documentation, CI matrix, and independent reviews.

Exit: all definition-of-done checks pass with recorded evidence and no critical-path
placeholder.

## Acceptance scenario traceability

| # | Scenario | Primary milestones | Required evidence |
|---|---|---|---|
| 1 | Coding task | M3 | offline E2E, journal/artifact/diff assertions |
| 2 | Pre-tool modification | M2–M3 | original/modified event chain and executed args |
| 3 | Tool denial | M2–M3 | denial event and execution sentinel remains absent |
| 4 | Durable approval | M2 | restart test and exactly-once continuation |
| 5 | Context replacement | M2 | structured projection/provenance, no fake message |
| 6 | Streaming cancellation | M2 | partial output, cancellation, next request |
| 7 | Large output | M2–M4 | bounded-memory assertion and ranged artifact read |
| 8 | Background process | M4 | disconnect/reattach/input/interrupt/kill suite |
| 9 | MCP | M6 | local MCP fixture discovery/progress/cancel/reconnect |
| 10 | Web | M5 | local search/fetch/API fixtures and citation assertions |
| 11 | LSP | M6 | fixture server diagnostics/symbol/ref/restart suite |
| 12 | Replay and branch | M2 | immutable original and divergent child replay |
| 13 | Ephemeral turn | M7 | provider projection captures and artifact handoff |
| 14 | Planner-worker-reviewer | M7 | rejection/revision/approval with actual tests |
| 15 | Plugin authority | M8 | accepted interceptor and three rejected manifests |
| 16 | Crash recovery | M2–M9 | kill-point matrix across five operation classes |
| 17 | Frontend parity | M4/M8 | shared transcript assertions for TUI/CLI/ACP |
| 18 | N-tier replacement | M1+ | alternate dependency fixture and unchanged upper crates |
| 19 | Process isolation | M4/M8 | host/harness/frontend crash matrix |

## Release and evidence policy

A release candidate requires:

- formatting, clippy with warnings denied, tests, docs, feature matrix, dependency
  policy, licenses, advisories, architecture checks, and platform CI all green;
- recorded benchmark commands, machine/OS/toolchain, raw results, and regression
  thresholds;
- stress evidence for thousands of dormant sessions and at least 100 active mock
  sessions;
- threat-model and security review closure for persistence, protocols, permissions,
  process isolation, plugins, secrets, and recovery;
- packaging smoke tests and reproducible install/upgrade instructions;
- `STATUS.md` containing only evidence-backed classifications.

## Initial engineering decisions to record as ADRs

1. JSONL canonical journal plus content-addressed artifacts and derived SQLite indexes.
2. Framed/checksummed append with tail truncation recovery and corruption quarantine.
3. Tokio local IPC using Unix sockets and Windows named pipes with a common protocol.
4. Serde versioned envelopes with capability negotiation and explicit error contracts.
5. TOML declarative graphs plus a non-Turing-complete expression language.
6. Provider-specific adapters isolated in harness dependency.
7. Per-capability tool-host processes rather than a process per tool call.
8. Out-of-process third-party plugins; optional WASI components; no stable Rust dylib ABI.
9. Ratatui for the TUI and Clap for the headless CLI.
10. Architecture validation from Cargo metadata plus source-boundary checks and negative
    fixtures.

## Completion rule

The lead continues through milestones while safe progress is possible. `STATUS.md`
must expose incomplete work plainly. “Complete” is reserved for the user's full
definition of done, not a milestone, scaffold, MVP, or mock-only vertical slice.
