# AgentMod Verification and Test Specification

Status: mandatory release contract  
Default mode: deterministic, offline, credential-free  
Platforms: Windows MSVC, Linux GNU, macOS

## Evidence principles

- Tests validate observable state, events, artifacts, side-effect sentinels, and
  recovery results, not self-reported agent completion.
- Canonical replay never repeats external effects.
- Every layer is tested with only the layer directly below mocked.
- Protocol, dependency, and cross-process tests use deterministic local fixtures.
- Paid APIs, public internet, installed MCP servers, and installed language servers
  are never required by default.
- A scenario is complete only when its required test is automated and green on the
  applicable CI platforms.

## Test suites

### Architecture

- Parse `cargo metadata` and assert every workspace dependency edge against a checked-in
  allowlist and crate role manifest.
- Scan imports/features/build dependencies to prevent layer skipping, upward imports,
  cross-process internal imports, SDK imports above dependency, protocol DTOs in
  logic/data, shared business DTO crates, cycles, and upper-layer callbacks.
- Compile or scan intentional failing fixtures for every prohibited edge and prove
  each produces a readable diagnostic.
- Validate pure-core crate dependency budgets and forbidden semantic expansion.

### Service

Mock logic and test transport parsing, caller identity, endpoint validation,
service-owned DTOs, service-to-logic mapping, logic-to-service mapping, error
redaction, streaming, backpressure, cancellation, and connection lifecycle.

### Logic

Mock data only. Test business invariants, transitions, proposal capabilities,
interceptor order, mandatory-policy precedence, continuation rules, retry semantics,
style/graph evaluation, context replacement, session branching, scheduling
idempotency, child budgets, and explicit logic/data mappings. No files, sockets,
databases, providers, clocks, randomness, or processes may be used directly.

### Data

Mock dependency interfaces. Test dependency selection, multi-source dataset assembly,
normalization, stable ordering, deduplication, identifier mapping, bounded projection,
artifact overflow decisions, and dependency-error translation.

### Dependency

Use temp workspaces and local fixture servers/processes to test journal append,
filesystem behavior, process groups/PTY, local IPC, provider HTTP projections, MCP,
web, LSP, Git, browser supervision, secrets, SQLite, and artifact storage. No external
network is required.

### Core properties and golden tests

- IDs, sequence monotonicity, content hashes, protocol negotiation, and continuation
  resume-once use property tests.
- Event/protocol/config/graph/plugin manifest serialization use versioned golden files.
- Reducers obey deterministic replay, prefix equivalence, snapshot equivalence, and
  branch isolation properties.
- Graph and pipeline compilers detect cycles, unreachable nodes, invalid termination,
  unbounded loops, missing dependencies, duplicate owners, unsafe parallel writes,
  and unsupported capabilities.
- Expression evaluation is deterministic, bounded, non-Turing-complete, and rejects
  malformed/deep/expensive inputs.

### Fuzzing

Targets include event record decoding, journal-tail recovery, protocol envelopes,
graph/expression parsing, plugin manifests, unified patches, provider streaming,
tool-call arguments, HTTP metadata, MCP frames, LSP frames, artifact indices, snapshot
validation, and configuration merges. Seed corpora include all regression fixtures.

### Concurrency and recovery

- Concurrent event append preserves sequence/checksum/duplicate invariants.
- Bounded observer and stream channels apply declared backpressure/drop policy.
- Continuations, schedules, and idempotent RPC operations execute exactly once.
- Kill points cover journal append, artifact commit, tool execution, provider stream,
  continuation wait, process execution, index update, and snapshot write.
- Recovery distinguishes incomplete, retryable, externally-uncertain, and committed
  operations without silently repeating side effects.

## Required end-to-end scenarios

### E2E-01 Coding task

Fixture repository contains a deterministic failing feature. Mock model script reads,
greps/symbol-searches, edits multiple files, runs tests, observes failure, fixes, and
reruns successfully. Assert final files, test exit, ordered lifecycle events, bounded
provider projections, artifacts, and diff.

### E2E-02 Pre-tool modification

Interceptor rewrites an unsafe path/argument. Assert immutable original proposal,
decision with replacement, only replacement side-effect sentinel, tool result
projection, and replay equality.

### E2E-03 Tool denial

Mandatory policy denies a command. Assert execution sentinel absent, denial event and
structured model-visible result present, and a later safe action can complete.

### E2E-04 Durable approval

Create approval continuation, terminate daemon, restart, resolve approval, and race
duplicate resumes. Assert no preapproval effect and exactly one execution/commit.

### E2E-05 Context replacement

Compact a typed conversation into summary/artifact references. Assert history prefix
unchanged, replacement provenance/source range/strategy persisted, no fabricated user
entry, and captured next provider projection equals approved state.

### E2E-06 Streaming cancellation

Mock provider emits gated deltas. Cancel after visible output; assert provider cancel,
partial committed visible output, no hidden-reasoning claim, rebuilt context, and a
successful next request.

### E2E-07 Large output

Tool emits data larger than projection and channel bounds. Assert peak memory bound,
full content hash/size artifact, bounded context projection, and correct later byte
and line range reads.

### E2E-08 Background process

Start fixture process, disconnect frontend, reconnect, read historical/live output,
send input, interrupt, start another, kill, and exercise daemon shutdown policies.
Assert separated stdout/stderr and durable logs.

### E2E-09 MCP

Local fixture MCP servers over stdio and streamable HTTP provide tools/resources/
prompts/templates. Assert negotiation, discovery, namespacing, policy, progress,
cancellation, artifact overflow, health, disconnect, and reconnection.

### E2E-10 Web

Local search-provider fixture returns a result linking to a local HTML page and JSON
API. Agent searches, fetches/extracts/cites, requests JSON, and edits fixture code.
Assert redirect/domain/method/size policy, provenance, citations, caching, and
artifacts.

### E2E-11 LSP

Fixture LSP server covers diagnostics, document/workspace symbols, definition,
references, hover, signature, rename, format, and actions. Terminate it and assert
supervised restart and optional degradation.

### E2E-12 Replay and branch

Complete a session, replay to an earlier sequence, inspect exact state, branch with a
different style/context, continue, and assert original files/events remain immutable
and both histories share only the expected prefix.

### E2E-13 Ephemeral turn

Capture two provider requests. Assert each is freshly assembled, full event history
persists, explicit handoff artifacts are included, and omitted prior state does not
leak.

### E2E-14 Planner-worker-reviewer

Planner emits structured tasks; isolated workers produce changes; reviewer inspects
actual diff/tests and rejects a seeded omission; revision fixes it; reviewer approves.
Assert budget/iteration limits and result/artifact collection.

### E2E-15 Plugin authority

Load authorized modifier. Reject observer with canonical writes, an ordering cycle,
and a plugin with missing capability/version. Assert diagnostics, audit events,
timeout/crash isolation, and safe disable.

### E2E-16 Crash recovery

Inject hard termination during journal append, tool execution, provider stream,
continuation wait, and process execution. For each, assert valid journal prefix,
quarantine/recovery report as applicable, no duplicate committed effect, and explicit
status for externally uncertain work.

### E2E-17 Frontend parity

Drive the same scripted operations through CLI, TUI test backend, and ACP fixture.
Normalize transport-only events and assert equivalent canonical committed behavior,
permissions, streaming, and cancellation.

### E2E-18 N-tier replacement

Run identical logic/service tests with two journal/provider/filesystem dependency
implementations selected only by composition root/data routing. Assert service and
logic source hashes unchanged and behavior equivalent after normalization.

### E2E-19 Process isolation

Independently crash harness, each representative tool host, plugin host, and frontend.
Assert daemon survives, records disconnect/health events, applies retry/uncertain-state
policy, reconnects or quarantines appropriately, and preserves unrelated sessions.

## Security tests

- Path traversal, alternate path separators, Windows device names, junctions, Unix
  symlinks, races, case normalization, and approved-root boundary cases.
- Command allow/ask/deny matching, executable resolution, shell metacharacters,
  environment filtering, secret injection/redaction, directory restrictions,
  process-tree cleanup, timeouts, and resource adapters.
- DNS rebinding simulations, private/loopback/link-local IP policy, redirect
  revalidation, method/header rules, proxy policy, TLS failure, decompression bombs,
  response limits, and log redaction.
- Plugin capability escalation, scope crossing, observer writes, forged identities,
  replayed protocol IDs, malformed frames, timeouts, crashes, rate limits, and
  migration failures.
- Local IPC ownership/permissions, version downgrade, invalid capabilities,
  cancellation spoofing, reconnect idempotency, and backpressure exhaustion.
- Journal tamper/checksum/sequence/duplicate/corrupt-tail cases and artifact hash/path
  traversal cases.

Independent review is required before release for persistence, protocols, permission
policy, process isolation, plugin isolation, secrets, and recovery.

## Performance and stress

Criterion benchmarks record median and tail where meaningful:

- event append at durability modes;
- replay from genesis and snapshot;
- context construction/compaction;
- graph compilation and cached lookup;
- pipeline execution with interceptor counts;
- artifact streaming writes/ranged reads;
- dormant-session registry lookup/list;
- protocol serialization and local round trip;
- TUI event throughput;
- concurrent mock sessions.

Stress profiles:

- 10,000 dormant session metadata entries with no per-session task/thread/process;
- at least 100 concurrent streaming mock sessions;
- mixed tool calls and cancellations;
- child-agent fan-out within configured bounds;
- background processes with output rotation;
- slow/failed observers under each drop/backpressure policy.

Each result records commit, toolchain, build profile, OS, CPU, memory, command, dataset,
raw output artifact, and interpretation. No target is claimed until measured.

## CI and release gates

Required commands, adjusted only when workspace tooling demands:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --doc --workspace
cargo test -p architecture-tests
cargo hack test --workspace --feature-powerset
cargo deny check
cargo audit
cargo bench --workspace --no-run
```

CI additionally runs negative architecture fixtures, golden drift detection, selected
fuzz smoke runs, packaging smoke tests, Windows/Linux/macOS matrices, and scheduled
full stress/benchmark jobs.

## Completion accounting

`STATUS.md` uses only:

- Implemented and unit tested
- Integration tested
- End-to-end validated
- Benchmark validated
- Partially implemented
- Planned
- Blocked

No unchecked, ignored, flaky-quarantined, mock-inappropriate, credential-gated, or
platform-skipped required test can satisfy a release gate.
