# Plugin System

AgentMod has a live, layered plugin path. `agentmod-plugin-sdk` owns strict
TOML/JSON manifests and version, capability, trust, authority, scope, timeout,
failure-policy, and cross-plugin ordering validation.
`agentmod-plugin-protocol` owns the versioned process contract.
`agentmod-plugin-host` maps that contract through service → logic → data →
dependency and supervises approved process workers with keyed, nonce-bearing
grants, bounded frames, timeouts, retries, cancellation, migration state,
observer queues, disablement, and quarantine primitives.

The runtime composition root reads only explicitly configured manifest paths,
compiles them through the SDK, computes the activated plugin-set hash, and
injects runtime-owned plugin data and logic ports. Runtime dependency owns the
plugin-host process transport; runtime data owns catalog normalization,
activation state, and routing; runtime logic activates the exact plugins
allowed by the immutable compiled session style.

Blocking interceptor declarations are taken from the compiled style in compiled
order. Their typed proposal decisions re-enter the existing action pipeline,
mandatory permission chain, canonical journal, tool-host routing, grants,
receipts, and continuation paths. Plugins cannot change proposal identity,
style, or workspace. Successful blocking invocations are recorded as
`plugin.invocation_completed`; the exact host-accepted set is recorded as
`plugin.set_activated` and reconstructed during replay.

Observer plugins are selected from the style's allowed plugin set. The runtime
delivers only service projections of already committed events after the turn
range is durable. Observers receive no canonical write port, are queued outside
the blocking path, and SDK/host validation rejects canonical-state write
authority. Observer delivery failure is diagnostic and cannot roll back or
modify committed state.

Session-scoped lifecycle management follows the same process boundaries:

```text
runtime service → runtime logic → runtime data → runtime dependency
                → plugin-host service → logic → data → dependency
```

Disable, enable, quarantine, and unquarantine requests bind the target session, exact plugin ID and
catalog version, action, redacted reason code where required, cancellation
lineage, and keyed host authorization. Runtime commits
`plugin.lifecycle_change_requested` before contacting the host and
`plugin.lifecycle_changed` only after validating the exact host state and
audit. Replay blocks later turns when an allowed plugin is pending, disabled,
or quarantined. An exact retry can reconcile a pending request, but runtime
does not silently substitute another action or plugin version. Host transition
cancels registered invocations and rejects new work for that plugin.
`runtime_plugin_lifecycle.ps1` and its hermetic Linux counterpart exercise this
through the real CLI, runtime daemon, plugin host, and isolated worker. Both
suites hold lifecycle calls before and after host I/O and prove the requested
event is already durable, then verify exact disable/enable and
quarantine/unquarantine receipts, receipt-only retry with the same cancellation
identity, rejection while inactive, and termination of the in-flight worker
without a late effect. Startup scans pending lifecycle requests and reconciles
the exact persisted action, plugin/version/configuration, and cancellation
identity. Legacy pending events without that identity fail closed.
When lifecycle events advance the journal while a dispatched node is returning,
the plugin coordinator reloads and reseals only the terminal canonical event;
it never redispatches the plugin invocation. Pre-dispatch work and substituted
node identities remain rejected after plugin activation is removed.

`tests/e2e/runtime_plugin_composition.ps1` and its POSIX counterpart exercise a
plugin-sourced style, a process interceptor that changes a real filesystem
call, canonical proposed/dispatched/completed observer delivery, activation and
invocation replay, and reactivation after daemon restart on Windows and
Ubuntu/WSL2. This is a partial product slice: exact node executors and ordered
context transforms are live and process-tested, while protocol version 10 maps
typed interceptor, node, context, node-state, memory, and compaction commands
through the complete plugin-host service → logic → data → dependency →
isolated-worker path. Each active cancellation target binds the exact plugin,
implementation, declaration, immutable configuration, handler, timeout, typed
input, readable state, and operation identity, and the host independently
recomputes the semantic request hash. Eight process tests pass on Windows and
Ubuntu/WSL2. They prove live interceptor/node/context/memory preemption, exact
receipt replay, non-idempotent ambiguity without relaunch, and node-state
authorization/replay/substitution. They also prove disable/quarantine
preemption and rejection of later plugin work. State CAS/read is synchronous under its
persistence lock, so it intentionally has no timing-race preemption claim.

An exact plugin-host node executor may also run inside one admitted bounded
parallel region. The parallel outbox retains the plugin invocation identity,
branch work, action/artifact evidence, and cancellation lineage before host I/O.
Windows and Ubuntu/WSL2 Graph C process runs prove validated action execution,
persisted artifact propagation into the exact join, correlated in-flight
cancellation, terminal receipt recovery, duplicate suppression, and replay
without redispatch. Nested parallel regions and plugin-proposed runtime-action
classes beyond the validated tool/network set remain unsupported.

Automatic plugin memory writes now traverse the production runtime and
plugin-host boundaries. The immutable style selection binds the exact plugin,
declaration, implementation, handler, configuration reference, scope, typed
input, and semantic request hash. Runtime-owned proposal and durable `ask`
approval precede one-shot dispatch; sealed terminal receipts permit
receipt-only restart recovery, while missing, corrupt, invalid, or timed-out
post-dispatch results fail closed without relaunch. Dedicated Windows and
Ubuntu/WSL2 process suites prove approval restart, post-persist crash recovery,
invalid-result and timeout ambiguity, declaration loss after session creation,
and duplicate-effect suppression.

Plugin retrieval and compaction now use exact immutable runtime selections and
the complete runtime/plugin-host N-tier path. Runtime-owned proposal,
authorization, dispatch, terminal receipt, application authorization, and
replacement events make the operations recoverable without redispatch.
Terminal receipt reduction does not query the worker; live composition must
revalidate before a later effect. Cross-platform process suites cover the
supported turn-start, context-node, before-model, and repeated iteration-start
timing paths, compaction, invalid/timeout output, and duplicate suppression.
Repeated retrieval binds a distinct operation identity and terminal receipt
while preserving the exact provenance of entries introduced by earlier
operations. The Windows suite additionally covers a receipt crash cut, offline
reduction, live revalidation, and plugin unavailability. Runtime management
endpoints for failed/rejected interceptor audit remain incomplete. Committed
turn ranges now enter the canonical observer coordinator: runtime logic appends
the exact proposal and dispatch intent before host I/O, validates the terminal
receipt, and appends one terminal delivery event. Startup scans only replayed
pending deliveries and reconstructs the exact original event range under
bounded session, delivery, and journal-byte limits.

Observer delivery now has an exact immutable identity and durable host receipt.
The runtime reducer and coordinator support
`Proposed → Dispatched → Completed|Rejected|Failed|Ambiguous`; queue acceptance
is never treated as completion. Restart after host-side pending persistence
seals an exact ambiguous receipt and never enqueues a duplicate worker. The
host health contract reports queued/active observer work and confirms durable
state is flushed with no unterminated observer delivery. Runtime transport
teardown is permitted only when canonical observer work and continuations are
empty, every plugin transport operation class is idle, the host reports no
active invocation/observer, and durable state is flushed. Windows and WSL2
process tests prove the guarded teardown and host process exit. Terminal turn
orchestration invokes the same guard only after canonical observer deliveries
and approval-owned continuations are quiescent.

See [Plugin SDK reference](../reference/plugin-sdk.md) for the author-facing
format and current activation contract.
