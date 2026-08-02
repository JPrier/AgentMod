# Plugin System

AgentMod has a live, layered plugin path. `agentmod-plugin-sdk` owns strict
TOML/JSON manifests and version, capability, trust, authority, scope, timeout,
failure-policy, and cross-plugin ordering validation.
`agentmod-plugin-protocol` owns the versioned process contract (wire protocol
v2). `agentmod-plugin-host` maps that contract through service → logic → data →
dependency and supervises approved process workers with keyed, nonce-bearing
grants, bounded frames, timeouts, retries, cancellation, migration state,
durable observer delivery, disablement, quarantine, reload, and idle teardown.

The runtime composition root reads only explicitly configured manifest paths,
compiles them through the SDK, computes the activated plugin-set hash, merges
plugin graph-node executor registrations into the runtime node-executor
registry, and injects runtime-owned plugin data and logic ports. Runtime
dependency owns the plugin-host process transport with lazy restart and
per-session idle reaping; runtime data owns catalog normalization, activation
state, and routing; runtime logic activates the exact plugins allowed by the
immutable compiled session style and exposes lifecycle management below the
frontend.

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
authority. Delivery semantics are explicit: `best_effort` (bounded queue),
`at_most_once` (invocation-id deduplication), and `at_least_once` with a
runtime-issued idempotency key and a durable journal persisting the event
range, observer identity, attempts, retry policy, and next retry. Observer
delivery failure cannot roll back committed events; exhausted or ambiguous
deliveries fail closed without redelivery. Delivery attempts/drops are
canonically auditable as `plugin.audit_recorded` events (which are never
re-delivered to observers), and terminal delivery outcomes are readable from
the host audit ring.

Plugin-provided graph node executors are declared in the manifest, validated by
the SDK, registered in the runtime node-executor registry, and executed through
the plugin host with input/output schema validation, run/node identity checks,
undeclared-effect rejection, and canonical audit. Plugin memory backends
(describe/retrieve/commit/health) commit writes only after the proposal/policy
pipeline approves; plugin compaction proposals are validated against declared
bounds and source-range hashes; plugin context transforms run in compiled
pipeline order at lifecycle boundaries (before/after memory retrieval,
before/after compaction, before provider projection, before turn completion)
and cannot modify protected context keys, canonical history, identity,
workspace, or undeclared secrets.

Lifecycle operations (list/inspect, activate, disable, quarantine,
unquarantine under policy, reload, health, active sessions, state generation,
failures, observer lag) are implemented below the frontend layer through
`PluginManagementLogicPort` with a mandatory `LifecyclePolicyGate`, and are
exposed on the runtime protocol. Consequential lifecycle actions require the
gate's approval.

Activated plugin sessions retain no process while dormant: the host flushes
durable state and exits when idle with no active invocation and no pending
delivery, and the runtime dependency reaps idle connections. A fresh host
restores the durable loaded catalog, revalidates compatibility, and serves the
next request lazily; restore failures quarantine the plugin so the runtime
fails closed.

`tests/e2e/runtime_plugin_expansion.ps1` exercises the expanded catalog (graph
node, memory, compaction, context transform, durable observer, interceptor),
canonical audit, durable delivery, idle host teardown, lazy restart, and
daemon-restart recovery on Windows. `tests/e2e/runtime_plugin_composition.ps1`
continues to cover the interceptor/observer slice.

See [Plugin SDK reference](../reference/plugin-sdk.md) for the author-facing
format and current activation contract, and
[docs/integration/TASK-07-plugin-expansion.md](../integration/TASK-07-plugin-expansion.md)
for the exact wiring and remaining integration steps.
