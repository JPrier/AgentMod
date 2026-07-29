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

`tests/e2e/runtime_plugin_composition.ps1` exercises a plugin-sourced style, a
process interceptor that changes a real filesystem call, an asynchronous
observer, canonical activation/invocation replay, and reactivation after daemon
restart. This is a partial product slice: plugin-provided memory, compaction,
context transforms, runtime management endpoints for disable/quarantine,
failed/rejected invocation audit, same-session in-flight observer recovery, and
idle plugin-host teardown remain incomplete. Activated plugin sessions currently
retain their supervised host until daemon teardown; that is not yet the dormant
session target.

See [Plugin SDK reference](../reference/plugin-sdk.md) for the author-facing
format and current activation contract.
