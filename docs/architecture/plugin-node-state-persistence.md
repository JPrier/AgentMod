# Plugin-node preserved-state persistence

Plugin-node preserved state uses the existing plugin-host process and its
per-plugin durable state file. It does not create a runtime-side or second
plugin-host state store.

The boundary is:

```text
runtime logic PluginNodeStatePersistenceLogicPort
  -> runtime data PluginDataPort::persist_plugin_node_state
  -> runtime dependency RuntimePluginDependencyPort::persist_plugin_node_state
  -> plugin protocol PersistNodeState
  -> plugin-host service
  -> plugin-host logic
  -> plugin-host data
  -> plugin-host dependency PluginDependencyPort::persist_node_state
  -> existing per-plugin generation file
```

The compare-and-swap identity binds the plugin and invocation, invocation
digest, executor ID and version, executor declaration hash, immutable
configuration reference, declared state scope, prior generation and state
hash, new bounded state hash, action and authorization digests, nonce, exact
authenticated cancellation target, cancellation identity, and idempotency key.
The keyed authorization grant covers the action digest, state-operation nonce,
cancellation identity, and idempotency key.

Only `invocation` and `session` scopes are currently accepted. Invocation
scope has an isolated key and begins at generation zero. Session scope derives
its predecessor from replayed canonical `PluginNodeStatePreserved` events for
the same plugin and exact executor declaration; the generation chain must be
contiguous. Runtime logic, runtime data, and canonical event preparation all
reject `model_call`, `turn`, `project`, `user`, and `runtime` scopes because
the persistence command does not yet carry canonical identities for them.

An exact replay returns the previously committed generation and receipt
identity. Reusing an idempotency key for changed state or authority fails with
`state_conflict`. A mismatched predecessor fails with
`stale_state_generation`. Cancellation is checked before the atomic
generation-file commit. State CAS and read complete synchronously while holding
the persistence lock; process tests therefore assert exact authorization,
replay, and target/configuration substitution rather than making a
timing-sensitive live-preemption claim. A lost, timed-out, or malformed host response is
classified as ambiguous and is not automatically retried; recovery may submit
only the same exact idempotency identity to reconcile the stored receipt.

`PluginStateTurnCoordinator` is the runtime-logic orchestration seam. Its
command carries the exact canonical invocation identity, outcome-validation
hash, bounded raw state, declared scope, and the actual Turn cancellation
identity. Before plugin-host I/O it reloads replay and requires the validation
marker, budget charge, and every proposed action's successful terminal
receipt. It derives the prior generation and state hash from replay, creates a
stable nonce and idempotency key, and binds the caller's cancellation identity
into the authorization digest.

The coordinator calls
`PluginNodeStatePersistenceLogicPort::persist_plugin_node_state` once per
attempt. After receiving a terminal receipt, journal compare-and-append
conflicts only reload and reclassify that retained receipt; they do not call
plugin-host again. An ambiguous persistence result is returned fail-closed
without a canonical success event or automatic retry. A restart after the
hash-only event was committed recognizes it from replay and does not cross the
plugin-host boundary again.

The coordinator calls `prepare_plugin_node_state_preservation` with the
replayed session state, exact command, and exact durable receipt:

- `Append` supplies the hash-only event for journal compare-and-append.
- `AlreadyCommitted` completes without another append.
- `Conflict` and `InvalidOrder` fail closed.

The event binds the receipt ID and digest, prior and new generations, exact
executor declaration, scope, idempotency identity, and action and authorization
digests. It never contains raw preserved state. Legacy events without these
version-two receipt bindings deserialize only to produce a migration or
branch-with-recompiled-execution diagnostic and cannot reduce.

Raw state loading uses the same N-tier path and the same per-plugin generation
file:

```text
runtime logic PluginNodeStateReadLogicPort
  -> runtime data PluginDataPort::load_plugin_node_state
  -> runtime dependency RuntimePluginDependencyPort::load_plugin_node_state
  -> plugin protocol LoadNodeState
  -> plugin-host service -> logic -> data -> dependency
  -> existing per-plugin generation file
```

The caller supplies the expected generation and state hash reconstructed from
the canonical hash-only event chain. The authenticated request also binds the
session, plugin, exact requesting invocation and digest, executor ID and
version, executor declaration hash, immutable configuration reference, scope,
nonce, exact authenticated cancellation target, cancellation identity, and
idempotency key. The host returns the bounded raw value plus a terminal receipt
whose digest binds that same identity. Runtime dependency, data, and logic each
recompute the value hash and validate the complete receipt before returning
it. Exact duplicate reads reconcile a stable receipt identity; missing,
advanced, substituted, cancelled, or ambiguous reads fail closed.

Read receipts are retained in a bounded ledger inside the existing plugin
state file and audits remain redacted. The host fails closed when that ledger
is full. Raw state is never placed in the receipt, canonical event, or log.
Only `invocation` and `session` reads are accepted. This is a composable
runtime-logic boundary; injecting the loaded value into a later Turn invocation
is a separate orchestration step and is not implied by the boundary alone.
