# Plugin SDK Reference

`agentmod-plugin-sdk` is the manifest and validation API used by the runtime and
plugin host. It parses strict TOML or JSON, rejects unknown fields, returns
stable `PLUG001`–`PLUG024` diagnostics, validates a plugin set, and serializes
canonical manifests.

A manifest declares:

- identity, semantic version, and runtime plugin API requirement;
- category, scope, blocking/observer classification, trust, and isolation;
- process, trusted Rust, or WASI entrypoint;
- required/provided capabilities and event subscriptions;
- read and proposed-write authority;
- tool and network permissions;
- deterministic ordering, timeout, failure/retry policy, and migration version;
- a versioned inline or file-backed configuration schema.

The live runtime currently accepts validated process entrypoints from explicitly
configured executable roots. Configure exact manifest paths with
`AGENTMOD_PLUGIN_MANIFESTS`, approved worker roots with
`AGENTMOD_PLUGIN_EXECUTABLE_ROOTS`, and the supervised host with
`AGENTMOD_PLUGIN_HOST_PROGRAM`. A session style must list the plugin under
`allowed_plugins`; blocking handlers must also have a matching compiled
`[[interceptors]]` declaration.

Blocking workers receive `initialize`, optional `migrate`, and `intercept`
requests. They may return continue, replace, or reject. Runtime validation
rejects replacement of immutable proposal identity or session scope.
Observer workers receive committed `observe` requests asynchronously and return
`observed`; they cannot request canonical-state write authority or receive a
canonical write interface.

The deterministic examples are
`apps/plugin-host/fixture-worker`,
`tests/fixtures/plugins/plugin-composed-style.toml`, and
`tests/e2e/runtime_plugin_composition.ps1`.

Exact node executors and ordered context transforms are active through the
runtime and plugin-host. Protocol version 10 carries typed interceptor, node,
context, node-state, memory, and compaction operations through the host's
complete N-tier path. Each cancellable command binds its exact plugin,
implementation, declaration, immutable configuration, handler, timeout, typed
input, readable state, and operation identity; the host independently
recomputes that semantic request hash before registering the active target.
Eight process tests pass on Windows and Ubuntu/WSL2, including live
interceptor/node/context/memory preemption and exact node-state replay.
They also cover disable/quarantine cancellation and rejection of future work.
State CAS/read is synchronous, so that proof covers authorization, replay, and
substitution rather than a timing-dependent preemption race. Runtime
disable/quarantine commands are session-scoped and require the target plugin to
be explicitly allowed by the immutable style; the canonical lifecycle record
binds the exact catalog version and action.

Runtime-side plugin memory retrieval and compaction are live through the exact
immutable selections. Runtime logic owns proposal and application policy,
commits dispatch before plugin-host entry, validates the bounded typed result,
and seals a durable terminal receipt before applying a canonical replacement.
Restart may reduce an exact receipt without loading or redispatching the
worker, but live plugin composition is revalidated before any later effect.
Windows and Ubuntu/WSL2 process suites cover turn-start, context-node,
before-model, and repeated iteration-start retrieval, compaction, invalid
output, timeout, and duplicate suppression. Each iteration-start invocation
owns a distinct identity and receipt; sealing the new result binds only the
entries introduced by that invocation and preserves prior receipt provenance.
The Windows suite additionally covers the terminal-receipt crash cut, offline
reduction, live revalidation before the next effect, and unavailable-plugin
rejection before a new proposal.
Tools beyond the existing protocol seam remain incomplete. Canonical observer
turn integration, bounded startup reconciliation of exact pending observer
deliveries, enable/unquarantine management, and exact startup reconciliation
of pending lifecycle requests are active. WASI and trusted in-process
entrypoints are represented and validated by the SDK but are not accepted by
the current runtime catalog mapper.
