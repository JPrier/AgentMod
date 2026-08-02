# Plugin SDK Reference

`agentmod-plugin-sdk` is the manifest and validation API used by the runtime and
plugin host. It parses strict TOML or JSON, rejects unknown fields, returns
stable `PLUG001`–`PLUG030` diagnostics, validates a plugin set, and serializes
canonical manifests.

A manifest declares:

- identity, semantic version, and runtime plugin API requirement;
- category, scope, blocking/observer classification, trust, and isolation;
- process, trusted Rust, or WASI entrypoint;
- required/provided capabilities and event subscriptions;
- read and proposed-write authority;
- tool and network permissions;
- deterministic ordering, timeout, failure/retry policy, and migration version;
- a versioned inline or file-backed configuration schema;
- optional `[[node_executors]]` graph-node executor declarations (executor ID,
  version, node kind, runtime API, required capabilities, input/output JSON
  Schema, timeout, failure policy, idempotency, external-effect declaration,
  read authority, state scope);
- optional `[memory]` backend declaration (scopes, capabilities, bounded bytes);
- optional `[compaction]` strategy declaration (strategy ID, idempotency,
  bounded replacement bytes);
- optional `[[context_transforms]]` declarations (transform ID, lifecycle
  boundary, stage/priority, before/after constraints);
- an `[observer_delivery]` section (`best_effort`, `at_most_once`, or
  `at_least_once` with bounded attempts and backoff).

The live runtime currently accepts validated process entrypoints from explicitly
configured executable roots. Configure exact manifest paths with
`AGENTMOD_PLUGIN_MANIFESTS`, approved worker roots with
`AGENTMOD_PLUGIN_EXECUTABLE_ROOTS`, the supervised host with
`AGENTMOD_PLUGIN_HOST_PROGRAM`, and idle teardown with
`AGENTMOD_PLUGIN_IDLE_TIMEOUT_MS`. A session style must list a plugin under
`allowed_plugins`; blocking handlers must also have a matching compiled
`[[interceptors]]` declaration.

Blocking workers receive `initialize`, optional `migrate`, and `intercept`
requests. They may return continue, replace, or reject. Runtime validation
rejects replacement of immutable proposal identity or session scope.
Observer workers receive committed `observe` requests asynchronously and return
`observed`; they cannot request canonical-state write authority or receive a
canonical write interface. At-least-once observers deduplicate on the
runtime-issued idempotency key.

Graph-node plugins receive `execute_node` requests with the executor, node, and
kind identity plus input and a bounded variable environment; their output is
validated against the declared output schema and run/node identity before any
canonical effect. Memory plugins implement `memory_describe`, `memory_retrieve`,
`memory_commit_write` (only after runtime policy approval), and `memory_health`.
Compaction plugins implement `compaction_propose` with the exact source range
and hash. Context-transform plugins implement `context_transform` at declared
lifecycle boundaries and cannot modify protected context keys.

The deterministic examples are `apps/plugin-host/fixture-worker`,
`tests/fixtures/plugins/plugin-composed-style.toml`,
`tests/fixtures/plugins/plugin-expanded-style.toml`,
`tests/e2e/runtime_plugin_composition.ps1`, and
`tests/e2e/runtime_plugin_expansion.ps1`.

WASI and trusted in-process entrypoints are represented and validated by the
SDK but are not accepted by the current runtime catalog mapper. The frontend
management surfaces for disablement/quarantine/reload are wired at the runtime
protocol boundary; CLI/TUI panels remain pending.
