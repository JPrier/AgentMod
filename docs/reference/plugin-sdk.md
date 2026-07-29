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

Not yet activated through the runtime are plugin-provided memory, compaction,
context transforms, tools beyond the existing protocol seam, and management
endpoints for disablement/quarantine. WASI and trusted in-process entrypoints are
represented and validated by the SDK but are not accepted by the current
runtime catalog mapper.
