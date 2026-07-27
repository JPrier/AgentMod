# Plugin System

The implemented portion is a wire contract and reusable pipeline primitives.
`agentmod-plugin-protocol` defines `PluginManifest`, plugin classes, load,
intercept, observe, cancel, disable, and health commands, plus structured
responses. The manifest currently carries identity/version, API requirement,
category, scope, capabilities, read/proposed-write authority, ordering, and
timeout.

The event pipeline can compile and execute trusted Rust interceptor/observer
implementations supplied by a caller. There is no plugin SDK crate, plugin host,
manifest file loader, capability registry, configuration-schema validator,
process/WASI isolation, migration, restart, rate limiting, or runtime activation
flow yet. In particular, the wire contract alone does not prove observer-write
rejection or third-party isolation.

See [Plugin SDK reference](../reference/plugin-sdk.md) for the currently usable
surface and explicit gaps.
