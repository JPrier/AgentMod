# Creating a Plugin

Third-party plugins cannot yet be installed or executed. There is no plugin SDK,
plugin host, manifest loader, or stable entrypoint ABI.

Development that can proceed now is limited to:

1. model the desired authority with `PluginManifest` wire fields;
2. keep observer proposed-write authority empty;
3. express deterministic ordering with `before` and `after`;
4. prototype trusted Rust interceptor behavior against
   `agentmod-event-pipeline`;
5. test every decision against explicit `ActionCapabilities`.

Do not distribute a manifest as installable AgentMod software yet. The future
workflow must add configuration validation, capability approval, process/WASI
isolation, timeout/cancellation, crash policy, state migration, and host-level
tests. See [Plugin architecture](../architecture/plugin-system.md).
