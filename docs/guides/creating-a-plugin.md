# Creating a Plugin

AgentMod can execute explicitly configured process plugins through the isolated
plugin host. Use `agentmod-plugin-sdk` to parse and validate a strict TOML or
JSON manifest before activation.

For the currently live runtime path:

1. declare a process entrypoint under an approved executable root;
2. use runtime plugin API `^0.1`;
3. declare exact capabilities, event subscriptions, authority, timeout, failure
   policy, and ordering;
4. add the plugin ID to the selected session style's `allowed_plugins`;
5. for a blocker, add a matching compiled `[[interceptors]]` declaration;
6. configure exact manifest paths with `AGENTMOD_PLUGIN_MANIFESTS`, worker roots
   with `AGENTMOD_PLUGIN_EXECUTABLE_ROOTS`, and the host executable with
   `AGENTMOD_PLUGIN_HOST_PROGRAM`;
7. test the worker through the real runtime/plugin-host process boundary.

Blocking workers may continue, replace, or reject typed proposals. They cannot
change proposal identity, style, or workspace. Observer workers receive only
already committed event projections and cannot request canonical-state write
authority.

The packaged reference is
`apps/plugin-host/fixture-worker` together with
`tests/fixtures/plugins/plugin-composed-style.toml`.

This is not yet a general plugin installation experience. Runtime activation of
plugin-provided memory, compaction, context transforms, and management
disable/quarantine endpoints remains incomplete. See
[Plugin architecture](../architecture/plugin-system.md).
