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

This is not yet a general plugin installation experience. Runtime, CLI, and TUI
management support session-scoped disable, enable, quarantine, and unquarantine
with canonical request-before-host audit and fail-closed invocation preemption.
Lifecycle dispatch binds the exact plugin/version/configuration/action and
cancellation identity; the host persists a terminal receipt, and daemon startup
reconciles a matching pending canonical operation without changing that
identity. Same-identity retries are receipt-only. Legacy pending records without
an exact cancellation identity require explicit migration, and an ambiguous
non-idempotent operation is never automatically retried.

The runtime tears down an idle plugin host only after canonical lifecycle,
observer, interceptor, node, context, memory, durable-state, host, and transport
operation classes are all quiescent. Windows and Ubuntu/WSL2 process matrices
cover disable/enable, quarantine/unquarantine, receipt-gap startup recovery,
in-flight cancellation, observer delivery, and idle teardown.
Plugin-provided memory retrieval, compaction, and automatic memory writes are
active when selected immutably by the session style. Their declarations bind the
exact plugin and implementation versions, schemas, handler, timeout,
configuration reference, permissions, state scope, and idempotency. Runtime
policy authorizes invocation and application separately; automatic writes also
bind the semantic request, scope, and typed value, and durable approval resumes
that same operation. Restart recovery never substitutes or automatically
redispatches the selected worker. Ordered plugin context transforms and
plugin-node Session/Invocation state follow the same exact identity,
sealed-receipt, runtime-validation, and fail-closed recovery rules.
Additional effectful transform/interceptor lifecycle boundaries and a packaged
installation/update experience remain incomplete. See
[Plugin architecture](../architecture/plugin-system.md).
