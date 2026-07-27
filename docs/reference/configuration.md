# Configuration Reference

Layered product configuration is not implemented. The current binaries use
composition-root defaults:

| Value | Current source |
|---|---|
| Runtime session root | `AGENTMOD_SESSION_ROOT`, default `sessions` |
| Runtime version | Cargo package version |
| Runtime local endpoint | `AGENTMOD_RUNTIME_ENDPOINT`, with platform default |
| Runtime RPC bootstrap secret | `AGENTMOD_RUNTIME_AUTH_TOKEN` (mandatory for `serve`, minimum 32 bytes) |
| Harness executable | `AGENTMOD_HARNESS_PROGRAM`, default sibling binary |
| Scheduler executable | `AGENTMOD_SCHEDULER_PROGRAM`, default sibling binary |
| Scheduler state root | `AGENTMOD_SCHEDULER_ROOT`, default sibling of the session root |
| Scheduler poll interval | `AGENTMOD_SCHEDULER_POLL_MS`, default `1000`; `0` disables |
| Scheduler poll claim bound | `AGENTMOD_SCHEDULER_POLL_LIMIT`, default `16`, range 1–1000 |
| Tool-host executables | capability-specific `AGENTMOD_*_HOST_PROGRAM`, default sibling binaries |
| Harness providers | deterministic mock plus configured first-party adapters |
| CLI runtime endpoint | `AGENTMOD_RUNTIME_ENDPOINT`, with platform default |

There is no user/project/session configuration file loader, merge engine,
effective-value provenance report, reload, platform-directory selection, or
secret-reference parser yet. Do not place credentials in repository files in
anticipation of a future format.

The target precedence is built-in, user, project, style, session, then CLI
override. A versioned schema and migration policy must be introduced before any
configuration format is considered stable.
