# Isolated plugin host

The plugin host is a separate process using the versioned `plugin-protocol`. Its only
business path is `service → logic → data → dependency`.

| Layer | Responsibility |
|---|---|
| service | Protocol DTO parsing/mapping and stable failure envelopes |
| logic | ID/auth shape validation, activation/invocation/observation/state-change use cases |
| data | Dependency routing, layer-local manifest/decision/audit normalization |
| dependency | Plugin-SDK validation, keyed grant verification, durable replay/state, process isolation, timeouts, cancellation, retries, rate limits, observer backpressure |
| bin | Secure composition, bounded concurrent JSONL transport, shutdown |

Third-party workers receive one JSON request on stdin and emit one bounded JSON response.
They inherit no environment, use no shell, run only from composition-approved executable
roots, are killed on cancellation/timeout/drop, and never receive runtime internals.
Each invocation starts a fresh worker, so a crash cannot corrupt the host process.

The SDK validator rejects observer canonical writes, missing capabilities, incompatible
API versions, invalid policies, and ordering cycles before load. Consequential load,
invoke, observe, disable, and quarantine operations require owner/session/call/action/
digest/expiry/nonce-bound keyed grants at the dependency boundary. Nonces and plugin
state use synced immutable generation files so replacement works safely on Windows and
survives restart.

Remaining work is a packaged example worker suite and full runtime-supervision E2E.
