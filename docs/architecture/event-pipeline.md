# Event Pipeline

`agentmod-event-pipeline` currently implements:

- deterministic ordering compilation with priority, `before`, and `after`;
- duplicate, missing-dependency, and cycle diagnostics;
- typed `Decision<T>` values and per-action decision capability validation;
- ordered asynchronous blocking interceptors with timeouts and failure policy;
- execution reports containing handler inputs, decisions/failures, and timing;
- bounded asynchronous observer dispatch with backpressure policy and statistics.

The library is deliberately generic and owns no sessions, persistence, provider,
or tool semantics. Runtime logic has proposal and permission primitives, but the
runtime health endpoint does not yet assemble a complete mandatory action
pipeline. Plugin manifest validation, state authority, and the required
style-plugin-user-mandatory ordering are therefore not end-to-end implemented.
