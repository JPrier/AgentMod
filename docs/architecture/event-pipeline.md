# Event Pipeline

`agentmod-event-pipeline` currently implements:

- deterministic ordering compilation with priority, `before`, and `after`;
- duplicate, missing-dependency, and cycle diagnostics;
- typed `Decision<T>` values and per-action decision capability validation;
- ordered asynchronous blocking interceptors with timeouts and failure policy;
- execution reports containing handler inputs, decisions/failures, and timing;
- bounded asynchronous observer dispatch with backpressure policy and statistics.

The library is deliberately generic and owns no sessions, persistence, provider,
or tool semantics. Runtime logic assembles it for provider requests, tool
actions, context construction and replacement, memory operations, and
compaction. Consequential actions continue through the mandatory permission and
canonical event paths after blocking interception.

Compiled styles retain their declared interceptor order, but live runtime
activation of style-selected plugin workers is not complete. Plugin manifest
validation and host isolation exist; end-to-end style/plugin/user/mandatory
ordering, observer activation, quarantine, and restart recovery remain planned
integration work.
