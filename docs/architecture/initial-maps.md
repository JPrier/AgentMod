# Initial Architecture, Process, and Dependency Maps

## Process ownership

| Process | Owns | Must not own |
|---|---|---|
| Runtime | canonical sessions/events/artifacts, policy, continuations, styles, memory coordination, schedules, child sessions | provider APIs, frontend rendering, capability-host effects |
| Harness | one provider execution environment, provider projections/streaming/cancellation | canonical runtime history, global policy, scheduling |
| Filesystem host | approved filesystem operations | runtime state or policy decisions |
| Process host | OS processes, PTY, logs, groups | runtime continuations or events |
| Web host | HTTP, fetch, search adapters | browser UI or runtime policy |
| Browser host | supervised rendered browsing | canonical sessions |
| Git host | repository/worktree/checkpoint operations | automatic destructive Git policy |
| LSP host | language-server discovery/supervision/operations | basic filesystem/process functionality |
| MCP host | MCP transports/capabilities/catalog normalization | the internal canonical tool model |
| Plugin host | scoped third-party execution | unrestricted runtime internals |
| Scheduler | durable trigger evaluation where isolated | session business transitions |
| TUI/CLI/ACP | endpoint interaction and presentation | runtime internals or direct side effects |

## Business dependency rule

```text
wire/framework
      |
      v
service-owned request
      | explicit map
      v
logic-owned command
      | explicit map
      v
data-owned request
      | explicit map and adapter selection
      v
dependency-owned request
      | SDK/protocol map
      v
external system
```

Responses travel upward through new layer-owned representations. Each layer exposes
narrow traits only to the layer immediately above. Concrete assembly exists solely in
the binary composition root.

## Initial crate dependency constraints

```text
core-primitives <- core event/graph/continuation/expression crates
core-primitives <- protocol crates

protocol crate <- service layer (receive/send mapping only)
protocol crate <- dependency layer (IPC adapter only)

process-service -> process-logic -> process-data -> process-dependency
process-bin -> all four concrete process layers (assembly only)

sdk -> stable protocols + primitives
```

Forbidden:

- any process layer importing another process's internal crate;
- service importing data/dependency;
- logic importing service/dependency/protocols/external SDKs;
- data importing service/logic/protocols/external SDKs;
- dependency importing any upper layer;
- pure core importing process, protocol, frontend, provider, or tool business types;
- callbacks that give lower layers control over upper-layer business logic.

## Primary runtime flows

### Consequential action

```text
proposal event
 -> style interceptors
 -> plugin interceptors
 -> configured policy
 -> mandatory security policy
 -> approved host/harness/plugin request
 -> structured result
 -> committed event
 -> bounded asynchronous observers
```

### Provider request

```text
runtime canonical conversation
 -> approved context projection
 -> harness protocol
 -> harness service/logic/data/dependency
 -> provider stream
 -> harness lifecycle proposal
 -> runtime continuation decision
 -> explicit harness resume/cancel/replace
```

### Recovery

```text
valid snapshot
 -> checksummed committed events after snapshot
 -> pure reducers
 -> explicit reconciliation of incomplete external actions
 -> rebuilt derived indexes
```

## Architecture enforcement

The repository will carry:

1. a machine-readable crate-role/dependency policy;
2. a Cargo metadata analyzer for dependency edges and cycles;
3. source scans for protocol/SDK/import leakage and prohibited layer callbacks;
4. public API checks preventing lower-owned types from leaking upward;
5. intentionally invalid fixture workspaces for each rule;
6. CI jobs proving valid workspace acceptance and invalid fixture rejection.

This map is the Phase 0 baseline and will evolve only through recorded ADRs.
