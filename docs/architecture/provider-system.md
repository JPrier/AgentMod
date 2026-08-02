# Provider System

The provider-neutral harness protocol carries approved structured projections,
execute/continue/cancel/health commands, visible text and tool-call deltas,
tool-call proposals, normalized completion/failure, usage, and structured
context-replacement decisions.

The native harness and deterministic fixture are separately registered bounded
JSONL process endpoints. The native implementation's complete
service → logic → data → dependency path currently implements a deterministic
mock provider with text, streaming fragments, one or multiple tool calls,
malformed arguments, timeout, rate limit, partial failure, cancellation,
disconnect, usage, and explicit continuation fixtures.

Runtime provider execution follows this implemented sequence:

1. reload and replay canonical session state;
2. commit typed user input;
3. build the provider projection from structured conversation entries;
4. run session-style and plugin interceptor pipelines;
5. run user policy and mandatory runtime policy;
6. commit the original proposal and final approved action;
7. issue a short-lived keyed harness grant;
8. execute through the exact per-session selected supervised harness adapter;
9. commit started, delta/tool proposal, completion/failure/cancellation events;
10. commit consolidated visible assistant content.

The runtime and CLI E2E checks validate this sequence through real Windows
processes and named-pipe transport. Equivalent Unix automation is present.

The harness process emits normalized lifecycle events as individual bounded
reply frames. Runtime pulls them through bounded dependency/data streams,
commits each event before making it visible, and emits ordered runtime RPC
`StreamItem` frames followed by `StreamEnd`. Slow socket writers propagate
backpressure through bounded service, logic, and data channels to the harness
stdout reader. Runtime tracks active cancellation IDs outside the serialized
process-I/O lock, so a concurrent runtime request can stop generation, preserve
already received visible deltas, discard the harness process, and reconnect for
a fresh request. The CLI dependency validates the stream and exposes it through
data, logic, and service-owned mappings. `agentmod run --stream-json` flushes
each committed item as NDJSON before provider completion; `--json` retains
backwards-compatible aggregation. Negotiated request-bound credits limit
nonterminal delivery, and canonical session-event pages support
reconnect-from-sequence without repeating effects. Interactive TUI rendering
is live. After bounded catch-up, the TUI starts one bounded cursor worker that
repeatedly reconnects from the last accepted canonical sequence, applies only
unseen events, and propagates backpressure through a fixed-capacity channel.
The worker stops on session replacement or frontend shutdown.

OpenAI-compatible, OpenRouter, OpenAI, Anthropic, Gemini, and local HTTP
adapters, authentication, live discovery, structured output, cost metadata, and
provider-specific retry classification remain to be implemented. No
live-provider compatibility claim is made yet.
