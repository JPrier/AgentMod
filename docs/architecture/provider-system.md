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
is live; continuous live subscription after catch-up remains incomplete.

OpenAI-compatible, OpenRouter, OpenAI, Anthropic, Gemini, and local HTTP
adapters are now implemented in the harness dependency layer with shared SSE
normalization (fragmented UTF-8, keepalives, bounded frames), provider-specific
wire serialization and errors isolated per adapter, retry classification that
never auto-retries ambiguous exchanges, redacted authentication, usage/cost
metadata with pricing-record identity, and environment/file secret references.
A bounded provider/model catalog is exposed through the additive `Catalog`
wire command. Every wire format has deterministic local HTTP fixture coverage;
live-provider claims still require the opt-in smoke scripts and are not
required by default CI. A genuinely independent second harness binary
(`agentmod-harness-fixture`, `independent-fixture` v2.0.0) implements the
protocol with its own N-tier crates and distinct capabilities, proving harness
selection is not hard-coded. See `docs/guides/providers.md` and
`docs/integration/TASK-08-live-providers.md`.
