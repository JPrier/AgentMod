# Protocols

Current wire crates:

- `agentmod-runtime-protocol`: health, session create/list/inspect/replay/branch,
  turn streaming, subscription, approval, and cancellation DTOs;
- `agentmod-harness-protocol`: provider execution lifecycle DTOs, including
  individually framed events with an explicit terminal marker;
- `agentmod-tool-protocol`: capability-host DTOs;
- `agentmod-plugin-protocol`: isolated plugin DTOs;
- `agentmod-frontend-protocol`: frontend capabilities and lifecycle DTOs.

`agentmod-protocol-support` implements version/capability negotiation, request,
correlation, causation, idempotency and cancellation metadata, bounded CBOR
framing, stream frame kinds, heartbeats, window updates, and error envelopes.
It also owns the common keyed authorization primitive used by capability hosts:
claims bind owner, session, call, action, exact operation digest, expiry, and
nonce.

The public harness protocol is implemented by both the native harness and the
test-only `agentmod-independent-harness-fixture`. The latter is a separately
compiled executable whose only AgentMod crate dependency is the public harness
protocol; it has no dependency on the native harness service, logic, data, or
dependency crates. It parses bounded JSONL frames, validates the
runtime-issued keyed grant and one-shot nonce at its own dependency boundary,
and returns a distinct deterministic provider event stream. Runtime E2Es inject
it only for the `fixture` harness descriptor; the production native descriptor
and implementation are unchanged. Windows and hermetic Ubuntu/WSL2 process
runs prove exact independent output before and after runtime restart.

Tool protocol 1.0 is used by the reconnectable process host over Unix sockets
and Windows named pipes. A token-authenticated handshake negotiates bounded
request/response, streaming, cancellation, idempotency, and backpressure
capabilities before any `ToolHostCommand` is decoded. Every response repeats
the exact request, correlation, causation, idempotency, and cancellation
identity with a monotonic stream sequence. Service failures become terminal
`ToolHostEvent::Failed` frames instead of tearing down the listener.

The runtime service uses this framing over Unix sockets and Windows named pipes,
with mandatory token-authenticated negotiation before endpoint dispatch.
Runtime wire version 2.1 includes session-scoped approval resolution and resumed
provider events. Provider lifecycle items are emitted as ordered `StreamItem`
frames only after their canonical event commit; `StreamEnd` carries the turn
sequence range and continuation state. When `credit_windows` is negotiated, one
initial nonterminal item is permitted; every later item requires a
request/correlation/idempotency-bound `WindowUpdate` acknowledging the exact
last contiguous stream sequence. Zero, excessive, stale, or cross-request
credits are rejected. Bounded channels propagate slow-client backpressure
through service, logic, data, and harness-process reads. The CLI
dependency validates request identity and monotonic stream sequence, exposes a
bounded stream through all four CLI layers, and either flushes live NDJSON or
aggregates a backwards-compatible batch result. Version 1.x peers fail major-version
negotiation instead of guessing the changed request shape.

`Subscribe` provides bounded durable catch-up strictly after a canonical
sequence. Each page reports the verified head, last delivered cursor, and
whether another immediate page exists. This gives reconnecting frontends
gap-free replay without repeating effects. The TUI implements continuous live
subscription by issuing these bounded requests from one reconnecting cursor
worker after catch-up; the wire operation itself remains page-bounded rather
than an unbounded response stream. OS-owner validation and cross-process golden
interoperability remain planned. Protocol DTOs are permitted only at service
and dependency boundaries, never in logic or data.

Plugin wire protocol version 10 provides distinct typed operations for
interceptors, node executors, context transforms, node-state reads and writes,
plugin-provided memory retrieval, approved memory writes, and
provider-projection compaction. It retains the version-8 correlated transport
and version-7 authenticated cancellation receipt without compatibility
fallbacks. Every cancellable invocation carries a complete semantic target:
the exact plugin and implementation versions, declaration and immutable
configuration hashes, handler and timeout, session/run/node context, typed
input and readable state, operation and invocation identities, request hash,
idempotency key, and attempt. Payloads carry typed canonical and artifact
references, security classification, and hard item/reference/inline/frame
bounds. Pure retrieval and compaction declarations must be idempotent and
effect-free; a non-idempotent write or externally effective node may declare
only one attempt.

Version 10 is an intentional incompatible lifecycle-management upgrade.
Disable, enable, quarantine, and unquarantine now carry the exact immutable
plugin version and configuration reference. Version 9 peers are rejected at
negotiation; fields are never defaulted and lifecycle commands are never
silently downgraded. This prevents an authenticated management action from
being replayed against a differently configured implementation.

The protocol crate exposes strict response decoding that rejects oversized
frames, unknown fields, invalid echoed identities/audit operations, and content
hash drift. Results remain proposals: runtime logic must still validate schemas,
permissions, preservation requirements, budgets, and canonical state before
acceptance. Protocol version 10 traverses the complete
plugin-host service→logic→data→dependency→isolated-worker boundary. The host
independently normalizes and recomputes each complete command hash before it
registers an active cancellation target. Protocol availability alone does not
make a style-selected memory provider or compactor live: runtime policy
orchestration, canonical receipts, and recovery must also be connected.

The version-7 cancellation contract removes the unauthenticated
`Cancel { invocation_id }` command without a compatibility fallback.
`CancelInvocation` binds the exact session, run, plugin ID/version, invocation
ID/digest, operation ID, declaration hash, and request hash. Its
domain-separated action also binds a bounded reason code, explicit grant nonce,
idempotency key, and cancellation lineage ID under the exact
`plugin.invocation.cancel` keyed-grant action. The host persists bounded
signal-only receipts: an exact retry reconciles the same receipt, conflicting
key reuse and target substitution fail closed, and an already-terminal result
does not become a terminal receipt for the original effect. Protocol version 10
requires this complete non-fabricated target for interceptor, node, context,
node-state, memory, and compaction operations. Target, nonce, idempotency,
configuration, timeout, declaration, handler, or typed-input substitution
fails closed. Cancellation of a non-idempotent or externally effective node
is classified as ambiguous and is never automatically relaunched.

Version 8 wraps every command and response in a strict bounded correlation
frame. The runtime dependency owns a bounded 128-entry pending map, one writer,
and one reader that routes out-of-order responses to exact waiters. The host
continues reading while at most 32 service requests execute concurrently.
Malformed, oversized, unknown-correlation, EOF, and writer failures poison the
connection, close every pending waiter deterministically, and terminate the
host. An operation timeout does not erase its correlation or prove a terminal
effect; a later authenticated cancellation can still preempt the live worker.
Windows and Ubuntu/WSL2 process executions of
`cargo test -p agentmod-plugin-host --test multiplex --all-features --
--nocapture` pass all eight cases. They prove live interceptor, node, context,
and memory-worker preemption; exact cancellation-receipt replay; out-of-order
routing; unknown-correlation rejection with all-waiter closure; target and
configuration substitution rejection; non-idempotent node ambiguity without
relaunch; exact node-state write/read receipt replay; and disable/quarantine
cancellation of registered invocations with rejection of later work. Node-state CAS/read
completes synchronously under the persistence lock, so its process proof
intentionally validates exact authorization, replay, and substitution instead
of relying on a timing-sensitive live-preemption race.
