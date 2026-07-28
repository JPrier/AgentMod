# Agent Client Protocol frontend

The ACP adapter is a separate stdio JSON-RPC process using the official
`agent-client-protocol` Rust SDK and stable ACP wire version 1. It imports no
runtime internals and follows `service → logic → data → dependency`.

| Layer | Responsibility and owned types |
|---|---|
| service | ACP initialization/capabilities, JSON-RPC handlers, ACP content mapping, update notifications, permission requests, and stdio lifecycle |
| logic | Session identity/workspace invariants, prompt construction, bounded prompt streams, active-turn cancellation state, disconnect cancellation, and approval coordination |
| data | Session/turn datasets, bounded layer-owned streams, and explicit runtime-dependency normalization |
| dependency | Authenticated runtime socket/named-pipe negotiation, bounded layer-owned streams, exact credit windows, cancellation, session creation/loading, turns, and approvals |
| bin | Environment bootstrap and concrete assembly |

ACP sessions are runtime sessions. Prompt and cancellation requests therefore
enter the same runtime interception, event, permission, provider, and tool paths
as CLI and TUI requests. ACP SDK types remain confined to the service crate.
Runtime protocol types remain confined to the dependency crate.

Current limitations:

- Only text and resource-link prompt blocks are accepted; image, audio, and
  embedded-resource capabilities are not advertised.
- Per-session MCP declarations and additional workspace directories are
  rejected until their runtime activation contracts are available.
- Stable session listing, deletion, resume/close, and terminal callbacks are not
  advertised.

Provider events cross every ACP layer through capacity-one, layer-owned streams.
The dependency grants the runtime one additional credit only after the current
item enters the bounded dependency stream; the service emits each ordered ACP
notification immediately. Dropped frontend streams cancel the matching runtime
turn. The Windows process E2E uses paced harness frames and proves the first ACP
update is observable before provider completion; the Unix equivalent is
automated for CI.
