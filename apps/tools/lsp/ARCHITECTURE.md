# LSP Tool Host

The LSP host is a separate, newline-delimited tool-protocol process. It follows
the mandatory dependency chain:

```text
tool protocol
    -> service
    -> logic
    -> data
    -> dependency
    -> LSP 3.17 stdio server
```

## Layer responsibilities

- `service` owns tool descriptors, wire validation, JSONL framing, and explicit
  mappings between tool-protocol DTOs and logic commands. Rename, formatting,
  and code-action responses are labeled and returned as proposals.
- `logic` owns input invariants and safe-edit validation. It rejects empty
  rename targets, invalid ranges, zero tab sizes, and overlapping edits.
- `data` owns provider-independent LSP datasets and every mapping to/from the
  dependency layer.
- `dependency` is the only layer that uses filesystem, process, clock, stdio,
  JSON-RPC, or LSP wire types. It owns project-root containment, server
  selection, process lifecycle, framing, timeouts, cancellation, one automatic
  restart, capability negotiation, response normalization, and final
  authorization enforcement.
- `bin` is composition only. It reads bootstrap environment configuration,
  assembles adjacent layers, and starts the service.

No executable imports another executable's internals. The only frontend/runtime
contract used here is `agentmod-tool-protocol`, and it is consumed only by the
service endpoint.

## Security model

Execution is fail-closed when `AGENTMOD_LSP_AUTH_KEY_HEX`,
`AGENTMOD_LSP_AUTH_OWNER`, and `AGENTMOD_LSP_AUTH_SESSION` are not all
configured. The dependency boundary recomputes a BLAKE3 digest of the normalized
operation, then verifies a keyed grant binding owner, session, call ID, expiry,
nonce, and digest. Expiry is bounded and `(session, nonce)` is single-use.
Health and discovery never start a language-server process.

All document paths and returned file URIs must canonicalize inside the configured
workspace. The host invokes configured executables directly without a shell.
It never applies a workspace edit and never executes a command returned by a
code action.

## Configuration

`AGENTMOD_LSP_SERVERS_JSON` is an array:

```json
[
  {
    "id": "rust-analyzer",
    "command": "rust-analyzer",
    "arguments": [],
    "extensions": [".rs"],
    "language_id": "rust",
    "environment": {}
  }
]
```

The authorization key is exactly 32 bytes encoded as 64 hexadecimal characters.
The runtime permission system must issue the opaque grant only after its
mandatory policy pipeline approves the exact normalized request.

## Supported operations

Project-root detection, health, diagnostics, document symbols, workspace
symbols, definition, references, hover, signature help, rename proposals,
formatting proposals, and code-action proposals are implemented. Missing
servers or unsupported capabilities produce explicit unavailable results.
Cancellation uses `$/cancelRequest`; shutdown uses `shutdown` followed by
`exit`.

The dependency follows the official
[Language Server Protocol 3.17 specification](https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/).

## Verification

The `agentmod-lsp-fixture` binary is a deterministic local LSP server used by
dependency integration tests. It covers initialization, notification capture,
all supported methods, cancellation, shutdown, crash/restart, grant replay
denial, and fail-closed authorization. No network, credentials, or installed
language server is required.
