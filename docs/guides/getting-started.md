# Getting Started

The repository currently demonstrates verified architectural and core-runtime
foundations, not a daily-driver agent.

## Prerequisites

- Rust toolchain from `rust-toolchain.toml`
- Git

## Validate the checkout

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
cargo run -p xtask -- architecture --manifest-path Cargo.toml
```

## Run current vertical slices

```sh
cargo run -p agentmod-runtime
cargo run -p agentmod-cli -- doctor --json --strict
```

The runtime command reports health and exits by default. To run the authenticated
local listener, set a private bootstrap secret of at least 32 bytes:

```sh
cargo build -p agentmod-harness -p agentmod-runtime -p agentmod-cli
AGENTMOD_RUNTIME_AUTH_TOKEN=<bootstrap-secret> \
cargo run -p agentmod-runtime -- serve
```

The runtime locates the sibling `agentmod-harness` binary, creates an ephemeral
authorization key, and supervises the child. In a second terminal, export the
same runtime token:

```sh
agentmod session create --workspace . --json
agentmod run "hello" --session <created-id> \
  --option 'mock_scenario="streaming_text"' --json
```

This revision can create/list durable sessions and execute an offline provider
turn through the runtime protocol, but does not yet provide the complete native
tool loop, live provider adapters, interactive agent loop, or TUI. See
[CLI reference](../reference/cli.md), [RPC reference](../reference/rpc.md), and
[Architecture overview](../architecture/overview.md).
