# AgentMod

AgentMod is an event-driven developer-agent platform written in Rust. It is being
built as a local-first terminal product and as an embeddable execution runtime with
durable sessions, interceptible actions, replayable event history, isolated provider
and tool processes, and replaceable execution policies.

The repository is under active implementation. Current evidence and incomplete work
are tracked in [STATUS.md](STATUS.md); planned behavior is not presented as shipped.

## Architecture

Every deployable subsystem follows:

```text
service -> logic -> data -> dependency
```

The runtime owns canonical session state. A separate harness owns provider execution.
Capability hosts own filesystem, process, web, browser, Git, LSP, and MCP effects.
Frontends use versioned runtime protocols only.

See [the initial maps](docs/architecture/initial-maps.md) and the architecture ADRs
under `docs/adr/`.

## Development

The pinned toolchain is Rust 1.91.1.

```shell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo architecture
```

The default test suite is designed to run without network credentials or paid APIs.

The current authenticated daemon/session vertical slice can be exercised with:

```shell
export AGENTMOD_RUNTIME_AUTH_TOKEN=<private-secret-at-least-32-bytes>
cargo run -p agentmod-runtime -- serve
# then, from another terminal:
cargo run -p agentmod-cli -- session create --workspace . --json
cargo run -p agentmod-cli -- run "hello" --session <id> --json
```

On Windows, `tests/e2e/runtime_cli.ps1` builds the two binaries and verifies a
real named-pipe health/create/list cycle. Unix uses `tests/e2e/runtime_cli.sh`.

## License

Licensed under either Apache License, Version 2.0 or MIT license at your option.
