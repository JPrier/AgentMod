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
cargo run -p agentmod-cli -- style list
cargo run -p agentmod-cli -- harness list
cargo run -p agentmod-cli -- session create --workspace . \
  --style persistent-chat --harness native \
  --memory sqlite-fts --compaction sliding_window \
  --max-iterations 8 --max-steps 250 --max-tokens 500000 \
  --max-cost-micros 50000000 --max-duration-ms 1800000 --json
cargo run -p agentmod-cli -- run "hello" --session <id> --json
```

On Windows, `tests/e2e/runtime_cli.ps1`,
`tests/e2e/runtime_style_registry.ps1`, and
`tests/e2e/runtime_style_context.ps1`, and
`tests/e2e/runtime_harness_selection.ps1` exercise the real daemon over a named
pipe. Matching Unix scripts exist, but `STATUS.md` records which platforms were
actually executed.

## Live providers and the independent harness

The native harness executes live provider adapters in its dependency layer:
generic OpenAI-compatible, OpenRouter, OpenAI, Anthropic, Gemini, and local
endpoints. Secrets are configured through environment references or `file:`
references (never inline options); TLS verification defaults to enabled. See
`docs/guides/providers.md` for setup. A separate `agentmod-harness-fixture`
binary provides a genuinely independent second harness implementation and is
registered in the runtime harness registry as `independent`.

The immutable style binding may select a live model-generated summary by
setting `compaction.summary` in the style manifest. This runs through the
normal proposal/policy/dispatch/terminal-evidence path with canonical
`context.summary_*` events and durable recovery; the deterministic typed
summary remains the default. See `docs/architecture/context-model.md` and
`docs/architecture/execution-plan.md` for the reconciliation record.

## License

Licensed under either Apache License, Version 2.0 or MIT license at your option.
