# Recovery and Debugging

## Current checks

```sh
cargo run -p agentmod-cli -- doctor --json --strict
cargo run -p agentmod-runtime
cargo run -p xtask -- architecture --manifest-path Cargo.toml
```

The CLI doctor currently uses a deterministic client, so it does not establish
that a runtime process is reachable.

## Journal behavior

The JSONL dependency validates complete records and checksum chains. An invalid
final record can be quarantined and truncated through its recovery API; interior
corruption is refused. There is no user-facing recovery command or daemon
startup coordinator yet. Preserve the session directory before attempting
manual diagnosis.

## Safe diagnostics

Use normal Rust backtraces and test output. Do not paste prompts, source files,
tool output, event payloads, or secrets into issues unless explicitly redacted.
Normal product logging and protected debug-artifact workflows are still planned.

If a test fails, record the exact command, platform, Rust version, and first
causal error. Do not delete journals, artifacts, branches, or worktrees as a
routine recovery step.
