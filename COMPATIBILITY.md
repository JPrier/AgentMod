# Compatibility

AgentMod currently targets Rust 1.91.1 and the current stable Cargo resolver.
Supported release targets will include:

- Windows on MSVC;
- Linux on GNU;
- macOS on Apple Silicon and Intel where CI runners are available.

Protocol and persistence schemas are versioned from their first implementation.
Before the first stable release, compatibility may change, but incompatible data or
wire versions must fail explicitly rather than being guessed.

The current local runtime protocol is 2.0. Version 2 adds session-scoped durable
approval resolution and returns resumed provider events. Runtime 1.x clients are
rejected during major-version negotiation. Durable continuation records use
schema 2; earlier storage-only records cannot reconstruct an executable pending
action and therefore fail with an explicit unsupported-schema error.

Provider live compatibility requires supported official APIs and is tested separately
from the credential-free default suite. Exact validated versions will be recorded in
release evidence. The live provider adapters (OpenAI-compatible, OpenRouter, OpenAI,
Anthropic, Gemini, local) are implemented in the native harness dependency layer;
official-API smoke tests remain opt-in and are not required by the default offline
suite. Harness protocol `Usage` and completion events gain serde-defaulted
reasoning-token, estimated-usage, and cost fields, and the `catalog` command is an
additive wire variant, so older harnesses and runtime clients remain readable.
