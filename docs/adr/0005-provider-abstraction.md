# ADR 0005: Provider abstraction

Status: Accepted

Provider execution belongs only to the harness dependency layer. Harness data exposes
provider-neutral operation records, while each dependency adapter owns SDK/HTTP
mapping, authentication, streaming, tool-call decoding, and retry classification.

The contract models capabilities rather than assuming OpenAI message semantics.
Official APIs are used for OpenAI-compatible, OpenRouter, OpenAI, Anthropic, Gemini,
and local endpoints. A deterministic mock provider supports credential-free tests.
