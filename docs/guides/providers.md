# Provider Setup Guide

AgentMod's native harness executes provider adapters in its dependency layer.
Live adapters exist for:

| Adapter ID | Endpoint | Wire format | Secret header |
|---|---|---|---|
| `openai-compatible` | any OpenAI-compatible server | Chat Completions SSE | `Authorization: Bearer` |
| `openrouter` | `https://openrouter.ai/api/v1` | Chat Completions SSE | `Authorization: Bearer` |
| `openai` | `https://api.openai.com/v1` | Chat Completions SSE | `Authorization: Bearer` |
| `anthropic` | `https://api.anthropic.com` | Messages SSE | `x-api-key` |
| `gemini` | `https://generativelanguage.googleapis.com` | `streamGenerateContent` SSE | `x-goog-api-key` |
| `local` | any local OpenAI-compatible endpoint | Chat Completions SSE | none (or `Authorization: Bearer`) |

The deterministic `deterministic-mock` provider remains credential-free and is
always available for offline development and tests.

## Building and starting

```shell
cargo build -p agentmod-harness
```

The harness is a bounded JSONL process endpoint. The runtime supervises it; to
exercise a provider adapter directly, run the harness binary with an
authorization key and drive it over stdin:

```shell
export AGENTMOD_HARNESS_AUTH_KEY=<64-hex-character-key>
printf '%s\n' '{"command":"health"}' \
  | AGENTMOD_HARNESS_AUTH_KEY=$AGENTMOD_HARNESS_AUTH_KEY ./target/debug/agentmod-harness
```

## Environment configuration

Each provider reads `AGENTMOD_PROVIDER_<ID>_*` variables from the harness
process environment:

| Variable | Meaning | Default |
|---|---|---|
| `AGENTMOD_PROVIDER_<ID>_BASE_URL` | Explicit base URL | provider default (below) |
| `AGENTMOD_PROVIDER_<ID>_API_KEY` | Secret API key | none |
| `AGENTMOD_PROVIDER_<ID>_MODEL` | Default model | adapter default |
| `AGENTMOD_PROVIDER_<ID>_MODELS` | Comma-separated model list for discovery | adapter default |
| `AGENTMOD_PROVIDER_<ID>_TIMEOUT_MS` | Per-request deadline | `120000` |
| `AGENTMOD_PROVIDER_<ID>_TLS_VERIFY` | Peer verification (`false` disables) | `true` |
| `AGENTMOD_PROVIDER_<ID>_REQUIRE_KEY` | Fail when the key is absent | unset |
| `AGENTMOD_PROVIDER_<ID>_PRICING_JSON` | Optional pricing table (see Cost) | none |

Default base URLs:

- `openai`: `https://api.openai.com/v1`
- `openrouter`: `https://openrouter.ai/api/v1`
- `anthropic`: `https://api.anthropic.com`
- `gemini`: `https://generativelanguage.googleapis.com`
- `openai-compatible` and `local`: required; no default.

## OpenRouter example

```shell
export AGENTMOD_PROVIDER_OPENROUTER_API_KEY=sk-or-v1-...
export AGENTMOD_PROVIDER_OPENROUTER_MODELS="openrouter/auto,anthropic/claude-3.5-sonnet"
```

Model discovery (`{"command":"catalog"}`) reports the configured models with
capabilities, context limits, and pricing-record source.

## Local OpenAI-compatible example

Run any OpenAI-compatible server locally (for example
[llama.cpp](https://github.com/ggml-org/llama.cpp) or vLLM), then:

```shell
export AGENTMOD_PROVIDER_LOCAL_BASE_URL=http://127.0.0.1:8080/v1
export AGENTMOD_PROVIDER_LOCAL_MODEL=my-local-model
```

No API key is required. `base_url`, `tls_verify`, `timeout_ms`, and
`api_key_ref` may also be passed as per-request options for all adapters.

## Model discovery and listing

`HarnessCommand::Catalog` returns a bounded provider/model catalog:

```text
{"command":"catalog"}
{"reply":"catalog","value":{"providers":[{"id":"openrouter",...}]}}
```

Each entry includes `models`, `capabilities`, `context_limit`, `tool_support`,
`image_support`, `structured_output_support`, `streaming_support`,
`pricing_source`, and `available`. Live discovery may be cached by callers;
offline configuration remains usable because the catalog is derived from
configuration, not network calls.

## Safe credential handling

- Secret values are never accepted inside protocol frames, options, events,
  ordinary logs, or error messages. Passing a literal `api_key` option is
  rejected.
- Secrets are resolved from environment references (`api_key_ref`, default
  `AGENTMOD_PROVIDER_<ID>_API_KEY`) or from a `file:` path reference, for
  example `api_key_ref: "file:/run/secrets/openrouter.key"`. File references
  are bounded to 64 KiB and trailing newlines are trimmed.
- Authorization headers are never echoed in failure diagnostics; transport and
  provider error messages are redacted and bounded.
- The runtime supervises the harness with a cleared environment, so provider
  variables must be forwarded by the runtime composition root (see
  `docs/integration/TASK-08-live-providers.md`).

## Cost metadata

Cost metadata is only attached when a pricing record exists for the exact
model; there are no invented zero-cost values. Configure records through
`AGENTMOD_PROVIDER_<ID>_PRICING_JSON` or the `pricing_json` option:

```json
{
  "source": "my-pricing-record",
  "version": "2026-07",
  "currency": "USD",
  "models": {
    "gpt-4o-mini": {
      "input_per_1k_micros": 150,
      "output_per_1k_micros": 600,
      "cache_read_per_1k_micros": 75,
      "cache_write_per_1k_micros": 150
    }
  }
}
```

## Retry behavior

The harness classifies failures but never retries automatically:

- safe pre-dispatch transport failures and timeouts: retryable
- HTTP 429 with `Retry-After`: retry after the supplied delay
- HTTP 5xx: retry after a bounded delay
- authentication failures, invalid requests, and unsupported capabilities:
  never retried
- disconnects or failures after partial output: ambiguous, never retried

Runtime business logic decides whether and how to retry.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `provider has no configured base URL` | missing `AGENTMOD_PROVIDER_<ID>_BASE_URL` or default |
| `secret values must be provided through an environment reference` | literal `api_key` option was passed |
| `provider rejected the supplied credentials (HTTP 401)` | wrong or missing key |
| `provider rate limited the request (HTTP 429)` | provider limit; retry delay is classified |
| `provider endpoint or model was not found (HTTP 404)` | wrong base URL or model ID |
| `provider stream exceeded the bounded output budget` | very large responses; bounds are fail-closed |
| `ambiguous_disconnect` | stream ended after partial output; never auto-retried |

The default test suite requires no credentials: every live adapter wire format
is covered by deterministic local HTTP fixtures in
`apps/harness/dependency/tests/live_fixtures.rs`. Opt-in live smoke scripts in
`tests/e2e/` run only when explicit environment variables are set; see
`docs/integration/TASK-08-live-providers.md`.
