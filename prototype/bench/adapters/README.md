# External harness adapters (optional)

Each adapter lets `bench/run.mjs --external <name>` run the same tasks through
another coding harness for comparison. None is a test dependency: an adapter
whose CLI is not installed is reported as skipped.

An adapter exports:

```js
export async function available() -> boolean
export async function run({ workspace, prompt, model, timeoutMs }) -> { usage?: { input_tokens, output_tokens, cost, … } }
```

The runner grades the workspace exactly as it grades AgentMod runs (hidden
checks, regression suite). Usage numbers are whatever the external tool
reports; AgentMod's own numbers come from its session logs.

The bundled adapters are thin and best-effort (CLI flags change between
releases); check them against the tool's current documentation before
trusting a comparison.
