# Requirements Evidence Schema

`traceability.toml` is the current machine-readable requirements index. It uses
`schema_version = 1` and repeated `[[requirement]]` tables.

Required fields:

| Field | Meaning |
|---|---|
| `id` | Stable requirement identifier such as `E2E-01` |
| `capability` | Stable capability name |
| `owner` | Responsible process or subsystem |
| `tests` | Exact intended or implemented test identifiers |
| `platforms` | Platforms on which evidence is required |
| `evidence` | Paths or immutable CI artifact references |
| `status` | Evidence classification below |
| `limitation` | Precise gap or qualification |

Allowed status vocabulary:

- `implemented_unit_tested`
- `integration_tested`
- `e2e_validated`
- `benchmark_validated`
- `partial`
- `planned`
- `blocked`

Statuses are evidence levels, not percentages. A trait, scaffold, mock-only
adapter, ignored test, or documentation claim cannot justify `implemented`.
`e2e_validated` requires the process boundaries specified by the scenario.
`benchmark_validated` requires raw results, environment, commit, and command.

An evidence entry should identify a reproducible artifact, for example:

```toml
evidence = [
  "ci://run/123/jobs/e2e-04",
  "artifacts/acceptance/e2e-04-windows.json",
]
```

The current traceability file marks the 19 required scenarios as planned. It
must not be advanced until executable scenario evidence exists.
