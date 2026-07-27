# Kernel Benchmark Baseline — 2026-07-27

This is a measured development baseline, not a product-wide performance claim.
The upstream repository had no initial commit, so the run identifies the exact
command and dirty working tree rather than a commit SHA.

## Environment

- Command: `cargo run --release -p agentmod-benchmarks`
- Profile: `release`
- OS/target: Windows x86_64, MSVC
- CPU: AMD Ryzen 7 5800X3D, 8 cores / 16 logical processors
- Rust: `rustc 1.91.1 (ed61e7d7e 2025-11-07)`
- Repository state: unborn branch with uncommitted implementation files
- Runner schema: 1

## Results

| Benchmark | Iterations | ns/op | operations/second |
|---|---:|---:|---:|
| Event envelope seal | 100,000 | 3,490.825 | 286,465 |
| Event envelope verify | 250,000 | 3,169.691 | 315,488 |
| Protocol CBOR round trip | 50,000 | 2,479.868 | 403,247 |
| Expression parse | 100,000 | 1,494.739 | 669,013 |
| Expression evaluate | 500,000 | 80.318 | 12,450,478 |
| Graph compile | 10,000 | 14,969.890 | 66,801 |
| Protocol decode only | 100,000 | 1,672.602 | 597,871 |

The runner performs a short warm-up but is not a statistical harness. Results
cover only deterministic in-process kernels. Journal append, replay, snapshot
restore, context construction, artifacts, dormant sessions, concurrent sessions,
TUI throughput, and cross-process round trips still require measured benchmarks.
