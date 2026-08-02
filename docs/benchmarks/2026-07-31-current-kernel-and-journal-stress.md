# Current Kernel Benchmark and Journal Stress Evidence — 2026-07-31

This document records reproducible development evidence, not product-wide
performance or capacity claims. Results are filled only from the raw files in
`.omx/validation` after the commands below run on each platform.

## Scope and limitations

`agentmod-benchmarks` measures deterministic in-process kernels in a single
process and thread. It covers event envelope sealing and verification, protocol
CBOR encode/decode, expression parsing and evaluation, and graph compilation.
It does not exercise the runtime daemon, harness, plugin host, scheduler, tool
hosts, frontend, IPC, journal, artifacts, recovery, or provider execution.

`runtime_journal_stress` exercises only the filesystem JSONL journal dependency
with `Buffered` durability in temporary directories. It is a correctness and
contention stress test, not a throughput benchmark. It does not measure
data-sync latency, runtime event commitment, reducer replay, snapshots, or
cross-process concurrency.

No result in this document is a claim about runtime/process throughput, tail
latency, production capacity, or supported concurrency.

## Revision and environment

- HEAD: `abbf97b82687a4d1e7463aab33382258d6d38fd9`
- Working tree: dirty, with 334 porcelain entries at both platform metadata
  captures; the requested implementation exists as uncommitted work.
- Run timestamps (UTC): Windows metadata captured at
  `2026-07-31T11:47:19.7560656Z`; Linux metadata captured at
  `2026-07-31T11:52:07+00:00`.
- Windows: Microsoft Windows 11 Pro 10.0.26200 build 26200, x86_64 MSVC.
- Windows CPU: AMD Ryzen 7 5800X3D, 8 physical cores / 16 logical processors.
- Windows Rust/Cargo: Rust 1.91.1 / Cargo 1.91.1,
  `x86_64-pc-windows-msvc`.
- Linux: Ubuntu 24.04.3 LTS under WSL2 kernel
  `6.6.87.2-microsoft-standard-WSL2`, with the same AMD CPU exposed as 8 cores /
  16 logical processors; Rust 1.91.1 / Cargo 1.91.1,
  `x86_64-unknown-linux-gnu`.

Metadata commands:

```powershell
Get-Date -AsUTC -Format o
git rev-parse HEAD
git status --porcelain=v1
rustc -Vv
cargo -Vv
Get-CimInstance Win32_Processor |
  Select-Object Name, Manufacturer, NumberOfCores,
    NumberOfLogicalProcessors, MaxClockSpeed |
  ConvertTo-Json -Compress
Get-CimInstance Win32_OperatingSystem |
  Select-Object Caption, Version, BuildNumber, OSArchitecture |
  ConvertTo-Json -Compress
```

```sh
date --iso-8601=seconds --utc
git rev-parse HEAD
git status --porcelain=v1
rustc -Vv
cargo -Vv
uname -a
cat /etc/os-release
lscpu --json
```

## Exact commands

Windows:

```powershell
cargo run --release --locked -p agentmod-benchmarks
cargo test --release --locked -p agentmod-integration-tests `
  --test runtime_journal_stress -- --nocapture
```

Ubuntu/WSL2, run serially after Windows:

```powershell
wsl.exe bash -lc 'cd /mnt/c/Users/jkpri/AgentMod && \
  cargo run --release --locked -p agentmod-benchmarks'
wsl.exe bash -lc 'cd /mnt/c/Users/jkpri/AgentMod && \
  cargo test --release --locked -p agentmod-integration-tests \
  --test runtime_journal_stress -- --nocapture'
```

Raw evidence paths:

- `.omx/validation/2026-07-31-windows-kernel-benchmark.json`
- `.omx/validation/2026-07-31-linux-kernel-benchmark.json`
- `.omx/validation/2026-07-31-windows-journal-stress.json`
- `.omx/validation/2026-07-31-linux-journal-stress.json`

## Fixed benchmark workload

| Kernel operation | Iterations |
|---|---:|
| Event envelope seal | 100,000 |
| Event envelope verify | 250,000 |
| Protocol CBOR round trip | 50,000 |
| Expression parse | 100,000 |
| Expression evaluate | 500,000 |
| Graph compile | 10,000 |
| Protocol decode only | 100,000 |

The runner performs at most 100 untimed warm-up operations per case and then
reports one elapsed sample. It is not a statistical benchmark harness and does
not report median or tail percentiles.

## Benchmark results

### Windows x86_64 MSVC

Command wall time was 39.813 seconds, including a 37.58-second release build.
Those values are not included in any per-operation result.

| Kernel operation | Iterations | ns/op | operations/second |
|---|---:|---:|---:|
| Event envelope seal | 100,000 | 3,719.166 | 268,877 |
| Event envelope verify | 250,000 | 3,400.088 | 294,110 |
| Protocol CBOR round trip | 50,000 | 2,516.712 | 397,344 |
| Expression parse | 100,000 | 1,586.939 | 630,144 |
| Expression evaluate | 500,000 | 88.331 | 11,321,105 |
| Graph compile | 10,000 | 17,843.320 | 56,043 |
| Protocol decode only | 100,000 | 1,762.569 | 567,354 |

### Linux x86_64 GNU under WSL2

The successful direct command wall time was 22.0 seconds, including a
19.81-second release build. An earlier capture-wrapper attempt built and ran the
binary but then interpreted its multiline JSON as shell input; it is excluded
from the results, and the exact requested command was rerun directly.

| Kernel operation | Iterations | ns/op | operations/second |
|---|---:|---:|---:|
| Event envelope seal | 100,000 | 2,253.314 | 443,791 |
| Event envelope verify | 250,000 | 2,104.679 | 475,132 |
| Protocol CBOR round trip | 50,000 | 2,157.271 | 463,549 |
| Expression parse | 100,000 | 783.462 | 1,276,386 |
| Expression evaluate | 500,000 | 77.185 | 12,955,831 |
| Graph compile | 10,000 | 14,805.732 | 67,541 |
| Protocol decode only | 100,000 | 1,547.967 | 646,009 |

## Journal stress coverage and results

The contended-session test starts 8 writer threads behind one barrier. Each
writer appends 50 unique events to the same session, for exactly 400 accepted
events. Every retry scans the verified journal, derives the next sequence and
expected head event ID, and retries only sequence/head CAS conflicts. The final
scan requires 400 unique event IDs, contiguous sequence numbers, and an exact
checksum predecessor chain.

The isolation test starts 32 session-writer threads. Each writes 25 events to
its own session, for exactly 800 accepted events. Final scans require 25 events
per session and reject cross-session event-ID contamination. Together the two
tests validate 1,200 accepted buffered journal appends.

| Platform | Contended 8 × 50 | Isolated 32 × 25 | Test-reported elapsed | Command wall time |
|---|---|---|---:|---:|
| Windows | Passed, 400 events | Passed, 800 events | 3.52 s | 222.636 s |
| Linux/WSL2 | Passed, 400 events | Passed, 800 events | 0.87 s | 167.6 s |

The elapsed values above are test-run observations only. They must not be
converted into runtime throughput claims.

Both commands passed 2 tests with 0 failures. The Windows wall time includes a
Cargo-reported 3m38s release build; Linux includes a 2m46s release build.
