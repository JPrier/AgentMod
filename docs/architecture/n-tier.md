# N-Tier Architecture

Every deployable process uses `service -> logic -> data -> dependency`. A layer
may call only the layer immediately below it. Boundary mappings are owned by the
caller, and each layer defines separate request, result, and error types.

## Runtime

| Layer | Current responsibility and principal types |
|---|---|
| Service | Maps `RuntimeRequest::Health` to `ServiceHealthRequest`, calls `RuntimeLogicPort`, and maps `ServiceHealthResponse` to `RuntimeResponse`. Other wire requests return `UnsupportedEndpoint`. |
| Logic | `RuntimeLogic` evaluates health. The `action`, `conversation`, `permission`, and `session` modules implement proposal, projection, policy, and reducer primitives, but are not yet exposed as endpoints. |
| Data | `RuntimeDataPort` constructs health data. `JournalEventDataPort` maps verified generic events to append/scan/recovery dependency requests. `SnapshotDataPort` normalizes, selects, and verifies versioned snapshots. |
| Dependency | `LocalRuntimeDependencies` checks storage. `JsonlJournalDependency` implements the journal, `LocalSnapshotDependency` stores immutable snapshots, and `LocalArtifactDependency` implements transactional content-addressed artifacts. |

The composition root is `apps/runtime/bin`. It constructs concrete layers and
executes one health request. `agentmod-runtime-protocol` is the wire contract;
only its health operation is currently served.

## Harness

| Layer | Current responsibility and principal types |
|---|---|
| Service | `HarnessService` maps `HarnessCommand::Health` to `ServiceHealthRequest` and maps `HarnessHealthResult` to `ServiceHealthResponse`. |
| Logic | `HarnessHealthManager` validates requested capability names and classifies readiness as ready, degraded, or unavailable. |
| Data | `HarnessHealthDataStore` aggregates configured/ready provider counts and capability sets. |
| Dependency | `StaticProviderCatalogDependency` returns the deterministic mock provider catalog. |

The composition root is `apps/harness/bin`. Provider execution commands are wire
types only and currently return an unsupported-command error.

## CLI

| Layer | Current responsibility and principal types |
|---|---|
| Service | `CliService` owns Clap parsing, `ServiceDoctorRequest`, output selection, rendering, and exit-code mapping. |
| Logic | `CliLogic` evaluates `RunDoctorCommand` and produces `DoctorResult`, `DoctorState`, and checks. |
| Data | `CliData` maps logic health requests into dependency health requests and normalizes availability. |
| Dependency | `DeterministicRuntimeClient` constructs and normalizes runtime health wire DTOs without a real transport. |

The composition root is `apps/cli/bin`. It assembles the layers and runs the
single implemented `doctor` command.

## Allowed and prohibited dependencies

Allowed: service to logic and its endpoint protocol; logic to data; data to
dependency and stable core primitives; dependency to external APIs or outbound
protocols; composition root to its own four layers.

Prohibited: skipped or upward layer calls, protocol DTOs in logic/data,
cross-process internal imports, external SDKs above dependency, shared business
DTO crates, cross-layer aliases/re-exports, and lower-layer callbacks carrying
upper-layer types. `xtask architecture` enforces these rules.
