$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build --locked -p agentmod-runtime -p agentmod-harness -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-iteration-memory-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $styleRoot = Join-Path $runRoot "styles\user"
    New-Item -ItemType Directory -Path $styleRoot -Force | Out-Null
    $stylePath = Join-Path $styleRoot "iteration-memory.toml"
    @'
schema_version = 1
kind = "custom"
required_capabilities = ["approval", "artifacts", "context", "model", "tools"]
allowed_tool_groups = ["filesystem"]
allowed_providers = ["deterministic-mock"]
allowed_plugins = ["runtime.security"]

[identity]
id = "e2e-iteration-memory"
version = "1.0.0"
runtime_api = "^1.0"

[graph]
kind = "inline"
source = '''
format_version = 1
entry = "fresh-context"

[budget]
max_steps = 500
max_tokens = 750000
max_cost_micros = 75000000
max_duration_ms = 2700000

[declarations]
capabilities = ["artifacts", "context", "model", "tools"]
tools = ["filesystem.read"]
providers = ["deterministic-mock"]

[[variables]]
name = "model_disposition"
type = { kind = "enum", values = ["response_complete", "tool_requests"] }
scope = "run"
producer = "research"
consumers = ["tool-batch"]
mutability = "mutable"
max_size_bytes = 64
security_classification = "internal"

[[variables]]
name = "model_result"
type = { kind = "node_result_reference" }
scope = "run"
producer = "research"
consumers = ["tool-batch"]
mutability = "mutable"
max_size_bytes = 512
security_classification = "internal"

[[variables]]
name = "research_receipt"
type = { kind = "node_result_reference" }
scope = "run"
producer = "tool-batch"
consumers = ["persist"]
mutability = "mutable"
max_size_bytes = 512
security_classification = "internal"

[[variables]]
name = "receipt_artifact"
type = { kind = "artifact_reference" }
scope = "run"
producer = "persist"
consumers = ["done"]
mutability = "mutable"
max_size_bytes = 512
security_classification = "confidential"

[[variables]]
name = "iteration"
type = { kind = "map", value_type = { kind = "boolean" }, max_entries = 1 }
scope = "run"
producer = "repeat"
consumers = ["repeat"]
mutability = "mutable"
max_size_bytes = 128
security_classification = "internal"

[[nodes]]
id = "fresh-context"
kind = "context_transform"
configuration = { type = "context_transform", strategy = "fresh" }

[[nodes]]
id = "research"
kind = "model_call"
provider = "deterministic-mock"
write_variables = ["model_disposition", "model_result"]
retry_limit = 2
configuration = { type = "model_request", disposition_output = "model_disposition", result_output = "model_result" }

[[nodes]]
id = "tool-batch"
kind = "tool_execution_gate"
read_variables = ["model_disposition", "model_result"]
write_variables = ["research_receipt"]
read_scopes = ["workspace"]
configuration = { type = "provider_tool_batch_execution", request_reference_variable = "model_result", disposition_variable = "model_disposition", maximum_calls = 32, allowed_tools = ["filesystem.read"] }

[[nodes]]
id = "persist"
kind = "persist_artifact"
read_variables = ["research_receipt"]
write_variables = ["receipt_artifact"]
configuration = { type = "persist_artifact", content = { kind = "provider_result_text", reference_variable = "research_receipt" }, mime_type = "text/markdown", security = "private", retention = "session" }

[[nodes]]
id = "repeat"
kind = "loop"
read_variables = ["iteration"]
write_variables = ["iteration"]
max_iterations = 3

[[nodes]]
id = "done"
kind = "complete_session"
read_variables = ["receipt_artifact"]

[[edges]]
from = "fresh-context"
to = "research"

[[edges]]
from = "research"
to = "tool-batch"

[[edges]]
from = "tool-batch"
to = "persist"

[[edges]]
from = "persist"
to = "repeat"

[[edges]]
from = "repeat"
to = "fresh-context"
condition = "iteration.remaining == true"
label = "continue"

[[edges]]
from = "repeat"
to = "done"
condition = "iteration.remaining == false"
label = "complete"
'''

[[interceptors]]
id = "runtime-style-policy"
owner = "runtime.security"
event = "action.proposed"
stage = 10
priority = 100
before = []
after = []
supported_decisions = [
  "continue",
  "replace",
  "reject",
  "require_approval",
  "cancel",
]
required_capabilities = ["approval"]

[memory]
provider = "file"
scopes = ["session"]
retrieval_timing = "iteration_start"
max_items = 64
max_injected_bytes = 524288
write_policy = "iteration_completion"
injection_location = "before_current_input"

[memory.query]
source = "session_goal"
include_active_artifacts = true
include_style_context = true
max_query_bytes = 32768

[compaction]
strategy = "none"
reserved_context_tokens = 0
max_provider_projection_tokens = 0
preserve_unresolved_tasks = true
preserve_active_processes = true
preservation_requirements = [
  "system_instructions",
  "current_input",
  "pending_control_state",
  "artifact_references",
  "memory_provenance",
  "active_graph_state",
  "tool_call_correlation",
]

[approvals]
default = "ask"
groups = { "filesystem.read" = "allow" }

[budgets]
max_iterations = 16
max_steps = 500
max_tokens = 750000
max_cost_micros = 75000000
max_duration_ms = 2700000

[child_agents]
max_children = 0
max_concurrent = 0
max_depth = 0
per_child_token_budget = 0

[retry]
max_attempts = 4
initial_backoff_ms = 0
max_backoff_ms = 0
retryable_failures = ["provider.unavailable"]

[termination]
allowed_outcomes = ["complete_session", "fail"]
on_hard_limit = "fail"
require_explicit_terminal_node = true

[selection]
requires_explicit_selection = true
model_may_select = false
'@ | Set-Content -LiteralPath $stylePath -NoNewline

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-iteration-memory-e2e-" +
        [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_MEMORY_WRITE_POST_PERSIST_DELAY_MS = "10000"
    $runtimeErr = Join-Path $runRoot "runtime.stderr.log"

    function Wait-RuntimeReady {
        for ($attempt = 0; $attempt -lt 100; $attempt++) {
            try {
                & $cli doctor --json 2>$null | Out-Null
                if ($LASTEXITCODE -eq 0) { return }
            } catch {}
            Start-Sleep -Milliseconds 100
        }
        if (Test-Path -LiteralPath $runtimeErr) {
            Get-Content -LiteralPath $runtimeErr -ErrorAction SilentlyContinue |
                Write-Error
        }
        throw "runtime did not become ready"
    }

    function Start-Runtime {
        $script:daemon = Start-Process -FilePath $runtime `
            -ArgumentList "serve" -WorkingDirectory $runRoot `
            -WindowStyle Hidden -RedirectStandardError $runtimeErr -PassThru
        Wait-RuntimeReady
    }

    function Stop-Runtime {
        if ($null -ne $script:daemon -and -not $script:daemon.HasExited) {
            Stop-Process -Id $script:daemon.Id -Force
            $script:daemon.WaitForExit()
        }
        $script:daemon = $null
    }

    function Read-Journal($sessionId) {
        $journal = Join-Path $runRoot (
            "sessions\" + $sessionId + "\events.jsonl"
        )
        return @(Get-Content -LiteralPath $journal | ForEach-Object {
            $_ | ConvertFrom-Json
        })
    }

    function Event-Count($events, $eventType) {
        return @($events | Where-Object {
            $_.event.metadata.event_type -eq $eventType
        }).Count
    }

    function Json-Identity($identity) {
        return $identity | ConvertTo-Json -Depth 30 -Compress
    }

    function Start-CutTurn($sessionId, $runId) {
        $info = [System.Diagnostics.ProcessStartInfo]::new()
        $info.FileName = $cli
        $info.WorkingDirectory = $repository
        $info.UseShellExecute = $false
        $info.CreateNoWindow = $true
        $info.RedirectStandardOutput = $true
        $info.RedirectStandardError = $true
        foreach ($argument in @(
            "run",
            "map the repository architecture",
            "--session", $sessionId,
            "--cancellation-id", $runId,
            "--option", 'mock_scenario="streaming_text"',
            "--option", 'mock_text="generic research finding"',
            "--json"
        )) {
            [void]$info.ArgumentList.Add($argument)
        }
        $process = [System.Diagnostics.Process]::new()
        $process.StartInfo = $info
        [void]$process.Start()
        return $process
    }

    $daemon = $null
    $turnProcess = $null
    Start-Runtime
    try {
        $validation = & $cli style validate $stylePath --json |
            ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or -not $validation.valid) {
            throw "iteration-memory style validation failed: $(
                $validation | ConvertTo-Json -Depth 20 -Compress
            )"
        }
        $session = & $cli session create --workspace $repository `
            --style e2e-iteration-memory --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "session creation failed" }
        $sessionId = $session.session_id
        $runId = "018f0f5d-8c2a-7d31-9e42-4f6b8a1c2d3e"
        $journalPath = Join-Path $runRoot (
            "sessions\" + $sessionId + "\events.jsonl"
        )
        $memoryPath = Join-Path $runRoot "memory\file.jsonl"
        $turnProcess = Start-CutTurn $sessionId $runId

        $reachedCut = $false
        for ($attempt = 0; $attempt -lt 300; $attempt++) {
            if ($turnProcess.HasExited) {
                $runtimeFailure = if (Test-Path -LiteralPath $runtimeErr) {
                    Get-Content -LiteralPath $runtimeErr -Raw
                } else {
                    "<no runtime stderr>"
                }
                throw "turn exited before crash cut: $(
                    $turnProcess.StandardError.ReadToEnd()
                ); runtime stderr: $runtimeFailure"
            }
            if ((Test-Path -LiteralPath $journalPath) -and
                (Test-Path -LiteralPath $memoryPath)) {
                $cutEvents = Read-Journal $sessionId
                $fileLines = @(Get-Content -LiteralPath $memoryPath)
                if ((Event-Count $cutEvents "memory.write_dispatched") -eq 1 -and
                    $fileLines.Count -eq 1) {
                    $reachedCut = $true
                    break
                }
            }
            Start-Sleep -Milliseconds 50
        }
        if (-not $reachedCut) {
            throw "iteration memory did not reach post-persist crash cut"
        }
        $beforeCut = Read-Journal $sessionId
        if ((Event-Count $beforeCut "memory.write_completed") -ne 0) {
            throw "first iteration memory completed before crash cut"
        }
        $providerTypes = @(
            "model.request_proposed",
            "model.request_approved",
            "model.request_started",
            "model.response_completed"
        )
        foreach ($eventType in $providerTypes) {
            if ((Event-Count $beforeCut $eventType) -ne 3) {
                throw "provider work was incomplete before memory crash cut: $eventType"
            }
        }

        Stop-Runtime
        if (-not $turnProcess.WaitForExit(5000)) {
            $turnProcess.Kill($true)
            $turnProcess.WaitForExit()
        }
        $turnProcess = $null

        $env:AGENTMOD_MEMORY_WRITE_POST_PERSIST_DELAY_MS = "0"
        $env:AGENTMOD_HARNESS_PROGRAM = Join-Path $runRoot (
            "harness-must-not-be-spawned.exe"
        )
        $env:AGENTMOD_FIXTURE_HARNESS_PROGRAM = Join-Path $runRoot (
            "fixture-harness-must-not-be-spawned.exe"
        )
        Start-Runtime
        $recovered = & $cli run "map the repository architecture" `
            --session $sessionId --cancellation-id $runId `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="generic research finding"' --json |
            ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "iteration memory recovery failed" }
        if (@($recovered.events).Count -ne 0) {
            throw "restart redispatched provider-visible work"
        }

        $events = Read-Journal $sessionId
        foreach ($eventType in $providerTypes) {
            if ((Event-Count $events $eventType) -ne 3) {
                throw "provider lifecycle changed after restart: $eventType"
            }
        }
        foreach ($eventType in @(
            "memory.write_proposed",
            "memory.write_approved",
            "memory.write_dispatched",
            "memory.write_completed"
        )) {
            if ((Event-Count $events $eventType) -ne 3) {
                throw "iteration memory lifecycle count is invalid: $eventType"
            }
        }
        if ((Event-Count $events "model.request_failed") -ne 0 -or
            (Event-Count $events "model.request_cancelled") -ne 0) {
            throw "provider work failed or was cancelled during recovery"
        }

        $loopTransitions = @(
            $events | Where-Object {
                $_.event.metadata.event_type -eq "style.transition_selected" -and
                $_.event.payload.payload.from_node_id -eq "repeat"
            } | Sort-Object { [int64]$_.event.metadata.sequence }
        )
        if ($loopTransitions.Count -ne 3) {
            throw "expected exactly three successful loop transitions"
        }
        $memoryEvents = @(
            $events | Where-Object {
                $_.event.metadata.event_type -like "memory.write_*"
            } | Sort-Object { [int64]$_.event.metadata.sequence }
        )
        $groups = @($memoryEvents | Group-Object {
            $_.event.payload.payload.identity.write_id
        })
        if ($groups.Count -ne 3) {
            throw "iteration memory did not retain three unique identities"
        }
        $ids = [Collections.Generic.HashSet[string]]::new()
        $sources = [Collections.Generic.HashSet[string]]::new()
        $iterations = [Collections.Generic.HashSet[string]]::new()
        $fileRecords = @(
            Get-Content -LiteralPath $memoryPath | ForEach-Object {
                $_ | ConvertFrom-Json
            }
        )
        if ($fileRecords.Count -ne 3) {
            throw "file memory did not retain exactly three records"
        }
        $expectedLifecycle = @(
            "memory.write_proposed",
            "memory.write_approved",
            "memory.write_dispatched",
            "memory.write_completed"
        )
        foreach ($group in $groups) {
            $lifecycle = @(
                $group.Group |
                    Sort-Object { [int64]$_.event.metadata.sequence }
            )
            if ($lifecycle.Count -ne 4) {
                throw "one iteration memory lifecycle is incomplete"
            }
            for ($index = 0; $index -lt $expectedLifecycle.Count; $index++) {
                if ($lifecycle[$index].event.metadata.event_type -ne
                    $expectedLifecycle[$index]) {
                    throw "iteration memory lifecycle order is invalid"
                }
            }
            $proposal = $lifecycle[0].event.payload.payload
            $identity = $proposal.identity
            $identityJson = Json-Identity $identity
            foreach ($event in $lifecycle) {
                if ((Json-Identity $event.event.payload.payload.identity) -ne
                    $identityJson) {
                    throw "iteration memory identity changed during lifecycle"
                }
            }
            $digest = $lifecycle[1].event.payload.payload.action_digest
            if ($lifecycle[2].event.payload.payload.action_digest -ne $digest -or
                $lifecycle[3].event.payload.payload.action_digest -ne $digest) {
                throw "iteration memory action digest changed"
            }
            if ($identity.policy -ne "iteration_completion" -or
                $identity.provider -ne "file" -or
                $identity.scope -ne ("session:" + $sessionId) -or
                $identity.run_id -ne $runId -or
                $null -ne $identity.session_completion -or
                $null -eq $identity.iteration_completion) {
                throw "iteration memory identity is not boundary-bound"
            }
            if (-not $ids.Add([string]$identity.write_id) -or
                -not $sources.Add([string]$identity.source)) {
                throw "iteration memory IDs or sources are not unique"
            }
            $sourcePrefix = (
                "runtime.automatic_memory:iteration_completion:" +
                $runId + ":"
            )
            if ($identity.write_id -notmatch '^[0-9a-f]{64}$' -or
                -not $identity.source.StartsWith($sourcePrefix) -or
                $identity.source.Substring($sourcePrefix.Length) -notmatch
                    '^[0-9a-f]{64}$') {
                throw "iteration memory v2 ID or boundary source is malformed"
            }

            $boundary = $identity.iteration_completion
            [void]$iterations.Add([string]$boundary.loop_iteration)
            $matchingTransition = @(
                $loopTransitions | Where-Object {
                    [int64]$_.event.metadata.sequence -eq
                        [int64]$boundary.sequence
                }
            )
            if ($matchingTransition.Count -ne 1) {
                throw "iteration boundary sequence does not select one transition"
            }
            $transition = $matchingTransition[0]
            $selected = $transition.event.payload.payload
            if ($boundary.loop_node_id -ne $selected.from_node_id -or
                $boundary.destination_node_id -ne $selected.to_node_id -or
                [int64]$boundary.attempt -ne [int64]$selected.attempt -or
                [int64]$boundary.loop_iteration -ne
                    [int64]$selected.loop_iteration -or
                [int64]$boundary.step -ne [int64]$selected.step -or
                $boundary.event_checksum -ne
                    $transition.event.integrity_checksum) {
                throw "iteration boundary does not match its retained transition"
            }

            $typed = $proposal.content | ConvertFrom-Json
            if ($typed.schema -ne
                "agentmod.iteration-completion-memory.v1" -or
                (Json-Identity $typed.successful_iteration) -ne
                    (Json-Identity $boundary) -or
                @($typed.artifact_references).Count -lt 1) {
                throw "iteration memory omitted typed boundary or artifact evidence"
            }
            $persistOutputs = @(
                $typed.node_outputs | Where-Object {
                    $_.node_id -eq "persist" -and
                    [int64]$_.loop_iteration -eq
                        [int64]$boundary.loop_iteration -and
                    $null -ne $_.artifact_reference
                }
            )
            if ($persistOutputs.Count -ne 1 -or
                @($typed.artifact_references) -notcontains
                    $persistOutputs[0].artifact_reference) {
                throw "iteration memory omitted exact persisted artifact evidence"
            }

            $completed = $lifecycle[3].event.payload.payload
            $file = @($fileRecords | Where-Object {
                $_.source -eq $identity.source
            })
            if ($file.Count -ne 1 -or -not $completed.retained -or
                $file[0].schema_version -ne 1 -or
                $file[0].id -ne $completed.reference -or
                $file[0].scope -ne $identity.scope -or
                $file[0].content -ne $proposal.content -or
                [int64]$file[0].created_at_millis -ne
                    [int64]$identity.created_at_millis) {
                throw "file record does not bind the exact iteration lifecycle"
            }
        }
        if ($iterations.Count -ne 3 -or
            -not $iterations.Contains("0") -or
            -not $iterations.Contains("1") -or
            -not $iterations.Contains("2")) {
            throw "iteration memory boundaries are not exact iterations 0, 1, and 2"
        }

        $inspect = & $cli session inspect $sessionId --json |
            ConvertFrom-Json
        $records = @(
            $inspect.state.automatic_memory_writes.PSObject.Properties.Value
        )
        if ($records.Count -ne 3 -or
            @($inspect.state.successful_iteration_completions).Count -ne 3 -or
            @($records | Where-Object {
                $_.state -ne "completed" -or -not $_.retained
            }).Count -ne 0) {
            throw "iteration memory replay projection is incomplete"
        }
        $beforeCount = $events.Count
        $beforeHead = $inspect.state.last_sequence
        $beforeAutomatic = (
            $inspect.state.automatic_memory_writes |
                ConvertTo-Json -Depth 50 -Compress
        )
        $beforeBoundaries = (
            $inspect.state.successful_iteration_completions |
                ConvertTo-Json -Depth 30 -Compress
        )
        $beforeJournalBytes = [Convert]::ToBase64String(
            [IO.File]::ReadAllBytes($journalPath)
        )
        $beforeMemoryBytes = [Convert]::ToBase64String(
            [IO.File]::ReadAllBytes($memoryPath)
        )

        Stop-Runtime
        Start-Runtime
        $replayed = & $cli session replay $sessionId --json |
            ConvertFrom-Json
        if ($replayed.command -ne "session_replay" -or
            $replayed.state.last_sequence -ne $beforeHead -or
            ($replayed.state.automatic_memory_writes |
                ConvertTo-Json -Depth 50 -Compress) -ne $beforeAutomatic -or
            ($replayed.state.successful_iteration_completions |
                ConvertTo-Json -Depth 30 -Compress) -ne $beforeBoundaries -or
            (Read-Journal $sessionId).Count -ne $beforeCount -or
            [Convert]::ToBase64String(
                [IO.File]::ReadAllBytes($journalPath)
            ) -ne $beforeJournalBytes -or
            [Convert]::ToBase64String(
                [IO.File]::ReadAllBytes($memoryPath)
            ) -ne $beforeMemoryBytes) {
            throw "pure replay mutated iteration memory state or bytes"
        }

        Write-Output (
            "runtime iteration-memory 3-loop/v2/crash/restart/replay E2E passed"
        )
    }
    finally {
        if ($null -ne $turnProcess -and -not $turnProcess.HasExited) {
            $turnProcess.Kill($true)
            $turnProcess.WaitForExit()
        }
        Stop-Runtime
        if ($runRoot.StartsWith([System.IO.Path]::GetTempPath())) {
            Remove-Item -LiteralPath $runRoot -Recurse -Force `
                -ErrorAction SilentlyContinue
        }
    }
}
finally {
    Remove-Item Env:AGENTMOD_MEMORY_WRITE_POST_PERSIST_DELAY_MS `
        -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_FIXTURE_HARNESS_PROGRAM `
        -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_HARNESS_PROGRAM -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_RUNTIME_ENDPOINT -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_RUNTIME_AUTH_TOKEN -ErrorAction SilentlyContinue
    Pop-Location
}
$global:LASTEXITCODE = 0
