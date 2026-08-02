$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build --locked -p agentmod-runtime -p agentmod-harness `
        -p agentmod-scheduler -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $scheduler = (Resolve-Path "target\debug\agentmod-scheduler.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_SCHEDULER_PROGRAM = $scheduler
    $env:AGENTMOD_SCHEDULER_ROOT = Join-Path $runRoot "scheduler"
    $runtimeStderr = Join-Path $runRoot "runtime.stderr.log"
    $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
        -WorkingDirectory $runRoot -WindowStyle Hidden `
        -RedirectStandardError $runtimeStderr -PassThru
    try {
        $ready = $false
        for ($attempt = 0; $attempt -lt 50; $attempt++) {
            $doctor = & $cli doctor --json 2>$null
            if ($LASTEXITCODE -eq 0) {
                $ready = $true
                break
            }
            Start-Sleep -Milliseconds 100
        }
        if (-not $ready) { throw "runtime did not become ready" }
        $doctorResult = $doctor | ConvertFrom-Json
        if ($doctorResult.state -ne "ready") {
            throw "runtime health was not ready"
        }

        $created = & $cli session create --workspace $repository `
            --style persistent-chat --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "session creation failed" }
        $listed = & $cli session list --limit 10 --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "session listing failed" }
        if ($listed.sessions.Count -ne 1) { throw "expected one session" }
        if ($listed.sessions[0].id -ne $created.session_id) {
            throw "created/listed session mismatch"
        }
        $turn = & $cli run "complete the deterministic turn" `
            --session $created.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="daemon-turn-ok"' --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) {
            $runtimeDiagnostics = Get-Content $runtimeStderr -ErrorAction SilentlyContinue
            $failedJournal = Get-Content (
                Join-Path $runRoot ("sessions\" + $created.session_id + "\events.jsonl")
            ) -ErrorAction SilentlyContinue
            throw "turn execution failed`n$runtimeDiagnostics`n$failedJournal"
        }
        if ($turn.last_committed_sequence -le 0) {
            throw "turn did not commit its provider lifecycle"
        }
        $visible = ($turn.events | Where-Object event -eq "text" |
            ForEach-Object text) -join ""
        if ($visible -ne "alpha beta daemon-turn-ok") {
            throw "unexpected provider output: $visible"
        }
        $sessionRoot = Join-Path $runRoot ("sessions\" + $created.session_id)
        foreach ($required in @(
            "metadata.json", "events.jsonl", "style.json", "style.lock",
            "workspace.json", "continuations", "snapshots", "artifacts",
            "process-logs", "branches"
        )) {
            if (-not (Test-Path (Join-Path $sessionRoot $required))) {
                throw "missing session entry: $required"
            }
        }
        $journal = Get-Content (Join-Path $sessionRoot "events.jsonl")
        if ($journal.Count -ne $turn.last_committed_sequence) {
            throw "canonical journal head diverged from the turn receipt"
        }
        $eventTypes = @($journal | ForEach-Object {
            ($_ | ConvertFrom-Json).event.metadata.event_type
        })
        $expectedCounts = @{
            "session.created" = 1
            "conversation.entry_committed" = 2
            "style.execution_initialized" = 1
            "style.node_entered" = 4
            "style.node_completed" = 4
            "style.transition_selected" = 3
            "model.request_proposed" = 1
            "model.request_approved" = 1
            "model.request_started" = 1
            "model.response_completed" = 1
            "memory.write_proposed" = 2
            "memory.write_approved" = 2
            "memory.write_dispatched" = 2
            "memory.write_completed" = 2
        }
        foreach ($entry in $expectedCounts.GetEnumerator()) {
            $actual = @($eventTypes | Where-Object { $_ -eq $entry.Key }).Count
            if ($actual -ne $entry.Value) {
                throw "expected $($entry.Value) $($entry.Key), found $actual"
            }
        }
        $actualDeltaCount = @(
            $eventTypes | Where-Object { $_ -eq "model.output_delta_observed" }
        ).Count
        if ($actualDeltaCount -lt 1) {
            throw "expected at least one model.output_delta_observed event"
        }
        $providerOrder = @(
            "model.request_proposed",
            "model.request_approved",
            "model.request_started",
            "model.response_completed"
        ) | ForEach-Object { [Array]::IndexOf($eventTypes, $_) }
        $expectedMemoryTail = @(
            "memory.write_proposed",
            "memory.write_approved",
            "memory.write_dispatched",
            "memory.write_completed",
            "memory.write_proposed",
            "memory.write_approved",
            "memory.write_dispatched",
            "memory.write_completed"
        )
        $actualMemoryTail = @($eventTypes | Select-Object -Last 8)
        if ($eventTypes[0] -ne "session.created" -or
            ($actualMemoryTail -join ",") -ne
                ($expectedMemoryTail -join ",") -or
            $providerOrder[0] -lt 0 -or
            $providerOrder[0] -ge $providerOrder[1] -or
            $providerOrder[1] -ge $providerOrder[2] -or
            $providerOrder[2] -ge $providerOrder[3]) {
            throw (
                "canonical provider/memory lifecycle event order was incorrect: " +
                ($eventTypes -join ",")
            )
        }
        $inspected = & $cli session inspect $created.session_id --json |
            ConvertFrom-Json
        $binding = $inspected.state.style_binding
        $execution = $inspected.state.style_execution
        $expectedExecutors = @{
            "prepare-context" = @("runtime.context-construction", "1.0.0")
            "respond" = @("runtime.model-request", "1.0.0")
            "tool-batch" = @("runtime.tool-gate", "1.1.0")
            "done" = @("runtime.turn-completion", "1.0.0")
        }
        if ($binding.id -ne "persistent-chat" -or
            $binding.version -ne "1.2.0" -or
            $binding.execution_plan.compilation.compiler -ne
                "agentmod-runtime-node-plan@3" -or
            @($binding.execution_plan.nodes).Count -ne 4 -or
            $execution.execution_contract.execution_plan_hash -ne
                $binding.execution_plan_hash -or
            $execution.execution_contract.registry_hash -ne
                $binding.execution_plan.registry_hash -or
            $null -eq $execution -or
            $null -ne $execution.active_node -or
            @($execution.completed_nodes).Count -ne 4 -or
            @($execution.transitions).Count -ne 3 -or
            @($execution.completed_nodes).node_id[0] -ne "prepare-context" -or
            @($execution.completed_nodes).node_id[1] -ne "respond" -or
            @($execution.completed_nodes).node_id[2] -ne "tool-batch" -or
            @($execution.completed_nodes).node_id[3] -ne "done" -or
            @($inspected.state.conversation.provider_projection).Count -ne 2) {
            throw "session inspection did not expose the completed style graph"
        }
        foreach ($nodeId in $expectedExecutors.Keys) {
            $resolution = @(
                $binding.execution_plan.nodes |
                    Where-Object node_id -eq $nodeId
            )
            if ($resolution.Count -ne 1 -or
                $resolution[0].executor_id -ne
                    $expectedExecutors[$nodeId][0] -or
                $resolution[0].executor_version -ne
                    $expectedExecutors[$nodeId][1] -or
                $resolution[0].source.kind -ne "runtime" -or
                $resolution[0].boundary -ne "runtime_logic") {
                throw "invalid exact executor resolution for $nodeId"
            }
        }
        Write-Output (
            "runtime/CLI/harness persistent-chat 1.2 generic durable-turn " +
            "E2E passed"
        )
    }
    finally {
        if ($null -ne $daemon -and -not $daemon.HasExited) {
            Stop-Process -Id $daemon.Id -Force
        }
        $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
        $resolvedRun = (Resolve-Path $runRoot).Path
        if ($resolvedRun.StartsWith($resolvedTemp) -and
            (Split-Path $resolvedRun -Leaf).StartsWith("agentmod-e2e-")) {
            Remove-Item -LiteralPath $resolvedRun -Recurse -Force
        }
    }
}
finally {
    Remove-Item Env:AGENTMOD_RUNTIME_ENDPOINT -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_RUNTIME_AUTH_TOKEN -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_HARNESS_PROGRAM -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_SCHEDULER_PROGRAM -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_SCHEDULER_ROOT -ErrorAction SilentlyContinue
    Pop-Location
}
