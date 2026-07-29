$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-research-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-research-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $runtimeOut = Join-Path $runRoot "runtime.stdout.log"
    $runtimeErr = Join-Path $runRoot "runtime.stderr.log"

    function Start-TestRuntime {
        $process = Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput $runtimeOut -RedirectStandardError $runtimeErr
        for ($attempt = 0; $attempt -lt 80; $attempt++) {
            try {
                & $cli doctor --json 2>$null | Out-Null
                if ($LASTEXITCODE -eq 0) { return $process }
            } catch {}
            Start-Sleep -Milliseconds 100
        }
        if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force }
        throw "runtime did not become ready"
    }

    function Stop-TestRuntime($process) {
        if ($null -ne $process -and -not $process.HasExited) {
            Stop-Process -Id $process.Id -Force
            $process.WaitForExit()
        }
    }

    function Assert-ResearchState($inspection) {
        if ($inspection.state.style_binding.id -ne "research-loop" -or
            $inspection.state.style_binding.version -ne "1.1.0") {
            throw "research style binding mismatch"
        }
        if ($inspection.state.lifecycle -ne "completed") {
            throw "research session did not complete"
        }
        if ($inspection.state.style_execution.termination_reason -ne
                "complete_session") {
            throw "research graph termination was not retained"
        }
        $introspection = $inspection.state.style_introspection
        if ($introspection.style.id -ne "research-loop" -or
            $introspection.graph.active_node -ne $null -or
            $introspection.graph.loop_count -ne 3 -or
            $introspection.graph.retry_count -ne 0 -or
            $introspection.graph.next_eligible_transitions.Count -ne 0 -or
            $introspection.termination_reason -ne "complete_session") {
            throw "research graph introspection mismatch"
        }
        if ($introspection.graph.completed_nodes.Count -lt 15 -or
            $introspection.graph.previous_transitions.Count -lt 14 -or
            $introspection.remaining_budgets.steps -lt 0 -or
            $introspection.remaining_budgets.tokens -lt 0 -or
            $introspection.pipeline.blocking_interceptor_order -eq $null -or
            $introspection.memory.retrieved_provenance -eq $null -or
            $introspection.compaction.history.Count -lt 3 -or
            $introspection.child_agents.executions -eq $null -or
            $introspection.child_agents.joins -eq $null -or
            $introspection.child_agents.reviewer_findings -eq $null) {
            throw "research orchestration inspection is incomplete"
        }
        if (@($inspection.state.artifact_persistences.PSObject.Properties).Count -ne 3) {
            throw "expected three canonical research artifacts"
        }
        $persisted = @($inspection.state.style_execution.completed_nodes |
            Where-Object node_id -eq "persist")
        $iterations = @($inspection.state.style_execution.completed_nodes |
            Where-Object node_id -eq "repeat")
        if ($persisted.Count -ne 3 -or $iterations.Count -ne 3) {
            throw "research graph did not retain three inspectable iterations"
        }
    }

    $daemon = Start-TestRuntime
    try {
        $session = & $cli session create --workspace $repository `
            --style research-loop@1.1.0 --json | ConvertFrom-Json
        $result = & $cli run "map the repository architecture" `
            --session $session.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="deterministic finding"' `
            --option 'research_complete_after=3' --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) {
            Stop-TestRuntime $daemon
            Get-Content -LiteralPath $runtimeErr
            throw "research loop failed"
        }

        $inspection = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        Assert-ResearchState $inspection
        $journalPath = Join-Path $runRoot (
            "sessions\" + $session.session_id + "\events.jsonl"
        )
        $journal = Get-Content -LiteralPath $journalPath
        if (@($journal | Select-String '"artifact.persistence_completed"').Count -ne 3 -or
            @($journal | Select-String 'research_fresh_context').Count -ne 3) {
            throw "research provenance events are incomplete"
        }
        if ($result.last_committed_sequence -ne @($journal).Count) {
            throw "research result does not match journal head"
        }

        Stop-TestRuntime $daemon
        $daemon = Start-TestRuntime
        $restarted = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        Assert-ResearchState $restarted
        $replayed = & $cli session replay $session.session_id --json |
            ConvertFrom-Json
        if ($replayed.command -ne "session_replay") {
            throw "research replay was not reported"
        }
        Assert-ResearchState $replayed

        Write-Output "runtime research-loop iteration/artifact/introspection/restart/replay E2E passed"
    }
    finally {
        Stop-TestRuntime $daemon
        $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
        $resolvedRun = (Resolve-Path $runRoot).Path
        if ($resolvedRun.StartsWith($resolvedTemp) -and
            (Split-Path $resolvedRun -Leaf).StartsWith(
                "agentmod-research-e2e-"
            )) {
            Remove-Item -LiteralPath $resolvedRun -Recurse -Force
        }
    }
}
finally {
    Pop-Location
}
$global:LASTEXITCODE = 0
