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
        "agentmod-planner-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-planner-e2e-" + [guid]::NewGuid().ToString("N")
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
        for ($attempt = 0; $attempt -lt 100; $attempt++) {
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

    function Assert-PlannerState($inspection) {
        if ($inspection.state.style_binding.id -ne "planner-worker" -or
            $inspection.state.style_binding.version -ne "1.1.0") {
            throw "planner style binding mismatch"
        }
        if ($inspection.state.lifecycle -ne "completed" -or
            $inspection.state.style_execution.termination_reason -ne
                "complete_session") {
            throw "planner graph did not complete"
        }
        if (@($inspection.state.planner_worker.tasks.PSObject.Properties).Count -ne 2) {
            throw "planner did not retain two runtime-owned tasks"
        }
        if (@($inspection.state.planner_worker.reviews).Count -ne 2 -or
            $inspection.state.planner_worker.reviews[0].approved -ne $false -or
            $inspection.state.planner_worker.reviews[1].approved -ne $true) {
            throw "reviewer did not reject once and then approve"
        }
        if (@($inspection.state.child_agents.PSObject.Properties).Count -ne 3) {
            throw "expected two initial workers and one revision worker"
        }
        if (@($inspection.state.planner_worker.joins).Count -ne 2) {
            throw "expected one exact join per planner iteration"
        }
    }

    $daemon = Start-TestRuntime
    try {
        $session = & $cli session create --workspace $repository `
            --style planner-worker@1.1.0 --json | ConvertFrom-Json
        $result = & $cli run "verify modular child orchestration" `
            --session $session.session_id `
            --option 'mock_scenario="planner_worker"' --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) {
            Stop-TestRuntime $daemon
            Get-Content -LiteralPath $runtimeErr
            throw "planner-worker turn failed"
        }

        $inspection = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        Assert-PlannerState $inspection
        $journalPath = Join-Path $runRoot (
            "sessions\" + $session.session_id + "\events.jsonl"
        )
        $journal = Get-Content -LiteralPath $journalPath
        if (@($journal | Select-String '"child_agent.created"').Count -ne 3 -or
            @($journal | Select-String '"child_agent.join_completed"').Count -ne 2 -or
            @($journal | Select-String '"style.reviewer_findings_committed"').Count -ne 2) {
            throw "planner canonical orchestration events are incomplete"
        }
        if ($result.last_committed_sequence -ne @($journal).Count) {
            throw "planner result does not match journal head"
        }

        Stop-TestRuntime $daemon
        $daemon = Start-TestRuntime
        $restarted = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        Assert-PlannerState $restarted
        Write-Output (
            "runtime planner-worker child/join/reject-once/restart E2E passed"
        )
    }
    finally {
        Stop-TestRuntime $daemon
        $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
        $resolvedRun = (Resolve-Path $runRoot).Path
        if ($resolvedRun.StartsWith($resolvedTemp) -and
            $resolvedRun -like "*agentmod-planner-e2e-*") {
            Remove-Item -LiteralPath $resolvedRun -Recurse -Force
        }
    }
}
finally {
    Pop-Location
}
