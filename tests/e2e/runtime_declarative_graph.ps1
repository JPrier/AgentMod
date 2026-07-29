$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-filesystem-host -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $filesystem = (
        Resolve-Path "target\debug\agentmod-filesystem-host.exe"
    ).Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-declarative-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-declarative-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_FILESYSTEM_HOST_PROGRAM = $filesystem
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

    function Assert-CompletedGraph($inspection, $toolCount) {
        if ($inspection.state.style_binding.id -ne "declarative-graph" -or
            $inspection.state.style_binding.version -ne "1.1.0") {
            throw "declarative style binding mismatch"
        }
        if ($inspection.state.lifecycle -ne "completed" -or
            $inspection.state.style_execution.termination_reason -ne
                "complete_session") {
            throw "declarative graph did not complete"
        }
        if (@($inspection.state.tool_executions.PSObject.Properties).Count -ne
            $toolCount) {
            throw "unexpected declarative tool execution count"
        }
        foreach ($node in @("branch", "tool", "repeat", "done")) {
            if (-not @($inspection.state.style_execution.completed_nodes |
                    Where-Object node_id -eq $node)) {
                throw "missing completed graph node $node"
            }
        }
    }

    $daemon = Start-TestRuntime
    try {
        $direct = & $cli session create --workspace $repository `
            --style declarative-graph@1.1.0 --json | ConvertFrom-Json
        & $cli run "read the repository graph fixture" `
            --session $direct.session_id `
            --option 'graph_requires_approval=false' `
            --option 'graph_iterations=3' `
            --json | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Stop-TestRuntime $daemon
            Get-Content -LiteralPath $runtimeErr
            $failedJournal = Join-Path $runRoot (
                "sessions\" + $direct.session_id + "\events.jsonl"
            )
            if (Test-Path -LiteralPath $failedJournal) {
                Get-Content -LiteralPath $failedJournal
            }
            throw "direct declarative graph failed"
        }
        $directInspection = & $cli session inspect $direct.session_id --json |
            ConvertFrom-Json
        Assert-CompletedGraph $directInspection 3

        $approval = & $cli session create --workspace $repository `
            --style declarative-graph@1.1.0 --json | ConvertFrom-Json
        $waiting = & $cli run "read after an explicit graph approval" `
            --session $approval.session_id `
            --option 'graph_requires_approval=true' `
            --option 'graph_iterations=1' `
            --json | ConvertFrom-Json
        $continuation = $waiting.awaiting_continuation
        if ([string]::IsNullOrWhiteSpace($continuation)) {
            throw "declarative approval continuation was not returned"
        }
        $waitingInspection = & $cli session inspect $approval.session_id --json |
            ConvertFrom-Json
        if ($waitingInspection.state.style_execution.active_node.node_id -ne
            "approval") {
            throw "graph was not waiting at the approval node"
        }

        Stop-TestRuntime $daemon
        $daemon = Start-TestRuntime
        $resolved = & $cli approval resolve $approval.session_id `
            $continuation approve --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or $resolved.command -ne "approval_resolve") {
            throw "declarative approval did not resume after restart"
        }
        $approvedInspection = & $cli session inspect $approval.session_id --json |
            ConvertFrom-Json
        Assert-CompletedGraph $approvedInspection 1
        if (-not @($approvedInspection.state.style_execution.completed_nodes |
                Where-Object node_id -eq "approval")) {
            throw "approval node was not retained in graph history"
        }

        $duplicate = & $cli approval resolve $approval.session_id `
            $continuation approve --json | ConvertFrom-Json
        if ($duplicate.transitioned) {
            throw "duplicate graph approval transitioned twice"
        }
        $afterDuplicate = & $cli session inspect $approval.session_id --json |
            ConvertFrom-Json
        Assert-CompletedGraph $afterDuplicate 1

        $replayed = & $cli session replay $approval.session_id --json |
            ConvertFrom-Json
        if ($replayed.command -ne "session_replay") {
            throw "declarative replay was not reported"
        }
        Assert-CompletedGraph $replayed 1

        Write-Output (
            "runtime declarative branch/loop/tool/approval/restart/replay E2E passed"
        )
    }
    finally {
        Stop-TestRuntime $daemon
        $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
        $resolvedRun = (Resolve-Path $runRoot).Path
        if ($resolvedRun.StartsWith($resolvedTemp) -and
            (Split-Path $resolvedRun -Leaf).StartsWith(
                "agentmod-declarative-e2e-"
            )) {
            Remove-Item -LiteralPath $resolvedRun -Recurse -Force
        }
    }
}
finally {
    Pop-Location
}
$global:LASTEXITCODE = 0
