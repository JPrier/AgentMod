$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build --locked -p agentmod-runtime -p agentmod-harness `
        -p agentmod-filesystem-host -p agentmod-scheduler -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $filesystem = (
        Resolve-Path "target\debug\agentmod-filesystem-host.exe"
    ).Path
    $scheduler = (
        Resolve-Path "target\debug\agentmod-scheduler.exe"
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
    $env:AGENTMOD_SCHEDULER_PROGRAM = $scheduler
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

    function Read-Journal($sessionId) {
        $journal = Join-Path $runRoot (
            "sessions\" + $sessionId + "\events.jsonl"
        )
        if (-not (Test-Path -LiteralPath $journal)) { return @() }
        @(
            Get-Content -LiteralPath $journal |
                ForEach-Object { ($_ | ConvertFrom-Json).event }
        )
    }

    function Assert-EventCount($sessionId, $eventType, $expected) {
        $actual = @(
            Read-Journal $sessionId |
                Where-Object { $_.metadata.event_type -eq $eventType }
        ).Count
        if ($actual -ne $expected) {
            throw "expected $expected $eventType events, found $actual"
        }
    }

    function Assert-GenericPlan($inspection) {
        $plan = $inspection.state.style_binding.execution_plan
        if ($plan.compilation.compiler -ne "agentmod-runtime-node-plan@3" -or
            @($plan.nodes).Count -ne 5 -or
            [string]::IsNullOrWhiteSpace([string]$plan.registry_hash) -or
            [string]::IsNullOrWhiteSpace(
                [string]$inspection.state.style_binding.execution_plan_hash
            )) {
            throw "declarative v1.2 did not retain a generation-3 exact plan"
        }
        $expected = @{
            "branch" = "runtime.conditional"
            "approval" = "runtime.user-approval"
            "tool" = "runtime.tool-gate"
            "repeat" = "runtime.loop"
            "done" = "runtime.session-completion"
        }
        foreach ($nodeId in $expected.Keys) {
            $resolution = @(
                $plan.nodes | Where-Object node_id -eq $nodeId
            )
            if ($resolution.Count -ne 1 -or
                $resolution[0].executor_id -ne $expected[$nodeId] -or
                $resolution[0].source.kind -ne "runtime" -or
                $resolution[0].boundary -ne "runtime_logic" -or
                [string]::IsNullOrWhiteSpace(
                    $resolution[0].executor_version
                )) {
                throw "invalid exact executor resolution for $nodeId"
            }
        }
    }

    function Assert-NoLegacyEvidence($inspection, $sessionId) {
        $serialized = $inspection | ConvertTo-Json -Depth 100 -Compress
        [string]$journal = (Read-Journal $sessionId) |
            ConvertTo-Json -Depth 100 -Compress
        foreach ($legacy in @(
            "declarative-request:approval:",
            "declarative-approval:",
            "declarative-tool:"
        )) {
            if ($serialized.Contains($legacy) -or $journal.Contains($legacy)) {
                throw "legacy declarative evidence survived generic execution: $legacy"
            }
        }
    }

    function Assert-CompletedGraph(
        $inspection,
        $sessionId,
        [bool]$requiresApproval
    ) {
        if ($inspection.state.style_binding.id -ne "declarative-graph" -or
            $inspection.state.style_binding.version -ne "1.2.0") {
            throw "declarative style binding mismatch"
        }
        Assert-GenericPlan $inspection
        if ($inspection.state.lifecycle -ne "completed" -or
            $inspection.state.style_execution.termination_reason -ne
                "complete_session") {
            throw "declarative graph did not complete"
        }
        $tools = @(
            $inspection.state.tool_executions.PSObject.Properties |
                ForEach-Object { $_.Value }
        )
        if ($tools.Count -ne 3 -or @(
                $tools | Where-Object {
                    $_.state -ne "terminal" -or
                    -not $_.call_id.StartsWith("graph:tool:")
                }
            ).Count -ne 0) {
            throw "declarative graph did not retain three exact generic tool effects"
        }
        $completed = @($inspection.state.style_execution.completed_nodes)
        foreach ($expectation in @(
            @("branch", 1),
            @("tool", 3),
            @("repeat", 3),
            @("done", 1)
        )) {
            if (@($completed |
                    Where-Object node_id -eq $expectation[0]).Count -ne
                $expectation[1]) {
                throw "unexpected completion count for $($expectation[0])"
            }
        }
        $approvalNodes = @($completed | Where-Object node_id -eq "approval")
        if ($approvalNodes.Count -ne [int]$requiresApproval -or
            ($requiresApproval -and
                -not $approvalNodes[0].result_reference.StartsWith(
                    "generic-approval:"
                ))) {
            throw "generic approval evidence did not match the selected branch"
        }
        $toolNodes = @($completed | Where-Object node_id -eq "tool")
        if (@($toolNodes | Where-Object {
                    -not $_.result_reference.StartsWith("tool:graph:tool:")
                }).Count -ne 0) {
            throw "tool nodes did not retain generic result references"
        }

        $entries = $inspection.state.style_execution.canonical_variables.
            environment.entries
        if ($entries.request.version -ne 1 -or
            $entries.request.value.kind -ne "map" -or
            $entries.request.value.value.requires_approval.value -ne
                $requiresApproval -or
            $entries.tool_arguments.version -ne 1 -or
            $entries.tool_arguments.value.kind -ne "map" -or
            $entries.tool_arguments.value.value.path.value -ne "README.md" -or
            $entries.iteration.version -ne 3 -or
            $entries.iteration.value.kind -ne "map" -or
            $entries.iteration.value.value.remaining.value -ne $false) {
            throw "canonical declarative variables were not reconstructed exactly"
        }
        Assert-EventCount $sessionId "tool.execution_dispatched" 3
        Assert-EventCount $sessionId "tool.execution_completed" 3
        Assert-NoLegacyEvidence $inspection $sessionId
    }

    $daemon = Start-TestRuntime
    try {
        $direct = & $cli session create --workspace $repository `
            --style declarative-graph@1.2.0 --json | ConvertFrom-Json
        $directCreated = & $cli session inspect $direct.session_id --json |
            ConvertFrom-Json
        Assert-GenericPlan $directCreated
        & $cli run "read the repository graph fixture" `
            --session $direct.session_id `
            --option 'request={"requires_approval":false}' `
            --option 'tool_arguments={"path":"README.md"}' `
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
        Assert-CompletedGraph $directInspection $direct.session_id $false

        $approval = & $cli session create --workspace $repository `
            --style declarative-graph@1.2.0 --json | ConvertFrom-Json
        $approvalCreated = & $cli session inspect $approval.session_id --json |
            ConvertFrom-Json
        Assert-GenericPlan $approvalCreated
        $waiting = & $cli run "read after an explicit graph approval" `
            --session $approval.session_id `
            --option 'request={"requires_approval":true}' `
            --option 'tool_arguments={"path":"README.md"}' `
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
        Assert-EventCount $approval.session_id "tool.execution_dispatched" 0
        Assert-NoLegacyEvidence $waitingInspection $approval.session_id

        Stop-TestRuntime $daemon
        $daemon = Start-TestRuntime
        $resolved = & $cli approval resolve $approval.session_id `
            $continuation approve --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or $resolved.command -ne "approval_resolve") {
            throw "declarative approval did not resume after restart"
        }
        $approvedInspection = & $cli session inspect $approval.session_id --json |
            ConvertFrom-Json
        Assert-CompletedGraph $approvedInspection $approval.session_id $true

        $beforeDuplicate = @(Read-Journal $approval.session_id).Count
        $duplicate = & $cli approval resolve $approval.session_id `
            $continuation approve --json | ConvertFrom-Json
        if ($duplicate.transitioned -or
            @(Read-Journal $approval.session_id).Count -ne $beforeDuplicate) {
            throw "duplicate graph approval transitioned twice"
        }
        $afterDuplicate = & $cli session inspect $approval.session_id --json |
            ConvertFrom-Json
        Assert-CompletedGraph $afterDuplicate $approval.session_id $true

        $beforeReplay = @(Read-Journal $approval.session_id).Count
        $replayed = & $cli session replay $approval.session_id --json |
            ConvertFrom-Json
        if ($replayed.command -ne "session_replay" -or
            $replayed.state.style_binding.execution_plan_hash -ne
                $approvalCreated.state.style_binding.execution_plan_hash -or
            @(Read-Journal $approval.session_id).Count -ne $beforeReplay) {
            throw "declarative replay was not reported"
        }
        Assert-CompletedGraph $replayed $approval.session_id $true

        Write-Output (
            "runtime declarative v1.2 exact-plan/generic-dispatch/branch/loop/" +
            "three-tool/approval/restart/replay E2E passed"
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
