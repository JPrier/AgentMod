$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build --locked -p agentmod-runtime -p agentmod-harness `
        -p agentmod-filesystem-host -p agentmod-scheduler -p agentmod-cli `
        -p agentmod-tui
    if ($LASTEXITCODE -ne 0) { throw "Graph A process fixture build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $filesystem = (
        Resolve-Path "target\debug\agentmod-filesystem-host.exe"
    ).Path
    $scheduler = (Resolve-Path "target\debug\agentmod-scheduler.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $tui = (Resolve-Path "target\debug\agentmod-tui.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-graph-a-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    $userStyles = Join-Path $runRoot "styles\user"
    New-Item -ItemType Directory -Path $workspace, $userStyles -Force |
        Out-Null
    Copy-Item -LiteralPath (
        Join-Path $repository "tests\fixtures\styles\arbitrary-graph-a.toml"
    ) -Destination $userStyles
    Copy-Item -LiteralPath (Join-Path $repository "README.md") `
        -Destination $workspace

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-graph-a-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_FILESYSTEM_HOST_PROGRAM = $filesystem
    $env:AGENTMOD_SCHEDULER_PROGRAM = $scheduler
    $env:AGENTMOD_PERMISSION_MODE = "allow"
    $env:AGENTMOD_SCHEDULER_POLL_MS = "0"
    $daemon = $null
    $succeeded = $false
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

    function Read-Journal($sessionId) {
        $path = Join-Path $runRoot (
            "sessions\" + $sessionId + "\events.jsonl"
        )
        return @(
            Get-Content -LiteralPath $path |
                ForEach-Object { ($_ | ConvertFrom-Json).event }
        )
    }

    function Assert-EventCount($events, $eventType, $expected) {
        $count = @(
            $events | Where-Object { $_.metadata.event_type -eq $eventType }
        ).Count
        if ($count -ne $expected) {
            throw "expected $expected $eventType events, found $count"
        }
    }

    function Assert-ExactExecutorSet($nodes, $label) {
        $expected = @{
            "context" = "runtime.context-construction"
            "model" = "runtime.model-request"
            "route" = "runtime.conditional"
            "fanout" = "runtime.parallel"
            "approve" = "runtime.user-approval"
            "read-tool" = "runtime.tool-gate"
            "save-artifact" = "runtime.artifact-persistence"
            "join" = "runtime.join"
            "bounded-loop" = "runtime.loop"
            "loop-check" = "runtime.conditional"
            "emit-progress" = "runtime.event-emission"
            "durable-delay" = "runtime.delay"
            "finish" = "runtime.session-completion"
            "invalid-input" = "runtime.structured-failure"
        }
        if (@($nodes).Count -ne $expected.Count) {
            throw "$label did not retain all exact Graph A node executors"
        }
        foreach ($nodeId in $expected.Keys) {
            $resolution = @($nodes | Where-Object node_id -eq $nodeId)
            $expectedVersion = if ($nodeId -eq "read-tool" -or
                $nodeId -eq "model") {
                "1.1.0"
            } else {
                "1.0.0"
            }
            if ($resolution.Count -ne 1 -or
                $resolution[0].executor_id -ne $expected[$nodeId] -or
                $resolution[0].executor_version -ne $expectedVersion -or
                $resolution[0].source.kind -ne "runtime" -or
                $resolution[0].boundary -ne "runtime_logic") {
                throw "$label has an invalid exact resolution for $nodeId"
            }
        }
    }

    function Assert-ExactBindingPlan($inspection) {
        $binding = $inspection.state.style_binding
        $plan = $binding.execution_plan
        if ($plan.compilation.compiler -ne
                "agentmod-runtime-node-plan@3" -or
            [string]::IsNullOrWhiteSpace(
                [string]$binding.execution_plan_hash
            ) -or [string]::IsNullOrWhiteSpace(
                [string]$plan.registry_hash
            )) {
            throw "Graph A generation-3 immutable execution plan was not persisted"
        }
        Assert-ExactExecutorSet $plan.nodes "Graph A binding"
    }

    function Assert-ExactExecutionContract($inspection) {
        $binding = $inspection.state.style_binding
        $contract = $inspection.state.style_execution.execution_contract
        if ($null -eq $contract -or
            $contract.execution_plan_hash -ne $binding.execution_plan_hash -or
            $contract.registry_hash -ne
                $binding.execution_plan.registry_hash) {
            throw "Graph A execution contract diverged from its immutable binding"
        }
        Assert-ExactExecutorSet $contract.node_executors "Graph A contract"
    }

    $daemon = Start-TestRuntime
    try {
        $validated = & $cli style validate (
            Join-Path $userStyles "arbitrary-graph-a.toml"
        ) --json | ConvertFrom-Json
        if (-not $validated.valid) {
            throw "Graph A manifest did not validate: $($validated | ConvertTo-Json -Depth 20)"
        }
        $style = & $cli style inspect user-graph-a@1.0.0 --json |
            ConvertFrom-Json
        if ($style.summary.availability -ne "available" -or
            $style.summary.source -ne "user") {
            throw "Graph A was not admitted as an available user style"
        }

        $session = & $cli session create --workspace $workspace `
            --style user-graph-a@1.0.0 --json | ConvertFrom-Json
        $created = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        Assert-ExactBindingPlan $created

        $waiting = & $cli run "execute arbitrary Graph A" `
            --session $session.session_id `
            --provider deterministic-mock --model mock-model `
            --option 'ready=true' `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="graph-a-model-complete"' --json |
            ConvertFrom-Json
        $approvalContinuation = $waiting.awaiting_continuation
        if ([string]::IsNullOrWhiteSpace($approvalContinuation)) {
            throw "Graph A did not durably wait for user approval"
        }
        $beforeApprovalRestart = Read-Journal $session.session_id
        Assert-EventCount $beforeApprovalRestart "style.execution_initialized" 1
        Assert-EventCount $beforeApprovalRestart "approval.requested" 1
        Assert-EventCount $beforeApprovalRestart `
            "artifact.persistence_completed" 1

        Stop-TestRuntime $daemon
        $daemon = Start-TestRuntime
        $pending = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        if ($pending.state.lifecycle -ne "active") {
            throw "Graph A approval wait did not survive runtime restart"
        }
        Assert-ExactBindingPlan $pending
        Assert-ExactExecutionContract $pending
        $resolved = & $cli approval resolve $session.session_id `
            $approvalContinuation approve --json | ConvertFrom-Json
        $delayContinuation = $resolved.awaiting_continuation
        if ([string]::IsNullOrWhiteSpace($delayContinuation)) {
            throw "Graph A did not reach its durable delay after approval"
        }
        $delayInspection = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        $scheduleRecords = @(
            $delayInspection.state.style_execution.graph_schedules.
                PSObject.Properties | ForEach-Object { $_.Value }
        )
        if ($scheduleRecords.Count -ne 1 -or
            $scheduleRecords[0].state -ne "stored") {
            throw "Graph A delay was not canonically stored before restart"
        }
        $variableEntries = $delayInspection.state.style_execution.
            canonical_variables.environment.entries
        foreach ($name in @(
            "ready",
            "approval_result",
            "tool_result",
            "artifact_result",
            "iteration"
        )) {
            if ($null -eq $variableEntries.$name) {
                throw "Graph A did not reconstruct canonical variable $name"
            }
        }
        $beforeDuplicateApproval = (Read-Journal $session.session_id).Count
        $duplicateOutput = @(
            & $cli approval resolve $session.session_id `
                $approvalContinuation approve --json 2>&1
        )
        $duplicateExit = $LASTEXITCODE
        if ($duplicateExit -ne 0) {
            throw "duplicate Graph A approval was not accepted idempotently"
        }
        $duplicate = ($duplicateOutput -join [Environment]::NewLine) |
            ConvertFrom-Json
        if ($duplicate.transitioned) {
            throw "duplicate Graph A approval transitioned twice"
        }
        if ((Read-Journal $session.session_id).Count -ne
            $beforeDuplicateApproval) {
            throw "duplicate Graph A approval appended effects"
        }
        $afterDuplicate = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        if ($afterDuplicate.state.lifecycle -ne "active" -or
            @(
                $afterDuplicate.state.style_execution.graph_schedules.
                    PSObject.Properties | ForEach-Object { $_.Value }
            )[0].state -ne "stored") {
            throw "duplicate Graph A approval disturbed its durable wait"
        }

        Stop-TestRuntime $daemon
        $env:AGENTMOD_SCHEDULER_POLL_MS = "25"
        $daemon = Start-TestRuntime
        $completed = $null
        for ($attempt = 0; $attempt -lt 160; $attempt++) {
            $completed = & $cli session inspect $session.session_id --json |
                ConvertFrom-Json
            if ($completed.state.lifecycle -eq "completed") { break }
            Start-Sleep -Milliseconds 50
        }
        if ($completed.state.lifecycle -ne "completed" -or
            $completed.state.style_execution.termination_reason -ne
                "complete_session") {
            throw "Graph A delay did not resume to terminal completion"
        }
        foreach ($name in @("resolved_at", "resolved_duration", "delay_result")) {
            if ($null -eq $completed.state.style_execution.
                canonical_variables.environment.entries.$name) {
                throw "Graph A did not retain delay variable $name"
            }
        }
        if ($completed.state.style_execution.canonical_variables.environment.
            entries.iteration.version -ne 2) {
            throw "Graph A bounded loop did not commit exactly two variable versions"
        }
        $resources = @(
            & $tui --smoke-session-command $session.session_id "/resources" 2>&1
        ) -join [Environment]::NewLine
        if ($LASTEXITCODE -ne 0 -or
            $resources -notmatch "selected=$($session.session_id)" -or
            $resources -notmatch "resources=1/0/0") {
            throw "TUI canonical artifact resource projection failed: $resources"
        }

        $events = Read-Journal $session.session_id
        foreach ($eventExpectation in @(
            @("style.execution_initialized", 1),
            @("approval.requested", 1),
            @("approval.resolved", 1),
            @("tool.execution_dispatched", 1),
            @("tool.execution_completed", 1),
            @("artifact.persistence_dispatched", 1),
            @("artifact.persistence_completed", 1),
            @("graph.parallel_initialized", 1),
            @("graph.schedule_dispatched", 1),
            @("graph.schedule_stored", 1),
            @("scheduler.fired", 1),
            @("graph.node_wait_resolved", 1),
            @("graph.user_space_event_emitted", 1)
        )) {
            Assert-EventCount $events $eventExpectation[0] $eventExpectation[1]
        }
        foreach ($node in @(
            "context",
            "model",
            "route",
            "fanout",
            "join",
            "bounded-loop",
            "loop-check",
            "emit-progress",
            "durable-delay",
            "finish"
        )) {
            if (-not @(
                $completed.state.style_execution.completed_nodes |
                    Where-Object node_id -eq $node
            )) {
                throw "Graph A missing completed root node $node"
            }
        }

        $beforeReplayCount = $events.Count
        $replayed = & $cli session replay $session.session_id --json |
            ConvertFrom-Json
        if ($replayed.state.lifecycle -ne "completed" -or
            $replayed.state.style_binding.execution_plan_hash -ne
                $created.state.style_binding.execution_plan_hash -or
            $replayed.state.style_execution.canonical_variables.environment.
                entries.resolved_duration.value.value -ne 750) {
            throw "Graph A pure replay did not reconstruct its exact contract and variables"
        }
        Assert-ExactBindingPlan $replayed
        Assert-ExactExecutionContract $replayed
        if ((Read-Journal $session.session_id).Count -ne $beforeReplayCount) {
            throw "pure Graph A replay appended journal events"
        }

        Write-Output (
            "runtime arbitrary Graph A plan/approval/parallel/tool/artifact/" +
            "loop/event/delay/join/restart/replay E2E passed"
        )
        $succeeded = $true
    }
    catch {
        if (Test-Path -LiteralPath $runtimeErr) {
            Get-Content -LiteralPath $runtimeErr
        }
        Get-ChildItem -LiteralPath (Join-Path $runRoot "sessions") `
            -Filter events.jsonl -Recurse -ErrorAction SilentlyContinue |
            ForEach-Object {
                Write-Output "journal: $($_.FullName)"
                Get-Content -LiteralPath $_.FullName
            }
        throw
    }
    finally {
        Stop-TestRuntime $daemon
        foreach ($name in @(
            "AGENTMOD_RUNTIME_ENDPOINT",
            "AGENTMOD_RUNTIME_AUTH_TOKEN",
            "AGENTMOD_HARNESS_PROGRAM",
            "AGENTMOD_FILESYSTEM_HOST_PROGRAM",
            "AGENTMOD_SCHEDULER_PROGRAM",
            "AGENTMOD_PERMISSION_MODE",
            "AGENTMOD_SCHEDULER_POLL_MS"
        )) {
            Remove-Item "Env:\$name" -ErrorAction SilentlyContinue
        }
        if ($succeeded -and (Test-Path -LiteralPath $runRoot)) {
            $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
            $resolvedRun = (Resolve-Path $runRoot).Path
            if ($resolvedRun.StartsWith($resolvedTemp) -and
                (Split-Path $resolvedRun -Leaf).StartsWith(
                    "agentmod-graph-a-e2e-"
                )) {
                Remove-Item -LiteralPath $resolvedRun -Recurse -Force
            }
        } elseif (Test-Path -LiteralPath $runRoot) {
            Write-Output "retained failed Graph A E2E root: $runRoot"
        }
    }
}
finally {
    Pop-Location
}
$global:LASTEXITCODE = 0
