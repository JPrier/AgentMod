$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$runRoot = $null
$daemon = $null
$runtimeLog = $null
$launchCounter = 0
$succeeded = $false
Push-Location $repository
try {
    cargo build --locked -p agentmod-runtime -p agentmod-harness `
        -p agentmod-scheduler -p agentmod-cli
    if ($LASTEXITCODE -ne 0) {
        throw "arbitrary schedule graph process fixture build failed"
    }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $scheduler = (Resolve-Path "target\debug\agentmod-scheduler.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-graph-schedule-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    $userStyles = Join-Path $runRoot "styles\user"
    New-Item -ItemType Directory -Path $workspace, $userStyles -Force |
        Out-Null

    $templatePath = Join-Path $repository (
        "tests\fixtures\styles\arbitrary-graph-schedule.toml"
    )
    $stylePath = Join-Path $userStyles "arbitrary-graph-schedule.toml"
    $wakeTimestamp = [DateTimeOffset]::UtcNow.AddSeconds(1).ToString(
        "yyyy-MM-ddTHH:mm:ss.fffZ",
        [Globalization.CultureInfo]::InvariantCulture
    )
    $styleText = [IO.File]::ReadAllText($templatePath).Replace(
        "2099-01-01T00:00:00Z",
        $wakeTimestamp
    )
    [IO.File]::WriteAllText(
        $stylePath,
        $styleText,
        [Text.UTF8Encoding]::new($false)
    )

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-graph-schedule-e2e-" +
        [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_SCHEDULER_PROGRAM = $scheduler
    $env:AGENTMOD_SCHEDULER_ROOT = Join-Path $runRoot "scheduler"
    $env:AGENTMOD_PERMISSION_MODE = "allow"
    $env:AGENTMOD_SCHEDULER_POLL_MS = "0"

    function Start-TestRuntime {
        $script:launchCounter++
        $script:runtimeLog = Join-Path $runRoot (
            "runtime-" + $script:launchCounter + ".stderr.log"
        )
        $process = Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru `
            -RedirectStandardError $script:runtimeLog
        for ($attempt = 0; $attempt -lt 100; $attempt++) {
            try {
                & $cli doctor --json 2>$null | Out-Null
                if ($LASTEXITCODE -eq 0) { return $process }
            }
            catch {}
            Start-Sleep -Milliseconds 100
        }
        if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force }
        $detail = if (Test-Path -LiteralPath $script:runtimeLog) {
            Get-Content -LiteralPath $script:runtimeLog -Raw
        } else {
            "runtime produced no diagnostic log"
        }
        throw "runtime did not become ready: $detail"
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
        $actual = @(
            $events | Where-Object { $_.metadata.event_type -eq $eventType }
        ).Count
        if ($actual -ne $expected) {
            throw "expected $expected $eventType events, found $actual"
        }
    }

    function Assert-ExactPlan($inspection) {
        $binding = $inspection.state.style_binding
        $plan = $binding.execution_plan
        if ($binding.id -ne "user-graph-schedule" -or
            $binding.version -ne "1.0.0" -or
            $plan.compilation.compiler -ne
                "agentmod-runtime-node-plan@3" -or
            [string]::IsNullOrWhiteSpace(
                [string]$binding.execution_plan_hash
            ) -or
            [string]::IsNullOrWhiteSpace([string]$plan.registry_hash) -or
            @($plan.nodes).Count -ne 2) {
            throw "arbitrary schedule graph immutable plan is incomplete"
        }
        $expected = @{
            "await-wake" = "runtime.schedule"
            "finish" = "runtime.session-completion"
        }
        foreach ($nodeId in $expected.Keys) {
            $resolution = @($plan.nodes | Where-Object node_id -eq $nodeId)
            if ($resolution.Count -ne 1 -or
                $resolution[0].executor_id -ne $expected[$nodeId] -or
                $resolution[0].executor_version -ne "1.0.0" -or
                $resolution[0].source.kind -ne "runtime" -or
                $resolution[0].boundary -ne "runtime_logic") {
                throw "invalid exact schedule graph resolution for $nodeId"
            }
        }
        return $binding
    }

    $daemon = Start-TestRuntime
    $validated = & $cli style validate $stylePath --json | ConvertFrom-Json
    if (-not $validated.valid) {
        throw (
            "arbitrary schedule graph did not validate: " +
            ($validated | ConvertTo-Json -Depth 20 -Compress)
        )
    }
    $style = & $cli style inspect user-graph-schedule@1.0.0 --json |
        ConvertFrom-Json
    if ($style.summary.availability -ne "available" -or
        $style.summary.source -ne "user") {
        throw "arbitrary schedule graph was not admitted as a user style"
    }

    $session = & $cli session create --workspace $workspace `
        --style user-graph-schedule@1.0.0 --json | ConvertFrom-Json
    $created = & $cli session inspect $session.session_id --json |
        ConvertFrom-Json
    $binding = Assert-ExactPlan $created
    $planHash = [string]$binding.execution_plan_hash
    $registryHash = [string]$binding.execution_plan.registry_hash
    $styleLockPath = Join-Path $runRoot (
        "sessions\" + $session.session_id + "\style.lock"
    )
    $styleLockBefore = [IO.File]::ReadAllBytes($styleLockPath)

    $waiting = & $cli run "execute exact one-time schedule graph" `
        --session $session.session_id `
        --provider deterministic-mock --model mock-model --json |
        ConvertFrom-Json
    if ([string]::IsNullOrWhiteSpace(
        [string]$waiting.awaiting_continuation
    )) {
        throw "schedule graph did not create its durable wait continuation"
    }
    $storedInspection = & $cli session inspect $session.session_id --json |
        ConvertFrom-Json
    $storedBinding = Assert-ExactPlan $storedInspection
    $contract = $storedInspection.state.style_execution.execution_contract
    if ($contract.execution_plan_hash -ne $planHash -or
        $contract.registry_hash -ne $registryHash -or
        @($contract.node_executors).Count -ne 2) {
        throw "schedule graph execution contract diverged from its plan"
    }
    $records = @(
        $storedInspection.state.style_execution.graph_schedules.
            PSObject.Properties | ForEach-Object { $_.Value }
    )
    if ($records.Count -ne 1 -or
        $records[0].state -ne "stored" -or
        -not $records[0].wait_for_trigger -or
        $records[0].identity.continuation_id -ne
            $waiting.awaiting_continuation) {
        throw "schedule graph did not retain one exact stored wait"
    }

    $storedEvents = Read-Journal $session.session_id
    foreach ($expectation in @(
        @("style.execution_initialized", 1),
        @("graph.schedule_resolved", 1),
        @("graph.schedule_approved", 1),
        @("graph.schedule_dispatched", 1),
        @("graph.schedule_stored", 1),
        @("scheduler.fired", 0),
        @("graph.node_wait_resolved", 0)
    )) {
        Assert-EventCount $storedEvents $expectation[0] $expectation[1]
    }
    $approved = @($storedEvents | Where-Object {
        $_.metadata.event_type -eq "graph.schedule_approved"
    })[0].payload.payload
    $dispatched = @($storedEvents | Where-Object {
        $_.metadata.event_type -eq "graph.schedule_dispatched"
    })[0].payload.payload
    $stored = @($storedEvents | Where-Object {
        $_.metadata.event_type -eq "graph.schedule_stored"
    })[0].payload.payload
    $scheduleResolved = @($storedEvents | Where-Object {
        $_.metadata.event_type -eq "graph.schedule_resolved"
    })[0].payload.payload
    $schedulePlanNode = @(
        $storedBinding.execution_plan.nodes |
            Where-Object node_id -eq "await-wake"
    )[0]
    if ($scheduleResolved.identity.work.node_id -ne "await-wake" -or
        $scheduleResolved.identity.execution_plan_hash -ne $planHash -or
        $scheduleResolved.identity.configuration_hash -ne
            $schedulePlanNode.adapter_configuration_reference -or
        $scheduleResolved.trigger.kind -ne "at_millis" -or
        -not $scheduleResolved.wait_for_trigger -or
        -not $scheduleResolved.consequential -or
        $scheduleResolved.cancellation -ne "cancel_trigger" -or
        $scheduleResolved.identity.schedule_id -ne
            $approved.identity.schedule_id -or
        $scheduleResolved.identity.schedule_id -ne
            $dispatched.identity.schedule_id -or
        $scheduleResolved.identity.schedule_id -ne
            $stored.identity.schedule_id -or
        $scheduleResolved.identity.idempotency_id -ne
            $approved.identity.idempotency_id -or
        $scheduleResolved.identity.continuation_id -ne
            $waiting.awaiting_continuation) {
        throw "schedule resolution/outbox did not retain its exact plan identity"
    }
    if ([string]::IsNullOrWhiteSpace([string]$approved.action_digest) -or
        $approved.action_digest -ne $dispatched.action_digest -or
        $approved.action_digest -ne $stored.action_digest -or
        $stored.replayed) {
        throw "schedule policy/outbox/receipt identity is not exact"
    }

    Stop-TestRuntime $daemon
    $daemon = $null
    $env:AGENTMOD_SCHEDULER_POLL_MS = "25"
    $daemon = Start-TestRuntime
    $completed = $null
    for ($attempt = 0; $attempt -lt 300; $attempt++) {
        $completed = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        if ($completed.state.lifecycle -eq "completed") { break }
        Start-Sleep -Milliseconds 50
    }
    if ($completed.state.lifecycle -ne "completed" -or
        $completed.state.style_execution.termination_reason -ne
            "complete_session") {
        throw "schedule graph did not resume after daemon replacement"
    }
    $completedBinding = Assert-ExactPlan $completed
    if ($completedBinding.execution_plan_hash -ne $planHash -or
        $completedBinding.execution_plan.registry_hash -ne $registryHash) {
        throw "schedule graph was rebound during restart"
    }
    $styleLockAfter = [IO.File]::ReadAllBytes($styleLockPath)
    if ([Convert]::ToBase64String($styleLockBefore) -ne
        [Convert]::ToBase64String($styleLockAfter)) {
        throw "schedule graph style lock changed across restart"
    }

    $events = Read-Journal $session.session_id
    foreach ($expectation in @(
        @("style.execution_initialized", 1),
        @("graph.schedule_resolved", 1),
        @("graph.schedule_approved", 1),
        @("graph.schedule_dispatched", 1),
        @("graph.schedule_stored", 1),
        @("scheduler.fired", 1),
        @("graph.node_wait_resolved", 1)
    )) {
        Assert-EventCount $events $expectation[0] $expectation[1]
    }
    $resolved = @($events | Where-Object {
        $_.metadata.event_type -eq "graph.node_wait_resolved"
    })[0].payload.payload
    if ($resolved.disposition -ne "resumed") {
        throw "schedule continuation did not resolve as resumed"
    }
    foreach ($nodeId in @("await-wake", "finish")) {
        if (@(
            $completed.state.style_execution.completed_nodes |
                Where-Object node_id -eq $nodeId
        ).Count -ne 1) {
            throw "schedule graph did not complete $nodeId exactly once"
        }
    }
    $fired = @($events | Where-Object {
        $_.metadata.event_type -eq "scheduler.fired"
    })[0].payload.payload
    $executionId = [string]$fired.execution_id
    if ([string]::IsNullOrWhiteSpace($executionId)) {
        throw "schedule graph firing omitted its durable execution identity"
    }
    $terminalMarker = Join-Path $env:AGENTMOD_SCHEDULER_ROOT (
        "executions\" + $executionId + ".succeeded"
    )
    if (-not (Test-Path -LiteralPath $terminalMarker)) {
        throw "schedule graph occurrence has no durable terminal marker"
    }

    $beforeReplayCount = $events.Count
    Stop-TestRuntime $daemon
    $daemon = Start-TestRuntime
    Start-Sleep -Milliseconds 250
    $pending = & $cli schedule claim --limit 4 --json | ConvertFrom-Json
    if (@($pending.executions).Count -ne 0) {
        throw "completed schedule graph left a duplicate due occurrence"
    }
    $replayed = & $cli session replay $session.session_id --json |
        ConvertFrom-Json
    $replayedBinding = Assert-ExactPlan $replayed
    if ($replayed.state.lifecycle -ne "completed" -or
        $replayedBinding.execution_plan_hash -ne $planHash -or
        (Read-Journal $session.session_id).Count -ne $beforeReplayCount) {
        throw "schedule graph replay/restart duplicated canonical work"
    }

    Write-Output (
        "runtime arbitrary schedule graph exact-plan/policy/outbox/" +
        "restart/wake-once/replay E2E passed"
    )
    $succeeded = $true
}
finally {
    Stop-TestRuntime $daemon
    foreach ($name in @(
        "AGENTMOD_RUNTIME_ENDPOINT",
        "AGENTMOD_RUNTIME_AUTH_TOKEN",
        "AGENTMOD_HARNESS_PROGRAM",
        "AGENTMOD_SCHEDULER_PROGRAM",
        "AGENTMOD_SCHEDULER_ROOT",
        "AGENTMOD_PERMISSION_MODE",
        "AGENTMOD_SCHEDULER_POLL_MS"
    )) {
        Remove-Item "Env:$name" -ErrorAction SilentlyContinue
    }
    Pop-Location
    if ($null -ne $runRoot -and (Test-Path -LiteralPath $runRoot)) {
        $resolvedRun = [IO.Path]::GetFullPath($runRoot)
        $resolvedTemp = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
        if (-not $resolvedRun.StartsWith($resolvedTemp) -or
            -not (Split-Path $resolvedRun -Leaf).StartsWith(
                "agentmod-graph-schedule-e2e-"
            )) {
            throw "refusing to remove unexpected schedule graph E2E root"
        }
        if ($succeeded) {
            Remove-Item -LiteralPath $resolvedRun -Recurse -Force
        } else {
            Write-Output "retained failed schedule graph E2E root: $resolvedRun"
        }
    }
}
