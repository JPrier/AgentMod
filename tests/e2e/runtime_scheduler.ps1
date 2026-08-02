$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$runRoot = $null
$daemon = $null
Push-Location $repository
try {
    cargo build --locked -p agentmod-runtime -p agentmod-harness `
        -p agentmod-scheduler -p agentmod-cli -p agentmod-tui
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $scheduler = (Resolve-Path "target\debug\agentmod-scheduler.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $tui = (Resolve-Path "target\debug\agentmod-tui.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-runtime-scheduler-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-runtime-scheduler-e2e-" +
        [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_SCHEDULER_PROGRAM = $scheduler
    $env:AGENTMOD_SCHEDULER_ROOT = Join-Path $runRoot "scheduler"

    function Start-Runtime {
        Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru
    }
    function Wait-Runtime {
        for ($attempt = 0; $attempt -lt 50; $attempt++) {
            try {
                & $cli doctor --json 2>$null | Out-Null
                if ($LASTEXITCODE -eq 0) { return }
            } catch {}
            Start-Sleep -Milliseconds 100
        }
        throw "runtime did not become ready"
    }

    $daemon = Start-Runtime
    Wait-Runtime
    $created = & $cli session create --workspace $workspace `
        --style persistent-chat --json | ConvertFrom-Json
    $stored = @(
        & $tui --smoke-command (
            "/schedule-once daily-driver 0 execute scheduled development work"
        ) 2>&1
    ) -join [Environment]::NewLine
    if ($LASTEXITCODE -ne 0 -or
        $stored -notmatch "schedule daily-driver stored") {
        throw "TUI schedule was not stored: $stored"
    }
    $listed = @(& $tui --smoke-command "/schedules" 2>&1) -join `
        [Environment]::NewLine
    if ($LASTEXITCODE -ne 0 -or $listed -notmatch "status=1 schedules") {
        throw "TUI runtime schedule listing failed: $listed"
    }
    $journalPath = Join-Path $runRoot (
        "sessions\" + $created.session_id + "\events.jsonl"
    )
    $automaticallyRan = $false
    for ($attempt = 0; $attempt -lt 50; $attempt++) {
        if (Test-Path -LiteralPath $journalPath) {
            try {
                $candidateEvents = @(Get-Content $journalPath | ForEach-Object {
                    ($_ | ConvertFrom-Json).event
                })
            }
            catch {
                Start-Sleep -Milliseconds 100
                continue
            }
            $firedCount = @($candidateEvents | Where-Object {
                $_.metadata.event_type -eq "scheduler.fired"
            }).Count
            $responseCount = @($candidateEvents | Where-Object {
                $_.metadata.event_type -eq "model.response_completed"
            }).Count
            if ($firedCount -eq 1 -and $responseCount -eq 1) {
                $automaticallyRan = $true
                break
            }
        }
        Start-Sleep -Milliseconds 100
    }
    if (-not $automaticallyRan) {
        throw "daemon poller did not execute due schedule"
    }
    $manualAfterPoll = & $cli schedule run --limit 4 --json |
        ConvertFrom-Json
    if (@($manualAfterPoll.runs).Count -ne 0) {
        throw "manual poll reclaimed an automatically executed occurrence"
    }
    $events = @(Get-Content $journalPath | ForEach-Object {
        ($_ | ConvertFrom-Json).event
    })
    if (@($events | Where-Object {
        $_.metadata.event_type -eq "scheduler.fired"
    }).Count -ne 1) {
        throw "scheduler.fired was not committed exactly once"
    }
    $fired = @($events | Where-Object {
        $_.metadata.event_type -eq "scheduler.fired"
    })[0]
    $executionId = $fired.payload.payload.execution_id
    if ($fired.payload.payload.schedule_id -ne "daily-driver") {
        throw "scheduler event provenance is invalid"
    }
    if (@($events | Where-Object {
        $_.metadata.event_type -eq "model.response_completed"
    }).Count -ne 1) {
        throw "scheduled work bypassed the normal provider path"
    }
    $terminalMarker = Join-Path $env:AGENTMOD_SCHEDULER_ROOT (
        "executions\" + $executionId + ".succeeded"
    )
    for ($attempt = 0; $attempt -lt 50 -and
        -not (Test-Path -LiteralPath $terminalMarker); $attempt++) {
        Start-Sleep -Milliseconds 100
    }
    if (-not (Test-Path -LiteralPath $terminalMarker)) {
        throw "scheduled occurrence was not durably completed"
    }

    $eventStored = & $cli schedule add "after-model-response" `
        --session $created.session_id `
        --prompt "review the committed model response" `
        --on-event "model.response_completed" --json | ConvertFrom-Json
    if ($eventStored.schedule_id -ne "after-model-response" -or
        $eventStored.replayed) {
        throw "runtime-event schedule was not stored"
    }
    & $cli run "emit one runtime event" --session $created.session_id `
        --json | Out-Null
    $eventDeliveryRan = $false
    for ($attempt = 0; $attempt -lt 50; $attempt++) {
        try {
            $eventDeliveryEvents = @(Get-Content $journalPath | ForEach-Object {
                ($_ | ConvertFrom-Json).event
            })
        }
        catch {
            Start-Sleep -Milliseconds 100
            continue
        }
        if (@($eventDeliveryEvents | Where-Object {
            $_.metadata.event_type -eq "scheduler.fired"
        }).Count -eq 2 -and
            @($eventDeliveryEvents | Where-Object {
                $_.metadata.event_type -eq "model.response_completed"
            }).Count -eq 3) {
            $eventDeliveryRan = $true
            break
        }
        Start-Sleep -Milliseconds 100
    }
    if (-not $eventDeliveryRan) {
        throw "committed runtime event did not execute its matching schedule"
    }
    if (@($eventDeliveryEvents | Where-Object {
        $_.metadata.event_type -eq "model.response_completed"
    }).Count -ne 3) {
        throw "runtime-event delivery bypassed or recursively repeated the provider path"
    }

    $deferredAt = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() + 5000
    $deferredStored = & $cli schedule add "restart-deferred-turn" `
        --session $created.session_id `
        --prompt "resume this durable turn after restart" `
        --at-ms $deferredAt --deferred --json | ConvertFrom-Json
    if ($deferredStored.schedule_id -ne "restart-deferred-turn" -or
        $deferredStored.replayed) {
        throw "deferred schedule was not stored"
    }
    $withDeferred = & $cli schedule list --json | ConvertFrom-Json
    $deferredSchedule = @($withDeferred.schedules | Where-Object {
        $_.schedule_id -eq "restart-deferred-turn"
    })[0]
    $deferredContinuation = $deferredSchedule.payload.value.continuation_id
    if ([string]::IsNullOrWhiteSpace($deferredContinuation)) {
        throw "deferred continuation identity was not inspectable"
    }
    & $cli approval resolve $created.session_id $deferredContinuation `
        approve --json 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) {
        throw "manual approval bypassed the scheduler wake condition"
    }

    Stop-Process -Id $daemon.Id -Force
    $daemon.WaitForExit()
    $daemon = Start-Runtime
    Wait-Runtime
    $deferredRan = $false
    for ($attempt = 0; $attempt -lt 100; $attempt++) {
        try {
            $eventsAfter = @(Get-Content $journalPath | ForEach-Object {
                ($_ | ConvertFrom-Json).event
            })
        }
        catch {
            Start-Sleep -Milliseconds 100
            continue
        }
        if (@($eventsAfter | Where-Object {
            $_.metadata.event_type -eq "scheduler.fired"
        }).Count -eq 3 -and
            @($eventsAfter | Where-Object {
                $_.metadata.event_type -eq "model.response_completed"
            }).Count -eq 4) {
            $deferredRan = $true
            break
        }
        Start-Sleep -Milliseconds 100
    }
    if (-not $deferredRan) {
        throw "deferred continuation did not wake after restart"
    }
    if (@($eventsAfter | Where-Object {
        $_.metadata.event_type -eq "scheduler.fired"
    }).Count -ne 3) {
        throw "time, event, or continuation delivery duplicated after restart"
    }
    if (@($eventsAfter | Where-Object {
        $_.metadata.event_type -eq "model.response_completed"
    }).Count -ne 4) {
        throw "deferred continuation bypassed the provider path"
    }
    $continuationPath = Join-Path $runRoot (
        "sessions\" + $created.session_id + "\continuations\" +
        $deferredContinuation + ".json"
    )
    $continuationRecord = Get-Content $continuationPath | ConvertFrom-Json
    if ($continuationRecord.state -ne "resumed") {
        throw "deferred continuation did not durably transition once"
    }
    $afterRestart = & $cli schedule run --limit 4 --json |
        ConvertFrom-Json
    if (@($afterRestart.runs).Count -ne 0) {
        throw "completed occurrence executed again after restart"
    }
    $removed = @(
        & $tui --smoke-command "/schedule-remove daily-driver" 2>&1
    ) -join [Environment]::NewLine
    if ($LASTEXITCODE -ne 0 -or
        $removed -notmatch "schedule daily-driver removed") {
        throw "TUI schedule removal failed: $removed"
    }
    Write-Output (
        "runtime-owned time, event, deferred continuation, and TUI " +
        "schedule management E2E passed"
    )
}
finally {
    if ($null -ne $daemon -and -not $daemon.HasExited) {
        Stop-Process -Id $daemon.Id -Force
        $daemon.WaitForExit()
    }
    foreach ($name in @(
        "AGENTMOD_RUNTIME_ENDPOINT",
        "AGENTMOD_RUNTIME_AUTH_TOKEN",
        "AGENTMOD_HARNESS_PROGRAM",
        "AGENTMOD_SCHEDULER_PROGRAM",
        "AGENTMOD_SCHEDULER_ROOT"
    )) {
        Remove-Item "Env:$name" -ErrorAction SilentlyContinue
    }
    if ($null -ne $runRoot -and (Test-Path -LiteralPath $runRoot)) {
        $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
        $resolvedRun = (Resolve-Path $runRoot).Path
        if ($resolvedRun.StartsWith($resolvedTemp) -and
            (Split-Path $resolvedRun -Leaf).StartsWith(
                "agentmod-runtime-scheduler-e2e-"
            )) {
            for ($attempt = 0; $attempt -lt 20 -and
                (Test-Path -LiteralPath $resolvedRun); $attempt++) {
                Remove-Item -LiteralPath $resolvedRun -Recurse -Force `
                    -ErrorAction SilentlyContinue
                if (Test-Path -LiteralPath $resolvedRun) {
                    Start-Sleep -Milliseconds 100
                }
            }
        }
    }
    Pop-Location
}
