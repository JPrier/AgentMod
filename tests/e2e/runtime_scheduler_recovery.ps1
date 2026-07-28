$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$runRoot = $null
$daemon = $null
$runtimeLog = $null
$launchCounter = 0
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-scheduler -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $scheduler = (Resolve-Path "target\debug\agentmod-scheduler.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-runtime-scheduler-recovery-e2e-" +
        [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-runtime-scheduler-recovery-" +
        [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_SCHEDULER_PROGRAM = $scheduler
    $env:AGENTMOD_SCHEDULER_ROOT = Join-Path $runRoot "scheduler"
    $env:AGENTMOD_SCHEDULER_POLL_MS = "0"

    function Start-Runtime {
        $script:launchCounter++
        $script:runtimeLog = Join-Path $runRoot (
            "runtime-" + $script:launchCounter + ".stderr.log"
        )
        Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru `
            -RedirectStandardError $script:runtimeLog
    }
    function Wait-Runtime {
        for ($attempt = 0; $attempt -lt 80; $attempt++) {
            try {
                & $cli doctor --json 2>$null | Out-Null
                if ($LASTEXITCODE -eq 0) { return }
            } catch {}
            Start-Sleep -Milliseconds 100
        }
        $detail = if (Test-Path -LiteralPath $script:runtimeLog) {
            Get-Content $script:runtimeLog -Raw
        } else {
            "runtime produced no diagnostic log"
        }
        throw "runtime did not become ready: $detail"
    }
    function Stop-Runtime {
        if ($null -ne $script:daemon -and -not $script:daemon.HasExited) {
            Stop-Process -Id $script:daemon.Id -Force
            $script:daemon.WaitForExit()
        }
    }

    $daemon = Start-Runtime
    Wait-Runtime
    $created = & $cli session create --workspace $workspace `
        --style persistent-chat --json | ConvertFrom-Json
    & $cli schedule add "crash-recovery" `
        --session $created.session_id `
        --prompt "recover claimed scheduled work" `
        --at-ms 0 --json | Out-Null
    $claimed = & $cli schedule claim --limit 4 --json | ConvertFrom-Json
    if (@($claimed.executions).Count -ne 1) {
        throw "due occurrence was not claimed for recovery"
    }
    $executionId = $claimed.executions[0].execution_id
    $executionRecord = Join-Path $env:AGENTMOD_SCHEDULER_ROOT (
        "executions\" + $executionId + ".json"
    )
    if (-not (Test-Path -LiteralPath $executionRecord)) {
        throw "claimed execution was not durable"
    }
    Stop-Runtime

    $env:AGENTMOD_SCHEDULER_COMPLETION_DELAY_MS = "10000"
    $daemon = Start-Runtime
    $journalPath = Join-Path $runRoot (
        "sessions\" + $created.session_id + "\events.jsonl"
    )
    $canonicalCompletionObserved = $false
    for ($attempt = 0; $attempt -lt 100; $attempt++) {
        if (Test-Path -LiteralPath $journalPath) {
            try {
                $events = @(Get-Content $journalPath | ForEach-Object {
                    ($_ | ConvertFrom-Json).event
                })
            }
            catch {
                Start-Sleep -Milliseconds 100
                continue
            }
            if (@($events | Where-Object {
                $_.metadata.event_type -eq "model.response_completed"
            }).Count -eq 1) {
                $canonicalCompletionObserved = $true
                break
            }
        }
        Start-Sleep -Milliseconds 100
    }
    if (-not $canonicalCompletionObserved) {
        throw "startup recovery did not execute the unstarted claim"
    }
    $terminalMarker = Join-Path $env:AGENTMOD_SCHEDULER_ROOT (
        "executions\" + $executionId + ".succeeded"
    )
    if (Test-Path -LiteralPath $terminalMarker) {
        throw "crash injection missed the pre-terminal completion window"
    }
    Stop-Runtime

    Remove-Item "Env:AGENTMOD_SCHEDULER_COMPLETION_DELAY_MS" `
        -ErrorAction SilentlyContinue
    $daemon = Start-Runtime
    Wait-Runtime
    if (-not (Test-Path -LiteralPath $terminalMarker)) {
        throw "canonical completion was not reconciled to the scheduler"
    }
    $eventsAfter = @(Get-Content $journalPath | ForEach-Object {
        ($_ | ConvertFrom-Json).event
    })
    if (@($eventsAfter | Where-Object {
        $_.metadata.event_type -eq "scheduler.fired"
    }).Count -ne 1) {
        throw "recovery duplicated scheduler provenance"
    }
    if (@($eventsAfter | Where-Object {
        $_.metadata.event_type -eq "model.response_completed"
    }).Count -ne 1) {
        throw "recovery duplicated provider execution"
    }
    $reconciled = @($eventsAfter | Where-Object {
        $_.metadata.event_type -eq "scheduler.delivery_reconciled"
    })
    if ($reconciled.Count -ne 1 -or
        $reconciled[0].payload.payload.execution_id -ne $executionId -or
        $reconciled[0].payload.payload.outcome -ne "succeeded") {
        throw "recovery did not commit one exact canonical reconciliation outcome"
    }
    $pending = & $cli schedule claim --limit 4 --json | ConvertFrom-Json
    if (@($pending.executions).Count -ne 0) {
        throw "terminal recovery left a due occurrence"
    }
    Write-Output "runtime scheduler claim and terminal reconciliation E2E passed"
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
        "AGENTMOD_SCHEDULER_ROOT",
        "AGENTMOD_SCHEDULER_POLL_MS",
        "AGENTMOD_SCHEDULER_COMPLETION_DELAY_MS"
    )) {
        Remove-Item "Env:$name" -ErrorAction SilentlyContinue
    }
    if ($null -ne $runRoot -and (Test-Path -LiteralPath $runRoot)) {
        $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
        $resolvedRun = (Resolve-Path $runRoot).Path
        if ($resolvedRun.StartsWith($resolvedTemp) -and
            (Split-Path $resolvedRun -Leaf).StartsWith(
                "agentmod-runtime-scheduler-recovery-e2e-"
            )) {
            Remove-Item -LiteralPath $resolvedRun -Recurse -Force `
                -ErrorAction SilentlyContinue
        }
    }
    Pop-Location
}
