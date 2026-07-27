$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$root = $null
Push-Location $repository
try {
    cargo build -p agentmod-scheduler
    if ($LASTEXITCODE -ne 0) { throw "scheduler build failed" }
    $scheduler = (Resolve-Path "target\debug\agentmod-scheduler.exe").Path
    $root = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-scheduler-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $root -Force | Out-Null
    $env:AGENTMOD_SCHEDULER_ROOT = $root
    $env:AGENTMOD_SCHEDULER_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef"
    )

    function CommandJson($command, $value) {
        @{ command = $command; value = $value } |
            ConvertTo-Json -Compress -Depth 12
    }
    $negotiate = CommandJson "negotiate" @{
        protocol_version = 1
        capabilities = @("durable_schedules")
        authentication_token = $env:AGENTMOD_SCHEDULER_AUTH_TOKEN
    }
    $once = @{
        schedule_id = "once"
        session_id = "session:1"
        idempotency_id = "idempotency:once"
        style = "persistent-chat"
        workspace = "workspace"
        permission_policy = "safe-background"
        provider = "deterministic-mock"
        model = "mock"
        token_budget = 100
        cost_budget_micros = 0
        trigger = @{ kind = "at_millis"; value = 0 }
        payload = @{
            kind = "prompt"
            value = @{ prompt = "scheduled work" }
        }
        active = $true
    }
    $eventSchedule = $once.Clone()
    $eventSchedule.schedule_id = "event"
    $eventSchedule.idempotency_id = "idempotency:event"
    $eventSchedule.trigger = @{
        kind = "runtime_event"
        value = @{ event_type = "tool.execution_completed" }
    }
    $commands = @(
        $negotiate,
        (CommandJson "upsert" @{ schedule = $once }),
        (CommandJson "upsert" @{ schedule = $once }),
        (CommandJson "upsert" @{ schedule = $eventSchedule }),
        (CommandJson "list" @{ limit = 10 }),
        (CommandJson "claim_due" @{ limit = 10 }),
        (CommandJson "claim_due" @{ limit = 10 }),
        (CommandJson "fire_runtime_event" @{
            event_id = "event:1"
            event_type = "tool.execution_completed"
        }),
        (CommandJson "fire_runtime_event" @{
            event_id = "event:1"
            event_type = "tool.execution_completed"
        })
    )
    $responses = @($commands | & $scheduler | ForEach-Object {
        $_ | ConvertFrom-Json
    })
    if ($LASTEXITCODE -ne 0) { throw "scheduler protocol failed" }
    if ($responses[0].result -ne "negotiated" -or
        $responses[2].value.replayed -ne $true -or
        @($responses[4].value.schedules).Count -ne 2 -or
        @($responses[5].value.executions).Count -ne 1 -or
        @($responses[6].value.executions).Count -ne 0 -or
        @($responses[7].value.executions).Count -ne 1 -or
        @($responses[8].value.executions).Count -ne 0) {
        throw ($responses | ConvertTo-Json -Depth 20)
    }
    $executionId = $responses[5].value.executions[0].execution_id

    $completion = @(
        $negotiate,
        (CommandJson "complete_execution" @{
            execution_id = $executionId
            succeeded = $true
        }),
        (CommandJson "complete_execution" @{
            execution_id = $executionId
            succeeded = $true
        }),
        (CommandJson "claim_due" @{ limit = 10 })
    ) | & $scheduler | ForEach-Object { $_ | ConvertFrom-Json }
    if ($LASTEXITCODE -ne 0 -or
        $completion[1].value.changed -ne $true -or
        $completion[2].value.changed -ne $false -or
        @($completion[3].value.executions).Count -ne 0) {
        throw ($completion | ConvertTo-Json -Depth 20)
    }
    if (@(Get-ChildItem (Join-Path $root "executions") `
        -Filter "*.json").Count -ne 2) {
        throw "durable execution claims are missing"
    }
    Write-Output "durable scheduler worker E2E passed"
}
finally {
    Remove-Item Env:AGENTMOD_SCHEDULER_ROOT -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_SCHEDULER_AUTH_TOKEN -ErrorAction SilentlyContinue
    if ($null -ne $root -and (Test-Path -LiteralPath $root)) {
        $resolvedTemp = (
            Resolve-Path ([System.IO.Path]::GetTempPath())
        ).Path
        $resolvedRoot = (Resolve-Path $root).Path
        if ($resolvedRoot.StartsWith($resolvedTemp) -and
            (Split-Path $resolvedRoot -Leaf).StartsWith(
                "agentmod-scheduler-e2e-"
            )) {
            Remove-Item -LiteralPath $resolvedRoot -Recurse -Force
        }
    }
    Pop-Location
}
