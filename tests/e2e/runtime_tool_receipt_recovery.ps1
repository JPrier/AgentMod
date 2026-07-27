$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-filesystem-host -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-receipt-recovery-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null
    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $filesystem = (
        Resolve-Path "target\debug\agentmod-filesystem-host.exe"
    ).Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-receipt-recovery-e2e-" +
        [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_FILESYSTEM_HOST_PROGRAM = $filesystem
    $env:AGENTMOD_TOOL_RECEIPT_DELAY_MS = "8000"
    Remove-Item Env:AGENTMOD_PERMISSION_MODE -ErrorAction SilentlyContinue

    $daemon = $null
    $approval = $null
    try {
        $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru
        $ready = $false
        for ($attempt = 0; $attempt -lt 50; $attempt++) {
            try {
                & $cli doctor --json 2>$null | Out-Null
                if ($LASTEXITCODE -eq 0) {
                    $ready = $true
                    break
                }
            } catch {}
            Start-Sleep -Milliseconds 100
        }
        if (-not $ready) { throw "runtime did not become ready" }

        $created = & $cli session create --workspace $workspace `
            --style persistent-chat --json | ConvertFrom-Json
        $turn = & $cli run "write once across a crash" `
            --session $created.session_id `
            --option 'mock_scenario="approval_write"' --json | ConvertFrom-Json
        if ([string]::IsNullOrWhiteSpace($turn.awaiting_continuation)) {
            throw "turn did not request approval"
        }
        $journal = Join-Path $runRoot (
            "sessions\" + $created.session_id + "\events.jsonl"
        )
        $receiptRoot = Join-Path $runRoot (
            "sessions\" + $created.session_id +
            "\artifacts\tool-receipts"
        )
        $approvalOut = Join-Path $runRoot "approval.stdout"
        $approvalErr = Join-Path $runRoot "approval.stderr"
        $approval = Start-Process -FilePath $cli -ArgumentList @(
            "approval",
            "resolve",
            $created.session_id,
            $turn.awaiting_continuation,
            "approve",
            "--json"
        ) -WorkingDirectory $workspace -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput $approvalOut `
            -RedirectStandardError $approvalErr

        $receiptReady = $false
        for ($attempt = 0; $attempt -lt 80; $attempt++) {
            $receipts = @(
                Get-ChildItem -LiteralPath $receiptRoot -Filter "*.json" `
                    -ErrorAction SilentlyContinue
            )
            if ($receipts.Count -eq 1) {
                $receiptReady = $true
                break
            }
            if ($approval.HasExited) {
                throw "approval completed before crash window: $(
                    Get-Content $approvalErr -Raw
                )"
            }
            Start-Sleep -Milliseconds 100
        }
        if (-not $receiptReady) {
            throw "durable terminal receipt was not created"
        }
        $eventsBeforeCrash = @(Get-Content $journal | ForEach-Object {
            ($_ | ConvertFrom-Json).event
        })
        if (@($eventsBeforeCrash | Where-Object {
            $_.metadata.event_type -eq "tool.execution_dispatched"
        }).Count -ne 1 -or @($eventsBeforeCrash | Where-Object {
            $_.metadata.event_type -eq "tool.execution_completed"
        }).Count -ne 0) {
            throw "crash was not injected after dispatch and before terminal commit"
        }
        $target = Join-Path $workspace "approved.txt"
        if (-not (Test-Path -LiteralPath $target)) {
            throw "host side effect did not occur before receipt"
        }

        Stop-Process -Id $daemon.Id -Force
        $daemon.WaitForExit()
        $daemon = $null
        if (-not $approval.WaitForExit(5000)) {
            Stop-Process -Id $approval.Id -Force
        }
        $approval = $null

        Remove-Item Env:AGENTMOD_TOOL_RECEIPT_DELAY_MS
        $env:AGENTMOD_FILESYSTEM_HOST_PROGRAM = Join-Path $runRoot (
            "host-must-not-be-spawned.exe"
        )
        $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru
        $ready = $false
        for ($attempt = 0; $attempt -lt 50; $attempt++) {
            try {
                & $cli doctor --json 2>$null | Out-Null
                if ($LASTEXITCODE -eq 0) {
                    $ready = $true
                    break
                }
            } catch {}
            Start-Sleep -Milliseconds 100
        }
        if (-not $ready) { throw "restarted runtime did not become ready" }

        $recovered = & $cli approval resolve $created.session_id `
            $turn.awaiting_continuation approve --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or $recovered.awaiting_continuation) {
            throw "receipt reconciliation did not complete the turn"
        }
        $visible = ($recovered.events | Where-Object event -eq "text" |
            ForEach-Object text) -join ""
        if ($visible -ne "continued after durable approval decision") {
            throw "provider did not continue after receipt recovery: $visible"
        }
        if ((Get-Content -LiteralPath $target -Raw) -ne "executed once`n") {
            throw "recovered side effect was not exactly once"
        }
        $eventsAfter = @(Get-Content $journal | ForEach-Object {
            ($_ | ConvertFrom-Json).event
        })
        foreach ($eventType in @(
            "approval.resolved",
            "tool.execution_dispatched",
            "tool.execution_started",
            "tool.execution_completed"
        )) {
            if (@($eventsAfter | Where-Object {
                $_.metadata.event_type -eq $eventType
            }).Count -ne 1) {
                throw "$eventType was not committed exactly once"
            }
        }
        if ($eventsAfter | Where-Object {
            $_.metadata.event_type -eq "tool.execution_failed"
        }) {
            throw "receipt reconciliation recorded a false host failure"
        }
        Write-Output "runtime post-dispatch receipt recovery E2E passed"
    }
    finally {
        if ($null -ne $approval -and -not $approval.HasExited) {
            Stop-Process -Id $approval.Id -Force
        }
        if ($null -ne $daemon -and -not $daemon.HasExited) {
            Stop-Process -Id $daemon.Id -Force
        }
        if (Test-Path -LiteralPath $runRoot) {
            $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
            $resolvedRun = (Resolve-Path $runRoot).Path
            if ($resolvedRun.StartsWith($resolvedTemp) -and
                (Split-Path $resolvedRun -Leaf).StartsWith(
                    "agentmod-receipt-recovery-e2e-"
                )) {
                Remove-Item -LiteralPath $resolvedRun -Recurse -Force
            }
        }
    }
}
finally {
    Remove-Item Env:AGENTMOD_TOOL_RECEIPT_DELAY_MS `
        -ErrorAction SilentlyContinue
    Pop-Location
}
