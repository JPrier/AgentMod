$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-filesystem-host -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-startup-recovery-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    $source = Join-Path $workspace "src"
    New-Item -ItemType Directory -Path $source -Force | Out-Null
    Set-Content -LiteralPath (Join-Path $source "lib.rs") `
        -Value "pub fn recovered() -> bool { true }" -NoNewline
    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $filesystem = (
        Resolve-Path "target\debug\agentmod-filesystem-host.exe"
    ).Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-startup-recovery-e2e-" +
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
    $turn = $null
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
        $journal = Join-Path $runRoot (
            "sessions\" + $created.session_id + "\events.jsonl"
        )
        $receiptRoot = Join-Path $runRoot (
            "sessions\" + $created.session_id +
            "\artifacts\tool-receipts"
        )
        $turnOut = Join-Path $runRoot "turn.stdout"
        $turnErr = Join-Path $runRoot "turn.stderr"
        $turn = Start-Process -FilePath $cli -ArgumentList @(
            "run",
            "read-before-the-daemon-crashes",
            "--session",
            $created.session_id,
            "--option",
            'mock_scenario="one_tool_call"',
            "--json"
        ) -WorkingDirectory $workspace -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput $turnOut -RedirectStandardError $turnErr

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
            if ($turn.HasExited) {
                throw "turn completed before crash window: $(
                    Get-Content $turnErr -Raw
                )"
            }
            Start-Sleep -Milliseconds 100
        }
        if (-not $receiptReady) {
            throw "durable terminal receipt was not created"
        }
        $before = @(Get-Content $journal | ForEach-Object {
            ($_ | ConvertFrom-Json).event
        })
        if (@($before | Where-Object {
            $_.metadata.event_type -eq "tool.execution_dispatched"
        }).Count -ne 1 -or @($before | Where-Object {
            $_.metadata.event_type -eq "tool.execution_completed"
        }).Count -ne 0) {
            throw "crash was not injected in the nonterminal dispatch window"
        }

        Stop-Process -Id $daemon.Id -Force
        $daemon.WaitForExit()
        $daemon = $null
        if (-not $turn.WaitForExit(5000)) {
            Stop-Process -Id $turn.Id -Force
        }
        $turn = $null

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
        if (-not $ready) {
            throw "runtime did not recover before accepting RPC"
        }

        $after = @(Get-Content $journal | ForEach-Object {
            ($_ | ConvertFrom-Json).event
        })
        foreach ($eventType in @(
            "tool.execution_dispatched",
            "tool.execution_started",
            "tool.execution_completed"
        )) {
            if (@($after | Where-Object {
                $_.metadata.event_type -eq $eventType
            }).Count -ne 1) {
                throw "$eventType was not reconciled exactly once"
            }
        }
        $serialized = Get-Content -LiteralPath $journal -Raw
        if ($serialized -notmatch "tool_call_request" -or
            $serialized -notmatch "tool_result") {
            throw "startup recovery did not project the receipt into context"
        }
        & $cli run "continue after startup recovery" `
            --session $created.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="startup-recovery-ok"' --json | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "session could not continue after startup reconciliation"
        }
        Write-Output "runtime startup-wide tool receipt recovery E2E passed"
    }
    finally {
        if ($null -ne $turn -and -not $turn.HasExited) {
            Stop-Process -Id $turn.Id -Force
        }
        if ($null -ne $daemon -and -not $daemon.HasExited) {
            Stop-Process -Id $daemon.Id -Force
        }
        if (Test-Path -LiteralPath $runRoot) {
            $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
            $resolvedRun = (Resolve-Path $runRoot).Path
            if ($resolvedRun.StartsWith($resolvedTemp) -and
                (Split-Path $resolvedRun -Leaf).StartsWith(
                    "agentmod-startup-recovery-e2e-"
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
