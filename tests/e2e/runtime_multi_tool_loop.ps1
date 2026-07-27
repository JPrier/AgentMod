$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-filesystem-host -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-multi-tool-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path (Join-Path $workspace "src") -Force |
        Out-Null
    Set-Content -LiteralPath (Join-Path $workspace "src\lib.rs") `
        -Value "pub const NEEDLE: &str = `"needle`";"

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $filesystem = (Resolve-Path "target\debug\agentmod-filesystem-host.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-multi-tool-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_FILESYSTEM_HOST_PROGRAM = $filesystem
    Remove-Item Env:AGENTMOD_PERMISSION_MODE -ErrorAction SilentlyContinue

    $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
        -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru
    try {
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
        $turn = & $cli run "read and search in one provider response" `
            --session $created.session_id `
            --option 'mock_scenario="multiple_tool_calls"' --json |
            ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or $turn.awaiting_continuation) {
            throw "multi-tool turn did not complete"
        }
        $visible = ($turn.events | Where-Object event -eq "text" |
            ForEach-Object text) -join ""
        if ($visible -ne "continued after approved runtime decision") {
            throw "unexpected joined output: $visible"
        }

        $journalPath = Join-Path $runRoot (
            "sessions\" + $created.session_id + "\events.jsonl"
        )
        $events = @(Get-Content $journalPath | ForEach-Object {
            ($_ | ConvertFrom-Json).event
        })
        foreach ($eventType in @(
            "model.tool_call_proposed",
            "tool.call_proposed",
            "tool.call_approved",
            "tool.execution_started",
            "tool.execution_completed"
        )) {
            if (@($events | Where-Object {
                $_.metadata.event_type -eq $eventType
            }).Count -ne 2) {
                throw "expected two $eventType events"
            }
        }
        $toolResults = @($events | Where-Object {
            $_.metadata.event_type -eq "conversation.entry_committed" -and
            $_.payload.payload.entry.kind -eq "tool_result"
        })
        if ($toolResults.Count -ne 2) {
            throw "provider projection did not contain both tool results"
        }
        if (@($events | Where-Object {
            $_.metadata.event_type -eq "model.request_started"
        }).Count -ne 2) {
            throw "tool batch caused more than one resumed provider request"
        }
        Write-Output "runtime multi-tool batch/join E2E passed"
    }
    finally {
        if ($null -ne $daemon -and -not $daemon.HasExited) {
            Stop-Process -Id $daemon.Id -Force
        }
        if (Test-Path -LiteralPath $runRoot) {
            $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
            $resolvedRun = (Resolve-Path $runRoot).Path
            if ($resolvedRun.StartsWith($resolvedTemp) -and
                (Split-Path $resolvedRun -Leaf).StartsWith(
                    "agentmod-multi-tool-e2e-"
                )) {
                Remove-Item -LiteralPath $resolvedRun -Recurse -Force
            }
        }
    }
}
finally {
    Pop-Location
}
