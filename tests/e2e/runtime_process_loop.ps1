$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-filesystem-host -p agentmod-process-host -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $filesystem = (Resolve-Path "target\debug\agentmod-filesystem-host.exe").Path
    $processHost = (Resolve-Path "target\debug\agentmod-process-host.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-process-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-process-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_FILESYSTEM_HOST_PROGRAM = $filesystem
    $env:AGENTMOD_PROCESS_HOST_PROGRAM = $processHost
    $env:AGENTMOD_PROCESS_ALLOWED_EXECUTABLES = "cargo"
    $env:AGENTMOD_PERMISSION_MODE = "allow"
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
        $turn = & $cli run "run the tool and continue" `
            --session $created.session_id `
            --option 'mock_scenario="one_process_call"' --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "process turn failed" }
        if ($turn.last_committed_sequence -ne 19) {
            throw "unexpected process turn sequence $($turn.last_committed_sequence)"
        }
        $journalPath = Join-Path $runRoot (
            "sessions\" + $created.session_id + "\events.jsonl"
        )
        $journal = Get-Content $journalPath
        $eventTypes = @($journal | ForEach-Object {
            ($_ | ConvertFrom-Json).event.metadata.event_type
        })
        foreach ($required in @(
            "tool.call_proposed",
            "tool.call_approved",
            "tool.execution_dispatched",
            "tool.execution_started",
            "tool.output_observed",
            "tool.execution_completed"
        )) {
            if ($eventTypes -notcontains $required) {
                throw "missing process lifecycle event: $required"
            }
        }
        if (($journal -join "`n") -notmatch "cargo [0-9]") {
            throw "cargo output did not enter the canonical tool result path"
        }
        Write-Output "runtime/CLI/harness/process-host tool-loop E2E passed"
    }
    finally {
        if ($null -ne $daemon -and -not $daemon.HasExited) {
            Stop-Process -Id $daemon.Id -Force
        }
        $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
        $resolvedRun = (Resolve-Path $runRoot).Path
        if ($resolvedRun.StartsWith($resolvedTemp) -and
            (Split-Path $resolvedRun -Leaf).StartsWith("agentmod-process-e2e-")) {
            Remove-Item -LiteralPath $resolvedRun -Recurse -Force
        }
    }
}
finally {
    Pop-Location
}
