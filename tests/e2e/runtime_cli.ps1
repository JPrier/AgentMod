$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
        -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru
    try {
        $ready = $false
        for ($attempt = 0; $attempt -lt 50; $attempt++) {
            $doctor = & $cli doctor --json 2>$null
            if ($LASTEXITCODE -eq 0) {
                $ready = $true
                break
            }
            Start-Sleep -Milliseconds 100
        }
        if (-not $ready) { throw "runtime did not become ready" }
        $doctorResult = $doctor | ConvertFrom-Json
        if ($doctorResult.state -ne "ready") {
            throw "runtime health was not ready"
        }

        $created = & $cli session create --workspace $repository `
            --style persistent-chat --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "session creation failed" }
        $listed = & $cli session list --limit 10 --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "session listing failed" }
        if ($listed.sessions.Count -ne 1) { throw "expected one session" }
        if ($listed.sessions[0].id -ne $created.session_id) {
            throw "created/listed session mismatch"
        }
        $turn = & $cli run "complete the deterministic turn" `
            --session $created.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="daemon-turn-ok"' --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "turn execution failed" }
        if ($turn.last_committed_sequence -ne 10) {
            throw "turn did not commit its complete provider lifecycle"
        }
        $visible = ($turn.events | Where-Object event -eq "text" |
            ForEach-Object text) -join ""
        if ($visible -ne "alpha beta daemon-turn-ok") {
            throw "unexpected provider output: $visible"
        }
        $sessionRoot = Join-Path $runRoot ("sessions\" + $created.session_id)
        foreach ($required in @(
            "metadata.json", "events.jsonl", "style.json", "style.lock",
            "workspace.json", "continuations", "snapshots", "artifacts",
            "process-logs", "branches"
        )) {
            if (-not (Test-Path (Join-Path $sessionRoot $required))) {
                throw "missing session entry: $required"
            }
        }
        $journal = Get-Content (Join-Path $sessionRoot "events.jsonl")
        if ($journal.Count -ne 10) {
            throw "canonical journal does not contain the complete turn"
        }
        $eventTypes = @($journal | ForEach-Object {
            ($_ | ConvertFrom-Json).event.metadata.event_type
        })
        $expectedTypes = @(
            "session.created",
            "conversation.entry_committed",
            "model.request_proposed",
            "model.request_approved",
            "model.request_started",
            "model.output_delta_observed",
            "model.output_delta_observed",
            "model.output_delta_observed",
            "model.response_completed",
            "conversation.entry_committed"
        )
        if ((Compare-Object $eventTypes $expectedTypes -SyncWindow 0).Count -ne 0) {
            throw "canonical provider lifecycle event order was incorrect"
        }
        Write-Output "runtime/CLI/harness durable-turn E2E passed"
    }
    finally {
        if ($null -ne $daemon -and -not $daemon.HasExited) {
            Stop-Process -Id $daemon.Id -Force
        }
        $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
        $resolvedRun = (Resolve-Path $runRoot).Path
        if ($resolvedRun.StartsWith($resolvedTemp) -and
            (Split-Path $resolvedRun -Leaf).StartsWith("agentmod-e2e-")) {
            Remove-Item -LiteralPath $resolvedRun -Recurse -Force
        }
    }
}
finally {
    Pop-Location
}
