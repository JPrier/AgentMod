$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-filesystem-host -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $filesystem = (Resolve-Path "target\debug\agentmod-filesystem-host.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-tool-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot | Out-Null
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path (Join-Path $workspace "src") | Out-Null
    Set-Content -LiteralPath (Join-Path $workspace "src\lib.rs") `
        -Value "pub fn fixture() -> bool { true }"
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-tool-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_FILESYSTEM_HOST_PROGRAM = $filesystem
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
        if ($LASTEXITCODE -ne 0) { throw "session creation failed" }
        $turn = & $cli run "read src/lib.rs and continue" `
            --session $created.session_id `
            --option 'mock_scenario="one_tool_call"' --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "tool turn failed" }
        if ($turn.last_committed_sequence -ne 27) {
            throw "tool turn committed unexpected sequence $($turn.last_committed_sequence)"
        }
        $visible = ($turn.events | Where-Object event -eq "text" |
            ForEach-Object text) -join ""
        if ($visible -ne "continued after approved runtime decision") {
            throw "unexpected continued output: $visible"
        }
        $journalPath = Join-Path $runRoot (
            "sessions\" + $created.session_id + "\events.jsonl"
        )
        $journal = Get-Content $journalPath
        $eventTypes = @($journal | ForEach-Object {
            ($_ | ConvertFrom-Json).event.metadata.event_type
        })
        foreach ($required in @(
            "model.tool_call_proposed",
            "tool.call_proposed",
            "tool.call_approved",
            "tool.execution_dispatched",
            "tool.execution_started",
            "tool.execution_completed"
        )) {
            if ($eventTypes -notcontains $required) {
                $details = ($journal -join "`n")
                throw "missing tool lifecycle event: $required`n$details"
            }
        }
        $conversationEntries = @($journal | ForEach-Object {
            $frame = $_ | ConvertFrom-Json
            if ($frame.event.metadata.event_type -eq "conversation.entry_committed") {
                $frame.event.payload.payload.entry
            }
        })
        $serialized = $conversationEntries | ConvertTo-Json -Depth 20
        if ($serialized -notmatch "tool_call_request" -or
            $serialized -notmatch "tool_result") {
            throw "canonical conversation is missing tool request/result"
        }
        Write-Output "runtime/CLI/harness/filesystem tool-loop E2E passed"
    }
    finally {
        if ($null -ne $daemon -and -not $daemon.HasExited) {
            Stop-Process -Id $daemon.Id -Force
        }
        $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
        $resolvedRun = (Resolve-Path $runRoot).Path
        if ($resolvedRun.StartsWith($resolvedTemp) -and
            (Split-Path $resolvedRun -Leaf).StartsWith("agentmod-tool-e2e-")) {
            Remove-Item -LiteralPath $resolvedRun -Recurse -Force
        }
    }
}
finally {
    Pop-Location
}
