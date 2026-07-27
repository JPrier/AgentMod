$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-git-host -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $gitHost = (Resolve-Path "target\debug\agentmod-git-host.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-git-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null
    & git -C $workspace init --quiet
    if ($LASTEXITCODE -ne 0) { throw "Git fixture initialization failed" }
    & git -C $workspace -c user.name=AgentMod `
        -c user.email=agentmod@example.invalid commit --allow-empty `
        --quiet -m initial
    if ($LASTEXITCODE -ne 0) { throw "Git fixture commit failed" }
    Set-Content -LiteralPath (Join-Path $workspace "untracked.txt") `
        -Value "runtime Git host fixture"

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-git-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_GIT_HOST_PROGRAM = $gitHost
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
        $turn = & $cli run "inspect the Git status and continue" `
            --session $created.session_id `
            --option 'mock_scenario="git_status"' --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "Git tool turn failed" }
        $visible = ($turn.events | Where-Object event -eq "text" |
            ForEach-Object text) -join ""
        if ($visible -ne "continued after approved runtime decision") {
            throw "unexpected continued output: $visible"
        }

        $journalPath = Join-Path $runRoot (
            "sessions\" + $created.session_id + "\events.jsonl"
        )
        $events = @(Get-Content $journalPath | ForEach-Object {
            ($_ | ConvertFrom-Json).event
        })
        foreach ($required in @(
            "tool.call_proposed",
            "tool.call_approved",
            "tool.execution_started",
            "tool.execution_completed"
        )) {
            if (@($events | Where-Object {
                $_.metadata.event_type -eq $required
            }).Count -ne 1) {
                $details = $events | ConvertTo-Json -Depth 20
                throw "missing or duplicated Git lifecycle event: $required`n$details"
            }
        }
        $resultEntry = @($events | Where-Object {
            $_.metadata.event_type -eq "conversation.entry_committed" -and
            $_.payload.payload.entry.kind -eq "tool_result"
        })
        if ($resultEntry.Count -ne 1) {
            throw "Git result did not enter canonical conversation"
        }
        $projection = $resultEntry[0].payload.payload.entry.content.content |
            ConvertFrom-Json
        $actualRoot = $projection.repository_root -replace '^\\\\\?\\', ''
        $expectedRoot = $workspace -replace '^\\\\\?\\', ''
        if (-not $actualRoot.Equals(
                $expectedRoot,
                [System.StringComparison]::OrdinalIgnoreCase
            ) -or
            @($projection.changes | Where-Object {
                $_.path -eq "untracked.txt"
            }).Count -ne 1) {
            $details = $projection | ConvertTo-Json -Depth 20
            throw "Git status projection was incorrect`n$details"
        }
        Write-Output "runtime/CLI/harness/Git tool-loop E2E passed"
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
                    "agentmod-git-e2e-"
                )) {
                Remove-Item -LiteralPath $resolvedRun -Recurse -Force
            }
        }
    }
}
finally {
    Pop-Location
}
