$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-session-reconnect-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null
    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-session-reconnect-e2e-" +
        [guid]::NewGuid().ToString("N")
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
        & $cli run "create reconnect history" --session $created.session_id `
            --option 'mock_scenario="streaming_text"' --json | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "turn failed" }

        $first = & $cli session events $created.session_id `
            --limit 3 --json | ConvertFrom-Json
        $second = & $cli session events $created.session_id `
            --after $first.last_delivered_sequence --limit 4 --json |
            ConvertFrom-Json
        $third = & $cli session events $created.session_id `
            --after $second.last_delivered_sequence --limit 4 --json |
            ConvertFrom-Json
        $caughtUp = & $cli session events $created.session_id `
            --after $third.last_delivered_sequence --limit 4 --json |
            ConvertFrom-Json

        $sequences = @(
            @($first.events).sequence +
            @($second.events).sequence +
            @($third.events).sequence
        )
        if ((Compare-Object $sequences (1..10) -SyncWindow 0).Count -ne 0) {
            throw "reconnect pages omitted or duplicated canonical events"
        }
        if (-not $first.has_more -or -not $second.has_more -or
            $third.has_more -or $third.head_sequence -ne 10 -or
            $third.last_delivered_sequence -ne 10) {
            throw "reconnect cursor metadata was incorrect"
        }
        if (@($caughtUp.events).Count -ne 0 -or $caughtUp.has_more -or
            $caughtUp.head_sequence -ne 10 -or
            $caughtUp.last_delivered_sequence -ne 10) {
            throw "caught-up reconnect page was not stable and empty"
        }
        Write-Output "runtime credit-window reconnect-from-sequence E2E passed"
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
                    "agentmod-session-reconnect-e2e-"
                )) {
                Remove-Item -LiteralPath $resolvedRun -Recurse -Force
            }
        }
    }
}
finally {
    Pop-Location
}
