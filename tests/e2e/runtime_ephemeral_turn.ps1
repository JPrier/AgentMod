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
        "agentmod-ephemeral-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-ephemeral-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness

    function Start-TestRuntime {
        $process = Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru
        for ($attempt = 0; $attempt -lt 80; $attempt++) {
            try {
                & $cli doctor --json 2>$null | Out-Null
                if ($LASTEXITCODE -eq 0) { return $process }
            } catch {}
            Start-Sleep -Milliseconds 100
        }
        if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force }
        throw "runtime did not become ready"
    }

    function Stop-TestRuntime($process) {
        if ($null -ne $process -and -not $process.HasExited) {
            Stop-Process -Id $process.Id -Force
            $process.WaitForExit()
        }
    }

    function Assert-EmptyProjection($sessionId) {
        $inspection = & $cli session inspect $sessionId --json | ConvertFrom-Json
        if (@($inspection.state.conversation.provider_projection).Count -ne 0) {
            throw "ephemeral provider projection was retained"
        }
        return $inspection
    }

    $daemon = Start-TestRuntime
    try {
        $session = & $cli session create --workspace $repository `
            --style ephemeral-turn@1.1.0 --json | ConvertFrom-Json
        $firstPrompt = "turn-one-secret-input"
        $firstOutput = "turn-one-secret-output"
        & $cli run $firstPrompt --session $session.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option ('mock_text="' + $firstOutput + '"') --json | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "first ephemeral turn failed" }
        $firstInspection = Assert-EmptyProjection $session.session_id
        $firstHistory = $firstInspection.state.conversation.history |
            ConvertTo-Json -Depth 20 -Compress
        if (-not $firstHistory.Contains($firstPrompt) -or
            -not $firstHistory.Contains($firstOutput)) {
            throw "first canonical turn history was not retained"
        }

        Stop-TestRuntime $daemon
        $daemon = Start-TestRuntime
        $restartInspection = Assert-EmptyProjection $session.session_id
        $restartHistory = $restartInspection.state.conversation.history |
            ConvertTo-Json -Depth 20 -Compress
        if (-not $restartHistory.Contains($firstPrompt) -or
            -not $restartHistory.Contains($firstOutput)) {
            throw "canonical history did not survive restart"
        }

        $secondPrompt = "turn-two-current-input"
        $secondOutput = "turn-two-output"
        & $cli run $secondPrompt --session $session.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option ('mock_text="' + $secondOutput + '"') --json | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "second ephemeral turn failed" }
        $finalInspection = Assert-EmptyProjection $session.session_id
        $historyJson = $finalInspection.state.conversation.history |
            ConvertTo-Json -Depth 20 -Compress
        foreach ($expected in @(
            $firstPrompt, $firstOutput, $secondPrompt, $secondOutput
        )) {
            if (-not $historyJson.Contains($expected)) {
                throw "canonical history is missing $expected"
            }
        }

        $journalPath = Join-Path $runRoot (
            "sessions\" + $session.session_id + "\events.jsonl"
        )
        $journal = @(Get-Content -LiteralPath $journalPath | ForEach-Object {
            $_ | ConvertFrom-Json
        })
        $fresh = @($journal | Where-Object {
            ($_.event | ConvertTo-Json -Depth 30 -Compress).Contains(
                "ephemeral_fresh_context"
            )
        })
        $discard = @($journal | Where-Object {
            ($_.event | ConvertTo-Json -Depth 30 -Compress).Contains(
                "ephemeral_discard"
            )
        })
        if ($fresh.Count -ne 2 -or $discard.Count -ne 2) {
            throw "expected one fresh projection and one discard per turn"
        }
        $secondFresh = $fresh[1].event | ConvertTo-Json -Depth 30 -Compress
        if (-not $secondFresh.Contains($secondPrompt) -or
            $secondFresh.Contains($firstPrompt) -or
            $secondFresh.Contains($firstOutput)) {
            throw "second fresh projection leaked unselected turn-one state"
        }

        Write-Output "runtime ephemeral-turn fresh-context/restart E2E passed"
    }
    finally {
        Stop-TestRuntime $daemon
        $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
        $resolvedRun = (Resolve-Path $runRoot).Path
        if ($resolvedRun.StartsWith($resolvedTemp) -and
            (Split-Path $resolvedRun -Leaf).StartsWith(
                "agentmod-ephemeral-e2e-"
            )) {
            Remove-Item -LiteralPath $resolvedRun -Recurse -Force
        }
    }
}
finally {
    Pop-Location
}
$global:LASTEXITCODE = 0
