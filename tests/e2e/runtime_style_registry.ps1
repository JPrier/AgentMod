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
        "agentmod-style-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-style-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness

    function Start-TestRuntime {
        $process = Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru
        for ($attempt = 0; $attempt -lt 80; $attempt++) {
            & $cli doctor --json 2>$null | Out-Null
            if ($LASTEXITCODE -eq 0) { return $process }
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

    $daemon = Start-TestRuntime
    try {
        $styles = & $cli style list --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "style listing failed" }
        if ($styles.styles.Count -ne 5) { throw "expected five built-in styles" }
        foreach ($required in @(
            "persistent-chat", "ephemeral-turn", "research-loop",
            "planner-worker", "declarative-graph"
        )) {
            $style = $styles.styles | Where-Object id -eq $required
            if ($null -eq $style -or $style.availability -ne "available") {
                throw "required style is unavailable: $required"
            }
        }

        $persistent = & $cli session create --workspace $repository `
            --style persistent-chat --json | ConvertFrom-Json
        $ephemeral = & $cli session create --workspace $repository `
            --style ephemeral-turn@1.0.0 --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "style-bound session creation failed" }

        foreach ($entry in @(
            @($persistent.session_id, "persistent-chat"),
            @($ephemeral.session_id, "ephemeral-turn")
        )) {
            $inspection = & $cli session inspect $entry[0] --json | ConvertFrom-Json
            if ($inspection.state.style_binding.id -ne $entry[1]) {
                throw "session style binding mismatch"
            }
            if ($inspection.state.style_binding.version -ne "1.0.0") {
                throw "session style version mismatch"
            }
            if ($inspection.state.style_binding.harness -ne "native") {
                throw "session harness binding mismatch"
            }
            if ($inspection.state.style_compatibility.status -ne "compatible") {
                throw "new style binding is not compatible"
            }
        }

        $persistentTurn = & $cli run "persistent before restart" `
            --session $persistent.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="persistent-before"' --json | ConvertFrom-Json
        $ephemeralTurn = & $cli run "ephemeral before restart" `
            --session $ephemeral.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="ephemeral-before"' --json | ConvertFrom-Json
        if ($persistentTurn.last_committed_sequence -ne 10 -or
            $ephemeralTurn.last_committed_sequence -ne 10) {
            throw "pre-restart turns did not complete"
        }

        Stop-TestRuntime $daemon
        $daemon = Start-TestRuntime

        foreach ($sessionId in @($persistent.session_id, $ephemeral.session_id)) {
            $inspection = & $cli session inspect $sessionId --json | ConvertFrom-Json
            if ($inspection.state.style_compatibility.status -ne "compatible") {
                throw "style binding did not survive restart"
            }
        }
        $persistentAfter = & $cli run "persistent after restart" `
            --session $persistent.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="persistent-after"' --json | ConvertFrom-Json
        $ephemeralAfter = & $cli run "ephemeral after restart" `
            --session $ephemeral.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="ephemeral-after"' --json | ConvertFrom-Json
        if ($persistentAfter.last_committed_sequence -ne 19 -or
            $ephemeralAfter.last_committed_sequence -ne 19) {
            throw "post-restart turns did not complete"
        }

        $branch = & $cli session branch $persistent.session_id --at 10 `
            --style ephemeral-turn --json | ConvertFrom-Json
        $branchInspection = & $cli session inspect $branch.session_id --json |
            ConvertFrom-Json
        if ($branchInspection.state.style_binding.id -ne "ephemeral-turn") {
            throw "branch did not receive its explicitly selected style"
        }

        $userStyleRoot = Join-Path $runRoot "styles\user"
        New-Item -ItemType Directory -Path $userStyleRoot -Force | Out-Null
        Set-Content -LiteralPath (
            Join-Path $userStyleRoot "persistent-chat.disabled"
        ) -Value "disabled" -NoNewline
        $disabledInspection = & $cli session inspect $persistent.session_id --json |
            ConvertFrom-Json
        if ($disabledInspection.state.style_compatibility.status -ne "incompatible") {
            throw "disabled persisted style was not reported as incompatible"
        }
        & $cli run "must not silently substitute" --session $persistent.session_id `
            --option 'mock_scenario="streaming_text"' --json 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) {
            throw "disabled persisted style executed through a fallback"
        }
        Remove-Item -LiteralPath (
            Join-Path $userStyleRoot "persistent-chat.disabled"
        )

        foreach ($sessionId in @($persistent.session_id, $ephemeral.session_id)) {
            $sessionRoot = Join-Path $runRoot ("sessions\" + $sessionId)
            $metadata = Get-Content (Join-Path $sessionRoot "metadata.json") |
                ConvertFrom-Json
            $styleLock = Get-Content (Join-Path $sessionRoot "style.lock") |
                ConvertFrom-Json
            if ($metadata.schema_version -ne 2 -or
                $null -eq $metadata.style_binding -or
                $null -eq $styleLock.binding -or
                $null -eq $styleLock.compiled) {
                throw "complete style identity was not durably persisted"
            }
        }

        Write-Output "runtime session-style registry/restart/branch E2E passed"
    }
    finally {
        Stop-TestRuntime $daemon
        $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
        $resolvedRun = (Resolve-Path $runRoot).Path
        if ($resolvedRun.StartsWith($resolvedTemp) -and
            (Split-Path $resolvedRun -Leaf).StartsWith("agentmod-style-e2e-")) {
            Remove-Item -LiteralPath $resolvedRun -Recurse -Force
        }
    }
}
finally {
    Pop-Location
}
