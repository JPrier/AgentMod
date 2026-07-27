$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-stream-cancel-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-stream-cancel-e2e-" +
        [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_HARNESS_FRAME_PACING_MS = "750"

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
        $cancellationId = [guid]::NewGuid().ToString()
        $stdout = Join-Path $runRoot "turn.stdout"
        $stderr = Join-Path $runRoot "turn.stderr"
        $turn = Start-Process -FilePath $cli -ArgumentList @(
            "run",
            "stream-until-cancelled",
            "--session",
            $created.session_id,
            "--option",
            "mock_scenario=streaming_text",
            "--cancellation-id",
            $cancellationId,
            "--json"
        ) -WorkingDirectory $workspace -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput $stdout -RedirectStandardError $stderr

        $harnessStarted = $false
        for ($attempt = 0; $attempt -lt 50; $attempt++) {
            if ($turn.HasExited) {
                throw "turn exited before harness start: $(Get-Content $stderr -Raw)"
            }
            if (Get-Process agentmod-harness -ErrorAction SilentlyContinue) {
                $harnessStarted = $true
                break
            }
            Start-Sleep -Milliseconds 100
        }
        if (-not $harnessStarted) { throw "harness did not start" }
        Start-Sleep -Milliseconds 1100

        $cancelled = $false
        for ($attempt = 0; $attempt -lt 20; $attempt++) {
            try {
                $cancel = & $cli cancel $cancellationId `
                    --reason "E2E cancellation" --json 2>$null |
                    ConvertFrom-Json
                if ($LASTEXITCODE -eq 0 -and $cancel.cancelled) {
                    $cancelled = $true
                    break
                }
            } catch {}
            Start-Sleep -Milliseconds 100
        }
        if (-not $cancelled) { throw "active cancellation was not accepted" }

        if (-not $turn.WaitForExit(30000)) {
            Stop-Process -Id $turn.Id -Force
            throw "cancelled turn did not return"
        }
        $turn.WaitForExit()
        if ($null -ne $turn.ExitCode -and $turn.ExitCode -ne 0) {
            throw "cancelled turn failed with $($turn.ExitCode): stderr=$(Get-Content $stderr -Raw) stdout=$(Get-Content $stdout -Raw)"
        }
        $result = Get-Content $stdout -Raw | ConvertFrom-Json
        if (-not ($result.events | Where-Object event -eq "cancelled")) {
            throw "turn response omitted cancellation"
        }
        if (-not ($result.events | Where-Object event -eq "text")) {
            throw "turn response omitted partial visible output"
        }
        if ($result.events | Where-Object event -eq "completed") {
            throw "cancelled provider request completed"
        }

        $journalPath = Join-Path $runRoot (
            "sessions\" + $created.session_id + "\events.jsonl"
        )
        $events = @(Get-Content $journalPath | ForEach-Object {
            ($_ | ConvertFrom-Json).event
        })
        if (-not ($events | Where-Object {
            $_.metadata.event_type -eq "model.output_delta_observed"
        })) {
            throw "partial output was not committed"
        }
        if (-not ($events | Where-Object {
            $_.metadata.event_type -eq "model.request_cancelled"
        })) {
            throw "cancellation event was not committed"
        }
        if ($events | Where-Object {
            $_.metadata.event_type -eq "model.response_completed"
        }) {
            throw "cancelled request committed completion"
        }

        $next = & $cli run "new request after cancellation" `
            --session $created.session_id --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or
            -not ($next.events | Where-Object event -eq "completed")) {
            throw "fresh provider request did not recover after cancellation"
        }
        Write-Output "runtime incremental stream cancellation E2E passed"
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
                    "agentmod-stream-cancel-e2e-"
                )) {
                Remove-Item -LiteralPath $resolvedRun -Recurse -Force
            }
        }
    }
}
finally {
    Remove-Item Env:AGENTMOD_HARNESS_FRAME_PACING_MS `
        -ErrorAction SilentlyContinue
    Pop-Location
}
