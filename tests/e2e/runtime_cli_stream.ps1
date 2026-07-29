$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-cli-stream-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-cli-stream-e2e-" +
        [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_HARNESS_FRAME_PACING_MS = "500"

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

        $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
        $startInfo.FileName = $cli
        $startInfo.WorkingDirectory = $workspace
        $startInfo.UseShellExecute = $false
        $startInfo.CreateNoWindow = $true
        $startInfo.RedirectStandardOutput = $true
        $startInfo.RedirectStandardError = $true
        foreach ($argument in @(
            "run",
            "emit committed frames",
            "--session",
            $created.session_id,
            "--option",
            'mock_scenario="streaming_text"',
            "--option",
            'mock_text="live-cli"',
            "--stream-json"
        )) {
            $startInfo.ArgumentList.Add($argument)
        }
        $turn = [System.Diagnostics.Process]::new()
        $turn.StartInfo = $startInfo
        if (-not $turn.Start()) { throw "streaming CLI did not start" }

        $firstTask = $turn.StandardOutput.ReadLineAsync()
        if (-not $firstTask.Wait(5000)) {
            $turn.Kill($true)
            throw "first stream item was not emitted promptly"
        }
        $firstLine = $firstTask.Result
        if ([string]::IsNullOrWhiteSpace($firstLine)) {
            throw "first stream item was empty"
        }
        $first = $firstLine | ConvertFrom-Json
        if ($first.command -ne "run_event" -or
            $first.event.event -ne "started") {
            throw "first item was not the committed provider-start event"
        }
        if ($turn.HasExited) {
            throw "CLI buffered output until the turn completed"
        }

        $items = @($first)
        while ($true) {
            $line = $turn.StandardOutput.ReadLine()
            if ($null -eq $line) { break }
            if (-not [string]::IsNullOrWhiteSpace($line)) {
                $items += ($line | ConvertFrom-Json)
            }
        }
        if (-not $turn.WaitForExit(30000)) {
            $turn.Kill($true)
            throw "streaming CLI did not terminate"
        }
        if ($turn.ExitCode -ne 0) {
            throw "streaming CLI failed: $($turn.StandardError.ReadToEnd())"
        }

        $events = @($items | Where-Object command -eq "run_event")
        $terminal = @($items | Where-Object command -eq "run_complete")
        if ($terminal.Count -ne 1 -or
            $items[-1].command -ne "run_complete") {
            throw "stream did not end with exactly one terminal item"
        }
        for ($index = 1; $index -lt $events.Count; $index++) {
            if ($events[$index].committed_sequence -le
                $events[$index - 1].committed_sequence) {
                throw "committed stream sequences were not strictly increasing"
            }
        }
        $visible = ($events | Where-Object {
            $_.event.event -eq "text"
        } | ForEach-Object { $_.event.text }) -join ""
        if ($visible -ne "alpha beta live-cli") {
            throw "unexpected streamed provider output: $visible"
        }
        if ($terminal[0].last_committed_sequence -ne 19) {
            throw "terminal item did not report the full committed turn"
        }
        Write-Output "runtime live CLI streaming JSON E2E passed"
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
                    "agentmod-cli-stream-e2e-"
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
