$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-browser-host -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }
    $fixture = Join-Path $repository "target\webdriver-fixture.exe"
    rustc tests\fixtures\webdriver_server.rs --edition=2024 -o $fixture
    if ($LASTEXITCODE -ne 0) { throw "fixture build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $browserHost = (
        Resolve-Path "target\debug\agentmod-browser-host.exe"
    ).Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-browser-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    $readyFile = Join-Path $runRoot "webdriver.ready"
    $driverLog = Join-Path $runRoot "webdriver.log"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null

    $driver = Start-Process -FilePath $fixture `
        -ArgumentList @($readyFile, $driverLog) -WindowStyle Hidden -PassThru
    for ($attempt = 0; $attempt -lt 50 -and
        -not (Test-Path -LiteralPath $readyFile); $attempt++) {
        Start-Sleep -Milliseconds 100
    }
    if (-not (Test-Path -LiteralPath $readyFile)) {
        throw "WebDriver fixture did not become ready"
    }

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-browser-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_BROWSER_HOST_PROGRAM = $browserHost
    $env:AGENTMOD_BROWSER_WEBDRIVER_URL = (
        Get-Content -LiteralPath $readyFile -Raw
    )
    $env:AGENTMOD_BROWSER_NAME = "fixture"
    $env:AGENTMOD_BROWSER_ALLOW_LOOPBACK = "true"
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
        $turn = & $cli run "exercise the managed browser" `
            --session $created.session_id `
            --option 'mock_scenario="browser_fixture_flow"' --json |
            ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) {
            $debugJournal = Join-Path $runRoot (
                "sessions\" + $created.session_id + "\events.jsonl"
            )
            $details = if (Test-Path $debugJournal) {
                Get-Content $debugJournal -Raw
            } else {
                "journal unavailable"
            }
            $driverDetails = if (Test-Path $driverLog) {
                Get-Content $driverLog -Raw
            } else {
                "driver log unavailable"
            }
            throw "browser flow failed`n$driverDetails`n$details"
        }
        $visible = ($turn.events | Where-Object event -eq "text" |
            ForEach-Object text) -join ""
        if ($visible -ne "continued after approved runtime decision") {
            throw "unexpected provider continuation: $visible"
        }

        $sessionRoot = Join-Path $runRoot (
            "sessions\" + $created.session_id
        )
        $journal = @(Get-Content (Join-Path $sessionRoot "events.jsonl") |
            ForEach-Object { ($_ | ConvertFrom-Json).event })
        if (@($journal | Where-Object {
            $_.metadata.event_type -eq "tool.execution_completed"
        }).Count -ne 9) {
            throw "not every browser operation completed"
        }
        if (@($journal | Where-Object {
            $_.metadata.event_type -eq "tool.execution_failed"
        }).Count -ne 0) {
            throw "a browser operation failed"
        }
        $artifactRoot = Join-Path $sessionRoot "artifacts\browser"
        if (@(Get-ChildItem $artifactRoot -Filter "*.bin").Count -ne 2 -or
            @(Get-ChildItem $artifactRoot -Filter "*.metadata.json").Count -ne
            2) {
            throw "screenshot/download artifacts were not persisted"
        }
        if (@(Get-ChildItem (
            Join-Path $artifactRoot "authorization-replay"
        ) -Filter "*.used").Count -ne 9) {
            throw "browser grants were not consumed exactly once"
        }
        $requests = Get-Content -LiteralPath $driverLog
        foreach ($required in @(
            "POST /session",
            "GET /session/fixture-session/source",
            "GET /session/fixture-session/screenshot",
            "POST /session/fixture-session/execute/async",
            "DELETE /session/fixture-session"
        )) {
            if ($requests -notcontains $required) {
                throw "missing WebDriver operation: $required"
            }
        }
        Write-Output "managed browser runtime E2E passed"
    }
    finally {
        if ($null -ne $daemon -and -not $daemon.HasExited) {
            Stop-Process -Id $daemon.Id -Force
        }
        if ($null -ne $driver -and -not $driver.HasExited) {
            Stop-Process -Id $driver.Id -Force
        }
        foreach ($name in @(
            "AGENTMOD_BROWSER_HOST_PROGRAM",
            "AGENTMOD_BROWSER_WEBDRIVER_URL",
            "AGENTMOD_BROWSER_NAME",
            "AGENTMOD_BROWSER_ALLOW_LOOPBACK",
            "AGENTMOD_PERMISSION_MODE"
        )) {
            Remove-Item "Env:$name" -ErrorAction SilentlyContinue
        }
        if (Test-Path -LiteralPath $runRoot) {
            $resolvedTemp = (
                Resolve-Path ([System.IO.Path]::GetTempPath())
            ).Path
            $resolvedRun = (Resolve-Path $runRoot).Path
            if ($resolvedRun.StartsWith($resolvedTemp) -and
                (Split-Path $resolvedRun -Leaf).StartsWith(
                    "agentmod-browser-e2e-"
                )) {
                Remove-Item -LiteralPath $resolvedRun -Recurse -Force
            }
        }
    }
}
finally {
    Pop-Location
}
