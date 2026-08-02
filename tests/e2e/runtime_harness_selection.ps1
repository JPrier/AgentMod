$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build --locked -p agentmod-runtime -p agentmod-harness `
        -p agentmod-independent-harness-fixture -p agentmod-scheduler `
        -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }
    $dependencyTree = cargo tree --locked `
        -p agentmod-independent-harness-fixture --edges normal --prefix none
    if ($LASTEXITCODE -ne 0 -or
        ($dependencyTree -join "`n") -match
            "agentmod-harness-(service|logic|data|dependency)") {
        throw "independent harness fixture depends on native harness internals"
    }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $independentHarness = (
        Resolve-Path "target\debug\agentmod-independent-harness-fixture.exe"
    ).Path
    $scheduler = (Resolve-Path "target\debug\agentmod-scheduler.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-harness-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    $userStyles = Join-Path $runRoot "styles\user"
    New-Item -ItemType Directory -Path $workspace, $userStyles -Force | Out-Null
    Copy-Item -LiteralPath (
        Join-Path $repository "tests\fixtures\styles\fixture-harness-chat.toml"
    ) -Destination $userStyles

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-harness-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_FIXTURE_HARNESS_PROGRAM = $independentHarness
    $env:AGENTMOD_SCHEDULER_PROGRAM = $scheduler
    $daemon = $null

    function Start-TestRuntime {
        $process = Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru
        for ($attempt = 0; $attempt -lt 100; $attempt++) {
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

    $daemon = Start-TestRuntime
    try {
        $catalog = & $cli harness list --json | ConvertFrom-Json
        foreach ($id in @("native", "fixture")) {
            $entry = @($catalog.harnesses | Where-Object id -eq $id)
            if ($entry.Count -ne 1 -or $entry[0].availability -ne "available") {
                throw "missing available harness: $id"
            }
        }
        $fixtureDescriptor = & $cli harness inspect fixture --json | ConvertFrom-Json
        if (@($fixtureDescriptor.harnesses[0].capabilities) -contains "images" -or
            @($fixtureDescriptor.harnesses[0].capabilities) -notcontains "tool_calls") {
            throw "fixture harness capability fixture is incorrect"
        }

        $native = & $cli session create --workspace $workspace `
            --style persistent-chat --harness native --json | ConvertFrom-Json
        $fixture = & $cli session create --workspace $workspace `
            --style persistent-chat --harness fixture --json | ConvertFrom-Json

        $savedErrorPreference = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            & $cli session create --workspace $workspace `
                --style fixture-harness-incompatible --json 2>$null | Out-Null
            $incompatibleExit = $LASTEXITCODE
        }
        finally {
            $ErrorActionPreference = $savedErrorPreference
        }
        if ($incompatibleExit -eq 0) {
            throw "incompatible style/harness combination was accepted"
        }

        foreach ($session in @(
            @($native.session_id, "native", "native-ok", "native-ok"),
            @(
                $fixture.session_id,
                "fixture",
                "fixture-ok",
                "independent-harness:fixture-ok"
            )
        )) {
            $inspection = & $cli session inspect $session[0] --json | ConvertFrom-Json
            if ($inspection.state.style_binding.harness -ne $session[1] -or
                $inspection.state.style_binding.harness_version -ne "1.0.0" -or
                [string]::IsNullOrWhiteSpace(
                    $inspection.state.style_binding.harness_capability_set_hash
                )) {
                throw "session did not persist complete harness identity"
            }
            & $cli run "run through $($session[1])" --session $session[0] `
                --option 'mock_scenario="streaming_text"' `
                --option ('mock_text="' + $session[2] + '"') --json | Out-Null
            if ($LASTEXITCODE -ne 0) { throw "compatible harness turn failed" }
            $journal = Get-Content -LiteralPath (
                Join-Path $runRoot ("sessions\" + $session[0] + "\events.jsonl")
            ) -Raw
            if ($journal -notmatch ('"harness":"' + $session[1] + '"')) {
                throw "canonical model request did not retain harness identity"
            }
            if (-not $journal.Contains($session[3])) {
                throw (
                    "selected $($session[1]) harness did not emit " +
                    "expected output $($session[3])"
                )
            }
        }

        Stop-TestRuntime $daemon
        $daemon = Start-TestRuntime
        $recovered = & $cli session inspect $fixture.session_id --json | ConvertFrom-Json
        if ($recovered.state.style_binding.harness -ne "fixture" -or
            $recovered.state.style_compatibility.status -ne "compatible") {
            throw "fixture harness binding did not survive restart"
        }
        & $cli run "fixture after restart" --session $fixture.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="fixture-restarted"' --json | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "fixture harness did not resume after restart" }
        $restartedJournal = Get-Content -LiteralPath (
            Join-Path $runRoot (
                "sessions\" + $fixture.session_id + "\events.jsonl"
            )
        ) -Raw
        if (-not $restartedJournal.Contains(
            "independent-harness:fixture-restarted"
        )) {
            throw "independent fixture output was not retained after restart"
        }
        Write-Output (
            "runtime independent-harness selection/capability/restart E2E passed"
        )
    }
    finally {
        Stop-TestRuntime $daemon
        if (Test-Path -LiteralPath $runRoot) {
            $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
            $resolvedRun = (Resolve-Path $runRoot).Path
            if ($resolvedRun.StartsWith($resolvedTemp) -and
                (Split-Path $resolvedRun -Leaf).StartsWith("agentmod-harness-e2e-")) {
                Remove-Item -LiteralPath $resolvedRun -Recurse -Force
            }
        }
    }
}
finally {
    Remove-Item Env:AGENTMOD_HARNESS_PROGRAM -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_FIXTURE_HARNESS_PROGRAM `
        -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_SCHEDULER_PROGRAM -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_RUNTIME_ENDPOINT -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_RUNTIME_AUTH_TOKEN -ErrorAction SilentlyContinue
    Pop-Location
}
$global:LASTEXITCODE = 0
