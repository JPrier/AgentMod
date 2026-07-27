$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-filesystem-host -p agentmod-process-host -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-coding-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path (Join-Path $workspace "src") -Force | Out-Null
    Set-Content -LiteralPath (Join-Path $workspace "Cargo.toml") -Value @(
        "[package]",
        'name = "coding-fixture"',
        'version = "0.1.0"',
        'edition = "2024"'
    )
    Set-Content -LiteralPath (Join-Path $workspace "src\lib.rs") -Value @(
        "pub fn add(left: i32, right: i32) -> i32 {",
        "    left + right + 1",
        "}",
        "",
        "#[cfg(test)]",
        "mod tests {",
        "    use super::add;",
        "    #[test]",
        "    fn adds_two_numbers() {",
        "        assert_eq!(add(2, 3), 5);",
        "    }",
        "}"
    )

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $filesystem = (Resolve-Path "target\debug\agentmod-filesystem-host.exe").Path
    $processHost = (Resolve-Path "target\debug\agentmod-process-host.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-coding-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_FILESYSTEM_HOST_PROGRAM = $filesystem
    $env:AGENTMOD_PROCESS_HOST_PROGRAM = $processHost
    $env:AGENTMOD_PROCESS_ALLOWED_EXECUTABLES = "cargo"
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
        $turn = & $cli run "fix add and verify it" `
            --session $created.session_id `
            --option 'mock_scenario="coding_task"' --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "coding turn failed" }
        $visible = ($turn.events | Where-Object event -eq "text" |
            ForEach-Object text) -join ""
        if ($visible -ne "implemented the fix and verified the tests pass") {
            throw "unexpected final response: $visible"
        }
        $source = Get-Content (Join-Path $workspace "src\lib.rs") -Raw
        if ($source -notmatch "left \+ right(\r?\n|\s*})" -or
            $source -match "left \+ right \+ [12]") {
            throw "workspace did not contain the final fix"
        }

        $journalPath = Join-Path $runRoot (
            "sessions\" + $created.session_id + "\events.jsonl"
        )
        $events = @(Get-Content $journalPath | ForEach-Object {
            ($_ | ConvertFrom-Json).event
        })
        $toolProposals = @($events | Where-Object {
            $_.metadata.event_type -eq "tool.call_proposed"
        })
        if ($toolProposals.Count -ne 5) {
            throw "expected five intercepted tool proposals"
        }
        $processCompletions = @($events | Where-Object {
            $_.metadata.event_type -eq "tool.execution_completed" -and
            $_.payload.payload.call_id -like "coding-test-*"
        })
        if ($processCompletions.Count -ne 2) {
            throw "expected failing and passing test executions"
        }
        $failed = $processCompletions | Where-Object {
            $_.payload.payload.call_id -eq "coding-test-failing"
        }
        $passed = $processCompletions | Where-Object {
            $_.payload.payload.call_id -eq "coding-test-passing"
        }
        if ($failed.payload.payload.result.exit.success -ne $false -or
            $passed.payload.payload.result.exit.success -ne $true) {
            $details = @($events | Where-Object {
                $_.metadata.event_type -eq "tool.output_observed" -or
                ($_.metadata.event_type -eq "tool.execution_completed" -and
                    $_.payload.payload.call_id -like "coding-test-*")
            }) | ConvertTo-Json -Depth 30
            throw "test failure/pass transition was not recorded`n$details"
        }
        & cargo test --quiet --manifest-path (Join-Path $workspace "Cargo.toml")
        if ($LASTEXITCODE -ne 0) { throw "final independent test failed" }
        Write-Output "complete coding-task E2E passed"
    }
    finally {
        if ($null -ne $daemon -and -not $daemon.HasExited) {
            Stop-Process -Id $daemon.Id -Force
        }
        $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
        $resolvedRun = (Resolve-Path $runRoot).Path
        if ($resolvedRun.StartsWith($resolvedTemp) -and
            (Split-Path $resolvedRun -Leaf).StartsWith("agentmod-coding-e2e-")) {
            Remove-Item -LiteralPath $resolvedRun -Recurse -Force
        }
    }
}
finally {
    Pop-Location
}
