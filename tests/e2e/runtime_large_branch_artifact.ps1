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
        "agentmod-large-branch-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-large-branch-e2e-" +
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
        $parent = $created.session_id
        for ($turnIndex = 1; $turnIndex -le 17; $turnIndex++) {
            & $cli run "parent-$turnIndex" --session $parent `
                --option 'mock_scenario="streaming_text"' `
                --option "mock_text=`"answer-$turnIndex`"" --json | Out-Null
            if ($LASTEXITCODE -ne 0) { throw "parent turn $turnIndex failed" }
        }
        $parentBefore = & $cli session inspect $parent --json | ConvertFrom-Json
        $parentHistoryCount = @(
            $parentBefore.state.conversation.history
        ).Count
        if ($parentHistoryCount -le 32) {
            throw "fixture did not exceed the inline branch bound"
        }
        $branched = & $cli session branch $parent `
            --at ([uint64]$parentBefore.head_sequence) --json | ConvertFrom-Json
        $child = $branched.session_id
        $childBefore = & $cli session inspect $child --json | ConvertFrom-Json
        $childHistory = @($childBefore.state.conversation.history)
        if ($childHistory.Count -ge $parentHistoryCount -or
            $childHistory.Count -gt 17) {
            throw "child journal was not bounded"
        }
        $artifactEntry = @($childHistory | Where-Object {
            $_.kind -eq "artifact_reference"
        })
        if ($artifactEntry.Count -ne 1 -or
            $childBefore.state.conversation.projection_provenance.method -ne
                "branch_artifact_handoff") {
            throw "child lacks an explicit artifact handoff"
        }
        $artifactId = $artifactEntry[0].content.artifact_id
        $artifactPath = Join-Path $runRoot (
            "sessions\" + $child + "\artifacts\" + $artifactId + ".json"
        )
        if (-not (Test-Path -LiteralPath $artifactPath)) {
            throw "branch context artifact is missing"
        }
        $snapshot = Get-Content -LiteralPath $artifactPath -Raw |
            ConvertFrom-Json
        if (@($snapshot.history).Count -ne $parentHistoryCount -or
            $snapshot.parent_session_id -ne $parent) {
            throw "artifact does not preserve complete parent context"
        }

        & $cli run "child-after-artifact" --session $child `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="bounded-child-ok"' --json | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "artifact-backed child could not continue"
        }
        $parentAfter = & $cli session inspect $parent --json | ConvertFrom-Json
        if ($parentAfter.head_sequence -ne $parentBefore.head_sequence) {
            throw "artifact-backed child mutated its parent"
        }
        Write-Output "runtime artifact-backed bounded branch E2E passed"
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
                    "agentmod-large-branch-e2e-"
                )) {
                Remove-Item -LiteralPath $resolvedRun -Recurse -Force
            }
        }
    }
}
finally {
    Pop-Location
}
