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
        "agentmod-branch-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot | Out-Null
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path $workspace | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-branch-e2e-" + [guid]::NewGuid().ToString("N")
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
        $turn = & $cli run "parent prompt" --session $parent `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="parent answer"' --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "parent turn failed" }
        $fork = [uint64]$turn.last_committed_sequence

        $head = & $cli session inspect $parent --json | ConvertFrom-Json
        $replayed = & $cli session replay $parent --at 1 --json | ConvertFrom-Json
        if ($replayed.inspected_sequence -ne 1 -or
            $replayed.state.conversation.history.Count -ne 0) {
            throw "point-in-time replay did not reconstruct the initial state"
        }
        $branched = & $cli session branch $parent --at $fork `
            --style ephemeral-turn --json | ConvertFrom-Json
        if ($branched.parent_session_id -ne $parent -or
            $branched.fork_sequence -ne $fork) {
            throw "branch ancestry response is invalid"
        }
        $child = $branched.session_id
        $childBefore = & $cli session inspect $child --json | ConvertFrom-Json
        if ($childBefore.state.ancestry.parent_session_id -ne $parent -or
            $childBefore.state.ancestry.fork_sequence -ne $fork -or
            $childBefore.state.style -ne "ephemeral-turn") {
            throw "child replay did not preserve ancestry/style"
        }
        $parentHistory = $head.state.conversation.history | ConvertTo-Json -Depth 30 -Compress
        $childHistory = $childBefore.state.conversation.history |
            ConvertTo-Json -Depth 30 -Compress
        if ($parentHistory -ne $childHistory) {
            throw "child canonical conversation did not match the fork point"
        }

        & $cli run "child prompt" --session $child `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="child answer"' --json | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "child continuation failed" }
        $parentAfter = & $cli session inspect $parent --json | ConvertFrom-Json
        $childAfter = & $cli session inspect $child --json | ConvertFrom-Json
        if ($parentAfter.head_sequence -ne $head.head_sequence) {
            throw "child continuation mutated the parent journal"
        }
        $parentText = $parentAfter.state.conversation.history |
            ConvertTo-Json -Depth 30 -Compress
        $childText = $childAfter.state.conversation.history |
            ConvertTo-Json -Depth 30 -Compress
        if ($parentText -match "child answer" -or $childText -notmatch "child answer") {
            throw "branch continuation was not isolated"
        }
        Write-Output "runtime replay/branch/continue E2E passed"
    }
    finally {
        if ($null -ne $daemon -and -not $daemon.HasExited) {
            Stop-Process -Id $daemon.Id -Force
        }
        $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
        $resolvedRun = (Resolve-Path $runRoot).Path
        if ($resolvedRun.StartsWith($resolvedTemp) -and
            (Split-Path $resolvedRun -Leaf).StartsWith("agentmod-branch-e2e-")) {
            Remove-Item -LiteralPath $resolvedRun -Recurse -Force
        }
    }
}
finally {
    Pop-Location
}
