$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$runRoot = $null
$daemon = $null
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-cli -p agentmod-tui
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $tui = (Resolve-Path "target\debug\agentmod-tui.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-tui-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot -Force | Out-Null

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-tui-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_SCHEDULER_POLL_MS = "0"
    $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
        -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru

    for ($attempt = 0; $attempt -lt 50; $attempt++) {
        try {
            & $cli doctor --json 2>$null | Out-Null
            if ($LASTEXITCODE -eq 0) { break }
        } catch {}
        Start-Sleep -Milliseconds 100
    }
    if ($LASTEXITCODE -ne 0) { throw "runtime did not become ready" }

    $created = & $cli session create --workspace $runRoot `
        --style persistent-chat --json | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw "session creation failed" }
    $smoke = & $tui --smoke
    if ($LASTEXITCODE -ne 0) { throw "TUI smoke bootstrap failed" }
    if ($smoke -notmatch "ready=true" -or $smoke -notmatch "sessions=1") {
        throw "TUI did not map runtime health and sessions: $smoke"
    }
    $turn = & $tui --smoke-turn "verify committed TUI streaming"
    if ($LASTEXITCODE -ne 0) { throw "TUI streamed turn failed" }
    if ($turn -notmatch "deterministic response" -or
        $turn -notmatch "turn committed") {
        throw "TUI did not map the committed runtime stream: $turn"
    }
    $journalPath = Join-Path $runRoot (
        "sessions\" + $created.session_id + "\events.jsonl"
    )
    $events = @(Get-Content -LiteralPath $journalPath | ForEach-Object {
        ($_ | ConvertFrom-Json).event
    })
    if (@($events | Where-Object {
        $_.metadata.event_type -eq "model.response_completed"
    }).Count -ne 1) {
        throw "TUI turn did not commit one provider completion"
    }
    if (@($events | Where-Object {
        $_.metadata.event_type -eq "conversation.entry_committed"
    }).Count -lt 2) {
        throw "TUI turn did not commit canonical conversation entries"
    }
    $parentBeforeBranch = & $cli session inspect $created.session_id --json |
        ConvertFrom-Json
    $branchSmoke = & $tui --smoke-command "/branch 1 ephemeral-turn"
    if ($LASTEXITCODE -ne 0 -or
        $branchSmoke -notmatch "branched ([0-9a-f-]+) from") {
        throw "TUI branch command failed: $branchSmoke"
    }
    $branchId = $Matches[1]
    $branchInspection = & $cli session inspect $branchId --json | ConvertFrom-Json
    $parentAfterBranch = & $cli session inspect $created.session_id --json |
        ConvertFrom-Json
    if ($branchInspection.state.style_binding.id -ne "ephemeral-turn" -or
        $branchInspection.state.ancestry.parent_session_id -ne
            $created.session_id -or
        $parentAfterBranch.head_sequence -ne $parentBeforeBranch.head_sequence -or
        $parentAfterBranch.state.style_binding.id -ne "persistent-chat") {
        throw "TUI branch did not preserve the parent and select the child style"
    }
    $componentSmoke = & $tui --smoke-command (
        "/new . ephemeral-turn native sqlite-fts sliding_window"
    )
    if ($LASTEXITCODE -ne 0 -or
        $componentSmoke -notmatch "selected=([0-9a-f-]+)") {
        throw "TUI component-selected creation failed: $componentSmoke"
    }
    $componentSession = & $cli session inspect $Matches[1] --json |
        ConvertFrom-Json
    if ($componentSession.state.style_binding.memory.provider -ne "sqlite-fts" -or
        $componentSession.state.style_binding.compaction.strategy -ne
            "sliding_window") {
        throw "TUI component selections did not reach the immutable binding"
    }
    $cliSelected = & $cli session create --workspace $runRoot `
        --style ephemeral-turn --memory file `
        --compaction tool_output_eviction --json | ConvertFrom-Json
    $cliInspection = & $cli session inspect $cliSelected.session_id --json |
        ConvertFrom-Json
    if ($cliInspection.state.style_binding.memory.provider -ne "file" -or
        $cliInspection.state.style_binding.compaction.strategy -ne
            "tool_output_eviction") {
        throw "CLI component selections did not reach the immutable binding"
    }
    Write-Output "runtime TUI smoke/branch/component-selection E2E passed"
}
finally {
    if ($null -ne $daemon -and -not $daemon.HasExited) {
        Stop-Process -Id $daemon.Id -Force
        $daemon.WaitForExit()
    }
    Pop-Location
    if ($null -ne $runRoot -and (Test-Path -LiteralPath $runRoot)) {
        $resolved = [System.IO.Path]::GetFullPath($runRoot)
        $temp = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
        if (-not $resolved.StartsWith($temp)) {
            throw "refusing to remove non-temporary path"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}
