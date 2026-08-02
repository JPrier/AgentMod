$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$runRoot = $null
$runtimeProcess = $null
$succeeded = $false

function Stop-TestRuntime {
    if ($null -ne $script:runtimeProcess -and
        -not $script:runtimeProcess.HasExited) {
        Stop-Process -Id $script:runtimeProcess.Id -Force
        $script:runtimeProcess.WaitForExit()
    }
    $script:runtimeProcess = $null
}

function Start-TestRuntime {
    param([string]$LogStem)
    $script:runtimeProcess = Start-Process -FilePath $script:runtime `
        -ArgumentList "serve" -WorkingDirectory $script:runRoot `
        -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $script:runRoot "$LogStem.out.log") `
        -RedirectStandardError (Join-Path $script:runRoot "$LogStem.err.log")
    for ($attempt = 0; $attempt -lt 100; $attempt++) {
        & $script:cli doctor --json 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) { return }
        if ($script:runtimeProcess.HasExited) {
            throw "runtime stopped before becoming ready"
        }
        Start-Sleep -Milliseconds 100
    }
    throw "runtime did not become ready"
}

Push-Location $repository
try {
    cargo build --locked -p agentmod-runtime -p agentmod-harness `
        -p agentmod-cli -p agentmod-tui
    if ($LASTEXITCODE -ne 0) { throw "build failed" }
    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $tui = (Resolve-Path "target\debug\agentmod-tui.exe").Path
    $python = (Get-Command python -ErrorAction Stop).Source
    $driver = Join-Path $repository `
        "tests\e2e\runtime_tui_rich_attachments_driver.py"
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-tui-rich-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null
    $image = Join-Path $workspace "pixel.png"
    $blob = Join-Path $workspace "evidence.bin"
    $outside = Join-Path $runRoot "outside.bin"
    [System.IO.File]::WriteAllBytes($image, [Convert]::FromBase64String(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
    ))
    [System.IO.File]::WriteAllBytes(
        $blob, [Text.Encoding]::UTF8.GetBytes("tui-rich-blob-evidence")
    )
    [System.IO.File]::WriteAllBytes(
        $outside, [Text.Encoding]::UTF8.GetBytes("outside-workspace")
    )
    $state = Join-Path $runRoot "rich-state.json"

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-tui-rich-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_SCHEDULER_POLL_MS = "0"

    Start-TestRuntime "runtime-initial"
    $created = & $cli session create --workspace $workspace `
        --style persistent-chat --json | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw "session creation failed" }
    & $tui --smoke-session-command $created.session_id `
        "/attach ../outside.bin" 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { throw "TUI accepted a workspace escape" }

    $turn = & $tui --smoke-attachment-turn `
        "inspect the TUI image and blob" "pixel.png" "evidence.bin"
    if ($LASTEXITCODE -ne 0 -or $turn -notmatch "attachments=2" -or
        $turn -notmatch "pending_after_submit=0" -or
        $turn -notmatch "deterministic response" -or
        $turn -notmatch "turn committed") {
        throw "TUI rich turn failed: $turn"
    }
    & $python $driver --phase execute --root $runRoot `
        --session $created.session_id --image $image --blob $blob `
        --state $state --cli $cli --tui $tui
    if ($LASTEXITCODE -ne 0) { throw "canonical rich-envelope proof failed" }

    Stop-TestRuntime
    Start-TestRuntime "runtime-restarted"
    & $python $driver --phase replay --root $runRoot `
        --session $created.session_id --image $image --blob $blob `
        --state $state --cli $cli --tui $tui
    if ($LASTEXITCODE -ne 0) { throw "restart/replay proof failed" }
    $succeeded = $true
    Write-Output "runtime TUI rich-attachment/restart E2E passed"
}
finally {
    Stop-TestRuntime
    foreach ($name in @(
        "AGENTMOD_RUNTIME_ENDPOINT", "AGENTMOD_RUNTIME_AUTH_TOKEN",
        "AGENTMOD_HARNESS_PROGRAM", "AGENTMOD_SCHEDULER_POLL_MS"
    )) {
        Remove-Item "Env:$name" -ErrorAction SilentlyContinue
    }
    if (-not $succeeded -and $null -ne $runRoot -and
        (Test-Path -LiteralPath $runRoot)) {
        Get-ChildItem -LiteralPath $runRoot -Filter "*.log" `
            -ErrorAction SilentlyContinue | ForEach-Object {
                Write-Error ("--- " + $_.Name + " ---`n" +
                    (Get-Content -LiteralPath $_.FullName -Raw)) `
                    -ErrorAction Continue
            }
    }
    Pop-Location
    if ($null -ne $runRoot -and (Test-Path -LiteralPath $runRoot)) {
        $resolved = [System.IO.Path]::GetFullPath($runRoot)
        $temp = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
        if (-not $resolved.StartsWith($temp) -or
            -not (Split-Path $resolved -Leaf).StartsWith(
                "agentmod-tui-rich-e2e-"
            )) {
            throw "refusing to remove non-AgentMod temporary path"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}
