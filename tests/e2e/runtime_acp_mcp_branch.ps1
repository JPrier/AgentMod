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
        -p agentmod-acp -p agentmod-mcp-host -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }
    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $acp = (Resolve-Path "target\debug\agentmod-acp.exe").Path
    $mcpHost = (Resolve-Path "target\debug\agentmod-mcp-host.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $python = (Get-Command python -ErrorAction Stop).Source
    $driver = Join-Path $repository "tests\e2e\runtime_acp_mcp_branch_driver.py"
    $fixtureSource = Join-Path $repository "tests\fixtures\mcp_stdio_server.rs"
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-acp-mcp-branch-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot -Force | Out-Null
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null
    $fixture = Join-Path $runRoot "mcp-stdio-fixture.exe"
    rustc $fixtureSource --edition=2024 -o $fixture
    if ($LASTEXITCODE -ne 0) { throw "fixture build failed" }
    $auditFile = Join-Path $runRoot "mcp-effects.jsonl"
    $stateFile = Join-Path $runRoot "branch-state.json"

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-acp-mcp-branch-e2e-" +
        [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_MCP_HOST_PROGRAM = $mcpHost
    $env:AGENTMOD_PERMISSION_MODE = "allow"
    $env:AGENTMOD_SCHEDULER_POLL_MS = "0"
    $env:AGENTMOD_ACP_PROVIDER_OPTIONS = '{"mock_scenario":"mcp_fixture_call"}'
    Remove-Item Env:AGENTMOD_MCP_SERVERS_JSON -ErrorAction SilentlyContinue

    Start-TestRuntime "runtime-initial"
    & $python $driver --phase execute --acp $acp --cli $cli `
        --fixture $fixture --root $runRoot --workspace $workspace `
        --audit-file $auditFile --state-file $stateFile
    if ($LASTEXITCODE -ne 0) { throw "branch execution proof failed" }

    Stop-TestRuntime
    Start-TestRuntime "runtime-restarted"
    & $python $driver --phase replay --acp $acp --cli $cli `
        --fixture $fixture --root $runRoot --workspace $workspace `
        --audit-file $auditFile --state-file $stateFile
    if ($LASTEXITCODE -ne 0) { throw "branch restart/replay proof failed" }

    $succeeded = $true
    Write-Output "runtime ACP exact MCP branch bootstrap E2E passed"
}
finally {
    Stop-TestRuntime
    foreach ($name in @(
        "AGENTMOD_RUNTIME_ENDPOINT", "AGENTMOD_RUNTIME_AUTH_TOKEN",
        "AGENTMOD_HARNESS_PROGRAM", "AGENTMOD_MCP_HOST_PROGRAM",
        "AGENTMOD_PERMISSION_MODE", "AGENTMOD_SCHEDULER_POLL_MS",
        "AGENTMOD_ACP_PROVIDER_OPTIONS", "AGENTMOD_MCP_SERVERS_JSON"
    )) {
        Remove-Item "Env:$name" -ErrorAction SilentlyContinue
    }
    if (-not $succeeded -and $null -ne $runRoot -and
        (Test-Path -LiteralPath $runRoot)) {
        foreach ($log in Get-ChildItem -LiteralPath $runRoot -Filter "*.log" `
            -ErrorAction SilentlyContinue) {
            Write-Error ("--- " + $log.Name + " ---`n" +
                (Get-Content -LiteralPath $log.FullName -Raw)) `
                -ErrorAction Continue
        }
        if (Test-Path -LiteralPath (Join-Path $runRoot "mcp-effects.jsonl")) {
            Write-Error ("--- mcp-effects.jsonl ---`n" +
                (Get-Content -LiteralPath (
                    Join-Path $runRoot "mcp-effects.jsonl"
                ) -Raw)) -ErrorAction Continue
        }
    }
    Pop-Location
    if ($null -ne $runRoot -and (Test-Path -LiteralPath $runRoot)) {
        $resolved = [System.IO.Path]::GetFullPath($runRoot)
        $temp = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
        if (-not $resolved.StartsWith($temp) -or
            -not (Split-Path $resolved -Leaf).StartsWith(
                "agentmod-acp-mcp-branch-e2e-"
            )) {
            throw "refusing to remove non-AgentMod temporary path"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}
