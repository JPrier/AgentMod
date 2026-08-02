$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$runRoot = $null
$runtimeProcess = $null
$fixtureProcess = $null
$succeeded = $false
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
    $fixture = Join-Path $repository "tests\fixtures\mcp_http_sse_server.py"
    $driver = Join-Path $repository "tests\e2e\runtime_acp_mcp_http_sse_driver.py"
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-acp-mcp-http-sse-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot -Force | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-acp-mcp-http-sse-e2e-" +
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
    $runtimeLog = Join-Path $runRoot "runtime.log"
    $runtimeProcess = Start-Process -FilePath $runtime -ArgumentList "serve" `
        -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput $runtimeLog -RedirectStandardError (
            Join-Path $runRoot "runtime.err.log"
        )
    $ready = $false
    for ($attempt = 0; $attempt -lt 100; $attempt++) {
        & $cli doctor --json 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) {
            $ready = $true
            break
        }
        Start-Sleep -Milliseconds 100
    }
    if (-not $ready) { throw "runtime did not become ready" }

    foreach ($mode in @("streamable_http", "legacy_sse")) {
        $portFile = Join-Path $runRoot "$mode.port"
        $auditFile = Join-Path $runRoot "$mode.audit.jsonl"
        $fixtureError = Join-Path $runRoot "$mode.fixture.err.log"
        $fixtureProcess = Start-Process -FilePath $python -ArgumentList @(
            $fixture, "--mode", $mode, "--port-file", $portFile,
            "--audit-file", $auditFile
        ) -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput (Join-Path $runRoot "$mode.fixture.out.log") `
            -RedirectStandardError $fixtureError
        for ($attempt = 0; $attempt -lt 100; $attempt++) {
            if (Test-Path -LiteralPath $portFile) { break }
            if ($fixtureProcess.HasExited) {
                throw "$mode fixture stopped: $(Get-Content $fixtureError -Raw)"
            }
            Start-Sleep -Milliseconds 50
        }
        if (-not (Test-Path -LiteralPath $portFile)) {
            throw "$mode fixture did not publish a port"
        }
        $port = (Get-Content -LiteralPath $portFile -Raw).Trim()
        & $python $driver --acp $acp --root $runRoot --mode $mode `
            --origin "http://127.0.0.1:$port" --audit-file $auditFile
        if ($LASTEXITCODE -ne 0) {
            throw "$mode ACP MCP process proof failed"
        }
        Stop-Process -Id $fixtureProcess.Id -Force
        $fixtureProcess.WaitForExit()
        $fixtureProcess = $null
    }
    $succeeded = $true
    Write-Output "runtime ACP Streamable HTTP and legacy SSE MCP E2E passed"
}
finally {
    if ($null -ne $fixtureProcess -and -not $fixtureProcess.HasExited) {
        Stop-Process -Id $fixtureProcess.Id -Force
        $fixtureProcess.WaitForExit()
    }
    if ($null -ne $runtimeProcess -and -not $runtimeProcess.HasExited) {
        Stop-Process -Id $runtimeProcess.Id -Force
        $runtimeProcess.WaitForExit()
    }
    foreach ($name in @(
        "AGENTMOD_RUNTIME_ENDPOINT", "AGENTMOD_RUNTIME_AUTH_TOKEN",
        "AGENTMOD_HARNESS_PROGRAM", "AGENTMOD_MCP_HOST_PROGRAM",
        "AGENTMOD_PERMISSION_MODE", "AGENTMOD_SCHEDULER_POLL_MS",
        "AGENTMOD_ACP_PROVIDER_OPTIONS", "AGENTMOD_MCP_SERVERS_JSON"
    )) {
        Remove-Item "Env:$name" -ErrorAction SilentlyContinue
    }
    if (-not $succeeded -and $null -ne $runRoot) {
        foreach ($log in Get-ChildItem -LiteralPath $runRoot -Filter "*.log" `
            -ErrorAction SilentlyContinue) {
            Write-Error ("--- " + $log.Name + " ---`n" +
                (Get-Content -LiteralPath $log.FullName -Raw)) `
                -ErrorAction Continue
        }
        foreach ($audit in Get-ChildItem -LiteralPath $runRoot `
            -Filter "*.audit.jsonl" -ErrorAction SilentlyContinue) {
            Write-Error ("--- " + $audit.Name + " ---`n" +
                (Get-Content -LiteralPath $audit.FullName -Raw)) `
                -ErrorAction Continue
        }
    }
    Pop-Location
    if ($null -ne $runRoot -and (Test-Path -LiteralPath $runRoot)) {
        $resolved = [System.IO.Path]::GetFullPath($runRoot)
        $temp = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
        if (-not $resolved.StartsWith($temp) -or
            -not (Split-Path $resolved -Leaf).StartsWith(
                "agentmod-acp-mcp-http-sse-e2e-"
            )) {
            throw "refusing to remove non-AgentMod temporary path"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}
