$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$runRoot = $null
$daemon = $null
$acpProcess = $null
$harness = $null
$processHost = $null
$scheduler = $null
$testStartedAt = Get-Date
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-process-host -p agentmod-cli -p agentmod-acp
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $processHost = (Resolve-Path "target\debug\agentmod-process-host.exe").Path
    $scheduler = (Resolve-Path "target\debug\agentmod-scheduler.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $acp = (Resolve-Path "target\debug\agentmod-acp.exe").Path
    $shell = (Get-Process -Id $PID).Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-acp-tool-cancel-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-acp-tool-cancel-e2e-" +
        [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_PROCESS_HOST_PROGRAM = $processHost
    $env:AGENTMOD_PROCESS_ALLOWED_EXECUTABLES = $shell
    $env:AGENTMOD_PERMISSION_MODE = "allow"
    $env:AGENTMOD_SCHEDULER_POLL_MS = "0"
    $processArguments = @{
        executable = $shell
        arguments = @(
            "-NoProfile",
            "-Command",
            "Write-Output process-started; Start-Sleep -Seconds 30"
        )
        output_limit_bytes = 65536
        timeout_ms = 60000
        cleanup = "remove_logs_always"
    } | ConvertTo-Json -Compress -Depth 10
    $env:AGENTMOD_ACP_PROVIDER_OPTIONS = @{
        mock_scenario = "process_action"
        mock_process_tool = "process.run"
        mock_process_arguments = $processArguments
    } | ConvertTo-Json -Compress -Depth 10

    $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
        -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru
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

    $start = [System.Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $acp
    $start.WorkingDirectory = $runRoot
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $start.RedirectStandardInput = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $acpProcess = [System.Diagnostics.Process]::new()
    $acpProcess.StartInfo = $start
    if (-not $acpProcess.Start()) { throw "ACP process did not start" }

    function Send-Acp([hashtable]$message) {
        $acpProcess.StandardInput.WriteLine(
            ($message | ConvertTo-Json -Compress -Depth 20)
        )
        $acpProcess.StandardInput.Flush()
    }
    function Read-Acp {
        $line = $acpProcess.StandardOutput.ReadLine()
        if ($null -eq $line) {
            throw "ACP closed: " + $acpProcess.StandardError.ReadToEnd()
        }
        return $line | ConvertFrom-Json
    }

    Send-Acp @{
        jsonrpc = "2.0"; id = 1; method = "initialize"
        params = @{ protocolVersion = 1; clientCapabilities = @{} }
    }
    if ((Read-Acp).id -ne 1) { throw "ACP initialization failed" }
    Send-Acp @{
        jsonrpc = "2.0"; id = 2; method = "session/new"
        params = @{ cwd = $workspace; mcpServers = @() }
    }
    $sessionId = (Read-Acp).result.sessionId
    $baselineShells = @(Get-Process | Where-Object {
        $_.Path -eq $shell
    } | Select-Object -ExpandProperty Id)
    Send-Acp @{
        jsonrpc = "2.0"; id = 3; method = "session/prompt"
        params = @{
            sessionId = $sessionId
            prompt = @(@{ type = "text"; text = "cancel process tool" })
        }
    }
    $sawTool = $false
    for ($frame = 0; $frame -lt 20; $frame++) {
        $message = Read-Acp
        if ($message.method -eq "session/update" -and
            $message.params.update.sessionUpdate -eq "tool_call") {
            $sawTool = $true
            break
        }
    }
    if (-not $sawTool) { throw "ACP omitted process tool proposal" }
    $toolProcess = $null
    for ($attempt = 0; $attempt -lt 50; $attempt++) {
        $toolProcess = Get-Process -ErrorAction SilentlyContinue |
            Where-Object {
                $_.Path -eq $shell -and
                $_.Id -notin $baselineShells
            } |
            Select-Object -First 1
        if ($null -ne $toolProcess) { break }
        Start-Sleep -Milliseconds 100
    }
    if ($null -eq $toolProcess) { throw "process tool did not start" }
    Send-Acp @{
        jsonrpc = "2.0"; method = "session/cancel"
        params = @{ sessionId = $sessionId }
    }
    $stopReason = $null
    for ($frame = 0; $frame -lt 30; $frame++) {
        $message = Read-Acp
        if ($message.id -eq 3) {
            $stopReason = $message.result.stopReason
            break
        }
    }
    if ($stopReason -ne "cancelled") {
        throw "ACP process-tool cancellation returned $stopReason"
    }
    for ($attempt = 0; $attempt -lt 50; $attempt++) {
        if ($toolProcess.HasExited) { break }
        Start-Sleep -Milliseconds 100
        $toolProcess.Refresh()
    }
    if (-not $toolProcess.HasExited) {
        throw "cancelled process tool remained alive"
    }

    $acpProcess.StandardInput.Close()
    if (-not $acpProcess.WaitForExit(5000) -or $acpProcess.ExitCode -ne 0) {
        throw "ACP did not shut down cleanly"
    }
    if (-not $daemon.HasExited) {
        Stop-Process -Id $daemon.Id -Force
        $daemon.WaitForExit()
    }
    $journal = Join-Path $runRoot (
        "sessions\" + $sessionId + "\events.jsonl"
    )
    $events = @(Get-Content -LiteralPath $journal | ForEach-Object {
        ($_ | ConvertFrom-Json).event
    })
    $types = @($events | ForEach-Object { $_.metadata.event_type })
    foreach ($required in @(
        "tool.execution_started",
        "tool.execution_failed",
        "model.request_cancelled"
    )) {
        if ($types -notcontains $required) {
            throw "process-tool cancellation omitted $required"
        }
    }
    if ($types -contains "tool.execution_completed" -or
        $types -contains "model.response_completed") {
        throw "cancelled process tool committed a successful terminal event"
    }
    $cancelledFailure = $events | Where-Object {
        $_.metadata.event_type -eq "tool.execution_failed" -and
        $_.payload.payload.code -eq "cancelled"
    }
    if (-not $cancelledFailure) {
        throw "cancelled process tool lacked structured cancellation"
    }
    Write-Output "runtime ACP tool cancellation E2E passed"
}
finally {
    if ($null -ne $acpProcess -and -not $acpProcess.HasExited) {
        $acpProcess.Kill()
        $acpProcess.WaitForExit()
    }
    if ($null -ne $daemon -and -not $daemon.HasExited) {
        Stop-Process -Id $daemon.Id -Force
        $daemon.WaitForExit()
    }
    Get-Process -ErrorAction SilentlyContinue | Where-Object {
        $_.StartTime -ge $testStartedAt -and
        $_.Path -in @($harness, $processHost, $scheduler)
    } | Stop-Process -Force
    Remove-Item Env:AGENTMOD_ACP_PROVIDER_OPTIONS -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_PERMISSION_MODE -ErrorAction SilentlyContinue
    Pop-Location
    if ($null -ne $runRoot -and (Test-Path -LiteralPath $runRoot)) {
        $resolved = [System.IO.Path]::GetFullPath($runRoot)
        $temp = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
        if (-not $resolved.StartsWith($temp) -or
            -not (Split-Path $resolved -Leaf).StartsWith(
                "agentmod-acp-tool-cancel-e2e-"
            )) {
            throw "refusing to remove non-AgentMod temporary path"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}
