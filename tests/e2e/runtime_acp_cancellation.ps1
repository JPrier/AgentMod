$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$runRoot = $null
$daemon = $null
$acpProcess = $null
$harness = $null
$scheduler = $null
$testStartedAt = Get-Date
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-cli -p agentmod-acp
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $scheduler = (Resolve-Path "target\debug\agentmod-scheduler.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $acp = (Resolve-Path "target\debug\agentmod-acp.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-acp-cancel-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot -Force | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-acp-cancel-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_HARNESS_FRAME_PACING_MS = "750"
    $env:AGENTMOD_ACP_PROVIDER_OPTIONS = (
        '{"mock_scenario":"streaming_text"}'
    )
    $env:AGENTMOD_SCHEDULER_POLL_MS = "0"
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
    function New-AcpSession([int]$id) {
        $workspace = Join-Path $runRoot ("workspace-" + $id)
        New-Item -ItemType Directory -Path $workspace -Force | Out-Null
        Send-Acp @{
            jsonrpc = "2.0"; id = $id; method = "session/new"
            params = @{ cwd = $workspace; mcpServers = @() }
        }
        $response = Read-Acp
        if ($response.id -ne $id) { throw "session creation failed" }
        return $response.result.sessionId
    }
    function Read-EventTypes([string]$journal) {
        for ($attempt = 0; $attempt -lt 50; $attempt++) {
            try {
                return @(Get-Content -LiteralPath $journal | ForEach-Object {
                    ($_ | ConvertFrom-Json).event.metadata.event_type
                })
            } catch {
                if ($attempt -eq 49) { throw }
                Start-Sleep -Milliseconds 100
            }
        }
    }
    function Assert-CancelledJournal([string]$sessionId, [bool]$requireText) {
        $journal = Join-Path $runRoot (
            "sessions\" + $sessionId + "\events.jsonl"
        )
        $types = @(Read-EventTypes $journal)
        if (@($types | Where-Object { $_ -eq "model.request_cancelled" }).Count -ne 1) {
            throw "ACP cancellation did not commit exactly one cancellation"
        }
        if ($types | Where-Object { $_ -eq "model.response_completed" }) {
            throw "ACP cancellation committed completion"
        }
        if ($requireText -and
            -not ($types | Where-Object {
                $_ -eq "model.output_delta_observed"
            })) {
            throw "ACP streaming cancellation omitted partial output"
        }
    }

    Send-Acp @{
        jsonrpc = "2.0"; id = 1; method = "initialize"
        params = @{ protocolVersion = 1; clientCapabilities = @{} }
    }
    if ((Read-Acp).id -ne 1) { throw "ACP initialization failed" }

    $beforeSession = New-AcpSession 10
    Send-Acp @{
        jsonrpc = "2.0"; id = 11; method = "session/prompt"
        params = @{
            sessionId = $beforeSession
            prompt = @(@{ type = "text"; text = "cancel immediately" })
        }
    }
    Send-Acp @{
        jsonrpc = "2.0"; method = "session/cancel"
        params = @{ sessionId = $beforeSession }
    }
    $beforeStop = $null
    for ($frame = 0; $frame -lt 20; $frame++) {
        $message = Read-Acp
        if ($message.id -eq 11) {
            $beforeStop = $message.result.stopReason
            break
        }
    }
    if ($beforeStop -ne "cancelled") {
        throw "pre-start ACP cancellation returned $beforeStop"
    }

    $streamSession = New-AcpSession 20
    Send-Acp @{
        jsonrpc = "2.0"; id = 21; method = "session/prompt"
        params = @{
            sessionId = $streamSession
            prompt = @(@{ type = "text"; text = "cancel during stream" })
        }
    }
    $sawText = $false
    $streamStop = $null
    for ($frame = 0; $frame -lt 30; $frame++) {
        $message = Read-Acp
        if ($message.method -eq "session/update" -and
            $message.params.update.sessionUpdate -eq "agent_message_chunk" -and
            -not $sawText) {
            $sawText = $true
            Send-Acp @{
                jsonrpc = "2.0"; method = "session/cancel"
                params = @{ sessionId = $streamSession }
            }
        }
        if ($message.id -eq 21) {
            $streamStop = $message.result.stopReason
            break
        }
    }
    if (-not $sawText -or $streamStop -ne "cancelled") {
        throw "mid-stream ACP cancellation did not preserve partial output"
    }

    $acpProcess.StandardInput.Close()
    if (-not $acpProcess.WaitForExit(5000) -or $acpProcess.ExitCode -ne 0) {
        throw "ACP did not shut down cleanly"
    }
    if (-not $daemon.HasExited) {
        Stop-Process -Id $daemon.Id -Force
        $daemon.WaitForExit()
    }
    Assert-CancelledJournal $beforeSession $false
    Assert-CancelledJournal $streamSession $true
    Write-Output "runtime ACP cancellation E2E passed"
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
        $_.Path -in @($harness, $scheduler)
    } | Stop-Process -Force
    Remove-Item Env:AGENTMOD_HARNESS_FRAME_PACING_MS `
        -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_ACP_PROVIDER_OPTIONS -ErrorAction SilentlyContinue
    Pop-Location
    if ($null -ne $runRoot -and (Test-Path -LiteralPath $runRoot)) {
        $resolved = [System.IO.Path]::GetFullPath($runRoot)
        $temp = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
        if (-not $resolved.StartsWith($temp) -or
            -not (Split-Path $resolved -Leaf).StartsWith(
                "agentmod-acp-cancel-e2e-"
            )) {
            throw "refusing to remove non-AgentMod temporary path"
        }
        for ($cleanupAttempt = 0; $cleanupAttempt -lt 20; $cleanupAttempt++) {
            try {
                Remove-Item -LiteralPath $resolved -Recurse -Force
                break
            } catch {
                if ($cleanupAttempt -eq 19) {
                    Write-Warning "temporary ACP cancellation fixture remains at $resolved"
                    break
                }
                Start-Sleep -Milliseconds 100
            }
        }
    }
}
