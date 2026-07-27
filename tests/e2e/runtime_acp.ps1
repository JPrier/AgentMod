$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$runRoot = $null
$daemon = $null
$acpProcess = $null
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-cli -p agentmod-acp
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $acp = (Resolve-Path "target\debug\agentmod-acp.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-acp-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot -Force | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-acp-e2e-" + [guid]::NewGuid().ToString("N")
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
        $line = $message | ConvertTo-Json -Compress -Depth 20
        $acpProcess.StandardInput.WriteLine($line)
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
    $initialized = Read-Acp
    if ($initialized.id -ne 1 -or
        -not $initialized.result.agentCapabilities.loadSession) {
        throw "ACP initialization/capabilities failed"
    }
    Send-Acp @{
        jsonrpc = "2.0"; id = 2; method = "session/new"
        params = @{ cwd = $runRoot; mcpServers = @() }
    }
    $created = Read-Acp
    $sessionId = $created.result.sessionId
    if ([string]::IsNullOrWhiteSpace($sessionId)) {
        throw "ACP session creation failed"
    }
    Send-Acp @{
        jsonrpc = "2.0"; id = 3; method = "session/prompt"
        params = @{
            sessionId = $sessionId
            prompt = @(@{ type = "text"; text = "hello through ACP" })
        }
    }
    $sawText = $false
    $completed = $false
    for ($frame = 0; $frame -lt 20; $frame++) {
        $message = Read-Acp
        if ($message.method -eq "session/update" -and
            $message.params.update.sessionUpdate -eq "agent_message_chunk" -and
            $message.params.update.content.text -eq "deterministic response") {
            $sawText = $true
        }
        if ($message.id -eq 3) {
            if ($message.result.stopReason -ne "end_turn") {
                throw "ACP prompt returned the wrong stop reason"
            }
            $completed = $true
            break
        }
    }
    if (-not $sawText -or -not $completed) {
        throw "ACP did not emit update then prompt completion"
    }
    $journal = Join-Path $runRoot ("sessions\" + $sessionId + "\events.jsonl")
    if (@(Get-Content -LiteralPath $journal | Where-Object {
        $_ -match '"event_type":"model.response_completed"'
    }).Count -ne 1) {
        throw "ACP prompt did not use the canonical runtime provider path"
    }
    $acpProcess.StandardInput.Close()
    if (-not $acpProcess.WaitForExit(5000) -or $acpProcess.ExitCode -ne 0) {
        throw "ACP did not shut down cleanly"
    }
    Write-Output "runtime ACP E2E passed"
}
finally {
    if ($null -ne $acpProcess -and -not $acpProcess.HasExited) {
        $acpProcess.Kill($true)
        $acpProcess.WaitForExit()
    }
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
