$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$runRoot = $null
$daemon = $null
$acpProcess = $null
Push-Location $repository
try {
    cargo build --locked -p agentmod-runtime -p agentmod-harness -p agentmod-acp -p agentmod-mcp-host -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $acp = (Resolve-Path "target\debug\agentmod-acp.exe").Path
    $mcpHost = (Resolve-Path "target\debug\agentmod-mcp-host.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $fixture = Join-Path $repository "target\acp-mcp-stdio-fixture.exe"
    rustc tests\fixtures\mcp_stdio_server.rs --edition=2024 -o $fixture
    if ($LASTEXITCODE -ne 0) { throw "MCP fixture build failed" }
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-acp-rich-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot -Force | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-acp-rich-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_MCP_HOST_PROGRAM = $mcpHost
    $env:AGENTMOD_PERMISSION_MODE = "allow"
    $env:AGENTMOD_SCHEDULER_POLL_MS = "0"
    Remove-Item Env:AGENTMOD_MCP_SERVERS_JSON -ErrorAction SilentlyContinue
    $runtimeError = Join-Path $runRoot "runtime.err.log"
    $runtimeOutput = Join-Path $runRoot "runtime.out.log"
    $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
        -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru `
        -RedirectStandardError $runtimeError -RedirectStandardOutput $runtimeOutput
    Start-Sleep -Milliseconds 300

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
            $runtimeDetails = if (Test-Path -LiteralPath $runtimeError) {
                Get-Content -LiteralPath $runtimeError -Raw
            } else { "" }
            throw (
                "ACP closed: " + $acpProcess.StandardError.ReadToEnd() +
                [Environment]::NewLine + "runtime:" +
                [Environment]::NewLine + $runtimeDetails
            )
        }
        return $line | ConvertFrom-Json
    }

    Send-Acp @{
        jsonrpc = "2.0"; id = 1; method = "initialize"
        params = @{ protocolVersion = 1; clientCapabilities = @{} }
    }
    $initialized = Read-Acp
    $promptCapabilities = $initialized.result.agentCapabilities.promptCapabilities
    if (-not $promptCapabilities.image -or -not $promptCapabilities.audio -or
        -not $promptCapabilities.embeddedContext) {
        throw "ACP omitted rich prompt capabilities"
    }
    $sessionId = $null
    $mcpSecret = "acp-per-session-secret-never-persist-plaintext"
    $mcpServers = @(@{
        name = "fixture"; command = $fixture; args = @()
        env = @(@{ name = "FIXTURE_TOKEN"; value = $mcpSecret })
    })
    for ($attempt = 0; $attempt -lt 50; $attempt++) {
        Send-Acp @{
            jsonrpc = "2.0"; id = (100 + $attempt); method = "session/new"
            params = @{ cwd = $runRoot; mcpServers = $mcpServers }
        }
        $created = Read-Acp
        if ($null -ne $created.result.sessionId) {
            $sessionId = $created.result.sessionId
            break
        }
        Start-Sleep -Milliseconds 100
    }
    if ([string]::IsNullOrWhiteSpace($sessionId)) {
        throw "ACP session creation failed"
    }
    $sessionRoot = Join-Path $runRoot ("sessions\" + $sessionId)
    $styleLock = Get-Content -LiteralPath (Join-Path $sessionRoot "style.lock") -Raw
    $encrypted = Get-Content -LiteralPath (Join-Path $sessionRoot "mcp-bootstrap.enc.json") -Raw
    $mcpBinding = ($styleLock | ConvertFrom-Json).binding.mcp
    if ($styleLock.Contains($mcpSecret) -or $encrypted.Contains($mcpSecret) -or
        $mcpBinding.configuration_reference -notlike "session-mcp:blake3:*") {
        throw "MCP binding did not encrypt and sanitize its exact declaration"
    }
    Send-Acp @{
        jsonrpc = "2.0"; id = 2; method = "session/load"
        params = @{ sessionId = $sessionId; cwd = $runRoot; mcpServers = $mcpServers }
    }
    $loaded = Read-Acp
    if ($loaded.id -ne 2 -or $null -eq $loaded.result) {
        throw ("exact MCP session load failed: " + ($loaded | ConvertTo-Json -Depth 10))
    }

    Send-Acp @{
        jsonrpc = "2.0"; id = 3; method = "session/prompt"
        params = @{
            sessionId = $sessionId
            prompt = @(@{
                type = "image"; data = "not-base64"; mimeType = "image/png"
            })
        }
    }
    $invalid = Read-Acp
    if ($invalid.id -ne 3 -or $null -eq $invalid.error) {
        throw "malformed rich content was not rejected"
    }

    Send-Acp @{
        jsonrpc = "2.0"; id = 4; method = "session/prompt"
        params = @{
            sessionId = $sessionId
            prompt = @(
                @{ type = "text"; text = "inspect the attached content" },
                @{
                    type = "image"; data = "iVBORw=="; mimeType = "image/png"
                    uri = "file:///workspace/image.png"
                },
                @{ type = "audio"; data = "c291bmQ="; mimeType = "audio/wav" },
                @{
                    type = "resource"
                    resource = @{
                        uri = "file:///workspace/context.txt"
                        mimeType = "text/plain"
                        text = "embedded context"
                    }
                },
                @{
                    type = "resource"
                    resource = @{
                        uri = "file:///workspace/data.bin"
                        mimeType = "application/octet-stream"
                        blob = "YmxvYg=="
                    }
                }
            )
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
        if ($message.id -eq 4) {
            if ($message.result.stopReason -ne "end_turn") {
                throw "rich prompt returned the wrong stop reason"
            }
            $completed = $true
            break
        }
    }
    if (-not $sawText -or -not $completed) {
        throw "rich prompt did not complete through the runtime"
    }
    $journal = Join-Path $runRoot ("sessions\" + $sessionId + "\events.jsonl")
    $journalText = Get-Content -LiteralPath $journal -Raw
    foreach ($required in @(
        "agentmod_acp_content_version", "iVBORw==", "c291bmQ=",
        "embedded context", "YmxvYg=="
    )) {
        if (-not $journalText.Contains($required)) {
            throw "canonical rich prompt omitted $required"
        }
    }
    if (@(Get-Content -LiteralPath $journal | Where-Object {
        $_ -match '"event_type":"model.response_completed"'
    }).Count -ne 1) {
        throw "malformed rich prompt dispatched or valid prompt duplicated"
    }
    $turn = & $cli run "invoke the configured MCP echo tool" --session $sessionId --option 'mock_scenario="mcp_fixture_call"' --json | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or
        (($turn.events | Where-Object event -eq "text").text -join "") -ne
        "continued after approved runtime decision") {
        throw "per-session MCP invocation failed"
    }
    $journalText = Get-Content -LiteralPath $journal -Raw
    $hasMcpResult = $journalText.Contains("echoed-through-runtime")
    $hasPlaintextSecret = $journalText.Contains($mcpSecret)
    if (-not $hasMcpResult -or $hasPlaintextSecret) {
        throw (
            "MCP journal evidence invalid: result=$hasMcpResult " +
            "plaintext_secret=$hasPlaintextSecret"
        )
    }
    $substituted = @(@{
        name = "fixture"; command = $fixture; args = @()
        env = @(@{ name = "FIXTURE_TOKEN"; value = "substituted" })
    })
    Send-Acp @{
        jsonrpc = "2.0"; id = 22; method = "session/load"
        params = @{ sessionId = $sessionId; cwd = $runRoot; mcpServers = $substituted }
    }
    if ($null -eq (Read-Acp).error) {
        throw "substituted MCP declaration was accepted"
    }
    $acpProcess.StandardInput.Close()
    if (-not $acpProcess.WaitForExit(5000) -or $acpProcess.ExitCode -ne 0) {
        throw "ACP did not shut down cleanly"
    }
    Write-Output "runtime ACP rich-content and per-session MCP E2E passed"
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
    foreach ($name in @(
        "AGENTMOD_MCP_HOST_PROGRAM",
        "AGENTMOD_PERMISSION_MODE",
        "AGENTMOD_MCP_SERVERS_JSON"
    )) {
        Remove-Item "Env:$name" -ErrorAction SilentlyContinue
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
