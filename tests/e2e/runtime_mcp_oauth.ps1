$ErrorActionPreference = "Stop"

function Get-LoopbackPort {
    $listener = [System.Net.Sockets.TcpListener]::new(
        [System.Net.IPAddress]::Loopback,
        0
    )
    $listener.Start()
    try { return ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port }
    finally { $listener.Stop() }
}

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    if ($env:AGENTMOD_E2E_SKIP_BUILD -ne "1") {
        cargo build --locked -p agentmod-runtime -p agentmod-mcp-host `
            -p agentmod-cli -p agentmod-tui
        if ($LASTEXITCODE -ne 0) { throw "build failed" }
    }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $mcpHost = (Resolve-Path "target\debug\agentmod-mcp-host.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $tui = (Resolve-Path "target\debug\agentmod-tui.exe").Path
    $fixturePort = Get-LoopbackPort
    $callbackPort = Get-LoopbackPort
    $origin = "http://127.0.0.1:$fixturePort"
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-mcp-oauth-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null

    $fixture = Start-Process -FilePath "python" -ArgumentList @(
        "tests\fixtures\mcp_oauth_server.py",
        "--port",
        "$fixturePort"
    ) -WorkingDirectory $repository -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $runRoot "fixture.stdout.log") `
        -RedirectStandardError (Join-Path $runRoot "fixture.stderr.log")
    $fixtureReady = $false
    for ($attempt = 0; $attempt -lt 100; $attempt++) {
        $client = [System.Net.Sockets.TcpClient]::new()
        try {
            $client.Connect("127.0.0.1", $fixturePort)
            $fixtureReady = $true
            break
        } catch {
            Start-Sleep -Milliseconds 50
        } finally {
            $client.Dispose()
        }
    }
    if (-not $fixtureReady) { throw "OAuth fixture did not become ready" }
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-mcp-oauth-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_MCP_HOST_PROGRAM = $mcpHost
    $env:AGENTMOD_MCP_OAUTH_KEY = (
        "5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b" +
        "5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b"
    )
    $server = @{
        id = "protected"
        display_name = "Protected process fixture"
        active = $true
        transport = "streamable_http_oauth"
        url = "$origin/mcp"
        authorization_server = "$origin/issuer"
        client_id = "agentmod-process-client"
        client_secret_environment = $null
        redirect_uri = "http://127.0.0.1:$callbackPort/callback"
        scopes = @("tools.read")
    }
    $env:AGENTMOD_MCP_SERVERS_JSON = $server |
        ConvertTo-Json -Compress -Depth 8 -AsArray

    $daemon = $null
    $succeeded = $false
    try {
        $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput (Join-Path $runRoot "runtime-1.stdout.log") `
            -RedirectStandardError (Join-Path $runRoot "runtime-1.stderr.log")
        for ($attempt = 0; $attempt -lt 100; $attempt++) {
            & $cli doctor --json 2>$null | Out-Null
            if ($LASTEXITCODE -eq 0) { break }
            Start-Sleep -Milliseconds 100
        }
        if ($LASTEXITCODE -ne 0) { throw "runtime did not become ready" }

        $created = & $cli session create --workspace $workspace `
            --style persistent-chat --json | ConvertFrom-Json
        $resources = @(
            & $tui --smoke-session-command $created.session_id "/resources" 2>&1
        ) -join [Environment]::NewLine
        if ($LASTEXITCODE -ne 0 -or
            $resources -notmatch "selected=$($created.session_id)" -or
            $resources -notmatch "resources=0/0/0") {
            throw "TUI canonical empty resource projection failed: $resources"
        }
        $tuiBegun = @(
            & $tui --smoke-session-command $created.session_id `
                "/mcp-oauth-begin protected" 2>&1
        ) -join [Environment]::NewLine
        if ($LASTEXITCODE -ne 0 -or
            $tuiBegun -notmatch "selected=$($created.session_id)" -or
            $tuiBegun -notmatch "mcp=protected:pending:([^ ]+)") {
            throw "TUI OAuth begin failed: $tuiBegun"
        }
        $tuiTransaction = $Matches[1]
        $tuiStatus = @(
            & $tui --smoke-session-command $created.session_id `
                "/mcp-oauth-status protected" 2>&1
        ) -join [Environment]::NewLine
        if ($LASTEXITCODE -ne 0 -or
            $tuiStatus -notmatch "selected=$($created.session_id)" -or
            $tuiStatus -notmatch (
                "mcp=protected:pending:" + [regex]::Escape($tuiTransaction)
            )) {
            throw "TUI OAuth status failed: $tuiStatus"
        }
        $tuiCancelled = @(
            & $tui --smoke-session-command $created.session_id `
                "/mcp-oauth-cancel protected $tuiTransaction" 2>&1
        ) -join [Environment]::NewLine
        if ($LASTEXITCODE -ne 0 -or
            $tuiCancelled -notmatch "selected=$($created.session_id)" -or
            $tuiCancelled -notmatch "mcp=protected:unauthorized:none") {
            throw "TUI OAuth cancel failed: $tuiCancelled"
        }
        $cancelledStatus = & $cli mcp oauth status protected `
            --session $created.session_id --json | ConvertFrom-Json
        if ($cancelledStatus.status -ne "unauthorized" -or
            $null -ne $cancelledStatus.transaction_id) {
            throw "TUI OAuth cancellation was not authoritative"
        }
        $begun = & $cli mcp oauth begin protected `
            --session $created.session_id --json | ConvertFrom-Json
        if ($begun.server_id -ne "protected" -or
            -not $begun.authorization_url -or
            $begun.authorization_url_hash.Length -ne 64) {
            throw "invalid OAuth begin projection"
        }

        Stop-Process -Id $daemon.Id -Force
        $daemon.WaitForExit()
        $daemon = $null
        Start-Sleep -Milliseconds 250
        $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput (Join-Path $runRoot "runtime-2.stdout.log") `
            -RedirectStandardError (Join-Path $runRoot "runtime-2.stderr.log")
        for ($attempt = 0; $attempt -lt 100; $attempt++) {
            & $cli doctor --json 2>$null | Out-Null
            if ($LASTEXITCODE -eq 0) { break }
            Start-Sleep -Milliseconds 100
        }
        if ($LASTEXITCODE -ne 0) { throw "restarted runtime did not become ready" }

        $recovered = & $cli mcp oauth status protected `
            --session $created.session_id --json | ConvertFrom-Json
        if ($recovered.status -ne "pending" -or
            $recovered.transaction_id -ne $begun.transaction_id) {
            throw "pending OAuth callback was not reconstructed"
        }
        Invoke-WebRequest -Uri $begun.authorization_url `
            -MaximumRedirection 5 -UseBasicParsing | Out-Null
        $status = $null
        for ($attempt = 0; $attempt -lt 100; $attempt++) {
            $status = & $cli mcp oauth status protected `
                --session $created.session_id --json | ConvertFrom-Json
            if ($status.status -ne "pending") { break }
            Start-Sleep -Milliseconds 50
        }
        if ($status.status -ne "authorized" -or
            $status.status_hash.Length -ne 64 -or
            $status.scopes -notcontains "tools.read") {
            $status | ConvertTo-Json -Depth 8
            throw "OAuth did not become authorized after restart"
        }

        $journalPath = Join-Path $runRoot (
            "sessions\" + $created.session_id + "\events.jsonl"
        )
        $journal = Get-Content -LiteralPath $journalPath -Raw
        foreach ($secret in @(
            $begun.authorization_url,
            "process-authorization-code",
            "process-access-token",
            "process-refresh-token",
            "code_verifier"
        )) {
            if ($journal.Contains($secret)) {
                throw "secret or transient OAuth material leaked into journal"
            }
        }
        $events = @(Get-Content -LiteralPath $journalPath | ForEach-Object {
            ($_ | ConvertFrom-Json).event
        })
        $audits = @($events | Where-Object {
            $_.metadata.event_type -eq "mcp.oauth_management_audited"
        })
        if ($audits.Count -lt 2) { throw "canonical OAuth audits were not committed" }
        foreach ($audit in $audits) {
            if ($audit.payload.payload.request_hash.Length -ne 64 -or
                $audit.payload.payload.configuration_hash.Length -ne 64 -or
                $audit.payload.payload.result_hash.Length -ne 64) {
                throw "OAuth audit hash binding is incomplete"
            }
        }

        $oauthFiles = Get-ChildItem -LiteralPath (
            Join-Path $runRoot "sessions"
        ) -Recurse -File | Where-Object FullName -Match "oauth"
        $persisted = ($oauthFiles | ForEach-Object {
            [System.Text.Encoding]::UTF8.GetString(
                [System.IO.File]::ReadAllBytes($_.FullName)
            )
        }) -join ""
        foreach ($secret in @(
            "process-authorization-code",
            "process-access-token",
            "process-refresh-token"
        )) {
            if ($persisted.Contains($secret)) {
                throw "OAuth secret leaked into durable host state"
            }
        }
        $succeeded = $true
        Write-Output "runtime MCP OAuth restart/audit process E2E passed"
    }
    finally {
        if ($null -ne $daemon -and -not $daemon.HasExited) {
            Stop-Process -Id $daemon.Id -Force
        }
        if ($null -ne $fixture -and -not $fixture.HasExited) {
            Stop-Process -Id $fixture.Id -Force
        }
        foreach ($name in @(
            "AGENTMOD_MCP_SERVERS_JSON",
            "AGENTMOD_MCP_OAUTH_KEY"
        )) {
            Remove-Item "Env:$name" -ErrorAction SilentlyContinue
        }
        if ($succeeded -and (Test-Path -LiteralPath $runRoot)) {
            $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
            $resolvedRun = (Resolve-Path $runRoot).Path
            if ($resolvedRun.StartsWith($resolvedTemp) -and
                (Split-Path $resolvedRun -Leaf).StartsWith(
                    "agentmod-mcp-oauth-e2e-"
                )) {
                Remove-Item -LiteralPath $resolvedRun -Recurse -Force
            }
        } elseif (Test-Path -LiteralPath $runRoot) {
            Write-Warning "preserving failed MCP OAuth E2E at $runRoot"
        }
    }
}
finally {
    Pop-Location
}
