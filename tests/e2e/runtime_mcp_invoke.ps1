$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-mcp-host -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $mcpHost = (Resolve-Path "target\debug\agentmod-mcp-host.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $fixture = Join-Path $repository "target\mcp-stdio-fixture.exe"
    rustc tests\fixtures\mcp_stdio_server.rs --edition=2024 -o $fixture
    if ($LASTEXITCODE -ne 0) { throw "MCP fixture build failed" }

    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-mcp-invoke-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-mcp-invoke-e2e-" +
        [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_MCP_HOST_PROGRAM = $mcpHost
    $env:AGENTMOD_PERMISSION_MODE = "allow"
    $server = @{
        id = "fixture"
        display_name = "Runtime E2E fixture"
        active = $true
        transport = "stdio"
        program = $fixture
        arguments = @()
        environment = @{}
    }
    $env:AGENTMOD_MCP_SERVERS_JSON = $server |
        ConvertTo-Json -Compress -Depth 8 -AsArray

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
        $turn = & $cli run "invoke the configured MCP echo tool" `
            --session $created.session_id `
            --option 'mock_scenario="mcp_fixture_call"' --json |
            ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "configured MCP invocation failed" }
        $visible = ($turn.events | Where-Object event -eq "text" |
            ForEach-Object text) -join ""
        if ($visible -ne "continued after approved runtime decision") {
            throw "unexpected continued output: $visible"
        }

        $sessionRoot = Join-Path $runRoot (
            "sessions\" + $created.session_id
        )
        $journalPath = Join-Path $sessionRoot "events.jsonl"
        $events = @(Get-Content $journalPath | ForEach-Object {
            ($_ | ConvertFrom-Json).event
        })
        foreach ($required in @(
            "tool.call_proposed",
            "tool.call_approved",
            "tool.execution_dispatched",
            "tool.execution_started",
            "tool.execution_completed"
        )) {
            if (@($events | Where-Object {
                $_.metadata.event_type -eq $required
            }).Count -ne 1) {
                $types = ($events.metadata.event_type -join ", ")
                $failures = @($events | Where-Object {
                    $_.metadata.event_type -eq "tool.execution_failed"
                }) | ConvertTo-Json -Depth 20
                throw (
                    "missing or duplicated MCP lifecycle event: " +
                    "$required`n$types`n$failures"
                )
            }
        }
        if (@($events | Where-Object {
            $_.metadata.event_type -eq "tool.output_observed"
        }).Count -lt 1) {
            throw "MCP progress was not committed"
        }
        $resultEntry = @($events | Where-Object {
            $_.metadata.event_type -eq "conversation.entry_committed" -and
            $_.payload.payload.entry.kind -eq "tool_result"
        })
        if ($resultEntry.Count -ne 1 -or
            $resultEntry[0].payload.payload.entry.content.content -notmatch
            "echoed-through-runtime") {
            $details = $resultEntry | ConvertTo-Json -Depth 20
            throw "MCP result was not projected into context`n$details"
        }
        $hostState = Join-Path $sessionRoot "artifacts\mcp\authorization-replay"
        if (-not (Test-Path -LiteralPath $hostState)) {
            throw "MCP authorization replay state was not persisted"
        }
        Write-Output (
            "configured stdio MCP invocation through runtime E2E passed"
        )
    }
    finally {
        if ($null -ne $daemon -and -not $daemon.HasExited) {
            Stop-Process -Id $daemon.Id -Force
        }
        foreach ($name in @(
            "AGENTMOD_PERMISSION_MODE",
            "AGENTMOD_MCP_SERVERS_JSON"
        )) {
            Remove-Item "Env:$name" -ErrorAction SilentlyContinue
        }
        if (Test-Path -LiteralPath $runRoot) {
            $resolvedTemp = (
                Resolve-Path ([System.IO.Path]::GetTempPath())
            ).Path
            $resolvedRun = (Resolve-Path $runRoot).Path
            if ($resolvedRun.StartsWith($resolvedTemp) -and
                (Split-Path $resolvedRun -Leaf).StartsWith(
                    "agentmod-mcp-invoke-e2e-"
                )) {
                Remove-Item -LiteralPath $resolvedRun -Recurse -Force
            }
        }
    }
}
finally {
    Pop-Location
}
