$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-lsp-host -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $lspHost = (Resolve-Path "target\debug\agentmod-lsp-host.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-lsp-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path (Join-Path $workspace "src") `
        -Force | Out-Null
    Set-Content -LiteralPath (Join-Path $workspace "Cargo.toml") `
        -Value "[package]`nname = `"fixture`"`nversion = `"0.1.0`"`n"
    Set-Content -LiteralPath (Join-Path $workspace "src\lib.rs") `
        -Value "pub fn fixture() {}"

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-lsp-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_LSP_HOST_PROGRAM = $lspHost
    Remove-Item Env:AGENTMOD_LSP_SERVERS_JSON -ErrorAction SilentlyContinue
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
        $turn = & $cli run "detect the LSP project root" `
            --session $created.session_id `
            --option 'mock_scenario="lsp_project_root"' --json |
            ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "LSP tool turn failed" }
        $visible = ($turn.events | Where-Object event -eq "text" |
            ForEach-Object text) -join ""
        if ($visible -ne "continued after approved runtime decision") {
            throw "unexpected continued output: $visible"
        }

        $journalPath = Join-Path $runRoot (
            "sessions\" + $created.session_id + "\events.jsonl"
        )
        $events = @(Get-Content $journalPath | ForEach-Object {
            ($_ | ConvertFrom-Json).event
        })
        foreach ($required in @(
            "tool.call_proposed",
            "tool.call_approved",
            "tool.execution_started",
            "tool.execution_completed"
        )) {
            if (@($events | Where-Object {
                $_.metadata.event_type -eq $required
            }).Count -ne 1) {
                throw "missing or duplicated LSP lifecycle event: $required"
            }
        }
        $resultEntry = @($events | Where-Object {
            $_.metadata.event_type -eq "conversation.entry_committed" -and
            $_.payload.payload.entry.kind -eq "tool_result"
        })
        $projection = if ($resultEntry.Count -eq 1) {
            $resultEntry[0].payload.payload.entry.content.content |
                ConvertFrom-Json
        } else {
            $null
        }
        $actualRoot = $projection.root -replace '^\\\\\?\\', ''
        $expectedRoot = $workspace -replace '^\\\\\?\\', ''
        if ($null -eq $projection -or -not $actualRoot.Equals(
                $expectedRoot,
                [System.StringComparison]::OrdinalIgnoreCase
            )) {
            $details = $resultEntry | ConvertTo-Json -Depth 20
            throw "LSP project-root projection was incorrect`n$details"
        }
        Write-Output "runtime/CLI/harness/LSP tool-loop E2E passed"
    }
    finally {
        if ($null -ne $daemon -and -not $daemon.HasExited) {
            Stop-Process -Id $daemon.Id -Force
        }
        if (Test-Path -LiteralPath $runRoot) {
            $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
            $resolvedRun = (Resolve-Path $runRoot).Path
            if ($resolvedRun.StartsWith($resolvedTemp) -and
                (Split-Path $resolvedRun -Leaf).StartsWith(
                    "agentmod-lsp-e2e-"
                )) {
                Remove-Item -LiteralPath $resolvedRun -Recurse -Force
            }
        }
    }
}
finally {
    Pop-Location
}
