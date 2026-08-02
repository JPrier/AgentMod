$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build --locked -p agentmod-runtime -p agentmod-scheduler `
        -p agentmod-harness -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $targetRoot = if ([string]::IsNullOrWhiteSpace($env:CARGO_TARGET_DIR)) {
        Join-Path $repository "target"
    } elseif ([IO.Path]::IsPathRooted($env:CARGO_TARGET_DIR)) {
        [IO.Path]::GetFullPath($env:CARGO_TARGET_DIR)
    } else {
        [IO.Path]::GetFullPath((Join-Path $repository $env:CARGO_TARGET_DIR))
    }
    $debugRoot = Join-Path $targetRoot "debug"
    $runtime = (Resolve-Path (Join-Path $debugRoot "agentmod-runtime.exe")).Path
    $harness = (Resolve-Path (Join-Path $debugRoot "agentmod-harness.exe")).Path
    $cli = (Resolve-Path (Join-Path $debugRoot "agentmod.exe")).Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-typed-summary-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $succeeded = $false
    $workspace = Join-Path $runRoot "workspace"
    $styleRoot = Join-Path $runRoot "styles\user"
    New-Item -ItemType Directory -Path $workspace, $styleRoot -Force | Out-Null
    $summaryStyle = (
        Get-Content tests\fixtures\styles\persistent-none-sliding.toml -Raw
    ).Replace(
        'id = "e2e-persistent-sliding"',
        'id = "e2e-persistent-summary"'
    ).Replace(
        'strategy = "sliding_window"',
        'strategy = "summary"'
    )
    $stylePath = Join-Path $styleRoot "persistent-summary.toml"
    Set-Content -LiteralPath $stylePath -Value $summaryStyle -NoNewline

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-typed-summary-e2e-" +
        [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $runtimeErr = Join-Path $runRoot "runtime.stderr.log"

    function Wait-RuntimeReady {
        for ($attempt = 0; $attempt -lt 100; $attempt++) {
            try {
                & $cli doctor --json 2>$null | Out-Null
                if ($LASTEXITCODE -eq 0) { return }
            } catch {}
            Start-Sleep -Milliseconds 100
        }
        throw "runtime did not become ready"
    }

    function Read-Journal($sessionId) {
        $journal = Join-Path $runRoot (
            "sessions\" + $sessionId + "\events.jsonl"
        )
        return @(Get-Content -LiteralPath $journal | ForEach-Object {
            $_ | ConvertFrom-Json
        })
    }

    $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
        -WorkingDirectory $runRoot -WindowStyle Hidden `
        -RedirectStandardError $runtimeErr -PassThru
    try {
        Wait-RuntimeReady
        $validation = & $cli style validate $stylePath --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or -not $validation.valid) {
            throw "typed-summary style validation failed"
        }
        $session = & $cli session create --workspace $workspace `
            --style e2e-persistent-summary --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "session creation failed" }
        $turn = & $cli run "summarize the canonical context" `
            --session $session.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="summary-output"' --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "typed-summary turn failed" }
        $visible = @($turn.events | Where-Object event -eq "text" |
            ForEach-Object text) -join ""
        if ($visible -ne "alpha beta summary-output") {
            throw "typed-summary turn changed provider output"
        }

        $before = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        $projection = @($before.state.conversation.provider_projection)
        $summaries = @($projection | Where-Object kind -eq "context_summary")
        if ($summaries.Count -ne 1 -or
            @($before.state.conversation.history).Count -ne 2 -or
            @($before.state.conversation.history |
                Where-Object kind -eq "context_summary").Count -ne 0 -or
            $before.state.conversation.projection_provenance.method -ne "summary") {
            throw "typed summary did not replace only the provider projection"
        }
        $typed = $summaries[0].content.text | ConvertFrom-Json
        if ($typed.schema -ne "agentmod.context-summary.v1" -or
            $typed.source_entry_count -lt 1 -or
            $typed.entries.Count -lt 1) {
            throw "typed summary payload was not bounded structured context"
        }

        $journal = Read-Journal $session.session_id
        $compactions = @($journal | Where-Object {
            $_.event.metadata.event_type -eq "context.projection_replaced" -and
            $_.event.payload.payload.provenance.method -eq "summary"
        })
        $proposals = @($journal | Where-Object {
            $_.event.metadata.event_type -eq "model.request_proposed"
        })
        if ($compactions.Count -ne 1 -or $proposals.Count -ne 1 -or
            $compactions[0].event.metadata.sequence -ge
                $proposals[0].event.metadata.sequence) {
            throw "typed summary was not canonically committed before provider dispatch"
        }
        $beforeProjection = (
            $before.state.conversation.provider_projection |
                ConvertTo-Json -Depth 30 -Compress
        )
        $planHash = $before.state.style_binding.execution_plan_hash
        $beforeCount = $journal.Count

        Stop-Process -Id $daemon.Id -Force
        $daemon.WaitForExit()
        $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden `
            -RedirectStandardError $runtimeErr -PassThru
        Wait-RuntimeReady

        $after = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        $afterProjection = (
            $after.state.conversation.provider_projection |
                ConvertTo-Json -Depth 30 -Compress
        )
        if ($after.state.style_binding.execution_plan_hash -ne $planHash -or
            $after.state.style_binding.compaction.strategy -ne "summary" -or
            $afterProjection -ne $beforeProjection) {
            throw "typed-summary binding or projection changed after restart"
        }
        $replayed = & $cli session replay $session.session_id --json |
            ConvertFrom-Json
        $replayedProjection = (
            $replayed.state.conversation.provider_projection |
                ConvertTo-Json -Depth 30 -Compress
        )
        if ($replayedProjection -ne $beforeProjection -or
            (Read-Journal $session.session_id).Count -ne $beforeCount) {
            throw "pure replay changed typed-summary state or journal"
        }

        Write-Output (
            "runtime typed-summary compaction/restart/replay E2E passed"
        )
        $succeeded = $true
    }
    finally {
        if ($null -ne $daemon -and -not $daemon.HasExited) {
            Stop-Process -Id $daemon.Id -Force -ErrorAction SilentlyContinue
            $daemon.WaitForExit()
        }
        $resolvedRunRoot = [System.IO.Path]::GetFullPath($runRoot)
        $tempRoot = [System.IO.Path]::GetFullPath(
            [System.IO.Path]::GetTempPath()
        )
        if ($succeeded -and $resolvedRunRoot.StartsWith(
            $tempRoot + "agentmod-typed-summary-e2e-"
        )) {
            Remove-Item -LiteralPath $resolvedRunRoot -Recurse -Force `
                -ErrorAction SilentlyContinue
        } elseif (-not $succeeded) {
            Write-Warning "retained failed E2E root: $resolvedRunRoot"
        }
    }
}
finally {
    Pop-Location
}
