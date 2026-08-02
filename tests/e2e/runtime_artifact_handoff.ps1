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
        "agentmod-artifact-handoff-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $succeeded = $false
    $workspace = Join-Path $runRoot "workspace"
    $styleRoot = Join-Path $runRoot "styles\user"
    New-Item -ItemType Directory -Path $workspace, $styleRoot -Force | Out-Null
    $handoffStyle = (
        Get-Content tests\fixtures\styles\persistent-none-sliding.toml -Raw
    ).Replace(
        'id = "e2e-persistent-sliding"',
        'id = "e2e-persistent-artifact-handoff"'
    ).Replace(
        'strategy = "sliding_window"',
        'strategy = "artifact_handoff"'
    )
    $stylePath = Join-Path $styleRoot "persistent-artifact-handoff.toml"
    Set-Content -LiteralPath $stylePath -Value $handoffStyle -NoNewline

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-artifact-handoff-e2e-" +
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
            throw "artifact-handoff style validation failed"
        }
        $session = & $cli session create --workspace $workspace `
            --style e2e-persistent-artifact-handoff --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "session creation failed" }
        $turn = & $cli run "persist the complete provider context" `
            --session $session.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="artifact-output"' --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "artifact-handoff turn failed" }
        $visible = @($turn.events | Where-Object event -eq "text" |
            ForEach-Object text) -join ""
        if ($visible -ne "alpha beta artifact-output") {
            throw "artifact handoff changed provider output"
        }

        $before = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        $projection = @($before.state.conversation.provider_projection)
        $artifacts = @($projection | Where-Object kind -eq "artifact_reference")
        $historyArtifacts = @($before.state.conversation.history |
            Where-Object kind -eq "artifact_reference")
        $records = @($before.state.artifact_persistences.PSObject.Properties |
            ForEach-Object { $_.Value })
        if ($artifacts.Count -ne 1 -or $historyArtifacts.Count -ne 0 -or
            $records.Count -ne 1 -or
            $before.state.conversation.projection_provenance.method -ne
                "artifact_handoff") {
            throw "artifact handoff did not replace only provider projection"
        }
        $record = $records[0]
        if ($record.state -ne "completed" -or
            $record.identity.context_phase.phase -ne "compaction" -or
            $artifacts[0].content.artifact_reference -ne
                $record.artifact_reference -or
            $artifacts[0].content.content_hash -ne
                $record.identity.content_hash) {
            throw "artifact projection and canonical outbox do not agree"
        }
        $hash = [string]$record.identity.content_hash
        $prefix = $hash.Substring(0, 2)
        $contentPath = Join-Path $runRoot (
            "sessions\" + $session.session_id +
            "\artifacts\context\objects\" + $prefix + "\" + $hash +
            "\content"
        )
        $document = Get-Content -LiteralPath $contentPath -Raw |
            ConvertFrom-Json
        if ($document.schema -ne "agentmod.context-artifact.v1" -or
            $document.source_entry_count -lt 1 -or
            @($document.provider_projection).Count -lt 1) {
            throw "stored context artifact document is invalid"
        }

        $journal = Read-Journal $session.session_id
        foreach ($eventType in @(
            "artifact.persistence_proposed",
            "artifact.persistence_approved",
            "artifact.persistence_dispatched",
            "artifact.persistence_completed",
            "context.projection_replacement_approved"
        )) {
            if (@($journal | Where-Object {
                $_.event.metadata.event_type -eq $eventType
            }).Count -ne 1) {
                throw "expected exactly one $eventType event"
            }
        }
        $replacements = @($journal | Where-Object {
            $_.event.metadata.event_type -eq "context.projection_replaced" -and
            $_.event.payload.payload.provenance.method -eq "artifact_handoff"
        })
        if ($replacements.Count -ne 1 -or
            @($replacements[0].event.metadata.artifacts).Count -ne 1 -or
            $replacements[0].event.metadata.artifacts[0].content_hash -ne $hash) {
            throw "replacement envelope does not bind the exact artifact"
        }
        $beforeProjection = (
            $before.state.conversation.provider_projection |
                ConvertTo-Json -Depth 40 -Compress
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
                ConvertTo-Json -Depth 40 -Compress
        )
        if ($after.state.style_binding.execution_plan_hash -ne $planHash -or
            $after.state.style_binding.compaction.strategy -ne
                "artifact_handoff" -or $afterProjection -ne $beforeProjection) {
            throw "artifact-handoff binding or projection changed after restart"
        }
        $replayed = & $cli session replay $session.session_id --json |
            ConvertFrom-Json
        $replayedProjection = (
            $replayed.state.conversation.provider_projection |
                ConvertTo-Json -Depth 40 -Compress
        )
        if ($replayedProjection -ne $beforeProjection -or
            (Read-Journal $session.session_id).Count -ne $beforeCount) {
            throw "pure replay changed artifact-handoff state or journal"
        }

        Write-Output (
            "runtime artifact-handoff compaction/restart/replay E2E passed"
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
            $tempRoot + "agentmod-artifact-handoff-e2e-"
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
