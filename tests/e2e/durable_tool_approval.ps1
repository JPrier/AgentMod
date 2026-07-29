$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-filesystem-host -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-approval-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $filesystem = (Resolve-Path "target\debug\agentmod-filesystem-host.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-approval-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_FILESYSTEM_HOST_PROGRAM = $filesystem
    Remove-Item Env:AGENTMOD_PERMISSION_MODE -ErrorAction SilentlyContinue

    $daemon = $null
    try {
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

        $created = & $cli session create --workspace $workspace `
            --style persistent-chat --json | ConvertFrom-Json
        $turn = & $cli run "write the approved fixture" `
            --session $created.session_id `
            --option 'mock_scenario="approval_write"' --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "approval turn failed" }
        if ([string]::IsNullOrWhiteSpace($turn.awaiting_continuation)) {
            throw "turn did not return a durable approval continuation"
        }
        $continuation = $turn.awaiting_continuation
        $target = Join-Path $workspace "approved.txt"
        if (Test-Path -LiteralPath $target) {
            throw "tool executed before approval"
        }

        $journalPath = Join-Path $runRoot (
            "sessions\" + $created.session_id + "\events.jsonl"
        )
        $beforeRestart = @(Get-Content $journalPath | ForEach-Object {
            ($_ | ConvertFrom-Json).event
        })
        if (@($beforeRestart | Where-Object {
            $_.metadata.event_type -eq "approval.requested"
        }).Count -ne 1) {
            throw "durable approval request event was not recorded"
        }
        if (@($beforeRestart | Where-Object {
            $_.metadata.event_type -eq "tool.execution_started" -or
            $_.metadata.event_type -eq "tool.execution_dispatched"
        }).Count -ne 0) {
            throw "tool execution began before approval"
        }

        Stop-Process -Id $daemon.Id -Force
        $daemon.WaitForExit()
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
        if (-not $ready) { throw "restarted runtime did not become ready" }

        $sequenceBeforeBlockedTurn = $beforeRestart[-1].metadata.sequence
        $savedErrorPreference = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        & $cli run "must remain blocked while approval is pending" `
            --session $created.session_id `
            --option 'mock_scenario="streaming_text"' --json 2>$null | Out-Null
        $blockedExit = $LASTEXITCODE
        $ErrorActionPreference = $savedErrorPreference
        if ($blockedExit -eq 0) {
            throw "a second turn was accepted while durable approval was pending"
        }
        $afterBlockedTurn = @(Get-Content $journalPath | ForEach-Object {
            ($_ | ConvertFrom-Json).event
        })
        if ($afterBlockedTurn[-1].metadata.sequence -ne $sequenceBeforeBlockedTurn -or
            $afterBlockedTurn.Count -ne $beforeRestart.Count) {
            throw "rejected pending-approval turn mutated canonical state"
        }

        $resolved = & $cli approval resolve $created.session_id `
            $continuation approve --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or $resolved.transitioned -ne $true) {
            throw "approval did not win the durable transition"
        }
        if ($resolved.awaiting_continuation) {
            throw "approval unexpectedly produced another continuation"
        }
        $visible = ($resolved.events | Where-Object event -eq "text" |
            ForEach-Object text) -join ""
        if ($visible -ne "continued after durable approval decision") {
            throw "unexpected resumed output: $visible"
        }
        if (-not (Test-Path -LiteralPath $target) -or
            (Get-Content -LiteralPath $target -Raw) -ne "executed once`n") {
            $details = @(Get-Content $journalPath | ForEach-Object {
                ($_ | ConvertFrom-Json).event
            } | Where-Object {
                $_.metadata.event_type -like "tool.*" -or
                $_.metadata.event_type -like "approval.*"
            }) | ConvertTo-Json -Depth 20
            throw "approved action did not execute exactly once`n$details"
        }

        $afterFirst = @(Get-Content $journalPath | ForEach-Object {
            ($_ | ConvertFrom-Json).event
        })
        $sequenceAfterFirst = $afterFirst[-1].metadata.sequence
        if (@($afterFirst | Where-Object {
            $_.metadata.event_type -eq "approval.resolved"
        }).Count -ne 1 -or @($afterFirst | Where-Object {
            $_.metadata.event_type -eq "tool.execution_dispatched"
        }).Count -ne 1 -or @($afterFirst | Where-Object {
            $_.metadata.event_type -eq "tool.execution_started"
        }).Count -ne 1 -or @($afterFirst | Where-Object {
            $_.metadata.event_type -eq "tool.execution_completed"
        }).Count -ne 1 -or @($afterFirst | Where-Object {
            $_.metadata.event_type -eq "tool.execution_failed"
        }).Count -ne 0) {
            throw "approved lifecycle did not execute exactly once"
        }

        $duplicate = & $cli approval resolve $created.session_id `
            $continuation approve --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or $duplicate.transitioned -ne $false) {
            throw "duplicate approval was not idempotent"
        }
        $afterDuplicate = @(Get-Content $journalPath | ForEach-Object {
            ($_ | ConvertFrom-Json).event
        })
        if ($afterDuplicate[-1].metadata.sequence -ne $sequenceAfterFirst -or
            @($afterDuplicate | Where-Object {
                $_.metadata.event_type -eq "tool.execution_dispatched"
            }).Count -ne 1 -or @($afterDuplicate | Where-Object {
                $_.metadata.event_type -eq "tool.execution_started"
            }).Count -ne 1 -or @($afterDuplicate | Where-Object {
                $_.metadata.event_type -eq "tool.execution_completed"
            }).Count -ne 1 -or @($afterDuplicate | Where-Object {
                $_.metadata.event_type -eq "tool.execution_failed"
            }).Count -ne 0 -or @($afterDuplicate | Where-Object {
                $_.metadata.event_type -eq "conversation.entry_committed" -and
                $_.payload.payload.entry.kind -eq "tool_result" -and
                $_.payload.payload.entry.content.content -match "permission_denied"
            }).Count -ne 0) {
            throw "duplicate approval changed canonical state or reran the tool"
        }
        $continued = & $cli run "continue after approved graph completion" `
            --session $created.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="post-approval-turn"' --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or
            ($continued.events | Where-Object event -eq "text" |
                ForEach-Object text) -notcontains "post-approval-turn") {
            throw "style graph did not return to its entry after approval completion"
        }

        $deniedWorkspace = Join-Path $runRoot "denied-workspace"
        New-Item -ItemType Directory -Path $deniedWorkspace -Force | Out-Null
        $deniedSession = & $cli session create --workspace $deniedWorkspace `
            --style persistent-chat --json | ConvertFrom-Json
        $deniedTurn = & $cli run "do not execute the denied fixture" `
            --session $deniedSession.session_id `
            --option 'mock_scenario="approval_write"' --json | ConvertFrom-Json
        $denied = & $cli approval resolve $deniedSession.session_id `
            $deniedTurn.awaiting_continuation deny --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or $denied.transitioned -ne $true) {
            throw "denial did not win the durable transition"
        }
        $deniedVisible = ($denied.events | Where-Object event -eq "text" |
            ForEach-Object text) -join ""
        if ($deniedVisible -ne "continued after durable approval decision") {
            throw "model did not continue after structured denial"
        }
        if (Test-Path -LiteralPath (Join-Path $deniedWorkspace "approved.txt")) {
            throw "denied tool call executed"
        }
        $deniedJournalPath = Join-Path $runRoot (
            "sessions\" + $deniedSession.session_id + "\events.jsonl"
        )
        $deniedEvents = @(Get-Content $deniedJournalPath | ForEach-Object {
            ($_ | ConvertFrom-Json).event
        })
        if (@($deniedEvents | Where-Object {
            $_.metadata.event_type -eq "tool.execution_started"
        }).Count -ne 0 -or @($deniedEvents | Where-Object {
            $_.metadata.event_type -eq "tool.execution_failed" -and
            $_.payload.payload.code -eq "permission_denied"
        }).Count -ne 1) {
            throw "denied action lifecycle was not recorded without execution"
        }
        $deniedToolResults = @($deniedEvents | Where-Object {
            $_.metadata.event_type -eq "conversation.entry_committed" -and
            $_.payload.payload.entry.kind -eq "tool_result"
        })
        $deniedProjection = if ($deniedToolResults.Count -eq 1) {
            $deniedToolResults[0].payload.payload.entry.content.content |
                ConvertFrom-Json
        } else {
            $null
        }
        if ($deniedToolResults.Count -ne 1 -or
            $deniedProjection.error.code -ne "permission_denied") {
            $details = $deniedToolResults | ConvertTo-Json -Depth 20
            throw "model context did not receive the structured denial`n$details"
        }

        $batchWorkspace = Join-Path $runRoot "approval-batch-workspace"
        New-Item -ItemType Directory -Path (Join-Path $batchWorkspace "src") `
            -Force | Out-Null
        Set-Content -LiteralPath (Join-Path $batchWorkspace "src\lib.rs") `
            -Value "pub fn batch_fixture() {}"
        $batchSession = & $cli session create --workspace $batchWorkspace `
            --style persistent-chat --json | ConvertFrom-Json
        $batchTurn = & $cli run "approve the write, then finish the batch" `
            --session $batchSession.session_id `
            --option 'mock_scenario="approval_multi"' --json | ConvertFrom-Json
        if ([string]::IsNullOrWhiteSpace($batchTurn.awaiting_continuation)) {
            throw "approval-bearing batch did not pause"
        }
        $batchResolved = & $cli approval resolve $batchSession.session_id `
            $batchTurn.awaiting_continuation approve --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or $batchResolved.transitioned -ne $true -or
            $batchResolved.awaiting_continuation) {
            throw "approval-bearing batch did not resume to completion"
        }
        $batchJournalPath = Join-Path $runRoot (
            "sessions\" + $batchSession.session_id + "\events.jsonl"
        )
        $batchEvents = @(Get-Content $batchJournalPath | ForEach-Object {
            ($_ | ConvertFrom-Json).event
        })
        if (@($batchEvents | Where-Object {
            $_.metadata.event_type -eq "tool.execution_started"
        }).Count -ne 2 -or @($batchEvents | Where-Object {
            $_.metadata.event_type -eq "conversation.entry_committed" -and
            $_.payload.payload.entry.kind -eq "tool_result"
        }).Count -ne 2) {
            throw "remaining sibling tool call was lost behind approval"
        }
        if ((Get-Content -LiteralPath (
            Join-Path $batchWorkspace "approved.txt"
        ) -Raw) -ne "batch approved`n") {
            throw "approved batch write did not execute"
        }
        Write-Output "durable restart-safe exactly-once tool approval E2E passed"
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
                    "agentmod-approval-e2e-"
                )) {
                Remove-Item -LiteralPath $resolvedRun -Recurse -Force
            }
        }
    }
}
finally {
    Pop-Location
}
