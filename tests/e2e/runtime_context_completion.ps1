$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli -p agentmod-scheduler
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-context-completion-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    $styleRoot = Join-Path $runRoot "styles\user"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null
    New-Item -ItemType Directory -Path $styleRoot -Force | Out-Null
    Copy-Item tests\fixtures\styles\persistent-file-summary.toml $styleRoot
    Copy-Item tests\fixtures\styles\persistent-file-artifact.toml $styleRoot
    Copy-Item tests\fixtures\styles\persistent-file-auto-write.toml $styleRoot
    Copy-Item tests\fixtures\styles\persistent-none-none.toml $styleRoot

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-context-completion-e2e-" +
        [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $runtimeStderr = Join-Path $runRoot "runtime.stderr.log"

    function Read-Journal($sessionId) {
        $path = Join-Path $runRoot ("sessions\" + $sessionId + "\events.jsonl")
        return @(Get-Content -LiteralPath $path | ForEach-Object {
            $_ | ConvertFrom-Json
        })
    }

    function Events-Of-Type($journal, $eventType) {
        return @($journal | Where-Object {
            $_.event.metadata.event_type -eq $eventType
        })
    }

    function Wait-RuntimeReady {
        for ($attempt = 0; $attempt -lt 80; $attempt++) {
            try {
                & $cli doctor --json 2>$null | Out-Null
                if ($LASTEXITCODE -eq 0) { return }
            } catch {}
            Start-Sleep -Milliseconds 100
        }
        throw "runtime did not become ready"
    }

    $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
        -WorkingDirectory $runRoot -WindowStyle Hidden `
        -RedirectStandardError $runtimeStderr -PassThru
    try {
        Wait-RuntimeReady

        foreach ($fixture in @(
            "persistent-file-summary.toml",
            "persistent-file-artifact.toml",
            "persistent-file-auto-write.toml",
            "persistent-none-none.toml"
        )) {
            $validation = & $cli style validate `
                (Join-Path $styleRoot $fixture) --json | ConvertFrom-Json
            if ($LASTEXITCODE -ne 0 -or -not $validation.valid) {
                $detail = $validation | ConvertTo-Json -Depth 10 -Compress
                throw "style fixture failed runtime validation: $fixture`n$detail"
            }
        }

        $summarySession = & $cli session create --workspace $workspace `
            --style e2e-persistent-summary --json | ConvertFrom-Json
        $artifactSession = & $cli session create --workspace $workspace `
            --style e2e-persistent-artifact --json | ConvertFrom-Json
        $autoWriteSession = & $cli session create --workspace $workspace `
            --style e2e-persistent-auto-write --json | ConvertFrom-Json
        $noneSession = & $cli session create --workspace $workspace `
            --style e2e-persistent-none --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "session creation failed" }

        function Run-Turn($sessionId, $prompt, $output) {
            & $cli run $prompt --session $sessionId `
                --option 'mock_scenario="streaming_text"' `
                --option ('mock_text="' + $output + '"') --json | ConvertFrom-Json
            if ($LASTEXITCODE -ne 0) {
                $runtimeDetail = Get-Content $runtimeStderr `
                    -ErrorAction SilentlyContinue | Select-Object -Last 10
                $journalDetail = @(Read-Journal $sessionId | Select-Object -Last 8 | `
                    ForEach-Object {
                        $t = $_.event.metadata.event_type
                        if ($t -eq "context.summary_failed" -or $t -eq "context.artifact_failed") {
                            ($t + "=" + $_.event.payload.payload.code + ":" + $_.event.payload.payload.message)
                        } elseif ($t -eq "style.node_failed") {
                            ($t + "=" + $_.event.payload.payload.reason)
                        } else {
                            $t
                        }
                    }) -join ","
                throw (
                    "turn failed for $sessionId`n" +
                    "journal tail: $journalDetail`n$runtimeDetail"
                )
            }
        }

        # Drive projection pressure until the low summary/artifact triggers
        # fire, while the none session accumulates an identical transcript.
        $summaryCompacted = $false
        $artifactCompacted = $false
        for ($turnNumber = 1; $turnNumber -le 22; $turnNumber++) {
            $prompt = "equivalent compaction pressure turn $turnNumber"
            $output = "stable-output-$turnNumber"
            Run-Turn $summarySession.session_id $prompt $output
            Run-Turn $artifactSession.session_id $prompt $output
            Run-Turn $noneSession.session_id $prompt $output
            Run-Turn $autoWriteSession.session_id $prompt $output
            $summaryJournal = Read-Journal $summarySession.session_id
            $artifactJournal = Read-Journal $artifactSession.session_id
            $summaryCompacted = (Events-Of-Type $summaryJournal `
                "context.summary_completed").Count -gt 0
            $artifactCompacted = (Events-Of-Type $artifactJournal `
                "context.artifact_completed").Count -gt 0
            if ($summaryCompacted -and $artifactCompacted) { break }
        }
        if (-not $summaryCompacted) {
            throw "typed-summary compaction never executed live"
        }
        if (-not $artifactCompacted) {
            throw "artifact-handoff compaction never executed live"
        }

        $summaryInspection = & $cli session inspect `
            $summarySession.session_id --json | ConvertFrom-Json
        $artifactInspection = & $cli session inspect `
            $artifactSession.session_id --json | ConvertFrom-Json
        $noneInspection = & $cli session inspect `
            $noneSession.session_id --json | ConvertFrom-Json

        # Summary projection is bounded, typed, and never a fake user message.
        $summaryProjection = @(
            $summaryInspection.state.conversation.provider_projection
        )
        $summaryEntries = @($summaryProjection | Where-Object {
            $_.kind -eq "context_summary"
        })
        if ($summaryEntries.Count -lt 1 -or
            [string]::IsNullOrWhiteSpace($summaryEntries[0].content.text) -or
            $summaryInspection.state.conversation.projection_provenance.method -ne
                "summary") {
            throw "typed summary was not committed as a bounded typed entry"
        }
        if ($summaryInspection.state.conversation.projection_provenance.method -ne
                "summary") {
            throw "summary provenance is missing"
        }
        $summaryUserMessages = @(
            $summaryInspection.state.conversation.history |
                Where-Object kind -eq "user_message"
        )
        if ($summaryUserMessages.Count -lt 10) {
            throw "summary compaction removed canonical user history"
        }
        $summaryJ = Read-Journal $summarySession.session_id
        $summaryOutbox = @(Events-Of-Type $summaryJ "context.summary_completed")
        if ($summaryOutbox.Count -lt 1) {
            throw "summary outbox has no terminal provider evidence"
        }
        $summaryBoundary = @(Events-Of-Type $summaryJ `
            "context.boundary_completed" | Where-Object {
                $_.event.payload.payload.identity.boundary -eq
                    "before_model_request"
            })[-1]
        if ($null -eq $summaryBoundary -or
            $summaryBoundary.event.payload.payload.serialized_bytes -gt
                (16 * 1024 * 1024)) {
            throw "summary compacted projection exceeded the byte cap"
        }
        # The summary provider call is a normal model request with usage
        # evidence retained in the outbox.
        if ($summaryOutbox[-1].event.payload.payload.input_tokens -le 0 -or
            $summaryOutbox[-1].event.payload.payload.content_hash.Length -ne 64) {
            throw "summary terminal evidence lacks usage or content hash"
        }

        # Artifact handoff: a bounded typed reference replaces the projection.
        $artifactProjection = @(
            $artifactInspection.state.conversation.provider_projection
        )
        $artifactReferences = @($artifactProjection | Where-Object {
            $_.kind -eq "artifact_reference"
        })
        if ($artifactReferences.Count -lt 1 -or
            $artifactReferences[0].content.mime_type -ne
                "application/vnd.agentmod.context+json" -or
            $artifactInspection.state.conversation.projection_provenance.method -ne
                "artifact_handoff") {
            throw "artifact handoff reference was not committed"
        }
        $artifactJ = Read-Journal $artifactSession.session_id
        $artifactOutbox = @(Events-Of-Type $artifactJ "context.artifact_completed")
        if ($artifactOutbox.Count -lt 1 -or
            [string]::IsNullOrWhiteSpace(
                $artifactOutbox[0].event.payload.payload.artifact_id
            )) {
            throw "artifact outbox has no terminal artifact receipt"
        }
        $artifactFiles = @(Get-ChildItem -Recurse -File (
            Join-Path $runRoot ("sessions\" + $artifactSession.session_id +
                "\artifacts")
        ) -ErrorAction SilentlyContinue)
        if ($artifactFiles.Count -lt 1) {
            throw "artifact handoff did not persist an immutable artifact"
        }
        if (@($artifactInspection.state.conversation.history |
                Where-Object kind -eq "user_message").Count -lt 10) {
            throw "artifact handoff removed canonical user history"
        }
        if (@($artifactProjection | Where-Object {
                $_.kind -eq "user_message" -and
                $_.content.text -like "equivalent compaction pressure*"
            }).Count -eq 0) {
            throw "artifact handoff did not preserve the current user input"
        }

        # Provider behavior is identical across compaction strategies.
        $noneText = @(
            $noneInspection.state.conversation.history |
                Where-Object kind -eq "assistant_message" |
                ForEach-Object { $_.content.text }
        ) -join ""
        $summaryText = @(
            $summaryInspection.state.conversation.history |
                Where-Object kind -eq "assistant_message" |
                ForEach-Object { $_.content.text }
        ) -join ""
        if ($noneText -ne $summaryText) {
            throw "summary compaction changed canonical provider behavior"
        }

        # Automatic memory writes follow the canonical outbox with a real
        # provider receipt and restart-safe deduplication.
        $autoJournal = Read-Journal $autoWriteSession.session_id
        $writeProposals = @(Events-Of-Type $autoJournal "memory.write_proposed")
        $writeCompletions = @(Events-Of-Type $autoJournal "memory.write_completed")
        if ($writeCompletions.Count -lt 1 -or
            $writeProposals[0].event.payload.payload.trigger -ne
                "turn_completion") {
            throw "automatic memory write was not proposed at turn completion"
        }
        if ($writeCompletions[0].event.payload.payload.retained -ne $true -or
            [string]::IsNullOrWhiteSpace(
                $writeCompletions[0].event.payload.payload.reference
            )) {
            throw "automatic memory write lacks a provider receipt"
        }
        $memoryFile = Join-Path $runRoot "memory\file.jsonl"
        $memoryContent = Get-Content -LiteralPath $memoryFile -Raw `
            -ErrorAction Stop
        if (-not $memoryContent.Contains("stable-output-")) {
            throw "automatic memory write did not reach the memory provider"
        }
        # Re-running the identical turn produces identical content; the
        # canonical write identity must deduplicate the provider write.
        Run-Turn $autoWriteSession.session_id "auto-write dedup probe" "dedup-output"
        $afterFirst = (Get-Content -LiteralPath $memoryFile).Count
        Run-Turn $autoWriteSession.session_id "auto-write dedup probe" "dedup-output"
        $afterSecond = (Get-Content -LiteralPath $memoryFile).Count
        if ($afterSecond -ne $afterFirst) {
            throw "automatic memory write duplicated identical canonical content"
        }
        $dedupJournal = Read-Journal $autoWriteSession.session_id
        $dedupCompletions = @(Events-Of-Type $dedupJournal "memory.write_completed")
        $dedupProposals = @(Events-Of-Type $dedupJournal "memory.write_proposed" | Where-Object {
            $_.event.payload.payload.trigger -eq "turn_completion"
        })
        # The identical turn is deduplicated canonically: its write identity is
        # already terminal, so no new proposal or receipt is committed.
        $afterSecondTurnProposals = @(
            Events-Of-Type $dedupJournal "memory.write_proposed" | Where-Object {
                $_.event.payload.payload.trigger -eq "turn_completion"
            }
        )
        if ($afterSecondTurnProposals.Count -ne $dedupProposals.Count -or
            $afterSecondTurnProposals.Count -ne $dedupCompletions.Count) {
            throw "automatic memory write deduplication evidence is missing"
        }

        # Restart: selection, summary evidence, artifact receipts, and memory
        # write receipts must survive; a subsequent turn still executes.
        Stop-Process -Id $daemon.Id -Force
        $daemon.WaitForExit()
        $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden `
            -RedirectStandardError $runtimeStderr -PassThru
        Wait-RuntimeReady
        foreach ($expected in @(
            @($summarySession.session_id, "summary"),
            @($artifactSession.session_id, "artifact_handoff"),
            @($autoWriteSession.session_id, "none")
        )) {
            $afterRestart = & $cli session inspect $expected[0] --json |
                ConvertFrom-Json
            if ($afterRestart.state.style_compatibility.status -ne "compatible" -or
                $afterRestart.state.style_binding.compaction.strategy -ne
                    $expected[1]) {
                throw "compaction selection did not survive restart for $($expected[0])"
            }
        }
        $summaryAfterRestart = & $cli session inspect `
            $summarySession.session_id --json | ConvertFrom-Json
        if ($summaryAfterRestart.state.conversation.projection_provenance.method -ne
                "summary") {
            throw "summary projection did not survive restart"
        }
        Run-Turn $summarySession.session_id "after-restart summary turn" "post-restart"
        Run-Turn $artifactSession.session_id "after-restart artifact turn" "post-restart"
        if ($LASTEXITCODE -ne 0) { throw "post-restart turn failed" }

        # A branch inherits the compacted parent projection and continues
        # without mutating the parent.
        $artifactBeforeBranch = & $cli session inspect `
            $artifactSession.session_id --json | ConvertFrom-Json
        $branch = & $cli session branch $artifactSession.session_id `
            --at $artifactBeforeBranch.state.last_sequence --json |
            ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "branch failed" }
        Run-Turn $branch.session_id "branch after artifact handoff" "branch-output"
        $branchInspection = & $cli session inspect $branch.session_id --json |
            ConvertFrom-Json
        $parentAfter = & $cli session inspect $artifactSession.session_id --json |
            ConvertFrom-Json
        if (($artifactBeforeBranch.state.conversation.provider_projection |
                ConvertTo-Json -Depth 20 -Compress) -ne
            ($parentAfter.state.conversation.provider_projection |
                ConvertTo-Json -Depth 20 -Compress)) {
            throw "branch context continuation mutated the compacted parent"
        }
        if (@($branchInspection.state.conversation.history |
                Where-Object kind -eq "user_message").Count -lt 1) {
            throw "branch did not continue the compacted conversation"
        }

        Write-Output "runtime context completion (summary/artifact/auto-write) process E2E passed"
    }
    finally {
        if ($null -ne $daemon -and -not $daemon.HasExited) {
            Stop-Process -Id $daemon.Id -Force
            $daemon.WaitForExit()
        }
        if (Test-Path -LiteralPath $runRoot) {
            $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
            $resolvedRun = (Resolve-Path $runRoot).Path
            if ($resolvedRun.StartsWith($resolvedTemp) -and
                (Split-Path $resolvedRun -Leaf).StartsWith(
                    "agentmod-context-completion-e2e-"
                )) {
                Remove-Item -LiteralPath $resolvedRun -Recurse -Force
            }
        }
    }
}
finally {
    Pop-Location
}
