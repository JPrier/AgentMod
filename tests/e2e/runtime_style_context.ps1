$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-style-context-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    $styleRoot = Join-Path $runRoot "styles\user"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null
    New-Item -ItemType Directory -Path $styleRoot -Force | Out-Null
    Copy-Item tests\fixtures\styles\persistent-file-none.toml $styleRoot
    Copy-Item tests\fixtures\styles\persistent-none-none.toml $styleRoot
    Copy-Item tests\fixtures\styles\persistent-none-sliding.toml $styleRoot

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-style-context-e2e-" +
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

    function Assert-Same-Conversation-History($left, $right) {
        $leftSemantic = @($left | ForEach-Object {
            $_.kind + "|" + $_.content.text
        })
        $rightSemantic = @($right | ForEach-Object {
            $_.kind + "|" + $_.content.text
        })
        if ((Compare-Object $leftSemantic $rightSemantic -SyncWindow 0).Count -ne 0) {
            throw "compaction changed canonical conversation history"
        }
    }

    $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
        -WorkingDirectory $runRoot -WindowStyle Hidden `
        -RedirectStandardError $runtimeStderr -PassThru
    try {
        $ready = $false
        for ($attempt = 0; $attempt -lt 80; $attempt++) {
            & $cli doctor --json 2>$null | Out-Null
            if ($LASTEXITCODE -eq 0) {
                $ready = $true
                break
            }
            Start-Sleep -Milliseconds 100
        }
        if (-not $ready) { throw "runtime did not become ready" }

        foreach ($fixture in @(
            "persistent-file-none.toml",
            "persistent-none-none.toml",
            "persistent-none-sliding.toml"
        )) {
            $validation = & $cli style validate (
                Join-Path "tests\fixtures\styles" $fixture
            ) --json | ConvertFrom-Json
            if ($LASTEXITCODE -ne 0 -or -not $validation.valid) {
                $detail = $validation | ConvertTo-Json -Depth 10 -Compress
                throw (
                    "style fixture failed runtime validation: " +
                    "$fixture`n$detail"
                )
            }
        }

        $fileSession = & $cli session create --workspace $workspace `
            --style e2e-persistent-file --json | ConvertFrom-Json
        $noneSession = & $cli session create --workspace $workspace `
            --style e2e-persistent-none --json | ConvertFrom-Json
        $noneCompactionSession = & $cli session create --workspace $workspace `
            --style e2e-persistent-none --json | ConvertFrom-Json
        $slidingSession = & $cli session create --workspace $workspace `
            --style e2e-persistent-sliding --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "style-bound session creation failed" }

        # Use the real checksum-protected dependency adapter for setup. The
        # fixture is ignored in default test runs and cannot enter production.
        $env:AGENTMOD_TEST_MEMORY_FILE = Join-Path $runRoot "memory\file.jsonl"
        $env:AGENTMOD_TEST_MEMORY_SCOPE = (
            "session:" + $fileSession.session_id
        )
        $env:AGENTMOD_TEST_MEMORY_SOURCE = "process-e2e-fixture"
        $env:AGENTMOD_TEST_MEMORY_CONTENT = (
            "orchid memory probe retained only for the file-backed session"
        )
        $env:AGENTMOD_TEST_MEMORY_CREATED_AT_MS = "1000"
        cargo test -p agentmod-runtime-dependency --locked `
            --test e2e_memory_seed `
            seed_file_memory_for_process_e2e -- --ignored --exact
        if ($LASTEXITCODE -ne 0) { throw "memory seed fixture failed" }
        Remove-Item Env:\AGENTMOD_TEST_MEMORY_FILE
        Remove-Item Env:\AGENTMOD_TEST_MEMORY_SCOPE
        Remove-Item Env:\AGENTMOD_TEST_MEMORY_SOURCE
        Remove-Item Env:\AGENTMOD_TEST_MEMORY_CONTENT
        Remove-Item Env:\AGENTMOD_TEST_MEMORY_CREATED_AT_MS

        $fileTurn = & $cli run "orchid memory probe" `
            --session $fileSession.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="context-output"' --json | ConvertFrom-Json
        $noneTurn = & $cli run "orchid memory probe" `
            --session $noneSession.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="context-output"' --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "memory comparison turns failed" }
        $fileText = @($fileTurn.events | Where-Object event -eq "text" |
            ForEach-Object text) -join ""
        $noneText = @($noneTurn.events | Where-Object event -eq "text" |
            ForEach-Object text) -join ""
        if ($fileText -ne $noneText -or
            $fileText -ne "alpha beta context-output") {
            throw "deterministic provider behavior changed across memory selections"
        }

        $fileInspection = & $cli session inspect $fileSession.session_id --json |
            ConvertFrom-Json
        $noneInspection = & $cli session inspect $noneSession.session_id --json |
            ConvertFrom-Json
        $fileProjection = @(
            $fileInspection.state.conversation.provider_projection
        )
        $noneProjection = @(
            $noneInspection.state.conversation.provider_projection
        )
        $retrieved = @($fileProjection | Where-Object kind -eq "retrieved_memory")
        if ($retrieved.Count -ne 1 -or
            $retrieved[0].content.provider -ne "file" -or
            $retrieved[0].content.query -ne "orchid memory probe" -or
            $retrieved[0].content.scope -ne (
                "session:" + $fileSession.session_id
            ) -or
            $retrieved[0].content.source -ne "process-e2e-fixture" -or
            [string]::IsNullOrWhiteSpace($retrieved[0].content.reference) -or
            [string]::IsNullOrWhiteSpace($retrieved[0].content.injection_event)) {
            throw "file memory injection did not retain complete provenance"
        }
        if (@($noneProjection | Where-Object kind -eq "retrieved_memory").Count -ne 0) {
            throw "no-memory session received a memory injection"
        }
        $memoryIndex = -1
        $currentInputIndex = -1
        for ($index = 0; $index -lt $fileProjection.Count; $index++) {
            if ($fileProjection[$index].kind -eq "retrieved_memory") {
                $memoryIndex = $index
            }
            if ($fileProjection[$index].kind -eq "user_message" -and
                $fileProjection[$index].content.text -eq "orchid memory probe") {
                $currentInputIndex = $index
            }
        }
        if ($memoryIndex -lt 0 -or $currentInputIndex -lt 0 -or
            $memoryIndex -ge $currentInputIndex) {
            throw "before_current_input memory injection ordering was not applied"
        }

        $fileJournal = Read-Journal $fileSession.session_id
        $noneJournal = Read-Journal $noneSession.session_id
        $fileContext = Events-Of-Type $fileJournal "context.projection_replaced"
        $noneContext = Events-Of-Type $noneJournal "context.projection_replaced"
        if ($fileContext.Count -ne 1 -or
            $fileContext[0].event.payload.payload.provenance.method -ne "memory:file" -or
            $noneContext.Count -ne 0) {
            throw "memory context replacement was not canonical and style-selected"
        }
        $fileProposal = (Events-Of-Type $fileJournal "model.request_proposed")[-1]
        $noneProposal = (Events-Of-Type $noneJournal "model.request_proposed")[-1]
        if ($fileProposal.event.payload.payload.provider -ne
                $noneProposal.event.payload.payload.provider -or
            $fileProposal.event.payload.payload.model -ne
                $noneProposal.event.payload.payload.model -or
            $fileProposal.event.payload.payload.projection_hash -eq
                $noneProposal.event.payload.payload.projection_hash) {
            throw "memory did not alter only the provider-visible projection"
        }
        if ($fileContext[0].event.metadata.sequence -ge
            $fileProposal.event.metadata.sequence) {
            throw "memory projection was not committed before model proposal"
        }

        # Give both compaction sessions identical canonical content and
        # provider usage. The low style trigger activates sliding-window from
        # the second turn while the none strategy remains untouched.
        for ($turnNumber = 1; $turnNumber -le 18; $turnNumber++) {
            $prompt = "equivalent context pressure turn $turnNumber"
            $output = "stable-output-$turnNumber"
            $noneResult = & $cli run $prompt `
                --session $noneCompactionSession.session_id `
                --option 'mock_scenario="streaming_text"' `
                --option ('mock_text="' + $output + '"') --json |
                ConvertFrom-Json
            if ($LASTEXITCODE -ne 0) {
                $runtimeDetail = Get-Content $runtimeStderr `
                    -ErrorAction SilentlyContinue
                throw "none-compaction turn $turnNumber failed`n$runtimeDetail"
            }
            $slidingResult = & $cli run $prompt `
                --session $slidingSession.session_id `
                --option 'mock_scenario="streaming_text"' `
                --option ('mock_text="' + $output + '"') --json |
                ConvertFrom-Json
            if ($LASTEXITCODE -ne 0) {
                $runtimeDetail = Get-Content $runtimeStderr `
                    -ErrorAction SilentlyContinue
                $journalDetail = Read-Journal $slidingSession.session_id |
                    Select-Object -Last 8 |
                    ConvertTo-Json -Depth 20 -Compress
                throw (
                    "sliding-window turn $turnNumber failed`n" +
                    "$runtimeDetail`n$journalDetail"
                )
            }
            $noneVisible = @($noneResult.events |
                Where-Object event -eq "text" | ForEach-Object text) -join ""
            $slidingVisible = @($slidingResult.events |
                Where-Object event -eq "text" | ForEach-Object text) -join ""
            if ($noneVisible -ne $slidingVisible) {
                throw "provider behavior changed across compaction strategies"
            }
        }

        $noneAfter = & $cli session inspect `
            $noneCompactionSession.session_id --json |
            ConvertFrom-Json
        $slidingAfter = & $cli session inspect $slidingSession.session_id --json |
            ConvertFrom-Json
        $noneConversation = $noneAfter.state.conversation
        $slidingConversation = $slidingAfter.state.conversation
        Assert-Same-Conversation-History `
            @($noneConversation.history) @($slidingConversation.history)
        if (@($slidingConversation.provider_projection).Count -ge
                @($noneConversation.provider_projection).Count -or
            $slidingConversation.projection_provenance.method -ne "sliding_window") {
            throw "sliding-window style did not produce a bounded distinct projection"
        }

        $noneJournal = Read-Journal $noneCompactionSession.session_id
        $slidingJournal = Read-Journal $slidingSession.session_id
        if ((Events-Of-Type $noneJournal "context.projection_replaced").Count -ne 0) {
            throw "none compaction strategy replaced provider context"
        }
        $compactions = @(Events-Of-Type $slidingJournal "context.projection_replaced" |
            Where-Object {
                $_.event.payload.payload.provenance.method -eq "sliding_window"
            })
        if ($compactions.Count -lt 1) {
            throw "sliding-window compaction was not committed canonically"
        }
        $lastCompaction = $compactions[-1]
        $nextProposal = @(Events-Of-Type $slidingJournal "model.request_proposed" |
            Where-Object {
                $_.event.metadata.sequence -gt
                    $lastCompaction.event.metadata.sequence
            })[0]
        if ($null -eq $nextProposal) {
            throw "compacted projection was not sent through the normal model path"
        }

        Write-Output (
            "runtime style-selected memory/compaction process E2E passed"
        )
    }
    finally {
        foreach ($name in @(
            "AGENTMOD_TEST_MEMORY_FILE",
            "AGENTMOD_TEST_MEMORY_SCOPE",
            "AGENTMOD_TEST_MEMORY_SOURCE",
            "AGENTMOD_TEST_MEMORY_CONTENT",
            "AGENTMOD_TEST_MEMORY_CREATED_AT_MS"
        )) {
            Remove-Item ("Env:\" + $name) -ErrorAction SilentlyContinue
        }
        if ($null -ne $daemon -and -not $daemon.HasExited) {
            Stop-Process -Id $daemon.Id -Force
            $daemon.WaitForExit()
        }
        if (Test-Path -LiteralPath $runRoot) {
            $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
            $resolvedRun = (Resolve-Path $runRoot).Path
            if ($resolvedRun.StartsWith($resolvedTemp) -and
                (Split-Path $resolvedRun -Leaf).StartsWith(
                    "agentmod-style-context-e2e-"
                )) {
                Remove-Item -LiteralPath $resolvedRun -Recurse -Force
            }
        }
    }
}
finally {
    Pop-Location
}
