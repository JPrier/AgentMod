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
    $sqliteStyle = (
        Get-Content tests\fixtures\styles\persistent-file-none.toml -Raw
    ).Replace(
        'id = "e2e-persistent-file"',
        'id = "e2e-persistent-sqlite"'
    ).Replace('provider = "file"', 'provider = "sqlite-fts"')
    Set-Content -LiteralPath (
        Join-Path $styleRoot "persistent-sqlite-none.toml"
    ) -Value $sqliteStyle -NoNewline
    $boundedFileStyle = (
        Get-Content tests\fixtures\styles\persistent-file-none.toml -Raw
    ).Replace(
        'id = "e2e-persistent-file"',
        'id = "e2e-persistent-file-bounded"'
    ).Replace("max_items = 8", "max_items = 1").Replace(
        "max_injected_bytes = 16384",
        "max_injected_bytes = 1024"
    ).Replace("max_query_bytes = 16384", "max_query_bytes = 12")
    Set-Content -LiteralPath (
        Join-Path $styleRoot "persistent-file-bounded.toml"
    ) -Value $boundedFileStyle -NoNewline
    $tightProjectionStyle = (
        Get-Content tests\fixtures\styles\persistent-none-sliding.toml -Raw
    ).Replace(
        'id = "e2e-persistent-sliding"',
        'id = "e2e-persistent-tight"'
    ).Replace("reserved_context_tokens = 1024", "reserved_context_tokens = 128"
    ).Replace("max_provider_projection_tokens = 8192",
        "max_provider_projection_tokens = 256")
    Set-Content -LiteralPath (
        Join-Path $styleRoot "persistent-tight.toml"
    ) -Value $tightProjectionStyle -NoNewline
    $unsupportedTimingStyle = (
        Get-Content tests\fixtures\styles\persistent-file-none.toml -Raw
    ).Replace(
        'id = "e2e-persistent-file"',
        'id = "e2e-persistent-context-node"'
    ).Replace(
        'retrieval_timing = "before_model_request"',
        'retrieval_timing = "context_node"'
    )
    Set-Content -LiteralPath (
        Join-Path $styleRoot "persistent-context-node.toml"
    ) -Value $unsupportedTimingStyle -NoNewline

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
        $normalize = {
            param($entries)
            @($entries | ForEach-Object {
                $copy = $_ | ConvertTo-Json -Depth 20 | ConvertFrom-Json
                $copy.content.PSObject.Properties.Remove("id")
                $copy.content.PSObject.Properties.Remove("source_sequence")
                $copy | ConvertTo-Json -Depth 20 -Compress
            })
        }
        $leftTyped = & $normalize $left
        $rightTyped = & $normalize $right
        if ((Compare-Object $leftTyped $rightTyped -SyncWindow 0).Count -ne 0) {
            throw "compaction changed canonical conversation history"
        }
    }

    function Seed-Memory($provider, $path, $scope, $source, $content, $createdAt) {
        $env:AGENTMOD_TEST_MEMORY_PROVIDER = $provider
        $env:AGENTMOD_TEST_MEMORY_PATH = $path
        $env:AGENTMOD_TEST_MEMORY_SCOPE = $scope
        $env:AGENTMOD_TEST_MEMORY_SOURCE = $source
        $env:AGENTMOD_TEST_MEMORY_CONTENT = $content
        $env:AGENTMOD_TEST_MEMORY_CREATED_AT_MS = [string]$createdAt
        cargo test -p agentmod-runtime-dependency --locked `
            --test e2e_memory_seed `
            seed_file_memory_for_process_e2e -- --ignored --exact
        if ($LASTEXITCODE -ne 0) { throw "$provider memory seed fixture failed" }
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
            "persistent-file-none.toml",
            "persistent-none-none.toml",
            "persistent-none-sliding.toml",
            (Join-Path $styleRoot "persistent-sqlite-none.toml"),
            (Join-Path $styleRoot "persistent-file-bounded.toml"),
            (Join-Path $styleRoot "persistent-tight.toml"),
            (Join-Path $styleRoot "persistent-context-node.toml")
        )) {
            $fixturePath = if ([System.IO.Path]::IsPathRooted($fixture)) {
                $fixture
            } else {
                Join-Path "tests\fixtures\styles" $fixture
            }
            $validation = & $cli style validate $fixturePath --json |
                ConvertFrom-Json
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
        $isolatedFileSession = & $cli session create --workspace $workspace `
            --style e2e-persistent-file --json | ConvertFrom-Json
        $sqliteSession = & $cli session create --workspace $workspace `
            --style e2e-persistent-sqlite --json | ConvertFrom-Json
        $boundedFileSession = & $cli session create --workspace $workspace `
            --style e2e-persistent-file-bounded --json | ConvertFrom-Json
        $tightSession = & $cli session create --workspace $workspace `
            --style e2e-persistent-tight --json | ConvertFrom-Json
        $unsupportedTimingSession = & $cli session create --workspace $workspace `
            --style e2e-persistent-context-node --json | ConvertFrom-Json
        $noneSession = & $cli session create --workspace $workspace `
            --style e2e-persistent-none --json | ConvertFrom-Json
        $noneCompactionSession = & $cli session create --workspace $workspace `
            --style e2e-persistent-none --json | ConvertFrom-Json
        $slidingSession = & $cli session create --workspace $workspace `
            --style e2e-persistent-sliding --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "style-bound session creation failed" }

        # Use the real checksum-protected dependency adapter for setup. The
        # fixture is ignored in default test runs and cannot enter production.
        Seed-Memory "file" (Join-Path $runRoot "memory\file.jsonl") `
            ("session:" + $fileSession.session_id) "process-e2e-fixture" `
            "orchid memory probe retained only for the file-backed session" 1000
        Seed-Memory "sqlite-fts" (
            Join-Path $runRoot "memory\sqlite-fts.sqlite3"
        ) ("session:" + $sqliteSession.session_id) "sqlite-e2e-fixture" `
            "orchid memory probe retained only for the sqlite-backed session" 1001
        Seed-Memory "file" (Join-Path $runRoot "memory\file.jsonl") `
            ("session:" + $boundedFileSession.session_id) "bounded-e2e-one" `
            "orchid probe first bounded record" 1002
        Seed-Memory "file" (Join-Path $runRoot "memory\file.jsonl") `
            ("session:" + $boundedFileSession.session_id) "bounded-e2e-two" `
            "orchid probe second bounded record" 1003

        $fileTurn = & $cli run "orchid memory probe" `
            --session $fileSession.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="context-output"' --json | ConvertFrom-Json
        $noneTurn = & $cli run "orchid memory probe" `
            --session $noneSession.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="context-output"' --json | ConvertFrom-Json
        $isolatedFileTurn = & $cli run "orchid memory probe" `
            --session $isolatedFileSession.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="context-output"' --json | ConvertFrom-Json
        $sqliteTurn = & $cli run "orchid memory probe" `
            --session $sqliteSession.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="context-output"' --json | ConvertFrom-Json
        & $cli run "orchid probe and deliberately longer query text" `
            --session $boundedFileSession.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="bounded-output"' --json | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "bounded memory turn failed" }
        $savedErrorPreference = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            & $cli run ("oversized-current-input-" + ("x" * 1024)) `
                --session $tightSession.session_id `
                --option 'mock_scenario="streaming_text"' --json 2>$null | Out-Null
            $oversizedExit = $LASTEXITCODE
            & $cli run "unsupported timing must fail preflight" `
                --session $unsupportedTimingSession.session_id `
                --option 'mock_scenario="streaming_text"' --json 2>$null | Out-Null
            $unsupportedExit = $LASTEXITCODE
        }
        finally {
            $ErrorActionPreference = $savedErrorPreference
        }
        if ($oversizedExit -eq 0) {
            throw "oversized first projection was dispatched instead of failing closed"
        }
        if ($unsupportedExit -eq 0) {
            throw "unsupported context-node timing executed without a lifecycle hook"
        }
        $global:LASTEXITCODE = 0
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
        $isolatedInspection = & $cli session inspect `
            $isolatedFileSession.session_id --json | ConvertFrom-Json
        $sqliteInspection = & $cli session inspect $sqliteSession.session_id --json |
            ConvertFrom-Json
        $boundedInspection = & $cli session inspect `
            $boundedFileSession.session_id --json | ConvertFrom-Json
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
        if (@($isolatedInspection.state.conversation.provider_projection |
                Where-Object kind -eq "retrieved_memory").Count -ne 0) {
            throw "file memory crossed its session scope"
        }
        $sqliteRetrieved = @(
            $sqliteInspection.state.conversation.provider_projection |
                Where-Object kind -eq "retrieved_memory"
        )
        if ($sqliteRetrieved.Count -ne 1 -or
            $sqliteRetrieved[0].content.provider -ne "sqlite-fts" -or
            $sqliteRetrieved[0].content.scope -ne (
                "session:" + $sqliteSession.session_id
            ) -or
            $sqliteRetrieved[0].content.source -ne "sqlite-e2e-fixture") {
            throw "SQLite FTS memory was not routed and scoped per session"
        }
        $boundedRetrieved = @(
            $boundedInspection.state.conversation.provider_projection |
                Where-Object kind -eq "retrieved_memory"
        )
        if ($boundedRetrieved.Count -ne 1 -or
            $boundedRetrieved[0].content.query -ne "orchid probe") {
            throw "compiled memory item/query bounds were not enforced"
        }
        $boundedBytes = [Text.Encoding]::UTF8.GetByteCount(
            ($boundedRetrieved[0] | ConvertTo-Json -Depth 20 -Compress)
        )
        if ($boundedBytes -gt 1024 -or
            $boundedRetrieved[0].content.size_bytes -ne $boundedBytes) {
            throw "serialized memory entry contribution was not byte-bounded"
        }
        $tightJournal = Read-Journal $tightSession.session_id
        if ((Events-Of-Type $tightJournal "model.request_proposed").Count -ne 0) {
            throw "oversized first projection crossed the provider proposal boundary"
        }
        $unsupportedJournal = @(Read-Journal $unsupportedTimingSession.session_id)
        if ($unsupportedJournal.Count -ne 1) {
            $unsupportedTypes = @($unsupportedJournal | ForEach-Object {
                $_.event.metadata.event_type
            }) -join ","
            throw (
                "unsupported retrieval timing mutated the journal during preflight: " +
                $unsupportedTypes
            )
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
        $fileContext = @(Events-Of-Type $fileJournal "context.projection_replaced")
        $noneContext = @(Events-Of-Type $noneJournal "context.projection_replaced")
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

        # Restart with all three provider selections dormant. Inspection and a
        # subsequent retrieval must reconstruct selection and provenance from
        # durable metadata/events rather than retained process state.
        Stop-Process -Id $daemon.Id -Force
        $daemon.WaitForExit()
        $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden `
            -RedirectStandardError $runtimeStderr -PassThru
        Wait-RuntimeReady
        foreach ($expected in @(
            @($fileSession.session_id, "file"),
            @($isolatedFileSession.session_id, "file"),
            @($sqliteSession.session_id, "sqlite-fts"),
            @($noneSession.session_id, "none")
        )) {
            $afterRestart = & $cli session inspect $expected[0] --json |
                ConvertFrom-Json
            if ($afterRestart.state.style_binding.memory.provider -ne $expected[1] -or
                $afterRestart.state.style_compatibility.status -ne "compatible") {
                throw "memory selection did not survive restart/replay"
            }
        }
        & $cli run "orchid memory probe" --session $fileSession.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="file-after-restart"' --json | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "file memory turn after restart failed" }
        & $cli run "orchid memory probe" --session $sqliteSession.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="sqlite-after-restart"' --json | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "sqlite memory turn after restart failed" }
        $fileAfterRestart = & $cli session inspect $fileSession.session_id --json |
            ConvertFrom-Json
        $sqliteAfterRestart = & $cli session inspect $sqliteSession.session_id --json |
            ConvertFrom-Json
        $fileRestartMemory = @(
            $fileAfterRestart.state.conversation.provider_projection |
                Where-Object kind -eq "retrieved_memory"
        )
        $sqliteRestartMemory = @(
            $sqliteAfterRestart.state.conversation.provider_projection |
                Where-Object kind -eq "retrieved_memory"
        )
        if ($fileRestartMemory.Count -ne 1 -or
            $fileRestartMemory[0].content.provider -ne "file" -or
            $fileRestartMemory[0].content.source -ne "process-e2e-fixture" -or
            [string]::IsNullOrWhiteSpace(
                $fileRestartMemory[0].content.injection_event
            )) {
            throw "file memory retrieval did not recover after restart"
        }
        if ($sqliteRestartMemory.Count -ne 1 -or
            $sqliteRestartMemory[0].content.provider -ne "sqlite-fts" -or
            $sqliteRestartMemory[0].content.source -ne "sqlite-e2e-fixture" -or
            [string]::IsNullOrWhiteSpace(
                $sqliteRestartMemory[0].content.injection_event
            )) {
            throw "sqlite memory retrieval did not recover after restart"
        }

        # A branch inherits the parent's explicit projection at the fork, then
        # its newly selected no-memory style must remove inherited memory
        # before proposing a model request. The parent projection is immutable.
        $parentBeforeBranch = & $cli session inspect $fileSession.session_id --json |
            ConvertFrom-Json
        $branch = & $cli session branch $fileSession.session_id `
            --at $parentBeforeBranch.state.last_sequence `
            --style e2e-persistent-none --json | ConvertFrom-Json
        $branchBeforeTurn = & $cli session inspect $branch.session_id --json |
            ConvertFrom-Json
        if (@($branchBeforeTurn.state.conversation.provider_projection |
                Where-Object kind -eq "retrieved_memory").Count -ne 1) {
            throw "branch fixture did not inherit the parent memory projection"
        }
        & $cli run "branch must not inherit parent memory" `
            --session $branch.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="branch-output"' --json | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "no-memory branch turn failed" }
        $branchAfterTurn = & $cli session inspect $branch.session_id --json |
            ConvertFrom-Json
        $parentAfterBranch = & $cli session inspect $fileSession.session_id --json |
            ConvertFrom-Json
        if (@($branchAfterTurn.state.conversation.provider_projection |
                Where-Object kind -eq "retrieved_memory").Count -ne 0) {
            throw "no-memory branch leaked inherited parent memory"
        }
        if (($parentBeforeBranch.state.conversation.provider_projection |
                ConvertTo-Json -Depth 20 -Compress) -ne
            ($parentAfterBranch.state.conversation.provider_projection |
                ConvertTo-Json -Depth 20 -Compress)) {
            throw "branch context normalization mutated the parent"
        }
        $branchJournal = Read-Journal $branch.session_id
        $branchCleanup = @(Events-Of-Type $branchJournal `
            "context.projection_replaced" | Where-Object {
                $_.event.payload.payload.provenance.method -eq "memory:none"
            })
        $branchProposal = (Events-Of-Type $branchJournal "model.request_proposed")[0]
        if ($branchCleanup.Count -ne 1 -or
            $branchCleanup[0].event.metadata.sequence -ge
                $branchProposal.event.metadata.sequence) {
            throw "branch memory cleanup was not canonical before model proposal"
        }

        # Give both compaction sessions identical canonical content and
        # projection pressure. The low style trigger activates sliding-window
        # while the none strategy remains untouched.
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
        $firstProposal = (Events-Of-Type $slidingJournal "model.request_proposed")[0]
        if ($compactions[0].event.metadata.sequence -ge
            $firstProposal.event.metadata.sequence) {
            throw "first projection pressure was not compacted before dispatch"
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
        $lastBoundary = @(Events-Of-Type $slidingJournal `
            "context.boundary_completed" | Where-Object {
                $_.event.payload.payload.identity.boundary -eq
                    "before_model_request"
            })[-1]
        if ($null -eq $lastBoundary -or
            $lastBoundary.event.payload.payload.estimated_tokens -gt
                (8192 - 1024) -or
            $lastBoundary.event.payload.payload.serialized_bytes -gt
                (16 * 1024 * 1024)) {
            throw "token projection limit or independent byte cap was not enforced"
        }

        Write-Output (
            "runtime style-selected memory/compaction process E2E passed"
        )
    }
    finally {
        foreach ($name in @(
            "AGENTMOD_TEST_MEMORY_PROVIDER",
            "AGENTMOD_TEST_MEMORY_PATH",
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
