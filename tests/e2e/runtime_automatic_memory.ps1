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
        "agentmod-automatic-memory-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $succeeded = $false
    $workspace = Join-Path $runRoot "workspace"
    $styleRoot = Join-Path $runRoot "styles\user"
    New-Item -ItemType Directory -Path $workspace, $styleRoot -Force | Out-Null
    $style = (
        Get-Content tests\fixtures\styles\persistent-file-none.toml -Raw
    ).Replace(
        'id = "e2e-persistent-file"',
        'id = "e2e-automatic-memory"'
    ).Replace(
        'write_policy = "explicit_only"',
        'write_policy = "turn_completion"'
    )
    $stylePath = Join-Path $styleRoot "automatic-memory.toml"
    Set-Content -LiteralPath $stylePath -Value $style -NoNewline

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-automatic-memory-e2e-" +
        [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_MEMORY_WRITE_POST_PERSIST_DELAY_MS = "10000"
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

    function Start-Runtime {
        $script:daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden `
            -RedirectStandardError $runtimeErr -PassThru
        Wait-RuntimeReady
    }

    function Read-Journal($sessionId) {
        $journal = Join-Path $runRoot (
            "sessions\" + $sessionId + "\events.jsonl"
        )
        return @(Get-Content -LiteralPath $journal | ForEach-Object {
            $_ | ConvertFrom-Json
        })
    }

    function Event-Count($events, $eventType) {
        return @($events | Where-Object {
            $_.event.metadata.event_type -eq $eventType
        }).Count
    }

    function ConvertTo-WindowsArgument([AllowEmptyString()][string]$Value) {
        if ($null -eq $Value) { throw "process argument cannot be null" }
        if ($Value.Length -gt 0 -and $Value -notmatch '[\s"]') {
            return $Value
        }
        $quoted = [Text.StringBuilder]::new()
        [void]$quoted.Append('"')
        $backslashes = 0
        foreach ($character in $Value.ToCharArray()) {
            if ($character -eq [char]92) {
                $backslashes++
            }
            elseif ($character -eq '"') {
                [void]$quoted.Append([char]92, 2 * $backslashes + 1)
                [void]$quoted.Append('"')
                $backslashes = 0
            }
            else {
                [void]$quoted.Append([char]92, $backslashes)
                [void]$quoted.Append($character)
                $backslashes = 0
            }
        }
        [void]$quoted.Append([char]92, 2 * $backslashes)
        [void]$quoted.Append('"')
        return $quoted.ToString()
    }

    foreach ($case in @(
        @{ Input = ''; Expected = '""' },
        @{ Input = 'plain'; Expected = 'plain' },
        @{ Input = 'two words'; Expected = '"two words"' },
        @{ Input = 'a"b'; Expected = '"a\"b"' },
        @{ Input = 'C:\path with space\'; Expected = '"C:\path with space\\"' }
    )) {
        if ((ConvertTo-WindowsArgument $case.Input) -ne $case.Expected) {
            throw "Windows process argument quoting self-check failed"
        }
    }

    function Start-CutTurn($sessionId, $runId) {
        $stdout = Join-Path $runRoot "cut-turn.stdout.log"
        $stderr = Join-Path $runRoot "cut-turn.stderr.log"
        $info = [System.Diagnostics.ProcessStartInfo]::new()
        $info.FileName = $cli
        $info.WorkingDirectory = $repository
        $info.UseShellExecute = $false
        $info.CreateNoWindow = $true
        $info.RedirectStandardOutput = $true
        $info.RedirectStandardError = $true
        $arguments = @(
            "run",
            "remember the automatic memory boundary",
            "--session", $sessionId,
            "--cancellation-id", $runId,
            "--option", 'mock_scenario="streaming_text"',
            "--option", 'mock_text="automatic-memory-output"',
            "--json"
        )
        $info.Arguments = ($arguments | ForEach-Object {
            ConvertTo-WindowsArgument $_
        }) -join ' '
        $process = [System.Diagnostics.Process]::new()
        $process.StartInfo = $info
        [void]$process.Start()
        return @{
            Process = $process
            Stdout = $stdout
            Stderr = $stderr
        }
    }

    $daemon = $null
    $turnProcess = $null
    Start-Runtime
    try {
        $validation = & $cli style validate $stylePath --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or -not $validation.valid) {
            throw "automatic-memory style validation failed"
        }
        $session = & $cli session create --workspace $workspace `
            --style e2e-automatic-memory --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "session creation failed" }
        $runId = "018f0f5d-8c2a-7d31-9e42-4f6b8a1c2d3e"
        $cut = Start-CutTurn $session.session_id $runId
        $turnProcess = $cut.Process
        $journalPath = Join-Path $runRoot (
            "sessions\" + $session.session_id + "\events.jsonl"
        )
        $memoryPath = Join-Path $runRoot "memory\file.jsonl"
        $reachedCut = $false
        for ($attempt = 0; $attempt -lt 200; $attempt++) {
            if ($turnProcess.HasExited) {
                $cutStdout = $turnProcess.StandardOutput.ReadToEnd()
                $cutStderr = $turnProcess.StandardError.ReadToEnd()
                throw (
                    "cut turn exited before the crash window; stdout: " +
                    $cutStdout + "; stderr: " + $cutStderr
                )
            }
            if ((Test-Path -LiteralPath $journalPath) -and
                (Test-Path -LiteralPath $memoryPath)) {
                $events = Read-Journal $session.session_id
                if ((Event-Count $events "memory.write_dispatched") -eq 1 -and
                    @(Get-Content -LiteralPath $memoryPath).Count -eq 1) {
                    $reachedCut = $true
                    break
                }
            }
            Start-Sleep -Milliseconds 50
        }
        if (-not $reachedCut) {
            throw "automatic memory process did not reach the post-persist crash cut"
        }
        $beforeCut = Read-Journal $session.session_id
        if ((Event-Count $beforeCut "memory.write_completed") -ne 0) {
            throw "memory write completed before the configured crash cut"
        }

        Stop-Process -Id $daemon.Id -Force
        $daemon.WaitForExit()
        if (-not $turnProcess.WaitForExit(5000)) {
            $turnProcess.Kill($true)
            $turnProcess.WaitForExit()
        }
        $turnProcess = $null

        $env:AGENTMOD_MEMORY_WRITE_POST_PERSIST_DELAY_MS = "0"
        $env:AGENTMOD_HARNESS_PROGRAM = Join-Path $runRoot (
            "harness-must-not-be-spawned.exe"
        )
        $env:AGENTMOD_FIXTURE_HARNESS_PROGRAM = Join-Path $runRoot (
            "fixture-harness-must-not-be-spawned.exe"
        )
        Start-Runtime
        $recovered = & $cli run "remember the automatic memory boundary" `
            --session $session.session_id --cancellation-id $runId `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="automatic-memory-output"' --json |
            ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "automatic memory recovery failed" }
        if (@($recovered.events).Count -ne 0) {
            throw "recovery redispatched provider-visible work"
        }

        $events = Read-Journal $session.session_id
        foreach ($eventType in @(
            "memory.write_proposed",
            "memory.write_approved",
            "memory.write_dispatched",
            "memory.write_completed"
        )) {
            if ((Event-Count $events $eventType) -ne 1) {
                throw "automatic memory lifecycle was not committed exactly once: $eventType"
            }
        }
        foreach ($eventType in @(
            "model.request_proposed",
            "model.request_approved",
            "model.request_started",
            "model.response_completed"
        )) {
            if ((Event-Count $events $eventType) -ne 1) {
                throw "provider lifecycle was not committed exactly once: $eventType"
            }
        }
        if ((Event-Count $events "model.request_failed") -ne 0 -or
            (Event-Count $events "model.request_cancelled") -ne 0 -or
            @(Get-Content -LiteralPath $memoryPath).Count -ne 1) {
            throw "restart duplicated provider or memory effects"
        }
        $memoryLifecycle = @(
            $events | Where-Object {
                $_.event.metadata.event_type -like "memory.write_*"
            } | Sort-Object { [int64]$_.event.metadata.sequence }
        )
        $expectedTypes = @(
            "memory.write_proposed",
            "memory.write_approved",
            "memory.write_dispatched",
            "memory.write_completed"
        )
        for ($index = 0; $index -lt $expectedTypes.Count; $index++) {
            if ($memoryLifecycle[$index].event.metadata.event_type -ne
                $expectedTypes[$index]) {
                throw "automatic memory lifecycle order is not canonical"
            }
        }
        $canonicalIdentity = (
            $memoryLifecycle[0].event.payload.payload.identity |
                ConvertTo-Json -Depth 20 -Compress
        )
        foreach ($entry in $memoryLifecycle) {
            $identity = (
                $entry.event.payload.payload.identity |
                    ConvertTo-Json -Depth 20 -Compress
            )
            if ($identity -ne $canonicalIdentity) {
                throw "automatic memory lifecycle identity changed"
            }
        }
        $approvedDigest = $memoryLifecycle[1].event.payload.payload.action_digest
        if ($memoryLifecycle[2].event.payload.payload.action_digest -ne
                $approvedDigest -or
            $memoryLifecycle[3].event.payload.payload.action_digest -ne
                $approvedDigest) {
            throw "automatic memory action digest changed"
        }
        $identity = $memoryLifecycle[0].event.payload.payload.identity
        $completed = $memoryLifecycle[3].event.payload.payload
        if ($identity.provider -ne "file" -or
            $identity.policy -ne "turn_completion" -or
            $identity.run_id -ne $runId -or
            $identity.scope -ne ("session:" + $session.session_id) -or
            -not $completed.retained) {
            throw "automatic memory identity or terminal receipt is invalid"
        }
        $fileRecord = Get-Content -LiteralPath $memoryPath -Raw |
            ConvertFrom-Json
        if ($fileRecord.schema_version -ne 1 -or
            $fileRecord.id -ne $completed.reference -or
            $fileRecord.scope -ne $identity.scope -or
            $fileRecord.source -ne $identity.source -or
            $fileRecord.content -ne
                $memoryLifecycle[0].event.payload.payload.content -or
            $fileRecord.created_at_millis -ne $identity.created_at_millis -or
            $fileRecord.checksum -notmatch '^[0-9a-f]{64}$' -or
            [Text.Encoding]::UTF8.GetByteCount($fileRecord.content) -ne
                [int64]$identity.byte_size) {
            throw "retained file memory does not bind the canonical receipt"
        }
        if ($fileRecord.source -ne
            ("runtime.automatic_memory:turn_completion:" + $runId)) {
            throw "automatic memory source is not run-bound"
        }
        $typedFile = $fileRecord.content | ConvertFrom-Json
        if ($typedFile.schema -ne "agentmod.context-summary.v1" -or
            $fileRecord.content -notmatch "remember the automatic memory boundary" -or
            $fileRecord.content -notmatch "automatic-memory-output") {
            throw "retained automatic memory omitted typed turn content"
        }
        $inspect = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        $records = @(
            $inspect.state.automatic_memory_writes.PSObject.Properties.Value
        )
        if ($records.Count -ne 1 -or $records[0].state -ne "completed" -or
            -not $records[0].retained -or
            $records[0].identity.run_id -ne $runId -or
            $records[0].identity.policy -ne "turn_completion") {
            throw "automatic memory replay projection is incomplete"
        }
        $typed = $records[0].content | ConvertFrom-Json
        if ($typed.schema -ne "agentmod.context-summary.v1") {
            throw "automatic memory content is not bounded typed context"
        }
        $beforeCount = $events.Count
        $beforeHead = $inspect.state.last_sequence
        $beforeAutomatic = (
            $inspect.state.automatic_memory_writes |
                ConvertTo-Json -Depth 30 -Compress
        )
        $beforeJournalBytes = [Convert]::ToBase64String(
            [IO.File]::ReadAllBytes($journalPath)
        )
        $beforeMemoryBytes = [Convert]::ToBase64String(
            [IO.File]::ReadAllBytes($memoryPath)
        )

        Stop-Process -Id $daemon.Id -Force
        $daemon.WaitForExit()
        Start-Runtime
        $replayed = & $cli session replay $session.session_id --json |
            ConvertFrom-Json
        $replayedAutomatic = (
            $replayed.state.automatic_memory_writes |
                ConvertTo-Json -Depth 30 -Compress
        )
        if ($replayed.state.last_sequence -ne $beforeHead -or
            $replayedAutomatic -ne $beforeAutomatic -or
            (Read-Journal $session.session_id).Count -ne $beforeCount -or
            [Convert]::ToBase64String(
                [IO.File]::ReadAllBytes($journalPath)
            ) -ne $beforeJournalBytes -or
            [Convert]::ToBase64String(
                [IO.File]::ReadAllBytes($memoryPath)
            ) -ne $beforeMemoryBytes) {
            throw "pure replay mutated automatic memory state"
        }

        Stop-Process -Id $daemon.Id -Force
        $daemon.WaitForExit()
        $env:AGENTMOD_MEMORY_WRITE_PERMISSION_MODE = "ask"
        $env:AGENTMOD_HARNESS_PROGRAM = $harness
        Start-Runtime

        $askSession = & $cli session create --workspace $workspace `
            --style e2e-automatic-memory --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "native Ask session creation failed" }
        $askRunId = "018f0f5d-8c2a-7d31-9e42-4f6b8a1c2d4e"
        $askWaiting = & $cli run "approve native automatic memory" `
            --session $askSession.session_id --cancellation-id $askRunId `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="native-ask-output"' --json |
            ConvertFrom-Json
        if ([string]::IsNullOrWhiteSpace($askWaiting.awaiting_continuation)) {
            throw "native automatic memory did not durably wait for Ask approval"
        }
        $askEvents = Read-Journal $askSession.session_id
        if ((Event-Count $askEvents "memory.write_proposed") -ne 1 -or
            (Event-Count $askEvents "approval.requested") -ne 1 -or
            (Event-Count $askEvents "memory.write_approved") -ne 0 -or
            @(Get-Content -LiteralPath $memoryPath).Count -ne 1) {
            throw "native Ask crossed the memory boundary before approval"
        }

        Stop-Process -Id $daemon.Id -Force
        $daemon.WaitForExit()
        Start-Runtime
        $askApproved = & $cli approval resolve $askSession.session_id `
            $askWaiting.awaiting_continuation approve --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or -not $askApproved.transitioned) {
            throw "native Ask approval did not resume after restart"
        }
        $askEvents = Read-Journal $askSession.session_id
        foreach ($eventType in @(
            "approval.resolved",
            "memory.write_approved",
            "memory.write_dispatched",
            "memory.write_completed"
        )) {
            if ((Event-Count $askEvents $eventType) -ne 1) {
                throw "native Ask lifecycle was not committed exactly once: $eventType"
            }
        }
        if (@(Get-Content -LiteralPath $memoryPath).Count -ne 2) {
            throw "native Ask approval did not retain exactly one memory"
        }
        $askBeforeDuplicate = $askEvents.Count
        $askDuplicate = & $cli approval resolve $askSession.session_id `
            $askWaiting.awaiting_continuation approve --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or $askDuplicate.transitioned -or
            (Read-Journal $askSession.session_id).Count -ne $askBeforeDuplicate -or
            @(Get-Content -LiteralPath $memoryPath).Count -ne 2) {
            throw "duplicate native Ask approval was not effect-free"
        }

        $denySession = & $cli session create --workspace $workspace `
            --style e2e-automatic-memory --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "native denial session creation failed" }
        $denyRunId = "018f0f5d-8c2a-7d31-9e42-4f6b8a1c2d5e"
        $denyWaiting = & $cli run "deny native automatic memory" `
            --session $denySession.session_id --cancellation-id $denyRunId `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="native-deny-output"' --json |
            ConvertFrom-Json
        if ([string]::IsNullOrWhiteSpace($denyWaiting.awaiting_continuation)) {
            throw "native denial did not produce an approval continuation"
        }
        $denyResult = & $cli approval resolve $denySession.session_id `
            $denyWaiting.awaiting_continuation deny --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or -not $denyResult.transitioned) {
            throw "native automatic-memory denial did not transition"
        }
        $denyEvents = Read-Journal $denySession.session_id
        if ((Event-Count $denyEvents "approval.resolved") -ne 1 -or
            (Event-Count $denyEvents "memory.write_failed") -ne 1 -or
            (Event-Count $denyEvents "memory.write_dispatched") -ne 0 -or
            @(Get-Content -LiteralPath $memoryPath).Count -ne 2) {
            throw "native denial crossed the memory effect boundary"
        }
        $denyInspect = & $cli session inspect $denySession.session_id --json |
            ConvertFrom-Json
        $denyRecords = @(
            $denyInspect.state.automatic_memory_writes.PSObject.Properties.Value
        )
        if ($denyRecords.Count -ne 1 -or $denyRecords[0].state -ne "failed" -or
            $denyRecords[0].failure_code -ne "user_denied") {
            throw "native denial did not retain canonical terminal state"
        }

        Write-Output (
            "runtime automatic-memory crash/restart/replay/native-ask E2E passed"
        )
        $succeeded = $true
    }
    finally {
        if ($null -ne $turnProcess -and -not $turnProcess.HasExited) {
            $turnProcess.Kill($true)
            $turnProcess.WaitForExit()
        }
        if ($null -ne $daemon -and -not $daemon.HasExited) {
            Stop-Process -Id $daemon.Id -Force -ErrorAction SilentlyContinue
            $daemon.WaitForExit()
        }
        $resolvedRunRoot = [System.IO.Path]::GetFullPath($runRoot)
        $tempRoot = [System.IO.Path]::GetFullPath(
            [System.IO.Path]::GetTempPath()
        )
        if ($succeeded -and $resolvedRunRoot.StartsWith(
            $tempRoot + "agentmod-automatic-memory-e2e-"
        )) {
            Remove-Item -LiteralPath $resolvedRunRoot -Recurse -Force `
                -ErrorAction SilentlyContinue
        } elseif (-not $succeeded) {
            Write-Warning "retained failed E2E root: $resolvedRunRoot"
        }
    }
}
finally {
    Remove-Item Env:AGENTMOD_MEMORY_WRITE_POST_PERSIST_DELAY_MS `
        -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_FIXTURE_HARNESS_PROGRAM `
        -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_MEMORY_WRITE_PERMISSION_MODE `
        -ErrorAction SilentlyContinue
    Pop-Location
}
