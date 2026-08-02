$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build --locked -p agentmod-runtime -p agentmod-scheduler `
        -p agentmod-harness `
        -p agentmod-cli -p agentmod-plugin-host `
        -p agentmod-plugin-fixture-worker
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
    $pluginHost = (
        Resolve-Path (Join-Path $debugRoot "agentmod-plugin-host.exe")
    ).Path
    $pluginWorker = (
        Resolve-Path (Join-Path $debugRoot "agentmod-plugin-fixture-worker.exe")
    ).Path
    $runRoot = Join-Path ([IO.Path]::GetTempPath()) (
        "agentmod-plugin-context-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    $styleRoot = Join-Path $runRoot "styles\user"
    New-Item -ItemType Directory -Path $workspace, $styleRoot -Force |
        Out-Null
    $succeeded = $false
    $wrapper = Join-Path $runRoot "agentmod-plugin-fixture-worker.exe"
    $offlineWrapper = "$wrapper.offline"
    $marker = Join-Path $runRoot "context-invocations.log"
    Copy-Item -LiteralPath $pluginWorker -Destination $wrapper
    $normalizedWrapper = $wrapper.Replace("\", "/")
    $pluginArguments = @(
        "--memory-marker", $marker.Replace("\", "/")
    ) | ForEach-Object {
        '"' + $_.Replace('\', '\\').Replace('"', '\"') + '"'
    }
    function Write-Manifest($fixture, $destination) {
        (Get-Content -LiteralPath $fixture -Raw).Replace(
            "__PLUGIN_PROGRAM__",
            $normalizedWrapper
        ).Replace(
            "__PLUGIN_ARGS__",
            "[" + ($pluginArguments -join ", ") + "]"
        ) | Set-Content -LiteralPath $destination -NoNewline
    }
    $memoryManifest = Join-Path $runRoot "plugin-context-memory.toml"
    $compactorManifest = Join-Path $runRoot "plugin-context-compactor.toml"
    Write-Manifest "tests\fixtures\plugins\plugin-context.toml" `
        $memoryManifest
    Write-Manifest "tests\fixtures\plugins\plugin-compaction.toml" `
        $compactorManifest

    $providerHashes = @{
        "fixture.context-memory.success" =
            "2b72f4cd63fe66f74672098d981ba2e38d1d2b1ba3e51f9df178966d7885fc40"
        "fixture.context-memory.invalid" =
            "a4028f30754d127c743f064045462ffce0ca7daae1d590834d1979a284010cc6"
        "fixture.context-memory.timeout" =
            "2e59bf93479578db0e76f547b37b02a6a7dc6d44c99c9a3157ad8993def50f96"
    }
    $compactorHashes = @{
        "fixture.context-compactor.success" =
            "e5c9320e9582f147a37f6f1f7fd0726c83e4f1c996bf8c52d8a32ad037723938"
        "fixture.context-compactor.invalid" =
            "6135a6b4689d5cf3a7d23a406da04ba9731e2ef2c8873ffe97351352c0e9d1a2"
        "fixture.context-compactor.timeout" =
            "4dc2c55dd5d476170166aabc04684f9e09c27a8974c0b2eee1abd9b34817af33"
    }
    $combinedTemplate = Get-Content `
        "tests\fixtures\styles\plugin-context.toml" -Raw
    $iterationTemplate = Get-Content `
        "tests\fixtures\styles\plugin-context-iteration.toml" -Raw
    $memoryOnlyTemplate = (
        Get-Content "tests\fixtures\styles\plugin-automatic-memory.toml" -Raw
    ).Replace(
        'fixture.automatic-memory',
        'fixture.plugin-context'
    ).Replace(
        'write_policy = "turn_completion"',
        'write_policy = "never"'
    ).Replace(
        'injection_location = "none"',
        'injection_location = "before_current_input"'
    ).Replace(
        'max_query_bytes = 0',
        'max_query_bytes = 16384'
    ).Replace(
        ('1' * 64),
        '6e46dd10defc9b56c29a6ec56b508c21f54c08192194e4df25bf36f0c9c3c279'
    )

    function Write-MemoryStyle($name, $providerId, $timing) {
        $style = $memoryOnlyTemplate.Replace(
            "__STYLE_ID__",
            "e2e-plugin-context-" + $name
        ).Replace(
            "__PROVIDER_ID__",
            $providerId
        ).Replace(
            "__DECLARATION_HASH__",
            $providerHashes[$providerId]
        ).Replace(
            'retrieval_timing = "never"',
            'retrieval_timing = "' + $timing + '"'
        )
        $path = Join-Path $styleRoot ($name + ".toml")
        Set-Content -LiteralPath $path -Value $style -NoNewline
        return $path
    }

    function Write-CombinedStyle(
        $name,
        $providerId,
        $timing,
        $compactorId
    ) {
        $style = $combinedTemplate.Replace(
            "__STYLE_ID__",
            "e2e-plugin-context-" + $name
        ).Replace(
            "__PROVIDER_ID__",
            $providerId
        ).Replace(
            "__PROVIDER_HASH__",
            $providerHashes[$providerId]
        ).Replace(
            "__RETRIEVAL_TIMING__",
            $timing
        ).Replace(
            "__COMPACTION_STRATEGY__",
            "plugin"
        ).Replace(
            "__COMPACTOR_ID__",
            $compactorId
        ).Replace(
            "__COMPACTOR_HASH__",
            $compactorHashes[$compactorId]
        )
        $path = Join-Path $styleRoot ($name + ".toml")
        Set-Content -LiteralPath $path -Value $style -NoNewline
        return $path
    }

    $styles = @{}
    $styles.turnStart = Write-MemoryStyle "turn-start" `
        "fixture.context-memory.success" "turn_start"
    $styles.beforeModel = Write-MemoryStyle "before-model" `
        "fixture.context-memory.success" "before_model_request"
    $styles.contextNode = Write-CombinedStyle "context-node" `
        "fixture.context-memory.success" "context_node" `
        "fixture.context-compactor.success"
    $styles.iterationStart = Join-Path $styleRoot "iteration-start.toml"
    $iterationTemplate.Replace(
        "__STYLE_ID__",
        "e2e-plugin-context-iteration-start"
    ).Replace(
        "__PROVIDER_ID__",
        "fixture.context-memory.success"
    ).Replace(
        "__PROVIDER_HASH__",
        $providerHashes["fixture.context-memory.success"]
    ) | Set-Content -LiteralPath $styles.iterationStart -NoNewline
    $styles.invalidMemory = Write-MemoryStyle "invalid-memory" `
        "fixture.context-memory.invalid" "before_model_request"
    $styles.timeoutMemory = Write-MemoryStyle "timeout-memory" `
        "fixture.context-memory.timeout" "before_model_request"
    $styles.invalidCompactor = Write-CombinedStyle "invalid-compactor" `
        "fixture.context-memory.success" "context_node" `
        "fixture.context-compactor.invalid"
    $styles.timeoutCompactor = Write-CombinedStyle "timeout-compactor" `
        "fixture.context-memory.success" "context_node" `
        "fixture.context-compactor.timeout"

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-plugin-context-e2e-" +
        [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_PLUGIN_HOST_PROGRAM = $pluginHost
    $env:AGENTMOD_PLUGIN_MANIFESTS = @(
        $memoryManifest, $compactorManifest
    ) -join [IO.Path]::PathSeparator
    $env:AGENTMOD_PLUGIN_EXECUTABLE_ROOTS = @(
        $runRoot
    ) -join [IO.Path]::PathSeparator

    function Wait-RuntimeReady {
        for ($attempt = 0; $attempt -lt 150; $attempt++) {
            & $cli doctor --json 2>$null | Out-Null
            if ($LASTEXITCODE -eq 0) { return }
            Start-Sleep -Milliseconds 100
        }
        throw "runtime did not become ready"
    }
    function Start-Runtime {
        $script:runtimeStarts++
        $runtimeErr = Join-Path $runRoot (
            "runtime-" + $script:runtimeStarts + ".stderr.log"
        )
        $script:daemon = Start-Process -FilePath $runtime `
            -ArgumentList "serve" -WorkingDirectory $runRoot `
            -WindowStyle Hidden -RedirectStandardError $runtimeErr -PassThru
        Wait-RuntimeReady
    }
    function Stop-Runtime {
        if ($null -ne $script:daemon -and -not $script:daemon.HasExited) {
            Stop-Process -Id $script:daemon.Id -Force
            $script:daemon.WaitForExit()
        }
        $script:daemon = $null
    }
    function Set-WrapperOffline($offline) {
        if ($offline -and (Test-Path -LiteralPath $wrapper)) {
            Move-Item -LiteralPath $wrapper -Destination $offlineWrapper
        } elseif (-not $offline -and (Test-Path -LiteralPath $offlineWrapper)) {
            Move-Item -LiteralPath $offlineWrapper -Destination $wrapper
        }
    }
    function Read-Journal($sessionId) {
        $path = Join-Path $runRoot (
            "sessions\" + $sessionId + "\events.jsonl"
        )
        for ($attempt = 0; $attempt -lt 80; $attempt++) {
            try {
                return @(Get-Content -LiteralPath $path -ErrorAction Stop |
                    ForEach-Object { $_ | ConvertFrom-Json })
            }
            catch [System.IO.IOException] {
                Start-Sleep -Milliseconds 25
            }
        }
        throw "journal remained locked: $path"
    }
    function Event-Count($events, $eventType) {
        return @($events | Where-Object {
            $_.event.metadata.event_type -eq $eventType
        }).Count
    }
    function Invocation-Count($invocationId) {
        if (-not (Test-Path -LiteralPath $marker)) { return 0 }
        return @(Get-Content -LiteralPath $marker | Where-Object {
            $_ -like ($invocationId + "|*")
        }).Count
    }
    function Create-Session($name) {
        $created = & $cli session create --workspace $workspace `
            --style ("e2e-plugin-context-" + $name) --json |
            ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "session creation failed: $name" }
        return $created.session_id
    }
    function Invoke-Turn($sessionId, $runId, $expectSuccess) {
        $output = & $cli run "plugin context process proof" `
            --session $sessionId --cancellation-id $runId `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="plugin-context-output"' --json 2>$null
        $exitCode = $LASTEXITCODE
        if ($expectSuccess -and $exitCode -ne 0) {
            throw "plugin context turn failed: $output"
        }
        if (-not $expectSuccess -and $exitCode -eq 0) {
            throw "plugin context failure unexpectedly succeeded"
        }
    }
    function Assert-ExactOperation(
        $events,
        $kind,
        $implementationId,
        $declarationHash,
        $configurationReference,
        $boundary
    ) {
        $records = @($events | Where-Object {
            $_.event.metadata.event_type -eq "plugin.context_operation_proposed" `
                -and $_.event.payload.payload.identity.kind -eq $kind
        })
        if ($records.Count -ne 1) {
            throw "expected one $kind proposal, got $($records.Count)"
        }
        $identity = $records[0].event.payload.payload.identity
        if ($identity.implementation_id -ne $implementationId -or
            $identity.declaration_hash -ne $declarationHash -or
            $identity.configuration_reference -ne $configurationReference -or
            $identity.phase.boundary.boundary -ne $boundary) {
            throw (
                "exact $kind identity mismatch: " +
                ($identity | ConvertTo-Json -Depth 20 -Compress)
            )
        }
        return $identity.invocation_id
    }

    $runtimeStarts = 0
    $daemon = $null
    Start-Runtime
    try {
        foreach ($path in $styles.Values) {
            $validation = & $cli style validate $path --json |
                ConvertFrom-Json
            if ($LASTEXITCODE -ne 0 -or -not $validation.valid) {
                throw (
                    "plugin context style validation failed: " +
                    ($validation | ConvertTo-Json -Depth 20 -Compress)
                )
            }
        }

        foreach ($timing in @(
            @{
                Name = "turn-start"
                Boundary = "turn_start"
                Combined = $false
            },
            @{
                Name = "before-model"
                Boundary = "before_model_request"
                Combined = $false
            },
            @{
                Name = "context-node"
                Boundary = "context_node"
                Combined = $true
            }
        )) {
            $sessionId = Create-Session $timing.Name
            Invoke-Turn $sessionId ([guid]::NewGuid().ToString()) $true
            $events = Read-Journal $sessionId
            $memoryInvocation = Assert-ExactOperation $events `
                "memory_retrieve" "fixture.context-memory.success" `
                $providerHashes["fixture.context-memory.success"] `
                "6e46dd10defc9b56c29a6ec56b508c21f54c08192194e4df25bf36f0c9c3c279" `
                $timing.Boundary
            if ((Invocation-Count $memoryInvocation) -ne 1 -or
                (Event-Count $events "plugin.context_operation_completed") `
                    -ne $(if ($timing.Combined) { 2 } else { 1 }) -or
                (Event-Count $events "plugin.context_operation_applied") `
                    -ne $(if ($timing.Combined) { 2 } else { 1 })) {
                throw "live $($timing.Name) operation lifecycle is incomplete"
            }
            if ($timing.Combined) {
                $compactionInvocation = Assert-ExactOperation $events `
                    "compaction" "fixture.context-compactor.success" `
                    $compactorHashes["fixture.context-compactor.success"] `
                    "6e46dd10defc9b56c29a6ec56b508c21f54c08192194e4df25bf36f0c9c3c279" `
                    "before_model_request"
                if ((Invocation-Count $compactionInvocation) -ne 1) {
                    throw "plugin compaction was not invoked exactly once"
                }
            }
        }

        foreach ($failure in @(
            @{ Name = "invalid-memory"; Handler = "invalid_memory_retrieve" },
            @{ Name = "timeout-memory"; Handler = "timeout_memory_retrieve" },
            @{ Name = "invalid-compactor"; Handler = "invalid_compaction" },
            @{ Name = "timeout-compactor"; Handler = "timeout_compaction" }
        )) {
            $sessionId = Create-Session $failure.Name
            $runId = [guid]::NewGuid().ToString()
            Invoke-Turn $sessionId $runId $false
            $events = Read-Journal $sessionId
            $matching = @(
                Get-Content -LiteralPath $marker | Where-Object {
                    $_ -like ("*|" + $failure.Handler)
                }
            )
            if ($matching.Count -lt 1 -or
                ((Event-Count $events "plugin.context_operation_failed") +
                 (Event-Count $events "plugin.context_operation_ambiguous")) `
                    -ne 1) {
                throw "$($failure.Name) did not fail closed exactly once"
            }
            Invoke-Turn $sessionId $runId $false
            $after = Read-Journal $sessionId
            if (((Event-Count $after "plugin.context_operation_failed") +
                 (Event-Count $after "plugin.context_operation_ambiguous")) `
                    -ne 1) {
                throw "$($failure.Name) duplicated terminal context evidence"
            }
        }

        $iterationSession = Create-Session "iteration-start"
        $iterationRun = [guid]::NewGuid().ToString()
        Invoke-Turn $iterationSession $iterationRun $true
        $iterationEvents = Read-Journal $iterationSession
        $iterationOperations = @($iterationEvents | Where-Object {
            $_.event.metadata.event_type -eq
                "plugin.context_operation_proposed" -and
            $_.event.payload.payload.identity.kind -eq "memory_retrieve"
        })
        if ($iterationOperations.Count -ne 2 -or
            (Event-Count $iterationEvents "plugin.context_operation_completed") `
                -ne 2 -or
            (Event-Count $iterationEvents "plugin.context_operation_applied") `
                -ne 2 -or
            (Event-Count $iterationEvents "model.request_started") -ne 3) {
            throw "iteration-start plugin retrieval lifecycle is incomplete"
        }
        $iterationIds = @{}
        $iterationSourceHeads = @{}
        $iterationProjectionHashes = @{}
        $iterationLoopCounters = @{}
        $iterationProjectionCounts = @{}
        foreach ($operation in $iterationOperations) {
            $payload = $operation.event.payload.payload
            $identity = $payload.identity
            $runtimeValues = `
                $payload.readable_state.readable_state.recorded_runtime_values
            if ($identity.implementation_id -ne
                    "fixture.context-memory.success" -or
                $identity.declaration_hash -ne
                    $providerHashes["fixture.context-memory.success"] -or
                $identity.configuration_reference -ne
                    "6e46dd10defc9b56c29a6ec56b508c21f54c08192194e4df25bf36f0c9c3c279" -or
                $identity.phase.boundary.boundary -ne "iteration_start" -or
                (Invocation-Count $identity.invocation_id) -ne 1 -or
                $iterationIds.ContainsKey($identity.invocation_id) -or
                $runtimeValues.source_head -ne
                    $identity.phase.boundary.source_head -or
                $runtimeValues.projection_hash.Length -ne 64) {
                throw (
                    "iteration-start identity is invalid: " +
                    ($payload | ConvertTo-Json -Depth 20 -Compress)
                )
            }
            $iterationIds[$identity.invocation_id] = $true
            $iterationSourceHeads[
                [string]$runtimeValues.source_head
            ] = $true
            $iterationProjectionHashes[
                [string]$runtimeValues.projection_hash
            ] = $true
            $iterationLoopCounters[
                [string]$runtimeValues.loop_iteration
            ] = $true
            $iterationProjectionCounts[
                [string]$runtimeValues.projection_entry_count
            ] = $true
        }
        if ($iterationSourceHeads.Count -ne 2 -or
            $iterationProjectionHashes.Count -ne 2 -or
            $iterationLoopCounters.Count -ne 2 -or
            -not $iterationLoopCounters.ContainsKey("1") -or
            -not $iterationLoopCounters.ContainsKey("2") -or
            $iterationProjectionCounts.Count -ne 2 -or
            -not $iterationProjectionCounts.ContainsKey("1") -or
            -not $iterationProjectionCounts.ContainsKey("2")) {
            throw "iteration-start identity omitted prior state or loop counters"
        }
        $iterationBeforeCount = $iterationEvents.Count
        $iterationBeforeBytes = [Convert]::ToBase64String(
            [IO.File]::ReadAllBytes(
                (Join-Path $runRoot (
                    "sessions\" + $iterationSession + "\events.jsonl"
                ))
            )
        )
        & $cli session replay $iterationSession --json | Out-Null
        $iterationAfter = Read-Journal $iterationSession
        if ($iterationAfter.Count -ne $iterationBeforeCount -or
            [Convert]::ToBase64String(
                [IO.File]::ReadAllBytes(
                    (Join-Path $runRoot (
                        "sessions\" + $iterationSession + "\events.jsonl"
                    ))
                )
            ) -ne $iterationBeforeBytes) {
            throw "iteration-start pure replay mutated canonical history"
        }

        $receiptSession = Create-Session "before-model"
        $receiptRun = [guid]::NewGuid().ToString()
        $env:AGENTMOD_PLUGIN_RECEIPT_POST_PERSIST_DELAY_MS = "10000"
        Stop-Runtime
        Start-Runtime
        $runInfo = [Diagnostics.ProcessStartInfo]::new()
        $runInfo.FileName = $cli
        $runInfo.WorkingDirectory = $repository
        $runInfo.UseShellExecute = $false
        $runInfo.CreateNoWindow = $true
        $runInfo.RedirectStandardOutput = $true
        $runInfo.RedirectStandardError = $true
        foreach ($argument in @(
            "run", "plugin context receipt cut", "--session",
            $receiptSession, "--cancellation-id", $receiptRun,
            "--option", 'mock_scenario="streaming_text"',
            "--option", 'mock_text="plugin-context-output"', "--json"
        )) {
            [void]$runInfo.ArgumentList.Add($argument)
        }
        $runProcess = [Diagnostics.Process]::new()
        $runProcess.StartInfo = $runInfo
        [void]$runProcess.Start()
        $receiptPath = $null
        $receiptInvocation = $null
        for ($attempt = 0; $attempt -lt 250; $attempt++) {
            $events = Read-Journal $receiptSession
            $proposed = @($events | Where-Object {
                $_.event.metadata.event_type -eq `
                    "plugin.context_operation_proposed"
            })
            if ($proposed.Count -eq 1) {
                $receiptInvocation = (
                    $proposed[0].event.payload.payload.identity.invocation_id
                )
                $receiptDirectory = Join-Path $runRoot (
                    "sessions\" + $receiptSession +
                    "\artifacts\plugin-invocation-receipts"
                )
                $receipts = @(Get-ChildItem -LiteralPath $receiptDirectory `
                    -Filter *.json -ErrorAction SilentlyContinue)
                if ((Invocation-Count $receiptInvocation) -eq 1 -and
                    $receipts.Count -eq 1 -and
                    (Event-Count $events `
                        "plugin.context_operation_dispatched") -eq 1 -and
                    (Event-Count $events `
                        "plugin.context_operation_completed") -eq 0) {
                    $receiptPath = $receipts[0].FullName
                    break
                }
            }
            if ($runProcess.HasExited) {
                throw "turn exited before plugin context receipt cut"
            }
            Start-Sleep -Milliseconds 50
        }
        if ($null -eq $receiptPath) {
            throw "plugin context receipt crash cut was not reached"
        }
        Stop-Runtime
        if (-not $runProcess.WaitForExit(5000)) {
            $runProcess.Kill($true)
            $runProcess.WaitForExit()
        }
        Set-WrapperOffline $true
        $env:AGENTMOD_PLUGIN_RECEIPT_POST_PERSIST_DELAY_MS = "0"
        Start-Runtime
        & $cli run "plugin context receipt cut" --session $receiptSession `
            --cancellation-id $receiptRun `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="plugin-context-output"' --json |
            Out-Null
        if ($LASTEXITCODE -eq 0) {
            throw (
                "receipt-only recovery advanced into a later effect without " +
                "live plugin revalidation"
            )
        }
        $recovered = Read-Journal $receiptSession
        if ((Invocation-Count $receiptInvocation) -ne 1 -or
            (Event-Count $recovered "plugin.context_operation_dispatched") `
                -ne 1 -or
            (Event-Count $recovered "plugin.context_operation_completed") `
                -ne 1 -or
            (Event-Count $recovered "plugin.context_operation_applied") `
                -ne 1) {
            throw "receipt-only restart duplicated plugin context execution"
        }
        Stop-Runtime
        Set-WrapperOffline $false
        Start-Runtime
        & $cli run "plugin context receipt cut" --session $receiptSession `
            --cancellation-id $receiptRun `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="plugin-context-output"' --json |
            Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "live-revalidated plugin context recovery failed"
        }
        $revalidated = Read-Journal $receiptSession
        if ((Invocation-Count $receiptInvocation) -ne 1 -or
            (Event-Count $revalidated "plugin.context_operation_dispatched") `
                -ne 1 -or
            (Event-Count $revalidated "plugin.context_operation_completed") `
                -ne 1 -or
            (Event-Count $revalidated "plugin.context_operation_applied") `
                -ne 1) {
            throw "live revalidation duplicated the recovered plugin operation"
        }

        $unavailableSession = Create-Session "before-model"
        Stop-Runtime
        Set-WrapperOffline $true
        Start-Runtime
        Invoke-Turn $unavailableSession ([guid]::NewGuid().ToString()) $false
        $unavailableEvents = Read-Journal $unavailableSession
        foreach ($eventType in @(
            "plugin.context_operation_proposed",
            "plugin.context_operation_dispatched",
            "plugin.context_operation_completed",
            "plugin.context_operation_ambiguous"
        )) {
            if ((Event-Count $unavailableEvents $eventType) -ne 0) {
                throw (
                    "unavailable selected context plugin crossed a canonical " +
                    "operation boundary: $eventType"
                )
            }
        }

        Write-Output (
            "runtime/plugin-host plugin-context timing/validation/receipt/" +
            "ambiguity E2E passed"
        )
        $succeeded = $true
    }
    finally {
        Stop-Runtime
        Set-WrapperOffline $false
        if ($succeeded -and
            $runRoot.StartsWith([IO.Path]::GetTempPath())) {
            Remove-Item -LiteralPath $runRoot -Recurse -Force `
                -ErrorAction SilentlyContinue
        } elseif (-not $succeeded) {
            Write-Warning "retained failed E2E root: $runRoot"
        }
    }
}
finally {
    @(
        "AGENTMOD_RUNTIME_ENDPOINT",
        "AGENTMOD_RUNTIME_AUTH_TOKEN",
        "AGENTMOD_HARNESS_PROGRAM",
        "AGENTMOD_PLUGIN_HOST_PROGRAM",
        "AGENTMOD_PLUGIN_MANIFESTS",
        "AGENTMOD_PLUGIN_EXECUTABLE_ROOTS",
        "AGENTMOD_PLUGIN_RECEIPT_POST_PERSIST_DELAY_MS"
    ) | ForEach-Object {
        Remove-Item ("Env:\" + $_) -ErrorAction SilentlyContinue
    }
    Pop-Location
}
$global:LASTEXITCODE = 0
