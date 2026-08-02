$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build --locked -p agentmod-runtime -p agentmod-harness `
        -p agentmod-scheduler -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $scheduler = (Resolve-Path "target\debug\agentmod-scheduler.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-ephemeral-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-ephemeral-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_SCHEDULER_PROGRAM = $scheduler
    $env:AGENTMOD_SCHEDULER_ROOT = Join-Path $runRoot "scheduler"
    $runtimeOut = Join-Path $runRoot "runtime.stdout.log"
    $runtimeErr = Join-Path $runRoot "runtime.stderr.log"

    function Start-TestRuntime {
        $process = Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput $runtimeOut -RedirectStandardError $runtimeErr
        for ($attempt = 0; $attempt -lt 80; $attempt++) {
            try {
                & $cli doctor --json 2>$null | Out-Null
                if ($LASTEXITCODE -eq 0) { return $process }
            } catch {}
            Start-Sleep -Milliseconds 100
        }
        if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force }
        throw "runtime did not become ready"
    }

    function Stop-TestRuntime($process) {
        if ($null -ne $process -and -not $process.HasExited) {
            Stop-Process -Id $process.Id -Force
            $process.WaitForExit()
        }
    }

    function Read-Journal($sessionId) {
        $journal = Join-Path $runRoot (
            "sessions\" + $sessionId + "\events.jsonl"
        )
        if (-not (Test-Path -LiteralPath $journal)) { return @() }
        @(
            Get-Content -LiteralPath $journal |
                ForEach-Object { $_ | ConvertFrom-Json }
        )
    }

    function Assert-EventCount($sessionId, $eventType, $expected) {
        $actual = @(
            Read-Journal $sessionId |
                Where-Object { $_.event.metadata.event_type -eq $eventType }
        ).Count
        if ($actual -ne $expected) {
            throw "expected $expected $eventType events, found $actual"
        }
    }

    function Assert-ExactExecutorSet($nodes, $label) {
        $expected = @{
            "fresh-context" = @("context_transform", "runtime.context-construction")
            "respond" = @("model_call", "runtime.model-request")
            "tool-batch" = @("tool_execution_gate", "runtime.tool-gate")
            "done" = @("complete_turn", "runtime.turn-completion")
        }
        if (@($nodes).Count -ne $expected.Count) {
            throw "$label did not retain exactly four Ephemeral node executors"
        }
        foreach ($nodeId in $expected.Keys) {
            $resolution = @($nodes | Where-Object node_id -eq $nodeId)
            $expectedVersion = if ($nodeId -eq "tool-batch") {
                "1.1.0"
            } else {
                "1.0.0"
            }
            if ($resolution.Count -ne 1 -or
                $resolution[0].node_kind -ne $expected[$nodeId][0] -or
                $resolution[0].executor_id -ne $expected[$nodeId][1] -or
                $resolution[0].executor_version -ne $expectedVersion -or
                $resolution[0].source.kind -ne "runtime" -or
                $resolution[0].boundary -ne "runtime_logic") {
                throw "$label has an invalid exact resolution for $nodeId"
            }
        }
    }

    function Assert-GenericPlan($inspection) {
        $binding = $inspection.state.style_binding
        $plan = $binding.execution_plan
        if ($binding.id -ne "ephemeral-turn" -or
            $binding.version -ne "1.2.0" -or
            $plan.compilation.compiler -ne "agentmod-runtime-node-plan@3" -or
            [string]::IsNullOrWhiteSpace(
                [string]$binding.execution_plan_hash
            ) -or
            [string]::IsNullOrWhiteSpace([string]$plan.registry_hash)) {
            throw "ephemeral-turn 1.2 did not retain a generation-3 exact plan"
        }
        Assert-ExactExecutorSet $plan.nodes "Ephemeral binding"
    }

    function Assert-GenericContract($inspection) {
        $binding = $inspection.state.style_binding
        $contract = $inspection.state.style_execution.execution_contract
        if ($null -eq $contract -or
            $contract.execution_plan_hash -ne $binding.execution_plan_hash -or
            $contract.registry_hash -ne $binding.execution_plan.registry_hash) {
            throw "Ephemeral execution contract diverged from its immutable binding"
        }
        Assert-ExactExecutorSet $contract.node_executors "Ephemeral contract"
    }

    function Assert-EmptyProjection($inspection) {
        if (@($inspection.state.conversation.provider_projection).Count -ne 0) {
            throw "ephemeral provider projection was retained"
        }
    }

    function Assert-CanonicalHistory(
        $inspection,
        [string[]]$expectedUsers,
        [string[]]$expectedAssistants
    ) {
        $history = @($inspection.state.conversation.history)
        [string[]]$users = @(
            $history | Where-Object kind -eq "user_message" |
                ForEach-Object { $_.content.text }
        )
        [string[]]$assistants = @(
            $history | Where-Object kind -eq "assistant_message" |
                ForEach-Object { $_.content.text }
        )
        if (($users | ConvertTo-Json -Compress) -ne
                ($expectedUsers | ConvertTo-Json -Compress) -or
            ($assistants | ConvertTo-Json -Compress) -ne
                ($expectedAssistants | ConvertTo-Json -Compress)) {
            throw "canonical Ephemeral user/assistant history was not exact"
        }
    }

    function Assert-NoLegacyAdapterEvidence($inspection, $sessionId) {
        $serialized = $inspection | ConvertTo-Json -Depth 100 -Compress
        [string]$journal = (Read-Journal $sessionId) |
            ConvertTo-Json -Depth 100 -Compress
        foreach ($legacy in @(
            '"method":"ephemeral_fresh_context"',
            '"projection_id":"ephemeral-fresh-context',
            '"result_reference":"fresh-context:'
        )) {
            if ($serialized.Contains($legacy) -or $journal.Contains($legacy)) {
                throw "legacy Ephemeral adapter evidence survived: $legacy"
            }
        }
        if (-not $journal.Contains('"method":"generic_fresh_context"')) {
            throw "generic fresh-context execution evidence was not committed"
        }
    }

    function Assert-ExactLegacyBinding($inspection, $expectedBindingJson) {
        $binding = $inspection.state.style_binding
        if ($binding.id -ne "ephemeral-turn" -or
            $binding.version -ne "1.1.0" -or
            ($binding | ConvertTo-Json -Depth 100 -Compress) -ne
                $expectedBindingJson) {
            throw "explicit ephemeral-turn 1.1 binding changed or was upgraded"
        }
    }

    function Wait-ProviderCompletionReceipt($sessionId) {
        $directory = Join-Path $runRoot (
            "sessions\" + $sessionId +
            "\artifacts\provider-completion-receipts"
        )
        for ($attempt = 0; $attempt -lt 80; $attempt++) {
            $receipts = @(
                Get-ChildItem -LiteralPath $directory -Filter "*.json" `
                    -File -ErrorAction SilentlyContinue
            )
            if ($receipts.Count -eq 1) {
                $stored = Get-Content -LiteralPath $receipts[0].FullName -Raw |
                    ConvertFrom-Json
                if ($stored.payload.session_id -ne $sessionId -or
                    [string]::IsNullOrWhiteSpace(
                        [string]$stored.payload.invocation_id
                    ) -or
                    [string]::IsNullOrWhiteSpace(
                        [string]$stored.payload.receipt_json
                    )) {
                    throw "provider completion receipt was not identity-bound"
                }
                return $receipts[0]
            }
            if ($receipts.Count -gt 1) {
                throw "provider completion receipt was persisted more than once"
            }
            Start-Sleep -Milliseconds 100
        }
        throw "provider completion receipt was not durable during crash window"
    }

    function Stop-CrashRunJob($job) {
        if ($null -eq $job) { return }
        if ($job.State -eq "Running") {
            Stop-Job -Job $job
        }
        Receive-Job -Job $job -ErrorAction SilentlyContinue | Out-Null
        Remove-Job -Job $job -Force
    }

    $crashRunJob = $null
    $env:AGENTMOD_PROVIDER_COMPLETION_RECEIPT_DELAY_MS = "5000"
    $daemon = Start-TestRuntime
    try {
        $crashSession = & $cli session create --workspace $repository `
            --style ephemeral-turn@1.2.0 --json | ConvertFrom-Json
        $crashCreated = & $cli session inspect $crashSession.session_id --json |
            ConvertFrom-Json
        Assert-GenericPlan $crashCreated
        Assert-EmptyProjection $crashCreated
        $crashPrompt = "receipt-window-input"
        $crashOutput = "receipt-window-output"
        $crashAssistant = "alpha beta " + $crashOutput
        $crashCancellation = "0193f7d4-8c10-7000-8000-000000000012"
        $crashRunJob = Start-Job -ScriptBlock {
            param(
                $cliPath,
                $turnPrompt,
                $turnOutput,
                $sessionId,
                $cancellationId
            )
            & $cliPath run $turnPrompt --session $sessionId `
                --cancellation-id $cancellationId `
                --option 'mock_scenario="streaming_text"' `
                --option ('mock_text="' + $turnOutput + '"') --json
            if ($LASTEXITCODE -ne 0) {
                throw "receipt-window turn exited with $LASTEXITCODE"
            }
        } -ArgumentList @(
            $cli,
            $crashPrompt,
            $crashOutput,
            $crashSession.session_id,
            $crashCancellation
        )
        $receipt = Wait-ProviderCompletionReceipt $crashSession.session_id
        $receiptHash = (Get-FileHash -LiteralPath $receipt.FullName `
            -Algorithm SHA256).Hash
        Assert-EventCount $crashSession.session_id "model.request_started" 1
        Assert-EventCount $crashSession.session_id "model.response_completed" 0

        Stop-TestRuntime $daemon
        $daemon = $null
        Wait-Job -Job $crashRunJob -Timeout 10 | Out-Null
        Stop-CrashRunJob $crashRunJob
        $crashRunJob = $null
        Remove-Item Env:AGENTMOD_PROVIDER_COMPLETION_RECEIPT_DELAY_MS `
            -ErrorAction SilentlyContinue

        $daemon = Start-TestRuntime
        & $cli run $crashPrompt --session $crashSession.session_id `
            --cancellation-id $crashCancellation `
            --option 'mock_scenario="streaming_text"' `
            --option ('mock_text="' + $crashOutput + '"') --json | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "receipt-backed Ephemeral turn recovery failed"
        }
        $crashRecovered = & $cli session inspect `
            $crashSession.session_id --json | ConvertFrom-Json
        Assert-GenericPlan $crashRecovered
        Assert-GenericContract $crashRecovered
        Assert-EmptyProjection $crashRecovered
        Assert-CanonicalHistory $crashRecovered `
            @($crashPrompt) @($crashAssistant)
        Assert-NoLegacyAdapterEvidence `
            $crashRecovered $crashSession.session_id
        if ($null -ne $crashRecovered.state.style_execution.active_node) {
            throw "receipt-backed Ephemeral turn did not reach final completion"
        }
        Assert-EventCount $crashSession.session_id "model.request_started" 1
        Assert-EventCount $crashSession.session_id "model.response_completed" 1
        $receiptsAfterRecovery = @(
            Get-ChildItem -LiteralPath $receipt.DirectoryName `
                -Filter "*.json" -File
        )
        if ($receiptsAfterRecovery.Count -ne 1 -or
            (Get-FileHash -LiteralPath $receiptsAfterRecovery[0].FullName `
                -Algorithm SHA256).Hash -ne $receiptHash) {
            throw "provider recovery redispatched or replaced the harness effect"
        }

        $crashJournalBeforeReplay = @(
            Read-Journal $crashSession.session_id
        ).Count
        $crashReplay = & $cli session replay `
            $crashSession.session_id --json | ConvertFrom-Json
        if ($crashReplay.command -ne "session_replay" -or
            @(Read-Journal $crashSession.session_id).Count -ne
                $crashJournalBeforeReplay -or
            (Get-FileHash -LiteralPath $receipt.FullName `
                -Algorithm SHA256).Hash -ne $receiptHash) {
            throw "pure receipt-backed replay changed durable state"
        }
        Assert-GenericPlan $crashReplay
        Assert-GenericContract $crashReplay
        Assert-EmptyProjection $crashReplay
        Assert-CanonicalHistory $crashReplay `
            @($crashPrompt) @($crashAssistant)
        Assert-EventCount $crashSession.session_id "model.request_started" 1
        Assert-EventCount $crashSession.session_id "model.response_completed" 1

        $session = & $cli session create --workspace $repository `
            --style ephemeral-turn@1.2.0 --json | ConvertFrom-Json
        $created = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        Assert-GenericPlan $created
        Assert-EmptyProjection $created

        $firstPrompt = "turn-one-secret-input"
        $firstOutput = "turn-one-secret-output"
        $firstAssistant = "alpha beta " + $firstOutput
        & $cli run $firstPrompt --session $session.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option ('mock_text="' + $firstOutput + '"') --json | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "first ephemeral turn failed" }
        $firstInspection = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        Assert-GenericPlan $firstInspection
        Assert-GenericContract $firstInspection
        Assert-EmptyProjection $firstInspection
        Assert-CanonicalHistory $firstInspection @($firstPrompt) @($firstAssistant)
        Assert-NoLegacyAdapterEvidence $firstInspection $session.session_id

        Stop-TestRuntime $daemon
        $daemon = Start-TestRuntime
        $restartInspection = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        Assert-GenericPlan $restartInspection
        Assert-GenericContract $restartInspection
        Assert-EmptyProjection $restartInspection
        Assert-CanonicalHistory $restartInspection @($firstPrompt) @($firstAssistant)

        $secondPrompt = "turn-two-current-input"
        $secondOutput = "turn-two-output"
        $secondAssistant = "alpha beta " + $secondOutput
        & $cli run $secondPrompt --session $session.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option ('mock_text="' + $secondOutput + '"') --json | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "second ephemeral turn failed" }
        $finalInspection = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        Assert-GenericPlan $finalInspection
        Assert-GenericContract $finalInspection
        Assert-EmptyProjection $finalInspection
        Assert-CanonicalHistory $finalInspection `
            @($firstPrompt, $secondPrompt) @($firstAssistant, $secondAssistant)
        Assert-NoLegacyAdapterEvidence $finalInspection $session.session_id

        $journal = @(Read-Journal $session.session_id)
        $fresh = @($journal | Where-Object {
            ($_.event | ConvertTo-Json -Depth 100 -Compress).Contains(
                '"method":"generic_fresh_context"'
            )
        })
        $discard = @($journal | Where-Object {
            ($_.event | ConvertTo-Json -Depth 100 -Compress).Contains(
                '"method":"ephemeral_discard"'
            )
        })
        if ($fresh.Count -ne 2 -or $discard.Count -ne 2) {
            throw "expected one generic fresh projection and one discard per turn"
        }
        $secondFresh = $fresh[1].event | ConvertTo-Json -Depth 100 -Compress
        if (-not $secondFresh.Contains($secondPrompt) -or
            $secondFresh.Contains($firstPrompt) -or
            $secondFresh.Contains($firstOutput)) {
            throw "second fresh projection leaked unselected turn-one state"
        }
        Assert-EventCount $session.session_id "model.request_started" 2
        Assert-EventCount $session.session_id "model.response_completed" 2

        $beforeReplay = $journal.Count
        $replayed = & $cli session replay $session.session_id --json |
            ConvertFrom-Json
        if ($replayed.command -ne "session_replay" -or
            $replayed.state.style_binding.execution_plan_hash -ne
                $created.state.style_binding.execution_plan_hash -or
            @(Read-Journal $session.session_id).Count -ne $beforeReplay) {
            throw "pure Ephemeral 1.2 replay changed its plan or journal"
        }
        Assert-GenericPlan $replayed
        Assert-GenericContract $replayed
        Assert-EmptyProjection $replayed
        Assert-CanonicalHistory $replayed `
            @($firstPrompt, $secondPrompt) @($firstAssistant, $secondAssistant)
        Assert-NoLegacyAdapterEvidence $replayed $session.session_id
        Assert-EventCount $session.session_id "model.request_started" 2
        Assert-EventCount $session.session_id "model.response_completed" 2

        $legacy = & $cli session create --workspace $repository `
            --style ephemeral-turn@1.1.0 --json | ConvertFrom-Json
        $legacyCreated = & $cli session inspect $legacy.session_id --json |
            ConvertFrom-Json
        $legacyBinding = $legacyCreated.state.style_binding |
            ConvertTo-Json -Depth 100 -Compress
        Assert-ExactLegacyBinding $legacyCreated $legacyBinding
        & $cli run "legacy-turn-input" --session $legacy.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="legacy-turn-output"' --json | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "explicit legacy Ephemeral turn failed" }
        $legacyAfter = & $cli session inspect $legacy.session_id --json |
            ConvertFrom-Json
        Assert-ExactLegacyBinding $legacyAfter $legacyBinding
        Assert-EmptyProjection $legacyAfter
        $legacyBeforeReplay = @(Read-Journal $legacy.session_id).Count
        $legacyReplay = & $cli session replay $legacy.session_id --json |
            ConvertFrom-Json
        if ($legacyReplay.command -ne "session_replay" -or
            @(Read-Journal $legacy.session_id).Count -ne $legacyBeforeReplay) {
            throw "explicit Ephemeral 1.1 replay appended events"
        }
        Assert-ExactLegacyBinding $legacyReplay $legacyBinding
        Assert-EmptyProjection $legacyReplay
        Assert-EventCount $legacy.session_id "model.request_started" 1
        Assert-EventCount $legacy.session_id "model.response_completed" 1

        Write-Output (
            "runtime ephemeral-turn v1.2 receipt recovery/exact-plan/" +
            "generic-dispatch/isolated-restart/replay and explicit v1.1 " +
            "compatibility E2E passed"
        )
    }
    finally {
        Stop-CrashRunJob $crashRunJob
        Stop-TestRuntime $daemon
        $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
        $resolvedRun = (Resolve-Path $runRoot).Path
        if ($resolvedRun.StartsWith($resolvedTemp) -and
            (Split-Path $resolvedRun -Leaf).StartsWith(
                "agentmod-ephemeral-e2e-"
            )) {
            Remove-Item -LiteralPath $resolvedRun -Recurse -Force
        }
    }
}
finally {
    foreach ($name in @(
        "AGENTMOD_HARNESS_PROGRAM",
        "AGENTMOD_SCHEDULER_PROGRAM",
        "AGENTMOD_SCHEDULER_ROOT",
        "AGENTMOD_PROVIDER_COMPLETION_RECEIPT_DELAY_MS"
    )) {
        Remove-Item "Env:$name" -ErrorAction SilentlyContinue
    }
    Pop-Location
}
$global:LASTEXITCODE = 0
