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
        "agentmod-research-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-research-e2e-" + [guid]::NewGuid().ToString("N")
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
                $doctor = & $cli doctor --json 2>$null | ConvertFrom-Json
                if ($LASTEXITCODE -eq 0 -and $doctor.state -eq "ready") {
                    return $process
                }
            }
            catch {}
            Start-Sleep -Milliseconds 100
        }
        if (-not $process.HasExited) {
            Stop-Process -Id $process.Id -Force
            $process.WaitForExit()
        }
        throw "runtime did not become ready"
    }

    function Stop-TestRuntime($process) {
        if ($null -ne $process -and -not $process.HasExited) {
            Stop-Process -Id $process.Id -Force
            $process.WaitForExit()
        }
    }

    function Get-SessionRoot([string]$sessionId) {
        return Join-Path $runRoot ("sessions\" + $sessionId)
    }

    function Read-Journal([string]$sessionId) {
        $journalPath = Join-Path (Get-SessionRoot $sessionId) "events.jsonl"
        return @(Get-Content -LiteralPath $journalPath | ForEach-Object {
            $_ | ConvertFrom-Json
        })
    }

    function Assert-EventCount(
        [string]$sessionId,
        [string]$eventType,
        [int]$expected
    ) {
        $actual = @(
            Read-Journal $sessionId |
                Where-Object { $_.event.metadata.event_type -eq $eventType }
        ).Count
        if ($actual -ne $expected) {
            throw "expected $expected $eventType events, found $actual"
        }
    }

    function Assert-LegacyResearchState($inspection, [string]$sessionId) {
        if ($inspection.state.style_binding.id -ne "research-loop" -or
            $inspection.state.style_binding.version -ne "1.1.0") {
            throw "frozen research style binding mismatch"
        }
        if ($inspection.state.lifecycle -ne "completed" -or
            $inspection.state.style_execution.termination_reason -ne
                "complete_session") {
            throw "frozen research session did not complete"
        }
        $introspection = $inspection.state.style_introspection
        if ($introspection.style.id -ne "research-loop" -or
            $null -ne $introspection.graph.active_node -or
            $introspection.graph.loop_count -ne 3 -or
            $introspection.graph.retry_count -ne 0 -or
            @($introspection.graph.next_eligible_transitions).Count -ne 0 -or
            $introspection.termination_reason -ne "complete_session") {
            throw "frozen research graph introspection mismatch"
        }
        if (@($introspection.graph.completed_nodes).Count -lt 15 -or
            @($introspection.graph.previous_transitions).Count -lt 14 -or
            $introspection.remaining_budgets.steps -lt 0 -or
            $introspection.remaining_budgets.tokens -lt 0 -or
            $null -eq $introspection.pipeline.blocking_interceptor_order -or
            $null -eq $introspection.memory.retrieved_provenance -or
            @($introspection.compaction.history).Count -lt 3 -or
            $null -eq $introspection.child_agents.executions -or
            $null -eq $introspection.child_agents.joins -or
            $null -eq $introspection.child_agents.reviewer_findings) {
            throw "frozen research orchestration inspection is incomplete"
        }
        if (@(
                $inspection.state.artifact_persistences.PSObject.Properties
            ).Count -ne 3) {
            throw "expected three canonical frozen research artifacts"
        }
        $completed = @($inspection.state.style_execution.completed_nodes)
        if (@($completed | Where-Object node_id -eq "persist").Count -ne 3 -or
            @($completed | Where-Object node_id -eq "repeat").Count -ne 3) {
            throw "frozen research graph did not retain three iterations"
        }
        $journalJson = (Read-Journal $sessionId | ConvertTo-Json -Depth 100)
        if ([regex]::Matches(
                $journalJson,
                [regex]::Escape("research_fresh_context")
            ).Count -ne 3) {
            throw "frozen research context provenance is incomplete"
        }
        Assert-EventCount $sessionId "artifact.persistence_completed" 3
    }

    function Assert-GenericPlan($inspection) {
        $binding = $inspection.state.style_binding
        $plan = $binding.execution_plan
        $expected = @{
            "fresh-context" = @("runtime.context-construction", "1.0.0")
            "research" = @("runtime.model-request", "1.0.0")
            "tool-batch" = @("runtime.tool-gate", "1.1.0")
            "persist" = @("runtime.artifact-persistence", "1.0.0")
            "repeat" = @("runtime.loop", "1.0.0")
            "done" = @("runtime.session-completion", "1.0.0")
        }
        if ($binding.id -ne "research-loop" -or
            $binding.version -ne "1.2.0" -or
            $plan.compilation.compiler -ne "agentmod-runtime-node-plan@3" -or
            @($plan.nodes).Count -ne 6 -or
            [string]::IsNullOrWhiteSpace(
                [string]$binding.execution_plan_hash
            ) -or
            [string]::IsNullOrWhiteSpace([string]$plan.registry_hash)) {
            throw "current research exact V3 plan is incomplete"
        }
        foreach ($nodeId in $expected.Keys) {
            $resolution = @(
                $plan.nodes | Where-Object node_id -eq $nodeId
            )
            if ($resolution.Count -ne 1 -or
                $resolution[0].executor_id -ne $expected[$nodeId][0] -or
                $resolution[0].executor_version -ne $expected[$nodeId][1] -or
                $resolution[0].source.kind -ne "runtime" -or
                $resolution[0].boundary -ne "runtime_logic") {
                throw "invalid exact executor resolution for $nodeId"
            }
        }
    }

    function Assert-GenericResearchState(
        $inspection,
        [string]$sessionId,
        [string]$expectedText
    ) {
        Assert-GenericPlan $inspection
        $state = $inspection.state
        $execution = $state.style_execution
        $binding = $state.style_binding
        if ($state.lifecycle -ne "completed" -or
            $execution.termination_reason -ne "complete_session" -or
            $null -ne $execution.active_node -or
            @($execution.completed_nodes).Count -ne 16 -or
            @($execution.transitions).Count -ne 15) {
            throw "current research generic graph did not complete exactly"
        }
        $contract = $execution.execution_contract
        if ($contract.execution_plan_hash -ne $binding.execution_plan_hash -or
            $contract.registry_hash -ne $binding.execution_plan.registry_hash -or
            @($contract.node_executors).Count -ne 6) {
            throw "current research execution contract hash mismatch"
        }
        $expectedOrder = @(
            "fresh-context", "research", "tool-batch", "persist", "repeat",
            "fresh-context", "research", "tool-batch", "persist", "repeat",
            "fresh-context", "research", "tool-batch", "persist", "repeat",
            "done"
        )
        $actualOrder = @($execution.completed_nodes | ForEach-Object node_id)
        if (($actualOrder -join ",") -ne ($expectedOrder -join ",")) {
            throw "current research node order was not deterministic"
        }
        foreach ($nodeId in @(
                "fresh-context", "research", "tool-batch", "persist", "repeat"
            )) {
            if (@(
                    $execution.completed_nodes |
                        Where-Object node_id -eq $nodeId
                ).Count -ne 3) {
                throw "expected three completed $nodeId nodes"
            }
        }
        if (@(
                $execution.completed_nodes | Where-Object node_id -eq "done"
            ).Count -ne 1 -or
            @(
                $execution.generic_model_invocations.PSObject.Properties
            ).Count -ne 3) {
            throw "generic research model/session completion evidence is incomplete"
        }

        $boundaries = @($execution.context_boundaries)
        $groups = @($boundaries | Group-Object -Property {
            $_.identity.run_id
        })
        if ($boundaries.Count -ne 6 -or $groups.Count -ne 3) {
            throw "expected three distinct context identity pairs"
        }
        foreach ($group in $groups) {
            $boundaryNames = @(
                $group.Group | ForEach-Object { $_.identity.boundary } |
                    Sort-Object
            )
            $nodeIds = @(
                $group.Group | ForEach-Object { $_.identity.node_id } |
                    Sort-Object
            )
            if ($group.Count -ne 2 -or
                ($boundaryNames -join ",") -ne
                    "before_model_request,turn_start" -or
                ($nodeIds -join ",") -ne "fresh-context,research") {
                throw "context boundary pair did not retain exact work identity"
            }
        }

        $entries = $execution.canonical_variables.environment.entries
        $entryNames = @(
            $entries.PSObject.Properties | ForEach-Object Name | Sort-Object
        )
        if (($entryNames -join ",") -ne
            "iteration,model_disposition,model_result,receipt_artifact,research_receipt" -or
            $entries.model_disposition.version -ne 3 -or
            $entries.model_disposition.value.kind -ne "enum" -or
            $entries.model_disposition.value.value -ne "response_complete" -or
            $entries.model_result.version -ne 3 -or
            $entries.model_result.value.kind -ne "node_result_reference" -or
            $entries.research_receipt.version -ne 3 -or
            $entries.research_receipt.value.kind -ne
                "node_result_reference" -or
            $entries.receipt_artifact.version -ne 3 -or
            $entries.receipt_artifact.value.kind -ne "artifact_reference" -or
            $entries.iteration.version -ne 3 -or
            $entries.iteration.value.kind -ne "map" -or
            $entries.iteration.value.value.remaining.value -ne $false) {
            throw "current research canonical variables are incomplete"
        }

        $records = @(
            $state.artifact_persistences.PSObject.Properties |
                ForEach-Object Value
        )
        if ($records.Count -ne 3) {
            throw "expected three generic research artifact receipts"
        }
        $sessionRoot = Get-SessionRoot $sessionId
        foreach ($record in $records) {
            $hash = [string]$record.identity.content_hash
            $contentPath = Join-Path $sessionRoot (
                "artifacts\style\objects\" + $hash.Substring(0, 2) +
                "\" + $hash + "\content"
            )
            if ($record.state -ne "completed" -or
                $record.mime_type -ne "text/markdown" -or
                -not (Test-Path -LiteralPath $contentPath) -or
                [System.IO.File]::ReadAllText($contentPath) -ne $expectedText) {
                throw "generic research artifact was not provider-visible text"
            }
        }

        foreach ($event in @(
                @("context.boundary_started", 6),
                @("context.boundary_completed", 6),
                @("graph.model_invocation_bound", 3),
                @("model.request_proposed", 3),
                @("model.request_approved", 3),
                @("model.request_started", 3),
                @("model.response_completed", 3),
                @("artifact.persistence_proposed", 3),
                @("artifact.persistence_approved", 3),
                @("artifact.persistence_dispatched", 3),
                @("artifact.persistence_completed", 3),
                @("graph.variable_declared", 5),
                @("graph.variable_assigned", 15),
                @("style.node_entered", 16),
                @("style.node_completed", 16),
                @("style.transition_selected", 15)
            )) {
            Assert-EventCount $sessionId $event[0] $event[1]
        }
        $journalJson = (Read-Journal $sessionId | ConvertTo-Json -Depth 100)
        if ([regex]::Matches(
                $journalJson,
                [regex]::Escape("generic_fresh_context")
            ).Count -ne 3 -or $journalJson.Contains(
                "research_fresh_context"
            )) {
            throw "generic research provenance was not exact"
        }
    }

    $daemon = $null
    try {
        $daemon = Start-TestRuntime

        $legacy = & $cli session create --workspace $repository `
            --style research-loop@1.1.0 --json | ConvertFrom-Json
        $legacyResult = & $cli run "map the repository architecture" `
            --session $legacy.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="deterministic finding"' `
            --option 'research_complete_after=3' --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) {
            Get-Content -LiteralPath $runtimeErr -ErrorAction SilentlyContinue
            throw "frozen research loop failed"
        }
        $legacyInspection = & $cli session inspect $legacy.session_id --json |
            ConvertFrom-Json
        Assert-LegacyResearchState $legacyInspection $legacy.session_id
        $legacyJournalPath = Join-Path (
            Get-SessionRoot $legacy.session_id
        ) "events.jsonl"
        if ($legacyResult.last_committed_sequence -ne
            @(Read-Journal $legacy.session_id).Count) {
            throw "frozen research result does not match journal head"
        }
        $legacyJournalHash = (Get-FileHash -LiteralPath $legacyJournalPath).Hash

        $generic = & $cli session create --workspace $repository `
            --style research-loop@1.2.0 --json | ConvertFrom-Json
        $genericCreated = & $cli session inspect $generic.session_id --json |
            ConvertFrom-Json
        Assert-GenericPlan $genericCreated
        $expectedText = "alpha beta generic research finding"
        $genericResult = & $cli run "map the repository architecture" `
            --session $generic.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="generic research finding"' `
            --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) {
            Get-Content -LiteralPath $runtimeErr -ErrorAction SilentlyContinue
            $failedJournal = Join-Path (
                Get-SessionRoot $generic.session_id
            ) "events.jsonl"
            if (Test-Path -LiteralPath $failedJournal) {
                Get-Content -LiteralPath $failedJournal
            }
            throw "current generic research loop failed"
        }
        $visible = @(
            $genericResult.events | Where-Object event -eq "text" |
                ForEach-Object text
        ) -join ""
        if ($visible -ne ($expectedText + $expectedText + $expectedText)) {
            throw "unexpected generic research provider output: $visible"
        }
        $genericInspection = & $cli session inspect $generic.session_id --json |
            ConvertFrom-Json
        Assert-GenericResearchState `
            $genericInspection $generic.session_id $expectedText
        $genericSessionRoot = Get-SessionRoot $generic.session_id
        $genericJournalPath = Join-Path $genericSessionRoot "events.jsonl"
        if ($genericResult.last_committed_sequence -ne
            @(Read-Journal $generic.session_id).Count) {
            throw "generic research result does not match journal head"
        }
        $genericBinding = $genericInspection.state.style_binding |
            ConvertTo-Json -Depth 100 -Compress
        $genericJournalHash = (
            Get-FileHash -LiteralPath $genericJournalPath
        ).Hash
        $genericStyleJsonHash = (
            Get-FileHash -LiteralPath (
                Join-Path $genericSessionRoot "style.json"
            )
        ).Hash
        $genericStyleLockHash = (
            Get-FileHash -LiteralPath (
                Join-Path $genericSessionRoot "style.lock"
            )
        ).Hash

        Stop-TestRuntime $daemon
        $daemon = Start-TestRuntime

        $legacyRestarted = & $cli session inspect $legacy.session_id --json |
            ConvertFrom-Json
        Assert-LegacyResearchState $legacyRestarted $legacy.session_id
        $legacyReplayed = & $cli session replay $legacy.session_id --json |
            ConvertFrom-Json
        if ($legacyReplayed.command -ne "session_replay") {
            throw "frozen research replay was not reported"
        }
        Assert-LegacyResearchState $legacyReplayed $legacy.session_id
        if ((Get-FileHash -LiteralPath $legacyJournalPath).Hash -ne
            $legacyJournalHash) {
            throw "frozen research replay mutated the canonical journal"
        }

        $genericRestarted = & $cli session inspect $generic.session_id --json |
            ConvertFrom-Json
        Assert-GenericResearchState `
            $genericRestarted $generic.session_id $expectedText
        if (($genericRestarted.state.style_binding |
                ConvertTo-Json -Depth 100 -Compress) -ne $genericBinding -or
            (Get-FileHash -LiteralPath (
                    Join-Path $genericSessionRoot "style.json"
                )).Hash -ne $genericStyleJsonHash -or
            (Get-FileHash -LiteralPath (
                    Join-Path $genericSessionRoot "style.lock"
                )).Hash -ne $genericStyleLockHash -or
            (Get-FileHash -LiteralPath $genericJournalPath).Hash -ne
                $genericJournalHash) {
            throw "generic research binding or journal changed after restart"
        }
        $genericReplayed = & $cli session replay $generic.session_id --json |
            ConvertFrom-Json
        if ($genericReplayed.command -ne "session_replay") {
            throw "generic research replay was not reported"
        }
        Assert-GenericResearchState `
            $genericReplayed $generic.session_id $expectedText
        if (($genericReplayed.state.style_binding |
                ConvertTo-Json -Depth 100 -Compress) -ne $genericBinding -or
            (Get-FileHash -LiteralPath $genericJournalPath).Hash -ne
                $genericJournalHash) {
            throw "generic research replay was not pure"
        }

        Write-Output (
            "runtime research-loop frozen-1.1/current-1.2 exact-plan/" +
            "generic-dispatch/typed-variables/artifacts/context-identities/" +
            "restart/replay E2E passed"
        )
    }
    finally {
        Stop-TestRuntime $daemon
        $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
        $resolvedRun = (Resolve-Path $runRoot).Path
        if ($resolvedRun.StartsWith($resolvedTemp) -and
            (Split-Path $resolvedRun -Leaf).StartsWith(
                "agentmod-research-e2e-"
            )) {
            Remove-Item -LiteralPath $resolvedRun -Recurse -Force
        }
    }
}
finally {
    Remove-Item Env:AGENTMOD_RUNTIME_ENDPOINT -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_RUNTIME_AUTH_TOKEN -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_HARNESS_PROGRAM -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_SCHEDULER_PROGRAM -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_SCHEDULER_ROOT -ErrorAction SilentlyContinue
    Pop-Location
}
$global:LASTEXITCODE = 0
