$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build --locked -p agentmod-runtime -p agentmod-harness `
        -p agentmod-scheduler -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "planner-worker process build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $scheduler = (Resolve-Path "target\debug\agentmod-scheduler.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-planner-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-planner-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_SCHEDULER_PROGRAM = $scheduler
    $env:AGENTMOD_SCHEDULER_ROOT = Join-Path $runRoot "scheduler"
    $runtimeOut = Join-Path $runRoot "runtime.stdout.log"
    $runtimeErr = Join-Path $runRoot "runtime.stderr.log"
    $daemon = $null
    $succeeded = $false

    function Invoke-CliJson([string[]]$Arguments) {
        $output = @(& $cli @Arguments 2>&1)
        $exit = $LASTEXITCODE
        if ($exit -ne 0) {
            throw (
                "CLI failed ($exit): agentmod " + ($Arguments -join " ") +
                [Environment]::NewLine +
                (($output | ForEach-Object { $_.ToString() }) -join
                    [Environment]::NewLine)
            )
        }
        (($output | ForEach-Object { $_.ToString() }) -join
            [Environment]::NewLine) | ConvertFrom-Json
    }

    function Start-TestRuntime {
        $process = Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput $runtimeOut -RedirectStandardError $runtimeErr
        for ($attempt = 0; $attempt -lt 120; $attempt++) {
            try {
                $doctor = Invoke-CliJson @("doctor", "--json")
                if ($doctor.state -eq "ready") { return $process }
            }
            catch {
                if ($process.HasExited) { break }
            }
            Start-Sleep -Milliseconds 100
        }
        if (-not $process.HasExited) {
            Stop-Process -Id $process.Id -Force
            $process.WaitForExit()
        }
        throw "planner-worker runtime did not become ready"
    }

    function Stop-TestRuntime($Process) {
        if ($null -ne $Process -and -not $Process.HasExited) {
            Stop-Process -Id $Process.Id -Force
            $Process.WaitForExit()
        }
    }

    function Get-SessionRoot([string]$SessionId) {
        Join-Path $runRoot ("sessions\" + $SessionId)
    }

    function Read-Journal([string]$SessionId) {
        $path = Join-Path (Get-SessionRoot $SessionId) "events.jsonl"
        if (-not (Test-Path -LiteralPath $path)) { return @() }
        @(
            Get-Content -LiteralPath $path | ForEach-Object {
                ($_ | ConvertFrom-Json).event
            }
        )
    }

    function Assert-EventCount(
        $Events,
        [string]$EventType,
        [int]$Expected
    ) {
        $actual = @(
            $Events | Where-Object { $_.metadata.event_type -eq $EventType }
        ).Count
        if ($actual -ne $Expected) {
            throw "expected $Expected $EventType events, found $actual"
        }
    }

    function Assert-LegacyPlannerState($Inspection) {
        $state = $Inspection.state
        if ($state.style_binding.id -ne "planner-worker" -or
            $state.style_binding.version -ne "1.1.0" -or
            $state.lifecycle -ne "completed" -or
            $state.style_execution.termination_reason -ne "complete_session") {
            throw "frozen planner-worker@1.1.0 did not complete exactly"
        }
        if (@($state.planner_worker.tasks.PSObject.Properties).Count -ne 2 -or
            @($state.planner_worker.reviews).Count -ne 2 -or
            $state.planner_worker.reviews[0].approved -ne $false -or
            $state.planner_worker.reviews[1].approved -ne $true -or
            @($state.child_agents.PSObject.Properties).Count -ne 3 -or
            @($state.planner_worker.joins).Count -ne 2) {
            throw "frozen planner-worker orchestration evidence is incomplete"
        }
    }

    function Assert-GenericPlan($Inspection) {
        $binding = $Inspection.state.style_binding
        $plan = $binding.execution_plan
        $expected = @{
            "plan" = @("runtime.model-request", "1.0.0")
            "plan-route" = @("runtime.conditional", "1.0.0")
            "spawn-planner" = @("runtime.child-spawn", "1.0.0")
            "spawn-evidence" = @("runtime.child-spawn", "1.0.0")
            "worker-fanout" = @("runtime.parallel", "1.0.0")
            "wait-planner" = @("runtime.child-wait", "1.0.0")
            "wait-evidence" = @("runtime.child-wait", "1.0.0")
            "join-workers" = @("runtime.join", "1.0.0")
            "integrate" = @("runtime.model-request", "1.0.0")
            "integration-route" = @("runtime.conditional", "1.0.0")
            "persist-integration" = @(
                "runtime.artifact-persistence", "1.0.0"
            )
            "review" = @("runtime.review", "1.0.0")
            "revision" = @("runtime.loop", "1.0.0")
            "done" = @("runtime.session-completion", "1.0.0")
            "structured-failure" = @(
                "runtime.structured-failure", "1.0.0"
            )
        }
        if ($binding.id -ne "planner-worker" -or
            $binding.version -ne "1.3.0" -or
            $plan.compilation.compiler -ne "agentmod-runtime-node-plan@3" -or
            @($plan.nodes).Count -ne $expected.Count -or
            [string]::IsNullOrWhiteSpace($binding.execution_plan_hash) -or
            [string]::IsNullOrWhiteSpace($plan.registry_hash) -or
            $plan.compilation.compiled_style_hash -ne
                $binding.compiled_style_hash -or
            $plan.compilation.compiled_cache_key -ne
                $binding.compiled_cache_key) {
            throw "current planner-worker immutable execution plan is incomplete"
        }
        foreach ($nodeId in $expected.Keys) {
            $resolution = @($plan.nodes | Where-Object node_id -eq $nodeId)
            if ($resolution.Count -ne 1 -or
                $resolution[0].executor_id -ne $expected[$nodeId][0] -or
                $resolution[0].executor_version -ne $expected[$nodeId][1] -or
                $resolution[0].source.kind -ne "runtime" -or
                $resolution[0].boundary -ne "runtime_logic") {
                throw "invalid exact planner executor resolution for $nodeId"
            }
        }
    }

    function Assert-GenericPlannerState(
        $Inspection,
        [string]$SessionId
    ) {
        Assert-GenericPlan $Inspection
        $state = $Inspection.state
        $execution = $state.style_execution
        $binding = $state.style_binding
        if ($state.lifecycle -ne "completed" -or
            $execution.termination_reason -ne "complete_session" -or
            $null -ne $execution.active_node) {
            throw "current planner-worker generic execution did not complete"
        }
        $contract = $execution.execution_contract
        if ($contract.execution_plan_hash -ne $binding.execution_plan_hash -or
            $contract.registry_hash -ne $binding.execution_plan.registry_hash -or
            @($contract.node_executors).Count -ne 15) {
            throw "current planner-worker execution contract changed"
        }

        $expectedCompletions = @{
            "plan" = 1
            "plan-route" = 1
            "worker-fanout" = 2
            "spawn-planner" = 0
            "spawn-evidence" = 0
            "wait-planner" = 0
            "wait-evidence" = 0
            "join-workers" = 2
            "integrate" = 2
            "integration-route" = 2
            "persist-integration" = 2
            "review" = 2
            "revision" = 1
            "done" = 1
            "structured-failure" = 0
        }
        foreach ($nodeId in $expectedCompletions.Keys) {
            $actual = @(
                $execution.completed_nodes | Where-Object node_id -eq $nodeId
            ).Count
            if ($actual -ne $expectedCompletions[$nodeId]) {
                throw "unexpected completion count for planner node $nodeId"
            }
        }

        $entries = $execution.canonical_variables.environment.entries
        $names = @(
            $entries.PSObject.Properties | ForEach-Object Name | Sort-Object
        )
        $expectedNames = @(
            "evidence_child", "evidence_task", "integration_artifact",
            "integration_disposition", "integration_result", "iteration",
            "joined_results", "plan_disposition", "plan_result",
            "planner_child", "planner_task"
        )
        if (($names -join ",") -ne ($expectedNames -join ",") -or
            $entries.plan_disposition.version -ne 1 -or
            $entries.plan_disposition.value.value -ne "response_complete" -or
            $entries.planner_task.version -ne 1 -or
            $entries.planner_task.value.kind -ne "string" -or
            $entries.evidence_task.version -ne 1 -or
            $entries.evidence_task.value.kind -ne "string" -or
            $entries.planner_child.version -ne 2 -or
            $entries.planner_child.value.kind -ne "child_id" -or
            $entries.evidence_child.version -ne 2 -or
            $entries.evidence_child.value.kind -ne "child_id" -or
            $entries.joined_results.version -ne 2 -or
            $entries.joined_results.value.kind -ne "node_result_reference" -or
            $entries.integration_disposition.version -ne 2 -or
            $entries.integration_disposition.value.value -ne
                "response_complete" -or
            $entries.integration_result.version -ne 2 -or
            $entries.integration_result.value.kind -ne
                "node_result_reference" -or
            $entries.integration_artifact.version -ne 2 -or
            $entries.integration_artifact.value.kind -ne
                "artifact_reference" -or
            $entries.iteration.version -ne 1 -or
            $entries.iteration.value.value.remaining.value -ne
                $true) {
            throw "planner-worker canonical variable reconstruction is incomplete"
        }

        $children = @($state.child_agents.PSObject.Properties |
            ForEach-Object Value)
        if ($children.Count -ne 4 -or
            @($children | Where-Object {
                $_.identity.task_id -eq "planner-task-0"
            }).Count -ne 2 -or
            @($children | Where-Object {
                $_.identity.task_id -eq "evidence-task-0"
            }).Count -ne 2 -or
            @($children | Where-Object {
                $_.state -ne "completed"
            }).Count -ne 0) {
            throw "planner-worker child revisions were not reconstructed"
        }
        foreach ($child in $children) {
            $lease = $child.workspace_lease
            if ($null -eq $lease -or
                $lease.mode.mode -ne "shared_read_only" -or
                $lease.ownership -ne "borrowed_read_only" -or
                $lease.owner.parent_session_id -ne $SessionId -or
                $lease.owner.task_id -ne $child.identity.task_id -or
                [string]::IsNullOrWhiteSpace($lease.lease_id) -or
                [string]::IsNullOrWhiteSpace($lease.lease_hash) -or
                $lease.source_snapshot_hash -ne
                    $lease.materialized_snapshot_hash) {
                throw "planner-worker child workspace lease is not exact"
            }
            $childEvents = Read-Journal $child.child_session_id
            Assert-EventCount $childEvents `
                "child_session.workspace_lease_bound" 1
        }

        $artifacts = @($state.artifact_persistences.PSObject.Properties |
            ForEach-Object Value)
        if ($artifacts.Count -ne 2) {
            throw "planner-worker did not retain two integration artifacts"
        }
        $iterations = @()
        foreach ($artifact in $artifacts) {
            $hash = [string]$artifact.identity.content_hash
            $contentPath = Join-Path (Get-SessionRoot $SessionId) (
                "artifacts\style\objects\" + $hash.Substring(0, 2) +
                "\" + $hash + "\content"
            )
            if ($artifact.state -ne "completed" -or
                $artifact.mime_type -ne "text/markdown" -or
                -not (Test-Path -LiteralPath $contentPath)) {
                throw "planner-worker artifact receipt is incomplete"
            }
            $content = [System.IO.File]::ReadAllText($contentPath) |
                ConvertFrom-Json
            if ($content.integration -ne
                    "combined runtime-owned child handoffs" -or
                $content.tests -ne "deterministic fixture passed") {
                throw "planner-worker artifact was not exact provider text"
            }
            $iterations += [int]$artifact.identity.loop_iteration
        }
        if ((($iterations | Sort-Object) -join ",") -ne "0,1") {
            throw "planner-worker integration artifacts lost loop identity"
        }

        $events = Read-Journal $SessionId
        foreach ($expected in @(
            @("style.execution_initialized", 1),
            @("graph.variable_declared", 11),
            @("child_agent.generic_creation_dispatched", 4),
            @("child_agent.generic_created", 4),
            @("graph.parallel_initialized", 2),
            @("graph.parallel_branch_dispatched", 4),
            @("graph.generic_join_ready", 2),
            @("artifact.persistence_completed", 2),
            @("style.generic_review_routed", 2)
        )) {
            Assert-EventCount $events $expected[0] $expected[1]
        }
        $reviews = @($events | Where-Object {
            $_.metadata.event_type -eq "style.generic_review_routed"
        })
        $revisionReview = $reviews[0].payload.payload
        $revisionEvidence = $revisionReview.evidence
        $approvedReview = $reviews[1].payload.payload
        $approvedEvidence = $approvedReview.evidence
        if ($revisionReview.approved -ne $false -or
            ($revisionReview.rejected_task_ids -join ",") -ne
                "evidence-task-0" -or
            ($revisionReview.findings -join ",") -ne
                "evidence task requires one artifact-bound revision" -or
            $revisionEvidence.disposition -ne "revision" -or
            $revisionEvidence.destination_node_id -ne "revision" -or
            $revisionEvidence.current_revision -ne 0 -or
            $revisionEvidence.next_revision -ne 1 -or
            @($revisionEvidence.structured_findings).Count -ne 1 -or
            $revisionEvidence.structured_findings[0].code -ne
                "planner.evidence_revision" -or
            $revisionEvidence.structured_findings[0].message -ne
                "evidence task requires one artifact-bound revision" -or
            (@(
                $revisionEvidence.structured_findings[0].artifact_references
            ) -join ",") -ne "integration_artifact" -or
            [string]::IsNullOrWhiteSpace($revisionEvidence.evidence_hash) -or
            [string]::IsNullOrWhiteSpace(
                $revisionEvidence.application_hash
            ) -or
            $approvedReview.approved -ne $true -or
            @($approvedReview.rejected_task_ids).Count -ne 0 -or
            @($approvedReview.findings).Count -ne 0 -or
            $approvedEvidence.disposition -ne "approved" -or
            $approvedEvidence.destination_node_id -ne "done" -or
            $approvedEvidence.current_revision -ne 1 -or
            $null -ne $approvedEvidence.next_revision -or
            @($approvedEvidence.structured_findings).Count -ne 0 -or
            [string]::IsNullOrWhiteSpace($approvedEvidence.evidence_hash) -or
            [string]::IsNullOrWhiteSpace(
                $approvedEvidence.application_hash
            )) {
            throw "planner-worker structured reviewer evidence is not exact"
        }
        $parallelEntries = @($events | Where-Object {
            $_.metadata.event_type -eq "graph.parallel_branch_node_entered"
        })
        if ($parallelEntries.Count -ne 8 -or
            @($parallelEntries | Where-Object {
                $_.payload.payload.work.loop_iteration -ne
                    $_.payload.payload.owner.loop_iteration
            }).Count -ne 0) {
            throw "planner-worker parallel work lost stable loop identity"
        }
        $parallelCompletions = @($events | Where-Object {
            $_.metadata.event_type -eq
                "graph.parallel_branch_node_completed"
        })
        if ($parallelCompletions.Count -ne 8 -or
            @($parallelCompletions | Where-Object {
                $_.payload.payload.entered.work.node_id -eq "spawn-planner"
            }).Count -ne 2 -or
            @($parallelCompletions | Where-Object {
                $_.payload.payload.entered.work.node_id -eq "spawn-evidence"
            }).Count -ne 2 -or
            @($parallelCompletions | Where-Object {
                $_.payload.payload.entered.work.node_id -eq "wait-planner"
            }).Count -ne 2 -or
            @($parallelCompletions | Where-Object {
                $_.payload.payload.entered.work.node_id -eq "wait-evidence"
            }).Count -ne 2) {
            throw "planner-worker branch completions were not reconstructed"
        }
    }

    function Invoke-GenericRun([string]$SessionId) {
        Invoke-CliJson @(
            "run", "verify generic planner child orchestration",
            "--session", $SessionId,
            "--provider", "deterministic-mock",
            "--model", "mock-model",
            "--cancellation-id", $script:genericTurnId,
            "--option", 'mock_scenario="planner_worker"',
            "--json"
        )
    }

    function Resolve-SpawnApprovals($Result, [int]$Expected = 2) {
        $current = $Result
        $continuation = [string]$current.awaiting_continuation
        for ($approval = 0; $approval -lt $Expected; $approval++) {
            if ([string]::IsNullOrWhiteSpace($continuation)) {
                throw "expected $Expected child approvals, found $approval"
            }
            $current = Invoke-CliJson @(
                "approval", "resolve", $script:genericSessionId,
                $continuation, "approve", "--json"
            )
            if ($approval + 1 -lt $Expected) {
                $inspection = Invoke-CliJson @(
                    "session", "inspect", $script:genericSessionId, "--json"
                )
                $pending = @($inspection.state.approvals.PSObject.Properties |
                    Where-Object { $_.Value.state -eq "pending" } |
                    Sort-Object Name)
                if ($pending.Count -ne ($Expected - $approval - 1)) {
                    throw "pending child approval set is not exact"
                }
                $continuation = [string]$pending[0].Name
            }
        }
        $current
    }

    function Get-GenericChildren {
        $listed = Invoke-CliJson @("session", "list", "--limit", "64", "--json")
        $children = @()
        foreach ($summary in @($listed.sessions)) {
            if ($summary.id -eq $script:genericSessionId) { continue }
            $inspection = Invoke-CliJson @(
                "session", "inspect", $summary.id, "--json"
            )
            if ($inspection.state.child_origin.parent_session_id -eq
                $script:genericSessionId) {
                $children += $inspection
            }
        }
        @($children | Sort-Object `
            @{ Expression = { [int]$_.state.child_origin.revision } }, `
            @{ Expression = { $_.state.child_origin.task_id } })
    }

    function Complete-Child($Child, [string]$Text) {
        $childId = $Child.state.id
        $task = $Child.state.child_origin.task
        Invoke-CliJson @(
            "run", $task,
            "--session", $childId,
            "--provider", "deterministic-mock",
            "--model", "mock-model",
            "--option", 'mock_scenario="streaming_text"',
            "--option", ('mock_text="' + $Text + '"'),
            "--cancellation-id", ([guid]::NewGuid().ToString()),
            "--json"
        ) | Out-Null
        $inspection = Invoke-CliJson @(
            "session", "inspect", $childId, "--json"
        )
        if ($inspection.state.lifecycle -ne "completed") {
            throw "planner child $childId did not complete"
        }
    }

    $daemon = Start-TestRuntime
    try {
        $legacy = Invoke-CliJson @(
            "session", "create", "--workspace", $repository,
            "--style", "planner-worker@1.1.0", "--json"
        )
        $legacyResult = Invoke-CliJson @(
            "run", "verify frozen planner child orchestration",
            "--session", $legacy.session_id,
            "--provider", "mock",
            "--model", "mock-model",
            "--option", 'mock_scenario="planner_worker"', "--json"
        )
        $legacyInspection = Invoke-CliJson @(
            "session", "inspect", $legacy.session_id, "--json"
        )
        Assert-LegacyPlannerState $legacyInspection
        $legacyEvents = Read-Journal $legacy.session_id
        Assert-EventCount $legacyEvents "child_agent.created" 3
        Assert-EventCount $legacyEvents "child_agent.join_completed" 2
        Assert-EventCount $legacyEvents "style.reviewer_findings_committed" 2
        if ($legacyResult.last_committed_sequence -ne $legacyEvents.Count) {
            throw "frozen planner result does not match journal head"
        }
        $legacyJournalPath = Join-Path (
            Get-SessionRoot $legacy.session_id
        ) "events.jsonl"
        $legacyJournalHash = (
            Get-FileHash -LiteralPath $legacyJournalPath
        ).Hash

        $generic = Invoke-CliJson @(
            "session", "create", "--workspace", $repository,
            "--style", "planner-worker@1.3.0", "--json"
        )
        $script:genericSessionId = $generic.session_id
        $script:genericTurnId = [guid]::NewGuid().ToString()
        $created = Invoke-CliJson @(
            "session", "inspect", $genericSessionId, "--json"
        )
        Assert-GenericPlan $created

        $waiting = Invoke-GenericRun $genericSessionId
        if ([string]::IsNullOrWhiteSpace($waiting.awaiting_continuation)) {
            throw "current planner did not durably request spawn approval"
        }
        Stop-TestRuntime $daemon
        $daemon = Start-TestRuntime
        $waiting = Resolve-SpawnApprovals $waiting
        if ([string]::IsNullOrWhiteSpace($waiting.awaiting_continuation)) {
            throw "current planner did not retain its child wait"
        }

        $children = Get-GenericChildren
        if ($children.Count -ne 2) {
            throw "current planner did not create two initial children"
        }
        Complete-Child $children[1] "evidence-child-completed-first"
        Stop-TestRuntime $daemon
        $daemon = Start-TestRuntime
        $waiting = Invoke-GenericRun $genericSessionId
        $midEvents = Read-Journal $genericSessionId
        if (@($midEvents | Where-Object {
            $_.metadata.event_type -eq "graph.generic_join_ready"
        }).Count -ne 0) {
            throw "current planner joined before both children completed"
        }

        Complete-Child $children[0] "planner-child-completed-second"
        $waiting = Invoke-GenericRun $genericSessionId
        $waiting = Resolve-SpawnApprovals $waiting
        if ([string]::IsNullOrWhiteSpace($waiting.awaiting_continuation)) {
            throw "current planner revision did not retain its child wait"
        }
        $allChildren = Get-GenericChildren
        if ($allChildren.Count -ne 4) {
            throw "current planner review did not create two revised children"
        }
        $revisionChildren = @($allChildren | Where-Object {
            $_.state.child_origin.revision -eq 1
        })
        if ($revisionChildren.Count -ne 2) {
            throw "current planner revision identities are incomplete"
        }
        Complete-Child $revisionChildren[1] "revised-evidence-completed-first"
        Complete-Child $revisionChildren[0] "revised-planner-completed-second"

        $completedResult = $null
        $completed = Invoke-CliJson @(
            "session", "inspect", $genericSessionId, "--json"
        )
        for ($recovery = 0; $recovery -lt 4; $recovery++) {
            $completedResult = Invoke-GenericRun $genericSessionId
            $completed = Invoke-CliJson @(
                "session", "inspect", $genericSessionId, "--json"
            )
            if ($completed.state.lifecycle -eq "completed") { break }
            if ([string]::IsNullOrWhiteSpace(
                    $completedResult.awaiting_continuation
                )) {
                throw "current planner recovery stalled without a durable wait"
            }
        }
        Assert-GenericPlannerState $completed $genericSessionId
        $genericSessionRoot = Get-SessionRoot $genericSessionId
        $genericJournalPath = Join-Path $genericSessionRoot "events.jsonl"
        if ($completedResult.last_committed_sequence -ne
            @(Read-Journal $genericSessionId).Count) {
            throw "current planner result does not match journal head"
        }
        $bindingJson = $completed.state.style_binding |
            ConvertTo-Json -Depth 100 -Compress
        $journalHash = (
            Get-FileHash -LiteralPath $genericJournalPath
        ).Hash
        $styleJsonHash = (
            Get-FileHash -LiteralPath (
                Join-Path $genericSessionRoot "style.json"
            )
        ).Hash
        $styleLockHash = (
            Get-FileHash -LiteralPath (
                Join-Path $genericSessionRoot "style.lock"
            )
        ).Hash

        Stop-TestRuntime $daemon
        $daemon = Start-TestRuntime

        $legacyRestarted = Invoke-CliJson @(
            "session", "inspect", $legacy.session_id, "--json"
        )
        Assert-LegacyPlannerState $legacyRestarted
        $legacyReplayed = Invoke-CliJson @(
            "session", "replay", $legacy.session_id, "--json"
        )
        if ($legacyReplayed.command -ne "session_replay") {
            throw "frozen planner replay was not reported"
        }
        Assert-LegacyPlannerState $legacyReplayed
        if ((Get-FileHash -LiteralPath $legacyJournalPath).Hash -ne
            $legacyJournalHash) {
            throw "frozen planner replay mutated its journal"
        }

        $restarted = Invoke-CliJson @(
            "session", "inspect", $genericSessionId, "--json"
        )
        Assert-GenericPlannerState $restarted $genericSessionId
        if (($restarted.state.style_binding |
                ConvertTo-Json -Depth 100 -Compress) -ne $bindingJson -or
            (Get-FileHash -LiteralPath $genericJournalPath).Hash -ne
                $journalHash -or
            (Get-FileHash -LiteralPath (
                    Join-Path $genericSessionRoot "style.json"
                )).Hash -ne $styleJsonHash -or
            (Get-FileHash -LiteralPath (
                    Join-Path $genericSessionRoot "style.lock"
                )).Hash -ne $styleLockHash) {
            throw "current planner immutable binding changed after restart"
        }
        $replayed = Invoke-CliJson @(
            "session", "replay", $genericSessionId, "--json"
        )
        if ($replayed.command -ne "session_replay") {
            throw "current planner pure replay was not reported"
        }
        Assert-GenericPlannerState $replayed $genericSessionId
        if (($replayed.state.style_binding |
                ConvertTo-Json -Depth 100 -Compress) -ne $bindingJson -or
            (Get-FileHash -LiteralPath $genericJournalPath).Hash -ne
                $journalHash) {
            throw "current planner pure replay mutated canonical state"
        }

        Write-Output (
            "runtime planner-worker frozen-1.1/generic-1.3 exact-plan/" +
            "children/parallel/join/artifact/review-revision/restart/replay " +
            "E2E passed"
        )
        $succeeded = $true
    }
    catch {
        if (Test-Path -LiteralPath $runtimeErr) {
            Get-Content -LiteralPath $runtimeErr -Tail 120
        }
        throw
    }
    finally {
        Stop-TestRuntime $daemon
        if ($succeeded) {
            $temp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
            $resolved = (Resolve-Path $runRoot).Path
            if ($resolved.StartsWith($temp) -and
                $resolved -like "*agentmod-planner-e2e-*") {
                Remove-Item -LiteralPath $resolved -Recurse -Force
            }
        }
        else {
            Write-Output "retained failed planner-worker E2E root: $runRoot"
        }
        Remove-Item Env:AGENTMOD_RUNTIME_ENDPOINT -ErrorAction SilentlyContinue
        Remove-Item Env:AGENTMOD_RUNTIME_AUTH_TOKEN -ErrorAction SilentlyContinue
        Remove-Item Env:AGENTMOD_HARNESS_PROGRAM -ErrorAction SilentlyContinue
        Remove-Item Env:AGENTMOD_SCHEDULER_PROGRAM -ErrorAction SilentlyContinue
        Remove-Item Env:AGENTMOD_SCHEDULER_ROOT -ErrorAction SilentlyContinue
    }
}
finally {
    Pop-Location
}
