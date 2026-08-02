$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build --locked -p agentmod-runtime -p agentmod-harness `
        -p agentmod-cli -p agentmod-tui
    if ($LASTEXITCODE -ne 0) { throw "Graph B process build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $tui = (Resolve-Path "target\debug\agentmod-tui.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-graph-b-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $styles = Join-Path $runRoot "styles\user"
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path $styles -Force | Out-Null
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null
    Copy-Item -LiteralPath "tests\fixtures\styles\arbitrary-graph-b.toml" `
        -Destination (Join-Path $styles "arbitrary-graph-b.toml")
    Copy-Item -LiteralPath "tests\fixtures\styles\graph-b-worker.toml" `
        -Destination (Join-Path $styles "graph-b-worker.toml")
    if ($env:AGENTMOD_GRAPH_B_CANCELLATION_ONLY -eq "1") {
        $graphBPath = Join-Path $styles "arbitrary-graph-b.toml"
        $graphB = Get-Content -LiteralPath $graphBPath -Raw
        $graphB = $graphB.Replace(
            'timeout_ms = 60000, cancellation = "cascade"',
            'timeout_ms = 1000, cancellation = "cascade"'
        )
        Set-Content -LiteralPath $graphBPath -Value $graphB -NoNewline
    }

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-graph-b-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_USER_STYLES_DIR = $styles
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
                Invoke-CliJson @("doctor", "--json") | Out-Null
                return $process
            }
            catch {
                if ($process.HasExited) { break }
                Start-Sleep -Milliseconds 100
            }
        }
        if (-not $process.HasExited) {
            Stop-Process -Id $process.Id -Force
            $process.WaitForExit()
        }
        throw "Graph B runtime did not become ready"
    }

    function Stop-TestRuntime($process) {
        if ($null -ne $process -and -not $process.HasExited) {
            Stop-Process -Id $process.Id -Force
            $process.WaitForExit()
        }
    }

    function Read-Journal([string]$SessionId) {
        $path = Join-Path $runRoot (
            "sessions\" + $SessionId + "\events.jsonl"
        )
        if (-not (Test-Path -LiteralPath $path)) { return @() }
        @(
            Get-Content -LiteralPath $path | ForEach-Object {
                ($_ | ConvertFrom-Json).event
            }
        )
    }

    function Event-Count($Events, [string]$Type) {
        @($Events | Where-Object { $_.metadata.event_type -eq $Type }).Count
    }

    function Assert-EventCount(
        $Events,
        [string]$Type,
        [int]$Expected
    ) {
        $actual = Event-Count $Events $Type
        if ($actual -ne $Expected) {
            throw "expected $Expected $Type events, found $actual"
        }
    }

    function Assert-GenerationThreeBinding($Inspection, [string]$Label) {
        $binding = $Inspection.state.style_binding
        $plan = $binding.execution_plan
        if ($plan.compilation.compiler -ne
                "agentmod-runtime-node-plan@3" -or
            [string]::IsNullOrWhiteSpace($binding.execution_plan_hash) -or
            $plan.compilation.compiled_style_hash -ne
                $binding.compiled_style_hash -or
            $plan.compilation.compiled_cache_key -ne
                $binding.compiled_cache_key) {
            throw "$Label did not retain its exact generation-3 execution plan"
        }
    }

    $parentTurnId = [guid]::NewGuid().ToString()
    $parentOptions = @(
        "--option", 'mock_scenario="graph_b_review_sequence"'
    )

    function Invoke-ParentRun([string]$SessionId) {
        $arguments = @(
            "run", "execute arbitrary Graph B child orchestration",
            "--session", $SessionId,
            "--provider", "deterministic-mock",
            "--model", "mock-model",
            "--cancellation-id", $parentTurnId
        ) + $parentOptions + @("--json")
        Invoke-CliJson $arguments
    }

    function Resolve-SpawnApprovals($Result, [int]$Expected = 2) {
        $current = $Result
        for ($approval = 0; $approval -lt $Expected; $approval++) {
            if ([string]::IsNullOrWhiteSpace($current.awaiting_continuation)) {
                throw (
                    "Graph B expected $Expected spawn approvals but found " +
                    "$approval"
                )
            }
            $current = Invoke-CliJson @(
                "approval", "resolve", $script:parentSessionId,
                $current.awaiting_continuation, "approve", "--json"
            )
        }
        $current
    }

    function Get-Children {
        $listed = Invoke-CliJson @("session", "list", "--limit", "64", "--json")
        $children = @()
        foreach ($summary in @($listed.sessions)) {
            if ($summary.id -eq $script:parentSessionId) { continue }
            $inspection = Invoke-CliJson @(
                "session", "inspect", $summary.id, "--json"
            )
            if ($inspection.state.child_origin.parent_session_id -eq
                $script:parentSessionId) {
                $children += $inspection
            }
        }
        @($children | Sort-Object `
            @{ Expression = {
                [int]$_.state.child_origin.revision
            } }, `
            @{ Expression = {
                [long]$_.state.child_origin.parent_action_sequence
            } })
    }

    function Complete-Child($Child, [string]$Text) {
        $childId = $Child.state.id
        $task = $Child.state.child_origin.task
        $result = Invoke-CliJson @(
            "run", $task,
            "--session", $childId,
            "--provider", "deterministic-mock",
            "--model", "mock-model",
            "--option", 'mock_scenario="streaming_text"',
            "--option", ('mock_text="' + $Text + '"'),
            "--cancellation-id", ([guid]::NewGuid().ToString()),
            "--json"
        )
        $inspection = Invoke-CliJson @(
            "session", "inspect", $childId, "--json"
        )
        if ($inspection.state.lifecycle -ne "completed") {
            throw "Graph B child $childId did not complete"
        }
        $result
    }

    $daemon = Start-TestRuntime
    try {
        $validation = Invoke-CliJson @(
            "style", "validate",
            (Join-Path $styles "arbitrary-graph-b.toml"), "--json"
        )
        if (-not $validation.valid) {
            throw "Graph B manifest did not validate"
        }
        $style = Invoke-CliJson @(
            "style", "inspect", "user-graph-b@1.0.0", "--json"
        )
        if ($style.summary.availability -ne "available" -or
            $style.summary.source -ne "user") {
            throw "Graph B user style was not runtime executable"
        }
        $session = Invoke-CliJson @(
            "session", "create", "--workspace", $workspace,
            "--style", "user-graph-b@1.0.0", "--json"
        )
        $script:parentSessionId = $session.session_id
        $created = Invoke-CliJson @(
            "session", "inspect", $parentSessionId, "--json"
        )
        Assert-GenerationThreeBinding $created "Graph B parent"
        $plan = $created.state.style_binding.execution_plan
        if (@($plan.nodes).Count -ne 12 -or
            [string]::IsNullOrWhiteSpace(
                $created.state.style_binding.execution_plan_hash
            ) -or [string]::IsNullOrWhiteSpace($plan.registry_hash)) {
            throw "Graph B immutable execution plan was not persisted"
        }
        foreach ($node in @($plan.nodes)) {
            if ($node.source.kind -ne "runtime" -or
                $node.boundary -ne "runtime_logic" -or
                [string]::IsNullOrWhiteSpace($node.executor_id) -or
                [string]::IsNullOrWhiteSpace($node.executor_version)) {
                throw "Graph B retained an incomplete executor resolution"
            }
        }

        $waiting = Invoke-ParentRun $parentSessionId
        if ([string]::IsNullOrWhiteSpace($waiting.awaiting_continuation)) {
            throw "Graph B did not durably request child-creation approval"
        }
        Stop-TestRuntime $daemon
        $daemon = Start-TestRuntime
        $waiting = Resolve-SpawnApprovals $waiting
        if ([string]::IsNullOrWhiteSpace($waiting.awaiting_continuation)) {
            throw "Graph B did not return the durable child-wait continuation"
        }
        $activeParent = Invoke-CliJson @(
            "session", "inspect", $parentSessionId, "--json"
        )
        $activeContract =
            $activeParent.state.style_execution.execution_contract
        if ($activeContract.execution_plan_hash -ne
                $activeParent.state.style_binding.execution_plan_hash -or
            $activeContract.registry_hash -ne
                $activeParent.state.style_binding.execution_plan.registry_hash -or
            @($activeContract.node_executors).Count -ne @($plan.nodes).Count) {
            throw "Graph B generic dispatch did not retain its exact plan contract"
        }

        $children = Get-Children
        if ($children.Count -ne 2) {
            throw "Graph B did not create exactly two first-revision children"
        }
        $childPlanHash = $null
        foreach ($child in $children) {
            Assert-GenerationThreeBinding $child "Graph B child"
            if ($null -eq $childPlanHash) {
                $childPlanHash =
                    $child.state.style_binding.execution_plan_hash
            }
            elseif ($child.state.style_binding.execution_plan_hash -ne
                    $childPlanHash) {
                throw "Graph B worker bindings did not retain one exact plan"
            }
            if ($child.state.lifecycle -ne "active") {
                throw "Graph B message target was not active"
            }
            $childJournal = Read-Journal $child.state.id
            Assert-EventCount $childJournal "child_agent.message_received" 1
            if (@(
                $childJournal |
                    Where-Object {
                        $_.metadata.event_type -eq "conversation.entry_committed" -and
                        $_.payload.payload.entry.kind -eq "user_message" -and
                        $_.payload.payload.entry.text -like "*revision_context*"
                    }
            ).Count -ne 0) {
                throw "Graph B child message was fabricated as a user message"
            }
        }

        if ($env:AGENTMOD_GRAPH_B_CANCELLATION_ONLY -eq "1") {
            Start-Sleep -Milliseconds 1500
            $cancellation = Invoke-ParentRun $parentSessionId
            $requested = Read-Journal $parentSessionId
            Assert-EventCount $requested `
                "child_agent.generic_cancellation_requested" 2
            Assert-EventCount $requested `
                "child_agent.generic_cancellation_authorized" 0
            Assert-EventCount $requested `
                "child_agent.generic_cancellation_dispatched" 0
            if ([string]::IsNullOrWhiteSpace(
                    $cancellation.awaiting_continuation
                )) {
                throw "Graph B cancellation did not enter durable Ask"
            }
            Stop-TestRuntime $daemon
            $daemon = Start-TestRuntime
            $cancelled = Resolve-SpawnApprovals $cancellation 2
            $cancelEvents = Read-Journal $parentSessionId
            Assert-EventCount $cancelEvents `
                "child_agent.generic_cancellation_requested" 2
            Assert-EventCount $cancelEvents `
                "child_agent.generic_cancellation_authorized" 2
            Assert-EventCount $cancelEvents `
                "child_agent.generic_cancellation_dispatched" 2
            Assert-EventCount $cancelEvents `
                "child_agent.generic_cancellation_completed" 2
            foreach ($child in Get-Children) {
                if ($child.state.lifecycle -ne "cancelled") {
                    throw "Graph B accepted cancellation left a child active"
                }
                $childJournal = Read-Journal $child.state.id
                if (@(
                    $childJournal |
                        Where-Object {
                            $_.metadata.event_type -eq
                                "conversation.entry_committed" -and
                            $_.payload.payload.entry.kind -eq "user_message"
                        }
                ).Count -ne 0) {
                    throw "Graph B cancellation fabricated child user input"
                }
            }
            Stop-TestRuntime $daemon
            $daemon = Start-TestRuntime
            $replayedParent = Invoke-CliJson @(
                "session", "inspect", $parentSessionId, "--json"
            )
            if ($replayedParent.state.lifecycle -ne "failed") {
                throw "Graph B cancellation terminal state did not replay"
            }
            $afterReplay = Read-Journal $parentSessionId
            Assert-EventCount $afterReplay `
                "child_agent.generic_cancellation_dispatched" 2
            Assert-EventCount $afterReplay `
                "child_agent.generic_cancellation_completed" 2
            Write-Output (
                "runtime arbitrary Graph B accepted cancellation/" +
                "Ask-restart/exact-receipt E2E passed"
            )
            $succeeded = $true
            return
        }

        $parentBeforeChildren = Read-Journal $parentSessionId
        Assert-EventCount $parentBeforeChildren "child_agent.generic_created" 2
        Assert-EventCount $parentBeforeChildren "child_agent.message_delivered" 2

        Complete-Child $children[1] "worker-b-complete-first" | Out-Null
        Stop-TestRuntime $daemon
        $daemon = Start-TestRuntime
        $waiting = Invoke-ParentRun $parentSessionId
        $midEvents = Read-Journal $parentSessionId
        if ((Event-Count $midEvents "child_agent.wait_projected") -lt 2 -or
            (Event-Count $midEvents "graph.generic_join_ready") -ne 0) {
            throw "Graph B join became ready before every child completed"
        }

        Complete-Child $children[0] "worker-a-complete-second" | Out-Null
        $waiting = Invoke-ParentRun $parentSessionId
        $waiting = Resolve-SpawnApprovals $waiting
        if ([string]::IsNullOrWhiteSpace($waiting.awaiting_continuation)) {
            throw "Graph B revision did not return its child-wait continuation"
        }
        $fanoutEntries = @(
            Read-Journal $parentSessionId |
                Where-Object {
                    $_.metadata.event_type -eq
                        "graph.parallel_branch_node_entered"
                }
        )
        if ($fanoutEntries.Count -ne 4 -or @(
            $fanoutEntries | Where-Object {
                $_.payload.payload.work.loop_iteration -ne
                    $_.payload.payload.owner.loop_iteration
            }
        ).Count -ne 0) {
            throw "Graph B parallel branch work did not inherit its owner revision"
        }
        $allChildren = Get-Children
        if ($allChildren.Count -ne 4) {
            throw "Graph B review revision did not create two revised children"
        }
        foreach ($child in $allChildren) {
            Assert-GenerationThreeBinding $child "Graph B child"
            if ($child.state.style_binding.execution_plan_hash -ne
                    $childPlanHash) {
                throw "Graph B revised worker rebound its execution plan"
            }
        }
        $revisionChildren = @(
            $allChildren | Where-Object {
                $_.state.child_origin.revision -eq 1
            }
        )
        if ($revisionChildren.Count -ne 2) {
            throw "Graph B review revision child identities were not exact"
        }
        foreach ($child in @($revisionChildren | Sort-Object {
            $_.state.child_origin.task_id
        } -Descending)) {
            Complete-Child $child (
                "revision-" + $child.state.child_origin.task_id + "-complete"
            ) | Out-Null
        }

        $completedTurn = $null
        $completed = Invoke-CliJson @(
            "session", "inspect", $parentSessionId, "--json"
        )
        $priorSequence = [long]$completed.state.last_sequence
        for ($recovery = 0; $recovery -lt 4; $recovery++) {
            $completedTurn = Invoke-ParentRun $parentSessionId
            $completed = Invoke-CliJson @(
                "session", "inspect", $parentSessionId, "--json"
            )
            if ($completed.state.lifecycle -eq "completed") { break }
            $currentSequence = [long]$completed.state.last_sequence
            if ([string]::IsNullOrWhiteSpace(
                    $completedTurn.awaiting_continuation
                ) -or $currentSequence -le $priorSequence) {
                throw "Graph B recovery drive stalled without a durable wait"
            }
            $priorSequence = $currentSequence
        }
        if ($completed.state.lifecycle -ne "completed" -or
            $completed.state.style_execution.termination_reason -ne
                "complete_session") {
            throw "Graph B did not reach successful terminal completion"
        }
        $resources = @(
            & $tui --smoke-session-command $parentSessionId "/resources" 2>&1
        ) -join [Environment]::NewLine
        if ($LASTEXITCODE -ne 0 -or
            $resources -notmatch "selected=$parentSessionId" -or
            $resources -notmatch "resources=0/4/0") {
            throw "TUI canonical child resource projection failed: $resources"
        }

        $events = Read-Journal $parentSessionId
        foreach ($expectation in @(
            @("style.execution_initialized", 1),
            @("child_agent.generic_creation_dispatched", 4),
            @("child_agent.generic_created", 4),
            @("child_agent.message_dispatched", 4),
            @("child_agent.message_delivered", 4),
            @("graph.parallel_initialized", 2),
            @("graph.generic_join_ready", 2),
            @("style.generic_review_routed", 2)
        )) {
            Assert-EventCount $events $expectation[0] $expectation[1]
        }
        $reviews = @(
            $events |
                Where-Object {
                    $_.metadata.event_type -eq "style.generic_review_routed"
                }
        )
        if ($reviews[0].payload.payload.evidence.disposition -ne "revision" -or
            $reviews[1].payload.payload.evidence.disposition -ne "approved") {
            throw "Graph B reviewer did not reject then approve"
        }
        $variables = $completed.state.style_execution.canonical_variables.
            environment.entries
        if ($variables.worker_a.version -ne 2 -or
            $variables.worker_b.version -ne 2) {
            throw "Graph B revised child identities were not canonical"
        }

        $beforeReplay = $events.Count
        $replayed = Invoke-CliJson @(
            "session", "replay", $parentSessionId, "--json"
        )
        if ($replayed.state.lifecycle -ne "completed" -or
            $replayed.state.style_binding.execution_plan_hash -ne
                $created.state.style_binding.execution_plan_hash -or
            @($replayed.state.child_agents.PSObject.Properties).Count -ne 4) {
            throw "Graph B pure replay did not reconstruct exact child state"
        }
        if ((Read-Journal $parentSessionId).Count -ne $beforeReplay) {
            throw "Graph B replay appended journal effects"
        }

        Write-Output (
            "runtime arbitrary Graph B spawn/message/wait/join/review/" +
            "revision/restart/replay E2E passed"
        )
        $succeeded = $true
    }
    catch {
        if (Test-Path -LiteralPath $runtimeErr) {
            Get-Content -LiteralPath $runtimeErr -Tail 100
        }
        if ($script:parentSessionId) {
            $journal = Join-Path $runRoot (
                "sessions\" + $script:parentSessionId + "\events.jsonl"
            )
            if (Test-Path -LiteralPath $journal) {
                Write-Output "journal: $journal"
                Get-Content -LiteralPath $journal -Tail 60
            }
        }
        throw
    }
    finally {
        Stop-TestRuntime $daemon
        if ($succeeded) {
            $temp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
            $resolved = (Resolve-Path $runRoot).Path
            if ($resolved.StartsWith($temp) -and
                $resolved -like "*agentmod-graph-b-e2e-*") {
                Remove-Item -LiteralPath $resolved -Recurse -Force
            }
        }
        else {
            Write-Output "retained failed Graph B E2E root: $runRoot"
        }
    }
}
finally {
    Pop-Location
}
