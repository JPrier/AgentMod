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
        "agentmod-style-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-style-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_SCHEDULER_PROGRAM = $scheduler
    $env:AGENTMOD_SCHEDULER_ROOT = Join-Path $runRoot "scheduler"

    function Start-TestRuntime {
        $process = Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru
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

    $daemon = Start-TestRuntime
    try {
        $styles = & $cli style list --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "style listing failed" }
        if ($styles.styles.Count -ne 13) {
            throw "expected thirteen exact built-in style versions"
        }
        foreach ($required in @(
            @("persistent-chat", "1.1.0"),
            @("persistent-chat", "1.2.0"),
            @("ephemeral-turn", "1.1.0"),
            @("ephemeral-turn", "1.2.0"),
            @("research-loop", "1.1.0"),
            @("research-loop", "1.2.0"),
            @("research-loop", "1.3.0"),
            @("planner-worker", "1.1.0"),
            @("planner-worker", "1.2.0"),
            @("planner-worker", "1.3.0"),
            @("planner-worker", "1.4.0"),
            @("declarative-graph", "1.1.0"),
            @("declarative-graph", "1.2.0")
        )) {
            $style = @($styles.styles | Where-Object {
                $_.id -eq $required[0] -and $_.version -eq $required[1]
            })
            if ($style.Count -ne 1 -or
                $style[0].availability -ne "available") {
                throw "required style is unavailable: $($required -join '@')"
            }
        }

        $persistent = & $cli session create --workspace $repository `
            --style persistent-chat --json | ConvertFrom-Json
        $persistentLegacy = & $cli session create --workspace $repository `
            --style persistent-chat@1.1.0 --json | ConvertFrom-Json
        $ephemeral = & $cli session create --workspace $repository `
            --style ephemeral-turn@1.1.0 --json | ConvertFrom-Json
        $planner = & $cli session create --workspace $repository `
            --style planner-worker --json | ConvertFrom-Json
        $plannerLegacy = & $cli session create --workspace $repository `
            --style planner-worker@1.1.0 --json | ConvertFrom-Json
        $selected = & $cli session create --workspace $repository `
            --style ephemeral-turn@1.1.0 --memory sqlite-fts `
            --compaction sliding_window --max-iterations 3 --max-steps 40 `
            --max-tokens 100000 --max-cost-micros 1000000 `
            --max-duration-ms 60000 --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "style-bound session creation failed" }

        foreach ($entry in @(
            @($persistent.session_id, "persistent-chat", "1.2.0"),
            @($persistentLegacy.session_id, "persistent-chat", "1.1.0"),
            @($ephemeral.session_id, "ephemeral-turn", "1.1.0"),
            @($planner.session_id, "planner-worker", "1.4.0"),
            @($plannerLegacy.session_id, "planner-worker", "1.1.0"),
            @($selected.session_id, "ephemeral-turn", "1.1.0")
        )) {
            $inspection = & $cli session inspect $entry[0] --json | ConvertFrom-Json
            if ($inspection.state.style_binding.id -ne $entry[1]) {
                throw "session style binding mismatch"
            }
            if ($inspection.state.style_binding.version -ne $entry[2]) {
                throw "session style version mismatch"
            }
            if ($inspection.state.style_binding.harness -ne "native") {
                throw "session harness binding mismatch"
            }
            if ($inspection.state.style_compatibility.status -ne "compatible") {
                throw "new style binding is not compatible"
            }
        }
        $selectedInspection = & $cli session inspect $selected.session_id --json |
            ConvertFrom-Json
        $persistentBindingBeforeRestart = (
            & $cli session inspect $persistent.session_id --json |
                ConvertFrom-Json
        ).state.style_binding | ConvertTo-Json -Depth 100 -Compress
        $persistentLegacyBindingBeforeRestart = (
            & $cli session inspect $persistentLegacy.session_id --json |
                ConvertFrom-Json
        ).state.style_binding | ConvertTo-Json -Depth 100 -Compress
        $plannerInspection = & $cli session inspect $planner.session_id --json |
            ConvertFrom-Json
        $plannerLegacyInspection = & $cli session inspect `
            $plannerLegacy.session_id --json | ConvertFrom-Json
        $plannerBindingBeforeRestart = $plannerInspection.state.style_binding |
            ConvertTo-Json -Depth 100 -Compress
        $plannerLegacyBindingBeforeRestart = (
            $plannerLegacyInspection.state.style_binding |
                ConvertTo-Json -Depth 100 -Compress
        )
        $plannerBinding = $plannerInspection.state.style_binding
        $plannerPlan = $plannerBinding.execution_plan
        $plannerCompiled = $plannerBinding.compiled_style_json |
            ConvertFrom-Json
        $plannerExpected = @{
            "plan" = @("runtime.model-request", "1.0.0")
            "plan-route" = @("runtime.conditional", "1.0.0")
            "spawn-planner" = @("runtime.child-spawn", "1.1.0")
            "spawn-evidence" = @("runtime.child-spawn", "1.1.0")
            "worker-fanout" = @("runtime.parallel", "1.0.0")
            "wait-planner" = @("runtime.child-wait", "1.0.0")
            "wait-evidence" = @("runtime.child-wait", "1.0.0")
            "join-workers" = @("runtime.join", "1.0.0")
            "integrate" = @("runtime.model-request", "1.1.0")
            "integration-route" = @("runtime.conditional", "1.0.0")
            "persist-integration" = @("runtime.artifact-persistence", "1.0.0")
            "review" = @("runtime.review", "1.1.0")
            "revision" = @("runtime.loop", "1.0.0")
            "done" = @("runtime.session-completion", "1.0.0")
            "structured-failure" = @("runtime.structured-failure", "1.0.0")
        }
        if ($plannerBinding.version -ne "1.4.0" -or
            $plannerPlan.compilation.compiler -ne
                "agentmod-runtime-node-plan@3" -or
            @($plannerPlan.nodes).Count -ne 15 -or
            @($plannerCompiled.graph.variables).Count -ne 11) {
            throw "planner-worker current binding is not the typed generic V3 graph"
        }
        foreach ($nodeId in $plannerExpected.Keys) {
            $resolution = @(
                $plannerPlan.nodes | Where-Object node_id -eq $nodeId
            )
            if ($resolution.Count -ne 1 -or
                $resolution[0].executor_id -ne $plannerExpected[$nodeId][0] -or
                $resolution[0].executor_version -ne $plannerExpected[$nodeId][1] -or
                $resolution[0].source.kind -ne "runtime" -or
                $resolution[0].boundary -ne "runtime_logic") {
                throw "planner-worker 1.4 executor mismatch: $nodeId"
            }
        }
        foreach ($configured in @(
            @("plan", "model_request"),
            @("spawn-planner", "spawn_child_agent"),
            @("spawn-evidence", "spawn_child_agent"),
            @("worker-fanout", "parallel_branch"),
            @("wait-planner", "wait_for_agents"),
            @("wait-evidence", "wait_for_agents"),
            @("join-workers", "join_results"),
            @("integrate", "model_request"),
            @("persist-integration", "persist_artifact"),
            @("review", "review")
        )) {
            $node = @(
                $plannerCompiled.graph.nodes |
                    Where-Object id -eq $configured[0]
            )
            if ($node.Count -ne 1 -or
                $node[0].configuration.type -ne $configured[1]) {
                throw "planner-worker 1.4 typed node mismatch: $($configured[0])"
            }
        }
        $plannerLegacyBinding =
            $plannerLegacyInspection.state.style_binding
        $plannerLegacyPlan = $plannerLegacyBinding.execution_plan
        $plannerLegacyCompiled =
            $plannerLegacyBinding.compiled_style_json | ConvertFrom-Json
        $plannerLegacyExpected = @{
            "plan" = @("runtime.model-request", "1.1.0")
            "spawn-workers" = @("runtime.child-spawn", "1.1.0")
            "wait-workers" = @("runtime.child-wait", "1.0.0")
            "integrate" = @("runtime.model-request", "1.1.0")
            "review" = @("runtime.review", "1.1.0")
            "revision" = @("runtime.loop", "1.0.0")
            "done" = @("runtime.session-completion", "1.0.0")
        }
        $legacyNodeIds = @(
            $plannerLegacyCompiled.graph.nodes | ForEach-Object id
        )
        if ($plannerLegacyBinding.version -ne "1.1.0" -or
            $plannerLegacyPlan.compilation.compiler -ne
                "agentmod-runtime-node-plan@2" -or
            @($plannerLegacyPlan.nodes).Count -ne 7 -or
            @($plannerLegacyCompiled.graph.variables).Count -ne 0 -or
            ($legacyNodeIds -join ",") -ne
                "done,integrate,plan,review,revision,spawn-workers,wait-workers" -or
            @(
                $plannerLegacyCompiled.graph.nodes |
                    Where-Object { $null -ne $_.configuration }
            ).Count -ne 0) {
            throw (
                "planner-worker 1.1 exact legacy topology changed: " +
                "version=$($plannerLegacyBinding.version), " +
                "plan_nodes=$(@($plannerLegacyPlan.nodes).Count), " +
                "variables=$(@($plannerLegacyCompiled.graph.variables).Count), " +
                "nodes=$($legacyNodeIds -join ','), " +
                "configured=$(@(
                    $plannerLegacyCompiled.graph.nodes |
                        Where-Object { $null -ne $_.configuration }
                ).Count)"
            )
        }
        foreach ($nodeId in $plannerLegacyExpected.Keys) {
            $resolution = @(
                $plannerLegacyPlan.nodes | Where-Object node_id -eq $nodeId
            )
            if ($resolution.Count -ne 1 -or
                $resolution[0].executor_id -ne
                    $plannerLegacyExpected[$nodeId][0] -or
                $resolution[0].executor_version -ne
                    $plannerLegacyExpected[$nodeId][1] -or
                $resolution[0].source.kind -ne "runtime" -or
                $resolution[0].boundary -ne "runtime_logic") {
                throw "planner-worker 1.1 executor mismatch: $nodeId"
            }
        }
        if ($selectedInspection.state.style_binding.memory.provider -ne
                "sqlite-fts" -or
            $selectedInspection.state.style_binding.compaction.strategy -ne
                "sliding_window" -or
            $selectedInspection.state.style_binding.budgets.max_iterations -ne 3 -or
            $selectedInspection.state.style_binding.budgets.max_tokens -ne 100000) {
            throw "component/budget-selected binding was not persisted"
        }

        $persistentTurn = & $cli run "persistent before restart" `
            --session $persistent.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="persistent-before"' --json | ConvertFrom-Json
        $ephemeralTurn = & $cli run "ephemeral before restart" `
            --session $ephemeral.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="ephemeral-before"' --json | ConvertFrom-Json
        $ephemeralJournal = Join-Path $runRoot (
            "sessions\" + $ephemeral.session_id + "\events.jsonl"
        )
        $persistentJournal = Join-Path $runRoot (
            "sessions\" + $persistent.session_id + "\events.jsonl"
        )
        if ($persistentTurn.last_committed_sequence -ne
                @(Get-Content $persistentJournal).Count -or
            $ephemeralTurn.last_committed_sequence -ne
                @(Get-Content $ephemeralJournal).Count) {
            throw "pre-restart style execution state was incorrect"
        }

        Stop-TestRuntime $daemon
        $daemon = Start-TestRuntime

        foreach ($sessionId in @(
            $persistent.session_id, $persistentLegacy.session_id,
            $ephemeral.session_id, $planner.session_id,
            $plannerLegacy.session_id, $selected.session_id
        )) {
            $inspection = & $cli session inspect $sessionId --json | ConvertFrom-Json
            if ($inspection.state.style_compatibility.status -ne "compatible") {
                throw "style binding did not survive restart"
            }
        }
        $persistentBindingAfterRestart = (
            & $cli session inspect $persistent.session_id --json |
                ConvertFrom-Json
        ).state.style_binding | ConvertTo-Json -Depth 100 -Compress
        if ($persistentBindingAfterRestart -ne $persistentBindingBeforeRestart) {
            throw "persistent exact execution plan was rebound during restart"
        }
        $persistentLegacyBindingAfterRestart = (
            & $cli session inspect $persistentLegacy.session_id --json |
                ConvertFrom-Json
        ).state.style_binding | ConvertTo-Json -Depth 100 -Compress
        if ($persistentLegacyBindingAfterRestart -ne
                $persistentLegacyBindingBeforeRestart) {
            throw "persistent 1.1 binding was rebound during restart"
        }
        $plannerBindingAfterRestart = (
            & $cli session inspect $planner.session_id --json |
                ConvertFrom-Json
        ).state.style_binding | ConvertTo-Json -Depth 100 -Compress
        $plannerLegacyBindingAfterRestart = (
            & $cli session inspect $plannerLegacy.session_id --json |
                ConvertFrom-Json
        ).state.style_binding | ConvertTo-Json -Depth 100 -Compress
        if ($plannerBindingAfterRestart -ne
                $plannerBindingBeforeRestart -or
            $plannerLegacyBindingAfterRestart -ne
                $plannerLegacyBindingBeforeRestart) {
            throw "planner-worker exact bindings were rebound during restart"
        }
        $selectedAfterRestart = & $cli session inspect $selected.session_id --json |
            ConvertFrom-Json
        if ($selectedAfterRestart.state.style_binding.memory.provider -ne
                "sqlite-fts" -or
            $selectedAfterRestart.state.style_binding.compaction.strategy -ne
                "sliding_window" -or
            $selectedAfterRestart.state.style_binding.budgets.max_iterations -ne 3 -or
            $selectedAfterRestart.state.style_binding.budgets.max_tokens -ne 100000) {
            throw "component/budget-selected binding changed after restart"
        }
        & $cli run "selected components after restart" `
            --session $selected.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="selected-after"' --json | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "component-selected session failed after restart"
        }
        $persistentAfter = & $cli run "persistent after restart" `
            --session $persistent.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="persistent-after"' --json | ConvertFrom-Json
        $ephemeralAfter = & $cli run "ephemeral after restart" `
            --session $ephemeral.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="ephemeral-after"' --json | ConvertFrom-Json
        if ($persistentAfter.last_committed_sequence -le
                $persistentTurn.last_committed_sequence -or
            $persistentAfter.last_committed_sequence -ne
                @(Get-Content $persistentJournal).Count -or
            $ephemeralAfter.last_committed_sequence -le
                $ephemeralTurn.last_committed_sequence -or
            $ephemeralAfter.last_committed_sequence -ne
                @(Get-Content $ephemeralJournal).Count) {
            throw "post-restart style execution state was incorrect"
        }

        $branch = & $cli session branch $persistent.session_id `
            --at $persistentTurn.last_committed_sequence `
            --style ephemeral-turn --json | ConvertFrom-Json
        $branchInspection = & $cli session inspect $branch.session_id --json |
            ConvertFrom-Json
        if ($branchInspection.state.style_binding.id -ne "ephemeral-turn") {
            throw "branch did not receive its explicitly selected style"
        }
        & $cli run "parent continues persistently" `
            --session $persistent.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="parent-continuation"' --json | Out-Null
        & $cli run "branch continues ephemerally" `
            --session $branch.session_id `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="branch-continuation"' --json | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "branch continuation failed" }
        $parentAfterBranch = & $cli session inspect $persistent.session_id --json |
            ConvertFrom-Json
        $branchAfter = & $cli session inspect $branch.session_id --json |
            ConvertFrom-Json
        if ($parentAfterBranch.state.style_binding.id -ne "persistent-chat" -or
            $branchAfter.state.style_binding.id -ne "ephemeral-turn" -or
            @($branchAfter.state.conversation.provider_projection).Count -ne 0) {
            throw "branch style execution changed parent or retained ephemeral projection"
        }

        $userStyleRoot = Join-Path $runRoot "styles\user"
        New-Item -ItemType Directory -Path $userStyleRoot -Force | Out-Null
        Set-Content -LiteralPath (
            Join-Path $userStyleRoot "persistent-chat.disabled"
        ) -Value "disabled" -NoNewline
        $disabledInspection = & $cli session inspect $persistent.session_id --json |
            ConvertFrom-Json
        if ($disabledInspection.state.style_compatibility.status -ne "incompatible") {
            throw "disabled persisted style was not reported as incompatible"
        }
        $savedErrorPreference = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            & $cli run "must not silently substitute" `
                --session $persistent.session_id `
                --option 'mock_scenario="streaming_text"' --json 2>$null | Out-Null
            $disabledExit = $LASTEXITCODE
        }
        finally {
            $ErrorActionPreference = $savedErrorPreference
        }
        if ($disabledExit -eq 0) {
            throw "disabled persisted style executed through a fallback"
        }
        Remove-Item -LiteralPath (
            Join-Path $userStyleRoot "persistent-chat.disabled"
        )

        foreach ($sessionId in @(
            $persistent.session_id, $persistentLegacy.session_id,
            $ephemeral.session_id, $planner.session_id,
            $plannerLegacy.session_id
        )) {
            $sessionRoot = Join-Path $runRoot ("sessions\" + $sessionId)
            $metadata = Get-Content (Join-Path $sessionRoot "metadata.json") |
                ConvertFrom-Json
            $styleLock = Get-Content (Join-Path $sessionRoot "style.lock") |
                ConvertFrom-Json
            if ($metadata.schema_version -ne 2 -or
                $null -eq $metadata.style_binding -or
                $null -eq $styleLock.binding -or
                $null -eq $styleLock.compiled) {
                throw "complete style identity was not durably persisted"
            }
        }

        Write-Output "runtime session-style registry/restart/branch E2E passed"
    }
    finally {
        Stop-TestRuntime $daemon
        $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
        $resolvedRun = (Resolve-Path $runRoot).Path
        if ($resolvedRun.StartsWith($resolvedTemp) -and
            (Split-Path $resolvedRun -Leaf).StartsWith("agentmod-style-e2e-")) {
            Remove-Item -LiteralPath $resolvedRun -Recurse -Force
        }
    }
}
finally {
    Remove-Item Env:AGENTMOD_HARNESS_PROGRAM -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_SCHEDULER_PROGRAM -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_SCHEDULER_ROOT -ErrorAction SilentlyContinue
    Pop-Location
}
$global:LASTEXITCODE = 0
