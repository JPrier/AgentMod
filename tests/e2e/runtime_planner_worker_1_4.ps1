$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build --locked -p agentmod-runtime -p agentmod-harness `
        -p agentmod-scheduler -p agentmod-filesystem-host `
        -p agentmod-process-host -p agentmod-git-host -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "planner-worker v1.4 process build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $scheduler = (Resolve-Path "target\debug\agentmod-scheduler.exe").Path
    $filesystemHost = (Resolve-Path "target\debug\agentmod-filesystem-host.exe").Path
    $processHost = (Resolve-Path "target\debug\agentmod-process-host.exe").Path
    $gitHost = (Resolve-Path "target\debug\agentmod-git-host.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    # Keep branch-worktree Cargo outputs below legacy Windows linker path limits.
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "am14-" + [guid]::NewGuid().ToString("N").Substring(0, 8)
    )
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null
    Set-Content -LiteralPath (Join-Path $workspace "worker.txt") `
        -Value "parent-owned" -NoNewline
    New-Item -ItemType Directory -Path (Join-Path $workspace "src") -Force | Out-Null
    @'
[package]
name = "agentmod-planner-worker-fixture"
version = "0.0.0"
edition = "2024"

[workspace]
'@ | Set-Content -LiteralPath (Join-Path $workspace "Cargo.toml") `
        -NoNewline
    @'
#[cfg(test)]
mod tests {
    #[test]
    fn child_workspace_contains_the_owned_edit() {
        assert_eq!(std::fs::read_to_string("worker.txt").unwrap(), "child-owned\n");
    }
}
'@ | Set-Content -LiteralPath (Join-Path $workspace "src\lib.rs") -NoNewline
    foreach ($arguments in @(
        @("init"),
        @("config", "user.email", "agentmod@example.invalid"),
        @("config", "user.name", "AgentMod Fixture"),
        @("add", "worker.txt", "Cargo.toml", "src/lib.rs"),
        @("commit", "-m", "planner v1.4 fixture base")
    )) {
        & git -C $workspace @arguments | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "temporary Git fixture failed" }
    }

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-planner-1-4-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_SCHEDULER_PROGRAM = $scheduler
    $env:AGENTMOD_SCHEDULER_ROOT = Join-Path $runRoot "scheduler"
    $env:AGENTMOD_FILESYSTEM_HOST_PROGRAM = $filesystemHost
    $env:AGENTMOD_PROCESS_HOST_PROGRAM = $processHost
    $env:AGENTMOD_GIT_HOST_PROGRAM = $gitHost
    $env:AGENTMOD_PROCESS_ALLOWED_EXECUTABLES = "cargo"
    $env:AGENTMOD_HARNESS_TEST_GATE_ROOT = Join-Path $runRoot "harness-gates"
    $env:AGENTMOD_PERMISSION_MODE = "allow"
    $runtimeOut = Join-Path $runRoot "runtime.stdout.log"
    $runtimeErr = Join-Path $runRoot "runtime.stderr.log"
    $daemon = $null
    $childRuns = @()
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
        throw "planner-worker v1.4 runtime did not become ready"
    }

    function Stop-TestRuntime($Process) {
        if ($null -ne $Process) {
            $descendants = @()
            $frontier = @($Process.Id)
            while ($frontier.Count -gt 0) {
                $next = @()
                foreach ($parentId in $frontier) {
                    $children = @(Get-CimInstance Win32_Process |
                        Where-Object ParentProcessId -eq $parentId)
                    foreach ($child in $children) {
                        $descendants += [int]$child.ProcessId
                        $next += [int]$child.ProcessId
                    }
                }
                $frontier = $next
            }
            foreach ($processId in @($descendants | Select-Object -Unique |
                    Sort-Object -Descending)) {
                Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
            }
            if (-not $Process.HasExited) {
                Stop-Process -Id $Process.Id -Force
                $Process.WaitForExit()
            }
            $Process.Dispose()
        }
    }

    function Read-Journal([string]$SessionId) {
        $path = Join-Path $runRoot ("sessions\" + $SessionId + "\events.jsonl")
        if (-not (Test-Path -LiteralPath $path)) { return @() }
        @(Get-Content -LiteralPath $path | ForEach-Object {
            ($_ | ConvertFrom-Json).event
        })
    }

    function Invoke-ParentRun([string]$SessionId, [string]$CancellationId) {
        Invoke-CliJson @(
            "run", "produce child-owned diff and test evidence",
            "--session", $SessionId,
            "--provider", "deterministic-mock",
            "--model", "mock-model",
            "--cancellation-id", $CancellationId,
            "--option", 'mock_scenario="planner_worker"',
            "--json"
        )
    }

    function Resolve-Approvals($Result, [string]$ParentSessionId, [int]$Count) {
        $current = $Result
        for ($index = 0; $index -lt $Count; $index++) {
            $inspection = Invoke-CliJson @(
                "session", "inspect", $ParentSessionId, "--json"
            )
            $pending = @($inspection.state.approvals.PSObject.Properties |
                Where-Object { $_.Value.state -eq "pending" } |
                Sort-Object Name)
            if ($pending.Count -lt 1) {
                throw "expected $Count pending child approvals, found $index"
            }
            $current = Invoke-CliJson @(
                "approval", "resolve", $ParentSessionId,
                ([string]$pending[0].Name), "approve", "--json"
            )
        }
        $current
    }

    function Get-Children([string]$ParentSessionId) {
        $listed = Invoke-CliJson @("session", "list", "--limit", "64", "--json")
        $children = @()
        foreach ($summary in @($listed.sessions)) {
            if ($summary.id -eq $ParentSessionId) { continue }
            $inspection = Invoke-CliJson @(
                "session", "inspect", $summary.id, "--json"
            )
            if ($inspection.state.child_origin.parent_session_id -eq $ParentSessionId) {
                $children += $inspection
            }
        }
        @($children | Sort-Object `
            @{ Expression = { [int]$_.state.child_origin.revision } }, `
            @{ Expression = { $_.state.child_origin.task_id } })
    }

    function Start-ChildRun($Child, [string]$GateId) {
        $state = $Child.state
        $stdout = Join-Path $runRoot ("child-" + $state.id + ".stdout.log")
        $stderr = Join-Path $runRoot ("child-" + $state.id + ".stderr.log")
        $arguments = @(
            "run", $state.child_origin.task,
            "--session", $state.id,
            "--provider", "deterministic-mock",
            "--model", "mock-model",
            "--option", 'mock_scenario="planner_worker_child"',
            "--cancellation-id", ([guid]::NewGuid().ToString()),
            "--json"
        )
        if (-not [string]::IsNullOrWhiteSpace($GateId)) {
            $arguments = $arguments[0..7] + @(
                "--option", ('mock_gate_id="' + $GateId + '"'),
                "--option", 'mock_gate_timeout_ms="120000"'
            ) + $arguments[8..($arguments.Count - 1)]
        }
        $info = [System.Diagnostics.ProcessStartInfo]::new()
        $info.FileName = $cli
        $info.WorkingDirectory = $runRoot
        $info.UseShellExecute = $false
        $info.CreateNoWindow = $true
        $info.RedirectStandardOutput = $true
        $info.RedirectStandardError = $true
        foreach ($argument in $arguments) { $info.ArgumentList.Add($argument) }
        $process = [System.Diagnostics.Process]::new()
        $process.StartInfo = $info
        if (-not $process.Start()) { throw "child CLI did not start" }
        [pscustomobject]@{
            Process = $process
            Stdout = $stdout
            Stderr = $stderr
            Child = $Child
        }
    }

    function Wait-ChildRun($Run) {
        $stdout = $Run.Process.StandardOutput.ReadToEndAsync()
        $stderr = $Run.Process.StandardError.ReadToEndAsync()
        if (-not $Run.Process.WaitForExit(120000)) {
            $Run.Process.Kill($true)
            throw "child CLI timed out"
        }
        $outText = $stdout.GetAwaiter().GetResult()
        $errText = $stderr.GetAwaiter().GetResult()
        Set-Content -LiteralPath $Run.Stdout -Value $outText -NoNewline
        Set-Content -LiteralPath $Run.Stderr -Value $errText -NoNewline
        if ($Run.Process.ExitCode -ne 0) {
            throw "child CLI failed ($($Run.Process.ExitCode))`n$outText`n$errText"
        }
        $Run.Process.Dispose()
        $Run.Process = $null
    }

    function Assert-ChildEvidence($Child) {
        $state = $Child.state
        $completed = Invoke-CliJson @("session", "inspect", $state.id, "--json")
        if ($completed.state.lifecycle -ne "completed") {
            throw "planner-worker v1.4 child did not complete"
        }
        $events = Read-Journal $state.id
        $expectedCalls = @{
            "planner-worker-edit" = "filesystem.edit"
            "planner-worker-test" = "process.run"
            "planner-worker-diff" = "git.diff"
        }
        foreach ($callId in $expectedCalls.Keys) {
            $modelProposals = @($events | Where-Object {
                $_.metadata.event_type -eq "model.tool_call_proposed" -and
                $_.payload.payload.call_id -eq $callId -and
                $_.payload.payload.tool -eq $expectedCalls[$callId]
            })
            if ($modelProposals.Count -ne 1) {
                throw "child model tool $callId was missing or proposed more than once"
            }
            $modelProposal = $modelProposals[0]
            $nextModelSequence = @($events | Where-Object {
                $_.metadata.event_type -eq "model.tool_call_proposed" -and
                $_.metadata.sequence -gt $modelProposal.metadata.sequence
            } | Sort-Object { $_.metadata.sequence } | Select-Object -First 1)
            $expectedArguments = $modelProposal.payload.payload.arguments |
                ConvertTo-Json -Compress -Depth 20
            $proposals = @($events | Where-Object {
                $_.metadata.event_type -eq "tool.call_proposed" -and
                $_.metadata.sequence -gt $modelProposal.metadata.sequence -and
                ($nextModelSequence.Count -eq 0 -or
                    $_.metadata.sequence -lt
                        $nextModelSequence[0].metadata.sequence) -and
                $_.payload.payload.tool -eq $expectedCalls[$callId] -and
                (($_.payload.payload.arguments |
                    ConvertTo-Json -Compress -Depth 20) -eq $expectedArguments)
            })
            if ($proposals.Count -ne 1) {
                throw "child tool $callId did not map to one canonical proposal"
            }
            $canonicalCallId = [string]$proposals[0].payload.payload.call_id
            if (-not $canonicalCallId.StartsWith("generic-provider-tool:")) {
                throw "child tool $callId did not receive a canonical call ID"
            }
            $dispatches = @($events | Where-Object {
                $_.metadata.event_type -eq "tool.execution_dispatched" -and
                $_.payload.payload.call_id -eq $canonicalCallId
            })
            $terminals = @($events | Where-Object {
                $_.metadata.event_type -eq "tool.execution_completed" -and
                $_.payload.payload.call_id -eq $canonicalCallId
            })
            if ($proposals.Count -ne 1 -or $dispatches.Count -ne 1 -or
                $terminals.Count -ne 1) {
                throw "child tool $callId was missing a receipt or was redispatched"
            }
        }
        $artifacts = @($completed.state.artifact_persistences.PSObject.Properties |
            ForEach-Object Value)
        if ($artifacts.Count -lt 3) {
            throw "child terminal state omitted owned evidence artifacts"
        }
        $contents = @()
        foreach ($artifact in $artifacts) {
            $hash = [string]$artifact.identity.content_hash
            $contentPath = Join-Path $runRoot (
                "sessions\" + $state.id + "\artifacts\style\objects\" +
                $hash.Substring(0, 2) + "\" + $hash + "\content"
            )
            if (-not (Test-Path -LiteralPath $contentPath)) {
                throw "child artifact content is missing"
            }
            $contents += [System.IO.File]::ReadAllText($contentPath)
        }
        $evidence = $contents -join "`n"
        if (-not $evidence.Contains("worker.txt") -or
            -not $evidence.Contains("parent-owned") -or
            -not $evidence.Contains("child-owned") -or
            -not ($evidence.Contains('"success":true') -or
                $evidence.Contains('"success": true'))) {
            throw "child artifacts omitted real bounded diff/test evidence"
        }
        $completed
    }

    function Complete-Child($Child) {
        $run = Start-ChildRun $Child ""
        Wait-ChildRun $run
        Assert-ChildEvidence $Child
    }

    try {
        $daemon = Start-TestRuntime
        $created = Invoke-CliJson @(
            "session", "create", "--workspace", $workspace,
            "--style", "planner-worker@1.4.0", "--json"
        )
        $parentId = $created.session_id
        $turnId = [guid]::NewGuid().ToString()
        $createdInspection = Invoke-CliJson @(
            "session", "inspect", $parentId, "--json"
        )
        if ($createdInspection.state.style_binding.version -ne "1.4.0") {
            throw "planner-worker v1.4 binding was not selected"
        }
        $plan = $createdInspection.state.style_binding.execution_plan
        foreach ($nodeId in @(
            "spawn-planner", "spawn-evidence", "integrate", "review"
        )) {
            $resolution = @($plan.nodes | Where-Object node_id -eq $nodeId)
            if ($resolution.Count -ne 1 -or $resolution[0].executor_version -ne "1.1.0") {
                throw "planner-worker v1.4 did not select the evidence-aware $nodeId executor"
            }
        }
        foreach ($nodeId in @("plan", "persist-integration")) {
            $resolution = @($plan.nodes | Where-Object node_id -eq $nodeId)
            if ($resolution.Count -ne 1 -or $resolution[0].executor_version -ne "1.0.0") {
                throw "planner-worker v1.4 changed historical $nodeId executor ABI"
            }
        }

        $waiting = Invoke-ParentRun $parentId $turnId
        if ([string]::IsNullOrWhiteSpace($waiting.awaiting_continuation)) {
            throw "planner-worker v1.4 did not request child approval"
        }
        $waiting = Resolve-Approvals $waiting $parentId 1
        Stop-TestRuntime $daemon
        $daemon = Start-TestRuntime
        $waiting = Resolve-Approvals $waiting $parentId 1

        $children = Get-Children $parentId
        if ($children.Count -ne 2) {
            throw "planner-worker v1.4 did not create two children"
        }
        $parentWithChildren = Invoke-CliJson @(
            "session", "inspect", $parentId, "--json"
        )
        foreach ($child in $children) {
            $record = @(
                $parentWithChildren.state.child_agents.PSObject.Properties |
                    ForEach-Object Value |
                    Where-Object { $_.child_session_id -eq $child.state.id }
            )
            if ($record.Count -ne 1) {
                throw "parent omitted the exact child ownership record"
            }
            $lease = $record[0].workspace_lease
            if ($lease.mode.mode -ne "branch_workspace" -or
                $lease.mode.merge_policy -ne "manual_review" -or
                $lease.ownership -ne "runtime_owned_branch" -or
                [string]::IsNullOrWhiteSpace($lease.branch_name) -or
                $lease.effective_root -eq $lease.source_root) {
                throw "planner-worker v1.4 branch workspace lease is incomplete"
            }
        }

        $gateIds = @(
            "planner-v1-4-first-worker-0",
            "planner-v1-4-first-worker-1"
        )
        $gates = @($gateIds | ForEach-Object {
            Join-Path $env:AGENTMOD_HARNESS_TEST_GATE_ROOT $_
        })
        $childRuns = @(
            (Start-ChildRun $children[0] $gateIds[0]),
            (Start-ChildRun $children[1] $gateIds[1])
        )
        $started = 0
        for ($attempt = 0; $attempt -lt 300; $attempt++) {
            $started = @($gates | Where-Object {
                @(Get-ChildItem -LiteralPath $_ -Filter "started-*" `
                    -ErrorAction SilentlyContinue).Count -ge 1
            }).Count
            if ($started -ge 2) { break }
            if ($childRuns[0].Process.HasExited -or
                $childRuns[1].Process.HasExited) { break }
            Start-Sleep -Milliseconds 100
        }
        if ($started -lt 2 -or $childRuns[0].Process.HasExited -or
            $childRuns[1].Process.HasExited) {
            throw "first planner workers did not overlap at the harness gate"
        }
        # Children are canonically task-sorted. Complete task 1 before task 0 to
        # prove integration does not inherit external completion order.
        New-Item -ItemType File -Path (Join-Path $gates[1] "release") -Force |
            Out-Null
        Wait-ChildRun $childRuns[1]
        Assert-ChildEvidence $children[1] | Out-Null
        if ($childRuns[0].Process.HasExited) {
            throw "task-order-first child completed before its gate was released"
        }
        New-Item -ItemType File -Path (Join-Path $gates[0] "release") -Force |
            Out-Null
        Wait-ChildRun $childRuns[0]
        Assert-ChildEvidence $children[0] | Out-Null
        Stop-TestRuntime $daemon
        $daemon = Start-TestRuntime
        $waiting = Invoke-ParentRun $parentId $turnId
        for ($attempt = 0; $attempt -lt 8; $attempt++) {
            $approvalInspection = Invoke-CliJson @(
                "session", "inspect", $parentId, "--json"
            )
            $pendingRevisionApprovals = @(
                $approvalInspection.state.approvals.PSObject.Properties |
                    Where-Object { $_.Value.state -eq "pending" }
            )
            if ($pendingRevisionApprovals.Count -ge 2) { break }
            if ($approvalInspection.state.lifecycle -eq "completed") {
                throw "reviewer completed without requesting revision children"
            }
            $waiting = Invoke-ParentRun $parentId $turnId
        }
        $waiting = Resolve-Approvals $waiting $parentId 2
        $revisionChildren = @(Get-Children $parentId | Where-Object {
            $_.state.child_origin.revision -eq 1
        })
        if ($revisionChildren.Count -ne 2) {
            throw "structured reviewer rejection did not create two revisions"
        }
        foreach ($child in $revisionChildren) {
            Complete-Child $child | Out-Null
        }

        for ($attempt = 0; $attempt -lt 6; $attempt++) {
            $result = Invoke-ParentRun $parentId $turnId
            $inspection = Invoke-CliJson @(
                "session", "inspect", $parentId, "--json"
            )
            if ($inspection.state.lifecycle -eq "completed") { break }
            if ([string]::IsNullOrWhiteSpace($result.awaiting_continuation)) {
                throw "planner-worker v1.4 recovery lost its continuation"
            }
        }
        if ($inspection.state.lifecycle -ne "completed") {
            throw "planner-worker v1.4 did not complete"
        }
        $integrationContents = @()
        foreach ($artifact in @(
            $inspection.state.artifact_persistences.PSObject.Properties |
                ForEach-Object Value
        )) {
            $hash = [string]$artifact.identity.content_hash
            $contentPath = Join-Path $runRoot (
                "sessions\" + $parentId + "\artifacts\style\objects\" +
                $hash.Substring(0, 2) + "\" + $hash + "\content"
            )
            if (Test-Path -LiteralPath $contentPath) {
                $integrationContents += [System.IO.File]::ReadAllText($contentPath)
            }
        }
        $integrationEvidence = $integrationContents -join "`n"
        $evidenceAt = $integrationEvidence.IndexOf('"member_order":["evidence"')
        $plannerAt = $integrationEvidence.IndexOf('"planner"', $evidenceAt + 1)
        if ($evidenceAt -lt 0 -or $plannerAt -le $evidenceAt) {
            throw "integration did not preserve canonical evidence-before-planner member order"
        }
        if ([System.IO.File]::ReadAllText(
                (Join-Path $workspace "worker.txt")) -ne "parent-owned") {
            throw "branch workspace was implicitly merged into the parent"
        }
        $events = Read-Journal $parentId
        foreach ($expected in @(
            @("graph.generic_join_ready", 2),
            @("style.generic_review_routed", 2),
            @("artifact.persistence_completed", 2)
        )) {
            $count = @($events | Where-Object {
                $_.metadata.event_type -eq $expected[0]
            }).Count
            if ($count -ne $expected[1]) {
                throw "expected $($expected[1]) $($expected[0]), found $count"
            }
        }
        $reviews = @($events | Where-Object {
            $_.metadata.event_type -eq "style.generic_review_routed"
        })
        if ($reviews[0].payload.payload.evidence.disposition -ne "revision" -or
            @($reviews[0].payload.payload.evidence.structured_findings).Count -ne 1 -or
            $reviews[1].payload.payload.evidence.disposition -ne "approved") {
            throw "structured reviewer revision evidence is incomplete"
        }
        $reviewEvidenceOutputs = @($events | Where-Object {
            $_.metadata.event_type -eq "model.output_delta_observed" -and
            ([string]$_.payload.payload.text).Contains(
                "planner.evidence_revision"
            ) -and
            ([string]$_.payload.payload.text).Contains("artifact:blake3:")
        })
        if ($reviewEvidenceOutputs.Count -ne 1) {
            throw "integration/reviewer did not consume exact artifact evidence"
        }
        Write-Output (
            "planner-worker v1.4 branch workspace/child tool/artifact/" +
            "join/reviewer/restart Windows E2E passed"
        )
        $succeeded = $true
    }
    finally {
        foreach ($run in $childRuns) {
            if ($null -ne $run.Process) {
                if (-not $run.Process.HasExited) {
                    Stop-Process -Id $run.Process.Id -Force -ErrorAction SilentlyContinue
                    $run.Process.WaitForExit()
                }
                $run.Process.Dispose()
                $run.Process = $null
            }
        }
        Stop-TestRuntime $daemon
        if (-not $succeeded) {
            if (Test-Path -LiteralPath $runtimeErr) {
                Get-Content -LiteralPath $runtimeErr -ErrorAction SilentlyContinue
            }
            Get-ChildItem -LiteralPath (Join-Path $runRoot "sessions") `
                -Filter "events.jsonl" -Recurse -ErrorAction SilentlyContinue |
                ForEach-Object {
                    Write-Output ("journal: " + $_.FullName)
                    Get-Content -LiteralPath $_.FullName -Tail 30
                }
        }
        foreach ($name in @(
            "AGENTMOD_RUNTIME_ENDPOINT", "AGENTMOD_RUNTIME_AUTH_TOKEN",
            "AGENTMOD_HARNESS_PROGRAM", "AGENTMOD_SCHEDULER_PROGRAM",
            "AGENTMOD_SCHEDULER_ROOT", "AGENTMOD_FILESYSTEM_HOST_PROGRAM",
            "AGENTMOD_PROCESS_HOST_PROGRAM", "AGENTMOD_GIT_HOST_PROGRAM",
            "AGENTMOD_PROCESS_ALLOWED_EXECUTABLES",
            "AGENTMOD_HARNESS_TEST_GATE_ROOT", "AGENTMOD_PERMISSION_MODE"
        )) {
            Remove-Item -LiteralPath ("Env:" + $name) -ErrorAction SilentlyContinue
        }
        if (Test-Path -LiteralPath $runRoot) {
            $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
            $resolvedRun = (Resolve-Path $runRoot).Path
            if ($resolvedRun.StartsWith($resolvedTemp) -and
                (Split-Path $resolvedRun -Leaf).StartsWith(
                    "am14-"
                )) {
                for ($attempt = 0; $attempt -lt 20; $attempt++) {
                    try {
                        Remove-Item -LiteralPath $resolvedRun -Recurse -Force
                        break
                    }
                    catch {
                        if ($attempt -eq 19) {
                            Write-Warning "temporary planner workspace remains locked"
                        }
                        else { Start-Sleep -Milliseconds 100 }
                    }
                }
            }
        }
    }
}
finally {
    Pop-Location
}
