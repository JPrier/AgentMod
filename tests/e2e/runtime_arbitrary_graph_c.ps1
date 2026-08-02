$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build --locked -p agentmod-runtime -p agentmod-harness `
        -p agentmod-cli -p agentmod-plugin-host `
        -p agentmod-plugin-fixture-worker -p agentmod-filesystem-host
    if ($LASTEXITCODE -ne 0) { throw "Graph C process build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $pluginHost = (Resolve-Path "target\debug\agentmod-plugin-host.exe").Path
    $filesystemHost = (
        Resolve-Path "target\debug\agentmod-filesystem-host.exe"
    ).Path
    $sourceWorker = (
        Resolve-Path "target\debug\agentmod-plugin-fixture-worker.exe"
    ).Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-graph-c-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    $userStyles = Join-Path $runRoot "styles\user"
    $fixtureBin = Join-Path $runRoot "fixture-bin"
    New-Item -ItemType Directory -Path $workspace, $userStyles, $fixtureBin `
        -Force | Out-Null
    [System.IO.File]::WriteAllText(
        (Join-Path $workspace "README.md"),
        "Graph C plugin branch action evidence.`n"
    )
    $pluginWorker = Join-Path $fixtureBin (
        Split-Path $sourceWorker -Leaf
    )
    Copy-Item -LiteralPath $sourceWorker -Destination $pluginWorker

    $styleTemplate = [System.IO.File]::ReadAllText(
        (Join-Path $repository "tests\fixtures\styles\arbitrary-graph-c.toml")
    )
    [System.IO.File]::WriteAllText(
        (Join-Path $userStyles "arbitrary-graph-c.toml"),
        $styleTemplate
    )
    function Write-GraphVariant {
        param(
            [string]$Id,
            [string]$Capability,
            [string]$Executor,
            [string]$Terminal = "renamed_done"
        )
        $content = $styleTemplate.
            Replace('id = "user-graph-c"', ('id = "' + $Id + '"')).
            Replace("plugin.graph", $Capability).
            Replace("fixture.graph", $Executor)
        if ($Terminal -ne "renamed_done") {
            $content = $content.Replace("renamed_done", $Terminal)
        }
        [System.IO.File]::WriteAllText(
            (Join-Path $userStyles ($Id + ".toml")),
            $content
        )
    }
    Write-GraphVariant "user-graph-c-invalid-transition" `
        "plugin.graph_invalid" "fixture.graph.invalid_transition" `
        "different_done"
    Write-GraphVariant "user-graph-c-invalid-output" `
        "plugin.invalid" "fixture.invalid"
    Write-GraphVariant "user-graph-c-timeout" `
        "plugin.timeout" "fixture.timeout"
    Write-GraphVariant "user-graph-c-cancel" `
        "plugin.cancel" "fixture.cancel"
    Write-GraphVariant "user-graph-c-unavailable" `
        "plugin.unavailable" "fixture.unavailable"

    $manifestTemplate = [System.IO.File]::ReadAllText(
        (Join-Path $repository (
            "tests\fixtures\plugins\arbitrary-graph-c-node.toml"
        ))
    )
    $manifest = Join-Path $runRoot "arbitrary-graph-c-node.toml"
    [System.IO.File]::WriteAllText(
        $manifest,
        $manifestTemplate.Replace(
            "__PLUGIN_WORKER__",
            $pluginWorker.Replace("\", "/")
        )
    )

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-graph-c-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_PLUGIN_HOST_PROGRAM = $pluginHost
    $env:AGENTMOD_FILESYSTEM_HOST_PROGRAM = $filesystemHost
    $env:AGENTMOD_PLUGIN_MANIFESTS = $manifest
    $env:AGENTMOD_PLUGIN_EXECUTABLE_ROOTS = $fixtureBin
    $env:AGENTMOD_PERMISSION_MODE = "allow"
    $runtimeOut = Join-Path $runRoot "runtime.stdout.log"
    $runtimeErr = Join-Path $runRoot "runtime.stderr.log"
    $daemon = $null
    $succeeded = $false

    function Stop-TestRuntime {
        if ($null -ne $script:daemon -and
            -not $script:daemon.HasExited) {
            Stop-Process -Id $script:daemon.Id -Force
            $script:daemon.WaitForExit()
        }
        $script:daemon = $null
    }
    function Start-TestRuntime {
        $script:daemon = Start-Process -FilePath $runtime `
            -ArgumentList "serve" -WorkingDirectory $runRoot `
            -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput $runtimeOut `
            -RedirectStandardError $runtimeErr
        for ($attempt = 0; $attempt -lt 120; $attempt++) {
            try {
                & $cli doctor --json 2>$null | Out-Null
                if ($LASTEXITCODE -eq 0) { return }
            } catch {}
            if ($script:daemon.HasExited) { break }
            Start-Sleep -Milliseconds 100
        }
        if (Test-Path -LiteralPath $runtimeErr) {
            Get-Content -LiteralPath $runtimeErr
        }
        throw "Graph C runtime did not become ready"
    }
    function Read-Journal {
        param([string]$SessionId)
        $path = Join-Path $runRoot (
            "sessions\" + $SessionId + "\events.jsonl"
        )
        if (-not (Test-Path -LiteralPath $path)) { return @() }
        $content = $null
        $share = [System.IO.FileShare]::ReadWrite -bor `
            [System.IO.FileShare]::Delete
        for ($attempt = 0; $attempt -lt 20; $attempt++) {
            $stream = $null
            $reader = $null
            try {
                $stream = [System.IO.File]::Open(
                    $path,
                    [System.IO.FileMode]::Open,
                    [System.IO.FileAccess]::Read,
                    $share
                )
                $reader = [System.IO.StreamReader]::new($stream)
                $content = $reader.ReadToEnd()
                break
            }
            catch [System.IO.IOException] {
                Start-Sleep -Milliseconds 10
            }
            finally {
                if ($null -ne $reader) { $reader.Dispose() }
                elseif ($null -ne $stream) { $stream.Dispose() }
            }
        }
        if ([string]::IsNullOrWhiteSpace($content)) { return @() }
        return @($content.TrimEnd().Split("`n") | ForEach-Object {
            ($_.TrimEnd("`r") | ConvertFrom-Json).event
        })
    }
    function Event-Count {
        param([object[]]$Events, [string]$Type)
        return @($Events | Where-Object {
            $_.metadata.event_type -eq $Type
        }).Count
    }
    function Invocation-Count {
        if (-not (Test-Path -LiteralPath $runRoot)) { return 0 }
        $markers = @(
            Get-ChildItem -LiteralPath $runRoot -Recurse `
                -Filter "fixture-node-invocations.log" `
                -ErrorAction SilentlyContinue
        )
        return @(
            $markers | ForEach-Object { Get-Content -LiteralPath $_.FullName }
        ).Count
    }
    function Invoke-CliAllowFailure {
        param([string[]]$Arguments)
        $previousPreference = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            $output = @(& $cli @Arguments 2>&1)
            return [pscustomobject]@{
                ExitCode = $LASTEXITCODE
                Output = ($output -join [Environment]::NewLine)
            }
        }
        finally {
            $ErrorActionPreference = $previousPreference
        }
    }
    function New-GraphSession {
        param([string]$Style)
        $session = & $cli session create --workspace $workspace `
            --style ($Style + "@1.0.0") --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) {
            throw "could not create Graph C session for $Style"
        }
        return $session
    }
    function Assert-PluginPlan {
        param(
            [object]$Inspection,
            [string]$Executor,
            [string]$Capability
        )
        $binding = $Inspection.state.style_binding
        if ($binding.execution_plan.compilation.compiler -ne
                "agentmod-runtime-node-plan@3" -or
            [string]::IsNullOrWhiteSpace($binding.execution_plan_hash) -or
            [string]::IsNullOrWhiteSpace(
                $binding.execution_plan.registry_hash
            )) {
            throw "Graph C immutable execution-plan identity is incomplete"
        }
        $resolution = @(
            $binding.execution_plan.nodes |
                Where-Object node_id -eq "renamed_plugin"
        )
        if ($resolution.Count -ne 1 -or
            $resolution[0].executor_id -ne $Executor -or
            $resolution[0].executor_version -ne "1.0.0" -or
            $resolution[0].source.kind -ne "plugin" -or
            $resolution[0].source.plugin_id -ne "fixture.node" -or
            $resolution[0].boundary -ne "plugin_host" -or
            @($resolution[0].required_capabilities) -notcontains
                $Capability -or
            [string]::IsNullOrWhiteSpace(
                $resolution[0].executor_declaration_hash
            )) {
            throw "Graph C did not retain exact plugin executor $Executor"
        }
        $runtimeResolution = @(
            $binding.execution_plan.nodes |
                Where-Object node_id -eq "runtime_context"
        )
        if ($runtimeResolution.Count -ne 1 -or
            $runtimeResolution[0].executor_id -ne
                "runtime.context-construction" -or
            $runtimeResolution[0].source.kind -ne "runtime") {
            throw "Graph C did not mix runtime and plugin executors"
        }
    }
    function Assert-NoRedispatch {
        param(
            [string]$SessionId,
            [int]$BeforeInvocations,
            [int]$BeforeEvents
        )
        $null = Invoke-CliAllowFailure @(
            "run", "recover Graph C without redispatch",
            "--session", $SessionId,
            "--provider", "deterministic-mock",
            "--model", "mock-model", "--json"
        )
        if ((Invocation-Count) -ne $BeforeInvocations -or
            (Read-Journal $SessionId).Count -ne $BeforeEvents) {
            throw "Graph C recovery automatically redispatched an effect"
        }
    }

    try {
        Start-TestRuntime
        foreach ($styleId in @(
            "user-graph-c",
            "user-graph-c-invalid-transition",
            "user-graph-c-invalid-output",
            "user-graph-c-timeout",
            "user-graph-c-cancel",
            "user-graph-c-unavailable"
        )) {
            $style = & $cli style inspect ($styleId + "@1.0.0") --json |
                ConvertFrom-Json
            if ($style.summary.source -ne "user" -or
                $style.summary.availability -ne "available") {
                throw (
                    "$styleId was not admitted as an arbitrary user style: " +
                    ($style | ConvertTo-Json -Depth 20 -Compress)
                )
            }
        }

        $success = New-GraphSession "user-graph-c"
        $successCreated = & $cli session inspect $success.session_id --json |
            ConvertFrom-Json
        Assert-PluginPlan $successCreated "fixture.graph" "plugin.graph"
        $successRun = Invoke-CliAllowFailure @(
            "run", "execute arbitrary user Graph C",
            "--session", $success.session_id,
            "--provider", "deterministic-mock",
            "--model", "mock-model", "--json"
        )
        if ($successRun.ExitCode -ne 0) {
            throw "Graph C success execution failed: $($successRun.Output)"
        }
        $successInspection = & $cli session inspect `
            $success.session_id --json | ConvertFrom-Json
        if ($successInspection.state.lifecycle -ne "active") {
            throw "complete_turn Graph C did not retain its active session"
        }
        $successExecution = $successInspection.state.style_execution
        $successInvocations = @(
            $successExecution.plugin_node_invocations.PSObject.Properties |
                ForEach-Object { $_.Value }
        )
        if ($successInvocations.Count -ne 1 -or
            $successInvocations[0].state -ne "completed" -or
            $null -eq $successInvocations[0].outcome_application) {
            throw "Graph C plugin outcome was not runtime-validated"
        }
        $successEvents = Read-Journal $success.session_id
        foreach ($expectation in @(
            @("style.execution_initialized", 1),
            @("plugin.set_activated", 1),
            @("plugin.node_invocation_proposed", 1),
            @("plugin.node_invocation_authorized", 1),
            @("plugin.node_invocation_dispatched", 1),
            @("plugin.node_invocation_completed", 1),
            @("plugin.node_outcome_validated", 1),
            @("plugin.node_budget_charged", 1),
            @("plugin.node_action_proposed", 1),
            @("plugin.node_action_applied", 1),
            @("tool.call_proposed", 1),
            @("tool.call_approved", 1),
            @("tool.execution_dispatched", 1),
            @("tool.execution_started", 1),
            @("tool.execution_completed", 1),
            @("artifact.persistence_completed", 1)
        )) {
            $actual = Event-Count $successEvents $expectation[0]
            if ($actual -ne $expectation[1]) {
                throw "expected $($expectation[1]) $($expectation[0]), found $actual"
            }
        }
        $artifactCompleted = @($successEvents | Where-Object {
            $_.metadata.event_type -eq "artifact.persistence_completed"
        })
        $artifactReference = $artifactCompleted[0].payload.payload.
            artifact_reference
        $pluginBranchOutcomes = @($successEvents | Where-Object {
            $_.metadata.event_type -eq `
                "graph.parallel_branch_effect_outcome_recorded" -and
            $_.payload.payload.identity.work.node_id -eq "renamed_plugin"
        })
        $joinReady = @($successEvents | Where-Object {
            $_.metadata.event_type -eq "graph.generic_join_ready"
        })
        $joinedArtifacts = @($joinReady[0].payload.payload.decision.results |
            ForEach-Object { @($_.artifact_references) })
        $toolProposal = @($successEvents | Where-Object {
            $_.metadata.event_type -eq "tool.call_proposed"
        })
        if ([string]::IsNullOrWhiteSpace($artifactReference) -or
            $pluginBranchOutcomes.Count -ne 1 -or
            @($pluginBranchOutcomes[0].payload.payload.outcome.output.
                artifact_references) -notcontains $artifactReference -or
            $joinReady.Count -ne 1 -or
            $joinedArtifacts -notcontains $artifactReference -or
            $toolProposal.Count -ne 1 -or
            $toolProposal[0].payload.payload.tool -ne "filesystem.read") {
            throw (
                "Graph C plugin branch did not carry the persisted artifact " +
                "through its validated outcome and generic join"
            )
        }
        $successMarkerCount = Invocation-Count
        if ($successMarkerCount -ne 1) {
            throw "Graph C success invoked the worker $successMarkerCount times"
        }
        $successJournalCount = $successEvents.Count
        $successPlanHash = $successCreated.state.style_binding.
            execution_plan_hash
        Stop-TestRuntime
        Start-TestRuntime
        $restarted = & $cli session inspect $success.session_id --json |
            ConvertFrom-Json
        Assert-PluginPlan $restarted "fixture.graph" "plugin.graph"
        if ($restarted.state.style_binding.execution_plan_hash -ne
            $successPlanHash) {
            throw "Graph C rebound its executor plan after daemon restart"
        }
        $replayed = & $cli session replay $success.session_id --json |
            ConvertFrom-Json
        Assert-PluginPlan $replayed "fixture.graph" "plugin.graph"
        if ((Read-Journal $success.session_id).Count -ne
                $successJournalCount -or
            (Invocation-Count) -ne $successMarkerCount) {
            throw "Graph C pure replay appended events or invoked the plugin"
        }

        $invalidTransition = New-GraphSession `
            "user-graph-c-invalid-transition"
        $invalidTransitionCreated = & $cli session inspect `
            $invalidTransition.session_id --json | ConvertFrom-Json
        Assert-PluginPlan $invalidTransitionCreated `
            "fixture.graph.invalid_transition" "plugin.graph_invalid"
        $beforeInvalidTransition = Invocation-Count
        $null = Invoke-CliAllowFailure @(
            "run", "reject invalid Graph C transition",
            "--session", $invalidTransition.session_id,
            "--provider", "deterministic-mock",
            "--model", "mock-model", "--json"
        )
        $invalidTransitionEvents = Read-Journal `
            $invalidTransition.session_id
        if ((Event-Count $invalidTransitionEvents `
                "plugin.node_invocation_completed") -ne 1 -or
            (Event-Count $invalidTransitionEvents `
                "plugin.node_outcome_rejected") -ne 1 -or
            (Invocation-Count) -ne ($beforeInvalidTransition + 1)) {
            throw "Graph C invalid transition was not rejected once"
        }
        Assert-NoRedispatch $invalidTransition.session_id `
            (Invocation-Count) $invalidTransitionEvents.Count

        $invalidOutput = New-GraphSession "user-graph-c-invalid-output"
        $invalidOutputCreated = & $cli session inspect `
            $invalidOutput.session_id --json | ConvertFrom-Json
        Assert-PluginPlan $invalidOutputCreated `
            "fixture.invalid" "plugin.invalid"
        $beforeInvalidOutput = Invocation-Count
        $null = Invoke-CliAllowFailure @(
            "run", "reject invalid Graph C output",
            "--session", $invalidOutput.session_id,
            "--provider", "deterministic-mock",
            "--model", "mock-model", "--json"
        )
        $invalidOutputEvents = Read-Journal $invalidOutput.session_id
        if ((Event-Count $invalidOutputEvents `
                "plugin.node_invocation_ambiguous") -ne 1 -or
            (Invocation-Count) -ne ($beforeInvalidOutput + 1)) {
            throw "Graph C malformed plugin output did not fail closed once"
        }
        Assert-NoRedispatch $invalidOutput.session_id `
            (Invocation-Count) $invalidOutputEvents.Count

        $timeout = New-GraphSession "user-graph-c-timeout"
        $timeoutCreated = & $cli session inspect $timeout.session_id --json |
            ConvertFrom-Json
        Assert-PluginPlan $timeoutCreated "fixture.timeout" "plugin.timeout"
        $beforeTimeout = Invocation-Count
        $null = Invoke-CliAllowFailure @(
            "run", "time out Graph C plugin effect",
            "--session", $timeout.session_id,
            "--provider", "deterministic-mock",
            "--model", "mock-model", "--json"
        )
        $timeoutEvents = Read-Journal $timeout.session_id
        if ((Event-Count $timeoutEvents `
                "plugin.node_invocation_ambiguous") -ne 1 -or
            (Invocation-Count) -ne ($beforeTimeout + 1)) {
            throw "Graph C timeout was not classified ambiguous once"
        }
        Stop-TestRuntime
        Start-TestRuntime
        Assert-NoRedispatch $timeout.session_id `
            (Invocation-Count) $timeoutEvents.Count

        $cancelled = New-GraphSession "user-graph-c-cancel"
        $cancelledCreated = & $cli session inspect `
            $cancelled.session_id --json | ConvertFrom-Json
        Assert-PluginPlan $cancelledCreated `
            "fixture.cancel" "plugin.cancel"
        $cancelId = [guid]::NewGuid().ToString()
        $cancelOut = Join-Path $runRoot "cancel-turn.stdout.log"
        $cancelErr = Join-Path $runRoot "cancel-turn.stderr.log"
        $cancelRun = Start-Process -FilePath $cli -ArgumentList @(
            "run", "cancel-Graph-C-plugin",
            "--session", $cancelled.session_id,
            "--provider", "deterministic-mock",
            "--model", "mock-model",
            "--cancellation-id", $cancelId,
            "--json"
        ) -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput $cancelOut `
            -RedirectStandardError $cancelErr
        $dispatched = $false
        for ($attempt = 0; $attempt -lt 100; $attempt++) {
            if ((Event-Count (Read-Journal $cancelled.session_id) `
                    "plugin.node_invocation_dispatched") -eq 1) {
                $dispatched = $true
                break
            }
            if ($cancelRun.HasExited) { break }
            Start-Sleep -Milliseconds 50
        }
        if (-not $dispatched) {
            if (-not $cancelRun.HasExited) {
                Stop-Process -Id $cancelRun.Id -Force
            }
            throw "Graph C cancellation did not reach plugin dispatch"
        }
        $cancelResult = Invoke-CliAllowFailure @(
            "cancel", $cancelId,
            "--reason", "cancel plugin parallel branch",
            "--json"
        )
        if ($cancelResult.ExitCode -ne 0) {
            Stop-Process -Id $cancelRun.Id -Force
            throw "Graph C parallel cancellation was not accepted"
        }
        if (-not $cancelRun.WaitForExit(15000)) {
            Stop-Process -Id $cancelRun.Id -Force
            throw "Graph C cancelled plugin invocation did not terminate"
        }
        $cancelledEvents = Read-Journal $cancelled.session_id
        $pluginCancellationTerminals =
            (Event-Count $cancelledEvents "plugin.node_invocation_failed") +
            (Event-Count $cancelledEvents "plugin.node_invocation_ambiguous")
        if ((Event-Count $cancelledEvents `
                "graph.parallel_cancellation_requested") -ne 1 -or
            (Event-Count $cancelledEvents `
                "graph.parallel_cancellation_completed") -ne 1 -or
            $pluginCancellationTerminals -ne 1 -or
            (Event-Count $cancelledEvents `
                "plugin.node_invocation_completed") -ne 0) {
            throw (
                "Graph C exact parallel plugin cancellation was not " +
                "canonical"
            )
        }
        $cancelledInvocationCount = Invocation-Count
        $cancelledEventCount = $cancelledEvents.Count
        $duplicateCancel = Invoke-CliAllowFailure @(
            "cancel", $cancelId,
            "--reason", "duplicate terminal cancellation",
            "--json"
        )
        if ($duplicateCancel.ExitCode -eq 0 -or
            (Read-Journal $cancelled.session_id).Count -ne
                $cancelledEventCount) {
            throw "Graph C retained a live cancellation binding after terminal"
        }
        Stop-TestRuntime
        Start-TestRuntime
        Assert-NoRedispatch $cancelled.session_id `
            $cancelledInvocationCount $cancelledEventCount

        $unavailable = New-GraphSession "user-graph-c-unavailable"
        $unavailableCreated = & $cli session inspect `
            $unavailable.session_id --json | ConvertFrom-Json
        Assert-PluginPlan $unavailableCreated `
            "fixture.unavailable" "plugin.unavailable"
        Remove-Item -LiteralPath $pluginWorker -Force
        $beforeUnavailable = Invocation-Count
        $unavailableAttempt = Invoke-CliAllowFailure @(
            "run", "execute unavailable Graph C plugin",
            "--session", $unavailable.session_id,
            "--provider", "deterministic-mock",
            "--model", "mock-model", "--json"
        )
        $unavailableEvents = Read-Journal $unavailable.session_id
        if ($unavailableAttempt.ExitCode -eq 0 -or
            (Event-Count $unavailableEvents `
                "plugin.node_invocation_proposed") -ne 0 -or
            (Event-Count $unavailableEvents `
                "plugin.node_invocation_dispatched") -ne 0 -or
            (Invocation-Count) -ne $beforeUnavailable) {
            throw (
                "post-creation plugin unavailability crossed the " +
                "canonical dispatch boundary"
            )
        }
        Assert-NoRedispatch $unavailable.session_id `
            $beforeUnavailable $unavailableEvents.Count

        Write-Output (
            "runtime arbitrary Graph C user-style/plugin-plan/invocation/" +
            "validation/restart/timeout/ambiguity/unavailability/replay E2E passed"
        )
        $succeeded = $true
    }
    catch {
        if (Test-Path -LiteralPath $runtimeErr) {
            Get-Content -LiteralPath $runtimeErr
        }
        throw
    }
    finally {
        Stop-TestRuntime
        foreach ($name in @(
            "AGENTMOD_RUNTIME_ENDPOINT",
            "AGENTMOD_RUNTIME_AUTH_TOKEN",
            "AGENTMOD_HARNESS_PROGRAM",
            "AGENTMOD_PLUGIN_HOST_PROGRAM",
            "AGENTMOD_FILESYSTEM_HOST_PROGRAM",
            "AGENTMOD_PLUGIN_MANIFESTS",
            "AGENTMOD_PLUGIN_EXECUTABLE_ROOTS",
            "AGENTMOD_PERMISSION_MODE"
        )) {
            Remove-Item "Env:\$name" -ErrorAction SilentlyContinue
        }
        if ($succeeded -and (Test-Path -LiteralPath $runRoot)) {
            $resolvedTemp = (
                Resolve-Path ([System.IO.Path]::GetTempPath())
            ).Path
            $resolvedRun = (Resolve-Path $runRoot).Path
            if ($resolvedRun.StartsWith($resolvedTemp) -and
                (Split-Path $resolvedRun -Leaf).StartsWith(
                    "agentmod-graph-c-e2e-"
                )) {
                Remove-Item -LiteralPath $resolvedRun -Recurse -Force
            }
        } elseif (Test-Path -LiteralPath $runRoot) {
            Write-Output "retained failed Graph C E2E root: $runRoot"
        }
    }
}
finally {
    Pop-Location
}
$global:LASTEXITCODE = 0
