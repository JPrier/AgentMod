$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build --locked -p agentmod-runtime -p agentmod-scheduler `
        -p agentmod-harness `
        -p agentmod-cli -p agentmod-tui -p agentmod-plugin-host `
        -p agentmod-plugin-fixture-worker
    if ($LASTEXITCODE -ne 0) {
        throw "plugin lifecycle process build failed"
    }

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
    $tui = (Resolve-Path (Join-Path $debugRoot "agentmod-tui.exe")).Path
    $pluginHost = (
        Resolve-Path (Join-Path $debugRoot "agentmod-plugin-host.exe")
    ).Path
    $sourceWorker = (
        Resolve-Path (Join-Path $debugRoot "agentmod-plugin-fixture-worker.exe")
    ).Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-plugin-lifecycle-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    $userStyles = Join-Path $runRoot "styles\user"
    $fixtureBin = Join-Path $runRoot "fixture-bin"
    New-Item -ItemType Directory `
        -Path $workspace, $userStyles, $fixtureBin -Force | Out-Null
    $pluginWorker = Join-Path $fixtureBin (Split-Path $sourceWorker -Leaf)
    Copy-Item -LiteralPath $sourceWorker -Destination $pluginWorker

    $styleTemplate = [System.IO.File]::ReadAllText(
        (Join-Path $repository "tests\fixtures\styles\arbitrary-graph-c.toml")
    )
    $style = $styleTemplate.
        Replace('id = "user-graph-c"', 'id = "plugin-lifecycle"').
        Replace("plugin.graph", "plugin.timeout").
        Replace("fixture.graph", "fixture.timeout")
    [System.IO.File]::WriteAllText(
        (Join-Path $userStyles "plugin-lifecycle.toml"),
        $style
    )

    $manifestTemplate = [System.IO.File]::ReadAllText(
        (Join-Path $repository (
            "tests\fixtures\plugins\arbitrary-graph-c-node.toml"
        ))
    )
    $manifest = $manifestTemplate.Replace(
        "__PLUGIN_WORKER__",
        $pluginWorker.Replace("\", "/")
    )
    $manifest = [regex]::Replace(
        $manifest,
        '(?m)^timeout_ms = 1000\r?$',
        "timeout_ms = 5000"
    )
    $manifest = [regex]::Replace(
        $manifest,
        '(?m)^timeout_ms = 50\r?$',
        "timeout_ms = 5000"
    )
    $manifestPath = Join-Path $runRoot "plugin-lifecycle-node.toml"
    [System.IO.File]::WriteAllText($manifestPath, $manifest)

    $dispatchMarker = Join-Path $runRoot "lifecycle-dispatch.log"
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-plugin-lifecycle-e2e-" +
        [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_PLUGIN_HOST_PROGRAM = $pluginHost
    $env:AGENTMOD_PLUGIN_MANIFESTS = $manifestPath
    $env:AGENTMOD_PLUGIN_EXECUTABLE_ROOTS = $fixtureBin
    $env:AGENTMOD_PERMISSION_MODE = "allow"
    $env:RUST_MIN_STACK = "16777216"
    $env:AGENTMOD_PLUGIN_LIFECYCLE_PRE_DISPATCH_DELAY_MS = "1200"
    $env:AGENTMOD_PLUGIN_LIFECYCLE_PRE_DISPATCH_MARKER = $dispatchMarker
    $runtimeOut = Join-Path $runRoot "runtime.stdout.log"
    $runtimeErr = Join-Path $runRoot "runtime.stderr.log"
    $daemon = $null
    $succeeded = $false

    function Stop-TestRuntime {
        if ($null -ne $script:daemon -and -not $script:daemon.HasExited) {
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
        for ($attempt = 0; $attempt -lt 150; $attempt++) {
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
        throw "plugin lifecycle runtime did not become ready"
    }

    function Read-Journal {
        param([string]$SessionId)
        $journal = Join-Path $runRoot (
            "sessions\" + $SessionId + "\events.jsonl"
        )
        if (-not (Test-Path -LiteralPath $journal)) { return @() }
        for ($attempt = 0; $attempt -lt 50; $attempt++) {
            try {
                return @(
                    Get-Content -LiteralPath $journal -ErrorAction Stop |
                        ForEach-Object {
                            ($_ | ConvertFrom-Json -ErrorAction Stop).event
                        }
                )
            } catch {
                if ($attempt -eq 49) { throw }
                Start-Sleep -Milliseconds 10
            }
        }
    }

    function Event-Count {
        param([object[]]$Events, [string]$Type)
        return @($Events | Where-Object {
            $_.metadata.event_type -eq $Type
        }).Count
    }

    function Marker-LineCount {
        param([string]$Name)
        $markers = @(
            Get-ChildItem -LiteralPath $runRoot -Recurse -Filter $Name `
                -ErrorAction SilentlyContinue
        )
        return @(
            $markers | ForEach-Object {
                Get-Content -LiteralPath $_.FullName
            }
        ).Count
    }

    function Invoke-CliAllowFailure {
        param([string[]]$Arguments)
        $output = @(& $cli @Arguments 2>&1)
        return [pscustomobject]@{
            ExitCode = $LASTEXITCODE
            Output = ($output -join [Environment]::NewLine)
        }
    }

    function Assert-LifecycleAction {
        param(
            [ValidateSet("disable", "quarantine")]
            [string]$Action
        )
        $session = & $cli session create --workspace $workspace `
            --style plugin-lifecycle@1.0.0 --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) {
            throw "could not create lifecycle session for $Action"
        }
        $sessionId = $session.session_id
        $turnCancellation = [guid]::NewGuid().ToString()
        $turnOut = Join-Path $runRoot "$Action-turn.stdout.log"
        $turnErr = Join-Path $runRoot "$Action-turn.stderr.log"
        $beforeInvocations = Marker-LineCount "fixture-node-invocations.log"
        $turn = Start-Process -FilePath $cli -ArgumentList @(
            "run", "lifecycle-$Action",
            "--session", $sessionId,
            "--cancellation-id", $turnCancellation,
            "--json"
        ) -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput $turnOut -RedirectStandardError $turnErr
        for ($attempt = 0; $attempt -lt 150; $attempt++) {
            if ((Marker-LineCount "fixture-node-invocations.log") -gt
                    $beforeInvocations) {
                break
            }
            if ($turn.HasExited) {
                if (Test-Path -LiteralPath $turnOut) {
                    Get-Content -LiteralPath $turnOut
                }
                if (Test-Path -LiteralPath $turnErr) {
                    Get-Content -LiteralPath $turnErr
                }
                if (Test-Path -LiteralPath $runtimeErr) {
                    Get-Content -LiteralPath $runtimeErr
                }
                throw "plugin worker exited before lifecycle $Action"
            }
            Start-Sleep -Milliseconds 50
        }
        if ((Marker-LineCount "fixture-node-invocations.log") -ne
                ($beforeInvocations + 1)) {
            throw "plugin worker did not enter exactly once for $Action"
        }

        $lifecycleCancellation = [guid]::NewGuid().ToString()
        $lifecycleOut = Join-Path $runRoot "$Action-lifecycle.stdout.log"
        $lifecycleErr = Join-Path $runRoot "$Action-lifecycle.stderr.log"
        $arguments = @(
            "plugin", $Action, "fixture.node",
            "--session", $sessionId,
            "--cancellation-id", $lifecycleCancellation,
            "--json"
        )
        if ($Action -eq "quarantine") {
            $arguments = @(
                "plugin", "quarantine", "fixture.node",
                "--session", $sessionId,
                "--reason", "integrity_failure",
                "--cancellation-id", $lifecycleCancellation,
                "--json"
            )
        }
        $beforeDispatches = Marker-LineCount "lifecycle-dispatch.log"
        $lifecycle = Start-Process -FilePath $cli -ArgumentList $arguments `
            -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput $lifecycleOut `
            -RedirectStandardError $lifecycleErr
        $requested = $false
        for ($attempt = 0; $attempt -lt 150; $attempt++) {
            $events = Read-Journal $sessionId
            if ((Event-Count $events "plugin.lifecycle_change_requested") -eq 1) {
                $requested = $true
                break
            }
            if ($lifecycle.HasExited) { break }
            Start-Sleep -Milliseconds 25
        }
        if (-not $requested) {
            throw "canonical lifecycle request was not observed for $Action"
        }
        $eventsAtRequest = Read-Journal $sessionId
        if ((Event-Count $eventsAtRequest "plugin.lifecycle_changed") -ne 0 -or
            (Marker-LineCount "lifecycle-dispatch.log") -ne
                $beforeDispatches -or
            $lifecycle.HasExited) {
            throw (
                "lifecycle $Action reached host I/O before its requested " +
                "event was durably observable"
            )
        }

        if (-not $lifecycle.WaitForExit(20000)) {
            Stop-Process -Id $lifecycle.Id -Force
            throw "lifecycle $Action did not complete"
        }
        if ($lifecycle.ExitCode -ne 0) {
            Get-Content -LiteralPath $lifecycleErr
            throw "lifecycle $Action failed"
        }
        $result = Get-Content -LiteralPath $lifecycleOut -Raw |
            ConvertFrom-Json
        $expectedState = if ($Action -eq "disable") {
            "disabled"
        } else {
            "quarantined"
        }
        if ($result.state -ne $expectedState -or $result.replayed) {
            throw "fresh lifecycle $Action returned an invalid result"
        }
        if ((Marker-LineCount "lifecycle-dispatch.log") -ne
                ($beforeDispatches + 1)) {
            throw "lifecycle $Action did not dispatch exactly once"
        }

        if (-not $turn.WaitForExit(20000)) {
            Stop-Process -Id $turn.Id -Force
            throw "cancelled lifecycle worker did not terminate"
        }
        $events = Read-Journal $sessionId
        if (@($events | Where-Object {
                $_.metadata.event_type -eq
                    "plugin.node_invocation_completed"
            }).Count -ne 0 -or
            @($events | Where-Object {
                $_.metadata.event_type -eq
                    "plugin.node_invocation_ambiguous"
            }).Count -ne 1) {
            throw "lifecycle $Action did not cancel the active worker closed"
        }
        $requestedEvents = @($events | Where-Object {
            $_.metadata.event_type -eq "plugin.lifecycle_change_requested"
        })
        $changedEvents = @($events | Where-Object {
            $_.metadata.event_type -eq "plugin.lifecycle_changed"
        })
        if ($requestedEvents.Count -ne 1 -or $changedEvents.Count -ne 1 -or
            [uint64]$requestedEvents[0].metadata.sequence -ge
                [uint64]$changedEvents[0].metadata.sequence) {
            throw "lifecycle $Action canonical ordering is invalid"
        }

        $journalCount = $events.Count
        $retry = Invoke-CliAllowFailure $arguments
        if ($retry.ExitCode -ne 0) {
            throw "exact lifecycle $Action retry failed: $($retry.Output)"
        }
        $retryResult = $retry.Output | ConvertFrom-Json
        if (-not $retryResult.replayed -or
            $retryResult.state -ne $expectedState -or
            (Marker-LineCount "lifecycle-dispatch.log") -ne
                ($beforeDispatches + 1) -or
            (Read-Journal $sessionId).Count -ne $journalCount) {
            throw "exact lifecycle $Action retry was not receipt-only"
        }

        $beforeFuture = Marker-LineCount "fixture-node-invocations.log"
        $future = Invoke-CliAllowFailure @(
            "run", "future-$Action",
            "--session", $sessionId,
            "--cancellation-id", ([guid]::NewGuid().ToString()),
            "--json"
        )
        if ($future.ExitCode -eq 0 -or
            (Marker-LineCount "fixture-node-invocations.log") -ne
                $beforeFuture) {
            throw "future turn did not fail closed after lifecycle $Action"
        }

        $reverseAction = if ($Action -eq "disable") {
            "enable"
        } else {
            "unquarantine"
        }
        $reverseCancellation = [guid]::NewGuid().ToString()
        $reverseArguments = @(
            "plugin", $reverseAction, "fixture.node",
            "--session", $sessionId,
            "--cancellation-id", $reverseCancellation,
            "--json"
        )
        $beforeReverseDispatches = Marker-LineCount "lifecycle-dispatch.log"
        $reverse = Invoke-CliAllowFailure $reverseArguments
        if ($reverse.ExitCode -ne 0) {
            throw "plugin $reverseAction failed: $($reverse.Output)"
        }
        $reverseResult = $reverse.Output | ConvertFrom-Json
        if ($reverseResult.state -ne "active" -or $reverseResult.replayed -or
            (Marker-LineCount "lifecycle-dispatch.log") -ne
                ($beforeReverseDispatches + 1)) {
            throw "fresh plugin $reverseAction returned an invalid result"
        }
        $reverseJournalCount = (Read-Journal $sessionId).Count
        $reverseRetry = Invoke-CliAllowFailure $reverseArguments
        if ($reverseRetry.ExitCode -ne 0) {
            throw "exact plugin $reverseAction retry failed"
        }
        $reverseRetryResult = $reverseRetry.Output | ConvertFrom-Json
        if (-not $reverseRetryResult.replayed -or
            $reverseRetryResult.state -ne "active" -or
            (Marker-LineCount "lifecycle-dispatch.log") -ne
                ($beforeReverseDispatches + 1) -or
            (Read-Journal $sessionId).Count -ne $reverseJournalCount) {
            throw "exact plugin $reverseAction retry was not receipt-only"
        }

        if ($Action -eq "disable") {
            $watchOut = Join-Path $runRoot "tui-watch.stdout.log"
            $watchErr = Join-Path $runRoot "tui-watch.stderr.log"
            $watch = Start-Process -FilePath $tui -ArgumentList @(
                "--smoke-watch", "6000"
            ) -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru `
                -RedirectStandardOutput $watchOut `
                -RedirectStandardError $watchErr
            Start-Sleep -Milliseconds 400
            $beforeTuiDispatches = Marker-LineCount "lifecycle-dispatch.log"
            $tuiResult = @(
                & $tui --smoke-command "/plugin-disable fixture.node" 2>&1
            ) -join [Environment]::NewLine
            if ($LASTEXITCODE -ne 0 -or
                $tuiResult -notmatch "plugin fixture.node@" -or
                $tuiResult -notmatch "disabled") {
                throw "TUI plugin disable failed: $tuiResult"
            }
            if ((Marker-LineCount "lifecycle-dispatch.log") -ne
                    ($beforeTuiDispatches + 1)) {
                throw "TUI plugin disable did not dispatch exactly once"
            }
            $tuiEnable = Invoke-CliAllowFailure @(
                "plugin", "enable", "fixture.node",
                "--session", $sessionId,
                "--cancellation-id", ([guid]::NewGuid().ToString()),
                "--json"
            )
            if ($tuiEnable.ExitCode -ne 0 -or
                ($tuiEnable.Output | ConvertFrom-Json).state -ne "active") {
                throw "plugin recovery after TUI disable failed"
            }
            if (-not $watch.WaitForExit(15000) -or $watch.ExitCode -ne 0) {
                if (Test-Path -LiteralPath $watchErr) {
                    Get-Content -LiteralPath $watchErr
                }
                throw "TUI continuous event watch failed"
            }
            $watchResult = Get-Content -LiteralPath $watchOut -Raw
            $watchMatch = [regex]::Match($watchResult, "events_delta=(\d+)")
            if (-not $watchMatch.Success -or
                [int]$watchMatch.Groups[1].Value -lt 4) {
                throw "TUI watch missed canonical lifecycle events: $watchResult"
            }
        }
    }

    function Assert-StartupLifecycleRecovery {
        Stop-TestRuntime
        $env:AGENTMOD_PLUGIN_LIFECYCLE_POST_RECEIPT_DELAY_MS = "5000"
        $env:AGENTMOD_PLUGIN_LIFECYCLE_POST_RECEIPT_MARKER = (
            Join-Path $runRoot "lifecycle-post-receipt.log"
        )
        Start-TestRuntime
        $session = & $cli session create --workspace $workspace `
            --style plugin-lifecycle@1.0.0 --json | ConvertFrom-Json
        $sessionId = $session.session_id
        $beforeInvocations = Marker-LineCount "fixture-node-invocations.log"
        $turn = Start-Process -FilePath $cli -ArgumentList @(
            "run", "startup-lifecycle-recovery",
            "--session", $sessionId,
            "--cancellation-id", ([guid]::NewGuid().ToString()),
            "--json"
        ) -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru
        for ($attempt = 0; $attempt -lt 150; $attempt++) {
            if ((Marker-LineCount "fixture-node-invocations.log") -gt
                    $beforeInvocations) {
                break
            }
            Start-Sleep -Milliseconds 50
        }
        if ((Marker-LineCount "fixture-node-invocations.log") -ne
                ($beforeInvocations + 1)) {
            throw "startup recovery fixture did not enter"
        }
        $lifecycleCancellation = [guid]::NewGuid().ToString()
        $beforeDispatches = Marker-LineCount "lifecycle-dispatch.log"
        $beforeReceipts = Marker-LineCount "lifecycle-post-receipt.log"
        $lifecycle = Start-Process -FilePath $cli -ArgumentList @(
            "plugin", "disable", "fixture.node",
            "--session", $sessionId,
            "--cancellation-id", $lifecycleCancellation,
            "--json"
        ) -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru
        for ($attempt = 0; $attempt -lt 200; $attempt++) {
            if ((Marker-LineCount "lifecycle-post-receipt.log") -gt
                    $beforeReceipts) {
                break
            }
            Start-Sleep -Milliseconds 25
        }
        $cutEvents = Read-Journal $sessionId
        if ((Marker-LineCount "lifecycle-post-receipt.log") -ne
                ($beforeReceipts + 1) -or
            (Event-Count $cutEvents "plugin.lifecycle_change_requested") -ne 1 -or
            (Event-Count $cutEvents "plugin.lifecycle_changed") -ne 0) {
            throw "startup recovery cut did not stop after the exact host receipt"
        }
        Stop-TestRuntime
        foreach ($process in @($turn, $lifecycle)) {
            if (-not $process.HasExited) {
                Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
            }
        }
        Start-TestRuntime
        $events = Read-Journal $sessionId
        if ((Event-Count $events "plugin.lifecycle_change_requested") -ne 1 -or
            (Event-Count $events "plugin.lifecycle_changed") -ne 1 -or
            (Marker-LineCount "lifecycle-dispatch.log") -ne
                ($beforeDispatches + 2) -or
            (Marker-LineCount "fixture-node-invocations.log") -ne
                ($beforeInvocations + 1)) {
            throw "startup lifecycle recovery did not reconcile receipt-only"
        }
        $retry = Invoke-CliAllowFailure @(
            "plugin", "disable", "fixture.node",
            "--session", $sessionId,
            "--cancellation-id", $lifecycleCancellation,
            "--json"
        )
        if ($retry.ExitCode -ne 0 -or
            -not (($retry.Output | ConvertFrom-Json).replayed)) {
            throw "startup-reconciled lifecycle receipt did not replay"
        }
        Remove-Item Env:AGENTMOD_PLUGIN_LIFECYCLE_POST_RECEIPT_DELAY_MS `
            -ErrorAction SilentlyContinue
        Remove-Item Env:AGENTMOD_PLUGIN_LIFECYCLE_POST_RECEIPT_MARKER `
            -ErrorAction SilentlyContinue
    }

    try {
        Start-TestRuntime
        $style = & $cli style inspect plugin-lifecycle --json |
            ConvertFrom-Json
        if ($style.summary.availability -ne "available") {
            throw "plugin lifecycle style is unavailable"
        }
        Assert-LifecycleAction "disable"
        Assert-LifecycleAction "quarantine"
        Start-Sleep -Milliseconds 3500
        if ((Marker-LineCount "fixture-node-late-effects.log") -ne 0) {
            throw "a cancelled plugin worker produced a late external effect"
        }
        Assert-StartupLifecycleRecovery
        $succeeded = $true
        Write-Output (
            "runtime daemon/CLI plugin disable/enable and " +
            "quarantine/unquarantine plus TUI management lifecycle E2E " +
            "passed"
        )
    }
    finally {
        Stop-TestRuntime
        foreach ($name in @(
            "AGENTMOD_RUNTIME_ENDPOINT",
            "AGENTMOD_RUNTIME_AUTH_TOKEN",
            "AGENTMOD_HARNESS_PROGRAM",
            "AGENTMOD_PLUGIN_HOST_PROGRAM",
            "AGENTMOD_PLUGIN_MANIFESTS",
            "AGENTMOD_PLUGIN_EXECUTABLE_ROOTS",
            "AGENTMOD_PERMISSION_MODE",
            "RUST_MIN_STACK",
            "AGENTMOD_PLUGIN_LIFECYCLE_PRE_DISPATCH_DELAY_MS",
            "AGENTMOD_PLUGIN_LIFECYCLE_PRE_DISPATCH_MARKER",
            "AGENTMOD_PLUGIN_LIFECYCLE_POST_RECEIPT_DELAY_MS",
            "AGENTMOD_PLUGIN_LIFECYCLE_POST_RECEIPT_MARKER"
        )) {
            Remove-Item "Env:$name" -ErrorAction SilentlyContinue
        }
        if (-not $succeeded) {
            Write-Warning "preserving failed lifecycle E2E at $runRoot"
        } elseif (Test-Path -LiteralPath $runRoot) {
            $resolvedTemp = (
                Resolve-Path ([System.IO.Path]::GetTempPath())
            ).Path
            $resolvedRun = (Resolve-Path $runRoot).Path
            if ($resolvedRun.StartsWith($resolvedTemp) -and
                $resolvedRun -like "*agentmod-plugin-lifecycle-e2e-*") {
                Remove-Item -LiteralPath $resolvedRun -Recurse -Force
            }
        }
    }
    if (-not $succeeded) {
        throw "plugin lifecycle E2E did not complete"
    }
}
finally {
    Pop-Location
}
