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
        "agentmod-plugin-automatic-memory-e2e-" +
        [guid]::NewGuid().ToString("N")
    )
    $succeeded = $false
    $workspace = Join-Path $runRoot "workspace"
    $styleRoot = Join-Path $runRoot "styles\user"
    New-Item -ItemType Directory -Path $workspace, $styleRoot -Force |
        Out-Null
    $marker = Join-Path $runRoot "memory-invocations.log"
    $pluginExecutable = Join-Path $runRoot "plugin-fixture-worker.exe"
    $offlinePluginExecutable = "$pluginExecutable.offline"
    Copy-Item -LiteralPath $pluginWorker -Destination $pluginExecutable
    $normalizedPluginExecutable = $pluginExecutable.Replace("\", "/")
    $normalizedMarker = $marker.Replace("\", "/")
    $pluginArguments = @(
        "--memory-marker",
        $normalizedMarker
    ) | ForEach-Object {
        '"' + $_.Replace('\', '\\').Replace('"', '\"') + '"'
    }
    $manifest = (
        Get-Content tests\fixtures\plugins\automatic-memory.toml -Raw
    ).Replace(
        "__PLUGIN_PROGRAM__",
        $normalizedPluginExecutable
    ).Replace(
        "__PLUGIN_ARGS__",
        "[" + ($pluginArguments -join ", ") + "]"
    )
    $manifestPath = Join-Path $runRoot "automatic-memory-plugin.toml"
    Set-Content -LiteralPath $manifestPath -Value $manifest -NoNewline

    $providers = @(
        @{
            Name = "success"
            Id = "fixture.memory.success"
            Hash = "2e4f6dc7fa1e3ad211c32148bacb5c208c99ed726e021866ac31699742240266"
        },
        @{
            Name = "invalid"
            Id = "fixture.memory.invalid"
            Hash = "572f03ae7b5fde6771c40521785c287a722da29a2814a0c228e320eaa94aca66"
        },
        @{
            Name = "timeout"
            Id = "fixture.memory.timeout"
            Hash = "6025dd4db5a87e2d72055147bc5f9022ab1ffd8850d034be47d98384d08ad338"
        }
    )
    $styleTemplate = Get-Content `
        tests\fixtures\styles\plugin-automatic-memory.toml -Raw
    foreach ($provider in $providers) {
        $style = $styleTemplate.Replace(
            "__STYLE_ID__",
            "e2e-plugin-memory-" + $provider.Name
        ).Replace(
            "__PROVIDER_ID__",
            $provider.Id
        ).Replace(
            "__DECLARATION_HASH__",
            $provider.Hash
        )
        Set-Content -LiteralPath (
            Join-Path $styleRoot ($provider.Name + ".toml")
        ) -Value $style -NoNewline
    }

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-plugin-memory-e2e-" +
        [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_PLUGIN_HOST_PROGRAM = $pluginHost
    $env:AGENTMOD_PLUGIN_MANIFESTS = $manifestPath
    $env:AGENTMOD_PLUGIN_EXECUTABLE_ROOTS = @(
        (Split-Path -Parent $pluginExecutable),
        (Split-Path -Parent $pluginWorker)
    ) -join [IO.Path]::PathSeparator
    $env:AGENTMOD_MEMORY_WRITE_PERMISSION_MODE = "ask"
    $env:AGENTMOD_FIXTURE_WORKER_PROGRAM = $pluginWorker
    $env:AGENTMOD_FIXTURE_MEMORY_MARKER = $marker

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

    function Read-Journal($sessionId) {
        $path = Join-Path $runRoot (
            "sessions\" + $sessionId + "\events.jsonl"
        )
        for ($attempt = 0; $attempt -lt 80; $attempt++) {
            try {
                return @(Get-Content -LiteralPath $path -ErrorAction Stop |
                    ForEach-Object {
                        $_ | ConvertFrom-Json
                    })
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

    function Create-Session($providerName) {
        $created = & $cli session create --workspace $workspace `
            --style ("e2e-plugin-memory-" + $providerName) --json |
            ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw "session creation failed" }
        return $created.session_id
    }

    function Begin-Approval($sessionId, $runId, $prompt) {
        $result = & $cli run $prompt --session $sessionId `
            --cancellation-id $runId `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="plugin-memory-output"' --json |
            ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or
            [string]::IsNullOrWhiteSpace($result.awaiting_continuation)) {
            $runtimeLog = Get-Content -LiteralPath (
                Join-Path $runRoot (
                    "runtime-" + $runtimeStarts + ".stderr.log"
                )
            ) -Raw
            throw (
                "plugin automatic memory did not create a durable approval: " +
                $runtimeLog
            )
        }
        $events = Read-Journal $sessionId
        $proposal = @($events | Where-Object {
            $_.event.metadata.event_type -eq "memory.write_proposed"
        })
        if ($proposal.Count -ne 1 -or
            (Event-Count $events "memory.write_approved") -ne 0 -or
            (Event-Count $events "memory.write_dispatched") -ne 0) {
            throw "pre-approval plugin write lifecycle is invalid"
        }
        return @{
            Continuation = $result.awaiting_continuation
            Invocation = (
                $proposal[0].event.payload.payload.identity.plugin.invocation_id
            )
        }
    }

    function Start-Approval($sessionId, $continuationId) {
        $info = [Diagnostics.ProcessStartInfo]::new()
        $info.FileName = $cli
        $info.WorkingDirectory = $repository
        $info.UseShellExecute = $false
        $info.CreateNoWindow = $true
        $info.RedirectStandardOutput = $true
        $info.RedirectStandardError = $true
        foreach ($argument in @(
            "approval", "resolve", $sessionId, $continuationId,
            "approve", "--json"
        )) {
            [void]$info.ArgumentList.Add($argument)
        }
        $process = [Diagnostics.Process]::new()
        $process.StartInfo = $info
        [void]$process.Start()
        return $process
    }

    function Set-WrapperOffline($offline) {
        if ($offline) {
            if (Test-Path -LiteralPath $pluginExecutable) {
                Move-Item -LiteralPath $pluginExecutable `
                    -Destination $offlinePluginExecutable
            }
        } elseif (Test-Path -LiteralPath $offlinePluginExecutable) {
            Move-Item -LiteralPath $offlinePluginExecutable `
                -Destination $pluginExecutable
        }
    }

    function Invoke-ApprovalRecovery(
        $sessionId,
        $continuationId,
        $expectSuccess
    ) {
        $output = & $cli approval resolve $sessionId $continuationId `
            approve --json 2>(Join-Path $runRoot "approval-recovery.stderr.log")
        $exitCode = $LASTEXITCODE
        if ($expectSuccess -and $exitCode -ne 0) {
            throw "receipt recovery approval failed: $output"
        }
        if (-not $expectSuccess -and $exitCode -eq 0) {
            throw "ambiguous receipt recovery unexpectedly succeeded"
        }
    }

    function Run-ReceiptCut($mode, $restartPendingApproval) {
        $sessionId = Create-Session "success"
        $runId = [guid]::NewGuid().ToString()
        $approval = Begin-Approval $sessionId $runId (
            "remember plugin automatic memory " + $mode
        )
        if ($restartPendingApproval) {
            Stop-Runtime
            Start-Runtime
        }
        $env:AGENTMOD_PLUGIN_RECEIPT_POST_PERSIST_DELAY_MS = "10000"
        Stop-Runtime
        Start-Runtime
        $approvalProcess = Start-Approval $sessionId $approval.Continuation
        $receiptDirectory = Join-Path $runRoot (
            "sessions\" + $sessionId +
            "\artifacts\plugin-invocation-receipts"
        )
        $receiptPath = $null
        for ($attempt = 0; $attempt -lt 250; $attempt++) {
            $events = Read-Journal $sessionId
            $receipts = @(
                Get-ChildItem -LiteralPath $receiptDirectory -Filter *.json `
                    -ErrorAction SilentlyContinue
            )
            if ((Invocation-Count $approval.Invocation) -eq 1 -and
                $receipts.Count -eq 1 -and
                (Event-Count $events "memory.write_dispatched") -eq 1 -and
                (Event-Count $events "memory.write_completed") -eq 0) {
                $receiptPath = $receipts[0].FullName
                break
            }
            if ($approvalProcess.HasExited) {
                throw "approval exited before the durable receipt crash cut: $(
                    $approvalProcess.StandardError.ReadToEnd()
                )"
            }
            Start-Sleep -Milliseconds 50
        }
        if ($null -eq $receiptPath) {
            throw "plugin receipt crash cut was not reached"
        }
        Stop-Runtime
        if (-not $approvalProcess.WaitForExit(5000)) {
            $approvalProcess.Kill($true)
            $approvalProcess.WaitForExit()
        }
        if ($mode -eq "missing") {
            Remove-Item -LiteralPath $receiptPath -Force
        } elseif ($mode -eq "corrupt") {
            $bytes = [IO.File]::ReadAllBytes($receiptPath)
            $bytes[[Math]::Floor($bytes.Length / 2)] = (
                $bytes[[Math]::Floor($bytes.Length / 2)] -bxor 1
            )
            [IO.File]::WriteAllBytes($receiptPath, $bytes)
        }
        Set-WrapperOffline $true
        $env:AGENTMOD_PLUGIN_RECEIPT_POST_PERSIST_DELAY_MS = "0"
        Start-Runtime
        Invoke-ApprovalRecovery $sessionId $approval.Continuation (
            $mode -eq "complete"
        )
        $events = Read-Journal $sessionId
        if ((Invocation-Count $approval.Invocation) -ne 1 -or
            (Event-Count $events "memory.write_proposed") -ne 1 -or
            (Event-Count $events "memory.write_approved") -ne 1 -or
            (Event-Count $events "memory.write_dispatched") -ne 1) {
            throw "receipt recovery duplicated the plugin write lifecycle"
        }
        if ($mode -eq "complete") {
            if ((Event-Count $events "memory.write_completed") -ne 1 -or
                (Event-Count $events "memory.write_ambiguous") -ne 0) {
                throw "terminal receipt did not complete exactly once"
            }
        } elseif ((Event-Count $events "memory.write_completed") -ne 0 -or
            (Event-Count $events "memory.write_ambiguous") -ne 1) {
            throw "$mode receipt was not classified ambiguous exactly once"
        }
        Stop-Runtime
        Set-WrapperOffline $false
        Start-Runtime
    }

    function Run-AmbiguousProvider($providerName, $expectedHandler) {
        $sessionId = Create-Session $providerName
        $runId = [guid]::NewGuid().ToString()
        $prompt = "exercise plugin automatic memory " + $providerName
        $approval = Begin-Approval $sessionId $runId $prompt
        & $cli approval resolve $sessionId $approval.Continuation `
            approve --json 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) {
            throw "$providerName plugin ambiguity unexpectedly succeeded"
        }
        $events = Read-Journal $sessionId
        $line = @(
            Get-Content -LiteralPath $marker | Where-Object {
                $_ -like ($approval.Invocation + "|*")
            }
        )
        if ($line.Count -ne 1 -or
            $line[0] -ne ($approval.Invocation + "|" + $expectedHandler) -or
            (Event-Count $events "memory.write_ambiguous") -ne 1 -or
            (Event-Count $events "memory.write_completed") -ne 0) {
            throw "$providerName ambiguity evidence is incomplete"
        }
        & $cli run $prompt --session $sessionId --cancellation-id $runId `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="plugin-memory-output"' --json 2>$null |
            Out-Null
        if ($LASTEXITCODE -eq 0 -or
            (Invocation-Count $approval.Invocation) -ne 1 -or
            (Event-Count (Read-Journal $sessionId) "memory.write_ambiguous") `
                -ne 1) {
            throw "$providerName ambiguous effect was redispatched"
        }
    }

    $runtimeStarts = 0
    $daemon = $null
    Start-Runtime
    try {
        foreach ($provider in $providers) {
            $validation = & $cli style validate (
                Join-Path $styleRoot ($provider.Name + ".toml")
            ) --json | ConvertFrom-Json
            if ($LASTEXITCODE -ne 0 -or -not $validation.valid) {
                $runtimeLog = Get-Content -LiteralPath (
                    Join-Path $runRoot (
                        "runtime-" + $runtimeStarts + ".stderr.log"
                    )
                ) -Raw
                throw (
                    "plugin memory style validation failed: $($provider.Name): " +
                    ($validation | ConvertTo-Json -Depth 20 -Compress) +
                    "; runtime=$runtimeLog"
                )
            }
        }

        Run-ReceiptCut "complete" $true
        Run-ReceiptCut "missing" $false
        Run-ReceiptCut "corrupt" $false
        Run-AmbiguousProvider "invalid" "wrong_identity_memory_write"
        Run-AmbiguousProvider "timeout" "timeout_memory_write"

        $unavailableSession = Create-Session "success"
        Stop-Runtime
        Set-WrapperOffline $true
        Start-Runtime
        $unavailableRun = [guid]::NewGuid().ToString()
        $unavailablePrompt = "plugin unavailable after session creation"
        $markerCountBeforeUnavailable = @(
            Get-Content -LiteralPath $marker
        ).Count
        & $cli run $unavailablePrompt --session $unavailableSession `
            --cancellation-id $unavailableRun `
            --option 'mock_scenario="streaming_text"' `
            --option 'mock_text="plugin-memory-output"' --json 2>$null |
            Out-Null
        if ($LASTEXITCODE -eq 0) {
            throw "unavailable selected plugin unexpectedly completed"
        }
        $unavailableEvents = Read-Journal $unavailableSession
        if (@(Get-Content -LiteralPath $marker).Count -ne
                $markerCountBeforeUnavailable -or
            (Event-Count $unavailableEvents "memory.write_proposed") -ne 0 -or
            (Event-Count $unavailableEvents "memory.write_dispatched") -ne 0 -or
            (Event-Count $unavailableEvents "memory.write_completed") -ne 0 -or
            (Event-Count $unavailableEvents "memory.write_ambiguous") -ne 0) {
            throw "plugin unavailability was not fail-closed before worker entry"
        }

        Write-Output (
            "runtime/plugin-host automatic-memory approval/receipt/" +
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
        "AGENTMOD_MEMORY_WRITE_PERMISSION_MODE",
        "AGENTMOD_FIXTURE_WORKER_PROGRAM",
        "AGENTMOD_FIXTURE_MEMORY_MARKER",
        "AGENTMOD_PLUGIN_RECEIPT_POST_PERSIST_DELAY_MS"
    ) | ForEach-Object {
        Remove-Item ("Env:\" + $_) -ErrorAction SilentlyContinue
    }
    Pop-Location
}
$global:LASTEXITCODE = 0
