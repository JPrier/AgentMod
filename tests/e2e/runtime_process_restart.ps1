$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-filesystem-host -p agentmod-process-host `
        -p agentmod-scheduler -p agentmod-cli -p agentmod-tui
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $filesystem = (Resolve-Path "target\debug\agentmod-filesystem-host.exe").Path
    $processHost = (Resolve-Path "target\debug\agentmod-process-host.exe").Path
    $scheduler = (Resolve-Path "target\debug\agentmod-scheduler.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $tui = (Resolve-Path "target\debug\agentmod-tui.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-process-restart-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null
    $source = Join-Path $runRoot "interactive.rs"
    $fixture = Join-Path $runRoot (
        "agentmod-interactive-" + [guid]::NewGuid().ToString("N") + ".exe"
    )
    @'
use std::io::{self, BufRead, Write};
fn main() {
    std::thread::spawn(|| {
        std::thread::sleep(std::time::Duration::from_secs(180));
        std::process::exit(124);
    });
    println!("ready");
    io::stdout().flush().expect("flush");
    for line in io::stdin().lock().lines() {
        let line = line.expect("line");
        println!("echo:{line}");
        io::stdout().flush().expect("flush");
        if line == "exit" { break; }
    }
}
'@ | Set-Content -LiteralPath $source -Encoding utf8
    & rustc $source -o $fixture
    if ($LASTEXITCODE -ne 0) { throw "fixture build failed" }

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-process-restart-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_FILESYSTEM_HOST_PROGRAM = $filesystem
    $env:AGENTMOD_PROCESS_HOST_PROGRAM = $processHost
    $env:AGENTMOD_SCHEDULER_PROGRAM = $scheduler
    $env:AGENTMOD_SCHEDULER_ROOT = Join-Path $runRoot "scheduler"
    $env:AGENTMOD_PROCESS_ALLOWED_EXECUTABLES = $fixture
    $env:AGENTMOD_PROCESS_IDLE_TIMEOUT_MS = "750"
    $env:AGENTMOD_PERMISSION_MODE = "allow"
    $daemon = $null
    $runtimeGeneration = 0
    $succeeded = $false
    $initialProcessHosts = @(
        Get-Process -Name "agentmod-process-host" -ErrorAction SilentlyContinue |
            ForEach-Object { $_.Id }
    )

    function Start-TestRuntime {
        $script:runtimeGeneration++
        return Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput (
                Join-Path $runRoot "runtime-$script:runtimeGeneration.stdout.log"
            ) -RedirectStandardError (
                Join-Path $runRoot "runtime-$script:runtimeGeneration.stderr.log"
            )
    }

    function Wait-TestRuntime {
        for ($attempt = 0; $attempt -lt 100; $attempt++) {
            try {
                & $cli doctor --json 2>$null | Out-Null
                if ($LASTEXITCODE -eq 0) { return }
            } catch {}
            Start-Sleep -Milliseconds 50
        }
        throw "runtime did not become ready"
    }

    function Invoke-ProcessAction {
        param(
            [string]$Session,
            [string]$Tool,
            [hashtable]$Arguments
        )
        if ($Arguments.ContainsKey("executable")) {
            $Arguments["executable"] = $Arguments["executable"].Replace("\", "/")
        }
        $argumentJson = $Arguments | ConvertTo-Json -Compress -Depth 12
        $encodedArgumentJson = [Convert]::ToBase64String(
            [Text.Encoding]::UTF8.GetBytes($argumentJson)
        )
        $callId = "process-action-" + [guid]::NewGuid().ToString("N")
        $response = & $cli run "execute deterministic process action" `
            --session $Session `
            --option 'mock_scenario="process_action"' `
            --option "mock_process_tool=$Tool" `
            --option "mock_process_arguments_base64=$encodedArgumentJson" `
            --option "mock_process_call_id=$callId" `
            --json
        if ($LASTEXITCODE -ne 0) {
            throw "process action failed: $Tool"
        }
        $parsed = $response | ConvertFrom-Json
        $failed = @($parsed.events | Where-Object { $_.event -eq "failed" })
        if ($failed.Count -ne 0) {
            throw "process action $Tool returned provider failure: $response"
        }
        return $parsed
    }

    try {
        $daemon = Start-TestRuntime
        Wait-TestRuntime
        $created = & $cli session create --workspace $workspace `
            --style persistent-chat --json | ConvertFrom-Json
        Invoke-ProcessAction -Session $created.session_id `
            -Tool "process.start_pty" -Arguments @{
                executable = $fixture
                arguments = @()
                working_directory = $null
                environment = @{}
                timeout_ms = $null
                output_limit_bytes = 65536
                cleanup = "retain"
                terminal = @{
                    columns = 80
                    rows = 24
                    pixel_width = 0
                    pixel_height = 0
                }
            } | Out-Null

        $processRoot = Join-Path $workspace ".agentmod\process-logs"
        $processRecords = @(Get-ChildItem -LiteralPath $processRoot -Directory)
        if ($processRecords.Count -ne 1) {
            throw "expected exactly one dispatched process, found $($processRecords.Count)"
        }
        $processId = $processRecords[0].Name

        Stop-Process -Id $daemon.Id -Force
        Wait-Process -Id $daemon.Id -ErrorAction SilentlyContinue
        $daemon = Start-TestRuntime
        Wait-TestRuntime

        Invoke-ProcessAction -Session $created.session_id `
            -Tool "process.reattach" -Arguments @{ process_id = $processId } | Out-Null
        & $cli schedule add "on-process-echo" `
            --session $created.session_id `
            --prompt "inspect the newly observed process output" `
            --process-id $processId `
            --contains "echo:hello-after-runtime-restart" `
            --json | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "process-output schedule was not stored"
        }
        Invoke-ProcessAction -Session $created.session_id `
            -Tool "process.input" -Arguments @{
                process_id = $processId
                content = "hello-after-runtime-restart`r`n"
                close = $false
            } | Out-Null

        $journalPath = Join-Path $runRoot (
            "sessions\" + $created.session_id + "\events.jsonl"
        )
        $observed = $false
        for ($attempt = 0; $attempt -lt 30; $attempt++) {
            Invoke-ProcessAction -Session $created.session_id `
                -Tool "process.read" -Arguments @{
                    process_id = $processId
                    stream = "terminal"
                    offset = 0
                    length = 65536
                } | Out-Null
            if ((Get-Content -LiteralPath $journalPath -Raw) -match
                "echo:hello-after-runtime-restart") {
                $observed = $true
                break
            }
            Start-Sleep -Milliseconds 50
        }
        if (-not $observed) {
            throw "reattached PTY output did not enter canonical history"
        }
        $scheduleRan = $false
        for ($attempt = 0; $attempt -lt 50; $attempt++) {
            try {
                $scheduled = @(
                    Get-Content -LiteralPath $journalPath |
                        ForEach-Object { ($_ | ConvertFrom-Json).event } |
                        Where-Object {
                            $_.metadata.event_type -eq "scheduler.fired" -and
                            $_.payload.payload.schedule_id -eq "on-process-echo"
                        }
                )
            }
            catch {
                Start-Sleep -Milliseconds 50
                continue
            }
            if ($scheduled.Count -eq 1) {
                $scheduleRan = $true
                break
            }
            Start-Sleep -Milliseconds 50
        }
        if (-not $scheduleRan) {
            throw "matching process output did not execute its schedule"
        }
        Invoke-ProcessAction -Session $created.session_id `
            -Tool "process.read" -Arguments @{
                process_id = $processId
                stream = "terminal"
                offset = 0
                length = 65536
            } | Out-Null
        Start-Sleep -Milliseconds 250
        $sameRangeDeliveries = @(
            Get-Content -LiteralPath $journalPath |
                ForEach-Object { ($_ | ConvertFrom-Json).event } |
                Where-Object {
                    $_.metadata.event_type -eq "scheduler.fired" -and
                    $_.payload.payload.schedule_id -eq "on-process-echo"
                }
        )
        if ($sameRangeDeliveries.Count -ne 1) {
            throw "the same durable process-output range executed more than once"
        }

        Stop-Process -Id $daemon.Id -Force
        Wait-Process -Id $daemon.Id -ErrorAction SilentlyContinue
        $daemon = Start-TestRuntime
        Wait-TestRuntime
        $scheduledAfterRestart = @(
            Get-Content -LiteralPath $journalPath |
                ForEach-Object { ($_ | ConvertFrom-Json).event } |
                Where-Object {
                    $_.metadata.event_type -eq "scheduler.fired" -and
                    $_.payload.payload.schedule_id -eq "on-process-echo"
                }
        )
        if ($scheduledAfterRestart.Count -ne 1) {
            throw "process-output delivery duplicated after restart"
        }
        Invoke-ProcessAction -Session $created.session_id `
            -Tool "process.reattach" -Arguments @{ process_id = $processId } | Out-Null
        Invoke-ProcessAction -Session $created.session_id `
            -Tool "process.input" -Arguments @{
                process_id = $processId
                content = "exit`r`n"
                close = $false
            } | Out-Null
        Invoke-ProcessAction -Session $created.session_id `
            -Tool "process.wait" -Arguments @{ process_id = $processId } | Out-Null

        $processRecords = @(Get-ChildItem -LiteralPath $processRoot -Directory)
        if ($processRecords.Count -ne 1) {
            throw "runtime restart redispatched the process"
        }
        $journal = Get-Content -LiteralPath $journalPath -Raw
        foreach ($tool in @(
            "process.start_pty",
            "process.reattach",
            "process.input",
            "process.read",
            "process.wait"
        )) {
            if ($journal -notmatch [regex]::Escape($tool)) {
                throw "canonical history is missing $tool"
            }
        }
        $records = @(
            Get-Content -LiteralPath $journalPath |
                ForEach-Object { $_ | ConvertFrom-Json }
        )
        $reconciliationStarted = @(
            $records | Where-Object {
                $_.event.metadata.event_type -eq "process.reconciliation_started"
            }
        )
        $reconciliationCompleted = @(
            $records | Where-Object {
                $_.event.metadata.event_type -eq "process.reconciliation_completed"
            }
        )
        if ($reconciliationStarted.Count -ne 2 -or
            $reconciliationCompleted.Count -ne 2) {
            throw "expected one canonical reconciliation pair per runtime restart"
        }
        foreach ($event in $reconciliationStarted + $reconciliationCompleted) {
            if ($event.event.payload.payload.process_id -ne $processId) {
                throw "canonical process reconciliation identity is incorrect"
            }
        }
        if (@($reconciliationCompleted | Where-Object {
            $_.event.payload.payload.status -ne "live"
        }).Count -ne 0) {
            throw "canonical process reconciliation classification is incorrect"
        }
        foreach ($completed in $reconciliationCompleted) {
            $started = @($reconciliationStarted | Where-Object {
                $_.event.payload.payload.call_id -eq
                    $completed.event.payload.payload.call_id
            })
            $terminal = @(
                $records | Where-Object {
                    $_.event.metadata.event_type -eq "tool.execution_completed" -and
                    $_.event.payload.payload.call_id -eq
                        $completed.event.payload.payload.call_id
                }
            )
            if ($started.Count -ne 1 -or $terminal.Count -ne 1 -or
                $started[0].sequence -ge $completed.sequence -or
                $completed.sequence -ge $terminal[0].sequence) {
                throw "canonical process reconciliation ordering is incorrect"
            }
        }
        $resources = @(
            & $tui --smoke-session-command $created.session_id "/resources" 2>&1
        ) -join [Environment]::NewLine
        if ($LASTEXITCODE -ne 0 -or
            $resources -notmatch "selected=$($created.session_id)" -or
            $resources -notmatch "resources=0/0/2") {
            throw "TUI canonical process resource projection failed: $resources"
        }
        $succeeded = $true
        Write-Output (
            "runtime restart preserved one PTY, reattached it, exchanged input/output, " +
            "delivered output once, and committed exit without redispatch"
        )
    }
    finally {
        if ($null -ne $daemon -and -not $daemon.HasExited) {
            Stop-Process -Id $daemon.Id -Force
            Wait-Process -Id $daemon.Id -ErrorAction SilentlyContinue
        }
        Get-CimInstance Win32_Process |
            Where-Object {
                $null -ne $_.ExecutablePath -and
                $_.ExecutablePath.EndsWith(
                    $fixture,
                    [System.StringComparison]::OrdinalIgnoreCase
                )
            } |
            ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
        Get-Process -Name "agentmod-process-host" -ErrorAction SilentlyContinue |
            Where-Object { $initialProcessHosts -notcontains $_.Id } |
            ForEach-Object {
                Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
            }
        Start-Sleep -Milliseconds 500
        if ($succeeded) {
            $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
            $resolvedRun = (Resolve-Path $runRoot).Path
            if ($resolvedRun.StartsWith($resolvedTemp) -and
                (Split-Path $resolvedRun -Leaf).StartsWith(
                    "agentmod-process-restart-e2e-"
                )) {
                Remove-Item -LiteralPath $resolvedRun -Recurse -Force
            }
        }
        else {
            Write-Warning "preserved failed E2E artifacts at $runRoot"
        }
    }
}
finally {
    Pop-Location
}
