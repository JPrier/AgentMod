$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-filesystem-host -p agentmod-process-host -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $filesystem = (Resolve-Path "target\debug\agentmod-filesystem-host.exe").Path
    $processHost = (Resolve-Path "target\debug\agentmod-process-host.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
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
        std::thread::sleep(std::time::Duration::from_secs(30));
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
        $argumentJson = $Arguments | ConvertTo-Json -Compress -Depth 12
        $callId = "process-action-" + [guid]::NewGuid().ToString("N")
        $response = & $cli run "execute deterministic process action" `
            --session $Session `
            --option 'mock_scenario="process_action"' `
            --option "mock_process_tool=$Tool" `
            --option "mock_process_arguments=$argumentJson" `
            --option "mock_process_call_id=$callId" `
            --json
        if ($LASTEXITCODE -ne 0) {
            throw "process action failed: $Tool"
        }
        return $response | ConvertFrom-Json
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
        if ($reconciliationStarted.Count -ne 1 -or
            $reconciliationCompleted.Count -ne 1) {
            throw "expected one canonical process reconciliation event pair"
        }
        if ($reconciliationStarted[0].event.payload.payload.process_id -ne $processId -or
            $reconciliationCompleted[0].event.payload.payload.process_id -ne $processId -or
            $reconciliationCompleted[0].event.payload.payload.status -ne "live") {
            throw "canonical process reconciliation classification is incorrect"
        }
        $reconciliationCallId = (
            $reconciliationCompleted[0].event.payload.payload.call_id
        )
        $terminal = @(
            $records | Where-Object {
                $_.event.metadata.event_type -eq "tool.execution_completed" -and
                $_.event.payload.payload.call_id -eq $reconciliationCallId
            }
        )
        if ($terminal.Count -ne 1 -or
            $reconciliationStarted[0].sequence -ge
                $reconciliationCompleted[0].sequence -or
            $reconciliationCompleted[0].sequence -ge $terminal[0].sequence) {
            throw "canonical process reconciliation ordering is incorrect"
        }
        $succeeded = $true
        Write-Output (
            "runtime restart preserved one PTY, reattached it, exchanged input/output, " +
            "and committed exit without redispatch"
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
