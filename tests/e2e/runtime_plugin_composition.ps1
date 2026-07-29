$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli `
        -p agentmod-plugin-host -p agentmod-plugin-fixture-worker `
        -p agentmod-filesystem-host
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $pluginHost = (Resolve-Path "target\debug\agentmod-plugin-host.exe").Path
    $pluginWorker = (
        Resolve-Path "target\debug\agentmod-plugin-fixture-worker.exe"
    ).Path
    $filesystemHost = (
        Resolve-Path "target\debug\agentmod-filesystem-host.exe"
    ).Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-plugin-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    $pluginStyles = Join-Path $runRoot "styles\plugins"
    New-Item -ItemType Directory -Path $workspace, $pluginStyles -Force |
        Out-Null
    Copy-Item -LiteralPath (
        Join-Path $repository "tests\fixtures\plugins\plugin-composed-style.toml"
    ) -Destination $pluginStyles
    Set-Content -LiteralPath (Join-Path $workspace "plugin-selected.txt") `
        -Value "plugin rewrite reached the selected file"
    $manifest = Join-Path $runRoot "fixture-rewriter.toml"
    @"
schema_version = 1
category = "interceptor"
scope = "session"
classification = "blocking"
trust = "approved_third_party"
isolation = "process"
required_capabilities = ["events"]
provided_capabilities = []
subscribed_events = ["action.proposed"]
timeout_ms = 5000
state_migration_version = 1

[identity]
id = "fixture.rewriter"
version = "1.0.0"
runtime_api = "^0.1"

[entrypoint]
kind = "process"
program = '$pluginWorker'
args = []

[authorities]
read = ["session_state"]
proposed_write = ["canonical_state"]

[permissions]
tools = ["filesystem.read"]
network = []

[ordering]
stage = 20
priority = 100
before = []
after = []

[configuration]
schema_id = "fixture.rewriter.config"
schema_version = 1
required = false

[configuration.source]
kind = "inline_json"
document = '{"type":"object","additionalProperties":false}'

[failure_policy]
kind = "reject"
"@ | Set-Content -LiteralPath $manifest
    $observerManifest = Join-Path $runRoot "fixture-observer.toml"
    @"
schema_version = 1
category = "observer"
scope = "session"
classification = "observer"
trust = "approved_third_party"
isolation = "process"
required_capabilities = ["events"]
provided_capabilities = []
subscribed_events = ["plugin.invocation_completed", "tool.execution_completed"]
timeout_ms = 5000
state_migration_version = 1

[identity]
id = "fixture.observer"
version = "1.0.0"
runtime_api = "^0.1"

[entrypoint]
kind = "process"
program = '$pluginWorker'
args = []

[authorities]
read = ["session_state"]
proposed_write = []

[permissions]
tools = []
network = []

[ordering]
stage = 100
priority = 0
before = []
after = ["fixture.rewriter"]

[configuration]
schema_id = "fixture.observer.config"
schema_version = 1
required = false

[configuration.source]
kind = "inline_json"
document = '{"type":"object","additionalProperties":false}'

[failure_policy]
kind = "continue"
"@ | Set-Content -LiteralPath $observerManifest

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-plugin-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_FILESYSTEM_HOST_PROGRAM = $filesystemHost
    $env:AGENTMOD_PLUGIN_HOST_PROGRAM = $pluginHost
    $env:AGENTMOD_PLUGIN_MANIFESTS = (
        $manifest + [System.IO.Path]::PathSeparator + $observerManifest
    )
    $env:AGENTMOD_PLUGIN_EXECUTABLE_ROOTS = Split-Path $pluginWorker -Parent
    $runtimeOut = Join-Path $runRoot "runtime.stdout.log"
    $runtimeErr = Join-Path $runRoot "runtime.stderr.log"
    $daemon = $null

    try {
        $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput $runtimeOut -RedirectStandardError $runtimeErr
        for ($attempt = 0; $attempt -lt 100; $attempt++) {
            try {
                & $cli doctor --json 2>$null | Out-Null
                if ($LASTEXITCODE -eq 0) { break }
            } catch {}
            Start-Sleep -Milliseconds 100
        }
        if ($daemon.HasExited) {
            Get-Content -LiteralPath $runtimeErr
            throw "runtime exited before plugin test"
        }
        $styles = & $cli style list --json | ConvertFrom-Json
        if (-not @($styles.styles | Where-Object {
            $_.id -eq "plugin-composed" -and $_.availability -eq "available"
        })) {
            $details = & $cli style inspect plugin-composed --json |
                Out-String
            throw "plugin-composed style is unavailable: $details"
        }
        $styleDetails = & $cli style inspect plugin-composed --json |
            ConvertFrom-Json
        if ($styleDetails.summary.source -ne "plugin") {
            throw "plugin-composed style did not retain plugin source identity"
        }
        $session = & $cli session create --workspace $workspace `
            --style plugin-composed@1.0.0 --json | ConvertFrom-Json
        & $cli run "read through the selected plugin" `
            --session $session.session_id `
            --option 'mock_scenario="one_tool_call"' --json | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Get-Content -LiteralPath $runtimeErr
            Get-ChildItem -LiteralPath $runRoot -Recurse |
                Select-Object FullName, Length
            throw "plugin-composed turn failed"
        }
        $journalPath = Join-Path $runRoot (
            "sessions\" + $session.session_id + "\events.jsonl"
        )
        $journal = Get-Content -LiteralPath $journalPath -Raw
        if ($journal -notmatch "plugin rewrite reached the selected file") {
            throw "plugin replacement did not reach filesystem execution"
        }
        if ($journal -notmatch "plugin.set_activated" -or
            $journal -notmatch "plugin.invocation_completed") {
            throw "plugin activation and invocation were not canonical"
        }
        $observerMarker = Join-Path (
            Split-Path $journalPath -Parent
        ) "fixture-observer-received.log"
        for ($attempt = 0; $attempt -lt 100 -and
            -not (Test-Path -LiteralPath $observerMarker); $attempt++) {
            Start-Sleep -Milliseconds 50
        }
        if (-not (Test-Path -LiteralPath $observerMarker) -or
            (Get-Content -LiteralPath $observerMarker -Raw) -notmatch
                "plugin.invocation_completed") {
            Get-Content -LiteralPath $runtimeErr
            throw "committed event was not delivered to observer"
        }
        $inspection = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        if ($inspection.state.style_binding.id -ne "plugin-composed" -or
            @($inspection.state.style_binding.interceptor_order) -notcontains
                "rewrite-tool" -or
            @($inspection.state.plugins.activated_plugin_ids) -notcontains
                "fixture.observer" -or
            @($inspection.state.plugins.invocations).Count -lt 1) {
            throw "plugin-composed binding is not inspectable"
        }

        Stop-Process -Id $daemon.Id -Force
        $daemon.WaitForExit()
        $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput $runtimeOut -RedirectStandardError $runtimeErr
        for ($attempt = 0; $attempt -lt 100; $attempt++) {
            try {
                & $cli doctor --json 2>$null | Out-Null
                if ($LASTEXITCODE -eq 0) { break }
            } catch {}
            Start-Sleep -Milliseconds 100
        }
        $restartedSession = & $cli session create --workspace $workspace `
            --style plugin-composed@1.0.0 --json | ConvertFrom-Json
        & $cli run "read again after plugin runtime recovery" `
            --session $restartedSession.session_id `
            --option 'mock_scenario="one_tool_call"' --json | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Get-Content -LiteralPath $runtimeErr
            throw "plugin-composed turn failed after restart"
        }
        $recovered = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        $reactivated = & $cli session inspect $restartedSession.session_id --json |
            ConvertFrom-Json
        if ($recovered.state.style_binding.id -ne "plugin-composed" -or
            @($recovered.state.plugins.invocations).Count -lt 1 -or
            $reactivated.state.style_binding.id -ne "plugin-composed" -or
            @($reactivated.state.plugins.invocations).Count -lt 1) {
            throw "plugin composition did not recover after runtime restart"
        }
        Write-Output (
            "runtime plugin interceptor, observer, canonical audit, and " +
            "restart E2E passed"
        )
    }
    finally {
        if ($null -ne $daemon -and -not $daemon.HasExited) {
            Stop-Process -Id $daemon.Id -Force
            $daemon.WaitForExit()
        }
        $resolvedTemp = (Resolve-Path ([System.IO.Path]::GetTempPath())).Path
        $resolvedRun = (Resolve-Path $runRoot).Path
        if ($resolvedRun.StartsWith($resolvedTemp) -and
            $resolvedRun -like "*agentmod-plugin-e2e-*") {
            Remove-Item -LiteralPath $resolvedRun -Recurse -Force
        }
    }
}
finally {
    Pop-Location
}
