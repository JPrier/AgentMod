$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli `
        -p agentmod-plugin-host -p agentmod-plugin-fixture-worker `
        -p agentmod-filesystem-host -p agentmod-scheduler
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
        "agentmod-plugin-expansion-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $workspace = Join-Path $runRoot "workspace"
    $pluginStyles = Join-Path $runRoot "styles\plugins"
    New-Item -ItemType Directory -Path $workspace, $pluginStyles -Force |
        Out-Null
    Copy-Item -LiteralPath (
        Join-Path $repository "tests\fixtures\plugins\plugin-expanded-style.toml"
    ) -Destination $pluginStyles
    Set-Content -LiteralPath (Join-Path $workspace "plugin-selected.txt") `
        -Value "plugin rewrite reached the selected file"

    function New-PluginManifest {
        param(
            [string]$Id,
            [string]$Category,
            [string]$Classification,
            [string]$Worker,
            [string]$SubscribedEvents,
            [string]$FailurePolicy,
            [string]$ObserverMode,
            [string]$Extra
        )
        @"
schema_version = 1
category = "$Category"
scope = "session"
classification = "$Classification"
trust = "approved_third_party"
isolation = "process"
required_capabilities = ["events"]
provided_capabilities = []
subscribed_events = [$SubscribedEvents]
timeout_ms = 5000
state_migration_version = 1

[identity]
id = "$Id"
version = "1.0.0"
runtime_api = "^0.1"

[entrypoint]
kind = "process"
program = '$Worker'
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
after = []

[configuration]
schema_id = "$Id.config"
schema_version = 1
required = false

[configuration.source]
kind = "inline_json"
document = '{"type":"object","additionalProperties":false}'

[failure_policy]
kind = "$FailurePolicy"

[observer_delivery]
mode = "$ObserverMode"
$Extra
"@
    }

    $rewriter = Join-Path $runRoot "fixture-rewriter.toml"
    New-PluginManifest `
        -Id "fixture.rewriter" `
        -Category "interceptor" `
        -Classification "blocking" `
        -Worker $pluginWorker `
        -SubscribedEvents '"action.proposed"' `
        -FailurePolicy "reject" `
        -ObserverMode "best_effort" `
        -Extra "" | Set-Content -LiteralPath $rewriter

    $observer = Join-Path $runRoot "fixture-durable-observer.toml"
    New-PluginManifest `
        -Id "fixture.durable-observer" `
        -Category "observer" `
        -Classification "observer" `
        -Worker $pluginWorker `
        -SubscribedEvents '"plugin.invocation_completed", "tool.execution_completed"' `
        -FailurePolicy "continue" `
        -ObserverMode "at_least_once" `
        -Extra "max_attempts = 3`nretry_backoff_ms = 50" |
        Set-Content -LiteralPath $observer

    $graphNode = Join-Path $runRoot "fixture-graph-node.toml"
    @"
schema_version = 1
category = "graph_node"
scope = "session"
classification = "blocking"
trust = "approved_third_party"
isolation = "process"
required_capabilities = ["events"]
provided_capabilities = []
subscribed_events = []
timeout_ms = 5000
state_migration_version = 1

[identity]
id = "fixture.graph-node"
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
after = []

[configuration]
schema_id = "fixture.graph-node.config"
schema_version = 1
required = false

[configuration.source]
kind = "inline_json"
document = '{"type":"object","additionalProperties":false}'

[failure_policy]
kind = "reject"

[[node_executors]]
executor_id = "fixture.node"
version = "1.0.0"
node_kind = "emit_event"
runtime_api = "^1.0"
required_capabilities = ["events"]
input_schema = '{"type":"object"}'
output_schema = '{"type":"object"}'
timeout_ms = 3000
failure_policy = "reject"
idempotent = true
external_effect = false
read_authority = ["session_state"]
state_scope = "plugin_state"
"@ | Set-Content -LiteralPath $graphNode

    $memory = Join-Path $runRoot "fixture-memory.toml"
    @"
schema_version = 1
category = "memory"
scope = "session"
classification = "blocking"
trust = "approved_third_party"
isolation = "process"
required_capabilities = ["events"]
provided_capabilities = []
subscribed_events = []
timeout_ms = 5000
state_migration_version = 1

[identity]
id = "fixture.memory"
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
after = []

[configuration]
schema_id = "fixture.memory.config"
schema_version = 1
required = false

[configuration.source]
kind = "inline_json"
document = '{"type":"object","additionalProperties":false}'

[failure_policy]
kind = "reject"

[memory]
scopes = ["session", "project"]
capabilities = ["retrieve", "write"]
bounded_bytes = 1048576
"@ | Set-Content -LiteralPath $memory

    $compaction = Join-Path $runRoot "fixture-compaction.toml"
    @"
schema_version = 1
category = "compaction"
scope = "session"
classification = "blocking"
trust = "approved_third_party"
isolation = "process"
required_capabilities = ["events"]
provided_capabilities = []
subscribed_events = []
timeout_ms = 5000
state_migration_version = 1

[identity]
id = "fixture.compaction"
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
after = []

[configuration]
schema_id = "fixture.compaction.config"
schema_version = 1
required = false

[configuration.source]
kind = "inline_json"
document = '{"type":"object","additionalProperties":false}'

[failure_policy]
kind = "reject"

[compaction]
strategy_id = "fixture.plugin-summary"
idempotent = true
bounded_bytes = 65536
"@ | Set-Content -LiteralPath $compaction

    $transform = Join-Path $runRoot "fixture-context-transform.toml"
    @"
schema_version = 1
category = "context_transform"
scope = "session"
classification = "blocking"
trust = "approved_third_party"
isolation = "process"
required_capabilities = ["events"]
provided_capabilities = []
subscribed_events = []
timeout_ms = 5000
state_migration_version = 1

[identity]
id = "fixture.context-transform"
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
after = []

[configuration]
schema_id = "fixture.context-transform.config"
schema_version = 1
required = false

[configuration.source]
kind = "inline_json"
document = '{"type":"object","additionalProperties":false}'

[failure_policy]
kind = "reject"

[[context_transforms]]
transform_id = "fixture.anonymize"
boundary = "before_provider_projection"
stage = 10
priority = 5
before = []
after = []
"@ | Set-Content -LiteralPath $transform

    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-plugin-expansion-e2e-" + [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_FILESYSTEM_HOST_PROGRAM = $filesystemHost
    $env:AGENTMOD_PLUGIN_HOST_PROGRAM = $pluginHost
    $env:AGENTMOD_PLUGIN_MANIFESTS = (
        $rewriter + [System.IO.Path]::PathSeparator + $observer +
        [System.IO.Path]::PathSeparator + $graphNode +
        [System.IO.Path]::PathSeparator + $memory +
        [System.IO.Path]::PathSeparator + $compaction +
        [System.IO.Path]::PathSeparator + $transform
    )
    $env:AGENTMOD_PLUGIN_EXECUTABLE_ROOTS = Split-Path $pluginWorker -Parent
    $env:AGENTMOD_PLUGIN_IDLE_TIMEOUT_MS = "2000"
    $env:RUST_BACKTRACE = "1"
    $runtimeOut = Join-Path $runRoot "runtime.stdout.log"
    $runtimeErr = Join-Path $runRoot "runtime.stderr.log"
    $daemon = $null

    function Start-TestDaemon {
        Start-Process -FilePath $runtime -ArgumentList "serve" `
            -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput $runtimeOut -RedirectStandardError $runtimeErr
        for ($attempt = 0; $attempt -lt 100; $attempt++) {
            try {
                & $cli doctor --json 2>$null | Out-Null
                if ($LASTEXITCODE -eq 0) { break }
            } catch {}
            Start-Sleep -Milliseconds 100
        }
    }

    try {
        $daemon = Start-TestDaemon
        if ($daemon.HasExited) {
            Get-Content -LiteralPath $runtimeErr
            throw "runtime exited before plugin expansion test"
        }
        $styles = & $cli style list --json | ConvertFrom-Json
        if (-not @($styles.styles | Where-Object {
            $_.id -eq "plugin-expanded" -and $_.availability -eq "available"
        })) {
            throw "plugin-expanded style is unavailable"
        }
        $session = & $cli session create --workspace $workspace `
            --style plugin-expanded@1.0.0 --json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) {
            throw "session create failed for plugin-expanded style"
        }
        & $cli run "read through the expanded plugin set" `
            --session $session.session_id `
            --option 'mock_scenario="one_tool_call"' --json | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Get-Content -LiteralPath $runtimeErr
            throw "plugin-expanded turn failed"
        }
        $sessionDir = Join-Path $runRoot ("sessions\" + $session.session_id)
        $journalPath = Join-Path $sessionDir "events.jsonl"
        $journal = Get-Content -LiteralPath $journalPath -Raw
        if ($journal -notmatch "plugin rewrite reached the selected file") {
            throw "plugin replacement did not reach filesystem execution"
        }
        if ($journal -notmatch "plugin.set_activated" -or
            $journal -notmatch "plugin.invocation_completed") {
            throw "plugin activation and invocation were not canonical"
        }
        if ($journal -notmatch 'plugin.audit_recorded' -or
            $journal -notmatch 'observer_delivery_attempted') {
            # Observer delivery and canonical audit commit asynchronously after
            # the turn's terminal frame; poll for the audit events while
            # tolerating transient journal-lock windows during append.
            for ($attempt = 0; $attempt -lt 600; $attempt++) {
                Start-Sleep -Milliseconds 50
                try {
                    $journal = Get-Content -LiteralPath $journalPath -Raw -ErrorAction Stop
                } catch {
                    continue
                }
                if ($journal -match 'plugin.audit_recorded' -and
                    $journal -match 'observer_delivery_attempted') {
                    break
                }
            }
        }
        if ($journal -notmatch 'plugin.audit_recorded' -or
            $journal -notmatch 'observer_delivery_attempted') {
            Get-Content -LiteralPath $runtimeErr
            Get-Content -LiteralPath $journalPath | Select-Object -Last 12
            throw "observer delivery was not canonically audited"
        }
        $observerMarker = Join-Path $sessionDir "fixture-observer-received.log"
        for ($attempt = 0; $attempt -lt 200 -and
            -not (Test-Path -LiteralPath $observerMarker); $attempt++) {
            Start-Sleep -Milliseconds 50
        }
        if (-not (Test-Path -LiteralPath $observerMarker)) {
            Get-Content -LiteralPath $runtimeErr
            throw "committed event was not delivered to durable observer"
        }
        $deliveryState = Join-Path $sessionDir ".agentmod\plugin-state\deliveries.json"
        $deliveryFile = Get-ChildItem -Path $deliveryState -ErrorAction SilentlyContinue
        if ($null -eq $deliveryFile) {
            $generation = Get-ChildItem -Path (Join-Path $sessionDir ".agentmod\plugin-state") `
                -Filter "deliveries.json.gen-*.json" -ErrorAction SilentlyContinue |
                Select-Object -First 1
            if ($null -eq $generation) {
                Get-ChildItem -LiteralPath $sessionDir -Recurse |
                    Select-Object FullName, Length
                throw "durable delivery journal was not persisted"
            }
            $deliveryFile = $generation
        }
        $deliveryJson = Get-Content -LiteralPath $deliveryFile.FullName -Raw
        if ($deliveryJson -notmatch "observer_delivery_completed") {
            throw "durable delivery did not reach terminal completed"
        }
        $markerLines = @(Get-Content -LiteralPath $observerMarker)
        $baselineHostCount = @(Get-Process -Name "agentmod-plugin-host" `
            -ErrorAction SilentlyContinue).Count

        # Idle teardown: with a 2s idle timeout the supervised plugin host must
        # exit so the dormant session retains no process.
        Start-Sleep -Seconds 6
        $idleHostCount = @(Get-Process -Name "agentmod-plugin-host" `
            -ErrorAction SilentlyContinue).Count
        if ($idleHostCount -gt $baselineHostCount) {
            Get-Content -LiteralPath $runtimeErr
            throw "plugin host was not torn down while idle"
        }

        # Lazy restart: a fresh turn on a new session must restart the host,
        # restore the loaded catalog, and complete without a new activation
        # request failing. A fresh session is used because the deterministic
        # mock provider reuses tool call IDs across turns on one session, which
        # the runtime correctly rejects as a duplicate dispatch.
        $secondSession = & $cli session create --workspace $workspace `
            --style plugin-expanded@1.0.0 --json | ConvertFrom-Json
        $secondRun = & $cli run "read again after idle host teardown" `
            --session $secondSession.session_id `
            --option 'mock_scenario="one_tool_call"' --json 2>&1
        if ($LASTEXITCODE -ne 0) {
            Get-Content -LiteralPath $runtimeErr
            Write-Output $secondRun
            throw "lazy-restarted plugin host turn failed"
        }
        $secondDir = Join-Path $runRoot ("sessions\" + $secondSession.session_id)
        $secondJournal = Get-Content -LiteralPath (Join-Path $secondDir "events.jsonl") -Raw
        if ($secondJournal -notmatch "plugin rewrite reached the selected file") {
            throw "rewriter did not run after lazy restart"
        }
        $secondMarker = Join-Path $secondDir "fixture-observer-received.log"
        for ($attempt = 0; $attempt -lt 200 -and
            -not (Test-Path -LiteralPath $secondMarker); $attempt++) {
            Start-Sleep -Milliseconds 50
        }
        if (-not (Test-Path -LiteralPath $secondMarker)) {
            Get-Content -LiteralPath $runtimeErr
            throw "lazy-restarted observer did not deliver the new event"
        }

        # Restart recovery: kill the daemon, restart, and prove durable
        # deliveries and activated plugin state survive.
        Stop-Process -Id $daemon.Id -Force
        $daemon.WaitForExit()
        $daemon = Start-TestDaemon
        $recovered = & $cli session inspect $session.session_id --json |
            ConvertFrom-Json
        if ($recovered.state.style_binding.id -ne "plugin-expanded" -or
            @($recovered.state.plugins.activated_plugin_ids) -notcontains
                "fixture.durable-observer") {
            throw "plugin composition did not recover after daemon restart"
        }
        $restartMarkerLines = @(Get-Content -LiteralPath $observerMarker)
        if ($restartMarkerLines.Count -ne $markerLines.Count) {
            throw "restart redelivered an already committed observer event"
        }
        Write-Output (
            "runtime plugin graph-node/memory/compaction/context-transform " +
            "catalog, durable observer delivery, canonical audit, idle host " +
            "teardown, lazy restart, and daemon-restart recovery E2E passed"
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
            $resolvedRun -like "*agentmod-plugin-expansion-e2e-*") {
            for ($attempt = 0; $attempt -lt 10; $attempt++) {
                try {
                    Remove-Item -LiteralPath $resolvedRun -Recurse -Force -ErrorAction Stop
                    break
                } catch {
                    Start-Sleep -Milliseconds 200
                }
            }
        }
    }
}
finally {
    Pop-Location
}
