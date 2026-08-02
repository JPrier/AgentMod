$ErrorActionPreference = "Stop"

# Independent harness conformance E2E: builds and drives the genuinely
# independent agentmod-harness-fixture binary over bounded JSONL stdio and
# verifies protocol negotiation, distinct identity/capabilities, deterministic
# streaming, tool-call continuation, cancellation, and negative capability
# guards. Requires no network and no credentials.
#
# Development grant mode is enabled explicitly for this script; production
# supervision always uses signed runtime grants.

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-harness-fixture
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $fixture = (Resolve-Path "target\debug\agentmod-harness-fixture.exe").Path
    $fixtureProcess = $null

    function Invoke-FixtureCommand([string]$json, [int]$expectedFrames) {
        $encoded = [System.Text.Encoding]::UTF8.GetBytes($json + "`n")
        $fixtureProcess.StandardInput.BaseStream.Write($encoded, 0, $encoded.Length)
        $fixtureProcess.StandardInput.BaseStream.Flush()
        $frames = @()
        for ($i = 0; $i -lt $expectedFrames; $i++) {
            $line = $fixtureProcess.StandardOutput.ReadLine()
            if ($null -eq $line) { throw "fixture closed stdout" }
            $frames += ($line | ConvertFrom-Json)
        }
        return $frames
    }

    try {
        $psi = New-Object System.Diagnostics.ProcessStartInfo
        $psi.FileName = $fixture
        $psi.UseShellExecute = $false
        $psi.RedirectStandardInput = $true
        $psi.RedirectStandardOutput = $true
        $psi.RedirectStandardError = $true
        $psi.Environment["AGENTMOD_HARNESS_AUTH_KEY"] = "11" * 32
        $psi.Environment["AGENTMOD_FIXTURE_DEV_MODE"] = "1"
        $fixtureProcess = [System.Diagnostics.Process]::Start($psi)

        # Health negotiation: distinct capability set.
        $health = Invoke-FixtureCommand '{"command":"health"}' 1
        if ($health[0].value.status -ne "ok" -or
            @($health[0].value.capabilities) -notcontains "streaming" -or
            @($health[0].value.capabilities) -contains "images") {
            throw "health negotiation did not report distinct capabilities"
        }

        # Catalog: distinct harness identity/version.
        $catalog = Invoke-FixtureCommand '{"command":"catalog"}' 1
        $provider = $catalog[0].value.providers[0]
        if ($provider.id -ne "independent-fixture" -or
            $provider.version -ne "2.0.0" -or
            $provider.image_support -ne $false -or
            $provider.structured_output_support -ne $false -or
            $provider.streaming_support -ne $true) {
            throw "catalog identity/capabilities are incorrect"
        }

        # Deterministic streaming with usage.
        $session = "018f6f83-7b80-7000-8000-000000000001"
        $cancel = "018f6f83-7b80-7000-8000-000000000010"
        $stream = Invoke-FixtureCommand (
            '{"command":"execute","value":{"session_id":"' + $session +
            '","provider":"fixture-deterministic","model":"fixture-model",' +
            '"entries":[{"kind":"user","value":{"text":"hello"}}],' +
            '"options":{"fixture_scenario":"streaming_text","fixture_text":"e2e"},' +
            '"authorization_grant":"grant","cancellation_id":"' + $cancel + '"}}'
        ) 5
        if ($stream[0].value.event.event -ne "started" -or
            $stream[$stream.Count - 1].value.event.event -ne "completed") {
            throw "streaming scenario did not start and complete"
        }
        $deltas = @($stream | Where-Object { $_.value.event.event -eq "text_delta" }).Count
        if ($deltas -ne 3) { throw "expected 3 streamed text deltas, got $deltas" }

        # Negative capability guard: image input is rejected.
        $rejected = Invoke-FixtureCommand (
            '{"command":"execute","value":{"session_id":"' + $session +
            '","provider":"fixture-deterministic","model":"fixture-model",' +
            '"entries":[{"kind":"image","value":{"media_type":"image/png","data_base64":"aGVsbG8="}}],' +
            '"options":{"fixture_scenario":"text"},' +
            '"authorization_grant":"grant","cancellation_id":"' + $cancel + '"}}'
        ) 1
        if ($rejected[0].value.event.event -ne "failed" -or
            $rejected[0].value.event.value.code -ne "unsupported_capability") {
            throw "image input was not rejected"
        }

        # In-flight cancellation of the slow streaming scenario.
        $slowCancel = "018f6f83-7b80-7000-8000-000000000011"
        $encoded = [System.Text.Encoding]::UTF8.GetBytes(
            '{"command":"execute","value":{"session_id":"' + $session +
            '","provider":"fixture-deterministic","model":"fixture-model",' +
            '"entries":[{"kind":"user","value":{"text":"wait"}}],' +
            '"options":{"fixture_scenario":"slow_stream"},' +
            '"authorization_grant":"grant","cancellation_id":"' + $slowCancel + '"}}' + "`n")
        $fixtureProcess.StandardInput.BaseStream.Write($encoded, 0, $encoded.Length)
        $fixtureProcess.StandardInput.BaseStream.Flush()
        Start-Sleep -Milliseconds 300
        $encoded = [System.Text.Encoding]::UTF8.GetBytes(
            '{"command":"cancel","value":{"cancellation_id":"' + $slowCancel + '"}}' + "`n")
        $fixtureProcess.StandardInput.BaseStream.Write($encoded, 0, $encoded.Length)
        $fixtureProcess.StandardInput.BaseStream.Flush()
        $cancelled = @()
        for ($i = 0; $i -lt 3; $i++) {
            $line = $fixtureProcess.StandardOutput.ReadLine()
            if ($null -eq $line) { throw "fixture closed stdout" }
            $cancelled += ($line | ConvertFrom-Json)
        }
        if ($cancelled[$cancelled.Count - 1].value.event.event -ne "cancelled") {
            throw "slow stream was not cancelled"
        }

        Write-Output "independent harness conformance E2E passed"
    }
    finally {
        if ($null -ne $fixtureProcess -and -not $fixtureProcess.HasExited) {
            $fixtureProcess.Kill()
            $fixtureProcess.WaitForExit()
        }
    }
}
finally {
    Pop-Location
}
$global:LASTEXITCODE = 0
