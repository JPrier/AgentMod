$ErrorActionPreference = "Stop"

# Opt-in live provider smoke test. Never runs by default: it requires explicit
# credentials through environment variables and never requires paid APIs for
# the default suite. Drives the native harness binary directly (not through
# the runtime daemon) so provider variables are visible to the harness.
#
# Required:
#   $env:AGENTMOD_LIVE_SMOKE = "1"
#   $env:AGENTMOD_PROVIDER_<ID>_API_KEY (or api_key_ref option to a file)
#
# Optional:
#   $env:AGENTMOD_LIVE_SMOKE_PROVIDER = "openrouter" (default)
#   $env:AGENTMOD_LIVE_SMOKE_MODEL = "..." (defaults to adapter default)
#   $env:AGENTMOD_PROVIDER_<ID>_BASE_URL (explicit endpoint)

if ($env:AGENTMOD_LIVE_SMOKE -ne "1") {
    Write-Output "live provider smoke skipped (set AGENTMOD_LIVE_SMOKE=1 to enable)"
    exit 0
}

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build -p agentmod-harness
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $provider = if ($env:AGENTMOD_LIVE_SMOKE_PROVIDER) { $env:AGENTMOD_LIVE_SMOKE_PROVIDER } else { "openrouter" }
    $model = if ($env:AGENTMOD_LIVE_SMOKE_MODEL) { $env:AGENTMOD_LIVE_SMOKE_MODEL } else { "" }
    $modelOption = if ($model) { ",""model"":""$model""" } else { "" }
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path

    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $harness
    $psi.UseShellExecute = $false
    $psi.RedirectStandardInput = $true
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.Environment["AGENTMOD_HARNESS_AUTH_KEY"] = "ab" * 32
    $psi.Environment["AGENTMOD_HARNESS_DEV_MODE"] = "1"
    $process = $null
    try {
        $process = [System.Diagnostics.Process]::Start($psi)

        $command = ('{"command":"execute","value":{"session_id":"018f6f83-7b80-7000-8000-000000000001",' +
            '"provider":"' + $provider + '","model":"' + $model + '",' +
            '"entries":[{"kind":"user","value":{"text":"Reply with the single word: pong"}}],' +
            '"options":{"max_tokens":64,"streaming":true}' + $modelOption + ',' +
            '"authorization_grant":"grant","cancellation_id":"018f6f83-7b80-7000-8000-000000000002"}}')
        $encoded = [System.Text.Encoding]::UTF8.GetBytes($command + "`n")
        $process.StandardInput.BaseStream.Write($encoded, 0, $encoded.Length)
        $process.StandardInput.BaseStream.Flush()

        $events = @()
        for ($i = 0; $i -lt 4096; $i++) {
            $line = $process.StandardOutput.ReadLine()
            if ($null -eq $line) { break }
            $frame = $line | ConvertFrom-Json
            if ($frame.reply -eq "failed") {
                throw "harness failed: $($frame.value.code): $($frame.value.message)"
            }
            $events += $frame.value
            if ($frame.value.terminal) { break }
        }
        if ($events.Count -eq 0) { throw "no events received" }
        $completed = $events | Where-Object { $_.event -eq "completed" } | Select-Object -First 1
        if ($null -eq $completed) {
            throw "live provider did not complete: last event $($events[-1].event)"
        }
        Write-Output "live provider smoke passed: provider=$provider model=$model"
    }
    finally {
        if ($null -ne $process -and -not $process.HasExited) {
            $process.Kill()
            $process.WaitForExit()
        }
    }
}
finally {
    Pop-Location
}
$global:LASTEXITCODE = 0
