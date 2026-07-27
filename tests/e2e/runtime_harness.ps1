param(
    [string]$Configuration = "debug"
)

$ErrorActionPreference = "Stop"
$Repository = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Push-Location $Repository
try {
    cargo build -p agentmod-harness -p agentmod-runtime
    $Suffix = if ($IsWindows -or $env:OS -eq "Windows_NT") { ".exe" } else { "" }
    $Harness = Join-Path $Repository "target\$Configuration\agentmod-harness$Suffix"
    $Runtime = Join-Path $Repository "target\$Configuration\agentmod-runtime$Suffix"
    $env:AGENTMOD_HARNESS_PROGRAM = $Harness
    $Raw = & $Runtime harness-smoke
    if ($LASTEXITCODE -ne 0) {
        throw "runtime harness smoke exited with $LASTEXITCODE"
    }
    $Result = $Raw | ConvertFrom-Json
    if ($Result.status -ne "ok" -or
        $Result.boundary -ne "runtime_service_to_harness_process" -or
        $Result.text -ne "alpha beta runtime-harness-ok" -or
        $Result.event_count -ne 5) {
        throw "runtime harness response did not match the expected lifecycle"
    }
    Write-Output "runtime/harness process E2E passed"
}
finally {
    Remove-Item Env:AGENTMOD_HARNESS_PROGRAM -ErrorAction SilentlyContinue
    Pop-Location
}
