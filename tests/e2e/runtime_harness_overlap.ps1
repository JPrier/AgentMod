param(
    [string]$Configuration = "debug"
)

$ErrorActionPreference = "Stop"
$Repository = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$GateRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("agentmod-harness-overlap-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $GateRoot | Out-Null
Push-Location $Repository
try {
    cargo build -p agentmod-harness -p agentmod-runtime
    $Suffix = if ($IsWindows -or $env:OS -eq "Windows_NT") { ".exe" } else { "" }
    $env:AGENTMOD_HARNESS_PROGRAM = Join-Path $Repository "target\$Configuration\agentmod-harness$Suffix"
    $env:AGENTMOD_HARNESS_TEST_GATE_ROOT = $GateRoot
    $Raw = & (Join-Path $Repository "target\$Configuration\agentmod-runtime$Suffix") harness-overlap-smoke
    if ($LASTEXITCODE -ne 0) {
        throw "runtime harness overlap smoke exited with $LASTEXITCODE"
    }
    $Result = $Raw | ConvertFrom-Json
    if ($Result.status -ne "ok" -or
        $Result.boundary -ne "runtime_service_to_bounded_harness_pool" -or
        $Result.started_before_release -ne 2 -or
        $Result.released -ne 2 -or
        $Result.maximum_connections -ne 2) {
        throw "runtime harness requests did not overlap through the bounded pool"
    }
    Write-Output "runtime/harness bounded-overlap E2E passed"
}
finally {
    Remove-Item Env:AGENTMOD_HARNESS_PROGRAM -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_HARNESS_TEST_GATE_ROOT -ErrorAction SilentlyContinue
    Pop-Location
    if (Test-Path -LiteralPath $GateRoot) {
        Remove-Item -LiteralPath $GateRoot -Recurse -Force
    }
}
