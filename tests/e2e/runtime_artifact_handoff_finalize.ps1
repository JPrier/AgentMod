$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build --locked -p agentmod-runtime -p agentmod-scheduler `
        -p agentmod-harness -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }
    $targetRoot = if ([string]::IsNullOrWhiteSpace($env:CARGO_TARGET_DIR)) {
        Join-Path $repository "target"
    } elseif ([IO.Path]::IsPathRooted($env:CARGO_TARGET_DIR)) {
        [IO.Path]::GetFullPath($env:CARGO_TARGET_DIR)
    } else {
        [IO.Path]::GetFullPath((Join-Path $repository $env:CARGO_TARGET_DIR))
    }
    $debugRoot = Join-Path $targetRoot "debug"
    python tests\e2e\artifact_handoff_finalize_e2e.py `
        --repository $repository `
        --runtime (Resolve-Path (Join-Path $debugRoot "agentmod-runtime.exe")).Path `
        --cli (Resolve-Path (Join-Path $debugRoot "agentmod.exe")).Path `
        --harness (Resolve-Path (Join-Path $debugRoot "agentmod-harness.exe")).Path `
        --platform windows
    if ($LASTEXITCODE -ne 0) {
        throw "artifact-handoff finalize E2E failed"
    }
}
finally {
    Pop-Location
}
