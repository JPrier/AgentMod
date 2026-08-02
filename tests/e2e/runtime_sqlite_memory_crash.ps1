$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repository
try {
    cargo build --locked -p agentmod-runtime -p agentmod-harness -p agentmod-cli
    if ($LASTEXITCODE -ne 0) { throw "build failed" }
    python tests\e2e\sqlite_memory_crash_e2e.py `
        --repository $repository `
        --runtime (Resolve-Path target\debug\agentmod-runtime.exe).Path `
        --cli (Resolve-Path target\debug\agentmod.exe).Path `
        --harness (Resolve-Path target\debug\agentmod-harness.exe).Path `
        --platform windows
    if ($LASTEXITCODE -ne 0) {
        throw "SQLite automatic-memory crash-cut E2E failed"
    }
}
finally {
    Pop-Location
}
