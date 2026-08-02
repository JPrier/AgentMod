$ErrorActionPreference = "Stop"

$repository = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Push-Location $repository
try {
    cargo build --locked -p agentmod-plugin-host -p agentmod-plugin-fixture-worker
    if ($LASTEXITCODE -ne 0) {
        throw "plugin process fixture build failed with exit code $LASTEXITCODE"
    }
    $env:AGENTMOD_TEST_PLUGIN_HOST = (
        Resolve-Path "target\debug\agentmod-plugin-host.exe"
    ).Path
    $env:AGENTMOD_TEST_PLUGIN_WORKER = (
        Resolve-Path "target\debug\agentmod-plugin-fixture-worker.exe"
    ).Path
    cargo test --locked -p agentmod-integration-tests `
        --test plugin_node_process `
        -- --ignored
    if ($LASTEXITCODE -ne 0) {
        throw "plugin-node process tests failed with exit code $LASTEXITCODE"
    }
}
finally {
    Remove-Item Env:\AGENTMOD_TEST_PLUGIN_HOST -ErrorAction SilentlyContinue
    Remove-Item Env:\AGENTMOD_TEST_PLUGIN_WORKER -ErrorAction SilentlyContinue
    Pop-Location
}
