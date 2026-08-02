$ErrorActionPreference = "Stop"

$prior = $env:AGENTMOD_GRAPH_B_CANCELLATION_ONLY
try {
    $env:AGENTMOD_GRAPH_B_CANCELLATION_ONLY = "1"
    & (Join-Path $PSScriptRoot "runtime_arbitrary_graph_b.ps1")
    if ($LASTEXITCODE -ne 0) {
        throw "Graph B accepted-cancellation E2E failed"
    }
}
finally {
    $env:AGENTMOD_GRAPH_B_CANCELLATION_ONLY = $prior
}
