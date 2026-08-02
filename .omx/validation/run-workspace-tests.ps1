$ErrorActionPreference = "Continue"

$root = "C:\Users\jkpri\AgentMod"
$result = Join-Path $root ".omx\validation\workspace-test.result.json"
$stdout = Join-Path $root ".omx\validation\workspace-test.stdout.log"
$stderr = Join-Path $root ".omx\validation\workspace-test.stderr.log"
$timer = [System.Diagnostics.Stopwatch]::StartNew()

Push-Location $root
try {
    & cargo test --workspace --all-targets --all-features --locked 1> $stdout 2> $stderr
    $exitCode = $LASTEXITCODE
}
finally {
    Pop-Location
}

$timer.Stop()
[pscustomobject]@{
    command = "cargo test --workspace --all-targets --all-features --locked"
    exit_code = $exitCode
    elapsed_seconds = $timer.Elapsed.TotalSeconds
    completed_at = [DateTimeOffset]::Now.ToString("o")
} | ConvertTo-Json | Set-Content -LiteralPath $result -Encoding utf8

exit $exitCode
