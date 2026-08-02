param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9]{2}[a-z0-9]*-[a-z0-9-]+$')]
    [string]$StepId,

    [Parameter(Mandatory = $true)]
    [ValidateSet('cargo')]
    [string]$Executable,

    [Parameter(Mandatory = $true)]
    [string[]]$Arguments
)

$ErrorActionPreference = "Stop"
$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$prefix = Join-Path $PSScriptRoot (
    "2026-07-31-windows-final-" + $StepId
)
$stdoutPath = $prefix + ".stdout.log"
$stderrPath = $prefix + ".stderr.log"
$resultPath = $prefix + ".result.json"

foreach ($path in @($stdoutPath, $stderrPath, $resultPath)) {
    if (Test-Path -LiteralPath $path) {
        throw "refusing to overwrite existing final-validation evidence: $path"
    }
}

$resolvedExecutable = (Get-Command $Executable -ErrorAction Stop).Source
$displayCommand = $Executable + " " + ($Arguments -join " ")
$startedAt = [DateTimeOffset]::UtcNow
$head = (& git -C $repository rev-parse HEAD).Trim()
$dirtyEntries = @(& git -C $repository status --porcelain=v1).Count
$timer = [Diagnostics.Stopwatch]::StartNew()
$exitCode = 127
$launchFailure = $null
$priorCargoColor = $env:CARGO_TERM_COLOR
$priorBacktrace = $env:RUST_BACKTRACE

try {
    $env:CARGO_TERM_COLOR = "never"
    $env:RUST_BACKTRACE = "1"
    try {
        $process = Start-Process -FilePath $resolvedExecutable `
            -ArgumentList $Arguments -WorkingDirectory $repository `
            -WindowStyle Hidden -Wait -PassThru `
            -RedirectStandardOutput $stdoutPath `
            -RedirectStandardError $stderrPath
        $exitCode = $process.ExitCode
    }
    catch {
        $launchFailure = $_.Exception.Message
        [IO.File]::WriteAllText(
            $stderrPath,
            $launchFailure + [Environment]::NewLine,
            [Text.UTF8Encoding]::new($false)
        )
        if (-not (Test-Path -LiteralPath $stdoutPath)) {
            [IO.File]::WriteAllBytes($stdoutPath, [byte[]]::new(0))
        }
    }
}
finally {
    $env:CARGO_TERM_COLOR = $priorCargoColor
    $env:RUST_BACKTRACE = $priorBacktrace
    $timer.Stop()
}

$completedAt = [DateTimeOffset]::UtcNow
$result = [ordered]@{
    schema_version = 1
    step_id = $StepId
    command = $displayCommand
    arguments = $Arguments
    exit_code = $exitCode
    passed = ($exitCode -eq 0)
    elapsed_seconds = $timer.Elapsed.TotalSeconds
    started_at_utc = $startedAt.ToString("o")
    completed_at_utc = $completedAt.ToString("o")
    head = $head
    working_tree_dirty = ($dirtyEntries -gt 0)
    working_tree_porcelain_entries = $dirtyEntries
    stdout_path = [IO.Path]::GetRelativePath($repository, $stdoutPath)
    stderr_path = [IO.Path]::GetRelativePath($repository, $stderrPath)
    launch_failure = $launchFailure
}
$resultJson = $result | ConvertTo-Json -Depth 5
[IO.File]::WriteAllText(
    $resultPath,
    $resultJson + [Environment]::NewLine,
    [Text.UTF8Encoding]::new($false)
)

Write-Output $resultJson
if ($exitCode -ne 0) {
    Write-Output "--- stdout tail ---"
    Get-Content -LiteralPath $stdoutPath -Tail 80 -ErrorAction SilentlyContinue
    Write-Output "--- stderr tail ---"
    Get-Content -LiteralPath $stderrPath -Tail 120 -ErrorAction SilentlyContinue
}
exit $exitCode
