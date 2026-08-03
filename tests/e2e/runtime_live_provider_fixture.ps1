# Runtime-supervised live-provider fixture E2E (Windows): starts a deterministic
# local OpenAI-compatible HTTP fixture, configures the supervised native harness
# to use the live `local` provider adapter through the curated
# `AGENTMOD_PROVIDER_*` environment channel, and drives a real runtime session
# through the full service -> runtime -> harness -> live-provider path.
# Requires no network and no credentials.
$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $repository

$python = Get-Command python -ErrorAction SilentlyContinue
if (-not $python) {
    Write-Error "python is required for the local HTTP fixture"
}

cargo build --locked -p agentmod-runtime -p agentmod-harness -p agentmod-cli -p agentmod-scheduler
if ($LASTEXITCODE -ne 0) { exit 1 }

$runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
$harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
$scheduler = (Resolve-Path "target\debug\agentmod-scheduler.exe").Path
$cli = (Resolve-Path "target\debug\agentmod.exe").Path
$runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
    "agentmod-live-fixture-" + [guid]::NewGuid().ToString("N")
)
New-Item -ItemType Directory -Path $runRoot | Out-Null
$workspace = Join-Path $runRoot "workspace"
$stylesUser = Join-Path $runRoot "styles"
New-Item -ItemType Directory -Path $workspace | Out-Null
New-Item -ItemType Directory -Path (Join-Path $stylesUser "user") | Out-Null
Copy-Item -Path (Join-Path $repository "tests\fixtures\styles\live-provider-chat.toml") -Destination (Join-Path $stylesUser "user")

$env:AGENTMOD_RUNTIME_ENDPOINT = (
    "\\.\pipe\agentmod-live-fixture-" + [guid]::NewGuid().ToString("N")
)
$env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
    "0123456789abcdef0123456789abcdef0123456789abcdef"
)
$env:AGENTMOD_HARNESS_PROGRAM = $harness
$env:AGENTMOD_SCHEDULER_PROGRAM = $scheduler
$env:AGENTMOD_SCHEDULER_ROOT = Join-Path $runRoot "scheduler"

# Deterministic local OpenAI-compatible fixture server.
$fixtureScript = Join-Path $runRoot "fixture_server.py"
$fixturePortFile = Join-Path $runRoot "fixture-port"
@'
import http.server, json, socketserver, sys, threading

PORT_FILE = sys.argv[1]
ready = threading.Event()

class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        self.rfile.read(length)
        body = json.dumps({
            "id": "chatcmpl-fixture",
            "object": "chat.completion.chunk",
            "model": "fixture-model",
            "choices": [{"index": 0, "delta": {"content": "runtime-live-fixture-ok"}, "finish_reason": None}],
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        chunk = b"data: " + body + b"\n\n"
        self.wfile.write(b"%x\r\n%s\r\n" % (len(chunk), chunk))
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()

    def log_message(self, *args):
        pass

class Server(socketserver.TCPServer):
    def server_bind(self):
        super().server_bind()
        with open(PORT_FILE, "w") as f:
            f.write(str(self.server_address[1]))
        ready.set()

with Server(("127.0.0.1", 0), Handler) as server:
    server.serve_forever()
'@ | Set-Content -Path $fixtureScript -Encoding ASCII

$fixture = Start-Process -FilePath $python.Source -ArgumentList `
    $fixtureScript, $fixturePortFile -WindowStyle Hidden -PassThru
$runtimeStderr = Join-Path $runRoot "runtime.stderr.log"
$daemon = $null

try {
    $fixturePort = $null
    for ($attempt = 0; $attempt -lt 50; $attempt++) {
        if (Test-Path $fixturePortFile) {
            $fixturePort = (Get-Content $fixturePortFile -Raw).Trim()
            if ($fixturePort) { break }
        }
        Start-Sleep -Milliseconds 100
    }
    if (-not $fixturePort) {
        Write-Error "fixture server did not start"
    }

    # Configure the live `local` provider for the supervised harness process.
    $env:AGENTMOD_PROVIDER_LOCAL_BASE_URL = "http://127.0.0.1:$fixturePort/v1"
    $env:AGENTMOD_PROVIDER_LOCAL_MODELS = "fixture-model"

    $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
        -WorkingDirectory $runRoot -WindowStyle Hidden `
        -RedirectStandardError $runtimeStderr -PassThru

    $ready = $false
    for ($attempt = 0; $attempt -lt 100; $attempt++) {
        $doctor = & $cli doctor --json 2>$null
        if ($LASTEXITCODE -eq 0) {
            $ready = $true
            break
        }
        Start-Sleep -Milliseconds 100
    }
    if (-not $ready) {
        Get-Content $runtimeStderr -ErrorAction SilentlyContinue
        Write-Error "runtime did not become ready"
    }

    $created = & $cli session create --workspace $workspace `
        --style live-provider-chat --harness native `
        --memory none --compaction none `
        --max-iterations 1 --max-steps 20 --max-tokens 10000 `
        --max-cost-micros 1000000 --max-duration-ms 60000 --json
    $sessionId = ($created | ConvertFrom-Json).session_id

    $result = & $cli run "hello from live fixture" --session $sessionId `
        --provider local --model fixture-model `
        --option "base_url=http://127.0.0.1:$fixturePort/v1" --json
    $text = -join (($result | ConvertFrom-Json).events |
        Where-Object { $_.event -eq "text" } |
        ForEach-Object { $_.text })

    if ($text -notlike "*runtime-live-fixture-ok*") {
        Write-Output "result: $result"
        Get-Content $runtimeStderr -ErrorAction SilentlyContinue
        Write-Error "runtime-supervised live fixture did not produce provider output"
    }

    Write-Output "runtime-supervised live-provider fixture E2E passed: $text"
}
finally {
    if ($daemon -and -not $daemon.HasExited) {
        Stop-Process -Id $daemon.Id -Force -ErrorAction SilentlyContinue
    }
    if ($fixture -and -not $fixture.HasExited) {
        Stop-Process -Id $fixture.Id -Force -ErrorAction SilentlyContinue
    }
    Remove-Item Env:AGENTMOD_RUNTIME_ENDPOINT -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_PROVIDER_LOCAL_BASE_URL -ErrorAction SilentlyContinue
    Remove-Item Env:AGENTMOD_PROVIDER_LOCAL_MODELS -ErrorAction SilentlyContinue
    Remove-Item -Recurse -Force $runRoot -ErrorAction SilentlyContinue
}
