# linux-sandbox — agents that code in the browser

`linux-sandbox` gives an agent a Linux machine to code on **inside the user's browser tab**.
It runs an x86 Debian VM with [CheerpX](https://cheerpx.io) in the plugin's Web Worker, so
compiling, testing, and running code use the user's CPU and RAM; nothing executes on a server.
The model still comes from the model plugin (OpenRouter), and every step is an event in the
session log like any other tool call.

## Try it

1. Open the in-browser runtime (GitHub Pages, or `dist/` from `scripts/build-site.sh`).
2. In the sidebar, pick the **`coder`** definition and press **New session**.
3. The session shows **Enable the Linux sandbox**. CheerpX needs `SharedArrayBuffer`, which
   browsers only expose to cross-origin-isolated pages; static hosting cannot send those
   headers, so enabling installs `ui/coi-sw.js` (a 20-line service worker that adds them) and
   reloads once. In-browser sessions live in memory, so the current one ends on that reload.
   **Turn off isolation** undoes it.
4. Ask for something: *"Write a C program that prints the first 20 primes, compile it with gcc,
   and run it."* The first tool call boots the VM (the disk image streams on demand; later boots
   reuse the IndexedDB cache).

CI exercises Chrome; Edge and Firefox support the same isolation path. Safari is untested. When
the page cannot be isolated, the session says why instead of failing silently.

## Tools

All execution targets answer the same tools (`plugins/sdk/workspace-tools.js`):

| Tool | Does |
| --- | --- |
| `run` | bash command in the workspace; exit code, stdout and stderr returned separately; timeout (default 120 s, max 900 s); stdin is empty |
| `read_file` | text file, paged by line (400 lines per call) |
| `write_file` | create or overwrite; emits a `workspace-change` event with a `diff` UI hint |
| `edit_file` | exact-text replace (must match once unless `replace_all`); emits a diff |
| `list_files` | directory tree (skips `.git`, `node_modules`) |
| `import_repo` | copy a public GitHub repository into the workspace and `git init` + commit it |

Paths are confined to `/workspace` for the file tools and `run`'s `cwd`. A command can still
touch anything in the VM — the VM is the sandbox.

## How it fits the plugin system

Nothing in the kernel, the wire protocol, or the browser host knows about coding. A tool call is
an event; a plugin that answers it is a tool. `linux-sandbox` is an ordinary async tool plugin:

```text
user-message ─► chat-context ─► model-request ─► openrouter-model ─► model-response
                                                                         │
             ┌───────────────────────────────────────────────────────────┘
             ▼
        tool-call ──► approval-gate (blocking; vetoes guarded tools)
             │
             └──► linux-sandbox (async) ──► CheerpX VM in this worker
                        │  publishes, as outputs of its invocation:
                        ├─ workspace-status   booting / ready   (progress hint)
                        ├─ workspace-change   path + unified diff (diff hint)
                        └─ tool-result        ──► chat-context ──► model-request …
```

### What it depends on

| Plugin | Kind of dependency | Why |
| --- | --- | --- |
| `chat-context` (or anything that emits `tool-call` / consumes `tool-result`) | **compile-time** — the compiler matches `tool-call` demand (`call_id`, `name`, `args`) to its supply | drives the model ↔ tool loop and folds results into `messages` |
| a model plugin (`openrouter-model`, `openai-model`) | convention — reads the `tools` and `system` context slots via `sdk/openai-compat.js` | the sandbox contributes its tool specs and a system note describing the VM |
| `approval-gate` | optional policy, by config | in the browser the VM is the boundary, so `coder` guards nothing extra by default; add `run` to `require` to approve every command |
| `web-ui` | optional rendering | renders the `tool`, `progress`, and `diff` hints; any frontend falls back to raw payloads |
| core lifecycle events | `session-started`, `config-applied` | offer tools and the system note |

### Why its own plugin, and why one plugin

- **Not part of an existing plugin.** No bundled plugin owns an execution environment:
  `chat-context` is a deliberately stateless convention, the `tool-*` plugins are single pure
  tools, and the model plugins only talk to providers. A VM is long-lived, stateful, and
  effectful, which is exactly what the plugin boundary (a supervised process/worker with
  restart tolerance) is for.
- **One plugin, not separate process / filesystem / git plugins.** The research note mirrors the
  root implementation's capability hosts. In the plugin model each plugin is its own worker, and
  the VM is a single stateful resource that lives in exactly one of them. Splitting files,
  processes, and Git across plugins would need workers to share one VM, which the model does not
  provide (and should not: cross-plugin effects go through events). Git works through `run`.
- **The execution target is config, not code.** The tool semantics live once in
  `sdk/workspace-tools.js`; `linux-sandbox` (CheerpX) and `local-workspace` (a directory on the
  machine running the native runtime) are thin plugins over it, exactly like `openrouter-model`
  and `openai-model` share `sdk/openai-compat.js`. A definition lists the target it wants, so the
  research note's `ExecutionTarget` routing *is* the session definition. A future remote
  container or SSH target is another plugin answering the same tool names.
- **Host scoping.** `linux-sandbox` has a `module` and no `command` (browser only);
  `local-workspace` has a `command` and no `module` (native only). Each host disables what it
  cannot run (the native runtime now does this for worker-only plugins, mirroring the browser),
  and the compiler reports the skip as `disabled-plugin` info.

## Inside the target

`target.js` is deliberately dumb: it boots the VM and moves bytes and commands in and out.

| Mount | Device | Persistence |
| --- | --- | --- |
| `/` | the read-only Debian image (WebVM's public image, streamed over `wss://disks.webvm.io`) + an IndexedDB overlay | disposable: OS changes only |
| `/workspace` | its own IndexedDB store, `agentmod-workspace-<workspace>` | the work; survives reloads and OS resets |
| `/agentmod-in` | `DataDevice` | page → guest bytes (file writes, tar batches) |
| `/agentmod-out` | IndexedDB scratch | guest → page bytes (stdout, stderr, file reads) |

- **Commands** run as `cx.run('/bin/bash', ['-c', wrapper, marker, cwd, timeout, command])`. The
  command travels as an argument (no quoting), runs under `timeout -k 5`, and its stdout and
  stderr are redirected into `/agentmod-out`, so they come back exactly and separately, with the
  real exit code. The console is not part of the protocol.
- **Writes** go through `DataDevice` then one `cp`; many files (repository import) go as one tar
  archive and one `tar -x`, not one process per file.
- **Operations are serialized** per VM. A cancelled call returns at once; the running command is
  interrupted with `pkill` and bounded by its timeout.
- **One VM per workspace across tabs** (Web Locks), because the IndexedDB disks are not safe to
  share between two live VMs.
- **Repositories are imported outside the guest** (GitHub API + raw files, from the worker), so
  the VM needs no network. Public repositories only; unauthenticated GitHub API limits apply
  (three API calls per import).

## Security model

The browser is an execution worker, not a trust anchor. The runtime still records what was
requested and what came back; a tampered or broken sandbox can only produce wrong tool results,
which are attributed to `linux-sandbox` in the log.

- No network inside the VM (CheerpX networking is not configured).
- No secrets in the guest: the OpenRouter key stays in the model plugin's worker.
- The guest sees only its own disks; there is no host filesystem access.
- `local-workspace` is **not** a sandbox. It is in no default definition; opt in deliberately and
  guard `run` with `approval-gate`.

## Feasibility gate and evidence

| Check | Where |
| --- | --- |
| tool semantics: path confinement, edits, diffs (randomized, verified with `patch(1)`), truncation, timeouts, cancellation, repo import | `tests/workspace-tools.test.mjs` |
| the CheerpX target's boot sequence, wrapper, byte channels, tar batches, serialization — against a fake CheerpX | `tests/linux-sandbox-target.test.mjs` |
| the plugin file the browser loads, over the real wire protocol | `tests/linux-sandbox-plugin.test.mjs` |
| the shared tool layer through a real native session and kernel (`local-workspace`) | `tests/e2e.sh` step 8 |
| **real CheerpX in headless Chrome**: boot, exit codes, stderr, write/edit, `gcc`, `python3`, `git`, timeouts, persistence across reload | `tests/sandbox-browser.mjs` (CI step; reports to the job summary and `sandbox-report.json`) |

The last check needs Leaning Technologies' CDN and the WebVM disk server, so CI runs it without
blocking the build. **Its first run is the feasibility gate**; until it passes, treat the
browser sandbox as unproven. The specific risks it settles:

1. **CheerpX inside a Web Worker.** Plugins run in module workers. WebVM runs CheerpX on the main
   thread; CheerpX's API (`setCustomConsole`, `run`, devices) needs no DOM, and everything it uses
   (WebAssembly, `SharedArrayBuffer`, IndexedDB, WebSocket, nested workers) exists in dedicated
   workers, but this is not documented as supported. If it fails, the fallback keeps the plugin
   and moves only the VM: the browser host would own a page-side VM and hand the plugin worker a
   `MessagePort` to it (the target interface is already that narrow).
2. **IDB `dir` mounts and `readFileAsBlob` visibility** of files the guest just wrote.
3. **`timeout`/`pkill` semantics** under CheerpX's process model.
4. **The public disk image URL** (`image` in config) staying available; self-hosting an image is
   one config change (`image_type = "bytes"` with an HTTP-range-capable server).

## Limits and next steps

- 32-bit x86 only (CheerpX today), so toolchains are i686: the default Debian image has gcc,
  python3, and git; Rust and Node depend on the image. A purpose-built image (the research note's
  `agentmod-rust.ext2` family) is the next step for real Rust work, with `cargo vendor` for
  offline dependencies.
- No package downloads inside the VM yet. The planned path is a policy-controlled fetch
  capability (an event, so it is logged and can be gated), not open guest networking.
- No PTY, background processes, or LSP yet; `run` is foreground and non-interactive.
- One shared `/workspace` per browser (named by `workspace`); per-session worktrees would let
  parallel sub-agents work without collisions.
- No sandbox reset tool yet (`IDBDevice.reset()` on the root overlay); clearing site data resets
  everything.
- The browser runtime keeps session logs in memory; the workspace outlives them.

## Licensing

CheerpX is proprietary software by Leaning Technologies, loaded at run time from their CDN. It is
free for personal and open-source projects; other uses require their commercial licence. AgentMod
does not vendor it.
