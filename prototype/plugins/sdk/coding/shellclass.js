// Coarse, conservative classification of shell command lines.
//
// Used for two decisions: whether a command needs a workspace checkpoint before
// it runs (anything not provably read-only gets one), and which policy rules
// apply (network, destructive, publishing). It is a heuristic over the command
// text, never a security boundary: containment is the execution target's job.
// Unknown commands are treated as mutating.

const READ_ONLY = new Set([
  'ls', 'cat', 'head', 'tail', 'less', 'more', 'grep', 'egrep', 'fgrep', 'rg', 'ag', 'wc', 'pwd', 'echo', 'printf',
  'which', 'type', 'file', 'stat', 'du', 'df', 'tree', 'sort', 'uniq', 'cut', 'tr', 'nl', 'column', 'diff', 'cmp',
  'jq', 'yq', 'env', 'printenv', 'date', 'uname', 'whoami', 'id', 'hostname', 'true', 'false', 'test', '[',
  'basename', 'dirname', 'realpath', 'readlink', 'md5sum', 'sha1sum', 'sha256sum', 'shasum', 'xxd', 'od', 'hexdump',
  'strings', 'ps', 'pgrep', 'uptime', 'free', 'nproc', 'lscpu', 'command', 'man', 'help', 'seq', 'expr', 'sleep',
]);

const GIT_READ_ONLY = new Set(['status', 'log', 'diff', 'show', 'blame', 'ls-files', 'ls-tree', 'rev-parse', 'describe', 'shortlog', 'grep', 'cat-file', 'reflog', 'whatchanged', 'config', 'remote', 'branch', 'tag', 'stash']);
const GIT_NETWORK = new Set(['clone', 'fetch', 'pull', 'push', 'ls-remote', 'submodule']);
const NETWORK_TOOLS = new Set(['curl', 'wget', 'ssh', 'scp', 'sftp', 'rsync', 'nc', 'ncat', 'telnet', 'ftp', 'http', 'httpie', 'aria2c']);
const INSTALL = {
  npm: new Set(['install', 'i', 'ci', 'add', 'update', 'publish', 'exec', 'x']),
  npx: null,
  pnpm: new Set(['install', 'i', 'add', 'update', 'publish', 'dlx']),
  yarn: new Set(['install', 'add', 'upgrade', 'publish', 'dlx']),
  pip: new Set(['install', 'download']),
  pip3: new Set(['install', 'download']),
  cargo: new Set(['install', 'add', 'fetch', 'publish', 'update']),
  go: new Set(['get', 'install', 'mod']),
  apt: null,
  'apt-get': null,
  brew: null,
  gem: new Set(['install', 'push']),
  docker: new Set(['pull', 'push', 'run', 'build', 'login']),
  gh: null,
};
const PUBLISH = [/^git\s+push\b/, /^(npm|pnpm|yarn)\s+publish\b/, /^cargo\s+publish\b/, /^gem\s+push\b/, /^docker\s+push\b/, /^gh\s+(pr|release|repo)\s+(create|merge|delete|edit)\b/];
const DESTRUCTIVE = [
  /^rm\s+(-[a-zA-Z]*r[a-zA-Z]*f|-[a-zA-Z]*f[a-zA-Z]*r|--recursive\s+--force|--force\s+--recursive)\b/,
  /^git\s+reset\s+--hard\b/,
  /^git\s+clean\s+-[a-zA-Z]*f/,
  /^git\s+push\s+.*(--force|-f\b)/,
  /^git\s+(checkout|restore)\s+(--\s+)?\.$/,
  /^(dd|mkfs(\.\w+)?|shred|wipefs)\b/,
  /^(chmod|chown)\s+-R\b/,
  /^(shutdown|reboot|halt|poweroff)\b/,
  /^kill(all)?\s+-9\s+(-1|1)\b/,
];

const WRAPPERS = new Set(['sudo', 'time', 'nice', 'nohup', 'env', 'command', 'exec', 'xargs', 'timeout', 'stdbuf']);

/** Split a command line into simple-command segments (outside quotes). */
export function segments(line) {
  const out = [];
  let cur = '';
  let q = null;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (q) {
      cur += c;
      if (c === q && line[i - 1] !== '\\') q = null;
      continue;
    }
    if (c === '"' || c === "'") { q = c; cur += c; continue; }
    if (c === ';' || c === '\n' || c === '|' || (c === '&' && line[i + 1] === '&')) {
      if (cur.trim()) out.push(cur.trim());
      cur = '';
      if ((c === '|' && line[i + 1] === '|') || (c === '&' && line[i + 1] === '&')) i++;
      continue;
    }
    if (c === '&' && line[i - 1] !== '>' && line[i + 1] !== '>') {
      if (cur.trim()) out.push(cur.trim());
      cur = '';
      continue;
    }
    cur += c;
  }
  if (cur.trim()) out.push(cur.trim());
  return out;
}

function words(seg) {
  return (seg.match(/"[^"]*"|'[^']*'|\S+/g) || []).map((w) => w.replace(/^["']|["']$/g, ''));
}

/** Does the segment redirect output into a file (not /dev/null, not fd dup)? */
function writesFile(seg) {
  const unq = seg.replace(/"[^"]*"|'[^']*'/g, '""');
  const re = /(\d?>>?|&>)\s*([^\s&|;]+)/g;
  let m;
  while ((m = re.exec(unq))) {
    const target = m[2];
    if (target === '/dev/null' || /^&\d$/.test(target) || target.startsWith('&')) continue;
    return true;
  }
  return false;
}

/**
 * Classify a command line.
 * @returns {{ readOnly: boolean, network: boolean, destructive: boolean, publish: boolean, reasons: string[] }}
 */
export function classifyCommand(line) {
  const text = String(line || '').trim();
  const r = { readOnly: true, network: false, destructive: false, publish: false, reasons: [] };
  const mut = (why) => { r.readOnly = false; r.reasons.push(why); };
  if (!text) return r;
  if (/\$\(|`|<\(|>\(/.test(text)) mut('command substitution');
  for (const seg of segments(text)) {
    const w = words(seg);
    while (w.length && (/^[A-Za-z_][A-Za-z0-9_]*=/.test(w[0]) || WRAPPERS.has(w[0]) || /^-/.test(w[0]))) w.shift();
    if (!w.length) continue;
    const cmd = w[0].split('/').pop();
    const sub = w[1] || '';
    const normalized = [cmd, ...w.slice(1)].join(' ');
    if (writesFile(seg)) mut(`redirects output into a file (${cmd})`);
    if (DESTRUCTIVE.some((re) => re.test(normalized))) {
      r.destructive = true;
      r.reasons.push(`destructive: ${normalized.slice(0, 60)}`);
    }
    if (PUBLISH.some((re) => re.test(normalized))) {
      r.publish = true;
      r.network = true;
      r.reasons.push(`publishes: ${normalized.slice(0, 60)}`);
    }
    if (NETWORK_TOOLS.has(cmd)) {
      r.network = true;
      r.reasons.push(`network tool: ${cmd}`);
    }
    if (cmd === 'git') {
      if (GIT_NETWORK.has(sub)) {
        r.network = true;
        r.reasons.push(`git ${sub} uses the network`);
      }
      const ro = GIT_READ_ONLY.has(sub) && !(sub === 'branch' && w.length > 2 && !w[2].startsWith('-')) && !(sub === 'stash' && w[2] && w[2] !== 'list' && w[2] !== 'show') && !(sub === 'config' && w.length > 3) && !(sub === 'tag' && w.length > 2 && !w[2].startsWith('-l'));
      if (!ro) mut(`git ${sub || ''}`.trim());
      continue;
    }
    if (cmd in INSTALL) {
      const subs = INSTALL[cmd];
      if (!subs || subs.has(sub)) {
        r.network = true;
        r.reasons.push(`package manager: ${cmd} ${sub}`.trim());
      }
      mut(`${cmd} ${sub}`.trim());
      continue;
    }
    if (cmd === 'find') {
      if (w.some((x) => x === '-delete' || x === '-exec' || x === '-execdir' || x === '-ok' || x === '-fprint')) mut('find with an action');
      continue;
    }
    if (cmd === 'sed' || cmd === 'perl') {
      if (w.some((x) => /^-i/.test(x) || x === '--in-place')) mut(`${cmd} -i`);
      continue;
    }
    if (cmd === 'sort' && w.some((x) => x === '-o' || x.startsWith('--output'))) { mut('sort -o'); continue; }
    if (cmd === 'tee') { mut('tee writes files'); continue; }
    if (cmd === 'awk' && /inplace/.test(seg)) { mut('awk inplace'); continue; }
    if (cmd === 'cd' || cmd === 'export' || cmd === 'set' || cmd === 'source' || cmd === '.') {
      if (cmd === 'source' || cmd === '.') mut(`${cmd} runs a script`);
      continue;
    }
    if (!READ_ONLY.has(cmd)) mut(cmd);
  }
  return r;
}
