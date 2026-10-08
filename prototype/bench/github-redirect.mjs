// Preloaded into plugin processes for offline benchmarks
// (NODE_OPTIONS=--import=<this file>, AGENTMOD_GITHUB_MOCK=http://127.0.0.1:PORT):
// redirects fetches to GitHub to the mock's fake GitHub, so `import_repo`
// behaves identically for any build under test without code changes.
const mock = process.env.AGENTMOD_GITHUB_MOCK;
if (mock && typeof globalThis.fetch === 'function') {
  const real = globalThis.fetch;
  globalThis.fetch = (input, init) => {
    const url = typeof input === 'string' ? input : input?.url;
    if (url?.startsWith('https://api.github.com/')) return real(`${mock}/gh-api/${url.slice(23)}`, init);
    if (url?.startsWith('https://raw.githubusercontent.com/')) return real(`${mock}/gh-raw/${url.slice(34)}`, init);
    return real(input, init);
  };
}
