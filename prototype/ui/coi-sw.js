// Cross-origin isolation for static hosting.
//
// The linux-sandbox plugin runs CheerpX, which needs SharedArrayBuffer, which
// browsers only expose to cross-origin-isolated pages (COOP + COEP headers).
// GitHub Pages cannot set response headers, so this service worker adds them to
// same-origin responses. It is opt-in: the page registers it only after the
// user enables the Linux sandbox (see runtime/isolation.js), and unregisters it
// when they turn it off. Cross-origin requests pass through untouched; under
// `require-corp` they must be CORS requests (fetch, module imports) or carry a
// Cross-Origin-Resource-Policy header.

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()));

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.cache === 'only-if-cached' && req.mode !== 'same-origin') return;
  if (new URL(req.url).origin !== self.location.origin) return;
  event.respondWith(
    fetch(req).then((res) => {
      if (res.status === 0 || res.type === 'opaque' || res.type === 'opaqueredirect') return res;
      const headers = new Headers(res.headers);
      headers.set('Cross-Origin-Opener-Policy', 'same-origin');
      headers.set('Cross-Origin-Embedder-Policy', 'require-corp');
      headers.set('Cross-Origin-Resource-Policy', 'same-origin');
      return new Response(res.body, { status: res.status, statusText: res.statusText, headers });
    }),
  );
});
