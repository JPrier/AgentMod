// Opt-in cross-origin isolation for the Linux sandbox (CheerpX needs
// SharedArrayBuffer). Turning it on registers ../coi-sw.js, which adds the
// COOP/COEP headers static hosting cannot, and reloads the page once.

const FLAG = 'agentmod.isolation';
const TRIED = 'agentmod.isolation-reloaded';
const AFTER = 'agentmod.after-reload';
const SW = 'coi-sw.js';

export const isolated = () => !!globalThis.crossOriginIsolated;
export const isolationWanted = () => localStorage.getItem(FLAG) === 'on';
export const isolationSupported = () => 'serviceWorker' in navigator && globalThis.isSecureContext;

async function register() {
  await navigator.serviceWorker.register(SW);
  await navigator.serviceWorker.ready;
}

/**
 * Call first at page start. If isolation is wanted but not in effect, make sure
 * the service worker is installed and reload (once per tab, so a browser that
 * cannot isolate does not loop). Returns true when a reload is under way.
 */
export async function ensureIsolation() {
  if (isolated()) {
    sessionStorage.removeItem(TRIED);
    return false;
  }
  if (!isolationWanted() || !isolationSupported() || sessionStorage.getItem(TRIED)) return false;
  try {
    await register();
  } catch (e) {
    console.warn('agentmod: could not register the isolation service worker', e);
    return false;
  }
  sessionStorage.setItem(TRIED, '1');
  location.reload();
  return true;
}

/** Turn isolation on and reload; `after` names a session definition to open next. */
export async function enableIsolation(after) {
  localStorage.setItem(FLAG, 'on');
  sessionStorage.removeItem(TRIED);
  if (after) sessionStorage.setItem(AFTER, after);
  await register();
  sessionStorage.setItem(TRIED, '1');
  location.reload();
}

/** Turn isolation off: unregister the service worker and reload. */
export async function disableIsolation() {
  localStorage.removeItem(FLAG);
  for (const r of await navigator.serviceWorker.getRegistrations()) {
    if ([r.active, r.waiting, r.installing].some((w) => w?.scriptURL.endsWith(`/${SW}`))) await r.unregister();
  }
  location.reload();
}

/** The definition to open after an isolation reload, if any (read once). */
export function takeAfterReload() {
  const d = sessionStorage.getItem(AFTER);
  sessionStorage.removeItem(AFTER);
  return d;
}
