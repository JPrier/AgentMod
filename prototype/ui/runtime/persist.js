// Durable storage for the in-browser runtime (IndexedDB).
//
// The native runtime appends each session's records to a JSONL file and
// fsyncs before dispatching anything; this is the browser's equivalent. Every
// record is written in one IndexedDB transaction per effect batch, and the
// runtime dispatches invocations only after that transaction commits, so a
// reload resumes from history exactly like a native restart: stored
// compilations are installed, every session's log is replayed through a fresh
// kernel, and orphaned invocations are retried.
//
// Unavoidable differences from native storage (documented in
// docs/design/coding-harness.md): the browser may evict site data under
// storage pressure unless persistent storage is granted
// (navigator.storage.persist()); data is per browser profile and origin; and
// private windows may provide no IndexedDB at all, in which case the runtime
// falls back to memory and says so. Only one tab writes the store at a time
// (a Web Lock); another tab runs in memory.

const DB = 'agentmod-runtime';
const VERSION = 1;

function req(r) {
  return new Promise((resolve, reject) => {
    r.onsuccess = () => resolve(r.result);
    r.onerror = () => reject(r.error);
  });
}

function done(tx) {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error || new Error('transaction aborted'));
  });
}

/**
 * Open the store. Returns null when IndexedDB is unavailable.
 * @param {object} [o]
 * @param {IDBFactory} [o.idb]   injectable for tests
 * @param {string} [o.name]
 */
export async function openStore({ idb = globalThis.indexedDB, name = DB, locks = globalThis.navigator?.locks } = {}) {
  if (!idb) return null;
  // One writer per store: two tabs replaying and appending the same logs with
  // separate kernels would interleave sequences. The first tab holds a Web
  // Lock for its lifetime; a second tab runs in memory and says so.
  if (locks?.request) {
    const held = await new Promise((resolve) => {
      locks.request(`${name}:writer`, { ifAvailable: true }, (lock) => {
        resolve(!!lock);
        return lock ? new Promise(() => {}) : undefined;
      }).catch(() => resolve(true));
    });
    if (!held) return { blocked: 'another tab owns this browser\'s stored sessions' };
  }
  const open = idb.open(name, VERSION);
  open.onupgradeneeded = () => {
    const db = open.result;
    if (!db.objectStoreNames.contains('records')) db.createObjectStore('records', { keyPath: ['session_id', 'sequence'] });
    if (!db.objectStoreNames.contains('compilations')) db.createObjectStore('compilations', { keyPath: 'hash' });
    if (!db.objectStoreNames.contains('meta')) db.createObjectStore('meta');
  };
  let db;
  try {
    db = await req(open);
  } catch {
    return null;
  }
  let persisted = null;
  try {
    persisted = (await globalThis.navigator?.storage?.persist?.()) ?? null;
  } catch { /* not granted */ }

  return {
    persisted,
    /** Append records durably (one transaction). */
    async appendRecords(records) {
      if (!records.length) return;
      const tx = db.transaction('records', 'readwrite');
      const st = tx.objectStore('records');
      for (const r of records) st.put(r);
      await done(tx);
    },
    async putCompilation(c) {
      const tx = db.transaction('compilations', 'readwrite');
      tx.objectStore('compilations').put(c);
      await done(tx);
    },
    async setMeta(key, value) {
      const tx = db.transaction('meta', 'readwrite');
      tx.objectStore('meta').put(value, key);
      await done(tx);
    },
    async getMeta(key) {
      const tx = db.transaction('meta', 'readonly');
      return req(tx.objectStore('meta').get(key));
    },
    /** Every stored compilation and session log (records in sequence order). */
    async loadAll() {
      const tx = db.transaction(['records', 'compilations'], 'readonly');
      const [records, compilations] = await Promise.all([req(tx.objectStore('records').getAll()), req(tx.objectStore('compilations').getAll())]);
      const sessions = new Map();
      for (const r of records) {
        if (!sessions.has(r.session_id)) sessions.set(r.session_id, []);
        sessions.get(r.session_id).push(r);
      }
      for (const list of sessions.values()) list.sort((a, b) => a.sequence - b.sequence);
      return { compilations, sessions };
    },
    async clear() {
      const tx = db.transaction(['records', 'compilations', 'meta'], 'readwrite');
      for (const s of ['records', 'compilations', 'meta']) tx.objectStore(s).clear();
      await done(tx);
    },
    close() {
      db.close();
    },
  };
}
