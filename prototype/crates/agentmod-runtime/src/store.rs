//! Durable storage: one append-only JSONL log per session, content-addressed
//! spill files for oversized payloads, stored compilations, a runtime journal,
//! and a derived (rebuildable) session index.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use agentmod_core::compiler::Compilation;
use agentmod_core::record::{Body, Record};
use agentmod_core::types::{Cause, ContextOp};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// Index entry for one session (derived read model; loss is never data loss).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct IndexEntry {
    pub session_id: String,
    pub definition: String,
    pub created_at: u64,
    pub last_sequence: u64,
    pub updated_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    /// Has unfinished work (bounds the orphan scan after a crash).
    pub active: bool,
    #[serde(default)]
    pub state: String,
}

/// The whole index file.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Index {
    pub next_session: u64,
    pub sessions: BTreeMap<String, IndexEntry>,
}

/// Filesystem store.
pub struct Store {
    dir: PathBuf,
    spill_threshold: usize,
    writers: BTreeMap<String, File>,
    pub index: Index,
    index_dirty: bool,
}

impl Store {
    /// Open (creating) a data directory.
    ///
    /// # Errors
    /// I/O failures.
    pub fn open(dir: &Path, spill_threshold: usize) -> Result<Self, String> {
        for sub in ["sessions", "spill", "configs"] {
            fs::create_dir_all(dir.join(sub)).map_err(|e| format!("creating {}: {e}", dir.join(sub).display()))?;
        }
        let mut store = Store { dir: dir.to_path_buf(), spill_threshold, writers: BTreeMap::new(), index: Index::default(), index_dirty: false };
        match fs::read_to_string(dir.join("index.json")).ok().and_then(|t| serde_json::from_str::<Index>(&t).ok()) {
            Some(idx) if store.index_consistent(&idx) => store.index = idx,
            _ => store.rebuild_index()?,
        }
        Ok(store)
    }

    fn session_path(&self, sid: &str) -> PathBuf {
        self.dir.join("sessions").join(format!("{sid}.jsonl"))
    }

    fn index_consistent(&self, idx: &Index) -> bool {
        let Ok(rd) = fs::read_dir(self.dir.join("sessions")) else { return false };
        let on_disk = rd.filter_map(Result::ok).filter(|e| e.path().extension().is_some_and(|x| x == "jsonl")).count();
        on_disk == idx.sessions.len()
    }

    /// Rebuild the index from the logs (one pass over every record).
    ///
    /// # Errors
    /// I/O failures.
    pub fn rebuild_index(&mut self) -> Result<(), String> {
        let mut idx = Index { next_session: 1, sessions: BTreeMap::new() };
        let rd = fs::read_dir(self.dir.join("sessions")).map_err(|e| e.to_string())?;
        for entry in rd.filter_map(Result::ok) {
            let path = entry.path();
            if path.extension().is_none_or(|x| x != "jsonl") {
                continue;
            }
            let records = read_jsonl(&path, &self.dir)?;
            let mut e = IndexEntry::default();
            for r in &records {
                observe(&mut e, r);
            }
            // Unknown work state after an index loss: scan it on recovery.
            e.active = true;
            if let Some(n) = e.session_id.strip_prefix('s').and_then(|n| n.parse::<u64>().ok()) {
                idx.next_session = idx.next_session.max(n + 1);
            }
            if !e.session_id.is_empty() {
                idx.sessions.insert(e.session_id.clone(), e);
            }
        }
        self.index = idx;
        self.index_dirty = true;
        self.flush_index()
    }

    /// Append a record (spilling oversized values) and update the index entry.
    ///
    /// # Errors
    /// I/O failures.
    pub fn append(&mut self, r: &Record) -> Result<(), String> {
        let mut v = serde_json::to_value(r).map_err(|e| e.to_string())?;
        if let Some(obj) = v.as_object_mut() {
            for (k, field) in obj.iter_mut() {
                if !matches!(k.as_str(), "session_id" | "sequence" | "at" | "type") {
                    spill(field, 0, self.spill_threshold, &self.dir)?;
                }
            }
        }
        let line = serde_json::to_string(&v).map_err(|e| e.to_string())?;
        if !self.writers.contains_key(&r.session_id) {
            let f = OpenOptions::new().create(true).append(true).open(self.session_path(&r.session_id)).map_err(|e| e.to_string())?;
            self.writers.insert(r.session_id.clone(), f);
        }
        let f = self.writers.get_mut(&r.session_id).expect("writer");
        f.write_all(line.as_bytes()).and_then(|()| f.write_all(b"\n")).map_err(|e| e.to_string())?;
        let e = self.index.sessions.entry(r.session_id.clone()).or_default();
        observe(e, r);
        if let Some(n) = r.session_id.strip_prefix('s').and_then(|n| n.parse::<u64>().ok()) {
            self.index.next_session = self.index.next_session.max(n + 1);
        }
        self.index_dirty = true;
        Ok(())
    }

    /// Flush appended records to the OS and fsync (called before acting on them).
    ///
    /// # Errors
    /// I/O failures.
    pub fn sync(&mut self) -> Result<(), String> {
        for f in self.writers.values_mut() {
            f.flush().map_err(|e| e.to_string())?;
            f.sync_data().map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Close a session's writer (unloaded sessions hold no file handle).
    pub fn close(&mut self, sid: &str) {
        self.writers.remove(sid);
    }

    /// Read and rehydrate a session's records.
    ///
    /// # Errors
    /// I/O or decode failures.
    pub fn read(&self, sid: &str) -> Result<Vec<Record>, String> {
        read_jsonl(&self.session_path(sid), &self.dir)
    }

    pub fn set_active(&mut self, sid: &str, active: bool, state: &str) -> bool {
        let Some(e) = self.index.sessions.get_mut(sid) else { return false };
        let changed = e.active != active || e.state != state;
        if changed {
            let became_active = active && !e.active;
            e.active = active;
            state.clone_into(&mut e.state);
            self.index_dirty = true;
            return became_active;
        }
        false
    }

    /// Persist the index if it changed.
    ///
    /// # Errors
    /// I/O failures.
    pub fn flush_index(&mut self) -> Result<(), String> {
        if !self.index_dirty {
            return Ok(());
        }
        let tmp = self.dir.join("index.json.tmp");
        fs::write(&tmp, serde_json::to_vec_pretty(&self.index).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
        fs::rename(&tmp, self.dir.join("index.json")).map_err(|e| e.to_string())?;
        self.index_dirty = false;
        Ok(())
    }

    /// Store a compilation by hash (compile artifacts are reused at recovery).
    ///
    /// # Errors
    /// I/O failures.
    pub fn save_compilation(&self, c: &Compilation) -> Result<(), String> {
        let p = self.dir.join("configs").join(format!("{}.json", c.hash));
        if p.exists() {
            return Ok(());
        }
        fs::write(p, serde_json::to_vec(c).map_err(|e| e.to_string())?).map_err(|e| e.to_string())
    }

    /// Every stored compilation.
    #[must_use]
    pub fn compilations(&self) -> Vec<Compilation> {
        let Ok(rd) = fs::read_dir(self.dir.join("configs")) else { return Vec::new() };
        rd.filter_map(Result::ok)
            .filter_map(|e| fs::read(e.path()).ok())
            .filter_map(|b| serde_json::from_slice::<Compilation>(&b).ok())
            .collect()
    }

    /// Append a runtime-level journal entry (config applies, unrecordable refusals).
    pub fn journal(&self, at: u64, kind: &str, detail: &Value) {
        let line = json!({ "at": at, "kind": kind, "detail": detail }).to_string();
        if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(self.dir.join("journal.jsonl")) {
            let _ = writeln!(f, "{line}");
        }
    }
}

fn observe(e: &mut IndexEntry, r: &Record) {
    e.last_sequence = r.sequence;
    e.updated_at = r.at;
    match &r.body {
        Body::SessionCreated { definition, cause, .. } => {
            e.session_id.clone_from(&r.session_id);
            e.definition.clone_from(definition);
            e.created_at = r.at;
            if let Cause::Invocation { invocation_id } = cause {
                e.parent = invocation_id.split('/').next().map(str::to_owned);
            }
        }
        Body::InvocationCompleted { contributions, .. } => {
            for c in contributions {
                if let ContextOp::Add { slot, value } = c {
                    if slot == "title" {
                        e.title = value.as_str().map(str::to_owned);
                    }
                }
            }
        }
        _ => {}
    }
}

fn spill(v: &mut Value, depth: usize, threshold: usize, dir: &Path) -> Result<(), String> {
    let size = serde_json::to_vec(v).map(|b| b.len()).unwrap_or(0);
    if size <= threshold {
        return Ok(());
    }
    if depth < 3 {
        match v {
            Value::Object(m) => {
                for child in m.values_mut() {
                    spill(child, depth + 1, threshold, dir)?;
                }
            }
            Value::Array(a) => {
                for child in a.iter_mut() {
                    spill(child, depth + 1, threshold, dir)?;
                }
            }
            _ => {}
        }
        let size = serde_json::to_vec(v).map(|b| b.len()).unwrap_or(0);
        if size <= threshold {
            return Ok(());
        }
    }
    let bytes = serde_json::to_vec(v).map_err(|e| e.to_string())?;
    let hash = hex::encode(Sha256::digest(&bytes));
    let p = dir.join("spill").join(format!("{hash}.json"));
    if !p.exists() {
        fs::write(&p, &bytes).map_err(|e| e.to_string())?;
    }
    *v = json!({ "$spill": format!("sha256:{hash}"), "bytes": bytes.len() });
    Ok(())
}

fn rehydrate(v: &mut Value, dir: &Path) -> Result<(), String> {
    match v {
        Value::Object(m) => {
            if let Some(Value::String(h)) = m.get("$spill") {
                let hash = h.trim_start_matches("sha256:");
                let bytes = fs::read(dir.join("spill").join(format!("{hash}.json"))).map_err(|e| format!("spill {hash}: {e}"))?;
                let digest = hex::encode(Sha256::digest(&bytes));
                if digest != hash {
                    return Err(format!("spill {hash} failed integrity check"));
                }
                *v = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
                return rehydrate(v, dir);
            }
            for child in m.values_mut() {
                rehydrate(child, dir)?;
            }
        }
        Value::Array(a) => {
            for child in a.iter_mut() {
                rehydrate(child, dir)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn read_jsonl(path: &Path, dir: &Path) -> Result<Vec<Record>, String> {
    let f = File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut out = Vec::new();
    let lines: Vec<String> = BufReader::new(f).lines().collect::<Result<_, _>>().map_err(|e| e.to_string())?;
    let n = lines.len();
    for (i, line) in lines.into_iter().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let mut v: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            // A torn final line (crash mid-write) is discarded: it was never acted upon.
            Err(_) if i + 1 == n => break,
            Err(e) => return Err(format!("{}:{}: {e}", path.display(), i + 1)),
        };
        rehydrate(&mut v, dir)?;
        out.push(serde_json::from_value(v).map_err(|e| format!("{}:{}: {e}", path.display(), i + 1))?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentmod_core::record::Body;
    use agentmod_core::types::{Cause, EventRecord, Lane, Origin};

    fn rec(sid: &str, seq: u64, body: Body) -> Record {
        Record { session_id: sid.into(), sequence: seq, at: seq * 10, body }
    }

    #[test]
    fn spill_round_trip_and_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::open(dir.path(), 256).unwrap();
        let created = rec("s0001", 1, Body::SessionCreated { definition: "chat".into(), config: "c".into(), cause: Cause::Root { plugin: "ui".into() }, fork_of: None });
        let big = "x".repeat(5000);
        let ev = rec(
            "s0001",
            2,
            Body::EventAppended {
                event: EventRecord { event_id: "s0001/e2".into(), event_name: "user-message".into(), lane: Lane::Normal, cause: Cause::Root { plugin: "ui".into() }, origin: Origin::Core, depth: 0, payload: json!({ "text": big, "small": 1 }), ui: None },
            },
        );
        s.append(&created).unwrap();
        s.append(&ev).unwrap();
        s.sync().unwrap();
        let raw = fs::read_to_string(dir.path().join("sessions/s0001.jsonl")).unwrap();
        assert!(raw.contains("$spill"), "large payload spilled");
        assert!(raw.contains("\"small\":1"), "small siblings stay inline");
        assert!(raw.len() < 2000);
        // A torn final line is ignored.
        let mut f = OpenOptions::new().append(true).open(dir.path().join("sessions/s0001.jsonl")).unwrap();
        f.write_all(b"{\"session_id\":\"s0001\",\"seq").unwrap();
        let back = s.read("s0001").unwrap();
        assert_eq!(back, vec![created, ev]);
        assert_eq!(s.index.sessions["s0001"].definition, "chat");
        // Index rebuild from logs.
        s.flush_index().unwrap();
        fs::remove_file(dir.path().join("index.json")).unwrap();
        let s2 = Store::open(dir.path(), 256).unwrap();
        assert_eq!(s2.index.next_session, 2);
        assert!(s2.index.sessions["s0001"].active);
    }
}
