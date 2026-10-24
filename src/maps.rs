//! Merged maps: one loro document per name. A write applies locally, is pushed
//! to every peer at once, and a pull every second repairs whatever a push
//! missed, so replicas converge after any partition without a coordinator.

use crate::transport::{parts, Blobs};
use crate::wal::write_atomic;
use crate::Result;
use loro::{ExportMode, LoroDoc, LoroMap, LoroResult, LoroValue, ValueOrContainer, VersionVector};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

pub(crate) struct Maps {
    inner: Mutex<Inner>,
    dir: Option<PathBuf>,
    /// One flush at a time: they share temp files, and a later snapshot must
    /// not be overwritten by an earlier one.
    flushing: Mutex<()>,
}

struct Inner {
    docs: HashMap<String, LoroDoc>,
    dirty: HashSet<String>,
    /// Random per process: loro needs every concurrent writer to be distinct.
    peer: u64,
}

impl Inner {
    fn doc(&mut self, name: &str) -> Result<&LoroDoc> {
        if !self.docs.contains_key(name) {
            let d = LoroDoc::new();
            d.set_peer_id(self.peer)?;
            self.docs.insert(name.to_string(), d);
        }
        Ok(&self.docs[name])
    }
}

fn values(doc: &LoroDoc) -> HashMap<String, Vec<u8>> {
    let mut out = HashMap::new();
    doc.get_map("m").for_each(|k, v| {
        if let Some(v) = value(v) {
            out.insert(k.to_string(), v);
        }
    });
    out
}

fn value(v: ValueOrContainer) -> Option<Vec<u8>> {
    match v {
        ValueOrContainer::Value(LoroValue::Binary(b)) => Some(b.to_vec()),
        _ => None,
    }
}

impl Maps {
    pub fn memory() -> Self {
        Self {
            inner: Mutex::new(Inner {
                docs: HashMap::new(),
                dirty: HashSet::new(),
                peer: crate::raft::random_u64() >> 1,
            }),
            dir: None,
            flushing: Mutex::new(()),
        }
    }

    /// Load every snapshot under `dir`; later flushes write there.
    pub fn open(dir: PathBuf) -> Result<Self> {
        let maps = Self {
            dir: Some(dir.clone()),
            ..Self::memory()
        };
        std::fs::create_dir_all(&dir)?;
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            let Some(name) = path
                .file_name()
                .and_then(|f| f.to_str())
                .filter(|f| !f.ends_with(".tmp"))
                .and_then(unhex)
                .and_then(|b| String::from_utf8(b).ok())
            else {
                continue;
            };
            let bytes = std::fs::read(&path)?;
            maps.lock().doc(&name)?.import(&bytes)?;
        }
        Ok(maps)
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Returns the update to push to peers.
    fn edit(&self, name: &str, f: impl FnOnce(&LoroMap) -> LoroResult<()>) -> Result<Vec<u8>> {
        let mut g = self.lock();
        let update = {
            let doc = g.doc(name)?;
            let before = doc.oplog_vv();
            f(&doc.get_map("m"))?;
            doc.commit();
            doc.export(ExportMode::updates(&before))?
        };
        g.dirty.insert(name.to_string());
        Ok(update)
    }

    pub fn set(&self, name: &str, key: &str, val: Vec<u8>) -> Result<Vec<u8>> {
        self.edit(name, |m| m.insert(key, LoroValue::from(val)))
    }

    pub fn remove(&self, name: &str, key: &str) -> Result<Vec<u8>> {
        self.edit(name, |m| m.delete(key))
    }

    pub fn get(&self, name: &str, key: &str) -> Option<Vec<u8>> {
        value(self.lock().docs.get(name)?.get_map("m").get(key)?)
    }

    pub fn entries(&self, name: &str) -> Vec<(String, Vec<u8>)> {
        let g = self.lock();
        let mut out = Vec::new();
        if let Some(doc) = g.docs.get(name) {
            doc.get_map("m").for_each(|k, v| {
                if let Some(v) = value(v) {
                    out.push((k.to_string(), v));
                }
            });
        }
        out.sort();
        out
    }

    /// Returns the keys whose value changed.
    // ponytail: diffs the whole map per import; use loro's event API if maps
    // get large.
    pub fn import(&self, name: &str, update: &[u8]) -> Result<Vec<String>> {
        let mut g = self.lock();
        let changed = {
            let doc = g.doc(name)?;
            let before = values(doc);
            doc.import(update)?;
            let after = values(doc);
            let mut keys: Vec<String> = before
                .keys()
                .chain(after.keys())
                .filter(|k| before.get(*k) != after.get(*k))
                .cloned()
                .collect();
            keys.sort();
            keys.dedup();
            keys
        };
        if !changed.is_empty() {
            g.dirty.insert(name.to_string());
        }
        Ok(changed)
    }

    /// `parts[name, version, ...]` for every map held here.
    pub fn versions(&self) -> Vec<u8> {
        let g = self.lock();
        let vvs: Vec<(String, Vec<u8>)> = g
            .docs
            .iter()
            .map(|(n, d)| (n.clone(), d.oplog_vv().encode()))
            .collect();
        let pieces: Vec<&[u8]> = vvs
            .iter()
            .flat_map(|(n, v)| [n.as_bytes(), v.as_slice()])
            .collect();
        parts(&pieces)
    }

    /// Everything the caller is missing, given its `versions()`.
    pub fn missing(&self, versions: &[u8]) -> Result<Vec<u8>> {
        let mut theirs = HashMap::new();
        let mut b = Blobs::new(versions);
        while !b.is_empty() {
            let name = b.text()?;
            theirs.insert(name, VersionVector::decode(b.take()?).unwrap_or_default());
        }
        let g = self.lock();
        let mut updates: Vec<(String, Vec<u8>)> = Vec::new();
        for (name, doc) in &g.docs {
            let empty = VersionVector::default();
            let vv = theirs.get(name).unwrap_or(&empty);
            if vv.includes_vv(&doc.oplog_vv()) {
                continue;
            }
            updates.push((name.clone(), doc.export(ExportMode::updates(vv))?));
        }
        let pieces: Vec<&[u8]> = updates
            .iter()
            .flat_map(|(n, u)| [n.as_bytes(), u.as_slice()])
            .collect();
        Ok(parts(&pieces))
    }

    #[cfg(test)]
    pub fn import_missing(&self, body: &[u8]) -> Result<()> {
        let mut b = Blobs::new(body);
        while !b.is_empty() {
            let name = b.text()?;
            self.import(&name, b.take()?)?;
        }
        Ok(())
    }

    /// Changes as (map name, keys) after importing a pull response.
    pub fn import_all(&self, body: &[u8]) -> Result<Vec<(String, Vec<String>)>> {
        let mut b = Blobs::new(body);
        let mut out = Vec::new();
        while !b.is_empty() {
            let name = b.text()?;
            let keys = self.import(&name, b.take()?)?;
            out.push((name, keys));
        }
        Ok(out)
    }

    /// Snapshot every changed map to disk.
    // ponytail: whole-document snapshots; switch to appending updates if maps
    // grow past a few megabytes.
    pub fn flush(&self) -> Result<()> {
        let Some(dir) = &self.dir else {
            return Ok(());
        };
        let _one = self.flushing.lock().unwrap_or_else(|e| e.into_inner());
        let snaps: Vec<(String, Vec<u8>)> = {
            let mut g = self.lock();
            let names: Vec<String> = g.dirty.drain().collect();
            let mut out = Vec::new();
            for n in names {
                if let Some(d) = g.docs.get(&n) {
                    out.push((n.clone(), d.export(ExportMode::Snapshot)?));
                }
            }
            out
        };
        for (i, (name, snap)) in snaps.iter().enumerate() {
            if let Err(e) = write_atomic(dir, &hex(name.as_bytes()), snap) {
                let mut g = self.lock();
                g.dirty.extend(snaps[i..].iter().map(|(n, _)| n.clone()));
                return Err(e.into());
            }
        }
        Ok(())
    }
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub(crate) fn unhex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_writers_converge_through_pulls() {
        let (a, b) = (Maps::memory(), Maps::memory());
        a.set("m", "k", b"from-a".to_vec()).unwrap();
        b.set("m", "k", b"from-b".to_vec()).unwrap();
        b.set("m", "only-b", b"x".to_vec()).unwrap();
        a.import_missing(&b.missing(&a.versions()).unwrap())
            .unwrap();
        b.import_missing(&a.missing(&b.versions()).unwrap())
            .unwrap();
        assert_eq!(a.entries("m"), b.entries("m"), "replicas converge");
        assert_eq!(a.get("m", "only-b"), Some(b"x".to_vec()));
        assert!(
            b.missing(&a.versions()).unwrap().is_empty(),
            "nothing left to send"
        );
    }

    #[test]
    fn removes_replicate_and_snapshots_reload() {
        let dir = std::env::temp_dir().join(format!("mycelium-maps-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let a = Maps::open(dir.clone()).unwrap();
        let b = Maps::memory();
        let up = a.set("m", "gone", b"1".to_vec()).unwrap();
        b.import("m", &up).unwrap();
        let up = a.remove("m", "gone").unwrap();
        b.import("m", &up).unwrap();
        a.set("m", "kept", b"2".to_vec()).unwrap();
        assert_eq!(b.get("m", "gone"), None, "the remove reached b");
        a.flush().unwrap();
        drop(a);
        let a = Maps::open(dir).unwrap();
        assert_eq!(a.entries("m"), vec![("kept".to_string(), b"2".to_vec())]);
    }
}
