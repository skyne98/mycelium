//! What survives a crash: the raft log and (term, vote).
//!
//! The log is one append-only file of records `[len u32][crc32 u32][term u64][cmd]`.
//! A torn or corrupt tail is cut off on open; everything before it is intact.
//! (term, vote) is replaced atomically: write a temp file, fsync, rename, fsync dir.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const HEADER: usize = 8;

pub(crate) struct Wal {
    file: Arc<File>,
    dir: PathBuf,
    /// Byte offset where entry `i + 1` starts.
    offsets: Vec<u64>,
    end: u64,
    /// Bumped on every truncation, so a sync that raced one is not trusted.
    pub generation: u64,
}

#[derive(Default)]
pub(crate) struct Loaded {
    pub entries: Vec<(u64, Vec<u8>)>,
    pub term: u64,
    pub vote: Option<SocketAddr>,
}

impl Wal {
    pub fn open(dir: &Path) -> io::Result<(Wal, Loaded)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("log");
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;

        let mut entries = Vec::new();
        let mut offsets = Vec::new();
        let mut at = 0usize;
        while bytes.len() >= at + HEADER {
            let len = u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
            let crc = u32::from_be_bytes(bytes[at + 4..at + 8].try_into().unwrap());
            let body = at + HEADER;
            if len < 8 || bytes.len() < body + len || crc32(&bytes[body..body + len]) != crc {
                break;
            }
            let term = u64::from_be_bytes(bytes[body..body + 8].try_into().unwrap());
            entries.push((term, bytes[body + 8..body + len].to_vec()));
            offsets.push(at as u64);
            at = body + len;
        }
        if at != bytes.len() {
            // Torn tail from a crash mid-write: nothing after it was acknowledged.
            file.set_len(at as u64)?;
            file.sync_all()?;
        }
        let (term, vote) = read_state(&dir.join("state"));
        Ok((
            Wal {
                file: Arc::new(file),
                dir: dir.to_path_buf(),
                offsets,
                end: at as u64,
                generation: 0,
            },
            Loaded {
                entries,
                term,
                vote,
            },
        ))
    }

    /// Buffered write; durable only after `sync`.
    pub fn append(&mut self, term: u64, cmd: &[u8]) -> io::Result<()> {
        let len = 8 + cmd.len();
        let mut rec = Vec::with_capacity(HEADER + len);
        rec.extend_from_slice(&(len as u32).to_be_bytes());
        rec.extend_from_slice(&[0; 4]);
        rec.extend_from_slice(&term.to_be_bytes());
        rec.extend_from_slice(cmd);
        let crc = crc32(&rec[HEADER..]);
        rec[4..8].copy_from_slice(&crc.to_be_bytes());
        (&*self.file).write_all(&rec)?;
        self.offsets.push(self.end);
        self.end += rec.len() as u64;
        Ok(())
    }

    /// Drop entry `from` (1-based) and everything after it.
    pub fn truncate(&mut self, from: u64) -> io::Result<()> {
        let keep = (from - 1) as usize;
        if keep >= self.offsets.len() {
            return Ok(());
        }
        self.end = self.offsets[keep];
        self.offsets.truncate(keep);
        self.file.set_len(self.end)?;
        self.generation += 1;
        Ok(())
    }

    pub fn file(&self) -> Arc<File> {
        self.file.clone()
    }

    pub fn state_job(
        &self,
        term: u64,
        vote: Option<SocketAddr>,
    ) -> impl FnOnce() -> io::Result<()> {
        let dir = self.dir.clone();
        move || write_state(&dir, term, vote)
    }
}

fn read_state(path: &Path) -> (u64, Option<SocketAddr>) {
    let Ok(text) = std::fs::read_to_string(path) else {
        return (0, None);
    };
    let mut parts = text.split_whitespace();
    let term = parts.next().and_then(|t| t.parse().ok()).unwrap_or(0);
    let vote = parts.next().and_then(|a| a.parse().ok());
    (term, vote)
}

fn write_state(dir: &Path, term: u64, vote: Option<SocketAddr>) -> io::Result<()> {
    let body = format!(
        "{term} {}\n",
        vote.map(|a| a.to_string()).unwrap_or_else(|| "-".into())
    );
    write_atomic(dir, "state", body.as_bytes())
}

/// Temp file, fsync, rename, fsync the directory: all or nothing.
pub(crate) fn write_atomic(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    let mut f = File::create(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    std::fs::rename(&tmp, dir.join(name))?;
    File::open(dir)?.sync_all()
}

const CRC_TABLE: [u32; 256] = {
    let mut t = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0xEDB8_8320 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        t[i] = c;
        i += 1;
    }
    t
};

fn crc32(data: &[u8]) -> u32 {
    let mut c = !0u32;
    for &b in data {
        c = CRC_TABLE[((c ^ b as u32) & 0xff) as usize] ^ (c >> 8);
    }
    !c
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("mycelium-wal-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn crc_matches_the_standard_check_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn entries_state_and_truncation_survive_reopen() {
        let d = dir("reopen");
        let (mut w, l) = Wal::open(&d).unwrap();
        assert!(l.entries.is_empty() && l.term == 0 && l.vote.is_none());
        for i in 1..=5u64 {
            w.append(i, format!("cmd{i}").as_bytes()).unwrap();
        }
        w.truncate(4).unwrap();
        w.append(9, b"new4").unwrap();
        w.state_job(7, Some("127.0.0.1:9".parse().unwrap()))().unwrap();
        drop(w);

        let (_, l) = Wal::open(&d).unwrap();
        let got: Vec<(u64, &[u8])> = l.entries.iter().map(|(t, c)| (*t, c.as_slice())).collect();
        assert_eq!(
            got,
            vec![(1, &b"cmd1"[..]), (2, b"cmd2"), (3, b"cmd3"), (9, b"new4")]
        );
        assert_eq!(l.term, 7);
        assert_eq!(l.vote, Some("127.0.0.1:9".parse().unwrap()));
    }

    #[test]
    fn a_torn_tail_is_cut_not_fatal() {
        let d = dir("torn");
        let (mut w, _) = Wal::open(&d).unwrap();
        w.append(1, b"whole").unwrap();
        w.append(1, b"torn-entry").unwrap();
        drop(w);
        let path = d.join("log");
        let len = std::fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(len - 3)
            .unwrap();

        let (mut w, l) = Wal::open(&d).unwrap();
        assert_eq!(l.entries, vec![(1, b"whole".to_vec())]);
        w.append(2, b"after").unwrap();
        drop(w);
        let (_, l) = Wal::open(&d).unwrap();
        assert_eq!(
            l.entries.len(),
            2,
            "appends after a cut tail must be readable"
        );
    }
}
