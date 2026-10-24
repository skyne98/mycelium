//! What survives a crash: the raft log, (term, vote), and the latest snapshot.
//!
//! `log`: a 16-byte header `[magic][base u64]`, then records
//! `[len u32][crc32 u32][term u64][cmd]` for entries `base+1, base+2, ...`.
//! A torn or corrupt tail is cut off on open; everything before it is intact.
//! `state` and `snapshot` are replaced atomically: temp file, fsync, rename,
//! fsync the directory.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const LOG_MAGIC: &[u8; 8] = b"MYCLOG01";
const SNAP_MAGIC: &[u8; 8] = b"MYCSNP01";
const LOG_HEADER: usize = 16;
const RECORD_HEADER: usize = 8;

pub(crate) struct Wal {
    file: Arc<File>,
    dir: PathBuf,
    /// Index of the entry before the first one in the file.
    base: u64,
    /// Byte offset of each entry in the file.
    offsets: Vec<u64>,
    end: u64,
    /// Bumped whenever the file is cut or replaced, so a sync that raced it
    /// is not trusted.
    pub generation: u64,
}

#[derive(Clone)]
pub(crate) struct Snapshot {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
}

#[derive(Default)]
pub(crate) struct Loaded {
    /// Index of the entry before `entries[0]`.
    pub base: u64,
    pub entries: Vec<(u64, Vec<u8>)>,
    pub term: u64,
    pub vote: Option<SocketAddr>,
    pub snapshot: Option<Snapshot>,
}

fn header(base: u64) -> [u8; LOG_HEADER] {
    let mut h = [0u8; LOG_HEADER];
    h[..8].copy_from_slice(LOG_MAGIC);
    h[8..].copy_from_slice(&base.to_be_bytes());
    h
}

fn record(term: u64, cmd: &[u8]) -> Vec<u8> {
    let len = 8 + cmd.len();
    let mut rec = Vec::with_capacity(RECORD_HEADER + len);
    rec.extend_from_slice(&(len as u32).to_be_bytes());
    rec.extend_from_slice(&[0; 4]);
    rec.extend_from_slice(&term.to_be_bytes());
    rec.extend_from_slice(cmd);
    let crc = crc32(&rec[RECORD_HEADER..]);
    rec[4..8].copy_from_slice(&crc.to_be_bytes());
    rec
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
        if bytes.is_empty() {
            file.write_all(&header(0))?;
            file.sync_all()?;
            bytes.extend_from_slice(&header(0));
        }
        if bytes.len() < LOG_HEADER || &bytes[..8] != LOG_MAGIC {
            return Err(io::Error::other(format!(
                "{} is not a mycelium log",
                path.display()
            )));
        }
        let base = u64::from_be_bytes(bytes[8..16].try_into().unwrap());

        let mut entries = Vec::new();
        let mut offsets = Vec::new();
        let mut at = LOG_HEADER;
        while bytes.len() >= at + RECORD_HEADER {
            let len = u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
            let crc = u32::from_be_bytes(bytes[at + 4..at + 8].try_into().unwrap());
            let body = at + RECORD_HEADER;
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
        let snapshot = read_snapshot(dir)?;
        Ok((
            Wal {
                file: Arc::new(file),
                dir: dir.to_path_buf(),
                base,
                offsets,
                end: at as u64,
                generation: 0,
            },
            Loaded {
                base,
                entries,
                term,
                vote,
                snapshot,
            },
        ))
    }

    /// Buffered write; durable only after a sync.
    pub fn append(&mut self, term: u64, cmd: &[u8]) -> io::Result<()> {
        let rec = record(term, cmd);
        (&*self.file).write_all(&rec)?;
        self.offsets.push(self.end);
        self.end += rec.len() as u64;
        Ok(())
    }

    /// Drop entry `from` and everything after it.
    pub fn truncate(&mut self, from: u64) -> io::Result<()> {
        let keep = from.saturating_sub(self.base + 1) as usize;
        if keep >= self.offsets.len() {
            return Ok(());
        }
        self.end = self.offsets[keep];
        self.offsets.truncate(keep);
        self.file.set_len(self.end)?;
        self.generation += 1;
        Ok(())
    }

    /// Replace the log with `entries`, which follow `base`. Durable and
    /// atomic: a crash leaves either the old log or the new one.
    pub fn reset<'a>(
        &mut self,
        base: u64,
        entries: impl IntoIterator<Item = (u64, &'a [u8])>,
    ) -> io::Result<()> {
        let mut buf = header(base).to_vec();
        let mut offsets = Vec::new();
        for (term, cmd) in entries {
            offsets.push(buf.len() as u64);
            buf.extend_from_slice(&record(term, cmd));
        }
        write_atomic(&self.dir, "log", &buf)?;
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(self.dir.join("log"))?;
        self.file = Arc::new(file);
        self.base = base;
        self.end = buf.len() as u64;
        self.offsets = offsets;
        self.generation += 1;
        Ok(())
    }

    pub fn file(&self) -> Arc<File> {
        self.file.clone()
    }

    pub fn dir(&self) -> &Path {
        &self.dir
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

/// `[magic][index][term][crc32][data]`
pub(crate) fn write_snapshot(dir: &Path, s: &Snapshot) -> io::Result<()> {
    let mut buf = Vec::with_capacity(28 + s.data.len());
    buf.extend_from_slice(SNAP_MAGIC);
    buf.extend_from_slice(&s.index.to_be_bytes());
    buf.extend_from_slice(&s.term.to_be_bytes());
    buf.extend_from_slice(&crc32(&s.data).to_be_bytes());
    buf.extend_from_slice(&s.data);
    write_atomic(dir, "snapshot", &buf)
}

pub(crate) fn read_snapshot(dir: &Path) -> io::Result<Option<Snapshot>> {
    let bytes = match std::fs::read(dir.join("snapshot")) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    // A snapshot is renamed into place whole, so a bad one is real damage.
    if bytes.len() < 28 || &bytes[..8] != SNAP_MAGIC {
        return Err(io::Error::other("snapshot file is damaged"));
    }
    let data = bytes[28..].to_vec();
    if crc32(&data) != u32::from_be_bytes(bytes[24..28].try_into().unwrap()) {
        return Err(io::Error::other("snapshot checksum mismatch"));
    }
    Ok(Some(Snapshot {
        index: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
        term: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
        data,
    }))
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
        assert!(l.entries.is_empty() && l.term == 0 && l.vote.is_none() && l.snapshot.is_none());
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
        assert_eq!((l.base, l.term), (0, 7));
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

    #[test]
    fn reset_and_snapshot_survive_reopen() {
        let d = dir("reset");
        let (mut w, _) = Wal::open(&d).unwrap();
        for i in 1..=10u64 {
            w.append(1, &i.to_be_bytes()).unwrap();
        }
        let tail: Vec<(u64, Vec<u8>)> =
            (9..=10u64).map(|i| (1, i.to_be_bytes().to_vec())).collect();
        w.reset(8, tail.iter().map(|(t, c)| (*t, c.as_slice())))
            .unwrap();
        w.append(2, &11u64.to_be_bytes()).unwrap();
        w.truncate(11).unwrap();
        w.append(3, b"eleven").unwrap();
        write_snapshot(
            &d,
            &Snapshot {
                index: 8,
                term: 1,
                data: b"machine".to_vec(),
            },
        )
        .unwrap();
        drop(w);

        let (_, l) = Wal::open(&d).unwrap();
        assert_eq!(l.base, 8);
        assert_eq!(l.entries.len(), 3);
        assert_eq!(l.entries[2], (3, b"eleven".to_vec()));
        let s = l.snapshot.unwrap();
        assert_eq!(
            (s.index, s.term, s.data.as_slice()),
            (8, 1, &b"machine"[..])
        );
    }
}
