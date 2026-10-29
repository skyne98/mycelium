//! Mycelium: ordered streams and merged maps shared by every machine on a tailnet.
//!
//! ```text
//! let node = Mycelium::local();                        // or Client::to(addr).connect()
//! let chat = node.stream::<Msg>("chat");               // ordered log, linearizable
//! chat.append(&msg).await?;
//! let presence = node.map::<String, Status>("online"); // merged state, always writable
//! presence.set(&"laptop".into(), &Status::Up).await?;
//! ```
//!
//! Streams are replicated with Raft (PreVote, CheckQuorum, ReadIndex, client
//! sessions) and acknowledged only once a majority holds them on disk. Maps are
//! loro CRDTs: every node accepts writes, even when partitioned, and replicas
//! converge by push plus anti-entropy pull.
//!
//! Types are a local lens, not a wire contract. Every value carries an 8-byte
//! type fingerprint; entries that do not decode are skipped and counted
//! (`skipped()`), never fatal.
//!
//! Identity: with `Node::with_tls` every node serves a real certificate (for
//! example from `tailscale cert`) and presents it when it calls a peer, so
//! only members can vote or replicate, and `Node::allow` limits clients.
//! Without TLS everything must be on loopback, by construction.
//!
//! Membership changes one server at a time (`Node::join`, `Running::remove`),
//! the log is compacted into snapshots, and `Running::stop` hands leadership
//! over before leaving.

// The request handler's future is deep; older compilers need more room to
// prove it Send.
#![recursion_limit = "256"]

/// MYCELIUM_TRACE=1: elections, leadership changes and slow requests on stderr.
fn tracing() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("MYCELIUM_TRACE").is_some())
}

macro_rules! trace {
    ($($a:tt)*) => {
        if crate::tracing() {
            eprintln!("[mycelium] {}", format!($($a)*));
        }
    };
}

mod maps;
mod node;
mod raft;
mod transport;
mod wal;

pub use transport::{Error, Result};

use bincode::config::standard;
use bytes::Bytes;
use node::Shared;
use quinn::{Connection, Endpoint};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use transport::{parts, Blobs};

const FP_LEN: usize = 8;

/// Raw bytes from a stream, fingerprint stripped.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Raw(pub Vec<u8>);

/// Another node in the cluster: where it listens, and the name on its certificate.
#[derive(Clone, Debug)]
pub struct Peer {
    pub addr: SocketAddr,
    pub name: Option<String>,
}

impl Peer {
    /// Unverified, loopback only.
    pub fn local(addr: SocketAddr) -> Self {
        Self { addr, name: None }
    }

    /// Verified against the system trust store under `name`.
    pub fn verified(addr: SocketAddr, name: impl Into<String>) -> Self {
        Self {
            addr,
            name: Some(name.into()),
        }
    }

    fn server_name(&self) -> &str {
        self.name.as_deref().unwrap_or("localhost")
    }
}

/// The default type tag: the type's name with every module path and space
/// removed, e.g. `Option<(u64, String)>`. So moving a type to another module
/// or crate, or building with another compiler, keeps its streams readable;
/// rustc only promises the path parts of `type_name`, not their spelling.
/// Two different types with the same name in one stream need `type_tag`.
fn type_tag<T>() -> String {
    let full = std::any::type_name::<T>();
    let mut out = String::with_capacity(full.len());
    let mut token = String::new();
    let flush = |token: &mut String, out: &mut String| {
        out.push_str(token.rsplit("::").next().unwrap_or_default());
        token.clear();
    };
    for c in full.chars() {
        if c.is_alphanumeric() || c == '_' || c == ':' {
            token.push(c);
        } else {
            flush(&mut token, &mut out);
            if !c.is_whitespace() {
                out.push(c);
            }
        }
    }
    flush(&mut token, &mut out);
    out
}

/// FNV-1a of a type tag: fully specified, so every build agrees.
fn fingerprint(tag: &str) -> u64 {
    tag.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ b as u64).wrapping_mul(0x100_0000_01b3)
    })
}

fn encode<T: Serialize>(fp: u64, v: &T) -> Result<Vec<u8>> {
    let mut out = fp.to_be_bytes().to_vec();
    bincode::serde::encode_into_std_write(v, &mut out, standard())?;
    Ok(out)
}

/// None means "written as another type", not "corrupt".
fn decode<T: DeserializeOwned>(fp: u64, bytes: &[u8]) -> Result<Option<T>> {
    if bytes.len() < FP_LEN || bytes[..FP_LEN] != fp.to_be_bytes() {
        return Ok(None);
    }
    let (v, _) = bincode::serde::decode_from_slice(&bytes[FP_LEN..], standard())?;
    Ok(Some(v))
}

fn key_text<K: Serialize>(key: &K) -> Result<String> {
    Ok(maps::hex(&bincode::serde::encode_to_vec(key, standard())?))
}

// -------------------------------------------------------------------- client

/// A connection to one node of a cluster, which moves to the next address on
/// failure. Every request is safe to repeat: appends carry (client, seq), map
/// writes are idempotent, reads are reads.
struct Remote {
    targets: Vec<(SocketAddr, String)>,
    /// Index into `targets` of the node in use.
    at: std::sync::atomic::AtomicUsize,
    endpoint: Endpoint,
    cfg: quinn::ClientConfig,
    conn: tokio::sync::Mutex<Option<(usize, Connection)>>,
}

impl Remote {
    async fn conn(&self) -> Result<(usize, Connection)> {
        let mut slot = self.conn.lock().await;
        if let Some((i, c)) = slot.as_ref().filter(|(_, c)| c.close_reason().is_none()) {
            return Ok((*i, c.clone()));
        }
        let start = self.at.load(Ordering::Relaxed);
        let mut last: Error = "no address to connect to".into();
        for k in 0..self.targets.len() {
            let i = (start + k) % self.targets.len();
            let (addr, name) = &self.targets[i];
            let dial = async {
                Ok::<_, Error>(
                    self.endpoint
                        .connect_with(self.cfg.clone(), *addr, name)?
                        .await?,
                )
            };
            // Long enough for a handshake on a loaded machine, short enough to
            // move on from a dead node quickly.
            match tokio::time::timeout(Duration::from_secs(5), dial).await {
                Ok(Ok(c)) => {
                    self.at.store(i, Ordering::Relaxed);
                    *slot = Some((i, c.clone()));
                    return Ok((i, c));
                }
                Ok(Err(e)) => last = e,
                Err(_) => last = format!("{addr} did not answer").into(),
            }
        }
        Err(last)
    }

    /// Forget a connection that failed, and try the next address next time.
    async fn drop_conn(&self, i: usize) {
        let mut slot = self.conn.lock().await;
        if slot.as_ref().is_some_and(|(j, _)| *j == i) {
            *slot = None;
            self.at
                .store((i + 1) % self.targets.len(), Ordering::Relaxed);
        }
    }

    async fn once(&self, conn: &Connection, role: u8, body: &[u8]) -> Result<Vec<u8>> {
        if !tracing() {
            return self.exchange(conn, role, body).await;
        }
        let started = Instant::now();
        let before = conn.stats().path;
        let out = self.exchange(conn, role, body).await;
        if started.elapsed() > Duration::from_millis(40) {
            let after = conn.stats().path;
            trace!(
                "slow request {role} to {}: {:?}, lost {} -> {}, sent {} -> {}, rtt {:?}",
                conn.remote_address(),
                started.elapsed(),
                before.lost_packets,
                after.lost_packets,
                before.sent_packets,
                after.sent_packets,
                after.rtt
            );
        }
        out
    }

    async fn exchange(&self, conn: &Connection, role: u8, body: &[u8]) -> Result<Vec<u8>> {
        let call = async {
            let (mut send, mut recv) = conn.open_bi().await?;
            send.write_all(&[role]).await?;
            transport::put(&mut send, body).await?;
            let _ = send.finish();
            transport::get(&mut recv).await
        };
        tokio::time::timeout(node::DEADLINE + Duration::from_secs(2), call)
            .await
            .map_err(|_| "the node did not answer")?
    }

    /// Retries lost connections, across every address, until the deadline.
    async fn call(&self, role: u8, body: Vec<u8>) -> Result<Vec<u8>> {
        let deadline = Instant::now() + node::DEADLINE;
        loop {
            let attempt = match self.conn().await {
                Ok((i, c)) => match self.once(&c, role, &body).await {
                    Ok(frame) => return node::reply(frame),
                    Err(e) => {
                        self.drop_conn(i).await;
                        e
                    }
                },
                Err(e) => e,
            };
            if Instant::now() + Duration::from_millis(200) > deadline {
                return Err(attempt);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn open(&self, role: u8, body: &[u8]) -> Result<(usize, quinn::RecvStream)> {
        let (i, c) = self.conn().await?;
        let opened = async {
            let (mut send, recv) = c.open_bi().await?;
            send.write_all(&[role]).await?;
            transport::put(&mut send, body).await?;
            // Keep the send side alive: dropping it would tell the node we left.
            std::mem::forget(send);
            Ok::<_, Error>(recv)
        };
        match opened.await {
            Ok(r) => Ok((i, r)),
            Err(e) => {
                self.drop_conn(i).await;
                Err(e)
            }
        }
    }

    /// A long-lived subscription that reconnects, to any address, on failure.
    /// `resume` builds the request from the last frame seen; `decode` turns a
    /// frame into an item, or None for a frame that means "start over".
    fn subscribe<T: Send + 'static>(
        self: Arc<Self>,
        role: u8,
        mut request: Vec<u8>,
        mut next_request: impl FnMut(&T) -> Option<Vec<u8>> + Send + 'static,
        decode: impl Fn(&[u8]) -> Option<T> + Send + 'static,
    ) -> mpsc::Receiver<T> {
        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(async move {
            loop {
                if let Ok((i, mut recv)) = self.open(role, &request).await {
                    loop {
                        let frame = tokio::select! {
                            f = transport::get(&mut recv) => f,
                            _ = tx.closed() => return,
                        };
                        let Ok(frame) = frame else {
                            self.drop_conn(i).await;
                            break;
                        };
                        let Some(item) = decode(&frame) else { break };
                        if let Some(r) = next_request(&item) {
                            request = r;
                        }
                        if tx.send(item).await.is_err() {
                            return;
                        }
                    }
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(200)) => {}
                    _ = tx.closed() => return,
                }
            }
        });
        rx
    }

    fn follow(
        self: Arc<Self>,
        name: String,
        from: u64,
        ephemeral: bool,
    ) -> mpsc::Receiver<(u64, Bytes)> {
        let req =
            move |n: &str, at: u64| parts(&[n.as_bytes(), &at.to_be_bytes(), &[ephemeral as u8]]);
        let first = req(&name, from);
        self.subscribe(
            node::FOLLOW,
            first,
            move |(idx, _): &(u64, Bytes)| Some(req(&name, idx + 1)),
            |frame| {
                let mut b = Blobs::new(frame);
                Some((b.u64().ok()?, Bytes::copy_from_slice(b.take().ok()?)))
            },
        )
    }

    fn map_watch(self: Arc<Self>, name: String) -> mpsc::Receiver<(String, Option<Vec<u8>>)> {
        let req = parts(&[name.as_bytes()]);
        self.subscribe(
            node::MAP_WATCH,
            req,
            |_| None,
            |frame| {
                let mut b = Blobs::new(frame);
                let key = b.text().ok()?;
                let present = b.take().ok()? == [1];
                let v = b.take().ok()?.to_vec();
                Some((key, present.then_some(v)))
            },
        )
    }
}

#[derive(Clone)]
enum Backend {
    Local(Arc<Shared>),
    Remote(Arc<Remote>),
}

impl Backend {
    async fn append(
        &self,
        ephemeral: bool,
        body: Vec<u8>,
        name: &str,
        payloads: &[&[u8]],
    ) -> Result<u64> {
        match self {
            Backend::Local(s) if ephemeral => Ok(s.append_ephemeral(
                name,
                payloads.iter().map(|p| Bytes::copy_from_slice(p)).collect(),
            )),
            Backend::Local(s) => s.append(body, true).await,
            Backend::Remote(r) => {
                let wire = if ephemeral {
                    let mut pieces: Vec<&[u8]> = vec![name.as_bytes()];
                    pieces.extend_from_slice(payloads);
                    [&parts(&[&[1]])[..], &parts(&pieces)].concat()
                } else {
                    [&parts(&[&[0]])[..], &body].concat()
                };
                node::index(&r.call(node::APPEND, wire).await?)
            }
        }
    }

    async fn len(&self, name: &str, ephemeral: bool) -> Result<u64> {
        match self {
            Backend::Local(s) if ephemeral => Ok(s.ephemeral_len(name)),
            Backend::Local(s) => s.len(name).await,
            Backend::Remote(r) => node::index(
                &r.call(node::LEN, parts(&[name.as_bytes(), &[ephemeral as u8]]))
                    .await?,
            ),
        }
    }

    async fn first(&self, name: &str) -> Result<u64> {
        match self {
            Backend::Local(s) => s.first(name).await,
            Backend::Remote(r) => {
                node::index(&r.call(node::FIRST, parts(&[name.as_bytes()])).await?)
            }
        }
    }

    async fn trim(&self, name: &str, before: u64) -> Result<()> {
        match self {
            Backend::Local(s) => s.trim(name, before).await,
            Backend::Remote(r) => r
                .call(node::TRIM, parts(&[name.as_bytes(), &before.to_be_bytes()]))
                .await
                .map(drop),
        }
    }

    async fn follow(
        &self,
        name: &str,
        from: u64,
        ephemeral: bool,
    ) -> Result<mpsc::Receiver<(u64, Bytes)>> {
        match self {
            Backend::Local(s) if ephemeral => Ok(s.follow_ephemeral(name.to_string(), from)),
            Backend::Local(s) => Ok(s.follow(name.to_string(), from)),
            Backend::Remote(r) => {
                r.conn().await?;
                Ok(r.clone().follow(name.to_string(), from, ephemeral))
            }
        }
    }

    async fn map_set(&self, name: &str, key: &str, val: Vec<u8>) -> Result<()> {
        match self {
            Backend::Local(s) => s.map_set(name, key, val).await,
            Backend::Remote(r) => r
                .call(
                    node::MAP_SET,
                    parts(&[name.as_bytes(), key.as_bytes(), &val]),
                )
                .await
                .map(drop),
        }
    }

    async fn map_remove(&self, name: &str, key: &str) -> Result<()> {
        match self {
            Backend::Local(s) => s.map_remove(name, key).await,
            Backend::Remote(r) => r
                .call(node::MAP_REMOVE, parts(&[name.as_bytes(), key.as_bytes()]))
                .await
                .map(drop),
        }
    }

    async fn map_get(&self, name: &str, key: &str) -> Result<Option<Vec<u8>>> {
        match self {
            Backend::Local(s) => Ok(s.map_get(name, key)),
            Backend::Remote(r) => {
                let out = r
                    .call(node::MAP_GET, parts(&[name.as_bytes(), key.as_bytes()]))
                    .await?;
                Ok(match out.split_first() {
                    Some((1, v)) => Some(v.to_vec()),
                    _ => None,
                })
            }
        }
    }

    async fn map_entries(&self, name: &str) -> Result<Vec<(String, Vec<u8>)>> {
        match self {
            Backend::Local(s) => Ok(s.map_entries(name)),
            Backend::Remote(r) => {
                let out = r.call(node::MAP_ENTRIES, parts(&[name.as_bytes()])).await?;
                let mut b = Blobs::new(&out);
                let mut v = Vec::new();
                while !b.is_empty() {
                    v.push((b.text()?, b.take()?.to_vec()));
                }
                Ok(v)
            }
        }
    }

    async fn map_watch(&self, name: &str) -> Result<mpsc::Receiver<(String, Option<Vec<u8>>)>> {
        match self {
            Backend::Local(s) => Ok(s.map_watch(name.to_string())),
            Backend::Remote(r) => {
                r.conn().await?;
                Ok(r.clone().map_watch(name.to_string()))
            }
        }
    }

    async fn members(&self) -> Result<Vec<raft::Member>> {
        match self {
            Backend::Local(s) => Ok(s.members()),
            Backend::Remote(r) => node::decode_members(&r.call(node::MEMBERS, Vec::new()).await?),
        }
    }
}

// ------------------------------------------------------------------ handles

/// How long a stream keeps entries. Applies to the whole stream once any
/// append carries it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Retention {
    /// Keep at most this many newest entries.
    pub keep_last: Option<u64>,
    /// Drop entries older than this, by the leader's clock.
    pub max_age: Option<Duration>,
}

/// An ordered log. Every node sees the same entries at the same indices.
///
/// Durable streams (the default) are replicated with Raft and survive any
/// minority of nodes failing. `ephemeral()` streams are ordered by the node
/// you are connected to, kept in its memory only, and gone when it restarts:
/// for presence pings, progress ticks and other data that is stale in seconds.
pub struct Stream<T> {
    name: String,
    fp: u64,
    db: Mycelium,
    skipped: Arc<AtomicU64>,
    ephemeral: bool,
    retention: raft::Retention,
    _t: PhantomData<fn() -> T>,
}

impl<T> Stream<T> {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Name the entry type explicitly, instead of by its Rust name: for a
    /// renamed type, or to keep two types called the same apart.
    pub fn type_tag(mut self, tag: &str) -> Self {
        self.fp = fingerprint(tag);
        self
    }

    /// Memory-only on the node this handle talks to: no consensus, no disk.
    pub fn ephemeral(mut self) -> Self {
        self.ephemeral = true;
        self
    }

    /// Bound the stream by count or age. Stored with the stream on the next append.
    pub fn retain(mut self, r: Retention) -> Self {
        self.retention = raft::Retention {
            keep_last: r.keep_last.unwrap_or(0),
            max_age_ms: r.max_age.map_or(0, |d| d.as_millis().max(1) as u64),
        };
        self
    }

    /// Entries a watcher skipped because they were written as another type.
    pub fn skipped(&self) -> u64 {
        self.skipped.load(Ordering::Relaxed)
    }

    /// Linearizable: includes every append acknowledged before the call.
    /// Counts trimmed entries too: it is the next index, not what is stored.
    pub async fn len(&self) -> Result<u64> {
        self.db.backend.len(&self.name, self.ephemeral).await
    }

    /// The first index still stored; everything before it was trimmed or expired.
    pub async fn first(&self) -> Result<u64> {
        if self.ephemeral {
            return Err("first() is for durable streams".into());
        }
        self.db.backend.first(&self.name).await
    }

    /// Drop every entry before `index`, on every node.
    pub async fn trim(&self, before: u64) -> Result<()> {
        if self.ephemeral {
            return Err("trim() is for durable streams".into());
        }
        self.db.backend.trim(&self.name, before).await
    }

    async fn push(&self, payloads: &[&[u8]]) -> Result<u64> {
        let seq = self.db.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let body = raft::append_body(&self.name, self.db.client, seq, self.retention, payloads);
        self.db
            .backend
            .append(self.ephemeral, body, &self.name, payloads)
            .await
    }

    pub async fn append_raw(&self, bytes: &[u8]) -> Result<u64> {
        let mut out = 0u64.to_be_bytes().to_vec();
        out.extend_from_slice(bytes);
        self.push(&[&out]).await
    }

    pub async fn watch_raw(&self, from: u64) -> Result<mpsc::Receiver<(u64, Raw)>> {
        let mut raw = self
            .db
            .backend
            .follow(&self.name, from, self.ephemeral)
            .await?;
        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(async move {
            while let Some((idx, bytes)) = raw.recv().await {
                let body = bytes.get(FP_LEN..).unwrap_or(&bytes).to_vec();
                if tx.send((idx, Raw(body))).await.is_err() {
                    return;
                }
            }
        });
        Ok(rx)
    }
}

impl<T: Serialize + DeserializeOwned> Stream<T> {
    /// Durable: returns once a majority holds the entry on disk.
    pub async fn append(&self, value: &T) -> Result<u64> {
        self.push(&[&encode(self.fp, value)?]).await
    }

    /// One atomic, contiguous batch in one round trip.
    pub async fn append_many(&self, values: &[T]) -> Result<Vec<u64>> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        let items = values
            .iter()
            .map(|v| encode(self.fp, v))
            .collect::<Result<Vec<_>>>()?;
        let refs: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
        let first = self.push(&refs).await?;
        Ok((first..first + values.len() as u64).collect())
    }

    /// Every committed entry from `from` on, then new ones as they commit.
    /// Over the network it resumes after reconnecting, on any node. If
    /// entries were trimmed meanwhile it continues at the first one left.
    pub async fn watch(&self, from: u64) -> Result<mpsc::Receiver<(u64, T)>>
    where
        T: Send + 'static,
    {
        let mut raw = self
            .db
            .backend
            .follow(&self.name, from, self.ephemeral)
            .await?;
        let (tx, rx) = mpsc::channel(256);
        let (fp, skipped) = (self.fp, self.skipped.clone());
        tokio::spawn(async move {
            while let Some((idx, bytes)) = raw.recv().await {
                match decode::<T>(fp, &bytes) {
                    Ok(Some(v)) => {
                        if tx.send((idx, v)).await.is_err() {
                            return;
                        }
                    }
                    _ => {
                        skipped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
        Ok(rx)
    }
}

/// Merged state: every node accepts writes, concurrent writes converge.
pub struct Map<K, V> {
    name: String,
    fp: u64,
    db: Mycelium,
    _t: PhantomData<fn() -> (K, V)>,
}

impl<K, V> Map<K, V> {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Name the value type explicitly, instead of by its Rust name: for a
    /// renamed type, or to keep two types called the same apart.
    pub fn type_tag(mut self, tag: &str) -> Self {
        self.fp = fingerprint(tag);
        self
    }
}

impl<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> Map<K, V> {
    /// Returns once the write is on this node's disk; peers get it by push now
    /// and by pull within a second, even after a partition heals.
    pub async fn set(&self, key: &K, value: &V) -> Result<()> {
        let v = encode(self.fp, value)?;
        self.db
            .backend
            .map_set(&self.name, &key_text(key)?, v)
            .await
    }

    pub async fn remove(&self, key: &K) -> Result<()> {
        self.db
            .backend
            .map_remove(&self.name, &key_text(key)?)
            .await
    }

    /// This replica's view. None also when the value was written as another type.
    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        match self.db.backend.map_get(&self.name, &key_text(key)?).await? {
            Some(bytes) => decode::<V>(self.fp, &bytes),
            None => Ok(None),
        }
    }

    /// Every entry of this type, sorted by encoded key.
    pub async fn entries(&self) -> Result<Vec<(K, V)>> {
        let mut out = Vec::new();
        for (k, v) in self.db.backend.map_entries(&self.name).await? {
            let Some(kb) = maps::unhex(&k) else { continue };
            let (Ok((k, _)), Ok(Some(v))) = (
                bincode::serde::decode_from_slice::<K, _>(&kb, standard()),
                decode::<V>(self.fp, &v),
            ) else {
                continue;
            };
            out.push((k, v));
        }
        Ok(out)
    }

    /// Every change from now on, local or remote: (key, value, or None when
    /// removed). Changes may coalesce; the value is the latest at delivery.
    pub async fn watch(&self) -> Result<mpsc::Receiver<(K, Option<V>)>>
    where
        K: Send + 'static,
        V: Send + 'static,
    {
        let mut raw = self.db.backend.map_watch(&self.name).await?;
        let (tx, rx) = mpsc::channel(256);
        let fp = self.fp;
        tokio::spawn(async move {
            while let Some((k, v)) = raw.recv().await {
                let Some(kb) = maps::unhex(&k) else { continue };
                let Ok((k, _)) = bincode::serde::decode_from_slice::<K, _>(&kb, standard()) else {
                    continue;
                };
                let v = match v {
                    Some(bytes) => match decode::<V>(fp, &bytes) {
                        Ok(Some(v)) => Some(v),
                        _ => continue,
                    },
                    None => None,
                };
                if tx.send((k, v)).await.is_err() {
                    return;
                }
            }
        });
        Ok(rx)
    }
}

// ------------------------------------------------------------------- facade

/// A handle to a node: in this process, or over the network.
#[derive(Clone)]
pub struct Mycelium {
    backend: Backend,
    /// Names this handle's writes, so a retried append lands once.
    client: u64,
    seq: Arc<AtomicU64>,
}

impl Mycelium {
    fn with(backend: Backend) -> Self {
        Self {
            backend,
            client: raft::random_u64(),
            seq: Arc::new(AtomicU64::new(0)),
        }
    }

    /// One process, in memory, no sockets.
    pub fn local() -> Self {
        Self::with(Backend::Local(Shared::memory()))
    }

    /// Serve a standalone node on `addr` (loopback without TLS) until the process ends.
    pub async fn serve(addr: SocketAddr) -> Result<()> {
        Node::bind(addr).serve().await
    }

    /// Connect to a node on loopback.
    pub async fn join(addr: SocketAddr) -> Result<Self> {
        Client::to(addr).connect().await
    }

    pub fn stream<T: Serialize + DeserializeOwned>(&self, name: &str) -> Stream<T> {
        Stream {
            name: name.to_string(),
            fp: fingerprint(&type_tag::<T>()),
            db: self.clone(),
            skipped: Arc::new(AtomicU64::new(0)),
            ephemeral: false,
            retention: raft::Retention::default(),
            _t: PhantomData,
        }
    }

    pub fn map<K: Serialize, V: Serialize + DeserializeOwned>(&self, name: &str) -> Map<K, V> {
        Map {
            name: name.to_string(),
            fp: fingerprint(&type_tag::<V>()),
            db: self.clone(),
            _t: PhantomData,
        }
    }

    /// The cluster's members, as the node you talk to sees them.
    pub async fn members(&self) -> Result<Vec<Member>> {
        Ok(self
            .backend
            .members()
            .await?
            .into_iter()
            .map(Member::from)
            .collect())
    }
}

/// A member of the cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub addr: SocketAddr,
    /// The name on its certificate, if it uses one.
    pub name: Option<String>,
    /// Learners receive the log while catching up, but do not vote yet.
    pub voter: bool,
}

impl From<raft::Member> for Member {
    fn from(m: raft::Member) -> Self {
        Self {
            addr: m.addr,
            name: m.name,
            voter: m.voter,
        }
    }
}

/// How to reach a cluster: one or more of its nodes.
#[derive(Clone, Debug)]
pub struct Client {
    targets: Vec<(SocketAddr, Option<String>)>,
}

impl Client {
    /// Unverified, loopback only.
    pub fn to(addr: SocketAddr) -> Self {
        Self {
            targets: vec![(addr, None)],
        }
    }

    /// Verified against the system trust store under `name`.
    pub fn verified(addr: SocketAddr, name: impl Into<String>) -> Self {
        Self {
            targets: vec![(addr, Some(name.into()))],
        }
    }

    /// Any of these nodes; the client moves to the next one when one fails.
    pub fn any(peers: impl IntoIterator<Item = Peer>) -> Self {
        Self {
            targets: peers.into_iter().map(|p| (p.addr, p.name)).collect(),
        }
    }

    /// Also try this node when others fail.
    pub fn or(mut self, peer: Peer) -> Self {
        self.targets.push((peer.addr, peer.name));
        self
    }

    pub async fn connect(self) -> Result<Mycelium> {
        let first = self.targets.first().ok_or("no address to connect to")?;
        let verified = first.1.is_some();
        for (addr, name) in &self.targets {
            if name.is_some() != verified {
                return Err("mix of verified and unverified addresses".into());
            }
            if name.is_none() && !addr.ip().is_loopback() {
                return Err(format!(
                    "refusing unverified TLS off loopback ({addr}); use Client::verified"
                )
                .into());
            }
        }
        let remote = Remote {
            targets: self
                .targets
                .iter()
                .map(|(a, n)| (*a, n.clone().unwrap_or_else(|| "localhost".into())))
                .collect(),
            at: std::sync::atomic::AtomicUsize::new(0),
            endpoint: transport::client_endpoint(first.0)?,
            cfg: transport::client_config(verified, None)?,
            conn: tokio::sync::Mutex::new(None),
        };
        remote.conn().await?;
        Ok(Mycelium::with(Backend::Remote(Arc::new(remote))))
    }
}

/// Describe a node, then `start` it.
#[derive(Clone, Debug)]
pub struct Node {
    bind: SocketAddr,
    tls: Option<(PathBuf, PathBuf)>,
    peers: Vec<Peer>,
    join: Option<Vec<Peer>>,
    allow: Option<Vec<String>>,
    data_dir: PathBuf,
    snapshot_every: u64,
}

impl Node {
    /// A node with no peers: its own cluster, leading immediately.
    pub fn bind(addr: SocketAddr) -> Self {
        Self {
            bind: addr,
            tls: None,
            peers: Vec::new(),
            join: None,
            allow: None,
            data_dir: default_data_dir(),
            snapshot_every: raft::SNAPSHOT_EVERY,
        }
    }

    /// One node of a new cluster. `addr` is where the other nodes reach this
    /// one; each node lists every other node. Used only on first start: after
    /// that, membership lives in the replicated log.
    pub fn cluster(addr: SocketAddr, peers: Vec<Peer>) -> Self {
        Self {
            peers,
            ..Self::bind(addr)
        }
    }

    /// A new node joining a running cluster through any of `via`. It catches
    /// up as a non-voting learner, then becomes a voter.
    pub fn join(addr: SocketAddr, via: Vec<Peer>) -> Self {
        Self {
            join: Some(via),
            ..Self::bind(addr)
        }
    }

    /// Serve this certificate (e.g. from `tailscale cert`) and present it to peers.
    pub fn with_tls(mut self, cert: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Self {
        self.tls = Some((cert.into(), key.into()));
        self
    }

    /// Only clients whose verified certificate carries one of these names may
    /// use this node (members always may). Needs TLS.
    pub fn allow(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.allow = Some(names.into_iter().map(Into::into).collect());
        self
    }

    /// Where the log, (term, vote), snapshots and maps live. Default:
    /// `$XDG_DATA_HOME/mycelium`, else `~/.local/share/mycelium`. Never a temp
    /// dir: a node that forgets its vote on reboot can vote twice in a term.
    pub fn with_data_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.data_dir = dir.into();
        self
    }

    /// Snapshot and compact the log after this many entries. Default 10 000.
    pub fn snapshot_every(mut self, entries: u64) -> Self {
        self.snapshot_every = entries.max(1);
        self
    }

    fn check(&self) -> Result<()> {
        let clustered = !self.peers.is_empty() || self.join.is_some();
        if clustered && self.bind.ip().is_unspecified() {
            return Err(format!(
                "a cluster node binds the address its peers use, not {}",
                self.bind
            )
            .into());
        }
        let others: Vec<&Peer> = self
            .peers
            .iter()
            .chain(self.join.iter().flatten())
            .collect();
        if self.tls.is_some() {
            if let Some(p) = others.iter().find(|p| p.name.is_none()) {
                return Err(
                    format!("peer {} needs a certificate name (Peer::verified)", p.addr).into(),
                );
            }
            return Ok(());
        }
        if self.allow.is_some() {
            return Err("Node::allow needs Node::with_tls: names come from certificates".into());
        }
        if !self.bind.ip().is_loopback() {
            return Err(format!(
                "refusing unverified TLS off loopback ({}); use Node::with_tls",
                self.bind
            )
            .into());
        }
        if let Some(p) = others
            .iter()
            .find(|p| p.name.is_some() || !p.addr.ip().is_loopback())
        {
            return Err(format!("peer {} needs Node::with_tls on this node too", p.addr).into());
        }
        Ok(())
    }

    /// Bind and run in the background.
    pub async fn start(self) -> Result<Running> {
        self.check()?;
        let identity = match &self.tls {
            Some((cert, key)) => Some(transport::Identity::load(cert, key)?),
            None => None,
        };
        let shared = node::start(node::Config {
            bind: self.bind,
            peers: self.peers,
            identity,
            dir: self.data_dir,
            join: self.join,
            allow: self.allow,
            snapshot_every: self.snapshot_every,
        })?;
        Ok(Running {
            addr: shared.me,
            shared,
        })
    }

    /// Start and run until the process gets Ctrl-C or SIGTERM, then hand over
    /// leadership and stop.
    pub async fn serve(self) -> Result<()> {
        let running = self.start().await?;
        shutdown_signal().await;
        running.stop().await;
        Ok(())
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut term) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

fn default_data_dir() -> PathBuf {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share")))
        .unwrap_or_else(std::env::temp_dir);
    base.join("mycelium")
}

fn tailscale_status() -> Result<serde_json::Value> {
    let out = std::process::Command::new("tailscale")
        .args(["status", "--json"])
        .output()
        .map_err(|e| format!("cannot run tailscale: {e}"))?;
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).into_owned().into());
    }
    Ok(serde_json::from_slice(&out.stdout)?)
}

fn tailnet_peer(p: &serde_json::Value, port: u16) -> Option<Peer> {
    let ip: std::net::IpAddr = p["TailscaleIPs"][0].as_str()?.parse().ok()?;
    let name = p["DNSName"].as_str()?.trim_end_matches('.');
    Some(Peer::verified(SocketAddr::new(ip, port), name))
}

/// This machine on the tailnet: its address on `port`, and the name its
/// `tailscale cert` certificate carries.
pub fn tailnet_self(port: u16) -> Result<Peer> {
    tailnet_peer(&tailscale_status()?["Self"], port)
        .ok_or_else(|| "tailscale reports no address for this machine".into())
}

/// Every online machine on this tailnet, as a verified peer on `port`. Feed
/// it to `Node::cluster`, `Node::join` or `Client::any`. Requires the
/// `tailscale` CLI.
pub fn tailnet_peers(port: u16) -> Result<Vec<Peer>> {
    let v = tailscale_status()?;
    let peer = |p: &serde_json::Value| tailnet_peer(p, port);
    let mut peers: Vec<Peer> = v["Peer"]
        .as_object()
        .into_iter()
        .flat_map(|m| m.values())
        .filter(|p| p["Online"].as_bool() == Some(true))
        .filter_map(peer)
        .collect();
    peers.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(peers)
}

/// A running node. Dropping it stops it at once.
pub struct Running {
    pub addr: SocketAddr,
    shared: Arc<Shared>,
}

impl Running {
    pub async fn is_leader(&self) -> bool {
        self.shared.is_leader()
    }

    /// Use this node from its own process, without a socket.
    pub fn handle(&self) -> Mycelium {
        Mycelium::with(Backend::Local(self.shared.clone()))
    }

    /// The membership this node has.
    pub fn members(&self) -> Vec<Member> {
        self.shared
            .members()
            .into_iter()
            .map(Member::from)
            .collect()
    }

    /// Add a node to the cluster: it catches up as a learner, then votes.
    pub async fn add(&self, peer: Peer) -> Result<()> {
        self.shared.add_member(peer).await
    }

    /// Remove a node from the cluster, for good.
    pub async fn remove(&self, addr: SocketAddr) -> Result<()> {
        self.shared.remove_member(addr).await
    }

    /// Hand leadership to a caught-up voter, then stop. Writes pause for one
    /// round trip instead of a full election timeout.
    pub async fn stop(&self) {
        self.shared.shutdown_gracefully().await;
    }

    /// Stop at once: release the disk and close every connection. Idempotent.
    /// The port is free once QUIC finishes draining, about a second later.
    pub fn shutdown(&self) {
        self.shared.shutdown();
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        self.shared.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    mod deep {
        pub mod inside {
            pub struct Note;
        }
    }

    #[test]
    fn type_tags_drop_module_paths_and_spaces() {
        assert_eq!(type_tag::<String>(), "String");
        assert_eq!(type_tag::<Vec<u64>>(), "Vec<u64>");
        assert_eq!(type_tag::<Option<(u64, String)>>(), "Option<(u64,String)>");
        assert_eq!(
            type_tag::<HashMap<String, Vec<deep::inside::Note>>>(),
            "HashMap<String,Vec<Note>>"
        );
        assert_eq!(type_tag::<[u8; 4]>(), "[u8;4]");
        assert_eq!(type_tag::<deep::inside::Note>(), "Note");
    }

    #[test]
    fn fingerprints_are_fixed_values() {
        // FNV-1a test vectors: a change here breaks every stored stream.
        assert_eq!(fingerprint(""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fingerprint("a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fingerprint("foobar"), 0x8594_4171_f739_67e8);
    }
}
