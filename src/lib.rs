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
//! only listed peers can vote or replicate. Without TLS everything must be on
//! loopback, by construction.

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

// ponytail: FNV-1a of the type's name. Stable across runs and machines built
// by the same compiler; rustc may spell names differently across versions.
fn fingerprint<T>() -> u64 {
    std::any::type_name::<T>()
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325, |h, b| {
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

struct Remote {
    addr: SocketAddr,
    name: String,
    endpoint: Endpoint,
    cfg: quinn::ClientConfig,
    conn: tokio::sync::Mutex<Option<Connection>>,
}

impl Remote {
    async fn conn(&self) -> Result<Connection> {
        let mut slot = self.conn.lock().await;
        if let Some(c) = slot.as_ref().filter(|c| c.close_reason().is_none()) {
            return Ok(c.clone());
        }
        let c = self
            .endpoint
            .connect_with(self.cfg.clone(), self.addr, &self.name)?
            .await?;
        *slot = Some(c.clone());
        Ok(c)
    }

    async fn once(&self, role: u8, body: &[u8]) -> Result<Vec<u8>> {
        let conn = self.conn().await?;
        if !tracing() {
            return self.exchange(&conn, role, body).await;
        }
        let started = Instant::now();
        let before = conn.stats().path;
        let out = self.exchange(&conn, role, body).await;
        if started.elapsed() > Duration::from_millis(40) {
            let after = conn.stats().path;
            trace!(
                "slow request {role} to {}: {:?}, lost {} -> {}, sent {} -> {}, rtt {:?}",
                self.addr,
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

    /// Retries lost connections until the deadline. Safe for every request:
    /// appends carry (client, seq), map writes are idempotent, reads are reads.
    async fn call(&self, role: u8, body: Vec<u8>) -> Result<Vec<u8>> {
        let deadline = Instant::now() + node::DEADLINE;
        loop {
            match self.once(role, &body).await {
                Ok(frame) => return node::reply(frame),
                Err(e) => {
                    *self.conn.lock().await = None;
                    if Instant::now() + Duration::from_millis(200) > deadline {
                        return Err(e);
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    async fn open_follow(
        &self,
        name: &str,
        from: u64,
    ) -> Result<(quinn::SendStream, quinn::RecvStream)> {
        let (mut send, recv) = self.conn().await?.open_bi().await?;
        send.write_all(&[node::FOLLOW]).await?;
        transport::put(&mut send, &parts(&[name.as_bytes(), &from.to_be_bytes()])).await?;
        Ok((send, recv))
    }

    /// Survives the node restarting: reconnects and resumes where it stopped.
    async fn follow(
        self: Arc<Self>,
        name: String,
        from: u64,
    ) -> Result<mpsc::Receiver<(u64, Bytes)>> {
        let (tx, rx) = mpsc::channel(256);
        let mut stream = self.open_follow(&name, from).await?;
        tokio::spawn(async move {
            let mut next = from;
            loop {
                loop {
                    let frame = tokio::select! {
                        f = transport::get(&mut stream.1) => f,
                        _ = tx.closed() => return,
                    };
                    let Ok(frame) = frame else { break };
                    let mut b = Blobs::new(&frame);
                    let (Ok(idx), Ok(entry)) = (b.u64(), b.take()) else {
                        break;
                    };
                    if idx != next {
                        break;
                    }
                    if tx.send((idx, Bytes::copy_from_slice(entry))).await.is_err() {
                        return;
                    }
                    next += 1;
                }
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(200)) => {}
                        _ = tx.closed() => return,
                    }
                    *self.conn.lock().await = None;
                    if let Ok(s) = self.open_follow(&name, next).await {
                        stream = s;
                        break;
                    }
                }
            }
        });
        Ok(rx)
    }
}

#[derive(Clone)]
enum Backend {
    Local(Arc<Shared>),
    Remote(Arc<Remote>),
}

impl Backend {
    async fn append(&self, body: Vec<u8>) -> Result<u64> {
        match self {
            Backend::Local(s) => s.append(body, true).await,
            Backend::Remote(r) => node::index(&r.call(node::APPEND, body).await?),
        }
    }

    async fn len(&self, name: &str) -> Result<u64> {
        match self {
            Backend::Local(s) => s.len(name).await,
            Backend::Remote(r) => node::index(&r.call(node::LEN, parts(&[name.as_bytes()])).await?),
        }
    }

    async fn follow(&self, name: &str, from: u64) -> Result<mpsc::Receiver<(u64, Bytes)>> {
        match self {
            Backend::Local(s) => Ok(s.follow(name.to_string(), from)),
            Backend::Remote(r) => r.clone().follow(name.to_string(), from).await,
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
}

// ------------------------------------------------------------------ handles

/// An ordered log. Every node sees the same entries at the same indices.
pub struct Stream<T> {
    name: String,
    fp: u64,
    db: Mycelium,
    skipped: Arc<AtomicU64>,
    _t: PhantomData<fn() -> T>,
}

impl<T> Stream<T> {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Entries a watcher skipped because they were written as another type.
    pub fn skipped(&self) -> u64 {
        self.skipped.load(Ordering::Relaxed)
    }

    /// Linearizable: includes every append acknowledged before the call.
    pub async fn len(&self) -> Result<u64> {
        self.db.backend.len(&self.name).await
    }

    async fn push(&self, payloads: &[&[u8]]) -> Result<u64> {
        let seq = self.db.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let body = raft::append_body(&self.name, self.db.client, seq, payloads);
        self.db.backend.append(body).await
    }

    pub async fn append_raw(&self, bytes: &[u8]) -> Result<u64> {
        let mut out = 0u64.to_be_bytes().to_vec();
        out.extend_from_slice(bytes);
        self.push(&[&out]).await
    }

    pub async fn watch_raw(&self, from: u64) -> Result<mpsc::Receiver<(u64, Raw)>> {
        let mut raw = self.db.backend.follow(&self.name, from).await?;
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
    /// Returns once a majority holds the entry on disk.
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
    pub async fn watch(&self, from: u64) -> Result<mpsc::Receiver<(u64, T)>>
    where
        T: Send + 'static,
    {
        let mut raw = self.db.backend.follow(&self.name, from).await?;
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
    db: Mycelium,
    _t: PhantomData<fn() -> (K, V)>,
}

impl<K, V> Map<K, V> {
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> Map<K, V> {
    /// Returns once the write is on this node's disk; peers get it by push now
    /// and by pull within a second, even after a partition heals.
    pub async fn set(&self, key: &K, value: &V) -> Result<()> {
        let v = encode(fingerprint::<V>(), value)?;
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
            Some(bytes) => decode::<V>(fingerprint::<V>(), &bytes),
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
                decode::<V>(fingerprint::<V>(), &v),
            ) else {
                continue;
            };
            out.push((k, v));
        }
        Ok(out)
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
            fp: fingerprint::<T>(),
            db: self.clone(),
            skipped: Arc::new(AtomicU64::new(0)),
            _t: PhantomData,
        }
    }

    pub fn map<K: Serialize, V: Serialize + DeserializeOwned>(&self, name: &str) -> Map<K, V> {
        Map {
            name: name.to_string(),
            db: self.clone(),
            _t: PhantomData,
        }
    }
}

/// How to reach a node.
#[derive(Clone, Debug)]
pub struct Client {
    addr: SocketAddr,
    server_name: Option<String>,
}

impl Client {
    /// Unverified, loopback only.
    pub fn to(addr: SocketAddr) -> Self {
        Self {
            addr,
            server_name: None,
        }
    }

    /// Verified against the system trust store under `name`.
    pub fn verified(addr: SocketAddr, name: impl Into<String>) -> Self {
        Self {
            addr,
            server_name: Some(name.into()),
        }
    }

    pub async fn connect(self) -> Result<Mycelium> {
        if self.server_name.is_none() && !self.addr.ip().is_loopback() {
            return Err(format!(
                "refusing unverified TLS off loopback ({}); use Client::verified",
                self.addr
            )
            .into());
        }
        let remote = Remote {
            addr: self.addr,
            name: self
                .server_name
                .clone()
                .unwrap_or_else(|| "localhost".into()),
            endpoint: transport::client_endpoint(self.addr)?,
            cfg: transport::client_config(self.server_name.is_some(), None)?,
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
    data_dir: PathBuf,
}

impl Node {
    /// A node with no peers: its own cluster, leading immediately.
    pub fn bind(addr: SocketAddr) -> Self {
        Self {
            bind: addr,
            tls: None,
            peers: Vec::new(),
            data_dir: default_data_dir(),
        }
    }

    /// A node in a cluster. `addr` is where the other nodes reach this one;
    /// each node lists every other node as a peer.
    pub fn cluster(addr: SocketAddr, peers: Vec<Peer>) -> Self {
        Self {
            peers,
            ..Self::bind(addr)
        }
    }

    /// Serve this certificate (e.g. from `tailscale cert`) and present it to peers.
    pub fn with_tls(mut self, cert: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Self {
        self.tls = Some((cert.into(), key.into()));
        self
    }

    /// Where the log, (term, vote) and maps live. Default:
    /// `$XDG_DATA_HOME/mycelium`, else `~/.local/share/mycelium`. Never a temp
    /// dir: a node that forgets its vote on reboot can vote twice in a term.
    pub fn with_data_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.data_dir = dir.into();
        self
    }

    fn check(&self) -> Result<()> {
        if !self.peers.is_empty() && self.bind.ip().is_unspecified() {
            return Err(format!(
                "a cluster node binds the address its peers use, not {}",
                self.bind
            )
            .into());
        }
        if self.tls.is_some() {
            if let Some(p) = self.peers.iter().find(|p| p.name.is_none()) {
                return Err(
                    format!("peer {} needs a certificate name (Peer::verified)", p.addr).into(),
                );
            }
            return Ok(());
        }
        if !self.bind.ip().is_loopback() {
            return Err(format!(
                "refusing unverified TLS off loopback ({}); use Node::with_tls",
                self.bind
            )
            .into());
        }
        if let Some(p) = self
            .peers
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
        })?;
        Ok(Running {
            addr: shared.me,
            shared,
        })
    }

    /// Start and run until the process ends.
    pub async fn serve(self) -> Result<()> {
        let _running = self.start().await?;
        std::future::pending::<()>().await;
        Ok(())
    }
}

fn default_data_dir() -> PathBuf {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share")))
        .unwrap_or_else(std::env::temp_dir);
    base.join("mycelium")
}

/// A running node. Dropping it shuts it down.
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

    /// Stop serving, release the disk, close every connection. Idempotent.
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
