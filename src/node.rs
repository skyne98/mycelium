//! The runtime around raft::Core: elections, replication, snapshots, the
//! disk, membership, map gossip, and the request handlers.
//!
//! Core sits behind a std mutex that is never held across an await. Every
//! state change bumps `changed`; anything waiting re-checks on a bump.

use crate::maps::Maps;
use crate::raft::{
    self, AppendReq, AppendResp, Core, Machine, Member, Outbound, Role, SnapChunk, VoteReq, VoteResp,
};
use crate::transport::{self, parts, Blobs, Identity};
use crate::wal::{self, Snapshot, Wal};
use crate::{Peer, Result};
use bytes::Bytes;
use quinn::{Connection, Endpoint, RecvStream, SendStream};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, watch, Notify};
use tokio::task::JoinSet;

// Client requests: anyone who can reach the node (and passes `allow`).
pub(crate) const APPEND: u8 = 1;
pub(crate) const FOLLOW: u8 = 2;
pub(crate) const LEN: u8 = 3;
pub(crate) const MAP_SET: u8 = 4;
pub(crate) const MAP_REMOVE: u8 = 5;
pub(crate) const MAP_GET: u8 = 6;
pub(crate) const MAP_ENTRIES: u8 = 7;
pub(crate) const MAP_WATCH: u8 = 8;
pub(crate) const MEMBERS: u8 = 9;
pub(crate) const TRIM: u8 = 10;
pub(crate) const FIRST: u8 = 11;
// Peer requests: only from a member of the cluster.
const VOTE: u8 = 20;
const APPEND_ENTRIES: u8 = 21;
const FORWARD: u8 = 22;
const READ_INDEX: u8 = 23;
const MAP_PUSH: u8 = 24;
const MAP_PULL: u8 = 25;
const SNAPSHOT_CHUNK: u8 = 26;
const TIMEOUT_NOW: u8 = 27;
const PROPOSE: u8 = 28;
const JOIN: u8 = 29;
const LEAVE: u8 = 30;

const RPC_TIMEOUT: Duration = Duration::from_secs(2);
/// How long a write or a read keeps trying across leader changes.
pub(crate) const DEADLINE: Duration = Duration::from_secs(10);
const PULL_EVERY: Duration = Duration::from_secs(1);
const FOLLOW_BATCH: usize = 256;
const SNAPSHOT_CHUNK_BYTES: usize = 1 << 20;
const EPHEMERAL_CAP: usize = 100_000;

struct Link {
    peer: Peer,
    cfg: quinn::ClientConfig,
    /// A live connection holds the endpoint's socket open, so shutdown clears
    /// this. Never held across an await; `dial` serializes reconnects.
    conn: Mutex<Option<Connection>>,
    dial: tokio::sync::Mutex<()>,
    kick: Notify,
}

impl Link {
    fn slot(&self) -> MutexGuard<'_, Option<Connection>> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn live(&self) -> Option<Connection> {
        self.slot().clone().filter(|c| c.close_reason().is_none())
    }
}

#[derive(Clone)]
enum Caller {
    /// Not a cluster member: may use streams and maps, if allowed.
    Client(Option<Vec<String>>),
    /// A member. With certificates: the addresses its certificate covers
    /// (several nodes on one machine share a name).
    Peer(Option<Arc<Vec<SocketAddr>>>),
}

pub(crate) struct Config {
    pub bind: SocketAddr,
    pub peers: Vec<Peer>,
    pub identity: Option<Identity>,
    pub dir: PathBuf,
    /// Join a running cluster through these nodes instead of bootstrapping.
    pub join: Option<Vec<Peer>>,
    /// Certificate names allowed to act as clients. None: anyone.
    pub allow: Option<Vec<String>>,
    pub snapshot_every: u64,
}

/// Memory-only streams: ordered by this node, gone on restart.
#[derive(Default)]
struct Ephemeral {
    streams: HashMap<String, (u64, VecDeque<Bytes>)>,
}

pub(crate) struct Shared {
    pub me: SocketAddr,
    core: Mutex<Core>,
    maps: Maps,
    map_tx: broadcast::Sender<(String, Vec<u8>)>,
    ephemeral: Mutex<Ephemeral>,
    changed: watch::Sender<u64>,
    disk_kick: Notify,
    links: Mutex<HashMap<SocketAddr, Arc<Link>>>,
    replicators: Mutex<HashMap<SocketAddr, tokio::task::JoinHandle<()>>>,
    endpoint: Mutex<Option<Endpoint>>,
    stop: watch::Sender<bool>,
    identity: Option<Identity>,
    /// Certificates in use: peer requests must carry a member's certificate.
    secure: bool,
    allow: Option<Vec<String>>,
    /// A snapshot arriving from the leader, chunk by chunk.
    incoming: Mutex<Option<(u64, Vec<u8>)>>,
    /// The membership version links were last built for.
    linked_version: std::sync::atomic::AtomicU64,
    /// While joining: the membership the cluster reported, so this node can
    /// recognise the leader before its own log says who the members are.
    trusted: Mutex<Vec<Member>>,
}

/// A reply frame is `[1] payload` or `[0] error`.
pub(crate) fn reply(out: Vec<u8>) -> Result<Vec<u8>> {
    match out.first() {
        Some(1) => Ok(out[1..].to_vec()),
        Some(0) => Err(String::from_utf8_lossy(&out[1..]).into_owned().into()),
        _ => Err("malformed reply".into()),
    }
}

pub(crate) fn index(out: &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(out.try_into().map_err(|_| "malformed index")?))
}

fn member(p: &Peer, voter: bool) -> Member {
    Member {
        addr: p.addr,
        name: p.name.clone(),
        voter,
    }
}

pub(crate) fn encode_members(c: &[Member]) -> Vec<u8> {
    raft::encode_config(c)
}

pub(crate) fn decode_members(b: &[u8]) -> Result<Vec<Member>> {
    raft::decode_config(b)
}

pub(crate) fn start(cfg: Config) -> Result<Arc<Shared>> {
    let endpoint = match &cfg.identity {
        Some(id) => transport::server_endpoint_cert(cfg.bind, id)?,
        None => transport::server_endpoint_local(cfg.bind)?,
    };
    let solo = cfg.peers.is_empty() && cfg.join.is_none();
    let me = if solo { endpoint.local_addr()? } else { cfg.bind };
    let my_name = cfg.identity.as_ref().and_then(|i| i.name());
    let dir = cfg.dir.join(me.to_string().replace([':', '.', '[', ']'], "_"));
    let (wal, loaded) = Wal::open(&dir.join("raft"))?;
    let maps = Maps::open(dir.join("maps"))?;
    let bootstrap = if cfg.join.is_some() {
        Vec::new()
    } else {
        let mut all = vec![Member {
            addr: me,
            name: my_name,
            voter: true,
        }];
        all.extend(cfg.peers.iter().map(|p| member(p, true)));
        all
    };
    let mut core = Core::new(me, bootstrap, Some(wal), loaded, Instant::now())?;
    core.snapshot_every = cfg.snapshot_every;
    let s = Shared::new(me, core, maps, Some(endpoint.clone()), cfg.identity, cfg.allow);
    for p in cfg.peers.iter().chain(cfg.join.iter().flatten()) {
        s.link_for(p)?;
    }
    s.sync_links();
    tokio::spawn(s.clone().accept(endpoint));
    tokio::spawn(s.clone().elections());
    tokio::spawn(s.clone().disk());
    tokio::spawn(s.clone().snapshots());
    tokio::spawn(s.clone().gossip());
    tokio::spawn(s.clone().expiry());
    if let Some(seeds) = cfg.join {
        tokio::spawn(s.clone().join(seeds));
    }
    s.kick();
    Ok(s)
}

impl Shared {
    fn new(
        me: SocketAddr,
        core: Core,
        maps: Maps,
        endpoint: Option<Endpoint>,
        identity: Option<Identity>,
        allow: Option<Vec<String>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            me,
            core: Mutex::new(core),
            maps,
            map_tx: broadcast::channel(1024).0,
            ephemeral: Mutex::new(Ephemeral::default()),
            changed: watch::channel(0).0,
            disk_kick: Notify::new(),
            links: Mutex::new(HashMap::new()),
            replicators: Mutex::new(HashMap::new()),
            endpoint: Mutex::new(endpoint),
            stop: watch::channel(false).0,
            secure: identity.is_some(),
            identity,
            allow,
            incoming: Mutex::new(None),
            linked_version: std::sync::atomic::AtomicU64::new(u64::MAX),
            trusted: Mutex::new(Vec::new()),
        })
    }

    /// One process, no sockets, no disk.
    pub fn memory() -> Arc<Self> {
        let me = SocketAddr::from(([127, 0, 0, 1], 0));
        let solo = vec![Member {
            addr: me,
            name: None,
            voter: true,
        }];
        let core = Core::new(me, solo, None, Default::default(), Instant::now()).expect("an empty core");
        let s = Self::new(me, core, Maps::memory(), None, None, None);
        tokio::spawn(s.clone().elections());
        tokio::spawn(s.clone().snapshots());
        tokio::spawn(s.clone().expiry());
        s
    }

    fn core(&self) -> MutexGuard<'_, Core> {
        self.core.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn bump(&self) {
        self.changed.send_modify(|v| *v = v.wrapping_add(1));
    }

    fn kick(&self) {
        self.disk_kick.notify_one();
        for l in self.all_links() {
            l.kick.notify_one();
        }
    }

    fn stopped(&self) -> bool {
        *self.stop.borrow()
    }

    pub fn is_leader(&self) -> bool {
        self.core().is_leader()
    }

    pub fn members(&self) -> Vec<Member> {
        self.core().config.clone()
    }

    /// Hand leadership to the most caught-up voter, then stop. Without a
    /// voter to take over, just stop.
    pub async fn shutdown_gracefully(self: &Arc<Self>) {
        if self.is_leader() {
            let _ = self.transfer(Instant::now() + 2 * raft::ELECTION_MIN).await;
        }
        self.shutdown();
    }

    pub fn shutdown(&self) {
        if self.stop.send_replace(true) {
            return;
        }
        trace!("{} shutting down", self.me);
        {
            let mut c = self.core();
            c.stop("node shut down".into());
            c.release_disk();
        }
        let ep = self.endpoint.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(ep) = ep {
            ep.close(0u32.into(), b"shutdown");
        }
        for l in self.all_links() {
            l.slot().take();
        }
        let _ = self.maps.flush();
        self.bump();
    }

    // -------------------------------------------------------------- links

    fn all_links(&self) -> Vec<Arc<Link>> {
        self.links.lock().unwrap_or_else(|e| e.into_inner()).values().cloned().collect()
    }

    fn link(&self, addr: SocketAddr) -> Option<Arc<Link>> {
        self.links.lock().unwrap_or_else(|e| e.into_inner()).get(&addr).cloned()
    }

    fn link_for(&self, p: &Peer) -> Result<Arc<Link>> {
        if let Some(l) = self.link(p.addr) {
            return Ok(l);
        }
        let l = Arc::new(Link {
            peer: p.clone(),
            cfg: transport::client_config(p.name.is_some(), self.identity.as_ref())?,
            conn: Mutex::new(None),
            dial: tokio::sync::Mutex::new(()),
            kick: Notify::new(),
        });
        self.links
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(p.addr, l.clone());
        Ok(l)
    }

    /// Links and replicators follow the membership in the log.
    fn sync_links(self: &Arc<Self>) {
        let members = self.members();
        for m in &members {
            if m.addr != self.me {
                let _ = self.link_for(&Peer {
                    addr: m.addr,
                    name: m.name.clone(),
                });
            }
        }
        let mut reps = self.replicators.lock().unwrap_or_else(|e| e.into_inner());
        reps.retain(|addr, h| {
            let keep = members.iter().any(|m| m.addr == *addr) && !h.is_finished();
            if !keep {
                h.abort();
            }
            keep
        });
        for m in &members {
            if m.addr == self.me || reps.contains_key(&m.addr) {
                continue;
            }
            if let Some(l) = self.link(m.addr) {
                reps.insert(m.addr, tokio::spawn(self.clone().replicate(l)));
            }
        }
    }

    /// Wait for the next state change. Err on stop or at `deadline`.
    async fn wait(&self, rx: &mut watch::Receiver<u64>, deadline: Instant) -> Result<()> {
        if self.stopped() {
            return Err("node stopped".into());
        }
        match tokio::time::timeout_at(deadline.into(), rx.changed()).await {
            Ok(Ok(())) if !self.stopped() => Ok(()),
            Ok(_) => Err("node stopped".into()),
            Err(_) => Err("timed out".into()),
        }
    }

    /// Like wait, but returns after 100 ms anyway: for retrying a peer.
    async fn pause(&self, rx: &mut watch::Receiver<u64>, deadline: Instant) -> Result<()> {
        let soon = deadline.min(Instant::now() + Duration::from_millis(100));
        let _ = tokio::time::timeout_at(soon.into(), rx.changed()).await;
        if self.stopped() {
            return Err("node stopped".into());
        }
        if Instant::now() >= deadline {
            return Err("timed out".into());
        }
        Ok(())
    }

    // ------------------------------------------------------------- writes

    /// Commit a command, from here or via the leader. Retries across leader
    /// changes; appends carry (client, seq), so a retry applies once.
    async fn commit(self: &Arc<Self>, cmd: Bytes, forward: Option<(u8, Vec<u8>)>) -> Result<u64> {
        let deadline = Instant::now() + DEADLINE;
        let mut rx = self.changed.subscribe();
        let mut why = String::from("no leader");
        loop {
            if self.stopped() {
                return Err("node stopped".into());
            }
            let proposed = {
                let mut c = self.core();
                let r = c.propose(cmd.clone());
                if r.is_ok() {
                    c.apply();
                }
                r
            };
            match proposed {
                Ok(done) => {
                    self.bump();
                    self.kick();
                    match tokio::time::timeout_at(deadline.into(), done).await {
                        Ok(Ok(Ok(idx))) => return Ok(idx),
                        Ok(Ok(Err(e))) => why = e,
                        Ok(Err(_)) => why = "node stopped".into(),
                        Err(_) => {
                            return Err(format!(
                                "write not confirmed within {DEADLINE:?}; it may still commit"
                            )
                            .into())
                        }
                    }
                    continue;
                }
                Err(Some(leader)) => match &forward {
                    None => return Err("not the leader".into()),
                    Some((role, body)) => match self.call_peer(leader, *role, body.clone(), DEADLINE).await {
                        Ok(out) => return index(&out),
                        Err(e) => why = e.to_string(),
                    },
                },
                Err(None) => {}
            }
            if self.pause(&mut rx, deadline).await.is_err() {
                return Err(format!("write failed: {why}").into());
            }
        }
    }

    pub async fn append(self: &Arc<Self>, body: Vec<u8>, forward: bool) -> Result<u64> {
        let cmd = raft::append_command(&body, raft::wall_ms())?;
        self.commit(cmd, forward.then_some((FORWARD, body))).await
    }

    pub async fn trim(self: &Arc<Self>, name: &str, before: u64) -> Result<()> {
        let body = parts(&[&[0u8], name.as_bytes(), &before.to_be_bytes()]);
        self.commit(raft::trim_command(name, before), Some((PROPOSE, body)))
            .await
            .map(drop)
    }

    async fn call_peer(&self, addr: SocketAddr, role: u8, body: Vec<u8>, timeout: Duration) -> Result<Vec<u8>> {
        let link = self.link(addr).ok_or("the leader is not a known member")?;
        self.rpc(&link, role, body, timeout).await
    }

    // -------------------------------------------------------------- reads

    /// ReadIndex: an index such that reading once it is applied is
    /// linearizable. The leader confirms it still leads; followers ask it.
    async fn read_index(self: &Arc<Self>, forward: bool, deadline: Instant) -> Result<u64> {
        enum Next {
            Confirm(u64),
            Ask(SocketAddr),
            Wait,
        }
        let mut rx = self.changed.subscribe();
        loop {
            let next = {
                let c = self.core();
                if c.is_leader() {
                    if c.commit >= c.term_start {
                        Next::Confirm(c.commit)
                    } else {
                        Next::Wait
                    }
                } else if !forward {
                    return Err("not the leader".into());
                } else if let Some(l) = c.leader {
                    Next::Ask(l)
                } else {
                    Next::Wait
                }
            };
            match next {
                Next::Confirm(idx) => {
                    let since = Instant::now();
                    self.kick();
                    loop {
                        {
                            let c = self.core();
                            if !c.is_leader() {
                                break;
                            }
                            if c.confirmed_since(since) {
                                return Ok(idx);
                            }
                        }
                        self.wait(&mut rx, deadline).await?;
                    }
                }
                Next::Ask(l) => {
                    if let Ok(out) = self.call_peer(l, READ_INDEX, Vec::new(), RPC_TIMEOUT).await {
                        if let Ok(idx) = index(&out) {
                            return Ok(idx);
                        }
                    }
                    self.pause(&mut rx, deadline).await?;
                }
                Next::Wait => self.pause(&mut rx, deadline).await?,
            }
        }
    }

    async fn applied(&self, idx: u64, deadline: Instant) -> Result<()> {
        let mut rx = self.changed.subscribe();
        loop {
            if self.core().applied >= idx {
                return Ok(());
            }
            self.wait(&mut rx, deadline).await?;
        }
    }

    /// Linearizable: counts every append acknowledged before the call.
    pub async fn len(self: &Arc<Self>, name: &str) -> Result<u64> {
        let deadline = Instant::now() + DEADLINE;
        let idx = self.read_index(true, deadline).await?;
        self.applied(idx, deadline).await?;
        Ok(self.core().machine.len(name))
    }

    /// The first index still held, after trimming and expiry. Linearizable.
    pub async fn first(self: &Arc<Self>, name: &str) -> Result<u64> {
        let deadline = Instant::now() + DEADLINE;
        let idx = self.read_index(true, deadline).await?;
        self.applied(idx, deadline).await?;
        Ok(self.core().machine.first(name))
    }

    /// Committed entries of `name` from `next`, waiting when caught up.
    /// Returns where the batch starts: later than `next` if entries were trimmed.
    async fn batch(&self, name: &str, next: u64, rx: &mut watch::Receiver<u64>) -> Option<(u64, Vec<Bytes>)> {
        loop {
            let (start, b) = self.core().machine.since(name, next, FOLLOW_BATCH);
            if !b.is_empty() {
                return Some((start, b));
            }
            if self.stopped() || rx.changed().await.is_err() {
                return None;
            }
        }
    }

    pub fn follow(self: &Arc<Self>, name: String, from: u64) -> mpsc::Receiver<(u64, Bytes)> {
        let (tx, out) = mpsc::channel(FOLLOW_BATCH);
        let s = self.clone();
        tokio::spawn(async move {
            let mut rx = s.changed.subscribe();
            let mut next = from;
            loop {
                let batch = tokio::select! {
                    b = s.batch(&name, next, &mut rx) => b,
                    _ = tx.closed() => None,
                };
                let Some((start, batch)) = batch else { return };
                next = start;
                for e in batch {
                    if tx.send((next, e)).await.is_err() {
                        return;
                    }
                    next += 1;
                }
            }
        });
        out
    }

    async fn follow_remote(self: &Arc<Self>, body: &[u8], send: &mut SendStream) -> Result<()> {
        let mut b = Blobs::new(body);
        let (name, mut next, ephemeral) = (b.text()?, b.u64()?, b.take()? == [1]);
        if ephemeral {
            let mut rx = self.follow_ephemeral(name, next);
            while let Some((i, e)) = rx.recv().await {
                transport::put(send, &parts(&[&i.to_be_bytes(), &e])).await?;
            }
            return Ok(());
        }
        let mut rx = self.changed.subscribe();
        let gone = send.stopped();
        tokio::pin!(gone);
        loop {
            let batch = tokio::select! {
                b = self.batch(&name, next, &mut rx) => b,
                _ = &mut gone => None,
            };
            let Some((start, batch)) = batch else { return Ok(()) };
            next = start;
            for e in batch {
                transport::put(send, &parts(&[&next.to_be_bytes(), &e])).await?;
                next += 1;
            }
        }
    }

    // --------------------------------------------------- ephemeral streams

    /// Ordered by this node alone: no consensus, no disk, no replication.
    pub fn append_ephemeral(&self, name: &str, payloads: Vec<Bytes>) -> u64 {
        let first = {
            let mut e = self.ephemeral.lock().unwrap_or_else(|e| e.into_inner());
            let (base, q) = e.streams.entry(name.to_string()).or_default();
            let first = *base + q.len() as u64;
            q.extend(payloads);
            // ponytail: a fixed in-memory cap; make it per stream if needed.
            let over = q.len().saturating_sub(EPHEMERAL_CAP);
            q.drain(..over);
            *base += over as u64;
            first
        };
        self.bump();
        first
    }

    pub fn ephemeral_len(&self, name: &str) -> u64 {
        let e = self.ephemeral.lock().unwrap_or_else(|e| e.into_inner());
        e.streams.get(name).map_or(0, |(b, q)| b + q.len() as u64)
    }

    fn ephemeral_since(&self, name: &str, from: u64) -> (u64, Vec<Bytes>) {
        let e = self.ephemeral.lock().unwrap_or_else(|e| e.into_inner());
        match e.streams.get(name) {
            Some((base, q)) => {
                let start = from.max(*base);
                let out = q.iter().skip((start - base) as usize).take(FOLLOW_BATCH).cloned().collect();
                (start, out)
            }
            None => (from, Vec::new()),
        }
    }

    pub fn follow_ephemeral(self: &Arc<Self>, name: String, from: u64) -> mpsc::Receiver<(u64, Bytes)> {
        let (tx, out) = mpsc::channel(FOLLOW_BATCH);
        let s = self.clone();
        tokio::spawn(async move {
            let mut rx = s.changed.subscribe();
            let mut next = from;
            loop {
                let (start, batch) = s.ephemeral_since(&name, next);
                if batch.is_empty() {
                    tokio::select! {
                        r = rx.changed() => if r.is_err() || s.stopped() { return },
                        _ = tx.closed() => return,
                    }
                    continue;
                }
                next = start;
                for e in batch {
                    if tx.send((next, e)).await.is_err() {
                        return;
                    }
                    next += 1;
                }
            }
        });
        out
    }

    // --------------------------------------------------------------- maps

    pub async fn map_set(self: &Arc<Self>, name: &str, key: &str, val: Vec<u8>) -> Result<()> {
        let update = self.maps.set(name, key, val)?;
        let _ = self.map_tx.send((name.to_string(), key.as_bytes().to_vec()));
        self.share(name, update).await
    }

    pub async fn map_remove(self: &Arc<Self>, name: &str, key: &str) -> Result<()> {
        let update = self.maps.remove(name, key)?;
        let _ = self.map_tx.send((name.to_string(), key.as_bytes().to_vec()));
        self.share(name, update).await
    }

    pub fn map_get(&self, name: &str, key: &str) -> Option<Vec<u8>> {
        self.maps.get(name, key)
    }

    pub fn map_entries(&self, name: &str) -> Vec<(String, Vec<u8>)> {
        self.maps.entries(name)
    }

    fn map_import(&self, name: &str, update: &[u8]) -> Result<()> {
        for key in self.maps.import(name, update)? {
            let _ = self.map_tx.send((name.to_string(), key.into_bytes()));
        }
        Ok(())
    }

    /// Every change to `name`, local or remote: (key, new value or None).
    pub fn map_watch(self: &Arc<Self>, name: String) -> mpsc::Receiver<(String, Option<Vec<u8>>)> {
        let (tx, out) = mpsc::channel(256);
        let mut sub = self.map_tx.subscribe();
        let s = self.clone();
        tokio::spawn(async move {
            loop {
                let key = tokio::select! {
                    k = sub.recv() => k,
                    _ = tx.closed() => return,
                };
                let keys = match key {
                    Ok((n, k)) if n == name => vec![String::from_utf8_lossy(&k).into_owned()],
                    Ok(_) => continue,
                    // Fell behind: report every key, so nothing is missed.
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        s.maps.entries(&name).into_iter().map(|(k, _)| k).collect()
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                };
                for k in keys {
                    let v = s.maps.get(&name, &k);
                    if tx.send((k, v)).await.is_err() {
                        return;
                    }
                }
            }
        });
        out
    }

    async fn map_watch_remote(self: &Arc<Self>, body: &[u8], send: &mut SendStream) -> Result<()> {
        let name = Blobs::new(body).text()?;
        let mut rx = self.map_watch(name);
        let gone = send.stopped();
        tokio::pin!(gone);
        loop {
            let change = tokio::select! {
                c = rx.recv() => c,
                _ = &mut gone => None,
            };
            let Some((k, v)) = change else { return Ok(()) };
            let frame = match v {
                Some(v) => parts(&[k.as_bytes(), &[1], &v]),
                None => parts(&[k.as_bytes(), &[0], b""]),
            };
            transport::put(send, &frame).await?;
        }
    }

    /// Push to every member now, and be on local disk before returning.
    async fn share(self: &Arc<Self>, name: &str, update: Vec<u8>) -> Result<()> {
        let body = parts(&[name.as_bytes(), &update]);
        for l in self.all_links() {
            let (s, body) = (self.clone(), body.clone());
            tokio::spawn(async move {
                let _ = s.rpc(&l, MAP_PUSH, body, RPC_TIMEOUT).await;
            });
        }
        let s = self.clone();
        tokio::task::spawn_blocking(move || s.maps.flush())
            .await
            .map_err(|e| e.to_string())?
    }

    /// Anti-entropy: pull whatever a push missed, and snapshot to disk.
    async fn gossip(self: Arc<Self>) {
        let mut stop = self.stop.subscribe();
        loop {
            tokio::select! {
                _ = stop.changed() => return,
                _ = tokio::time::sleep(PULL_EVERY) => {}
            }
            for l in self.all_links() {
                if let Ok(missing) = self.rpc(&l, MAP_PULL, self.maps.versions(), RPC_TIMEOUT).await {
                    for (name, keys) in self.maps.import_all(&missing).unwrap_or_default() {
                        for k in keys {
                            let _ = self.map_tx.send((name.clone(), k.into_bytes()));
                        }
                    }
                }
            }
            let s = self.clone();
            let _ = tokio::task::spawn_blocking(move || s.maps.flush()).await;
        }
    }

    // --------------------------------------------------------- membership

    /// Ask the cluster to add this node, then wait until it is a voter.
    async fn join(self: Arc<Self>, seeds: Vec<Peer>) {
        let me = Member {
            addr: self.me,
            name: self.identity.as_ref().and_then(|i| i.name()),
            voter: true,
        };
        let body = encode_members(std::slice::from_ref(&me));
        let mut stop = self.stop.subscribe();
        loop {
            if self.core().is_voter(self.me) {
                trace!("{} joined the cluster", self.me);
                return;
            }
            for p in &seeds {
                if let Some(l) = self.link(p.addr) {
                    match self.rpc(&l, JOIN, body.clone(), DEADLINE).await {
                        Ok(config) => {
                            if let Ok(c) = decode_members(&config) {
                                for m in c.iter().filter(|m| m.addr != self.me) {
                                    let _ = self.link_for(&Peer {
                                        addr: m.addr,
                                        name: m.name.clone(),
                                    });
                                }
                                *self.trusted.lock().unwrap_or_else(|e| e.into_inner()) = c;
                            }
                            break;
                        }
                        Err(e) => trace!("{} join via {}: {e}", self.me, p.addr),
                    }
                }
            }
            tokio::select! {
                _ = stop.changed() => return,
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
        }
    }

    /// Commit a membership change, one server at a time.
    async fn change(self: &Arc<Self>, edit: impl Fn(&[Member]) -> Option<Vec<Member>>) -> Result<()> {
        let deadline = Instant::now() + DEADLINE;
        let mut rx = self.changed.subscribe();
        loop {
            let proposed = {
                let mut c = self.core();
                if !c.is_leader() {
                    return Err("not the leader".into());
                }
                match edit(&c.config) {
                    None => return Ok(()),
                    Some(next) => c.propose_config(next),
                }
            };
            match proposed {
                Ok(done) => {
                    self.bump();
                    self.sync_links();
                    self.kick();
                    match tokio::time::timeout_at(deadline.into(), done).await {
                        Ok(Ok(Ok(_))) => continue,
                        Ok(Ok(Err(e))) => return Err(e.into()),
                        _ => return Err("membership change not confirmed".into()),
                    }
                }
                Err(e) if e.contains("in progress") => self.pause(&mut rx, deadline).await?,
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Leader side of a join: add as a learner now; catching up and the
    /// promotion to voter follow (thesis 4.2.1), so a slow joiner never
    /// stalls commits. `wait`: return only once it votes.
    async fn admit(self: &Arc<Self>, m: Member, wait: bool) -> Result<()> {
        let learner = Member { voter: false, ..m.clone() };
        self.change(|c| {
            if c.iter().any(|x| x.addr == m.addr) {
                return None;
            }
            let mut next = c.to_vec();
            next.push(learner.clone());
            Some(next)
        })
        .await?;
        if wait {
            return self.promote(m).await;
        }
        let s = self.clone();
        tokio::spawn(async move {
            if let Err(e) = s.promote(m.clone()).await {
                trace!("{} could not promote {}: {e}", s.me, m.addr);
            }
        });
        Ok(())
    }

    async fn promote(self: &Arc<Self>, m: Member) -> Result<()> {
        // A large snapshot can take a while to reach a new node.
        let deadline = Instant::now() + 6 * DEADLINE;
        let mut rx = self.changed.subscribe();
        while !self.core().caught_up(m.addr) {
            self.wait(&mut rx, deadline).await?;
        }
        self.change(|c| {
            let mut next = c.to_vec();
            let x = next.iter_mut().find(|x| x.addr == m.addr)?;
            if x.voter {
                return None;
            }
            x.voter = true;
            Some(next)
        })
        .await
    }

    pub async fn remove_member(self: &Arc<Self>, addr: SocketAddr) -> Result<()> {
        if !self.is_leader() {
            let leader = self.core().leader.ok_or("no leader")?;
            return self
                .call_peer(leader, LEAVE, addr.to_string().into_bytes(), DEADLINE)
                .await
                .map(drop);
        }
        self.change(|c| {
            let mut next = c.to_vec();
            let before = next.len();
            next.retain(|x| x.addr != addr);
            (next.len() != before).then_some(next)
        })
        .await
    }

    pub async fn add_member(self: &Arc<Self>, p: Peer) -> Result<()> {
        let m = member(&p, true);
        if self.is_leader() {
            self.link_for(&p)?;
            return self.admit(m, true).await;
        }
        let leader = self.core().leader.ok_or("no leader")?;
        self.call_peer(leader, JOIN, encode_members(&[m]), DEADLINE).await.map(drop)
    }

    /// Leadership transfer (thesis 3.10): bring the best voter up to date,
    /// tell it to campaign at once, and stop taking writes meanwhile.
    async fn transfer(self: &Arc<Self>, deadline: Instant) -> Result<()> {
        let target = {
            let mut c = self.core();
            let t = c.transfer_target().ok_or("no other voter")?;
            c.transferring = true;
            t
        };
        self.kick();
        let mut rx = self.changed.subscribe();
        while !self.core().caught_up(target) {
            if self.wait(&mut rx, deadline).await.is_err() {
                self.core().transferring = false;
                return Err("transfer target did not catch up".into());
            }
        }
        let term = self.core().term;
        trace!("{} hands leadership to {target}", self.me);
        let _ = self
            .call_peer(target, TIMEOUT_NOW, parts(&[&term.to_be_bytes(), self.me.to_string().as_bytes()]), RPC_TIMEOUT)
            .await;
        while self.is_leader() {
            if self.wait(&mut rx, deadline).await.is_err() {
                self.core().transferring = false;
                return Err("transfer did not complete".into());
            }
        }
        Ok(())
    }

    // ---------------------------------------------------------- elections

    async fn elections(self: Arc<Self>) {
        let mut stop = self.stop.subscribe();
        loop {
            let wake = {
                let c = self.core();
                if c.is_leader() {
                    Instant::now() + raft::HEARTBEAT
                } else {
                    c.deadline
                }
            };
            tokio::select! {
                _ = stop.changed() => return,
                _ = tokio::time::sleep_until(wake.into()) => {}
            }
            let round = {
                let mut c = self.core();
                let now = Instant::now();
                if c.is_leader() {
                    if c.check_quorum(now) {
                        trace!("{} lost touch with a majority, stepping down", self.me);
                        drop(c);
                        self.bump();
                    }
                    continue;
                }
                if c.io_error.is_some() || now < c.deadline || !c.is_voter(self.me) {
                    if now >= c.deadline {
                        c.reset_deadline(now);
                    }
                    continue;
                }
                c.reset_deadline(now);
                c.deadline
            };
            self.campaign(Some(round), false).await;
        }
    }

    /// PreVote, then a real election. `force`: after a transfer, skip both
    /// the PreVote and the voters' stickiness.
    async fn campaign(self: &Arc<Self>, round: Option<Instant>, force: bool) {
        if !force && !self.poll(true, false).await {
            return;
        }
        let term = {
            let mut c = self.core();
            let stale = round.is_some_and(|r| c.deadline != r);
            if stale || c.role == Role::Leader || c.io_error.is_some() {
                return;
            }
            c.start_election(Instant::now());
            c.term
        };
        trace!("{} campaigning in term {term}", self.me);
        if self.persisted(0).await.is_err() {
            return;
        }
        let won = self.poll(false, force).await;
        let mut c = self.core();
        if won && c.role == Role::Candidate && c.term == term {
            c.become_leader(Instant::now());
            c.apply();
            trace!("{} leads term {term}", self.me);
            drop(c);
            self.bump();
            self.sync_links();
            self.kick();
        }
    }

    /// One round of (pre-)votes. True once a majority of voters grants.
    async fn poll(self: &Arc<Self>, pre: bool, force: bool) -> bool {
        let (req, quorum, term, voters, me_votes) = {
            let c = self.core();
            (c.vote_req(pre, force), c.quorum(), c.term, c.other_voters(), c.is_voter(self.me))
        };
        let mut granted = me_votes as usize;
        if granted >= quorum {
            return true;
        }
        let body = req.encode();
        let mut set = JoinSet::new();
        for addr in voters {
            let Some(l) = self.link(addr) else { continue };
            let (s, body) = (self.clone(), body.clone());
            set.spawn(async move { s.rpc(&l, VOTE, body, RPC_TIMEOUT).await });
        }
        while let Some(r) = set.join_next().await {
            let Some(resp) = r.ok().and_then(Result::ok).and_then(|o| VoteResp::decode(&o).ok()) else {
                continue;
            };
            if resp.granted {
                granted += 1;
                if granted >= quorum {
                    return true;
                }
            } else if resp.term > term {
                let mut c = self.core();
                if resp.term > c.term {
                    c.become_follower(resp.term, None);
                }
                drop(c);
                self.bump();
                self.disk_kick.notify_one();
                return false;
            }
        }
        false
    }

    // -------------------------------------------------------- replication

    /// Keeps one follower in sync, one request in flight at a time. Entries
    /// that arrive meanwhile ride in the next request, so batching is free.
    async fn replicate(self: Arc<Self>, link: Arc<Link>) {
        let mut stop = self.stop.subscribe();
        let peer = link.peer.addr;
        loop {
            let out = self.core().outbound(peer);
            let mut again = false;
            match out {
                None => {
                    tokio::select! {
                        _ = stop.changed() => return,
                        _ = link.kick.notified() => {}
                    }
                    continue;
                }
                Some(Outbound::Snapshot) => {
                    again = self.send_snapshot(&link).await;
                }
                Some(Outbound::Append(req)) => {
                    let sent_at = Instant::now();
                    let timeout = if req.entries.is_empty() { RPC_TIMEOUT } else { 2 * RPC_TIMEOUT };
                    if let Ok(out) = self.rpc(&link, APPEND_ENTRIES, req.encode(), timeout).await {
                        if let Ok(resp) = AppendResp::decode(&out) {
                            let moved = {
                                let mut c = self.core();
                                let moved = c.on_append_resp(peer, &req, resp, sent_at, Instant::now());
                                if moved {
                                    c.apply();
                                }
                                again = c.has_work(peer) && (resp.ok || resp.index > 0);
                                moved
                            };
                            self.after_commit(moved);
                        }
                    }
                }
            }
            if again {
                continue;
            }
            tokio::select! {
                _ = stop.changed() => return,
                _ = link.kick.notified() => {}
                _ = tokio::time::sleep(raft::HEARTBEAT) => {}
            }
        }
    }

    fn after_commit(self: &Arc<Self>, moved: bool) {
        let version = self.core().config_version;
        self.bump();
        if moved {
            for l in self.all_links() {
                l.kick.notify_one();
            }
        }
        // Membership may have changed with this commit or this append.
        if self.linked_version.swap(version, std::sync::atomic::Ordering::Relaxed) != version {
            self.sync_links();
        }
    }

    /// Stream the latest snapshot to a follower that is too far behind.
    async fn send_snapshot(self: &Arc<Self>, link: &Link) -> bool {
        let (snap, term) = {
            let c = self.core();
            (c.snapshot.clone(), c.term)
        };
        let Some(snap) = snap else { return false };
        trace!("{} sends snapshot {} to {}", self.me, snap.index, link.peer.addr);
        let sent_at = Instant::now();
        let total = snap.data.len() as u64;
        let mut offset = 0u64;
        loop {
            let end = (offset as usize + SNAPSHOT_CHUNK_BYTES).min(snap.data.len());
            let chunk = SnapChunk {
                term,
                leader: self.me,
                index: snap.index,
                snap_term: snap.term,
                offset,
                total,
                data: Bytes::copy_from_slice(&snap.data[offset as usize..end]),
            };
            let Ok(out) = self.rpc(link, SNAPSHOT_CHUNK, chunk.encode(), 4 * RPC_TIMEOUT).await else {
                return false;
            };
            let Ok(resp) = AppendResp::decode(&out) else { return false };
            if !resp.ok || resp.term > term || end == snap.data.len() {
                let moved = {
                    let mut c = self.core();
                    let m = c.on_snapshot_resp(link.peer.addr, term, snap.index, resp, sent_at, Instant::now());
                    if m {
                        c.apply();
                    }
                    m
                };
                self.after_commit(moved);
                return resp.ok;
            }
            offset = end as u64;
        }
    }

    async fn receive_snapshot(self: &Arc<Self>, chunk: SnapChunk) -> Result<AppendResp> {
        if let Err(term) = self.core().handle_snapshot_chunk(chunk.term, chunk.leader, Instant::now()) {
            return Ok(AppendResp { term, ok: false, index: 0 });
        }
        let complete = {
            let mut slot = self.incoming.lock().unwrap_or_else(|e| e.into_inner());
            if chunk.offset == 0 {
                *slot = Some((chunk.index, Vec::with_capacity(chunk.total.min(1 << 30) as usize)));
            }
            match slot.as_mut() {
                Some((idx, buf)) if *idx == chunk.index && buf.len() as u64 == chunk.offset => {
                    buf.extend_from_slice(&chunk.data);
                    if buf.len() as u64 == chunk.total {
                        slot.take().map(|(_, b)| b)
                    } else {
                        None
                    }
                }
                _ => return Err("snapshot chunk out of order".into()),
            }
        };
        let term = self.core().term;
        let Some(data) = complete else {
            return Ok(AppendResp { term, ok: true, index: 0 });
        };
        let machine = Machine::decode(&data)?;
        let snap = Arc::new(Snapshot {
            index: chunk.index,
            term: chunk.snap_term,
            data,
        });
        let dir = self.core().snapshot_dir();
        if let Some(dir) = dir {
            let s = snap.clone();
            tokio::task::spawn_blocking(move || wal::write_snapshot(&dir, &s))
                .await
                .map_err(|e| e.to_string())??;
        }
        let resp = self.core().install_snapshot(snap, machine);
        trace!("{} installed snapshot {}", self.me, chunk.index);
        self.bump();
        self.sync_links();
        self.persisted(0).await?;
        Ok(resp)
    }

    // --------------------------------------------------------------- disk

    /// Group commit: one fsync covers every entry written since the last.
    async fn disk(self: Arc<Self>) {
        let mut stop = self.stop.subscribe();
        loop {
            let job = self.core().job();
            let Some(job) = job else {
                tokio::select! {
                    _ = stop.changed() => return,
                    _ = self.disk_kick.notified() => {}
                }
                continue;
            };
            let done = tokio::task::spawn_blocking(move || job.run())
                .await
                .unwrap_or_else(|e| Err(std::io::Error::other(e.to_string())));
            let moved = {
                let mut c = self.core();
                c.synced(done);
                if let Some(e) = &c.io_error {
                    trace!("{} stopped: {e}", self.me);
                }
                let moved = c.advance_commit();
                c.apply();
                moved
            };
            self.after_commit(moved);
        }
    }

    /// Compaction: snapshot the state machine and cut the log behind it.
    async fn snapshots(self: Arc<Self>) {
        let mut stop = self.stop.subscribe();
        let mut rx = self.changed.subscribe();
        loop {
            tokio::select! {
                _ = stop.changed() => return,
                r = rx.changed() => if r.is_err() { return },
            }
            let Some(job) = self.core().snapshot_job() else { continue };
            let dir = self.core().snapshot_dir();
            let written = tokio::task::spawn_blocking(move || {
                let snap = job.encode();
                if let Some(dir) = dir {
                    wal::write_snapshot(&dir, &snap)?;
                }
                Ok::<_, std::io::Error>((Arc::new(snap), job.config()))
            })
            .await;
            let mut c = self.core();
            match written {
                Ok(Ok((snap, config))) => {
                    trace!("{} snapshot at {}", self.me, snap.index);
                    c.compact(snap, config);
                }
                Ok(Err(e)) => c.snapshot_failed(e.to_string()),
                Err(e) => c.snapshot_failed(e.to_string()),
            }
        }
    }

    /// The leader proposes trims for entries past their streams' max age.
    async fn expiry(self: Arc<Self>) {
        let mut stop = self.stop.subscribe();
        loop {
            tokio::select! {
                _ = stop.changed() => return,
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
            let due = {
                let c = self.core();
                if !c.is_leader() {
                    continue;
                }
                c.machine.expired(raft::wall_ms())
            };
            for (name, before) in due {
                let _ = self.commit(raft::trim_command(&name, before), None).await;
            }
        }
    }

    /// Term, vote and the log up to `idx` are durable.
    async fn persisted(&self, idx: u64) -> Result<()> {
        let mut rx = self.changed.subscribe();
        let deadline = Instant::now() + DEADLINE;
        loop {
            {
                let c = self.core();
                if let Some(e) = &c.io_error {
                    return Err(e.clone().into());
                }
                if c.persisted(idx) {
                    return Ok(());
                }
            }
            self.disk_kick.notify_one();
            self.wait(&mut rx, deadline).await?;
        }
    }

    // ------------------------------------------------------------ network

    async fn conn(&self, link: &Link) -> Result<Connection> {
        if let Some(c) = link.live() {
            return Ok(c);
        }
        let _one = link.dial.lock().await;
        if let Some(c) = link.live() {
            return Ok(c);
        }
        let ep = self
            .endpoint
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or("node stopped")?;
        let c = ep
            .connect_with(link.cfg.clone(), link.peer.addr, link.peer.server_name())?
            .await?;
        if self.stopped() {
            return Err("node stopped".into());
        }
        *link.slot() = Some(c.clone());
        Ok(c)
    }

    /// One request on its own stream. Dialing gets at most RPC_TIMEOUT, so a
    /// dead peer costs that, not a QUIC handshake timeout and not the whole
    /// request budget; the request itself gets the rest of `timeout`.
    async fn rpc(&self, link: &Link, role: u8, body: Vec<u8>, timeout: Duration) -> Result<Vec<u8>> {
        let call = async {
            let conn = match tokio::time::timeout(RPC_TIMEOUT.min(timeout), self.conn(link)).await {
                Ok(c) => c?,
                Err(_) => return Err(format!("cannot reach {}", link.peer.addr).into()),
            };
            let (mut send, mut recv) = conn.open_bi().await?;
            send.write_all(&[role]).await?;
            transport::put(&mut send, &body).await?;
            let _ = send.finish();
            transport::get(&mut recv).await
        };
        match tokio::time::timeout(timeout, call).await {
            Ok(Ok(out)) => reply(out),
            Ok(Err(e)) => {
                link.slot().take();
                Err(e)
            }
            Err(_) => {
                link.slot().take();
                Err(format!("{} did not answer within {timeout:?}", link.peer.addr).into())
            }
        }
    }

    fn caller(&self, names: &[String], loopback: bool) -> Caller {
        if !self.secure {
            // Unverified mode only runs on loopback, where any process could
            // speak for a peer anyway.
            return if loopback {
                Caller::Peer(None)
            } else {
                Caller::Client(None)
            };
        }
        let names = names.to_vec();
        let known: Vec<Member> = {
            let c = self.core();
            let trusted = self.trusted.lock().unwrap_or_else(|e| e.into_inner());
            c.config.iter().chain(trusted.iter()).cloned().collect()
        };
        let covered: Vec<SocketAddr> = known
            .iter()
            .filter(|m| m.name.as_ref().is_some_and(|n| names.iter().any(|x| x.eq_ignore_ascii_case(n))))
            .map(|m| m.addr)
            .collect();
        if covered.is_empty() {
            Caller::Client(Some(names))
        } else {
            Caller::Peer(Some(Arc::new(covered)))
        }
    }

    fn allowed(&self, caller: &Caller) -> bool {
        let Some(allow) = &self.allow else { return true };
        match caller {
            Caller::Peer(_) => true,
            Caller::Client(None) => false,
            Caller::Client(Some(names)) => names
                .iter()
                .any(|n| allow.iter().any(|a| a.eq_ignore_ascii_case(n))),
        }
    }

    async fn accept(self: Arc<Self>, ep: Endpoint) {
        while let Some(incoming) = ep.accept().await {
            let s = self.clone();
            tokio::spawn(async move {
                let Ok(conn) = incoming.await else { return };
                let names: Arc<Vec<String>> = Arc::new(transport::caller_names(&conn));
                let loopback = conn.remote_address().ip().is_loopback();
                // Membership can change while the connection lives, so the
                // caller is looked up per request.
                while let Ok((send, recv)) = conn.accept_bi().await {
                    let (s, caller, names) = (s.clone(), s.caller(&names, loopback), names.clone());
                    tokio::spawn(async move {
                        let _ = s.serve(caller, names, send, recv).await;
                    });
                }
            });
        }
    }

    async fn serve(
        self: Arc<Self>,
        caller: Caller,
        names: Arc<Vec<String>>,
        mut send: SendStream,
        mut recv: RecvStream,
    ) -> Result<()> {
        let mut role = [0u8];
        recv.read_exact(&mut role).await?;
        let body = transport::get(&mut recv).await?;
        let reply = if role[0] == JOIN {
            Box::pin(self.handle_join(&names, &body)).await
        } else if !self.allowed(&caller) {
            Err("this client's certificate is not allowed".into())
        } else if role[0] == FOLLOW {
            return self.follow_remote(&body, &mut send).await;
        } else if role[0] == MAP_WATCH {
            return self.map_watch_remote(&body, &mut send).await;
        } else {
            Box::pin(self.dispatch(caller, role[0], body)).await
        };
        let frame = match reply {
            Ok(p) => [&[1u8][..], &p].concat(),
            Err(e) => [&[0u8][..], e.to_string().as_bytes()].concat(),
        };
        transport::put(&mut send, &frame).await?;
        let _ = send.finish();
        Ok(())
    }

    async fn dispatch(self: &Arc<Self>, caller: Caller, role: u8, body: Vec<u8>) -> Result<Vec<u8>> {
        let mut b = Blobs::new(&body);
        match role {
            APPEND => {
                let ephemeral = b.take()? == [1];
                let rest = &body[4 + 1..];
                if ephemeral {
                    let mut r = Blobs::new(rest);
                    let name = r.text()?;
                    let mut payloads = Vec::new();
                    while !r.is_empty() {
                        payloads.push(Bytes::copy_from_slice(r.take()?));
                    }
                    return Ok(self.append_ephemeral(&name, payloads).to_be_bytes().to_vec());
                }
                return Ok(self.append(rest.to_vec(), true).await?.to_be_bytes().to_vec());
            }
            LEN => {
                let (name, ephemeral) = (b.text()?, b.take()? == [1]);
                let n = if ephemeral { self.ephemeral_len(&name) } else { self.len(&name).await? };
                return Ok(n.to_be_bytes().to_vec());
            }
            FIRST => return Ok(self.first(&b.text()?).await?.to_be_bytes().to_vec()),
            TRIM => {
                let (name, before) = (b.text()?, b.u64()?);
                self.trim(&name, before).await?;
                return Ok(Vec::new());
            }
            MAP_SET => {
                let (name, key) = (b.text()?, b.text()?);
                self.map_set(&name, &key, b.take()?.to_vec()).await?;
                return Ok(Vec::new());
            }
            MAP_REMOVE => {
                let (name, key) = (b.text()?, b.text()?);
                self.map_remove(&name, &key).await?;
                return Ok(Vec::new());
            }
            MAP_GET => {
                let (name, key) = (b.text()?, b.text()?);
                return Ok(match self.maps.get(&name, &key) {
                    Some(v) => [&[1u8][..], &v].concat(),
                    None => vec![0],
                });
            }
            MAP_ENTRIES => {
                let e = self.maps.entries(&b.text()?);
                let pieces: Vec<&[u8]> = e.iter().flat_map(|(k, v)| [k.as_bytes(), v.as_slice()]).collect();
                return Ok(parts(&pieces));
            }
            MEMBERS => return Ok(encode_members(&self.members())),
            _ => {}
        }
        let Caller::Peer(who) = caller else {
            return Err("peer requests need a member's certificate".into());
        };
        let forged = |claimed: SocketAddr| who.as_ref().is_some_and(|w| !w.contains(&claimed));
        match role {
            VOTE => {
                let req = VoteReq::decode(&body)?;
                if forged(req.candidate) {
                    return Err("candidate does not match the caller's certificate".into());
                }
                let resp = self.core().handle_vote(req, Instant::now());
                self.bump();
                self.persisted(0).await?;
                Ok(resp.encode())
            }
            APPEND_ENTRIES => {
                let req = AppendReq::decode(&Bytes::from(body))?;
                if forged(req.leader) {
                    return Err("leader does not match the caller's certificate".into());
                }
                let resp = {
                    let mut c = self.core();
                    let r = c.handle_append(req, Instant::now());
                    c.apply();
                    r
                };
                self.after_commit(false);
                self.persisted(if resp.ok { resp.index } else { 0 }).await?;
                Ok(resp.encode())
            }
            SNAPSHOT_CHUNK => {
                let chunk = SnapChunk::decode(&Bytes::from(body))?;
                if forged(chunk.leader) {
                    return Err("leader does not match the caller's certificate".into());
                }
                Ok(self.receive_snapshot(chunk).await?.encode())
            }
            TIMEOUT_NOW => {
                let (term, leader) = (b.u64()?, b.text()?.parse()?);
                if forged(leader) {
                    return Err("leader does not match the caller's certificate".into());
                }
                if self.core().timeout_now(term, leader) {
                    let s = self.clone();
                    tokio::spawn(async move { s.campaign(None, true).await });
                }
                Ok(Vec::new())
            }
            FORWARD => Ok(self.append(body, false).await?.to_be_bytes().to_vec()),
            PROPOSE => {
                let (_, name, before) = (b.take()?, b.text()?, b.u64()?);
                Ok(self.commit(raft::trim_command(&name, before), None).await?.to_be_bytes().to_vec())
            }
            READ_INDEX => {
                let idx = self.read_index(false, Instant::now() + RPC_TIMEOUT).await?;
                Ok(idx.to_be_bytes().to_vec())
            }
            MAP_PUSH => {
                let name = b.text()?;
                self.map_import(&name, b.take()?)?;
                Ok(Vec::new())
            }
            MAP_PULL => self.maps.missing(&body),
            LEAVE => {
                let addr: SocketAddr = String::from_utf8_lossy(&body).parse()?;
                Box::pin(self.remove_member(addr)).await?;
                Ok(Vec::new())
            }
            _ => Err(format!("unknown request {role}").into()),
        }
    }

    /// Joins arrive from nodes that are not members yet, so they are checked
    /// against the certificate, not the membership.
    async fn handle_join(self: &Arc<Self>, conn_names: &[String], body: &[u8]) -> Result<Vec<u8>> {
        let m = decode_members(body)?.into_iter().next().ok_or("empty join")?;
        if self.secure {
            let name = m.name.as_deref().ok_or("a joining node needs a certificate name")?;
            if !conn_names.iter().any(|n| n.eq_ignore_ascii_case(name)) {
                return Err("join does not match the caller's certificate".into());
            }
            if !self.allowed(&Caller::Client(Some(conn_names.to_vec()))) {
                return Err("this node's certificate is not allowed to join".into());
            }
        }
        if !self.is_leader() {
            let leader = self.core().leader.ok_or("no leader")?;
            return self.call_peer(leader, JOIN, body.to_vec(), DEADLINE).await;
        }
        self.link_for(&Peer { addr: m.addr, name: m.name.clone() })?;
        self.admit(m, false).await?;
        Ok(encode_members(&self.members()))
    }
}
