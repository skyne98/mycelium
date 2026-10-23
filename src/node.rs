//! The runtime around raft::Core: elections, replication, the disk, map
//! gossip, and the request handlers for clients and peers.
//!
//! Core sits behind a std mutex that is never held across an await. Every
//! state change bumps `changed`; anything waiting re-checks on a bump.

use crate::maps::Maps;
use crate::raft::{self, AppendReq, AppendResp, Core, Role, VoteReq, VoteResp};
use crate::transport::{self, parts, Blobs, Identity};
use crate::wal::Wal;
use crate::{Peer, Result};
use bytes::Bytes;
use quinn::{Connection, Endpoint, RecvStream, SendStream};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch, Notify};
use tokio::task::JoinSet;

// Client requests: anyone who can reach the node.
pub(crate) const APPEND: u8 = 1;
pub(crate) const FOLLOW: u8 = 2;
pub(crate) const LEN: u8 = 3;
pub(crate) const MAP_SET: u8 = 4;
pub(crate) const MAP_REMOVE: u8 = 5;
pub(crate) const MAP_GET: u8 = 6;
pub(crate) const MAP_ENTRIES: u8 = 7;
// Peer requests: only from a configured peer.
const VOTE: u8 = 20;
const APPEND_ENTRIES: u8 = 21;
const FORWARD: u8 = 22;
const READ_INDEX: u8 = 23;
const MAP_PUSH: u8 = 24;
const MAP_PULL: u8 = 25;

const RPC_TIMEOUT: Duration = Duration::from_secs(2);
/// How long a write or a read keeps trying across leader changes.
pub(crate) const DEADLINE: Duration = Duration::from_secs(10);
const PULL_EVERY: Duration = Duration::from_secs(1);
const FOLLOW_BATCH: usize = 256;

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
    Client,
    /// A peer. With certificates: the peer addresses its certificate covers
    /// (several nodes on one machine share a name).
    Peer(Option<Arc<Vec<SocketAddr>>>),
}

pub(crate) struct Config {
    pub bind: SocketAddr,
    pub peers: Vec<Peer>,
    pub identity: Option<Identity>,
    pub dir: PathBuf,
}

pub(crate) struct Shared {
    pub me: SocketAddr,
    core: Mutex<Core>,
    maps: Maps,
    changed: watch::Sender<u64>,
    disk_kick: Notify,
    links: Vec<Arc<Link>>,
    endpoint: Mutex<Option<Endpoint>>,
    stop: watch::Sender<bool>,
    /// Certificates in use: peer requests must carry a peer's certificate.
    secure: bool,
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
    Ok(u64::from_be_bytes(
        out.try_into().map_err(|_| "malformed index")?,
    ))
}

/// A cluster of one elects itself.
fn lead_alone(c: &mut Core) {
    let now = Instant::now();
    c.start_election(now);
    c.become_leader(now);
    c.apply();
}

pub(crate) fn start(cfg: Config) -> Result<Arc<Shared>> {
    let endpoint = match &cfg.identity {
        Some(id) => transport::server_endpoint_cert(cfg.bind, id)?,
        None => transport::server_endpoint_local(cfg.bind)?,
    };
    let me = if cfg.peers.is_empty() {
        endpoint.local_addr()?
    } else {
        cfg.bind
    };
    let dir = cfg
        .dir
        .join(me.to_string().replace([':', '.', '[', ']'], "_"));
    let disk = Wal::open(&dir.join("raft"))?;
    let maps = Maps::open(dir.join("maps"))?;
    let links = cfg
        .peers
        .iter()
        .map(|p| {
            Ok(Arc::new(Link {
                peer: p.clone(),
                cfg: transport::client_config(p.name.is_some(), cfg.identity.as_ref())?,
                conn: Mutex::new(None),
                dial: tokio::sync::Mutex::new(()),
                kick: Notify::new(),
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let addrs = cfg.peers.iter().map(|p| p.addr).collect();
    let mut core = Core::new(me, addrs, Some(disk), Instant::now());
    if links.is_empty() {
        lead_alone(&mut core);
    }
    let s = Shared::new(
        me,
        core,
        maps,
        links,
        Some(endpoint.clone()),
        cfg.identity.is_some(),
    );
    tokio::spawn(s.clone().accept(endpoint));
    tokio::spawn(s.clone().elections());
    tokio::spawn(s.clone().disk());
    tokio::spawn(s.clone().gossip());
    for l in &s.links {
        tokio::spawn(s.clone().replicate(l.clone()));
    }
    s.kick();
    Ok(s)
}

impl Shared {
    fn new(
        me: SocketAddr,
        core: Core,
        maps: Maps,
        links: Vec<Arc<Link>>,
        endpoint: Option<Endpoint>,
        secure: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            me,
            core: Mutex::new(core),
            maps,
            changed: watch::channel(0).0,
            disk_kick: Notify::new(),
            links,
            endpoint: Mutex::new(endpoint),
            stop: watch::channel(false).0,
            secure,
        })
    }

    /// One process, no sockets, no disk.
    pub fn memory() -> Arc<Self> {
        let me = SocketAddr::from(([127, 0, 0, 1], 0));
        let mut core = Core::new(me, Vec::new(), None, Instant::now());
        lead_alone(&mut core);
        Self::new(me, core, Maps::memory(), Vec::new(), None, false)
    }

    fn core(&self) -> MutexGuard<'_, Core> {
        self.core.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn bump(&self) {
        self.changed.send_modify(|v| *v = v.wrapping_add(1));
    }

    fn kick(&self) {
        self.disk_kick.notify_one();
        for l in &self.links {
            l.kick.notify_one();
        }
    }

    fn stopped(&self) -> bool {
        *self.stop.borrow()
    }

    pub fn is_leader(&self) -> bool {
        self.core().is_leader()
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
        let ep = self
            .endpoint
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(ep) = ep {
            ep.close(0u32.into(), b"shutdown");
        }
        for l in &self.links {
            l.slot().take();
        }
        let _ = self.maps.flush();
        self.bump();
    }

    fn link(&self, addr: SocketAddr) -> Option<&Arc<Link>> {
        self.links.iter().find(|l| l.peer.addr == addr)
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

    /// Commit an append, from here or via the leader. Retries across leader
    /// changes; the (client, seq) in the body makes a retry apply once.
    pub async fn append(self: &Arc<Self>, body: Vec<u8>, forward: bool) -> Result<u64> {
        let cmd = raft::append_command(&body)?;
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
                Err(_) if !forward => return Err("not the leader".into()),
                Err(Some(leader)) => match self.forward(leader, &body).await {
                    Ok(idx) => return Ok(idx),
                    Err(e) => why = e.to_string(),
                },
                Err(None) => {}
            }
            if self.pause(&mut rx, deadline).await.is_err() {
                return Err(format!("write failed: {why}").into());
            }
        }
    }

    async fn forward(&self, leader: SocketAddr, body: &[u8]) -> Result<u64> {
        let link = self
            .link(leader)
            .ok_or("the leader is not in the peer list")?;
        index(&self.rpc(link, FORWARD, body.to_vec(), DEADLINE).await?)
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
                    if let Some(link) = self.link(l) {
                        if let Ok(out) = self.rpc(link, READ_INDEX, Vec::new(), RPC_TIMEOUT).await {
                            if let Ok(idx) = index(&out) {
                                return Ok(idx);
                            }
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

    /// Committed entries of `name` from `next`, waiting when caught up.
    /// None once the node stops.
    async fn batch(
        &self,
        name: &str,
        next: u64,
        rx: &mut watch::Receiver<u64>,
    ) -> Option<Vec<Bytes>> {
        loop {
            let b = self.core().machine.since(name, next, FOLLOW_BATCH);
            if !b.is_empty() {
                return Some(b);
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
                let Some(batch) = batch else { return };
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
        let (name, mut next) = (b.text()?, b.u64()?);
        let mut rx = self.changed.subscribe();
        let gone = send.stopped();
        tokio::pin!(gone);
        loop {
            let batch = tokio::select! {
                b = self.batch(&name, next, &mut rx) => b,
                _ = &mut gone => None,
            };
            let Some(batch) = batch else { return Ok(()) };
            for e in batch {
                transport::put(send, &parts(&[&next.to_be_bytes(), &e])).await?;
                next += 1;
            }
        }
    }

    // --------------------------------------------------------------- maps

    pub async fn map_set(self: &Arc<Self>, name: &str, key: &str, val: Vec<u8>) -> Result<()> {
        let update = self.maps.set(name, key, val)?;
        self.share(name, update).await
    }

    pub async fn map_remove(self: &Arc<Self>, name: &str, key: &str) -> Result<()> {
        let update = self.maps.remove(name, key)?;
        self.share(name, update).await
    }

    pub fn map_get(&self, name: &str, key: &str) -> Option<Vec<u8>> {
        self.maps.get(name, key)
    }

    pub fn map_entries(&self, name: &str) -> Vec<(String, Vec<u8>)> {
        self.maps.entries(name)
    }

    /// Push to every peer now, and be on local disk before returning.
    async fn share(self: &Arc<Self>, name: &str, update: Vec<u8>) -> Result<()> {
        let body = parts(&[name.as_bytes(), &update]);
        for l in &self.links {
            let (s, l, body) = (self.clone(), l.clone(), body.clone());
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
            for l in &self.links {
                if let Ok(missing) = self
                    .rpc(l, MAP_PULL, self.maps.versions(), RPC_TIMEOUT)
                    .await
                {
                    let _ = self.maps.import_missing(&missing);
                }
            }
            let s = self.clone();
            let _ = tokio::task::spawn_blocking(move || s.maps.flush()).await;
        }
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
                if c.io_error.is_some() || now < c.deadline {
                    continue;
                }
                c.reset_deadline(now);
                c.deadline
            };
            if !self.poll(true).await {
                continue;
            }
            let term = {
                let mut c = self.core();
                if c.deadline != round || c.role == Role::Leader || c.io_error.is_some() {
                    continue;
                }
                c.start_election(Instant::now());
                c.term
            };
            trace!("{} campaigning in term {term}", self.me);
            if self.persisted(0).await.is_err() {
                continue;
            }
            let won = self.poll(false).await;
            let mut c = self.core();
            if won && c.role == Role::Candidate && c.term == term {
                c.become_leader(Instant::now());
                trace!("{} leads term {term}", self.me);
                drop(c);
                self.bump();
                self.kick();
            }
        }
    }

    /// One round of (pre-)votes. True once a majority grants.
    async fn poll(self: &Arc<Self>, pre: bool) -> bool {
        let (req, quorum, term) = {
            let c = self.core();
            (c.vote_req(pre), c.quorum(), c.term)
        };
        let mut granted = 1;
        if granted >= quorum {
            return true;
        }
        let body = req.encode();
        let mut set = JoinSet::new();
        for l in &self.links {
            let (s, l, body) = (self.clone(), l.clone(), body.clone());
            set.spawn(async move { s.rpc(&l, VOTE, body, RPC_TIMEOUT).await });
        }
        while let Some(r) = set.join_next().await {
            let Some(resp) = r
                .ok()
                .and_then(Result::ok)
                .and_then(|o| VoteResp::decode(&o).ok())
            else {
                continue;
            };
            if resp.granted {
                granted += 1;
                if granted >= quorum {
                    return true;
                }
            } else if resp.term > term {
                // Someone is ahead: adopt its term, as for any newer message.
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
        loop {
            let req = self.core().append_req(link.peer.addr);
            let Some(req) = req else {
                tokio::select! {
                    _ = stop.changed() => return,
                    _ = link.kick.notified() => {}
                }
                continue;
            };
            let sent_at = Instant::now();
            let timeout = if req.entries.is_empty() {
                RPC_TIMEOUT
            } else {
                2 * RPC_TIMEOUT
            };
            let mut again = false;
            if let Ok(out) = self.rpc(&link, APPEND_ENTRIES, req.encode(), timeout).await {
                if let Ok(resp) = AppendResp::decode(&out) {
                    let moved = {
                        let mut c = self.core();
                        let moved =
                            c.on_append_resp(link.peer.addr, &req, resp, sent_at, Instant::now());
                        if moved {
                            c.apply();
                        }
                        again = c.has_work(link.peer.addr) && (resp.ok || resp.index > 0);
                        moved
                    };
                    self.bump();
                    if moved {
                        // Followers learn the new commit index on their next round.
                        for l in &self.links {
                            l.kick.notify_one();
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
            {
                let mut c = self.core();
                c.synced(done);
                if let Some(e) = &c.io_error {
                    trace!("{} stopped: {e}", self.me);
                }
                c.advance_commit();
                c.apply();
            }
            self.bump();
            for l in &self.links {
                l.kick.notify_one();
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

    /// One request on its own stream. The dial is inside the timeout: a dead
    /// peer must cost one timeout, not a QUIC handshake timeout.
    async fn rpc(
        &self,
        link: &Link,
        role: u8,
        body: Vec<u8>,
        timeout: Duration,
    ) -> Result<Vec<u8>> {
        let call = async {
            let conn = self.conn(link).await?;
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

    fn caller(&self, conn: &Connection) -> Caller {
        if !self.secure {
            // Unverified mode only runs on loopback, where any process could
            // speak for a peer anyway.
            return if conn.remote_address().ip().is_loopback() {
                Caller::Peer(None)
            } else {
                Caller::Client
            };
        }
        let covered: Vec<SocketAddr> = self
            .links
            .iter()
            .filter(|l| {
                l.peer
                    .name
                    .as_deref()
                    .is_some_and(|n| transport::caller_is(conn, n))
            })
            .map(|l| l.peer.addr)
            .collect();
        if covered.is_empty() {
            Caller::Client
        } else {
            Caller::Peer(Some(Arc::new(covered)))
        }
    }

    async fn accept(self: Arc<Self>, ep: Endpoint) {
        while let Some(incoming) = ep.accept().await {
            let s = self.clone();
            tokio::spawn(async move {
                let Ok(conn) = incoming.await else { return };
                let caller = s.caller(&conn);
                while let Ok((send, recv)) = conn.accept_bi().await {
                    let (s, caller) = (s.clone(), caller.clone());
                    tokio::spawn(async move {
                        let _ = s.serve(caller, send, recv).await;
                    });
                }
            });
        }
    }

    async fn serve(
        self: Arc<Self>,
        caller: Caller,
        mut send: SendStream,
        mut recv: RecvStream,
    ) -> Result<()> {
        let mut role = [0u8];
        recv.read_exact(&mut role).await?;
        let body = transport::get(&mut recv).await?;
        if role[0] == FOLLOW {
            return self.follow_remote(&body, &mut send).await;
        }
        let reply = match self.dispatch(caller, role[0], body).await {
            Ok(p) => {
                let mut r = Vec::with_capacity(1 + p.len());
                r.push(1);
                r.extend_from_slice(&p);
                r
            }
            Err(e) => {
                let mut r = vec![0];
                r.extend_from_slice(e.to_string().as_bytes());
                r
            }
        };
        transport::put(&mut send, &reply).await?;
        let _ = send.finish();
        Ok(())
    }

    async fn dispatch(
        self: &Arc<Self>,
        caller: Caller,
        role: u8,
        body: Vec<u8>,
    ) -> Result<Vec<u8>> {
        let mut b = Blobs::new(&body);
        match role {
            APPEND => return Ok(self.append(body, true).await?.to_be_bytes().to_vec()),
            LEN => return Ok(self.len(&b.text()?).await?.to_be_bytes().to_vec()),
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
                let pieces: Vec<&[u8]> = e
                    .iter()
                    .flat_map(|(k, v)| [k.as_bytes(), v.as_slice()])
                    .collect();
                return Ok(parts(&pieces));
            }
            _ => {}
        }
        let Caller::Peer(who) = caller else {
            return Err("peer requests need a peer certificate".into());
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
                self.bump();
                self.persisted(if resp.ok { resp.index } else { 0 }).await?;
                Ok(resp.encode())
            }
            FORWARD => Ok(self.append(body, false).await?.to_be_bytes().to_vec()),
            READ_INDEX => {
                let idx = self.read_index(false, Instant::now() + RPC_TIMEOUT).await?;
                Ok(idx.to_be_bytes().to_vec())
            }
            MAP_PUSH => {
                let name = b.text()?;
                self.maps.import(&name, b.take()?)?;
                Ok(Vec::new())
            }
            MAP_PULL => self.maps.missing(&body),
            _ => Err(format!("unknown request {role}").into()),
        }
    }
}
