//! Prototype 11 — the leader is replaceable.
//!
//! Raft's core, minus storage: terms, votes, heartbeats, and a rule that a
//! candidate must hold the longest log before it can win. Writes still need a
//! majority ack, so a killed leader leaves no committed entry behind.
//!
//! One piece of durability is unavoidable: a node that forgets which term it
//! voted in can vote twice in the same term and produce two leaders. So the
//! two numbers (term, voted_for) go to a 16-byte file. The log stays in RAM,
//! as agreed.
//!
//! Run: cargo run --example 11-failover

mod util;

use std::error::Error;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const WRITE: u8 = 1;
const REPLICATE: u8 = 2;
const VOTE: u8 = 3;
const HEARTBEAT: u8 = 4;
const STATUS: u8 = 5;

const N: usize = 3;
const TICK: Duration = Duration::from_millis(40);

#[derive(Clone, Copy, PartialEq)]
enum Role {
    Follower,
    Leader,
}

struct State {
    term: u64,
    voted_for: Option<usize>,
    role: Role,
    log: Vec<String>,
}

struct Node {
    id: usize,
    addrs: Vec<SocketAddr>,
    state: Mutex<State>,
    client: quinn::Endpoint,
    last_heard: Mutex<Instant>,
    rng: AtomicU64,
    /// A killed node must stop campaigning: a process death ends its outbound
    /// connections too, even though this prototype only closed its listener.
    dead: AtomicU64,
}

fn path(id: usize) -> String {
    format!("/tmp/mycelium-vote-{id}")
}

/// The only durable state in the whole design, and the only part that must be.
fn persist(id: usize, term: u64, voted_for: Option<usize>) {
    let v = (term, voted_for.map(|x| x as u64).unwrap_or(u64::MAX));
    let _ = std::fs::write(path(id), format!("{} {}", v.0, v.1));
}

fn load(id: usize) -> (u64, Option<usize>) {
    std::fs::read_to_string(path(id))
        .ok()
        .and_then(|s| {
            let mut it = s.split_whitespace();
            let t = it.next()?.parse().ok()?;
            let v: u64 = it.next()?.parse().ok()?;
            Some((
                t,
                if v == u64::MAX {
                    None
                } else {
                    Some(v as usize)
                },
            ))
        })
        .unwrap_or((0, None))
}

impl Node {
    fn rng(&self) -> u64 {
        let x = self.rng.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    async fn rpc(&self, addr: SocketAddr, role: u8, payload: &[u8]) -> Result<Vec<u8>, String> {
        if self.dead.load(Ordering::Relaxed) == 1 {
            return Err("node is dead".into());
        }
        let fut = async {
            let conn = self
                .client
                .connect(addr, "mycelium")
                .map_err(|e| e.to_string())?
                .await
                .map_err(|e| e.to_string())?;
            let (mut send, mut recv) = conn.open_bi().await.map_err(|e| e.to_string())?;
            send.write_all(&[role]).await.map_err(|e| e.to_string())?;
            send.write_all(&(payload.len() as u32).to_be_bytes())
                .await
                .map_err(|e| e.to_string())?;
            send.write_all(payload).await.map_err(|e| e.to_string())?;
            let mut lb = [0u8; 4];
            recv.read_exact(&mut lb).await.map_err(|e| e.to_string())?;
            let mut out = vec![0u8; u32::from_be_bytes(lb) as usize];
            recv.read_exact(&mut out).await.map_err(|e| e.to_string())?;
            conn.close(0u32.into(), b"done");
            Ok::<Vec<u8>, String>(out)
        };
        tokio::time::timeout(Duration::from_millis(300), fut)
            .await
            .map_err(|_| "rpc timeout".to_string())?
    }

    /// Leader path: replicate to a majority, then acknowledge.
    async fn append(&self, body: &str) -> Result<u64, String> {
        let idx = {
            let s = self.state.lock().await;
            if s.role != Role::Leader {
                return Err("not leader".into());
            }
            s.log.len() as u64
        };
        let mut acks = 1usize;
        for (peer, addr) in self.addrs.iter().enumerate() {
            if peer == self.id {
                continue;
            }
            if self.send_log(*addr, idx, body).await.is_ok() {
                acks += 1;
            }
        }
        let mut s = self.state.lock().await;
        if acks * 2 <= N {
            return Err(format!("no quorum: {acks}/{N}"));
        }
        if s.role != Role::Leader || s.log.len() as u64 != idx {
            return Err("leadership changed mid-write".into());
        }
        s.log.push(body.to_string());
        Ok(idx)
    }

    async fn send_log(&self, addr: SocketAddr, idx: u64, body: &str) -> Result<(), String> {
        let mut p = Vec::new();
        p.extend_from_slice(&idx.to_be_bytes());
        p.extend_from_slice(&(body.len() as u32).to_be_bytes());
        p.extend_from_slice(body.as_bytes());
        self.rpc(addr, REPLICATE, &p).await.map(|_| ())
    }
}

async fn handle(node: Arc<Node>, conn: quinn::Connection) {
    while let Ok((mut send, mut recv)) = conn.accept_bi().await {
        let node = node.clone();
        tokio::spawn(async move {
            let mut role = [0u8; 1];
            if recv.read_exact(&mut role).await.is_err() {
                return;
            }
            let mut lb = [0u8; 4];
            if recv.read_exact(&mut lb).await.is_err() {
                return;
            }
            let mut body = vec![0u8; u32::from_be_bytes(lb) as usize];
            if recv.read_exact(&mut body).await.is_err() {
                return;
            }

            let reply: Vec<u8> = match role[0] {
                REPLICATE => {
                    let idx = u64::from_be_bytes(body[..8].try_into().unwrap());
                    let text = String::from_utf8_lossy(&body[12..]).to_string();
                    let mut s = node.state.lock().await;
                    if idx as usize == s.log.len() {
                        s.log.push(text);
                        vec![1]
                    } else {
                        vec![0]
                    }
                }
                VOTE => {
                    let term = u64::from_be_bytes(body[..8].try_into().unwrap());
                    let cand_len = u64::from_be_bytes(body[8..16].try_into().unwrap());
                    let mut s = node.state.lock().await;
                    let mut granted = false;
                    if term > s.term {
                        s.term = term;
                        s.voted_for = None;
                        s.role = Role::Follower;
                        persist(node.id, s.term, None);
                    }
                    if term == s.term && s.voted_for.is_none() && cand_len >= s.log.len() as u64 {
                        s.voted_for = Some(body[16] as usize);
                        granted = true;
                        persist(node.id, s.term, s.voted_for);
                    }
                    vec![granted as u8]
                }
                HEARTBEAT => {
                    let term = u64::from_be_bytes(body[..8].try_into().unwrap());
                    let mut s = node.state.lock().await;
                    if term >= s.term {
                        s.term = term;
                        s.role = Role::Follower;
                    }
                    *node.last_heard.lock().await = Instant::now();
                    s.log.len().to_be_bytes().to_vec()
                }
                WRITE => {
                    let text = String::from_utf8_lossy(&body[4..]).to_string();
                    match node.append(&text).await {
                        Ok(idx) => {
                            let mut out = vec![1u8];
                            out.extend_from_slice(&idx.to_be_bytes());
                            out
                        }
                        Err(e) => {
                            let mut out = vec![0u8];
                            out.extend_from_slice(e.as_bytes());
                            out
                        }
                    }
                }
                STATUS => {
                    let s = node.state.lock().await;
                    let mut out = Vec::new();
                    out.push((s.role == Role::Leader) as u8);
                    out.extend_from_slice(&s.term.to_be_bytes());
                    out.extend_from_slice(&(s.log.len() as u64).to_be_bytes());
                    out
                }
                _ => vec![0],
            };
            let _ = send.write_all(&(reply.len() as u32).to_be_bytes()).await;
            let _ = send.write_all(&reply).await;
            let _ = send.finish();
        });
    }
}

async fn election_loop(node: Arc<Node>) {
    loop {
        tokio::time::sleep(TICK).await;
        if node.dead.load(Ordering::Relaxed) == 1 {
            return;
        }
        let quiet = node.last_heard.lock().await.elapsed();
        let timeout = Duration::from_millis(200 + node.rng() % 200);
        if quiet < timeout {
            continue;
        }
        let (term, is_leader, len) = {
            let s = node.state.lock().await;
            (s.term, s.role == Role::Leader, s.log.len() as u64)
        };
        if is_leader {
            continue;
        }

        let term = term + 1;
        {
            let mut s = node.state.lock().await;
            s.term = term;
            s.voted_for = Some(node.id);
            persist(node.id, term, Some(node.id));
        }
        *node.last_heard.lock().await = Instant::now();

        let mut votes = 1usize;
        for (peer, addr) in node.addrs.iter().enumerate() {
            if peer == node.id {
                continue;
            }
            let mut p = Vec::new();
            p.extend_from_slice(&term.to_be_bytes());
            p.extend_from_slice(&len.to_be_bytes());
            p.push(node.id as u8);
            if let Ok(r) = node.rpc(*addr, VOTE, &p).await {
                if r.first() == Some(&1) {
                    votes += 1;
                }
            }
        }

        if votes * 2 > N {
            let mut s = node.state.lock().await;
            s.role = Role::Leader;
            *node.last_heard.lock().await = Instant::now();
            println!(
                "[node {}] elected leader in term {term} ({votes} votes)",
                node.id
            );
        }
    }
}

async fn heartbeat_loop(node: Arc<Node>) {
    loop {
        tokio::time::sleep(TICK).await;
        if node.dead.load(Ordering::Relaxed) == 1 {
            return;
        }
        let (is_leader, term, len) = {
            let s = node.state.lock().await;
            (s.role == Role::Leader, s.term, s.log.len() as u64)
        };
        if !is_leader {
            continue;
        }
        let mut p = Vec::new();
        p.extend_from_slice(&term.to_be_bytes());
        p.extend_from_slice(&len.to_be_bytes());

        let mut reachable = 1usize;
        for (peer, addr) in node.addrs.iter().enumerate() {
            if peer == node.id {
                continue;
            }
            if node.rpc(*addr, HEARTBEAT, &p).await.is_ok() {
                reachable += 1;
            }
        }
        if reachable * 2 <= N {
            let mut s = node.state.lock().await;
            s.role = Role::Follower;
            println!("[node {}] lost majority, stepping down", node.id);
        }
    }
}

async fn status(node: &Arc<Node>, addr: SocketAddr) -> Option<(bool, u64, u64)> {
    let r = node.rpc(addr, STATUS, &[]).await.ok()?;
    if r.len() != 17 {
        return None;
    }
    Some((
        r[0] == 1,
        u64::from_be_bytes(r[1..9].try_into().ok()?),
        u64::from_be_bytes(r[9..17].try_into().ok()?),
    ))
}

/// Highest term wins: a stale leader from an older term must not be trusted.
async fn find_leader(nodes: &[Arc<Node>], addrs: &[SocketAddr]) -> Option<usize> {
    for _ in 0..100 {
        // Ask everyone at once: a dead peer must not cost a serial timeout.
        let (a, b, c) = tokio::join!(
            status(&nodes[0], addrs[0]),
            status(&nodes[0], addrs[1]),
            status(&nodes[0], addrs[2])
        );
        let mut best: Option<(usize, u64)> = None;
        for (i, r) in [a, b, c].into_iter().enumerate() {
            if let Some((true, term, _)) = r {
                if best.map(|(_, t)| term > t).unwrap_or(true) {
                    best = Some((i, term));
                }
            }
        }
        match best {
            Some((i, _)) => return Some(i),
            None => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    None
}

/// Churn is real: wait until two scans 250 ms apart agree before trusting a leader.
async fn stable_leader(nodes: &[Arc<Node>], addrs: &[SocketAddr]) -> Option<usize> {
    for _ in 0..20 {
        let a = find_leader(nodes, addrs).await?;
        tokio::time::sleep(Duration::from_millis(250)).await;
        if find_leader(nodes, addrs).await == Some(a) {
            return Some(a);
        }
    }
    None
}

/// A client retry loop, like any real client of a replicated log.
async fn write_retry(
    nodes: &[Arc<Node>],
    addrs: &[SocketAddr],
    hint: &mut usize,
    body: &str,
) -> Result<u64, String> {
    let mut last = String::from("no leader");
    for _ in 0..20 {
        if let Ok(i) = write_entry(nodes, addrs, *hint, body).await {
            return Ok(i);
        }
        if let Some(l) = stable_leader(nodes, addrs).await {
            *hint = l;
        }
        match write_entry(nodes, addrs, *hint, body).await {
            Ok(i) => return Ok(i),
            Err(e) => last = e,
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(last)
}

async fn write_entry(
    nodes: &[Arc<Node>],
    addrs: &[SocketAddr],
    leader: usize,
    body: &str,
) -> Result<u64, String> {
    let mut p = (body.len() as u32).to_be_bytes().to_vec();
    p.extend_from_slice(body.as_bytes());
    let r = nodes[0].rpc(addrs[leader], WRITE, &p).await?;
    if r.len() == 9 && r[0] == 1 {
        Ok(u64::from_be_bytes(r[1..9].try_into().unwrap()))
    } else {
        Err(String::from_utf8_lossy(&r).to_string())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    util::install_crypto();
    for i in 0..N {
        let _ = std::fs::remove_file(path(i));
    }

    let mut endpoints = Vec::new();
    let mut addrs = Vec::new();
    for _ in 0..N {
        let (ep, _c) = util::server("127.0.0.1:0".parse()?).map_err(|e| e.to_string())?;
        addrs.push(ep.local_addr().map_err(|e| e.to_string())?);
        endpoints.push(ep);
    }

    let mut nodes = Vec::new();
    for i in 0..N {
        let (term, voted_for) = load(i);
        nodes.push(Arc::new(Node {
            id: i,
            addrs: addrs.clone(),
            state: Mutex::new(State {
                term,
                voted_for,
                role: Role::Follower,
                log: Vec::new(),
            }),
            client: util::insecure_client().map_err(|e| e.to_string())?,
            last_heard: Mutex::new(Instant::now()),
            rng: AtomicU64::new(0x1234_5678 + i as u64),
            dead: AtomicU64::new(0),
        }));
    }
    for i in 0..N {
        let ep = endpoints[i].clone();
        let n = nodes[i].clone();
        tokio::spawn(async move {
            while let Some(inc) = ep.accept().await {
                let Ok(c) = inc.await else { continue };
                tokio::spawn(handle(n.clone(), c));
            }
        });
        tokio::spawn(election_loop(nodes[i].clone()));
        tokio::spawn(heartbeat_loop(nodes[i].clone()));
    }

    tokio::time::sleep(Duration::from_millis(400)).await;
    let first = stable_leader(&nodes, &addrs)
        .await
        .ok_or("no leader elected")?;
    println!("  phase 1  leader is node {first}");
    let mut leader = first;
    for i in 0..30 {
        write_retry(&nodes, &addrs, &mut leader, &format!("a{i}"))
            .await
            .map_err(|e| format!("write a{i}: {e}"))?;
    }

    nodes[first].dead.store(1, Ordering::Relaxed);
    endpoints[first].close(0u32.into(), b"killed");
    println!("  phase 2  killed node {first}");
    let second = stable_leader(&nodes, &addrs).await.ok_or("no failover")?;
    assert_ne!(second, first, "the dead node cannot be the new leader");
    println!("  phase 3  leader is now node {second}");
    for i in 0..20 {
        write_retry(&nodes, &addrs, &mut leader, &format!("b{i}"))
            .await
            .map_err(|e| format!("write b{i}: {e}"))?;
    }

    let l_new = nodes[second].state.lock().await.log.clone();
    let l_old = nodes[first].state.lock().await.log.clone();
    let l_third = nodes[3 - first - second].state.lock().await.log.clone();

    assert_eq!(
        l_new.len(),
        50,
        "new leader must hold every committed entry"
    );
    for (i, body) in l_new.iter().take(30).enumerate() {
        assert_eq!(
            body,
            &format!("a{i}"),
            "entry {i} changed across the failover"
        );
    }
    for (i, body) in l_new.iter().skip(30).enumerate() {
        assert_eq!(body, &format!("b{i}"), "post-failover entry {i} is wrong");
    }
    assert_eq!(
        l_old.len(),
        30,
        "the killed leader keeps exactly what it committed"
    );
    assert_eq!(
        l_third, l_new,
        "the surviving follower must match the new leader"
    );

    println!();
    println!("  committed       50 entries, 30 before the kill, 20 after");
    println!("  new leader      node {second}, log identical to the surviving replica");
    println!("  old leader      node {first}, stopped at 30, as one would expect");
    println!();
    println!("ok: leader died, a new one was elected, no committed entry lost, order intact");
    Ok(())
}
