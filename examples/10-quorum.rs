//! Prototype 10 — the same ordered log, but replicated.
//!
//! A write is only acknowledged once a majority of nodes holds it. One node
//! dying changes nothing; a second death stops writes instead of silently
//! accepting them. That is the difference between one leader and a cluster.
//!
//! Not built here: leader election (this leader is fixed at startup) and
//! durability (logs are still in RAM, as agreed).
//!
//! Run: cargo run --example 10-quorum

mod util;

use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const REPLICATE: u8 = 2;
const N: usize = 3;
const REPL_TIMEOUT: Duration = Duration::from_millis(400);

type Log = Arc<Mutex<Vec<String>>>;

async fn replicate(conn: &quinn::Connection, idx: u64, body: &str) -> Result<(), String> {
    let fut = async {
        let (mut send, mut recv) = conn.open_bi().await.map_err(|e| e.to_string())?;
        send.write_all(&[REPLICATE])
            .await
            .map_err(|e| e.to_string())?;
        send.write_all(&idx.to_be_bytes())
            .await
            .map_err(|e| e.to_string())?;
        send.write_all(&(body.len() as u32).to_be_bytes())
            .await
            .map_err(|e| e.to_string())?;
        send.write_all(body.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        let mut ack = [0u8; 1];
        recv.read_exact(&mut ack).await.map_err(|e| e.to_string())?;
        if ack[0] == 1 {
            Ok(())
        } else {
            Err("follower rejected index".to_string())
        }
    };
    tokio::time::timeout(REPL_TIMEOUT, fut)
        .await
        .map_err(|_| "replicate timeout".to_string())?
}

/// Accepts client writes (leader) and replica streams (follower) on one socket.
async fn serve(endpoint: quinn::Endpoint, log: Log) {
    while let Some(inc) = endpoint.accept().await {
        let Ok(conn) = inc.await else { continue };
        let log = log.clone();
        tokio::spawn(async move {
            while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                let mut role = [0u8; 1];
                if recv.read_exact(&mut role).await.is_err() {
                    break;
                }
                if role[0] != REPLICATE {
                    continue;
                }
                let mut ib = [0u8; 8];
                if recv.read_exact(&mut ib).await.is_err() {
                    break;
                }
                let idx = u64::from_be_bytes(ib);
                let mut lb = [0u8; 4];
                if recv.read_exact(&mut lb).await.is_err() {
                    break;
                }
                let mut body = vec![0u8; u32::from_be_bytes(lb) as usize];
                if recv.read_exact(&mut body).await.is_err() {
                    break;
                }
                let mut l = log.lock().await;
                let ok = idx as usize == l.len();
                if ok {
                    l.push(String::from_utf8_lossy(&body).to_string());
                    let _ = send.write_all(&[1]).await;
                } else {
                    let _ = send.write_all(&[0]).await;
                }
            }
        });
    }
}

struct Leader {
    n: usize,
    log: Log,
    followers: Mutex<Vec<Option<quinn::Connection>>>,
}

impl Leader {
    /// One lock held across the replication round trips: writes are serialised,
    /// which is what gives the log its single order.
    /// ponytail: serialised round trips cap throughput at 1 write per RTT.
    async fn append(&self, body: &str) -> Result<u64, String> {
        let mut log = self.log.lock().await;
        let idx = log.len() as u64;
        let peers: Vec<quinn::Connection> = self
            .followers
            .lock()
            .await
            .iter()
            .filter_map(|c| c.clone())
            .collect();

        let mut handles = Vec::new();
        for c in peers {
            let body = body.to_string();
            handles.push(tokio::spawn(async move { replicate(&c, idx, &body).await }));
        }

        let mut acks = 1; // ourselves
        for h in handles {
            if matches!(h.await, Ok(Ok(()))) {
                acks += 1;
            }
        }

        if acks * 2 > self.n {
            log.push(body.to_string());
            Ok(idx)
        } else {
            Err(format!("no quorum: {acks}/{} acks", self.n))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    util::install_crypto();

    // Three nodes, three sockets, one process.
    let mut endpoints = Vec::new();
    let mut logs: Vec<Log> = Vec::new();
    let mut addrs: Vec<SocketAddr> = Vec::new();
    for _ in 0..N {
        let (ep, _cert) = util::server("127.0.0.1:0".parse()?).map_err(|e| e.to_string())?;
        addrs.push(ep.local_addr().map_err(|e| e.to_string())?);
        logs.push(Arc::new(Mutex::new(Vec::new())));
        endpoints.push(ep);
    }
    for i in 0..N {
        let ep = endpoints[i].clone();
        let log = logs[i].clone();
        tokio::spawn(serve(ep, log));
    }

    // Node 0 is the leader; "election" is out of scope, so it is fixed here.
    let mut conns = Vec::new();
    for addr in &addrs[1..N] {
        let c = util::insecure_client().map_err(|e| e.to_string())?;
        conns.push(c.connect(*addr, "mycelium")?.await?);
    }
    let leader = Leader {
        n: N,
        log: logs[0].clone(),
        followers: Mutex::new(conns.into_iter().map(Some).collect()),
    };

    let mut ok = 0usize;
    let mut refused = 0usize;
    let mut commit = Vec::new();

    // Phase 1 — all three nodes alive.
    for i in 0..50 {
        let t = Instant::now();
        leader.append(&format!("a{i}")).await?;
        commit.push(t.elapsed());
        ok += 1;
    }
    println!("  phase 1  all 3 alive                {ok} commits");

    // Phase 2 — kill node 2. Two of three is still a majority.
    endpoints[2].close(0u32.into(), b"killed");
    for i in 0..50 {
        let t = Instant::now();
        leader.append(&format!("b{i}")).await?;
        commit.push(t.elapsed());
        ok += 1;
    }
    println!("  phase 2  node 2 killed              {ok} commits total");

    // Phase 3 — kill node 1. One of three is not a majority: writes must stop.
    endpoints[1].close(0u32.into(), b"killed");
    for i in 0..10 {
        match leader.append(&format!("c{i}")).await {
            Ok(_) => ok += 1,
            Err(_) => refused += 1,
        }
    }
    println!("  phase 3  nodes 1+2 killed           {refused}/10 writes refused");

    let l0 = logs[0].lock().await.clone();
    let l1 = logs[1].lock().await.clone();
    let l2 = logs[2].lock().await.clone();

    assert_eq!(
        ok, 100,
        "a majority of nodes was available, every write should commit"
    );
    assert_eq!(
        refused, 10,
        "without a majority, no write may be acknowledged"
    );
    assert_eq!(l0.len(), 100, "leader log");
    assert_eq!(l1.len(), 100, "survivor must hold every committed entry");
    assert_eq!(
        l2.len(),
        50,
        "dead node keeps only what it saw before dying"
    );
    assert_eq!(
        &l0[..],
        &l1[..],
        "surviving replica disagrees with the leader"
    );

    commit.sort();
    let p = |q: f64| commit[((commit.len() as f64 - 1.0) * q) as usize].as_secs_f64() * 1e3;
    println!();
    println!("  committed       {ok} entries, survivor holds all of them in the same order");
    println!("  refused         {refused} entries with 1/3 nodes up");
    println!(
        "  quorum commit   p50 {:>7.3} ms   p95 {:>7.3} ms",
        p(0.50),
        p(0.95)
    );
    println!("  replication     leader + 1 follower ack before every ack to the writer");
    println!();
    println!("ok: replicated, ordered, and it refuses writes it cannot make durable on a majority");
    Ok(())
}
