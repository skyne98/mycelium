//! Prototype 9 — an ordered log that survives a flaky link.
//!
//! One sequencer assigns indices; subscribers read back one total order.
//! On top of that, three things this prototype had to grow:
//!   1. resume cursor  — a subscriber asks for entries from index N
//!   2. gap-free resync — after a lag or a reconnect, refill from the log
//!   3. reconnect loop  — exponential backoff, resume where it stopped
//!
//! The demo injects the faults: the server hangs up on every subscriber
//! every 80 ms, and one subscriber reads deliberately slowly. Both must
//! still end with the identical dense sequence 0..N.
//!
//! Run: cargo run --example 09-ordered

mod util;

use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, Mutex};

const WRITE: u8 = 1;
const READ: u8 = 2;
const ENTRIES: usize = 2000;
const CHAOS: Duration = Duration::from_millis(80);

struct Log {
    entries: Mutex<Vec<Vec<u8>>>,
    /// Signal only: a reader that wakes always re-reads the log from its cursor,
    /// so a slow reader can never lose an entry no matter how far it falls behind.
    tx: broadcast::Sender<()>,
    resyncs: AtomicUsize,
}

impl Log {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(8);
        Self {
            entries: Mutex::default(),
            tx,
            resyncs: AtomicUsize::new(0),
        }
    }

    /// The only place an index is assigned. One lock, therefore one order.
    async fn append(&self, data: Vec<u8>) -> u64 {
        let mut e = self.entries.lock().await;
        let idx = e.len() as u64;
        e.push(data);
        let _ = self.tx.send(());
        idx
    }

    async fn since(&self, from: u64) -> Vec<Vec<u8>> {
        let e = self.entries.lock().await;
        if from as usize >= e.len() {
            return Vec::new();
        }
        e[from as usize..].to_vec()
    }

    async fn len(&self) -> usize {
        self.entries.lock().await.len()
    }
}

async fn write_entry(send: &mut quinn::SendStream, idx: u64, data: &[u8]) -> Result<(), String> {
    send.write_all(&idx.to_be_bytes())
        .await
        .map_err(|e| e.to_string())?;
    send.write_all(&(data.len() as u32).to_be_bytes())
        .await
        .map_err(|e| e.to_string())?;
    send.write_all(data).await.map_err(|e| e.to_string())?;
    Ok(())
}

async fn read_blob(recv: &mut quinn::RecvStream) -> Result<Vec<u8>, String> {
    let mut b = [0u8; 4];
    recv.read_exact(&mut b).await.map_err(|e| e.to_string())?;
    let mut v = vec![0u8; u32::from_be_bytes(b) as usize];
    recv.read_exact(&mut v).await.map_err(|e| e.to_string())?;
    Ok(v)
}

/// Streams the log from `from` onwards and follows it live. Never skips an index:
/// after a lag or a reconnect it simply re-reads the log from its own cursor.
async fn forward(send: &mut quinn::SendStream, log: &Arc<Log>, from: u64) -> Result<(), String> {
    let mut rx = log.tx.subscribe();
    let mut next = from;
    loop {
        let batch = log.since(next).await;
        if !batch.is_empty() {
            log.resyncs.fetch_add(1, Ordering::Relaxed);
        }
        for body in &batch {
            write_entry(send, next, body).await?;
            next += 1;
        }
        match rx.recv().await {
            Ok(()) | Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => return Ok(()),
        }
    }
}

async fn serve(conn: quinn::Connection, log: Arc<Log>) -> Result<(), String> {
    loop {
        let Ok((mut send, mut recv)) = conn.accept_bi().await else {
            return Ok(());
        };
        let mut role = [0u8; 1];
        if recv.read_exact(&mut role).await.is_err() {
            return Ok(());
        }
        if role[0] == WRITE {
            while let Ok(blob) = read_blob(&mut recv).await {
                let idx = log.append(blob).await;
                send.write_all(&idx.to_be_bytes())
                    .await
                    .map_err(|e| e.to_string())?;
            }
        } else {
            let mut fb = [0u8; 8];
            recv.read_exact(&mut fb).await.map_err(|e| e.to_string())?;
            let from = u64::from_be_bytes(fb);

            // Injected fault: the server hangs up on every subscriber, repeatedly.
            {
                let c = conn.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(CHAOS).await;
                    c.close(0u32.into(), b"chaos");
                });
            }
            return forward(&mut send, &log, from).await;
        }
        let _ = send.finish();
    }
}

/// Reads from `next` until `want` entries are held, reconnecting forever.
/// Returns the number of connections it took.
async fn tail(
    addr: std::net::SocketAddr,
    from: u64,
    slow: bool,
) -> Result<(Vec<String>, usize), String> {
    let want = ENTRIES;
    let mut bodies: Vec<String> = Vec::new();
    let mut next = from;
    let mut connects = 0usize;
    let mut backoff = Duration::from_millis(20);
    while bodies.len() < want {
        connects += 1;
        let ep = util::insecure_client().map_err(|e| e.to_string())?;
        let joined = async {
            let conn = ep
                .connect(addr, "mycelium")
                .map_err(|e| e.to_string())?
                .await
                .map_err(|e| e.to_string())?;
            let (mut send, mut recv) = conn.open_bi().await.map_err(|e| e.to_string())?;
            send.write_all(&[READ]).await.map_err(|e| e.to_string())?;
            send.write_all(&next.to_be_bytes())
                .await
                .map_err(|e| e.to_string())?;
            while bodies.len() < want {
                let mut ib = [0u8; 8];
                recv.read_exact(&mut ib).await.map_err(|e| e.to_string())?;
                let idx = u64::from_be_bytes(ib);
                if idx != next {
                    return Err(format!("gap: wanted {next}, got {idx}"));
                }
                let body =
                    String::from_utf8(read_blob(&mut recv).await?).map_err(|e| e.to_string())?;
                bodies.push(body);
                next += 1;
                if slow && bodies.len() < 200 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            Ok::<(), String>(())
        };
        let out = tokio::time::timeout(Duration::from_secs(10), joined).await;
        ep.close(0u32.into(), b"");
        match out {
            Ok(Ok(())) => break,
            _ => {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_millis(400));
            }
        }
    }
    Ok((bodies, connects))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    util::install_crypto();
    let (endpoint, _cert) = util::server("127.0.0.1:0".parse()?).map_err(|e| e.to_string())?;
    let addr = endpoint.local_addr()?;
    let log = Arc::new(Log::new());

    {
        let log = log.clone();
        tokio::spawn(async move {
            while let Some(inc) = endpoint.accept().await {
                let Ok(conn) = inc.await else { continue };
                let log = log.clone();
                tokio::spawn(async move {
                    let _ = serve(conn, log).await;
                });
            }
        });
    }

    // Two followers, both starting at zero. One reads 1 ms per entry for its
    // first 200 entries, so it falls behind and has to catch up from the log.
    let slow_follow = tokio::spawn(tail(addr, 0, true));
    let fast_follow = tokio::spawn(tail(addr, 0, false));

    // Writer: sequential appends, one RTT each. The leader decides the order.
    let w = util::insecure_client().map_err(|e| e.to_string())?;
    let cw = w.connect(addr, "mycelium")?.await?;
    let (mut send, mut recv) = cw.open_bi().await?;
    send.write_all(&[WRITE]).await?;

    let t_all = Instant::now();
    let mut rtts = Vec::with_capacity(ENTRIES);
    for i in 0..ENTRIES {
        let body = format!("entry-{i}");
        let t = Instant::now();
        send.write_all(&(body.len() as u32).to_be_bytes()).await?;
        send.write_all(body.as_bytes()).await?;
        let mut ib = [0u8; 8];
        recv.read_exact(&mut ib).await?;
        assert_eq!(
            u64::from_be_bytes(ib),
            i as u64,
            "leader must hand out dense indices"
        );
        rtts.push(t.elapsed());
    }
    let total = t_all.elapsed();

    let (ba, connects_a) = match slow_follow.await {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => return Err(e.into()),
        Err(e) => return Err(e.to_string().into()),
    };
    let (bb, connects_b) = match fast_follow.await {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => return Err(e.into()),
        Err(e) => return Err(e.to_string().into()),
    };

    // The contract: identical, dense, nothing skipped, despite every injected fault.
    assert_eq!(ba.len(), ENTRIES, "slow follower is missing entries");
    assert_eq!(bb.len(), ENTRIES, "fast follower is missing entries");
    assert_eq!(ba, bb, "followers disagreed on the order");
    for (i, body) in ba.iter().enumerate() {
        assert_eq!(body, &format!("entry-{i}"), "entry {i} out of place");
    }
    assert_eq!(log.len().await, ENTRIES, "leader lost entries");
    assert!(connects_a > 1, "chaos never fired on the slow follower");
    assert!(connects_b > 1, "chaos never fired on the fast follower");
    assert!(
        log.resyncs.load(Ordering::Relaxed) > 0,
        "no catch-up was ever needed"
    );

    rtts.sort();
    let p = |q: f64| rtts[((rtts.len() as f64 - 1.0) * q) as usize].as_secs_f64() * 1e3;

    println!("  leader          {addr}");
    println!(
        "  injected        hangup every {} ms, one reader at 1 ms/entry",
        CHAOS.as_millis()
    );
    println!(
        "  entries         {ENTRIES}, all delivered to both followers, in order, exactly once"
    );
    println!("  slow follower   {connects_a} connections (chaos + backoff + resume from cursor)");
    println!("  fast follower   {connects_b} connections");
    println!(
        "  catch-ups       {} refills from the log",
        log.resyncs.load(Ordering::Relaxed)
    );
    println!(
        "  commit rtt      p50 {:>7.3} ms   p95 {:>7.3} ms",
        p(0.50),
        p(0.95)
    );
    println!(
        "  throughput      {:>7.1} writes/s",
        ENTRIES as f64 / total.as_secs_f64()
    );
    println!();
    println!("ok: flaky link, zero gaps, zero duplicates, one total order");
    Ok(())
}
