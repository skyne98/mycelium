//! In-process cluster benchmark: serial append latency and batch throughput.
//!
//!   cargo run --release --example bench -- [nodes] [appends] [data_dir]

use mycelium::{Node, Peer};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

fn free_addr() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap()
}

fn pct(v: &mut [Duration], q: f64) -> f64 {
    v.sort();
    v[((v.len() as f64 - 1.0) * q) as usize].as_secs_f64() * 1e3
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().collect();
    let n: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(3);
    let count: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(200);
    let dir = args.get(3).cloned().unwrap_or_else(|| {
        std::env::temp_dir()
            .join(format!("mycelium-bench-{}", std::process::id()))
            .display()
            .to_string()
    });

    let addrs: Vec<SocketAddr> = (0..n).map(|_| free_addr()).collect();
    let mut nodes = Vec::new();
    for (i, a) in addrs.iter().enumerate() {
        let peers = addrs
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, p)| Peer::local(*p))
            .collect();
        nodes.push(Node::cluster(*a, peers).with_data_dir(&dir).start().await?);
    }
    let leader = loop {
        let mut found = None;
        for (i, r) in nodes.iter().enumerate() {
            if r.is_leader().await {
                found = Some(i);
            }
        }
        if let Some(i) = found {
            break i;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let s = nodes[leader].handle().stream::<u64>("bench");
    s.append(&0).await?;

    let mut lat = Vec::with_capacity(count);
    let t0 = Instant::now();
    for i in 0..count as u64 {
        let t = Instant::now();
        s.append(&i).await?;
        lat.push(t.elapsed());
    }
    let serial = t0.elapsed();
    println!(
        "{n} nodes, serial: p50 {:.2} ms, p99 {:.2} ms, {:.0} appends/s",
        pct(&mut lat, 0.5),
        pct(&mut lat, 0.99),
        count as f64 / serial.as_secs_f64()
    );

    let t0 = Instant::now();
    let mut tasks = Vec::new();
    for w in 0..32u64 {
        let s = nodes[leader].handle().stream::<u64>("bench");
        let per = count as u64 / 4;
        tasks.push(tokio::spawn(async move {
            for i in 0..per {
                s.append(&(w * 1_000_000 + i)).await.unwrap();
            }
        }));
    }
    for t in tasks {
        t.await?;
    }
    let conc = 32 * (count / 4);
    println!(
        "{n} nodes, 32 concurrent writers: {:.0} appends/s",
        conc as f64 / t0.elapsed().as_secs_f64()
    );

    let batch: Vec<u64> = (0..1000).collect();
    let t0 = Instant::now();
    for _ in 0..10 {
        s.append_many(&batch).await?;
    }
    println!(
        "{n} nodes, batches of 1000: {:.0} entries/s",
        10_000.0 / t0.elapsed().as_secs_f64()
    );
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
