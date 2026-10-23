//! Scenario harness: measure the library under a shaped link.
//!
//!   cargo run --release --example scenarios -- <addr>[=name] <entries> <label>
//!
//! Prints two CSV rows: serial appends (one RTT each) and one pipelined batch.
//! Shape the link outside this binary (see scripts/net-scenarios.sh).

use mycelium::{Client, Mycelium};
use std::error::Error;
use std::time::{Duration, Instant};

fn pct(mut v: Vec<Duration>, q: f64) -> f64 {
    v.sort();
    v[((v.len() as f64 - 1.0) * q) as usize].as_secs_f64() * 1e3
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).cloned().unwrap_or("127.0.0.1:7777".into());
    let entries: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(50);
    let label = args.get(3).cloned().unwrap_or("unlabelled".into());
    let tag = format!("scenario-{}-{}", std::process::id(), entries);

    let connect = || async {
        match addr.split_once('=') {
            Some((a, name)) => Client::verified(a.parse()?, name).connect().await,
            None => Mycelium::join(addr.parse()?).await,
        }
    };
    let writer: Mycelium = connect().await?;
    let reader: Mycelium = connect().await?;

    // ---- serial: one round trip per append
    let serial = writer.stream::<u64>(&format!("{tag}-serial"));
    let mut rx = reader
        .stream::<u64>(&format!("{tag}-serial"))
        .watch(0)
        .await?;
    let started = Instant::now();
    let mut rtts = Vec::with_capacity(entries as usize);
    for i in 0..entries {
        let t = Instant::now();
        serial.append(&i).await?;
        rtts.push(t.elapsed());
    }
    let serial_window = started.elapsed();
    let mut seen = 0u64;
    while seen < entries {
        match tokio::time::timeout(Duration::from_secs(120), rx.recv()).await {
            Ok(Some(_)) => seen += 1,
            _ => break,
        }
    }
    let serial_e2e = started.elapsed();
    println!(
        "{label}-serial,{:.2},{:.2},{:.1},{:.0}",
        pct(rtts.clone(), 0.50),
        pct(rtts, 0.95),
        entries as f64 / serial_window.as_secs_f64(),
        serial_e2e.as_secs_f64() * 1e3
    );

    // ---- pipelined: the whole batch in one round trip
    let batch = writer.stream::<u64>(&format!("{tag}-batch"));
    let mut brx = reader
        .stream::<u64>(&format!("{tag}-batch"))
        .watch(0)
        .await?;
    let items: Vec<u64> = (0..entries).collect();
    let t = Instant::now();
    let idxs = batch.append_many(&items).await?;
    let batch_window = t.elapsed();
    assert_eq!(idxs.len(), entries as usize, "batch lost entries");
    let mut seen = 0u64;
    while seen < entries {
        match tokio::time::timeout(Duration::from_secs(120), brx.recv()).await {
            Ok(Some(_)) => seen += 1,
            _ => break,
        }
    }
    let batch_e2e = t.elapsed();
    println!(
        "{label}-batch,{:.2},{:.2},{:.1},{:.0}",
        batch_window.as_secs_f64() * 1e3,
        batch_window.as_secs_f64() * 1e3,
        entries as f64 / batch_window.as_secs_f64(),
        batch_e2e.as_secs_f64() * 1e3
    );
    Ok(())
}
