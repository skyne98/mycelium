//! Round-trip probe: p50 of appends and reads against one node, with and
//! without a concurrent watcher.
//!
//!   cargo run --release --example probe -- <addr>[=name] [count]

use mycelium::{Client, Mycelium};
use std::time::{Duration, Instant};

async fn connect(spec: &str) -> mycelium::Result<Mycelium> {
    match spec.split_once('=') {
        Some((a, name)) => Client::verified(a.parse()?, name).connect().await,
        None => Mycelium::join(spec.parse()?).await,
    }
}

fn p50(mut v: Vec<Duration>) -> String {
    v.sort();
    let ms = |q: f64| v[((v.len() - 1) as f64 * q) as usize].as_secs_f64() * 1e3;
    let slow = v.iter().filter(|d| d.as_millis() >= 50).count();
    format!(
        "p50 {:.2} ms, p90 {:.2}, p99 {:.2}, max {:.2}, {slow} of {} >= 50 ms",
        ms(0.5),
        ms(0.9),
        ms(0.99),
        ms(1.0),
        v.len()
    )
}

#[tokio::main]
async fn main() -> mycelium::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let spec = args.get(1).cloned().unwrap_or("127.0.0.1:7777".into());
    let n: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(50);
    let db = connect(&spec).await?;
    let s = db.stream::<u64>(&format!("probe-{}", std::process::id()));
    s.append(&0).await?;

    let mut v = Vec::new();
    for i in 0..n as u64 {
        let t = Instant::now();
        s.append(&i).await?;
        v.push(t.elapsed());
    }
    println!("append, no watcher: {}", p50(v));

    let mut v = Vec::new();
    for _ in 0..n {
        let t = Instant::now();
        s.len().await?;
        v.push(t.elapsed());
    }
    println!("len (ReadIndex):    {}", p50(v));

    let m = db.map::<u64, u64>("probe");
    let mut v = Vec::new();
    for i in 0..n as u64 {
        let t = Instant::now();
        m.set(&(i % 8), &i).await?;
        v.push(t.elapsed());
    }
    println!("map set (fsync):    {}", p50(v));

    let mut v = Vec::new();
    for i in 0..n as u64 {
        let t = Instant::now();
        m.get(&(i % 8)).await?;
        v.push(t.elapsed());
    }
    println!("map get (no work):  {}", p50(v));

    let watcher = connect(&spec).await?;
    let mut rx = watcher.stream::<u64>(s.name()).watch(0).await?;
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let mut v = Vec::new();
    for i in 0..n as u64 {
        let t = Instant::now();
        s.append(&i).await?;
        v.push(t.elapsed());
    }
    println!("append, watcher:    {}", p50(v));
    Ok(())
}
