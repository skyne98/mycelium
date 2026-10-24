//! Cross-machine scenario driver: writes through a failing-over client while
//! nodes are stopped and killed underneath it, then checks what it saw.
//!
//!   cargo run --release --example soak -- <seconds> <addr>=<name>...
//!
//! Checks: every acknowledged write is in the stream at the index it was
//! given, with its value; nothing appears twice; a watch through another node
//! sees the same sequence. A write that returned an error may or may not have
//! landed; those are counted, not failed.

use mycelium::{Client, Peer};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> mycelium::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let secs: u64 = args[1].parse()?;
    let mut peers = Vec::new();
    for s in &args[2..] {
        let (a, n) = s.split_once('=').ok_or("expected addr=name")?;
        peers.push(Peer::verified(a.parse()?, n));
    }
    let db = Client::any(peers.clone()).connect().await?;
    let watcher = Client::any(peers.into_iter().rev()).connect().await?;
    let name = format!("soak-{}", std::process::id());
    let s = db.stream::<u64>(&name);
    let mut rx = watcher.stream::<u64>(&name).watch(0).await?;

    let end = Instant::now() + Duration::from_secs(secs);
    let mut acked: HashMap<u64, u64> = HashMap::new();
    let mut unsure: HashSet<u64> = HashSet::new();
    let (mut slowest, mut value) = (Duration::ZERO, 0u64);
    let mut errors = Vec::new();
    while Instant::now() < end {
        let t = Instant::now();
        match s.append(&value).await {
            Ok(idx) => {
                if acked.insert(idx, value).is_some() {
                    return Err(format!("index {idx} acknowledged twice").into());
                }
            }
            Err(e) => {
                unsure.insert(value);
                errors.push(e.to_string());
            }
        }
        let took = t.elapsed();
        slowest = slowest.max(took);
        if took > Duration::from_millis(300) {
            println!("  slow write {value}: {took:?}");
        }
        value += 1;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let len = s.len().await?;
    let mut seen = HashSet::new();
    let mut landed_unsure = 0;
    for want in 0..len {
        let got = tokio::time::timeout(Duration::from_secs(15), rx.recv()).await.ok().flatten();
        let (idx, v) = got.ok_or_else(|| format!("watch stopped at {want} of {len}"))?;
        if idx != want {
            return Err(format!("watch skipped from {want} to {idx}").into());
        }
        if !seen.insert(v) {
            return Err(format!("value {v} appears twice").into());
        }
        match acked.get(&idx) {
            Some(a) if *a != v => return Err(format!("acknowledged write at {idx} changed").into()),
            Some(_) => {}
            None if unsure.contains(&v) => landed_unsure += 1,
            None => return Err(format!("value {v} at {idx} was never written").into()),
        }
    }
    if let Some(i) = acked.keys().find(|i| **i >= len) {
        return Err(format!("acknowledged write {i} is missing").into());
    }
    println!(
        "PASS: {} acknowledged, {} failed (of which {landed_unsure} landed), slowest {slowest:?}, stream {len}, watch matched",
        acked.len(),
        errors.len()
    );
    for e in errors.iter().take(3) {
        println!("  error: {e}");
    }
    let list: Vec<String> = db
        .members()
        .await?
        .iter()
        .map(|m| format!("{}{}", m.addr, if m.voter { "" } else { " (learner)" }))
        .collect();
    println!("members: {}", list.join(", "));
    Ok(())
}
