use mycelium::{Client, Peer};
use std::time::Instant;

#[tokio::main]
async fn main() -> mycelium::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let peers: Vec<Peer> = args[1..]
        .iter()
        .map(|s| {
            let (a, n) = s.split_once('=').expect("addr=name");
            Peer::verified(a.parse().unwrap(), n)
        })
        .collect();
    let db = Client::any(peers).connect().await?;
    for m in db.members().await? {
        println!("member {} voter={}", m.addr, m.voter);
    }
    let s = db.stream::<u64>("cross");
    let n = s.len().await?;
    let t = Instant::now();
    for i in 0..20u64 {
        s.append(&(n + i)).await?;
    }
    println!("20 appends in {:?}, len {}", t.elapsed(), s.len().await?);
    Ok(())
}
