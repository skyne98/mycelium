//! The front door: two primitives, typed, no config.
//!
//!   cargo run --example hello                              # one process, no sockets
//!   cargo run --example hello serve   [bind]               # standalone node
//!   cargo run --example hello cluster <addr> <peer>...     # one node of a new cluster
//!   cargo run --example hello add     <addr> <via>...      # join a running cluster
//!   cargo run --example hello tailnet <port>               # cluster with every tailnet machine
//!   cargo run --example hello join    <addr>[=name]...     # attach (fails over) and use
//!
//! A peer is `addr` (unverified, loopback only) or `addr=name` (verified).
//! MYCELIUM_CERT / MYCELIUM_KEY: serve with real certificates (tailscale cert).
//! MYCELIUM_DATA: where the log and maps live.

use mycelium::{Client, Mycelium, Node, Peer};
use serde::{Deserialize, Serialize};
use std::error::Error;

#[derive(Serialize, Deserialize, Debug)]
struct Msg {
    who: String,
    #[serde(default)]
    text: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
enum Status {
    Up,
    Down,
}

fn peer(spec: &str) -> Peer {
    match spec.split_once('=') {
        Some((addr, name)) => Peer::verified(addr.parse().expect("peer address"), name),
        None => Peer::local(spec.parse().expect("peer address")),
    }
}

fn configured(mut node: Node) -> Node {
    if let (Ok(cert), Ok(key)) = (
        std::env::var("MYCELIUM_CERT"),
        std::env::var("MYCELIUM_KEY"),
    ) {
        node = node.with_tls(cert, key);
    }
    if let Ok(dir) = std::env::var("MYCELIUM_DATA") {
        node = node.with_data_dir(dir);
    }
    node
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().collect();
    let arg = |i: usize, d: &str| args.get(i).map(String::as_str).unwrap_or(d).to_string();

    match args.get(1).map(String::as_str) {
        Some("serve") => {
            configured(Node::bind(arg(2, "127.0.0.1:7777").parse()?))
                .serve()
                .await?
        }
        Some("cluster") => {
            let peers = args[3..].iter().map(|s| peer(s)).collect();
            configured(Node::cluster(arg(2, "127.0.0.1:7777").parse()?, peers))
                .serve()
                .await?
        }
        Some("add") => {
            let via = args[3..].iter().map(|s| peer(s)).collect();
            configured(Node::join(arg(2, "127.0.0.1:7777").parse()?, via))
                .serve()
                .await?
        }
        Some("tailnet") => {
            let port: u16 = arg(2, "7777").parse()?;
            let me = mycelium::tailnet_self(port)?;
            let peers = mycelium::tailnet_peers(port)?;
            println!("cluster: {} + {} peers", me.addr, peers.len());
            configured(Node::cluster(me.addr, peers)).serve().await?
        }
        Some("join") => {
            let specs = if args.len() > 2 {
                args[2..].to_vec()
            } else {
                vec!["127.0.0.1:7777".into()]
            };
            run(Client::any(specs.iter().map(|s| peer(s))).connect().await?).await?
        }
        _ => run(Mycelium::local()).await?,
    }
    Ok(())
}

async fn run(node: Mycelium) -> Result<(), Box<dyn Error + Send + Sync>> {
    let chat = node.stream::<Msg>("chat");
    let presence = node.map::<String, Status>("presence");

    let first = chat
        .append(&Msg {
            who: "laptop".into(),
            text: "hello".into(),
        })
        .await?;
    chat.append(&Msg {
        who: "vps".into(),
        text: "hi back".into(),
    })
    .await?;
    presence.set(&"laptop".into(), &Status::Up).await?;
    presence.set(&"vps".into(), &Status::Down).await?;

    let mut rx = chat.watch(first).await?;
    for _ in 0..2 {
        let (idx, msg) = rx.recv().await.ok_or("stream closed")?;
        println!("  [{idx}] {}: {}", msg.who, msg.text);
    }
    for (who, status) in presence.entries().await? {
        println!("  {who} is {status:?}");
    }
    println!(
        "ok: {} entries, {} skipped",
        chat.len().await?,
        chat.skipped()
    );
    Ok(())
}
