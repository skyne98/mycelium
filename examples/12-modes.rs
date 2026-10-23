//! Prototype 12 — one library, two modes, chosen per stream.
//!
//! `Ordered`: a sequencer assigns indices. Everyone sees one order. Needs a node.
//! `Merged`:  peers exchange CRDT snapshots. Nobody assigns anything. Needs nobody.
//!
//! The demo runs both at once between the same two peers, and shows why the
//! choice is per stream: the same two writers, the same connection type,
//! different guarantees.
//!
//! Run: cargo run --example 12-modes

mod util;

use loro::{ExportMode, LoroDoc};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};

const APPEND: u8 = 1;
const FOLLOW: u8 = 2;
const SYNC_IN: u8 = 3;

// ---------------------------------------------------------------- ordered mode

struct Sequencer {
    log: Mutex<Vec<String>>,
    tx: broadcast::Sender<()>,
}

impl Sequencer {
    fn new() -> Arc<Self> {
        let (tx, _) = broadcast::channel(8);
        Arc::new(Self {
            log: Mutex::new(Vec::new()),
            tx,
        })
    }

    async fn append(&self, body: &str) -> u64 {
        let mut l = self.log.lock().await;
        let idx = l.len() as u64;
        l.push(body.to_string());
        let _ = self.tx.send(());
        idx
    }

    async fn since(&self, from: u64) -> Vec<String> {
        let l = self.log.lock().await;
        if from as usize >= l.len() {
            return Vec::new();
        }
        l[from as usize..].to_vec()
    }
}

async fn serve_sequencer(ep: quinn::Endpoint, seq: Arc<Sequencer>) {
    while let Some(inc) = ep.accept().await {
        let Ok(conn) = inc.await else { continue };
        let seq = seq.clone();
        tokio::spawn(async move {
            while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                let seq = seq.clone();
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
                    match role[0] {
                        APPEND => {
                            // body = [len u32][text]
                            let idx = seq.append(&String::from_utf8_lossy(&body[4..])).await;
                            let _ = send.write_all(&idx.to_be_bytes()).await;
                        }
                        FOLLOW => {
                            let from = u64::from_be_bytes(body[..8].try_into().unwrap());
                            let want = u32::from_be_bytes(body[8..12].try_into().unwrap()) as usize;
                            let mut rx = seq.tx.subscribe();
                            let mut next = from;
                            let mut sent = 0usize;
                            while sent < want {
                                for text in seq.since(next).await {
                                    let _ = send.write_all(&next.to_be_bytes()).await;
                                    let _ =
                                        send.write_all(&(text.len() as u32).to_be_bytes()).await;
                                    let _ = send.write_all(text.as_bytes()).await;
                                    next += 1;
                                    sent += 1;
                                }
                                if sent < want && rx.recv().await.is_err() {
                                    break;
                                }
                            }
                            let _ = send.finish();
                        }
                        _ => {}
                    }
                });
            }
        });
    }
}

struct Ordered {
    conn: quinn::Connection,
}

impl Ordered {
    async fn append(&self, body: &str) -> Result<u64, String> {
        let (mut send, mut recv) = self.conn.open_bi().await.map_err(|e| e.to_string())?;
        let mut p = (body.len() as u32).to_be_bytes().to_vec();
        p.extend_from_slice(body.as_bytes());
        send.write_all(&[APPEND]).await.map_err(|e| e.to_string())?;
        send.write_all(&(p.len() as u32).to_be_bytes())
            .await
            .map_err(|e| e.to_string())?;
        send.write_all(&p).await.map_err(|e| e.to_string())?;
        let mut ib = [0u8; 8];
        recv.read_exact(&mut ib).await.map_err(|e| e.to_string())?;
        Ok(u64::from_be_bytes(ib))
    }

    /// Reads the total order from `from`, `want` entries.
    async fn follow(&self, from: u64, want: usize) -> Result<Vec<(u64, String)>, String> {
        let (mut send, mut recv) = self.conn.open_bi().await.map_err(|e| e.to_string())?;
        let mut p = from.to_be_bytes().to_vec();
        p.extend_from_slice(&(want as u32).to_be_bytes());
        send.write_all(&[FOLLOW]).await.map_err(|e| e.to_string())?;
        send.write_all(&(p.len() as u32).to_be_bytes())
            .await
            .map_err(|e| e.to_string())?;
        send.write_all(&p).await.map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for _ in 0..want {
            let mut ib = [0u8; 8];
            recv.read_exact(&mut ib).await.map_err(|e| e.to_string())?;
            let mut lb = [0u8; 4];
            recv.read_exact(&mut lb).await.map_err(|e| e.to_string())?;
            let mut body = vec![0u8; u32::from_be_bytes(lb) as usize];
            recv.read_exact(&mut body)
                .await
                .map_err(|e| e.to_string())?;
            out.push((
                u64::from_be_bytes(ib),
                String::from_utf8_lossy(&body).to_string(),
            ));
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------- merged mode

/// Merged mode: send my whole state, take theirs back. No sequencer, no server
/// role beyond being the endpoint that happens to be listening.
async fn sync_with_peer(conn: &quinn::Connection, doc: &LoroDoc) -> Result<(), String> {
    doc.commit();
    let mine = doc
        .export(ExportMode::Snapshot)
        .map_err(|e| e.to_string())?;
    let (mut send, mut recv) = conn.open_bi().await.map_err(|e| e.to_string())?;
    send.write_all(&[SYNC_IN])
        .await
        .map_err(|e| e.to_string())?;
    send.write_all(&(mine.len() as u32).to_be_bytes())
        .await
        .map_err(|e| e.to_string())?;
    send.write_all(&mine).await.map_err(|e| e.to_string())?;
    let _ = send.finish();

    let mut lb = [0u8; 4];
    recv.read_exact(&mut lb).await.map_err(|e| e.to_string())?;
    let mut theirs = vec![0u8; u32::from_be_bytes(lb) as usize];
    recv.read_exact(&mut theirs)
        .await
        .map_err(|e| e.to_string())?;
    doc.import(&theirs).map_err(|e| e.to_string())?;
    doc.commit();
    Ok(())
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tokio::runtime::Runtime::new()?.block_on(async_main())
}

async fn async_main() -> Result<(), Box<dyn Error + Send + Sync>> {
    util::install_crypto();

    // One node is a sequencer. That is the only asymmetry in the whole demo.
    let (sep, _c1) = util::server("127.0.0.1:0".parse()?).map_err(|e| e.to_string())?;
    let seq_addr: SocketAddr = sep.local_addr().map_err(|e| e.to_string())?;
    let seq = Sequencer::new();
    tokio::spawn(serve_sequencer(sep, seq.clone()));

    // Peer A also listens: merged mode has no server, so A is just a peer.
    let (aep, _c2) = util::server("127.0.0.1:0".parse()?).map_err(|e| e.to_string())?;
    let a_addr: SocketAddr = aep.local_addr().map_err(|e| e.to_string())?;
    let doc_a = LoroDoc::new();
    doc_a.set_peer_id(1)?;
    doc_a.get_map("presence").insert("a", "online")?;
    doc_a.commit();

    let doc_a_srv = doc_a.clone();
    tokio::spawn(async move {
        while let Some(inc) = aep.accept().await {
            let Ok(conn) = inc.await else { continue };
            let doc = doc_a_srv.clone();
            tokio::spawn(async move {
                while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                    let doc = doc.clone();
                    tokio::spawn(async move {
                        let mut role = [0u8; 1];
                        if recv.read_exact(&mut role).await.is_err() {
                            return;
                        }
                        let mut lb = [0u8; 4];
                        if recv.read_exact(&mut lb).await.is_err() {
                            return;
                        }
                        let mut theirs = vec![0u8; u32::from_be_bytes(lb) as usize];
                        if recv.read_exact(&mut theirs).await.is_err() {
                            return;
                        }
                        let _ = doc.import(&theirs);
                        doc.commit();
                        let mine = doc.export(ExportMode::Snapshot).unwrap_or_default();
                        let _ = send.write_all(&(mine.len() as u32).to_be_bytes()).await;
                        let _ = send.write_all(&mine).await;
                        let _ = send.finish();
                    });
                }
            });
        }
    });

    // Peer B.
    let doc_b = LoroDoc::new();
    doc_b.set_peer_id(2)?;
    doc_b.get_map("presence").insert("b", "online")?;
    doc_b.commit();

    let ea = util::insecure_client().map_err(|e| e.to_string())?;
    let eb = util::insecure_client().map_err(|e| e.to_string())?;
    let ca_seq = ea.connect(seq_addr, "mycelium")?.await?;
    let cb_seq = eb.connect(seq_addr, "mycelium")?.await?;
    // B reaches A directly for merged mode; A's doc is what its listener holds.
    let cb_peer = eb.connect(a_addr, "mycelium")?.await?;

    let ordered_a = Ordered { conn: ca_seq };
    let ordered_b = Ordered { conn: cb_seq };

    // ---- merged mode: both peers write, nobody coordinates ----------------
    doc_a.get_map("presence").insert("a", "busy")?;
    doc_b.get_map("presence").insert("b", "away")?;
    doc_a.commit();
    doc_b.commit();
    sync_with_peer(&cb_peer, &doc_b).await?;
    let a_view = format!("{:?}", doc_a.get_deep_value());
    let b_view = format!("{:?}", doc_b.get_deep_value());
    assert_eq!(
        a_view, b_view,
        "merged peers must converge with no coordinator"
    );

    // ---- ordered mode: both writers, one sequencer ------------------------
    let mut handles = Vec::new();
    for (tag, ord) in [(0usize, &ordered_a), (1usize, &ordered_b)] {
        let tag = if tag == 0 { "a" } else { "b" };
        for i in 0..5 {
            let body = format!("{tag}{i}");
            let conn = ord.conn.clone();
            handles.push(tokio::spawn(async move {
                let o = Ordered { conn };
                o.append(&body).await
            }));
            let _ = i;
        }
    }
    let mut indices = Vec::new();
    for h in handles {
        indices.push(h.await.map_err(|e| e.to_string())??);
    }
    indices.sort();
    assert_eq!(
        indices,
        (0..10).collect::<Vec<u64>>(),
        "sequencer must hand out 0..10 once each"
    );

    let view_a = ordered_a.follow(0, 10).await?;
    let view_b = ordered_b.follow(0, 10).await?;
    assert_eq!(view_a, view_b, "both readers must see the same order");

    // ---- the contrast ----------------------------------------------------
    let seq_of = |v: &[(u64, String)]| {
        v.iter()
            .map(|(_, s)| s.as_str())
            .collect::<Vec<_>>()
            .join(" ")
    };
    println!();
    println!("  merged mode   2 peers wrote concurrently, nobody assigned anything,");
    println!("                both converged without the sequencer being involved");
    println!(
        "                both writes survive: {:?}",
        doc_a.get_map("presence").get_deep_value()
    );
    println!();
    println!("  ordered mode  2 writers, one sequencer, indices 0..10 each handed out once");
    println!("                reader A: {}", seq_of(&view_a));
    println!("                reader B: {}", seq_of(&view_b));
    println!();
    println!("ok: same peers, same transport, two different guarantees, picked per stream");
    Ok(())
}
