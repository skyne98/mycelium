//! Prototype 6 — the "single computer" illusion, and what breaks it.
//!
//! N in-process peers, each with a full copy of the document. Gossip is
//! snapshot exchange. A peer goes offline, keeps working, and merges cleanly
//! when it returns — that is the self-healing part.
//! Run: cargo run --example 06-space

use loro::{ExportMode, LoroDoc, LoroMap, LoroText};
use std::error::Error;

struct Peer {
    id: u64,
    doc: LoroDoc,
    online: bool,
}

impl Peer {
    fn new(id: u64) -> Self {
        let doc = LoroDoc::new();
        doc.set_peer_id(id).unwrap();
        Self {
            id,
            doc,
            online: true,
        }
    }
    fn text(&self) -> LoroText {
        self.doc.get_text("log")
    }
    fn map(&self) -> LoroMap {
        self.doc.get_map("state")
    }
    fn snapshot(&self) -> Vec<u8> {
        self.doc.commit();
        self.doc.export(ExportMode::Snapshot).unwrap()
    }
    fn absorb(&self, bytes: &[u8]) {
        self.doc.import(bytes).unwrap();
        self.doc.commit();
    }
}

/// One gossip round: every online peer pushes its snapshot to every other.
/// ponytail: O(n^2) full snapshots — fine for a prototype, swap to deltas +
/// a spanning tree when n or document size actually matters.
fn gossip(peers: &mut [Peer]) {
    let snaps: Vec<Option<Vec<u8>>> = peers
        .iter()
        .map(|p| p.online.then(|| p.snapshot()))
        .collect();
    for (i, peer) in peers.iter_mut().enumerate() {
        if !peer.online {
            continue;
        }
        for (j, snap) in snaps.iter().enumerate() {
            if i != j {
                if let Some(s) = snap {
                    peer.absorb(s);
                }
            }
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut peers: Vec<Peer> = (1..=4).map(Peer::new).collect();

    for p in &peers {
        p.text()
            .insert(p.text().len_unicode(), &format!("[{} joined] ", p.id))?;
        p.map().insert(&format!("p{}", p.id), p.id as i64)?;
        p.doc.commit();
    }
    gossip(&mut peers);

    // Partition: peer 3 drops off the network.
    peers[2].online = false;
    println!("peer 3 goes offline");

    // The live cluster keeps working.
    for p in peers.iter().filter(|p| p.online) {
        p.text()
            .insert(p.text().len_unicode(), &format!("[{} alive] ", p.id))?;
        p.doc.commit();
    }
    // Peer 3 keeps working too, from its stale copy.
    peers[2]
        .text()
        .insert(peers[2].text().len_unicode(), "[3 lonely] ")?;
    peers[2].map().insert("p3", 333).unwrap();
    peers[2].doc.commit();

    gossip(&mut peers);

    // Partition heals.
    peers[2].online = true;
    for _ in 0..2 {
        gossip(&mut peers);
    }

    let texts: Vec<String> = peers.iter().map(|p| p.text().to_string()).collect();
    println!("peer 1 log: {:?}", texts[0]);
    println!("peer 3 log: {:?}", texts[2]);

    for (i, t) in texts.iter().enumerate() {
        assert_eq!(t, &texts[0], "peer {} diverged", i + 1);
    }
    assert!(
        texts[0].contains("[3 lonely]"),
        "offline work survived the merge"
    );
    assert!(texts[0].contains("[1 alive]"));
    let mk0 = peers[0].map().get_deep_value();
    let m0 = mk0.as_map().unwrap();
    assert_eq!(m0.get("p3"), Some(&333.into()), "offline write survived");
    assert_eq!(m0.get("p1"), Some(&1.into()));
    for p in &peers[1..] {
        assert_eq!(p.map().get_deep_value(), mk0, "maps converged");
    }

    println!("ok: 4 peers, 1 partition, 0 coordinators, all state identical");
    Ok(())
}
