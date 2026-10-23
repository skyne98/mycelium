//! Prototype 2 — real CRDT document, offline edits, merge.
//!
//! Proves the "don't write conflict resolution yourself" claim: loro merges
//! concurrent text/map edits with no coordinator.
//! Run: cargo run --example 02-crdt

use loro::{ExportMode, LoroDoc, LoroValue};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let a = LoroDoc::new();
    a.set_peer_id(1)?;
    a.get_text("notes").insert(0, "hello world")?;
    a.get_map("meta").insert("owner", "fox")?;
    a.commit();

    // Peer B joins from a snapshot.
    let b = LoroDoc::new();
    b.set_peer_id(2)?;
    b.import(&a.export(ExportMode::Snapshot)?)?;

    // Partition: both edit the same text and the same map key, concurrently.
    a.get_text("notes").insert(0, "A saw: ")?;
    a.get_map("meta").insert("owner", "alice")?;
    a.commit();

    b.get_text("notes").insert(11, " and B")?;
    b.get_map("meta").insert("owner", "bob")?;
    b.commit();

    // Heal: exchange full state both ways.
    a.import(&b.export(ExportMode::Snapshot)?)?;
    b.import(&a.export(ExportMode::Snapshot)?)?;

    let (ta, tb) = (
        a.get_text("notes").to_string(),
        b.get_text("notes").to_string(),
    );
    println!("A text: {ta:?}");
    println!("B text: {tb:?}");
    println!("A owner: {:?}", a.get_map("meta").get_deep_value());
    println!("B owner: {:?}", b.get_map("meta").get_deep_value());

    assert_eq!(ta, tb, "text must converge");
    assert!(
        ta.contains("A saw: ") && ta.contains(" and B"),
        "both edits survive: {ta}"
    );

    let (oa, ob) = (
        a.get_map("meta").get_deep_value(),
        b.get_map("meta").get_deep_value(),
    );
    assert_eq!(oa, ob, "map must converge");
    assert_eq!(a.get_deep_value(), b.get_deep_value());
    assert_ne!(a.get_deep_value(), LoroValue::Null);

    // Idempotent under replay.
    let before = ta.clone();
    a.import(&b.export(ExportMode::Snapshot)?)?;
    assert_eq!(a.get_text("notes").to_string(), before);

    println!("ok: 2 concurrent writers on one doc, no coordinator, converged");
    Ok(())
}
