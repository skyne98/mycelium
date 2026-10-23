//! Prototype 7 — the wire format for the message channel.
//!
//! One enum, encoded compactly, dispatched by variant. Compare against JSON to
//! see what "lightweight" buys.
//! Run: cargo run --example 07-messages

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
enum Msg {
    Hello {
        node: u64,
        ip: String,
    },
    Set {
        key: String,
        value: String,
        ts: u64,
    },
    /// Hot-path payload: small, lossy-tolerant, sent as a datagram.
    Tick {
        seq: u64,
    },
    Bye,
}

type Handler = Box<dyn Fn(&Msg) + Send>;

#[derive(Default)]
struct Bus {
    handlers: HashMap<&'static str, Handler>,
}

impl Bus {
    fn on(&mut self, topic: &'static str, f: impl Fn(&Msg) + Send + 'static) {
        self.handlers.insert(topic, Box::new(f));
    }
    fn publish(&self, msg: Msg) {
        let topic = match &msg {
            Msg::Hello { .. } => "membership",
            Msg::Set { .. } => "state",
            Msg::Tick { .. } => "hot",
            Msg::Bye => "membership",
        };
        if let Some(h) = self.handlers.get(topic) {
            h(&msg);
        }
    }
}

fn kind(msg: &Msg) -> &'static str {
    match msg {
        Msg::Hello { .. } => "Hello",
        Msg::Set { .. } => "Set",
        Msg::Tick { .. } => "Tick",
        Msg::Bye => "Bye",
    }
}

fn encode(msg: &Msg) -> Vec<u8> {
    bincode::serde::encode_to_vec(msg, bincode::config::standard()).unwrap()
}

fn decode(bytes: &[u8]) -> Msg {
    let (msg, used): (Msg, usize) =
        bincode::serde::decode_from_slice(bytes, bincode::config::standard()).unwrap();
    assert_eq!(used, bytes.len(), "no trailing bytes");
    msg
}

fn main() {
    let samples = [
        Msg::Hello {
            node: 7,
            ip: "100.79.55.103".into(),
        },
        Msg::Set {
            key: "owner".into(),
            value: "fox".into(),
            ts: 1,
        },
        Msg::Tick { seq: 42 },
        Msg::Bye,
    ];

    let mut worst = 0usize;
    for m in &samples {
        let bin = encode(m);
        let json = serde_json::to_vec(m).unwrap();
        let back = decode(&bin);
        assert_eq!(&back, m, "roundtrip");
        println!(
            "{:<6} {:>3} bytes (json {:>3})",
            kind(m),
            bin.len(),
            json.len()
        );
        worst = worst.max(bin.len());
    }
    assert!(worst <= 32, "hot path messages must stay tiny, got {worst}");

    let mut bus = Bus::default();
    bus.on("hot", |m| println!("  hot handler got {m:?}"));
    for m in &samples {
        bus.publish(m.clone());
    }
    bus.on("state", |m| println!("  state handler got {m:?}"));
    bus.publish(Msg::Set {
        key: "k".into(),
        value: "v".into(),
        ts: 9,
    });

    println!("ok: {worst}-byte worst case, typed roundtrip, topic dispatch");
}
