//! Prototype 1 — conflict resolution with zero dependencies.
//!
//! A last-write-wins register keyed by (lamport, node). Deterministic: every
//! peer that sees the same writes picks the same winner, in any order.
//! Run: cargo run --example 01-lww

use std::collections::HashMap;

/// Total order. Node id breaks counter ties, so merge is deterministic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Stamp {
    counter: u64,
    node: u64,
}

#[derive(Clone, Debug, PartialEq)]
struct Reg {
    value: String,
    stamp: Stamp,
}

impl Reg {
    fn set(&mut self, value: &str, stamp: Stamp) {
        if stamp > self.stamp {
            self.value = value.to_string();
            self.stamp = stamp;
        }
    }
    fn merge(&mut self, other: &Reg) {
        self.set(&other.value, other.stamp);
    }
}

#[derive(Default, Clone, Debug)]
struct Space {
    keys: HashMap<String, Reg>,
}

impl Space {
    fn set(&mut self, key: &str, value: &str, stamp: Stamp) {
        self.keys
            .entry(key.to_string())
            .and_modify(|r| r.set(value, stamp))
            .or_insert_with(|| Reg {
                value: value.to_string(),
                stamp,
            });
    }
    /// Commutative, associative, idempotent -> order of merges cannot matter.
    fn merge(&mut self, other: &Space) {
        for (k, r) in &other.keys {
            self.keys
                .entry(k.clone())
                .and_modify(|mine| mine.merge(r))
                .or_insert(r.clone());
        }
    }
    fn read(&self, key: &str) -> Option<&str> {
        self.keys.get(key).map(|r| r.value.as_str())
    }
}

fn main() {
    // Two peers, partitioned. Both think they own "color".
    let mut a = Space::default();
    let mut b = Space::default();
    a.set(
        "color",
        "red",
        Stamp {
            counter: 1,
            node: 1,
        },
    );
    b.set(
        "color",
        "blue",
        Stamp {
            counter: 1,
            node: 2,
        },
    );

    // They only agree on "mode".
    a.set(
        "mode",
        "dark",
        Stamp {
            counter: 1,
            node: 1,
        },
    );
    b.merge(&a);

    // Partition heals. Merge in opposite orders on purpose.
    let (na, nb) = (a.clone(), b.clone());
    a.merge(&nb);
    b.merge(&na);
    println!("A: {a:?}");
    println!("B: {b:?}");

    assert_eq!(a.read("color"), b.read("color"), "must converge");
    assert_eq!(
        a.read("color"),
        Some("blue"),
        "tie broken by node id, not by arrival"
    );
    assert_eq!(a.read("mode"), Some("dark"));

    // Idempotent: merging twice changes nothing.
    let once = a.clone();
    a.merge(&b);
    a.merge(&nb);
    assert_eq!(
        format!("{a:?}"),
        format!("{once:?}"),
        "merge must be idempotent"
    );

    println!(
        "ok: converged on {:?} from 2 concurrent writes",
        a.read("color").unwrap()
    );
}
