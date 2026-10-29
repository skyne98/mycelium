//! Extensive tests for the public API: local and over a real socket.

use mycelium::{Mycelium, Peer, Raw};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct Msg {
    who: String,
    #[serde(default)]
    text: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
enum Cmd {
    Ping(u64),
    Named { id: u32, tag: String },
    Nothing,
}

async fn recv<T>(rx: &mut Receiver<T>) -> Option<T> {
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .ok()
        .flatten()
}

async fn node_over_socket() -> SocketAddr {
    let addr = free_addr();
    let dir = fresh_dir(&format!("solo-{}", addr.port()));
    tokio::spawn(async move {
        let _ = mycelium::Node::bind(addr).with_data_dir(dir).serve().await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    addr
}

// ------------------------------------------------------------------- local

#[tokio::test]
async fn appends_are_dense_and_ordered() {
    let node = Mycelium::local();
    let s = node.stream::<u64>("n");
    for i in 0..100u64 {
        assert_eq!(s.append(&i).await.unwrap(), i);
    }
    assert_eq!(s.len().await.unwrap(), 100);

    let mut rx = s.watch(0).await.unwrap();
    for i in 0..100u64 {
        let (idx, v) = recv(&mut rx).await.expect("entry");
        assert_eq!(idx, i);
        assert_eq!(v, i);
    }
}

#[tokio::test]
async fn watch_resumes_from_a_cursor() {
    let node = Mycelium::local();
    let s = node.stream::<String>("s");
    for i in 0..5 {
        s.append(&format!("v{i}")).await.unwrap();
    }
    let mut rx = s.watch(3).await.unwrap();
    let (idx, v) = recv(&mut rx).await.unwrap();
    assert_eq!((idx, v.as_str()), (3, "v3"));
    let (idx, v) = recv(&mut rx).await.unwrap();
    assert_eq!((idx, v.as_str()), (4, "v4"));
    assert!(
        recv(&mut rx).await.is_none(),
        "cursor must not replay earlier entries"
    );
}

#[tokio::test]
async fn watch_delivers_live_appends() {
    let node = Mycelium::local();
    let s = node.stream::<Msg>("live");
    let mut rx = s.watch(0).await.unwrap();
    s.append(&Msg {
        who: "a".into(),
        text: "late".into(),
    })
    .await
    .unwrap();
    let (idx, msg) = recv(&mut rx).await.unwrap();
    assert_eq!(idx, 0);
    assert_eq!(msg.text, "late");
}

#[tokio::test]
async fn watchers_are_independent() {
    let node = Mycelium::local();
    let s = node.stream::<u64>("multi");
    let mut a = s.watch(0).await.unwrap();
    let mut b = s.watch(0).await.unwrap();
    s.append(&7).await.unwrap();
    assert_eq!(recv(&mut a).await.unwrap(), (0, 7));
    assert_eq!(recv(&mut b).await.unwrap(), (0, 7));
}

#[tokio::test]
async fn every_serde_shape_roundtrips() {
    let node = Mycelium::local();

    let cmds = node.stream::<Cmd>("cmds");
    let inputs = vec![
        Cmd::Ping(42),
        Cmd::Named {
            id: 7,
            tag: "x".into(),
        },
        Cmd::Nothing,
    ];
    for c in &inputs {
        cmds.append(c).await.unwrap();
    }
    let mut rx = cmds.watch(0).await.unwrap();
    for c in &inputs {
        assert_eq!(&recv(&mut rx).await.unwrap().1, c);
    }

    let strings = node.stream::<String>("strings");
    strings.append(&"".to_string()).await.unwrap();
    strings.append(&"héllo → 🌍".to_string()).await.unwrap();
    let mut rx = strings.watch(0).await.unwrap();
    assert_eq!(recv(&mut rx).await.unwrap().1, "");
    assert_eq!(recv(&mut rx).await.unwrap().1, "héllo → 🌍");

    let big = vec![7u8; 1 << 20];
    let blobs = node.stream::<Vec<u8>>("blobs");
    blobs.append(&big).await.unwrap();
    let mut rx = blobs.watch(0).await.unwrap();
    assert_eq!(recv(&mut rx).await.unwrap().1.len(), 1 << 20);
}

#[tokio::test]
async fn names_do_not_leak() {
    let node = Mycelium::local();
    let a = node.stream::<String>("a");
    let b = node.stream::<String>("b");
    a.append(&"from-a".into()).await.unwrap();
    assert_eq!(a.len().await.unwrap(), 1);
    assert_eq!(b.len().await.unwrap(), 0);
    let mut rx = b.watch(0).await.unwrap();
    assert!(recv(&mut rx).await.is_none());
}

#[tokio::test]
async fn a_different_type_is_skipped_and_counted() {
    let node = Mycelium::local();
    let chat = node.stream::<Msg>("chat");
    let counter = node.stream::<u64>("chat");

    chat.append(&Msg {
        who: "a".into(),
        text: "one".into(),
    })
    .await
    .unwrap();
    chat.append(&Msg {
        who: "b".into(),
        text: "two".into(),
    })
    .await
    .unwrap();

    let mut rx = counter.watch(0).await.unwrap();
    assert!(
        recv(&mut rx).await.is_none(),
        "foreign entries must not decode"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(counter.skipped(), 2);

    // The original reader is unaffected.
    let mut rx = chat.watch(0).await.unwrap();
    assert_eq!(recv(&mut rx).await.unwrap().1.text, "one");
    assert_eq!(chat.skipped(), 0);
}

#[tokio::test]
async fn raw_sees_every_entry() {
    let node = Mycelium::local();
    let s = node.stream::<Msg>("mixed");
    s.append(&Msg {
        who: "typed".into(),
        text: "x".into(),
    })
    .await
    .unwrap();
    s.append_raw(b"plain bytes").await.unwrap();

    let mut rx = s.watch_raw(0).await.unwrap();
    let (idx, Raw(first)) = recv(&mut rx).await.unwrap();
    assert_eq!(idx, 0);
    assert!(!first.is_empty(), "typed entry body should not be empty");

    let (idx, Raw(second)) = recv(&mut rx).await.unwrap();
    assert_eq!(idx, 1);
    assert_eq!(second, b"plain bytes");

    // A typed reader skips the raw entry.
    let mut typed = s.watch(0).await.unwrap();
    assert_eq!(recv(&mut typed).await.unwrap().1.who, "typed");
}

#[tokio::test]
async fn concurrent_appenders_get_unique_indices() {
    let node = Mycelium::local();
    let s = Arc::new(node.stream::<u64>("concurrent"));
    let mut tasks = Vec::new();
    for t in 0..20u64 {
        let s = s.clone();
        tasks.push(tokio::spawn(async move {
            let mut idxs = Vec::new();
            for i in 0..10u64 {
                idxs.push(s.append(&(t * 100 + i)).await.unwrap());
            }
            idxs
        }));
    }
    let mut all = Vec::new();
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    let unique: HashSet<u64> = all.iter().copied().collect();
    assert_eq!(unique.len(), 200, "indices must be unique");
    assert_eq!(s.len().await.unwrap(), 200);

    let mut rx = s.watch(0).await.unwrap();
    for i in 0..200u64 {
        assert_eq!(
            recv(&mut rx).await.unwrap().0,
            i,
            "indices must arrive in order"
        );
    }
}

#[tokio::test]
async fn map_overwrites_and_many_keys() {
    let node = Mycelium::local();
    let m = node.map::<String, u64>("counters");
    for i in 0..50u64 {
        m.set(&format!("k{i}"), &i).await.unwrap();
    }
    assert_eq!(m.get(&"k0".into()).await.unwrap(), Some(0));
    assert_eq!(m.get(&"k49".into()).await.unwrap(), Some(49));

    m.set(&"k0".into(), &999).await.unwrap();
    assert_eq!(m.get(&"k0".into()).await.unwrap(), Some(999));
    assert_eq!(m.get(&"missing".into()).await.unwrap(), None);
}

#[tokio::test]
async fn map_type_mismatch_is_none_not_an_error() {
    let node = Mycelium::local();
    let as_u64 = node.map::<String, u64>("cfg");
    as_u64.set(&"port".into(), &8080).await.unwrap();

    let as_string = node.map::<String, String>("cfg");
    assert_eq!(as_string.get(&"port".into()).await.unwrap(), None);
    assert_eq!(as_u64.get(&"port".into()).await.unwrap(), Some(8080));
}

#[tokio::test]
async fn map_keys_are_typed_too() {
    let node = Mycelium::local();
    let by_str = node.map::<String, u64>("m");
    let by_int = node.map::<u32, u64>("m");
    by_str.set(&"1".into(), &10).await.unwrap();
    by_int.set(&1u32, &20).await.unwrap();
    assert_eq!(by_str.get(&"1".into()).await.unwrap(), Some(10));
    assert_eq!(by_int.get(&1u32).await.unwrap(), Some(20));
    assert_eq!(
        by_str.get(&"1".into()).await.unwrap(),
        Some(10),
        "typed keys do not collide"
    );
}

// ------------------------------------------------------------------ remote

#[tokio::test]
async fn remote_append_watch_and_len() {
    let addr = node_over_socket().await;
    let node = Mycelium::join(addr).await.unwrap();
    let s = node.stream::<Msg>("chat");

    assert_eq!(s.len().await.unwrap(), 0);
    assert_eq!(
        s.append(&Msg {
            who: "a".into(),
            text: "one".into()
        })
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        s.append(&Msg {
            who: "a".into(),
            text: "two".into()
        })
        .await
        .unwrap(),
        1
    );
    assert_eq!(s.len().await.unwrap(), 2);

    let mut rx = s.watch(0).await.unwrap();
    assert_eq!(recv(&mut rx).await.unwrap().1.text, "one");
    assert_eq!(recv(&mut rx).await.unwrap().1.text, "two");
}

#[tokio::test]
async fn remote_watch_is_live_and_resumable() {
    let addr = node_over_socket().await;
    let writer = Mycelium::join(addr).await.unwrap();
    let reader = Mycelium::join(addr).await.unwrap();
    let ws = writer.stream::<u64>("live");
    let rs = reader.stream::<u64>("live");

    let mut rx = rs.watch(0).await.unwrap();
    ws.append(&1).await.unwrap();
    assert_eq!(recv(&mut rx).await.unwrap(), (0, 1));
    ws.append(&2).await.unwrap();
    assert_eq!(recv(&mut rx).await.unwrap(), (1, 2));

    let mut resumed = rs.watch(1).await.unwrap();
    assert_eq!(recv(&mut resumed).await.unwrap(), (1, 2));
}

#[tokio::test]
async fn remote_two_clients_agree_on_order() {
    let addr = node_over_socket().await;
    let a = Mycelium::join(addr).await.unwrap();
    let b = Mycelium::join(addr).await.unwrap();

    let sa = a.stream::<String>("chat");
    let sb = b.stream::<String>("chat");
    for i in 0..10 {
        let s = if i % 2 == 0 { &sa } else { &sb };
        s.append(&format!("m{i}")).await.unwrap();
    }

    let mut ra = sa.watch(0).await.unwrap();
    let mut rb = sb.watch(0).await.unwrap();
    for i in 0..10 {
        let (ia, va) = recv(&mut ra).await.unwrap();
        let (ib, vb) = recv(&mut rb).await.unwrap();
        assert_eq!((ia, va.clone()), (ib, vb), "clients saw different orders");
        assert_eq!(va, format!("m{i}"));
    }
}

#[tokio::test]
async fn remote_map_roundtrip_and_mismatch() {
    let addr = node_over_socket().await;
    let node = Mycelium::join(addr).await.unwrap();
    let m = node.map::<String, String>("presence");
    m.set(&"laptop".into(), &"up".into()).await.unwrap();
    assert_eq!(m.get(&"laptop".into()).await.unwrap(), Some("up".into()));

    let other = Mycelium::join(addr).await.unwrap();
    let as_u64 = other.map::<String, u64>("presence");
    assert_eq!(as_u64.get(&"laptop".into()).await.unwrap(), None);
}

#[tokio::test]
async fn remote_survives_many_clients() {
    let addr = node_over_socket().await;
    let mut joins = Vec::new();
    for i in 0..8u64 {
        joins.push(tokio::spawn(async move {
            let n = Mycelium::join(addr).await.unwrap();
            let s = n.stream::<u64>("shared");
            for j in 0..10u64 {
                s.append(&(i * 10 + j)).await.unwrap();
            }
        }));
    }
    for j in joins {
        j.await.unwrap();
    }
    let node = Mycelium::join(addr).await.unwrap();
    let s = node.stream::<u64>("shared");
    assert_eq!(s.len().await.unwrap(), 80);

    let mut seen = HashSet::new();
    let mut rx = s.watch_raw(0).await.unwrap();
    for _ in 0..80 {
        let (idx, _) = recv(&mut rx).await.unwrap();
        seen.insert(idx);
    }
    assert_eq!(seen.len(), 80, "every index delivered exactly once");
}

#[tokio::test]
async fn a_slow_watcher_loses_nothing() {
    let node = Mycelium::local();
    let s = node.stream::<u64>("slow");
    let mut rx = s.watch(0).await.unwrap();

    // Nobody reads while 3000 entries land: far past every internal buffer.
    for i in 0..3000u64 {
        s.append(&i).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..3000u64 {
        let (idx, v) = recv(&mut rx)
            .await
            .unwrap_or_else(|| panic!("stalled at {i}"));
        assert_eq!((idx, v), (i, i), "gap or reorder at {i}");
    }
}

#[tokio::test]
async fn a_slow_remote_watcher_loses_nothing() {
    let addr = node_over_socket().await;
    let writer = Mycelium::join(addr).await.unwrap();
    let reader = Mycelium::join(addr).await.unwrap();
    let ws = writer.stream::<u64>("slow-remote");
    let mut rx = reader.stream::<u64>("slow-remote").watch(0).await.unwrap();

    for i in 0..1500u64 {
        ws.append(&i).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..1500u64 {
        let (idx, v) = recv(&mut rx)
            .await
            .unwrap_or_else(|| panic!("stalled at {i}"));
        assert_eq!((idx, v), (i, i), "gap or reorder at {i}");
    }
}

#[tokio::test]
async fn empty_names_and_empty_payloads_are_fine() {
    let node = Mycelium::local();
    let s = node.stream::<u64>("");
    assert_eq!(s.append(&1).await.unwrap(), 0);
    assert_eq!(s.len().await.unwrap(), 1);

    let raw = node.stream::<u64>("raw");
    assert_eq!(raw.append_raw(b"").await.unwrap(), 0);
    let mut rx = raw.watch_raw(0).await.unwrap();
    let (idx, Raw(body)) = recv(&mut rx).await.unwrap();
    assert_eq!(idx, 0);
    assert!(body.is_empty());
}

#[tokio::test]
async fn watching_past_the_end_waits_for_new_entries() {
    let node = Mycelium::local();
    let s = node.stream::<u64>("future");
    s.append(&1).await.unwrap();
    let mut rx = s.watch(50).await.unwrap();
    assert!(recv(&mut rx).await.is_none());

    for _ in 0..50 {
        s.append(&1).await.unwrap();
    }
    let (idx, _) = recv(&mut rx).await.unwrap();
    assert_eq!(idx, 50);
}

#[tokio::test]
async fn many_independent_streams() {
    let node = Mycelium::local();
    for i in 0..100 {
        let s = node.stream::<u64>(&format!("s{i}"));
        s.append(&i).await.unwrap();
    }
    for i in 0..100 {
        let s = node.stream::<u64>(&format!("s{i}"));
        assert_eq!(s.len().await.unwrap(), 1);
        let mut rx = s.watch(0).await.unwrap();
        assert_eq!(recv(&mut rx).await.unwrap().1, i);
    }
}

#[tokio::test]
async fn concurrent_map_writers_do_not_error() {
    let addr = node_over_socket().await;
    let mut tasks = Vec::new();
    for i in 0..10u64 {
        tasks.push(tokio::spawn(async move {
            let n = Mycelium::join(addr).await.unwrap();
            let m = n.map::<String, u64>("shared");
            for j in 0..20u64 {
                m.set(&format!("k{}", j % 5), &(i * 100 + j)).await.unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let node = Mycelium::join(addr).await.unwrap();
    let m = node.map::<String, u64>("shared");
    for j in 0..5 {
        assert!(m.get(&format!("k{j}")).await.unwrap().is_some());
    }
}

/// Runs against a real node when MYCELIUM_TEST_ADDR is set, e.g.
/// cargo test --test extensive -- --ignored cross_machine
#[tokio::test]
#[ignore]
async fn cross_machine() {
    let Ok(raw) = std::env::var("MYCELIUM_TEST_ADDR") else {
        eprintln!("skipped: set MYCELIUM_TEST_ADDR=host:port");
        return;
    };
    let connect = || async {
        match raw.split_once('=') {
            Some((a, name)) => {
                mycelium::Client::verified(a.parse().unwrap(), name)
                    .connect()
                    .await
            }
            None => Mycelium::join(raw.parse().unwrap()).await,
        }
    };

    let a = connect().await.unwrap();
    let b = connect().await.unwrap();
    let name = format!("x-{}", std::process::id());

    let sa = a.stream::<Msg>(&name);
    let sb = b.stream::<Msg>(&name);
    for i in 0..20 {
        sa.append(&Msg {
            who: "a".into(),
            text: format!("m{i}"),
        })
        .await
        .unwrap();
    }
    assert_eq!(sa.len().await.unwrap(), 20);

    let mut ra = sa.watch(0).await.unwrap();
    let mut rb = sb.watch(0).await.unwrap();
    for i in 0..20 {
        let (ia, va) = recv(&mut ra).await.unwrap();
        let (ib, vb) = recv(&mut rb).await.unwrap();
        assert_eq!((ia, va.clone()), (ib, vb));
        assert_eq!(va.text, format!("m{i}"));
    }

    let ma = a.map::<String, Msg>("presence");
    let mb = b.map::<String, Msg>("presence");
    ma.set(
        &"x".into(),
        &Msg {
            who: "a".into(),
            text: "up".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(mb.get(&"x".into()).await.unwrap().unwrap().text, "up");
}

#[tokio::test]
async fn random_operations_keep_the_invariants() {
    let node = Mycelium::local();
    let s = node.stream::<u64>("rand");
    let m = node.map::<u64, u64>("rand-map");
    let mut seed = 0x2545_f491_4f6c_dd1du64;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };

    let mut expected = Vec::new();
    for step in 0..500u64 {
        if next() % 3 == 0 {
            m.set(&(next() % 50), &step).await.unwrap();
        } else {
            s.append(&step).await.unwrap();
            expected.push(step);
        }
    }

    assert_eq!(s.len().await.unwrap(), expected.len() as u64);
    let mut rx = s.watch(0).await.unwrap();
    for (i, want) in expected.iter().enumerate() {
        let (idx, got) = recv(&mut rx).await.unwrap();
        assert_eq!(idx, i as u64);
        assert_eq!(&got, want);
    }
    for k in 0..50u64 {
        let _ = m.get(&k).await.unwrap();
    }
}

/// Volume over a real link: append 500 entries while a second client follows.
/// cargo test --test extensive -- --ignored cross_machine_stress
#[tokio::test]
#[ignore]
async fn cross_machine_stress() {
    let Ok(raw) = std::env::var("MYCELIUM_TEST_ADDR") else {
        eprintln!("skipped: set MYCELIUM_TEST_ADDR=host:port");
        return;
    };
    let connect = || async {
        match raw.split_once('=') {
            Some((a, name)) => {
                mycelium::Client::verified(a.parse().unwrap(), name)
                    .connect()
                    .await
            }
            None => Mycelium::join(raw.parse().unwrap()).await,
        }
    };
    let name = format!("stress-{}", std::process::id());

    let writer = connect().await.unwrap();
    let reader = connect().await.unwrap();
    let ws = writer.stream::<u64>(&name);
    let mut rx = reader.stream::<u64>(&name).watch(0).await.unwrap();

    const N: u64 = 500;
    let t0 = std::time::Instant::now();
    for i in 0..N {
        ws.append(&i).await.unwrap();
    }
    let write_time = t0.elapsed();

    for i in 0..N {
        let (idx, v) = recv(&mut rx)
            .await
            .unwrap_or_else(|| panic!("stalled at {i}"));
        assert_eq!((idx, v), (i, i));
    }
    let total = t0.elapsed();

    assert_eq!(ws.len().await.unwrap(), N);
    println!(
        "  {N} entries over the wire: {:.0} appends/s, {:.0} end-to-end/s, total {:.2}s",
        N as f64 / write_time.as_secs_f64(),
        N as f64 / total.as_secs_f64(),
        total.as_secs_f64()
    );
}

#[tokio::test]
async fn batch_append_is_dense_and_ordered() {
    let node = Mycelium::local();
    let s = node.stream::<u64>("batch");
    assert_eq!(s.append_many(&[1, 2, 3]).await.unwrap(), vec![0, 1, 2]);
    assert_eq!(s.append_many(&[4, 5]).await.unwrap(), vec![3, 4]);
    assert_eq!(s.append(&6).await.unwrap(), 5);
    assert_eq!(s.len().await.unwrap(), 6);

    let mut rx = s.watch(0).await.unwrap();
    for i in 0..6u64 {
        let (idx, v) = recv(&mut rx).await.unwrap();
        assert_eq!((idx, v), (i, i + 1));
    }

    assert!(s.append_many(&[]).await.unwrap().is_empty());
    assert_eq!(s.len().await.unwrap(), 6);
}

#[tokio::test]
async fn batch_append_over_the_wire() {
    let addr = node_over_socket().await;
    let node = Mycelium::join(addr).await.unwrap();
    let s = node.stream::<Msg>("batch-remote");
    let items: Vec<Msg> = (0..50)
        .map(|i| Msg {
            who: "a".into(),
            text: format!("m{i}"),
        })
        .collect();
    let idxs = s.append_many(&items).await.unwrap();
    assert_eq!(idxs, (0..50).collect::<Vec<u64>>());
    assert_eq!(s.len().await.unwrap(), 50);

    let mut rx = s.watch(0).await.unwrap();
    for i in 0..50 {
        let (idx, msg) = recv(&mut rx).await.unwrap();
        assert_eq!(idx, i);
        assert_eq!(msg.text, format!("m{i}"));
    }
}

#[tokio::test]
async fn batch_and_single_writers_interleave_correctly() {
    let addr = node_over_socket().await;
    let node = Mycelium::join(addr).await.unwrap();
    let s = node.stream::<u64>("mixed");
    tokio::join!(
        async {
            s.append_many(&[0, 1, 2, 3]).await.unwrap();
        },
        async {
            s.append(&99).await.unwrap();
        }
    );
    assert_eq!(s.len().await.unwrap(), 5);
    let mut rx = s.watch(0).await.unwrap();
    let mut seen = Vec::new();
    for _ in 0..5 {
        seen.push(recv(&mut rx).await.unwrap().0);
    }
    assert_eq!(seen, (0..5).collect::<Vec<u64>>());
}

// ------------------------------------------------------------ replication

/// A UDP port nobody holds right now (QUIC listens on UDP).
fn free_addr() -> SocketAddr {
    let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    s.local_addr().unwrap()
}

fn data_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("mycelium-test-{}-{tag}", std::process::id()))
}

/// A data dir with nothing left over from an earlier run.
fn fresh_dir(tag: &str) -> std::path::PathBuf {
    let d = data_dir(tag);
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// Start `n` nodes that all know each other.
async fn start_cluster(n: usize, tag: &str) -> Vec<mycelium::Running> {
    let addrs: Vec<SocketAddr> = (0..n).map(|_| free_addr()).collect();
    let dir = fresh_dir(tag);
    let mut running = Vec::new();
    for (i, addr) in addrs.iter().enumerate() {
        let peers: Vec<Peer> = addrs
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, a)| Peer::local(*a))
            .collect();
        let node = mycelium::Node::cluster(*addr, peers).with_data_dir(&dir);
        running.push(node.start().await.expect("node starts"));
    }
    // Wait for someone to win the first election, then let heartbeats reach
    // the followers so they know where to forward writes.
    for _ in 0..300 {
        if leader_index(&running).await.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    running
}

async fn leader_index(nodes: &[mycelium::Running]) -> Option<usize> {
    for (i, n) in nodes.iter().enumerate() {
        if n.is_leader().await {
            return Some(i);
        }
    }
    None
}

#[tokio::test]
async fn a_cluster_elects_one_leader() {
    let cluster = start_cluster(3, "elect").await;
    let leader = leader_index(&cluster)
        .await
        .expect("a leader must be elected");
    let node = Mycelium::join(cluster[leader].addr).await.unwrap();
    let s = node.stream::<u64>("elect");
    assert_eq!(s.append(&1).await.unwrap(), 0);
    for r in &cluster {
        r.shutdown();
    }
}

#[tokio::test]
async fn every_committed_entry_reaches_every_node() {
    let cluster = start_cluster(3, "spread").await;
    let leader = leader_index(&cluster).await.expect("leader");
    let node = Mycelium::join(cluster[leader].addr).await.unwrap();
    let s = node.stream::<u64>("spread");
    for i in 0..10u64 {
        s.append(&i).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    for (i, r) in cluster.iter().enumerate() {
        let n = Mycelium::join(r.addr).await.unwrap();
        assert_eq!(
            n.stream::<u64>("spread").len().await.unwrap(),
            10,
            "node {i} behind"
        );
    }
    for r in &cluster {
        r.shutdown();
    }
}

#[tokio::test]
async fn writes_survive_the_leader_dying() {
    let cluster = start_cluster(3, "failover").await;
    let leader = leader_index(&cluster).await.expect("leader");
    let node = Mycelium::join(cluster[leader].addr).await.unwrap();
    let s = node.stream::<u64>("failover");
    for i in 0..5u64 {
        s.append(&i).await.unwrap();
    }

    cluster[leader].shutdown();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A new leader must appear among the survivors, and it must hold the log.
    let survivors: Vec<&mycelium::Running> = cluster
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != leader)
        .map(|(_, r)| r)
        .collect();
    let mut new_leader = None;
    for _ in 0..300 {
        for (i, r) in survivors.iter().enumerate() {
            if r.is_leader().await {
                new_leader = Some(i);
                break;
            }
        }
        if new_leader.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let new_leader = new_leader.expect("survivors must elect a new leader");

    let node = Mycelium::join(survivors[new_leader].addr).await.unwrap();
    let s = node.stream::<u64>("failover");
    assert_eq!(
        s.len().await.unwrap(),
        5,
        "new leader lost committed entries"
    );
    append_when_ready(&s, &99).await;

    for r in &cluster {
        r.shutdown();
    }
}

#[tokio::test]
async fn without_a_majority_the_cluster_refuses_writes() {
    let cluster = start_cluster(3, "quorum").await;
    let leader = leader_index(&cluster).await.expect("leader");
    let node = Mycelium::join(cluster[leader].addr).await.unwrap();
    let s = node.stream::<u64>("quorum");
    s.append(&1).await.unwrap();

    for (i, r) in cluster.iter().enumerate() {
        if i != leader {
            r.shutdown();
        }
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(s.append(&2).await.is_err(), "1 of 3 must not acknowledge");
    for r in &cluster {
        r.shutdown();
    }
}

#[tokio::test]
async fn any_node_accepts_writes() {
    let cluster = start_cluster(3, "anywhere").await;
    // Pick a node that is not the leader and write through it.
    let leader = leader_index(&cluster).await.expect("leader");
    let other = (0..3).find(|i| *i != leader).unwrap();
    let node = Mycelium::join(cluster[other].addr).await.unwrap();
    let s = node.stream::<u64>("anywhere");
    assert_eq!(
        append_when_ready(&s, &7).await,
        0,
        "a follower must forward"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    for (i, r) in cluster.iter().enumerate() {
        let n = Mycelium::join(r.addr).await.unwrap();
        assert_eq!(
            n.stream::<u64>("anywhere").len().await.unwrap(),
            1,
            "node {i} missed it"
        );
    }
    for r in &cluster {
        r.shutdown();
    }
}

async fn append_when_ready<T: serde::Serialize + serde::de::DeserializeOwned + Send + 'static>(
    s: &mycelium::Stream<T>,
    v: &T,
) -> u64 {
    let mut last = String::new();
    for _ in 0..300 {
        match s.append(v).await {
            Ok(idx) => return idx,
            Err(e) => last = e.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader accepted the write within 15s: {last}");
}

#[tokio::test]
async fn a_rejoined_node_catches_up() {
    let cluster = start_cluster(3, "rejoin").await;
    let leader = leader_index(&cluster).await.expect("leader");
    let node = Mycelium::join(cluster[leader].addr).await.unwrap();
    let s = node.stream::<u64>("rejoin");
    for i in 0..5u64 {
        s.append(&i).await.unwrap();
    }

    // Take a follower out, keep writing, then bring it back with the same
    // address and peer list. Its state is RAM, so it starts empty.
    let victim = (0..3).find(|i| *i != leader).unwrap();
    let victim_addr = cluster[victim].addr;
    cluster[victim].shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;
    for i in 5..10u64 {
        append_when_ready(&s, &i).await;
    }

    let peers: Vec<Peer> = cluster
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != victim)
        .map(|(_, r)| Peer::local(r.addr))
        .collect();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut restarted = None;
    let mut last = String::new();
    for _ in 0..40 {
        match mycelium::Node::cluster(victim_addr, peers.clone())
            .with_data_dir(data_dir("rejoin"))
            .start()
            .await
        {
            Ok(r) => {
                restarted = Some(r);
                break;
            }
            Err(e) => {
                last = e.to_string();
                tokio::time::sleep(Duration::from_millis(100)).await
            }
        }
    }
    let restarted = restarted.unwrap_or_else(|| panic!("restart on the same port: {last}"));

    let back = Mycelium::join(restarted.addr).await.unwrap();
    let rs = back.stream::<u64>("rejoin");
    let mut caught_up = false;
    for _ in 0..300 {
        if rs.len().await.unwrap_or(0) == 10 {
            caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        caught_up,
        "rejoined node did not catch up: len {}",
        rs.len().await.unwrap_or(0)
    );

    let mut rx = rs.watch(0).await.unwrap();
    for i in 0..10u64 {
        let (idx, v) = recv(&mut rx).await.expect("entry after catch-up");
        assert_eq!((idx, v), (i, i), "repaired log is out of order");
    }

    restarted.shutdown();
    for r in &cluster {
        r.shutdown();
    }
}

// ------------------------------------------------------- durability, maps

/// Start node `i` of a cluster on fixed addresses, reusing its data dir.
async fn start_node(addrs: &[SocketAddr], i: usize, dir: &std::path::Path) -> mycelium::Running {
    let peers: Vec<Peer> = addrs
        .iter()
        .enumerate()
        .filter(|(j, _)| *j != i)
        .map(|(_, a)| Peer::local(*a))
        .collect();
    let mut last = String::new();
    for _ in 0..100 {
        match mycelium::Node::cluster(addrs[i], peers.clone())
            .with_data_dir(dir)
            .start()
            .await
        {
            Ok(r) => return r,
            Err(e) => last = e.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("node {i} did not start: {last}");
}

async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..200 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("not within 10 s: {what}");
}

#[tokio::test]
async fn a_whole_cluster_restart_keeps_every_acknowledged_write() {
    let addrs: Vec<SocketAddr> = (0..3).map(|_| free_addr()).collect();
    let dir = fresh_dir("restart-all");
    let mut nodes = Vec::new();
    for i in 0..3 {
        nodes.push(start_node(&addrs, i, &dir).await);
    }
    let s = nodes[0].handle().stream::<u64>("durable");
    for i in 0..20u64 {
        assert_eq!(append_when_ready(&s, &i).await, i);
    }
    s.append_many(&[20, 21, 22]).await.unwrap();
    drop(s);
    for n in &nodes {
        n.shutdown();
    }
    drop(nodes);

    let mut nodes = Vec::new();
    for i in 0..3 {
        nodes.push(start_node(&addrs, i, &dir).await);
    }
    for (i, n) in nodes.iter().enumerate() {
        let s = n.handle().stream::<u64>("durable");
        eventually(&format!("node {i} restores 23 entries"), || async {
            s.len().await.map(|l| l == 23).unwrap_or(false)
        })
        .await;
        let mut rx = s.watch(0).await.unwrap();
        for k in 0..23u64 {
            assert_eq!(recv(&mut rx).await, Some((k, k)), "node {i} entry {k}");
        }
    }
    let s = nodes[1].handle().stream::<u64>("durable");
    assert_eq!(append_when_ready(&s, &23).await, 23, "the log continues");
}

#[tokio::test]
async fn maps_converge_across_the_cluster() {
    let cluster = start_cluster(3, "maps").await;
    let handles: Vec<Mycelium> = cluster.iter().map(|r| r.handle()).collect();
    let mut tasks = Vec::new();
    for (i, h) in handles.iter().enumerate() {
        let m = h.map::<String, u64>("shared");
        tasks.push(tokio::spawn(async move {
            for j in 0..10u64 {
                m.set(&format!("n{i}-{j}"), &j).await.unwrap();
                m.set(&"contended".to_string(), &(i as u64)).await.unwrap();
            }
            m.remove(&format!("n{i}-0")).await.unwrap();
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let maps: Vec<_> = handles
        .iter()
        .map(|h| h.map::<String, u64>("shared"))
        .collect();
    eventually("all three replicas agree", || async {
        let mut views = Vec::new();
        for m in &maps {
            views.push(m.entries().await.unwrap());
        }
        views[0].len() == 28 && views.iter().all(|v| *v == views[0])
    })
    .await;
    let e = maps[2].entries().await.unwrap();
    assert!(
        e.iter().all(|(k, _)| !k.ends_with("-0")),
        "removes replicate"
    );
    assert!(maps[1]
        .get(&"contended".to_string())
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn maps_take_writes_while_partitioned_and_heal() {
    let addrs: Vec<SocketAddr> = (0..3).map(|_| free_addr()).collect();
    let dir = fresh_dir("maps-heal");
    let mut nodes = Vec::new();
    for i in 0..3 {
        nodes.push(Some(start_node(&addrs, i, &dir).await));
    }
    // Only node 0 survives: no majority, yet its map still takes writes.
    nodes[1].take().unwrap().shutdown();
    nodes[2].take().unwrap().shutdown();
    let lonely = nodes[0].as_ref().unwrap().handle();
    lonely
        .map::<String, String>("cfg")
        .set(&"mode".into(), &"solo".into())
        .await
        .unwrap();

    nodes[1] = Some(start_node(&addrs, 1, &dir).await);
    let back = nodes[1]
        .as_ref()
        .unwrap()
        .handle()
        .map::<String, String>("cfg");
    eventually("the returning node pulls the write it missed", || async {
        back.get(&"mode".into()).await.unwrap().as_deref() == Some("solo")
    })
    .await;
}

#[tokio::test]
async fn a_remote_watch_survives_its_node_restarting() {
    let addrs: Vec<SocketAddr> = (0..3).map(|_| free_addr()).collect();
    let dir = fresh_dir("watch-restart");
    let mut nodes = Vec::new();
    for i in 0..3 {
        nodes.push(Some(start_node(&addrs, i, &dir).await));
    }
    let writer = nodes[0].as_ref().unwrap().handle().stream::<u64>("w");
    append_when_ready(&writer, &0).await;

    let client = Mycelium::join(addrs[2]).await.unwrap();
    let mut rx = client.stream::<u64>("w").watch(0).await.unwrap();
    assert_eq!(recv(&mut rx).await, Some((0, 0)));

    nodes[2].take().unwrap().shutdown();
    for i in 1..5u64 {
        append_when_ready(&writer, &i).await;
    }
    nodes[2] = Some(start_node(&addrs, 2, &dir).await);
    for i in 1..5u64 {
        let got = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .ok()
            .flatten();
        assert_eq!(
            got,
            Some((i, i)),
            "the watch resumes after the restart, in order"
        );
    }
}

#[tokio::test]
async fn a_lone_restarted_node_serves_its_log() {
    let addr = free_addr();
    let dir = fresh_dir("solo-restart");
    let node = mycelium::Node::bind(addr)
        .with_data_dir(&dir)
        .start()
        .await
        .unwrap();
    let s = node.handle().stream::<String>("notes");
    s.append(&"kept".to_string()).await.unwrap();
    node.handle()
        .map::<String, u64>("m")
        .set(&"k".into(), &1)
        .await
        .unwrap();
    drop(s);
    node.shutdown();
    drop(node);

    let mut node = None;
    for _ in 0..50 {
        if let Ok(n) = mycelium::Node::bind(addr).with_data_dir(&dir).start().await {
            node = Some(n);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let node = node.expect("restart");
    let h = node.handle();
    let s = h.stream::<String>("notes");
    assert_eq!(s.len().await.unwrap(), 1);
    assert_eq!(
        recv(&mut s.watch(0).await.unwrap()).await,
        Some((0, "kept".to_string()))
    );
    assert_eq!(
        h.map::<String, u64>("m").get(&"k".into()).await.unwrap(),
        Some(1)
    );
}

#[tokio::test]
async fn misconfigured_nodes_refuse_to_start() {
    let lan: SocketAddr = "192.0.2.1:7777".parse().unwrap();
    assert!(
        mycelium::Node::bind(lan).start().await.is_err(),
        "no TLS off loopback"
    );
    let peers = vec![Peer::local(free_addr())];
    let any: SocketAddr = "0.0.0.0:7777".parse().unwrap();
    assert!(
        mycelium::Node::cluster(any, peers).start().await.is_err(),
        "cluster needs a reachable address"
    );
    let named = vec![Peer::verified(free_addr(), "x.example")];
    assert!(
        mycelium::Node::cluster(free_addr(), named)
            .start()
            .await
            .is_err(),
        "verified peers need TLS here"
    );
    assert!(
        mycelium::Client::to(lan).connect().await.is_err(),
        "no unverified client off loopback"
    );
}

// ------------------------------------------------- the features added last

async fn start_cluster_with(
    addrs: &[SocketAddr],
    dir: &std::path::Path,
    every: u64,
) -> Vec<mycelium::Running> {
    let mut nodes = Vec::new();
    for i in 0..addrs.len() {
        let peers: Vec<Peer> = addrs
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, a)| Peer::local(*a))
            .collect();
        let mut started = None;
        for _ in 0..50 {
            let node = mycelium::Node::cluster(addrs[i], peers.clone())
                .with_data_dir(dir)
                .snapshot_every(every);
            match node.start().await {
                Ok(r) => {
                    started = Some(r);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
        nodes.push(started.expect("node starts"));
    }
    nodes
}

async fn leader_of(nodes: &[mycelium::Running]) -> usize {
    for _ in 0..300 {
        if let Some(i) = leader_index(nodes).await {
            return i;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader");
}

#[tokio::test]
async fn a_client_fails_over_to_another_node() {
    let addrs: Vec<SocketAddr> = (0..3).map(|_| free_addr()).collect();
    let dir = fresh_dir("client-failover");
    let nodes = start_cluster_with(&addrs, &dir, 10_000).await;
    leader_of(&nodes).await;
    let client = mycelium::Client::any(addrs.iter().map(|a| Peer::local(*a)))
        .connect()
        .await
        .unwrap();
    let s = client.stream::<u64>("fo");
    assert_eq!(append_when_ready(&s, &0).await, 0);
    let mut rx = s.watch(0).await.unwrap();
    assert_eq!(recv(&mut rx).await, Some((0, 0)));

    // The client talks to addrs[0] first. Take that node away.
    nodes[0].shutdown();
    for i in 1..6u64 {
        assert_eq!(
            s.append(&i).await.unwrap(),
            i,
            "the client moved to a live node"
        );
    }
    for i in 1..6u64 {
        let got = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .ok()
            .flatten();
        assert_eq!(got, Some((i, i)), "the watch moved too, without gaps");
    }
    assert_eq!(s.len().await.unwrap(), 6);
}

#[tokio::test]
async fn compaction_keeps_restarts_and_laggards_whole() {
    let addrs: Vec<SocketAddr> = (0..3).map(|_| free_addr()).collect();
    let dir = fresh_dir("compact");
    let mut nodes: Vec<Option<mycelium::Running>> = start_cluster_with(&addrs, &dir, 20)
        .await
        .into_iter()
        .map(Some)
        .collect();
    let l = {
        let live: Vec<mycelium::Running> = Vec::new();
        drop(live);
        let mut l = None;
        for _ in 0..300 {
            for (i, n) in nodes.iter().enumerate() {
                if n.as_ref().unwrap().is_leader().await {
                    l = Some(i);
                }
            }
            if l.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        l.unwrap()
    };
    let lagger = (0..3).find(|i| *i != l).unwrap();
    nodes[lagger].take().unwrap().shutdown();

    let s = nodes[l].as_ref().unwrap().handle().stream::<u64>("c");
    for chunk in 0..10u64 {
        let batch: Vec<u64> = (chunk * 10..chunk * 10 + 10).collect();
        append_many_when_ready(&s, &batch).await;
        for i in 0..5u64 {
            s.append(&(1000 + chunk * 5 + i)).await.unwrap();
        }
    }
    let log = |i: usize| {
        std::fs::metadata(
            dir.join(addrs[i].to_string().replace([':', '.'], "_"))
                .join("raft/log"),
        )
        .unwrap()
        .len()
    };
    let snap = |i: usize| {
        dir.join(addrs[i].to_string().replace([':', '.'], "_"))
            .join("raft/snapshot")
    };
    eventually("the leader wrote a snapshot", || async { snap(l).exists() }).await;
    assert!(
        log(l) < 4096,
        "the leader's log was compacted to {} bytes",
        log(l)
    );

    // The lagging node is now behind the leader's snapshot: it gets one.
    nodes[lagger] = Some(start_node(&addrs, lagger, &dir).await);
    let back = nodes[lagger].as_ref().unwrap().handle().stream::<u64>("c");
    eventually("the lagging node is restored by snapshot", || async {
        back.len().await.map(|n| n == 150).unwrap_or(false)
    })
    .await;
    eventually("and keeps that snapshot on disk", || async {
        snap(lagger).exists()
    })
    .await;

    // Everyone restarts from snapshot + tail.
    for n in nodes.iter_mut() {
        n.take().unwrap().shutdown();
    }
    let mut nodes = Vec::new();
    for i in 0..3 {
        nodes.push(start_node(&addrs, i, &dir).await);
    }
    let s = nodes[0].handle().stream::<u64>("c");
    eventually("restarted from snapshot plus tail", || async {
        s.len().await.map(|n| n == 150).unwrap_or(false)
    })
    .await;
    let mut rx = s.watch(0).await.unwrap();
    let mut got = Vec::new();
    while got.len() < 150 {
        got.push(recv(&mut rx).await.expect("entry").1);
    }
    let mut want = Vec::new();
    for chunk in 0..10u64 {
        want.extend(chunk * 10..chunk * 10 + 10);
        want.extend((0..5).map(|i| 1000 + chunk * 5 + i));
    }
    assert_eq!(got, want, "order and content survive compaction");
}

async fn append_many_when_ready(s: &mycelium::Stream<u64>, v: &[u64]) -> Vec<u64> {
    let mut last = String::new();
    for _ in 0..300 {
        match s.append_many(v).await {
            Ok(i) => return i,
            Err(e) => last = e.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("batch not accepted: {last}");
}

#[tokio::test]
async fn nodes_join_and_leave_a_running_cluster() {
    let addrs: Vec<SocketAddr> = (0..4).map(|_| free_addr()).collect();
    let dir = fresh_dir("membership");
    let nodes = start_cluster_with(&addrs[..3], &dir, 10_000).await;
    let l = leader_of(&nodes).await;
    let s = nodes[l].handle().stream::<u64>("m");
    for i in 0..10u64 {
        append_when_ready(&s, &i).await;
    }

    let joiner = mycelium::Node::join(addrs[3], vec![Peer::local(addrs[0]), Peer::local(addrs[1])])
        .with_data_dir(&dir)
        .start()
        .await
        .unwrap();
    eventually("the new node becomes a voter", || async {
        joiner
            .members()
            .iter()
            .any(|m| m.addr == addrs[3] && m.voter)
    })
    .await;
    assert_eq!(nodes[l].members().iter().filter(|m| m.voter).count(), 4);
    let j = joiner.handle().stream::<u64>("m");
    assert_eq!(j.len().await.unwrap(), 10, "it has the whole log");

    // Remove an original follower; the other three keep committing.
    let gone = (0..3).find(|i| *i != l).unwrap();
    nodes[l].remove(addrs[gone]).await.unwrap();
    nodes[gone].shutdown();
    assert_eq!(nodes[l].members().len(), 3);
    assert_eq!(
        append_when_ready(&j, &10).await,
        10,
        "writes go through the joined node"
    );
    let client = Mycelium::join(addrs[3]).await.unwrap();
    let ms: Vec<SocketAddr> = client
        .members()
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.addr)
        .collect();
    assert!(
        !ms.contains(&addrs[gone]) && ms.contains(&addrs[3]),
        "members over the wire: {ms:?}"
    );

    // Membership is in the log: it survives a restart without the old peer list.
    joiner.shutdown();
    drop(joiner);
    let mut again = None;
    for _ in 0..100 {
        if let Ok(r) = mycelium::Node::join(addrs[3], vec![Peer::local(addrs[l])])
            .with_data_dir(&dir)
            .start()
            .await
        {
            again = Some(r);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let again = again.unwrap();
    eventually("the restarted joiner still knows the cluster", || async {
        again.members().iter().filter(|m| m.voter).count() == 3
    })
    .await;
}

#[tokio::test]
async fn a_graceful_stop_hands_over_leadership_quickly() {
    let addrs: Vec<SocketAddr> = (0..3).map(|_| free_addr()).collect();
    let dir = fresh_dir("transfer");
    let nodes = start_cluster_with(&addrs, &dir, 10_000).await;
    let l = leader_of(&nodes).await;
    let other = (0..3).find(|i| *i != l).unwrap();
    let s = nodes[other].handle().stream::<u64>("t");
    append_when_ready(&s, &0).await;

    let started = std::time::Instant::now();
    nodes[l].stop().await;
    let handed = started.elapsed();
    let t = std::time::Instant::now();
    s.append(&1).await.unwrap();
    let pause = t.elapsed();
    assert!(
        handed + pause < Duration::from_millis(900),
        "handover {handed:?} + first write {pause:?} must beat the 1 s election timeout"
    );
    let survivors: Vec<&mycelium::Running> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != l)
        .map(|(_, n)| n)
        .collect();
    let mut leaders = 0;
    for n in survivors {
        leaders += n.is_leader().await as usize;
    }
    assert_eq!(leaders, 1);
}

#[tokio::test]
async fn ephemeral_streams_are_fast_and_forgotten() {
    let addr = free_addr();
    let dir = fresh_dir("ephemeral");
    let node = mycelium::Node::bind(addr)
        .with_data_dir(&dir)
        .start()
        .await
        .unwrap();
    let client = Mycelium::join(addr).await.unwrap();
    let pings = client.stream::<String>("presence").ephemeral();
    let mut rx = pings.watch(0).await.unwrap();
    assert_eq!(pings.append(&"a".into()).await.unwrap(), 0);
    assert_eq!(
        pings.append_many(&["b".into(), "c".into()]).await.unwrap(),
        vec![1, 2]
    );
    assert_eq!(pings.len().await.unwrap(), 3);
    for (i, v) in ["a", "b", "c"].iter().enumerate() {
        assert_eq!(recv(&mut rx).await, Some((i as u64, v.to_string())));
    }
    let durable = client.stream::<String>("presence");
    assert_eq!(
        durable.len().await.unwrap(),
        0,
        "ephemeral and durable streams are separate"
    );
    assert!(
        !std::fs::read_dir(&dir).unwrap().flatten().any(|e| {
            std::fs::read(e.path().join("raft/log"))
                .is_ok_and(|b| b.windows(8).any(|w| w == b"presence"))
        }),
        "nothing ephemeral reaches the disk"
    );

    node.shutdown();
    drop(node);
    let mut node = None;
    for _ in 0..100 {
        if let Ok(n) = mycelium::Node::bind(addr).with_data_dir(&dir).start().await {
            node = Some(n);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let node = node.unwrap();
    assert_eq!(
        node.handle()
            .stream::<String>("presence")
            .ephemeral()
            .len()
            .await
            .unwrap(),
        0,
        "gone after a restart"
    );
}

#[tokio::test]
async fn retention_trims_by_count_age_and_on_request() {
    let node = Mycelium::local();
    let s = node.stream::<u64>("last3").retain(mycelium::Retention {
        keep_last: Some(3),
        max_age: None,
    });
    for i in 0..10u64 {
        s.append(&i).await.unwrap();
    }
    assert_eq!(s.len().await.unwrap(), 10, "len is the next index");
    assert_eq!(s.first().await.unwrap(), 7);
    let mut rx = s.watch(0).await.unwrap();
    assert_eq!(
        recv(&mut rx).await,
        Some((7, 7)),
        "watch starts at the first entry left"
    );

    let manual = node.stream::<u64>("manual");
    for i in 0..5u64 {
        manual.append(&i).await.unwrap();
    }
    manual.trim(3).await.unwrap();
    assert_eq!(manual.first().await.unwrap(), 3);

    let aged = node.stream::<u64>("aged").retain(mycelium::Retention {
        keep_last: None,
        max_age: Some(Duration::from_millis(300)),
    });
    aged.append(&1).await.unwrap();
    aged.append(&2).await.unwrap();
    for _ in 0..60 {
        if aged.first().await.unwrap() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        aged.first().await.unwrap(),
        2,
        "entries past max_age expire"
    );
    aged.append(&3).await.unwrap();
    assert_eq!(aged.len().await.unwrap(), 3);
}

#[tokio::test]
async fn map_watch_sees_local_and_remote_changes() {
    let cluster = start_cluster(3, "map-watch").await;
    let a = cluster[0].handle().map::<String, u64>("w");
    let b = Mycelium::join(cluster[2].addr)
        .await
        .unwrap()
        .map::<String, u64>("w");
    let mut rx = b.watch().await.unwrap();
    a.set(&"x".into(), &1).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap();
    assert_eq!(
        got,
        Some(("x".to_string(), Some(1))),
        "a remote set reaches a watcher"
    );
    b.set(&"y".into(), &2).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap();
    assert_eq!(got, Some(("y".to_string(), Some(2))), "so does a local one");
    a.remove(&"x".into()).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap();
    assert_eq!(got, Some(("x".to_string(), None)), "removes arrive as None");
}

#[tokio::test]
async fn tailnet_discovery_reads_tailscale_status() {
    match mycelium::tailnet_peers(7777) {
        Ok(peers) => {
            for p in &peers {
                assert_eq!(p.addr.port(), 7777);
                assert!(
                    p.name.as_deref().is_some_and(|n| n.contains('.')),
                    "a full tailnet name: {p:?}"
                );
            }
        }
        Err(e) => eprintln!("skipped, no tailscale here: {e}"),
    }
}

// ---------------------------------------------------------------- type tags

mod v1 {
    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    pub struct Reading {
        pub celsius: f64,
    }
}

mod v2 {
    pub mod sensors {
        /// Same name, moved to another module: the tag is still "Reading".
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        pub struct Reading {
            pub celsius: f64,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Renamed {
    celsius: f64,
}

#[tokio::test]
async fn a_type_moved_to_another_module_still_reads() {
    let node = Mycelium::local();
    let old = node.stream::<v1::Reading>("temps");
    old.append(&v1::Reading { celsius: 21.5 }).await.unwrap();
    let new = node.stream::<v2::sensors::Reading>("temps");
    let mut rx = new.watch(0).await.unwrap();
    assert_eq!(
        recv(&mut rx).await,
        Some((0, v2::sensors::Reading { celsius: 21.5 }))
    );
    assert_eq!(new.skipped(), 0);

    let m_old = node.map::<String, v1::Reading>("last");
    m_old
        .set(&"lab".into(), &v1::Reading { celsius: 3.0 })
        .await
        .unwrap();
    let m_new = node.map::<String, v2::sensors::Reading>("last");
    assert_eq!(
        m_new.get(&"lab".into()).await.unwrap(),
        Some(v2::sensors::Reading { celsius: 3.0 })
    );
}

#[tokio::test]
async fn type_tag_names_a_renamed_type_and_separates_namesakes() {
    let node = Mycelium::local();
    let old = node.stream::<v1::Reading>("renamed");
    old.append(&v1::Reading { celsius: 7.0 }).await.unwrap();
    let skips = node.stream::<Renamed>("renamed");
    let mut rx = skips.watch(0).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), rx.recv())
            .await
            .is_err(),
        "a different name is a different type"
    );
    let tagged = node.stream::<Renamed>("renamed").type_tag("Reading");
    let mut rx = tagged.watch(0).await.unwrap();
    assert_eq!(
        recv(&mut rx).await,
        Some((0, Renamed { celsius: 7.0 })),
        "the old tag reads it"
    );

    // Two types with one name stay apart once one of them is tagged.
    let a = node.stream::<v1::Reading>("mixed");
    let b = node
        .stream::<v2::sensors::Reading>("mixed")
        .type_tag("SensorReading");
    a.append(&v1::Reading { celsius: 1.0 }).await.unwrap();
    b.append(&v2::sensors::Reading { celsius: 2.0 })
        .await
        .unwrap();
    let mut ra = a.watch(0).await.unwrap();
    assert_eq!(recv(&mut ra).await, Some((0, v1::Reading { celsius: 1.0 })));
    assert!(tokio::time::timeout(Duration::from_millis(300), ra.recv())
        .await
        .is_err());
    assert_eq!(a.skipped(), 1, "the tagged entry was skipped, not misread");

    let m = node
        .map::<String, Renamed>("tagged-map")
        .type_tag("Reading");
    node.map::<String, v1::Reading>("tagged-map")
        .set(&"x".into(), &v1::Reading { celsius: 9.0 })
        .await
        .unwrap();
    assert_eq!(
        m.get(&"x".into()).await.unwrap(),
        Some(Renamed { celsius: 9.0 })
    );
}
