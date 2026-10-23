# mycelium

Ordered streams and merged maps, shared by every machine on a tailnet. Two
primitives, typed by default:

```rust
let node = Mycelium::local();                        // or Client::verified(addr, name).connect()
let chat = node.stream::<Msg>("chat");               // ordered log: Raft, linearizable
chat.append(&msg).await?;                            // returns once a majority has it on disk
let presence = node.map::<String, Status>("online"); // merged state: CRDT, always writable
presence.set(&"laptop".into(), &Status::Up).await?;
```

```text
cargo run --example hello                               # one process, no sockets
cargo run --example hello serve   [bind]                # standalone node
cargo run --example hello cluster <addr> <peer>...      # one node of a cluster
cargo run --example hello join    <addr>=<cert-name>    # attach over the tailnet
```

## The two primitives

| | `Stream<T>` | `Map<K, V>` |
| --- | --- | --- |
| model | one totally ordered log per name | last-writer-wins map per name (loro) |
| algorithm | Raft with PreVote, CheckQuorum, ReadIndex, client sessions | CRDT, push on write plus anti-entropy pull every second |
| write accepted | once a majority holds it on disk | once it is on the local disk |
| during a partition | majority side only; minority writes fail, never fork | every node, converges after healing |
| reads | `len`, `watch`: linearizable / committed entries only | this replica's view |
| API | `append`, `append_many` (atomic, one round trip), `len`, `watch(from)`, `*_raw` | `set`, `remove`, `get`, `entries` |

Types are a local lens, not a wire contract: every value carries an 8-byte type
fingerprint, and entries that do not decode as `T` are skipped and counted
(`skipped()`), never fatal. `append_raw` / `watch_raw` see every entry as bytes.

Retries are safe: each handle tags its appends with `(client, seq)`, and a
retried append lands once and reports its original index (thesis 6.3).

## A cluster

Every node lists every other node. With certificates:

```text
tailscale cert --cert-file node.crt --key-file node.key <host>.<tailnet>.ts.net
```

```rust
Node::cluster(my_addr, vec![
    Peer::verified(laptop, "fox-laptop.tailnet.ts.net"),
    Peer::verified(vps, "vps.tailnet.ts.net"),
])
.with_tls("node.crt", "node.key")
.with_data_dir("/var/lib/mycelium")   // default: ~/.local/share/mycelium, never a temp dir
.serve()
.await?;
```

- **Identity.** A node serves its certificate and presents it when it calls a
  peer. Votes and replication are accepted only from a caller whose verified
  certificate names a listed peer, and the claimed sender must match it.
  Clients need no certificate; they can read and write, not vote.
- **Unverified mode** (`Peer::local`, `Client::to`) is refused off loopback, by
  construction, for both nodes and clients.
- **Any node serves clients.** Followers forward writes to the leader and ask it
  for a read index, so a client never needs to know who leads.
- **Durability.** The Raft log is a checksummed append-only file, fsynced with
  group commit before any acknowledgement; a torn tail is cut on restart.
  `(term, vote)` is written atomically (temp file, fsync, rename, fsync dir).
  Maps are snapshotted the same way. A disk error stops the node from voting or
  acknowledging anything, rather than lying.
- **Rejoining.** A node that was down, or partitioned, is repaired by the leader
  from where its log diverges; maps catch up on the next pull.
- **Shutdown** releases the port and the files, so a node restarts in place.

## What the tests prove

```text
cargo test                     12 unit + 39 integration tests, ~15 s
CHAOS_SEEDS=500 cargo test --release --lib chaos
MYCELIUM_TEST_ADDR=<ip>:<port>=<cert-name> cargo test --release -- --ignored   # 2 cross-machine
```

- **Raft safety, by simulation** (`src/raft/tests.rs`). Whole clusters of 3 and
  5 nodes under 10% message loss, random delay and reordering, partitions,
  crash-restarts and nodes going down. After every simulated millisecond it
  checks: at most one leader per term, no node votes twice in a term, log
  matching, and that no committed entry ever changes. Then it heals the network
  and requires every state machine to agree. 500 seeds pass.
- **Targeted scenarios:** a partitioned leader steps down and its writes fail
  instead of hanging; a node that rejoins after a partition does not depose the
  leader or inflate the term (PreVote); retries apply exactly once.
- **Real clusters over QUIC** (`tests/extensive.rs`): election, every entry on
  every node, failover with the log intact, writes refused without a majority,
  writes through any node, a rejoined node catching up, a whole-cluster restart
  keeping every acknowledged write, maps converging across nodes, maps taking
  writes while partitioned and healing, a remote watch surviving its node's
  restart, and misconfigurations refused.
- **Disk:** WAL reopen, truncation, a torn tail; CRC-32 against the standard check value.

## Real numbers

In-process cluster on one machine (`cargo run --release --example bench`), XFS
on NVMe, fsync about 1.1 ms:

```text
                         1 node        3 nodes
serial append p50        1.24 ms       3.22 ms
32 concurrent writers    11 500/s      5 400/s
batches of 1000          636 000/s     314 000/s
```

Across machines: fox-workstation and fox-laptop on the tailnet (WiFi, about
3.4 ms ping), a 3-node cluster with verified certificates on both sides
(`cargo run --release --example probe`):

```text
append p50, client next to the leader                2.0 ms
append p50, client on the laptop, leader across      4.6 - 7.3 ms
linearizable len p50, through a follower              2.4 - 3.3 ms
```

Failover across machines: killing the leader moved leadership to the laptop
(term 2); writes continued through the laptop node, and the old leader rejoined
and caught up. All three log files ended byte-identical (same SHA-256).

Tail latency over that WiFi link is the link, not the library. A raw UDP echo
on the same path has a p99 of 47 to 54 ms with spikes to 99 ms; requests from
the laptop see p90 of 50 to 100 ms on some runs and not others. QUIC
statistics on those slow requests show zero lost packets and a smoothed RTT
that climbs from 4 to 60 ms, which is queueing on the link.

`scripts/net-scenarios.sh <host> <ip> [entries] [cert-name]` shapes both ends
of `tailscale0` with netem (lossy LAN, LTE, DSL, 3G, satellite, dial-up) and
measures serial appends against one batch.

## Debugging

`MYCELIUM_TRACE=1` prints elections, leadership changes and disk failures, and
on the client side any request slower than 40 ms, with the connection's
lost-packet count and RTT.

## Not built yet

- **Snapshots and log compaction.** The Raft log and the state machine grow
  without bound. On restart every node replays its whole log. Fine for
  homelab volumes, not for years of high-rate streams.
- **Membership changes.** The peer list is fixed at start. Adding a node means
  restarting all of them with the new list.
- **Lagging watchers on a remote node** are served from memory, so very long
  streams cost RAM on every node.
- **Ephemeral (memory-only) streams** in a cluster. `Mycelium::local()` is
  memory-only; cluster streams are always durable.

## Layout

```text
src/raft.rs        Raft as pure state: no sockets, no tasks, time passed in
src/raft/tests.rs  deterministic whole-cluster simulation with safety checks
src/node.rs        the runtime: elections, replication, disk, gossip, handlers
src/wal.rs         checksummed log and atomic (term, vote)
src/maps.rs        loro maps, push and pull, snapshots
src/transport.rs   QUIC, certificates, peer identity, framing
src/lib.rs         the public API
examples/01-12     the prototypes that led here, each asserting its own claim
```
