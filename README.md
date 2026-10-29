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
cargo run --example hello cluster <addr> <peer>...      # one node of a new cluster
cargo run --example hello add     <addr> <via>...       # join a running cluster
cargo run --example hello tailnet <port>                # every tailnet machine, discovered
cargo run --example hello join    <addr>=<name>...      # attach, failing over between nodes
```

## The two primitives

| | `Stream<T>` | `Map<K, V>` |
| --- | --- | --- |
| model | one totally ordered log per name | last-writer-wins map per name (loro) |
| durability | durable by default; `.ephemeral()` keeps it in one node's memory | durable on every node |
| algorithm | Raft with PreVote, CheckQuorum, ReadIndex, client sessions | CRDT, push on write plus anti-entropy pull every second |
| write accepted | once a majority holds it on disk | once it is on the local disk |
| during a partition | majority side only; minority writes fail, never fork | every node, converges after healing |
| reads | `len`, `watch`: linearizable / committed entries only | this replica's view |
| API | `append`, `append_many` (atomic, one round trip), `len`, `first`, `watch(from)`, `trim`, `*_raw` | `set`, `remove`, `get`, `entries`, `watch` |
| bounded by | `.retain(Retention { keep_last, max_age })`, `trim(before)` | |

Types are a local lens, not a wire contract: every value carries an 8-byte type
fingerprint, and entries that do not decode as `T` are skipped and counted
(`skipped()`), never fatal. `append_raw` / `watch_raw` see every entry as bytes.

The fingerprint is a hash of the type's name without module paths, e.g.
`Option<(u64,String)>`, so moving a type to another module or crate, or
building with another Rust version, keeps its streams readable. For a renamed
type, or two types that share a name, set it yourself:
`node.stream::<Reading>("temps").type_tag("Reading")` (same for maps).

Retries are safe: each handle tags its appends with `(client, seq)`, and a
retried append lands once and reports its original index (thesis 6.3).

`Client::any([...])` connects to any of several nodes and moves to the next
one when a node fails, for requests and for running `watch`es alike: a watch
resumes at the next index on the new node, without gaps or repeats.

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
- **Membership changes at runtime**, one server at a time (thesis 4.1).
  `Node::join(addr, via)` adds a node: it catches up as a non-voting learner,
  then becomes a voter, so a slow joiner never stalls commits.
  `Running::remove(addr)` takes one out; a removed leader hands over. The
  peer list given to `Node::cluster` is used once, on first start; after that
  membership lives in the replicated log.
- **Discovery.** `tailnet_peers(port)` and `tailnet_self(port)` read
  `tailscale status` and return verified peers, ready for `Node::cluster`,
  `Node::join` or `Client::any`.
- **Client allowlist.** `Node::allow([...names])` admits only clients whose
  verified certificate carries one of those names. Members always pass.
- **Any node serves clients.** Followers forward writes to the leader and ask it
  for a read index, so a client never needs to know who leads.
- **Durability.** The Raft log is a checksummed append-only file, fsynced with
  group commit before any acknowledgement; a torn tail is cut on restart.
  `(term, vote)` is written atomically (temp file, fsync, rename, fsync dir).
  Maps are snapshotted the same way. A disk error stops the node from voting or
  acknowledging anything, rather than lying.
- **Rejoining.** A node that was down, or partitioned, is repaired by the leader
  from where its log diverges; maps catch up on the next pull.
- **Compaction.** Every 10 000 entries (`Node::snapshot_every`) a node
  snapshots its state and cuts the log behind it. A follower too far behind
  gets the snapshot in 1 MiB chunks, then the tail. Restarts load the
  snapshot and replay only the tail.
- **Graceful stop.** `Running::stop()` (and `Node::serve` on Ctrl-C or
  SIGTERM) brings the most caught-up voter up to date and tells it to
  campaign at once (thesis 3.10), so writes pause for a round trip, not an
  election timeout. `shutdown()` stops at once.

## What the tests prove

CI (`.github/workflows/ci.yml`) runs rustfmt, clippy with warnings as
errors, the full suite and 500 chaos seeds on every push.

```text
cargo test                     18 unit + 49 integration tests, ~20 s
CHAOS_SEEDS=500 cargo test --release --lib chaos
MYCELIUM_TEST_ADDR=<ip>:<port>=<cert-name> cargo test --release -- --ignored   # 2 cross-machine
```

- **Raft safety, by simulation** (`src/raft/tests.rs`). Whole clusters of 3 and
  5 nodes under 10% message loss, random delay and reordering, partitions,
  crash-restarts and nodes going down. After every simulated millisecond it
  checks: at most one leader per term, no node votes twice in a term, log
  matching, and that no committed entry ever changes. Then it heals the network
  and requires every state machine to agree. 500 seeds pass.
  The chaos run also compacts every 7 entries and flips a node between voter
  and learner, so snapshot transfer and membership changes run under faults.
- **Targeted scenarios:** a partitioned leader steps down and its writes fail
  instead of hanging; a node that rejoins after a partition does not depose the
  leader or inflate the term (PreVote); retries apply exactly once; a node far
  behind is restored by snapshot and survives a restart from it; membership
  changes refuse two servers at once, keep learners out of the quorum, and a
  removed leader hands over; a transfer elects the target in under 200 ms.
- **Real clusters over QUIC** (`tests/extensive.rs`): election, every entry on
  every node, failover with the log intact, writes refused without a majority,
  writes through any node, a rejoined node catching up, a whole-cluster restart
  keeping every acknowledged write, maps converging across nodes, maps taking
  writes while partitioned and healing, a remote watch surviving its node's
  restart, misconfigurations refused, a client and its watch failing over to
  another node, compaction with snapshot catch-up and restart, nodes joining
  and leaving a running cluster, a graceful stop beating the election
  timeout, ephemeral streams that never touch disk, retention by count, age
  and trim, map watches seeing local and remote changes, tailnet discovery.
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

Under faults, across both machines (`examples/soak.rs`): two nodes on the
workstation, the laptop joining over TLS with `hello add`. A `Client::any`
writes every 20 ms for 60 s while the leader gets SIGTERM (it hands
leadership to the laptop), is restarted, and the laptop node is killed with
`kill -9` and restarted. Result: 1070 writes acknowledged, 0 failed, slowest
1.8 s (during the `kill -9`); every acknowledged write was in the stream at
its index, nothing twice, and a watch through another node matched entry for
entry. The three logs on the two machines ended byte-identical. The
workstation was compiling other projects at load average 50 throughout.

Tail latency over that WiFi link is the link, not the library. A raw UDP echo
on the same path has a p99 of 47 to 54 ms with spikes to 99 ms; requests from
the laptop see p90 of 50 to 100 ms on some runs and not others. QUIC
statistics on those slow requests show zero lost packets and a smoothed RTT
that climbs from 4 to 60 ms, which is queueing on the link.

`scripts/net-scenarios.sh <host> <ip> [entries] [cert-name]` shapes both ends
of `tailscale0` with netem (lossy LAN, LTE, DSL, 3G, satellite, dial-up) and
measures serial appends against one batch.

## Debugging

`running.status()` (in process) or `node.status().await` (over the network,
any node) reports role, term, leader, log, commit and snapshot indices, and on
the leader how far behind each member is and when it last answered:

```text
$ cargo run --example hello status 100.79.55.103:7802=fox-workstation.ts.net
100.79.55.103:7802 Leader, term 4, leader 100.79.55.103:7802
log 1412 (commit 1412, applied 1412, snapshot 0)
  100.79.55.103:7801 voter (fox-workstation.ts.net), behind 0, heard 38ms ago
  100.79.55.103:7802 voter (fox-workstation.ts.net), behind 0
  100.122.25.53:7803 voter (fox-laptop.ts.net), behind 0, heard 41ms ago
```

`MYCELIUM_TRACE=1` prints elections, leadership changes and disk failures, and
on the client side any request slower than 40 ms, with the connection's
lost-packet count and RTT.

## Limits

- **Single-server membership changes only**, as the thesis recommends;
  replacing several nodes means several steps.
- **Snapshots are held in memory** to send to followers, and the state machine
  is in memory too: fine for homelab volumes, not for a terabyte stream.
- **Ephemeral streams** live on the node you are connected to: a client that
  fails over to another node sees that node's ephemeral streams, not the old
  one's.
- **Map watches coalesce**: a watcher sees the latest value per key, not every
  intermediate one.

## Layout

```text
src/raft.rs        Raft as pure state: no sockets, no tasks, time passed in;
                   membership, snapshots, transfer, retention live here too
src/raft/tests.rs  deterministic whole-cluster simulation with safety checks
src/node.rs        the runtime: elections, replication, snapshots, membership,
                   ephemeral streams, map watch, gossip, handlers
src/wal.rs         checksummed log, atomic (term, vote), snapshot files
src/maps.rs        loro maps, push and pull, snapshots
src/transport.rs   QUIC, certificates, peer identity, framing
src/lib.rs         the public API
examples/01-12     the prototypes that led here, each asserting its own claim
```
