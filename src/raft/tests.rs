//! Deterministic simulation of whole clusters: random message loss, delay,
//! reordering, partitions, crash-restarts, compaction with snapshot transfer,
//! and membership changes, with Raft's safety properties checked after every
//! step (Figure 3 of the paper).

use super::*;
use std::collections::{BTreeMap, HashMap};

#[derive(Clone)]
enum Msg {
    Vote(VoteReq),
    VoteR(VoteResp, VoteReq),
    App(AppendReq, Instant),
    AppR(AppendReq, AppendResp, Instant),
    Snap(u64, SocketAddr, Arc<Snapshot>, Instant),
    SnapR(u64, u64, AppendResp, Instant),
}

struct Net {
    nodes: Vec<Core>,
    addrs: Vec<SocketAddr>,
    /// (deliver_at_ms, seq) -> (from, to, msg)
    queue: BTreeMap<(u64, u64), (usize, usize, Msg)>,
    seq: u64,
    now_ms: u64,
    epoch: Instant,
    rng: u64,
    cut: Vec<bool>,
    down: Vec<bool>,
    loss: u64,
    votes: Vec<HashMap<u64, usize>>,
    leaders: HashMap<u64, usize>,
    committed: BTreeMap<u64, Entry>,
    proposed: u64,
    /// (candidate, term, pre) -> voters who granted.
    tally: HashMap<(usize, u64, bool), Vec<usize>>,
    log: Vec<String>,
}

fn voter(a: SocketAddr) -> Member {
    Member {
        addr: a,
        name: None,
        voter: true,
    }
}

impl Net {
    fn new(n: usize, seed: u64) -> Self {
        Self::with(n, n, seed)
    }

    /// `n` nodes, of which the first `members` form the initial cluster;
    /// the rest start empty, as joining nodes do.
    fn with(n: usize, members: usize, seed: u64) -> Self {
        let epoch = Instant::now();
        let addrs: Vec<SocketAddr> = (0..n)
            .map(|i| format!("10.0.0.{}:1", i + 1).parse().unwrap())
            .collect();
        let initial: Vec<Member> = addrs[..members].iter().copied().map(voter).collect();
        let nodes = (0..n)
            .map(|i| {
                let boot = if i < members {
                    initial.clone()
                } else {
                    Vec::new()
                };
                let mut c = Core::new(addrs[i], boot, None, Loaded::default(), epoch).unwrap();
                c.rng = seed.wrapping_mul(31).wrapping_add(i as u64 * 7919) | 1;
                c.reset_deadline(epoch);
                c
            })
            .collect();
        Net {
            nodes,
            addrs,
            queue: BTreeMap::new(),
            seq: 0,
            now_ms: 0,
            epoch,
            rng: seed | 1,
            cut: vec![false; n],
            down: vec![false; n],
            loss: 0,
            votes: vec![HashMap::new(); n],
            leaders: HashMap::new(),
            committed: BTreeMap::new(),
            proposed: 0,
            tally: HashMap::new(),
            log: Vec::new(),
        }
    }

    fn rand(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
    }

    fn now(&self) -> Instant {
        self.epoch + Duration::from_millis(self.now_ms)
    }

    fn idx(&self, a: SocketAddr) -> usize {
        self.addrs.iter().position(|x| *x == a).unwrap()
    }

    fn send(&mut self, from: usize, to: usize, m: Msg) {
        if self.cut[from] != self.cut[to] || self.down[from] || self.down[to] {
            return;
        }
        if self.loss > 0 && self.rand() % 100 < self.loss {
            return;
        }
        let delay = 1 + self.rand() % 30;
        self.seq += 1;
        self.queue
            .insert((self.now_ms + delay, self.seq), (from, to, m));
    }

    fn ask_votes(&mut self, i: usize, req: VoteReq) {
        let voters: Vec<usize> = self.nodes[i]
            .other_voters()
            .into_iter()
            .map(|a| self.idx(a))
            .collect();
        for j in voters {
            self.send(i, j, Msg::Vote(req));
        }
    }

    fn tick(&mut self, i: usize) {
        if self.down[i] {
            return;
        }
        let now = self.now();
        let c = &mut self.nodes[i];
        // Disk is instant in the simulation; snapshots compact in place.
        c.durable = c.last_index();
        c.state_synced = c.state_seq;
        let _ = c.snapshot_job();
        if c.is_leader() {
            c.advance_commit();
            c.check_quorum(now);
        } else if now >= c.deadline {
            c.reset_deadline(now);
            if c.is_voter(c.me) && c.io_error.is_none() {
                let req = c.vote_req(true, false);
                self.ask_votes(i, req);
            }
        }
        let c = &self.nodes[i];
        if c.is_leader() && self.now_ms.is_multiple_of(50) {
            let out: Vec<(SocketAddr, Outbound)> = c
                .others()
                .into_iter()
                .filter_map(|p| c.outbound(p).map(|o| (p, o)))
                .collect();
            let (term, snap) = (c.term, c.snapshot.clone());
            for (p, o) in out {
                let j = self.idx(p);
                match o {
                    Outbound::Append(r) => self.send(i, j, Msg::App(r, now)),
                    Outbound::Snapshot => {
                        if let Some(s) = &snap {
                            self.send(i, j, Msg::Snap(term, self.addrs[i], s.clone(), now));
                        }
                    }
                }
            }
        }
        self.nodes[i].apply();
    }

    fn deliver(&mut self, from: usize, to: usize, m: Msg) {
        if self.down[to] {
            return;
        }
        let now = self.now();
        match m {
            Msg::Vote(req) => {
                let r = self.nodes[to].handle_vote(req, now);
                if r.granted && !req.pre {
                    let prev = self.votes[to].insert(req.term, from);
                    assert!(
                        prev.is_none() || prev == Some(from),
                        "node {to} voted twice in term {}",
                        req.term
                    );
                }
                self.send(to, from, Msg::VoteR(r, req));
            }
            Msg::VoteR(r, req) => {
                let c = &mut self.nodes[to];
                if r.term > c.term && !r.granted {
                    c.become_follower(r.term, None);
                    return;
                }
                self.on_vote(to, from, r, req);
            }
            Msg::App(req, sent_at) => {
                let r = self.nodes[to].handle_append(req.clone(), now);
                self.send(to, from, Msg::AppR(req, r, sent_at));
            }
            Msg::AppR(req, r, sent_at) => {
                let peer = self.addrs[from];
                self.nodes[to].on_append_resp(peer, &req, r, sent_at, now);
            }
            Msg::Snap(term, leader, snap, sent_at) => {
                let c = &mut self.nodes[to];
                let resp = match c.handle_snapshot_chunk(term, leader, now) {
                    Err(t) => AppendResp {
                        term: t,
                        ok: false,
                        index: 0,
                    },
                    Ok(()) => {
                        let m = Machine::decode(&snap.data).expect("snapshot decodes");
                        c.install_snapshot(snap.clone(), m)
                    }
                };
                self.send(to, from, Msg::SnapR(term, snap.index, resp, sent_at));
            }
            Msg::SnapR(term, index, resp, sent_at) => {
                let peer = self.addrs[from];
                self.nodes[to].on_snapshot_resp(peer, term, index, resp, sent_at, now);
            }
        }
    }

    fn on_vote(&mut self, me: usize, from: usize, r: VoteResp, req: VoteReq) {
        let now = self.now();
        let c = &self.nodes[me];
        let (pre, term) = (req.pre, req.term);
        // Only replies to the round this node is still in count.
        let current = if pre {
            c.role != Role::Leader && term == c.term + 1
        } else {
            c.role == Role::Candidate && term == c.term
        };
        if !r.granted || !current || !c.is_voter(self.addrs[from]) {
            return;
        }
        let v = self.tally.entry((me, term, pre)).or_default();
        if !v.contains(&from) {
            v.push(from);
        }
        let count = v.len() + 1;
        if count < self.nodes[me].quorum() {
            return;
        }
        if pre {
            self.campaign(me, false);
        } else {
            let c = &mut self.nodes[me];
            if let Some(prev) = self.leaders.insert(c.term, me) {
                assert_eq!(prev, me, "two leaders in term {}", c.term);
            }
            c.become_leader(now);
            self.log
                .push(format!("{} n{me} leads t{}", self.now_ms, c.term));
        }
    }

    fn campaign(&mut self, me: usize, force: bool) {
        let now = self.now();
        let c = &mut self.nodes[me];
        c.start_election(now);
        let req = c.vote_req(false, force);
        self.votes[me].insert(c.term, me);
        self.log
            .push(format!("{} n{me} campaigns t{}", self.now_ms, req.term));
        if self.nodes[me].quorum() == 1 {
            let c = &mut self.nodes[me];
            self.leaders.insert(c.term, me);
            c.become_leader(now);
            return;
        }
        self.ask_votes(me, req);
    }

    fn check(&mut self) {
        // State machine safety: a committed entry is the same on every node
        // that has it, and never changes.
        for (i, c) in self.nodes.iter().enumerate() {
            for k in c.snap_index + 1..=c.commit {
                let e = c.entry(k).expect("committed entry missing").clone();
                match self.committed.get(&k) {
                    Some(prev) => {
                        assert_eq!(prev, &e, "node {i} committed a different entry at {k}")
                    }
                    None => {
                        self.committed.insert(k, e);
                    }
                }
            }
        }
        // Log matching: same (index, term) means identical prefixes.
        for a in 0..self.nodes.len() {
            for b in a + 1..self.nodes.len() {
                let (x, y) = (&self.nodes[a], &self.nodes[b]);
                let lo = x.snap_index.max(y.snap_index);
                let hi = x.last_index().min(y.last_index());
                for k in (lo + 1..=hi).rev() {
                    if x.term_at(k) == y.term_at(k) {
                        for j in lo + 1..=k {
                            assert_eq!(
                                x.entry(j),
                                y.entry(j),
                                "logs {a},{b} differ at {j} under {k}"
                            );
                        }
                        break;
                    }
                }
                // Same applied index, same state.
                if x.applied == y.applied {
                    assert_eq!(
                        x.machine.len("s"),
                        y.machine.len("s"),
                        "nodes {a},{b} diverged at {}",
                        x.applied
                    );
                }
            }
        }
    }

    fn step(&mut self, ms: u64) {
        for _ in 0..ms {
            self.now_ms += 1;
            while let Some((&(at, s), _)) = self.queue.iter().next() {
                if at > self.now_ms {
                    break;
                }
                let (from, to, m) = self.queue.remove(&(at, s)).unwrap();
                self.deliver(from, to, m);
            }
            for i in 0..self.nodes.len() {
                self.tick(i);
            }
            self.check();
        }
    }

    fn leader(&self) -> Option<usize> {
        let mut best: Option<usize> = None;
        for (i, c) in self.nodes.iter().enumerate() {
            if c.is_leader() && !self.down[i] && best.is_none_or(|b| c.term > self.nodes[b].term) {
                best = Some(i);
            }
        }
        best
    }

    fn propose(&mut self) -> Option<oneshot::Receiver<Outcome>> {
        let l = self.leader()?;
        self.proposed += 1;
        let v = format!("v{}", self.proposed);
        let body = append_body("s", 7, self.proposed, Retention::default(), &[v.as_bytes()]);
        let cmd = append_command(&body, self.now_ms).unwrap();
        self.nodes[l].propose(cmd).ok()
    }

    fn reconfigure(
        &mut self,
        edit: impl FnOnce(&mut Vec<Member>),
    ) -> Option<oneshot::Receiver<Outcome>> {
        let l = self.leader()?;
        let mut cfg = self.nodes[l].config.clone();
        edit(&mut cfg);
        self.nodes[l].propose_config(cfg).ok()
    }
}

fn dump(net: &Net) -> String {
    let mut s = net.log.join("\n");
    for (i, c) in net.nodes.iter().enumerate() {
        s += &format!(
            "\nn{i}: {:?} t{} leader {:?} snap {} last {} commit {} voters {}",
            c.role,
            c.term,
            c.leader,
            c.snap_index,
            c.last_index(),
            c.commit,
            c.config.iter().filter(|m| m.voter).count()
        );
    }
    s
}

fn settle(net: &mut Net) -> usize {
    for _ in 0..200 {
        net.step(50);
        if let Some(l) = net.leader() {
            return l;
        }
    }
    panic!("no leader after 10 s of simulated time\n{}", dump(net));
}

fn done(rx: &mut oneshot::Receiver<Outcome>) -> bool {
    matches!(rx.try_recv(), Ok(Ok(_)))
}

#[test]
fn elects_a_leader_and_commits() {
    let mut net = Net::new(3, 1);
    settle(&mut net);
    let mut rx = net.propose().expect("leader accepts");
    net.step(500);
    assert_eq!(rx.try_recv().expect("committed"), Ok(0), "{}", dump(&net));
    for c in &net.nodes {
        assert_eq!(c.machine.len("s"), 1, "every node applies the entry");
    }
}

#[test]
fn a_partitioned_leader_steps_down_and_its_writes_are_discarded() {
    let mut net = Net::new(5, 2);
    let old = settle(&mut net);
    net.cut[old] = true;
    let mut lost = net.propose().expect("still believes it leads");
    net.step(3000);
    let new = net.leader().expect("majority side elects");
    assert_ne!(new, old);
    assert!(
        !net.nodes[old].is_leader(),
        "CheckQuorum demotes the isolated leader"
    );
    let mut kept = net.propose().unwrap();
    net.step(500);
    net.cut[old] = false;
    net.step(2000);
    assert!(
        matches!(lost.try_recv(), Ok(Err(_))),
        "isolated write must fail, not hang"
    );
    assert!(done(&mut kept));
    let want = net.nodes[new].machine.len("s");
    assert_eq!(
        net.nodes[old].machine.len("s"),
        want,
        "old leader repaired its log"
    );
}

#[test]
fn a_rejoining_node_does_not_disrupt_the_leader() {
    let mut net = Net::new(3, 3);
    let l = settle(&mut net);
    let term = net.nodes[l].term;
    let f = (0..3).find(|i| *i != l).unwrap();
    net.cut[f] = true;
    net.step(5000);
    net.cut[f] = false;
    net.step(2000);
    assert_eq!(
        net.leader(),
        Some(l),
        "PreVote keeps the cut-off node from deposing the leader"
    );
    assert_eq!(net.nodes[l].term, term, "no term inflation from the rejoin");
}

#[test]
fn crash_restart_keeps_term_and_vote() {
    let mut net = Net::new(3, 4);
    settle(&mut net);
    for _ in 0..5 {
        net.propose();
    }
    net.step(500);
    for i in 0..3 {
        let now = net.now();
        let (t, v, n) = (
            net.nodes[i].term,
            net.nodes[i].voted_for,
            net.nodes[i].last_index(),
        );
        net.nodes[i].crash_and_restart(now);
        assert_eq!(
            (
                net.nodes[i].term,
                net.nodes[i].voted_for,
                net.nodes[i].last_index()
            ),
            (t, v, n)
        );
        net.step(3000);
    }
    settle(&mut net);
    net.propose().unwrap();
    net.step(1000);
    let lens: Vec<u64> = net.nodes.iter().map(|c| c.machine.len("s")).collect();
    assert!(
        lens.iter().all(|l| *l == lens[0] && *l >= 5),
        "logs converge after restarts: {lens:?}"
    );
}

#[test]
fn retries_apply_exactly_once() {
    let mut net = Net::new(3, 5);
    let l = settle(&mut net);
    let body = append_body("s", 42, 1, Retention::default(), &[b"once"]);
    let mut a = net.nodes[l]
        .propose(append_command(&body, 0).unwrap())
        .unwrap();
    let mut b = net.nodes[l]
        .propose(append_command(&body, 0).unwrap())
        .unwrap();
    net.step(500);
    assert_eq!(a.try_recv().unwrap(), Ok(0));
    assert_eq!(
        b.try_recv().unwrap(),
        Ok(0),
        "the duplicate reports the original index"
    );
    assert_eq!(net.nodes[l].machine.len("s"), 1);
}

#[test]
fn compaction_and_snapshots_bring_a_far_behind_node_back() {
    let mut net = Net::new(3, 6);
    for c in &mut net.nodes {
        c.snapshot_every = 10;
    }
    let l = settle(&mut net);
    let f = (0..3).find(|i| *i != l).unwrap();
    net.down[f] = true;
    for _ in 0..50 {
        net.propose();
        net.step(20);
    }
    net.step(500);
    assert!(
        net.nodes[l].snap_index >= 40,
        "the leader compacted: {}",
        dump(&net)
    );
    assert!(
        net.nodes[l].last_index() - net.nodes[l].snap_index < 20,
        "and dropped the old log"
    );
    net.down[f] = false;
    net.step(2000);
    assert_eq!(
        net.nodes[f].machine.len("s"),
        50,
        "snapshot plus tail restored it:\n{}",
        dump(&net)
    );
    let now = net.now();
    net.nodes[f].crash_and_restart(now);
    assert_eq!(
        net.nodes[f].machine.len("s"),
        50,
        "and it survives a restart from the snapshot"
    );
}

#[test]
fn membership_changes_one_server_at_a_time() {
    let mut net = Net::with(4, 3, 7);
    settle(&mut net);
    for _ in 0..5 {
        net.propose();
    }
    net.step(500);
    let new = net.addrs[3];
    let two = net.reconfigure(|c| {
        c.remove(0);
        c.push(voter("10.9.9.9:1".parse().unwrap()));
    });
    assert!(two.is_none(), "two changes at once are refused");

    let mut add = net
        .reconfigure(|c| {
            c.push(Member {
                voter: false,
                ..voter(new)
            })
        })
        .unwrap();
    net.step(1000);
    assert!(done(&mut add), "learner added:\n{}", dump(&net));
    assert_eq!(net.nodes[3].machine.len("s"), 5, "the learner caught up");
    let l = net.leader().unwrap();
    assert_eq!(
        net.nodes[l].quorum(),
        2,
        "a learner does not raise the quorum"
    );

    let mut promote = net
        .reconfigure(|c| c.iter_mut().find(|m| m.addr == new).unwrap().voter = true)
        .unwrap();
    net.step(1000);
    assert!(done(&mut promote));
    let l = net.leader().unwrap();
    assert_eq!(net.nodes[l].quorum(), 3, "four voters need three");

    // The leader removes itself: it commits the change, then steps down.
    let gone = net.addrs[l];
    let mut remove = net.reconfigure(|c| c.retain(|m| m.addr != gone)).unwrap();
    net.step(3000);
    assert!(done(&mut remove), "{}", dump(&net));
    let l2 = settle(&mut net);
    assert_ne!(l2, l, "a removed leader hands over");
    assert!(!net.nodes[l2].is_voter(gone));
    let mut w = net.propose().unwrap();
    net.step(1000);
    assert!(done(&mut w), "the three remaining voters commit");
}

#[test]
fn leadership_transfer_skips_the_election_timeout() {
    let mut net = Net::new(3, 8);
    let l = settle(&mut net);
    net.propose();
    net.step(300);
    let target = net.nodes[l].transfer_target().unwrap();
    let t = net.idx(target);
    assert!(net.nodes[l].caught_up(target));
    net.nodes[l].transferring = true;
    assert!(net.propose().is_none(), "no writes while handing over");
    let (term, me) = (net.nodes[l].term, net.addrs[l]);
    assert!(net.nodes[t].timeout_now(term, me));
    net.campaign(t, true);
    net.step(200);
    assert_eq!(
        net.leader(),
        Some(t),
        "took over in 200 ms, far under the timeout:\n{}",
        dump(&net)
    );
}

#[test]
fn chaos_keeps_every_safety_property() {
    let seeds: u64 = std::env::var("CHAOS_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(40);
    for seed in 1..=seeds {
        let n = if seed.is_multiple_of(2) { 5 } else { 3 };
        let mut net = Net::new(n, seed);
        if seed.is_multiple_of(3) {
            for c in &mut net.nodes {
                c.snapshot_every = 7;
            }
        }
        net.loss = 10;
        for _ in 0..60 {
            match net.rand() % 7 {
                0 => {
                    let i = (net.rand() % n as u64) as usize;
                    net.cut[i] = !net.cut[i];
                }
                1 => {
                    let i = (net.rand() % n as u64) as usize;
                    let now = net.now();
                    net.nodes[i].crash_and_restart(now);
                    net.tally.retain(|k, _| k.0 != i);
                    net.log.push(format!("{} n{i} restarts", net.now_ms));
                }
                2 => {
                    let i = (net.rand() % n as u64) as usize;
                    let down = net.down.iter().filter(|d| **d).count();
                    if net.down[i] || down + 1 < n.div_ceil(2) {
                        net.down[i] = !net.down[i];
                    }
                }
                3 if n == 5 => {
                    // Toggle the last node between voter and learner.
                    let last = net.addrs[n - 1];
                    net.reconfigure(|c| {
                        if let Some(m) = c.iter_mut().find(|m| m.addr == last) {
                            m.voter = !m.voter;
                        }
                    });
                }
                _ => {
                    net.propose();
                }
            }
            let ms = 100 + net.rand() % 400;
            net.step(ms);
        }
        // Heal everything: the cluster must converge and make progress.
        net.cut = vec![false; n];
        net.down = vec![false; n];
        net.loss = 0;
        settle(&mut net);
        let mut rx = net.propose().unwrap();
        for _ in 0..100 {
            net.step(100);
            if !matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)) {
                break;
            }
        }
        net.step(2000);
        let lens: Vec<u64> = net.nodes.iter().map(|c| c.machine.len("s")).collect();
        assert!(
            lens.iter().all(|l| *l == lens[0]),
            "seed {seed}: state machines diverged after healing: {lens:?}\n{}",
            dump(&net)
        );
    }
}
