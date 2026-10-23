//! Deterministic simulation of whole clusters: random message loss, delay,
//! reordering, partitions and crash-restarts, with Raft's safety properties
//! checked after every step (Figure 3 of the paper).

use super::*;
use std::collections::{BTreeMap, HashMap};

#[derive(Clone)]
enum Msg {
    Vote(VoteReq),
    VoteR(VoteResp, VoteReq),
    App(AppendReq, Instant),
    AppR(AppendReq, AppendResp, Instant),
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

impl Net {
    fn new(n: usize, seed: u64) -> Self {
        let epoch = Instant::now();
        let addrs: Vec<SocketAddr> = (0..n)
            .map(|i| format!("10.0.0.{}:1", i + 1).parse().unwrap())
            .collect();
        let nodes = (0..n)
            .map(|i| {
                let peers = addrs.iter().copied().filter(|a| *a != addrs[i]).collect();
                let mut c = Core::new(addrs[i], peers, None, epoch);
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

    fn tick(&mut self, i: usize) {
        let now = self.now();
        let c = &mut self.nodes[i];
        if self.down[i] {
            return;
        }
        // Disk is instant in the simulation.
        c.durable = c.last_index();
        c.state_synced = c.state_seq;
        if c.is_leader() {
            c.advance_commit();
            c.check_quorum(now);
        } else if now >= c.deadline {
            // PreVote round first.
            c.reset_deadline(now);
            let req = c.vote_req(true);
            for j in 0..self.nodes.len() {
                if j != i {
                    self.send(i, j, Msg::Vote(req));
                }
            }
        }
        let c = &mut self.nodes[i];
        if c.is_leader() && self.now_ms % 50 == 0 {
            let reqs: Vec<(usize, AppendReq)> = c
                .peers
                .clone()
                .into_iter()
                .filter_map(|p| c.append_req(p).map(|r| (p, r)))
                .map(|(p, r)| (self.addrs.iter().position(|x| *x == p).unwrap(), r))
                .collect();
            for (j, r) in reqs {
                self.send(i, j, Msg::App(r, now));
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
        if !r.granted || !current {
            return;
        }
        let v = self.tally.entry((me, term, pre)).or_default();
        if !v.contains(&from) {
            v.push(from);
        }
        let count = v.len() + 1;
        let quorum = self.nodes[me].quorum();
        if count < quorum {
            return;
        }
        if pre {
            let c = &mut self.nodes[me];
            c.start_election(now);
            let req = c.vote_req(false);
            self.log
                .push(format!("{} n{me} campaigns t{}", self.now_ms, req.term));
            self.votes[me].insert(c.term, me);
            for j in 0..self.nodes.len() {
                if j != me {
                    self.send(me, j, Msg::Vote(req));
                }
            }
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

    fn check(&mut self) {
        // Leader completeness and state machine safety: every committed entry
        // is identical on every node that has it, and never changes.
        for (i, c) in self.nodes.iter().enumerate() {
            for k in 1..=c.commit {
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
                let n = x.last_index().min(y.last_index());
                for k in (1..=n).rev() {
                    if x.term_at(k) == y.term_at(k) {
                        for j in 1..=k {
                            assert_eq!(
                                x.entry(j),
                                y.entry(j),
                                "logs {a},{b} differ at {j} under {k}"
                            );
                        }
                        break;
                    }
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
        let body = append_body(
            "s",
            7,
            self.proposed,
            &[format!("v{}", self.proposed).as_bytes()],
        );
        self.nodes[l].propose(append_command(&body).unwrap()).ok()
    }
}

fn dump(net: &Net) -> String {
    let mut s = net.log.join("\n");
    for (i, c) in net.nodes.iter().enumerate() {
        s += &format!(
            "\nn{i}: {:?} t{} leader {:?} last {} commit {}",
            c.role,
            c.term,
            c.leader,
            c.last_index(),
            c.commit
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
    panic!("no leader after 10 s of simulated time");
}

#[test]
fn elects_a_leader_and_commits() {
    let mut net = Net::new(3, 1);
    let l = settle(&mut net);
    let mut rx = net.propose().expect("leader accepts");
    net.step(500);
    assert_eq!(rx.try_recv().expect("committed"), Ok(0), "{}", dump(&net));
    for c in &net.nodes {
        assert_eq!(c.machine.len("s"), 1, "every node applies the entry");
    }
    let _ = l;
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
    assert!(matches!(kept.try_recv(), Ok(Ok(_))));
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
    let body = append_body("s", 42, 1, &[b"once"]);
    let a = net.nodes[l]
        .propose(append_command(&body).unwrap())
        .unwrap();
    let b = net.nodes[l]
        .propose(append_command(&body).unwrap())
        .unwrap();
    net.step(500);
    let (mut a, mut b) = (a, b);
    assert_eq!(a.try_recv().unwrap(), Ok(0));
    assert_eq!(
        b.try_recv().unwrap(),
        Ok(0),
        "the duplicate reports the original index"
    );
    assert_eq!(net.nodes[l].machine.len("s"), 1);
}

#[test]
fn chaos_keeps_every_safety_property() {
    let seeds: u64 = std::env::var("CHAOS_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(40);
    for seed in 1..=seeds {
        let n = if seed % 2 == 0 { 5 } else { 3 };
        let mut net = Net::new(n, seed);
        net.loss = 10;
        for round in 0..60 {
            match net.rand() % 6 {
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
                _ => {
                    net.propose();
                }
            }
            let ms = 100 + net.rand() % 400;
            net.step(ms);
            let _ = round;
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
            "seed {seed}: state machines diverged after healing: {lens:?}"
        );
    }
}
