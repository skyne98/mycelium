//! Raft (Ongaro and Ousterhout, 2014) with the extensions from Ongaro's thesis:
//! PreVote (9.6), CheckQuorum (6.2), ReadIndex (6.4), client sessions (6.3),
//! single-server membership changes with catch-up as a learner (4.1-4.2),
//! log compaction with InstallSnapshot (5), and leadership transfer (3.10).
//!
//! Pure state: no sockets and no tasks. node.rs moves the messages; this
//! decides. Time is passed in, so the simulation in raft/tests.rs can drive it.
//!
//! The replicated state machine is the set of named streams plus the
//! committed membership. One log entry is one append of one or more payloads
//! to one stream, so a batch is atomic and contiguous, and every node assigns
//! the same stream indices.

use crate::transport::{parts, Blobs};
use crate::wal::{Loaded, Snapshot, Wal};
use crate::Result;
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::File;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// Election timeouts are drawn from [ELECTION_MIN, 2 * ELECTION_MIN).
pub(crate) const ELECTION_MIN: Duration = Duration::from_millis(1000);
pub(crate) const HEARTBEAT: Duration = Duration::from_millis(100);
/// Largest single append, so any batch fits a frame.
pub(crate) const MAX_ENTRY: usize = 16 << 20;
/// Take a snapshot after this many applied entries, by default.
pub(crate) const SNAPSHOT_EVERY: u64 = 10_000;
const MAX_BATCH_ENTRIES: usize = 1024;
const MAX_BATCH_BYTES: usize = 4 << 20;
/// Remembered (client, seq) pairs for exactly-once retries.
const SEEN_CAP: usize = 100_000;

const NOOP: u8 = 0;
const APPEND: u8 = 1;
const CONFIG: u8 = 2;
const TRIM: u8 = 3;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Entry {
    pub term: u64,
    pub cmd: Bytes,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Role {
    Follower,
    Candidate,
    Leader,
}

/// One server in the cluster. Learners receive the log but do not vote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Member {
    pub addr: SocketAddr,
    pub name: Option<String>,
    pub voter: bool,
}

pub(crate) struct Progress {
    pub next: u64,
    pub matched: u64,
    pub sent_commit: u64,
    /// When the last reply arrived (CheckQuorum).
    pub last_ack: Instant,
    /// When the newest acknowledged request was sent (ReadIndex).
    pub acked_sent: Option<Instant>,
}

/// What the leader should send a follower next.
pub(crate) enum Outbound {
    Append(AppendReq),
    /// The follower needs entries that were compacted away.
    Snapshot,
}

/// The stream index of the first payload, or why the write did not land.
pub(crate) type Outcome = std::result::Result<u64, String>;

type StateWrite = Box<dyn FnOnce() -> std::io::Result<()> + Send>;

pub(crate) struct Core {
    pub me: SocketAddr,
    pub term: u64,
    pub voted_for: Option<SocketAddr>,
    /// Entries snap_index + 1 onwards.
    log: Vec<Entry>,
    pub snap_index: u64,
    pub snap_term: u64,
    /// Membership as of snap_index: the snapshot's, or the bootstrap list.
    base_config: Vec<Member>,
    /// Membership in effect: the newest in the log, committed or not (4.1).
    pub config: Vec<Member>,
    pub config_index: u64,
    /// Bumped whenever `config` changes.
    pub config_version: u64,
    pub commit: u64,
    pub applied: u64,
    /// Highest index known to be on this node's disk.
    pub durable: u64,
    pub role: Role,
    pub leader: Option<SocketAddr>,
    pub last_contact: Option<Instant>,
    pub deadline: Instant,
    clock: Instant,
    /// This leader's first entry. Reads wait until it commits, or a fresh
    /// leader could answer from a stale commit index.
    pub term_start: u64,
    pub progress: HashMap<SocketAddr, Progress>,
    waiters: BTreeMap<u64, (u64, oneshot::Sender<Outcome>)>,
    pub machine: Machine,
    wal: Option<Wal>,
    state_seq: u64,
    state_issued: u64,
    state_synced: u64,
    snapshotting: bool,
    pub snapshot_every: u64,
    /// Handing leadership over: no new proposals meanwhile.
    pub transferring: bool,
    /// Set on a disk failure or shutdown: from then on the node acknowledges
    /// nothing, because what it says can no longer be trusted.
    pub io_error: Option<String>,
    /// xorshift state for election jitter; seeded per node, settable in tests.
    pub rng: u64,
    /// The latest durable snapshot, kept to send to lagging followers.
    // ponytail: held in memory; stream from the file if states outgrow RAM.
    pub snapshot: Option<Arc<Snapshot>>,
    /// The membership this node was started with; a simulated restart needs it.
    #[cfg_attr(not(test), allow(dead_code))]
    bootstrap: Vec<Member>,
}

/// Disk work, run off the async runtime: (term, vote) and an fsync.
pub(crate) struct Job {
    state: Option<(StateWrite, u64)>,
    file: Option<Arc<File>>,
    generation: u64,
    target: u64,
}

pub(crate) struct Done {
    state: Option<u64>,
    generation: u64,
    target: u64,
}

impl Job {
    pub fn run(self) -> std::io::Result<Done> {
        let mut state = None;
        if let Some((write, seq)) = self.state {
            write()?;
            state = Some(seq);
        }
        if let Some(f) = &self.file {
            f.sync_data()?;
        }
        Ok(Done {
            state,
            generation: self.generation,
            target: self.target,
        })
    }
}

/// A snapshot to encode and write off the runtime, then `compact` to.
pub(crate) struct SnapshotJob {
    pub index: u64,
    pub term: u64,
    machine: Machine,
}

impl SnapshotJob {
    pub fn encode(&self) -> Snapshot {
        Snapshot {
            index: self.index,
            term: self.term,
            data: self.machine.encode(),
        }
    }

    pub fn config(&self) -> Vec<Member> {
        self.machine.config.clone()
    }
}

/// A random u64 from the std hasher's per-process keys; no rand crate needed.
pub(crate) fn random_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    h.finish() | 1
}

fn latest_config(log: &[Entry]) -> Option<(usize, Vec<Member>)> {
    log.iter().enumerate().rev().find_map(|(i, e)| {
        (e.cmd.first() == Some(&CONFIG))
            .then(|| decode_config(&e.cmd[1..]).ok().map(|c| (i, c)))
            .flatten()
    })
}

impl Core {
    /// `bootstrap` is the membership to use when neither the log nor a
    /// snapshot records one: every node of a new cluster, or nobody when joining.
    pub fn new(
        me: SocketAddr,
        bootstrap: Vec<Member>,
        mut wal: Option<Wal>,
        loaded: Loaded,
        now: Instant,
    ) -> Result<Self> {
        let (mut machine, snap_index, snap_term) = match &loaded.snapshot {
            Some(s) => (Machine::decode(&s.data)?, s.index, s.term),
            None => (
                Machine {
                    config: bootstrap.clone(),
                    ..Machine::default()
                },
                0,
                0,
            ),
        };
        if loaded.base > snap_index {
            return Err(format!(
                "the log starts after entry {} but the snapshot ends at {snap_index}",
                loaded.base
            )
            .into());
        }
        // Keep only what follows the snapshot, and only if it continues it.
        let skip = (snap_index - loaded.base) as usize;
        let continues = skip == 0
            || loaded
                .entries
                .get(skip - 1)
                .is_some_and(|(t, _)| *t == snap_term);
        let kept: Vec<Entry> = loaded
            .entries
            .into_iter()
            .skip(skip)
            .filter(|_| continues)
            .map(|(term, cmd)| Entry {
                term,
                cmd: Bytes::from(cmd),
            })
            .collect();
        if let Some(w) = &mut wal {
            if skip > 0 || !continues {
                w.reset(snap_index, kept.iter().map(|e| (e.term, &e.cmd[..])))?;
            }
        }
        let snapshot = loaded.snapshot.map(Arc::new);
        let base_config = machine.config.clone();
        let (config, config_index) = match latest_config(&kept) {
            Some((i, c)) => (c, snap_index + 1 + i as u64),
            None => (base_config.clone(), snap_index),
        };
        machine.config = base_config.clone();
        let mut core = Self {
            me,
            term: loaded.term,
            voted_for: loaded.vote,
            durable: snap_index + kept.len() as u64,
            log: kept,
            snap_index,
            snap_term,
            base_config,
            config,
            config_index,
            config_version: 0,
            commit: snap_index,
            applied: snap_index,
            role: Role::Follower,
            leader: None,
            last_contact: None,
            deadline: now,
            clock: now,
            term_start: 0,
            progress: HashMap::new(),
            waiters: BTreeMap::new(),
            machine,
            wal,
            state_seq: 0,
            state_issued: 0,
            state_synced: 0,
            snapshotting: false,
            snapshot_every: SNAPSHOT_EVERY,
            transferring: false,
            io_error: None,
            rng: random_u64(),
            snapshot,
            bootstrap,
        };
        core.reset_deadline(now);
        Ok(core)
    }

    pub fn last_index(&self) -> u64 {
        self.snap_index + self.log.len() as u64
    }

    pub fn last_term(&self) -> u64 {
        self.log.last().map_or(self.snap_term, |e| e.term)
    }

    pub fn term_at(&self, i: u64) -> Option<u64> {
        if i == self.snap_index {
            return Some(self.snap_term);
        }
        if i < self.snap_index {
            return None;
        }
        self.log.get((i - self.snap_index - 1) as usize).map(|e| e.term)
    }

    #[cfg(test)]
    pub fn entry(&self, i: u64) -> Option<&Entry> {
        let at = i.checked_sub(self.snap_index + 1)?;
        self.log.get(at as usize)
    }

    // ---------------------------------------------------------- membership

    pub fn is_voter(&self, addr: SocketAddr) -> bool {
        self.config.iter().any(|m| m.addr == addr && m.voter)
    }

    /// Every other member, voters and learners: who the leader replicates to.
    pub fn others(&self) -> Vec<SocketAddr> {
        self.config
            .iter()
            .filter(|m| m.addr != self.me)
            .map(|m| m.addr)
            .collect()
    }

    /// Other voters: who a candidate asks.
    pub fn other_voters(&self) -> Vec<SocketAddr> {
        self.config
            .iter()
            .filter(|m| m.voter && m.addr != self.me)
            .map(|m| m.addr)
            .collect()
    }

    pub fn quorum(&self) -> usize {
        self.config.iter().filter(|m| m.voter).count() / 2 + 1
    }

    fn set_config(&mut self, config: Vec<Member>, index: u64) {
        self.config = config;
        self.config_index = index;
        self.config_version += 1;
        if self.role == Role::Leader {
            let others = self.others();
            self.progress.retain(|a, _| others.contains(a));
            let (next, clock) = (self.last_index() + 1, self.clock);
            for a in others {
                self.progress.entry(a).or_insert(Progress {
                    next,
                    matched: 0,
                    sent_commit: 0,
                    last_ack: clock,
                    acked_sent: None,
                });
            }
        }
    }

    /// After entries were cut, the newest surviving config is in effect again.
    fn recompute_config(&mut self) {
        let (config, index) = match latest_config(&self.log) {
            Some((i, c)) => (c, self.snap_index + 1 + i as u64),
            None => (self.base_config.clone(), self.snap_index),
        };
        if config != self.config || index != self.config_index {
            self.set_config(config, index);
        }
    }

    /// Propose a new membership. Only one change at a time, and each adds or
    /// removes one server or promotes one learner (thesis 4.1).
    pub fn propose_config(
        &mut self,
        config: Vec<Member>,
    ) -> std::result::Result<oneshot::Receiver<Outcome>, String> {
        if self.role != Role::Leader || self.transferring || self.io_error.is_some() {
            return Err("not the leader".into());
        }
        if self.config_index > self.commit || self.commit < self.term_start {
            // The second condition is Ongaro's fix for single-server changes:
            // a new leader commits an entry of its own term first.
            return Err("another membership change is still in progress".into());
        }
        let addrs = |c: &[Member], voters: bool| -> HashSet<SocketAddr> {
            c.iter().filter(|m| m.voter || !voters).map(|m| m.addr).collect()
        };
        let members = addrs(&self.config, false).symmetric_difference(&addrs(&config, false)).count();
        let voters = addrs(&self.config, true).symmetric_difference(&addrs(&config, true)).count();
        if members > 1 || voters > 1 {
            return Err("change one server at a time".into());
        }
        if !config.iter().any(|m| m.voter) {
            return Err("a cluster needs at least one voter".into());
        }
        let mut cmd = vec![CONFIG];
        cmd.extend_from_slice(&encode_config(&config));
        self.propose(Bytes::from(cmd))
            .map_err(|_| "not the leader".to_string())
    }

    // -------------------------------------------------------------- roles

    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    /// Randomized in [ELECTION_MIN, 2 * ELECTION_MIN), so nodes rarely split votes.
    pub fn reset_deadline(&mut self, now: Instant) {
        self.clock = self.clock.max(now);
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        let ms = ELECTION_MIN.as_millis() as u64;
        self.deadline = now + ELECTION_MIN + Duration::from_millis(x % ms);
    }

    /// Heard from a live leader recently, so votes for anyone else would only
    /// disrupt it (thesis 4.2.3). This also keeps removed servers harmless.
    fn sticky(&self, now: Instant) -> bool {
        self.role == Role::Leader
            || self
                .last_contact
                .is_some_and(|t| now.saturating_duration_since(t) < ELECTION_MIN)
    }

    fn mark_state(&mut self) {
        self.state_seq += 1;
        if self.wal.is_none() {
            self.state_synced = self.state_seq;
        }
    }

    fn push(&mut self, e: Entry) {
        if let Some(w) = &mut self.wal {
            if let Err(err) = w.append(e.term, &e.cmd) {
                self.stop(format!("log write failed: {err}"));
                return;
            }
        }
        let config = (e.cmd.first() == Some(&CONFIG))
            .then(|| decode_config(&e.cmd[1..]).ok())
            .flatten();
        self.log.push(e);
        if self.wal.is_none() {
            self.durable = self.last_index();
        }
        if let Some(c) = config {
            let idx = self.last_index();
            self.set_config(c, idx);
        }
    }

    /// Drop entry `from` and everything after it. Refuses to touch committed
    /// entries: that would mean the logs diverged, so this node stops instead.
    fn truncate(&mut self, from: u64) -> bool {
        if from <= self.commit {
            self.stop(format!(
                "log diverged from the leader at {from}, below the commit index {}",
                self.commit
            ));
            return false;
        }
        self.log.truncate((from - self.snap_index - 1) as usize);
        if let Some(w) = &mut self.wal {
            if let Err(err) = w.truncate(from) {
                self.stop(format!("log truncate failed: {err}"));
                return false;
            }
        }
        self.durable = self.durable.min(from - 1);
        for (_, (_, tx)) in self.waiters.split_off(&from) {
            let _ = tx.send(Err("discarded by a newer leader".into()));
        }
        if self.config_index >= from {
            self.recompute_config();
        }
        true
    }

    fn fail_waiters(&mut self, why: &str) {
        for (_, (_, tx)) in std::mem::take(&mut self.waiters) {
            let _ = tx.send(Err(why.into()));
        }
    }

    /// Stop participating: no votes, no acks, no writes.
    pub fn stop(&mut self, why: String) {
        if self.io_error.is_none() {
            self.io_error = Some(why);
        }
        let term = self.term;
        self.become_follower(term, None);
        self.fail_waiters("node stopped");
    }

    pub fn become_follower(&mut self, term: u64, leader: Option<SocketAddr>) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
            self.mark_state();
        }
        if self.role == Role::Leader {
            self.fail_waiters("leader changed before the write was confirmed; it may still commit");
            self.progress.clear();
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.transferring = false;
    }

    pub fn start_election(&mut self, now: Instant) {
        self.term += 1;
        self.voted_for = Some(self.me);
        self.mark_state();
        self.role = Role::Candidate;
        self.leader = None;
        self.reset_deadline(now);
    }

    pub fn become_leader(&mut self, now: Instant) {
        self.clock = self.clock.max(now);
        self.role = Role::Leader;
        self.leader = Some(self.me);
        self.transferring = false;
        let next = self.last_index() + 1;
        self.progress = self
            .others()
            .into_iter()
            .map(|p| {
                let p0 = Progress {
                    next,
                    matched: 0,
                    sent_commit: 0,
                    last_ack: now,
                    acked_sent: None,
                };
                (p, p0)
            })
            .collect();
        // Earlier terms' entries commit only beneath one from this term (5.4.2).
        self.push(Entry {
            term: self.term,
            cmd: Bytes::from_static(&[NOOP]),
        });
        self.term_start = self.last_index();
        self.advance_commit();
    }

    /// Append on the leader. Err carries the leader to try instead, if known.
    pub fn propose(
        &mut self,
        cmd: Bytes,
    ) -> std::result::Result<oneshot::Receiver<Outcome>, Option<SocketAddr>> {
        if self.role != Role::Leader || self.transferring || self.io_error.is_some() {
            return Err(self.leader.filter(|l| *l != self.me));
        }
        self.push(Entry {
            term: self.term,
            cmd,
        });
        let (tx, rx) = oneshot::channel();
        self.waiters.insert(self.last_index(), (self.term, tx));
        self.advance_commit();
        Ok(rx)
    }

    /// Highest index on a majority of voters, counted only if from this term.
    pub fn advance_commit(&mut self) -> bool {
        if self.role != Role::Leader {
            return false;
        }
        let mut m: Vec<u64> = self
            .config
            .iter()
            .filter(|v| v.voter)
            .map(|v| {
                if v.addr == self.me {
                    self.durable
                } else {
                    self.progress.get(&v.addr).map_or(0, |p| p.matched)
                }
            })
            .collect();
        if m.is_empty() {
            return false;
        }
        m.sort_unstable_by(|a, b| b.cmp(a));
        let n = m[self.quorum() - 1];
        if n > self.commit && self.term_at(n) == Some(self.term) {
            self.commit = n;
            return true;
        }
        false
    }

    pub fn apply(&mut self) -> bool {
        let mut any = false;
        while self.applied < self.commit {
            let i = self.applied + 1;
            let e = self.log[(i - self.snap_index - 1) as usize].clone();
            let first = self.machine.apply(&e.cmd);
            if let Some((t, tx)) = self.waiters.remove(&i) {
                let _ = tx.send(if t == e.term {
                    Ok(first)
                } else {
                    Err("discarded by a newer leader".into())
                });
            }
            self.applied = i;
            any = true;
            // A leader removed from the cluster hands over once that is
            // committed (thesis 4.2.2).
            if self.role == Role::Leader
                && e.cmd.first() == Some(&CONFIG)
                && !self.machine.config.iter().any(|m| m.addr == self.me)
            {
                let term = self.term;
                self.become_follower(term, None);
            }
        }
        any
    }

    // ------------------------------------------------------------ follower

    /// Shared by AppendEntries and InstallSnapshot: a current leader resets
    /// this node's timer. Err carries this node's newer term.
    fn accept_leader(&mut self, term: u64, leader: SocketAddr, now: Instant) -> std::result::Result<(), u64> {
        if self.io_error.is_some() || term < self.term {
            return Err(self.term);
        }
        self.become_follower(term, Some(leader));
        self.last_contact = Some(now);
        self.reset_deadline(now);
        Ok(())
    }

    pub fn handle_append(&mut self, req: AppendReq, now: Instant) -> AppendResp {
        let fail = |term, index| AppendResp {
            term,
            ok: false,
            index,
        };
        if let Err(t) = self.accept_leader(req.term, req.leader, now) {
            return fail(t, 0);
        }
        if req.prev_index > self.last_index() {
            return fail(self.term, self.last_index() + 1);
        }
        // Below the snapshot everything is committed, so it matches.
        if req.prev_index >= self.snap_index {
            if let Some(t) = self.term_at(req.prev_index).filter(|t| *t != req.prev_term) {
                if req.prev_index <= self.commit {
                    self.stop(format!("log diverged below the commit index at {}", req.prev_index));
                    return fail(self.term, 0);
                }
                // Skip back over the whole conflicting term in one round trip.
                let mut i = req.prev_index;
                while i > self.commit + 1 && self.term_at(i - 1) == Some(t) {
                    i -= 1;
                }
                return fail(self.term, i);
            }
        }
        let mut idx = req.prev_index;
        for e in req.entries {
            idx += 1;
            if idx <= self.snap_index {
                continue;
            }
            match self.term_at(idx) {
                Some(t) if t == e.term => {}
                Some(_) => {
                    if !self.truncate(idx) {
                        return fail(self.term, 0);
                    }
                    self.push(e);
                }
                None => self.push(e),
            }
            if self.io_error.is_some() {
                return fail(self.term, 0);
            }
        }
        if req.commit > self.commit {
            self.commit = self.commit.max(req.commit.min(idx));
        }
        AppendResp {
            term: self.term,
            ok: true,
            index: idx,
        }
    }

    /// One chunk of a snapshot arrived: Err(term) if the sender is stale.
    pub fn handle_snapshot_chunk(&mut self, term: u64, leader: SocketAddr, now: Instant) -> std::result::Result<(), u64> {
        self.accept_leader(term, leader, now)
    }

    /// Replace the state with a snapshot whose file is already durable.
    pub fn install_snapshot(&mut self, snap: Arc<Snapshot>, machine: Machine) -> AppendResp {
        let (index, term) = (snap.index, snap.term);
        let ok = |s: &Self| AppendResp {
            term: s.term,
            ok: true,
            index,
        };
        if index <= self.commit || self.io_error.is_some() {
            return ok(self);
        }
        let suffix: Vec<Entry> = if self.term_at(index) == Some(term) {
            self.log[(index - self.snap_index) as usize..].to_vec()
        } else {
            Vec::new()
        };
        if let Some(w) = &mut self.wal {
            if let Err(e) = w.reset(index, suffix.iter().map(|e| (e.term, &e.cmd[..]))) {
                self.stop(format!("log reset failed: {e}"));
                return AppendResp {
                    term: self.term,
                    ok: false,
                    index: 0,
                };
            }
        }
        self.log = suffix;
        self.snap_index = index;
        self.snap_term = term;
        self.base_config = machine.config.clone();
        self.machine = machine;
        self.commit = index;
        self.applied = index;
        self.durable = self.last_index();
        self.snapshotting = false;
        self.snapshot = Some(snap);
        self.recompute_config();
        ok(self)
    }

    pub fn handle_vote(&mut self, req: VoteReq, now: Instant) -> VoteResp {
        let up_to_date = (req.last_term, req.last_index) >= (self.last_term(), self.last_index());
        let deny = |term| VoteResp {
            term,
            granted: false,
        };
        if self.io_error.is_some() || (self.sticky(now) && !req.force) {
            return deny(self.term);
        }
        if req.pre {
            // Would vote, but changes nothing: a node that cannot win never
            // bumps anyone's term.
            return VoteResp {
                term: self.term,
                granted: req.term > self.term && up_to_date,
            };
        }
        if req.term < self.term {
            return deny(self.term);
        }
        if req.term > self.term {
            self.become_follower(req.term, None);
        }
        let free = self.voted_for.is_none() || self.voted_for == Some(req.candidate);
        if free && up_to_date {
            if self.voted_for.is_none() {
                self.voted_for = Some(req.candidate);
                self.mark_state();
            }
            self.reset_deadline(now);
            return VoteResp {
                term: self.term,
                granted: true,
            };
        }
        deny(self.term)
    }

    pub fn vote_req(&self, pre: bool, force: bool) -> VoteReq {
        VoteReq {
            term: if pre { self.term + 1 } else { self.term },
            candidate: self.me,
            last_index: self.last_index(),
            last_term: self.last_term(),
            pre,
            force,
        }
    }

    /// The leader asked this node to campaign now (3.10). True if it should.
    pub fn timeout_now(&mut self, term: u64, leader: SocketAddr) -> bool {
        term == self.term
            && self.leader == Some(leader)
            && self.role == Role::Follower
            && self.is_voter(self.me)
            && self.io_error.is_none()
    }

    // -------------------------------------------------------------- leader

    pub fn outbound(&self, peer: SocketAddr) -> Option<Outbound> {
        if self.role != Role::Leader {
            return None;
        }
        let p = self.progress.get(&peer)?;
        if p.next <= self.snap_index {
            return Some(Outbound::Snapshot);
        }
        let prev = (p.next - 1).min(self.last_index());
        let prev_term = self.term_at(prev)?;
        let mut entries = Vec::new();
        let mut bytes = 0;
        for e in &self.log[(prev - self.snap_index) as usize..] {
            if entries.len() >= MAX_BATCH_ENTRIES
                || (!entries.is_empty() && bytes + e.cmd.len() > MAX_BATCH_BYTES)
            {
                break;
            }
            bytes += e.cmd.len();
            entries.push(e.clone());
        }
        Some(Outbound::Append(AppendReq {
            term: self.term,
            leader: self.me,
            prev_index: prev,
            prev_term,
            commit: self.commit,
            entries,
        }))
    }

    pub fn has_work(&self, peer: SocketAddr) -> bool {
        self.progress
            .get(&peer)
            .is_some_and(|p| p.next <= self.last_index() || p.sent_commit < self.commit)
    }

    /// A majority of voters answered requests sent at or after `t`: nobody
    /// else led then.
    pub fn confirmed_since(&self, t: Instant) -> bool {
        let fresh = self
            .config
            .iter()
            .filter(|m| m.voter)
            .filter(|m| {
                m.addr == self.me
                    || self
                        .progress
                        .get(&m.addr)
                        .is_some_and(|p| p.acked_sent.is_some_and(|s| s >= t))
            })
            .count();
        fresh >= self.quorum()
    }

    fn reply_from(&mut self, peer: SocketAddr, term: u64, sent_term: u64, sent_at: Instant, now: Instant) -> bool {
        self.clock = self.clock.max(now);
        if term > self.term {
            self.become_follower(term, None);
            self.reset_deadline(now);
            return false;
        }
        if self.role != Role::Leader || self.term != sent_term {
            return false;
        }
        let Some(p) = self.progress.get_mut(&peer) else {
            return false;
        };
        p.last_ack = now;
        p.acked_sent = p.acked_sent.max(Some(sent_at));
        true
    }

    /// Returns true when the commit index moved.
    pub fn on_append_resp(
        &mut self,
        peer: SocketAddr,
        sent: &AppendReq,
        resp: AppendResp,
        sent_at: Instant,
        now: Instant,
    ) -> bool {
        if !self.reply_from(peer, resp.term, sent.term, sent_at, now) {
            return false;
        }
        let p = self.progress.get_mut(&peer).expect("checked by reply_from");
        if resp.ok {
            p.matched = p.matched.max(resp.index);
            p.next = p.next.max(p.matched + 1);
            p.sent_commit = p.sent_commit.max(sent.commit);
            self.advance_commit()
        } else {
            let prev = sent.prev_index;
            let back = if (1..=prev).contains(&resp.index) {
                resp.index
            } else {
                prev.max(1)
            };
            p.next = back.max(p.matched + 1);
            false
        }
    }

    /// The follower installed a snapshot ending at `index`.
    pub fn on_snapshot_resp(&mut self, peer: SocketAddr, sent_term: u64, index: u64, resp: AppendResp, sent_at: Instant, now: Instant) -> bool {
        if !self.reply_from(peer, resp.term, sent_term, sent_at, now) || !resp.ok {
            return false;
        }
        let p = self.progress.get_mut(&peer).expect("checked by reply_from");
        p.matched = p.matched.max(index);
        p.next = p.next.max(index + 1);
        self.advance_commit()
    }

    /// A leader that cannot reach a majority steps down, so a partitioned
    /// leader stops accepting writes it can never commit.
    pub fn check_quorum(&mut self, now: Instant) -> bool {
        self.clock = self.clock.max(now);
        if self.role != Role::Leader {
            return false;
        }
        let live = self
            .config
            .iter()
            .filter(|m| m.voter)
            .filter(|m| {
                m.addr == self.me
                    || self
                        .progress
                        .get(&m.addr)
                        .is_some_and(|p| now.saturating_duration_since(p.last_ack) < ELECTION_MIN)
            })
            .count();
        if live < self.quorum() {
            let term = self.term;
            self.become_follower(term, None);
            self.reset_deadline(now);
            return true;
        }
        false
    }

    /// The best voter to hand leadership to: the most caught up.
    pub fn transfer_target(&self) -> Option<SocketAddr> {
        self.other_voters()
            .into_iter()
            .max_by_key(|a| self.progress.get(a).map_or(0, |p| p.matched))
    }

    pub fn caught_up(&self, peer: SocketAddr) -> bool {
        self.progress.get(&peer).is_some_and(|p| p.matched >= self.last_index())
    }

    // ---------------------------------------------------------------- disk

    /// Whatever must reach disk before this node may answer.
    pub fn job(&mut self) -> Option<Job> {
        let wal = self.wal.as_ref()?;
        if self.io_error.is_some() {
            return None;
        }
        let state = (self.state_issued < self.state_seq).then(|| {
            let write: StateWrite = Box::new(wal.state_job(self.term, self.voted_for));
            (write, self.state_seq)
        });
        let file = (self.durable < self.last_index()).then(|| wal.file());
        if state.is_none() && file.is_none() {
            return None;
        }
        let generation = wal.generation;
        if state.is_some() {
            self.state_issued = self.state_seq;
        }
        Some(Job {
            state,
            file,
            generation,
            target: self.last_index(),
        })
    }

    pub fn synced(&mut self, done: std::io::Result<Done>) {
        match done {
            Ok(d) => {
                if let Some(seq) = d.state {
                    self.state_synced = self.state_synced.max(seq);
                }
                if self.wal.as_ref().map(|w| w.generation) == Some(d.generation) {
                    self.durable = self.durable.max(d.target.min(self.last_index()));
                }
            }
            Err(e) => self.stop(format!("disk write failed: {e}")),
        }
    }

    /// Term, vote and the log up to `idx` are on disk.
    pub fn persisted(&self, idx: u64) -> bool {
        self.state_synced >= self.state_seq && self.durable >= idx
    }

    pub fn snapshot_dir(&self) -> Option<std::path::PathBuf> {
        self.wal.as_ref().map(|w| w.dir().to_path_buf())
    }

    /// Enough has been applied since the last snapshot: take one. Without a
    /// disk the log is simply cut.
    pub fn snapshot_job(&mut self) -> Option<SnapshotJob> {
        if self.snapshotting || self.io_error.is_some() || self.applied < self.snap_index + self.snapshot_every.max(1) {
            return None;
        }
        let job = SnapshotJob {
            index: self.applied,
            term: self.term_at(self.applied)?,
            machine: self.machine.clone(),
        };
        if self.wal.is_none() {
            let config = job.config();
            self.compact(Arc::new(job.encode()), config);
            return None;
        }
        self.snapshotting = true;
        Some(job)
    }

    /// The snapshot is durable: drop the log it covers.
    pub fn compact(&mut self, snap: Arc<Snapshot>, config: Vec<Member>) {
        self.snapshotting = false;
        let index = snap.index;
        if index <= self.snap_index || index > self.applied {
            return;
        }
        self.snapshot = Some(snap);
        let Some(term) = self.term_at(index) else { return };
        self.log.drain(..(index - self.snap_index) as usize);
        self.snap_index = index;
        self.snap_term = term;
        self.base_config = config;
        if let Some(w) = &mut self.wal {
            if let Err(e) = w.reset(index, self.log.iter().map(|e| (e.term, &e.cmd[..]))) {
                self.stop(format!("log compaction failed: {e}"));
                return;
            }
            self.durable = self.last_index();
        }
    }

    pub fn snapshot_failed(&mut self, why: String) {
        self.snapshotting = false;
        self.stop(format!("snapshot failed: {why}"));
    }

    pub fn release_disk(&mut self) {
        self.wal = None;
    }

    /// What a restart would find on disk: the durable snapshot and log.
    #[cfg(test)]
    pub fn crash_and_restart(&mut self, now: Instant) {
        self.fail_waiters("crashed");
        let loaded = Loaded {
            base: self.snap_index,
            entries: self.log.iter().map(|e| (e.term, e.cmd.to_vec())).collect(),
            term: self.term,
            vote: self.voted_for,
            snapshot: self.snapshot.as_deref().cloned(),
        };
        let (rng, every) = (self.rng, self.snapshot_every);
        let mut fresh = Core::new(self.me, self.bootstrap.clone(), None, loaded, now).expect("restart");
        fresh.rng = rng;
        fresh.snapshot_every = every;
        fresh.reset_deadline(now);
        *self = fresh;
    }
}

// ----------------------------------------------------------- state machine

/// How long a stream keeps its entries. Zero means "no limit" and, carried
/// by an append, "leave the stream's current policy alone".
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Retention {
    pub keep_last: u64,
    pub max_age_ms: u64,
}

/// The body of an append: `parts[name, client, seq, keep_last, max_age_ms, payload...]`.
pub(crate) fn append_body(name: &str, client: u64, seq: u64, r: Retention, payloads: &[&[u8]]) -> Vec<u8> {
    let nums = [client, seq, r.keep_last, r.max_age_ms].map(u64::to_be_bytes);
    let mut pieces: Vec<&[u8]> = vec![name.as_bytes()];
    pieces.extend(nums.iter().map(|n| &n[..]));
    pieces.extend_from_slice(payloads);
    parts(&pieces)
}

/// Validate an append body from the wire and turn it into a log command,
/// stamped with the leader's clock so every replica expires it alike.
pub(crate) fn append_command(body: &[u8], now_ms: u64) -> Result<Bytes> {
    if body.len() > MAX_ENTRY {
        return Err(format!("append of {} bytes exceeds {MAX_ENTRY}", body.len()).into());
    }
    let mut b = Blobs::new(body);
    b.text()?;
    for _ in 0..4 {
        b.u64()?;
    }
    if b.is_empty() {
        return Err("append without a payload".into());
    }
    while !b.is_empty() {
        b.take()?;
    }
    Ok(Bytes::from([&[APPEND][..], &now_ms.to_be_bytes(), body].concat()))
}

pub(crate) fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Drop every entry of `name` before `before`.
pub(crate) fn trim_command(name: &str, before: u64) -> Bytes {
    Bytes::from([&[TRIM][..], &parts(&[name.as_bytes(), &before.to_be_bytes()])].concat())
}

pub(crate) fn encode_config(c: &[Member]) -> Vec<u8> {
    let rows: Vec<(String, String, [u8; 1])> = c
        .iter()
        .map(|m| (m.addr.to_string(), m.name.clone().unwrap_or_default(), [m.voter as u8]))
        .collect();
    let pieces: Vec<&[u8]> = rows
        .iter()
        .flat_map(|(a, n, v)| [a.as_bytes(), n.as_bytes(), &v[..]])
        .collect();
    parts(&pieces)
}

pub(crate) fn decode_config(b: &[u8]) -> Result<Vec<Member>> {
    let mut b = Blobs::new(b);
    let mut out = Vec::new();
    while !b.is_empty() {
        let addr = b.text()?.parse()?;
        let name = b.text()?;
        let voter = b.take()? == [1];
        out.push(Member {
            addr,
            name: (!name.is_empty()).then_some(name),
            voter,
        });
    }
    Ok(out)
}

#[derive(Clone, Default)]
struct StreamLog {
    /// Index of entries[0]: entries before it were trimmed.
    base: u64,
    /// (leader's wall clock in ms, payload)
    entries: VecDeque<(u64, Bytes)>,
    policy: Retention,
}

impl StreamLog {
    fn drop_front(&mut self, n: usize) {
        let n = n.min(self.entries.len());
        self.entries.drain(..n);
        self.base += n as u64;
    }

    /// Entries older than `max_age` before `now_ms`, and beyond `keep_last`.
    fn excess(&self, now_ms: u64) -> usize {
        let mut n = 0;
        if self.policy.keep_last > 0 {
            n = self.entries.len().saturating_sub(self.policy.keep_last as usize);
        }
        if self.policy.max_age_ms > 0 {
            let cutoff = now_ms.saturating_sub(self.policy.max_age_ms);
            n = n.max(self.entries.iter().take_while(|(ts, _)| *ts < cutoff).count());
        }
        n
    }
}

/// The replicated state: named streams, which (client, seq) already landed
/// so a retried append applies once, and the committed membership.
#[derive(Clone, Default)]
pub(crate) struct Machine {
    streams: HashMap<String, StreamLog>,
    seen: HashMap<(u64, u64), u64>,
    order: VecDeque<(u64, u64)>,
    pub config: Vec<Member>,
}

fn put(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_be_bytes());
    out.extend_from_slice(b);
}

impl Machine {
    /// Returns the stream index of the first payload.
    fn apply(&mut self, cmd: &Bytes) -> u64 {
        let body = cmd.slice(1.min(cmd.len())..);
        match cmd.first() {
            Some(&APPEND) => self.append(&body),
            Some(&CONFIG) => {
                if let Ok(c) = decode_config(&body) {
                    self.config = c;
                }
                0
            }
            Some(&TRIM) => {
                let mut b = Blobs::new(&body);
                if let (Ok(name), Ok(before)) = (b.text(), b.u64()) {
                    if let Some(s) = self.streams.get_mut(&name) {
                        let n = before.saturating_sub(s.base) as usize;
                        s.drop_front(n);
                    }
                }
                0
            }
            _ => 0,
        }
    }

    fn append(&mut self, cmd: &Bytes) -> u64 {
        let Some(ts) = cmd.get(..8).map(|t| u64::from_be_bytes(t.try_into().unwrap())) else {
            return 0;
        };
        let body = cmd.slice(8..);
        let mut b = Blobs::new(&body);
        let (Ok(name), Ok(client), Ok(seq), Ok(keep_last), Ok(max_age_ms)) =
            (b.text(), b.u64(), b.u64(), b.u64(), b.u64())
        else {
            return 0;
        };
        if client != 0 {
            if let Some(first) = self.seen.get(&(client, seq)) {
                return *first;
            }
        }
        let s = self.streams.entry(name).or_default();
        let first = s.base + s.entries.len() as u64;
        while let Ok(p) = b.take() {
            s.entries.push_back((ts, body.slice_ref(p)));
        }
        if keep_last > 0 {
            s.policy.keep_last = keep_last;
        }
        if max_age_ms > 0 {
            s.policy.max_age_ms = max_age_ms;
        }
        let excess = s.excess(ts);
        s.drop_front(excess);
        if client != 0 {
            // ponytail: a fixed window of remembered writes; a retry older than
            // SEEN_CAP appends would apply twice. Expire per client if needed.
            self.seen.insert((client, seq), first);
            self.order.push_back((client, seq));
            if self.order.len() > SEEN_CAP {
                if let Some(old) = self.order.pop_front() {
                    self.seen.remove(&old);
                }
            }
        }
        first
    }

    pub fn len(&self, name: &str) -> u64 {
        self.streams
            .get(name)
            .map_or(0, |s| s.base + s.entries.len() as u64)
    }

    /// The first index still held: everything before it was trimmed.
    pub fn first(&self, name: &str) -> u64 {
        self.streams.get(name).map_or(0, |s| s.base)
    }

    /// Up to `max` entries from `from` (or from the first one still held).
    pub fn since(&self, name: &str, from: u64, max: usize) -> (u64, Vec<Bytes>) {
        match self.streams.get(name) {
            Some(s) => {
                let start = from.max(s.base);
                let skip = (start - s.base) as usize;
                let out = s.entries.iter().skip(skip).take(max).map(|(_, e)| e.clone()).collect();
                (start, out)
            }
            None => (from, Vec::new()),
        }
    }

    /// Streams with entries past their age limit at `now_ms`: (name, first to keep).
    pub fn expired(&self, now_ms: u64) -> Vec<(String, u64)> {
        self.streams
            .iter()
            .filter(|(_, s)| s.policy.max_age_ms > 0)
            .filter_map(|(n, s)| {
                let k = s.excess(now_ms);
                (k > 0).then(|| (n.clone(), s.base + k as u64))
            })
            .collect()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put(&mut out, b"v1");
        put(&mut out, &encode_config(&self.config));
        put(&mut out, &(self.streams.len() as u64).to_be_bytes());
        let mut names: Vec<&String> = self.streams.keys().collect();
        names.sort();
        for name in names {
            let s = &self.streams[name];
            put(&mut out, name.as_bytes());
            for n in [s.base, s.policy.keep_last, s.policy.max_age_ms, s.entries.len() as u64] {
                put(&mut out, &n.to_be_bytes());
            }
            for (ts, e) in &s.entries {
                put(&mut out, &ts.to_be_bytes());
                put(&mut out, e);
            }
        }
        let mut seen = Vec::with_capacity(self.order.len() * 24);
        for key in &self.order {
            if let Some(first) = self.seen.get(key) {
                for v in [key.0, key.1, *first] {
                    seen.extend_from_slice(&v.to_be_bytes());
                }
            }
        }
        put(&mut out, &seen);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let bytes = Bytes::copy_from_slice(bytes);
        let mut b = Blobs::new(&bytes);
        if b.take()? != b"v1" {
            return Err("unknown snapshot format".into());
        }
        let mut m = Machine {
            config: decode_config(b.take()?)?,
            ..Machine::default()
        };
        for _ in 0..b.u64()? {
            let name = b.text()?;
            let base = b.u64()?;
            let policy = Retention {
                keep_last: b.u64()?,
                max_age_ms: b.u64()?,
            };
            let n = b.u64()?;
            let mut entries = VecDeque::with_capacity(n.min(1 << 20) as usize);
            for _ in 0..n {
                let ts = b.u64()?;
                entries.push_back((ts, bytes.slice_ref(b.take()?)));
            }
            m.streams.insert(name, StreamLog { base, entries, policy });
        }
        for row in b.take()?.as_chunks::<24>().0 {
            let v = |i: usize| u64::from_be_bytes(row[i * 8..i * 8 + 8].try_into().unwrap());
            m.seen.insert((v(0), v(1)), v(2));
            m.order.push_back((v(0), v(1)));
        }
        Ok(m)
    }
}

// ---------------------------------------------------------------- messages

#[derive(Clone, Debug)]
pub(crate) struct AppendReq {
    pub term: u64,
    pub leader: SocketAddr,
    pub prev_index: u64,
    pub prev_term: u64,
    pub commit: u64,
    pub entries: Vec<Entry>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AppendResp {
    pub term: u64,
    pub ok: bool,
    /// ok: the last index that now matches. Not ok: where to retry from.
    pub index: u64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct VoteReq {
    pub term: u64,
    pub candidate: SocketAddr,
    pub last_index: u64,
    pub last_term: u64,
    pub pre: bool,
    /// Sent after a leadership transfer: voters skip the stickiness check.
    pub force: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct VoteResp {
    pub term: u64,
    pub granted: bool,
}

/// One piece of a snapshot. The last one has offset + data.len() == total.
#[derive(Clone, Debug)]
pub(crate) struct SnapChunk {
    pub term: u64,
    pub leader: SocketAddr,
    pub index: u64,
    pub snap_term: u64,
    pub offset: u64,
    pub total: u64,
    pub data: Bytes,
}

fn addr(b: &mut Blobs) -> Result<SocketAddr> {
    Ok(b.text()?.parse()?)
}

impl AppendReq {
    pub fn encode(&self) -> Vec<u8> {
        let leader = self.leader.to_string();
        let head = [
            self.term,
            self.prev_index,
            self.prev_term,
            self.commit,
            self.entries.len() as u64,
        ]
        .map(u64::to_be_bytes);
        let terms: Vec<[u8; 8]> = self.entries.iter().map(|e| e.term.to_be_bytes()).collect();
        let mut pieces: Vec<&[u8]> = vec![leader.as_bytes()];
        pieces.extend(head.iter().map(|h| &h[..]));
        for (e, t) in self.entries.iter().zip(&terms) {
            pieces.push(t);
            pieces.push(&e.cmd);
        }
        parts(&pieces)
    }

    pub fn decode(body: &Bytes) -> Result<Self> {
        let mut b = Blobs::new(body);
        let leader = addr(&mut b)?;
        let term = b.u64()?;
        let prev_index = b.u64()?;
        let prev_term = b.u64()?;
        let commit = b.u64()?;
        let n = b.u64()?;
        let mut entries = Vec::with_capacity(n.min(MAX_BATCH_ENTRIES as u64) as usize);
        for _ in 0..n {
            let term = b.u64()?;
            entries.push(Entry {
                term,
                cmd: body.slice_ref(b.take()?),
            });
        }
        Ok(Self {
            term,
            leader,
            prev_index,
            prev_term,
            commit,
            entries,
        })
    }
}

impl AppendResp {
    pub fn encode(&self) -> Vec<u8> {
        parts(&[
            &self.term.to_be_bytes(),
            &[self.ok as u8],
            &self.index.to_be_bytes(),
        ])
    }

    pub fn decode(body: &[u8]) -> Result<Self> {
        let mut b = Blobs::new(body);
        Ok(Self {
            term: b.u64()?,
            ok: b.take()? == [1],
            index: b.u64()?,
        })
    }
}

impl VoteReq {
    pub fn encode(&self) -> Vec<u8> {
        parts(&[
            &self.term.to_be_bytes(),
            self.candidate.to_string().as_bytes(),
            &self.last_index.to_be_bytes(),
            &self.last_term.to_be_bytes(),
            &[self.pre as u8],
            &[self.force as u8],
        ])
    }

    pub fn decode(body: &[u8]) -> Result<Self> {
        let mut b = Blobs::new(body);
        Ok(Self {
            term: b.u64()?,
            candidate: addr(&mut b)?,
            last_index: b.u64()?,
            last_term: b.u64()?,
            pre: b.take()? == [1],
            force: b.take()? == [1],
        })
    }
}

impl VoteResp {
    pub fn encode(&self) -> Vec<u8> {
        parts(&[&self.term.to_be_bytes(), &[self.granted as u8]])
    }

    pub fn decode(body: &[u8]) -> Result<Self> {
        let mut b = Blobs::new(body);
        Ok(Self {
            term: b.u64()?,
            granted: b.take()? == [1],
        })
    }
}

impl SnapChunk {
    pub fn encode(&self) -> Vec<u8> {
        let leader = self.leader.to_string();
        let nums = [self.term, self.index, self.snap_term, self.offset, self.total].map(u64::to_be_bytes);
        let mut pieces: Vec<&[u8]> = vec![leader.as_bytes()];
        pieces.extend(nums.iter().map(|n| &n[..]));
        pieces.push(&self.data);
        parts(&pieces)
    }

    pub fn decode(body: &Bytes) -> Result<Self> {
        let mut b = Blobs::new(body);
        Ok(Self {
            leader: addr(&mut b)?,
            term: b.u64()?,
            index: b.u64()?,
            snap_term: b.u64()?,
            offset: b.u64()?,
            total: b.u64()?,
            data: body.slice_ref(b.take()?),
        })
    }
}

#[cfg(test)]
mod tests;
