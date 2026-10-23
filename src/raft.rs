//! Raft (Ongaro and Ousterhout, 2014) with PreVote, CheckQuorum, ReadIndex and
//! client sessions (Ongaro's thesis, sections 9.6, 6.2, 6.4 and 6.3).
//!
//! Pure state: no sockets and no tasks. node.rs moves the messages; this
//! decides. Time is passed in, so the simulation in raft/tests.rs can drive it.
//!
//! The replicated state machine is the set of named streams. One log entry is
//! one append of one or more payloads to one stream, so a batch is atomic and
//! contiguous, and every node assigns the same stream indices.

use crate::transport::{parts, Blobs};
use crate::wal::{Loaded, Wal};
use crate::Result;
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, VecDeque};
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
const MAX_BATCH_ENTRIES: usize = 1024;
const MAX_BATCH_BYTES: usize = 4 << 20;
/// Remembered (client, seq) pairs for exactly-once retries.
const SEEN_CAP: usize = 100_000;

const NOOP: u8 = 0;
const APPEND: u8 = 1;

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

pub(crate) struct Progress {
    pub next: u64,
    pub matched: u64,
    pub sent_commit: u64,
    /// When the last reply arrived (CheckQuorum).
    pub last_ack: Instant,
    /// When the newest acknowledged request was sent (ReadIndex).
    pub acked_sent: Option<Instant>,
}

/// The stream index of the first payload, or why the write did not land.
pub(crate) type Outcome = std::result::Result<u64, String>;

pub(crate) struct Core {
    pub me: SocketAddr,
    pub peers: Vec<SocketAddr>,
    pub term: u64,
    pub voted_for: Option<SocketAddr>,
    log: Vec<Entry>,
    pub commit: u64,
    pub applied: u64,
    /// Highest index known to be on this node's disk.
    pub durable: u64,
    pub role: Role,
    pub leader: Option<SocketAddr>,
    pub last_contact: Option<Instant>,
    pub deadline: Instant,
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
    /// Set on a disk failure or shutdown: from then on the node acknowledges
    /// nothing, because what it says can no longer be trusted.
    pub io_error: Option<String>,
    /// xorshift state for election jitter; seeded per node, settable in tests.
    pub rng: u64,
}

type StateWrite = Box<dyn FnOnce() -> std::io::Result<()> + Send>;

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

impl Core {
    pub fn new(
        me: SocketAddr,
        peers: Vec<SocketAddr>,
        disk: Option<(Wal, Loaded)>,
        now: Instant,
    ) -> Self {
        let (wal, loaded) = match disk {
            Some((w, l)) => (Some(w), l),
            None => (None, Loaded::default()),
        };
        let log: Vec<Entry> = loaded
            .entries
            .into_iter()
            .map(|(term, cmd)| Entry {
                term,
                cmd: Bytes::from(cmd),
            })
            .collect();
        Self {
            me,
            peers,
            term: loaded.term,
            voted_for: loaded.vote,
            durable: log.len() as u64,
            log,
            commit: 0,
            applied: 0,
            role: Role::Follower,
            leader: None,
            last_contact: None,
            deadline: now,
            term_start: 0,
            progress: HashMap::new(),
            waiters: BTreeMap::new(),
            machine: Machine::default(),
            wal,
            state_seq: 0,
            state_issued: 0,
            state_synced: 0,
            io_error: None,
            rng: random_u64(),
        }
        .with_deadline(now)
    }

    fn with_deadline(mut self, now: Instant) -> Self {
        self.reset_deadline(now);
        self
    }

    pub fn last_index(&self) -> u64 {
        self.log.len() as u64
    }

    pub fn last_term(&self) -> u64 {
        self.log.last().map_or(0, |e| e.term)
    }

    pub fn term_at(&self, i: u64) -> Option<u64> {
        if i == 0 {
            return Some(0);
        }
        self.log.get(i as usize - 1).map(|e| e.term)
    }

    #[cfg(test)]
    pub fn entry(&self, i: u64) -> Option<&Entry> {
        self.log.get((i as usize).checked_sub(1)?)
    }

    pub fn quorum(&self) -> usize {
        self.peers.len().div_ceil(2) + 1
    }

    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    /// Randomized in [ELECTION_MIN, 2 * ELECTION_MIN), so nodes rarely split votes.
    pub fn reset_deadline(&mut self, now: Instant) {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        let ms = ELECTION_MIN.as_millis() as u64;
        self.deadline = now + ELECTION_MIN + Duration::from_millis(x % ms);
    }

    /// Heard from a live leader recently, so votes for anyone else would only
    /// disrupt it (thesis 4.2.3).
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
        self.log.push(e);
        if self.wal.is_none() {
            self.durable = self.last_index();
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
        self.log.truncate((from - 1) as usize);
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
        self.role = Role::Leader;
        self.leader = Some(self.me);
        let next = self.last_index() + 1;
        self.progress = self
            .peers
            .iter()
            .map(|p| {
                let p0 = Progress {
                    next,
                    matched: 0,
                    sent_commit: 0,
                    last_ack: now,
                    acked_sent: None,
                };
                (*p, p0)
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
        if self.role != Role::Leader || self.io_error.is_some() {
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

    /// Highest index on a majority, counted only if it is from this term.
    pub fn advance_commit(&mut self) -> bool {
        if self.role != Role::Leader {
            return false;
        }
        let mut m: Vec<u64> = self.progress.values().map(|p| p.matched).collect();
        m.push(self.durable);
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
            let e = self.log[(i - 1) as usize].clone();
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
        }
        any
    }

    // ------------------------------------------------------------ follower

    pub fn handle_append(&mut self, req: AppendReq, now: Instant) -> AppendResp {
        let fail = |term, index| AppendResp {
            term,
            ok: false,
            index,
        };
        if self.io_error.is_some() || req.term < self.term {
            return fail(self.term, 0);
        }
        self.become_follower(req.term, Some(req.leader));
        self.last_contact = Some(now);
        self.reset_deadline(now);

        if req.prev_index > self.last_index() {
            return fail(self.term, self.last_index() + 1);
        }
        if let Some(t) = self.term_at(req.prev_index).filter(|t| *t != req.prev_term) {
            if req.prev_index <= self.commit {
                self.stop(format!(
                    "log diverged below the commit index at {}",
                    req.prev_index
                ));
                return fail(self.term, 0);
            }
            // Skip back over the whole conflicting term in one round trip.
            let mut i = req.prev_index;
            while i > self.commit + 1 && self.term_at(i - 1) == Some(t) {
                i -= 1;
            }
            return fail(self.term, i);
        }
        let mut idx = req.prev_index;
        for e in req.entries {
            idx += 1;
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

    pub fn handle_vote(&mut self, req: VoteReq, now: Instant) -> VoteResp {
        let up_to_date = (req.last_term, req.last_index) >= (self.last_term(), self.last_index());
        let deny = |term| VoteResp {
            term,
            granted: false,
        };
        if self.io_error.is_some() || self.sticky(now) {
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

    pub fn vote_req(&self, pre: bool) -> VoteReq {
        VoteReq {
            term: if pre { self.term + 1 } else { self.term },
            candidate: self.me,
            last_index: self.last_index(),
            last_term: self.last_term(),
            pre,
        }
    }

    // -------------------------------------------------------------- leader

    pub fn append_req(&self, peer: SocketAddr) -> Option<AppendReq> {
        if self.role != Role::Leader {
            return None;
        }
        let p = self.progress.get(&peer)?;
        let prev = (p.next - 1).min(self.last_index());
        let prev_term = self.term_at(prev)?;
        let mut entries = Vec::new();
        let mut bytes = 0;
        for e in &self.log[prev as usize..] {
            if entries.len() >= MAX_BATCH_ENTRIES
                || (!entries.is_empty() && bytes + e.cmd.len() > MAX_BATCH_BYTES)
            {
                break;
            }
            bytes += e.cmd.len();
            entries.push(e.clone());
        }
        Some(AppendReq {
            term: self.term,
            leader: self.me,
            prev_index: prev,
            prev_term,
            commit: self.commit,
            entries,
        })
    }

    pub fn has_work(&self, peer: SocketAddr) -> bool {
        self.progress
            .get(&peer)
            .is_some_and(|p| p.next <= self.last_index() || p.sent_commit < self.commit)
    }

    /// A majority answered requests sent at or after `t`: nobody else led then.
    pub fn confirmed_since(&self, t: Instant) -> bool {
        let fresh = self
            .progress
            .values()
            .filter(|p| p.acked_sent.is_some_and(|s| s >= t))
            .count();
        1 + fresh >= self.quorum()
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
        if resp.term > self.term {
            self.become_follower(resp.term, None);
            self.reset_deadline(now);
            return false;
        }
        if self.role != Role::Leader || self.term != sent.term {
            return false;
        }
        let Some(p) = self.progress.get_mut(&peer) else {
            return false;
        };
        p.last_ack = now;
        p.acked_sent = p.acked_sent.max(Some(sent_at));
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

    /// A leader that cannot reach a majority steps down, so a partitioned
    /// leader stops accepting writes it can never commit.
    pub fn check_quorum(&mut self, now: Instant) -> bool {
        if self.role != Role::Leader || self.peers.is_empty() {
            return false;
        }
        let live = 1 + self
            .progress
            .values()
            .filter(|p| now.saturating_duration_since(p.last_ack) < ELECTION_MIN)
            .count();
        if live < self.quorum() {
            let term = self.term;
            self.become_follower(term, None);
            self.reset_deadline(now);
            return true;
        }
        false
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

    pub fn release_disk(&mut self) {
        self.wal = None;
    }

    #[cfg(test)]
    pub fn crash_and_restart(&mut self, now: Instant) {
        self.fail_waiters("crashed");
        let entries = self.log.iter().map(|e| (e.term, e.cmd.to_vec())).collect();
        let loaded = Loaded {
            entries,
            term: self.term,
            vote: self.voted_for,
        };
        let (peers, rng) = (self.peers.clone(), self.rng);
        *self = Core::new(self.me, peers, None, now);
        self.rng = rng;
        self.reset_deadline(now);
        self.log = loaded
            .entries
            .into_iter()
            .map(|(term, cmd)| Entry {
                term,
                cmd: Bytes::from(cmd),
            })
            .collect();
        self.durable = self.last_index();
        self.term = loaded.term;
        self.voted_for = loaded.vote;
    }
}

// ----------------------------------------------------------- state machine

/// The body of an append: `parts[name, client, seq, payload...]`.
pub(crate) fn append_body(name: &str, client: u64, seq: u64, payloads: &[&[u8]]) -> Vec<u8> {
    let (c, s) = (client.to_be_bytes(), seq.to_be_bytes());
    let mut pieces: Vec<&[u8]> = vec![name.as_bytes(), &c, &s];
    pieces.extend_from_slice(payloads);
    parts(&pieces)
}

/// Validate an append body from the wire and turn it into a log command.
pub(crate) fn append_command(body: &[u8]) -> Result<Bytes> {
    if body.len() > MAX_ENTRY {
        return Err(format!("append of {} bytes exceeds {MAX_ENTRY}", body.len()).into());
    }
    let mut b = Blobs::new(body);
    b.text()?;
    b.u64()?;
    b.u64()?;
    if b.is_empty() {
        return Err("append without a payload".into());
    }
    while !b.is_empty() {
        b.take()?;
    }
    let mut out = Vec::with_capacity(1 + body.len());
    out.push(APPEND);
    out.extend_from_slice(body);
    Ok(Bytes::from(out))
}

/// The replicated state: named streams, plus which (client, seq) already
/// landed, so a retried append is applied once.
#[derive(Default)]
pub(crate) struct Machine {
    streams: HashMap<String, Vec<Bytes>>,
    seen: HashMap<(u64, u64), u64>,
    order: VecDeque<(u64, u64)>,
}

impl Machine {
    /// Returns the stream index of the first payload.
    fn apply(&mut self, cmd: &Bytes) -> u64 {
        if cmd.first() != Some(&APPEND) {
            return 0;
        }
        let body = cmd.slice(1..);
        let mut b = Blobs::new(&body);
        let (Ok(name), Ok(client), Ok(seq)) = (b.text(), b.u64(), b.u64()) else {
            return 0;
        };
        if client != 0 {
            if let Some(first) = self.seen.get(&(client, seq)) {
                return *first;
            }
        }
        let s = self.streams.entry(name).or_default();
        let first = s.len() as u64;
        while let Ok(p) = b.take() {
            s.push(body.slice_ref(p));
        }
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
        self.streams.get(name).map_or(0, |s| s.len() as u64)
    }

    pub fn since(&self, name: &str, from: u64, max: usize) -> Vec<Bytes> {
        match self.streams.get(name) {
            Some(s) if (from as usize) < s.len() => {
                s[from as usize..].iter().take(max).cloned().collect()
            }
            _ => Vec::new(),
        }
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
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct VoteResp {
    pub term: u64,
    pub granted: bool,
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

#[cfg(test)]
mod tests;
