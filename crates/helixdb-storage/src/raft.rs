use crate::{DbError, Result};

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub heartbeat_interval: Duration,
    pub rpc_timeout: Duration,
    pub snapshot_threshold_entries: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: Duration::from_millis(200),
            election_timeout_max: Duration::from_millis(400),
            heartbeat_interval: Duration::from_millis(50),
            rpc_timeout: Duration::from_millis(200),
            snapshot_threshold_entries: 64,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftNodeState {
    pub id: u64,
    pub role: RaftRole,
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub leader_id: Option<u64>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub snapshot_index: u64,
    pub snapshot_term: u64,
    pub log_len: usize,
    pub kv_len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Command {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LogEntry {
    index: u64,
    term: u64,
    command: Command,
}

#[derive(Debug)]
struct NodeSpec {
    id: u64,
    dir: PathBuf,
}

#[derive(Debug)]
struct NodeHandle {
    state: Arc<Mutex<NodeState>>,
    tx: Sender<Rpc>,
    join: Option<JoinHandle<()>>,
}

#[derive(Debug, Default)]
struct ClusterBus {
    senders: Mutex<HashMap<u64, Sender<Rpc>>>,
}

impl ClusterBus {
    fn register(&self, id: u64, tx: Sender<Rpc>) {
        self.senders.lock().unwrap().insert(id, tx);
    }

    fn remove(&self, id: u64) {
        self.senders.lock().unwrap().remove(&id);
    }

    fn send(&self, id: u64, rpc: Rpc) -> bool {
        let sender = self.senders.lock().unwrap().get(&id).cloned();
        match sender {
            Some(sender) => sender.send(rpc).is_ok(),
            None => false,
        }
    }
}

#[derive(Debug)]
struct NodeState {
    id: u64,
    dir: PathBuf,
    config: RaftConfig,
    peers: Vec<u64>,
    current_term: u64,
    voted_for: Option<u64>,
    role: RaftRole,
    leader_id: Option<u64>,
    log: Vec<LogEntry>,
    commit_index: u64,
    last_applied: u64,
    snapshot_index: u64,
    snapshot_term: u64,
    kv: BTreeMap<Vec<u8>, Vec<u8>>,
    next_index: BTreeMap<u64, u64>,
    match_index: BTreeMap<u64, u64>,
    election_deadline: Instant,
    heartbeat_due: Instant,
}

#[derive(Debug)]
enum Rpc {
    RequestVote {
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
        respond_to: Sender<VoteResponse>,
    },
    AppendEntries {
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
        respond_to: Sender<AppendResponse>,
    },
    InstallSnapshot {
        term: u64,
        leader_id: u64,
        last_included_index: u64,
        last_included_term: u64,
        snapshot: SnapshotData,
        respond_to: Sender<InstallSnapshotResponse>,
    },
    Propose {
        command: Command,
        respond_to: Sender<ProposeResponse>,
    },
    Shutdown,
}

#[derive(Debug)]
struct VoteResponse {
    term: u64,
    vote_granted: bool,
}

#[derive(Debug)]
struct AppendResponse {
    term: u64,
    success: bool,
    match_index: u64,
}

#[derive(Debug, Clone)]
struct InstallSnapshotResponse {
    term: u64,
    success: bool,
    last_included_index: u64,
}

#[derive(Debug)]
struct ProposeResponse {
    term: u64,
    success: bool,
    index: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SnapshotData {
    key_values: Vec<(Vec<u8>, Vec<u8>)>,
}

enum ReplicationAction {
    Append {
        prev_index: u64,
        prev_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    InstallSnapshot {
        snapshot: SnapshotData,
        last_included_index: u64,
        last_included_term: u64,
        leader_commit: u64,
    },
}

pub struct RaftCluster {
    inner: Mutex<ClusterInner>,
}

#[derive(Debug)]
struct ClusterInner {
    root: PathBuf,
    config: RaftConfig,
    bus: Arc<ClusterBus>,
    specs: BTreeMap<u64, NodeSpec>,
    nodes: BTreeMap<u64, NodeHandle>,
}

impl RaftCluster {
    pub fn bootstrap(root: impl AsRef<Path>, node_count: usize) -> Result<Self> {
        Self::bootstrap_with_config(root, node_count, RaftConfig::default())
    }

    pub fn bootstrap_with_config(
        root: impl AsRef<Path>,
        node_count: usize,
        config: RaftConfig,
    ) -> Result<Self> {
        if node_count == 0 {
            return Err(DbError::Corrupt(
                "raft cluster requires at least one node".into(),
            ));
        }

        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;

        let bus = Arc::new(ClusterBus::default());
        let mut specs = BTreeMap::new();
        let mut receivers = BTreeMap::new();

        for id in 1..=node_count as u64 {
            let dir = root.join(format!("node-{id:02}"));
            fs::create_dir_all(&dir)?;
            specs.insert(id, NodeSpec { id, dir });
            let (tx, rx) = mpsc::channel();
            bus.register(id, tx.clone());
            receivers.insert(id, (tx, rx));
        }

        let mut nodes = BTreeMap::new();
        for id in 1..=node_count as u64 {
            let spec = specs.get(&id).unwrap();
            let peers = specs
                .keys()
                .copied()
                .filter(|peer| *peer != id)
                .collect::<Vec<_>>();
            let (tx, rx) = receivers.remove(&id).expect("receiver");
            let state = Arc::new(Mutex::new(NodeState::open(
                spec.id,
                spec.dir.clone(),
                peers,
                config.clone(),
            )?));
            let join = spawn_node_thread(
                id,
                spec.dir.clone(),
                config.clone(),
                state.clone(),
                rx,
                bus.clone(),
            );
            nodes.insert(
                id,
                NodeHandle {
                    state,
                    tx,
                    join: Some(join),
                },
            );
        }

        Ok(Self {
            inner: Mutex::new(ClusterInner {
                root,
                config,
                bus,
                specs,
                nodes,
            }),
        })
    }

    pub fn leader_id(&self) -> Option<u64> {
        let inner = self.inner.lock().unwrap();
        inner
            .nodes
            .iter()
            .filter_map(|(id, handle)| {
                let state = handle.state.lock().unwrap();
                (state.role == RaftRole::Leader).then_some(*id)
            })
            .next()
    }

    pub fn wait_for_leader(&self, timeout: Duration) -> Result<u64> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(id) = self.leader_id() {
                return Ok(id);
            }
            if Instant::now() >= deadline {
                return Err(DbError::Corrupt(
                    "no raft leader elected within timeout".into(),
                ));
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    pub fn put(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<u64> {
        self.propose_with_retry(Command::Put(key.into(), value.into()))
    }

    pub fn delete(&self, key: impl Into<Vec<u8>>) -> Result<u64> {
        self.propose_with_retry(Command::Delete(key.into()))
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let leader = self.wait_for_leader(Duration::from_secs(5))?;
        let inner = self.inner.lock().unwrap();
        let handle = inner
            .nodes
            .get(&leader)
            .ok_or_else(|| DbError::Corrupt("raft leader missing".into()))?;
        let state = handle.state.lock().unwrap();
        Ok(state.kv.get(key.as_ref()).cloned())
    }

    pub fn all_entries(&self) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        let inner = self.inner.lock().unwrap();
        if let Some((_, handle)) = inner.nodes.iter().next() {
            let state = handle.state.lock().unwrap();
            return Ok(state.kv.clone());
        }

        Ok(BTreeMap::new())
    }

    pub fn node_state(&self, id: u64) -> Option<RaftNodeState> {
        let inner = self.inner.lock().unwrap();
        inner.nodes.get(&id).map(|handle| {
            let state = handle.state.lock().unwrap();
            state.snapshot()
        })
    }

    pub fn kill_node(&self, id: u64) -> Result<()> {
        let handle = {
            let mut inner = self.inner.lock().unwrap();
            inner.bus.remove(id);
            inner.nodes.remove(&id)
        };

        let Some(mut handle) = handle else {
            return Err(DbError::Corrupt(format!("raft node {id} not running")));
        };

        let _ = handle.tx.send(Rpc::Shutdown);
        if let Some(join) = handle.join.take() {
            let _ = join.join();
        }

        Ok(())
    }

    pub fn restart_node(&self, id: u64) -> Result<()> {
        let (spec, peers, config, bus) = {
            let inner = self.inner.lock().unwrap();
            if inner.nodes.contains_key(&id) {
                return Err(DbError::Corrupt(format!("raft node {id} already running")));
            }

            let spec = inner
                .specs
                .get(&id)
                .ok_or_else(|| DbError::Corrupt(format!("unknown raft node {id}")))?;
            let peers = inner
                .specs
                .keys()
                .copied()
                .filter(|peer| *peer != id)
                .collect::<Vec<_>>();
            (
                spec.dir.clone(),
                peers,
                inner.config.clone(),
                inner.bus.clone(),
            )
        };

        let (tx, rx) = mpsc::channel();
        bus.register(id, tx.clone());
        let state = Arc::new(Mutex::new(NodeState::open(
            id,
            spec.clone(),
            peers,
            config.clone(),
        )?));
        let join = spawn_node_thread(id, spec.clone(), config, state.clone(), rx, bus.clone());

        let mut inner = self.inner.lock().unwrap();
        inner.nodes.insert(
            id,
            NodeHandle {
                state,
                tx,
                join: Some(join),
            },
        );
        Ok(())
    }

    pub fn add_node(&self, id: u64) -> Result<()> {
        let (dir, peers, config, bus, snapshot, log, current_term, voted_for, commit_index) = {
            let inner = self.inner.lock().unwrap();
            if inner.nodes.contains_key(&id) || inner.specs.contains_key(&id) {
                return Err(DbError::Corrupt(format!("raft node {id} already exists")));
            }

            let leader_id = inner
                .nodes
                .iter()
                .find_map(|(node_id, handle)| {
                    let state = handle.state.lock().unwrap();
                    (state.role == RaftRole::Leader).then_some(*node_id)
                })
                .or_else(|| inner.nodes.keys().copied().next())
                .ok_or_else(|| DbError::Corrupt("raft cluster is empty".into()))?;

            let leader_handle = inner
                .nodes
                .get(&leader_id)
                .ok_or_else(|| DbError::Corrupt("raft leader missing".into()))?;
            let leader_state = leader_handle.state.lock().unwrap();
            let dir = inner.root.join(format!("node-{id:02}"));
            let peers = inner
                .nodes
                .keys()
                .copied()
                .filter(|peer| *peer != id)
                .collect::<Vec<_>>();
            let snapshot = SnapshotState {
                last_included_index: leader_state.snapshot_index,
                last_included_term: leader_state.snapshot_term,
                key_values: leader_state.kv.clone(),
            };
            (
                dir,
                peers,
                inner.config.clone(),
                inner.bus.clone(),
                snapshot,
                leader_state.log.clone(),
                leader_state.current_term,
                leader_state.voted_for,
                leader_state.commit_index,
            )
        };

        fs::create_dir_all(&dir)?;
        persist_snapshot_state(
            &dir.join("raft.snapshot"),
            snapshot.last_included_index,
            snapshot.last_included_term,
            &snapshot.key_values,
        )?;
        persist_disk_state(&dir, current_term, voted_for, commit_index, &log)?;

        let (tx, rx) = mpsc::channel();
        bus.register(id, tx.clone());
        let state = Arc::new(Mutex::new(NodeState::open(
            id,
            dir.clone(),
            peers,
            config.clone(),
        )?));
        let join = spawn_node_thread(id, dir.clone(), config.clone(), state.clone(), rx, bus.clone());

        let mut inner = self.inner.lock().unwrap();
        inner.specs.insert(id, NodeSpec { id, dir });
        inner.nodes.insert(
            id,
            NodeHandle {
                state,
                tx,
                join: Some(join),
            },
        );
        Self::refresh_membership_locked(&mut inner, id);
        Ok(())
    }

    pub fn remove_node(&self, id: u64) -> Result<()> {
        let handle = {
            let mut inner = self.inner.lock().unwrap();
            inner.specs.remove(&id);
            inner.bus.remove(id);
            let handle = inner.nodes.remove(&id);
            Self::refresh_membership_locked(&mut inner, id);
            handle
        };

        let Some(mut handle) = handle else {
            return Err(DbError::Corrupt(format!("raft node {id} not running")));
        };

        let _ = handle.tx.send(Rpc::Shutdown);
        if let Some(join) = handle.join.take() {
            let _ = join.join();
        }

        Ok(())
    }

    fn propose_to(&self, leader_id: u64, command: Command) -> Result<u64> {
        let inner = self.inner.lock().unwrap();
        let handle = inner
            .nodes
            .get(&leader_id)
            .ok_or_else(|| DbError::Corrupt("raft leader missing".into()))?;
        let (respond_to, response_rx) = mpsc::channel();
        if !handle
            .tx
            .send(Rpc::Propose {
                command,
                respond_to,
            })
            .is_ok()
        {
            return Err(DbError::Corrupt("raft leader is unavailable".into()));
        }
        let response = response_rx
            .recv_timeout(inner.config.rpc_timeout)
            .map_err(|_| DbError::Corrupt("raft proposal timed out".into()))?;
        if response.success {
            response
                .index
                .ok_or_else(|| DbError::Corrupt("raft proposal missing index".into()))
        } else {
            Err(DbError::Corrupt(format!(
                "raft proposal rejected at term {}",
                response.term
            )))
        }
    }

    fn propose_with_retry(&self, command: Command) -> Result<u64> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut last_error: Option<DbError> = None;

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(last_error.unwrap_or_else(|| {
                    DbError::Corrupt("raft write timed out waiting for leader".into())
                }));
            }

            let leader = self.wait_for_leader(remaining.min(Duration::from_secs(1)))?;
            match self.propose_to(leader, command.clone()) {
                Ok(index) => return Ok(index),
                Err(err) => {
                    last_error = Some(err);
                    thread::sleep(Duration::from_millis(25));
                }
            }
        }
    }

    fn refresh_membership_locked(inner: &mut ClusterInner, changed_id: u64) {
        let active_ids = inner.nodes.keys().copied().collect::<Vec<_>>();
        for (id, handle) in inner.nodes.iter() {
            let mut state = handle.state.lock().unwrap();
            state.peers = active_ids
                .iter()
                .copied()
                .filter(|peer| *peer != *id)
                .collect();
            if state.role == RaftRole::Leader {
                let next = state.last_log_index().saturating_add(1);
                state.next_index.insert(changed_id, next);
                state.match_index.insert(changed_id, 0);
            } else {
                state.next_index.remove(&changed_id);
                state.match_index.remove(&changed_id);
            }
            let _ = state.persist();
        }
    }
}

impl Drop for RaftCluster {
    fn drop(&mut self) {
        let handles = {
            let mut inner = self.inner.lock().unwrap();
            inner.bus.senders.lock().unwrap().clear();
            std::mem::take(&mut inner.nodes)
                .into_iter()
                .map(|(_, handle)| handle)
                .collect::<Vec<_>>()
        };

        for mut handle in handles {
            let _ = handle.tx.send(Rpc::Shutdown);
            if let Some(join) = handle.join.take() {
                let _ = join.join();
            }
        }
    }
}

impl NodeState {
    fn open(id: u64, dir: PathBuf, peers: Vec<u64>, config: RaftConfig) -> Result<Self> {
        let disk = load_disk_state(&dir)?;
        let mut state = Self {
            id,
            dir,
            config,
            peers,
            current_term: disk.meta.current_term,
            voted_for: disk.meta.voted_for,
            role: RaftRole::Follower,
            leader_id: None,
            log: disk.log,
            commit_index: disk.meta.commit_index,
            last_applied: disk.snapshot.last_included_index,
            snapshot_index: disk.snapshot.last_included_index,
            snapshot_term: disk.snapshot.last_included_term,
            kv: disk.snapshot.key_values,
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            election_deadline: Instant::now(),
            heartbeat_due: Instant::now(),
        };

        if state.commit_index < state.snapshot_index {
            state.commit_index = state.snapshot_index;
        }
        let last_index = state.last_log_index();
        if state.commit_index > last_index {
            state.commit_index = last_index;
        }
        state.apply_committed_entries();
        state.reset_election_deadline();
        state.heartbeat_due = Instant::now() + state.config.heartbeat_interval;
        Ok(state)
    }

    fn snapshot(&self) -> RaftNodeState {
        RaftNodeState {
            id: self.id,
            role: self.role,
            current_term: self.current_term,
            voted_for: self.voted_for,
            leader_id: self.leader_id,
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            snapshot_index: self.snapshot_index,
            snapshot_term: self.snapshot_term,
            log_len: self.last_log_index().saturating_sub(self.snapshot_index) as usize,
            kv_len: self.kv.len(),
        }
    }

    fn majority(&self) -> usize {
        (self.peers.len() + 1) / 2 + 1
    }

    fn last_log_index(&self) -> u64 {
        if self.log.len() <= 1 {
            self.snapshot_index
        } else {
            self.log
                .last()
                .map(|entry| entry.index)
                .unwrap_or(self.snapshot_index)
        }
    }

    fn last_log_term(&self) -> u64 {
        if self.log.len() <= 1 {
            self.snapshot_term
        } else {
            self.log
                .last()
                .map(|entry| entry.term)
                .unwrap_or(self.snapshot_term)
        }
    }

    fn entry_at(&self, index: u64) -> Option<&LogEntry> {
        if index <= self.snapshot_index {
            return None;
        }

        self.log.iter().skip(1).find(|entry| entry.index == index)
    }

    fn reset_election_deadline(&mut self) {
        self.election_deadline =
            Instant::now() + randomized_timeout(self.id, self.current_term, &self.config);
    }

    fn clear_leader_tracking(&mut self) {
        self.next_index.clear();
        self.match_index.clear();
    }

    fn step_down(&mut self, term: u64, leader_id: Option<u64>) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
        self.role = RaftRole::Follower;
        self.leader_id = leader_id;
        self.clear_leader_tracking();
        self.reset_election_deadline();
    }

    fn become_candidate(&mut self) {
        self.role = RaftRole::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.clear_leader_tracking();
        self.reset_election_deadline();
    }

    fn become_leader(&mut self) {
        self.role = RaftRole::Leader;
        self.leader_id = Some(self.id);
        self.next_index.clear();
        self.match_index.clear();
        let next = self.last_log_index() + 1;
        for peer in self.peers.iter().copied() {
            self.next_index.insert(peer, next);
            self.match_index.insert(peer, 0);
        }
        self.heartbeat_due = Instant::now();
    }

    fn apply_committed_entries(&mut self) {
        while self.last_applied < self.commit_index {
            let next = self.last_applied + 1;
            if let Some(entry) = self.entry_at(next).cloned() {
                self.apply_entry(&entry);
                self.last_applied = next;
            } else {
                break;
            }
        }
    }

    fn apply_entry(&mut self, entry: &LogEntry) {
        match &entry.command {
            Command::Put(key, value) => {
                self.kv.insert(key.clone(), value.clone());
            }
            Command::Delete(key) => {
                self.kv.remove(key);
            }
        }
    }

    fn persist(&self) -> Result<()> {
        persist_disk_state(
            &self.dir,
            self.current_term,
            self.voted_for,
            self.commit_index,
            &self.log,
        )
    }

    fn persist_snapshot(&self) -> Result<()> {
        persist_snapshot_state(
            &self.dir.join("raft.snapshot"),
            self.snapshot_index,
            self.snapshot_term,
            &self.kv,
        )
    }

    fn maybe_snapshot(&mut self) -> Result<()> {
        let snapshot = self.commit_index;
        if snapshot <= self.snapshot_index {
            return Ok(());
        }

        let threshold = self.config.snapshot_threshold_entries.max(1) as u64;
        if self.last_log_index().saturating_sub(self.snapshot_index) < threshold {
            return Ok(());
        }

        self.create_snapshot(snapshot)
    }

    fn create_snapshot(&mut self, last_included_index: u64) -> Result<()> {
        if last_included_index <= self.snapshot_index {
            return Ok(());
        }

        let last_included_term = if last_included_index == 0 {
            0
        } else {
            self.entry_at(last_included_index)
                .map(|entry| entry.term)
                .unwrap_or(self.snapshot_term)
        };

        self.snapshot_index = last_included_index;
        self.snapshot_term = last_included_term;

        self.log = compact_log_entries(&self.log, self.snapshot_index);
        if self.last_applied < self.snapshot_index {
            self.last_applied = self.snapshot_index;
        }
        if self.commit_index < self.snapshot_index {
            self.commit_index = self.snapshot_index;
        }
        self.persist_snapshot()?;
        self.persist()?;
        Ok(())
    }

    fn install_snapshot(&mut self, snapshot: SnapshotState) -> Result<()> {
        if snapshot.last_included_index < self.snapshot_index {
            return Ok(());
        }

        let snapshot_path = self.dir.join("raft.snapshot");
        let snapshot_index = snapshot.last_included_index;
        let snapshot_term = snapshot.last_included_term;
        let snapshot_key_values = snapshot.key_values;
        self.snapshot_index = snapshot.last_included_index;
        self.snapshot_term = snapshot.last_included_term;
        self.kv = snapshot_key_values.clone();
        self.log = compact_log_entries(&self.log, self.snapshot_index);
        self.last_applied = self.snapshot_index;
        if self.commit_index < self.snapshot_index {
            self.commit_index = self.snapshot_index;
        }
        persist_snapshot_state(
            &snapshot_path,
            snapshot_index,
            snapshot_term,
            &snapshot_key_values,
        )?;
        self.persist()?;
        Ok(())
    }
}

fn spawn_node_thread(
    id: u64,
    dir: PathBuf,
    config: RaftConfig,
    state: Arc<Mutex<NodeState>>,
    rx: Receiver<Rpc>,
    bus: Arc<ClusterBus>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let _ = run_node(id, dir, config, state, rx, bus);
    })
}

fn run_node(
    id: u64,
    _dir: PathBuf,
    config: RaftConfig,
    state: Arc<Mutex<NodeState>>,
    rx: Receiver<Rpc>,
    bus: Arc<ClusterBus>,
) -> Result<()> {
    loop {
        let wait = {
            let state = state.lock().unwrap();
            if state.role == RaftRole::Leader {
                state
                    .heartbeat_due
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(50))
            } else {
                state
                    .election_deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(50))
            }
        };

        match rx.recv_timeout(wait) {
            Ok(Rpc::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
                respond_to,
            }) => {
                let response =
                    handle_request_vote(&state, term, candidate_id, last_log_index, last_log_term)?;
                let _ = respond_to.send(response);
            }
            Ok(Rpc::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                respond_to,
            }) => {
                let response = handle_append_entries(
                    &state,
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                )?;
                let _ = respond_to.send(response);
            }
            Ok(Rpc::InstallSnapshot {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                snapshot,
                respond_to,
            }) => {
                let response = handle_install_snapshot(
                    &state,
                    term,
                    leader_id,
                    last_included_index,
                    last_included_term,
                    snapshot,
                )?;
                let _ = respond_to.send(response);
            }
            Ok(Rpc::Propose {
                command,
                respond_to,
            }) => {
                let response = handle_propose(&state, &bus, command)?;
                let _ = respond_to.send(response);
            }
            Ok(Rpc::Shutdown) => break,
            Err(RecvTimeoutError::Timeout) => {
                process_timeouts(&state, &bus, &config)?;
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }

        let _ = id;
    }

    Ok(())
}

fn process_timeouts(
    state: &Arc<Mutex<NodeState>>,
    bus: &Arc<ClusterBus>,
    config: &RaftConfig,
) -> Result<()> {
    let mut start_election = false;
    let mut send_heartbeat = false;

    {
        let mut state = state.lock().unwrap();
        let now = Instant::now();
        if state.role == RaftRole::Leader {
            if now >= state.heartbeat_due {
                send_heartbeat = true;
                state.heartbeat_due = now + state.config.heartbeat_interval;
            }
        } else if now >= state.election_deadline {
            start_election = true;
        }
    }

    if start_election {
        start_election_round(state, bus, config)?;
    }

    if send_heartbeat {
        send_heartbeats(state, bus, config)?;
    }

    Ok(())
}

fn start_election_round(
    state: &Arc<Mutex<NodeState>>,
    bus: &Arc<ClusterBus>,
    config: &RaftConfig,
) -> Result<()> {
    let (term, id, peers, last_log_index, last_log_term, majority) = {
        let mut state = state.lock().unwrap();
        if state.role == RaftRole::Leader {
            return Ok(());
        }
        state.become_candidate();
        state.persist()?;
        (
            state.current_term,
            state.id,
            state.peers.clone(),
            state.last_log_index(),
            state.last_log_term(),
            state.majority(),
        )
    };

    let mut votes = 1usize;
    for peer in peers {
        if let Some(response) = call_request_vote(
            bus,
            peer,
            term,
            id,
            last_log_index,
            last_log_term,
            config.rpc_timeout,
        )? {
            if response.term > term {
                let mut state = state.lock().unwrap();
                state.step_down(response.term, None);
                state.persist()?;
                return Ok(());
            }
            if response.vote_granted {
                votes += 1;
            }
        }
    }

    if votes >= majority {
        let (term, id, peers, last_log_index) = {
            let mut state = state.lock().unwrap();
            if state.current_term != term || state.role != RaftRole::Candidate {
                return Ok(());
            }
            state.become_leader();
            state.persist()?;
            (
                state.current_term,
                state.id,
                state.peers.clone(),
                state.last_log_index(),
            )
        };

        for peer in peers {
            let _ = replicate_peer_from_state(state, bus, peer, term, id, last_log_index, config)?;
        }
    }

    Ok(())
}

fn send_heartbeats(
    state: &Arc<Mutex<NodeState>>,
    bus: &Arc<ClusterBus>,
    config: &RaftConfig,
) -> Result<()> {
    let (term, id, peers) = {
        let state = state.lock().unwrap();
        if state.role != RaftRole::Leader {
            return Ok(());
        }
        (state.current_term, state.id, state.peers.clone())
    };

    for peer in peers {
        let _ = replicate_peer_with_retry(state, bus, peer, term, id, config)?;
    }

    Ok(())
}

fn handle_request_vote(
    state: &Arc<Mutex<NodeState>>,
    term: u64,
    candidate_id: u64,
    last_log_index: u64,
    last_log_term: u64,
) -> Result<VoteResponse> {
    let mut state = state.lock().unwrap();
    let mut term_changed = false;
    if term < state.current_term {
        return Ok(VoteResponse {
            term: state.current_term,
            vote_granted: false,
        });
    }

    if term > state.current_term {
        state.step_down(term, None);
        term_changed = true;
    }

    let up_to_date = last_log_term > state.last_log_term()
        || (last_log_term == state.last_log_term() && last_log_index >= state.last_log_index());
    let can_vote = state.voted_for.is_none() || state.voted_for == Some(candidate_id);
    let granted = can_vote && up_to_date;
    if granted {
        state.voted_for = Some(candidate_id);
        state.leader_id = None;
        state.reset_election_deadline();
        state.persist()?;
    } else if term_changed {
        state.persist()?;
    } else if term >= state.current_term {
        state.reset_election_deadline();
    }

    Ok(VoteResponse {
        term: state.current_term,
        vote_granted: granted,
    })
}

fn handle_append_entries(
    state: &Arc<Mutex<NodeState>>,
    term: u64,
    leader_id: u64,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: Vec<LogEntry>,
    leader_commit: u64,
) -> Result<AppendResponse> {
    let mut state = state.lock().unwrap();
    let mut term_changed = false;
    if term < state.current_term {
        return Ok(AppendResponse {
            term: state.current_term,
            success: false,
            match_index: state.last_log_index(),
        });
    }

    if term > state.current_term || state.role != RaftRole::Follower {
        state.step_down(term, Some(leader_id));
        term_changed = true;
    }

    state.leader_id = Some(leader_id);
    state.reset_election_deadline();

    if prev_log_index < state.snapshot_index {
        return Ok(AppendResponse {
            term: state.current_term,
            success: false,
            match_index: state.snapshot_index,
        });
    }

    if prev_log_index > state.last_log_index() {
        return Ok(AppendResponse {
            term: state.current_term,
            success: false,
            match_index: state.last_log_index(),
        });
    }

    if prev_log_index == state.snapshot_index {
        if state.snapshot_index > 0 && prev_log_term != state.snapshot_term {
            return Ok(AppendResponse {
                term: state.current_term,
                success: false,
                match_index: state.snapshot_index,
            });
        }
    } else if prev_log_index > 0 {
        let existing_term = state
            .entry_at(prev_log_index)
            .map(|entry| entry.term)
            .unwrap_or(0);
        if existing_term != prev_log_term {
            return Ok(AppendResponse {
                term: state.current_term,
                success: false,
                match_index: prev_log_index.saturating_sub(1),
            });
        }
    }

    let mut changed = false;
    let mut expected_index = prev_log_index + 1;
    for entry in entries {
        if entry.index != expected_index {
            return Ok(AppendResponse {
                term: state.current_term,
                success: false,
                match_index: state.last_log_index(),
            });
        }

        if entry.index <= state.snapshot_index {
            expected_index += 1;
            continue;
        }

        if let Some(existing_term) = state.entry_at(entry.index).map(|existing| existing.term) {
            if existing_term != entry.term {
                state
                    .log
                    .retain(|existing| existing.index < entry.index || existing.index == 0);
                state.log.push(entry);
                changed = true;
            }
        } else {
            state.log.push(entry);
            changed = true;
        }
        expected_index += 1;
    }

    if leader_commit > state.commit_index {
        state.commit_index = leader_commit.min(state.last_log_index());
        state.apply_committed_entries();
        changed = true;
    }

    if state.commit_index >= state.snapshot_index {
        let _ = state.maybe_snapshot();
    }

    if changed || term_changed {
        state.persist()?;
    }

    Ok(AppendResponse {
        term: state.current_term,
        success: true,
        match_index: state.last_log_index(),
    })
}

fn handle_install_snapshot(
    state: &Arc<Mutex<NodeState>>,
    term: u64,
    leader_id: u64,
    last_included_index: u64,
    last_included_term: u64,
    snapshot: SnapshotData,
) -> Result<InstallSnapshotResponse> {
    let mut state = state.lock().unwrap();
    if term < state.current_term {
        return Ok(InstallSnapshotResponse {
            term: state.current_term,
            success: false,
            last_included_index: state.snapshot_index,
        });
    }

    if term > state.current_term || state.role != RaftRole::Follower {
        state.step_down(term, Some(leader_id));
    }

    state.leader_id = Some(leader_id);
    state.reset_election_deadline();

    if last_included_index < state.snapshot_index {
        return Ok(InstallSnapshotResponse {
            term: state.current_term,
            success: true,
            last_included_index: state.snapshot_index,
        });
    }

    let snapshot_state = SnapshotState {
        last_included_index,
        last_included_term,
        key_values: snapshot.key_values.into_iter().collect::<BTreeMap<_, _>>(),
    };
    state.install_snapshot(snapshot_state)?;

    Ok(InstallSnapshotResponse {
        term: state.current_term,
        success: true,
        last_included_index: state.snapshot_index,
    })
}

fn handle_propose(
    state_arc: &Arc<Mutex<NodeState>>,
    bus: &Arc<ClusterBus>,
    command: Command,
) -> Result<ProposeResponse> {
    let (term, leader_id, peers, index) = {
        let mut guard = state_arc.lock().unwrap();
        if guard.role != RaftRole::Leader {
            return Ok(ProposeResponse {
                term: guard.current_term,
                success: false,
                index: None,
            });
        }

        let index = guard.last_log_index() + 1;
        let entry = LogEntry {
            index,
            term: guard.current_term,
            command,
        };
        guard.log.push(entry);
        guard.persist()?;
        (guard.current_term, guard.id, guard.peers.clone(), index)
    };

    let config = {
        let state = state_arc.lock().unwrap();
        state.config.clone()
    };

    let mut replicated = 1usize;
    for peer in peers {
        if replicate_peer_with_retry(state_arc, bus, peer, term, leader_id, &config)? {
            replicated += 1;
        }
    }

    let mut state = state_arc.lock().unwrap();
    if state.role != RaftRole::Leader || state.current_term != term {
        return Ok(ProposeResponse {
            term: state.current_term,
            success: false,
            index: None,
        });
    }

    if replicated >= state.majority() {
        state.commit_index = index;
        state.apply_committed_entries();
        let _ = state.maybe_snapshot();
        state.persist()?;
        let config = state.config.clone();
        drop(state);
        send_heartbeats(state_arc, bus, &config)?;
        Ok(ProposeResponse {
            term,
            success: true,
            index: Some(index),
        })
    } else {
        Ok(ProposeResponse {
            term,
            success: false,
            index: Some(index),
        })
    }
}

fn replicate_peer_with_retry(
    state: &Arc<Mutex<NodeState>>,
    bus: &Arc<ClusterBus>,
    peer: u64,
    term: u64,
    leader_id: u64,
    config: &RaftConfig,
) -> Result<bool> {
    for _ in 0..8 {
        let action = {
            let state = state.lock().unwrap();
            if state.role != RaftRole::Leader || state.current_term != term {
                return Ok(false);
            }
            let next_index = *state
                .next_index
                .get(&peer)
                .unwrap_or(&(state.last_log_index() + 1));
            if next_index <= state.snapshot_index {
                Some(ReplicationAction::InstallSnapshot {
                    snapshot: SnapshotData {
                        key_values: state
                            .kv
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect(),
                    },
                    last_included_index: state.snapshot_index,
                    last_included_term: state.snapshot_term,
                    leader_commit: state.commit_index,
                })
            } else {
                let prev_index = next_index.saturating_sub(1);
                let prev_term = if prev_index == state.snapshot_index {
                    state.snapshot_term
                } else if prev_index == 0 {
                    0
                } else {
                    state
                        .entry_at(prev_index)
                        .map(|entry| entry.term)
                        .unwrap_or(0)
                };
                let entries = state
                    .log
                    .iter()
                    .filter(|entry| entry.index >= next_index)
                    .cloned()
                    .collect::<Vec<_>>();
                Some(ReplicationAction::Append {
                    prev_index,
                    prev_term,
                    entries,
                    leader_commit: state.commit_index,
                })
            }
        };

        match action.expect("replication action") {
            ReplicationAction::InstallSnapshot {
                snapshot,
                last_included_index,
                last_included_term,
                leader_commit,
            } => {
                match call_install_snapshot(
                    bus,
                    peer,
                    term,
                    leader_id,
                    last_included_index,
                    last_included_term,
                    snapshot,
                    config.rpc_timeout,
                )? {
                    Some(response) => {
                        if response.term > term {
                            let mut state = state.lock().unwrap();
                            state.step_down(response.term, None);
                            state.persist()?;
                            return Ok(false);
                        }
                        if response.success {
                            let mut state = state.lock().unwrap();
                            state
                                .next_index
                                .insert(peer, response.last_included_index + 1);
                            state.match_index.insert(peer, response.last_included_index);
                            if leader_commit > state.commit_index {
                                state.commit_index = leader_commit;
                            }
                            return Ok(true);
                        }
                    }
                    None => {}
                }
            }
            ReplicationAction::Append {
                prev_index,
                prev_term,
                entries,
                leader_commit,
            } => {
                match call_append_entries(
                    bus,
                    peer,
                    term,
                    leader_id,
                    prev_index,
                    prev_term,
                    entries,
                    leader_commit,
                    config.rpc_timeout,
                )? {
                    Some(response) => {
                        if response.term > term {
                            let mut state = state.lock().unwrap();
                            state.step_down(response.term, None);
                            state.persist()?;
                            return Ok(false);
                        }
                        if response.success {
                            let mut state = state.lock().unwrap();
                            state.next_index.insert(peer, response.match_index + 1);
                            state.match_index.insert(peer, response.match_index);
                            return Ok(true);
                        }
                        let mut state = state.lock().unwrap();
                        let current_next = state.next_index.get(&peer).copied().unwrap_or(1);
                        let floor = state.snapshot_index.saturating_add(1).max(1);
                        state
                            .next_index
                            .insert(peer, current_next.saturating_sub(1).max(floor));
                    }
                    None => {
                        let mut state = state.lock().unwrap();
                        let current_next = state.next_index.get(&peer).copied().unwrap_or(1);
                        let floor = state.snapshot_index.saturating_add(1).max(1);
                        state
                            .next_index
                            .insert(peer, current_next.saturating_sub(1).max(floor));
                    }
                }
            }
        }
    }

    Ok(false)
}

fn replicate_peer_from_state(
    state: &Arc<Mutex<NodeState>>,
    bus: &Arc<ClusterBus>,
    peer: u64,
    term: u64,
    leader_id: u64,
    _last_log_index: u64,
    config: &RaftConfig,
) -> Result<bool> {
    replicate_peer_with_retry(state, bus, peer, term, leader_id, config)
}

fn call_request_vote(
    bus: &Arc<ClusterBus>,
    peer: u64,
    term: u64,
    candidate_id: u64,
    last_log_index: u64,
    last_log_term: u64,
    timeout: Duration,
) -> Result<Option<VoteResponse>> {
    let (respond_to, response_rx) = mpsc::channel();
    if !bus.send(
        peer,
        Rpc::RequestVote {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
            respond_to,
        },
    ) {
        return Ok(None);
    }

    match response_rx.recv_timeout(timeout) {
        Ok(response) => Ok(Some(response)),
        Err(RecvTimeoutError::Timeout) => Ok(None),
        Err(RecvTimeoutError::Disconnected) => Ok(None),
    }
}

fn call_append_entries(
    bus: &Arc<ClusterBus>,
    peer: u64,
    term: u64,
    leader_id: u64,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: Vec<LogEntry>,
    leader_commit: u64,
    timeout: Duration,
) -> Result<Option<AppendResponse>> {
    let (respond_to, response_rx) = mpsc::channel();
    if !bus.send(
        peer,
        Rpc::AppendEntries {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
            respond_to,
        },
    ) {
        return Ok(None);
    }

    match response_rx.recv_timeout(timeout) {
        Ok(response) => Ok(Some(response)),
        Err(RecvTimeoutError::Timeout) => Ok(None),
        Err(RecvTimeoutError::Disconnected) => Ok(None),
    }
}

fn call_install_snapshot(
    bus: &Arc<ClusterBus>,
    peer: u64,
    term: u64,
    leader_id: u64,
    last_included_index: u64,
    last_included_term: u64,
    snapshot: SnapshotData,
    timeout: Duration,
) -> Result<Option<InstallSnapshotResponse>> {
    let (respond_to, response_rx) = mpsc::channel();
    if !bus.send(
        peer,
        Rpc::InstallSnapshot {
            term,
            leader_id,
            last_included_index,
            last_included_term,
            snapshot,
            respond_to,
        },
    ) {
        return Ok(None);
    }

    match response_rx.recv_timeout(timeout) {
        Ok(response) => Ok(Some(response)),
        Err(RecvTimeoutError::Timeout) => Ok(None),
        Err(RecvTimeoutError::Disconnected) => Ok(None),
    }
}

fn randomized_timeout(id: u64, term: u64, config: &RaftConfig) -> Duration {
    let min = config.election_timeout_min.as_millis() as u64;
    let max = config.election_timeout_max.as_millis() as u64;
    if max <= min {
        return config.election_timeout_min;
    }

    let span = max - min;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.subsec_nanos() as u64)
        .unwrap_or(0);
    let seed = nanos ^ id.rotate_left(7) ^ term.rotate_right(3);
    Duration::from_millis(min + (seed % (span + 1)))
}

#[derive(Debug, Clone, Copy)]
struct PersistentMeta {
    current_term: u64,
    voted_for: Option<u64>,
    commit_index: u64,
}

struct DiskState {
    meta: PersistentMeta,
    snapshot: SnapshotState,
    log: Vec<LogEntry>,
}

#[derive(Debug, Clone)]
struct SnapshotState {
    last_included_index: u64,
    last_included_term: u64,
    key_values: BTreeMap<Vec<u8>, Vec<u8>>,
}

fn persist_disk_state(
    dir: &Path,
    current_term: u64,
    voted_for: Option<u64>,
    commit_index: u64,
    log: &[LogEntry],
) -> Result<()> {
    fs::create_dir_all(dir)?;
    let meta = PersistentMeta {
        current_term,
        voted_for,
        commit_index,
    };

    let log_path = dir.join("raft.log");
    let meta_path = dir.join("raft.meta");
    write_log(&log_path, log)?;
    write_meta(&meta_path, &meta)?;
    Ok(())
}

fn load_disk_state(dir: &Path) -> Result<DiskState> {
    let meta = read_meta(&dir.join("raft.meta"))?;
    let snapshot = read_snapshot_state(&dir.join("raft.snapshot"))?;
    let log = read_log(&dir.join("raft.log"))?;
    Ok(DiskState {
        meta,
        snapshot,
        log,
    })
}

fn write_meta(path: &Path, meta: &PersistentMeta) -> Result<()> {
    let mut file = fs::File::create(path)?;
    writeln!(file, "term={}", meta.current_term)?;
    match meta.voted_for {
        Some(value) => writeln!(file, "voted_for={value}")?,
        None => writeln!(file, "voted_for=none")?,
    }
    writeln!(file, "commit_index={}", meta.commit_index)?;
    file.flush()?;
    Ok(())
}

fn read_meta(path: &Path) -> Result<PersistentMeta> {
    if !path.exists() {
        return Ok(PersistentMeta {
            current_term: 0,
            voted_for: None,
            commit_index: 0,
        });
    }

    let text = fs::read_to_string(path)?;
    let mut current_term = 0;
    let mut voted_for = None;
    let mut commit_index = 0;

    for line in text.lines() {
        if let Some(value) = line.strip_prefix("term=") {
            current_term = value
                .trim()
                .parse()
                .map_err(|_| DbError::Corrupt("invalid raft term".into()))?;
        } else if let Some(value) = line.strip_prefix("voted_for=") {
            let value = value.trim();
            voted_for = if value.eq_ignore_ascii_case("none") {
                None
            } else {
                Some(
                    value
                        .parse()
                        .map_err(|_| DbError::Corrupt("invalid raft vote".into()))?,
                )
            };
        } else if let Some(value) = line.strip_prefix("commit_index=") {
            commit_index = value
                .trim()
                .parse()
                .map_err(|_| DbError::Corrupt("invalid raft commit index".into()))?;
        }
    }

    Ok(PersistentMeta {
        current_term,
        voted_for,
        commit_index,
    })
}

fn write_log(path: &Path, log: &[LogEntry]) -> Result<()> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"HRLG");
    bytes.push(1);
    bytes.extend_from_slice(&(log.len() as u64).to_le_bytes());
    for entry in log.iter().skip(1) {
        bytes.extend_from_slice(&entry.index.to_le_bytes());
        bytes.extend_from_slice(&entry.term.to_le_bytes());
        match &entry.command {
            Command::Put(key, value) => {
                bytes.push(1);
                bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
                bytes.extend_from_slice(key);
                bytes.extend_from_slice(value);
            }
            Command::Delete(key) => {
                bytes.push(2);
                bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&0u32.to_le_bytes());
                bytes.extend_from_slice(key);
            }
        }
    }

    let checksum = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    fs::write(path, bytes)?;
    Ok(())
}

fn read_log(path: &Path) -> Result<Vec<LogEntry>> {
    if !path.exists() {
        return Ok(vec![dummy_entry()]);
    }

    let bytes = fs::read(path)?;
    if bytes.len() < 13 {
        return Err(DbError::Corrupt("raft log too short".into()));
    }

    let checksum_offset = bytes.len() - 4;
    let expected = u32::from_le_bytes(bytes[checksum_offset..].try_into().unwrap());
    let actual = crc32fast::hash(&bytes[..checksum_offset]);
    if expected != actual {
        return Err(DbError::ChecksumMismatch("raft log"));
    }

    let mut cursor = Cursor::new(&bytes[..checksum_offset]);
    let mut magic = [0u8; 4];
    cursor.read_exact(&mut magic)?;
    if &magic != b"HRLG" {
        return Err(DbError::Corrupt("invalid raft log magic".into()));
    }

    let mut version = [0u8; 1];
    cursor.read_exact(&mut version)?;
    if version[0] != 1 {
        return Err(DbError::Corrupt("unsupported raft log version".into()));
    }

    let count = read_u64(&mut cursor)? as usize;
    let mut log = vec![dummy_entry()];
    for _ in 0..count.saturating_sub(1) {
        let index = read_u64(&mut cursor)?;
        let term = read_u64(&mut cursor)?;
        let mut kind = [0u8; 1];
        cursor.read_exact(&mut kind)?;
        let key_len = read_u32(&mut cursor)? as usize;
        let value_len = read_u32(&mut cursor)? as usize;
        let key = read_bytes(&mut cursor, key_len)?;
        let value = read_bytes(&mut cursor, value_len)?;
        let command = match kind[0] {
            1 => Command::Put(key, value),
            2 => Command::Delete(key),
            other => {
                return Err(DbError::Corrupt(format!(
                    "unknown raft log command kind {other}"
                )))
            }
        };
        log.push(LogEntry {
            index,
            term,
            command,
        });
    }

    Ok(log)
}

fn persist_snapshot_state(
    path: &Path,
    last_included_index: u64,
    last_included_term: u64,
    key_values: &BTreeMap<Vec<u8>, Vec<u8>>,
) -> Result<()> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"HRSN");
    bytes.push(1);
    bytes.extend_from_slice(&last_included_index.to_le_bytes());
    bytes.extend_from_slice(&last_included_term.to_le_bytes());
    bytes.extend_from_slice(&(key_values.len() as u64).to_le_bytes());

    for (key, value) in key_values {
        bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(value);
    }

    let checksum = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    fs::write(path, bytes)?;
    Ok(())
}

fn read_snapshot_state(path: &Path) -> Result<SnapshotState> {
    if !path.exists() {
        return Ok(SnapshotState {
            last_included_index: 0,
            last_included_term: 0,
            key_values: BTreeMap::new(),
        });
    }

    let bytes = fs::read(path)?;
    if bytes.len() < 21 {
        return Err(DbError::Corrupt("raft snapshot too short".into()));
    }

    let checksum_offset = bytes.len() - 4;
    let expected = u32::from_le_bytes(bytes[checksum_offset..].try_into().unwrap());
    let actual = crc32fast::hash(&bytes[..checksum_offset]);
    if expected != actual {
        return Err(DbError::ChecksumMismatch("raft snapshot"));
    }

    let mut cursor = Cursor::new(&bytes[..checksum_offset]);
    let mut magic = [0u8; 4];
    cursor.read_exact(&mut magic)?;
    if &magic != b"HRSN" {
        return Err(DbError::Corrupt("invalid raft snapshot magic".into()));
    }

    let mut version = [0u8; 1];
    cursor.read_exact(&mut version)?;
    if version[0] != 1 {
        return Err(DbError::Corrupt("unsupported raft snapshot version".into()));
    }

    let last_included_index = read_u64(&mut cursor)?;
    let last_included_term = read_u64(&mut cursor)?;
    let count = read_u64(&mut cursor)? as usize;
    let mut key_values = BTreeMap::new();
    for _ in 0..count {
        let key_len = read_u32(&mut cursor)? as usize;
        let value_len = read_u32(&mut cursor)? as usize;
        let key = read_bytes(&mut cursor, key_len)?;
        let value = read_bytes(&mut cursor, value_len)?;
        key_values.insert(key, value);
    }

    Ok(SnapshotState {
        last_included_index,
        last_included_term,
        key_values,
    })
}

fn dummy_entry() -> LogEntry {
    LogEntry {
        index: 0,
        term: 0,
        command: Command::Delete(Vec::new()),
    }
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut bytes = [0u8; 4];
    cursor.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut bytes = [0u8; 8];
    cursor.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_bytes(cursor: &mut Cursor<&[u8]>, len: usize) -> Result<Vec<u8>> {
    let mut bytes = vec![0u8; len];
    cursor.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn compact_log_entries(log: &[LogEntry], snapshot_index: u64) -> Vec<LogEntry> {
    let mut compacted = Vec::with_capacity(log.len());
    compacted.push(dummy_entry());
    for entry in log.iter().skip(1) {
        if entry.index > snapshot_index {
            compacted.push(entry.clone());
        }
    }
    compacted
}
