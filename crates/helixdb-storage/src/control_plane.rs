use crate::{DbError, RaftCluster, RaftConfig, RangeDescriptor, RangeRoutingError, ShardedCluster};

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use thiserror::Error;

pub type ControlPlaneResult<T> = std::result::Result<T, ControlPlaneError>;

const TIMESTAMP_KEY: &str = "timestamp/current";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampBatch {
    pub start: u64,
    pub end: u64,
}

impl TimestampBatch {
    pub fn len(&self) -> u64 {
        self.end.saturating_sub(self.start).saturating_add(1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Healthy,
    Suspect,
    Dead,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRecord {
    pub node_id: u64,
    pub capacity_units: u64,
    pub last_heartbeat_ms: u64,
    pub status: NodeStatus,
}

impl NodeRecord {
    fn new(node_id: u64, capacity_units: u64, last_heartbeat_ms: u64) -> Self {
        Self {
            node_id,
            capacity_units,
            last_heartbeat_ms,
            status: NodeStatus::Healthy,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangePlacement {
    pub descriptor: RangeDescriptor,
    pub replicas: Vec<u64>,
}

impl RangePlacement {
    fn new(descriptor: RangeDescriptor, replicas: Vec<u64>) -> Self {
        Self {
            descriptor,
            replicas,
        }
    }

    fn leader(&self) -> Option<u64> {
        self.descriptor.leader_hint
    }
}

#[derive(Debug, Clone)]
pub struct ControlPlaneConfig {
    pub heartbeat_timeout: Duration,
    pub suspect_timeout: Duration,
}

impl Default for ControlPlaneConfig {
    fn default() -> Self {
        Self {
            heartbeat_timeout: Duration::from_secs(10),
            suspect_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Error)]
pub enum ControlPlaneError {
    #[error("database error: {0}")]
    Db(#[from] DbError),
    #[error("routing error: {0}")]
    Route(#[from] RangeRoutingError),
    #[error("unknown node id {0}")]
    UnknownNode(u64),
    #[error("unknown range id {0}")]
    UnknownRange(u64),
    #[error("invalid metadata record: {0}")]
    InvalidRecord(String),
}

pub struct ControlPlane {
    metadata: RaftCluster,
    data: ShardedCluster,
    nodes: Mutex<BTreeMap<u64, NodeRecord>>,
    ranges: Mutex<BTreeMap<u64, RangePlacement>>,
    timestamp: Mutex<TimestampOracleState>,
    config: ControlPlaneConfig,
}

#[derive(Debug, Default, Clone, Copy)]
struct TimestampOracleState {
    next_timestamp: u64,
}

impl ControlPlane {
    pub fn bootstrap(
        root: impl AsRef<Path>,
        metadata_nodes: usize,
        data_nodes_per_group: usize,
        descriptors: Vec<RangeDescriptor>,
        raft_config: RaftConfig,
    ) -> ControlPlaneResult<Self> {
        Self::bootstrap_with_config(
            root,
            metadata_nodes,
            data_nodes_per_group,
            descriptors,
            raft_config,
            ControlPlaneConfig::default(),
        )
    }

    pub fn bootstrap_with_config(
        root: impl AsRef<Path>,
        metadata_nodes: usize,
        data_nodes_per_group: usize,
        descriptors: Vec<RangeDescriptor>,
        raft_config: RaftConfig,
        config: ControlPlaneConfig,
    ) -> ControlPlaneResult<Self> {
        let root = root.as_ref().to_path_buf();
        let metadata = RaftCluster::bootstrap_with_config(
            root.join("metadata"),
            metadata_nodes,
            raft_config.clone(),
        )?;
        let data = ShardedCluster::bootstrap_with_ranges(
            root.join("data"),
            descriptors.clone(),
            data_nodes_per_group,
            raft_config,
        )?;

        let mut cp = Self {
            metadata,
            data,
            nodes: Mutex::new(BTreeMap::new()),
            ranges: Mutex::new(BTreeMap::new()),
            timestamp: Mutex::new(TimestampOracleState::default()),
            config,
        };

        cp.load_from_metadata()?;
        if cp.ranges.lock().unwrap().is_empty() {
            for descriptor in descriptors {
                cp.ranges.lock().unwrap().insert(
                    descriptor.range_id,
                    RangePlacement::new(descriptor, Vec::new()),
                );
            }
            cp.persist_ranges()?;
        }

        Ok(cp)
    }

    pub fn metadata_cluster(&self) -> &RaftCluster {
        &self.metadata
    }

    pub fn data_cluster(&self) -> &ShardedCluster {
        &self.data
    }

    pub fn register_node(
        &self,
        node_id: u64,
        capacity_units: u64,
    ) -> ControlPlaneResult<NodeRecord> {
        let record = NodeRecord::new(node_id, capacity_units, current_unix_ms());
        self.nodes.lock().unwrap().insert(node_id, record.clone());
        self.persist_node(&record)?;
        Ok(record)
    }

    pub fn heartbeat(&self, node_id: u64) -> ControlPlaneResult<NodeRecord> {
        let mut nodes = self.nodes.lock().unwrap();
        let record = nodes
            .get_mut(&node_id)
            .ok_or(ControlPlaneError::UnknownNode(node_id))?;
        record.last_heartbeat_ms = current_unix_ms();
        record.status = NodeStatus::Healthy;
        let snapshot = record.clone();
        drop(nodes);
        self.persist_node(&snapshot)?;
        Ok(snapshot)
    }

    pub fn set_last_heartbeat_for_test(
        &self,
        node_id: u64,
        last_heartbeat_ms: u64,
    ) -> ControlPlaneResult<NodeRecord> {
        let mut nodes = self.nodes.lock().unwrap();
        let record = nodes
            .get_mut(&node_id)
            .ok_or(ControlPlaneError::UnknownNode(node_id))?;
        record.last_heartbeat_ms = last_heartbeat_ms;
        let snapshot = record.clone();
        drop(nodes);
        self.persist_node(&snapshot)?;
        Ok(snapshot)
    }

    pub fn node_registry(&self) -> BTreeMap<u64, NodeRecord> {
        self.nodes.lock().unwrap().clone()
    }

    pub fn range_registry(&self) -> BTreeMap<u64, RangePlacement> {
        self.ranges.lock().unwrap().clone()
    }

    pub fn register_range(
        &self,
        descriptor: RangeDescriptor,
        replicas: Vec<u64>,
    ) -> ControlPlaneResult<RangePlacement> {
        for replica in &replicas {
            if !self.nodes.lock().unwrap().contains_key(replica) {
                return Err(ControlPlaneError::UnknownNode(*replica));
            }
        }
        let placement = RangePlacement::new(descriptor.clone(), replicas);
        self.ranges
            .lock()
            .unwrap()
            .insert(descriptor.range_id, placement.clone());
        self.persist_range(&placement)?;
        Ok(placement)
    }

    pub fn add_replica(
        &self,
        range_id: u64,
        node_id: u64,
    ) -> ControlPlaneResult<RangePlacement> {
        self.ensure_registered_node(node_id)?;
        let mut ranges = self.ranges.lock().unwrap();
        let placement = ranges
            .get_mut(&range_id)
            .ok_or(ControlPlaneError::UnknownRange(range_id))?;
        if !placement.replicas.contains(&node_id) {
            placement.replicas.push(node_id);
            placement.replicas.sort_unstable();
        }
        let descriptor = self.data.add_group_node(range_id, node_id)?;
        placement.descriptor = descriptor;
        let snapshot = placement.clone();
        drop(ranges);
        self.persist_range(&snapshot)?;
        Ok(snapshot)
    }

    pub fn remove_replica(
        &self,
        range_id: u64,
        node_id: u64,
    ) -> ControlPlaneResult<RangePlacement> {
        let mut ranges = self.ranges.lock().unwrap();
        let placement = ranges
            .get_mut(&range_id)
            .ok_or(ControlPlaneError::UnknownRange(range_id))?;
        if !placement.replicas.contains(&node_id) {
            return Err(ControlPlaneError::UnknownNode(node_id));
        }
        if placement.replicas.len() <= 1 {
            return Err(ControlPlaneError::InvalidRecord(
                "range must keep at least one replica".into(),
            ));
        }

        placement.replicas.retain(|replica| *replica != node_id);
        let descriptor = self.data.remove_group_node(range_id, node_id)?;
        placement.descriptor = descriptor;
        let snapshot = placement.clone();
        drop(ranges);
        self.persist_range(&snapshot)?;
        Ok(snapshot)
    }

    pub fn rebalance(&self) -> ControlPlaneResult<usize> {
        self.sweep_health()?;
        let healthy_nodes = self.healthy_nodes();
        if healthy_nodes.is_empty() {
            return Ok(0);
        }

        let mut moved = 0usize;
        let mut pending_updates = Vec::new();
        {
            let mut ranges = self.ranges.lock().unwrap();
            for placement in ranges.values_mut() {
                let preferred = placement
                    .leader()
                    .filter(|node_id| self.is_healthy(*node_id))
                    .or_else(|| {
                        placement
                            .replicas
                            .iter()
                            .copied()
                            .find(|node_id| self.is_healthy(*node_id))
                    })
                    .or_else(|| healthy_nodes.first().copied());

                if placement.descriptor.leader_hint != preferred {
                    placement.descriptor.leader_hint = preferred;
                    placement.descriptor.epoch += 1;
                    moved += 1;
                    pending_updates.push(placement.clone());
                    continue;
                }

                if placement.replicas.is_empty() {
                    placement.replicas = healthy_nodes.clone();
                    pending_updates.push(placement.clone());
                }
            }
        }
        for placement in pending_updates {
            self.persist_range(&placement)?;
        }
        Ok(moved)
    }

    pub fn sweep_health(&self) -> ControlPlaneResult<usize> {
        let mut nodes = self.nodes.lock().unwrap();
        let now = current_unix_ms();
        let suspect_after = self.config.suspect_timeout.as_millis() as u64;
        let dead_after = self.config.heartbeat_timeout.as_millis() as u64;
        let mut updated = 0usize;

        for record in nodes.values_mut() {
            let age = now.saturating_sub(record.last_heartbeat_ms);
            let next_status = if age >= dead_after {
                NodeStatus::Dead
            } else if age >= suspect_after {
                NodeStatus::Suspect
            } else {
                NodeStatus::Healthy
            };

            if record.status != next_status {
                record.status = next_status;
                updated += 1;
            }
        }

        let snapshot = nodes.values().cloned().collect::<Vec<_>>();
        drop(nodes);
        for record in snapshot {
            self.persist_node(&record)?;
        }
        Ok(updated)
    }

    pub fn route_put(
        &self,
        key: impl AsRef<[u8]>,
        value: impl Into<Vec<u8>>,
    ) -> ControlPlaneResult<u64> {
        self.data.route_put(key, value).map_err(Into::into)
    }

    pub fn route_get(&self, key: impl AsRef<[u8]>) -> ControlPlaneResult<Option<Vec<u8>>> {
        self.data.route_get(key).map_err(Into::into)
    }

    pub fn route_delete(&self, key: impl AsRef<[u8]>) -> ControlPlaneResult<u64> {
        self.data.route_delete(key).map_err(Into::into)
    }

    pub fn allocate_timestamp_batch(&self, count: u64) -> ControlPlaneResult<TimestampBatch> {
        if count == 0 {
            return Err(ControlPlaneError::InvalidRecord(
                "timestamp batch size must be greater than zero".into(),
            ));
        }

        let mut timestamp = self.timestamp.lock().unwrap();
        let start = timestamp.next_timestamp.checked_add(1).ok_or_else(|| {
            ControlPlaneError::InvalidRecord("timestamp oracle overflow".into())
        })?;
        let end = timestamp.next_timestamp.checked_add(count).ok_or_else(|| {
            ControlPlaneError::InvalidRecord("timestamp oracle overflow".into())
        })?;

        self.persist_timestamp(end)?;
        timestamp.next_timestamp = end;
        Ok(TimestampBatch { start, end })
    }

    pub fn allocate_timestamp(&self) -> ControlPlaneResult<u64> {
        Ok(self.allocate_timestamp_batch(1)?.start)
    }

    pub fn current_timestamp(&self) -> u64 {
        self.timestamp.lock().unwrap().next_timestamp
    }

    pub fn refresh_from_metadata(&mut self) -> ControlPlaneResult<()> {
        self.load_from_metadata()?;
        Ok(())
    }

    fn healthy_nodes(&self) -> Vec<u64> {
        self.nodes
            .lock()
            .unwrap()
            .values()
            .filter(|record| matches!(record.status, NodeStatus::Healthy))
            .map(|record| record.node_id)
            .collect()
    }

    fn is_healthy(&self, node_id: u64) -> bool {
        self.nodes
            .lock()
            .unwrap()
            .get(&node_id)
            .map(|record| matches!(record.status, NodeStatus::Healthy))
            .unwrap_or(false)
    }

    fn persist_node(&self, record: &NodeRecord) -> ControlPlaneResult<()> {
        let key = format!("node/{:016}", record.node_id).into_bytes();
        let value = encode_node_record(record);
        self.metadata.put(key, value)?;
        Ok(())
    }

    fn persist_range(&self, placement: &RangePlacement) -> ControlPlaneResult<()> {
        let key = format!("range/{:016}", placement.descriptor.range_id).into_bytes();
        let value = encode_range_placement(placement);
        self.metadata.put(key, value)?;
        Ok(())
    }

    fn persist_timestamp(&self, timestamp: u64) -> ControlPlaneResult<()> {
        self.metadata.put(
            TIMESTAMP_KEY.as_bytes().to_vec(),
            timestamp.to_le_bytes().to_vec(),
        )?;
        Ok(())
    }

    fn persist_ranges(&self) -> ControlPlaneResult<()> {
        let ranges = self
            .ranges
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for placement in ranges {
            self.persist_range(&placement)?;
        }
        Ok(())
    }

    fn ensure_registered_node(&self, node_id: u64) -> ControlPlaneResult<()> {
        if self.nodes.lock().unwrap().contains_key(&node_id) {
            Ok(())
        } else {
            Err(ControlPlaneError::UnknownNode(node_id))
        }
    }

    fn load_from_metadata(&mut self) -> ControlPlaneResult<()> {
        let entries = self.metadata.all_entries()?;
        let mut nodes = BTreeMap::new();
        let mut ranges = BTreeMap::new();
        let mut timestamp = TimestampOracleState::default();

        for (key, value) in entries {
            let key_str = String::from_utf8(key)
                .map_err(|_| ControlPlaneError::InvalidRecord("metadata key is not utf8".into()))?;
            if let Some(id_str) = key_str.strip_prefix("node/") {
                let node_id = id_str
                    .parse::<u64>()
                    .map_err(|_| ControlPlaneError::InvalidRecord("invalid node key".into()))?;
                nodes.insert(node_id, decode_node_record(&value)?);
            } else if let Some(id_str) = key_str.strip_prefix("range/") {
                let range_id = id_str
                    .parse::<u64>()
                    .map_err(|_| ControlPlaneError::InvalidRecord("invalid range key".into()))?;
                let placement = decode_range_placement(&value)?;
                ranges.insert(range_id, placement);
            } else if key_str == TIMESTAMP_KEY {
                timestamp.next_timestamp = decode_timestamp(&value)?;
            }
        }

        *self.nodes.lock().unwrap() = nodes;
        *self.ranges.lock().unwrap() = ranges;
        *self.timestamp.lock().unwrap() = timestamp;
        Ok(())
    }
}

fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn encode_node_record(record: &NodeRecord) -> Vec<u8> {
    format!(
        "{}|{}|{}|{}",
        record.node_id,
        status_code(record.status),
        record.capacity_units,
        record.last_heartbeat_ms
    )
    .into_bytes()
}

fn decode_node_record(bytes: &[u8]) -> ControlPlaneResult<NodeRecord> {
    let text = String::from_utf8(bytes.to_vec())
        .map_err(|_| ControlPlaneError::InvalidRecord("node record is not utf8".into()))?;
    let mut parts = text.split('|');
    let node_id = parts
        .next()
        .ok_or_else(|| ControlPlaneError::InvalidRecord("missing node id".into()))?
        .parse::<u64>()
        .map_err(|_| ControlPlaneError::InvalidRecord("invalid node id".into()))?;
    let status = parse_status(
        parts
            .next()
            .ok_or_else(|| ControlPlaneError::InvalidRecord("missing node status".into()))?,
    )?;
    let capacity_units = parts
        .next()
        .ok_or_else(|| ControlPlaneError::InvalidRecord("missing node capacity".into()))?
        .parse::<u64>()
        .map_err(|_| ControlPlaneError::InvalidRecord("invalid node capacity".into()))?;
    let last_heartbeat_ms = parts
        .next()
        .ok_or_else(|| ControlPlaneError::InvalidRecord("missing node heartbeat".into()))?
        .parse::<u64>()
        .map_err(|_| ControlPlaneError::InvalidRecord("invalid heartbeat".into()))?;

    Ok(NodeRecord {
        node_id,
        capacity_units,
        last_heartbeat_ms,
        status,
    })
}

fn encode_range_placement(placement: &RangePlacement) -> Vec<u8> {
    let descriptor = &placement.descriptor;
    let end_hex = descriptor
        .end
        .as_ref()
        .map(|bytes| hex_encode(bytes))
        .unwrap_or_else(|| "-".to_string());
    let leader = descriptor
        .leader_hint
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string());
    let replicas = placement
        .replicas
        .iter()
        .map(|node_id| node_id.to_string())
        .collect::<Vec<_>>()
        .join(",");

    format!(
        "{}|{}|{}|{}|{}|{}|{}",
        descriptor.range_id,
        hex_encode(&descriptor.start),
        end_hex,
        descriptor.epoch,
        descriptor.raft_group_id,
        leader,
        replicas
    )
    .into_bytes()
}

fn decode_range_placement(bytes: &[u8]) -> ControlPlaneResult<RangePlacement> {
    let text = String::from_utf8(bytes.to_vec())
        .map_err(|_| ControlPlaneError::InvalidRecord("range record is not utf8".into()))?;
    let parts = text.split('|').collect::<Vec<_>>();
    if parts.len() != 7 && parts.len() != 8 {
        return Err(ControlPlaneError::InvalidRecord(
            "range record has unexpected field count".into(),
        ));
    }
    let range_id = parts
        .get(0)
        .ok_or_else(|| ControlPlaneError::InvalidRecord("missing range id".into()))?
        .parse::<u64>()
        .map_err(|_| ControlPlaneError::InvalidRecord("invalid range id".into()))?;
    let start = hex_decode(
        parts
            .get(1)
            .ok_or_else(|| ControlPlaneError::InvalidRecord("missing range start".into()))?,
    )?;
    let end_raw = parts
        .get(2)
        .ok_or_else(|| ControlPlaneError::InvalidRecord("missing range end".into()))?;
    let end = if *end_raw == "-" {
        None
    } else {
        Some(hex_decode(end_raw)?)
    };
    let epoch = parts
        .get(3)
        .ok_or_else(|| ControlPlaneError::InvalidRecord("missing range epoch".into()))?
        .parse::<u64>()
        .map_err(|_| ControlPlaneError::InvalidRecord("invalid range epoch".into()))?;
    let raft_group_id = parts
        .get(4)
        .ok_or_else(|| ControlPlaneError::InvalidRecord("missing raft group id".into()))?
        .parse::<u64>()
        .map_err(|_| ControlPlaneError::InvalidRecord("invalid raft group id".into()))?;
    let leader_raw = parts
        .get(5)
        .ok_or_else(|| ControlPlaneError::InvalidRecord("missing leader hint".into()))?;
    let leader_hint = if *leader_raw == "-" {
        None
    } else {
        Some(
            leader_raw
                .parse::<u64>()
                .map_err(|_| ControlPlaneError::InvalidRecord("invalid leader hint".into()))?,
        )
    };
    let replicas_raw = if parts.len() == 8 {
        parts
            .get(7)
            .ok_or_else(|| ControlPlaneError::InvalidRecord("missing replicas".into()))?
    } else {
        parts
            .get(6)
            .ok_or_else(|| ControlPlaneError::InvalidRecord("missing replicas".into()))?
    };
    let replicas = if replicas_raw.is_empty() {
        Vec::new()
    } else {
        replicas_raw
            .split(',')
            .map(|part| {
                part.parse::<u64>()
                    .map_err(|_| ControlPlaneError::InvalidRecord("invalid replica node id".into()))
            })
            .collect::<ControlPlaneResult<Vec<_>>>()?
    };

    Ok(RangePlacement {
        descriptor: RangeDescriptor {
            range_id,
            start,
            end,
            epoch,
            raft_group_id,
            leader_hint,
        },
        replicas,
    })
}

fn status_code(status: NodeStatus) -> &'static str {
    match status {
        NodeStatus::Healthy => "healthy",
        NodeStatus::Suspect => "suspect",
        NodeStatus::Dead => "dead",
    }
}

fn parse_status(status: &str) -> ControlPlaneResult<NodeStatus> {
    match status {
        "healthy" => Ok(NodeStatus::Healthy),
        "suspect" => Ok(NodeStatus::Suspect),
        "dead" => Ok(NodeStatus::Dead),
        other => Err(ControlPlaneError::InvalidRecord(format!(
            "invalid node status {other}"
        ))),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(text: &str) -> ControlPlaneResult<Vec<u8>> {
    let bytes = text.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(ControlPlaneError::InvalidRecord(
            "hex string must have even length".into(),
        ));
    }

    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut index = 0usize;
    while index < bytes.len() {
        let hi = from_hex_digit(bytes[index])?;
        let lo = from_hex_digit(bytes[index + 1])?;
        out.push((hi << 4) | lo);
        index += 2;
    }
    Ok(out)
}

fn decode_timestamp(bytes: &[u8]) -> ControlPlaneResult<u64> {
    if bytes.len() != 8 {
        return Err(ControlPlaneError::InvalidRecord(
            "timestamp record must be exactly 8 bytes".into(),
        ));
    }

    let mut raw = [0u8; 8];
    raw.copy_from_slice(bytes);
    Ok(u64::from_le_bytes(raw))
}

fn from_hex_digit(byte: u8) -> ControlPlaneResult<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(10 + byte - b'a'),
        b'A'..=b'F' => Ok(10 + byte - b'A'),
        _ => Err(ControlPlaneError::InvalidRecord("invalid hex digit".into())),
    }
}
