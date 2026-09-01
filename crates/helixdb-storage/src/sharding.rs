use crate::{DbError, RaftCluster, RaftConfig};

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

use thiserror::Error;

pub type ShardResult<T> = std::result::Result<T, RangeRoutingError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeDescriptor {
    pub range_id: u64,
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
    pub epoch: u64,
    pub raft_group_id: u64,
    pub leader_hint: Option<u64>,
}

impl RangeDescriptor {
    pub fn new(
        range_id: u64,
        start: impl Into<Vec<u8>>,
        end: Option<impl Into<Vec<u8>>>,
        epoch: u64,
        raft_group_id: u64,
    ) -> Self {
        Self {
            range_id,
            start: start.into(),
            end: end.map(Into::into),
            epoch,
            raft_group_id,
            leader_hint: None,
        }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        if key < self.start.as_slice() {
            return false;
        }

        match &self.end {
            Some(end) => key < end.as_slice(),
            None => true,
        }
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum RangeRoutingError {
    #[error("key is outside any known range: {0}")]
    KeyOutOfRange(String),
    #[error("range {range_id} moved to range {current_range_id}")]
    RangeMoved {
        range_id: u64,
        current_range_id: u64,
    },
    #[error("range {range_id} epoch mismatch: cached {cached_epoch}, current {current_epoch}")]
    EpochMismatch {
        range_id: u64,
        cached_epoch: u64,
        current_epoch: u64,
    },
    #[error("range {range_id} is not led by the cached leader")]
    NotLeader {
        range_id: u64,
        cached_leader: Option<u64>,
        current_leader: Option<u64>,
    },
    #[error("unknown range id {0}")]
    UnknownRange(u64),
    #[error("invalid range descriptor layout: {0}")]
    InvalidDescriptor(String),
    #[error("internal routing error: {0}")]
    Internal(String),
}

impl From<DbError> for RangeRoutingError {
    fn from(value: DbError) -> Self {
        Self::Internal(value.to_string())
    }
}

pub struct ShardedCluster {
    groups: BTreeMap<u64, RaftCluster>,
    authoritative: Mutex<BTreeMap<u64, RangeDescriptor>>,
    cached: Mutex<BTreeMap<u64, RangeDescriptor>>,
}

impl ShardedCluster {
    pub fn bootstrap_with_ranges(
        root: impl AsRef<Path>,
        descriptors: Vec<RangeDescriptor>,
        nodes_per_group: usize,
        raft_config: RaftConfig,
    ) -> ShardResult<Self> {
        if nodes_per_group == 0 {
            return Err(RangeRoutingError::InvalidDescriptor(
                "nodes_per_group must be greater than zero".into(),
            ));
        }
        if descriptors.is_empty() {
            return Err(RangeRoutingError::InvalidDescriptor(
                "at least one range descriptor is required".into(),
            ));
        }

        let mut descriptors = descriptors;
        descriptors.sort_by(|left, right| left.start.cmp(&right.start));
        validate_non_overlapping(&descriptors)?;

        let root = root.as_ref().to_path_buf();
        let mut groups = BTreeMap::new();
        let mut authoritative = BTreeMap::new();

        for descriptor in descriptors {
            let group_root = root.join(format!("range-{:016}", descriptor.range_id));
            let cluster = RaftCluster::bootstrap_with_config(
                &group_root,
                nodes_per_group,
                raft_config.clone(),
            )?;
            let leader_hint = cluster.leader_id();
            authoritative.insert(
                descriptor.range_id,
                RangeDescriptor {
                    leader_hint,
                    ..descriptor.clone()
                },
            );
            groups.insert(descriptor.range_id, cluster);
        }

        Ok(Self {
            groups,
            authoritative: Mutex::new(authoritative),
            cached: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn route_descriptor_for_key(&self, key: impl AsRef<[u8]>) -> ShardResult<RangeDescriptor> {
        let key = key.as_ref();
        let authoritative = self.authoritative_descriptor_for_key(key)?;
        if let Some(cached) = self
            .cached
            .lock()
            .unwrap()
            .get(&authoritative.range_id)
            .cloned()
        {
            self.validate_cached_descriptor(key, &cached, &authoritative)?;
            return Ok(cached);
        }

        if let Some(cached) = self.cached_descriptor_for_key(key) {
            return Err(RangeRoutingError::RangeMoved {
                range_id: cached.range_id,
                current_range_id: authoritative.range_id,
            });
        }

        self.cached
            .lock()
            .unwrap()
            .insert(authoritative.range_id, authoritative.clone());
        Ok(authoritative)
    }

    pub fn authoritative_descriptor_for_key(
        &self,
        key: impl AsRef<[u8]>,
    ) -> ShardResult<RangeDescriptor> {
        let key = key.as_ref();
        let authoritative = self.authoritative.lock().unwrap();
        authoritative
            .values()
            .find(|descriptor| descriptor.contains(key))
            .cloned()
            .ok_or_else(|| RangeRoutingError::KeyOutOfRange(format!("{:?}", key)))
    }

    pub fn descriptor(&self, range_id: u64) -> ShardResult<RangeDescriptor> {
        self.authoritative
            .lock()
            .unwrap()
            .get(&range_id)
            .cloned()
            .ok_or(RangeRoutingError::UnknownRange(range_id))
    }

    pub fn cached_descriptor(&self, range_id: u64) -> Option<RangeDescriptor> {
        self.cached.lock().unwrap().get(&range_id).cloned()
    }

    pub fn refresh_descriptor(&self, range_id: u64) -> ShardResult<RangeDescriptor> {
        let descriptor = self
            .authoritative
            .lock()
            .unwrap()
            .get(&range_id)
            .cloned()
            .ok_or(RangeRoutingError::UnknownRange(range_id))?;
        let mut cache = self.cached.lock().unwrap();
        cache.insert(range_id, descriptor.clone());
        Ok(descriptor)
    }

    pub fn bump_range_epoch(&self, range_id: u64) -> ShardResult<RangeDescriptor> {
        let mut authoritative = self.authoritative.lock().unwrap();
        let descriptor = authoritative
            .get_mut(&range_id)
            .ok_or(RangeRoutingError::UnknownRange(range_id))?;
        descriptor.epoch += 1;
        descriptor.leader_hint = self.current_leader(range_id);
        Ok(descriptor.clone())
    }

    pub fn move_range(
        &self,
        range_id: u64,
        new_start: impl Into<Vec<u8>>,
        new_end: Option<impl Into<Vec<u8>>>,
    ) -> ShardResult<RangeDescriptor> {
        let mut authoritative = self.authoritative.lock().unwrap();
        let descriptor = authoritative
            .get_mut(&range_id)
            .ok_or(RangeRoutingError::UnknownRange(range_id))?;
        descriptor.start = new_start.into();
        descriptor.end = new_end.map(Into::into);
        descriptor.epoch += 1;
        descriptor.leader_hint = self.current_leader(range_id);
        Ok(descriptor.clone())
    }

    pub fn leader_id(&self, range_id: u64) -> Option<u64> {
        self.groups
            .get(&range_id)
            .and_then(|cluster| cluster.leader_id())
    }

    pub fn group_leader_id(&self, range_id: u64) -> ShardResult<Option<u64>> {
        self.groups
            .get(&range_id)
            .map(|cluster| cluster.leader_id())
            .ok_or(RangeRoutingError::UnknownRange(range_id))
    }

    pub fn kill_group_node(&self, range_id: u64, node_id: u64) -> ShardResult<()> {
        let cluster = self
            .groups
            .get(&range_id)
            .ok_or(RangeRoutingError::UnknownRange(range_id))?;
        cluster.kill_node(node_id)?;
        Ok(())
    }

    pub fn restart_group_node(&self, range_id: u64, node_id: u64) -> ShardResult<()> {
        let cluster = self
            .groups
            .get(&range_id)
            .ok_or(RangeRoutingError::UnknownRange(range_id))?;
        cluster.restart_node(node_id)?;
        Ok(())
    }

    pub fn move_boundary(
        &self,
        left_range_id: u64,
        right_range_id: u64,
        boundary: impl Into<Vec<u8>>,
    ) -> ShardResult<()> {
        if left_range_id == right_range_id {
            return Err(RangeRoutingError::InvalidDescriptor(
                "boundary move requires two different ranges".into(),
            ));
        }

        let boundary = boundary.into();
        let mut authoritative = self.authoritative.lock().unwrap();
        let mut left = authoritative
            .get(&left_range_id)
            .cloned()
            .ok_or(RangeRoutingError::UnknownRange(left_range_id))?;
        let mut right = authoritative
            .get(&right_range_id)
            .cloned()
            .ok_or(RangeRoutingError::UnknownRange(right_range_id))?;

        left.end = Some(boundary.clone());
        right.start = boundary;
        left.epoch += 1;
        right.epoch += 1;
        left.leader_hint = self.current_leader(left_range_id);
        right.leader_hint = self.current_leader(right_range_id);
        authoritative.insert(left_range_id, left);
        authoritative.insert(right_range_id, right);
        Ok(())
    }

    pub fn route_put(&self, key: impl AsRef<[u8]>, value: impl Into<Vec<u8>>) -> ShardResult<u64> {
        let key = key.as_ref().to_vec();
        let value = value.into();
        let descriptor = self.route_descriptor_for_key(&key)?;
        self.route_put_with_descriptor(&key, value, descriptor)
    }

    pub fn put(&self, key: impl AsRef<[u8]>, value: impl Into<Vec<u8>>) -> ShardResult<u64> {
        let key = key.as_ref().to_vec();
        let value = value.into();
        self.retry_put(&key, value)
    }

    pub fn route_delete(&self, key: impl AsRef<[u8]>) -> ShardResult<u64> {
        let key = key.as_ref().to_vec();
        let descriptor = self.route_descriptor_for_key(&key)?;
        self.route_delete_with_descriptor(&key, descriptor)
    }

    pub fn delete(&self, key: impl AsRef<[u8]>) -> ShardResult<u64> {
        let key = key.as_ref().to_vec();
        self.retry_delete(&key)
    }

    pub fn route_get(&self, key: impl AsRef<[u8]>) -> ShardResult<Option<Vec<u8>>> {
        let key = key.as_ref().to_vec();
        let descriptor = self.route_descriptor_for_key(&key)?;
        self.route_get_with_descriptor(&key, descriptor)
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> ShardResult<Option<Vec<u8>>> {
        let key = key.as_ref().to_vec();
        self.retry_get(&key)
    }

    pub fn group_state(&self, range_id: u64) -> ShardResult<Option<u64>> {
        let cluster = self
            .groups
            .get(&range_id)
            .ok_or(RangeRoutingError::UnknownRange(range_id))?;
        Ok(cluster.leader_id())
    }

    fn cached_descriptor_for_key(&self, key: &[u8]) -> Option<RangeDescriptor> {
        self.cached
            .lock()
            .unwrap()
            .values()
            .find(|descriptor| descriptor.contains(key))
            .cloned()
    }

    fn validate_cached_descriptor(
        &self,
        key: &[u8],
        cached: &RangeDescriptor,
        authoritative: &RangeDescriptor,
    ) -> ShardResult<()> {
        if cached.range_id != authoritative.range_id {
            return Err(RangeRoutingError::RangeMoved {
                range_id: cached.range_id,
                current_range_id: authoritative.range_id,
            });
        }

        if cached.epoch != authoritative.epoch {
            return Err(RangeRoutingError::EpochMismatch {
                range_id: cached.range_id,
                cached_epoch: cached.epoch,
                current_epoch: authoritative.epoch,
            });
        }

        if !cached.contains(key) {
            return Err(RangeRoutingError::RangeMoved {
                range_id: cached.range_id,
                current_range_id: authoritative.range_id,
            });
        }

        let current_leader = self.current_leader(cached.range_id);
        if cached.leader_hint.is_some() && cached.leader_hint != current_leader {
            return Err(RangeRoutingError::NotLeader {
                range_id: cached.range_id,
                cached_leader: cached.leader_hint,
                current_leader,
            });
        }

        Ok(())
    }

    fn current_leader(&self, range_id: u64) -> Option<u64> {
        self.groups
            .get(&range_id)
            .and_then(|cluster| cluster.leader_id())
    }

    fn route_put_with_descriptor(
        &self,
        key: &[u8],
        value: Vec<u8>,
        mut descriptor: RangeDescriptor,
    ) -> ShardResult<u64> {
        self.validate_route(key, &descriptor)?;
        let cluster = self
            .groups
            .get(&descriptor.range_id)
            .ok_or(RangeRoutingError::UnknownRange(descriptor.range_id))?;
        let index = cluster
            .put(key.to_vec(), value)
            .map_err(RangeRoutingError::from)?;
        descriptor.leader_hint = cluster.leader_id();
        self.cached
            .lock()
            .unwrap()
            .insert(descriptor.range_id, descriptor);
        Ok(index)
    }

    fn route_delete_with_descriptor(
        &self,
        key: &[u8],
        mut descriptor: RangeDescriptor,
    ) -> ShardResult<u64> {
        self.validate_route(key, &descriptor)?;
        let cluster = self
            .groups
            .get(&descriptor.range_id)
            .ok_or(RangeRoutingError::UnknownRange(descriptor.range_id))?;
        let index = cluster
            .delete(key.to_vec())
            .map_err(RangeRoutingError::from)?;
        descriptor.leader_hint = cluster.leader_id();
        self.cached
            .lock()
            .unwrap()
            .insert(descriptor.range_id, descriptor);
        Ok(index)
    }

    fn route_get_with_descriptor(
        &self,
        key: &[u8],
        descriptor: RangeDescriptor,
    ) -> ShardResult<Option<Vec<u8>>> {
        self.validate_route(key, &descriptor)?;
        let cluster = self
            .groups
            .get(&descriptor.range_id)
            .ok_or(RangeRoutingError::UnknownRange(descriptor.range_id))?;
        let value = cluster.get(key.to_vec()).map_err(RangeRoutingError::from)?;
        let mut refreshed = descriptor.clone();
        refreshed.leader_hint = cluster.leader_id();
        self.cached
            .lock()
            .unwrap()
            .insert(refreshed.range_id, refreshed);
        Ok(value)
    }

    fn validate_route(&self, key: &[u8], descriptor: &RangeDescriptor) -> ShardResult<()> {
        let authoritative = self.authoritative_descriptor_for_key(key)?;
        self.validate_cached_descriptor(key, descriptor, &authoritative)
    }

    fn retry_put(&self, key: &[u8], value: Vec<u8>) -> ShardResult<u64> {
        let mut last_error = None;
        for attempt in 0..2 {
            match self.route_put_once(key, value.clone()) {
                Ok(index) => return Ok(index),
                Err(err) => {
                    if attempt == 0 && err.is_retryable() {
                        self.refresh_descriptor_for_key(key)?;
                        last_error = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        Err(last_error
            .unwrap_or_else(|| RangeRoutingError::Internal("routing retry exhausted".into())))
    }

    fn retry_delete(&self, key: &[u8]) -> ShardResult<u64> {
        let mut last_error = None;
        for attempt in 0..2 {
            match self.route_delete_once(key) {
                Ok(index) => return Ok(index),
                Err(err) => {
                    if attempt == 0 && err.is_retryable() {
                        self.refresh_descriptor_for_key(key)?;
                        last_error = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        Err(last_error
            .unwrap_or_else(|| RangeRoutingError::Internal("routing retry exhausted".into())))
    }

    fn retry_get(&self, key: &[u8]) -> ShardResult<Option<Vec<u8>>> {
        let mut last_error = None;
        for attempt in 0..2 {
            match self.route_get_once(key) {
                Ok(value) => return Ok(value),
                Err(err) => {
                    if attempt == 0 && err.is_retryable() {
                        self.refresh_descriptor_for_key(key)?;
                        last_error = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        Err(last_error
            .unwrap_or_else(|| RangeRoutingError::Internal("routing retry exhausted".into())))
    }

    fn route_put_once(&self, key: &[u8], value: Vec<u8>) -> ShardResult<u64> {
        let descriptor = self.route_descriptor_for_key(key)?;
        self.route_put_with_descriptor(key, value, descriptor)
    }

    fn route_delete_once(&self, key: &[u8]) -> ShardResult<u64> {
        let descriptor = self.route_descriptor_for_key(key)?;
        self.route_delete_with_descriptor(key, descriptor)
    }

    fn route_get_once(&self, key: &[u8]) -> ShardResult<Option<Vec<u8>>> {
        let descriptor = self.route_descriptor_for_key(key)?;
        self.route_get_with_descriptor(key, descriptor)
    }

    fn refresh_descriptor_for_key(&self, key: &[u8]) -> ShardResult<RangeDescriptor> {
        let descriptor = self.authoritative_descriptor_for_key(key)?;
        self.cached
            .lock()
            .unwrap()
            .insert(descriptor.range_id, descriptor.clone());
        Ok(descriptor)
    }
}

impl RangeRoutingError {
    fn is_retryable(&self) -> bool {
        matches!(
            self,
            RangeRoutingError::RangeMoved { .. }
                | RangeRoutingError::EpochMismatch { .. }
                | RangeRoutingError::NotLeader { .. }
        )
    }
}

fn validate_non_overlapping(descriptors: &[RangeDescriptor]) -> ShardResult<()> {
    for window in descriptors.windows(2) {
        let left = &window[0];
        let right = &window[1];
        match &left.end {
            Some(end) if end > &right.start => {
                return Err(RangeRoutingError::InvalidDescriptor(format!(
                    "ranges {} and {} overlap",
                    left.range_id, right.range_id
                )));
            }
            None => {
                return Err(RangeRoutingError::InvalidDescriptor(format!(
                    "range {} must be the last descriptor when end is open",
                    left.range_id
                )));
            }
            _ => {}
        }
    }
    Ok(())
}
