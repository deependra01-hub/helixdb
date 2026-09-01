use crate::{DbError, RaftCluster, RaftConfig};

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

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

#[derive(Clone)]
struct RangeGroup {
    cluster: Arc<RaftCluster>,
    split_lock: Arc<RwLock<()>>,
}

pub struct ShardedCluster {
    root: std::path::PathBuf,
    nodes_per_group: usize,
    raft_config: RaftConfig,
    split_threshold_entries: usize,
    groups: Mutex<BTreeMap<u64, RangeGroup>>,
    authoritative: Mutex<BTreeMap<u64, RangeDescriptor>>,
    cached: Mutex<BTreeMap<u64, RangeDescriptor>>,
    next_range_id: Mutex<u64>,
}

impl ShardedCluster {
    pub fn bootstrap_with_ranges(
        root: impl AsRef<Path>,
        descriptors: Vec<RangeDescriptor>,
        nodes_per_group: usize,
        raft_config: RaftConfig,
    ) -> ShardResult<Self> {
        Self::bootstrap_with_ranges_and_split_threshold(
            root,
            descriptors,
            nodes_per_group,
            raft_config,
            32,
        )
    }

    pub fn bootstrap_with_ranges_and_split_threshold(
        root: impl AsRef<Path>,
        descriptors: Vec<RangeDescriptor>,
        nodes_per_group: usize,
        raft_config: RaftConfig,
        split_threshold_entries: usize,
    ) -> ShardResult<Self> {
        if nodes_per_group == 0 {
            return Err(RangeRoutingError::InvalidDescriptor(
                "nodes_per_group must be greater than zero".into(),
            ));
        }
        if split_threshold_entries == 0 {
            return Err(RangeRoutingError::InvalidDescriptor(
                "split_threshold_entries must be greater than zero".into(),
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
        let mut next_range_id = 0u64;

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
            groups.insert(
                descriptor.range_id,
                RangeGroup {
                    cluster: Arc::new(cluster),
                    split_lock: Arc::new(RwLock::new(())),
                },
            );
            next_range_id = next_range_id.max(descriptor.range_id);
        }

        Ok(Self {
            root,
            nodes_per_group,
            raft_config,
            split_threshold_entries,
            groups: Mutex::new(groups),
            authoritative: Mutex::new(authoritative),
            cached: Mutex::new(BTreeMap::new()),
            next_range_id: Mutex::new(next_range_id.saturating_add(1)),
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
        self.range_group(range_id)
            .ok()
            .and_then(|group| group.cluster.leader_id())
    }

    pub fn group_leader_id(&self, range_id: u64) -> ShardResult<Option<u64>> {
        self.range_group(range_id).map(|group| group.cluster.leader_id())
    }

    pub fn kill_group_node(&self, range_id: u64, node_id: u64) -> ShardResult<()> {
        let group = self.range_group(range_id)?;
        group.cluster.kill_node(node_id)?;
        Ok(())
    }

    pub fn restart_group_node(&self, range_id: u64, node_id: u64) -> ShardResult<()> {
        let group = self.range_group(range_id)?;
        group.cluster.restart_node(node_id)?;
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

    pub fn split_range_at(
        &self,
        range_id: u64,
        boundary: impl Into<Vec<u8>>,
    ) -> ShardResult<(RangeDescriptor, RangeDescriptor)> {
        let group = self.range_group(range_id)?;
        let descriptor = self.descriptor(range_id)?;
        let _guard = group
            .split_lock
            .write()
            .map_err(|_| RangeRoutingError::Internal("split lock poisoned".into()))?;
        let entries = group.cluster.all_entries().map_err(RangeRoutingError::from)?;
        self.split_range_at_locked_descriptor(
            range_id,
            boundary.into(),
            &group,
            descriptor,
            entries,
        )
    }

    pub fn split_hot_range(
        &self,
        range_id: u64,
    ) -> ShardResult<(RangeDescriptor, RangeDescriptor)> {
        let group = self.range_group(range_id)?;
        let _guard = group
            .split_lock
            .write()
            .map_err(|_| RangeRoutingError::Internal("split lock poisoned".into()))?;
        let descriptor = self.descriptor(range_id)?;
        let entries = group.cluster.all_entries().map_err(RangeRoutingError::from)?;
        if entries.len() < 2 {
            return Err(RangeRoutingError::InvalidDescriptor(
                "range needs at least two keys before splitting".into(),
            ));
        }
        let split_index = entries.len() / 2;
        let boundary = entries
            .keys()
            .nth(split_index)
            .cloned()
            .ok_or_else(|| RangeRoutingError::Internal("failed to choose split boundary".into()))?;
        self.split_range_at_locked_descriptor(range_id, boundary, &group, descriptor, entries)
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
        let group = self.range_group(range_id)?;
        Ok(group.cluster.leader_id())
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
        self.range_group(range_id)
            .ok()
            .and_then(|group| group.cluster.leader_id())
    }

    fn route_put_with_descriptor(
        &self,
        key: &[u8],
        value: Vec<u8>,
        mut descriptor: RangeDescriptor,
    ) -> ShardResult<u64> {
        let group = self.range_group(descriptor.range_id)?;
        let index = {
            let _guard = group
                .split_lock
                .read()
                .map_err(|_| RangeRoutingError::Internal("split lock poisoned".into()))?;
            self.validate_route(key, &descriptor)?;
            let index = group
                .cluster
                .put(key.to_vec(), value)
                .map_err(RangeRoutingError::from)?;
            descriptor.leader_hint = group.cluster.leader_id();
            self.cached
                .lock()
                .unwrap()
                .insert(descriptor.range_id, descriptor.clone());
            index
        };
        self.maybe_auto_split_range(descriptor.range_id)?;
        Ok(index)
    }

    fn route_delete_with_descriptor(
        &self,
        key: &[u8],
        mut descriptor: RangeDescriptor,
    ) -> ShardResult<u64> {
        let group = self.range_group(descriptor.range_id)?;
        let _guard = group
            .split_lock
            .read()
            .map_err(|_| RangeRoutingError::Internal("split lock poisoned".into()))?;
        self.validate_route(key, &descriptor)?;
        let index = group
            .cluster
            .delete(key.to_vec())
            .map_err(RangeRoutingError::from)?;
        descriptor.leader_hint = group.cluster.leader_id();
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
        let group = self.range_group(descriptor.range_id)?;
        let _guard = group
            .split_lock
            .read()
            .map_err(|_| RangeRoutingError::Internal("split lock poisoned".into()))?;
        self.validate_route(key, &descriptor)?;
        let value = group
            .cluster
            .get(key.to_vec())
            .map_err(RangeRoutingError::from)?;
        let mut refreshed = descriptor.clone();
        refreshed.leader_hint = group.cluster.leader_id();
        self.cached
            .lock()
            .unwrap()
            .insert(refreshed.range_id, refreshed);
        Ok(value)
    }

    fn maybe_auto_split_range(&self, range_id: u64) -> ShardResult<()> {
        if self.split_threshold_entries == 0 {
            return Ok(());
        }
        let group = self.range_group(range_id)?;
        let _guard = group
            .split_lock
            .read()
            .map_err(|_| RangeRoutingError::Internal("split lock poisoned".into()))?;
        let entries = group.cluster.all_entries().map_err(RangeRoutingError::from)?;
        if entries.len() <= self.split_threshold_entries {
            return Ok(());
        }
        drop(_guard);
        let _ = self.split_hot_range(range_id)?;
        Ok(())
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

    fn range_group(&self, range_id: u64) -> ShardResult<RangeGroup> {
        self.groups
            .lock()
            .unwrap()
            .get(&range_id)
            .cloned()
            .ok_or(RangeRoutingError::UnknownRange(range_id))
    }

    fn next_split_range_id(&self) -> u64 {
        let mut next = self.next_range_id.lock().unwrap();
        let range_id = *next;
        *next = range_id.saturating_add(1);
        range_id
    }

    fn split_range_at_locked_descriptor(
        &self,
        range_id: u64,
        boundary: Vec<u8>,
        group: &RangeGroup,
        descriptor: RangeDescriptor,
        entries: BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> ShardResult<(RangeDescriptor, RangeDescriptor)> {
        if boundary <= descriptor.start {
            return Err(RangeRoutingError::InvalidDescriptor(
                "split boundary must be inside the range".into(),
            ));
        }
        if let Some(end) = &descriptor.end {
            if boundary >= *end {
                return Err(RangeRoutingError::InvalidDescriptor(
                    "split boundary must be before range end".into(),
                ));
            }
        }

        let mut left_items = Vec::new();
        let mut right_items = Vec::new();
        for (key, value) in entries {
            if key < boundary {
                left_items.push((key, value));
            } else {
                right_items.push((key, value));
            }
        }

        if left_items.is_empty() || right_items.is_empty() {
            return Err(RangeRoutingError::InvalidDescriptor(
                "split boundary must divide the keys".into(),
            ));
        }

        let new_range_id = self.next_split_range_id();
        let right_root = self.root.join(format!("range-{:016}", new_range_id));
        let right_cluster = Arc::new(RaftCluster::bootstrap_with_config(
            &right_root,
            self.nodes_per_group,
            self.raft_config.clone(),
        )?);

        for (key, value) in &right_items {
            right_cluster.put(key.clone(), value.clone())?;
        }
        for (key, _) in &right_items {
            group.cluster.delete(key.clone())?;
        }

        let left_descriptor = RangeDescriptor {
            range_id,
            start: descriptor.start.clone(),
            end: Some(boundary.clone()),
            epoch: descriptor.epoch + 1,
            raft_group_id: descriptor.raft_group_id,
            leader_hint: group.cluster.leader_id(),
        };
        let right_descriptor = RangeDescriptor {
            range_id: new_range_id,
            start: boundary.clone(),
            end: descriptor.end.clone(),
            epoch: descriptor.epoch + 1,
            raft_group_id: new_range_id,
            leader_hint: right_cluster.leader_id(),
        };

        {
            let mut authoritative = self.authoritative.lock().unwrap();
            authoritative.insert(range_id, left_descriptor.clone());
            authoritative.insert(new_range_id, right_descriptor.clone());
        }

        {
            let mut groups = self.groups.lock().unwrap();
            groups.insert(
                new_range_id,
                RangeGroup {
                    cluster: right_cluster,
                    split_lock: Arc::new(RwLock::new(())),
                },
            );
        }

        {
            let mut cache = self.cached.lock().unwrap();
            cache.insert(range_id, left_descriptor.clone());
            cache.insert(new_range_id, right_descriptor.clone());
        }

        Ok((left_descriptor, right_descriptor))
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
