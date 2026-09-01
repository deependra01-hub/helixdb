use std::sync::Arc;
use std::thread;
use std::time::Duration;

use helixdb_storage::{RaftConfig, RangeDescriptor, RangeRoutingError, ShardedCluster};

fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

fn fast_config() -> RaftConfig {
    RaftConfig {
        election_timeout_min: Duration::from_millis(120),
        election_timeout_max: Duration::from_millis(220),
        heartbeat_interval: Duration::from_millis(30),
        rpc_timeout: Duration::from_millis(150),
        snapshot_threshold_entries: 8,
    }
}

fn two_range_cluster(dir: &tempfile::TempDir) -> ShardedCluster {
    ShardedCluster::bootstrap_with_ranges(
        dir.path(),
        vec![
            RangeDescriptor::new(1, b"a", Some(b"m"), 1, 1),
            RangeDescriptor::new(2, b"m", None::<Vec<u8>>, 1, 2),
        ],
        3,
        fast_config(),
    )
    .expect("bootstrap")
}

fn split_ready_cluster(dir: &tempfile::TempDir) -> ShardedCluster {
    ShardedCluster::bootstrap_with_ranges_and_split_threshold(
        dir.path(),
        vec![
            RangeDescriptor::new(1, b"a", Some(b"m"), 1, 1),
            RangeDescriptor::new(2, b"m", None::<Vec<u8>>, 1, 2),
        ],
        3,
        fast_config(),
        2,
    )
    .expect("bootstrap")
}

#[test]
fn routes_keys_to_multiple_ranges() {
    let dir = temp_dir();
    let cluster = two_range_cluster(&dir);

    cluster.route_put(b"apple", b"red").expect("put apple");
    cluster.route_put(b"zulu", b"blue").expect("put zulu");

    let apple = cluster
        .route_descriptor_for_key(b"apple")
        .expect("descriptor");
    let zulu = cluster
        .route_descriptor_for_key(b"zulu")
        .expect("descriptor");
    assert_eq!(apple.range_id, 1);
    assert_eq!(zulu.range_id, 2);

    assert_eq!(
        cluster.get(b"apple").expect("get apple"),
        Some(b"red".to_vec())
    );
    assert_eq!(
        cluster.get(b"zulu").expect("get zulu"),
        Some(b"blue".to_vec())
    );
}

#[test]
fn stale_epoch_triggers_refresh_and_retry() {
    let dir = temp_dir();
    let cluster = two_range_cluster(&dir);

    cluster.route_put(b"beta", b"one").expect("prime cache");
    cluster.bump_range_epoch(1).expect("bump epoch");

    let err = cluster
        .route_put(b"beta", b"two")
        .expect_err("stale descriptor");
    assert!(matches!(
        err,
        RangeRoutingError::EpochMismatch {
            range_id: 1,
            cached_epoch: 1,
            current_epoch: 2,
        }
    ));

    cluster.put(b"beta", b"two").expect("retry put");
    assert_eq!(
        cluster.get(b"beta").expect("get beta"),
        Some(b"two".to_vec())
    );
}

#[test]
fn boundary_move_reports_range_moved() {
    let dir = temp_dir();
    let cluster = two_range_cluster(&dir);

    cluster.route_put(b"beta", b"one").expect("prime cache");
    cluster.move_boundary(1, 2, b"b").expect("move boundary");

    let err = cluster.route_put(b"beta", b"two").expect_err("stale route");
    assert!(matches!(
        err,
        RangeRoutingError::RangeMoved {
            range_id: 1,
            current_range_id: 2,
        }
    ));

    cluster.put(b"beta", b"two").expect("retry put");
    let descriptor = cluster
        .route_descriptor_for_key(b"beta")
        .expect("descriptor");
    assert_eq!(descriptor.range_id, 2);
    assert_eq!(
        cluster.get(b"beta").expect("get beta"),
        Some(b"two".to_vec())
    );
}

#[test]
fn independent_groups_keep_working_after_one_leader_fails() {
    let dir = temp_dir();
    let cluster = two_range_cluster(&dir);

    cluster.put(b"apple", b"red").expect("seed group 1");
    cluster.put(b"zulu", b"blue").expect("seed group 2");

    let leader = cluster
        .group_leader_id(1)
        .expect("group leader")
        .expect("leader");
    cluster.kill_group_node(1, leader).expect("kill leader");
    thread::sleep(Duration::from_millis(200));

    cluster.put(b"apple", b"green").expect("group 1 retry");
    cluster.put(b"zulu", b"cyan").expect("group 2 still alive");

    assert_eq!(
        cluster.get(b"apple").expect("get apple"),
        Some(b"green".to_vec())
    );
    assert_eq!(
        cluster.get(b"zulu").expect("get zulu"),
        Some(b"cyan".to_vec())
    );
}

#[test]
fn hot_range_auto_splits_on_write_threshold() {
    let dir = temp_dir();
    let cluster = split_ready_cluster(&dir);

    cluster.put(b"alpha", b"one").expect("put alpha");
    cluster.put(b"bravo", b"two").expect("put bravo");
    cluster.put(b"charlie", b"three").expect("put charlie");

    let left = cluster.descriptor(1).expect("left descriptor");
    assert!(left.end.is_some());
    let right = cluster
        .authoritative_descriptor_for_key(b"charlie")
        .expect("right descriptor");
    assert_ne!(right.range_id, 1);

    assert_eq!(
        cluster.get(b"alpha").expect("get alpha"),
        Some(b"one".to_vec())
    );
    assert_eq!(
        cluster.get(b"bravo").expect("get bravo"),
        Some(b"two".to_vec())
    );
    assert_eq!(
        cluster.get(b"charlie").expect("get charlie"),
        Some(b"three".to_vec())
    );
}

#[test]
fn requests_continue_while_split_is_in_flight() {
    let dir = temp_dir();
    let cluster = Arc::new(two_range_cluster(&dir));

    cluster.put(b"alpha", b"one").expect("seed alpha");
    cluster.put(b"bravo", b"two").expect("seed bravo");
    cluster.put(b"charlie", b"three").expect("seed charlie");

    let split_cluster = Arc::clone(&cluster);
    let handle = thread::spawn(move || {
        split_cluster
            .split_range_at(1, b"b")
            .expect("explicit split");
    });

    cluster.route_put(b"delta", b"four").expect("write during split");
    handle.join().expect("split thread");

    assert_eq!(
        cluster.get(b"delta").expect("get delta"),
        Some(b"four".to_vec())
    );
    assert_ne!(
        cluster
            .authoritative_descriptor_for_key(b"charlie")
            .expect("descriptor")
            .range_id,
        1
    );
}
