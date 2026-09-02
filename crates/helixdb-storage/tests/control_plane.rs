use std::time::{Duration, Instant};

use helixdb_storage::{
    ControlPlane, ControlPlaneConfig, ControlPlaneError, NodeStatus, RaftConfig, RangeDescriptor,
};

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

fn control_plane_config() -> ControlPlaneConfig {
    ControlPlaneConfig {
        heartbeat_timeout: Duration::from_millis(220),
        suspect_timeout: Duration::from_millis(110),
    }
}

fn bootstrap_control_plane(dir: &tempfile::TempDir) -> ControlPlane {
    ControlPlane::bootstrap_with_config(
        dir.path(),
        3,
        3,
        vec![
            RangeDescriptor::new(1, b"a", Some(b"m"), 1, 1),
            RangeDescriptor::new(2, b"m", None::<Vec<u8>>, 1, 2),
        ],
        fast_config(),
        control_plane_config(),
    )
    .expect("bootstrap control plane")
}

#[test]
fn routes_through_the_control_plane() {
    let dir = temp_dir();
    let cp = bootstrap_control_plane(&dir);

    cp.route_put(b"apple", b"red").expect("put apple");
    cp.route_put(b"zulu", b"blue").expect("put zulu");

    assert_eq!(
        cp.route_get(b"apple").expect("get apple"),
        Some(b"red".to_vec())
    );
    assert_eq!(
        cp.route_get(b"zulu").expect("get zulu"),
        Some(b"blue".to_vec())
    );

    cp.route_delete(b"apple").expect("delete apple");
    assert_eq!(
        cp.route_get(b"apple").expect("get apple after delete"),
        None
    );
}

#[test]
fn refresh_from_metadata_reloads_registry_state() {
    let dir = temp_dir();
    let mut cp = bootstrap_control_plane(&dir);

    cp.metadata_cluster()
        .put(
            format!("node/{:016}", 42).into_bytes(),
            b"42|healthy|7|12345".to_vec(),
        )
        .expect("seed node metadata");
    cp.metadata_cluster()
        .put(
            format!("range/{:016}", 42).into_bytes(),
            b"42|616c706861|-|9|77|42|42".to_vec(),
        )
        .expect("seed range metadata");

    cp.refresh_from_metadata().expect("refresh");

    let node = cp.node_registry().get(&42).cloned().expect("node record");
    assert_eq!(node.node_id, 42);
    assert_eq!(node.capacity_units, 7);
    assert_eq!(node.last_heartbeat_ms, 12345);
    assert_eq!(node.status, NodeStatus::Healthy);

    let range = cp
        .range_registry()
        .get(&42)
        .cloned()
        .expect("range placement");
    assert_eq!(range.descriptor.range_id, 42);
    assert_eq!(range.descriptor.start, b"alpha".to_vec());
    assert_eq!(range.descriptor.epoch, 9);
    assert_eq!(range.descriptor.raft_group_id, 77);
    assert_eq!(range.descriptor.leader_hint, Some(42));
    assert_eq!(range.replicas, vec![42]);
}

#[test]
fn heartbeat_sweep_and_rebalance_update_placement() {
    let dir = temp_dir();
    let cp = bootstrap_control_plane(&dir);

    cp.register_node(1, 100).expect("register node 1");
    cp.register_node(2, 100).expect("register node 2");
    cp.register_node(3, 100).expect("register node 3");

    let placement = cp
        .register_range(
            RangeDescriptor {
                range_id: 7,
                start: b"aa".to_vec(),
                end: Some(b"zz".to_vec()),
                epoch: 1,
                raft_group_id: 7,
                leader_hint: Some(99),
            },
            vec![3, 1, 2],
        )
        .expect("register range");
    assert_eq!(placement.replicas, vec![3, 1, 2]);

    cp.set_last_heartbeat_for_test(1, 0).expect("age node 1");
    cp.set_last_heartbeat_for_test(2, 0).expect("age node 2");
    cp.set_last_heartbeat_for_test(3, u64::MAX)
        .expect("refresh node 3");
    cp.sweep_health().expect("sweep");

    assert_eq!(
        cp.node_registry().get(&1).expect("node 1").status,
        NodeStatus::Dead
    );
    assert_eq!(
        cp.node_registry().get(&2).expect("node 2").status,
        NodeStatus::Dead
    );
    assert_eq!(
        cp.node_registry().get(&3).expect("node 3").status,
        NodeStatus::Healthy
    );

    cp.rebalance().expect("rebalance");

    let range = cp.range_registry().get(&7).cloned().expect("range");
    assert_eq!(range.descriptor.leader_hint, Some(3));
    assert_eq!(range.descriptor.epoch, 2);
}

#[test]
fn range_registration_rejects_unknown_replicas() {
    let dir = temp_dir();
    let cp = bootstrap_control_plane(&dir);

    cp.register_node(1, 100).expect("register node");

    let err = cp
        .register_range(
            RangeDescriptor::new(9, b"a", None::<Vec<u8>>, 1, 9),
            vec![1, 9],
        )
        .expect_err("unknown replica");

    assert!(matches!(err, ControlPlaneError::UnknownNode(9)));
}

#[test]
fn add_and_remove_replica_keeps_cluster_serving() {
    let dir = temp_dir();
    let mut cp = bootstrap_control_plane(&dir);

    cp.metadata_cluster()
        .put(
            format!("range/{:016}", 1).into_bytes(),
            b"1|61|6d|1|1|-|1,2,3".to_vec(),
        )
        .expect("seed placement metadata");
    cp.refresh_from_metadata().expect("refresh");
    cp.register_node(4, 100).expect("register node 4");

    cp.route_put(b"apple", b"red").expect("seed apple");

    let added = cp.add_replica(1, 4).expect("add replica");
    assert_eq!(added.replicas, vec![1, 2, 3, 4]);

    wait_for_node_kv_len(&cp, 1, 4, 1).expect("node 4 catch up");

    cp.route_put(b"apricot", b"gold").expect("write while added");
    wait_for_node_kv_len(&cp, 1, 4, 2).expect("node 4 replicated writes");

    let removed = cp.remove_replica(1, 4).expect("remove replica");
    assert_eq!(removed.replicas, vec![1, 2, 3]);
    assert!(cp.data_cluster().node_state(1, 4).expect("node state").is_none());

    cp.route_put(b"banana", b"yellow")
        .expect("write after removal");
    assert_eq!(
        cp.route_get(b"banana").expect("get banana"),
        Some(b"yellow".to_vec())
    );
}

#[test]
fn timestamp_oracle_batches_are_monotonic_and_survive_leader_failover() {
    let dir = temp_dir();
    let cp = bootstrap_control_plane(&dir);

    let first = cp.allocate_timestamp_batch(5).expect("first batch");
    assert_eq!(first.start, 1);
    assert_eq!(first.end, 5);
    assert_eq!(first.len(), 5);
    assert_eq!(cp.current_timestamp(), 5);

    let leader = cp.metadata_cluster().leader_id().expect("metadata leader");
    cp.metadata_cluster()
        .kill_node(leader)
        .expect("kill metadata leader");

    let second = cp.allocate_timestamp_batch(3).expect("second batch");
    assert_eq!(second.start, 6);
    assert_eq!(second.end, 8);
    assert_eq!(cp.allocate_timestamp().expect("single timestamp"), 9);
    assert_eq!(cp.current_timestamp(), 9);
}

fn wait_for_node_kv_len(
    cp: &ControlPlane,
    range_id: u64,
    node_id: u64,
    min_len: usize,
) -> Option<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(Some(state)) = cp.data_cluster().node_state(range_id, node_id) {
            if state.kv_len >= min_len {
                return Some(());
            }
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}
