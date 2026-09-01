use std::thread;
use std::time::Duration;

use helixdb_storage::{RaftCluster, RaftConfig};

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

#[test]
fn leader_failover_keeps_committed_values() {
    let dir = temp_dir();
    let cluster =
        RaftCluster::bootstrap_with_config(dir.path(), 3, fast_config()).expect("bootstrap");

    let leader = cluster
        .wait_for_leader(Duration::from_secs(5))
        .expect("leader");
    cluster.put(b"alpha", b"one").expect("put alpha");
    assert_eq!(
        cluster.get(b"alpha").expect("get alpha"),
        Some(b"one".to_vec())
    );

    cluster.kill_node(leader).expect("kill leader");

    let new_leader = cluster
        .wait_for_leader(Duration::from_secs(5))
        .expect("new leader");
    assert_ne!(leader, new_leader);

    assert_eq!(
        cluster.get(b"alpha").expect("get alpha after failover"),
        Some(b"one".to_vec())
    );
    cluster.put(b"beta", b"two").expect("put beta");
    assert_eq!(
        cluster.get(b"beta").expect("get beta"),
        Some(b"two".to_vec())
    );
}

#[test]
fn cluster_restart_recovers_committed_values() {
    let dir = temp_dir();

    {
        let cluster =
            RaftCluster::bootstrap_with_config(dir.path(), 3, fast_config()).expect("bootstrap");
        cluster
            .wait_for_leader(Duration::from_secs(5))
            .expect("leader");
        cluster.put(b"persist", b"yes").expect("put");
        cluster.put(b"durable", b"also").expect("put");
        assert_eq!(
            cluster.get(b"persist").expect("get persist"),
            Some(b"yes".to_vec())
        );
        assert_eq!(
            cluster.get(b"durable").expect("get durable"),
            Some(b"also".to_vec())
        );
    }

    let cluster =
        RaftCluster::bootstrap_with_config(dir.path(), 3, fast_config()).expect("rebootstrap");
    cluster
        .wait_for_leader(Duration::from_secs(5))
        .expect("leader after restart");

    assert_eq!(
        cluster.get(b"persist").expect("get persist after restart"),
        Some(b"yes".to_vec())
    );
    assert_eq!(
        cluster.get(b"durable").expect("get durable after restart"),
        Some(b"also".to_vec())
    );
}

#[test]
fn snapshots_compact_logs_and_survive_restart() {
    let dir = temp_dir();

    {
        let cluster =
            RaftCluster::bootstrap_with_config(dir.path(), 3, fast_config()).expect("bootstrap");
        cluster
            .wait_for_leader(Duration::from_secs(5))
            .expect("leader");

        for i in 0..16 {
            cluster
                .put(format!("snap-{i}"), format!("value-{i}"))
                .expect("put");
        }

        let leader = cluster.leader_id().expect("leader id");
        let state = cluster.node_state(leader).expect("leader state");
        assert!(state.snapshot_index >= 8);
        assert!(state.log_len <= 8);
        assert_eq!(
            cluster.get(b"snap-0").expect("get first"),
            Some(b"value-0".to_vec())
        );
        assert_eq!(
            cluster.get(b"snap-15").expect("get last"),
            Some(b"value-15".to_vec())
        );
    }

    let cluster =
        RaftCluster::bootstrap_with_config(dir.path(), 3, fast_config()).expect("rebootstrap");
    cluster
        .wait_for_leader(Duration::from_secs(5))
        .expect("leader after restart");
    assert_eq!(
        cluster.get(b"snap-0").expect("get first after restart"),
        Some(b"value-0".to_vec())
    );
    assert_eq!(
        cluster.get(b"snap-15").expect("get last after restart"),
        Some(b"value-15".to_vec())
    );
}

#[test]
fn restarted_node_rejoins_the_cluster() {
    let dir = temp_dir();
    let cluster =
        RaftCluster::bootstrap_with_config(dir.path(), 3, fast_config()).expect("bootstrap");

    let leader = cluster
        .wait_for_leader(Duration::from_secs(5))
        .expect("leader");
    let follower = [1u64, 2, 3]
        .into_iter()
        .find(|id| *id != leader)
        .expect("follower");

    cluster.put(b"join", b"me").expect("put");
    cluster.kill_node(follower).expect("kill follower");
    thread::sleep(Duration::from_millis(150));
    cluster.restart_node(follower).expect("restart follower");

    thread::sleep(Duration::from_millis(300));
    let state = cluster.node_state(follower).expect("node state");
    assert!(state.commit_index >= 1);
    assert!(state.kv_len >= 1);
    assert_eq!(
        cluster.get(b"join").expect("get join"),
        Some(b"me".to_vec())
    );
}

#[test]
fn lagging_follower_receives_snapshot_after_rejoin() {
    let dir = temp_dir();
    let cluster =
        RaftCluster::bootstrap_with_config(dir.path(), 3, fast_config()).expect("bootstrap");

    let leader = cluster
        .wait_for_leader(Duration::from_secs(5))
        .expect("leader");
    let follower = [1u64, 2, 3]
        .into_iter()
        .find(|id| *id != leader)
        .expect("follower");

    cluster.kill_node(follower).expect("kill follower");
    for i in 0..18 {
        cluster
            .put(format!("lag-{i}"), format!("value-{i}"))
            .expect("put");
    }

    let leader_state = cluster.node_state(leader).expect("leader state");
    assert!(leader_state.snapshot_index >= 8);

    cluster.restart_node(follower).expect("restart follower");
    thread::sleep(Duration::from_millis(250));
    cluster.put(b"trigger", b"snapshot-sync").expect("trigger");

    thread::sleep(Duration::from_millis(300));
    let follower_state = cluster.node_state(follower).expect("follower state");
    assert!(follower_state.snapshot_index >= leader_state.snapshot_index);
    assert_eq!(
        cluster.get(b"lag-0").expect("get lag-0"),
        Some(b"value-0".to_vec())
    );
    assert_eq!(
        cluster.get(b"trigger").expect("get trigger"),
        Some(b"snapshot-sync".to_vec())
    );
}
