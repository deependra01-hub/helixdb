use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, RwLock};
use std::thread;
use std::time::Duration;

use helixdb_storage::MvccDb;

fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

#[test]
fn snapshot_reads_are_stable() {
    let dir = temp_dir();
    let mut db = MvccDb::open(dir.path()).expect("open");

    db.put_at(b"acct", b"v1", 10).expect("put");
    db.put_at(b"acct", b"v2", 20).expect("put");
    db.delete_at(b"acct", 30).expect("delete");

    let snap10 = db.snapshot(10);
    let snap20 = db.snapshot(20);
    let snap30 = db.snapshot(30);

    assert_eq!(snap10.timestamp(), 10);
    assert_eq!(db.get_at(b"acct", snap10.timestamp()).expect("get"), Some(b"v1".to_vec()));
    assert_eq!(db.get_at(b"acct", snap20.timestamp()).expect("get"), Some(b"v2".to_vec()));
    assert_eq!(db.get_at(b"acct", snap30.timestamp()).expect("get"), None);
}

#[test]
fn mvcc_gc_drops_obsolete_versions() {
    let dir = temp_dir();
    let mut db = MvccDb::open(dir.path()).expect("open");

    db.put_at(b"acct", b"v1", 10).expect("put");
    db.put_at(b"acct", b"v2", 20).expect("put");
    db.delete_at(b"acct", 30).expect("delete");

    let removed = db.gc(20).expect("gc");
    assert_eq!(removed, 1);

    assert_eq!(db.get_at(b"acct", 10).expect("get"), None);
    assert_eq!(db.get_at(b"acct", 20).expect("get"), Some(b"v2".to_vec()));
    assert_eq!(db.get_at(b"acct", 30).expect("get"), None);

    let versions = db.versions_for_key(b"acct").expect("versions");
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].timestamp, 20);
    assert_eq!(versions[1].timestamp, 30);
}

#[test]
fn concurrent_readers_and_writers_respect_snapshots() {
    let dir = temp_dir();
    let db = Arc::new(RwLock::new(MvccDb::open(dir.path()).expect("open")));
    db.write().unwrap().put_at(b"counter", b"v1", 1).expect("seed");

    let barrier = Arc::new(Barrier::new(4));
    let ready_for_reads = Arc::new(AtomicBool::new(false));
    let writer_db = Arc::clone(&db);
    let writer_barrier = Arc::clone(&barrier);
    let writer_ready = Arc::clone(&ready_for_reads);
    let writer = thread::spawn(move || {
        writer_barrier.wait();
        for ts in 2..=10 {
            writer_db
                .write()
                .unwrap()
                .put_at(b"counter", format!("v{ts}"), ts)
                .expect("put");
            if ts == 5 {
                writer_ready.store(true, Ordering::Release);
                thread::sleep(Duration::from_millis(25));
            }
        }
    });

    let mut readers = Vec::new();
    for snapshot_ts in [1u64, 3, 5] {
        let reader_db = Arc::clone(&db);
        let reader_barrier = Arc::clone(&barrier);
        let reader_ready = Arc::clone(&ready_for_reads);
        readers.push(thread::spawn(move || {
            reader_barrier.wait();
            while !reader_ready.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(1));
            }
            let value = reader_db.read().unwrap().get_at(b"counter", snapshot_ts).expect("get");
            match snapshot_ts {
                1 => assert_eq!(value, Some(b"v1".to_vec())),
                3 => assert_eq!(value, Some(b"v3".to_vec())),
                5 => assert_eq!(value, Some(b"v5".to_vec())),
                _ => unreachable!(),
            }
        }));
    }

    writer.join().expect("writer");
    for reader in readers {
        reader.join().expect("reader");
    }
}
