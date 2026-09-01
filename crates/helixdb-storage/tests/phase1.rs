use helixdb_storage::{Db, DbOptions};

fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

#[test]
fn writes_survive_restart() {
    let dir = temp_dir();

    {
        let mut db = Db::open(dir.path()).expect("open");
        db.put(b"alpha", b"one").expect("put");
        db.put(b"beta", b"two").expect("put");
        db.delete(b"beta").expect("delete");
    }

    let db = Db::open(dir.path()).expect("reopen");
    assert_eq!(db.get(b"alpha").expect("get"), Some(b"one".to_vec()));
    assert_eq!(db.get(b"beta").expect("get"), None);
}

#[test]
fn flush_and_compaction_preserve_latest_values() {
    let dir = temp_dir();
    let mut options = DbOptions::default();
    options.memtable_flush_threshold = 64;
    options.sstable_block_size = 64;

    let mut db = Db::open_with_options(dir.path(), options).expect("open");
    for i in 0..20 {
        db.put(format!("key-{i}"), format!("value-{i}"))
            .expect("put");
    }
    db.flush().expect("flush");
    for i in 10..30 {
        db.put(format!("key-{i}"), format!("value-{i}-new"))
            .expect("put");
    }
    db.flush().expect("flush");
    db.compact().expect("compact");

    assert_eq!(db.get(b"key-0").expect("get"), Some(b"value-0".to_vec()));
    assert_eq!(
        db.get(b"key-15").expect("get"),
        Some(b"value-15-new".to_vec())
    );
    assert_eq!(
        db.get(b"key-29").expect("get"),
        Some(b"value-29-new".to_vec())
    );
}

#[test]
fn repeated_deletes_remain_tombstoned_after_restart() {
    let dir = temp_dir();

    {
        let mut db = Db::open(dir.path()).expect("open");
        db.put(b"dead", b"alive").expect("put");
        db.flush().expect("flush");
        db.delete(b"dead").expect("delete");
    }

    let db = Db::open(dir.path()).expect("reopen");
    assert_eq!(db.get(b"dead").expect("get"), None);
}
