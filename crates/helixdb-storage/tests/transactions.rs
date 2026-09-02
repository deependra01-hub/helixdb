use std::thread;

use helixdb_storage::{MvccTransactionError, TransactionalMvccDb};

fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

#[test]
fn snapshot_isolation_keeps_reads_stable_across_concurrent_commit() {
    let dir = temp_dir();
    let db = TransactionalMvccDb::open(dir.path()).expect("open");

    let mut seed = db.begin_transaction();
    seed.put(b"acct", b"v1");
    seed.commit().expect("seed commit");

    let reader = db.begin_transaction();
    assert_eq!(reader.get(b"acct").expect("reader get"), Some(b"v1".to_vec()));

    let writer = db.begin_transaction();
    let handle = thread::spawn(move || {
        let mut writer = writer;
        writer.put(b"acct", b"v2");
        writer.commit().expect("writer commit");
    });

    handle.join().expect("writer join");

    assert_eq!(
        reader.get(b"acct").expect("reader get after writer"),
        Some(b"v1".to_vec())
    );

    let post_commit = db.begin_transaction();
    assert_eq!(
        post_commit.get(b"acct").expect("post-commit get"),
        Some(b"v2".to_vec())
    );
}

#[test]
fn write_write_conflicts_abort_late_writer() {
    let dir = temp_dir();
    let db = TransactionalMvccDb::open(dir.path()).expect("open");

    let mut seed = db.begin_transaction();
    seed.put(b"balance", b"100");
    seed.commit().expect("seed commit");

    let mut first = db.begin_transaction();
    let mut second = db.begin_transaction();
    assert_eq!(first.get(b"balance").expect("first read"), Some(b"100".to_vec()));
    assert_eq!(second.get(b"balance").expect("second read"), Some(b"100".to_vec()));

    first.put(b"balance", b"90");
    let first_commit_ts = first.commit().expect("first commit");
    assert!(first_commit_ts > 0);

    second.put(b"balance", b"80");
    let err = second.commit().expect_err("second conflict");
    assert!(matches!(
        err,
        MvccTransactionError::WriteConflict { start_ts: _, .. }
    ));

    let verify = db.begin_transaction();
    assert_eq!(
        verify.get(b"balance").expect("verify balance"),
        Some(b"90".to_vec())
    );
}

#[test]
fn rollback_discards_buffered_writes() {
    let dir = temp_dir();
    let db = TransactionalMvccDb::open(dir.path()).expect("open");

    let mut tx = db.begin_transaction();
    tx.put(b"key", b"value");
    tx.rollback();

    let verify = db.begin_transaction();
    assert_eq!(verify.get(b"key").expect("verify"), None);
}
