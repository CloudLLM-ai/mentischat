//! Store migration tests: schemas left mid-migration or written by older
//! implementations must still open cleanly.

use rusqlite::Connection;

#[test]
fn wake_id_column_without_index_gets_index_on_open() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("partial");

    // Simulate an older/interrupted schema: roster already has `wake_id` but
    // no unique index was ever created.
    std::fs::create_dir_all(&data).expect("dir");
    let conn = Connection::open(data.join("pending.sqlite")).expect("raw open");
    conn.execute_batch(
        r#"
        CREATE TABLE roster (
          name TEXT PRIMARY KEY,
          last_seen TEXT NOT NULL,
          window_title_match TEXT,
          nudge_transport TEXT NOT NULL DEFAULT 'none',
          wake_id TEXT
        );
        CREATE TABLE messages (
          id TEXT PRIMARY KEY,
          sender TEXT NOT NULL,
          recipient TEXT NOT NULL,
          body TEXT NOT NULL,
          created_at TEXT NOT NULL,
          seq INTEGER NOT NULL,
          state TEXT NOT NULL CHECK (state IN ('pending', 'in_flight'))
        );
        INSERT INTO roster (name, last_seen) VALUES ('legacy', '2026-01-01T00:00:00.000Z');
        "#,
    )
    .expect("seed legacy schema");
    let before: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index'
             AND name = 'idx_roster_wake_id'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(before, 0, "precondition: index missing");
    drop(conn);

    mentischat::store::open(&data).expect("open migrates");

    let conn = Connection::open(data.join("pending.sqlite")).expect("reopen");
    let after: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index'
             AND name = 'idx_roster_wake_id'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(after, 1, "migration recreated the wake_id index");
}

#[test]
fn fully_migrated_db_reopens_without_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("roundtrip");
    drop(mentischat::store::open(&data).expect("first open"));
    mentischat::store::open(&data).expect("second open is a no-op migration");
}
