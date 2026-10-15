//! Store migration tests: databases written by older builds, including ones
//! left mid-migration, must open cleanly and keep their rows.

use rusqlite::Connection;

const MESSAGES_TABLE: &str = r#"
    CREATE TABLE messages (
      id TEXT PRIMARY KEY,
      sender TEXT NOT NULL,
      recipient TEXT NOT NULL,
      body TEXT NOT NULL,
      created_at TEXT NOT NULL,
      seq INTEGER NOT NULL,
      state TEXT NOT NULL CHECK (state IN ('pending', 'in_flight'))
    );
"#;

fn roster_columns(conn: &Connection) -> Vec<String> {
    let mut stmt = conn.prepare("PRAGMA table_info(roster)").unwrap();
    stmt.query_map([], |r| r.get::<_, String>(1))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn index_count(conn: &Connection, name: &str) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
        [name],
        |r| r.get(0),
    )
    .unwrap()
}

/// Seeds `pending.sqlite` under a fresh temp dir with `schema`, returning the
/// data dir to hand to `store::open`.
fn seeded(tag: &str, schema: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join(tag);
    std::fs::create_dir_all(&data).expect("dir");
    let conn = Connection::open(data.join("pending.sqlite")).expect("raw open");
    conn.execute_batch(schema).expect("seed legacy schema");
    (dir, data)
}

#[test]
fn a_fresh_database_has_only_name_and_last_seen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("fresh");
    drop(mentischat::store::open(&data).expect("open"));

    let conn = Connection::open(data.join("pending.sqlite")).expect("reopen");
    assert_eq!(roster_columns(&conn), ["name", "last_seen"]);
}

#[test]
fn wake_columns_and_their_index_are_dropped_keeping_rows() {
    let schema = format!(
        r#"
        CREATE TABLE roster (
          name TEXT PRIMARY KEY,
          last_seen TEXT NOT NULL,
          nudge_transport TEXT NOT NULL DEFAULT 'none',
          wake_id TEXT
        );
        CREATE UNIQUE INDEX idx_roster_wake_id ON roster (wake_id)
          WHERE wake_id IS NOT NULL AND length(trim(wake_id)) > 0;
        {MESSAGES_TABLE}
        INSERT INTO roster (name, last_seen, nudge_transport, wake_id)
          VALUES ('legacy', '2026-01-01T00:00:00.000Z', 'wrapper',
                  '11111111-1111-4111-8111-111111111111'),
                 ('plain', '2026-01-01T00:00:00.000Z', 'none', NULL);
        INSERT INTO messages VALUES
          ('m1', 'plain', 'legacy', 'queued before upgrade',
           '2026-01-01T00:00:00.000Z', 1, 'pending');
        "#
    );
    let (_dir, data) = seeded("wake", &schema);

    mentischat::store::open(&data).expect("open migrates");

    let conn = Connection::open(data.join("pending.sqlite")).expect("reopen");
    assert_eq!(roster_columns(&conn), ["name", "last_seen"]);
    assert_eq!(index_count(&conn, "idx_roster_wake_id"), 0);
    let names: Vec<String> = conn
        .prepare("SELECT name FROM roster ORDER BY name")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(names, ["legacy", "plain"], "roster rows survive");
    let body: String = conn
        .query_row("SELECT body FROM messages WHERE id = 'm1'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(body, "queued before upgrade", "queued mail survives");
}

#[test]
fn a_column_dropped_but_index_left_behind_still_opens() {
    let schema = format!(
        r#"
        CREATE TABLE roster (
          name TEXT PRIMARY KEY,
          last_seen TEXT NOT NULL,
          wake_id TEXT
        );
        CREATE UNIQUE INDEX idx_roster_wake_id ON roster (wake_id);
        {MESSAGES_TABLE}
        INSERT INTO roster (name, last_seen) VALUES ('legacy', '2026-01-01T00:00:00.000Z');
        "#
    );
    let (_dir, data) = seeded("partial", &schema);

    mentischat::store::open(&data).expect("index is dropped before its column");

    let conn = Connection::open(data.join("pending.sqlite")).expect("reopen");
    assert_eq!(roster_columns(&conn), ["name", "last_seen"]);
}

#[test]
fn v1_window_title_column_is_retired() {
    let schema = format!(
        r#"
        CREATE TABLE roster (
          name TEXT PRIMARY KEY,
          last_seen TEXT NOT NULL,
          window_title_match TEXT
        );
        {MESSAGES_TABLE}
        INSERT INTO roster (name, last_seen, window_title_match)
          VALUES ('legacy', '2026-01-01T00:00:00.000Z', 'Terminal — grok');
        "#
    );
    let (_dir, data) = seeded("v1", &schema);

    mentischat::store::open(&data).expect("open migrates a v1 database");

    let conn = Connection::open(data.join("pending.sqlite")).expect("reopen");
    assert_eq!(roster_columns(&conn), ["name", "last_seen"]);
}

#[test]
fn fully_migrated_db_reopens_without_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("roundtrip");
    drop(mentischat::store::open(&data).expect("first open"));
    mentischat::store::open(&data).expect("second open is a no-op migration");
}
