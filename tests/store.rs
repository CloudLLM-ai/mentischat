//! Store tests: the schema a fresh database gets, and that reopening keeps it.

use rusqlite::Connection;

fn columns(conn: &Connection, table: &str) -> Vec<String> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .unwrap();
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

#[test]
fn a_fresh_database_has_the_roster_and_messages_schema() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("fresh");
    drop(mentischat::store::open(&data).expect("open"));

    let conn = Connection::open(data.join("pending.sqlite")).expect("reopen");
    assert_eq!(
        columns(&conn, "roster"),
        ["name", "last_seen", "description"]
    );
    assert_eq!(
        columns(&conn, "messages"),
        ["id", "sender", "recipient", "body", "created_at", "seq"]
    );
    for index in ["idx_inbox", "idx_created"] {
        assert_eq!(index_count(&conn, index), 1, "{index}");
    }
}

#[test]
fn reopening_an_existing_database_keeps_its_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("reopen");
    let conn = mentischat::store::open(&data).expect("first open");
    conn.execute(
        "INSERT INTO roster (name, last_seen, description) VALUES ('a', 't', 'd')",
        [],
    )
    .unwrap();
    drop(conn);

    let conn = mentischat::store::open(&data).expect("second open");
    let kept: i64 = conn
        .query_row("SELECT COUNT(*) FROM roster", [], |r| r.get(0))
        .unwrap();
    assert_eq!(kept, 1);
}

#[test]
fn an_older_table_shape_is_refused_and_reset_clears_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("old");
    std::fs::create_dir_all(&data).unwrap();
    Connection::open(data.join("pending.sqlite"))
        .unwrap()
        .execute_batch(
            "CREATE TABLE roster (name TEXT PRIMARY KEY, last_seen TEXT NOT NULL, wake_id TEXT);",
        )
        .unwrap();

    match mentischat::store::open(&data) {
        Err(mentischat::store::StoreError::Schema { table, found }) => {
            assert_eq!(table, "roster");
            assert_eq!(found, ["name", "last_seen", "wake_id"]);
        }
        other => panic!("expected a schema error, got {:?}", other.map(|_| ())),
    }

    mentischat::store::reset(&data).expect("reset");
    mentischat::store::reset(&data).expect("reset of missing files is fine");
    drop(mentischat::store::open(&data).expect("fresh open after reset"));
}
