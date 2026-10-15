//! SQLite persistence: opening, pragmas, schema and migrations.
//!
//! All mailbox logic and SQL queries live in [`crate::bus`]; this module only
//! opens the database with the right on-disk permissions and brings the two
//! tables (`roster`, `messages`) up to the current schema.

use std::fmt;
use std::fs;
use std::io;
use std::path::Path;

use rusqlite::Connection;

/// Open (creating if needed) `{data_dir}/pending.sqlite` with mode `0700`
/// directory / `0600` file permissions, WAL journaling, and the mailbox
/// schema applied.
pub fn open(data_dir: &Path) -> Result<Connection, StoreError> {
    fs::create_dir_all(data_dir).map_err(StoreError::Io)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(data_dir, fs::Permissions::from_mode(0o700)).map_err(StoreError::Io)?;
    }

    let db_path = data_dir.join("pending.sqlite");
    let conn = Connection::open(&db_path).map_err(StoreError::Sqlite)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&db_path, fs::Permissions::from_mode(0o600)).map_err(StoreError::Io)?;
    }

    conn.busy_timeout(std::time::Duration::from_millis(5000))
        .map_err(StoreError::Sqlite)?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(StoreError::Sqlite)?;
    // NORMAL: WAL keeps the file consistent through a crash; at worst the
    // last commits roll back on power loss. Stronger durability buys nothing
    // here, since startup wipes the mailbox unless MENTISCHAT_PERSIST=1.
    conn.pragma_update(None, "synchronous", "NORMAL")
        .map_err(StoreError::Sqlite)?;
    // Overwrite deleted content with zeros instead of leaving it in free
    // pages: read and expired messages must not be recoverable from the file.
    conn.pragma_update(None, "secure_delete", "ON")
        .map_err(StoreError::Sqlite)?;

    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS roster (
          name TEXT PRIMARY KEY,
          last_seen TEXT NOT NULL,
          description TEXT
        );

        CREATE TABLE IF NOT EXISTS messages (
          id TEXT PRIMARY KEY,
          sender TEXT NOT NULL,
          recipient TEXT NOT NULL,
          body TEXT NOT NULL,
          created_at TEXT NOT NULL,
          seq INTEGER NOT NULL
        );
        "#,
    )
    .map_err(StoreError::Sqlite)?;

    migrate_roster(&conn).map_err(StoreError::Sqlite)?;
    migrate_messages(&conn).map_err(StoreError::Sqlite)?;
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_inbox ON messages (recipient, seq);
         CREATE INDEX IF NOT EXISTS idx_created ON messages (created_at);",
    )
    .map_err(StoreError::Sqlite)?;
    Ok(conn)
}

/// Brings a roster created by an older build up to the current shape: the
/// wake/nudge columns and their index are dropped and the `description`
/// column is added, keeping every row.
fn migrate_roster(conn: &Connection) -> Result<(), rusqlite::Error> {
    let mut stmt = conn.prepare("PRAGMA table_info(roster)")?;
    let cols: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    // The unique index must go before the column it covers. (DROP COLUMN needs
    // SQLite >= 3.35; the bundled build is well past that.)
    conn.execute("DROP INDEX IF EXISTS idx_roster_wake_id", [])?;
    for retired in ["window_title_match", "nudge_transport", "wake_id"] {
        if cols.iter().any(|c| c == retired) {
            conn.execute(&format!("ALTER TABLE roster DROP COLUMN {retired}"), [])?;
        }
    }
    if !cols.iter().any(|c| c == "description") {
        conn.execute("ALTER TABLE roster ADD COLUMN description TEXT", [])?;
    }
    Ok(())
}

/// Brings a `messages` table from the acknowledge-to-delete era up to the
/// delete-on-read shape: the `state` column (and the index over it) goes,
/// keeping every unread row. SQLite cannot drop a column a CHECK constraint
/// names, so the table is rebuilt in one transaction.
fn migrate_messages(conn: &Connection) -> Result<(), rusqlite::Error> {
    let has_state = conn
        .prepare("PRAGMA table_info(messages)")?
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .any(|c| c == "state");
    if !has_state {
        return Ok(());
    }
    conn.execute_batch(
        "BEGIN;
         CREATE TABLE messages_new (
           id TEXT PRIMARY KEY,
           sender TEXT NOT NULL,
           recipient TEXT NOT NULL,
           body TEXT NOT NULL,
           created_at TEXT NOT NULL,
           seq INTEGER NOT NULL
         );
         INSERT INTO messages_new (id, sender, recipient, body, created_at, seq)
           SELECT id, sender, recipient, body, created_at, seq FROM messages;
         DROP TABLE messages;
         ALTER TABLE messages_new RENAME TO messages;
         COMMIT;",
    )
}

/// Failure modes of opening/migrating the store.
#[derive(Debug)]
pub enum StoreError {
    /// Filesystem error creating directories or setting permissions.
    Io(io::Error),
    /// SQLite open/migration failure.
    Sqlite(rusqlite::Error),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "data dir: {e}"),
            Self::Sqlite(e) => write!(f, "sqlite: {e}"),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Sqlite(e) => Some(e),
        }
    }
}
