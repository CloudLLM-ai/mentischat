//! SQLite persistence: opening, pragmas and schema.
//!
//! All mailbox logic and SQL queries live in [`crate::bus`]; this module only
//! opens the database with the right on-disk permissions and creates the two
//! tables (`roster`, `messages`) if they are missing. A database whose tables
//! have a different shape is refused with [`StoreError::Schema`], never
//! upgraded; [`reset`] deletes it.

use std::fmt;
use std::fs;
use std::io;
use std::path::Path;

use rusqlite::Connection;

/// Open (creating if needed) `{data_dir}/pending.sqlite` with mode `0700`
/// directory / `0600` file permissions, WAL journaling, and the mailbox
/// schema applied. Names in `roster.name`, `messages.sender` and
/// `messages.recipient` compare without regard to ASCII case.
pub fn open(data_dir: &Path) -> Result<Connection, StoreError> {
    fs::create_dir_all(data_dir).map_err(StoreError::Io)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(data_dir, fs::Permissions::from_mode(0o700)).map_err(StoreError::Io)?;
    }

    let db_path = data_dir.join(DB_FILE);
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
    // pages: expired, discarded and pruned messages must not be recoverable
    // from the file.
    conn.pragma_update(None, "secure_delete", "ON")
        .map_err(StoreError::Sqlite)?;

    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS roster (
          name TEXT PRIMARY KEY COLLATE NOCASE,
          provider TEXT,
          description TEXT,
          registered_at TEXT NOT NULL,
          last_seen TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS messages (
          id TEXT PRIMARY KEY,
          sender TEXT NOT NULL COLLATE NOCASE,
          recipient TEXT NOT NULL COLLATE NOCASE,
          body TEXT NOT NULL,
          created_at TEXT NOT NULL,
          read_at TEXT,
          seq INTEGER NOT NULL
        );
        "#,
    )
    .map_err(StoreError::Sqlite)?;

    for (table, expected) in SCHEMA {
        let found = columns(&conn, table).map_err(StoreError::Sqlite)?;
        if found != *expected {
            return Err(StoreError::Schema { table, found });
        }
    }
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_inbox ON messages (recipient, seq);
         CREATE INDEX IF NOT EXISTS idx_sent ON messages (sender, seq);
         CREATE INDEX IF NOT EXISTS idx_read ON messages (read_at);",
    )
    .map_err(StoreError::Sqlite)?;
    Ok(conn)
}

/// The columns each table must have, in order.
const SCHEMA: [(&str, &[&str]); 2] = [
    (
        "roster",
        &[
            "name",
            "provider",
            "description",
            "registered_at",
            "last_seen",
        ],
    ),
    (
        "messages",
        &[
            "id",
            "sender",
            "recipient",
            "body",
            "created_at",
            "read_at",
            "seq",
        ],
    ),
];

fn columns(conn: &Connection, table: &str) -> Result<Vec<String>, rusqlite::Error> {
    conn.prepare(&format!("PRAGMA table_info({table})"))?
        .query_map([], |r| r.get::<_, String>(1))?
        .collect()
}

/// Open a second connection to the database [`open`] created, for work that
/// runs outside the bus lock (truncating the write-ahead log).
pub fn open_side(data_dir: &Path) -> Result<Connection, StoreError> {
    let conn = Connection::open(data_dir.join(DB_FILE)).map_err(StoreError::Sqlite)?;
    conn.busy_timeout(std::time::Duration::from_millis(5000))
        .map_err(StoreError::Sqlite)?;
    Ok(conn)
}

/// Delete the database and its `-wal` and `-shm` files under `data_dir`, so
/// the next [`open`] starts empty. Files that do not exist are skipped.
///
/// # Errors
///
/// Any filesystem error other than a missing file.
pub fn reset(data_dir: &Path) -> Result<(), StoreError> {
    for suffix in ["", "-wal", "-shm"] {
        match fs::remove_file(data_dir.join(format!("{DB_FILE}{suffix}"))) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(StoreError::Io(e)),
            _ => {}
        }
    }
    Ok(())
}

const DB_FILE: &str = "pending.sqlite";

/// Failure modes of opening the store.
#[derive(Debug)]
pub enum StoreError {
    /// Filesystem error creating directories or setting permissions.
    Io(io::Error),
    /// SQLite open or schema failure.
    Sqlite(rusqlite::Error),
    /// A table exists with columns other than the current ones, usually a
    /// database written by an older build.
    Schema {
        /// The table whose columns differ.
        table: &'static str,
        /// Its columns as found.
        found: Vec<String>,
    },
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "data dir: {e}"),
            Self::Sqlite(e) => write!(f, "sqlite: {e}"),
            Self::Schema { table, found } => write!(
                f,
                "{DB_FILE} has an older `{table}` table ({}); delete {DB_FILE} and its -wal and -shm files",
                found.join(", ")
            ),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Sqlite(e) => Some(e),
            Self::Schema { .. } => None,
        }
    }
}
