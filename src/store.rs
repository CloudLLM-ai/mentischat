//! SQLite persistence: schema creation and pragmas only.
//!
//! All mailbox logic lives in [`crate::bus`]; this module owns opening the
//! database with the right on-disk permissions and providing parameterised
//! SQL helpers over the two tables (`roster`, `messages`).

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
    conn.pragma_update(None, "synchronous", "NORMAL")
        .map_err(StoreError::Sqlite)?;

    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS roster (
          name TEXT PRIMARY KEY,
          last_seen TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS messages (
          id TEXT PRIMARY KEY,
          sender TEXT NOT NULL,
          recipient TEXT NOT NULL,
          body TEXT NOT NULL,
          created_at TEXT NOT NULL,
          seq INTEGER NOT NULL,
          state TEXT NOT NULL CHECK (state IN ('pending', 'in_flight'))
        );

        CREATE INDEX IF NOT EXISTS idx_inbox ON messages (recipient, state, seq);
        "#,
    )
    .map_err(StoreError::Sqlite)?;

    Ok(conn)
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
