//! UTC timestamps without a date-time dependency.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Current time as UTC RFC 3339 with millisecond precision.
pub fn now_rfc3339() -> String {
    rfc3339(SystemTime::now())
}

/// Milliseconds since the Unix epoch at `t`; 0 before the epoch. The engine
/// stores every time as one of these and builds RFC 3339 strings only when a
/// message or roster entry goes on the wire.
pub(super) fn unix_ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// [`unix_ms`] of the current time.
fn system_ms() -> u64 {
    unix_ms(SystemTime::now())
}

/// Where a [`crate::bus::Bus`] reads the time: the system clock, or a
/// function the caller supplies (see [`crate::bus::Bus::with_clock`]).
///
/// An enum rather than a boxed closure only so the default costs a branch,
/// not an indirect call, on every send and pull.
#[derive(Clone, Default)]
pub(super) enum Clock {
    /// `SystemTime::now()`.
    #[default]
    System,
    /// Milliseconds since the Unix epoch from the caller.
    Custom(Arc<dyn Fn() -> u64 + Send + Sync>),
}

impl Clock {
    /// Milliseconds since the Unix epoch now.
    #[inline]
    pub(super) fn now_ms(&self) -> u64 {
        match self {
            Self::System => system_ms(),
            Self::Custom(f) => f(),
        }
    }
}

/// `ms` (milliseconds since the Unix epoch) as UTC RFC 3339.
pub(super) fn rfc3339_ms(ms: u64) -> String {
    rfc3339(UNIX_EPOCH + Duration::from_millis(ms))
}

/// `t` as UTC RFC 3339 with millisecond precision. The fixed width makes these
/// strings sort in time order.
pub fn rfc3339(t: SystemTime) -> String {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = d.as_secs();
    let millis = d.subsec_millis();
    let (year, month, day) = civil_from_days((secs / 86_400) as i64);
    let sod = secs % 86_400;
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        year,
        month,
        day,
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60,
        millis
    )
}

/// Howard Hinnant's days-from-civil inverse: epoch day -> (y, m, d).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m as u32, d as u32)
}
