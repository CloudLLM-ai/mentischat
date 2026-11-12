//! Validated agent-name type shared by every entry point.

use std::fmt;

use crate::errors::MailboxError;

/// Longest agent or room name, in bytes (names are ASCII).
pub const MAX_NAME_LEN: usize = 63;

/// A roster name: 1–63 chars, starting `[a-zA-Z0-9]`, then
/// `[a-zA-Z0-9._-]`. The roster keeps the spelling it was registered with
/// but compares names without regard to case, so `Vega` and `vega` are the
/// same agent. A new registration must also not be numbered (see
/// [`AgentName::is_numbered`]); that rule is the roster's, not the
/// grammar's, so stored mail and existing registrations under such a name
/// stay valid.
///
/// Construct through [`AgentName::parse`] so every handler sees a validated
/// value; the regex lives here, never in tool JSON schemas (local GGUF /
/// OpenCode grammars choke on copied patterns).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AgentName(String);

impl AgentName {
    /// Validate and wrap a raw name, or fail with
    /// [`MailboxError::InvalidName`].
    ///
    /// # Examples
    ///
    /// ```
    /// use mentischat::names::AgentName;
    ///
    /// assert!(AgentName::parse("grok").is_ok());
    /// assert!(AgentName::parse("grok-review.v2").is_ok());
    /// assert!(AgentName::parse("-lead").is_err());
    /// assert!(AgentName::parse("").is_err());
    /// assert!(AgentName::parse(&"x".repeat(64)).is_err());
    /// ```
    pub fn parse(raw: &str) -> Result<Self, MailboxError> {
        let mut chars = raw.chars();
        let head_ok = chars.next().is_some_and(|c| c.is_ascii_alphanumeric());
        let tail_ok = chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
        if head_ok && tail_ok && raw.len() <= MAX_NAME_LEN {
            Ok(Self(raw.to_string()))
        } else {
            Err(MailboxError::InvalidName { agent: echo(raw) })
        }
    }

    /// The validated name.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The name lowercased: the form two spellings of one name share.
    pub fn folded(&self) -> String {
        self.0.to_ascii_lowercase()
    }

    /// Whether the name ends in `.`, `_` or `-` followed only by digits
    /// (`vega_1`, `vega-2`, `vega.10`): the shape of a numbered copy of
    /// another name. The roster refuses a new registration of such a name
    /// with [`MailboxError::NumberedName`]; a name already registered keeps
    /// working. `vega2` and `grok-review.v2` are not numbered.
    ///
    /// # Examples
    ///
    /// ```
    /// use mentischat::names::AgentName;
    ///
    /// let numbered = |n: &str| AgentName::parse(n).unwrap().is_numbered();
    /// assert!(numbered("vega_1"));
    /// assert!(numbered("Vega-02"));
    /// assert!(numbered("vega.10"));
    /// assert!(!numbered("vega2"));
    /// assert!(!numbered("grok-review.v2"));
    /// assert!(!numbered("vega"));
    /// ```
    pub fn is_numbered(&self) -> bool {
        let stem = self.0.trim_end_matches(|c: char| c.is_ascii_digit());
        stem.len() < self.0.len() && stem.ends_with(['.', '_', '-'])
    }
}

/// A room name: the same grammar as [`AgentName`] (1-63 characters, starting
/// `[a-zA-Z0-9]`, then `[a-zA-Z0-9._-]`), compared without regard to case,
/// in a namespace of its own: a room and an agent may share a name.
///
/// Construct through [`RoomName::parse`], which fails with
/// [`MailboxError::InvalidRoom`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoomName(String);

impl RoomName {
    /// Validate and wrap a raw room name.
    ///
    /// # Examples
    ///
    /// ```
    /// use mentischat::names::RoomName;
    ///
    /// assert!(RoomName::parse("release-war-room").is_ok());
    /// assert!(RoomName::parse("-lobby").is_err());
    /// assert!(RoomName::parse("").is_err());
    /// ```
    pub fn parse(raw: &str) -> Result<Self, MailboxError> {
        match AgentName::parse(raw) {
            Ok(name) => Ok(Self(name.0)),
            Err(_) => Err(MailboxError::InvalidRoom { room: echo(raw) }),
        }
    }

    /// The validated name.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The name lowercased: the form two spellings of one room share.
    pub fn folded(&self) -> String {
        self.0.to_ascii_lowercase()
    }
}

impl fmt::Display for RoomName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The harness or app an agent runs in (`claude_code`, `cursor`), lowercased.
/// A label on the roster entry, shown in `list_agents` and the terminal view;
/// it plays no part in the name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Provider(String);

impl Provider {
    /// Lowercase and validate a raw provider (the same rules as
    /// [`AgentName`]), or fail with [`MailboxError::InvalidName`].
    ///
    /// # Examples
    ///
    /// ```
    /// use mentischat::names::Provider;
    ///
    /// assert_eq!(Provider::parse("Claude_Code").unwrap().as_str(), "claude_code");
    /// assert!(Provider::parse("").is_err());
    /// assert!(Provider::parse("-cursor").is_err());
    /// assert!(Provider::parse(&"x".repeat(64)).is_err());
    /// ```
    pub fn parse(raw: &str) -> Result<Self, MailboxError> {
        let lower = raw.to_ascii_lowercase();
        match AgentName::parse(&lower) {
            Ok(_) => Ok(Self(lower)),
            Err(_) => Err(MailboxError::InvalidName { agent: echo(raw) }),
        }
    }

    /// The validated, lowercased provider.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Names offered to agents that have not picked one: the Greek alphabet, in
/// order. Agents may register any valid name; these are suggestions, and when
/// all are taken none are offered.
pub const SUGGESTED_NAMES: [&str; 24] = [
    "Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta", "Iota", "Kappa",
    "Lambda", "Mu", "Nu", "Xi", "Omicron", "Pi", "Rho", "Sigma", "Tau", "Upsilon", "Phi", "Chi",
    "Psi", "Omega",
];

/// How many free names `list_agents` and `name_taken` offer.
pub const SUGGESTION_COUNT: usize = 5;

/// Up to [`SUGGESTION_COUNT`] names from [`SUGGESTED_NAMES`] for which
/// `is_taken` is false, starting at a random point in the list so agents
/// connecting together are not all offered the same first name.
pub fn suggestions(is_taken: impl Fn(&str) -> bool) -> Vec<String> {
    let start = (uuid::Uuid::new_v4().as_u128() % SUGGESTED_NAMES.len() as u128) as usize;
    SUGGESTED_NAMES
        .iter()
        .cycle()
        .skip(start)
        .take(SUGGESTED_NAMES.len())
        .filter(|name| !is_taken(name))
        .take(SUGGESTION_COUNT)
        .map(|name| (*name).to_string())
        .collect()
}

/// The rejected input as echoed in `invalid_name`: at most 64 characters, so
/// a megabyte "name" is not sent straight back.
pub(crate) fn echo(raw: &str) -> String {
    const MAX_ECHO_CHARS: usize = 64;
    match raw.char_indices().nth(MAX_ECHO_CHARS) {
        Some((cut, _)) => format!("{}…", &raw[..cut]),
        None => raw.to_string(),
    }
}

impl fmt::Display for AgentName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for AgentName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
