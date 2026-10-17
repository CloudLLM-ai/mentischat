//! Validated agent-name type shared by every entry point.

use std::fmt;

use crate::errors::MailboxError;

/// A roster name: 1–63 chars, starting `[a-zA-Z0-9]`, then
/// `[a-zA-Z0-9._-]`.
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
        if head_ok && tail_ok && raw.len() <= 63 {
            Ok(Self(raw.to_string()))
        } else {
            Err(MailboxError::InvalidName { agent: echo(raw) })
        }
    }

    /// The validated name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The product an agent runs in (`claude_code`, `cursor`), lowercased: the
/// stem of the name the roster assigns it, `{provider}_{n}`.
///
/// At most [`MAX_PROVIDER_LEN`] characters, so the assigned name with any
/// numeric suffix still fits the 63-character name limit.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Provider(String);

/// Longest provider: leaves room for `_` and a 20-digit (`u64`) suffix under
/// the 63-character name limit.
pub const MAX_PROVIDER_LEN: usize = 42;

impl Provider {
    /// Lowercase and validate a raw provider, or fail with
    /// [`MailboxError::InvalidName`].
    ///
    /// # Examples
    ///
    /// ```
    /// use mentischat::names::Provider;
    ///
    /// assert_eq!(Provider::parse("Claude_Code").unwrap().as_str(), "claude_code");
    /// assert!(Provider::parse("").is_err());
    /// assert!(Provider::parse("-cursor").is_err());
    /// assert!(Provider::parse(&"x".repeat(43)).is_err());
    /// ```
    pub fn parse(raw: &str) -> Result<Self, MailboxError> {
        let lower = raw.to_ascii_lowercase();
        match AgentName::parse(&lower) {
            Ok(_) if lower.len() <= MAX_PROVIDER_LEN => Ok(Self(lower)),
            _ => Err(MailboxError::InvalidName { agent: echo(raw) }),
        }
    }

    /// The validated, lowercased provider.
    pub fn as_str(&self) -> &str {
        &self.0
    }
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
