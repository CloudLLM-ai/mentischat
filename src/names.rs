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
            Err(MailboxError::InvalidName {
                agent: raw.to_string(),
            })
        }
    }

    /// The validated name.
    pub fn as_str(&self) -> &str {
        &self.0
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
