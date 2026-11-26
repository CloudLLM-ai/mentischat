//! Room commands: what an agent (or a person at a terminal) types to manage a
//! room and check identities, signed by the client that holds the key.
//!
//! The same commands come from two places:
//!
//! - **In a chat.** Through `mentischat client run`, a `send_message` to a room
//!   whose body starts with `/` is a command, not a message: `/ban Mallory`
//!   bans Mallory from that room. The daemon never sees the command text, only
//!   the signed operation. Start a message with `//` to send it as text with
//!   one `/` (`//etc/hosts` sends `/etc/hosts`).
//! - **From the command line**, as `mentischat client room <command> ROOM
//!   [NAME] --agent NAME`.
//!
//! | Command | Who | What |
//! |---|---|---|
//! | `/help` | anyone | the commands |
//! | `/members` | members | members with role, key fingerprint; admins; bans |
//! | `/whois NAME` | anyone | NAME's key, fingerprint, provider, description |
//! | `/key` | anyone | this agent's own key and fingerprint |
//! | `/invite NAME` | admins | let NAME join a private room, pinning its key |
//! | `/kick NAME` | admins | remove a member or revoke an invitation |
//! | `/ban NAME` | admins | remove NAME and keep its name and key out |
//! | `/unban NAME` | admins | lift a ban |
//! | `/promote NAME` | admins | make a member an admin |
//! | `/demote NAME` | admins | make an admin a participant |
//! | `/leave` | members | leave the room |
//!
//! In an end-to-end encrypted room `/invite`, `/kick` and `/ban` also change
//! the MLS group (an add, or a Commit that removes the member's leaf), so the
//! removed cannot read what follows.

use serde_json::{json, Value};

use super::{Client, ClientError};
use crate::names::AgentName;

/// One room command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoomCommand {
    /// List the commands.
    Help,
    /// The room's members, roles, fingerprints and bans.
    Members,
    /// Who holds a name.
    Whois(String),
    /// This agent's own key and fingerprint.
    Key,
    /// Invite a name (admins).
    Invite(String),
    /// Remove a member or invitee (admins).
    Kick(String),
    /// Ban a name and its key (admins).
    Ban(String),
    /// Lift a ban (admins).
    Unban(String),
    /// Make a member an admin (admins).
    Promote(String),
    /// Make an admin a participant (admins).
    Demote(String),
    /// Leave the room.
    Leave,
}

/// The help text: one line per command.
pub const HELP: &str = "\
/members         members with their role and key fingerprint, admins, bans
/whois NAME      NAME's key, fingerprint, provider and description
/key             your own key and fingerprint
/invite NAME     admins: let NAME into a private room, pinning its key
/kick NAME       admins: remove a member, or revoke an invitation
/ban NAME        admins: remove NAME and keep its name and key out
/unban NAME      admins: lift a ban
/promote NAME    admins: make a member an admin
/demote NAME     admins: make an admin a participant
/leave           leave the room
//text           send \"/text\" as a message";

/// What a message body is, read as a possible command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Parsed {
    /// A message to send as it is.
    Text,
    /// A message to send without its first `/` (it started with `//`).
    Escaped(String),
    /// A command.
    Command(RoomCommand),
}

/// Why a `/...` body is not a command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandError(pub String);

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}\n{HELP}", self.0)
    }
}

impl std::error::Error for CommandError {}

impl RoomCommand {
    /// Read a room message body: text, an escaped `//` message, or a command.
    ///
    /// # Errors
    ///
    /// [`CommandError`] for a `/word` that is not a command, a command missing
    /// its name or given one it does not take, or a name that is not a valid
    /// agent name.
    ///
    /// # Examples
    ///
    /// ```
    /// use mentischat::client::commands::{Parsed, RoomCommand};
    ///
    /// assert_eq!(RoomCommand::parse("hello"), Ok(Parsed::Text));
    /// assert_eq!(
    ///     RoomCommand::parse("/ban Mallory"),
    ///     Ok(Parsed::Command(RoomCommand::Ban("Mallory".into())))
    /// );
    /// assert_eq!(RoomCommand::parse("//etc"), Ok(Parsed::Escaped("/etc".into())));
    /// assert!(RoomCommand::parse("/ban").is_err());
    /// assert!(RoomCommand::parse("/frobnicate").is_err());
    /// ```
    pub fn parse(body: &str) -> Result<Parsed, CommandError> {
        let text = body.trim();
        if let Some(rest) = text.strip_prefix("//") {
            return Ok(Parsed::Escaped(format!("/{rest}")));
        }
        let Some(rest) = text.strip_prefix('/') else {
            return Ok(Parsed::Text);
        };
        let mut words = rest.split_whitespace();
        let verb = words.next().unwrap_or_default().to_ascii_lowercase();
        let name = words.next();
        if words.next().is_some() {
            return Err(CommandError(format!("/{verb} takes at most one name")));
        }
        Self::from_words(&verb, name).map(Parsed::Command)
    }

    /// The command named `verb` (without its `/`) with the optional `name`.
    ///
    /// # Errors
    ///
    /// [`CommandError`] as for [`parse`](Self::parse).
    pub fn from_words(verb: &str, name: Option<&str>) -> Result<Self, CommandError> {
        let named = |make: fn(String) -> Self| -> Result<Self, CommandError> {
            let name = name.ok_or_else(|| CommandError(format!("/{verb} needs a NAME")))?;
            AgentName::parse(name)
                .map_err(|_| CommandError(format!("{name} is not a valid agent name")))?;
            Ok(make(name.to_string()))
        };
        let bare = |command: Self| match name {
            None => Ok(command),
            Some(_) => Err(CommandError(format!("/{verb} takes no name"))),
        };
        match verb {
            "help" => bare(Self::Help),
            "members" => bare(Self::Members),
            "key" => bare(Self::Key),
            "leave" => bare(Self::Leave),
            "whois" => named(Self::Whois),
            "invite" => named(Self::Invite),
            "kick" | "remove" => named(Self::Kick),
            "ban" => named(Self::Ban),
            "unban" => named(Self::Unban),
            "promote" => named(Self::Promote),
            "demote" => named(Self::Demote),
            other => Err(CommandError(format!("/{other} is not a command"))),
        }
    }

    /// Whether the command changes an end-to-end encrypted room's MLS group,
    /// and so must run in the one process that holds the group (the running
    /// `mentischat client run`, through the chat).
    pub fn changes_group(&self) -> bool {
        matches!(
            self,
            Self::Invite(_) | Self::Kick(_) | Self::Ban(_) | Self::Leave
        )
    }
}

/// Run `command` in `room` as `client`'s agent. The answer is JSON for the
/// harness or the terminal: `{"command", "room", ...}` with what the daemon
/// returned.
///
/// # Errors
///
/// [`ClientError::Daemon`] with the daemon's code (`not_an_admin`, `banned`,
/// `not_a_member`, ...), or the MLS errors of a secure room's add or removal.
pub async fn run(
    client: &mut Client,
    room: &str,
    command: &RoomCommand,
) -> Result<Value, ClientError> {
    let secure = client.is_secure(room);
    let (verb, result) = match command {
        RoomCommand::Help => ("help", json!(HELP.lines().collect::<Vec<_>>())),
        RoomCommand::Members => ("members", client.room(room).await?),
        RoomCommand::Whois(name) => ("whois", client.whois(name).await?),
        RoomCommand::Key => (
            "key",
            json!({
                "agent": client.agent(),
                "publicKey": client.public_key().to_hex(),
                "fingerprint": client.public_key().fingerprint(),
            }),
        ),
        RoomCommand::Invite(name) if secure => {
            client.add_member(room, name).await?;
            ("invite", client.room(room).await?)
        }
        RoomCommand::Invite(name) => ("invite", client.invite_signed(room, name).await?),
        RoomCommand::Kick(name) if secure => {
            client.remove_member(room, name).await?;
            ("kick", client.room(room).await?)
        }
        RoomCommand::Kick(name) => ("kick", client.remove_signed(room, name).await?),
        RoomCommand::Ban(name) => ("ban", client.ban(room, name).await?),
        RoomCommand::Unban(name) => ("unban", client.unban(room, name).await?),
        RoomCommand::Promote(name) => ("promote", client.promote(room, name).await?),
        RoomCommand::Demote(name) => ("demote", client.demote(room, name).await?),
        RoomCommand::Leave if secure => {
            client.leave_room(room).await?;
            ("leave", json!({ "room": room, "deleted": false }))
        }
        RoomCommand::Leave => ("leave", client.leave_signed(room).await?),
    };
    Ok(json!({ "command": verb, "room": room, "result": result }))
}
