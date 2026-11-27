//! Room commands as typed in a chat or on the command line: every verb, every
//! way a command can be malformed, the `//` escape, and which commands change
//! an end-to-end encrypted room's group.
#![cfg(feature = "e2e")]

use mentischat::client::commands::{Parsed, RoomCommand, HELP};

fn command(text: &str) -> RoomCommand {
    match RoomCommand::parse(text) {
        Ok(Parsed::Command(c)) => c,
        other => panic!("{text}: {other:?}"),
    }
}

fn refused(text: &str) -> String {
    RoomCommand::parse(text).unwrap_err().0
}

/// Every verb parses, in any case, with surrounding space; `/remove` is
/// `/kick`.
#[test]
fn every_command_parses() {
    let n = || "Mallory".to_string();
    let cases = [
        ("/help", RoomCommand::Help),
        ("/members", RoomCommand::Members),
        ("/key", RoomCommand::Key),
        ("/leave", RoomCommand::Leave),
        ("/whois Mallory", RoomCommand::Whois(n())),
        ("/invite Mallory", RoomCommand::Invite(n())),
        ("/kick Mallory", RoomCommand::Kick(n())),
        ("/remove Mallory", RoomCommand::Kick(n())),
        ("/ban Mallory", RoomCommand::Ban(n())),
        ("/unban Mallory", RoomCommand::Unban(n())),
        ("/promote Mallory", RoomCommand::Promote(n())),
        ("/demote Mallory", RoomCommand::Demote(n())),
        ("  /BAN   Mallory  ", RoomCommand::Ban(n())),
    ];
    for (text, want) in cases {
        assert_eq!(command(text), want, "{text}");
    }
}

/// Text is text; `//` escapes one slash.
#[test]
fn text_and_escapes_are_messages() {
    assert_eq!(RoomCommand::parse("hello /ban x"), Ok(Parsed::Text));
    assert_eq!(RoomCommand::parse(""), Ok(Parsed::Text));
    assert_eq!(
        RoomCommand::parse("//etc/hosts"),
        Ok(Parsed::Escaped("/etc/hosts".into()))
    );
    assert_eq!(RoomCommand::parse("//"), Ok(Parsed::Escaped("/".into())));
}

/// Each malformed command says what is wrong, and the error lists the
/// commands.
#[test]
fn malformed_commands_say_why() {
    assert!(refused("/ban").contains("needs a NAME"));
    assert!(refused("/members bob").contains("takes no name"));
    assert!(refused("/ban a b").contains("at most one name"));
    assert!(refused("/ban bad!name").contains("not a valid agent name"));
    assert!(refused("/frob").contains("not a command"));
    assert!(refused("/").contains("not a command"));
    let shown = RoomCommand::parse("/frob").unwrap_err().to_string();
    assert!(shown.contains(HELP));
}

/// Only the commands that add or remove a member change an MLS group.
#[test]
fn group_changing_commands_are_known() {
    for c in ["/invite x", "/kick x", "/ban x", "/leave"] {
        assert!(command(c).changes_group(), "{c}");
    }
    for c in [
        "/members",
        "/key",
        "/help",
        "/whois x",
        "/unban x",
        "/promote x",
        "/demote x",
    ] {
        assert!(!command(c).changes_group(), "{c}");
    }
}

/// The help text names every command.
#[test]
fn help_lists_every_command() {
    for verb in [
        "/members", "/whois", "/key", "/invite", "/kick", "/ban", "/unban", "/promote", "/demote",
        "/leave", "//",
    ] {
        assert!(HELP.contains(verb), "{verb}");
    }
}
