//! What the key-holding client reads from outside: the daemon's HTTP replies
//! (including chunked bodies) and its own command line. Replies parse to a
//! status and JSON value or a typed error, never a panic; a command line that
//! parses names a valid agent and a loopback daemon address.
#![no_main]

use libfuzzer_sys::fuzz_target;
use mentischat::client::cli::{parse, Command};
use mentischat::client::http::parse_reply;
use mentischat::names::AgentName;

fuzz_target!(|data: &[u8]| {
    if let Ok((status, _)) = parse_reply(data) {
        // A status line carries three digits; parsing keeps what u16 holds.
        let _ = status;
    }
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let args: Vec<String> = text.split(' ').map(str::to_string).collect();
    match parse(&args) {
        Ok(Command::Run { agent, addr }) => {
            assert!(AgentName::parse(&agent).is_ok());
            assert!(addr.ip().is_loopback());
        }
        Ok(Command::KeyInit { agent } | Command::KeyShow { agent }) => {
            assert!(AgentName::parse(&agent).is_ok());
        }
        Err(_) => {}
    }
});
