//! The `mentischat client` subcommands: argument parsing and the commands.
//!
//! ```text
//! mentischat client run --agent NAME [--addr 127.0.0.1:9480]   stdio MCP proxy
//! mentischat client key init|show --agent NAME                 the agent's public key
//! ```

use std::net::SocketAddr;
use std::path::Path;

use super::{default_client_dir, proxy, Client, ClientError, Daemon, Identity};
use crate::names::AgentName;

/// Usage text for `mentischat client`.
pub const USAGE: &str = "\
Usage: mentischat client run --agent NAME [--addr HOST:PORT]
       mentischat client key init|show --agent NAME

run   serve MCP over stdio for one agent, signing and encrypting for secure
      rooms; configure your harness with this as an MCP stdio server
key   create (init) or print (show) the agent's Ed25519 identity. Prints the
      public key as hex and as the public_key_bytes array MentisDB expects

Keys live in MENTISCHAT_CLIENT_DIR (default ~/.cloudllm/mentischat/client),
one 0600 file per agent. --addr defaults to the daemon's 127.0.0.1:9480.";

/// A parsed `mentischat client` command line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Serve the stdio proxy.
    Run {
        /// The agent name.
        agent: String,
        /// The daemon's address.
        addr: SocketAddr,
    },
    /// Create the identity if missing, then print its public key.
    KeyInit {
        /// The agent name.
        agent: String,
    },
    /// Print the public key of an existing identity (creating none).
    KeyShow {
        /// The agent name.
        agent: String,
    },
}

/// Why a command line is not a [`Command`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageError(pub String);

impl std::fmt::Display for UsageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for UsageError {}

/// Parse the arguments after `client`.
///
/// # Errors
///
/// [`UsageError`] for an unknown subcommand or flag, a missing or invalid
/// `--agent`, or an `--addr` that is not a loopback socket address.
///
/// # Examples
///
/// ```
/// use mentischat::client::cli::{parse, Command};
///
/// let args: Vec<String> = ["key", "show", "--agent", "Vega"].map(String::from).to_vec();
/// assert_eq!(parse(&args), Ok(Command::KeyShow { agent: "Vega".into() }));
/// assert!(parse(&["run".to_string()]).is_err());
/// ```
pub fn parse(args: &[String]) -> Result<Command, UsageError> {
    let err = |m: &str| UsageError(m.to_string());
    let (words, flags): (Vec<&String>, Vec<&String>) = {
        let split = args
            .iter()
            .position(|a| a.starts_with("--"))
            .unwrap_or(args.len());
        (
            args[..split].iter().collect(),
            args[split..].iter().collect(),
        )
    };
    let mut agent = None;
    let mut addr = crate::BIND_ADDR;
    let mut it = flags.into_iter();
    while let Some(flag) = it.next() {
        let value = it
            .next()
            .ok_or_else(|| err("a flag is missing its value"))?;
        match flag.as_str() {
            "--agent" => agent = Some(value.clone()),
            "--addr" => {
                addr = value.parse().map_err(|_| err("--addr is not HOST:PORT"))?;
                if !addr.ip().is_loopback() {
                    return Err(err("--addr must be a loopback address"));
                }
            }
            other => return Err(UsageError(format!("unknown flag {other}"))),
        }
    }
    let agent = agent.ok_or_else(|| err("--agent is required"))?;
    AgentName::parse(&agent).map_err(|_| err("--agent is not a valid agent name"))?;
    match words
        .iter()
        .map(|w| w.as_str())
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["run"] => Ok(Command::Run { agent, addr }),
        ["key", "init"] => Ok(Command::KeyInit { agent }),
        ["key", "show"] => Ok(Command::KeyShow { agent }),
        _ => Err(err("unknown subcommand")),
    }
}

/// The text `key init` and `key show` print: the public key as hex, as the
/// `public_key_bytes` array MentisDB's `add_agent_key` takes, and a suggested
/// MentisDB `key_id`.
pub fn describe_key(identity: &Identity) -> String {
    let key = identity.public();
    let hex = key.to_hex();
    let bytes: Vec<String> = key.as_bytes().iter().map(u8::to_string).collect();
    format!(
        "agent: {}\npublic key (hex): {hex}\nmentisdb key_id: mentischat-{}\nmentisdb public_key_bytes: [{}]\n",
        identity.agent(),
        &hex[..8],
        bytes.join(",")
    )
}

/// Run a parsed command; the process exit code.
pub async fn run(command: Command) -> i32 {
    match execute(command).await {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("mentischat client: {e}");
            1
        }
    }
}

async fn execute(command: Command) -> Result<(), ClientError> {
    let dir = default_client_dir()?;
    match command {
        Command::KeyInit { agent } => {
            print!("{}", describe_key(&Identity::open(&dir, &agent)?));
            Ok(())
        }
        Command::KeyShow { agent } => {
            if !key_exists(&dir, &agent) {
                return Err(ClientError::Usage(format!(
                    "no identity for {agent}; run `mentischat client key init --agent {agent}`"
                )));
            }
            print!("{}", describe_key(&Identity::open(&dir, &agent)?));
            Ok(())
        }
        Command::Run { agent, addr } => {
            let daemon = Daemon::new(addr);
            let client = Client::open(Identity::open(&dir, &agent)?, daemon)?;
            let stdin = tokio::io::BufReader::new(tokio::io::stdin());
            proxy::serve(client, daemon, stdin, tokio::io::stdout())
                .await
                .map_err(|e| ClientError::Transport(e.to_string()))
        }
    }
}

fn key_exists(dir: &Path, agent: &str) -> bool {
    dir.join(agent.to_ascii_lowercase())
        .join("identity.key")
        .exists()
}
