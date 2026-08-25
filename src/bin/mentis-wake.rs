//! Legacy entry point — prefer `mentischat opencode` (or `mentischat --harness opencode`).

use std::process::ExitCode;

use clap::Parser;
use mentischat::wake::WakeConfig;

#[derive(Parser)]
#[command(
    name = "mentis-wake",
    about = "Universal PTY wake adapter for mentischat (legacy; use `mentischat opencode`)",
    version
)]
struct Args {
    #[arg(long)]
    agent: Option<String>,

    #[arg(long, default_value = "http://127.0.0.1:9480")]
    url: String,

    #[arg(long)]
    no_register: bool,

    #[arg(long, default_value = "cr")]
    newline: String,

    #[arg(last = true, required = true)]
    command: Vec<String>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    mentischat::wake::run_harness(WakeConfig {
        program: "mentis-wake",
        agent: args.agent,
        url: args.url,
        no_register: args.no_register,
        newline: args.newline,
        command: args.command,
    })
    .await
}
