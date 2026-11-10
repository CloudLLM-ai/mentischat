//! Ping-pong between two agents through a running daemon: one says `1`, the
//! other `2`, and so on up to `--to` (default 100), each number answering the
//! last. It measures how fast two agents can hold a turn-by-turn conversation
//! through MentisChat.
//!
//! ```text
//! MENTISCHAT_PLAIN=1 MENTISCHAT_DIR=$(mktemp -d) MENTISCHAT_LISTEN_PORT=19580 \
//!     target/release/mentischat &
//! cargo run --release --example ping_pong -- --addr 127.0.0.1:19580
//! ```
//!
//! Options: `--addr 127.0.0.1:9480`, `--to 100`, `--mode ws|poll|notify|all`
//! (default `all`). Each player is a **separate process** running its own
//! hand-written client (`examples/ping_pong/agent.rs`): this binary re-runs
//! itself as `ping_pong player ...` twice, starts the opener only once the
//! other player is listening, and reads each player's received numbers from
//! its stdout. See `agent.rs` for the three modes; `notify` is throttled by
//! design (one wake-up frame at most every 3 s per agent), so expect about two
//! numbers per 3 s there.
//!
//! Players are named `ping-<mode>` and `pong-<mode>` and leave the roster at
//! the end. Run it against a throwaway daemon, never your live one.

mod agent;

use std::process::Stdio;
use std::time::Duration;

use agent::{Hop, Mode, Player, Summary};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

struct Config {
    addr: String,
    to: u32,
    modes: Vec<Mode>,
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Config {
    let mut cfg = Config {
        addr: "127.0.0.1:9480".into(),
        to: 100,
        modes: Mode::ALL.to_vec(),
    };
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .unwrap_or_else(|| panic!("{flag} needs a value"));
        match flag.as_str() {
            "--addr" => cfg.addr = value,
            "--to" => cfg.to = value.parse().expect("--to is a number"),
            "--mode" if value == "all" => cfg.modes = Mode::ALL.to_vec(),
            "--mode" => cfg.modes = vec![Mode::parse(&value).expect("--mode ws|poll|notify|all")],
            other => panic!("unknown flag {other}"),
        }
    }
    cfg
}

/// `ping_pong player <addr> <mode> <me> <peer> <opens 0|1> <to>`: print
/// `ready` once listening, then `hop <n> <sent_us> <recv_us>` per number
/// received, then `done`.
async fn player(mut args: impl Iterator<Item = String>) {
    let mut next = || args.next().expect("player arguments");
    let (addr, mode, me, peer) = (next(), next(), next(), next());
    let opens = next() == "1";
    let to: u32 = next().parse().expect("to");
    let mode = Mode::parse(&mode).expect("mode");
    let p = Player::join(&addr, &me, &peer, mode)
        .await
        .unwrap_or_else(|e| {
            eprintln!("{me}: {e}");
            std::process::exit(1)
        });
    println!("ready");
    match p.play(opens, to).await {
        Ok(hops) => {
            for h in hops {
                println!("hop {} {} {}", h.n, h.sent_us, h.recv_us);
            }
            println!("done");
        }
        Err(e) => {
            eprintln!("{me}: {e}");
            std::process::exit(1)
        }
    }
    Player::leave(&addr, &me).await;
}

/// Spawn one player process; its stdout lines.
fn spawn(cfg: &Config, mode: Mode, me: &str, peer: &str, opens: bool) -> tokio::process::Child {
    Command::new(std::env::current_exe().expect("own path"))
        .args([
            "player",
            &cfg.addr,
            mode.name(),
            me,
            peer,
            if opens { "1" } else { "0" },
            &cfg.to.to_string(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn player")
}

type Lines = tokio::io::Lines<BufReader<tokio::process::ChildStdout>>;

/// A player's stdout as lines.
fn lines_of(child: &mut tokio::process::Child) -> Lines {
    BufReader::new(child.stdout.take().expect("stdout")).lines()
}

/// Read a player's remaining stdout: its hops. The player must exit cleanly.
async fn collect(mut child: tokio::process::Child, mut lines: Lines) -> Vec<Hop> {
    let mut hops = Vec::new();
    while let Ok(Some(line)) = lines.next_line().await {
        let mut it = line.split(' ');
        if it.next() == Some("hop") {
            let mut num = || {
                it.next()
                    .and_then(|v| v.parse::<u64>().ok())
                    .expect("hop field")
            };
            hops.push(Hop {
                n: num() as u32,
                sent_us: num(),
                recv_us: num(),
            });
        }
    }
    let status = child.wait().await.expect("wait");
    assert!(status.success(), "a player failed: {status}");
    hops
}

async fn game(cfg: &Config, mode: Mode) -> Summary {
    let (ping, pong) = (
        format!("ping-{}", mode.name()),
        format!("pong-{}", mode.name()),
    );
    // The answering player first, and the opener only once it is listening.
    let mut answerer = spawn(cfg, mode, &pong, &ping, false);
    let mut answerer_out = lines_of(&mut answerer);
    let first = answerer_out
        .next_line()
        .await
        .expect("read")
        .unwrap_or_default();
    assert_eq!(first, "ready", "{pong} did not come up");
    let mut opener = spawn(cfg, mode, &ping, &pong, true);
    let opener_out = lines_of(&mut opener);
    let (a, b) = tokio::join!(collect(opener, opener_out), collect(answerer, answerer_out));
    let mut hops = a;
    hops.extend(b);
    assert_eq!(hops.len() as u32, cfg.to, "every number was delivered once");
    Summary::of(hops).expect("hops")
}

fn ms(d: Duration) -> String {
    format!("{:.3} ms", d.as_secs_f64() * 1e3)
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1).peekable();
    if args.peek().map(String::as_str) == Some("player") {
        args.next();
        return player(args).await;
    }
    let cfg = parse_args(args);
    println!(
        "ping-pong to {} through {} (each player a separate process)",
        cfg.to, cfg.addr
    );
    println!(
        "{:<7} {:>12} {:>12} {:>12} {:>12} {:>12}",
        "mode", "total", "numbers/s", "hop p50", "hop p99", "hop max"
    );
    for &mode in &cfg.modes {
        let s = game(&cfg, mode).await;
        println!(
            "{:<7} {:>12} {:>12.0} {:>12} {:>12} {:>12}",
            mode.name(),
            ms(s.total),
            s.per_second(),
            ms(s.p50),
            ms(s.p99),
            ms(s.max)
        );
    }
}
