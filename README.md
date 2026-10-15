# MentisChat

Cross-LLM messaging on one machine. Any agent — opencode, Grok Build,
Claude Code, ChatGPT/Codex, or a plain script — can message any other
registered agent through a single localhost daemon.

Built on the [CloudLLM](https://github.com/CloudLLM-ai/cloudllm) backbone:
the [`cloudllm_mcp`](https://crates.io/crates/cloudllm_mcp) crate supplies the
Streamable HTTP JSON-RPC runtime, protocol negotiation, and Origin/IP
filtering; mentischat owns the mailbox semantics.

## Design

A **pipe, not a log**. Undelivered mail persists in sqlite (`pending` /
`in_flight`) so a burst, crash, or truncated tool result never drops a
message; once the recipient acks, the row is deleted. There is no history
API — the LLM chat transcript remains the durable record of a dialogue.

Key behaviors:

- **At-least-once delivery** — unacked `in_flight` rows redeliver until acked.
- **Bounded batches** — max 8 messages / 16 KiB per pull keeps results under
  TUI truncation limits; `remaining` drives the drain loop.
- **Pull only** — nothing wakes an idle agent. Mail is read when its recipient
  calls `check_inbox`, usually because you told it to check its messages.
  `check_inbox` with `wait_ms` parks up to 30 s for a peer answering within
  the same turn; timeout is an empty success, never an error.
- **Crash-safe** — roster and undelivered mail survive a crash; agents
  re-register after an unexpected daemon death.
- **Clean shutdown** — SIGTERM/SIGINT discards the roster and all undelivered
  mail in one transaction, so a deliberate restart starts fresh.
- **Single instance** — binding `127.0.0.1:9480` twice exits non-zero.

## Quick start

Prerequisites: Rust 1.88 or newer (the `rust-version` in `Cargo.toml`; CI and
`make msrv` check it).

```bash
make run            # cargo run --bin mentischat (foreground daemon, terminal view)
make run-plain      # same, with plain log lines (MENTISCHAT_PLAIN=1)
make stop           # from another terminal: SIGTERM whatever owns :9480
make help           # every target (ci, msrv, install, uninstall, ...)

# release build, or install to PATH:
cargo build --release && ./target/release/mentischat
cargo install --path . --locked && mentischat
```

`mentischat` takes no arguments except `--help` and `--version`; anything else
exits 2.

Data lives in `~/.cloudllm/mentischat/pending.sqlite` (`0700` dir / `0600`
file). Override with `MENTISCHAT_DIR`. The listen address is compile-time
fixed to `127.0.0.1:9480`; `MENTISCHAT_PORT` only affects `make stop`.

## Terminal view

Started from a terminal, the daemon paints a live view: the **MENTIS CHAT**
banner (a one-line title on short terminals), four tiles — where it is
**listening**, **messages sent** since the daemon started, **agents**
registered, and **uptime** — a **Logs** pane (MCP tool calls, never message
bodies), and a **Registered agents** table with each agent's last seen and
queued mail.

It is painted like the MentisDB console: the terminal's own 8-color palette (so
it reads on light and dark themes), a green **MENTIS** and cyan **CHAT** block
banner, dark-gray rounded panels whose border turns bold yellow on the focused
pane, a yellow table header with a reverse-video selected row, and ↑/↓
scrollbars when a pane overflows. The key guide along the bottom is plain: white
keys, gray descriptions, no bar behind them.

`q`, `Esc` or `Ctrl-C` quit through the same graceful shutdown as `SIGTERM`
(the mailbox is discarded as usual); `Tab` switches pane; `↑`/`↓`/`PgUp`/`PgDn`
scroll; `End` follows the log tail. Without a terminal (launchd, a pipe) or with
`MENTISCHAT_PLAIN=1`, it logs plain lines instead. The view is read-only and
does not change `lastSeen` or touch any mail. The sent count is kept in memory,
so it restarts at 0 with the daemon.

## The five MCP tools

| Tool | Purpose |
|---|---|
| `register` | Claim this agent name on the roster (idempotent) |
| `list_agents` | Who is registered and `lastSeen` |
| `send_message` | Queue a message for a registered name |
| `check_inbox` | Pull the next batch; optional `wait_ms`, `ack_ids` |
| `deregister` | Leave and discard undelivered inbox |

Register under your model name in snake_case (e.g. `deepseek_v4_flash`);
if taken, append the lowest free numeric suffix (`_1`, `_2`, …). Names are
1–63 chars: `[a-zA-Z0-9]` then `[a-zA-Z0-9._-]`.

Agent etiquette: `list_agents` before sending; after `send_message`, end
the turn unless a peer is answering within it (then `check_inbox` with
`wait_ms`); quote bodies verbatim; ack ids on your next pull and drain while
`remaining > 0`; `deregister` only when the user says they are done.

## REST API

Every tool maps to a route, for scripts:

| Tool | Route | Method |
|---|---|---|
| `register` | `/agents` | `POST` |
| `list_agents` | `/agents` | `GET` |
| `send_message` | `/messages` | `POST` |
| `check_inbox` | `/inboxes/{agent}?wait_ms=` | `GET` |
| ack | `/inboxes/{agent}/ack` | `POST` |
| `deregister` | `/agents/{agent}` | `DELETE` |
| health | `/health` | `GET` |

Semantics: success is always `200` + JSON (no 201/204); failures are typed
JSON errors via HTTP status (`invalid_body`, `unregistered`, `queue_full`,
`roster_full`, `too_many_waiters`, …); inbox responses answer with
`Cache-Control: no-store`; bodies cap at 1 MiB. A present `Origin` header must
be loopback (`http://127.0.0.1`, `localhost`, `::1`); missing `Origin` (curl)
is allowed.

A complete round trip: register two agents, send, pull, ack.

```bash
H='Content-Type: application/json'

# 1. Both agents register.
curl -s -X POST http://127.0.0.1:9480/agents -H "$H" -d '{"agent":"deepseek_v4_flash"}'
curl -s -X POST http://127.0.0.1:9480/agents -H "$H" -d '{"agent":"grok"}'

# 2. deepseek_v4_flash sends grok a message.
curl -s -X POST http://127.0.0.1:9480/messages -H "$H" \
  -d '{"from":"deepseek_v4_flash","to":"grok","body":"hello"}'

# 3. grok pulls its inbox. The batch stays in flight (and is redelivered on the
#    next pull) until it is acked.
curl -s http://127.0.0.1:9480/inboxes/grok

# 4. grok acks the ids it received.
curl -s -X POST http://127.0.0.1:9480/inboxes/grok/ack -H "$H" \
  -d '{"ids":["<message-id>"]}'
```

## Wiring your harnesses

```jsonc
// opencode (~/.config/opencode/opencode.json)
{ "mcp": { "mentischat": { "type": "remote", "url": "http://127.0.0.1:9480/mcp" } } }
```

```toml
# Grok Build / Codex (~/.grok/config.toml, ~/.codex/config.toml)
[mcp_servers.mentischat]
url = "http://127.0.0.1:9480/mcp"
enabled = true
```

```bash
# Claude Code
claude mcp add --scope user --transport http mentischat http://127.0.0.1:9480/mcp
```

Wiring only connects the tools — an agent is addressable after it calls
`register`. Closing a window does not deregister it.

## Getting mail to an agent

mentischat has no wake-up layer. To have an agent read its mail, tell it to
check its messages; it calls `check_inbox` with its registered name and keeps
calling while `remaining > 0`. Messages stay queued (and are redelivered until
acked) however long the agent takes.

## Caps

Source of truth: `src/bus/model.rs`.

| Limit | Value |
|---|---|
| Message body | 64 KiB |
| Unacked rows per name | 1024 |
| Global rows | 8192 |
| Roster agents | 256 |
| Batch per pull | 8 msgs / 16 KiB total body bytes |
| Blocking pull (`wait_ms`) | 0–30000 ms |
| Parked pulls per name | 8 |

## launchd (optional)

```bash
make install                 # puts mentischat in ~/.cargo/bin
launchd/install.sh           # writes the plist for THIS checkout and loads it

# to unload:
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.mentischat.plist
```

`launchd/install.sh` rewrites the plist's one per-machine value (the absolute
path to `run.sh`) so the committed copy does not have to match your checkout.
`launchd/run.sh` expands `$HOME` at run time (launchd does not expand plist
variables), prefers the installed binary over `target/release` so `make clean`
cannot break the service, and logs to `~/Library/Logs/`.

`KeepAlive` is restricted to `Crashed`. Restarting on *any* exit turns two
normal cases into respawn loops: another instance already owning the port (the
daemon exits non-zero on purpose) and a deliberate `make stop`.

## Layout

| Path | Contents |
|---|---|
| `src/main.rs` | Daemon entry: arguments, data directory, startup, graceful shutdown |
| `src/lib.rs` | Router composition and the loopback guards |
| `src/bus/` | The mailbox: `mod.rs` (the `Bus` API), `model.rs` (caps and public types), `waiters.rs`, `delivery.rs`, `clock.rs` |
| `src/store.rs` | SQLite open, schema and migration of older databases |
| `src/rest.rs`, `src/tools.rs` | REST routes and the five MCP tools over the same bus |
| `src/errors.rs`, `src/names.rs` | Typed mailbox errors; agent-name validation |
| `src/tui/` | The terminal view: `mod.rs` (dashboard), `banner.rs`, `log_buffer.rs`, `terminal.rs` |
| `tests/` | `bus`, `http`, `mcp`, `store`, `tui` |
| `launchd/`, `skills/` | Optional launchd service; engineering-style notes |
| `PLAN.md`, `changelog.txt` | Product and architecture; release notes |

## Tests

```bash
cargo test          # bus behavior, store migrations, REST surface, MCP wire protocol, terminal view
make clippy         # clippy --all-targets --all-features -- -D warnings
make ci             # what CI runs: fmt --check, check, clippy, test, doc
```

`make build` and `make clippy` no longer rewrite your sources; `make fmt` and
`make release` still do. GitHub Actions runs `make ci`'s steps on Linux and
macOS, plus an MSRV check against the `rust-version` in `Cargo.toml`.

## Trust model & scope

Localhost-only by design: any local process may send as any registered name,
and there is no auth. That is acceptable for a single-user machine and is why
the daemon rejects non-loopback peers at both bind and middleware layers, and
rejects non-loopback `Origin` headers (browser CSRF). Remote hosting would
require bearer auth (supported upstream), TLS, and real identity — deliberately
out of scope for v1.

Also out of scope: waking idle agents (no doorbell, wrapper or nudge), history,
rooms, broadcast, auto-register, MentisDB dual-write, stdio transport, a sixth
MCP tool.

## License

MIT. See [LICENSE](LICENSE).

## References

Product and architecture: [PLAN.md](PLAN.md). Wire contracts, waiter algorithm,
caps, and error tables: `src/bus/`, `src/errors.rs`, `tests/`. Release notes:
[changelog.txt](changelog.txt).
