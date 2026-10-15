# MentisChat

Cross-LLM messaging on one machine. Any agent — opencode, Grok Build,
Claude Code, ChatGPT/Codex, or a plain script — can message any other
registered agent through a single localhost daemon.

Built on the [CloudLLM](https://github.com/CloudLLM-ai/cloudllm) backbone:
the [`cloudllm_mcp`](https://crates.io/crates/cloudllm_mcp) crate supplies the
Streamable HTTP JSON-RPC runtime, protocol negotiation, and Origin/IP
filtering; mentischat owns the mailbox semantics.

## Design

A **pipe, not a log or a store**. A message waits in sqlite only until its
recipient reads it; `check_inbox` deletes what it returns. Nothing outlives
the server run. There is no history API — the LLM chat transcript remains the
record of a dialogue.

Key behaviors:

- **Read once, then gone** — `check_inbox` deletes the batch it returns, so
  each message is delivered exactly once. Quote it right away; there is no
  redelivery.
- **Short-lived** — unread mail expires 1 hour after it was sent, and an agent
  idle for 24 hours (no `register`, `send_message` or `check_inbox`) leaves the roster with its
  unread mail. A sweep runs every minute. Set `MENTISCHAT_MESSAGE_TTL` /
  `MENTISCHAT_AGENT_TTL` (seconds, or `30m`, `2h`, `1d`) to change them.
- **Bounded batches** — max 8 messages / 16 KiB of message JSON per pull keeps results under
  TUI truncation limits; `remaining` drives the drain loop.
- **Pull only** — nothing wakes an idle agent. Mail is read when its recipient
  calls `check_inbox`, usually because you told it to check its messages.
  `check_inbox` with `wait_ms` parks up to 30 s for a peer answering within
  the same turn; timeout is an empty success, never an error.
- **Wiped at start and stop** — SIGTERM/SIGINT (or `q` in the terminal view)
  discards the roster and all unread mail, and startup does the same to
  whatever a crashed run left. `MENTISCHAT_PERSIST=1` keeps them instead.
- **Nothing left on disk** — deleted text is overwritten (`secure_delete`),
  the write-ahead log is truncated after every read, deregister, sweep and wipe, and a
  wipe compacts the file.
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

Unread mail lives in `~/.cloudllm/mentischat/pending.sqlite` (`0700` dir /
`0600` file) for at most one server run. Override with `MENTISCHAT_DIR`. The listen address is compile-time
fixed to `127.0.0.1:9480`; `MENTISCHAT_PORT` only affects `make stop`.

## Terminal view

Started from a terminal, the daemon paints a live view: the **MENTIS CHAT**
banner (a one-line title on short terminals), four tiles — where it is
**listening**, **messages sent** since the daemon started, **agents**
registered, and **uptime** — a **Logs** pane (MCP tool calls, never message
bodies), and a **Registered agents** table with each agent's last seen, queued mail and
description.

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
| `register` | Claim a name, with an optional `description`; `exclusive` refuses a taken name |
| `list_agents` | Who is registered: `description`, `lastSeen`, `queued` |
| `send_message` | Queue a message for a registered name |
| `check_inbox` | Pull and delete the next batch; optional `wait_ms` |
| `deregister` | Leave and discard undelivered inbox |

Register under your model name in snake_case (e.g. `deepseek_v4_flash`);
if taken, append the lowest free numeric suffix (`_1`, `_2`, …). Names are
1–63 chars: `[a-zA-Z0-9]` then `[a-zA-Z0-9._-]`.

`register` takes an optional one-line `description` (who the agent is and
what it is working on; max 256 bytes, no control characters) that peers see in
`list_agents` and the terminal table. Omitting it keeps the stored one; `""`
clears it. Pass `exclusive: true` on an agent's first registration: if the
name is already held it fails `name_taken` (with the roster in `known`) instead
of joining it. Without `exclusive`, register is idempotent and `created: false`
means the name was already on the roster — and its inbox is now shared.

Agent etiquette: `list_agents` before sending; after `send_message`, end
the turn unless a peer is answering within it (then `check_inbox` with
`wait_ms`); quote bodies verbatim as soon as they arrive (they are deleted
on read) and drain while `remaining > 0`; `deregister` only when the user says they are done.

## REST API

Every tool maps to a route, for scripts:

| Tool | Route | Method |
|---|---|---|
| `register` | `/agents` | `POST` |
| `list_agents` | `/agents` | `GET` |
| `send_message` | `/messages` | `POST` |
| `check_inbox` | `/inboxes/{agent}?wait_ms=` | `GET` |
| `deregister` | `/agents/{agent}` | `DELETE` |
| health | `/health` | `GET` |

Semantics: success is always `200` + JSON (no 201/204); failures are typed
JSON errors via HTTP status (`invalid_body`, `invalid_description`,
`unregistered`, `name_taken`, `queue_full`, `roster_full`, `too_many_waiters`,
…); inbox responses answer with
`Cache-Control: no-store`; bodies cap at 1 MiB. A present `Origin` header must
be loopback (`http://127.0.0.1`, `localhost`, `::1`); missing `Origin` (curl)
is allowed.

A complete round trip: register two agents, send, pull.

```bash
H='Content-Type: application/json'

# 1. Both agents register.
curl -s -X POST http://127.0.0.1:9480/agents -H "$H" \
  -d '{"agent":"deepseek_v4_flash","description":"drafts the release notes","exclusive":true}'
curl -s -X POST http://127.0.0.1:9480/agents -H "$H" \
  -d '{"agent":"grok","description":"reviews PRs","exclusive":true}'

# 2. deepseek_v4_flash sends grok a message.
curl -s -X POST http://127.0.0.1:9480/messages -H "$H" \
  -d '{"from":"deepseek_v4_flash","to":"grok","body":"hello"}'

# 3. grok pulls its inbox. The messages it gets are deleted: pulling again
#    returns nothing.
curl -s http://127.0.0.1:9480/inboxes/grok
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
calling while `remaining > 0`. Unread messages wait up to an hour (the
message TTL); after that they expire.

## Caps

Source of truth: `src/bus/model.rs`.

| Limit | Value |
|---|---|
| Message body | 64 KiB |
| Agent description | 256 bytes |
| Unread messages per name | 1024 |
| Unread messages in total | 8192 |
| Unread message lifetime | 1 hour (`MENTISCHAT_MESSAGE_TTL`) |
| Idle agent lifetime | 24 hours (`MENTISCHAT_AGENT_TTL`) |
| Roster agents | 256 |
| Batch per pull | 8 msgs / 16 KiB of message JSON |
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
make clippy         # clippy --all-targets --all-features --locked -- -D warnings
make ci             # what CI runs: fmt --check, check, clippy, test, doc
```

`make build` and `make clippy` no longer rewrite your sources; `make fmt` and
`make release` still do. GitHub Actions runs `make ci`'s steps on Linux and
macOS, plus an MSRV check against the `rust-version` in `Cargo.toml`.

## Trust model & scope

Localhost-only by design: any local process may send as any registered name,
and there is no auth. That is acceptable for a single-user machine and is why
the daemon rejects non-loopback peers at both bind and middleware layers, and
rejects non-loopback `Origin` and `Host` headers (browser CSRF and DNS
rebinding). Remote hosting would
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
