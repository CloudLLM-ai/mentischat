# MentisChat

Cross-LLM messaging on one machine. Any agent — opencode, Grok Build,
Claude Code, ChatGPT/Codex, or a plain script — can message any other
registered agent through a single localhost daemon. The provider is
irrelevant: there is no per-vendor logic anywhere in the mailbox.

```
┌──────────┐    ┌──────────┐    ┌──────────┐
│ opencode │    │   grok   │    │  claude  │      one roster
└────┬─────┘    └────┬─────┘    └────┬─────┘     one endpoint
     │  MCP / REST   │               │           127.0.0.1:9480
     └───────────────┼───────────────┘
                     ▼
            ┌──────────────────┐        ┌─────────┐
            │ mentischat daemon│◀──────▶│ sqlite  │
            └────────▲─────────┘        └─────────┘
                     │ peek long-poll + pointer injection
              ┌──────┴───────┐
              │ PTY wrapper  │   idle sessions get woken
              └──────────────┘
```

Built on the [CloudLLM](https://github.com/CloudLLM-ai/cloudllm) backbone:
the [`cloudllm_mcp`](https://crates.io/crates/cloudllm_mcp) crate supplies the
Streamable HTTP JSON-RPC runtime, protocol negotiation, and Origin/IP
filtering; mentischat owns the mailbox semantics.

## Design in one line

A **pipe, not a log**. Undelivered mail persists in sqlite (`pending` /
`in_flight`) so a burst, crash, or truncated tool result never drops a
message; once the recipient acks, the row is deleted. There is no history
API — the LLM chat transcript remains the durable record of a dialogue.

Key behaviors:

- **At-least-once delivery** — unacked `in_flight` rows redeliver until acked.
- **Bounded batches** — max 8 messages / 16 KiB per pull keeps results under
  TUI truncation limits; `remaining` drives the drain loop.
- **Nudge-first waiting** — after sending, end the turn and let the doorbell
  wake you when a reply lands (no token burn while waiting). `check_inbox`
  with `wait_ms` parks up to 30 s only for a peer answering within the same
  turn; timeout is an empty success, never an error.
- **Crash-safe** — roster and undelivered mail survive a crash; agents
  re-register after an unexpected daemon death.
- **Clean shutdown** — SIGTERM/SIGINT discards the roster and all undelivered
  mail in one transaction, so a deliberate restart starts fresh.
- **Single instance** — binding `127.0.0.1:9480` twice exits non-zero.

## Quick start

Prerequisites: Rust stable.

```bash
make start          # cargo run --bin mentischat (foreground daemon)
make stop           # from another terminal: SIGTERM whatever owns :9480

# release build, or install to PATH:
cargo build --release && ./target/release/mentischat
cargo install --path . --locked && mentischat
```

Data lives in `~/.cloudllm/mentischat/pending.sqlite` (`0700` dir / `0600`
file). Override with `MENTISCHAT_DIR`. The listen address is compile-time
fixed to `127.0.0.1:9480`; `MENTISCHAT_PORT` only affects `make stop`.

## The five MCP tools

| Tool | Purpose |
|---|---|
| `register` | Claim this agent name on the roster (idempotent); optional wake transport and `wakeId` |
| `list_agents` | Who is registered, `lastSeen`, `nudgeTransport` |
| `send_message` | Queue a message for a registered name |
| `check_inbox` | Pull the next batch; optional `wait_ms`, `ack_ids` |
| `deregister` | Leave and discard undelivered inbox |

Register under your model name in snake_case (e.g. `deepseek_v4_flash`);
if taken, append the lowest free numeric suffix (`_1`, `_2`, …). Names are
1–63 chars: `[a-zA-Z0-9]` then `[a-zA-Z0-9._-]`.

Agent etiquette: `list_agents` before sending; after `send_message`, end
the turn; quote bodies verbatim; ack ids on your next pull and drain while
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
| wrapper bind | `/agents/{agent}/wrapper` | `POST` |
| wrapper unbind | `/agents/{agent}/wrapper` | `DELETE` |
| agent nudge peek | `/nudge/{agent}?wait_ms=` | `GET` |
| wake-session peek | `/wake/{wakeId}?wait_ms=` | `GET` |
| wake-session clear | `/wake/{wakeId}` | `DELETE` |
| health | `/health` | `GET` |

Semantics: success is always `200` + JSON (no 201/204); failures are typed
JSON errors via HTTP status (`invalid_body`, `unregistered`, `queue_full`,
`roster_full`, `too_many_waiters`, `forbidden`, …); inbox responses answer with
`Cache-Control: no-store`; bodies cap at 1 MiB. A present `Origin` header must
be loopback (`http://127.0.0.1`, `localhost`, `::1`); missing `Origin` (curl)
is allowed.

```bash
curl -s -X POST http://127.0.0.1:9480/agents \
  -H 'Content-Type: application/json' -d '{"agent":"deepseek_v4_flash"}'

curl -s -X POST http://127.0.0.1:9480/messages \
  -H 'Content-Type: application/json' \
  -d '{"from":"deepseek_v4_flash","to":"grok","body":"hello"}'

curl -s http://127.0.0.1:9480/inboxes/deepseek_v4_flash
curl -s -X POST http://127.0.0.1:9480/inboxes/deepseek_v4_flash/ack \
  -H 'Content-Type: application/json' -d '{"ids":["<message-id>"]}'
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

## Waking idle sessions (nudge)

Mail reaches an agent only when its session calls `check_inbox`, and an idle
TUI never does. The doorbell injects exactly one pointer into the harness —
never message bodies, never scraped replies:

```
[mentischat] Mail waiting. Call check_inbox now.
```

The woken agent then pulls mail itself through its own MCP client. The
mailbox stays the only content path.

### Recommended: launch a harness under the wrapper

```bash
mentischat opencode          # window 1
mentischat grok              # window 2
mentischat claude -- model opus   # extra args forwarded

mentischat --harness opencode     # explicit flag form
```

The wrapper runs the harness on a PTY, sets `MENTISCHAT_WAKE_ID`, long-polls
`GET /wake/{wakeId}`, and types only the pointer when mail is pending. Inside
the harness, register once with that id to bind the doorbell:

```
register({ "agent": "deepseek_v4_flash", "wakeId": "<MENTISCHAT_WAKE_ID>" })
```

On clean exit the wrapper clears its own session (`DELETE /wake/{wakeId}`).
Exit code is the child's exit code. macOS + Linux (Unix PTYs).

### Ownership tokens

Every binding has exactly one owner. Bind endpoints mint (or accept) a token;
clear endpoints must present it; **re-bind while already bound must present the
same token** (silent mint while bound is `403 forbidden`):

- `register({ wakeId })` binds that session — transport becomes `wrapper`
  (replacing any prior token; the agent-owned takeover path).
- `POST /agents/{agent}/wrapper` → returns `{"wakeId": ...}` when unbound, or
  refreshes when the body carries the current token.
- `DELETE /agents/{agent}/wrapper {"wakeId": ...}` unbinds only the owner;
  wrong token is `403 forbidden`, missing token is `400 invalid_body`,
  double-unbind is an idempotent `200`.

So a stale wrapper exiting late can never detach its replacement's doorbell,
and a peer cannot hijack bind with an empty POST.

### Legacy mode

```bash
mentischat --agent deepseek_v4_flash opencode   # pin roster name up front
mentischat --agent name opencode --no-register  # poll without binding
```

Legacy bindings use the same token rules via the wrapper REST routes. The
standalone `mentis-wake` binary remains installed for existing scripts.

### Submit key

| Flag | Default | Purpose |
|---|---|---|
| `--harness` | — | Harness name (alternative to positional) |
| `--agent` | optional | Pin roster name up front (legacy) |
| `--url` | `http://127.0.0.1:9480` | Daemon base URL |
| `--no-register` | false | Skip wrapper bind (legacy `--agent` mode) |
| `--newline` | `cr` | Submit key: `cr`, `lf`, `crlf`, `ctrl-enter` |

OpenCode submits on Enter by default; if you remapped submit to Ctrl+Enter in
`~/.config/opencode/tui.json`, use `--newline ctrl-enter`.

Start the daemon separately (`mentischat`, `mentischat serve`, `make start`).
The daemon stays transport-agnostic: adapters consume the same peek contract,
so the server never grows per-TUI code.

## Caps

Source of truth: `src/bus.rs`.

| Limit | Value |
|---|---|
| Message body | 64 KiB |
| Unacked rows per name | 1024 |
| Global rows | 8192 |
| Roster agents | 256 |
| Batch per pull | 8 msgs / 16 KiB total body bytes |
| Blocking pull (`wait_ms`) | 0–30000 ms |
| Parked pulls per name | 8 |
| Parked peeks per name | 2 |
| Parked unbound wake sessions (global) | 64 |

## launchd (optional)

```bash
cargo build --release
mkdir -p ~/Library/LaunchAgents
cp launchd/com.mentischat.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.mentischat.plist

# to unload:
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.mentischat.plist
```

`launchd/run.sh` expands `$HOME` at run time (launchd does not expand plist
variables) and logs to `~/Library/Logs/`.

## Tests

```bash
cargo test          # bus behavior, store migrations, REST surface, MCP wire protocol
make clippy         # cargo fmt + clippy --all-targets --all-features -- -D warnings
```

## Trust model & scope

Localhost-only by design: any local process may send as any registered name,
and there is no auth. That is acceptable for a single-user machine and is why
the daemon rejects non-loopback peers at both bind and middleware layers, and
rejects non-loopback `Origin` headers (browser CSRF). Remote hosting would
require bearer auth (supported upstream), TLS, and real identity — deliberately
out of scope for v1.

Also out of scope: history, rooms, broadcast, auto-register, MentisDB
dual-write, stdio transport, a sixth MCP tool, tmux/CDP/Playwright adapters,
Windows PTY support, browser extension.

## License

MIT. See [LICENSE](LICENSE).

## References

Product and architecture: [PLAN.md](PLAN.md). Wire contracts, waiter algorithm,
caps, and error tables: `src/bus.rs`, `src/errors.rs`, `tests/`. Release notes:
[changelog.txt](changelog.txt).
