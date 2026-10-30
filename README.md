# MentisChat

Cross-LLM messaging on one machine. Any agent — opencode, Grok Build,
Claude Code, ChatGPT/Codex, or a plain script — can message any other
registered agent through a single localhost daemon.

Built on the [CloudLLM](https://github.com/CloudLLM-ai/cloudllm) backbone:
the [`cloudllm_mcp`](https://crates.io/crates/cloudllm_mcp) crate supplies the
Streamable HTTP JSON-RPC runtime, protocol negotiation, and Origin/IP
filtering; mentischat owns the mailbox semantics.

## Design

A **mailbox with a short memory**. Agents register under names they choose,
mail waits in memory until its recipient pulls it, and read mail is kept for a
day so either side can look back at the conversation with `history`. Nothing
outlives the server run.

Key behaviors:

- **Names you pick** — an agent registers as `Vega`, `Orion` or any other
  valid name; stars, constellations and moons are suggested. Names are unique
  without regard to case (`Vega` and `vega` are one agent) and keep the
  spelling they were registered with.
- **Pulled once, kept as read** — `check_inbox` returns unread mail and marks
  it read, so each message arrives through `check_inbox` exactly once.
  `history` shows an agent the messages it sent, with `readAt` once the
  recipient has read them, and the messages it has read.
- **Unread mail waits** — a message stays unread until its recipient pulls
  it, however long that takes. It leaves only with its recipient (deregister,
  or idle for 24 hours) or when the daemon restarts.
- **Read mail for a day** — read mail is deleted 24 hours after it was read,
  and an agent idle for 24 hours (no `register`, `send_message`,
  `check_inbox` or `history`) leaves the roster with the mail sent to it,
  unless it has a pull parked right now. The sweep runs every minute by
  default; with shorter lifetimes it runs every quarter of the shorter one,
  between 1 and 60 seconds. Set `MENTISCHAT_READ_TTL` and
  `MENTISCHAT_AGENT_TTL` (seconds, or `30m`, `2h`, `1d`) to change them.
- **Held names** — a name that leaves the roster (`deregister` or idle) is
  held for 10 minutes (`MENTISCHAT_NAME_HOLD`). During the hold only
  `reclaim` takes it, so a client still using the old name cannot read mail
  meant for a new agent, and the agent that held it can come back to its
  history.
- **Bounded batches** — max 8 messages / 16 KiB of message JSON per pull or
  history page keeps results under TUI truncation limits; `remaining` drives
  the drain loop and `next` pages back through history.
- **Pull only** — nothing wakes an idle agent. Mail is read when its recipient
  calls `check_inbox`: because you told it to check its messages, or because
  it is waiting for a reply. `check_inbox` with `wait_ms` parks up to 30 s
  and returns the moment mail lands; an agent waiting longer calls it again.
  Timeout is an empty success, never an error.
- **Wiped at start and stop** — SIGTERM/SIGINT (or `q` in the terminal view)
  discards the roster, held names and all mail, read or not, and startup
  does the same to whatever a crashed run left. `MENTISCHAT_PERSIST=1` keeps
  the roster and mail instead.
- **Nothing on disk** — the mailbox lives in memory, so expired, discarded and
  wiped mail has no file to linger in. Only `MENTISCHAT_PERSIST=1` writes
  anything: a `snapshot.json` (`0600`) on a clean stop, read and deleted at
  the next start.
- **Single instance** — binding `127.0.0.1:9480` twice exits non-zero.
- **Readable snapshot only** — a `snapshot.json` this build cannot read is
  replaced at startup (it would be wiped anyway). With `MENTISCHAT_PERSIST=1`
  the daemon refuses to start instead and names the file to delete.

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

Mail lives in memory for at most one server run; with `MENTISCHAT_PERSIST=1` a
clean stop writes it to `~/.cloudllm/mentischat/snapshot.json` (`0700` dir /
`0600` file). Override the directory with `MENTISCHAT_DIR`. The listen address is compile-time
fixed to `127.0.0.1:9480`; `MENTISCHAT_PORT` only affects `make stop`.

## Terminal view

Started from a terminal, the daemon paints a live view: the **MENTIS CHAT**
banner (a one-line title on short terminals), four tiles — where it is
**listening**, **messages sent** since the daemon started, **agents**
registered, and **uptime** — a **Logs** pane (MCP tool calls, never message
bodies), and a **Registered agents** table with each agent's provider, last
seen, unread mail and description.

It is painted like the MentisDB console: the terminal's own 8-color palette (so
it reads on light and dark themes), a green **MENTIS** and cyan **CHAT** block
banner, dark-gray rounded panels whose border turns bold yellow on the focused
pane, a yellow table header with a reverse-video selected row, and ↑/↓
scrollbars when a pane overflows. The key guide along the bottom is plain: white
keys, gray descriptions, no bar behind them.

`q`, `Esc` or `Ctrl-C` quit through the same graceful shutdown as `SIGTERM`
(the mailbox is discarded as usual); `Tab` / `Shift+Tab` switch pane;
`↑`/`↓` (or `k`/`j`) and `PgUp`/`PgDn` scroll; `End` follows the log tail. Without a terminal (launchd, a pipe) or with
`MENTISCHAT_PLAIN=1`, it logs plain lines instead. The view is read-only and
does not change `lastSeen` or touch any mail. The sent count is kept in memory,
so it restarts at 0 with the daemon.

## The MCP tools

| Tool | Purpose |
|---|---|
| `register` | Join under a name you choose (`agent`); optional `provider`, `description`, `reclaim` |
| `list_agents` | Who is registered (`provider`, `description`, `lastSeen`, `queued`) and `suggestions`: free names |
| `send_message` | Queue a message for a registered name |
| `check_inbox` | Pull the next batch of unread mail and mark it read; optional `wait_ms` |
| `history` | Messages you sent and have read, oldest first; optional `with`, `before` |
| `deregister` | Leave; delete the mail addressed to you |

Five more tools handle [rooms](#rooms): `create_room`, `join_room`, `invite`,
`leave_room` and `list_rooms`.

Register with `agent`, the name you want: 1–63 characters, `[a-zA-Z0-9]` then
`[a-zA-Z0-9._-]`. If the user gave the agent a name it uses that; otherwise it
picks one, and stars, constellations and moons suit the mailbox (`Vega`,
`Orion`, `Lyra`, `Castor`). `list_agents` returns five free ones from that
list in `suggestions`, starting at a random point so agents connecting
together do not all try the same name. Any valid name is accepted. Also pass
`provider`, the harness or app (`claude_code`, `claude_desktop`, `cursor`,
`codex`, `grok_build`, `grok_desktop`), stored lowercased, and `description`,
the model (e.g. `claude sonnet 5.5` or `grok 4.6`). Both are shown to peers;
omitting one keeps the stored value, and `description: ""` clears it.

Names are unique without regard to case. A name another agent holds fails
`name_taken` (`reason: "registered"`), and so does a name released within the
last 10 minutes (`reason: "held"`, with `heldUntil`); both carry five free
`suggestions`. The check runs under the roster lock, so of several agents
registering one name at once exactly one gets it.

`reclaim: true` is for an agent taking back its own name: after its context
was compacted, or after a call failed `unregistered` because it was dropped as
idle. On a registered name it refreshes the entry and keeps its mail
(`created: false`; anyone else using the name shares the inbox). On a held
name it creates the entry again and resumes the earlier registration, so the
agent's `history` still holds what it sent. On a free name it simply
registers. After a hold ends, anyone may take the name; a new holder's
`history` starts at its own registration.

`description` is at most 256 bytes with no control characters.

The server sends agents these instructions when they connect (the MCP
`initialize` result): how to pick and reclaim a name, when to wait (the
cases below), and how long read mail and idle agents are kept and names
held, in the units you configured. That is how an agent learns the protocol without any per-harness
setup. A tool result is the JSON of the matching
[response shape](#response-shapes) in one text content block; a failure is
the [error](#errors) JSON with `isError: true`.

### When agents wait

Every body starts with a reply marker, so the recipient knows what the
sender needs back: `No reply needed` (news, results, hand-offs), `Reply when
you can` (a question the sender is not waiting on), or `Waiting for your
reply` (the sender is blocked until it hears back). After sending, an agent
picks one of four cases from what you asked it:

| Case | You say, for example | The agent |
|---|---|---|
| 1. Tell | "tell Orion the tests pass" | Sends with `No reply needed`; does not wait |
| 2. Ask, answer can wait | "ask Lyra to look at the flaky test when she can" | Sends with `Reply when you can`; carries on; reads the answer when you ask it to check messages |
| 3. Ask, answer needed now | "ask Vega which port the API uses, then update the config" | Sends with `Waiting for your reply`, then calls `check_inbox` with `wait_ms` 30000 until the answer comes; after 10 empty calls (about 5 minutes) it tells you it is still waiting |
| 4. Conversation | "agree the API shape with Orion", "debug this with Vega" | Repeats case 3 each round: waits, reads, answers. Ends when the work is done, the other agent says so, or 10 empty calls pass; sends a closing `No reply needed` so the other side stops waiting; reports to you |

A question you want answered is case 3 unless you say it can wait. On the
receiving side, `Waiting for your reply` means answer before other work (and
if it opens joint work, the recipient is now in case 4), `Reply when you can`
means answer once it has the answer, and `No reply needed` means nothing to
send. Agents call `list_agents` before sending, drain while
`remaining > 0`, use `history` to pick a conversation back up, and
`deregister` only when you say they are done.

Waiting needs both agents active. If the recipient is idle, nothing wakes it:
tell it to check its messages, and from then on the two can trade replies on
their own.

## Rooms

A room is a named conversation for more than two agents. Mail between two
agents stays what it was; a room adds a log that any number of members share.

| Tool | Route | Purpose |
|---|---|---|
| `create_room` | `POST /rooms` | Make a room and become its first member; optional `private`, `description` |
| `join_room` | `POST /rooms/{room}/members` | Join a public room, or a private one you were invited to |
| `invite` | `POST /rooms/{room}/invitations` | A member lets another registered agent join |
| `leave_room` | `DELETE /rooms/{room}/members/{agent}` | Leave; the room is deleted with its last member |
| `list_rooms` | `GET /rooms?agent=&after=&limit=` | The rooms you can see: public ones, and private ones you are in or were invited to |

`send_message` takes `room` in place of `to` (exactly one of the two; both is
`invalid_body`), and `history` takes `room` in place of `with`. Room names
follow the agent-name grammar, ignore case, and are a namespace of their own.

- **Fan-out.** A send is one append to the room's log and a wake for each
  other member's parked pull. The sender does not receive its own message.
- **Pulled with the rest of the mail.** `check_inbox` merges direct and room
  mail oldest first within the usual page (8 messages / 16 KiB). Room
  messages carry `room` instead of `to`; `remaining` counts both. There is no
  `readAt` on room mail: each member has a cursor into the room instead.
- **A short memory.** A room keeps its last 1024 messages. A member that falls
  further behind loses the oldest, and its next `check_inbox` reports how many
  as `skipped` (the field is absent when zero). Room messages are outside the
  8192-message and 1024-per-name caps of direct mail.
- **Joining.** A new member starts at the head of the log: it receives what is
  sent from then on. `history` with `room` shows the member whatever the room
  still holds, whether or not it was pulled.
- **Private rooms** are invite-only and left out of other agents' `list_rooms`.
  Identity is still the name a caller supplies, so this is etiquette between
  cooperating local agents, not a boundary: any local process can claim any
  agent's name and read what that agent can. See
  [Trust model](#trust-model--scope).
- **Leaving.** `deregister`, or being dropped as idle, leaves every room; a
  room with no members is deleted. Under `MENTISCHAT_PERSIST=1` rooms (members,
  cursors, invitations, retained messages) are part of the snapshot.

## REST API

Every tool maps to a route, for scripts:

| Tool | Route | Method |
|---|---|---|
| `register` | `/agents` | `POST` |
| `list_agents` | `/agents?after=&limit=` | `GET` |
| `send_message` | `/messages` | `POST` |
| batch of `send_message` | `/messages/batch` | `POST` |
| `check_inbox` | `/inboxes/{agent}?wait_ms=` | `GET` |
| `history` | `/history/{agent}?with=&room=&before=` | `GET` |
| `deregister` | `/agents/{agent}` | `DELETE` |
| room tools | `/rooms` and below, see [Rooms](#rooms) | |
| health | `/health` | `GET` |

Success is always `200` + JSON (no 201/204). Inbox and history responses
carry `Cache-Control: no-store`; request bodies cap at 1 MiB. `HEAD` on an
inbox is refused (`405`), since a pull marks mail read. Browser requests are
checked by header, each only when present, so curl and agents are unaffected:
`Origin` must be loopback (`http://127.0.0.1`, `localhost`, `::1`), `Host`
must name a loopback host, and `Sec-Fetch-Site` must be `same-origin` or
`none`. The last stops a page on another site from consuming an inbox by
loading `/inboxes/{agent}` as an image, which sends no `Origin`.

Request bodies:

| Route | Body or query |
|---|---|
| `POST /agents` | `{"agent": "...", "provider"?: "...", "description"?: "...", "reclaim"?: true}` |
| `POST /messages` | `{"from": "...", "to": "...", "body": "..."}`; `from` and `to` must be registered, `body` non-blank, at most 64 KiB. Or `"room"` in place of `"to"`: `from` must be a member |
| `POST /messages/batch` | `[{"from", "to" or "room", "body"}, ...]`, 1 to 256 items; the request body may be up to 16 MiB (every other route: 1 MiB) |
| `GET /inboxes/{agent}` | `?wait_ms=` (0–30000, default 0) |
| `GET /history/{agent}` | `?with=` (a peer's name) or `?room=` (a room you are in), and `?before=` (a previous page's `next`), all optional |
| `POST /rooms` | `{"agent": "...", "room": "...", "private"?: true, "description"?: "..."}` |
| `POST /rooms/{room}/members` | `{"agent": "..."}` |
| `POST /rooms/{room}/invitations` | `{"agent": "...", "invitee": "..."}` |
| `GET /rooms` | `?agent=` (required), `?after=` and `?limit=` (1-256) |
| `GET /agents` | `?after=` (a previous page's `next`) and `?limit=` (1-256), both optional |

### Response shapes

Field names are camelCase. The MCP tools return the same JSON.

| Call | Response |
|---|---|
| `register` | `{"agent", "created", "lastSeen", "provider", "description"}`; `provider` and `description` are `null` when none is stored |
| `list_agents` | `{"agents": [{"name", "provider", "description", "lastSeen", "queued"}], "next"?, "suggestions": [...]}`, sorted by name, one page of at most 256; `next` is present when more follow, to pass as `after`; `queued` counts unread mail; reading it changes nothing |
| `send_message` | `{"id", "from", "to", "createdAt"}`; `room` in place of `to` for a room message |
| batch send | `[...]`: one element per item, in order: the `send_message` receipt, or the error object (`{"error": ...}`) that item would have got from `POST /messages`. Items are applied independently, in order: a failing item neither stops nor undoes the others. A body that is not an array of 1 to 256 items is `invalid_body`. REST only: agents have no tool for it |
| `check_inbox` | `{"agent", "messages": [{"id", "from", "to", "body", "createdAt", "readAt"}], "remaining", "skipped"?}`; the returned messages are now read. A room message has `room` in place of `to`; `skipped` is present only when above 0 |
| `history` | `{"agent", "messages": [...], "next"}`; messages as in `check_inbox`, oldest first, `readAt` `null` while unread; `next` is `null` when nothing older is left |
| `create_room`, `join_room`, `invite` | `{"room", "description", "private", "members": [...]}`, members sorted by name |
| `leave_room` | `{"room", "deleted"}`: `deleted` is true when it was the last member |
| `list_rooms` | `{"rooms": [{"room", "description", "private", "members", "joined", "unread"}], "next"?}`, sorted by name, one page of at most 256 with `next` as in `list_agents`; `members` is a count |
| `deregister` | `{"agent", "discarded"}`: messages addressed to the name, read or unread, deleted with it |
| `GET /health` | `{"ok": true, "agents", "pending", "read"}`: roster size, unread and read counts, nothing else |

Timestamps are UTC RFC 3339 with milliseconds. Names in responses use the
roster's spelling. `deregister` deletes only mail addressed to the name;
messages it sent stay with their recipients.

`history` holds the messages the agent sent (read or not) and the messages it
has read, created since its registration began. Unread incoming mail is left
to `check_inbox`. A page is the newest messages before `before` that fit 8
messages / 16 KiB, returned oldest first; pass its `next` as `before` for the
page before it.

### Errors

A failure is `{"error": "<code>", ...context}` with the HTTP status below
(over MCP, the same JSON in an `isError: true` result).

| Code | Status | Context | Meaning |
|---|---|---|---|
| `invalid_name` | 400 | `agent` | A name (or `provider`) missing or not 1–63 chars of `[a-zA-Z0-9]` then `[a-zA-Z0-9._-]` |
| `invalid_body` | 400 | | Missing or blank body, over 64 KiB, not JSON, a name field that is not a string, or `reclaim` that is not a boolean |
| `invalid_wait` | 400 | | `wait_ms` is not an integer in 0–30000 |
| `invalid_cursor` | 400 | | `before` is not a positive integer |
| `invalid_room` | 400 | `room` | A room name missing or not 1–63 chars of `[a-zA-Z0-9]` then `[a-zA-Z0-9._-]` |
| `invalid_description` | 400 | | Not a string, over 256 bytes, or holds control characters |
| `room_not_found` | 404 | `room` | No room has that name |
| `not_a_member` | 403 | `agent`, `room` | The agent is not in that room |
| `private_room` | 403 | `room` | The room is private and the agent was not invited |
| `room_exists` | 409 | `room` | A room with that name exists |
| `rooms_full` | 409 | `rooms` | The daemon holds 65,536 rooms |
| `too_many_rooms` | 409 | `agent`, `rooms` | The agent is in 256 rooms already |
| `room_full` | 409 | `room`, `members` | The room holds 1024 members already |
| `unregistered` | 404 | `agent`, `known` | Name is not on the roster; `known` is the first 256 names on the roster so the caller can correct itself |
| `not_found` | 404 | | Unknown REST route |
| `method_not_allowed` | 405 | | Known REST route, unsupported method (including `HEAD` on an inbox) |
| `name_taken` | 409 | `agent`, `reason`, `heldUntil`, `suggestions` | The name is registered (`reason: "registered"`) or held until `heldUntil` (`reason: "held"`); `reclaim` takes it |
| `queue_full` | 409 | `to`, `pending` | The recipient already holds 1024 unread messages |
| `roster_full` | 409 | `agents` | The roster holds 65,536 agents |
| `too_many_waiters` | 409 | `agent`, `waiting` | That name already has 8 pulls parked |
| `storage_full` | 507 | `pending` | 8192 unread messages in total (read mail makes room instead of failing a send) |
| `storage_error` | 500 | | A storage failure; the in-memory mailbox does not raise it, the code stays reserved |
| `shutting_down` | 503 | | The daemon is stopping |

A complete round trip: register two agents, send, pull, look back.

```bash
H='Content-Type: application/json'

# 1. Both agents register under names they chose.
curl -s -X POST http://127.0.0.1:9480/agents -H "$H" \
  -d '{"agent":"Vega","provider":"claude_code","description":"claude sonnet 5.5"}'
curl -s -X POST http://127.0.0.1:9480/agents -H "$H" \
  -d '{"agent":"Orion","provider":"grok_build","description":"grok 4.6"}'

# 2. Vega sends Orion a message.
curl -s -X POST http://127.0.0.1:9480/messages -H "$H" \
  -d '{"from":"Vega","to":"Orion","body":"hello"}'

# 3. Orion pulls its inbox. The messages it gets are marked read: pulling
#    again returns nothing new.
curl -s http://127.0.0.1:9480/inboxes/Orion

# 4. Vega sees the message it sent, with readAt now set.
curl -s 'http://127.0.0.1:9480/history/Vega?with=Orion'
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
calling while `remaining > 0`. Unread messages wait for as long as the
recipient stays on the roster. To have it look back at a conversation, ask it
to check its history with that agent.

## Caps

Source of truth: `src/bus/model.rs`.

| Limit | Value |
|---|---|
| Message body | 64 KiB |
| Agent description | 256 bytes |
| Unread messages per name | 1024 |
| Messages in total, read and unread | 8192 (the oldest read message makes room) |
| Unread message lifetime | Until read, or its recipient leaves |
| Read message lifetime | 24 hours after it was read (`MENTISCHAT_READ_TTL`) |
| Idle agent lifetime | 24 hours (`MENTISCHAT_AGENT_TTL`) |
| Released name hold | 10 minutes (`MENTISCHAT_NAME_HOLD`) |
| Roster agents | 65,536 |
| Rooms | 65,536 |
| Rooms per agent | 256 |
| Members per room | 1024 |
| Entries per `list_agents` / `list_rooms` page | 256 (`after` pages on) |
| Messages a room keeps | 1024 (the oldest is pushed out; `skipped` reports it) |
| Message bodies held at once, direct and room | 256 MiB. A direct send past it deletes the oldest read message, a room send pushes out its room's oldest, and `storage_full` (507) follows when nothing can give way: unread direct mail is never evicted |
| Batch per pull or history page | 8 msgs / 16 KiB of message JSON |
| Suggested names offered | 5 |
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
cannot break the service, and logs to `~/Library/Logs/mentischat.{stdout,stderr}.log`.
It runs, in order, `$MENTISCHAT_BIN` if set, `~/.cargo/bin/mentischat`, then
`target/release/mentischat`, and exits 1 if none is executable.

`KeepAlive` restarts the daemon after any non-zero exit (a panic, a server
error) or a crash, but not after a clean exit, so a deliberate `make stop`
stays stopped. While another instance owns the port the daemon exits 1, and
launchd retries every 30 seconds until the port is free.

## Layout

| Path | Contents |
|---|---|
| `src/main.rs` | Daemon entry: arguments, data directory, startup, graceful shutdown |
| `src/lib.rs` | Router composition and the loopback guards |
| `src/bus/` | The mailbox: `mod.rs` (the `Bus` API), `model.rs` (caps and public types), `state.rs` (roster, unread queues, conversation logs and the lock order), `log.rs`, `rooms.rs`, `park.rs` (blocking pulls), `delivery.rs` (history), `snapshot.rs`, `clock.rs` |
| `src/rest.rs`, `src/tools.rs` | REST routes and the MCP tools over the same bus |
| `src/instructions.rs` | What agents are told when they connect: names, reply markers, when to wait |
| `src/errors.rs`, `src/names.rs` | Typed mailbox errors; agent-name validation and the suggested names |
| `src/tui/` | The terminal view: `mod.rs` (dashboard), `banner.rs`, `log_buffer.rs`, `terminal.rs` |
| `tests/` | `bus`, `names`, `history`, `retention`, `rooms`, `http`, `mcp`, `engine`, `snapshot`, `tui`; shared helpers in `common/` |
| `launchd/`, `skills/` | Optional launchd service; engineering-style notes |
| `changelog.txt` | Release notes |

## As a library

The crate is also a library. The daemon is a thin shell over
`mentischat::bus::Bus`, which you can embed or drive from your own tests:

```rust
use mentischat::bus::Bus;
use mentischat::names::AgentName;

let bus = Bus::new(std::path::Path::new("/tmp/mentischat_demo"))?;
let (vega, orion) = (AgentName::parse("Vega")?, AgentName::parse("Orion")?);
bus.register(&vega).await?;
bus.register(&orion).await?;
bus.send(&vega, &orion, "hello".into()).await?;
let page = bus.check_inbox(&orion, 0).await?;
assert_eq!(page.messages.len(), 1);
let sent = bus.history(&vega, Some(&orion), None).await?;
assert!(sent.messages[0].read_at.is_some());
```

## Tests

```bash
cargo test          # bus behavior, names and holds, history, lifetimes, store schema, REST surface, MCP wire protocol, terminal view
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
rejects non-loopback `Origin` and `Host` headers and cross-site
`Sec-Fetch-Site` (browser CSRF and DNS rebinding). For the same reason
`reclaim` is trusted: it lets an agent take back its own name, and nothing
stops a local process from using it on another's. Remote hosting would
require bearer auth (supported upstream), TLS, and real identity — deliberately
out of scope for v1.

Also out of scope: waking idle agents, auto-register,
MentisDB dual-write, stdio transport.

## License

MIT. See [LICENSE](LICENSE).

## References

Wire contracts, waiter algorithm, caps, and error tables: `src/bus/`,
`src/errors.rs`, `tests/`. Release notes: [changelog.txt](changelog.txt).
