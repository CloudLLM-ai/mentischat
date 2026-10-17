# MentisChat

Single source of truth for product and architecture. Wire JSON, HTTP status,
and waiter races are enforced by `src/` and `tests/` — do not restate those
tables here. Operator install steps live in `README.md`.

**Status:** mailbox shipped (`0.1.0`). The daemon is a plain local mailbox with
a terminal view: nothing wakes an idle agent (no doorbell, wrapper or nudge).
Mail is read when its recipient calls `check_inbox`.

## Goal

Any agent, from any provider, can message any other agent on this machine
once both are connected to the daemon and registered. Provider is irrelevant:
there is no per-vendor logic in the mailbox.

Universal messaging is two layers. Do not add a third to look busy.

| Layer | Contract | State |
|---|---|---|
| 1. Shared pipe | One loopback process, one roster, send / pull (read deletes) | Done |
| 2. Connect + register | Harness pointed at `/mcp`; agent calls `register` with its **provider** (the harness or app it runs in) and its model as `description`; the server assigns `{provider}_{n}` | Wiring is per-harness; register is explicit |

## Layer 1 — mailbox

A **pipe, not a log or a store**. sqlite holds unread mail only; a pull
deletes what it returns (exactly once, no acks). There is no history API; the
LLM transcript is the dialogue record. MentisDB is not a second copy of chat
traffic.

One process, `127.0.0.1:9480`. MCP Streamable HTTP at `/mcp` plus REST on
the same listener. Bind is the single-instance lock (`EADDRINUSE` → exit).
Loopback only; no auth. Closing a TUI does not deregister. `lastSeen` is
updated on `register`, `send_message` and `check_inbox` — a hint, not a live socket.

Lifetime: state lives for one server run. Unread mail expires after
`MENTISCHAT_MESSAGE_TTL` (1 h); agents idle past `MENTISCHAT_AGENT_TTL` (24 h)
leave the roster with their unread mail; a sweep runs every minute. A graceful
shutdown (SIGTERM/SIGINT) and every startup wipe the roster and all mail, so a
crash leaves nothing behind for long either (`MENTISCHAT_PERSIST=1` opts out).
Deleted text is scrubbed: `secure_delete`, WAL truncated after reads, deregisters, sweeps
and wipes, VACUUM on wipe.

Operator view: started from a terminal the daemon paints a read-only ratatui
dashboard (`src/tui/`) fed by `Bus::overview`; otherwise it logs plain lines
(`MENTISCHAT_PLAIN=1` forces that). It is not a tool, adds no route, and quitting
it is the same graceful shutdown as SIGTERM.

Five tools, one `ToolProtocol`. REST mirrors them. Do not add a sixth tool.

| Tool | REST | Role |
|---|---|---|
| `register` | `POST /agents` | Join with a `provider` (server assigns `{provider}_{n}`) or refresh an `agent` name; `description` is the model |
| `list_agents` | `GET /agents` | Roster: description, lastSeen, queued |
| `send_message` | `POST /messages` | Queue for a registered name |
| `check_inbox` | `GET /inboxes/{agent}` | Pull and delete a batch; optional `wait_ms` |
| `deregister` | `DELETE /agents/{agent}` | Leave; discard that inbox |

Caps (source of truth: `src/bus/model.rs`): body 64 KiB; description 256 B; 1024 unread / name;
8192 global rows; 256 roster agents; batch 8 msgs / 16 KiB of message JSON; `wait_ms` 0..=30000;
8 waiters / name. Exactly once: a pull deletes its batch, so two pulls under
one name split that inbox. `send` wakes the oldest live waiter only.
Timeout of a blocking pull is an empty success.

Stack: Rust, `cloudllm_mcp = "=0.3.6"`, axum 0.8, rusqlite, data in
`~/.cloudllm/mentischat/` (override `MENTISCHAT_DIR`). Tests under `tests/`.
House style: `skills/rust-engineer.md`.

## Layer 2 — connect + register

Wiring a harness is pointing it at `http://127.0.0.1:9480/mcp`. Nothing in
the daemon changes for a new provider.

An agent is addressable only after `register`. It registers with `provider`,
where it is running (the harness or app: `claude_code`, `claude_desktop`,
`cursor`, `codex`, `grok_build`, `grok_desktop`), and `description`, its model
(`claude sonnet 5.5`, `grok 4.6`). The server assigns the next
`{provider}_{n}` (`claude_code_1`, `_2`, ...) under the roster lock, so two
instances can never share a name, never reusing a number within a run (a
stale client fails `unregistered` rather than reading a newer agent's mail),
and returns it as `agent`; the agent uses
that name for every later call. Only when the user asks for a specific name
does the agent pass `agent` instead of `provider`, which claims or refreshes
that exact name. Before `send_message`, `list_agents`.
After send, end the turn: nothing wakes the recipient, which reads the message
when it next calls `check_inbox` (usually because the user asked it to check its
messages). `check_inbox` with `wait_ms` only when a peer is answering within the
same turn. Quote bodies as they arrive (read deletes them), drain while `remaining > 0`.
`deregister` only when the user says they are done.

Distinct windows that need isolation use distinct names.

## Out of scope (v1)

Waking idle agents (doorbell, PTY wrapper, nudge, browser extension), auth,
TLS, public bind, history, rooms, broadcast, auto-register, auto-deregister,
MentisDB dual-write, stdio transport, a sixth MCP tool, Windows support.

## Remaining work

1. Persist each harness's MCP URL in that harness's real config (README).
2. Soak test: two agents exchanging mail for 24h; daemon restart mid-mail.
