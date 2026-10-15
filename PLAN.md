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
| 1. Shared pipe | One loopback process, one roster, send / pull / ack | Done |
| 2. Connect + register | Harness pointed at `/mcp`; agent calls `register` with its **model name in snake_case** | Wiring is per-harness; register is explicit |

## Layer 1 — mailbox

A **pipe, not a log**. sqlite holds unacked mail (`pending` / `in_flight`).
Ack deletes the row. There is no history API; the LLM transcript is the
dialogue record. MentisDB is not a second copy of chat traffic.

One process, `127.0.0.1:9480`. MCP Streamable HTTP at `/mcp` plus REST on
the same listener. Bind is the single-instance lock (`EADDRINUSE` → exit).
Loopback only; no auth. Closing a TUI does not deregister. `lastSeen` is
updated on `register` and `check_inbox` — a hint, not a live socket.

Persistence: a crash (SIGKILL) leaves roster + undelivered mail on disk so
the mailbox recovers; a graceful shutdown (SIGTERM/SIGINT) discards the
roster and every pending/in_flight row in one transaction, so a deliberate
restart starts clean — a bulk deregister of everyone.

Operator view: started from a terminal the daemon paints a read-only ratatui
dashboard (`src/tui/`) fed by `Bus::overview`; otherwise it logs plain lines
(`MENTISCHAT_PLAIN=1` forces that). It is not a tool, adds no route, and quitting
it is the same graceful shutdown as SIGTERM.

Five tools, one `ToolProtocol`. REST mirrors them. Do not add a sixth tool.

| Tool | REST | Role |
|---|---|---|
| `register` | `POST /agents` | Claim a name (idempotent) |
| `list_agents` | `GET /agents` | Roster + lastSeen |
| `send_message` | `POST /messages` | Queue for a registered name |
| `check_inbox` | `GET /inboxes/{agent}` | Pull batch; optional `wait_ms`; optional `ack_ids` |
| `deregister` | `DELETE /agents/{agent}` | Leave; discard that inbox |

Caps (source of truth: `src/bus/model.rs`): body 64 KiB; 1024 unacked / name;
8192 global rows; 256 roster agents; batch 8 msgs / 16 KiB; `wait_ms` 0..=30000;
8 waiters / name. At-least-once:
unacked `in_flight` redelivers until someone acks (same batch caps). Two pulls
under one name share that inbox. `send` wakes the oldest live waiter only.
Timeout of a blocking pull is an empty success.

Stack: Rust, `cloudllm_mcp = "=0.3.6"`, axum 0.8, rusqlite, data in
`~/.cloudllm/mentischat/` (override `MENTISCHAT_DIR`). Tests under `tests/`.
House style: `skills/rust-engineer.md`.

## Layer 2 — connect + register

Wiring a harness is pointing it at `http://127.0.0.1:9480/mcp`. Nothing in
the daemon changes for a new provider.

An agent is addressable only after `register`. It registers under its model
name in snake_case (e.g. `deepseek_v4_flash`); it must not invent an
unrelated name. If the exact name is already on the roster — multiple
instances of one model can coexist — it appends a numeric suffix starting at
1 (`deepseek_v4_flash_1`, `_2`, ...) and picks the lowest free name. Before
`send_message`, `list_agents`.
After send, end the turn: nothing wakes the recipient, which reads the message
when it next calls `check_inbox` (usually because the user asked it to check its
messages). `check_inbox` with `wait_ms` only when a peer is answering within the
same turn. Quote bodies, ack ids on the next pull, drain while `remaining > 0`.
`deregister` only when the user says they are done.

Distinct windows that need isolation use distinct names.

## Out of scope (v1)

Waking idle agents (doorbell, PTY wrapper, nudge, browser extension), auth,
TLS, public bind, history, rooms, broadcast, auto-register, auto-deregister,
MentisDB dual-write, stdio transport, a sixth MCP tool, Windows support.

## Remaining work

1. Persist each harness's MCP URL in that harness's real config (README).
2. Soak test: two agents exchanging mail for 24h; daemon restart mid-mail.
