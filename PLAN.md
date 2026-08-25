# MentisChat

Single source of truth for product and architecture. Wire JSON, HTTP status,
and waiter races are enforced by `src/` and `tests/` — do not restate those
tables here. Operator install steps live in `README.md`.

**Status:** mailbox shipped (`0.1.0`). TUI wake is **`mentis-wake` only**
(macOS + Linux, no focus theft). The v1 in-daemon AppleScript doorbell was
removed; see [NUDGE-V1-TERMINAL.md](NUDGE-V1-TERMINAL.md). Browser doorbell
planned via a same-machine extension (Layer 4).

## Goal

Any agent, from any provider, can message any other agent on this machine
once both are connected to the daemon and registered. Provider is irrelevant:
there is no per-vendor logic in the mailbox.

Universal messaging is three layers. Ship them in order. Do not start a
later layer to look busy.

| Layer | Contract | State |
|---|---|---|
| 1. Shared pipe | One loopback process, one roster, send / pull / ack | Done |
| 2. Connect + register | Harness pointed at `/mcp`; agent calls `register` with its **model name in snake_case** | Wiring is per-harness; register is explicit |
| 3. Wake idle sessions | After mail lands, force the idle TUI into a turn so *it* calls `check_inbox` | `mentis-wake` PTY wrapper + peek endpoint |
| 4. Browser | Extension bridges the doorbell to a browser tab; mailbox tools still MCP/REST | Planned; starts only after layer 3 ships |

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

Five tools, one `ToolProtocol`. REST mirrors them. Do not add a sixth tool.

| Tool | REST | Role |
|---|---|---|
| `register` | `POST /agents` | Claim a name (idempotent) |
| `list_agents` | `GET /agents` | Roster + lastSeen |
| `send_message` | `POST /messages` | Queue for a registered name |
| `check_inbox` | `GET /inboxes/{agent}` | Pull batch; optional `wait_ms`; optional `ack_ids` |
| `deregister` | `DELETE /agents/{agent}` | Leave; discard that inbox |

Caps (source of truth: `src/bus.rs`): body 64 KiB; 1024 unacked / name;
8192 global rows; 256 roster agents; batch 8 msgs / 16 KiB; `wait_ms` 0..=30000;
8 waiters / name; 2 peeks / name; 64 unbound wake sessions. At-least-once:
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
After send, end the turn — the doorbell nudges the agent when a reply lands.
`check_inbox` with `wait_ms` only when a peer is answering within the same
turn. Quote bodies, ack ids on the next pull, drain while `remaining > 0`.
`deregister` only when the user says they are done.

Distinct windows that need isolation use distinct names.

## Layer 3 — nudge (TUI doorbell)

Mail reaches an agent only when its session calls `check_inbox`. A TUI at
a prompt never pulls. The doorbell injects **only** this pointer (one
constant in `src/nudge.rs`):

```
[mentischat] New message waiting -- call check_inbox.
```

Never the body, never a scraped reply, never `ack_ids`. The woken agent
uses its own MCP tools. The mailbox stays the only content path.

### One process, one peek contract

The daemon owns the mailbox, roster, five MCP tools, and side-effect-free
peek/long-poll endpoints (`GET /wake/{wakeId}` for session auto-bind;
`GET /nudge/{agent}` for legacy `--agent` wrappers). They never pull or ack
mail and have no knowledge of how any particular TUI wants to be woken beyond
the `nudge_transport` column. Waking is a transport decision on the roster row:

| Transport | Who wakes | Notes |
|---|---|---|
| `wrapper` | `mentis-wake` PTY wrapper | Set by `POST /agents/{agent}/wrapper` |
| `none` | No one until wrapper binds | Default for new registrations |

The `wrapper` value is set by `POST /agents/{agent}/wrapper`, which returns an
ownership token (`wakeId`) that the matching `DELETE` **and any re-bind** must
present — bindings are owner-scoped, so a stale wrapper exiting late cannot
clear its replacement, and a peer cannot hijack bind with an empty POST.
`register({ wakeId })` remains the agent-owned path that may replace a token.
Registering with `nudgeTransport: "none"` clears any session token along with
the transport; omitting the field on a new name is the normal path under
`mentis-wake` / `mentischat <harness>`.

| Tool | Wake |
|---|---|
| `register` | Claim a name; optional `nudgeTransport` (`none` or `wrapper`; `terminal` is a deprecated alias for `none`). Re-register without the field keeps the existing transport. `nudgeWindow` is always null. |
| `list_agents` | Each entry includes `nudgeTransport` (`wrapper` \| `none`). |
| `deregister` | Deletes the roster row. Inbox discarded as today. |
| `send_message` | After queueing, wake any parked nudge-peek long-polls for the recipient. |

Register still succeeds with `nudgeTransport: none` — the mailbox works; the
session cannot be woken idle until `mentis-wake` binds wrapper transport.

### The universal adapter (`mentis-wake`)

`mentis-wake` (also `mentischat <harness>`) is a PTY wrapper in the same crate
that sits between Terminal and any harness. The default path sets
`MENTISCHAT_WAKE_ID` and long-polls `GET /wake/{wakeId}?wait_ms=` (read-only —
never promotes, acks, or touches `lastSeen`); legacy `--agent` mode long-polls
`GET /nudge/{agent}` and binds via `POST /agents/{agent}/wrapper`. On a pending
peek it injects the single pointer constant plus a submit key. Clean exit
clears the binding (`DELETE`, transport → `none`).

Forwarding is byte-transparent with a 60s id-set debounce. SIGWINCH propagates
the terminal size to the PTY; raw-mode stdin forwards bytes (including Ctrl-C)
to the child. A process-level SIGINT/SIGTERM on the wrapper kills the child and
exits. macOS + Linux v1. The server never implements PTY logic — the wrapper is
the only client-side adapter and stays thin (no sqlite, no MCP).

No tmux, iTerm, Playwright, CDP, or Node.

## Layer 4 — browser (extension)

Do not build until Layer 3 ships and the TUI beta proves the nudge rule.

A browser LLM has no MCP client, so the doorbell cannot hold — unless a
same-machine bridge injects the pointer the way the daemon does in
Terminal. That bridge is a **browser extension**, not a userscript, and
not CDP/Playwright.

### The rule still holds

The mailbox stays the only content path. The extension never reads the
chat, never scrapes a reply, and never calls the mailbox tools itself. On
a nudge it does two things, in order: raise a Web Notification (human
fallback) and inject **only** the pointer constant into the chat input
box, so the model's next turn calls `check_inbox` via its own client. Same
single pointer as `src/nudge.rs`.

### The target is a tab, not a window

Future extension work consumes the same peek endpoint as `mentis-wake`; roster,
caps, ack, and waiter logic do not change.

### Loopback, same trust

The extension connects to `127.0.0.1:9480` over loopback, so the existing
non-loopback rejection still holds. It registers as a nudge transport, not
as an agent: no roster row, no sixth tool, no mailbox access beyond the
"you have mail" signal.

### Honest limits

Third-party hosted UIs (ChatGPT web, Claude.ai) are React/synthetic-DOM;
input injection is best-effort. If injection fails, the Notification still
wakes the human to nudge the model. For a client you control, prefer an
SSE push instead — the extension is the fallback for hosted UIs you cannot
reach.

## Out of scope (v1)

Auth, TLS, public bind, history, rooms, broadcast, auto-register,
auto-deregister, MentisDB dual-write, stdio transport, a sixth MCP tool,
tmux/CDP/Playwright, a separate nudge daemon, Windows support for the PTY
wrapper, the browser extension (v1). Any alternative adapter (tmux script,
native harness plugin, SSE consumer) is expected to consume the same peek
endpoint so the server never grows transport-specific code.

## Remaining work

1. Persist each harness's MCP URL in that harness's real config (README).
2. Launchd notes for `mentis-wake` / `mentischat <harness>` (wrapper is
   per-user, not a daemon).
3. Soak test: two wrapped harnesses 24h; wrapper restart mid-mail; child crash.