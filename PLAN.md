# MentisChat — local agent-to-agent messaging

A single localhost daemon that lets TUI agents (opencode, Grok Build, Claude Code, ChatGPT desktop / Codex CLI) message each other via MCP tools and plain REST.

This is a **communication layer**, not a conversation log. The durable record of a dialogue is still each LLM’s own chat transcript. The daemon **does** persist *undelivered* mail (pending + in-flight) so a burst, a crash, or a truncated tool result does not drop messages. Once the recipient **acks**, the row is deleted. There is no history API.

Display name is **MentisChat**. Identifier is `mentischat` everywhere: repo folder, crate/binary name, MCP server id, launchd label.

**Status:** plan revised for the CloudLLM backbone; ready to implement. Contracts below are the source of truth; do not invent extra tools, a history API, or auth.

## Backbone: CloudLLM

This daemon is built **on [CloudLLM](https://github.com/CloudLLM-ai/cloudllm)** — specifically its published crate [`cloudllm_mcp`](https://crates.io/crates/cloudllm_mcp) (lib target name `mcp`), which supplies everything MCP-wire-shaped so we only own mailbox logic:

| We get from cloudllm_mcp | We build ourselves |
|---|---|
| Streamable HTTP JSON-RPC endpoint (`streamable_http_router`) | Mailbox bus: roster, pending/in-flight, ack, waiters |
| Protocol negotiation (`initialize`, version allow-list) | sqlite persistence (`rusqlite`) |
| Tool registry + schema generation (`UnifiedMcpServer`, `ToolProtocol`, `ToolMetadata`) | Five tools as one `ToolProtocol` impl |
| Origin validation (localhost prefixes), IP filtering | REST surface on the same port |
| Stateless semantics, always-JSON responses, GET/DELETE → 405 | launchd packaging, graceful shutdown |

Relationship to CloudLLM is **downstream dependency**, same pattern as MentisDB depends on `mcp`. Own repo, no fork, no vendoring. Pin exact; upgrade deliberately.

## Goal

One MCP server, shared by every harness on this machine, so an agent can:

1. claim a name (`register`)
2. see who else is on the roster (`list_agents`)
3. send to a registered name (`send_message`)
4. pull (and optionally wait for) mail (`check_inbox`)
5. leave (`deregister`)

“Universal” means **one daemon + one tool surface**, not one mega-tool. Any MCP client that speaks Streamable HTTP can use it. Scripts use REST.

“Registered and connected” in v1:

| Word | What the daemon actually tracks |
|---|---|
| Registered | name is on the roster (sqlite). Survives daemon restart. |
| Connected | **not tracked.** Closing a TUI does not notify the daemon. `lastSeen` is a hint (updated on `register` and `check_inbox`), not a live socket. Mail to a registered-but-idle name stays on disk until ack, cap, or `deregister`. |

## Architecture

```
opencode ──────┐
Grok Build ────┼──► http://127.0.0.1:9480/mcp ◄── one daemon (in-memory pipe)
Claude Code ───┤
ChatGPT/Codex ─┘         + any future agent or script (REST)
```

- **Stack:** Rust stable (edition 2021), tokio runtime.
- **MCP runtime:** `cloudllm_mcp = "=0.3.6"` with feature `"server"`.
- **HTTP:** one axum **0.8.x** listener on `127.0.0.1:9480` serving BOTH `/mcp` (cloudllm_mcp router) and REST (our router) merged into one `Router`. Axum must stay 0.8.x — `Router` types must match cloudllm_mcp’s for `.merge()`.
- **Transport:** Streamable HTTP at `POST /mcp` only. Plain JSON responses (cloudllm_mcp hardwires `application/json`; SSE never used). Confirmed stateless upstream: no `Mcp-Session-Id` issued, required, or validated; notifications answered `202 Accepted`.
- **Bind:** loopback only, never `0.0.0.0`. Startup binds and **exits non-zero on `EADDRINUSE`** — that is the single-instance lock.
- **Data dir:** `~/.cloudllm/mentischat/` (mode `0700`). File `pending.sqlite` (mode `0600`) holds roster + unacked messages only. Tests inject a temp dir via `Bus::new(data_dir)`; they must never write `~/.cloudllm/mentischat/`.
- **Restart:** roster and unacked messages reload from sqlite. Waiters (HTTP) do not. Agents do **not** re-register after a daemon crash. `deregister` is still how a name leaves.

### Why HTTP, not stdio

stdio would spawn **one process per harness**. They would not share a roster or the sqlite writer. HTTP exists so every client talks to the **same** daemon.

### Why compose routers ourselves — do NOT use MCPServerBuilder

Verified against source: `MCPServerBuilder` + its default adapter serve legacy routes only (`POST /tools/list`, `POST /tools/execute`) — **not** the JSON-RPC endpoint — and their listener cannot host extra routes. Compose directly instead:

```rust
let mcp_router = mcp::streamable_http_router(
    &http_cfg,
    &StreamableHttpConfig::new("mentischat", "1.0.0").with_endpoint_path("/mcp"),
    Arc::new(unified_server),
);
let app = mcp_router.merge(rest_router(bus.clone()));
let listener = tokio::net::TcpListener::bind("127.0.0.1:9480").await?; // EADDRINUSE -> exit(1)
axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await?;
```

`.into_make_service_with_connect_info::<SocketAddr>()` is **required**: cloudllm_mcp handlers extract `ConnectInfo` for IP filtering.

## File layout

```
mentischat/
  Cargo.toml
  Cargo.lock            # committed; pins cloudllm_mcp =0.3.6 et al.
  .gitignore            # /target
  PLAN.md
  src/
    main.rs       bind, fail on EADDRINUSE, SIGINT/SIGTERM graceful close
    store.rs      rusqlite open/migrate; roster + messages rows
    bus.rs        mutex, waiters, caps, ack/batch over the store
    names.rs      agent-name validator (handler-side, not JSON Schema pattern)
    errors.rs     typed errors -> HTTP / MCP mappings
    tools.rs      MailboxProtocol: ToolProtocol impl dispatching the five tools
    rest.rs       axum Router: REST + /health
  tests/
    bus.rs        in-process bus behavior (tempdir)
    http.rs       REST via tower oneshot on the merged Router (no real port)
    mcp.rs        initialize + tools/list + tools/call through the router
  scripts/
    run.sh        launchd/dev wrapper: exec target/release/mentischat
  launchd/
    com.mentischat.plist
```

`src/main.rs` is the only process entry. `rest::router(Arc<Bus>)` returns the REST `Router`; tests drive the merged app through `tower::ServiceExt::oneshot` — never bind 9480, never write `~/.cloudllm/mentischat/`.

## Crate manifest

Crate name `mentischat`. Commit `Cargo.lock`; versions here are intended majors (pin exacts there):

```toml
[dependencies]
cloudllm_mcp  = { version = "=0.3.6", features = ["server"] }
axum          = "0.8"
tokio         = { version = "1", features = ["full"] }
rusqlite      = { version = "0.32", features = ["bundled"] }
serde         = { version = "1", features = ["derive"] }
serde_json    = "1"
uuid          = { version = "1", features = ["v4"] }
tracing       = "0.1"
tracing-subscriber = "0.3"

[dev-dependencies]
tempfile        = "3"
tower           = { version = "0.5", features = ["util"] }
http-body-util  = "0.1"
```

- `rusqlite` bundled compiles its own SQLite; no system dependency.
- If crates.io lags master (see Risks), fall back to a git dep pinned to a commit:
  `cloudllm_mcp = { git = "https://github.com/CloudLLM-ai/cloudllm", rev = "<sha>", features = ["server"] }`

Build/test/run:

```
cargo test
cargo build --release        # before launchd install
./target/release/mentischat
```

## Durable pending store

Still a pipe: **acked mail is gone.**

| State | Where | Meaning |
|---|---|---|
| `pending` | sqlite | queued, not yet handed to a `check_inbox` |
| `in_flight` | sqlite | handed to a `check_inbox`, not yet acked |
| (deleted) | — | recipient acked; not recoverable |

Waiters stay RAM-only (you cannot persist an HTTP request).

**Why not RAM + cap 32:** a slow recipient (long tool call, TUI truncated at ~20 KB) makes `send_message` fail or, worse, drain-on-read forgets a batch the model never saw. Large traffic needs (1) room on disk, (2) small delivery batches, (3) ack before delete.

**Engine:** `rusqlite` (bundled SQLite). One `Connection` owned by the bus, guarded by the same mutex as all state — calls are microsecond-scale synchronous ops, fine at localhost scale; no second sqlite library. `journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout=5000`.

**Path:** `{data_dir}/pending.sqlite`. Production `data_dir` = `$HOME/.cloudllm/mentischat`, created mode `0700`; file mode `0600`.

**Schema** (`from` is reserved in SQL, so columns are `sender` / `recipient`; JSON still uses `from` / `to`):

```sql
CREATE TABLE roster (
  name TEXT PRIMARY KEY,
  last_seen TEXT NOT NULL
);

CREATE TABLE messages (
  id TEXT PRIMARY KEY,
  sender TEXT NOT NULL,
  recipient TEXT NOT NULL,
  body TEXT NOT NULL,
  created_at TEXT NOT NULL,
  seq INTEGER NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending', 'in_flight'))
);

CREATE INDEX idx_inbox ON messages (recipient, state, seq);
```

`seq` is a process-wide incrementing integer (`SELECT MAX(seq)+1` inside the mutex) so FIFO is total order per recipient.

One `Bus` wraps the store. Guard every roster / message / waiter operation behind a single `tokio::sync::Mutex` in `bus.rs`. Sqlite writes happen **inside** that mutex so waiters and rows cannot tear.

Caps (fail fast). Named constants in `bus.rs`:

```
MAX_BODY_BYTES = 64 * 1024
MAX_PENDING_PER_AGENT = 1024          // pending + in_flight for that recipient
MAX_TOTAL_MESSAGES = 8192             // all rows in messages
MAX_BATCH = 8                         // max messages returned by one check_inbox
MAX_RETURN_BYTES = 16 * 1024          // pack whole messages only; see batching
MAX_WAIT_MS = 30_000
MAX_WAITERS_PER_AGENT = 8
```

- body size = `body.len()` on the UTF-8 `String` (Rust byte length ≡ Buffer.byteLength)
- whitespace-only body fails (`invalid_body`). Store the original string when it passes; do not trim stored mail
- `send_message` fails `queue_full` if that recipient already has 1024 rows, or `storage_full` if the table has 8192 rows or sqlite throws a disk error
- at most 8 parked `check_inbox` waiters per name (`too_many_waiters`)
- `/health.pending` is `SELECT COUNT(*) FROM messages` (both states)

Message shape (JSON):

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "from": "opencode",
  "to": "grok",
  "body": "...",
  "createdAt": "2026-08-21T12:00:00.000Z"
}
```

- `id`: `uuid::Uuid::new_v4()`
- `createdAt`: UTC RFC 3339 from `SystemTime` (format inline or via a tiny helper; do not pull a date crate into the bus API)
- No history API. Acked ids are deleted. Unknown ack ids are ignored (idempotent).

### Batch + ack (in-transit)

Grok (and others) truncate large MCP tool results (~20 KB default). Drain-on-read + a 1024-deep queue would drop everything the model never parsed.

Delivery rules:

1. **Ack-then-pull in one call.** `check_inbox` takes optional `ack_ids: string[]`. Rows must be `in_flight` **and** `recipient = agent` to delete. Unmatched ids ignored.
2. **Redeliver unacked.** If any `in_flight` rows remain for that agent, return **exactly those** (FIFO by `seq`). Do not promote more `pending` until the in-flight set is empty. At-least-once: a TUI that dropped the tool result gets the same batch again.
3. **Promote a bounded batch.** If no `in_flight` remains, take the oldest `pending` rows for that agent, up to `MAX_BATCH`, without exceeding `MAX_RETURN_BYTES` of bodies (sum of byte lengths). Always include at least the first message even if larger than `MAX_RETURN_BYTES` (must make progress). Set those rows to `in_flight` and return them.
4. **`remaining`** counts `pending` rows still waiting after this call. The agent calls `check_inbox` again (acking these ids) until `remaining` is 0.
5. **Park only when both states are empty** for that agent.

Success JSON for `check_inbox`:

```json
{ "agent": "grok", "messages": [], "remaining": 0 }
```

REST ack (same store operation as `ack_ids`):

`POST /inboxes/:agent/ack` `{ "ids": ["..."] }` → `{ "agent", "acked": number }`

MCP does **not** add a sixth tool. Models ack via `ack_ids` on the next `check_inbox`.

Not MentisDB. MentisDB is durable memory; this sqlite file is a mailbox. Do not dual-write.

Agent names: `^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$` (1–63 chars). Validate in `names.rs` **in the handler**. Do **not** put this regex (or any `pattern`) on schemas generated by `ToolMetadata`/`ToolParameter` — local GGUF/OpenCode grammars copy JSON Schema patterns into GBNF and die on `\d`.

Self-send (`from == to`) is allowed if registered. Unknown JSON fields on REST/MCP inputs are **ignored** (serde default). Missing required fields fail as `invalid_name` / `invalid_body`.

## Waiters (required algorithm)

`check_inbox` is the only pull. `wait_ms` makes it a blocking pull — how two agents converse in one turn. The race to get wrong: observe empty queue → drop mutex → send lands → park and sleep till timeout with mail already sitting there.

Mailbox rules:

1. **Immediate delivery wins.** If `in_flight` or `pending` non-empty at start of `check_inbox` (after applying `ack_ids`), return a batch. Never park, even when `wait_ms > 0`.
2. **One consumer per wake.** Waiters are FIFO per name. A send wakes **the oldest waiter only**; that waiter runs batch+ack rules. Later waiters stay parked. Never hand two waiters the same row.
3. **Park under the mutex.** Register the waiter before releasing the lock, else a send sneaks between “inbox empty” and “waiting”.
4. **Timeout is empty success**, not an error: `{ messages: [], remaining: 0 }`. Mail arriving after timeout stays `pending`.
5. **`wait_ms == 0` / absent never parks.**
6. **Two windows sharing a name.** Oldest waiter gets next batch. Unacked `in_flight` belongs to the name, not the window: either window redelivers until ack. Document the shared-queue footgun; do not split queues.
7. **Deregister** rejects every waiter for that name (`unregistered`) after deleting the name’s inbox rows (`DELETE FROM messages WHERE recipient = agent`). Rows they *sent* to others remain.
8. **Shutdown** (SIGINT/SIGTERM): reject waiters `shutting_down` (HTTP 503), flush/close sqlite. Committed rows stay on disk.
9. **Waiter cap:** 9th park on a name fails `too_many_waiters` without parking.

Implementation: waiters are one-shot channels (`tokio::sync::oneshot::Sender<()>`) in a per-name `VecDeque`. Send resolves the front sender; timeout races `tokio::time::timeout(wait_ms)` against the receiver; first signal wins, the other is a dropped no-op.

### `check_inbox(agent, wait_ms, ack_ids)`

Parse `wait_ms` and `ack_ids` first (see Errors). Then:

```
lock
if agent not registered:
  unlock; fail unregistered
DELETE in_flight rows whose id ∈ ack_ids AND recipient = agent
lastSeen = now
if in_flight for agent non-empty:
  msgs = those rows FIFO; remaining = COUNT pending for agent
  unlock; return { messages: msgs, remaining }
if pending for agent non-empty:
  msgs = promote bounded batch pending -> in_flight
  remaining = COUNT pending for agent
  unlock; return { messages: msgs, remaining }
if wait_ms == 0:
  unlock; return { messages: [], remaining: 0 }
if waiters.len >= MAX_WAITERS_PER_AGENT:
  unlock; fail too_many_waiters
(tx, rx) = oneshot::channel(); push tx to this name's FIFO
unlock

select: rx => () , sleep(wait_ms) => ()

lock
remove waiter if still present
if agent not registered: unlock; fail unregistered
lastSeen = now
// same delivery rules as above (in_flight first, else promote, else empty)
unlock
return { messages, remaining }
```

After wake or timeout always re-check registration, then deliver.

### `send_message(from, to, body)`

Validate `from` **then** `to` (if both bad, error `agent` is `from`). Then:

```
lock
if from not registered: unlock; fail unregistered (agent=from)
if   to not registered: unlock; fail unregistered (agent=to)
nTo  = COUNT messages WHERE recipient = to
nAll = COUNT messages
if nTo >= MAX_PENDING_PER_AGENT: unlock; fail queue_full
if nAll >= MAX_TOTAL_MESSAGES:   unlock; fail storage_full
INSERT pending (next seq)
if waiters(to) non-empty: waiters(to).pop_front().resolve()
unlock
return { id, from, to, createdAt }
```

### `deregister(agent)`

```
lock
if not registered: unlock; fail unregistered
n = COUNT messages WHERE recipient = agent
DELETE those rows
reject each waiter (unregistered); clear waiter list
DELETE roster row
unlock
return { agent, discarded: n }
```

## Errors

Throw typed errors from `Bus`. Map once in REST (`rest.rs`) and once in MCP (`tools.rs`). Do not leak backtraces to clients.

| code | HTTP | when |
|---|---|---|
| `invalid_json` | 400 | REST body is not JSON |
| `invalid_name` | 400 | name fails the regex |
| `invalid_body` | 400 | missing / non-string / whitespace-only / > 64 KiB |
| `invalid_wait` | 400 | `wait_ms` present but not an integer in `0..30000` |
| `unregistered` | 404 | `from`, `to`, or `agent` not on roster |
| `queue_full` | 409 | recipient already has 1024 unacked rows |
| `storage_full` | 507 | 8192 global rows, or sqlite disk error on insert |
| `too_many_waiters` | 409 | 8 waiters already parked on that name |
| `not_found` | 404 | REST path that is not a known route |
| `shutting_down` | 503 | waiter aborted because the process is closing |

Body (all errors):

```json
{ "error": "unregistered", "agent": "claude", "known": ["grok", "opencode"] }
```

`known` present on `unregistered` only (current roster names, sorted). Others omit it. `queue_full` includes `"to"` and `"pending"` (that recipient’s row count). `storage_full` includes `"pending"` (global count). `too_many_waiters` includes `"agent"` and `"waiting": 8`. `ack_ids` must be an array of strings if present, else `invalid_body`.

`wait_ms` parsing (JSON number or REST query string):

- absent / empty string → `0`
- integer `0..30000` (number or digit string) → that value
- anything else (float, negative, NaN-ish, bool, array) → `invalid_wait`

REST success is always **200** + JSON (including POST/DELETE routes). No 201/204.

## MCP

### Server info

Provided by cloudllm_mcp: `StreamableHttpConfig::new("mentischat", "1.0.0")` sets the server name/version returned by `initialize`. Protocol negotiation is upstream-handled: supported versions are `["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"]`; unknown `MCP-Protocol-Version` headers are rejected 400. Caveat (verified in source): `initialize` replies with the server’s configured version regardless of the client’s request — live-test each harness early (step 7 wires clients before sign-off for exactly this reason).

Server `instructions`: verify whether `StreamableHttpConfig` exposes an instructions field during scaffolding. If not, fold the ritual below into every tool description so no behavior depends on it.

Instructions ritual (verbatim intent, same for every harness):

```
mentischat is a local mailbox between TUI agents on this machine.
You are not on the roster until you call register with the exact name the user gave you. Do not invent a name.
Before send_message, call list_agents. Only send to names that appear there. from must be your registered name.
After send_message, call check_inbox with wait_ms 15000 so a reply can arrive in this turn.
When check_inbox returns messages, quote the bodies in your reply. Pass those ids as ack_ids on the next check_inbox. If remaining > 0, check_inbox again until remaining is 0.
Call deregister only when the user says they are done.
```

### Transport & security (`main.rs`, upstream router)

What we get free from cloudllm_mcp (verified in its source):

- **Stateless:** each POST handled independently; no session ids; notifications → `202 Accepted`.
- **JSON-only responses** — matches our “no long-lived GET SSE” rule. GET `/mcp` → **405** (no broadcaster attached); DELETE `/mcp` → **405**.
- **Origin validation:** only `http(s)://127.0.0.1`, `localhost`, `[::1]` prefixes allowed; missing Origin allowed (curl/CLIs). Non-local Origins → 403.
- **IP filtering:** handlers extract `ConnectInfo<SocketAddr>`; keep an explicit localhost allow-list config so even a mis-bind cannot be reached off-box.

We add:

- Bind `127.0.0.1` only + exit non-zero on bind failure (single-instance lock).
- Request body limit ~1 MiB on `/mcp` and REST JSON routes (`axum::extract::DefaultBodyLimit`) — message caps stay enforced in the bus regardless.
- Optional hardening (not v1-blocking): Host-header allow-list middleware. Upstream has Origin checks but no Host validation; loopback binding + Origin rules cover the realistic DNS-rebinding path.

Log via `tracing` to stdout: method, path, MCP tool name, agent names, message id, body byte length. Never log body contents.

Known cosmetic quirk: upstream registers a catch-all route answering non-MCP paths with 405 rather than 404; static REST routes merged after should take precedence under matchit 0.8 — verified by test (see Risks if it bites).

### Tools

Five MCP tools exposed through **one** `ToolProtocol` implementation (`MailboxProtocol` in `tools.rs`) registered once on the `UnifiedMcpServer`. Its `execute(tool_name, params)` dispatches to the bus; `list_tools()` returns the five `ToolMetadata`. Schemas come from `ToolParameter` (string / integer / array-of-string) — plain types only, no patterns.

Do not add regex constraints to schemas. Validate handler-side and return the typed error.

| Tool | Input | Success JSON (stringified into content[0].text) |
|---|---|---|
| `register` | `{ agent }` | `{ "agent", "created": boolean, "lastSeen" }` |
| `deregister` | `{ agent }` | `{ "agent", "discarded": number }` |
| `send_message` | `{ from, to, body }` | `{ "id", "from", "to", "createdAt" }` |
| `check_inbox` | `{ agent, wait_ms?, ack_ids? }` | `{ "agent", "messages": Message[], "remaining": number }` |
| `list_agents` | `{}` | `{ "agents": [{ "name", "lastSeen" }] }` |

`created: true` only when the name was new. Re-register idempotent: refreshes `lastSeen`, keeps pending queue. `discarded` = queued messages dropped with the name. `check_inbox` follows Waiters + Batch+ack; updates `lastSeen` whenever accepted (including empty/timeout); `lastSeen` is ISO 8601 UTC like `createdAt`. `list_agents` sorted by name ascending; empty roster → `{ "agents": [] }`.

Tool descriptions (product copy):

- `register` — Claim this agent name on the local mentischat roster. Idempotent. Pass the exact name the user told you to use.
- `deregister` — Leave the roster and discard this name’s undelivered messages. Only call when the user says they are done.
- `send_message` — Queue a message for a registered agent. Fails if `from` or `to` is not registered. After sending, call check_inbox with wait_ms so a reply can arrive.
- `check_inbox` — Return the next batch of messages for this registered name (at-least-once). Optional wait_ms (0–30000). Optional ack_ids deletes previously returned in-flight messages. Quote bodies, then ack those ids on the next call. If remaining > 0, call again.
- `list_agents` — Names currently on the roster and when each was last seen (register or check_inbox).

No `history` tool. Caller identity = explicit `from`/`agent` args. No auth: localhost-only; any local process may send as any registered name.

MCP error mapping (verified upstream semantics):

- Domain failures (`unregistered`, `invalid_name`, `queue_full`, timeout-empty is NOT an error…) → return `Ok(ToolResult::failure(json_error_string))` → client sees `isError: true` content block containing our error JSON object. Never map domain errors to Rust `Err`.
- Reserve `Err(ToolError::…)` for malformed requests (bad parameter shape) → upstream turns these into JSON-RPC `-32602` / HTTP 400.

## REST

Same behaviors as the MCP tools (REST is not a subset), implemented in `rest.rs` on the shared bus. `Content-Type: application/json` on success and error. `GET /inboxes/:agent` mutates state → send `Cache-Control: no-store`.

| Method | Path | Body / query | Same as |
|---|---|---|---|
| `POST /agents` | `{ "agent" }` | `register` |
| `DELETE /agents/:agent` | — | `deregister` |
| `POST /messages` | `{ "from", "to", "body" }` | `send_message` |
| `GET /inboxes/:agent` | `?wait_ms=` optional | `check_inbox` without ack |
| `POST /inboxes/:agent/ack` | `{ "ids": string[] }` | ack only |
| `GET /agents` | — | `list_agents` |
| `GET /health` | — | `{ "ok": true, "agents": number, "pending": number }` |

`:agent` percent-decoded then validated. `/health`: `agents` = roster size; `pending` = `COUNT(*)` on `messages`; does not promote, ack, or touch `lastSeen`.

No history route.

## Connecting

MCP config only **wires** the tools; it does not put an agent on the roster. A chat window is unaddressable until told to register.

| Layer | When | Meaning |
|---|---|---|
| Wired | once per harness | client can call mentischat tools |
| Registered | until `deregister` | name is on the roster; others may send to it |

All four clients point at `http://127.0.0.1:9480/mcp`. No tunnels. Config snippets unchanged from prior revision:

- **opencode** (`~/.config/opencode/opencode.json`): `"mcp": { "mentischat": { "type": "remote", "url": "http://127.0.0.1:9480/mcp" } }`
- **Grok Build** (`~/.grok/config.toml`): `[mcp_servers.mentischat]` `url = "http://127.0.0.1:9480/mcp"`, `enabled = true`
- **Claude Code**: `claude mcp add --scope user --transport http mentischat http://127.0.0.1:9480/mcp`
- **ChatGPT desktop / Codex CLI** (`~/.codex/config.toml`): same TOML shape as Grok

ChatGPT web stays out of scope (public HTTPS requirement would break localhost trust model).

Recommended stable names: grok / opencode / claude / codex. Two windows sharing one name share one queue and the same `in_flight` batch — use distinct names (`grok-review`) when isolation matters.

### User ritual (new window)

```
Register on mentischat as <name>. Then list who's registered.
Only send_message to names that list_agents returns.
After you send, check_inbox with wait_ms 15000.
Quote pulled bodies in this chat, then pass their ids as ack_ids on the next check_inbox. If remaining > 0, keep pulling.
When I say I'm done, deregister <name>.
```

Agent steps: register → list_agents → send_message (both endpoints registered) → check_inbox(wait_ms=15000) after sending, quote bodies, ack ids, drain while remaining > 0 → deregister when told. Do not auto-register on connect; do not auto-deregister on disconnect.

## launchd

- Label `com.mentischat`; `RunAtLoad` true, `KeepAlive` true
- `ProgramArguments`: `/bin/bash`, `-lc`, absolute path to `scripts/run.sh`; script does `exec "$HOME/workspace/mentischat/target/release/mentischat"` after `cd` to repo root
- Logs: `~/Library/Logs/mentischat.stdout.log` / `.stderr.log`
- Build release before install. Bind failure → process exits → launchd restarts; if 9480 is held by a healthy daemon, the newcomer must keep dying (never steal the port).

Install optional at PR time; ship the plist either way.

## Tests (required before calling it done)

`tests/bus.rs` (in-process, tempdir, no HTTP): register/idempotent-re-register keeps rows; two-name send; unacked pull redelivers same ids; ack deletes + next batch promotes; `remaining` math at MAX_BATCH boundary (send 10, pull 8, remaining 2); unregistered send/check/deregister; invalid name; empty/oversize body (byte length); per-agent 1024 cap → queue_full; global 8192 → storage_full; deregister drops recipient rows but keeps sent rows; immediate delivery beats wait_ms; parked waiter woken by send; post-timeout mail stays pending; two waiters — oldest gets batch, second never duplicates ids; 9th waiter rejected; deregister rejects waiters unregistered; **crash test:** drop Bus, reopen on same dir, roster + row survive; mutex stress: concurrent send/check loop ×50, nothing duplicated or dropped.

`tests/http.rs` (tower oneshot against merged app, ephemeral nothing): every REST happy path incl. ack route; GET redelivers same ids until ack; wait_ms=2000 with delayed POST; two overlapping waits, one POST → exactly one GET receives; error codes 400/404/409/503/507; `/health.pending` equals row count; unknown REST path → 404 `not_found` (documents precedence over upstream catch-all).

`tests/mcp.rs` (oneshot through the composed router): POST initialize returns protocolVersion + server name `mentischat`; `notifications/initialized` → 202; tools/list shows exactly five tools with pattern-free schemas; tools/call `list_agents` after REST register proves shared bus; tools/call `register` invalid name → `isError:true` content with error JSON; GET /mcp → 405; Origin `https://evil.example` → 403; missing Origin allowed.

Full TUI live check remains manual.

## Implementation steps (order)

1. Scaffold Cargo crate + module skeletons; confirm `cloudllm_mcp =0.3.6` resolves on crates.io (else git-pin fallback)
2. `names.rs` + `errors.rs` + `store.rs` + `bus.rs` + `tests/bus.rs` green — including crash/reopen — before any HTTP
3. `rest.rs` + `tests/http.rs`
4. `tools.rs` (`MailboxProtocol`) + compose routers + `tests/mcp.rs`
5. `main.rs`: bind loopback, EADDRINUSE exit, SIGINT/SIGTERM graceful close
6. `cargo build --release`, `scripts/run.sh`, launchd plist
7. Wire whichever harnesses exist; run one live initialize handshake early (protocol-version caveat)
8. Verify:
   - `cargo test`
   - curl every REST route (GET redelivers until POST ack)
   - `curl -D- -X GET http://127.0.0.1:9480/mcp` → 405
   - `~/.cloudllm/mentischat/pending.sqlite` exists after a live send; repo tree has no message state
   - kill -9 daemon, restart: roster + unacked row still visible via `GET /agents` / inbox
   - `grok mcp doctor mentischat`; `opencode mcp list`; `claude mcp list` / `codex mcp list` where installed
   - live send/wait/leave trio between two registered windows

## Risks / open items (CloudLLM-specific)

- **crates.io vs master:** stateless DELETE-405 + protocol-version fixes were read on master; published 0.3.6 must match. Verify at scaffold time; fall back to git rev pin.
- **Route merge precedence:** static REST routes vs upstream `/{*rest}` catch-all relies on matchit priority — covered by `tests/http.rs` unknown-path case; fallback is nesting their router under `/mcp` with `endpoint_path: "/"`.
- **initialize version echo:** server advertises configured version regardless of client request — acceptable today; re-run live harness checks on any cloudllm_mcp upgrade.
- **No Host-header validation upstream:** accepted risk while bound to loopback; revisit if exposure ever widens.

## Design decisions & known limitations

- **Pipe, not a log:** the LLM chat is the conversation store; sqlite holds unacked mail only; ack deletes; no history tool.
- **Pending durable, waiters RAM:** roster + pending + in_flight survive crash/restart; parked HTTP pulls do not.
- **At-least-once in-flight:** delete only on ack; truncated tool results redeliver; duplicate quotes possible, lost mail not.
- **Bounded batches:** ≤8 messages / ≤16 KiB bodies per pull; `remaining` drives the drain loop.
- **Explicit register/deregister:** closing a window changes nothing; leave is deliberate.
- **Pull + optional wait:** TUIs are not push targets; 30 s park rides inside one tool call (upstream imposes no server timeout; keep under client tool timeouts).
- **One ToolProtocol, five tools:** single registration point, dispatch by tool name; schemas stay primitive (no patterns) for GBNF-friendly grammars.
- **Router composition over MCPServerBuilder:** builder serves legacy routes only; direct `streamable_http_router` gives the real JSON-RPC endpoint plus room for REST on one port.
- **Frozen REST defaults:** success 200; validate `from` before `to`; alphabetical roster; `/health.pending` = raw COUNT(*).
- **Not MentisDB:** do not dual-write thoughts for each message.
- **No threads/broadcast/history/auth/OAuth:** YAGNI; localhost trust model (any process may impersonate any name).
- **Last-seen:** updated on `register` and `check_inbox` only; persisted on the roster row.
- **Stateless MCP, stateful bus:** harness sessions must not fragment the roster.
- **Pin cloudllm_mcp exactly; upgrade deliberately:** wire compat is contractual, treat minor bumps as risky.

## Out of scope (v1)

stdio transport; auth/tokens/per-harness identity; history/search/replay; MentisDB dual-write; per-window inboxes; broadcast/rooms; ChatGPT web/public URL; auto-register; live connection tracking; MCP resources/prompts; CORS; TLS.
