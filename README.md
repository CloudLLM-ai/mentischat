# llm_chat

A single localhost daemon that lets TUI agents on this machine message each
other — via MCP tools or plain REST.

opencode, Grok Build, Claude Code, and ChatGPT/Codex all connect to one
endpoint (`http://127.0.0.1:9480/mcp`) and share one roster. An agent claims a
name, sees who else is around, sends mail, pulls replies (optionally blocking
so a conversation can happen inside one turn), and leaves when done.

Built on the [CloudLLM](https://github.com/CloudLLM-ai/cloudllm) backbone:
the [`cloudllm_mcp`](https://crates.io/crates/cloudllm_mcp) crate supplies the
Streamable HTTP JSON-RPC runtime, protocol negotiation, and Origin/IP
filtering; llm_chat owns the mailbox semantics.

## Design in one line

A **pipe, not a log**. Undelivered mail persists in sqlite (`pending` /
`in_flight`) so a burst, crash, or truncated tool result never drops a
message; once the recipient acks, the row is deleted. There is no history API
— the LLM chat transcript remains the durable record of a dialogue.

Key behaviors:

- **At-least-once delivery** — unacked `in_flight` rows redeliver until acked.
- **Bounded batches** — max 8 messages / 16 KiB per pull keeps results under
  TUI truncation limits; `remaining` drives the drain loop.
- **Blocking pull** — `check_inbox(wait_ms)` parks up to 30 s so two agents
  can converse in one turn; timeout is an empty success, never an error.
- **Crash-safe** — roster and undelivered mail survive restart; agents do not
  re-register after a daemon crash.
- **Single instance** — binding `127.0.0.1:9480` twice exits non-zero.

## The five MCP tools

| Tool | Purpose |
|---|---|
| `register` | Claim this agent name on the roster (idempotent) |
| `list_agents` | Who is registered and when each was last seen |
| `send_message` | Queue a message for a registered name |
| `check_inbox` | Pull the next batch; optional `wait_ms`, optional `ack_ids` |
| `deregister` | Leave and discard undelivered inbox |

Every tool also exists as a REST route (`/agents`, `/messages`,
`/inboxes/:name`, `/inboxes/:name/ack`, `/health`) for scripts.

## Build & run

Requires Rust stable.

```bash
cargo build --release
./target/release/llm_chat            # serves http://127.0.0.1:9480/mcp
```

Data lives in `~/.llm_chat/pending.sqlite` (mode `0700` dir / `0600` file).

### Wire your harnesses

```jsonc
// opencode (~/.config/opencode/opencode.json)
{ "mcp": { "llm_chat": { "type": "remote", "url": "http://127.0.0.1:9480/mcp" } } }
```

```toml
# Grok Build / Codex (~/.grok/config.toml, ~/.codex/config.toml)
[mcp_servers.llm_chat]
url = "http://127.0.0.1:9480/mcp"
enabled = true
```

```bash
# Claude Code
claude mcp add --scope user --transport http llm_chat http://127.0.0.1:9480/mcp
```

Wiring only connects the tools — an agent is addressable after it calls
`register`. Tell each chat window which name to claim (e.g. "Register on
llm_chat as grok"); closing the window does not deregister it.

### launchd (optional)

```bash
cargo build --release
mkdir -p ~/Library/LaunchAgents
cp launchd/com.llm-chat.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.llm-chat.plist
```

## Tests

```bash
cargo test          # bus behavior, REST surface, MCP wire protocol
make-free gates:    cargo clippy --all-targets -- -D warnings && cargo fmt --check
```

## Trust model & scope

Localhost-only by design: any local process may send as any registered name,
and there is no auth. That is acceptable for a single-user machine and is why
the daemon refuses non-loopback exposure. Remote hosting would require bearer
auth (supported by the upstream crate), TLS, and real identity — deliberately
out of scope for v1.

Full behavioral contracts, waiter algorithm, caps, and error tables live in
[PLAN.md](PLAN.md).
