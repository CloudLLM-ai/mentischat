# Nudge v1 — In-daemon AppleScript doorbell (removed)

**Status:** Historical reference only. This transport was removed from the
binary; wake is now **`mentis-wake` only** (see [README.md](README.md)).

This document preserves the first shipped nudge design so operators and
future contributors understand what existed before Layer 3b.

---

## What it was

Layer 3 v1 woke idle Terminal-hosted TUIs by running **AppleScript inside the
mentischat process**. A tokio worker scanned the roster, found agents with
unacked mail and a bound Terminal window, and typed a single pointer string
into that window via System Events.

The mailbox stayed the only content path: the doorbell never injected message
bodies, never called `check_inbox`, and never acked mail.

---

## Pointer constant

One string, shared by all transports:

```
[mentischat] New message waiting -- call check_inbox.
```

Defined in Rust as `POINTER_TEXT` in `src/nudge.rs` (still present for
`mentis-wake`).

---

## Architecture (v1)

```
send_message → sqlite queue
            → if recipient idle (no parked check_inbox waiter)
            → notify in-process nudge worker
            → worker scans nudge_targets()
            → AppleScript inject into bound Terminal window
            → harness sees pointer as user input → new turn → check_inbox
```

The worker lived in `src/nudge.rs` (`nudge::run`), started from `src/main.rs`
on macOS only. It retried every 2 s, debounced 60 s per `(agent, message-id-set)`,
and skipped agents with parked `check_inbox` waiters.

---

## Window binding

Stored on the roster row as `window_title_match`, exposed as `nudgeWindow` on
register / `list_agents`.

| Event | Binding |
|---|---|
| `register` with `windowTitleMatch` | Must match exactly one Terminal window (title substring or `/dev/tty`) |
| `register` without match | Auto-detect: unique title containing agent name, else front Terminal window |
| `register` with `nudgeTransport: none` or `wrapper` | No window binding |
| `deregister` | Row deleted; binding gone |

Dedicated window per agent name — keystrokes could interleave with a human in
the same window.

---

## AppleScript inject (reference)

This was the implementation removed from `src/nudge.rs`:

```applescript
on run argv
  set titleMatch to item 1 of argv
  set pointerText to item 2 of argv
  tell application "System Events"
    if not (exists process "Terminal") then return 0
    set prevFront to name of first application process whose frontmost is true
  end tell
  tell application "Terminal"
    set n to 0
    set targetWindow to missing value
    repeat with w in windows
      if (name of w contains titleMatch) or (tty of w contains titleMatch) then
        set n to n + 1
        set targetWindow to w
      end if
    end repeat
    if n is not 1 then return n
    set index of targetWindow to 1
    activate
  end tell
  delay 0.3
  tell application "System Events"
    tell process "Terminal"
      set frontmost to true
      keystroke pointerText
      key code 36
    end tell
  end tell
  delay 0.2
  if prevFront is not "Terminal" then
    tell application "System Events" to set frontmost of application process prevFront to true
  end if
  return 1
end run
```

Invoked via `osascript -` with argv `[window_match, POINTER_TEXT]`.

---

## Why it was removed

macOS delivers System Events keystrokes only to the **frontmost** application.
Even with focus restore, the inject **briefly hijacked the user's window** —
unacceptable for day-to-day use with multiple agents and other apps in front.

`mentis-wake` replaces this by writing the pointer to a PTY the wrapper owns
(no GUI focus steal). The daemon now exposes only the read-only peek endpoint
(`GET /nudge/{agent}`) for wrappers to long-poll.

---

## v1 requirements (for archaeology)

- **Platform:** macOS + Terminal.app only (worker compiled out elsewhere).
- **Accessibility:** System Events grant for the process running mentischat
  (Terminal if `make start`, or the binary under launchd).
- **Transport column:** `nudge_transport = 'terminal'` selected AppleScript;
  `wrapper` / `none` skipped the worker.
- **No sidecar:** no `nudge.sh`, no `targets.json`, no HTTP poll from the worker.

---

## Migration from v1

| v1 habit | Current habit |
|---|---|
| Bare `opencode` in Terminal | `mentis-wake --agent NAME -- opencode` |
| Default `register` (auto window bind) | `register` with `nudgeTransport: none` |
| AppleScript doorbell | `mentis-wake` + `GET /nudge/{agent}` peek |
| Accessibility grant for mentischat | Not needed for wrapped sessions |

---

## Related docs

- Current wake design: [PLAN.md](PLAN.md) — Layer 3 (nudge)
- Operator guide: [README.md](README.md) — "Waking idle sessions (nudge)"
