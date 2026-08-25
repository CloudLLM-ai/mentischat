# MentisChat Audit

**Date:** 2026-08-25  
**Scope:** Current working tree, including the daemon, REST and MCP surfaces,
SQLite persistence, PTY wake wrapper, launchd configuration, documentation,
and tests.

## Executive Summary

The mailbox implementation builds and its existing tests pass, but the wake
transport has lifecycle and cancellation defects that can prevent idle agents
from being woken. The project also contains stale design documentation and a
few release-readiness problems.

Recommended priority:

1. Fix wrapper ownership and cleanup races.
2. Make canceled waiters transparent to the wake queue.
3. Bound unregistered wake-session resources.
4. Make shutdown deletion atomic and observable.
5. Remove or rewrite obsolete wake documentation, then add regression tests.

## Findings and Recommended Fixes

### 1. Wrapper cleanup is not ownership-safe

**Severity:** High  
**Where:** `src/wake.rs:137-161`, `src/wake.rs:214-225`,
`src/bus.rs:664-695`

The legacy agent wrapper binds and unbinds by roster name. An older wrapper
that exits after a replacement can clear the newer wrapper's binding. The
`--no-register` path also skips cleanup, leaving `nudgeTransport: wrapper`
behind after the process exits.

**Fix:** Give every wrapper binding an ownership token and make DELETE
conditional on that token. The existing `wake_id` provides this model for the
default wake-session path. Either extend it to the legacy path or remove the
name-only binding path. Await or cancel the registration task before cleanup.
Add tests for replacement, early exit, and `--no-register` cleanup.

### 2. Canceled waiters consume the only wake signal

**Severity:** High  
**Where:** `src/bus.rs:462-470`, `src/bus.rs:481-485`

`send()` pops one waiter and ignores whether its receiver has already been
dropped. A client disconnecting while parked can therefore consume the wake,
leaving the next valid waiter asleep until its timeout, potentially 30 seconds.
The same issue affects inbox, nudge, and wake-session waiters.

**Fix:** Centralize wake delivery in a helper that keeps removing canceled
senders until one `send(())` succeeds or the queue is empty. Add tests that
abort the oldest parked request before sending mail.

### 3. Unbound wake sessions have no global resource limit

**Severity:** Medium  
**Where:** `src/bus.rs:612-625`

The `/wake/{wake_id}` endpoint accepts arbitrary valid UUIDs before they are
bound. The cap is only per UUID, so a local process can create unlimited map
entries in `wake_waiters` and hold them for up to 30 seconds.

**Fix:** Add a global cap for parked wake-session waiters, or reject unbound
long-polls after a bounded number of sessions. Decrement the count on timeout,
cancellation, deregistration, and shutdown. Test many distinct unbound UUIDs.

### 4. Wake IDs remain stale after explicit transport changes

**Severity:** Medium  
**Where:** `src/bus.rs:270-280`, `src/bus.rs:674-695`

Changing a registered agent to `nudgeTransport: none`, binding a legacy
wrapper, or calling `unbind_wrapper()` does not clear `wake_id`. An old
`/wake/{id}` poller can therefore remain associated with an agent after wake
has been disabled or changed.

**Fix:** Treat transport and session ownership as one state transition. Clear
`wake_id` whenever a binding is explicitly replaced or disabled, unless the
same ownership token is being refreshed. Add tests for transport changes while
a wake session is active.

### 5. Shutdown silently ignores database failures

**Severity:** Medium  
**Where:** `src/bus.rs:809-821`

`shutdown()` ignores transaction creation, DELETE, and COMMIT errors. A
failure can leave the roster or messages on disk, and ignoring individual
statement errors can undermine the promised all-or-nothing cleanup.

**Fix:** Use `?`-style error propagation inside the transaction, roll back on
any failure, and log a high-severity error before process exit. If the public
method must remain infallible, return a result from an internal cleanup method
and make the failure visible to the daemon. Add failure-path coverage where
practical.

### 6. Optional wrapper request bodies are rejected

**Severity:** Medium  
**Where:** `src/rest.rs:113-126`

The endpoint documents `{ "pid": 12345 }` as optional, but an empty POST body
is rejected by the JSON extractor with `invalid_body`.

**Fix:** Accept an empty body as equivalent to `{}` while continuing to reject
malformed JSON. Keep the PID non-authoritative and do not use it as the
ownership token.

### 7. Wake implementation is duplicated in a monolithic bus module

**Severity:** Low, maintainability  
**Where:** `src/bus.rs:505-646`

`nudge_peek()` and `nudge_peek_by_wake()` duplicate waiter registration,
timeout, cleanup, and pending-state logic. `src/bus.rs` is now 1,105 lines,
which makes the lifecycle bugs harder to see and maintain.

**Fix:** Extract shared pending-result and waiter-park helpers, then separate
wake-session bookkeeping from mailbox delivery if the module remains over 1k
lines. Preserve one canonical wake mechanism rather than adding more endpoint
special cases.

### 8. Migration does not always recreate the wake-session index

**Severity:** Low  
**Where:** `src/store.rs:80-86`

`idx_roster_wake_id` is created only when `wake_id` is newly added. A database
left between migration steps, or one created by an older implementation, can
have the column without the unique index.

**Fix:** Run `CREATE UNIQUE INDEX IF NOT EXISTS idx_roster_wake_id ...`
unconditionally after column migration. Add a migration test for a partially
migrated schema.

### 9. Documentation describes removed behavior

**Severity:** Medium, operational correctness  
**Where:** `PLAN-BACKGROUND-WAKE.md`, especially lines 3, 45, 55, 81, and
199-208; `NUDGE-V1-TERMINAL.md:151-153`

`PLAN-BACKGROUND-WAKE.md` is marked as a design plan but still describes the
removed AppleScript worker, terminal transport, and backward-compatible
fallback. The historical document links it as the current wake design. This
contradicts `PLAN.md`, `README.md`, and the actual Rust implementation.

**Fix:** Delete the obsolete plan or rewrite it as a current implementation
specification. Update the historical document to link only to the current
design. Remove stale AppleScript claims from the unreleased changelog entry.

### 10. Formatting and release hygiene are incomplete

**Severity:** Low  
**Where:** `src/bus.rs`, `src/wake.rs`, `Cargo.toml:1-6`, repository status

`cargo fmt -- --check` fails in three regions. The worktree contains untracked
implementation files, including `src/wake.rs`, `src/nudge.rs`, `src/bin/`, and
`launchd/run.sh`; a commit containing only tracked changes would omit part of
the feature. `cargo package` also warns that repository, homepage, and
documentation metadata are missing despite the Makefile exposing publish
targets.

**Fix:** Format before merging, explicitly review and add intended untracked
files, and either add crates.io metadata or remove the publish targets if this
is not a publishable crate.

## Validation Performed

- `cargo test --all-targets`: 67 passed
- `cargo test --doc`: 2 passed
- `cargo clippy --all-targets --all-features -- -D warnings`: passed
- `cargo check --all-targets --all-features`: passed
- `cargo doc --all-features --no-deps`: passed
- `bash -n launchd/run.sh`: passed
- `plutil -lint launchd/com.mentischat.plist`: passed
- `cargo fmt -- --check`: failed
- `cargo audit`: unavailable in the installed Cargo toolchain

## Accepted Design Choices

- Localhost-only operation without authentication is intentional for v1 and
  documented in `README.md`.
- At-least-once delivery and shared-name inboxes are intentional.
- The legacy `nudgeWindow` field and `terminal` transport alias are reasonable
  compatibility debt while old databases and clients are supported.
- Duplicate transitive dependencies reported by `cargo tree --duplicates`
  originate from upstream dependencies; no obvious redundant direct dependency
  was identified.
