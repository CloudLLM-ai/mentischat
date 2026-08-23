---
name: rust-engineer
description: >
  MentisChat Rust engineering standards for writing, reviewing, and fixing
  production code. Merged house style (MentisDB rust-engineer +
  DiarioBitcoin rust-backend-engineer). Use whenever implementing features,
  fixing bugs, adding APIs or MCP tools, or reviewing Rust changes in this
  repository. For micro-optimization and hot-path audits, also load
  rust-performance-reviewer. Triggers: rust code, bug fix, implement,
  refactor, clippy, add test, public API, review rust, MCP tool, enum design.
triggers:
  - rust engineer
  - rust coding style
  - implement rust
  - fix rust bug
  - clippy
  - public API
  - write tests
  - review rust
  - enum design
  - mcp tool
---

# MentisChat Rust Engineer Skill

Load this skill before writing or reviewing Rust in this repository.
It merges the MentisDB `rust-engineer` house style with the DiarioBitcoin
`rust-backend-engineer` standards, adapted to the mentischat crate.

## Companion skills (load when relevant)

| Skill | When |
|-------|------|
| [`rust-performance-reviewer.md`](rust-performance-reviewer.md) | Hot-path audits, allocation/cache footprint review, Criterion benches, flat profiles |

Do not re-derive performance conventions from memory — open the companion
skill and follow it.

## Non-negotiables

1. **Warning-free builds.** `make clippy` is the gate:
   ```bash
   make clippy   # cargo fmt + cargo clippy --all-targets --all-features -- -D warnings
   make test     # cargo test
   ```
   Treat every Clippy warning as a hard error. Do not paper over with `#[allow]`
   unless the reason is documented and unavoidable.

2. **Tests live outside production modules.** All tests are integration tests
   under `tests/` (`tests/bus.rs`, `tests/http.rs`, `tests/mcp.rs`). Never add
   `#[cfg(test)] mod tests { ... }` blocks to source files in `src/`. Small
   pure unit tests next to private helpers are acceptable only when they
   cannot be expressed through the public surface.

3. **Every bug fix needs a regression test.** Reproduce the failure first,
   then fix, then prove the test fails without the fix and passes with it.

4. **Public surface is a manual.** Every exported type, function, trait, enum
   variant, and struct field gets rustdoc. Prefer short prose plus a runnable
   `# Examples` block over long narrative. Document errors, invariants, and
   when *not* to use the API. Use `# Errors`, `# Panics`, `# Safety` sections
   per Rust convention where applicable.

5. **Parameters and types are correct by construction.** Prefer enums and
   newtypes over free-form strings (see `names.rs`, `errors.rs`). Validate at
   construction time. Fail early with specific error enums, not opaque
   `String` errors.

6. **Update `changelog.txt` when the work is done.** Add an entry under the
   topmost section of `changelog.txt` (usually `UNRELEASED`). Do not invent a
   new version header unless you are cutting a release. After adding your
   entry, re-sort all entries in that top section by impact:
   most important first, trivial last. Typical order:

   1. Security / data-loss / integrity breaks
   2. User-facing bugs (MCP wire, REST, mailbox semantics)
   3. Features and API surface changes
   4. Performance and scalability
   5. Tests, refactors, internal cleanups
   6. Docs-only and chore

   Match existing style: bullet lines with a short prefix such as
   `fix(scope):`, `feat(scope):`, `perf(scope):`, `docs(scope):`,
   `test(scope):`, `refactor(scope):`, then a clear one- or two-line
   description of *what changed and why it matters*.

## Core philosophy

**Immutability is the default. Mutability is the last resort.**

- Prefer immutable data structures and functional patterns (`map`, `filter`,
  iterators) over mutable state.
- When mutation is unavoidable, contain it to the smallest possible scope and
  document why.
- Prefer returning new values over modifying existing ones.

**Strongly-typed domains eliminate entire classes of bugs.**

- Never use raw `String` constants as function parameters, control values, or
  discriminators.
- Model every domain concept with enums that encode valid states at compile
  time (message state `pending`/`in_flight`, error codes, wait outcomes).
- Make enums ergonomic at the boundary with `FromStr`, `Display`, and serde;
  use `#[serde(rename_all = "snake_case")]` for consistent serialization.

**Simplicity and composability over cleverness.**

- Short, focused functions that do one thing well (typically under 30 lines).
- Compose small functions rather than writing monoliths.
- Explicit over implicit — no excessive trait magic or macro complexity unless
  it genuinely reduces code.
- If a function needs a comment to explain what it does, break it into
  smaller, well-named parts.

## Enum design

1. **Always derive**: `Debug, Clone, PartialEq, Eq, Serialize, Deserialize` at
   minimum.
2. Add `Copy` when variants carry no heap data.
3. Implement `Display` for human-readable output and `FromStr` for parsing,
   each with a dedicated error type — never `String` as an error type.
4. Consider `strum::EnumString` / `strum::Display` for boilerplate reduction.

## Error handling

- Use `thiserror` for library/domain errors; `anyhow` only in binary/test code
  if needed. This repo currently hand-rolls typed errors in `errors.rs` —
  follow that pattern unless the enum grows large.
- Define specific error enums per module — never `Box<dyn Error>` in library
  code.
- Reserve `unwrap()`/`expect()` for provably infallible cases; always give
  `expect()` a message. No `unwrap()` on daemon request paths.
- At the MCP boundary, domain failures return `Ok(ToolResult::failure(json))`
  (`isError: true`); only malformed requests become Rust `Err`.

## Design bias (KISS + robust)

- **One mechanism. No speculative fallbacks.** Ship exactly one way to do
  something; a fallback is a second mechanism to test, document, and debug.
- **Keep it simple and stupid.** Small functions, obvious control flow.
- **Code robust parts that are used together.** Co-locate related types,
  validation, and persistence.
- **Be intentional with memory.** Avoid needless clones on hot paths; prefer
  borrowing. Durability and correctness beat micro-optimizations — measure
  first (`rust-performance-reviewer`).
- **Lock granularity over cleverness.** The bus is guarded by one
  `tokio::sync::Mutex`; never hold any lock across `.await` beyond what the
  waiter algorithm specifies.
- **Comment only what is not obvious.** Comment *why* for concurrency,
  compatibility, or surprising invariants.

## Dependency direction

- mentischat is a downstream consumer of `cloudllm_mcp`: pin it exactly
  (`=0.3.6`) and upgrade deliberately — wire compat is contractual.
- Do not add dependencies casually; every addition must earn its compile time.

## Documentation style

```rust
//! Module-level overview: what this is, when to use it, storage model.

/// One-line summary of the type or function.
///
/// Longer explanation of policy, invariants, and failure modes.
///
/// # Examples
///
/// ```
/// use mentischat::names::is_valid_agent_name;
///
/// assert!(is_valid_agent_name("grok"));
/// assert!(!is_valid_agent_name("-bad name"));
/// ```
pub struct Example {
    /// Human-readable field documentation.
    pub alias: String,
}
```

- Document every public field.
- Document error variants with operator-facing meaning.
- Keep examples compiling under `cargo test --doc` where possible.

## Implementation checklist

| Step | Action |
|------|--------|
| 1 | Read neighboring modules for naming, error, and layout patterns |
| 2 | Prefer extending an existing type over adding a parallel abstraction |
| 3 | Use `Result` + domain error enums; implement `Display` + `Error` |
| 4 | Never log message bodies or secrets — log ids, names, byte lengths |
| 5 | For shared state: check lock scope against the bus mutex contract |
| 6 | Add or update tests under `tests/` for the behavior change |
| 7 | Run `make clippy` and relevant `cargo test` filters |
| 8 | If hot-path performance changed, consult `rust-performance-reviewer` |
| 9 | Update rustdoc if the public contract changed |
| 10 | Update `changelog.txt` under the **top** section; re-sort by impact |
| 11 | Granular commits: one logical change per commit, per file when practical |

## Testing conventions

- Integration tests under `tests/` with clear behavioral names:
  `unacked_in_flight_rows_redeliver_until_acked`.
- Use temp dirs (never touch `~/.cloudllm/mentischat/` in tests); always clean up.
- Cover success path, denial path, and restart/crash-reopen cases.
- For wire behavior: test initialize handshake, tools/list shape, isError
  mapping, and transport guards (405/403).
- Do not rely on env pollution; lock process-global env when needed.
- Assert ordering with deterministic capture, never wall-clock timing races.
- Pin external contract limits with production-shaped payloads, not toy fixtures.
- Hermetic suites miss real-world shape — run one live smoke against a real
  harness before calling a release done.

## Review lens (use when auditing a bug)

1. **Correctness:** Is the happy path correct for the least-privilege caller?
2. **Fail modes:** Do error bodies distinguish which agent/name failed and why?
3. **Defaults:** What happens when optional parameters are omitted?
4. **Delivery guarantees:** Can this change drop mail, duplicate rows, or tear
   a batch? At-least-once + ack-before-delete is the invariant.
5. **Regression surface:** Is there a test that would have caught this?
6. **Concurrency:** Waiter FIFO, one-consumer-per-wake, park-under-mutex —
   still honored?
7. **Simplicity:** Is there a smaller fix that preserves the contract?
8. **Changelog:** Does the top section describe this change, ordered by impact?

## Hard-earned lessons (workspace post-mortems)

Distilled from real production mistakes across sibling projects. These
override generic best practices when they conflict.

1. **Verify claims before asserting them.** General-knowledge assumptions
   about external systems die on live verification. Verify at the deepest
   level the pipeline depends on.
2. **Match the tool to the job.** Copying an existing pattern without
   evaluating fit is how you ship a broken core value prop.
3. **Simulate the exact external-API contract before shipping UX.** Reading
   the transport (curl 200) is not verifying rendering or protocol semantics.
4. **Timeouts must cover EVERY wait — including library internals and cleanup
   paths.** Wrap whole external operations in a watchdog, not just the steps
   that accept a timeout parameter.
5. **"Success" on empty input is data loss.** Pre-flight any invariant that a
   downstream trim/queue/buffer can silently violate; fail loudly first.
6. **Error strings that get persisted or displayed must never embed raw
   response bodies** — the parse-failure path is exactly when the body may
   carry live secrets. Summarize untrusted bodies with char-boundary-safe
   truncation.
7. **Never hold a `std::Mutex` guard across `.await`** — clippy
   `await_holding_lock` errors under `-D warnings`.
8. **Pin bogus provider credentials in "hermetic" tests.** Dev shells export
   real API keys; set the env var to junk so network stages cannot fire.
9. **Reachability ≠ 2xx.** Probes must match `Ok(_) | Err(StatusCode(401|403))`
   — a 401 proves presence.
10. **Evolve wire protocols with `#[serde(default)]` optional fields** and an
    explicit old-version degradation story pinned by serde tests.
11. **A lib fix ships nowhere until every binary linking it is reinstalled**
    — keep the binary matrix in mind on shared-lib changes.
12. **Never hardcode secrets in start scripts** — source from one props/env
    file of truth; hardcoded copies drift into auth outages.

## Anti-patterns

- Stringly-typed states or tool policies where an enum fits
- Putting test suites inside `src/` modules
- `unwrap()` on daemon request paths
- Vague error bodies that do not say which agent/name failed or why
- `Arc<RwLock<HashMap<…>>>` for multi-key concurrent maps (use DashMap)
- `std::sync::Mutex` held across `.await`
- Drive-by refactors unrelated to the task
- `#[allow(dead_code)]` / `#[allow(clippy::…)]` without a written reason
- Claiming a performance win without a bench or load number
- Shipping a fix/feature/test without a `changelog.txt` entry under the top
  section, appended in random order instead of sorted by impact
- Writing changelog lines that only restate the commit subject
- Dual-writing chat traffic into MentisDB (the mailbox is not the ledger)
- Adding a sixth MCP tool when five cover the contract

## Version history

- **2026-08-22** — Initial combined skill for mentischat from MentisDB
  `rust-engineer` and DiarioBitcoin `rust-backend-engineer`.
