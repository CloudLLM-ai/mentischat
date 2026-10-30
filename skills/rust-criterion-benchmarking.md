---
name: rust-criterion-benchmarking
description: >
  Writing, running, and reading Criterion benchmarks for mentischat: bench
  layout under benches/, what to bench in the mailbox store (send, pull,
  history, fan-out, contention, wake latency), async and multi-threaded
  benches, baselines, output capture, and gotchas. Adapted from MentisDB
  rust-criterion-benchmarking. Load when adding a bench or backing a
  performance claim with numbers.
triggers:
  - benchmark
  - cargo bench
  - criterion
  - throughput
  - latency
  - performance measurement
  - baseline
  - regression
---

# rust-criterion-benchmarking

Use Criterion to measure mentischat with statistical rigor. Never rely on
intuition; let the numbers decide. Prefer [`rust-engineer.md`](rust-engineer.md)
for general style and [`rust-performance-reviewer.md`](rust-performance-reviewer.md)
for deciding *what* is worth measuring.

## Project setup

```toml
# Cargo.toml
[dev-dependencies]
criterion = { version = "0.5", features = ["html_reports", "async_tokio"] }

[[bench]]
name = "store"
harness = false   # REQUIRED: disables the default test harness
```

Use whatever Criterion version `Cargo.toml` already pins; do not bump it in
a bench-only change.

## Layout

```text
benches/
  store.rs       # one file per subsystem, harness = false
tests/           # behavior tests live here, never in benches/
```

- Benches use the crate's public API like an external user; no
  `#[cfg(bench)]` hooks in `src/`.
- `make clippy` runs with `--all-targets`, so benches must build
  warning-free too.
- Each bench file opens with a `//!` doc saying what it measures and how to
  run it.

## What to bench in mentischat

| Bench | Measures | Shape |
|---|---|---|
| send | append to a conversation log | single op, then `Throughput::Elements` batches |
| pull | `check_inbox` returning N unread | vary N: 1, 10, 100, 1000 |
| history | read back K messages | vary K and conversation count |
| fan-out | one sender, many recipients | vary recipients |
| contention | T tasks sending/pulling at once | `iter_custom` with T threads or tasks |
| wake latency | send to a parked pull until it returns | `iter_custom`, report per-wake time |
| retention | sweep over M read messages | `iter_batched` with a fresh store |

Size inputs like production: realistic body lengths (hundreds of bytes to a
few KiB), not one-byte toys, and agent counts in the tens.

## Skeleton

```rust
//! Store benchmarks: send, pull and history on the in-memory mailbox.
//!
//! Run: cargo bench --bench store
//! Report: target/criterion/report/index.html

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

fn bench_pull(c: &mut Criterion) {
    let mut group = c.benchmark_group("pull");
    for n in [1usize, 10, 100, 1_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || store_with_unread(n),          // setup: not timed
                |store| black_box(pull_all(&store)),
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_pull);
criterion_main!(benches);
```

`store_with_unread` and `pull_all` stand for helpers in the bench file.

## Async and contention benches

- Async: build one runtime outside the loop and use `b.to_async(&rt)`
  (`async_tokio` feature). Do not build a runtime per iteration.
- Contention: `b.iter_custom(|iters| { ... })` spawning T workers that each
  run `iters / T` operations, returning the total elapsed `Duration`. Report
  T = 1, 2, 4, 8 so the scaling curve is visible.
- Wake latency: time from `send` returning to the parked pull returning,
  measured inside `iter_custom`; a timeout or sleep inside the timed region
  makes the number meaningless.

## Running

```bash
cargo bench --bench store                     # one file
cargo bench --bench store -- pull             # one group (regex filter)
cargo bench --bench store -- "pull/100"       # one case
cargo bench --bench store -- --save-baseline before   # record
cargo bench --bench store -- --baseline before        # compare against it
cargo bench 2>&1 | tee bench.txt              # capture: tee, not >
```

Use `make bench` instead if the Makefile has that target.

## Reading results

```text
pull/100  time:   [12.31 µs 12.50 µs 12.67 µs]
          thrpt:  [7.89 Melem/s 8.00 Melem/s 8.12 Melem/s]
          change: [-3.2% -1.5% +0.1%] (p = 0.12 > 0.05)
          No change in performance detected.
```

- The three numbers are the low / point estimate / high of the confidence
  interval.
- `p > 0.05`: noise, not a change.
- "Performance has regressed" compares against the last run, which may have
  been on an idle machine. It is a prompt to look, not a verdict.

**Golden rule: compare absolute medians across meaningful runs (named
baselines, same machine, same load), not the delta label.**

When a change claims a win, put the before/after medians in the commit
message or the changelog line.

## Gotchas

1. **Output capture:** `cargo bench > file.txt` swallows Criterion's output;
   use `2>&1 | tee`.
2. **Baseline drift:** two runs of the same code under different load
   "regress". Save named baselines and close other heavy work (builds, test
   suites, the other agent's `cargo` jobs) before measuring.
3. **`black_box`:** wrap results (and inputs the compiler could constant-fold)
   in `std::hint::black_box`; newer Criterion deprecates its own.
4. **`harness = false`:** without it the file does not build as a bench.
5. **Setup in the timed loop:** building the store or the messages inside
   `iter` measures allocation, not the operation. Use `iter_batched`.
6. **State that grows across iterations:** a `send` bench that keeps
   appending to one log measures an ever-larger log. Reset per batch or
   bench a bounded steady state.
7. **Debug builds:** `cargo bench` uses the `bench` profile (release
   settings); numbers from `cargo test` timings mean nothing.
8. **Micro vs real:** a bench that improves 30% and does not move the daemon
   under a realistic load is a footnote. HTTP end-to-end load (many agents
   over `/mcp`) belongs in a harness-free `[[bench]]` binary or a script
   against a running daemon, not a Criterion micro-bench.

## Version history

- **2026-10-30** — Initial mentischat version, adapted from MentisDB
  `rust-criterion-benchmarking` (2026-07-09): mailbox bench catalog, async and
  contention benches, named baselines, `std::hint::black_box`, setup and
  growing-state gotchas.
