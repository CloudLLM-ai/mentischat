# Engineering pipeline

How mentischat is built, tested, audited and measured, and how to reproduce
every CI job on your own machine. The layout is meant to be copied to the other
CloudLLM-ai Rust crates: the workflow files are thin, the logic is in the
`Makefile` and in `scripts/`, and this page is the manual.

## The shape of it

| Workflow | Triggers | What it answers |
|---|---|---|
| `ci.yml` | push to master, pull request, manual | Does it build, lint, test, document, hold its MSRV, meet the coverage gate, and replay the fuzz corpora? |
| `audit.yml` | push to master, pull request, **daily**, manual | Is any dependency vulnerable, unmaintained, yanked, wrongly licensed or from an unexpected source? |
| `fuzz.yml` | **weekly** (Monday), manual, called by `security.yml` | Does coverage-guided fuzzing of every target find anything new? |
| `bench.yml` | **weekly** (Sunday), manual, pull requests touching `src/` or `benches/` (advisory) | Did anything get slower? |
| `soak.yml` | **weekly** (Saturday), manual | Does the daemon survive an hour of traffic and a restart without losing mail or leaking? |
| `security.yml` | **weekly** (Monday), manual | One review: audit, security lints, unsafe inventory, a longer fuzz pass. |

Rules the workflows follow:

- **A step is a `make` target.** `make fmt-check`, `make coverage`, `make deny`
  do the same thing on your laptop and on the runner. If you change what a job
  does, you change the `Makefile` or a script, not YAML.
- **Actions are pinned by major version** (`actions/checkout@v4`,
  `taiki-e/install-action@v2`) and tools by exact version (`cargo-deny@0.20.2`,
  `cargo-fuzz 0.13.2`). Dependabot opens a pull request when a newer action
  exists.
- **No secrets.** Every workflow runs with `contents: read` (bench also reads
  `actions` to fetch the previous baseline). Nothing pushes to the repository;
  a person commits corpora and baselines.
- **Shared setup is a composite action** (`.github/actions/setup-fuzz`), so the
  nightly toolchain and the `cargo-fuzz` version live in one file.
- Each workflow file opens with a comment saying what it is for and which doc
  section to read.

## Reproducing a job locally

| Job | Command | Needs |
|---|---|---|
| Format, check, clippy, tests, docs | `make ci` (or one of `make fmt-check`, `check`, `clippy`, `test`, `doc`) | stable Rust |
| MSRV | `make msrv` | `rustup toolchain install 1.88` |
| Dependency policy | `make deny` | `cargo install cargo-deny` |
| Coverage | `make coverage` (`make coverage-open` to view) | `cargo install cargo-llvm-cov`, `rustup component add llvm-tools-preview` |
| Security lints | `make clippy-security` | stable Rust |
| Fuzz build and corpus replay | `make fuzz-build fuzz-replay` | nightly, `cargo install cargo-fuzz` |
| Fuzz one target | `make fuzz TARGET=rest_http FUZZ_SECONDS=300` | same |
| Minimize a corpus | `make fuzz-cmin TARGET=rest_http` | same |
| Benchmarks | `make bench bench-json`, then `make bench-baseline bench-compare` | `gh` logged in, for the baseline |
| Soak | `make soak SOAK_SECONDS=600` | stable Rust, `ps` and (on macOS) `lsof` |

`make help` lists every target.

## Dependency policy

`audit.yml`, `deny.toml`, `.github/dependabot.yml`.

`cargo deny check` runs four checks from `deny.toml`, on every push and pull
request and once a day (an advisory can appear against a dependency that did
not change):

- **advisories**: RustSec vulnerabilities, unmaintained and yanked crates. Any
  hit fails.
- **licenses**: an allowlist of permissive licenses (MIT, Apache-2.0, BSD,
  ISC, Unicode, Zlib, CC0, MPL-2.0). A new license is a decision for a person.
- **bans**: wildcard version requirements are denied. Duplicate versions of one
  crate are a warning, because most are forced by upstream pins (two `rand`
  majors under the OpenMLS stack, for example); the job log lists them.
- **sources**: crates.io only. A git or alternate-registry dependency has to be
  added to `deny.toml` on purpose.

Every `ignore` in `deny.toml` carries its advisory id and the reason it cannot
hurt us. Today there are five:

| Advisory | Crate | Why it is ignored |
|---|---|---|
| RUSTSEC-2026-0387 | `crypto-bigint 0.5.5` (timing) | Reached only through `p256` inside `openmls_rust_crypto`. The one MLS ciphersuite mentischat uses is X25519, AES-128-GCM, SHA-256, Ed25519 (`src/client/session.rs`, `SUITE`), so no P-256 operation ever runs. The fix is `crypto-bigint 0.7`, which `p256 0.13` does not allow; OpenMLS 0.9 moves on but needs Rust 1.91 and the MSRV is 1.88. |
| RUSTSEC-2026-0207, -0208 | `libcrux-sha3 0.0.8` | `hpke-rs 0.6.1` calls it only to derive X-Wing / ML-KEM keys (`hpke-rs` `src/kem.rs`); the X25519 suite never gets there. `hpke-rs` pins the exact version. |
| RUSTSEC-2026-0212 | `libcrux-secrets 0.0.5` | Used only by `libcrux-sha3`, so as unreachable as the two above. |
| RUSTSEC-2026-0173 | `proc-macro-error2` | Unmaintained, and a compile-time proc-macro dependency of ratatui's macros; it is not in the shipped binary. |

`cargo audit` reports nine vulnerabilities where `cargo deny` reports none.
The difference is not a weaker check: `cargo audit` reads every entry of
`Cargo.lock`, including `hpke-rs-libcrux`, `libcrux-aesgcm`, `libcrux-kem`
and friends, which are `dev-dependencies` of `hpke-rs` that Cargo records in
the lockfile but never builds (`cargo tree --all-features --target all` does
not contain them). `cargo deny` checks the graph that is actually built. The
weekly security run still runs `cargo audit` as a report so the difference stays
visible.

**Dependabot** (`.github/dependabot.yml`) opens weekly pull requests for Cargo
(the crate and `fuzz/`) and for GitHub Actions, grouping minor and patch
updates. `cloudllm_mcp` (pinned `=0.4.0`, unstable between minors) and the
OpenMLS minor line (needs Rust 1.91) are excluded on purpose. Every Dependabot
pull request runs the full CI before anyone merges it.

## Coverage

`ci.yml` job `coverage`, `scripts/coverage_gate.py`.

`make coverage` runs the whole test suite under `cargo llvm-cov`, writes an
HTML report, an `lcov.info` and a `summary.json` into `target/coverage/`, and
fails when any of **regions, functions or lines** is under **90%**. CI uploads
the HTML and lcov as the `coverage-report` artifact and writes the table into
the job summary.

Measured on 2026-11-06, `src/` only:

| Metric | Before this work | After |
|---|---:|---:|
| Regions | 88.70% | 93.02% |
| Functions | 89.66% | 93.69% |
| Lines | 90.38% | 94.56% |

What was added to get there (behavior tests, not padding): the built binary
run as a process (startup, the single-instance lock, the SIGTERM wipe,
`MENTISCHAT_PERSIST` across a restart, corrupt snapshot and bad configuration
refusals, the `client` subcommands: `tests/daemon_process.rs`); the identity
key file's permissions and damaged contents (`tests/client_identity.rs`); the
client's HTTP reply parser and `Daemon::call` against a scripted server
(`tests/client_http.rs`); one assertion per signed-journal replay rule with its
rejection reason (`tests/signed_replay.rs`); the error-code, status and body
table (`tests/errors_contract.rs`); the log buffer; and the soak scenario.

**What is excluded, and why.** One file: `src/tui/painter.rs`, the loop that
takes over a real terminal (raw mode, alternate screen, key events). It needs a
TTY and CI has none. It was split out of `src/tui/terminal.rs` so the exclusion
is exactly the untestable part: the panic-hook scoping and `run_guarded`, which
decide whether a view panic stops the daemon, stay in `terminal.rs` and are
tested. The exclusion is `COVERAGE_IGNORE` in the `Makefile`. The dashboard
itself (`src/tui/mod.rs`) is rendered into a test buffer and is covered.

**Branch coverage is measured, not enforced.** It needs nightly and the unstable
`-Z coverage-options=branch` flag (`cargo +nightly llvm-cov --branch`, with
`llvm-tools-preview` on that nightly), whose behavior has changed between
nightlies; a gate on it would break CI when the toolchain moves, not when the
code does. On 2026-11-06 it measured 79.92% (836 of 1046 branches), below the
bar the other three meet, and the nightly's region count differs from stable's
(90.43% there), so mixing the two in one gate would be wrong. `make
coverage-branch` prints the numbers for a person who wants to look; raise
branch coverage by testing the error arms listed below, and turn the gate on
when the flag is stable.

The remaining gaps are the daemon's terminal-view branch in `main.rs`, rarely
taken error arms (a `write` to a closed pipe, a poisoned lock), and parts of
`src/client/session.rs` that only run against a misbehaving daemon.

The gate is 90% on purpose and the measured numbers are above it, so an
ordinary change does not trip it, and a change that adds a lot of untested code
does. Raise the number as the margin grows; never lower it to land a change.

## Fuzzing

`ci.yml` job `fuzz`, `fuzz.yml`, `fuzz/`.

Every parser of untrusted bytes and every state machine reachable from outside
has a target in `fuzz/fuzz_targets/`. The harnesses assert an invariant, not
just "does not panic".

| Target | Surface |
|---|---|
| `names` | agent, room and provider name parsing |
| `tool_json` | MCP tool arguments |
| `snapshot_restore` | `snapshot.json` on disk |
| `ws_frame` | WebSocket client frames |
| `signed_wire` | signed-room envelopes, keys, signatures |
| `signed_journal` | a signed room's journal and retained envelopes |
| `client_wire` | daemon HTTP replies and the client's command line |
| `bus_ops` | the whole bus against a reference model |
| `rest_http` | **new.** The real front door: `build_router` with an arbitrary method, path, query, body and the browser headers. Asserts no panic, no 5xx but 503/507, that the loopback guards (peer, `Host`, `Origin`, `Sec-Fetch-Site`) refuse exactly what an independent reading of the rule says and leave no effect behind, and that every REST error is JSON with a documented code. |
| `proxy_stdio` | **new.** The client's stdio MCP proxy fed arbitrary lines, with no daemon behind it. One JSON-RPC reply per request, none per notification, `result` xor `error`, a clean end at EOF. |
| `client_files` | **new.** Damaged `identity.key` and `state.json`: a typed `Storage` error, never a panic, never a rewritten or replaced key; `Identity::open` accepts exactly 64 hex digits. |

`src/rest.rs` and `src/signed_api.rs` are reached through `rest_http`, which
goes through `build_router`, not around it, so the middleware is in scope.
`src/client/*` input parsing is `client_wire`, `proxy_stdio` and `client_files`.
The proxy's end-to-end encryption paths need a live group and daemon, so they
are covered by `tests/e2e_proxy.rs` rather than the fuzzer.

**Per push**, `ci.yml` builds every target and replays the committed corpora
(`make fuzz-build fuzz-replay`, seconds): this catches harness rot and
regressions on old findings.

**Weekly**, `fuzz.yml` fuzzes every target for five minutes (`seconds` input on a
manual run). The matrix comes from `make fuzz-targets` (`cargo fuzz list`), so a
new target is fuzzed without editing the workflow. `security.yml` calls it again
with 15 minutes per target.

**When a run finds something**: the job fails and uploads
`fuzz-crashes-<target>`. Download it, minimize with
`cargo +nightly fuzz tmin <target> <file>`, turn it into a named regression
test under `tests/` (see `tests/snapshot_fuzz.rs`), fix it, add the input to
`fuzz/corpus/<target>/`, and write the changelog line.

**Corpus growth**: every run uploads `fuzz-corpus-<target>` (30 days). A person
downloads it into `fuzz/corpus/<target>/`, runs `make fuzz-cmin TARGET=<target>`
so only inputs that add coverage stay, and commits the result. CI never pushes.

## Weekly security audit

`security.yml`.

One run, Monday 05:11 UTC, four jobs. Findings are in the job summaries.

1. **audit**: `make deny` (a gate, as above) and `cargo audit` (a report; see
   the table above for why it says more than `cargo deny`).
2. **lints**: `make clippy-security` on shipped code (`--lib --bins`, not tests).
   - *Gate*, clean today: `clippy::unwrap_used`, `todo`, `unimplemented`,
     `dbg_macro`, `mem_forget` are denied. This job and `ci.yml`'s `verify`
     job are separate, so a new `unwrap()` is caught here; promote the gate into
     `ci.yml` by adding `make clippy-security` after `make clippy` if you want it
     per push.
   - *Report*, not clean today (counts on 2026-11-06): `arithmetic_side_effects`
     124, `indexing_slicing` 71, `expect_used` 3, `panic` 1. They are listed per
     file in the summary. They are not gated because they are mostly
     intentional: bounds are checked on the line above, counters cannot
     overflow in a process lifetime, and the three `expect`s and one `panic!`
     are invariants of constants. Burn them down file by file and move a lint
     into the gate when its count reaches zero.
3. **unsafe**: `cargo geiger`, an inventory of `unsafe` in the dependency tree
   (report). Our own crate is `#![forbid(unsafe_code)]` in `src/lib.rs` and
   `src/main.rs`, which the compiler enforces on every build, so the inventory
   is only about dependencies.
4. **fuzz**: the longer pass over every target, 15 minutes each.

**Proposal, not implemented: an AI review.** A scheduled job could hand the
week's diff (`git log --since=1.week -p -- src/`) to a model with a
security-reviewer prompt and open an issue with its findings. It is left out
because it needs an API key stored as a repository secret and sends source to a
third party; if it is wanted, the key should be scoped to that one job, the
output should be an issue (never a commit or a status check that blocks), and
findings should be treated as leads to verify, not as results.

## Benchmarks

`bench.yml`, `scripts/bench.py`, `benches/`.

`make bench` runs the four Criterion suites in `benches/` (`bus`, `engine`,
`rest`, `signed`). `make bench-json` reduces them to
`target/bench/current.json`: the **median** time of each benchmark in
nanoseconds, medians because a mean is dragged around by the one slow iteration
a noisy neighbour causes.

- **Schedule**: Sunday 03:41 UTC on master. The run uploads `bench-results`
  (kept 90 days); those artifacts are the history. The baseline for a run is
  the `bench-results` of the previous successful scheduled run
  (`make bench-baseline`, needs `gh`).
- **Threshold**: `BENCH_THRESHOLD=25` percent. A benchmark more than 25% slower
  than its baseline is flagged with a warning annotation and a row in the job
  summary, and fails a *scheduled* run.
- **Why so wide**: GitHub-hosted runners are shared virtual machines. The same
  commit moves by 10 to 20% between runs on them (neighbours, CPU model and
  frequency), and the multi-pair benchmarks more than the single-thread ones.
  A tighter threshold would cry wolf every week and teach people to ignore it.
  A real regression is usually a multiple; 25% catches those and misses
  small drift, which is what a local `cargo bench` against a saved baseline
  (`cargo bench -- --save-baseline main`, then `--baseline main`) is for.
- **Pull requests** that touch `src/`, `benches/` or `Cargo.*` run the benches in
  Criterion's `--quick` mode and compare against the latest baseline, but only
  advisory: the job never fails on it. A manual run defaults to advisory as well
  (`advisory: false` makes it fail).
- **First run**: until a scheduled run has succeeded there is no baseline, and
  the summary says so. Trigger `bench.yml` manually on master once to seed it
  (a manual run uploads results but is not picked up as a baseline, which only
  scheduled runs provide; the first Sunday does it).

## Soak test

`soak.yml`, `tests/soak.rs`.

The real daemon binary, six agents in one room, mixed traffic: direct sends,
room sends, inbox pulls and history reads, each worker paced at a few
milliseconds, against a daemon with `MENTISCHAT_PERSIST=1` and a 20 second read
TTL so expiry runs many times. Halfway through, the workers pause, the daemon is
stopped with SIGTERM (its snapshot is written) and started again on the same
port and data directory, and the workers resume. A 130 second run on a laptop
makes about 190,000 requests (52,000 direct and 52,000 room messages accepted).

It asserts:

1. **Exactly-once delivery.** Every accepted direct message reaches its
   recipient once, across the restart. Every accepted room message reaches each
   other member once, except that a room is a bounded ring and a member that
   falls far behind loses the oldest: the soak requires that the messages that
   never arrived are exactly the ones the daemon told that member about in
   `skipped`. Nothing arrives at someone it was not for.
2. **The restart is clean.** The first run exits 0 and leaves `snapshot.json`;
   the second start serves `/health` with the whole roster back.
3. **No leaks.** Each half is judged alone (the restart resets the process).
   After a 25% warm-up, resident memory at the end may not exceed 1.5x its
   early level plus 48 MiB, and open file descriptors may not exceed their early
   maximum by more than 24. Every request opens and closes a connection, so a
   per-request leak grows linearly and dwarfs those allowances. The samples
   (every 5 seconds) are printed so a failure shows the curve.
4. **A clean life.** No log line says `panicked` or `ERROR`, and the second run
   exits 0 on the final SIGTERM.

`cargo test` runs `soak_smoke` (8 seconds, assertions 1, 2 and 4: too short to
say anything about growth) so the harness cannot rot. `make soak
SOAK_SECONDS=600` runs `soak_long` locally; `soak.yml` runs it for 3600 seconds
every Saturday, with a 150 minute job limit.

## Adopting this in another crate

To copy the pipeline to `cloudllm-dht` or `mentisdb`:

1. Copy `.github/workflows/`, `.github/actions/`, `.github/dependabot.yml`,
   `deny.toml`, `scripts/` and the Makefile sections (`ci`, `msrv`, `deny`,
   `coverage*`, `clippy-security`, `bench*`, `soak`, `fuzz*`).
2. In `deny.toml`, start with an empty `ignore` list and add an entry (id and
   reason) only for a finding you have looked at. Check `[licenses] allow`
   against `cargo deny check licenses`.
3. In the `Makefile`, set `MSRV`, `COVERAGE_MIN`, `COVERAGE_IGNORE` (the files
   that genuinely cannot run in CI, with a reason in this document) and the
   `SECURITY_DENY` list (only lints the code is clean for).
4. Give the crate a `tests/common/process.rs`-style helper for starting its
   binary on port 0 and a soak scenario that fits it; the thresholds in
   `tests/soak.rs` are constants at the top of the file.
5. Add the badges to the README and adjust the schedule so the weekly jobs do
   not all start in the same minute.
6. Run each job once by hand (`workflow_dispatch`) and fix it until it is
   green before relying on it.
