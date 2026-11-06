# Makefile
# Sections below: build and verify, dependencies and coverage, fuzzing, benchmarks, soak, run.
.PHONY: build release clean fmt fmt-check check test doc help tasks clippy publish publish-dry-run install uninstall run run-plain stop ci msrv deny clippy-security coverage coverage-branch coverage-open bench bench-json bench-baseline bench-compare soak fuzz-build fuzz-targets fuzz-replay fuzz fuzz-cmin

default: help
CARGO_CMD=/usr/bin/env cargo
# cargo-sweep (cargo install cargo-sweep), when installed, deletes whatever an older build
# left in target/ that this build didn't use, so only fresh release artifacts remain.
SWEEP := $(shell command -v cargo-sweep >/dev/null 2>&1 && echo cargo sweep)
MENTISCHAT_PORT?=9480
MSRV?=1.88
FUZZ_SECONDS?=60
# The crate has no `unsafe`, so AddressSanitizer would only slow the fuzzers down (about 2x); skip it.
FUZZ_FLAGS=-s none
# Coverage gate: percent required of each metric (docs/ENGINEERING.md, "Coverage").
COVERAGE_MIN?=90
# The one file the gate leaves out: the interactive TTY painter, which needs a terminal.
COVERAGE_IGNORE=src/tui/painter\.rs
COVERAGE_DIR=target/coverage
# Security-relevant clippy lints on shipped code (lib and bins, not tests). SECURITY_DENY is
# clean today and gates; SECURITY_REPORT has findings and is only reported (docs/ENGINEERING.md).
SECURITY_DENY=-D clippy::unwrap_used -D clippy::todo -D clippy::unimplemented -D clippy::dbg_macro -D clippy::mem_forget
SECURITY_REPORT=-W clippy::expect_used -W clippy::panic -W clippy::indexing_slicing -W clippy::arithmetic_side_effects
# Benchmarks: where results go, the regression threshold in percent (shared CI runners are
# noisy, so it is wide; docs/ENGINEERING.md, "Benchmarks"), and extra Criterion arguments.
BENCH_DIR=target/bench
BENCH_THRESHOLD?=25
BENCH_ARGS?=
# Soak test length in seconds (at least 120; CI runs an hour weekly).
SOAK_SECONDS?=600

# ----------------------------------------------------------------------------------------------------------------------
# Targets
# ----------------------------------------------------------------------------------------------------------------------

# Build the crate. Does not reformat: build targets should not rewrite sources.
build: ## Build the crate in release mode, then sweep stale artifacts
	$(if $(SWEEP),$(SWEEP) --stamp)
	${CARGO_CMD} build --release
	$(if $(SWEEP),$(SWEEP) --file)

# Full release process (ensures everything runs in the correct order)
release: fmt check clippy build test doc ## Perform a full release (fmt, check, clippy, build, test, doc)

# What CI runs: verifies formatting instead of rewriting it
ci: fmt-check check clippy test doc ## Verification-only pipeline (no source rewrites)

# Check the rust-version claim in Cargo.toml (needs rustup and the toolchain)
msrv: ## Type-check on the minimum supported Rust version (MSRV, default 1.88)
	${CARGO_CMD} +$(MSRV) check --release --all-targets --all-features --locked

# Dependency policy from deny.toml: advisories, licenses, bans, sources (cargo install cargo-deny).
deny: ## Check dependencies against deny.toml (advisories, licenses, bans, sources)
	cargo deny --locked check

# RUSTFLAGS is cleared so CI's `-D warnings` does not turn the report lints into errors.
clippy-security: ## Gate on unwrap/todo/dbg lints in shipped code; report panics, indexing, arithmetic
	RUSTFLAGS= ${CARGO_CMD} clippy --release --lib --bins --all-features --locked -- $(SECURITY_DENY)
	mkdir -p target
	RUSTFLAGS= ${CARGO_CMD} clippy --release --lib --bins --all-features --locked --message-format=json -- $(SECURITY_REPORT) > target/clippy-report.json
	python3 scripts/clippy_report.py target/clippy-report.json --markdown target/clippy-report.md

# Tests under instrumentation once, then three reports from the same profile data (needs
# cargo install cargo-llvm-cov and `rustup component add llvm-tools-preview`).
coverage: ## Coverage with HTML + lcov reports, failing under COVERAGE_MIN% (regions, functions, lines)
	mkdir -p $(COVERAGE_DIR)
	cargo llvm-cov clean --workspace
	cargo llvm-cov --no-report --all-features --locked
	cargo llvm-cov report --ignore-filename-regex '$(COVERAGE_IGNORE)' --html --output-dir $(COVERAGE_DIR)
	cargo llvm-cov report --ignore-filename-regex '$(COVERAGE_IGNORE)' --lcov --output-path $(COVERAGE_DIR)/lcov.info
	cargo llvm-cov report --ignore-filename-regex '$(COVERAGE_IGNORE)' --json --summary-only --output-path $(COVERAGE_DIR)/summary.json
	python3 scripts/coverage_gate.py $(COVERAGE_DIR)/summary.json --min regions=$(COVERAGE_MIN) --min functions=$(COVERAGE_MIN) --min lines=$(COVERAGE_MIN) --markdown $(COVERAGE_DIR)/summary.md

# Report only: needs nightly with llvm-tools-preview; the flag is unstable (docs/ENGINEERING.md).
coverage-branch: ## Branch coverage summary on nightly (not gated)
	CARGO_TARGET_DIR=target/branch ${CARGO_CMD} +nightly llvm-cov --branch --all-features --locked --ignore-filename-regex '$(COVERAGE_IGNORE)' --summary-only

coverage-open: coverage ## Open the HTML coverage report (macOS)
	open $(COVERAGE_DIR)/html/index.html

# Criterion benches (benches/*.rs). BENCH_ARGS=--quick gives a fast, rougher pass.
bench: ## Run every Criterion bench (BENCH_ARGS=--quick for a fast pass)
	${CARGO_CMD} bench --all-features --locked -- $(BENCH_ARGS)

bench-json: ## Collect the medians of the last `make bench` into target/bench/current.json
	python3 scripts/bench.py collect --out $(BENCH_DIR)/current.json

# Needs the GitHub CLI logged in. Takes the newest successful scheduled run of bench.yml on master.
bench-baseline: ## Download the last scheduled run's results as target/bench/baseline/current.json
	rm -rf $(BENCH_DIR)/baseline
	id=$$(gh run list --workflow bench.yml --branch master --event schedule --status success --limit 1 --json databaseId --jq '.[0].databaseId'); \
	test -n "$$id" || { echo "no successful scheduled bench run to compare against yet"; exit 1; }; \
	gh run download "$$id" --name bench-results --dir $(BENCH_DIR)/baseline

bench-compare: ## Compare current.json with the baseline; fails past BENCH_THRESHOLD% (BENCH_ADVISORY=1 never fails)
	python3 scripts/bench.py compare $(BENCH_DIR)/current.json $(BENCH_DIR)/baseline/current.json --threshold $(BENCH_THRESHOLD) --markdown $(BENCH_DIR)/compare.md $(if $(BENCH_ADVISORY),--advisory)

# The daemon binary under sustained traffic with a restart halfway (tests/soak.rs).
soak: ## Soak test: SOAK_SECONDS (default 600) of mixed traffic, a restart, leak and delivery checks
	SOAK_SECONDS=$(SOAK_SECONDS) ${CARGO_CMD} test --release --all-features --locked --test soak -- --ignored --nocapture soak_long

# Fuzzing needs nightly and cargo-fuzz (cargo install cargo-fuzz); it is not part of `make ci`.
fuzz-build: ## Build every fuzz target (needs nightly and cargo-fuzz)
	${CARGO_CMD} +nightly fuzz build $(FUZZ_FLAGS)

fuzz-targets: ## List the fuzz targets as a JSON array (the CI matrix reads this)
	@${CARGO_CMD} +nightly fuzz list | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read().split()))'

fuzz-replay: ## Replay each committed fuzz corpus once (fast regression pass)
	for t in $$(${CARGO_CMD} +nightly fuzz list); do ${CARGO_CMD} +nightly fuzz run $(FUZZ_FLAGS) $$t fuzz/corpus/$$t -- -runs=0 || exit 1; done

fuzz-cmin: ## Minimize one target's corpus in place before committing it: make fuzz-cmin TARGET=names
	@test -n "$(TARGET)" || { echo "usage: make fuzz-cmin TARGET=<name>"; exit 1; }
	${CARGO_CMD} +nightly fuzz cmin $(FUZZ_FLAGS) $(TARGET) fuzz/corpus/$(TARGET)

fuzz: ## Fuzz one target: make fuzz TARGET=names FUZZ_SECONDS=60
	@test -n "$(TARGET)" || { echo "usage: make fuzz TARGET=<name> [FUZZ_SECONDS=60]"; exit 1; }
	${CARGO_CMD} +nightly fuzz run $(FUZZ_FLAGS) $(TARGET) -- -max_total_time=$(FUZZ_SECONDS)

# Format the code
fmt: ## Format the code using cargo fmt
	${CARGO_CMD} fmt

# Verify formatting without rewriting (for CI / pre-push)
fmt-check: ## Fail if anything is unformatted
	${CARGO_CMD} fmt --check

# Check for errors without building. --all-targets covers bins, tests and benches.
check: ## Run cargo check --release to analyze the code without compiling
	${CARGO_CMD} check --release --all-targets --all-features --locked

# Strict linter, fails on warnings and suggests fixes
clippy: ## Run clippy and fail on warnings
	${CARGO_CMD} clippy --release --all-targets --all-features --locked -- -D warnings

# Run tests
test: ## Run all tests using cargo test --release
	${CARGO_CMD} test --release --all-features --locked

# Generate documentation. Broken intra-doc links fail the build.
doc: ## Generate project documentation using cargo doc --release
	RUSTDOCFLAGS="-D warnings" ${CARGO_CMD} doc --release --all-features --no-deps --locked

# Publish to crates.io
publish: ## Publish mentischat to crates.io
	${CARGO_CMD} publish

# Dry-run publish to crates.io
publish-dry-run: ## Dry-run publish to crates.io
	${CARGO_CMD} publish --dry-run

# Install the mentischat daemon locally
install: ## Install mentischat via cargo install
	${CARGO_CMD} install --path . --locked

# Remove the installed daemon
uninstall: ## Remove the installed mentischat binary
	${CARGO_CMD} uninstall mentischat

# Clean build artifacts
clean: ## Remove build artifacts using cargo clean
	${CARGO_CMD} clean

# Run the mentischat daemon in the foreground (Ctrl-C to stop)
run: ## Run mentischat in the foreground (cargo run --release --bin mentischat)
	${CARGO_CMD} run --release --bin mentischat

# Run the daemon with plain log lines instead of the terminal view
run-plain: ## Run mentischat with plain logs (MENTISCHAT_PLAIN=1)
	MENTISCHAT_PLAIN=1 ${CARGO_CMD} run --release --bin mentischat

# Gracefully stop a locally running mentischat daemon (SIGTERM on the mailbox port)
stop: ## Stop the running daemon with SIGTERM (port from MENTISCHAT_PORT, default 9480)
	@pids="$$(lsof -nP -iTCP:$(MENTISCHAT_PORT) -sTCP:LISTEN -t 2>/dev/null || true)"; \
	if [ -z "$$pids" ]; then \
		echo "mentischat: nothing listening on port $(MENTISCHAT_PORT)"; \
		exit 0; \
	fi; \
	echo "mentischat: sending SIGTERM to $$pids (port $(MENTISCHAT_PORT))"; \
	kill $$pids

# Show all available tasks
help tasks: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
