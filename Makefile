# Makefile
.PHONY: build release clean fmt fmt-check check test doc help tasks clippy publish publish-dry-run install uninstall run run-plain stop ci msrv

default: help
CARGO_CMD=/usr/bin/env cargo
MENTISCHAT_PORT?=9480
MSRV?=1.88

# ----------------------------------------------------------------------------------------------------------------------
# Targets
# ----------------------------------------------------------------------------------------------------------------------

# Build the crate. Does not reformat: build targets should not rewrite sources.
build: ## Build the crate in release mode
	${CARGO_CMD} build --release

# Full release process (ensures everything runs in the correct order)
release: fmt check clippy build test doc ## Perform a full release (fmt, check, clippy, build, test, doc)

# What CI runs: verifies formatting instead of rewriting it
ci: fmt-check check clippy test doc ## Verification-only pipeline (no source rewrites)

# Check the rust-version claim in Cargo.toml (needs rustup and the toolchain)
msrv: ## Type-check on the minimum supported Rust version (MSRV, default 1.88)
	${CARGO_CMD} +$(MSRV) check --all-targets --all-features --locked

# Format the code
fmt: ## Format the code using cargo fmt
	${CARGO_CMD} fmt

# Verify formatting without rewriting (for CI / pre-push)
fmt-check: ## Fail if anything is unformatted
	${CARGO_CMD} fmt --check

# Check for errors without building. --all-targets covers bins, tests and benches.
check: ## Run cargo check to analyze the code without compiling
	${CARGO_CMD} check --all-targets --all-features

# Strict linter, fails on warnings and suggests fixes
clippy: ## Run clippy and fail on warnings
	${CARGO_CMD} clippy --all-targets --all-features -- -D warnings

# Run tests
test: ## Run all tests using cargo test
	${CARGO_CMD} test --all-features

# Generate documentation. Broken intra-doc links fail the build.
doc: ## Generate project documentation using cargo doc
	RUSTDOCFLAGS="-D warnings" ${CARGO_CMD} doc --all-features --no-deps

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
run: ## Run mentischat in the foreground (cargo run --bin mentischat)
	${CARGO_CMD} run --bin mentischat

# Run the daemon with plain log lines instead of the terminal view
run-plain: ## Run mentischat with plain logs (MENTISCHAT_PLAIN=1)
	MENTISCHAT_PLAIN=1 ${CARGO_CMD} run --bin mentischat

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
