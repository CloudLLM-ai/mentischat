# Makefile
.PHONY: build release clean fmt check test doc help tasks clippy publish publish-dry-run install start stop

default: help
CARGO_CMD=/usr/bin/env cargo
MENTISCHAT_PORT?=9480

# ----------------------------------------------------------------------------------------------------------------------
# Targets
# ----------------------------------------------------------------------------------------------------------------------

# Default target (ensures formatting before building)
build: fmt ## Build the crate in release mode (runs fmt first)
	${CARGO_CMD} build --release

# Full release process (ensures everything runs in the correct order)
release: fmt check clippy build test doc ## Perform a full release (fmt, check, clippy, build, test, doc)

# Format the code
fmt: ## Format the code using cargo fmt
	${CARGO_CMD} fmt

# Check for errors without building
check: ## Run cargo check to analyze the code without compiling
	${CARGO_CMD} check
	${CARGO_CMD} check --bin mentischat

# Strict linter, fails on warnings and suggests fixes
clippy: ## Run clippy and fail on warnings
	${CARGO_CMD} fmt
	${CARGO_CMD} clippy --all-targets --all-features -- -D warnings

# Run tests
test: ## Run all tests using cargo test
	${CARGO_CMD} test

# Generate documentation
doc: ## Generate project documentation using cargo doc
	${CARGO_CMD} doc --all-features

# Publish to crates.io
publish: ## Publish mentischat to crates.io
	${CARGO_CMD} publish

# Dry-run publish to crates.io
publish-dry-run: ## Dry-run publish to crates.io
	${CARGO_CMD} publish --dry-run

# Install the mentischat executables locally (daemon + mentis-wake wrapper)
install: ## Install mentischat and mentis-wake via cargo install
	${CARGO_CMD} install --path . --locked

# Clean build artifacts
clean: ## Remove build artifacts using cargo clean
	${CARGO_CMD} clean

# Run the mentischat daemon in the foreground (Ctrl-C to stop)
start: ## Start mentischat in the foreground (cargo run --bin mentischat)
	${CARGO_CMD} run --bin mentischat

# Gracefully stop a locally running mentischat daemon (SIGTERM on the mailbox port)
stop: ## Stop the daemon listening on :$(MENTISCHAT_PORT) with SIGTERM
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
