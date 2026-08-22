#!/bin/bash
# launchd/dev wrapper for llm_chat. Build with `cargo build --release` first.
set -euo pipefail
cd "$(dirname "$0")/.."
exec ./target/release/llm_chat
