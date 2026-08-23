#!/bin/bash
# launchd/dev wrapper for mentischat. Build with `cargo build --release` first.
set -euo pipefail
cd "$(dirname "$0")/.."
exec ./target/release/mentischat
