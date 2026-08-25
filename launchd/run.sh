#!/bin/bash
# mentischat launchd entry point.
#
# launchd does NOT expand environment variables or `~` in plist strings, so
# every user-specific path is resolved here at run time. The only per-machine
# value left in com.mentischat.plist is this script's own absolute path.
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
log_dir="$HOME/Library/Logs"
mkdir -p "$log_dir"

cd "$repo"
exec >>"$log_dir/mentischat.stdout.log" 2>>"$log_dir/mentischat.stderr.log"
exec ./target/release/mentischat
