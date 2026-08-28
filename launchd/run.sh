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

# Prefer the installed binary (`make install`) over the build tree, so a
# `make clean` cannot leave the service pointing at a path that no longer
# exists. Fail loudly rather than letting launchd retry a missing file.
for candidate in \
  "${MENTISCHAT_BIN:-}" \
  "$HOME/.cargo/bin/mentischat" \
  "$repo/target/release/mentischat"
do
  if [ -n "$candidate" ] && [ -x "$candidate" ]; then
    echo "mentischat: launching $candidate at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    exec "$candidate"
  fi
done

echo "mentischat: no executable found. Run 'make install' (preferred) or" \
     "'make build', or set MENTISCHAT_BIN to an absolute path." >&2
exit 1
