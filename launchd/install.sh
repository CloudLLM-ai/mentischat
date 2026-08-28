#!/bin/bash
# Generate ~/Library/LaunchAgents/com.mentischat.plist for THIS checkout and
# load it. Re-run after moving the repo; the plist's only per-machine value is
# the absolute path to run.sh.
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
label="com.mentischat"
target="$HOME/Library/LaunchAgents/$label.plist"

mkdir -p "$(dirname "$target")"
sed "s|<string>/Users/[^<]*/launchd/run.sh</string>|<string>$repo/launchd/run.sh</string>|" \
  "$repo/launchd/$label.plist" > "$target"

if launchctl print "gui/$UID/$label" >/dev/null 2>&1; then
  launchctl bootout "gui/$UID/$label" || true
fi
launchctl bootstrap "gui/$UID" "$target"
echo "loaded $label from $target (run.sh: $repo/launchd/run.sh)"
echo "logs: ~/Library/Logs/mentischat.{stdout,stderr}.log"
