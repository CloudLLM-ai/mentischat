#!/usr/bin/env python3
"""Summarise `cargo clippy --message-format=json` output as Markdown: warnings
per lint, and the files with the most. A report for humans, never a gate: the
gate is the `-D` lints of `make clippy-security`.

Usage: clippy_report.py CLIPPY.json [--markdown PATH] [--top N]
"""
import argparse
import collections
import json
import pathlib
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("clippy_json")
    parser.add_argument("--markdown", metavar="PATH")
    parser.add_argument("--top", type=int, default=8, help="files to list per lint")
    args = parser.parse_args()

    per_lint = collections.Counter()
    per_file = collections.defaultdict(collections.Counter)
    for line in pathlib.Path(args.clippy_json).read_text().splitlines():
        try:
            message = json.loads(line)
        except ValueError:
            continue
        if message.get("reason") != "compiler-message":
            continue
        diagnostic = message["message"]
        code = (diagnostic.get("code") or {}).get("code", "")
        if not code.startswith("clippy::") or not diagnostic["spans"]:
            continue
        per_lint[code] += 1
        per_file[code][diagnostic["spans"][0]["file_name"]] += 1

    rows = []
    if not per_lint:
        rows.append("No findings from the report lints.")
    for lint, count in per_lint.most_common():
        rows.append(f"\n**`{lint}`**: {count}\n")
        rows.append("| file | count |")
        rows.append("|---|---:|")
        for file, n in per_file[lint].most_common(args.top):
            rows.append(f"| {file} | {n} |")
    text = "\n".join(rows) + "\n"
    sys.stdout.write(text)
    if args.markdown:
        out = pathlib.Path(args.markdown)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
