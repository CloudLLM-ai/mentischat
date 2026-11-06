#!/usr/bin/env python3
"""Coverage gate: read the JSON that `cargo llvm-cov report --json` writes,
print a Markdown table, and exit 1 when a metric is below its threshold.

Usage: coverage_gate.py SUMMARY.json [--min METRIC=PERCENT ...] [--worst N]

METRIC is one of regions, functions, lines, branches. `branches` is only
checked when the report has branch data (nightly with `--branch`); asking for
a threshold on a report without it is an error, not a silent pass. The table
goes to stdout, so CI appends it to $GITHUB_STEP_SUMMARY. See
docs/ENGINEERING.md for the thresholds and why.
"""
import argparse
import json
import sys

METRICS = ("regions", "functions", "lines", "branches")


def percent(entry):
    return 100.0 * entry["covered"] / entry["count"] if entry["count"] else 100.0


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("summary")
    parser.add_argument("--min", action="append", default=[], metavar="METRIC=PERCENT")
    parser.add_argument("--worst", type=int, default=8, help="files to list, least covered first")
    parser.add_argument("--markdown", metavar="PATH", help="also write the table to PATH (for $GITHUB_STEP_SUMMARY)")
    args = parser.parse_args()

    thresholds = {}
    for item in args.min:
        name, _, value = item.partition("=")
        if name not in METRICS or not value:
            parser.error(f"--min wants METRIC=PERCENT with METRIC in {METRICS}, got {item!r}")
        thresholds[name] = float(value)

    out = []
    emit = out.append
    data = json.load(open(args.summary))["data"][0]
    totals = data["totals"]
    failed = []

    emit("| metric | covered | total | percent | threshold | |")
    emit("|---|---:|---:|---:|---:|---|")
    for name in METRICS:
        entry = totals.get(name)
        has_data = entry is not None and entry["count"] > 0
        if not has_data:
            if name in thresholds:
                failed.append(f"{name}: no data in the report but a threshold was set")
            continue
        value = percent(entry)
        need = thresholds.get(name)
        ok = need is None or value >= need
        if not ok:
            failed.append(f"{name} {value:.2f}% is below {need:.2f}%")
        emit(
            f"| {name} | {entry['covered']} | {entry['count']} | {value:.2f}% | "
            f"{'' if need is None else f'{need:.0f}%'} | {'ok' if ok else '**FAIL**'} |"
        )

    files = sorted(
        data["files"], key=lambda f: percent(f["summary"]["regions"])
    )[: args.worst]
    if files:
        emit("\nLeast covered files (by regions):\n")
        emit("| file | regions | functions | lines |")
        emit("|---|---:|---:|---:|")
        for f in files:
            s = f["summary"]
            name = f["filename"].split("/src/", 1)[-1]
            emit(
                f"| src/{name} | {percent(s['regions']):.1f}% | "
                f"{percent(s['functions']):.1f}% | {percent(s['lines']):.1f}% |"
            )

    text = "\n".join(out) + "\n"
    sys.stdout.write(text)
    if args.markdown:
        with open(args.markdown, "w") as f:
            f.write(text)

    if failed:
        print("\nCoverage gate failed:\n", file=sys.stderr)
        for line in failed:
            print(f"  - {line}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
