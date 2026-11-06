#!/usr/bin/env python3
"""Criterion results as one small JSON file, and a regression check between two.

  bench.py collect [--criterion-dir target/criterion] [--out FILE]
      Gather every benchmark's median time (nanoseconds) from Criterion's
      estimates.json files into {"name": ns, ...}. Names are Criterion's own
      group/function/parameter path.

  bench.py compare CURRENT.json BASELINE.json [--threshold PERCENT] [--advisory] [--markdown PATH]
      Markdown table of the change per benchmark (stdout, for
      $GITHUB_STEP_SUMMARY) and a `::warning` annotation for each benchmark
      slower than BASELINE by more than PERCENT (default 25). Exit status 1
      when there is one, unless --advisory, which always exits 0. A benchmark
      missing from either side is listed, never fails the check.

Shared CI runners are noisy (neighbours, frequency scaling, a different CPU
between runs), so the default threshold is wide and the comparison is of
medians, not means. docs/ENGINEERING.md, "Benchmarks", has the reasoning.
"""
import argparse
import json
import pathlib
import sys


def collect(criterion_dir, out):
    results = {}
    root = pathlib.Path(criterion_dir)
    for path in sorted(root.glob("**/new/estimates.json")):
        name = "/".join(path.relative_to(root).parts[:-2])
        results[name] = json.loads(path.read_text())["median"]["point_estimate"]
    if not results:
        sys.exit(f"bench.py: no Criterion results under {root}; run `make bench` first")
    text = json.dumps(results, indent=2, sort_keys=True) + "\n"
    if out:
        pathlib.Path(out).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(out).write_text(text)
    else:
        sys.stdout.write(text)
    print(f"bench.py: {len(results)} benchmarks", file=sys.stderr)
    return 0


def human(ns):
    for unit, scale in (("s", 1e9), ("ms", 1e6), ("us", 1e3)):
        if ns >= scale:
            return f"{ns / scale:.2f} {unit}"
    return f"{ns:.0f} ns"


def compare(current, baseline, threshold, advisory, markdown):
    cur = json.loads(pathlib.Path(current).read_text())
    base = json.loads(pathlib.Path(baseline).read_text())
    regressions = []
    rows = ["| benchmark | baseline | current | change | |", "|---|---:|---:|---:|---|"]
    for name in sorted(cur.keys() | base.keys()):
        if name not in base:
            rows.append(f"| {name} | | {human(cur[name])} | new | |")
        elif name not in cur:
            rows.append(f"| {name} | {human(base[name])} | | removed | |")
        else:
            change = 100.0 * (cur[name] - base[name]) / base[name] if base[name] else 0.0
            slow = change > threshold
            if slow:
                regressions.append((name, change))
            flag = "**slower**" if slow else ("faster" if change < -threshold else "")
            rows.append(f"| {name} | {human(base[name])} | {human(cur[name])} | {change:+.1f}% | {flag} |")
    text = "\n".join(rows) + "\n"
    sys.stdout.write(text)
    if markdown:
        pathlib.Path(markdown).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(markdown).write_text(text)
    for name, change in regressions:
        print(f"::warning title=benchmark regression::{name} is {change:.1f}% slower (threshold {threshold:.0f}%)")
    if regressions and not advisory:
        print(f"bench.py: {len(regressions)} benchmark(s) regressed past {threshold:.0f}%", file=sys.stderr)
        return 1
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("collect")
    c.add_argument("--criterion-dir", default="target/criterion")
    c.add_argument("--out")
    d = sub.add_parser("compare")
    d.add_argument("current")
    d.add_argument("baseline")
    d.add_argument("--threshold", type=float, default=25.0)
    d.add_argument("--advisory", action="store_true")
    d.add_argument("--markdown", metavar="PATH", help="also write the table to PATH")
    args = parser.parse_args()
    if args.cmd == "collect":
        return collect(args.criterion_dir, args.out)
    return compare(args.current, args.baseline, args.threshold, args.advisory, args.markdown)


if __name__ == "__main__":
    sys.exit(main())
