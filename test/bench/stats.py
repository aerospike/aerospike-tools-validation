#!/usr/bin/env python3
"""Summarize a run.sh results CSV: min and median per side, and B vs A deltas.

The min is the primary number: on a shared machine interference only ever adds
time, so the best run is the closest to the true cost.
"""

import csv
import statistics
import sys

METRICS = [
    ("worker CPU ns/rec", lambda r: r["worker_cpu_ns"] / r["records"]),
    ("check ns/rec", lambda r: r["check_ns"] / r["records"]),
    ("wall ms", lambda r: r["wall_ns"] / 1e6),
    ("krec/s", lambda r: r["records"] / (r["wall_ns"] / 1e9) / 1e3),
    ("max RSS MiB", lambda r: r["max_rss_kb"] / 1024),
    ("server CPU ns/rec", lambda r: r["srv_cpu_us"] * 1000 / r["records"]),
]


def load(path):
    sides = {"A": [], "B": []}

    with open(path) as f:
        for row in csv.DictReader(f):
            side = row.pop("side")
            row.pop("run")
            sides[side].append({k: float(v) for k, v in row.items()})

    return sides


def main():
    sides = load(sys.argv[1])
    n = len(sides["A"])
    records = int(sides["A"][0]["records"]) if n else 0

    print(f"\n{n} runs per side, {records} records per run\n")
    print(f"{'metric':<20} {'A min':>10} {'B min':>10} {'B-A min':>8}"
          f" {'A median':>10} {'B median':>10} {'B-A med':>8} {'A spread':>9}")

    for name, fn in METRICS:
        a = [fn(r) for r in sides["A"]]
        b = [fn(r) for r in sides["B"]]
        a_med = statistics.median(a)
        b_med = statistics.median(b)
        spread = (max(a) - min(a)) / a_med * 100 if a_med else 0
        d_min = (min(b) - min(a)) / min(a) * 100 if min(a) else 0
        d_med = (b_med - a_med) / a_med * 100 if a_med else 0
        print(f"{name:<20} {min(a):>10.1f} {min(b):>10.1f} {d_min:>+7.1f}%"
              f" {a_med:>10.1f} {b_med:>10.1f} {d_med:>+7.1f}% {spread:>8.1f}%")


if __name__ == "__main__":
    main()
