#!/usr/bin/env bash
#
# Runs run.sh over the benchmark datasets.
#
#   suite.sh --out DIR [--a BIN] [--b BIN] [--a-env ENV] [--b-env ENV]
#            [--runs N] [--repeat K] [--args "..."] [dataset...]
#
# Default datasets: d1 d2m d2l d3 d4a d4b. Without --b this is an A/A run.
#
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)

out=
pass=()
datasets=()

while [ $# -gt 0 ]; do
	case "$1" in
		--out) out=$2; shift 2 ;;
		--a|--b|--a-env|--b-env|--runs|--repeat|--args) pass+=("$1" "$2"); shift 2 ;;
		-*) echo "suite.sh: unknown option $1" >&2; exit 2 ;;
		*) datasets+=("$1"); shift ;;
	esac
done

[ -n "$out" ] || { echo "suite.sh: --out is required" >&2; exit 2; }
mkdir -p "$out"

if [ ${#datasets[@]} -eq 0 ]; then
	datasets=(d1 d2m d2l d3 d4a d4b)
fi

for ns in "${datasets[@]}"; do
	echo "=== $ns" | tee -a "$out/summary.txt"
	"$here/run.sh" --ns "$ns" --out "$out/$ns.csv" ${pass[@]+"${pass[@]}"} \
		2>"$out/$ns.log" |
		tee -a "$out/summary.txt"
done
