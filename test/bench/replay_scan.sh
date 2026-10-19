#!/usr/bin/env bash
#
# Compares cdt_check() cost of two bench builds without network or server noise:
# each build scans a namespace with ASVAL_BENCH_REPLAY, keeps the records in
# memory, then times its own cdt_check() over them several times.
#
#   ASVAL_REPO=<checkout> test/bench/replay_scan.sh --a BIN --b BIN
#       [--passes 7] [--max 200000] [--callgrind N] ns...
#
# Each namespace runs A B B A; the table shows the best pass of each side.
# --callgrind N adds callgrind instructions per record, over the first N
# records (deterministic, independent of machine load).
#
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
testbed="$here/../docker/testbed.sh"
source "$here/../docker/env.sh"

bin_a=
bin_b=
passes=7
max=200000
cg=0
namespaces=()

while [ $# -gt 0 ]; do
	case "$1" in
		--a) bin_a=$2; shift 2 ;;
		--b) bin_b=$2; shift 2 ;;
		--passes) passes=$2; shift 2 ;;
		--max) max=$2; shift 2 ;;
		--callgrind) cg=$2; shift 2 ;;
		*) namespaces+=("$1"); shift ;;
	esac
done

[ -n "$bin_a" ] && [ -n "$bin_b" ] || { echo "replay_scan.sh: --a and --b are required" >&2; exit 2; }

"$testbed" run bash -c '
	set -euo pipefail
	a=$1 b=$2 passes=$3 max=$4 cg=$5 host=$6; shift 6

	best() {
		ASVAL_BENCH_REPLAY=$passes ASVAL_BENCH_REPLAY_MAX=$max \
			"$1" -h "$host" -n "$2" -o /tmp/replay.asb -r 2>&1 |
			sed -nE "s/.*BENCH_REPLAY records=([0-9]+) .*min_ns_per_rec=([0-9.]+).*/\1 \2/p"
	}

	instr() {
		(cd /tmp && ASVAL_BENCH_REPLAY=1 ASVAL_BENCH_REPLAY_MAX=$cg \
			valgrind --tool=callgrind --callgrind-out-file=/tmp/cg.out \
			--toggle-collect=bench_replay_pass "$1" -h "$host" -n "$2" \
			-o /tmp/replay.asb -r >/tmp/cg.log 2>&1)
		local ir n
		ir=$(awk "/^(summary|totals):/ { print \$2; exit }" /tmp/cg.out)
		n=$(sed -nE "s/.*BENCH_REPLAY records=([0-9]+).*/\1/p" /tmp/cg.log)
		awk "BEGIN { printf \"%.0f\", $ir / $n }"
	}

	printf "%-6s %9s %14s %14s %8s" ns records "A ns/rec" "B ns/rec" "B-A"
	[ "$cg" = 0 ] || printf " %14s %14s %8s" "A instr/rec" "B instr/rec" "B-A"
	echo

	for ns in "$@"; do
		read -r n a1 <<<"$(best "$a" "$ns")"
		read -r _ b1 <<<"$(best "$b" "$ns")"
		read -r _ b2 <<<"$(best "$b" "$ns")"
		read -r _ a2 <<<"$(best "$a" "$ns")"
		amin=$(awk "BEGIN { print ($a1 < $a2) ? $a1 : $a2 }")
		bmin=$(awk "BEGIN { print ($b1 < $b2) ? $b1 : $b2 }")

		printf "%-6s %9s %14.1f %14.1f %+7.1f%%" "$ns" "$n" "$amin" "$bmin" \
			"$(awk "BEGIN { print ($bmin - $amin) / $amin * 100 }")"

		if [ "$cg" != 0 ]; then
			ia=$(instr "$a" "$ns")
			ib=$(instr "$b" "$ns")
			printf " %14s %14s %+7.1f%%" "$ia" "$ib" \
				"$(awk "BEGIN { print ($ib - $ia) / $ia * 100 }")"
		fi

		echo
	done
' sh "$bin_a" "$bin_b" "$passes" "$max" "$cg" "$ASVAL_SRV" "${namespaces[@]}"
