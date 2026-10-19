#!/usr/bin/env bash
#
# Replay-harness gate: ns per bin (min of 3 runs) and callgrind instructions per
# bin for each validation path, on corpora dumped with "replay dump".
#
#   ASVAL_REPO=<walker checkout> test/bench/replay.sh
#
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
testbed="$here/../docker/testbed.sh"

"$testbed" run make -s -C /testbed/test replay WALKER=/src

"$testbed" run bash -c '
	set -euo pipefail
	bin=/testbed/test/bin
	paths="noop baseline walk A B"

	# corpus:iterations for timing:bins for callgrind
	for spec in d2m:1:200 d2l:2:20000 d3:2:200 d4a:1:1000 d4b:1:1000; do
		IFS=: read -r ns iters cg_bins <<<"$spec"
		corpus=$bin/corpus-$ns.bin

		printf "\n%s\n" "$ns"
		printf "  %-9s %12s %14s\n" path "ns/bin" "instr/bin"

		for p in $paths; do
			best=
			for _ in 1 2 3; do
				v=$($bin/replay time $corpus -p $p -i $iters | sed -E "s/.*ns\/bin ([0-9.]+).*/\1/")
				if [ -z "$best" ] || awk "BEGIN { exit !($v < $best) }"; then
					best=$v
				fi
			done

			valgrind --tool=callgrind --callgrind-out-file=/tmp/cg.out \
				--toggle-collect=run_path $bin/replay time $corpus -p $p -m $cg_bins \
				>/dev/null 2>&1
			ir=$(awk "/^(summary|totals):/ { print \$2; exit }" /tmp/cg.out)

			printf "  %-9s %12s %14.1f\n" $p $best "$(awk "BEGIN { print $ir / $cg_bins }")"
		done
	done
'
