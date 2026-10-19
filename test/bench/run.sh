#!/usr/bin/env bash
#
# A/B benchmark of two asvalidation-bench builds (or one build against itself for
# an A/A noise-floor run) against one namespace of the test bed server.
#
#   run.sh --ns d2m [--a bin/asvalidation-bench] [--b bin/other-bench]
#          [--a-env "ASVAL_BENCH_NOOP=1"] [--b-env ...] [--args "..."]
#          [--runs 5] [--repeat 1] [--out results.csv]
#
# One discarded warmup per side, then --runs measured runs per side, in
# alternating ABBA order. Each run is --repeat back-to-back scans whose numbers are
# summed. Prints a summary table (see stats.py) and keeps the raw rows in --out.
#
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
testbed="$repo/test/docker/testbed.sh"
source "$repo/test/docker/env.sh"

ns=
bin_a=bin/asvalidation-bench
bin_b=
env_a=
env_b=
extra_args=
runs=5
repeat=1
out=

while [ $# -gt 0 ]; do
	case "$1" in
		--ns) ns=$2; shift 2 ;;
		--a) bin_a=$2; shift 2 ;;
		--b) bin_b=$2; shift 2 ;;
		--a-env) env_a=$2; shift 2 ;;
		--b-env) env_b=$2; shift 2 ;;
		--args) extra_args=$2; shift 2 ;;
		--runs) runs=$2; shift 2 ;;
		--repeat) repeat=$2; shift 2 ;;
		--out) out=$2; shift 2 ;;
		*) echo "run.sh: unknown option $1" >&2; exit 2 ;;
	esac
done

[ -n "$ns" ] || { echo "run.sh: --ns is required" >&2; exit 2; }
[ -n "$bin_b" ] || bin_b=$bin_a
[ -n "$out" ] || out=$(mktemp "${TMPDIR:-/tmp}/asval-bench.XXXXXX")

echo "side,run,records,wall_ns,worker_cpu_ns,check_ns,max_rss_kb,srv_cpu_us" >"$out"

one_run() {
	local side=$1 run=$2 bin=$3 env=$4
	local before after line

	before=$("$testbed" srv-cpu)
	line=$("$testbed" run env $env sh -c '
		bin=$1 ns=$2 repeat=$3 host=$4; shift 4
		recs=0 wall=0 cpu=0 chk=0 rss=0
		i=0
		while [ $i -lt $repeat ]; do
			/usr/bin/time -f "TIME rss_kb=%M" "$bin" -h "$host" -n "$ns" \
				-o /tmp/out.asb -r "$@" >/tmp/log 2>&1 || { cat /tmp/log >&2; exit 1; }
			b=$(grep "^BENCH" /tmp/log)
			t=$(grep "^TIME" /tmp/log)
			for kv in $b $t; do
				case $kv in
					records=*) recs=$((recs + ${kv#*=})) ;;
					wall_ns=*) wall=$((wall + ${kv#*=})) ;;
					worker_cpu_ns=*) cpu=$((cpu + ${kv#*=})) ;;
					check_ns=*) chk=$((chk + ${kv#*=})) ;;
					rss_kb=*) [ ${kv#*=} -gt $rss ] && rss=${kv#*=} ;;
				esac
			done
			i=$((i + 1))
		done
		echo "$recs,$wall,$cpu,$chk,$rss"
	' sh "$bin" "$ns" "$repeat" "$ASVAL_SRV" $extra_args)
	after=$("$testbed" srv-cpu)

	if [ "$run" -ge 0 ]; then
		echo "$side,$run,$line,$((after - before))" >>"$out"
	fi

	printf '  %s run %-2s %s\n' "$side" "$run" "$line" >&2
}

echo "bench ns=$ns A=$bin_a [$env_a] B=$bin_b [$env_b] runs=$runs repeat=$repeat" >&2
echo "args: $extra_args" >&2

one_run A -1 "$bin_a" "$env_a"
one_run B -1 "$bin_b" "$env_b"

for ((r = 0; r < runs; r++)); do
	if ((r % 2 == 0)); then
		one_run A "$r" "$bin_a" "$env_a"
		one_run B "$r" "$bin_b" "$env_b"
	else
		one_run B "$r" "$bin_b" "$env_b"
		one_run A "$r" "$bin_a" "$env_a"
	fi
done

python3 "$here/stats.py" "$out"
echo "raw: $out"
