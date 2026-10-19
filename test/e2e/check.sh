#!/usr/bin/env bash
#
# Runs a new asvalidation build (and optionally master's) against loaded
# namespaces and checks:
#   - the summary counters equal master's, apart from the new depth block;
#   - the depth counters equal the expected classes from the stored bytes;
#   - the records written to the .asb are exactly the records expected to be
#     flagged (compared by digest).
#
#   ASVAL_REPO=<checkout with bin/asvalidation> test/e2e/check.sh \
#       [--master BIN] [--args "..."] ns...
#
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
testbed="$here/../docker/testbed.sh"
source "$here/../docker/env.sh"

master=
args=
namespaces=()

while [ $# -gt 0 ]; do
	case "$1" in
		--master) master=$2; shift 2 ;;
		--args) args=$2; shift 2 ;;
		*) namespaces+=("$1"); shift ;;
	esac
done

"$testbed" run make -s -C /testbed/test >/dev/null

"$testbed" run bash -c '
	set -euo pipefail
	host=$1 master=$2 args=$3; shift 3
	rc=0

	summary() {
		grep -E "\] +[0-9]+ " "$1" | sed -E "s/^.*\] +//"
	}

	for ns in "$@"; do
		echo "== $ns"
		bin/asvalidation -h "$host" -n "$ns" -o /tmp/new.asb -r $args >/tmp/new.log 2>&1 ||
			{ echo "  new build failed:"; tail -3 /tmp/new.log; rc=1; continue; }

		/testbed/test/bin/gen_cdt expect -h "$host" -n "$ns" --digests /tmp/exp.tsv >/tmp/exp.txt
		over=$(awk "/^  over / { print \$2 }" /tmp/exp.txt)
		strict=$(awk "/^  strict_only / { print \$2 }" /tmp/exp.txt)
		got_over=$(grep -E "Rejected by server 8.2" /tmp/new.log | awk "{ print \$(NF-4) }")
		got_strict=$(grep -E "Only via empty lists/maps" /tmp/new.log | awk "{ print \$(NF-10) }")

		printf "  depth bins: over %s (expected %s), strict_only %s (expected %s)\n" \
			"$got_over" "$over" "$got_strict" "$strict"
		[ "$got_over" = "$over" ] && [ "$got_strict" = "$strict" ] || rc=1

		{ grep "^+ d " /tmp/new.asb || true; } | cut -c5- |
			python3 -c "import sys, base64; [print(base64.b64decode(l.strip()).hex()) for l in sys.stdin]" |
			sort -u >/tmp/got.digests
		cut -f1 /tmp/exp.tsv | sort -u >/tmp/exp.digests
		missing=$(comm -13 /tmp/got.digests /tmp/exp.digests | wc -l)
		extra=$(comm -23 /tmp/got.digests /tmp/exp.digests | wc -l)

		printf "  records: %s written, %s expected, %s missing, %s extra\n" \
			"$(wc -l </tmp/got.digests)" "$(wc -l </tmp/exp.digests)" "$missing" "$extra"
		[ "$missing" = 0 ] && [ "$extra" = 0 ] || rc=1

		if [ -n "$master" ]; then
			"$master" -h "$host" -n "$ns" -o /tmp/master.asb -r $args >/tmp/master.log 2>&1 ||
				{ echo "  master failed (exit $?)"; continue; }
			summary /tmp/master.log >/tmp/master.sum
			summary /tmp/new.log | grep -vE "Nested deeper|Rejected by server|Only via empty|Fix skipped" \
				>/tmp/new.sum

			if diff -q /tmp/master.sum /tmp/new.sum >/dev/null; then
				echo "  counters: identical to master"
			else
				echo "  counters: differ from master"
				diff /tmp/master.sum /tmp/new.sum | sed "s/^/    /"
				rc=1
			fi
		fi
	done

	exit $rc
' sh "$ASVAL_SRV" "$master" "$args" "${namespaces[@]}"
