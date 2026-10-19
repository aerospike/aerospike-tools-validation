#!/usr/bin/env bash
#
# Loads the benchmark datasets into the test bed server (aerospike-perf.conf
# namespaces), then prints the expected classes computed from the stored bytes.
#
#   load.sh [dataset...]     default: all of d1 d2m d2l d3 d4a d4b d5
#
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
testbed="$repo/test/docker/testbed.sh"
source "$repo/test/docker/env.sh"

seed=${ASVAL_SEED:-1}

declare_args() {
	case "$1" in
		d1) echo "-d scalar -r 5000000" ;;
		d2m) echo "-d map -r 2000" ;;
		d2l) echo "-d list -r 300000" ;;
		d3) echo "-d ordered -r 4000" ;;
		d4a) echo "-d mixed -r 4000 --violators 200" ;;
		d4b) echo "-d mixed -r 4000 --violator-pct 10" ;;
		d5) echo "-d deep -r 4 --depth 200000" ;;
		*) echo "load.sh: unknown dataset $1" >&2; exit 2 ;;
	esac
}

datasets=("$@")

if [ ${#datasets[@]} -eq 0 ]; then
	datasets=(d1 d2m d2l d3 d4a d4b d5)
fi

"$testbed" run make -s -C test

for ns in "${datasets[@]}"; do
	docker exec "$ASVAL_SRV" asinfo -v "truncate-namespace:namespace=$ns" >/dev/null
	sleep 2
	"$testbed" run test/bin/gen_cdt load -h "$ASVAL_SRV" -n "$ns" -S "$seed" \
		$(declare_args "$ns")
	"$testbed" run test/bin/gen_cdt expect -h "$ASVAL_SRV" -n "$ns"
done
