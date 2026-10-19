#!/usr/bin/env bash
#
# Upgrade-impact repro: load deep list/map data on 8.1 with file storage, back
# it up, swap the server container to 8.2 on the same data volume, and show
# what works and what breaks. Prints a report; safe to rerun (volumes are
# recreated).
#
#   test/upgrade/run.sh | tee /tmp/upgrade-report.txt
#
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
testbed="$repo/test/docker/testbed.sh"
source "$repo/test/docker/env.sh"

conf="$repo/test/docker/aerospike-upgrade.conf"
data_vol=asval-upgrade-data
backup_vol=asval-upgrade-backup
d4=(--range 0:4200)
d5=(--range 1000000:4)

step() {
	printf '\n==== %s\n' "$*"
}

gen() {
	"$testbed" run test/bin/gen_cdt "$1" -h "$ASVAL_SRV" "${@:2}"
}

probe() {
	"$testbed" run test/bin/probe "$1" -h "$ASVAL_SRV" -n up "${@:2}"
}

tools() {
	docker run --rm --network "$ASVAL_NET" --cpuset-cpus "$ASVAL_CLIENT_CPUS" \
		-v "$backup_vol:/backup" "$ASVAL_BACKUP_IMAGE" "$@"
}

restore() {
	local rc=0
	local log

	log=$(tools asrestore -h "$ASVAL_SRV" -n up,up2 -d /backup/up "$@" 2>&1) || rc=$?
	echo "$log" | tail -4
	echo "  asrestore exit code $rc"
	objects up2
}

objects() {
	docker exec "$ASVAL_SRV" asinfo -v "namespace/$1" | tr ';' '\n' |
		grep '^objects=' | sed "s/^/  $1 /"
}

server_warnings() {
	docker logs "$ASVAL_SRV" 2>&1 | grep -E 'WARNING|CRITICAL' |
		grep -vE '\((os|config)\)' | sed -E 's/^.*GMT: //; s/[0-9a-f]{40}/<digest>/g' |
		sort | uniq -c | sort -rn | head -12 || true
}

"$testbed" run make -s -C test

step "fresh data and backup volumes"
"$testbed" down
docker volume rm -f "$data_vol" "$backup_vol" >/dev/null
docker volume create "$data_vol" >/dev/null
docker volume create "$backup_vol" >/dev/null

step "8.1: start with file storage"
"$testbed" up 81 "$conf" "$data_vol"

step "8.1: load 4,200 D4-style records (every other one has a deep bin) and 4 records 200,000 levels deep"
gen load -n up -d mixed -r 4200 --violators 2100
gen load -n up -d deep -r 4 --depth 200000 --key-offset 1000000
gen expect -n up

step "8.1: UDF reads the deep bin (8.1.2.5 is a hotfix build with the bounded deserializer)"
probe udf "${d4[@]}"

step "8.1: asbackup"
tools asbackup -h "$ASVAL_SRV" -n up -d /backup/up -r 2>&1 | tail -2

step "swap: stop 8.1, start 8.2 on the same data volume"
"$testbed" down
"$testbed" up 82 "$conf" "$data_vol"
objects up

step "8.2: stored bytes are unchanged (same expected classes)"
gen expect -n up

step "8.2: point reads, client defaults (max_error_rate 100)"
probe read "${d4[@]}" "${d5[@]}"

step "8.2: point reads with the client error limit raised (max_error_rate 100000)"
probe read "${d4[@]}" "${d5[@]}" --max-error-rate 100000

step "8.2: scans"
probe scan

step "8.2: whole-record rewrite of exactly the stored bytes"
probe rewrite "${d4[@]}" "${d5[@]}"

step "8.2: UDF reads the deep bin"
probe udf "${d4[@]}"

step "8.2: asrestore of the 8.1 backup into namespace up2 (default: batch writes)"
restore

step "8.2: asrestore again with --disable-batch-writes (single puts)"
docker exec "$ASVAL_SRV" asinfo -v 'truncate-namespace:namespace=up2' >/dev/null
sleep 2
restore --disable-batch-writes

step "8.2: asrestore with --disable-batch-writes --ignore-record-error"
docker exec "$ASVAL_SRV" asinfo -v 'truncate-namespace:namespace=up2' >/dev/null
sleep 2
restore --disable-batch-writes --ignore-record-error

step "8.2: UDF writes the bin back (keys 0: depth 63, 4: depth 65, 8: depth 200, 32: empty list at 65)"
probe udf-write --range 0:1 --range 4:1 --range 8:1 --range 32:1

step "8.2: server warnings (counts)"
server_warnings
