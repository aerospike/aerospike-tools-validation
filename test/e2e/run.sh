#!/usr/bin/env bash
#
# End-to-end check of asvalidation's list/map depth reporting.
#
# Starts Aerospike CE 8.1 with file storage, loads lists/maps nested around the
# 64-level limit (plus a few 200,000 levels deep), and checks asvalidation's
# depth counters, deepest level, flagged records, exit code and depth-only mode
# against the classes computed from the stored bytes. Then restarts the server
# as CE 8.2.0 on the same data, checks again, and rewrites every record to
# confirm that 8.2.0 rejects exactly the records reported as rejected by the
# server. Server 8.2.1+ also rejects the records too deep only through empty
# lists/maps (SERVER-1776), so the rewrite checks assume 8.2.0.
#
# Needs docker, plus bin/asvalidation and bin/test/gen_cdt (make e2e builds both
# and runs this script).
#
# E2E_EXEC     command prefix to run the binaries with (default: none)
# E2E_NETWORK  docker network to attach the server to instead of publishing
#              port 3000 (default: none)
#
set -euo pipefail

repo=$(cd "$(dirname "$0")/../.." && pwd)
cd "$repo"

image_81=aerospike/aerospike-server@sha256:bccce52129fecb2c33d073cc9f699737e81ebbdb5fbf3b639a32c3b2b301bb1b # 8.1.2.5
image_82=aerospike/aerospike-server@sha256:84999457c37800f711b195f562504710f7150d02134bb0c741563168a8105365 # 8.2.0.0
name=asval-e2e
volume=asval-e2e-data
exec_prefix=${E2E_EXEC:-}
network=${E2E_NETWORK:-}
host=127.0.0.1
failures=0

if [ -n "$network" ]; then
	host=$name
fi

work=out/e2e
mkdir -p "$work"

cleanup() {
	docker rm -f "$name" >/dev/null 2>&1 || true
	docker volume rm -f "$volume" >/dev/null 2>&1 || true
}

trap cleanup EXIT

run() {
	$exec_prefix "$@"
}

fail() {
	echo "  FAIL: $*"
	failures=$((failures + 1))
}

start_server() {
	local image=$1
	local args=(-d --name "$name" -v "$volume:/opt/aerospike/data"
		-v "$repo/test/e2e/aerospike.conf:/opt/asval/aerospike.conf:ro")

	if [ -n "$network" ]; then
		args+=(--network "$network")
	else
		args+=(-p 3000:3000)
	fi

	docker rm -f "$name" >/dev/null 2>&1 || true
	docker pull -q "$image" >/dev/null
	docker run "${args[@]}" "$image" --config-file /opt/asval/aerospike.conf >/dev/null

	for _ in $(seq 1 60); do
		if docker exec "$name" asinfo -v 'cluster-stable:ignore-migrations=true' \
				2>/dev/null | grep -qE '^[0-9A-F]+$'; then
			echo "server $(docker exec "$name" asinfo -v build) up"
			return 0
		fi

		sleep 1
	done

	docker logs --tail 20 "$name"
	echo "server did not start"
	exit 1
}

# Prints "deepest over strict_only flagged" from the stored bytes.
expected() {
	run bin/test/gen_cdt expect -h "$host" -n test --digests "$work/expected.digests" |
		awk '{ print $14, $6, $8, $4 }'
}

asb_digests() {
	{ grep "^+ d " "$1" || true; } | cut -c5- |
		python3 -c 'import base64, sys; [print(base64.b64decode(l).hex()) for l in sys.stdin]' |
		sort -u
}

check_validation() {
	local label=$1 mode=$2
	local exp deepest over strict flagged rc=0

	exp=$(expected)
	read -r deepest over strict flagged <<<"$exp"
	sort -u "$work/expected.digests" >"$work/expected.sorted"

	run bin/asvalidation -h "$host" -n test -o "$work/out.asb" -r \
		-m "$work/machine" --fail-on-cdt-depth $mode >"$work/log" 2>&1 || rc=$?

	local got
	got=$(grep "^CDT_DEPTH:" "$work/machine" | tail -1)

	echo "$label ${mode:-(full check)}: exit $rc, $got, expected deepest $deepest over $over strict $strict"
	[ "$rc" = 3 ] || fail "exit code $rc, expected 3"
	[ "$got" = "CDT_DEPTH:64:$deepest:$over:$strict" ] || fail "depth summary"

	asb_digests "$work/out.asb" >"$work/got.sorted"

	local missing extra
	missing=$(comm -13 "$work/got.sorted" "$work/expected.sorted" | wc -l | tr -d ' ')
	extra=$(comm -23 "$work/got.sorted" "$work/expected.sorted" | wc -l | tr -d ' ')

	echo "  records: $(wc -l <"$work/got.sorted" | tr -d ' ') written, $flagged expected, $missing missing, $extra extra"
	[ "$missing" = 0 ] && [ "$extra" = 0 ] || fail "flagged records differ"
	rm -f "$work/machine"
}

check_small_stack() {
	local rc=0

	run sh -c "ulimit -s 1024 && bin/asvalidation -h $host -n test -o $work/out.asb -r \
		--fail-on-cdt-depth >$work/log 2>&1" || rc=$?

	echo "$1 with a 1 MiB stack: exit $rc"
	[ "$rc" = 3 ] || fail "exit code $rc with a 1 MiB stack, expected 3"
}

docker volume rm -f "$volume" >/dev/null 2>&1 || true
docker volume create "$volume" >/dev/null

start_server "$image_81"
run bin/test/gen_cdt load -h "$host" -n test -d mixed -r 3000
run bin/test/gen_cdt load -h "$host" -n test -d deep -r 4 --key-offset 1000000

for server in 8.1 8.2; do
	if [ "$server" = 8.2 ]; then
		docker rm -f "$name" >/dev/null
		start_server "$image_82"
	fi

	check_validation "$server" ""
	check_validation "$server" "--cdt-check-depth-only"
	check_small_stack "$server"
done

echo "8.2.0 rewrite of the stored bytes:"
rewrite=$(run bin/test/gen_cdt rewrite -h "$host" -n test)
echo "$rewrite" | sed 's/^/  /'

over=$(expected | awk '{ print $2 }')
echo "$rewrite" | grep -qx "over accepted 0 rejected $over" ||
	fail "8.2.0 did not reject exactly the $over records reported as rejected by the server"
echo "$rewrite" | grep -qE "^(ok|strict_only) accepted [0-9]+ rejected 0$" ||
	fail "8.2.0 rejected records it should accept"
[ "$(echo "$rewrite" | grep -cE "^(ok|strict_only) accepted [0-9]+ rejected 0$")" = 2 ] ||
	fail "8.2.0 rejected records it should accept"
echo "$rewrite" | grep -qx "other_errors 0" || fail "unexpected rewrite errors"

if [ "$failures" -ne 0 ]; then
	echo "e2e: $failures failure(s)"
	exit 1
fi

echo "e2e: all checks passed"
