#!/usr/bin/env bash
#
# Local test bed: one Aerospike server container plus a Linux build/client container.
#
#   testbed.sh image                     build the asval-build image (C clients 7.3.0 and 7.6.2)
#   testbed.sh up [81|82] [conf] [vol]   start the server (default 8.1 with aerospike-perf.conf)
#   testbed.sh down                      remove the server container
#   testbed.sh run <cmd...>              run a command in the build image, repo mounted at /src
#   testbed.sh srv-cpu                   print the server container's CPU usage in microseconds
#
# ASVAL_REPO=<path> mounts another checkout at /src (default: this one); this
# test bed checkout is always mounted at /testbed too.
#
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
testbed_root=$(cd "$here/../.." && pwd)
repo=${ASVAL_REPO:-$testbed_root}
source "$here/env.sh"

die() {
	echo "testbed: $*" >&2
	exit 1
}

cmd_image() {
	docker build -t "$ASVAL_BUILD_IMAGE" "$here"
}

cmd_up() {
	local ver=${1:-81}
	local conf=${2:-$here/aerospike-perf.conf}
	local vol=${3:-}
	local image_var="ASVAL_SERVER_$ver"
	local image=${!image_var:-}

	[ -n "$image" ] || die "unknown server version '$ver' (use 81 or 82)"
	[ -f "$conf" ] || die "no such config file: $conf"
	conf=$(cd "$(dirname "$conf")" && pwd)/$(basename "$conf")

	docker network inspect "$ASVAL_NET" >/dev/null 2>&1 ||
		docker network create "$ASVAL_NET" >/dev/null
	docker rm -f "$ASVAL_SRV" >/dev/null 2>&1 || true

	local args=(-d --name "$ASVAL_SRV" --network "$ASVAL_NET"
		--cpuset-cpus "$ASVAL_SERVER_CPUS"
		-v "$conf:/opt/asval/aerospike.conf")

	if [ -n "$vol" ]; then
		args+=(-v "$vol:/opt/aerospike/data")
	fi

	docker run "${args[@]}" "$image" --config-file /opt/asval/aerospike.conf >/dev/null

	for _ in $(seq 1 60); do
		if docker exec "$ASVAL_SRV" asinfo -v 'cluster-stable:ignore-migrations=true' \
				2>/dev/null | grep -qE '^[0-9A-F]+$'; then
			echo "server $(docker exec "$ASVAL_SRV" asinfo -v build) up as $ASVAL_SRV"
			return 0
		fi
		sleep 1
	done

	docker logs --tail 30 "$ASVAL_SRV" >&2
	die "server did not come up"
}

cmd_down() {
	docker rm -f "$ASVAL_SRV" >/dev/null 2>&1 || true
}

cmd_run() {
	local tty=()

	if [ -t 0 ] && [ -t 1 ]; then
		tty=(-t)
	fi

	docker run --rm -i ${tty[@]+"${tty[@]}"} --network "$ASVAL_NET" \
		--cpuset-cpus "$ASVAL_CLIENT_CPUS" --ulimit core=0 \
		-v "$repo:/src" -v "$testbed_root:/testbed" -w /src "$ASVAL_BUILD_IMAGE" "$@"
}

cmd_srv_cpu() {
	docker exec "$ASVAL_SRV" awk '$1 == "usage_usec" { print $2 }' /sys/fs/cgroup/cpu.stat
}

sub=${1:-}
[ -n "$sub" ] || die "usage: testbed.sh image|up|down|run|srv-cpu"
shift

case "$sub" in
	image) cmd_image "$@" ;;
	up) cmd_up "$@" ;;
	down) cmd_down "$@" ;;
	run) cmd_run "$@" ;;
	srv-cpu) cmd_srv_cpu "$@" ;;
	*) die "unknown command '$sub'" ;;
esac
