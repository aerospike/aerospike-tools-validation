#!/usr/bin/env bash
#
# Reproduces the stack overflow in check_map_keys_internal() on deeply nested
# data (namespace d5, loaded by load.sh). Runs the given binary with 8 MiB and
# 1 MiB stacks, with the map-key check on (default) and off.
#
#   crash.sh [binary]        default: bin/asvalidation
#
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
source "$repo/test/docker/env.sh"

bin=${1:-bin/asvalidation}

"$repo/test/docker/testbed.sh" run sh -c '
	bin=$1 host=$2
	for stack in 8192 1024; do
		for keys in on off; do
			flag=
			[ $keys = off ] && flag=--no-cdt-check-map-keys
			{ (ulimit -s $stack; "$bin" -h "$host" -n d5 -o /tmp/out.asb -r $flag \
				>/tmp/log 2>&1); rc=$?; } 2>/dev/null
			case $rc in
				0) result="ok" ;;
				139) result="SIGSEGV (crash)" ;;
				*) result="exit $rc" ;;
			esac
			printf "stack %5s KiB  map-key check %-3s  %s\n" $stack $keys "$result"
		done
	done
' sh "$bin" "$ASVAL_SRV"
