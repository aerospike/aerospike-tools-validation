#!/usr/bin/env bash
set -xeuo pipefail

function assert_dynamic_deps() {
	local allowed="libc.so.6 libm.so.6 libpthread.so.0 libdl.so.2 librt.so.1
		libgcc_s.so.1 libz.so.1 ld-linux-x86-64.so.2 ld-linux-aarch64.so.1"

	local lib fail=0
	local bin="bin/asvalidation"
	local needed
	needed=$(readelf -d "$bin" | awk '/\(NEEDED\)/ { gsub(/[][]/, "", $NF); print $NF }')
	echo "asvalidation DT_NEEDED:" $needed
	for lib in $needed; do
		# A here-string, not a pipe: under pipefail, grep -q exiting on the
		# first match can kill printf with SIGPIPE and fail the check.
		if ! grep -qxF "$lib" <<<"$(printf '%s\n' $allowed)"; then
			echo "asvalidation has unexpected dynamic dependency $lib; link it statically or add it to the allowlist and the package depends" >&2
			fail=1
		fi
	done
	return $fail
}

function build_packages() {
	if [ "${ENV_DISTRO:-}" = "" ]; then
		echo "ENV_DISTRO is not set" >&2
		return 1
	fi
	GIT_DIR=$(git rev-parse --show-toplevel)
	cd "$GIT_DIR" || exit 1

	export CLIENTREPO="$GIT_DIR/client"
	make -C "$CLIENTREPO" -j"$(nproc)"

	# VERSION goes on the command line so the binary and the package names
	# both use it; build/version would add a dev-branch suffix of its own.
	VERSION=${PKG_VERSION:-$(build/version -v)}
	make clean
	make VERSION="$VERSION" OPENSSL_STATIC_PATH=/usr/local/lib

	assert_dynamic_deps

	echo "build_package.sh version: ${VERSION}"
	echo "building package for $BUILD_DISTRO"

	case "$ENV_DISTRO" in
	ubuntu* | debian*) make -C pkg deb VERSION="$VERSION" ;;
	el* | amzn*) make -C pkg rpm VERSION="$VERSION" ;;
	*)
		echo "no package format for $ENV_DISTRO" >&2
		return 1
		;;
	esac
}
