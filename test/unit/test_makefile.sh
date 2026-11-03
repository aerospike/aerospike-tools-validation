#!/usr/bin/env bash
#
# Checks how the Makefiles use VERSION: backup.o is rebuilt when the version
# changes, pkg/Makefile folds rcN into the package iteration, and a bad
# VERSION stops the package build too.
#
set -euo pipefail

repo=$(cd "$(dirname "$0")/../.." && pwd)
work=$(mktemp -d)
tree=$work/tree
checks=0
failures=0

trap 'rm -rf "$work"' EXIT

check() {
	checks=$((checks + 1))
	"$@"
}

fail() {
	echo "  FAIL: $*"
	failures=$((failures + 1))
}

mk() {
	env -u MAKEFLAGS -u MFLAGS -u MAKELEVEL GIT_CEILING_DIRECTORIES="$work" \
		make -C "$tree" CC="$work/cc" CLIENTREPO="$work" "$@"
}

cat >"$work/cc" <<'EOF'
#!/bin/sh
for arg; do
	[ "${prev-}" = -o ] && out=$arg
	prev=$arg
done
printf '#!/bin/sh\n# %s\n' "$*" >"$out"
chmod +x "$out"
case $out in *.o) echo "$out" >>"$(dirname "$0")/cc.log" ;; esac
EOF
chmod +x "$work/cc"

mkdir "$tree"
cp -R "$repo/Makefile" "$repo/build" "$repo/pkg" "$repo/src" "$repo/.github" "$tree"

printf '2.3.0\n' >"$tree/VERSION"
mk obj/backup.o obj/utils.o >/dev/null
check grep -q 'TOOL_VERSION="2.3.0"' "$tree/obj/backup.o" || fail "backup.o was not built with 2.3.0"
if check grep -q TOOL_VERSION "$tree/obj/utils.o"; then
	fail "utils.o is built with TOOL_VERSION but is not rebuilt when it changes"
fi

printf '2.3.1\n' >"$tree/VERSION"
touch -t 209901010000 "$tree/obj/backup.o"
mk obj/backup.o >/dev/null 2>&1
check grep -q 'TOOL_VERSION="2.3.1"' "$tree/obj/backup.o" ||
	fail "backup.o kept the old version after VERSION changed (rebuild depends on mtimes)"

compiles=$(wc -l <"$work/cc.log")
mk obj/backup.o >/dev/null
check [ "$(wc -l <"$work/cc.log")" -eq "$compiles" ] || fail "backup.o was rebuilt with no change"

rm -f "$tree/obj/version.stamp"
printf 'obj/backup.o: src/backup.c include/version.h\n' >"$tree/obj/backup.d"
check mk obj/backup.o >/dev/null 2>&1 || fail "a backup.d from an older build breaks make"

pkgvar() {
	mk -s -C pkg BUILD_DISTRO=ubuntu24.04 "print-$1"
}

check [ "$(pkgvar PKG_VERSION)" = 2.3.1 ] || fail "pkg/Makefile PKG_VERSION is not 2.3.1"
check [ "$(pkgvar ITERATION)" = 1 ] || fail "pkg/Makefile ITERATION is not 1"

printf '2.3.1-rc2\n' >"$tree/VERSION"
check [ "$(pkgvar PKG_VERSION)" = 2.3.1 ] || fail "pkg/Makefile PKG_VERSION is not 2.3.1 for 2.3.1-rc2"
check [ "$(pkgvar ITERATION)" = 2 ] || fail "pkg/Makefile ITERATION is not 2 for 2.3.1-rc2"
check [ "$(pkgvar DEB_ITERATION)" = 2ubuntu24.04 ] || fail "pkg/Makefile DEB_ITERATION is not 2ubuntu24.04"
check [ "$(mk -s -C pkg BUILD_DISTRO=ubuntu24.04 VERSION=2.3.1-abc123def print-RELEASE)" = 2.3.1-abc123def-1 ] ||
	fail "pkg/Makefile does not take VERSION from the command line"

printf 'garbage\n' >"$tree/VERSION"
check mk obj/backup.o >/dev/null 2>&1 && fail "make succeeded with a bad VERSION"
for target in deb rpm osx-pkg; do
	checks=$((checks + 1))
	if out=$(mk -s -C pkg BUILD_DISTRO=ubuntu24.04 "$target" 2>&1); then
		fail "make -C pkg $target succeeded with a bad VERSION"
	elif ! grep -q 'ERROR: VERSION is empty' <<<"$out"; then
		fail "make -C pkg $target did not stop at check-version: $out"
	fi
done

echo "test_makefile: $checks checks, $failures failures"
[ "$failures" -eq 0 ]
