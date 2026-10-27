#!/usr/bin/env bash
#
# Checks how the Makefiles use VERSION: backup.o is rebuilt when the version
# changes, and a bad VERSION stops the package Makefiles too.
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
cp -R "$repo/Makefile" "$repo/build" "$repo/pkg" "$repo/src" "$tree"

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

for f in pkg/Makefile.deb pkg/Makefile.rpm; do
	check [ "$(mk -s -f "$f" print-REV)" = 2.3.1 ] || fail "$f print-REV is not 2.3.1"
	check [ "$(mk -s -f "$f" print-BUILD_NUMBER)" = 1 ] || fail "$f print-BUILD_NUMBER is not 1"
done

printf 'garbage\n' >"$tree/VERSION"
check mk obj/backup.o >/dev/null 2>&1 && fail "make succeeded with a bad VERSION"
for f in pkg/Makefile.deb pkg/Makefile.rpm; do
	for var in REV BUILD_NUMBER; do
		checks=$((checks + 1))
		if out=$(mk -s -f "$f" "print-$var" 2>/dev/null); then
			fail "make -f $f print-$var succeeded with a bad VERSION (printed '$out')"
		fi
	done
done

echo "test_makefile: $checks checks, $failures failures"
[ "$failures" -eq 0 ]
