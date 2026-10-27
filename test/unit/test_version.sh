#!/usr/bin/env bash
#
# Checks build/version against throwaway git repos: the VERSION file decides
# the version, git only adds the build number and the dev-branch suffix.
#
set -euo pipefail

repo=$(cd "$(dirname "$0")/../.." && pwd)
work=$(mktemp -d)
checks=0
failures=0

trap 'rm -rf "$work"' EXIT

g() {
	git -c user.name=test -c user.email=test@example.com \
		-c commit.gpgsign=false -c tag.gpgsign=false -c core.hooksPath=/dev/null "$@"
}

version() {
	env -u GITHUB_HEAD_REF -u GITHUB_REF -u CI_COMMIT_REF_NAME \
		GIT_CEILING_DIRECTORIES="$work" build/version "$@"
}

expect() {
	local want=$1 got
	shift
	checks=$((checks + 1))
	if ! got=$(version "$@" 2>/dev/null); then
		echo "  FAIL: build/version $* in $(basename "$PWD") exited non-zero (want '$want')"
		failures=$((failures + 1))
	elif [ "$got" != "$want" ]; then
		echo "  FAIL: build/version $* in $(basename "$PWD"): got '$got', want '$want'"
		failures=$((failures + 1))
	fi
}

expect_error() {
	local out
	checks=$((checks + 1))
	if out=$(version "$@" 2>/dev/null) || [ -n "$out" ]; then
		echo "  FAIL: build/version $* in $(basename "$PWD") should fail with no output, got '$out'"
		failures=$((failures + 1))
	fi
}

expect_warning() {
	local want=$1 got
	shift
	checks=$((checks + 1))
	if ! got=$(version "$@" 2>"$work/stderr"); then
		echo "  FAIL: build/version $* in $(basename "$PWD") exited non-zero (want '$want' and a warning)"
		failures=$((failures + 1))
	elif [ "$got" != "$want" ]; then
		echo "  FAIL: build/version $* in $(basename "$PWD"): got '$got', want '$want'"
		failures=$((failures + 1))
	elif ! grep -q '^warning:' "$work/stderr"; then
		echo "  FAIL: build/version $* in $(basename "$PWD"): no warning on stderr"
		failures=$((failures + 1))
	fi
}

new_repo() {
	rm -rf "${work:?}/$1"
	mkdir -p "$work/$1/build"
	cp "$repo/build/version" "$work/$1/build/version"
	cd "$work/$1"
	g init -q -b master
	printf '%s\n' "$2" >VERSION
	g add -A
	g commit -qm "start at $2"
}

commit() {
	echo "$1" >>notes
	g add notes
	g commit -qm "$1"
}

write_version() {
	printf '%b' "$1" >VERSION
	g add VERSION
	g commit -qm "rewrite VERSION"
}

shallow_clone() {
	cd "$work"
	rm -rf "${work:?}/shallow"
	g clone -q --depth 1 --branch "$1" "file://$work/deep" "$work/shallow"
	cd "$work/shallow"
}

new_repo released 2.3.0
expect 2.3.0 -v
expect 1 -r
expect 2.3.0

commit "fix one"
commit "fix two"
expect 2.3.0 -v
expect 3 -r
expect 2.3.0-2

g tag 9.9.9
g tag v8.8.8
expect 2.3.0 -v
expect 3 -r

g switch -qc bump
printf '2.4.0\n' >VERSION
g commit -qam "bump to 2.4.0"
g switch -q master
commit "fix three"
g merge -q --no-ff -m "merge bump" bump
expect 2.4.0 -v
expect 1 -r
expect 2.4.0

g switch -qc feature
commit "feature work"
sha=$(git rev-parse --short=9 HEAD)
expect "2.4.0-1-g$sha" -v
expect 1 -r
expect "2.4.0-1-g$sha"

for bad in 2.3 v2.3.0 2.3.0.1 "" "2.3.0 2.3.1" "  2.3.0  " 2.3.0-rc1 2.3.0~rc1 2.3.0-5 \
	02.3.0 2.03.0 2.3.00; do
	new_repo bad "$bad"
	expect_error -v
	expect_error -r
	expect_error
done

for raw in '2.3.0' '2.3.0\r\n' '2.3.0\n\n' '\n2.3.0\n' '2.3.0 \n'; do
	new_repo raw 2.3.0
	commit "fix one"
	write_version "$raw"
	expect_error -v
	expect_error -r
	expect_error
done

new_repo deep 2.3.0
commit "fix one"
printf '2.4.0\n' >VERSION
g commit -qam "bump to 2.4.0"
commit "fix two"
commit "fix three"
expect 2.4.0 -v
expect 3 -r
g switch -qc feature
commit "feature work"
g switch -q master

shallow_clone master
expect 2.4.0 -v
expect_error -r
expect_error

shallow_clone feature
sha=$(git rev-parse --short=9 HEAD)
expect_warning "2.4.0-0-g$sha" -v
expect_warning "2.4.0-0-g$sha"
expect 1 -r

rm -rf "${work:?}/untracked"
mkdir -p "$work/untracked/build"
cp "$repo/build/version" "$work/untracked/build/version"
cd "$work/untracked"
g init -q -b master
g add -A
g commit -qm "no VERSION yet"
printf '2.3.0\n' >VERSION
expect 2.3.0 -v
expect_error -r
expect_error
g switch -qc feature
sha=$(git rev-parse --short=9 HEAD)
expect_warning "2.3.0-0-g$sha" -v
expect 1 -r

new_repo missing 2.3.0
g rm -q VERSION
g commit -qm "drop VERSION"
expect_error -v

rm -rf "${work:?}/plain"
mkdir -p "$work/plain/build"
cp "$repo/build/version" "$work/plain/build/version"
printf '2.3.0\n' >"$work/plain/VERSION"
cd "$work/plain"
expect 2.3.0 -v
expect 1 -r
expect 2.3.0

mkdir -p "$work/released/vendored/build"
cp "$repo/build/version" "$work/released/vendored/build/version"
printf '2.3.0\n' >"$work/released/vendored/VERSION"
cd "$work/released/vendored"
expect 2.3.0 -v
expect 1 -r

new_repo brokengit 2.3.0
rm -rf .git
echo "gitdir: $work/nowhere" >.git
expect_error -v
expect_error -r

new_repo cdpath 2.3.0
CDPATH=. expect 2.3.0 -v

new_repo usage 2.3.0
expect_error -x
expect_error -v -r

echo "test_version: $checks checks, $failures failures"
[ "$failures" -eq 0 ]
