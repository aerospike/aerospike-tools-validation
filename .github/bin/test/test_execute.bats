#!/usr/bin/env bats
#
# Post-install smoke tests, run against an INSTALLED asvalidation.
# Version assertions and EXPECTED_VERSION: see version_lib.sh.

setup() {
  REPO_ROOT="$(cd "$BATS_TEST_DIRNAME/../../.." && pwd)"
  VERSION_FILE="$REPO_ROOT/VERSION"
  load "$BATS_TEST_DIRNAME/version_lib.sh"
}

@test "can run asvalidation" {
  run asvalidation --usage
  [ "$status" -eq 0 ]
}

@test "asvalidation reports version" {
  run asvalidation --version
  [ "$status" -eq 0 ]
}

@test "asvalidation reports the version from the VERSION file" {
  local expected
  expected="$(expected_version "$VERSION_FILE")"

  run asvalidation --version
  [ "$status" -eq 0 ]
  echo "expected (from $expected):"
  expected_version_lines "$expected"
  echo "reported:"
  echo "$output"
  assert_version_output "$output" "$expected"
}
