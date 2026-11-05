#!/usr/bin/env bats
#
# Runs the "Read and validate VERSION" step of build-and-release.yml, read from
# the workflow with yq, in a throwaway repo. The step's env: block is resolved
# from the dispatch inputs each test passes.

setup() {
  WORKFLOW="$(cd "$BATS_TEST_DIRNAME/../../workflows" && pwd)/build-and-release.yml"
  REPO="$BATS_TEST_TMPDIR/repo"
  OUT="$BATS_TEST_TMPDIR/github_output"
  mkdir -p "$REPO/build"
  cp "$BATS_TEST_DIRNAME/../../../build/version" "$REPO/build/version"
  printf '2.3.1-rc1\n' >"$REPO/VERSION"
  git_ init -q -b master
  git_ add -A
  git_ commit -qm start
  : >"$OUT"
}

git_() {
  git -C "$REPO" -c user.name=test -c user.email=test@example.com \
    -c commit.gpgsign=false -c tag.gpgsign=false -c core.hooksPath=/dev/null "$@"
}

step() {
  yq ".jobs[\"validate-inputs\"].steps[] | select(.id == \"read\") | $1" "$WORKFLOW"
}

validate() {
  local script line key value env_args=()
  script="$(step .run)"
  if [ -z "$script" ] || [ "$script" = null ]; then
    echo "no Read and validate VERSION step in $WORKFLOW" >&2
    return 1
  fi
  while IFS= read -r line; do
    key="${line%%=*}"
    value="${line#*=}"
    case "$value" in
    '${{ inputs.release }}') env_args+=("$key=$2") ;;
    '${{ inputs.skip_tagging }}') env_args+=("$key=$3") ;;
    *)
      echo "unhandled env in the step: $line" >&2
      return 1
      ;;
    esac
  done < <(step '.env // {} | to_entries | .[] | .key + "=" + .value')
  cd "$REPO" || return 1
  run env -i PATH="$PATH" HOME="$HOME" GITHUB_REF="$1" GITHUB_OUTPUT="$OUT" \
    ${env_args[@]+"${env_args[@]}"} bash --noprofile --norc -eo pipefail -c "$script"
}

@test "a dry run on master is not blocked by a tag for VERSION" {
  git_ tag 2.3.1-rc1
  validate refs/heads/master false false
  [ "$status" -eq 0 ]
  grep -qx 'VERSION=2.3.1-rc1' "$OUT"
}

@test "a release on master refuses a VERSION that is already tagged" {
  git_ tag 2.3.1-rc1
  validate refs/heads/master true false
  [ "$status" -eq 1 ]
  [[ "$output" == *"Tag '2.3.1-rc1' already exists"* ]]
}

@test "a release on master refuses a VERSION tagged with a v prefix" {
  git_ tag v2.3.1-rc1
  validate refs/heads/master true false
  [ "$status" -eq 1 ]
  [[ "$output" == *"Tag 'v2.3.1-rc1' already exists"* ]]
}

@test "a release on master goes ahead when only older versions are tagged" {
  git_ tag 2.3.0
  git_ tag v2.3.0
  validate refs/heads/master true false
  [ "$status" -eq 0 ]
  grep -qx 'VERSION=2.3.1-rc1' "$OUT"
}

@test "skip_tagging lets a release on master reuse a tagged VERSION" {
  git_ tag 2.3.1-rc1
  validate refs/heads/master true true
  [ "$status" -eq 0 ]
}

@test "a release off master is not checked against tags" {
  git_ tag 2.3.1-rc1
  validate refs/heads/feature true false
  [ "$status" -eq 0 ]
}

@test "a malformed VERSION is refused before the tag check" {
  printf '2.3.1-RC1\n' >"$REPO/VERSION"
  validate refs/heads/master false false
  [ "$status" -ne 0 ]
  [[ "$output" == *"VERSION must be one line"* ]]
  [ ! -s "$OUT" ]
}
