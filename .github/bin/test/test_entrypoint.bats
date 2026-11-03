#!/usr/bin/env bats
#
# Unit tests for docker/docker-entrypoint.sh. ASVALIDATION_IMAGE_INFO points
# the script at a fixture, so none of this needs a built image; the release
# workflow's docker smoke test runs the same contract against the real one.
#
# The script is invoked as `sh <path>` rather than executed: /bin/sh is dash on
# the ubuntu runner, so this catches bashisms, and it keeps the test independent
# of the file's exec bit (the Dockerfile pins that with COPY --chmod).

bats_require_minimum_version 1.5.0

setup() {
    ENTRY="${BATS_TEST_DIRNAME}/../../../docker/docker-entrypoint.sh"
    export ASVALIDATION_IMAGE_INFO="${BATS_TEST_TMPDIR}/image-info"
    printf 'Aerospike asvalidation image (aerospike-asvalidation 0.0.0-1)\n' \
        > "${ASVALIDATION_IMAGE_INFO}"

    # A stand-in asvalidation: prints its arguments one per line and exits 42,
    # so both argument passing and exit-status propagation are observable.
    FAKE_BIN="${BATS_TEST_TMPDIR}/bin"
    mkdir -p "${FAKE_BIN}"
    printf '#!/bin/sh\nprintf "%%s\\n" "$@"\nexit 42\n' > "${FAKE_BIN}/asvalidation"
    chmod +x "${FAKE_BIN}/asvalidation"
}

usage_lists_asvalidation() {
    grep -qE '^  docker run --rm <image> asvalidation( |$)' <<<"$1"
}

@test "no args: usage on stdout, exit 0, title first, asvalidation listed, shell hint" {
    run -0 --separate-stderr sh "${ENTRY}"
    [ -z "${stderr}" ]
    [ "${lines[0]}" = "Aerospike asvalidation image (aerospike-asvalidation 0.0.0-1)" ]
    usage_lists_asvalidation "${output}"
    [[ "${output}" == *"--entrypoint bash"* ]]
}

@test "first argument starting with '-': exit 127, error and usage on stderr, stdout empty" {
    run -127 --separate-stderr sh "${ENTRY}" --version
    [ -z "${output}" ]
    [[ "${stderr}" == *"Error: '--version' is an option, not a binary. Name the binary first: docker run --rm <image> asvalidation --version"* ]]
    usage_lists_asvalidation "${stderr}"

    run -127 --separate-stderr sh "${ENTRY}" --
    [ -z "${output}" ]
    [[ "${stderr}" == *"Error: '--' is an option, not a binary."* ]]
}

@test "empty first argument: exit 127, error and usage on stderr" {
    run -127 --separate-stderr sh "${ENTRY}" ""
    [ -z "${output}" ]
    [[ "${stderr}" == *"Error: the first argument is empty; name the binary to run."* ]]
    usage_lists_asvalidation "${stderr}"
}

@test "first argument that is not an executable: exit 127, error and usage on stderr, stdout empty" {
    run -127 --separate-stderr sh "${ENTRY}" no-such-tool --version
    [ -z "${output}" ]
    [[ "${stderr}" == *"Error: 'no-such-tool' is not an executable in this image."* ]]
    usage_lists_asvalidation "${stderr}"
}

@test "asvalidation is exec'd with its arguments intact and its exit status propagated" {
    PATH="${FAKE_BIN}:${PATH}" run -42 --separate-stderr sh "${ENTRY}" asvalidation -n "a b"
    [ -z "${stderr}" ]
    [ "${#lines[@]}" -eq 2 ]
    [ "${lines[0]}" = "-n" ]
    [ "${lines[1]}" = "a b" ]
}

@test "passthrough: any other executable is exec'd as given" {
    run -3 --separate-stderr sh "${ENTRY}" sh -c 'exit 3'
    [ -z "${output}" ]
    [ -z "${stderr}" ]
}

@test "missing image-info: usage renders under the generic title" {
    rm -f "${ASVALIDATION_IMAGE_INFO}"
    run -0 sh "${ENTRY}"
    [ "${lines[0]}" = "Aerospike asvalidation image" ]
    usage_lists_asvalidation "${output}"
}
