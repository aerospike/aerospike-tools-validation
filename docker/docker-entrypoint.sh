#!/bin/sh
#
# Entrypoint for the Aerospike asvalidation image.
#
# Convention:   docker run --rm <image> asvalidation [args]
#
# The binary is named on the command line, so the image has no CMD. This
# script only replaces runc's unhelpful error when the name is left out:
#   no arguments                     usage on stdout, exit 0
#   first argument empty or "-..."   error + usage on stderr, exit 127
#   first argument not resolvable    error + usage on stderr, exit 127
#   anything else                    exec "$@"   (so `<image> bash` works too)
#
# The usage title is read from ${ASVALIDATION_IMAGE_INFO}, written when the
# image is built. The variable exists so the bats tests can point the script
# at a fixture.
#
set -eu

IMAGE_INFO_FILE="${ASVALIDATION_IMAGE_INFO:-/usr/local/share/asvalidation/image-info}"

usage() {
    if [ -r "${IMAGE_INFO_FILE}" ]; then
        cat "${IMAGE_INFO_FILE}"
    else
        echo "Aerospike asvalidation image"
    fi
    echo
    echo "Name the tool to run:"
    echo
    echo "  docker run --rm <image> asvalidation [args]"
    echo
    echo "Examples:"
    echo "  docker run --rm <image> asvalidation --usage"
    echo "  docker run --rm <image> asvalidation -h <host> -n <namespace> -o -"
    echo
    echo "For a shell inside the image:"
    echo "  docker run --rm -ti --entrypoint bash <image>"
}

fail() {
    {
        echo "Error: $1"
        echo
        usage
    } >&2
    exit 127
}

if [ "$#" -eq 0 ]; then
    usage
    exit 0
fi

# Before command -v: in dash `command -v --` succeeds with no output and
# `command -v -h` is a usage error, so an option in $1 must never reach it.
case "$1" in
-*) fail "'$1' is an option, not a binary. Name the binary first: docker run --rm <image> asvalidation $1" ;;
"") fail "the first argument is empty; name the binary to run." ;;
esac

command -v "$1" > /dev/null 2>&1 \
    || fail "'$1' is not an executable in this image."

exec "$@"
